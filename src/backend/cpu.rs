use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use argon2::{Algorithm, Argon2, Version};
use blocknet_pow_spec::{pow_params, POW_OUTPUT_LEN};
use crossbeam_channel::Sender;

use crate::backend::{
    BackendEvent, BackendInstanceId, MiningSolution, PowBackend, WorkAssignment, WORK_ID_MAX,
};
use crate::types::hash_meets_target;

const HASH_BATCH_SIZE: u64 = 64;
const HASH_FLUSH_INTERVAL: Duration = Duration::from_millis(50);
const SOLVED_MASK: u64 = 1u64 << 63;

struct ControlState {
    shutdown: bool,
    generation: u64,
    work: Option<Arc<WorkAssignment>>,
}

struct Shared {
    started: AtomicBool,
    instance_id: AtomicU64,
    solution_state: AtomicU64,
    work_generation: AtomicU64,
    active_workers: AtomicUsize,
    work_control: Mutex<ControlState>,
    work_cv: Condvar,
    idle_lock: Mutex<()>,
    idle_cv: Condvar,
    hash_slots: Vec<AtomicU64>,
    event_sink: RwLock<Option<Sender<BackendEvent>>>,
}

pub struct CpuBackend {
    threads: usize,
    shared: Arc<Shared>,
    worker_handles: Vec<JoinHandle<()>>,
}

impl CpuBackend {
    pub fn new(threads: usize) -> Self {
        let lanes = threads.max(1);
        Self {
            threads,
            shared: Arc::new(Shared {
                started: AtomicBool::new(false),
                instance_id: AtomicU64::new(0),
                solution_state: AtomicU64::new(0),
                work_generation: AtomicU64::new(0),
                active_workers: AtomicUsize::new(0),
                work_control: Mutex::new(ControlState {
                    shutdown: false,
                    generation: 0,
                    work: None,
                }),
                work_cv: Condvar::new(),
                idle_lock: Mutex::new(()),
                idle_cv: Condvar::new(),
                hash_slots: (0..lanes).map(|_| AtomicU64::new(0)).collect(),
                event_sink: RwLock::new(None),
            }),
            worker_handles: Vec::new(),
        }
    }
}

impl PowBackend for CpuBackend {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn lanes(&self) -> usize {
        self.threads.max(1)
    }

    fn set_instance_id(&mut self, id: BackendInstanceId) {
        self.shared.instance_id.store(id, Ordering::Release);
    }

    fn set_event_sink(&mut self, sink: Sender<BackendEvent>) {
        if let Ok(mut slot) = self.shared.event_sink.write() {
            *slot = Some(sink);
        }
    }

    fn start(&mut self) -> Result<()> {
        if self.shared.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        self.shared.solution_state.store(0, Ordering::SeqCst);
        self.shared.work_generation.store(0, Ordering::SeqCst);
        self.shared.active_workers.store(0, Ordering::SeqCst);
        reset_hash_slots(&self.shared.hash_slots);

        {
            let mut control = self
                .shared
                .work_control
                .lock()
                .map_err(|_| anyhow!("CPU control lock poisoned"))?;
            control.shutdown = false;
            control.generation = 0;
            control.work = None;
        }

        for thread_idx in 0..self.threads.max(1) {
            let shared = Arc::clone(&self.shared);
            self.worker_handles
                .push(thread::spawn(move || cpu_worker_loop(shared, thread_idx)));
        }

        Ok(())
    }

    fn stop(&mut self) {
        if !self.shared.started.swap(false, Ordering::SeqCst) {
            return;
        }

        request_shutdown(&self.shared);
        let _ = wait_for_idle(&self.shared);

        for handle in self.worker_handles.drain(..) {
            let _ = handle.join();
        }

        self.shared.solution_state.store(0, Ordering::SeqCst);
        self.shared.work_generation.store(0, Ordering::SeqCst);
        self.shared.active_workers.store(0, Ordering::SeqCst);
        reset_hash_slots(&self.shared.hash_slots);

        if let Ok(mut control) = self.shared.work_control.lock() {
            control.shutdown = false;
            control.generation = 0;
            control.work = None;
        }
    }

    fn assign_work(&self, work: WorkAssignment) -> Result<()> {
        if !self.shared.started.load(Ordering::SeqCst) {
            return Err(anyhow!("CPU backend is not started"));
        }
        if work.template.work_id == 0 || work.template.work_id > WORK_ID_MAX {
            return Err(anyhow!("invalid work id {}", work.template.work_id));
        }

        let work_id = work.template.work_id;
        let work = Arc::new(work);

        self.shared.solution_state.store(work_id, Ordering::Release);

        let generation = {
            let mut control = self
                .shared
                .work_control
                .lock()
                .map_err(|_| anyhow!("CPU control lock poisoned"))?;
            control.generation = control.generation.wrapping_add(1).max(1);
            control.work = Some(work);
            control.generation
        };

        self.shared
            .work_generation
            .store(generation, Ordering::Release);
        self.shared.work_cv.notify_all();
        Ok(())
    }

    fn quiesce(&self) -> Result<()> {
        if !self.shared.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        request_work_pause(&self.shared)?;
        wait_for_idle(&self.shared)
    }

    fn take_hashes(&self) -> u64 {
        self.shared
            .hash_slots
            .iter()
            .map(|slot| slot.swap(0, Ordering::AcqRel))
            .sum()
    }

    fn kernel_bench(&self, seconds: u64, shutdown: &AtomicBool) -> Result<u64> {
        let lanes = self.threads.max(1);
        let stop_at = Instant::now() + Duration::from_secs(seconds.max(1));
        let total_hashes = AtomicU64::new(0);

        thread::scope(|scope| {
            for lane in 0..lanes {
                let total_hashes = &total_hashes;
                scope.spawn(move || {
                    let params = match pow_params() {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

                    let mut header_base = [0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN];
                    for (i, byte) in header_base.iter_mut().enumerate() {
                        *byte = (i as u8).wrapping_mul(31).wrapping_add(7);
                    }

                    let mut output = [0u8; POW_OUTPUT_LEN];
                    let mut nonce = lane as u64;
                    let mut local_hashes = 0u64;
                    let stride = lanes as u64;

                    while Instant::now() < stop_at && !shutdown.load(Ordering::Relaxed) {
                        let nonce_bytes = nonce.to_le_bytes();
                        if argon2
                            .hash_password_into(&nonce_bytes, &header_base, &mut output)
                            .is_err()
                        {
                            break;
                        }
                        nonce = nonce.wrapping_add(stride);
                        local_hashes = local_hashes.saturating_add(1);
                    }

                    total_hashes.fetch_add(local_hashes, Ordering::Relaxed);
                });
            }
        });

        Ok(total_hashes.load(Ordering::Relaxed))
    }
}

fn cpu_worker_loop(shared: Arc<Shared>, thread_idx: usize) {
    let params = match pow_params() {
        Ok(p) => p,
        Err(_) => {
            emit_error(
                &shared,
                format!("cpu thread {thread_idx}: invalid Argon2 params"),
            );
            request_shutdown(&shared);
            return;
        }
    };

    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut output = [0u8; POW_OUTPUT_LEN];
    let mut local_generation = 0u64;
    let mut local_work: Option<Arc<WorkAssignment>> = None;
    let mut worker_active = false;
    let mut nonce = 0u64;
    let mut lane_iters = 0u64;
    let mut pending_hashes = 0u64;
    let mut last_flush = Instant::now();

    loop {
        let global_generation = shared.work_generation.load(Ordering::Acquire);
        if global_generation != local_generation || local_work.is_none() {
            if local_work.is_some() {
                flush_hashes(&shared, thread_idx, &mut pending_hashes);
                mark_worker_inactive(&shared, &mut worker_active);
            }
            match wait_for_work_update(&shared, local_generation) {
                Ok(Some((generation, work))) => {
                    nonce = work
                        .nonce_lease
                        .start_nonce
                        .wrapping_add(work.nonce_lease.lane_offset)
                        .wrapping_add(thread_idx as u64);
                    lane_iters = 0;
                    last_flush = Instant::now();
                    local_generation = generation;
                    local_work = Some(work);
                    mark_worker_active(&shared, &mut worker_active);
                    continue;
                }
                Ok(None) => break,
                Err(err) => {
                    emit_error(
                        &shared,
                        format!("cpu thread {thread_idx}: control wait failed ({err})"),
                    );
                    request_shutdown(&shared);
                    break;
                }
            }
        }

        let Some(work) = local_work.as_ref() else {
            continue;
        };
        let template = &work.template;
        let lease = &work.nonce_lease;

        if lane_iters >= lease.max_iters_per_lane || Instant::now() >= template.stop_at {
            flush_hashes(&shared, thread_idx, &mut pending_hashes);
            mark_worker_inactive(&shared, &mut worker_active);
            local_work = None;
            continue;
        }

        if shared.solution_state.load(Ordering::Acquire) != template.work_id {
            flush_hashes(&shared, thread_idx, &mut pending_hashes);
            mark_worker_inactive(&shared, &mut worker_active);
            local_work = None;
            continue;
        }

        let nonce_bytes = nonce.to_le_bytes();
        if argon2
            .hash_password_into(&nonce_bytes, &template.header_base, &mut output)
            .is_err()
        {
            emit_error(
                &shared,
                format!("cpu thread {thread_idx}: hash_password_into failed"),
            );
            request_shutdown(&shared);
            break;
        }

        lane_iters = lane_iters.saturating_add(1);
        pending_hashes = pending_hashes.saturating_add(1);
        if pending_hashes >= HASH_BATCH_SIZE || last_flush.elapsed() >= HASH_FLUSH_INTERVAL {
            flush_hashes(&shared, thread_idx, &mut pending_hashes);
            last_flush = Instant::now();
        }

        if hash_meets_target(&output, &template.target) {
            let solved_state = SOLVED_MASK | template.work_id;
            if shared
                .solution_state
                .compare_exchange(
                    template.work_id,
                    solved_state,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                emit_event(
                    &shared,
                    BackendEvent::Solution(MiningSolution {
                        epoch: template.epoch,
                        nonce,
                        backend_id: shared.instance_id.load(Ordering::Acquire),
                        backend: "cpu",
                    }),
                );
            }
        }

        nonce = nonce.wrapping_add(lease.global_stride.max(1));
    }

    flush_hashes(&shared, thread_idx, &mut pending_hashes);
    mark_worker_inactive(&shared, &mut worker_active);
}

fn wait_for_work_update(
    shared: &Shared,
    seen_generation: u64,
) -> Result<Option<(u64, Arc<WorkAssignment>)>> {
    let mut control = shared
        .work_control
        .lock()
        .map_err(|_| anyhow!("CPU control lock poisoned"))?;
    let mut observed_generation = seen_generation;

    loop {
        if control.shutdown {
            return Ok(None);
        }

        if control.generation != observed_generation {
            let generation = control.generation;
            if let Some(work) = control.work.clone() {
                return Ok(Some((generation, work)));
            }
            observed_generation = generation;
        }

        control = shared
            .work_cv
            .wait(control)
            .map_err(|_| anyhow!("CPU control lock poisoned"))?;
    }
}

fn request_work_pause(shared: &Shared) -> Result<()> {
    let generation = {
        let mut control = shared
            .work_control
            .lock()
            .map_err(|_| anyhow!("CPU control lock poisoned"))?;
        control.generation = control.generation.wrapping_add(1).max(1);
        control.work = None;
        control.generation
    };

    shared.work_generation.store(generation, Ordering::Release);
    shared.work_cv.notify_all();
    Ok(())
}

fn wait_for_idle(shared: &Shared) -> Result<()> {
    if shared.active_workers.load(Ordering::Acquire) == 0 {
        return Ok(());
    }

    let mut idle_guard = shared
        .idle_lock
        .lock()
        .map_err(|_| anyhow!("CPU idle lock poisoned"))?;
    while shared.active_workers.load(Ordering::Acquire) != 0 {
        let (next_guard, _) = shared
            .idle_cv
            .wait_timeout(idle_guard, Duration::from_millis(10))
            .map_err(|_| anyhow!("CPU idle lock poisoned"))?;
        idle_guard = next_guard;
    }
    Ok(())
}

fn request_shutdown(shared: &Shared) {
    if let Ok(mut control) = shared.work_control.lock() {
        control.shutdown = true;
        control.generation = control.generation.wrapping_add(1).max(1);
        control.work = None;
        shared
            .work_generation
            .store(control.generation, Ordering::Release);
    }
    shared.work_cv.notify_all();
}

fn mark_worker_active(shared: &Shared, worker_active: &mut bool) {
    if *worker_active {
        return;
    }
    shared.active_workers.fetch_add(1, Ordering::AcqRel);
    *worker_active = true;
}

fn mark_worker_inactive(shared: &Shared, worker_active: &mut bool) {
    if !*worker_active {
        return;
    }
    let prev = shared.active_workers.fetch_sub(1, Ordering::AcqRel);
    *worker_active = false;
    if prev <= 1 {
        shared.idle_cv.notify_all();
    }
}

fn reset_hash_slots(slots: &[AtomicU64]) {
    for slot in slots {
        slot.store(0, Ordering::Relaxed);
    }
}

fn flush_hashes(shared: &Shared, thread_idx: usize, pending_hashes: &mut u64) {
    if *pending_hashes == 0 {
        return;
    }
    if let Some(slot) = shared.hash_slots.get(thread_idx) {
        slot.fetch_add(*pending_hashes, Ordering::Relaxed);
    }
    *pending_hashes = 0;
}

fn emit_error(shared: &Shared, message: String) {
    emit_event(
        shared,
        BackendEvent::Error {
            backend_id: shared.instance_id.load(Ordering::Acquire),
            backend: "cpu",
            message,
        },
    );
}

fn emit_event(shared: &Shared, event: BackendEvent) {
    let tx = match shared.event_sink.read() {
        Ok(slot) => slot.clone(),
        Err(_) => None,
    };
    if let Some(tx) = tx {
        match event {
            BackendEvent::Solution(solution) => {
                let _ = tx.send(BackendEvent::Solution(solution));
            }
            BackendEvent::Error {
                backend_id,
                backend,
                message,
            } => {
                let _ = tx.try_send(BackendEvent::Error {
                    backend_id,
                    backend,
                    message,
                });
            }
        }
    }
}
