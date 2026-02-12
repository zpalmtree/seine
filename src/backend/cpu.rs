use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use argon2::{Algorithm, Argon2, Version};
use blocknet_pow_spec::{pow_params, POW_OUTPUT_LEN};
use crossbeam_channel::Sender;

use crate::backend::{BackendEvent, MiningSolution, MiningWork, PowBackend, WORK_ID_MAX};
use crate::types::hash_meets_target;

const HASH_BATCH_SIZE: u64 = 64;
const HASH_FLUSH_INTERVAL: Duration = Duration::from_millis(50);
const SOLVED_MASK: u64 = 1u64 << 63;

struct ControlState {
    shutdown: bool,
    generation: u64,
    work: Option<Arc<MiningWork>>,
}

struct Shared {
    started: AtomicBool,
    solution_state: AtomicU64,
    hashes_epoch: AtomicU64,
    work_generation: AtomicU64,
    work_control: Mutex<ControlState>,
    work_cv: Condvar,
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
                solution_state: AtomicU64::new(0),
                hashes_epoch: AtomicU64::new(0),
                work_generation: AtomicU64::new(0),
                work_control: Mutex::new(ControlState {
                    shutdown: false,
                    generation: 0,
                    work: None,
                }),
                work_cv: Condvar::new(),
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
        self.shared.hashes_epoch.store(0, Ordering::SeqCst);
        self.shared.work_generation.store(0, Ordering::SeqCst);
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

        for handle in self.worker_handles.drain(..) {
            let _ = handle.join();
        }

        self.shared.solution_state.store(0, Ordering::SeqCst);
        self.shared.hashes_epoch.store(0, Ordering::SeqCst);
        self.shared.work_generation.store(0, Ordering::SeqCst);
        reset_hash_slots(&self.shared.hash_slots);

        if let Ok(mut control) = self.shared.work_control.lock() {
            control.shutdown = false;
            control.generation = 0;
            control.work = None;
        }
    }

    fn set_work(&self, work: MiningWork) -> Result<()> {
        if !self.shared.started.load(Ordering::SeqCst) {
            return Err(anyhow!("CPU backend is not started"));
        }
        if work.work_id == 0 || work.work_id > WORK_ID_MAX {
            return Err(anyhow!("invalid work id {}", work.work_id));
        }

        let work_epoch = work.epoch;
        let work_id = work.work_id;
        let work = Arc::new(work);

        reset_hash_slots(&self.shared.hash_slots);
        self.shared
            .hashes_epoch
            .store(work_epoch, Ordering::Release);
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

    fn take_hashes(&self, epoch: u64) -> u64 {
        if self.shared.hashes_epoch.load(Ordering::Acquire) != epoch {
            return 0;
        }

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
    let mut local_work: Option<Arc<MiningWork>> = None;
    let mut nonce = 0u64;
    let mut lane_iters = 0u64;
    let mut pending_hashes = 0u64;
    let mut last_flush = Instant::now();

    loop {
        let global_generation = shared.work_generation.load(Ordering::Acquire);
        if global_generation != local_generation || local_work.is_none() {
            pending_hashes = 0;
            match wait_for_work_update(&shared, local_generation) {
                Ok(Some((generation, work))) => {
                    nonce = work
                        .start_nonce
                        .wrapping_add(work.lane_offset)
                        .wrapping_add(thread_idx as u64);
                    lane_iters = 0;
                    last_flush = Instant::now();
                    local_generation = generation;
                    local_work = Some(work);
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

        if lane_iters >= work.max_iters_per_lane || Instant::now() >= work.stop_at {
            flush_hashes(&shared, thread_idx, &mut pending_hashes);
            local_work = None;
            continue;
        }

        if shared.solution_state.load(Ordering::Acquire) != work.work_id {
            flush_hashes(&shared, thread_idx, &mut pending_hashes);
            local_work = None;
            continue;
        }

        let nonce_bytes = nonce.to_le_bytes();
        if argon2
            .hash_password_into(&nonce_bytes, &work.header_base, &mut output)
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

        if hash_meets_target(&output, &work.target) {
            let solved_state = SOLVED_MASK | work.work_id;
            if shared
                .solution_state
                .compare_exchange(
                    work.work_id,
                    solved_state,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                emit_event(
                    &shared,
                    BackendEvent::Solution(MiningSolution {
                        epoch: work.epoch,
                        nonce,
                        backend: "cpu".to_string(),
                    }),
                );
            }
        }

        nonce = nonce.wrapping_add(work.global_stride.max(1));
    }

    flush_hashes(&shared, thread_idx, &mut pending_hashes);
}

fn wait_for_work_update(
    shared: &Shared,
    seen_generation: u64,
) -> Result<Option<(u64, Arc<MiningWork>)>> {
    let mut control = shared
        .work_control
        .lock()
        .map_err(|_| anyhow!("CPU control lock poisoned"))?;

    while !control.shutdown && control.generation == seen_generation {
        control = shared
            .work_cv
            .wait(control)
            .map_err(|_| anyhow!("CPU control lock poisoned"))?;
    }

    if control.shutdown {
        return Ok(None);
    }

    let generation = control.generation;
    let work = control
        .work
        .clone()
        .ok_or_else(|| anyhow!("missing work for generation {}", generation))?;
    Ok(Some((generation, work)))
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
        let _ = tx.send(event);
    }
}
