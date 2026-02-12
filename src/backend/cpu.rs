use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use argon2::{Algorithm, Argon2, Block, Version};
use blocknet_pow_spec::{pow_params, POW_OUTPUT_LEN};
use crossbeam_channel::{SendTimeoutError, Sender};

use crate::backend::{
    BackendEvent, BackendInstanceId, BackendTelemetry, BenchBackend, MiningSolution, PowBackend,
    PreemptionGranularity, WorkAssignment, WORK_ID_MAX,
};
use crate::config::CpuAffinityMode;
use crate::types::hash_meets_target;

const HASH_BATCH_SIZE: u64 = 64;
const HASH_FLUSH_INTERVAL: Duration = Duration::from_millis(50);
const STOP_CHECK_INTERVAL_HASHES: u64 = 64;
const CRITICAL_EVENT_RETRY_WAIT: Duration = Duration::from_millis(5);
const SOLVED_MASK: u64 = 1u64 << 63;

#[repr(align(64))]
struct PaddedAtomicU64(AtomicU64);

impl PaddedAtomicU64 {
    fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    fn store(&self, value: u64, ordering: Ordering) {
        self.0.store(value, ordering);
    }

    fn swap(&self, value: u64, ordering: Ordering) -> u64 {
        self.0.swap(value, ordering)
    }

    fn fetch_add(&self, value: u64, ordering: Ordering) -> u64 {
        self.0.fetch_add(value, ordering)
    }
}

struct ControlState {
    shutdown: bool,
    generation: u64,
    work: Option<Arc<WorkAssignment>>,
}

struct Shared {
    started: AtomicBool,
    instance_id: AtomicU64,
    solution_state: AtomicU64,
    error_emitted: AtomicBool,
    work_generation: AtomicU64,
    active_workers: AtomicUsize,
    work_control: Mutex<ControlState>,
    work_cv: Condvar,
    idle_lock: Mutex<()>,
    idle_cv: Condvar,
    hash_slots: Vec<PaddedAtomicU64>,
    assignment_hashes: AtomicU64,
    assignment_generation: AtomicU64,
    assignment_reported_generation: AtomicU64,
    assignment_started_at: Mutex<Option<Instant>>,
    completed_assignments: AtomicU64,
    completed_assignment_hashes: AtomicU64,
    completed_assignment_micros: AtomicU64,
    dropped_events: AtomicU64,
    event_sink: RwLock<Option<Sender<BackendEvent>>>,
}

pub struct CpuBackend {
    threads: usize,
    affinity_mode: CpuAffinityMode,
    shared: Arc<Shared>,
    worker_handles: Vec<JoinHandle<()>>,
}

impl CpuBackend {
    pub fn new(threads: usize, affinity_mode: CpuAffinityMode) -> Self {
        let lanes = threads.max(1);
        Self {
            threads,
            affinity_mode,
            shared: Arc::new(Shared {
                started: AtomicBool::new(false),
                instance_id: AtomicU64::new(0),
                solution_state: AtomicU64::new(0),
                error_emitted: AtomicBool::new(false),
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
                hash_slots: (0..lanes).map(|_| PaddedAtomicU64::new(0)).collect(),
                assignment_hashes: AtomicU64::new(0),
                assignment_generation: AtomicU64::new(0),
                assignment_reported_generation: AtomicU64::new(0),
                assignment_started_at: Mutex::new(None),
                completed_assignments: AtomicU64::new(0),
                completed_assignment_hashes: AtomicU64::new(0),
                completed_assignment_micros: AtomicU64::new(0),
                dropped_events: AtomicU64::new(0),
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
        self.shared.error_emitted.store(false, Ordering::SeqCst);
        self.shared.work_generation.store(0, Ordering::SeqCst);
        self.shared.active_workers.store(0, Ordering::SeqCst);
        self.shared.assignment_hashes.store(0, Ordering::SeqCst);
        self.shared.assignment_generation.store(0, Ordering::SeqCst);
        self.shared
            .assignment_reported_generation
            .store(0, Ordering::SeqCst);
        self.shared.completed_assignments.store(0, Ordering::SeqCst);
        self.shared
            .completed_assignment_hashes
            .store(0, Ordering::SeqCst);
        self.shared
            .completed_assignment_micros
            .store(0, Ordering::SeqCst);
        self.shared.dropped_events.store(0, Ordering::SeqCst);
        reset_hash_slots(&self.shared.hash_slots);
        if let Ok(mut started_at) = self.shared.assignment_started_at.lock() {
            *started_at = None;
        }

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

        let core_ids = match self.affinity_mode {
            CpuAffinityMode::Off => None,
            CpuAffinityMode::Auto => core_affinity::get_core_ids().filter(|ids| !ids.is_empty()),
        };

        for thread_idx in 0..self.threads.max(1) {
            let shared = Arc::clone(&self.shared);
            let core_id = core_ids
                .as_ref()
                .and_then(|ids| ids.get(thread_idx % ids.len()))
                .copied();
            self.worker_handles.push(thread::spawn(move || {
                cpu_worker_loop(shared, thread_idx, core_id)
            }));
        }

        Ok(())
    }

    fn stop(&mut self) {
        if !self.shared.started.swap(false, Ordering::SeqCst) {
            return;
        }

        request_shutdown(&self.shared);
        let _ = wait_for_idle(&self.shared);
        maybe_finalize_assignment(&self.shared);

        for handle in self.worker_handles.drain(..) {
            let _ = handle.join();
        }

        self.shared.solution_state.store(0, Ordering::SeqCst);
        self.shared.error_emitted.store(false, Ordering::SeqCst);
        self.shared.work_generation.store(0, Ordering::SeqCst);
        self.shared.active_workers.store(0, Ordering::SeqCst);
        self.shared.assignment_hashes.store(0, Ordering::SeqCst);
        self.shared.assignment_generation.store(0, Ordering::SeqCst);
        self.shared
            .assignment_reported_generation
            .store(0, Ordering::SeqCst);
        self.shared.completed_assignments.store(0, Ordering::SeqCst);
        self.shared
            .completed_assignment_hashes
            .store(0, Ordering::SeqCst);
        self.shared
            .completed_assignment_micros
            .store(0, Ordering::SeqCst);
        self.shared.dropped_events.store(0, Ordering::SeqCst);
        reset_hash_slots(&self.shared.hash_slots);
        if let Ok(mut started_at) = self.shared.assignment_started_at.lock() {
            *started_at = None;
        }

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
        self.shared.error_emitted.store(false, Ordering::Release);

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
        start_assignment(&self.shared, generation);
        self.shared.work_cv.notify_all();
        Ok(())
    }

    fn cancel_work(&self) -> Result<()> {
        if !self.shared.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        request_work_pause(&self.shared)
    }

    fn fence(&self) -> Result<()> {
        if !self.shared.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        wait_for_idle(&self.shared)
    }

    fn take_hashes(&self) -> u64 {
        self.shared
            .hash_slots
            .iter()
            .map(|slot| slot.swap(0, Ordering::AcqRel))
            .sum()
    }

    fn take_telemetry(&self) -> BackendTelemetry {
        BackendTelemetry {
            active_lanes: self.shared.active_workers.load(Ordering::Acquire) as u64,
            pending_work: u64::from(has_pending_work(&self.shared)),
            dropped_events: self.shared.dropped_events.swap(0, Ordering::AcqRel),
            completed_assignments: self.shared.completed_assignments.swap(0, Ordering::AcqRel),
            completed_assignment_hashes: self
                .shared
                .completed_assignment_hashes
                .swap(0, Ordering::AcqRel),
            completed_assignment_micros: self
                .shared
                .completed_assignment_micros
                .swap(0, Ordering::AcqRel),
        }
    }

    fn preemption_granularity(&self) -> PreemptionGranularity {
        PreemptionGranularity::Hashes(1)
    }

    fn bench_backend(&self) -> Option<&dyn BenchBackend> {
        Some(self)
    }
}

impl BenchBackend for CpuBackend {
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
                    let mut memory_blocks = vec![Block::default(); params.block_count()];
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
                            .hash_password_into_with_memory(
                                &nonce_bytes,
                                &header_base,
                                &mut output,
                                &mut memory_blocks,
                            )
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

fn cpu_worker_loop(shared: Arc<Shared>, thread_idx: usize, core_id: Option<core_affinity::CoreId>) {
    if let Some(core_id) = core_id {
        let _ = core_affinity::set_for_current(core_id);
    }

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

    let mut memory_blocks = vec![Block::default(); params.block_count()];
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut output = [0u8; POW_OUTPUT_LEN];
    let mut local_generation = 0u64;
    let mut local_work: Option<Arc<WorkAssignment>> = None;
    let mut worker_active = false;
    let mut nonce = 0u64;
    let mut lane_iters = 0u64;
    let lane_stride = shared.hash_slots.len().max(1) as u64;
    let mut lane_quota = 0u64;
    let mut pending_hashes = 0u64;
    let mut last_flush = Instant::now();
    let mut next_stop_check = STOP_CHECK_INTERVAL_HASHES;

    loop {
        let global_generation = shared.work_generation.load(Ordering::Acquire);
        if global_generation != local_generation || local_work.is_none() {
            if local_work.is_some() {
                flush_hashes(&shared, thread_idx, &mut pending_hashes);
                mark_worker_inactive(&shared, &mut worker_active);
            }
            match wait_for_work_update(&shared, local_generation) {
                Ok(Some((generation, work))) => {
                    nonce = work.nonce_chunk.start_nonce.wrapping_add(thread_idx as u64);
                    lane_iters = 0;
                    lane_quota = lane_quota_for_chunk(
                        work.nonce_chunk.nonce_count,
                        thread_idx as u64,
                        lane_stride,
                    );
                    next_stop_check = STOP_CHECK_INTERVAL_HASHES.min(lane_quota.max(1));
                    last_flush = Instant::now();
                    local_generation = generation;
                    local_work = Some(work);
                    if lane_quota > 0 {
                        mark_worker_active(&shared, &mut worker_active);
                    } else {
                        local_work = None;
                    }
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

        if lane_iters >= lane_quota {
            flush_hashes(&shared, thread_idx, &mut pending_hashes);
            mark_worker_inactive(&shared, &mut worker_active);
            local_work = None;
            continue;
        }

        if lane_iters >= next_stop_check {
            if Instant::now() >= template.stop_at {
                flush_hashes(&shared, thread_idx, &mut pending_hashes);
                mark_worker_inactive(&shared, &mut worker_active);
                local_work = None;
                continue;
            }
            next_stop_check = next_stop_check.saturating_add(STOP_CHECK_INTERVAL_HASHES);
        }

        if shared.solution_state.load(Ordering::Acquire) != template.work_id {
            flush_hashes(&shared, thread_idx, &mut pending_hashes);
            mark_worker_inactive(&shared, &mut worker_active);
            local_work = None;
            continue;
        }

        let nonce_bytes = nonce.to_le_bytes();
        if argon2
            .hash_password_into_with_memory(
                &nonce_bytes,
                &template.header_base,
                &mut output,
                &mut memory_blocks,
            )
            .is_err()
        {
            emit_error(
                &shared,
                format!("cpu thread {thread_idx}: hash_password_into_with_memory failed"),
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

        nonce = nonce.wrapping_add(lane_stride);
    }

    flush_hashes(&shared, thread_idx, &mut pending_hashes);
    mark_worker_inactive(&shared, &mut worker_active);
}

fn lane_quota_for_chunk(nonce_count: u64, lane_idx: u64, lane_stride: u64) -> u64 {
    let stride = lane_stride.max(1);
    if nonce_count == 0 || lane_idx >= nonce_count {
        return 0;
    }
    1 + (nonce_count - 1 - lane_idx) / stride
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
    if shared.active_workers.load(Ordering::Acquire) == 0 {
        maybe_finalize_assignment(shared);
    }
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
        idle_guard = shared
            .idle_cv
            .wait(idle_guard)
            .map_err(|_| anyhow!("CPU idle lock poisoned"))?;
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
    if shared.active_workers.load(Ordering::Acquire) == 0 {
        maybe_finalize_assignment(shared);
    }
}

fn has_pending_work(shared: &Shared) -> bool {
    shared
        .work_control
        .lock()
        .map(|control| control.work.is_some() && !control.shutdown)
        .unwrap_or(false)
}

fn start_assignment(shared: &Shared, generation: u64) {
    shared.assignment_hashes.store(0, Ordering::Release);
    shared
        .assignment_generation
        .store(generation, Ordering::Release);
    if let Ok(mut started_at) = shared.assignment_started_at.lock() {
        *started_at = Some(Instant::now());
    }
}

fn maybe_finalize_assignment(shared: &Shared) {
    let generation = shared.assignment_generation.load(Ordering::Acquire);
    if generation == 0 {
        return;
    }

    let mut seen = shared
        .assignment_reported_generation
        .load(Ordering::Acquire);
    while generation > seen {
        match shared.assignment_reported_generation.compare_exchange(
            seen,
            generation,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                let started_at = match shared.assignment_started_at.lock() {
                    Ok(mut slot) => slot.take(),
                    Err(_) => None,
                };
                if let Some(started_at) = started_at {
                    let micros = started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
                    let hashes = shared.assignment_hashes.swap(0, Ordering::AcqRel);
                    shared.completed_assignments.fetch_add(1, Ordering::AcqRel);
                    shared
                        .completed_assignment_hashes
                        .fetch_add(hashes, Ordering::AcqRel);
                    shared
                        .completed_assignment_micros
                        .fetch_add(micros, Ordering::AcqRel);
                }
                return;
            }
            Err(current) => seen = current,
        }
    }
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
        maybe_finalize_assignment(shared);
        shared.idle_cv.notify_all();
    }
}

fn reset_hash_slots(slots: &[PaddedAtomicU64]) {
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
    shared
        .assignment_hashes
        .fetch_add(*pending_hashes, Ordering::Relaxed);
    *pending_hashes = 0;
}

fn emit_error(shared: &Shared, message: String) {
    if shared.error_emitted.swap(true, Ordering::AcqRel) {
        return;
    }
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
    let Some(tx) = tx else {
        return;
    };

    match event {
        BackendEvent::Solution(solution) => {
            send_critical_event(shared, &tx, BackendEvent::Solution(solution));
        }
        BackendEvent::Error {
            backend_id,
            backend,
            message,
        } => {
            send_critical_event(
                shared,
                &tx,
                BackendEvent::Error {
                    backend_id,
                    backend,
                    message,
                },
            );
        }
    }
}

fn send_critical_event(shared: &Shared, tx: &Sender<BackendEvent>, event: BackendEvent) {
    let mut queued = event;
    loop {
        if !shared.started.load(Ordering::Acquire) {
            return;
        }

        match tx.send_timeout(queued, CRITICAL_EVENT_RETRY_WAIT) {
            Ok(()) => return,
            Err(SendTimeoutError::Disconnected(_)) => return,
            Err(SendTimeoutError::Timeout(returned)) => {
                queued = returned;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{emit_error, lane_quota_for_chunk, BackendEvent, CpuBackend, MiningSolution};
    use crate::backend::PowBackend;
    use crate::config::CpuAffinityMode;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn lane_quota_even_chunk_distribution() {
        assert_eq!(lane_quota_for_chunk(40, 0, 4), 10);
        assert_eq!(lane_quota_for_chunk(40, 1, 4), 10);
        assert_eq!(lane_quota_for_chunk(40, 3, 4), 10);
    }

    #[test]
    fn lane_quota_handles_partial_tail() {
        assert_eq!(lane_quota_for_chunk(5, 0, 4), 2);
        assert_eq!(lane_quota_for_chunk(5, 1, 4), 1);
        assert_eq!(lane_quota_for_chunk(5, 3, 4), 1);
        assert_eq!(lane_quota_for_chunk(5, 6, 4), 0);
    }

    #[test]
    fn error_event_retries_until_queue_capacity_is_available() {
        let mut backend = CpuBackend::new(1, CpuAffinityMode::Off);
        backend.shared.started.store(true, Ordering::Release);
        backend.set_instance_id(7);
        let (event_tx, event_rx) = crossbeam_channel::bounded(1);
        backend.set_event_sink(event_tx.clone());

        event_tx
            .send(BackendEvent::Solution(MiningSolution {
                epoch: 1,
                nonce: 1,
                backend_id: 99,
                backend: "cpu",
            }))
            .expect("prefill should succeed");

        let shared = Arc::clone(&backend.shared);
        let emit_thread = thread::spawn(move || {
            emit_error(&shared, "synthetic failure".to_string());
        });

        let first = event_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("first event should be available");
        assert!(
            matches!(first, BackendEvent::Solution(_)),
            "expected prefilled solution event first"
        );

        let second = event_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("error event should be enqueued after capacity frees");
        match second {
            BackendEvent::Error {
                backend_id,
                backend,
                message,
            } => {
                assert_eq!(backend_id, 7);
                assert_eq!(backend, "cpu");
                assert_eq!(message, "synthetic failure");
            }
            other => panic!("expected error event, got {:?}", other),
        }

        emit_thread
            .join()
            .expect("error emitter thread should finish once queued");
    }
}
