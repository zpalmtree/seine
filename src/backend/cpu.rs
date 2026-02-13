use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use argon2::{Algorithm, Argon2, Block, Version};
use blocknet_pow_spec::{pow_params, POW_OUTPUT_LEN};
use crossbeam_channel::{bounded, Sender};

use crate::backend::{
    AssignmentSemantics, BackendCapabilities, BackendEvent, BackendExecutionModel,
    BackendInstanceId, BackendTelemetry, BenchBackend, DeadlineSupport, MiningSolution, PowBackend,
    PreemptionGranularity, WorkAssignment, WORK_ID_MAX,
};
use crate::config::CpuAffinityMode;

#[path = "cpu/events.rs"]
mod events;
#[path = "cpu/kernel.rs"]
mod kernel;

const HASH_BATCH_SIZE: u64 = 64;
const CONTROL_CHECK_INTERVAL_HASHES: u64 = 256;
const HASH_FLUSH_INTERVAL: Duration = Duration::from_millis(50);
const NONBLOCKING_POLL_MIN: Duration = Duration::from_micros(50);
const NONBLOCKING_POLL_MAX: Duration = Duration::from_millis(1);
const CRITICAL_EVENT_RETRY_WAIT: Duration = Duration::from_millis(5);
const CRITICAL_EVENT_RETRY_MAX_WAIT: Duration = Duration::from_millis(100);
const ERROR_EVENT_MAX_BLOCK: Duration = Duration::from_millis(500);
const EVENT_DISPATCH_CAPACITY: usize = 1024;
const SOLVED_MASK: u64 = 1u64 << 63;
const STARTUP_READY_WAIT_SLICE: Duration = Duration::from_millis(50);
const STARTUP_READY_TIMEOUT_BASE: Duration = Duration::from_secs(15);
const STARTUP_READY_TIMEOUT_PER_WORKER: Duration = Duration::from_secs(1);
const STARTUP_READY_TIMEOUT_MAX: Duration = Duration::from_secs(180);

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
    ready_workers: AtomicUsize,
    startup_failed: AtomicBool,
    work_control: Mutex<ControlState>,
    work_cv: Condvar,
    idle_lock: Mutex<()>,
    idle_cv: Condvar,
    ready_lock: Mutex<()>,
    ready_cv: Condvar,
    hash_slots: Vec<PaddedAtomicU64>,
    assignment_hashes: AtomicU64,
    assignment_generation: AtomicU64,
    assignment_reported_generation: AtomicU64,
    assignment_started_at: Mutex<Option<Instant>>,
    completed_assignments: AtomicU64,
    completed_assignment_hashes: AtomicU64,
    completed_assignment_micros: AtomicU64,
    dropped_events: AtomicU64,
    event_dispatch_tx: RwLock<Option<Sender<BackendEvent>>>,
    event_sink: RwLock<Option<Sender<BackendEvent>>>,
}

pub struct CpuBackend {
    threads: usize,
    affinity_mode: CpuAffinityMode,
    shared: Arc<Shared>,
    worker_handles: Mutex<Vec<JoinHandle<()>>>,
    event_forward_handle: Mutex<Option<JoinHandle<()>>>,
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
                ready_workers: AtomicUsize::new(0),
                startup_failed: AtomicBool::new(false),
                work_control: Mutex::new(ControlState {
                    shutdown: false,
                    generation: 0,
                    work: None,
                }),
                work_cv: Condvar::new(),
                idle_lock: Mutex::new(()),
                idle_cv: Condvar::new(),
                ready_lock: Mutex::new(()),
                ready_cv: Condvar::new(),
                hash_slots: (0..lanes).map(|_| PaddedAtomicU64::new(0)).collect(),
                assignment_hashes: AtomicU64::new(0),
                assignment_generation: AtomicU64::new(0),
                assignment_reported_generation: AtomicU64::new(0),
                assignment_started_at: Mutex::new(None),
                completed_assignments: AtomicU64::new(0),
                completed_assignment_hashes: AtomicU64::new(0),
                completed_assignment_micros: AtomicU64::new(0),
                dropped_events: AtomicU64::new(0),
                event_dispatch_tx: RwLock::new(None),
                event_sink: RwLock::new(None),
            }),
            worker_handles: Mutex::new(Vec::new()),
            event_forward_handle: Mutex::new(None),
        }
    }

    fn reset_runtime_state(&self) {
        self.shared.solution_state.store(0, Ordering::SeqCst);
        self.shared.error_emitted.store(false, Ordering::SeqCst);
        self.shared.work_generation.store(0, Ordering::SeqCst);
        self.shared.active_workers.store(0, Ordering::SeqCst);
        self.shared.ready_workers.store(0, Ordering::SeqCst);
        self.shared.startup_failed.store(false, Ordering::SeqCst);
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

    fn start_event_forwarder(&self) -> Result<()> {
        let instance_id = self.shared.instance_id.load(Ordering::Acquire);
        let (dispatch_tx, dispatch_rx) = bounded::<BackendEvent>(EVENT_DISPATCH_CAPACITY.max(1));
        if let Ok(mut slot) = self.shared.event_dispatch_tx.write() {
            *slot = Some(dispatch_tx);
        } else {
            return Err(anyhow!("CPU event dispatch lock poisoned"));
        }

        let shared = Arc::clone(&self.shared);
        let handle = thread::Builder::new()
            .name(format!("seine-cpu-events-{instance_id}"))
            .spawn(move || {
                while let Ok(event) = dispatch_rx.recv() {
                    forward_event(&shared, event);
                }
            })
            .map_err(|err| anyhow!("failed to spawn CPU event forwarder: {err}"))?;
        if let Ok(mut slot) = self.event_forward_handle.lock() {
            *slot = Some(handle);
            Ok(())
        } else {
            if let Ok(mut tx_slot) = self.shared.event_dispatch_tx.write() {
                *tx_slot = None;
            }
            let _ = handle.join();
            Err(anyhow!("CPU event forwarder handle lock poisoned"))
        }
    }

    fn stop_event_forwarder(&self) {
        if let Ok(mut slot) = self.shared.event_dispatch_tx.write() {
            *slot = None;
        }
        let handle = self
            .event_forward_handle
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        if let Some(handle) = handle {
            let _ = handle.join();
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

    fn set_instance_id(&self, id: BackendInstanceId) {
        self.shared.instance_id.store(id, Ordering::Release);
    }

    fn set_event_sink(&self, sink: Sender<BackendEvent>) {
        if let Ok(mut slot) = self.shared.event_sink.write() {
            *slot = Some(sink);
        }
    }

    fn start(&self) -> Result<()> {
        if self.shared.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        self.stop_event_forwarder();
        self.reset_runtime_state();
        if let Err(err) = self.start_event_forwarder() {
            self.shared.started.store(false, Ordering::SeqCst);
            self.stop_event_forwarder();
            self.reset_runtime_state();
            return Err(err);
        }

        let core_ids = match self.affinity_mode {
            CpuAffinityMode::Off => None,
            CpuAffinityMode::Auto => core_affinity::get_core_ids().filter(|ids| !ids.is_empty()),
        };

        let mut spawned_handles = Vec::with_capacity(self.threads.max(1));
        let mut spawn_error = None;
        for thread_idx in 0..self.threads.max(1) {
            let shared = Arc::clone(&self.shared);
            let core_id = core_ids
                .as_ref()
                .and_then(|ids| ids.get(thread_idx % ids.len()))
                .copied();
            let handle = thread::Builder::new()
                .name(format!("seine-cpu-worker-{thread_idx}"))
                .spawn(move || cpu_worker_loop(shared, thread_idx, core_id));
            match handle {
                Ok(handle) => spawned_handles.push(handle),
                Err(err) => {
                    spawn_error = Some((thread_idx, err));
                    break;
                }
            }
        }

        if let Some((thread_idx, err)) = spawn_error {
            request_shutdown(&self.shared);
            for handle in spawned_handles {
                let _ = handle.join();
            }
            self.shared.started.store(false, Ordering::SeqCst);
            self.stop_event_forwarder();
            self.reset_runtime_state();
            return Err(anyhow!(
                "failed to spawn CPU worker thread {thread_idx}: {err}"
            ));
        }

        let mut handles = match self.worker_handles.lock() {
            Ok(guard) => guard,
            Err(_) => {
                request_shutdown(&self.shared);
                for handle in spawned_handles {
                    let _ = handle.join();
                }
                self.shared.started.store(false, Ordering::SeqCst);
                self.stop_event_forwarder();
                self.reset_runtime_state();
                return Err(anyhow!("CPU worker handle lock poisoned"));
            }
        };
        *handles = spawned_handles;
        drop(handles);

        if let Err(err) = wait_for_workers_ready(
            &self.shared,
            self.threads.max(1),
            startup_ready_timeout(self.threads.max(1)),
        ) {
            request_shutdown(&self.shared);
            let handles = match self.worker_handles.lock() {
                Ok(mut guard) => std::mem::take(&mut *guard),
                Err(_) => Vec::new(),
            };
            for handle in handles {
                let _ = handle.join();
            }
            self.shared.started.store(false, Ordering::SeqCst);
            self.stop_event_forwarder();
            self.reset_runtime_state();
            return Err(err);
        }

        Ok(())
    }

    fn stop(&self) {
        if !self.shared.started.swap(false, Ordering::SeqCst) {
            return;
        }

        request_shutdown(&self.shared);
        let _ = wait_for_idle(&self.shared);
        maybe_finalize_assignment(&self.shared);

        let handles = match self.worker_handles.lock() {
            Ok(mut guard) => std::mem::take(&mut *guard),
            Err(_) => Vec::new(),
        };
        for handle in handles {
            let _ = handle.join();
        }

        self.stop_event_forwarder();
        self.reset_runtime_state();
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

    fn assign_work_batch_with_deadline(
        &self,
        work: &[WorkAssignment],
        deadline: Instant,
    ) -> Result<()> {
        if Instant::now() >= deadline {
            return Err(anyhow!("assignment deadline elapsed before dispatch"));
        }
        self.assign_work_batch(work)?;
        let now = Instant::now();
        if now > deadline {
            return Err(anyhow!(
                "assignment call exceeded deadline by {}ms",
                now.saturating_duration_since(deadline).as_millis()
            ));
        }
        Ok(())
    }

    fn cancel_work(&self) -> Result<()> {
        if !self.shared.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        request_work_pause(&self.shared)
    }

    fn request_timeout_interrupt(&self) -> Result<()> {
        if !self.shared.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        request_work_pause(&self.shared)
    }

    fn cancel_work_with_deadline(&self, deadline: Instant) -> Result<()> {
        if !self.shared.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        request_work_pause(&self.shared)?;
        wait_for_idle_until(&self.shared, deadline)
    }

    fn fence(&self) -> Result<()> {
        if !self.shared.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        wait_for_idle(&self.shared)
    }

    fn fence_with_deadline(&self, deadline: Instant) -> Result<()> {
        if !self.shared.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        wait_for_idle_until(&self.shared, deadline)
    }

    fn take_hashes(&self) -> u64 {
        self.shared
            .hash_slots
            .iter()
            .map(|slot| slot.swap(0, Ordering::AcqRel))
            .sum()
    }

    fn take_telemetry(&self) -> BackendTelemetry {
        let (inflight_assignment_hashes, inflight_assignment_micros) =
            if self.shared.active_workers.load(Ordering::Acquire) > 0 {
                let hashes = self.shared.assignment_hashes.load(Ordering::Acquire);
                let micros = self
                    .shared
                    .assignment_started_at
                    .lock()
                    .ok()
                    .and_then(|slot| {
                        slot.as_ref().map(|started| {
                            started.elapsed().as_micros().min(u64::MAX as u128) as u64
                        })
                    })
                    .unwrap_or(0);
                (hashes, micros)
            } else {
                (0, 0)
            };

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
            inflight_assignment_hashes,
            inflight_assignment_micros,
            ..BackendTelemetry::default()
        }
    }

    fn preemption_granularity(&self) -> PreemptionGranularity {
        PreemptionGranularity::Hashes(CONTROL_CHECK_INTERVAL_HASHES)
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            preferred_iters_per_lane: None,
            preferred_allocation_iters_per_lane: None,
            preferred_hash_poll_interval: Some(HASH_FLUSH_INTERVAL),
            preferred_assignment_timeout: None,
            preferred_control_timeout: None,
            preferred_assignment_timeout_strikes: None,
            max_inflight_assignments: 1,
            deadline_support: DeadlineSupport::Cooperative,
            assignment_semantics: AssignmentSemantics::Replace,
            execution_model: BackendExecutionModel::Blocking,
            nonblocking_poll_min: Some(NONBLOCKING_POLL_MIN),
            nonblocking_poll_max: Some(NONBLOCKING_POLL_MAX),
        }
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
    kernel::cpu_worker_loop(shared, thread_idx, core_id);
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
    shared.ready_cv.notify_all();
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

#[allow(dead_code)]
fn wait_for_idle_until(shared: &Shared, deadline: Instant) -> Result<()> {
    if shared.active_workers.load(Ordering::Acquire) == 0 {
        return Ok(());
    }

    let mut idle_guard = shared
        .idle_lock
        .lock()
        .map_err(|_| anyhow!("CPU idle lock poisoned"))?;
    while shared.active_workers.load(Ordering::Acquire) != 0 {
        let now = Instant::now();
        if now >= deadline {
            return Err(anyhow!(
                "idle wait exceeded deadline by {}ms",
                now.saturating_duration_since(deadline).as_millis()
            ));
        }
        let wait_for = deadline
            .saturating_duration_since(now)
            .min(Duration::from_millis(10));
        let (next_guard, wait_result) = shared
            .idle_cv
            .wait_timeout(idle_guard, wait_for)
            .map_err(|_| anyhow!("CPU idle lock poisoned"))?;
        idle_guard = next_guard;
        if wait_result.timed_out()
            && shared.active_workers.load(Ordering::Acquire) != 0
            && Instant::now() >= deadline
        {
            return Err(anyhow!("idle wait timed out"));
        }
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
    shared.ready_cv.notify_all();
    if shared.active_workers.load(Ordering::Acquire) == 0 {
        maybe_finalize_assignment(shared);
    }
}

fn startup_ready_timeout(workers: usize) -> Duration {
    STARTUP_READY_TIMEOUT_BASE
        .saturating_add(STARTUP_READY_TIMEOUT_PER_WORKER.saturating_mul(workers as u32))
        .min(STARTUP_READY_TIMEOUT_MAX)
}

fn mark_worker_ready(shared: &Shared) {
    shared.ready_workers.fetch_add(1, Ordering::AcqRel);
    shared.ready_cv.notify_all();
}

fn mark_startup_failed(shared: &Shared) {
    shared.startup_failed.store(true, Ordering::Release);
    shared.ready_cv.notify_all();
}

fn wait_for_workers_ready(
    shared: &Shared,
    expected_workers: usize,
    timeout: Duration,
) -> Result<()> {
    if expected_workers == 0 {
        return Ok(());
    }

    if shared.ready_workers.load(Ordering::Acquire) >= expected_workers {
        return Ok(());
    }

    let deadline = Instant::now() + timeout.max(Duration::from_millis(1));
    let mut ready_guard = shared
        .ready_lock
        .lock()
        .map_err(|_| anyhow!("CPU ready lock poisoned"))?;

    loop {
        let ready = shared.ready_workers.load(Ordering::Acquire);
        if ready >= expected_workers {
            return Ok(());
        }
        if shared.startup_failed.load(Ordering::Acquire) {
            return Err(anyhow!(
                "CPU worker startup failed before readiness barrier ({ready}/{expected_workers} ready)"
            ));
        }

        let now = Instant::now();
        if now >= deadline {
            let timed_out_by = now.saturating_duration_since(deadline);
            return Err(anyhow!(
                "CPU worker startup timed out by {}ms ({ready}/{expected_workers} ready)",
                timed_out_by.as_millis()
            ));
        }

        let wait_for = deadline
            .saturating_duration_since(now)
            .min(STARTUP_READY_WAIT_SLICE);
        let (next_guard, _) = shared
            .ready_cv
            .wait_timeout(ready_guard, wait_for)
            .map_err(|_| anyhow!("CPU ready lock poisoned"))?;
        ready_guard = next_guard;
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
    let idle = shared.active_workers.load(Ordering::Acquire) == 0;
    if idle {
        // When a reassignment happens while workers are still draining, resetting counters here
        // can drop or misattribute tail hashes. Finalize/reset only after lanes are quiesced.
        maybe_finalize_assignment(shared);
        shared.assignment_hashes.store(0, Ordering::Release);
        if let Ok(mut started_at) = shared.assignment_started_at.lock() {
            *started_at = Some(Instant::now());
        }
    } else if let Ok(mut started_at) = shared.assignment_started_at.lock() {
        if started_at.is_none() {
            *started_at = Some(Instant::now());
        }
    }
    shared
        .assignment_generation
        .store(generation, Ordering::Release);
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

fn should_flush_hashes(pending_hashes: u64, now: Instant, next_flush_at: Instant) -> bool {
    pending_hashes >= HASH_BATCH_SIZE || now >= next_flush_at
}

fn emit_error(shared: &Shared, message: String) {
    events::emit_error(shared, message);
}

fn emit_event(shared: &Shared, event: BackendEvent) {
    events::emit_event(shared, event);
}

fn forward_event(shared: &Shared, event: BackendEvent) {
    events::forward_event(shared, event);
}

#[cfg(test)]
mod tests {
    use super::{
        emit_error, forward_event, lane_quota_for_chunk, should_flush_hashes, start_assignment,
        BackendEvent, CpuBackend, MiningSolution, ERROR_EVENT_MAX_BLOCK, HASH_BATCH_SIZE,
    };
    use crate::backend::PowBackend;
    use crate::config::CpuAffinityMode;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

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
    fn hash_flush_triggers_on_time_or_batch_threshold() {
        let now = Instant::now();
        assert!(should_flush_hashes(
            HASH_BATCH_SIZE,
            now,
            now + Duration::from_secs(1)
        ));
        assert!(should_flush_hashes(1, now, now));
        assert!(!should_flush_hashes(1, now, now + Duration::from_secs(1)));
    }

    #[test]
    fn error_event_retries_until_queue_capacity_is_available() {
        let backend = CpuBackend::new(1, CpuAffinityMode::Off);
        backend.shared.started.store(true, Ordering::Release);
        backend.set_instance_id(7);
        let (event_tx, event_rx) = crossbeam_channel::bounded(1);
        backend.set_event_sink(event_tx.clone());
        let (dispatch_tx, dispatch_rx) = crossbeam_channel::unbounded::<BackendEvent>();
        if let Ok(mut slot) = backend.shared.event_dispatch_tx.write() {
            *slot = Some(dispatch_tx);
        }

        event_tx
            .send(BackendEvent::Solution(MiningSolution {
                epoch: 1,
                nonce: 1,
                backend_id: 99,
                backend: "cpu",
            }))
            .expect("prefill should succeed");

        let shared = Arc::clone(&backend.shared);
        let forwarder = thread::spawn(move || {
            while let Ok(event) = dispatch_rx.recv() {
                forward_event(&shared, event);
            }
        });
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
        if let Ok(mut slot) = backend.shared.event_dispatch_tx.write() {
            *slot = None;
        }
        forwarder
            .join()
            .expect("forwarder thread should stop after queue closes");
    }

    #[test]
    fn error_event_drops_after_sustained_backpressure() {
        let backend = CpuBackend::new(1, CpuAffinityMode::Off);
        backend.shared.started.store(true, Ordering::Release);
        backend.set_instance_id(9);
        let (event_tx, _event_rx) = crossbeam_channel::bounded(1);
        backend.set_event_sink(event_tx.clone());
        let (dispatch_tx, dispatch_rx) = crossbeam_channel::unbounded::<BackendEvent>();
        if let Ok(mut slot) = backend.shared.event_dispatch_tx.write() {
            *slot = Some(dispatch_tx);
        }

        event_tx
            .send(BackendEvent::Solution(MiningSolution {
                epoch: 1,
                nonce: 1,
                backend_id: 99,
                backend: "cpu",
            }))
            .expect("prefill should succeed");

        let shared = Arc::clone(&backend.shared);
        let forwarder = thread::spawn(move || {
            while let Ok(event) = dispatch_rx.recv() {
                forward_event(&shared, event);
            }
        });
        let shared = Arc::clone(&backend.shared);
        let emit_thread = thread::spawn(move || {
            emit_error(&shared, "synthetic backpressure".to_string());
        });

        emit_thread
            .join()
            .expect("error emitter thread should complete under backpressure");
        let deadline = Instant::now() + ERROR_EVENT_MAX_BLOCK + Duration::from_millis(250);
        while backend.shared.dropped_events.load(Ordering::Acquire) == 0
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            backend.shared.dropped_events.load(Ordering::Acquire) >= 1,
            "error event should eventually drop under sustained queue backpressure"
        );

        if let Ok(mut slot) = backend.shared.event_dispatch_tx.write() {
            *slot = None;
        }
        forwarder
            .join()
            .expect("forwarder thread should stop after queue closes");
    }

    #[test]
    fn start_assignment_does_not_reset_hashes_while_workers_active() {
        let backend = CpuBackend::new(1, CpuAffinityMode::Off);
        backend.shared.active_workers.store(1, Ordering::Release);
        backend
            .shared
            .assignment_hashes
            .store(91, Ordering::Release);
        backend
            .shared
            .assignment_generation
            .store(1, Ordering::Release);
        if let Ok(mut started_at) = backend.shared.assignment_started_at.lock() {
            *started_at = Some(Instant::now());
        }

        start_assignment(&backend.shared, 2);

        assert_eq!(backend.shared.assignment_hashes.load(Ordering::Acquire), 91);
        assert_eq!(
            backend.shared.assignment_generation.load(Ordering::Acquire),
            2
        );
    }

    #[test]
    fn start_assignment_finalizes_previous_window_when_idle() {
        let backend = CpuBackend::new(1, CpuAffinityMode::Off);
        backend.shared.active_workers.store(0, Ordering::Release);
        backend
            .shared
            .assignment_hashes
            .store(33, Ordering::Release);
        backend
            .shared
            .assignment_generation
            .store(1, Ordering::Release);
        backend
            .shared
            .assignment_reported_generation
            .store(0, Ordering::Release);
        if let Ok(mut started_at) = backend.shared.assignment_started_at.lock() {
            *started_at = Some(Instant::now() - Duration::from_millis(5));
        }

        start_assignment(&backend.shared, 2);

        assert_eq!(
            backend.shared.completed_assignments.load(Ordering::Acquire),
            1
        );
        assert_eq!(
            backend
                .shared
                .completed_assignment_hashes
                .load(Ordering::Acquire),
            33
        );
        assert_eq!(backend.shared.assignment_hashes.load(Ordering::Acquire), 0);
        assert_eq!(
            backend.shared.assignment_generation.load(Ordering::Acquire),
            2
        );
        assert!(backend
            .shared
            .assignment_started_at
            .lock()
            .expect("assignment_started_at lock should not be poisoned")
            .is_some());
    }

    #[test]
    fn fence_with_deadline_times_out_when_workers_stay_active() {
        let backend = CpuBackend::new(1, CpuAffinityMode::Off);
        backend.shared.started.store(true, Ordering::Release);
        backend.shared.active_workers.store(1, Ordering::Release);

        let err = backend
            .fence_with_deadline(Instant::now() + Duration::from_millis(5))
            .expect_err("fence should time out when workers never become idle");
        assert!(format!("{err:#}").contains("idle wait"));

        backend.shared.active_workers.store(0, Ordering::Release);
        backend.shared.idle_cv.notify_all();
    }

    #[test]
    fn cancel_with_deadline_times_out_when_workers_stay_active() {
        let backend = CpuBackend::new(1, CpuAffinityMode::Off);
        backend.shared.started.store(true, Ordering::Release);
        backend.shared.active_workers.store(1, Ordering::Release);

        let err = backend
            .cancel_work_with_deadline(Instant::now() + Duration::from_millis(5))
            .expect_err("cancel should time out when workers never become idle");
        assert!(format!("{err:#}").contains("idle wait"));

        backend.shared.active_workers.store(0, Ordering::Release);
        backend.shared.idle_cv.notify_all();
    }
}
