#[cfg(any(target_os = "windows", test))]
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use blocknet_pow_kernel as fixed_argon;
use blocknet_pow_spec::{CPU_LANE_MEMORY_BYTES, POW_MEMORY_KB, POW_OUTPUT_LEN};
use crossbeam_channel::{bounded, Sender};

use crate::backend::{
    AssignmentSemantics, BackendCapabilities, BackendEvent, BackendExecutionModel,
    BackendInstanceId, BackendTelemetry, BenchBackend, DeadlineSupport, KernelBenchSample,
    PowBackend, PreemptionGranularity, WorkAssignment, WORK_ID_MAX,
};
use crate::config::{CpuAffinityMode, CpuPageMode};

#[path = "cpu/events.rs"]
mod events;
#[path = "cpu/kernel.rs"]
mod kernel;
#[path = "cpu/sampling.rs"]
mod sampling;
#[cfg(target_os = "linux")]
#[path = "cpu/topology.rs"]
mod topology;

const DEFAULT_HASH_BATCH_SIZE: u64 = 64;
const DEFAULT_CONTROL_CHECK_INTERVAL_HASHES: u64 = 1;
const DEFAULT_HASH_FLUSH_INTERVAL: Duration = Duration::from_millis(50);
// Keep stop/deadline checks bounded even when hash-interval control checks are coarse.
const MAX_DEADLINE_CHECK_INTERVAL: Duration = Duration::from_millis(5);
const NONBLOCKING_POLL_MIN: Duration = Duration::from_micros(50);
const NONBLOCKING_POLL_MAX: Duration = Duration::from_millis(1);
const CRITICAL_EVENT_RETRY_WAIT: Duration = Duration::from_millis(5);
const CRITICAL_EVENT_RETRY_MAX_WAIT: Duration = Duration::from_millis(100);
const SOLVED_MASK: u64 = 1u64 << 63;
const STARTUP_READY_WAIT_SLICE: Duration = Duration::from_millis(50);
const STARTUP_READY_TIMEOUT_BASE: Duration = Duration::from_secs(15);
const STARTUP_READY_TIMEOUT_PER_WORKER: Duration = Duration::from_secs(1);
const STARTUP_READY_TIMEOUT_PER_GIB: Duration = Duration::from_secs(1);
const STARTUP_READY_TIMEOUT_MAX: Duration = Duration::from_secs(180);
const BYTES_PER_GIB: u64 = 1024 * 1024 * 1024;
const DEFAULT_EVENT_DISPATCH_CAPACITY: usize = 256;

/// On macOS, request the highest QoS class so the scheduler prefers performance
/// cores. This is a best-effort scheduling hint, not hard CPU pinning. No-op on
/// other platforms.
#[inline]
fn set_thread_high_perf() {
    #[cfg(target_os = "macos")]
    {
        extern "C" {
            fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
        }
        // QOS_CLASS_USER_INTERACTIVE = 0x21 — highest-priority scheduler preference.
        const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
        unsafe {
            let _ = pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CpuBackendTuning {
    pub hash_batch_size: u64,
    pub control_check_interval_hashes: u64,
    pub hash_flush_interval: Duration,
    pub event_dispatch_capacity: usize,
    pub page_mode: CpuPageMode,
    pub kernel: Option<fixed_argon::CpuKernel>,
}

impl Default for CpuBackendTuning {
    fn default() -> Self {
        Self {
            hash_batch_size: DEFAULT_HASH_BATCH_SIZE,
            control_check_interval_hashes: DEFAULT_CONTROL_CHECK_INTERVAL_HASHES,
            hash_flush_interval: DEFAULT_HASH_FLUSH_INTERVAL,
            event_dispatch_capacity: DEFAULT_EVENT_DISPATCH_CAPACITY,
            page_mode: CpuPageMode::Auto,
            kernel: None,
        }
    }
}

impl CpuBackendTuning {
    fn normalized(self) -> Self {
        Self {
            hash_batch_size: self.hash_batch_size.max(1),
            control_check_interval_hashes: self.control_check_interval_hashes.max(1),
            hash_flush_interval: self.hash_flush_interval.max(Duration::from_millis(1)),
            event_dispatch_capacity: self.event_dispatch_capacity.max(1),
            page_mode: self.page_mode,
            kernel: self.kernel,
        }
    }
}

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
    adaptive: bool,
    desired_workers: AtomicUsize,
    lifetime_hashes: AtomicU64,
    warmed_workers: AtomicUsize,
    worker_allocation_failures: AtomicU64,
    arena_committed_bytes: AtomicU64,
    instance_id: AtomicU64,
    solution_state: AtomicU64,
    error_emitted: AtomicBool,
    work_generation: AtomicU64,
    active_workers: AtomicUsize,
    ready_workers: AtomicUsize,
    startup_failed: AtomicBool,
    startup_error: Mutex<Option<String>>,
    work_control: Mutex<ControlState>,
    work_cv: Condvar,
    idle_lock: Mutex<()>,
    idle_cv: Condvar,
    ready_lock: Mutex<()>,
    ready_cv: Condvar,
    hash_slots: Vec<PaddedAtomicU64>,
    finished_generations: Vec<AtomicU64>,
    assignment_hashes: AtomicU64,
    assignment_generation: AtomicU64,
    assignment_reported_generation: AtomicU64,
    assignment_started_at: Mutex<Option<Instant>>,
    completed_assignments: AtomicU64,
    completed_assignment_hashes: AtomicU64,
    completed_assignment_micros: AtomicU64,
    dropped_events: AtomicU64,
    arena_explicit_large_workers: AtomicU64,
    arena_explicit_large_1g_workers: AtomicU64,
    arena_transparent_huge_workers: AtomicU64,
    arena_regular_workers: AtomicU64,
    arena_heap_workers: AtomicU64,
    arena_explicit_large_bytes: AtomicU64,
    arena_explicit_large_1g_bytes: AtomicU64,
    arena_transparent_huge_bytes: AtomicU64,
    arena_regular_bytes: AtomicU64,
    arena_heap_bytes: AtomicU64,
    arena_allocation_failures: AtomicU64,
    event_dispatch_tx: RwLock<Option<Sender<BackendEvent>>>,
    event_sink: RwLock<Option<Sender<BackendEvent>>>,
    hash_batch_size: u64,
    control_check_interval_hashes: u64,
    hash_flush_interval: Duration,
    event_dispatch_capacity: usize,
    page_mode: CpuPageMode,
    kernel: fixed_argon::CpuKernel,
    // Low byte identifies the path; upper bits identify this particular switch.
    kernel_request: AtomicU64,
    kernel_acknowledgements: Vec<AtomicU64>,
}

pub struct CpuBackend {
    threads: usize,
    initial_workers: usize,
    worker_capacity: AtomicUsize,
    affinity_mode: CpuAffinityMode,
    shared: Arc<Shared>,
    worker_handles: Mutex<Vec<JoinHandle<()>>>,
    event_forward_handle: Mutex<Option<JoinHandle<()>>>,
}

impl CpuBackend {
    #[cfg(test)]
    pub fn new(threads: usize, affinity_mode: CpuAffinityMode) -> Self {
        Self::with_tuning(threads, affinity_mode, CpuBackendTuning::default())
    }

    pub fn with_tuning(
        threads: usize,
        affinity_mode: CpuAffinityMode,
        tuning: CpuBackendTuning,
    ) -> Self {
        let tuning = tuning.normalized();
        let lanes = threads.max(1);
        Self {
            threads,
            initial_workers: lanes,
            worker_capacity: AtomicUsize::new(lanes),
            affinity_mode,
            shared: Arc::new(Shared {
                started: AtomicBool::new(false),
                adaptive: false,
                desired_workers: AtomicUsize::new(lanes),
                lifetime_hashes: AtomicU64::new(0),
                warmed_workers: AtomicUsize::new(0),
                worker_allocation_failures: AtomicU64::new(0),
                arena_committed_bytes: AtomicU64::new(0),
                instance_id: AtomicU64::new(0),
                solution_state: AtomicU64::new(0),
                error_emitted: AtomicBool::new(false),
                work_generation: AtomicU64::new(0),
                active_workers: AtomicUsize::new(0),
                ready_workers: AtomicUsize::new(0),
                startup_failed: AtomicBool::new(false),
                startup_error: Mutex::new(None),
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
                finished_generations: (0..lanes).map(|_| AtomicU64::new(0)).collect(),
                assignment_hashes: AtomicU64::new(0),
                assignment_generation: AtomicU64::new(0),
                assignment_reported_generation: AtomicU64::new(0),
                assignment_started_at: Mutex::new(None),
                completed_assignments: AtomicU64::new(0),
                completed_assignment_hashes: AtomicU64::new(0),
                completed_assignment_micros: AtomicU64::new(0),
                dropped_events: AtomicU64::new(0),
                arena_explicit_large_workers: AtomicU64::new(0),
                arena_explicit_large_1g_workers: AtomicU64::new(0),
                arena_transparent_huge_workers: AtomicU64::new(0),
                arena_regular_workers: AtomicU64::new(0),
                arena_heap_workers: AtomicU64::new(0),
                arena_explicit_large_bytes: AtomicU64::new(0),
                arena_explicit_large_1g_bytes: AtomicU64::new(0),
                arena_transparent_huge_bytes: AtomicU64::new(0),
                arena_regular_bytes: AtomicU64::new(0),
                arena_heap_bytes: AtomicU64::new(0),
                arena_allocation_failures: AtomicU64::new(0),
                event_dispatch_tx: RwLock::new(None),
                event_sink: RwLock::new(None),
                hash_batch_size: tuning.hash_batch_size,
                control_check_interval_hashes: tuning.control_check_interval_hashes,
                hash_flush_interval: tuning.hash_flush_interval,
                event_dispatch_capacity: tuning.event_dispatch_capacity,
                page_mode: tuning.page_mode,
                kernel_request: AtomicU64::new(kernel_id(
                    tuning
                        .kernel
                        .unwrap_or_else(fixed_argon::CpuKernel::preferred),
                )),
                kernel_acknowledgements: (0..lanes).map(|_| AtomicU64::new(0)).collect(),
                kernel: tuning
                    .kernel
                    .unwrap_or_else(fixed_argon::CpuKernel::preferred),
            }),
            worker_handles: Mutex::new(Vec::new()),
            event_forward_handle: Mutex::new(None),
        }
    }

    fn reset_runtime_state(&self) {
        self.shared.desired_workers.store(
            self.initial_workers.min(self.worker_capacity()),
            Ordering::Release,
        );
        self.shared.lifetime_hashes.store(0, Ordering::Release);
        for ack in &self.shared.kernel_acknowledgements {
            ack.store(0, Ordering::Release);
        }
        self.shared.warmed_workers.store(0, Ordering::Release);
        self.shared
            .arena_committed_bytes
            .store(0, Ordering::Release);
        self.shared
            .worker_allocation_failures
            .store(0, Ordering::Release);
        self.shared.solution_state.store(0, Ordering::Release);
        self.shared.error_emitted.store(false, Ordering::Release);
        self.shared.work_generation.store(0, Ordering::Release);
        self.shared.active_workers.store(0, Ordering::Release);
        self.shared.ready_workers.store(0, Ordering::Release);
        self.shared.startup_failed.store(false, Ordering::Release);
        if let Ok(mut startup_error) = self.shared.startup_error.lock() {
            *startup_error = None;
        }
        self.shared.assignment_hashes.store(0, Ordering::Release);
        self.shared
            .assignment_generation
            .store(0, Ordering::Release);
        self.shared
            .assignment_reported_generation
            .store(0, Ordering::Release);
        self.shared
            .completed_assignments
            .store(0, Ordering::Release);
        self.shared
            .completed_assignment_hashes
            .store(0, Ordering::Release);
        self.shared
            .completed_assignment_micros
            .store(0, Ordering::Release);
        self.shared.dropped_events.store(0, Ordering::Release);
        reset_arena_telemetry(&self.shared);
        reset_hash_slots(&self.shared.hash_slots);
        for generation in &self.shared.finished_generations {
            generation.store(0, Ordering::Release);
        }
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
        // Bound internal event buffering so sink stalls cannot grow memory without limit.
        let (dispatch_tx, dispatch_rx) =
            bounded::<BackendEvent>(self.shared.event_dispatch_capacity.max(1));
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

fn resource_spawn_error(err: &std::io::Error) -> bool {
    if matches!(
        err.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::OutOfMemory
    ) {
        return true;
    }
    #[cfg(target_os = "windows")]
    if err
        .raw_os_error()
        .is_some_and(windows_thread_resource_error)
    {
        return true;
    }
    false
}

#[cfg(any(target_os = "windows", test))]
fn windows_thread_resource_error(code: i32) -> bool {
    // WinError.h: NOT_ENOUGH_MEMORY/OUTOFMEMORY (8/14), TOO_MANY_TCBS/
    // MAX_THRDS_REACHED (155/164), and system/pool/working-set/pagefile/commit
    // exhaustion (1450..=1455). Keep unrelated failures out of the retry path.
    // https://learn.microsoft.com/en-us/windows/win32/debug/system-error-codes--0-499-
    // https://learn.microsoft.com/en-us/windows/win32/debug/system-error-codes--1300-1699-
    matches!(code, 8 | 14 | 155 | 164 | 1450..=1455)
}

impl CpuBackend {
    /// Provisioned OS workers. The scheduling lanes/nonce stride stay fixed at
    /// construction, including when startup must fall back to fewer workers.
    pub(crate) fn worker_capacity(&self) -> usize {
        self.worker_capacity.load(Ordering::Acquire)
    }

    fn start_with_spawner(
        &self,
        mut spawn: impl FnMut(
            Arc<Shared>,
            usize,
            Option<core_affinity::CoreId>,
        ) -> std::io::Result<JoinHandle<()>>,
    ) -> Result<()> {
        if !self.shared.kernel.is_supported() {
            return Err(anyhow!("unsupported CPU kernel {}", self.shared.kernel));
        }
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

        let core_ids = resolve_affinity_core_ids(self.affinity_mode);

        let spawned_handles = loop {
            let capacity = self.worker_capacity();
            let mut spawned_handles = Vec::with_capacity(capacity);
            let mut spawn_error = None;
            for thread_idx in 0..capacity {
                let core_id = core_ids
                    .as_ref()
                    .and_then(|ids| ids.get(thread_idx % ids.len()))
                    .copied();
                match spawn(Arc::clone(&self.shared), thread_idx, core_id) {
                    Ok(handle) => spawned_handles.push(handle),
                    Err(err) => {
                        spawn_error = Some((thread_idx, err));
                        break;
                    }
                }
            }
            let Some((thread_idx, err)) = spawn_error else {
                break spawned_handles;
            };
            // No work has been assigned yet. Tear down the whole attempt before
            // retrying so no lane/arena leaks and no partially initialized state
            // reach the scheduler. Halving leaves room for later control/network
            // threads, even if a limit raced the cgroup snapshot or is not v2.
            request_shutdown(&self.shared);
            for handle in spawned_handles {
                let _ = handle.join();
            }
            if self.shared.adaptive && thread_idx > 0 && resource_spawn_error(&err) {
                let reduced = (thread_idx / 2).max(1);
                self.worker_capacity.store(reduced, Ordering::Release);
                self.reset_runtime_state();
                emit_warning(&self.shared, format!(
                    "CPU thread capacity reduced from {capacity} to {reduced} after worker spawn failed: {err}; restart to retry higher capacity"
                ));
                continue;
            }
            self.shared.started.store(false, Ordering::SeqCst);
            self.stop_event_forwarder();
            self.reset_runtime_state();
            return Err(anyhow!(
                "failed to spawn CPU worker thread {thread_idx}: {err}"
            ));
        };

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
            self.worker_capacity(),
            startup_ready_timeout(self.worker_capacity()),
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
}

impl PowBackend for CpuBackend {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn lanes(&self) -> usize {
        self.threads.max(1)
    }

    fn enabled_lanes(&self) -> usize {
        self.shared.desired_workers.load(Ordering::Acquire)
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
        self.start_with_spawner(|shared, index, core_id| {
            thread::Builder::new()
                .name(format!("seine-cpu-worker-{index}"))
                .spawn(move || cpu_worker_loop(shared, index, core_id))
        })
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

        {
            let mut control = self
                .shared
                .work_control
                .lock()
                .map_err(|_| anyhow!("CPU control lock poisoned"))?;
            control.generation = control.generation.wrapping_add(1).max(1);
            control.work = Some(work);
            let generation = control.generation;
            start_assignment(&self.shared, generation);
            self.shared
                .work_generation
                .store(generation, Ordering::Release);
        }
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
            memory_explicit_large_workers: self
                .shared
                .arena_explicit_large_workers
                .load(Ordering::Acquire),
            memory_explicit_large_1g_workers: self
                .shared
                .arena_explicit_large_1g_workers
                .load(Ordering::Acquire),
            memory_transparent_huge_workers: self
                .shared
                .arena_transparent_huge_workers
                .load(Ordering::Acquire),
            memory_regular_workers: self.shared.arena_regular_workers.load(Ordering::Acquire),
            memory_heap_workers: self.shared.arena_heap_workers.load(Ordering::Acquire),
            memory_explicit_large_bytes: self
                .shared
                .arena_explicit_large_bytes
                .load(Ordering::Acquire),
            memory_explicit_large_1g_bytes: self
                .shared
                .arena_explicit_large_1g_bytes
                .load(Ordering::Acquire),
            memory_transparent_huge_bytes: self
                .shared
                .arena_transparent_huge_bytes
                .load(Ordering::Acquire),
            memory_regular_bytes: self.shared.arena_regular_bytes.load(Ordering::Acquire),
            memory_heap_bytes: self.shared.arena_heap_bytes.load(Ordering::Acquire),
            memory_allocation_failures: self
                .shared
                .arena_allocation_failures
                .swap(0, Ordering::AcqRel),
            ..BackendTelemetry::default()
        }
    }

    fn preemption_granularity(&self) -> PreemptionGranularity {
        PreemptionGranularity::Hashes(self.shared.control_check_interval_hashes.max(1))
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            preferred_iters_per_lane: None,
            preferred_allocation_iters_per_lane: None,
            preferred_hash_poll_interval: None,
            preferred_assignment_timeout: None,
            preferred_control_timeout: None,
            preferred_assignment_timeout_strikes: None,
            preferred_worker_queue_depth: None,
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
        Ok(self.sample_kernel(seconds, false, shutdown)?.hashes)
    }
}

impl CpuBackend {
    /// Allocate only the initial workers' arenas. Parked lanes retain their
    /// nonce cursors, and keep a fixed stride when the active count changes.
    pub(crate) fn adaptive(
        initial: usize,
        capacity: usize,
        affinity: CpuAffinityMode,
        tuning: CpuBackendTuning,
    ) -> Self {
        let mut backend = Self::with_tuning(capacity.max(1), affinity, tuning);
        backend.initial_workers = initial.min(capacity);
        let shared = Arc::get_mut(&mut backend.shared).expect("new CPU backend is unshared");
        shared.adaptive = true;
        shared
            .desired_workers
            .store(backend.initial_workers, Ordering::Relaxed);
        backend
    }

    pub(crate) fn set_active_threads(&self, count: usize) {
        if let Ok(_control) = self.shared.work_control.lock() {
            self.shared
                .desired_workers
                .store(count.min(self.worker_capacity()), Ordering::Release);
            self.shared.work_cv.notify_all();
        }
    }

    /// Request a switch at each worker's next hash boundary. Sampling must wait
    /// for `kernel_ready`, which includes one completed hash on the new path.
    pub(crate) fn set_active_kernel(&self, kernel: fixed_argon::CpuKernel) -> Result<()> {
        if !self.shared.adaptive {
            return Err(anyhow!("kernel switching requires an adaptive CPU backend"));
        }
        if !kernel.is_supported() {
            return Err(anyhow!("unsupported CPU kernel {kernel}"));
        }
        self.shared
            .kernel_request
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                (kernel_from_request(old) != kernel)
                    .then(|| (old.wrapping_add(256) & !255) | kernel_id(kernel))
            })
            .ok();
        Ok(())
    }

    pub(crate) fn adaptive_snapshot(&self) -> CpuAdaptiveSnapshot {
        let s = &self.shared;
        let threads = s.desired_workers.load(Ordering::Acquire);
        let request = s.kernel_request.load(Ordering::Acquire);
        CpuAdaptiveSnapshot {
            kernel: kernel_from_request(request),
            kernel_generation: request,
            kernel_ready: threads > 0
                && s.kernel_acknowledgements
                    .iter()
                    .take(threads)
                    .all(|ack| ack.load(Ordering::Acquire) == request),
            backing_bytes: [
                s.arena_explicit_large_bytes.load(Ordering::Acquire),
                s.arena_explicit_large_1g_bytes.load(Ordering::Acquire),
                s.arena_transparent_huge_bytes.load(Ordering::Acquire),
                s.arena_regular_bytes.load(Ordering::Acquire),
                s.arena_heap_bytes.load(Ordering::Acquire),
            ],
            running: s.started.load(Ordering::Acquire),
            threads: s.desired_workers.load(Ordering::Acquire),
            committed_bytes: s.arena_committed_bytes.load(Ordering::Acquire),
            resident_bytes: s.arena_explicit_large_bytes.load(Ordering::Acquire)
                + s.arena_explicit_large_1g_bytes.load(Ordering::Acquire)
                + s.arena_transparent_huge_bytes.load(Ordering::Acquire)
                + s.arena_regular_bytes.load(Ordering::Acquire)
                + s.arena_heap_bytes.load(Ordering::Acquire),
            hashes: s.lifetime_hashes.load(Ordering::Acquire),
            hashing: s.active_workers.load(Ordering::Acquire) > 0,
            allocation_failures: s.worker_allocation_failures.load(Ordering::Acquire),
            warmed: s.warmed_workers.load(Ordering::Acquire),
        }
    }

    /// Warm every arena before scoring steady-state hashing. Keep lifecycle wall
    /// time available for diagnostics, without penalizing larger arena counts.
    pub(crate) fn autotune_sample(
        &self,
        seconds: u64,
        shutdown: &AtomicBool,
    ) -> Result<KernelBenchSample> {
        self.sample_kernel(seconds, true, shutdown)
    }

    fn sample_kernel(
        &self,
        seconds: u64,
        warmup: bool,
        shutdown: &AtomicBool,
    ) -> Result<KernelBenchSample> {
        if !self.shared.kernel.is_supported() {
            return Err(anyhow!("unsupported CPU kernel {}", self.shared.kernel));
        }
        reset_arena_telemetry(&self.shared);
        let lanes = self.threads.max(1);
        let core_ids = resolve_affinity_core_ids(self.affinity_mode);
        let page_mode = self.shared.page_mode;
        sampling::sample(
            lanes,
            Duration::from_secs(seconds.max(1)),
            warmup,
            shutdown,
            |lane| {
                set_thread_high_perf();
                if let Some(core_id) = core_ids
                    .as_ref()
                    .and_then(|ids| ids.get(lane % ids.len()))
                    .copied()
                {
                    let _ = core_affinity::set_for_current(core_id);
                }
                let hasher =
                    fixed_argon::FixedArgon2id::with_kernel(POW_MEMORY_KB, self.shared.kernel)
                        .expect("kernel support checked before benchmark workers start");
                let block_count = hasher.block_count();
                let mut arena = kernel::PowArena::new(block_count, page_mode).map_err(|err| {
                    anyhow!(
                        "cpu lane {lane}: {} page-mode arena allocation failed ({err})",
                        page_mode.as_str()
                    )
                })?;
                kernel::record_arena_backing(&self.shared, &arena, block_count);
                let mut header_base = [0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN];
                for (i, byte) in header_base.iter_mut().enumerate() {
                    *byte = (i as u8).wrapping_mul(31).wrapping_add(7);
                }
                let mut output = [0u8; POW_OUTPUT_LEN];
                let mut nonce = lane as u64;
                Ok(move || {
                    hasher
                        .hash_password_into_with_memory(
                            &nonce.to_le_bytes(),
                            &header_base,
                            &mut output,
                            arena.as_mut_slice(),
                        )
                        .map_err(|err| anyhow!("cpu lane {lane}: hash failed ({err:?})"))?;
                    nonce = nonce.wrapping_add(lanes as u64);
                    Ok(())
                })
            },
        )
    }
}

fn cpu_worker_loop(shared: Arc<Shared>, thread_idx: usize, core_id: Option<core_affinity::CoreId>) {
    kernel::cpu_worker_loop(shared, thread_idx, core_id);
}

fn reset_arena_telemetry(shared: &Shared) {
    shared
        .arena_explicit_large_workers
        .store(0, Ordering::Release);
    shared
        .arena_explicit_large_1g_workers
        .store(0, Ordering::Release);
    shared
        .arena_transparent_huge_workers
        .store(0, Ordering::Release);
    shared.arena_regular_workers.store(0, Ordering::Release);
    shared.arena_heap_workers.store(0, Ordering::Release);
    shared
        .arena_explicit_large_bytes
        .store(0, Ordering::Release);
    shared
        .arena_explicit_large_1g_bytes
        .store(0, Ordering::Release);
    shared
        .arena_transparent_huge_bytes
        .store(0, Ordering::Release);
    shared.arena_regular_bytes.store(0, Ordering::Release);
    shared.arena_heap_bytes.store(0, Ordering::Release);
    shared.arena_allocation_failures.store(0, Ordering::Release);
}

fn resolve_affinity_core_ids(mode: CpuAffinityMode) -> Option<Vec<core_affinity::CoreId>> {
    if mode == CpuAffinityMode::Off {
        return None;
    }
    let core_ids = core_affinity::get_core_ids().filter(|ids| !ids.is_empty())?;
    if mode == CpuAffinityMode::Os {
        return Some(core_ids);
    }
    // Native Windows exposes reliable processor-core masks and benefits materially from
    // spreading memory-hard workers across physical cores before SMT siblings.
    // Linux spreads across NUMA nodes and L3 groups with complete, usable topology;
    // a single node/cache or incomplete topology retains OS order.
    #[cfg(target_os = "linux")]
    let core_ids = {
        let available = core_ids.iter().map(|cpu| cpu.id).collect();
        topology::detect(&available)
            .and_then(|topology| topology.ordered_cpus())
            .map(|ids| {
                ids.into_iter()
                    .map(|id| core_affinity::CoreId { id })
                    .collect()
            })
            .unwrap_or(core_ids)
    };
    #[cfg(target_os = "windows")]
    let core_ids = order_core_ids_physical_first(core_ids);
    #[cfg(target_os = "macos")]
    let core_ids = {
        let mut core_ids = core_ids;
        if mode == CpuAffinityMode::PcoreOnly {
            // core_affinity maps these values to Mach affinity tags on macOS, not hard CPU
            // identifiers. Limiting the distinct tag set complements the QoS preference above
            // but cannot guarantee that a worker runs on a particular P-core.
            if let Some(pcore_count) = macos_pcore_count() {
                core_ids.truncate(pcore_count.min(core_ids.len()));
            }
        }
        core_ids
    };
    if core_ids.is_empty() {
        None
    } else {
        Some(core_ids)
    }
}

pub(crate) fn affinity_topology_fingerprint() -> String {
    #[cfg(target_os = "linux")]
    {
        let available = core_affinity::get_core_ids()
            .unwrap_or_default()
            .into_iter()
            .map(|cpu| cpu.id)
            .collect();
        // Cache identity must change on cpuset/NUMA/L3 changes even at equal CPU counts.
        format!("{available:?}:{:?}", topology::detect(&available))
    }
    #[cfg(target_os = "windows")]
    {
        let available = core_affinity::get_core_ids()
            .unwrap_or_default()
            .into_iter()
            .map(|cpu| cpu.id)
            .collect();
        physical_topology_fingerprint(&available, platform_physical_core_groups(&available))
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    String::new()
}

#[cfg(any(target_os = "windows", test))]
fn physical_topology_fingerprint(
    available: &BTreeSet<usize>,
    groups: Option<Vec<Vec<usize>>>,
) -> String {
    let groups = groups.map(|mut groups| {
        for group in &mut groups {
            group.sort_unstable();
        }
        groups.sort_unstable();
        groups
    });
    // Equal-sized process masks can select different CCDs or SMT siblings.
    // Keep the mask even when Windows cannot supply complete core topology.
    format!("{available:?}:{groups:?}")
}

/// Orders native Windows logical CPUs so one hardware thread from every physical core is used
/// before SMT siblings. Incomplete or contradictory topology data retains the original order.
#[cfg(target_os = "windows")]
fn order_core_ids_physical_first(
    core_ids: Vec<core_affinity::CoreId>,
) -> Vec<core_affinity::CoreId> {
    let available = core_ids.iter().map(|core| core.id).collect::<BTreeSet<_>>();
    let Some(groups) = platform_physical_core_groups(&available) else {
        return core_ids;
    };
    order_core_ids_by_groups(core_ids, groups)
}

#[cfg(any(target_os = "windows", test))]
fn order_core_ids_by_groups(
    core_ids: Vec<core_affinity::CoreId>,
    groups: Vec<Vec<usize>>,
) -> Vec<core_affinity::CoreId> {
    let available = core_ids.iter().map(|core| core.id).collect::<BTreeSet<_>>();
    let mut topology_ids = BTreeSet::new();
    for group in &groups {
        if group.is_empty() {
            return core_ids;
        }
        for id in group {
            if !available.contains(id) || !topology_ids.insert(*id) {
                return core_ids;
            }
        }
    }
    if topology_ids != available {
        return core_ids;
    }

    let by_id = core_ids
        .iter()
        .copied()
        .map(|core| (core.id, core))
        .collect::<BTreeMap<_, _>>();
    let mut normalized = groups
        .into_iter()
        .map(|mut group| {
            group.sort_unstable();
            group
        })
        .collect::<Vec<_>>();
    normalized.sort_by_key(|group| group[0]);

    let mut ordered = Vec::with_capacity(core_ids.len());
    let mut seen = BTreeSet::new();
    let max_siblings = normalized.iter().map(Vec::len).max().unwrap_or(0);
    for sibling_index in 0..max_siblings {
        for group in &normalized {
            let Some(id) = group.get(sibling_index) else {
                continue;
            };
            if seen.insert(*id) {
                if let Some(core) = by_id.get(id) {
                    ordered.push(*core);
                }
            }
        }
    }

    debug_assert_eq!(seen, available);
    ordered
}

#[cfg(target_os = "windows")]
fn platform_physical_core_groups(available: &BTreeSet<usize>) -> Option<Vec<Vec<usize>>> {
    use std::mem::{size_of, MaybeUninit};
    use std::ptr;
    use windows_sys::Win32::System::SystemInformation::{
        GetLogicalProcessorInformation, RelationProcessorCore, SYSTEM_LOGICAL_PROCESSOR_INFORMATION,
    };

    let mut byte_len = 0u32;
    unsafe {
        // The first call reports the required buffer size.
        let _ = GetLogicalProcessorInformation(ptr::null_mut(), &mut byte_len);
    }
    let entry_size = size_of::<SYSTEM_LOGICAL_PROCESSOR_INFORMATION>();
    let entry_count = (byte_len as usize).checked_div(entry_size)?;
    if entry_count == 0 {
        return None;
    }

    let mut entries =
        vec![MaybeUninit::<SYSTEM_LOGICAL_PROCESSOR_INFORMATION>::uninit(); entry_count];
    let ok = unsafe { GetLogicalProcessorInformation(entries.as_mut_ptr().cast(), &mut byte_len) };
    if ok == 0 {
        return None;
    }

    let initialized_count = (byte_len as usize)
        .checked_div(entry_size)?
        .min(entry_count);
    let mut groups = Vec::new();
    for entry in &entries[..initialized_count] {
        let entry = unsafe { entry.assume_init_ref() };
        if entry.Relationship != RelationProcessorCore {
            continue;
        }
        let group = (0..usize::BITS as usize)
            .filter(|bit| entry.ProcessorMask & (1usize << bit) != 0)
            .filter(|bit| available.contains(bit))
            .collect::<Vec<_>>();
        if !group.is_empty() {
            groups.push(group);
        }
    }
    (!groups.is_empty()).then_some(groups)
}

#[cfg(target_os = "macos")]
fn macos_pcore_count() -> Option<usize> {
    extern "C" {
        fn sysctlbyname(
            name: *const u8,
            oldp: *mut std::ffi::c_void,
            oldlenp: *mut usize,
            newp: *const std::ffi::c_void,
            newlen: usize,
        ) -> i32;
    }
    let mut val: u32 = 0;
    let mut len = std::mem::size_of::<u32>();
    let name = b"hw.perflevel0.logicalcpu\0";
    let ret = unsafe {
        sysctlbyname(
            name.as_ptr(),
            &mut val as *mut u32 as *mut std::ffi::c_void,
            &mut len,
            std::ptr::null(),
            0,
        )
    };
    if ret == 0 && val > 0 {
        Some(val as usize)
    } else {
        None
    }
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
    thread_idx: usize,
) -> Result<CpuWorkUpdate> {
    let mut control = shared
        .work_control
        .lock()
        .map_err(|_| anyhow!("CPU control lock poisoned"))?;
    let mut observed_generation = seen_generation;

    loop {
        if control.shutdown {
            return Ok(CpuWorkUpdate::Shutdown);
        }
        if thread_idx >= shared.desired_workers.load(Ordering::Acquire) {
            return Ok(CpuWorkUpdate::Park);
        }

        if control.generation != observed_generation {
            let generation = control.generation;
            if let Some(work) = control.work.clone() {
                return Ok(CpuWorkUpdate::Work(generation, work));
            }
            observed_generation = generation;
        }

        control = shared
            .work_cv
            .wait(control)
            .map_err(|_| anyhow!("CPU control lock poisoned"))?;
    }
}

enum CpuWorkUpdate {
    Work(u64, Arc<WorkAssignment>),
    Park,
    Shutdown,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CpuAdaptiveSnapshot {
    pub kernel: fixed_argon::CpuKernel,
    pub kernel_generation: u64,
    pub kernel_ready: bool,
    /// Explicit large, 1 GiB, transparent huge, regular, and heap bytes.
    pub backing_bytes: [u64; 5],
    pub running: bool,
    pub threads: usize,
    // Includes allocations in progress, before their backing is observable.
    pub committed_bytes: u64,
    pub resident_bytes: u64,
    pub hashes: u64,
    pub hashing: bool,
    pub allocation_failures: u64,
    pub warmed: usize,
}

fn kernel_id(kernel: fixed_argon::CpuKernel) -> u64 {
    fixed_argon::CpuKernel::ALL
        .iter()
        .position(|&value| value == kernel)
        .unwrap() as u64
        + 1
}

fn kernel_from_request(request: u64) -> fixed_argon::CpuKernel {
    fixed_argon::CpuKernel::ALL[((request & 255) - 1) as usize]
}

fn wait_for_worker_enabled(shared: &Shared, thread_idx: usize) -> bool {
    let Ok(mut control) = shared.work_control.lock() else {
        return false;
    };
    while !control.shutdown && thread_idx >= shared.desired_workers.load(Ordering::Acquire) {
        let Ok(next) = shared.work_cv.wait(control) else {
            return false;
        };
        control = next;
    }
    !control.shutdown
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
    let per_lane_gib =
        CPU_LANE_MEMORY_BYTES.saturating_add(BYTES_PER_GIB.saturating_sub(1)) / BYTES_PER_GIB;
    let per_lane_gib = per_lane_gib.clamp(1, u32::MAX as u64) as u32;
    let per_worker_timeout = STARTUP_READY_TIMEOUT_PER_WORKER
        .saturating_add(STARTUP_READY_TIMEOUT_PER_GIB.saturating_mul(per_lane_gib));
    let worker_count = workers.min(u32::MAX as usize) as u32;
    STARTUP_READY_TIMEOUT_BASE
        .saturating_add(per_worker_timeout.saturating_mul(worker_count))
        .min(STARTUP_READY_TIMEOUT_MAX)
}

fn mark_worker_ready(shared: &Shared) {
    shared.ready_workers.fetch_add(1, Ordering::AcqRel);
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
            let detail = shared
                .startup_error
                .lock()
                .ok()
                .and_then(|slot| slot.clone())
                .unwrap_or_else(|| "unknown worker startup error".to_owned());
            return Err(anyhow!(
                "CPU worker startup failed before readiness barrier ({ready}/{expected_workers} ready): {detail}"
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
        .map(|control| {
            if control.work.is_none() || control.shutdown {
                return false;
            }
            let desired = shared.desired_workers.load(Ordering::Acquire);
            !shared.adaptive
                || desired == 0
                || shared
                    .finished_generations
                    .iter()
                    .take(desired)
                    .any(|finished| finished.load(Ordering::Acquire) != control.generation)
        })
        .unwrap_or(false)
}
fn start_assignment(shared: &Shared, generation: u64) {
    // Runtime callers hold work_control so the assignment and its published
    // generation become visible together, even on a resize notification.
    let _idle_guard = shared.idle_lock.lock().ok();
    let idle = shared.active_workers.load(Ordering::Acquire) == 0;
    if idle {
        // When a reassignment happens while workers are still draining, resetting counters here
        // can drop or misattribute tail hashes. Finalize/reset only after lanes are quiesced.
        finalize_assignment_counters(shared);
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
    let mut control = shared.work_control.lock().ok();
    let _idle_guard = shared.idle_lock.lock().ok();
    if shared.active_workers.load(Ordering::Acquire) != 0 {
        return;
    }
    finalize_assignment_counters(shared);
    clear_completed_fixed_work(shared, control.as_deref_mut());
}

fn clear_completed_fixed_work(shared: &Shared, control: Option<&mut ControlState>) {
    // Adaptive lanes may be parked with unfinished nonce quotas. Their pending
    // state is derived from enabled lanes' finished generations instead.
    if !shared.adaptive {
        if let Some(control) = control {
            if control.generation == shared.assignment_generation.load(Ordering::Acquire) {
                control.work = None;
            }
        }
    }
}

fn finalize_assignment_counters(shared: &Shared) {
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
    let _idle_guard = shared.idle_lock.lock().ok();
    let previous = shared.active_workers.fetch_add(1, Ordering::AcqRel);
    if previous == 0 && shared.adaptive {
        // A parked lane may resume the same work generation after its earlier
        // execution interval was finalized. Reopen timing/accounting without
        // resetting any lane's nonce cursor or cumulative hash counter.
        if let Ok(mut started) = shared.assignment_started_at.lock() {
            if started.is_none() {
                shared.assignment_hashes.store(0, Ordering::Release);
                let generation = shared.assignment_generation.load(Ordering::Acquire);
                shared
                    .assignment_reported_generation
                    .store(generation.saturating_sub(1), Ordering::Release);
                *started = Some(Instant::now());
            }
        }
    }
    *worker_active = true;
}

fn activate_worker(
    shared: &Shared,
    thread_idx: usize,
    generation: u64,
    active: &mut bool,
) -> Result<bool> {
    if *active {
        return Ok(true);
    }
    // Serialize admission with cancel and resize. A fence must not observe zero
    // workers and return just before a parked lane starts an old assignment.
    let control = shared
        .work_control
        .lock()
        .map_err(|_| anyhow!("CPU control lock poisoned"))?;
    if control.shutdown
        || control.generation != generation
        || control.work.is_none()
        || thread_idx >= shared.desired_workers.load(Ordering::Acquire)
    {
        return Ok(false);
    }
    mark_worker_active(shared, active);
    Ok(true)
}

fn mark_worker_inactive(shared: &Shared, worker_active: &mut bool) {
    if !*worker_active {
        return;
    }
    let mut control = shared.work_control.lock().ok();
    let _idle_guard = shared.idle_lock.lock().ok();
    let prev = shared.active_workers.fetch_sub(1, Ordering::AcqRel);
    *worker_active = false;
    if prev <= 1 {
        finalize_assignment_counters(shared);
        clear_completed_fixed_work(shared, control.as_deref_mut());
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
    shared
        .lifetime_hashes
        .fetch_add(*pending_hashes, Ordering::Relaxed);
    *pending_hashes = 0;
}

fn should_flush_hashes(
    pending_hashes: u64,
    now: Instant,
    next_flush_at: Instant,
    hash_batch_size: u64,
) -> bool {
    pending_hashes >= hash_batch_size.max(1) || now >= next_flush_at
}

fn emit_warning(shared: &Shared, message: String) {
    events::emit_warning(shared, message);
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
        emit_error, forward_event, lane_quota_for_chunk, maybe_finalize_assignment,
        order_core_ids_by_groups, should_flush_hashes, start_assignment, BackendEvent, CpuBackend,
        DEFAULT_HASH_BATCH_SIZE,
    };
    use crate::backend::{MiningSolution, NonceChunk, PowBackend, WorkAssignment, WorkTemplate};
    use crate::config::CpuAffinityMode;
    use blocknet_pow_spec::POW_HEADER_BASE_LEN;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn windows_thread_resource_codes_are_retryable() {
        for code in [8, 14, 155, 164, 1450, 1451, 1452, 1453, 1454, 1455] {
            assert!(
                super::windows_thread_resource_error(code),
                "Windows code {code}"
            );
            #[cfg(target_os = "windows")]
            assert!(super::resource_spawn_error(
                &std::io::Error::from_raw_os_error(code)
            ));
        }
        // Permission/parameter errors and adjacent unrelated WinError values.
        for code in [
            0, 5, 6, 7, 9, 13, 15, 87, 154, 156, 163, 165, 1449, 1456, -1,
        ] {
            assert!(
                !super::windows_thread_resource_error(code),
                "Windows code {code}"
            );
        }
    }

    #[test]
    fn spawn_error_retry_excludes_non_resource_failures() {
        use std::io::ErrorKind;
        for kind in [ErrorKind::WouldBlock, ErrorKind::OutOfMemory] {
            assert!(super::resource_spawn_error(&kind.into()));
        }
        for kind in [
            ErrorKind::PermissionDenied,
            ErrorKind::InvalidInput,
            ErrorKind::Unsupported,
            ErrorKind::Other,
        ] {
            assert!(!super::resource_spawn_error(&kind.into()));
        }
        #[cfg(not(target_os = "windows"))]
        assert!(
            !super::resource_spawn_error(&std::io::Error::from_raw_os_error(1455)),
            "do not interpret another OS's raw error codes as Windows errors"
        );
    }

    #[test]
    fn adaptive_resume_reopens_accounting_without_resetting_hash_totals() {
        let backend = CpuBackend::adaptive(
            1,
            2,
            CpuAffinityMode::Off,
            super::CpuBackendTuning::default(),
        );
        let s = &backend.shared;
        start_assignment(s, 1);
        let mut active = false;
        super::mark_worker_active(s, &mut active);
        let mut hashes = 2;
        super::flush_hashes(s, 0, &mut hashes);
        super::mark_worker_inactive(s, &mut active);
        super::mark_worker_active(s, &mut active);
        hashes = 3;
        super::flush_hashes(s, 0, &mut hashes);
        super::mark_worker_inactive(s, &mut active);
        assert_eq!(s.completed_assignment_hashes.load(Ordering::Acquire), 5);
        assert_eq!(s.completed_assignments.load(Ordering::Acquire), 2);
        assert_eq!(backend.take_hashes(), 5);
        assert_eq!(backend.adaptive_snapshot().hashes, 5);
    }

    #[test]
    fn adaptive_failures_exclude_successful_page_mode_fallbacks() {
        let backend = CpuBackend::adaptive(
            1,
            2,
            CpuAffinityMode::Off,
            super::CpuBackendTuning::default(),
        );
        backend
            .shared
            .arena_allocation_failures
            .store(1, Ordering::Release);
        assert_eq!(backend.adaptive_snapshot().allocation_failures, 0);
        backend
            .shared
            .worker_allocation_failures
            .store(1, Ordering::Release);
        assert_eq!(backend.adaptive_snapshot().allocation_failures, 1);
    }

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
    fn affinity_order_spreads_workers_across_physical_cores_before_smt() {
        let cores = (0..8)
            .map(|id| core_affinity::CoreId { id })
            .collect::<Vec<_>>();
        let groups = vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6, 7]];
        let ordered = order_core_ids_by_groups(cores, groups)
            .into_iter()
            .map(|core| core.id)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec![0, 2, 4, 6, 1, 3, 5, 7]);
    }

    #[test]
    fn affinity_fingerprint_distinguishes_equal_sized_cpu_masks() {
        let left = [0, 2, 4, 6].into_iter().collect();
        let right = [16, 18, 20, 22].into_iter().collect();
        assert_ne!(
            super::physical_topology_fingerprint(&left, None),
            super::physical_topology_fingerprint(&right, None),
        );
    }

    #[test]
    fn affinity_fingerprint_distinguishes_smt_layout_and_canonicalizes_order() {
        let cpus = [0, 1, 2, 3].into_iter().collect();
        let paired =
            super::physical_topology_fingerprint(&cpus, Some(vec![vec![0, 1], vec![2, 3]]));
        assert_eq!(
            paired,
            super::physical_topology_fingerprint(&cpus, Some(vec![vec![3, 2], vec![1, 0]]),),
        );
        assert_ne!(
            paired,
            super::physical_topology_fingerprint(
                &cpus,
                Some(vec![vec![0], vec![1], vec![2], vec![3]]),
            ),
        );
        assert_ne!(paired, super::physical_topology_fingerprint(&cpus, None));
    }

    #[cfg(target_os = "windows")]
    #[test]
    #[ignore = "changes this test process's CPU mask; run alone with --test-threads=1"]
    fn windows_fingerprint_changes_with_process_cpu_mask() {
        use windows_sys::Win32::System::Threading::{
            GetCurrentProcess, GetProcessAffinityMask, SetProcessAffinityMask,
        };
        let process = unsafe { GetCurrentProcess() };
        let mut original = 0usize;
        let mut system = 0usize;
        assert_ne!(
            unsafe { GetProcessAffinityMask(process, &mut original, &mut system) },
            0
        );
        if original.count_ones() < 2 {
            eprintln!("requires two available logical CPUs; native mask check skipped");
            return;
        }
        struct RestoreMask(usize);
        impl Drop for RestoreMask {
            fn drop(&mut self) {
                assert_ne!(
                    unsafe { SetProcessAffinityMask(GetCurrentProcess(), self.0) },
                    0
                );
            }
        }
        let restore = RestoreMask(original);
        let before = super::affinity_topology_fingerprint();
        let first = 1usize << original.trailing_zeros();
        let last = 1usize << (usize::BITS - 1 - original.leading_zeros());
        assert_ne!(unsafe { SetProcessAffinityMask(process, first) }, 0);
        let first_fingerprint = super::affinity_topology_fingerprint();
        assert_ne!(unsafe { SetProcessAffinityMask(process, last) }, 0);
        let last_fingerprint = super::affinity_topology_fingerprint();
        drop(restore);
        assert_eq!(before, super::affinity_topology_fingerprint());
        assert_ne!(first_fingerprint, last_fingerprint);
        println!("native masks {first:#x} => {first_fingerprint}; {last:#x} => {last_fingerprint}; restored {original:#x}");
    }

    #[test]
    fn affinity_order_rejects_incomplete_topology_groups() {
        let cores = (0..5)
            .map(|id| core_affinity::CoreId { id })
            .collect::<Vec<_>>();
        let ordered = order_core_ids_by_groups(cores, vec![vec![0, 1], vec![2, 3]])
            .into_iter()
            .map(|core| core.id)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn affinity_order_rejects_overlapping_topology_groups() {
        let cores = (0..4)
            .map(|id| core_affinity::CoreId { id })
            .collect::<Vec<_>>();
        let ordered = order_core_ids_by_groups(cores, vec![vec![0, 1], vec![1, 2], vec![3]])
            .into_iter()
            .map(|core| core.id)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec![0, 1, 2, 3]);
    }

    #[test]
    fn affinity_order_rejects_unknown_topology_cpu() {
        let cores = (0..4)
            .map(|id| core_affinity::CoreId { id })
            .collect::<Vec<_>>();
        let ordered = order_core_ids_by_groups(cores, vec![vec![0, 1], vec![2, 4], vec![3]])
            .into_iter()
            .map(|core| core.id)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec![0, 1, 2, 3]);
    }

    #[test]
    fn hash_flush_triggers_on_time_or_batch_threshold() {
        let now = Instant::now();
        assert!(should_flush_hashes(
            DEFAULT_HASH_BATCH_SIZE,
            now,
            now + Duration::from_secs(1),
            DEFAULT_HASH_BATCH_SIZE,
        ));
        assert!(should_flush_hashes(1, now, now, DEFAULT_HASH_BATCH_SIZE,));
        assert!(!should_flush_hashes(
            1,
            now,
            now + Duration::from_secs(1),
            DEFAULT_HASH_BATCH_SIZE,
        ));
    }

    #[test]
    fn capabilities_do_not_force_hash_poll_hint() {
        let backend = CpuBackend::new(1, CpuAffinityMode::Off);
        assert!(backend
            .capabilities()
            .preferred_hash_poll_interval
            .is_none());
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
                hash: None,
                share_binding_id: 0,
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
                hash: None,
                share_binding_id: 0,
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
            .expect("error emitter thread should complete after enqueueing");
        thread::sleep(Duration::from_millis(100));
        assert_eq!(
            backend.shared.dropped_events.load(Ordering::Acquire),
            0,
            "error event should remain lossless while backend stays started"
        );

        backend.shared.started.store(false, Ordering::Release);
        let deadline = Instant::now() + Duration::from_millis(250);
        while backend.shared.dropped_events.load(Ordering::Acquire) == 0
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            backend.shared.dropped_events.load(Ordering::Acquire) >= 1,
            "error event should be dropped only after backend shutdown interrupts delivery"
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
    fn maybe_finalize_assignment_clears_completed_work_when_no_replacement_arrived() {
        let backend = CpuBackend::new(1, CpuAffinityMode::Off);
        backend
            .shared
            .assignment_generation
            .store(7, Ordering::Release);
        backend
            .shared
            .assignment_reported_generation
            .store(0, Ordering::Release);
        if let Ok(mut started_at) = backend.shared.assignment_started_at.lock() {
            *started_at = Some(Instant::now() - Duration::from_millis(5));
        }
        if let Ok(mut control) = backend.shared.work_control.lock() {
            control.generation = 7;
            control.work = Some(Arc::new(WorkAssignment {
                template: Arc::new(WorkTemplate {
                    work_id: 7,
                    epoch: 1,
                    header_base: Arc::from(vec![0u8; POW_HEADER_BASE_LEN]),
                    target: [0xFF; 32],
                    dynamic_share_target: None,
                    pause_on_solution: false,
                    stop_at: Instant::now() + Duration::from_secs(1),
                }),
                nonce_chunk: NonceChunk {
                    start_nonce: 0,
                    nonce_count: 4,
                },
            }));
        }

        maybe_finalize_assignment(&backend.shared);

        let control = backend
            .shared
            .work_control
            .lock()
            .expect("work control lock should not be poisoned");
        assert!(
            control.work.is_none(),
            "completed assignments should not remain marked pending"
        );
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

    #[test]
    fn event_backpressure_drops_after_max_retries() {
        // Verify that send_event_with_backpressure gives up after MAX_EVENT_SEND_RETRIES
        // instead of blocking forever. The existing test
        // `error_event_drops_after_sustained_backpressure` covers the "started=false" exit;
        // this test verifies the retry-count exit path by keeping the backend started but
        // the sink permanently full.
        let backend = CpuBackend::new(1, CpuAffinityMode::Off);
        backend.shared.started.store(true, Ordering::Release);
        backend.set_instance_id(42);
        // Create a bounded(0) sink so sends always time out
        let (dispatch_tx, _dispatch_rx) = crossbeam_channel::bounded::<BackendEvent>(0);
        if let Ok(mut slot) = backend.shared.event_dispatch_tx.write() {
            *slot = Some(dispatch_tx);
        }

        let shared = Arc::clone(&backend.shared);
        let handle = thread::spawn(move || {
            emit_error(&shared, "backpressure test".to_string());
        });

        // The thread should complete within a bounded time (retries * max_wait ~ 1s)
        // because it gives up after MAX_EVENT_SEND_RETRIES.
        let completed = handle.join().is_ok();
        assert!(
            completed,
            "emit_error should complete after hitting retry limit"
        );
        assert!(
            backend.shared.dropped_events.load(Ordering::Acquire) >= 1,
            "event should be dropped after max retries exhausted"
        );
    }

    #[test]
    fn reset_runtime_state_uses_release_ordering() {
        // Verify that reset_runtime_state can be called after stop completes.
        // This is a smoke test to confirm the Release ordering change doesn't
        // break the reset sequence.
        let backend = CpuBackend::new(1, CpuAffinityMode::Off);
        backend.shared.solution_state.store(99, Ordering::SeqCst);
        backend.shared.error_emitted.store(true, Ordering::SeqCst);
        backend.shared.work_generation.store(42, Ordering::SeqCst);
        backend
            .shared
            .assignment_hashes
            .store(100, Ordering::SeqCst);

        backend.reset_runtime_state();

        assert_eq!(backend.shared.solution_state.load(Ordering::Acquire), 0);
        assert!(!backend.shared.error_emitted.load(Ordering::Acquire));
        assert_eq!(backend.shared.work_generation.load(Ordering::Acquire), 0);
        assert_eq!(backend.shared.assignment_hashes.load(Ordering::Acquire), 0);
    }
}

#[cfg(test)]
mod kernel_selection_tests {
    use super::*;

    #[test]
    fn live_kernel_requests_reject_fixed_backends_and_unsupported_paths() {
        let fixed = CpuBackend::new(1, CpuAffinityMode::Off);
        assert!(fixed
            .set_active_kernel(fixed_argon::CpuKernel::preferred())
            .is_err());
        let adaptive =
            CpuBackend::adaptive(1, 1, CpuAffinityMode::Off, CpuBackendTuning::default());
        let initial = adaptive.shared.kernel_request.load(Ordering::Acquire);
        for kernel in fixed_argon::CpuKernel::ALL
            .into_iter()
            .filter(|k| !k.is_supported())
        {
            assert!(adaptive.set_active_kernel(kernel).is_err());
            assert_eq!(
                adaptive.shared.kernel_request.load(Ordering::Acquire),
                initial
            );
        }
        adaptive
            .set_active_kernel(fixed_argon::CpuKernel::preferred())
            .unwrap();
        assert_eq!(
            adaptive.shared.kernel_request.load(Ordering::Acquire),
            initial,
            "requesting the same path must not invalidate a measurement window"
        );
    }

    #[test]
    fn unsupported_kernel_fails_before_worker_or_arena_startup() {
        for kernel in fixed_argon::CpuKernel::ALL
            .into_iter()
            .filter(|k| !k.is_supported())
        {
            let backend = CpuBackend::with_tuning(
                1,
                CpuAffinityMode::Off,
                CpuBackendTuning {
                    kernel: Some(kernel),
                    ..CpuBackendTuning::default()
                },
            );
            assert!(backend
                .start()
                .unwrap_err()
                .to_string()
                .contains("unsupported CPU kernel"));
            assert!(!backend.shared.started.load(Ordering::Relaxed));
            assert!(backend
                .kernel_bench(1, &AtomicBool::new(false))
                .unwrap_err()
                .to_string()
                .contains("unsupported CPU kernel"));
        }
    }
}
