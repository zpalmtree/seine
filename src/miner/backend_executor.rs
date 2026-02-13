use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use crossbeam_channel::{
    bounded, RecvTimeoutError, SendTimeoutError, Sender, TryRecvError, TrySendError,
};

use crate::backend::{
    normalize_backend_capabilities, AssignmentSemantics, BackendCallStatus, BackendCapabilities,
    BackendExecutionModel, BackendInstanceId, BackendTelemetry, PowBackend, WorkAssignment,
};

use super::ui::warn;
use super::BackendSlot;

pub(super) enum BackendTaskKind {
    Assign(WorkAssignment),
    AssignBatch(Vec<WorkAssignment>),
    Cancel,
    Fence,
}

impl BackendTaskKind {
    fn action_label(&self) -> &'static str {
        match self {
            Self::Assign(_) | Self::AssignBatch(_) => "assignment",
            Self::Cancel => "cancel",
            Self::Fence => "fence",
        }
    }

    fn is_control(&self) -> bool {
        matches!(self, Self::Cancel | Self::Fence)
    }

    fn timeout_counter_bucket(&self, kind: BackendTaskTimeoutKind) -> TaskTimeoutCounterBucket {
        match (self.is_control(), kind) {
            (false, BackendTaskTimeoutKind::Enqueue) => TaskTimeoutCounterBucket::AssignmentEnqueue,
            (false, BackendTaskTimeoutKind::Execution) => {
                TaskTimeoutCounterBucket::AssignmentExecution
            }
            (true, BackendTaskTimeoutKind::Enqueue) => TaskTimeoutCounterBucket::ControlEnqueue,
            (true, BackendTaskTimeoutKind::Execution) => TaskTimeoutCounterBucket::ControlExecution,
        }
    }
}

pub(super) struct BackendTask {
    pub idx: usize,
    pub backend_id: BackendInstanceId,
    pub backend: &'static str,
    pub backend_handle: Arc<dyn PowBackend>,
    pub kind: BackendTaskKind,
    pub timeout: Duration,
}

#[derive(Debug)]
struct AssignmentPreemptedByControl;

impl fmt::Display for AssignmentPreemptedByControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("assignment preempted by pending control request")
    }
}

impl std::error::Error for AssignmentPreemptedByControl {}

pub(super) fn is_assignment_preempted_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.downcast_ref::<AssignmentPreemptedByControl>().is_some())
}

pub(super) struct BackendTaskOutcome {
    pub idx: usize,
    pub result: Result<()>,
}

#[derive(Clone)]
struct TimeoutTaskContext {
    backend_id: BackendInstanceId,
    backend: &'static str,
    action: &'static str,
    backend_handle: Arc<dyn PowBackend>,
    metrics: Arc<BackendWorkerMetrics>,
    enqueue_timeout_bucket: TaskTimeoutCounterBucket,
    execution_timeout_bucket: TaskTimeoutCounterBucket,
    dispatch_started_at: Instant,
    enqueued_at: Option<Instant>,
    deadline: Instant,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum BackendTaskTimeoutKind {
    Enqueue,
    Execution,
}

pub(super) enum BackendTaskDispatchResult {
    Completed(Result<()>),
    TimedOut(BackendTaskTimeoutKind),
}

enum BackendWorkerCommand {
    Run {
        task: BackendTask,
        deadline: Instant,
        outcome_tx: Sender<BackendTaskOutcome>,
        pending_control: Arc<AtomicUsize>,
    },
    Stop {
        backend_id: BackendInstanceId,
        backend: &'static str,
        backend_handle: Arc<dyn PowBackend>,
        done_tx: Sender<()>,
    },
}

#[derive(Clone)]
struct BackendWorker {
    assignment_tx: Sender<BackendWorkerCommand>,
    control_tx: Sender<BackendWorkerCommand>,
    pending_control: Arc<AtomicUsize>,
    metrics: Arc<BackendWorkerMetrics>,
}

#[derive(Clone)]
struct BackendWorkerHandles {
    assignment_tx: Sender<BackendWorkerCommand>,
    control_tx: Sender<BackendWorkerCommand>,
    pending_control: Arc<AtomicUsize>,
    metrics: Arc<BackendWorkerMetrics>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
struct BackendWorkerKey {
    backend_id: BackendInstanceId,
    backend_ptr: usize,
}

pub(super) struct AssignmentTimeoutDecision {
    pub strikes: u32,
    pub threshold: u32,
    pub should_quarantine: bool,
}

#[derive(Debug, Clone, Copy)]
enum TaskTimeoutCounterBucket {
    AssignmentEnqueue,
    AssignmentExecution,
    ControlEnqueue,
    ControlExecution,
}

#[derive(Debug, Clone, Copy, Default)]
struct BackendTimeoutCounters {
    assignment_enqueue: u64,
    assignment_execution: u64,
    control_enqueue: u64,
    control_execution: u64,
}

impl BackendTimeoutCounters {
    fn increment(&mut self, bucket: TaskTimeoutCounterBucket) {
        match bucket {
            TaskTimeoutCounterBucket::AssignmentEnqueue => {
                self.assignment_enqueue = self.assignment_enqueue.saturating_add(1)
            }
            TaskTimeoutCounterBucket::AssignmentExecution => {
                self.assignment_execution = self.assignment_execution.saturating_add(1)
            }
            TaskTimeoutCounterBucket::ControlEnqueue => {
                self.control_enqueue = self.control_enqueue.saturating_add(1)
            }
            TaskTimeoutCounterBucket::ControlExecution => {
                self.control_execution = self.control_execution.saturating_add(1)
            }
        }
    }

    fn is_zero(&self) -> bool {
        self.assignment_enqueue == 0
            && self.assignment_execution == 0
            && self.control_enqueue == 0
            && self.control_execution == 0
    }
}

const TASK_LATENCY_BUCKET_UPPER_BOUNDS_MICROS: [u64; 15] = [
    10,
    25,
    50,
    100,
    250,
    500,
    1_000,
    2_000,
    5_000,
    10_000,
    20_000,
    50_000,
    100_000,
    250_000,
    u64::MAX,
];

#[derive(Debug, Clone, Copy)]
struct TaskLatencyHistogram {
    sample_count: u64,
    max_micros: u64,
    buckets: [u64; TASK_LATENCY_BUCKET_UPPER_BOUNDS_MICROS.len()],
}

impl Default for TaskLatencyHistogram {
    fn default() -> Self {
        Self {
            sample_count: 0,
            max_micros: 0,
            buckets: [0; TASK_LATENCY_BUCKET_UPPER_BOUNDS_MICROS.len()],
        }
    }
}

impl TaskLatencyHistogram {
    fn observe(&mut self, micros: u64) {
        self.sample_count = self.sample_count.saturating_add(1);
        self.max_micros = self.max_micros.max(micros);
        let mut bucket_idx = TASK_LATENCY_BUCKET_UPPER_BOUNDS_MICROS
            .len()
            .saturating_sub(1);
        for (idx, upper_bound) in TASK_LATENCY_BUCKET_UPPER_BOUNDS_MICROS.iter().enumerate() {
            if micros <= *upper_bound {
                bucket_idx = idx;
                break;
            }
        }
        self.buckets[bucket_idx] = self.buckets[bucket_idx].saturating_add(1);
    }

    fn is_zero(&self) -> bool {
        self.sample_count == 0
    }

    fn p95_micros(&self) -> u64 {
        if self.sample_count == 0 {
            return 0;
        }
        let target = self.sample_count.saturating_mul(95).saturating_add(99) / 100;
        let mut cumulative = 0u64;
        for (idx, bucket_count) in self.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(*bucket_count);
            if cumulative >= target {
                let upper = TASK_LATENCY_BUCKET_UPPER_BOUNDS_MICROS[idx];
                if upper == u64::MAX {
                    return self.max_micros;
                }
                return upper;
            }
        }
        self.max_micros
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct BackendTaskLatencyCounters {
    assignment_enqueue: TaskLatencyHistogram,
    assignment_execution: TaskLatencyHistogram,
    control_enqueue: TaskLatencyHistogram,
    control_execution: TaskLatencyHistogram,
}

impl BackendTaskLatencyCounters {
    fn observe(&mut self, bucket: TaskTimeoutCounterBucket, micros: u64) {
        match bucket {
            TaskTimeoutCounterBucket::AssignmentEnqueue => self.assignment_enqueue.observe(micros),
            TaskTimeoutCounterBucket::AssignmentExecution => {
                self.assignment_execution.observe(micros)
            }
            TaskTimeoutCounterBucket::ControlEnqueue => self.control_enqueue.observe(micros),
            TaskTimeoutCounterBucket::ControlExecution => self.control_execution.observe(micros),
        }
    }

    fn is_zero(&self) -> bool {
        self.assignment_enqueue.is_zero()
            && self.assignment_execution.is_zero()
            && self.control_enqueue.is_zero()
            && self.control_execution.is_zero()
    }
}

#[derive(Default)]
struct BackendWorkerMetrics {
    timeout_counters: Mutex<BackendTimeoutCounters>,
    latency_counters: Mutex<BackendTaskLatencyCounters>,
    assignment_timeout_strikes: AtomicU32,
}

impl BackendWorkerMetrics {
    fn record_task_timeout(&self, bucket: TaskTimeoutCounterBucket) {
        if let Ok(mut counters) = self.timeout_counters.lock() {
            counters.increment(bucket);
        }
    }

    fn record_task_latency(&self, bucket: TaskTimeoutCounterBucket, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        if let Ok(mut counters) = self.latency_counters.lock() {
            counters.observe(bucket, micros);
        }
    }

    fn take_delta(&self) -> BackendTelemetry {
        let counters = self
            .timeout_counters
            .lock()
            .map(|mut counters| std::mem::take(&mut *counters))
            .unwrap_or_default();
        let latencies = self
            .latency_counters
            .lock()
            .map(|mut counters| std::mem::take(&mut *counters))
            .unwrap_or_default();
        let assignment_timeout_strikes = self.assignment_timeout_strikes.load(Ordering::Acquire);
        build_backend_telemetry(counters, latencies, assignment_timeout_strikes)
    }

    fn reset_assignment_timeout_strikes(&self) {
        self.assignment_timeout_strikes.store(0, Ordering::Release);
    }

    fn note_assignment_timeout_with_threshold(&self, threshold: u32) -> AssignmentTimeoutDecision {
        let threshold = threshold.max(1);
        let mut current = self.assignment_timeout_strikes.load(Ordering::Acquire);
        loop {
            let next = current.saturating_add(1);
            match self.assignment_timeout_strikes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let should_quarantine = next >= threshold;
                    if should_quarantine {
                        self.assignment_timeout_strikes.store(0, Ordering::Release);
                    }
                    return AssignmentTimeoutDecision {
                        strikes: next,
                        threshold,
                        should_quarantine,
                    };
                }
                Err(observed) => current = observed,
            }
        }
    }
}

const BACKEND_WORKER_QUEUE_CAPACITY_MAX: usize = 64;
const BACKEND_WORKER_CONTROL_QUEUE_CAPACITY: usize = 16;
const BACKEND_STOP_ACK_TIMEOUT: Duration = Duration::from_secs(1);
const BACKEND_STOP_ACK_GRACE_TIMEOUT: Duration = Duration::from_secs(2);
const BACKEND_STOP_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(100);
const BACKEND_STOP_FALLBACK_TIMEOUT: Duration = Duration::from_secs(2);
const BACKEND_TASK_ENQUEUE_RETRY_MIN: Duration = Duration::from_micros(50);
const BACKEND_TASK_ENQUEUE_RETRY_MAX: Duration = Duration::from_millis(1);
const BACKEND_ASSIGN_BATCH_TIMEOUT_SCALE_MAX: u32 = 16;

#[derive(Clone)]
pub(super) struct BackendExecutor {
    workers: Arc<Mutex<BTreeMap<BackendWorkerKey, BackendWorker>>>,
    quarantined: Arc<Mutex<BTreeSet<BackendWorkerKey>>>,
}

impl BackendExecutor {
    pub(super) fn new() -> Self {
        Self {
            workers: Arc::new(Mutex::new(BTreeMap::new())),
            quarantined: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    fn metrics_for_backend(
        &self,
        backend_id: BackendInstanceId,
        backend_handle: &Arc<dyn PowBackend>,
    ) -> Option<Arc<BackendWorkerMetrics>> {
        let key = backend_worker_key(backend_id, backend_handle);
        self.workers
            .lock()
            .ok()
            .and_then(|workers| workers.get(&key).map(|worker| Arc::clone(&worker.metrics)))
    }

    #[cfg(test)]
    pub(super) fn take_backend_telemetry(
        &self,
        backend_id: BackendInstanceId,
        backend_handle: &Arc<dyn PowBackend>,
    ) -> BackendTelemetry {
        self.metrics_for_backend(backend_id, backend_handle)
            .map(|metrics| metrics.take_delta())
            .unwrap_or_default()
    }

    pub(super) fn take_backend_telemetry_ordered<'a, I>(&self, backends: I) -> Vec<BackendTelemetry>
    where
        I: IntoIterator<Item = &'a BackendSlot>,
    {
        let backends = backends.into_iter();
        let (lower_bound, _) = backends.size_hint();
        let worker_registry = self.workers.lock().ok();
        let mut telemetry = Vec::with_capacity(lower_bound);

        for slot in backends {
            let key = backend_worker_key(slot.id, &slot.backend);
            let entry = worker_registry
                .as_ref()
                .and_then(|workers| workers.get(&key).map(|worker| Arc::clone(&worker.metrics)))
                .map(|metrics| metrics.take_delta())
                .unwrap_or_default();
            telemetry.push(entry);
        }

        telemetry
    }
}

fn build_backend_telemetry(
    counters: BackendTimeoutCounters,
    latencies: BackendTaskLatencyCounters,
    assignment_timeout_strikes: u32,
) -> BackendTelemetry {
    if counters.is_zero() && latencies.is_zero() && assignment_timeout_strikes == 0 {
        return BackendTelemetry::default();
    }

    BackendTelemetry {
        assignment_enqueue_timeouts: counters.assignment_enqueue,
        assignment_execution_timeouts: counters.assignment_execution,
        control_enqueue_timeouts: counters.control_enqueue,
        control_execution_timeouts: counters.control_execution,
        assignment_timeout_strikes,
        assignment_enqueue_latency_samples: latencies.assignment_enqueue.sample_count,
        assignment_enqueue_latency_p95_micros: latencies.assignment_enqueue.p95_micros(),
        assignment_enqueue_latency_max_micros: latencies.assignment_enqueue.max_micros,
        assignment_execution_latency_samples: latencies.assignment_execution.sample_count,
        assignment_execution_latency_p95_micros: latencies.assignment_execution.p95_micros(),
        assignment_execution_latency_max_micros: latencies.assignment_execution.max_micros,
        control_enqueue_latency_samples: latencies.control_enqueue.sample_count,
        control_enqueue_latency_p95_micros: latencies.control_enqueue.p95_micros(),
        control_enqueue_latency_max_micros: latencies.control_enqueue.max_micros,
        control_execution_latency_samples: latencies.control_execution.sample_count,
        control_execution_latency_p95_micros: latencies.control_execution.p95_micros(),
        control_execution_latency_max_micros: latencies.control_execution.max_micros,
        ..BackendTelemetry::default()
    }
}

fn backend_worker_key(
    backend_id: BackendInstanceId,
    backend_handle: &Arc<dyn PowBackend>,
) -> BackendWorkerKey {
    BackendWorkerKey {
        backend_id,
        backend_ptr: Arc::as_ptr(backend_handle) as *const () as usize,
    }
}

fn normalized_backend_capabilities(backend_handle: &Arc<dyn PowBackend>) -> BackendCapabilities {
    normalize_backend_capabilities(
        backend_handle.capabilities(),
        backend_handle.supports_assignment_batching(),
    )
}

pub(super) fn effective_backend_worker_queue_capacity(capabilities: BackendCapabilities) -> usize {
    let default_depth = if capabilities.assignment_semantics == AssignmentSemantics::Append {
        capabilities.max_inflight_assignments.max(1)
    } else {
        1
    };
    let requested_depth = capabilities
        .preferred_worker_queue_depth
        .unwrap_or(default_depth)
        .max(1) as usize;
    requested_depth.clamp(1, BACKEND_WORKER_QUEUE_CAPACITY_MAX)
}

fn backend_worker_queue_capacity(backend_handle: &Arc<dyn PowBackend>) -> usize {
    let capabilities = normalized_backend_capabilities(backend_handle);
    effective_backend_worker_queue_capacity(capabilities)
}

fn run_backend_worker_command(command: BackendWorkerCommand) -> bool {
    match command {
        BackendWorkerCommand::Run {
            task,
            deadline,
            outcome_tx,
            pending_control,
        } => {
            run_backend_task(task, deadline, outcome_tx, pending_control);
            false
        }
        BackendWorkerCommand::Stop {
            backend_id,
            backend,
            backend_handle,
            done_tx,
        } => {
            if panic::catch_unwind(AssertUnwindSafe(|| backend_handle.stop())).is_err() {
                warn(
                    "BACKEND",
                    format!("backend stop panicked for {backend}#{backend_id}"),
                );
            }
            let _ = done_tx.send(());
            true
        }
    }
}

fn spawn_backend_worker(
    backend_id: BackendInstanceId,
    backend: &'static str,
    backend_ptr: usize,
    queue_capacity: usize,
) -> Option<BackendWorker> {
    let (assignment_tx, assignment_rx) = bounded::<BackendWorkerCommand>(queue_capacity.max(1));
    let (control_tx, control_rx) =
        bounded::<BackendWorkerCommand>(BACKEND_WORKER_CONTROL_QUEUE_CAPACITY.max(1));
    let pending_control = Arc::new(AtomicUsize::new(0));
    let metrics = Arc::new(BackendWorkerMetrics::default());
    let thread_name = format!("seine-backend-{backend}-{backend_id}-{backend_ptr:x}");
    let spawn_result = thread::Builder::new().name(thread_name).spawn(move || {
        let mut assignment_open = true;
        let mut control_open = true;

        loop {
            if control_open {
                match control_rx.try_recv() {
                    Ok(command) => {
                        if run_backend_worker_command(command) {
                            break;
                        }
                        continue;
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {
                        control_open = false;
                    }
                }
            }

            if !assignment_open && !control_open {
                break;
            }

            if control_open && !assignment_open {
                match control_rx.recv() {
                    Ok(command) => {
                        if run_backend_worker_command(command) {
                            break;
                        }
                    }
                    Err(_) => break,
                }
                continue;
            }

            if assignment_open && !control_open {
                match assignment_rx.recv() {
                    Ok(command) => {
                        if run_backend_worker_command(command) {
                            break;
                        }
                    }
                    Err(_) => break,
                }
                continue;
            }

            crossbeam_channel::select! {
                recv(control_rx) -> command => match command {
                    Ok(command) => {
                        if run_backend_worker_command(command) {
                            break;
                        }
                    }
                    Err(_) => {
                        control_open = false;
                    }
                },
                recv(assignment_rx) -> command => match command {
                    Ok(command) => {
                        if run_backend_worker_command(command) {
                            break;
                        }
                    }
                    Err(_) => {
                        assignment_open = false;
                    }
                },
            }
        }
    });

    if spawn_result.is_err() {
        return None;
    }
    Some(BackendWorker {
        assignment_tx,
        control_tx,
        pending_control,
        metrics,
    })
}

impl BackendExecutor {
    fn worker_senders_for_backend(
        &self,
        backend_id: BackendInstanceId,
        backend: &'static str,
        backend_handle: &Arc<dyn PowBackend>,
    ) -> Option<BackendWorkerHandles> {
        let key = backend_worker_key(backend_id, backend_handle);
        let mut registry = self.workers.lock().ok()?;
        if let Some(worker) = registry.get(&key) {
            return Some(BackendWorkerHandles {
                assignment_tx: worker.assignment_tx.clone(),
                control_tx: worker.control_tx.clone(),
                pending_control: Arc::clone(&worker.pending_control),
                metrics: Arc::clone(&worker.metrics),
            });
        }
        let queue_capacity = backend_worker_queue_capacity(backend_handle);
        let worker = spawn_backend_worker(backend_id, backend, key.backend_ptr, queue_capacity)?;
        let handles = BackendWorkerHandles {
            assignment_tx: worker.assignment_tx.clone(),
            control_tx: worker.control_tx.clone(),
            pending_control: Arc::clone(&worker.pending_control),
            metrics: Arc::clone(&worker.metrics),
        };
        registry.insert(key, worker);
        Some(handles)
    }

    #[cfg(test)]
    fn worker_sender_for_backend(
        &self,
        backend_id: BackendInstanceId,
        backend: &'static str,
        backend_handle: &Arc<dyn PowBackend>,
    ) -> Option<BackendWorkerHandles> {
        self.worker_senders_for_backend(backend_id, backend, backend_handle)
    }

    pub(super) fn clear(&self) {
        if let Ok(mut registry) = self.workers.lock() {
            registry.clear();
        }
        if let Ok(mut inflight) = self.quarantined.lock() {
            inflight.clear();
        }
    }

    pub(super) fn wait_for_quarantine_drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now()
            .checked_add(timeout.max(Duration::from_millis(1)))
            .unwrap_or_else(Instant::now);

        loop {
            let pending = self
                .quarantined
                .lock()
                .map(|inflight| !inflight.is_empty())
                .unwrap_or(false);
            if !pending {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub(super) fn prune(&self, backends: &[BackendSlot]) {
        let active_keys = backends
            .iter()
            .map(|slot| backend_worker_key(slot.id, &slot.backend))
            .collect::<BTreeSet<_>>();
        if let Ok(mut registry) = self.workers.lock() {
            registry.retain(|backend_key, _| active_keys.contains(backend_key));
        }
        if let Ok(mut inflight) = self.quarantined.lock() {
            inflight.retain(|backend_key| active_keys.contains(backend_key));
        }
    }

    pub(super) fn remove_backend_worker(
        &self,
        backend_id: BackendInstanceId,
        backend_handle: &Arc<dyn PowBackend>,
    ) {
        let key = backend_worker_key(backend_id, backend_handle);
        if let Ok(mut registry) = self.workers.lock() {
            registry.remove(&key);
        }
    }

    pub(super) fn quarantine_backend(
        &self,
        backend_id: BackendInstanceId,
        backend: Arc<dyn PowBackend>,
    ) {
        let key = backend_worker_key(backend_id, &backend);
        let should_quarantine = match self.quarantined.lock() {
            Ok(mut inflight) => inflight.insert(key),
            Err(_) => {
                warn(
                    "BACKEND",
                    "quarantine registry lock poisoned; running synchronous stop fallback",
                );
                if panic::catch_unwind(AssertUnwindSafe(|| backend.stop())).is_err() {
                    warn(
                        "BACKEND",
                        "backend stop panicked during synchronous quarantine fallback",
                    );
                }
                false
            }
        };
        if !should_quarantine {
            return;
        }

        let worker_tx = self
            .workers
            .lock()
            .ok()
            .and_then(|mut registry| registry.remove(&key))
            .map(|worker| worker.control_tx);
        let quarantined = Arc::clone(&self.quarantined);
        let detached = Arc::clone(&backend);
        let backend_name = backend.name();
        let worker_tx_for_thread = worker_tx.clone();
        if thread::Builder::new()
            .name("seine-backend-quarantine".to_string())
            .spawn(move || {
                perform_quarantine_stop(backend_id, backend_name, detached, worker_tx_for_thread);
                if let Ok(mut inflight) = quarantined.lock() {
                    inflight.remove(&key);
                }
            })
            .is_err()
        {
            warn(
                "BACKEND",
                "failed to spawn backend quarantine worker; running synchronous stop fallback",
            );
            perform_quarantine_stop(backend_id, backend_name, backend, worker_tx);
            if let Ok(mut inflight) = self.quarantined.lock() {
                inflight.remove(&key);
            }
        }
    }

    pub(super) fn note_assignment_success(
        &self,
        backend_id: BackendInstanceId,
        backend_handle: &Arc<dyn PowBackend>,
    ) {
        if let Some(metrics) = self.metrics_for_backend(backend_id, backend_handle) {
            metrics.reset_assignment_timeout_strikes();
        }
    }

    pub(super) fn note_assignment_timeout_with_threshold(
        &self,
        backend_id: BackendInstanceId,
        backend_handle: &Arc<dyn PowBackend>,
        threshold: u32,
    ) -> AssignmentTimeoutDecision {
        if let Some(metrics) = self.metrics_for_backend(backend_id, backend_handle) {
            return metrics.note_assignment_timeout_with_threshold(threshold);
        }
        let threshold = threshold.max(1);
        AssignmentTimeoutDecision {
            strikes: threshold,
            threshold,
            should_quarantine: true,
        }
    }

    pub(super) fn dispatch_backend_tasks(
        &self,
        tasks: Vec<BackendTask>,
    ) -> Vec<Option<BackendTaskDispatchResult>> {
        if tasks.is_empty() {
            return Vec::new();
        }

        let outcomes_len = tasks
            .iter()
            .map(|task| task.idx)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let mut outcomes: Vec<Option<BackendTaskDispatchResult>> =
            std::iter::repeat_with(|| None).take(outcomes_len).collect();
        let mut task_contexts: Vec<Option<TimeoutTaskContext>> =
            std::iter::repeat_with(|| None).take(outcomes_len).collect();
        let (outcome_tx, outcome_rx) = bounded::<BackendTaskOutcome>(tasks.len().max(1));
        let mut pending = vec![false; outcomes_len];
        let mut pending_count = 0usize;

        for task in tasks {
            let backend_id = task.backend_id;
            let backend = task.backend;
            let action = task.kind.action_label();
            let enqueue_timeout_bucket = task
                .kind
                .timeout_counter_bucket(BackendTaskTimeoutKind::Enqueue);
            let execution_timeout_bucket = task
                .kind
                .timeout_counter_bucket(BackendTaskTimeoutKind::Execution);
            let uses_control_lane = task.kind.is_control();
            let backend_handle = Arc::clone(&task.backend_handle);
            let idx = task.idx;
            let dispatch_started_at = Instant::now();
            let timeout = effective_backend_task_timeout(&task.kind, task.timeout);
            let task_deadline = Instant::now()
                .checked_add(timeout)
                .unwrap_or_else(Instant::now);
            let worker_handles =
                self.worker_senders_for_backend(backend_id, backend, &backend_handle);
            let Some(worker_handles) = worker_handles else {
                outcomes[idx] = Some(BackendTaskDispatchResult::Completed(Err(anyhow!(
                    "{action} dispatch failed: could not spawn worker for {backend}#{backend_id}"
                ))));
                continue;
            };
            task_contexts[idx] = Some(TimeoutTaskContext {
                backend_id,
                backend,
                action,
                backend_handle: Arc::clone(&backend_handle),
                metrics: Arc::clone(&worker_handles.metrics),
                enqueue_timeout_bucket,
                execution_timeout_bucket,
                dispatch_started_at,
                enqueued_at: None,
                deadline: task_deadline,
            });
            let worker_tx = if uses_control_lane {
                worker_handles.pending_control.fetch_add(1, Ordering::AcqRel);
                if let Err(err) = backend_handle.request_timeout_interrupt() {
                    warn(
                        "BACKEND",
                        format!(
                            "{action} preemption interrupt failed for {backend}#{backend_id}: {err:#}"
                        ),
                    );
                }
                worker_handles.control_tx.clone()
            } else {
                worker_handles.assignment_tx.clone()
            };

            let command = BackendWorkerCommand::Run {
                task,
                deadline: task_deadline,
                outcome_tx: outcome_tx.clone(),
                pending_control: Arc::clone(&worker_handles.pending_control),
            };
            match enqueue_backend_command_until_deadline(&worker_tx, command, task_deadline) {
                EnqueueCommandStatus::Enqueued => {
                    let now = Instant::now();
                    if let Some(context) = task_contexts.get_mut(idx).and_then(Option::as_mut) {
                        context.enqueued_at = Some(now);
                        context.metrics.record_task_latency(
                            context.enqueue_timeout_bucket,
                            now.saturating_duration_since(context.dispatch_started_at),
                        );
                    }
                    if !pending[idx] {
                        pending[idx] = true;
                        pending_count = pending_count.saturating_add(1);
                    }
                }
                EnqueueCommandStatus::Disconnected => {
                    if uses_control_lane {
                        release_pending_control_counter(&worker_handles.pending_control);
                    }
                    if let Some(context) = task_contexts.get(idx).and_then(Option::as_ref) {
                        context.metrics.record_task_latency(
                            context.enqueue_timeout_bucket,
                            Instant::now().saturating_duration_since(context.dispatch_started_at),
                        );
                    }
                    self.remove_backend_worker(backend_id, &backend_handle);
                    outcomes[idx] = Some(BackendTaskDispatchResult::Completed(Err(anyhow!(
                        "{action} dispatch failed: worker channel closed for {backend}#{backend_id}"
                    ))));
                    self.quarantine_backend(backend_id, backend_handle);
                }
                EnqueueCommandStatus::DeadlineElapsed => {
                    if uses_control_lane {
                        release_pending_control_counter(&worker_handles.pending_control);
                    }
                    if let Some(context) = task_contexts.get(idx).and_then(Option::as_ref) {
                        context.metrics.record_task_latency(
                            context.enqueue_timeout_bucket,
                            context
                                .deadline
                                .saturating_duration_since(context.dispatch_started_at),
                        );
                    }
                    outcomes[idx] = Some(BackendTaskDispatchResult::TimedOut(
                        BackendTaskTimeoutKind::Enqueue,
                    ));
                }
            }
        }
        drop(outcome_tx);

        while pending_count > 0 {
            let now = Instant::now();

            let mut next_deadline: Option<Instant> = None;
            for idx in 0..pending.len() {
                if !pending[idx] {
                    continue;
                }
                let Some(context) = task_contexts.get(idx).and_then(Option::as_ref) else {
                    pending[idx] = false;
                    pending_count = pending_count.saturating_sub(1);
                    continue;
                };
                if now >= context.deadline {
                    pending[idx] = false;
                    pending_count = pending_count.saturating_sub(1);
                    if outcomes[idx].is_none() {
                        outcomes[idx] = Some(BackendTaskDispatchResult::TimedOut(
                            BackendTaskTimeoutKind::Execution,
                        ));
                    }
                } else {
                    next_deadline = Some(
                        next_deadline
                            .map_or(context.deadline, |current| current.min(context.deadline)),
                    );
                }
            }
            let Some(next_deadline) = next_deadline else {
                break;
            };

            let wait_for = next_deadline
                .saturating_duration_since(now)
                .max(Duration::from_millis(1));
            match outcome_rx.recv_timeout(wait_for) {
                Ok(outcome) => resolve_dispatched_task_outcome(
                    outcome,
                    &mut pending,
                    &mut pending_count,
                    &mut outcomes,
                    &task_contexts,
                ),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }

            while let Ok(outcome) = outcome_rx.try_recv() {
                resolve_dispatched_task_outcome(
                    outcome,
                    &mut pending,
                    &mut pending_count,
                    &mut outcomes,
                    &task_contexts,
                );
            }
        }

        while let Ok(outcome) = outcome_rx.try_recv() {
            resolve_dispatched_task_outcome(
                outcome,
                &mut pending,
                &mut pending_count,
                &mut outcomes,
                &task_contexts,
            );
        }

        for (idx, context) in task_contexts.iter().enumerate() {
            let Some(context) = context.as_ref() else {
                continue;
            };
            let Some(timeout_kind) = (match outcomes[idx] {
                Some(BackendTaskDispatchResult::TimedOut(kind)) => Some(kind),
                _ => None,
            }) else {
                continue;
            };
            let (timeout_label, timeout_bucket) = match timeout_kind {
                BackendTaskTimeoutKind::Enqueue => ("enqueue", context.enqueue_timeout_bucket),
                BackendTaskTimeoutKind::Execution => {
                    ("execution", context.execution_timeout_bucket)
                }
            };
            let timeout_elapsed = match timeout_kind {
                BackendTaskTimeoutKind::Enqueue => context
                    .deadline
                    .saturating_duration_since(context.dispatch_started_at),
                BackendTaskTimeoutKind::Execution => context.deadline.saturating_duration_since(
                    context.enqueued_at.unwrap_or(context.dispatch_started_at),
                ),
            };
            context.metrics.record_task_latency(timeout_bucket, timeout_elapsed);
            context.metrics.record_task_timeout(timeout_bucket);
            if let Err(err) = context.backend_handle.request_timeout_interrupt() {
                warn(
                    "BACKEND",
                    format!(
                        "{} {} timeout interrupt failed for {}#{}: {err:#}",
                        context.action, timeout_label, context.backend, context.backend_id
                    ),
                );
            }
        }

        outcomes
    }
}

fn effective_backend_task_timeout(kind: &BackendTaskKind, base_timeout: Duration) -> Duration {
    let base_timeout = base_timeout.max(Duration::from_millis(1));
    match kind {
        BackendTaskKind::AssignBatch(batch) => {
            let parts = (batch.len() as u32).clamp(1, BACKEND_ASSIGN_BATCH_TIMEOUT_SCALE_MAX);
            let scaled = base_timeout.saturating_mul(parts);
            // Keep runaway queue hints bounded if a backend reports very deep inflight buffers.
            scaled.min(base_timeout.saturating_mul(BACKEND_ASSIGN_BATCH_TIMEOUT_SCALE_MAX))
        }
        _ => base_timeout,
    }
}

fn resolve_dispatched_task_outcome(
    outcome: BackendTaskOutcome,
    pending: &mut [bool],
    pending_count: &mut usize,
    outcomes: &mut [Option<BackendTaskDispatchResult>],
    task_contexts: &[Option<TimeoutTaskContext>],
) {
    let outcome_idx = outcome.idx;
    if outcome_idx >= pending.len() || !pending[outcome_idx] || outcomes[outcome_idx].is_some() {
        return;
    }

    if let Some(context) = task_contexts.get(outcome_idx).and_then(Option::as_ref) {
        let started = context.enqueued_at.unwrap_or(context.dispatch_started_at);
        context.metrics.record_task_latency(
            context.execution_timeout_bucket,
            Instant::now().saturating_duration_since(started),
        );
    }

    pending[outcome_idx] = false;
    *pending_count = (*pending_count).saturating_sub(1);
    outcomes[outcome_idx] = Some(BackendTaskDispatchResult::Completed(outcome.result));
}

enum EnqueueCommandStatus {
    Enqueued,
    DeadlineElapsed,
    Disconnected,
}

fn enqueue_backend_command_until_deadline(
    worker_tx: &Sender<BackendWorkerCommand>,
    mut command: BackendWorkerCommand,
    deadline: Instant,
) -> EnqueueCommandStatus {
    let mut retry_wait = BACKEND_TASK_ENQUEUE_RETRY_MIN;
    loop {
        if Instant::now() >= deadline {
            return EnqueueCommandStatus::DeadlineElapsed;
        }

        match worker_tx.try_send(command) {
            Ok(()) => return EnqueueCommandStatus::Enqueued,
            Err(TrySendError::Disconnected(_)) => return EnqueueCommandStatus::Disconnected,
            Err(TrySendError::Full(returned)) => {
                command = returned;
            }
        }

        let now = Instant::now();
        if now >= deadline {
            return EnqueueCommandStatus::DeadlineElapsed;
        }

        let wait_for = deadline
            .saturating_duration_since(now)
            .min(retry_wait)
            .max(Duration::from_micros(10));
        match worker_tx.send_timeout(command, wait_for) {
            Ok(()) => return EnqueueCommandStatus::Enqueued,
            Err(SendTimeoutError::Disconnected(_)) => return EnqueueCommandStatus::Disconnected,
            Err(SendTimeoutError::Timeout(returned)) => {
                command = returned;
                retry_wait = retry_wait
                    .saturating_mul(2)
                    .min(BACKEND_TASK_ENQUEUE_RETRY_MAX);
            }
        }
    }
}

fn release_pending_control_counter(counter: &AtomicUsize) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        current.checked_sub(1)
    });
}

fn run_backend_task(
    task: BackendTask,
    deadline: Instant,
    outcome_tx: Sender<BackendTaskOutcome>,
    pending_control: Arc<AtomicUsize>,
) {
    let BackendTask {
        idx,
        backend_id,
        backend,
        backend_handle,
        kind,
        ..
    } = task;
    let is_control = kind.is_control();
    let action_label = kind.action_label();
    let result = run_backend_call(
        Arc::clone(&backend_handle),
        kind,
        deadline,
        pending_control.as_ref(),
    )
    .with_context(|| format!("{action_label} failed for {backend}#{backend_id}"));
    if is_control {
        release_pending_control_counter(pending_control.as_ref());
    }
    let send_result = outcome_tx.send(BackendTaskOutcome { idx, result });
    if send_result.is_err() {
        let _ = backend_handle.request_timeout_interrupt();
    }
}

fn perform_quarantine_stop(
    backend_id: BackendInstanceId,
    backend: &'static str,
    backend_handle: Arc<dyn PowBackend>,
    worker_tx: Option<Sender<BackendWorkerCommand>>,
) {
    let mut should_run_fallback_stop = worker_tx.is_none();
    if let Some(worker_tx) = worker_tx {
        let enqueue_stop_command = |wait: Duration| {
            let (done_tx, done_rx) = bounded::<()>(1);
            let stop_command = BackendWorkerCommand::Stop {
                backend_id,
                backend,
                backend_handle: Arc::clone(&backend_handle),
                done_tx,
            };
            worker_tx.send_timeout(stop_command, wait).map(|_| done_rx)
        };

        match enqueue_stop_command(BACKEND_STOP_ENQUEUE_TIMEOUT) {
            Ok(done_rx) => {
                should_run_fallback_stop = false;
                wait_for_stop_ack(backend_id, backend, &backend_handle, done_rx);
            }
            Err(SendTimeoutError::Timeout(_)) => {
                should_run_fallback_stop = false;
                warn(
                    "BACKEND",
                    format!(
                        "backend stop command enqueue timed out for {backend}#{backend_id}; requesting interrupt and retrying with grace deadline"
                    ),
                );
                request_backend_stop_interrupt(backend_id, backend, &backend_handle);

                let (done_tx, done_rx) = bounded::<()>(1);
                let stop_command = BackendWorkerCommand::Stop {
                    backend_id,
                    backend,
                    backend_handle: Arc::clone(&backend_handle),
                    done_tx,
                };
                let retry_deadline = Instant::now()
                    .checked_add(BACKEND_STOP_ACK_GRACE_TIMEOUT)
                    .unwrap_or_else(Instant::now);
                match enqueue_backend_command_until_deadline(
                    &worker_tx,
                    stop_command,
                    retry_deadline,
                ) {
                    EnqueueCommandStatus::Enqueued => {
                        wait_for_stop_ack(backend_id, backend, &backend_handle, done_rx);
                    }
                    EnqueueCommandStatus::DeadlineElapsed => {
                        warn(
                            "BACKEND",
                            format!(
                                "backend stop command remained saturated for {backend}#{backend_id} through grace deadline; running fallback stop"
                            ),
                        );
                        should_run_fallback_stop = true;
                    }
                    EnqueueCommandStatus::Disconnected => should_run_fallback_stop = true,
                }
            }
            Err(SendTimeoutError::Disconnected(_)) => {
                should_run_fallback_stop = true;
            }
        }
    }

    if !should_run_fallback_stop {
        return;
    }

    run_synchronous_stop_fallback(backend_id, backend, backend_handle);
}

fn wait_for_stop_ack(
    backend_id: BackendInstanceId,
    backend: &'static str,
    backend_handle: &Arc<dyn PowBackend>,
    done_rx: crossbeam_channel::Receiver<()>,
) {
    match done_rx.recv_timeout(BACKEND_STOP_ACK_TIMEOUT) {
        Ok(()) | Err(RecvTimeoutError::Disconnected) => {}
        Err(RecvTimeoutError::Timeout) => {
            warn(
                "BACKEND",
                format!(
                    "backend stop is still in progress for {backend}#{backend_id}; requesting interrupt before detach"
                ),
            );
            request_backend_stop_interrupt(backend_id, backend, backend_handle);
            if matches!(
                done_rx.recv_timeout(BACKEND_STOP_ACK_GRACE_TIMEOUT),
                Err(RecvTimeoutError::Timeout)
            ) {
                warn(
                    "BACKEND",
                    format!(
                        "backend stop did not acknowledge for {backend}#{backend_id}; detached"
                    ),
                );
            }
        }
    }
}

fn request_backend_stop_interrupt(
    backend_id: BackendInstanceId,
    backend: &'static str,
    backend_handle: &Arc<dyn PowBackend>,
) {
    if let Err(err) = backend_handle.request_timeout_interrupt() {
        warn(
            "BACKEND",
            format!("backend stop interrupt failed for {backend}#{backend_id}: {err:#}"),
        );
    }
}

fn run_synchronous_stop_fallback(
    backend_id: BackendInstanceId,
    backend: &'static str,
    backend_handle: Arc<dyn PowBackend>,
) {
    let thread_name = format!("seine-backend-stop-fallback-{backend}-{backend_id}");
    let (done_tx, done_rx) = bounded::<()>(1);
    let backend_for_stop = Arc::clone(&backend_handle);

    let stop_thread = thread::Builder::new().name(thread_name).spawn(move || {
        if panic::catch_unwind(AssertUnwindSafe(|| backend_for_stop.stop())).is_err() {
            warn(
                "BACKEND",
                format!(
                    "backend stop panicked during synchronous quarantine fallback for {backend}#{backend_id}"
                ),
            );
        }
        let _ = done_tx.send(());
    });

    match stop_thread {
        Ok(handle) => match done_rx.recv_timeout(BACKEND_STOP_FALLBACK_TIMEOUT) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                if handle.join().is_err() {
                    warn(
                        "BACKEND",
                        format!(
                            "backend stop fallback helper thread panicked for {backend}#{backend_id}"
                        ),
                    );
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                warn(
                    "BACKEND",
                    format!(
                        "backend stop fallback timed out for {backend}#{backend_id}; detached (process restart may be required if backend is hard-hung)"
                    ),
                );
                drop(handle);
            }
        },
        Err(err) => {
            warn(
                "BACKEND",
                format!(
                    "failed to spawn backend stop fallback thread for {backend}#{backend_id}: {err}; attempting direct stop"
                ),
            );
            if panic::catch_unwind(AssertUnwindSafe(|| backend_handle.stop())).is_err() {
                warn(
                    "BACKEND",
                    format!(
                        "backend stop panicked during direct fallback for {backend}#{backend_id}"
                    ),
                );
            }
        }
    }
}

const DEFAULT_NONBLOCKING_BACKOFF_MIN: Duration = Duration::from_micros(50);
const DEFAULT_NONBLOCKING_BACKOFF_MAX: Duration = Duration::from_millis(1);
const NONBLOCKING_BACKOFF_HARD_MAX: Duration = Duration::from_millis(50);

fn backend_nonblocking_backoff_bounds(
    backend_handle: &Arc<dyn PowBackend>,
) -> (Duration, Duration) {
    let capabilities = normalized_backend_capabilities(backend_handle);
    let min_backoff = capabilities
        .nonblocking_poll_min
        .unwrap_or(DEFAULT_NONBLOCKING_BACKOFF_MIN)
        .max(Duration::from_micros(10));
    let max_backoff = capabilities
        .nonblocking_poll_max
        .unwrap_or(DEFAULT_NONBLOCKING_BACKOFF_MAX)
        .max(min_backoff)
        .min(NONBLOCKING_BACKOFF_HARD_MAX);
    (min_backoff, max_backoff)
}

fn run_backend_call(
    backend_handle: Arc<dyn PowBackend>,
    kind: BackendTaskKind,
    deadline: Instant,
    pending_control: &AtomicUsize,
) -> Result<()> {
    if Instant::now() >= deadline {
        return Err(anyhow!("task deadline elapsed before backend call"));
    }
    let assignment_preempt_requested = || pending_control.load(Ordering::Acquire) > 0;
    if matches!(kind, BackendTaskKind::Assign(_) | BackendTaskKind::AssignBatch(_))
        && assignment_preempt_requested()
    {
        return Err(anyhow!(AssignmentPreemptedByControl));
    }
    let capabilities = normalized_backend_capabilities(&backend_handle);
    let execution_model = capabilities.execution_model;
    match panic::catch_unwind(AssertUnwindSafe(|| match execution_model {
        BackendExecutionModel::Blocking => {
            run_blocking_call_with_deadline(Arc::clone(&backend_handle), kind, deadline)
        }
        BackendExecutionModel::Nonblocking => {
            let (min_backoff, max_backoff) = backend_nonblocking_backoff_bounds(&backend_handle);
            match kind {
                BackendTaskKind::Assign(work) => run_nonblocking_until_deadline(
                    deadline,
                    || backend_handle.assign_work_batch_nonblocking(std::slice::from_ref(&work)),
                    |wait| backend_handle.wait_for_nonblocking_progress(wait),
                    assignment_preempt_requested,
                    min_backoff,
                    max_backoff,
                    "assignment deadline elapsed before backend accepted work",
                ),
                BackendTaskKind::AssignBatch(batch) => run_nonblocking_until_deadline(
                    deadline,
                    || backend_handle.assign_work_batch_nonblocking(&batch),
                    |wait| backend_handle.wait_for_nonblocking_progress(wait),
                    assignment_preempt_requested,
                    min_backoff,
                    max_backoff,
                    "assignment deadline elapsed before backend accepted work",
                ),
                BackendTaskKind::Cancel => run_nonblocking_until_deadline(
                    deadline,
                    || backend_handle.cancel_work_nonblocking(),
                    |wait| backend_handle.wait_for_nonblocking_progress(wait),
                    || false,
                    min_backoff,
                    max_backoff,
                    "cancel deadline elapsed before backend acknowledged cancel",
                ),
                BackendTaskKind::Fence => run_nonblocking_until_deadline(
                    deadline,
                    || backend_handle.fence_nonblocking(),
                    |wait| backend_handle.wait_for_nonblocking_progress(wait),
                    || false,
                    min_backoff,
                    max_backoff,
                    "fence deadline elapsed before backend acknowledged fence",
                ),
            }
        }
    })) {
        Ok(result) => result,
        Err(_) => Err(anyhow!("backend task panicked")),
    }
}

fn run_blocking_call_with_deadline(
    backend_handle: Arc<dyn PowBackend>,
    kind: BackendTaskKind,
    deadline: Instant,
) -> Result<()> {
    match kind {
        BackendTaskKind::Assign(work) => {
            backend_handle.assign_work_batch_with_deadline(std::slice::from_ref(&work), deadline)
        }
        BackendTaskKind::AssignBatch(batch) => {
            backend_handle.assign_work_batch_with_deadline(&batch, deadline)
        }
        BackendTaskKind::Cancel => backend_handle.cancel_work_with_deadline(deadline),
        BackendTaskKind::Fence => backend_handle.fence_with_deadline(deadline),
    }
}

fn run_nonblocking_until_deadline<F, W, P>(
    deadline: Instant,
    mut op: F,
    mut wait_for_progress: W,
    mut should_preempt: P,
    min_backoff: Duration,
    max_backoff: Duration,
    timeout_message: &'static str,
) -> Result<()>
where
    F: FnMut() -> Result<BackendCallStatus>,
    W: FnMut(Duration) -> Result<()>,
    P: FnMut() -> bool,
{
    let mut backoff = min_backoff.max(Duration::from_micros(10));
    loop {
        if should_preempt() {
            return Err(anyhow!(AssignmentPreemptedByControl));
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(timeout_message));
        }

        match op()? {
            BackendCallStatus::Complete => {
                if Instant::now() > deadline {
                    return Err(anyhow!(timeout_message));
                }
                return Ok(());
            }
            BackendCallStatus::Pending => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(anyhow!(timeout_message));
                }
                let wait = deadline
                    .saturating_duration_since(now)
                    .min(backoff)
                    .max(Duration::from_micros(10));
                wait_for_progress(wait)?;
                if should_preempt() {
                    return Err(anyhow!(AssignmentPreemptedByControl));
                }
                backoff = backoff.saturating_mul(2).min(max_backoff);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::Sender;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::backend::{BackendCallStatus, BackendEvent, NonceChunk, PowBackend, WorkTemplate};

    struct NoopBackend;

    impl PowBackend for NoopBackend {
        fn name(&self) -> &'static str {
            "noop"
        }

        fn lanes(&self) -> usize {
            1
        }

        fn set_instance_id(&self, _id: BackendInstanceId) {}

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            Ok(())
        }

        fn stop(&self) {}

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            Ok(())
        }

        fn cancel_work(&self) -> Result<()> {
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            Ok(())
        }
    }

    struct CapabilityBackend {
        max_inflight_assignments: u32,
        assignment_semantics: AssignmentSemantics,
        supports_batching: bool,
        preferred_worker_queue_depth: Option<u32>,
        nonblocking_poll_min: Option<Duration>,
        nonblocking_poll_max: Option<Duration>,
    }

    impl CapabilityBackend {
        fn new(max_inflight_assignments: u32) -> Self {
            Self {
                max_inflight_assignments,
                assignment_semantics: AssignmentSemantics::Replace,
                supports_batching: false,
                preferred_worker_queue_depth: None,
                nonblocking_poll_min: None,
                nonblocking_poll_max: None,
            }
        }

        fn with_assignment_semantics(mut self, assignment_semantics: AssignmentSemantics) -> Self {
            self.assignment_semantics = assignment_semantics;
            self
        }

        fn with_batching(mut self, supports_batching: bool) -> Self {
            self.supports_batching = supports_batching;
            self
        }

        fn with_worker_queue_depth_hint(mut self, preferred_worker_queue_depth: u32) -> Self {
            self.preferred_worker_queue_depth = Some(preferred_worker_queue_depth);
            self
        }

        fn with_nonblocking_poll(
            mut self,
            min_poll: Option<Duration>,
            max_poll: Option<Duration>,
        ) -> Self {
            self.nonblocking_poll_min = min_poll;
            self.nonblocking_poll_max = max_poll;
            self
        }
    }

    impl PowBackend for CapabilityBackend {
        fn name(&self) -> &'static str {
            "capability"
        }

        fn lanes(&self) -> usize {
            1
        }

        fn set_instance_id(&self, _id: BackendInstanceId) {}

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            Ok(())
        }

        fn stop(&self) {}

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            Ok(())
        }

        fn cancel_work(&self) -> Result<()> {
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            Ok(())
        }

        fn supports_assignment_batching(&self) -> bool {
            self.supports_batching
        }

        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities {
                max_inflight_assignments: self.max_inflight_assignments,
                assignment_semantics: self.assignment_semantics,
                preferred_worker_queue_depth: self.preferred_worker_queue_depth,
                nonblocking_poll_min: self.nonblocking_poll_min,
                nonblocking_poll_max: self.nonblocking_poll_max,
                ..BackendCapabilities::default()
            }
        }
    }

    #[test]
    fn worker_queue_capacity_is_normalized_for_non_batching_backends() {
        let backend = Arc::new(
            CapabilityBackend::new(16)
                .with_assignment_semantics(AssignmentSemantics::Append)
                .with_batching(false),
        ) as Arc<dyn PowBackend>;

        assert_eq!(backend_worker_queue_capacity(&backend), 1);
    }

    #[test]
    fn worker_queue_capacity_uses_append_inflight_depth_with_batching() {
        let backend = Arc::new(
            CapabilityBackend::new(16)
                .with_assignment_semantics(AssignmentSemantics::Append)
                .with_batching(true),
        ) as Arc<dyn PowBackend>;

        assert_eq!(backend_worker_queue_capacity(&backend), 16);
    }

    #[test]
    fn worker_queue_capacity_applies_global_cap() {
        let backend = Arc::new(
            CapabilityBackend::new(512)
                .with_assignment_semantics(AssignmentSemantics::Append)
                .with_batching(true),
        ) as Arc<dyn PowBackend>;

        assert_eq!(
            backend_worker_queue_capacity(&backend),
            BACKEND_WORKER_QUEUE_CAPACITY_MAX
        );
    }

    #[test]
    fn worker_queue_capacity_can_override_replace_semantics_default_depth() {
        let backend = Arc::new(
            CapabilityBackend::new(1)
                .with_assignment_semantics(AssignmentSemantics::Replace)
                .with_worker_queue_depth_hint(8),
        ) as Arc<dyn PowBackend>;

        assert_eq!(backend_worker_queue_capacity(&backend), 8);
    }

    #[test]
    fn nonblocking_backoff_bounds_normalize_inverted_hints() {
        let backend = Arc::new(CapabilityBackend::new(1).with_nonblocking_poll(
            Some(Duration::from_millis(7)),
            Some(Duration::from_micros(10)),
        )) as Arc<dyn PowBackend>;

        let (min_backoff, max_backoff) = backend_nonblocking_backoff_bounds(&backend);
        assert_eq!(min_backoff, Duration::from_millis(7));
        assert_eq!(max_backoff, Duration::from_millis(7));
    }

    #[test]
    fn effective_batch_timeout_scaling_is_capped() {
        let work = WorkAssignment {
            template: Arc::new(WorkTemplate {
                work_id: 1,
                epoch: 1,
                header_base: Arc::from(vec![0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                stop_at: Instant::now() + Duration::from_secs(1),
            }),
            nonce_chunk: NonceChunk {
                start_nonce: 0,
                nonce_count: 1,
            },
        };
        let batch = vec![work; 256];
        let timeout = effective_backend_task_timeout(
            &BackendTaskKind::AssignBatch(batch),
            Duration::from_millis(10),
        );
        assert_eq!(
            timeout,
            Duration::from_millis(10).saturating_mul(BACKEND_ASSIGN_BATCH_TIMEOUT_SCALE_MAX)
        );
    }

    struct StopCountingBackend {
        stops: Arc<AtomicUsize>,
    }

    impl StopCountingBackend {
        fn new(stops: Arc<AtomicUsize>) -> Self {
            Self { stops }
        }
    }

    impl PowBackend for StopCountingBackend {
        fn name(&self) -> &'static str {
            "stop-counter"
        }

        fn lanes(&self) -> usize {
            1
        }

        fn set_instance_id(&self, _id: BackendInstanceId) {}

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            Ok(())
        }

        fn stop(&self) {
            self.stops.fetch_add(1, Ordering::Relaxed);
            thread::sleep(Duration::from_millis(40));
        }

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            Ok(())
        }

        fn cancel_work(&self) -> Result<()> {
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn dispatch_handles_sparse_indices_without_waiting_for_missing_slots() {
        let executor = BackendExecutor::new();
        executor.clear();

        let backend = Arc::new(NoopBackend) as Arc<dyn PowBackend>;
        let tasks = vec![
            BackendTask {
                idx: 0,
                backend_id: 1,
                backend: "noop",
                backend_handle: Arc::clone(&backend),
                kind: BackendTaskKind::Cancel,
                timeout: Duration::from_millis(250),
            },
            BackendTask {
                idx: 5,
                backend_id: 2,
                backend: "noop",
                backend_handle: backend,
                kind: BackendTaskKind::Fence,
                timeout: Duration::from_millis(250),
            },
        ];

        let started = Instant::now();
        let outcomes = executor.dispatch_backend_tasks(tasks);
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "sparse indices should not block waiting for missing outcomes"
        );
        assert_eq!(outcomes.len(), 6);
        assert!(outcomes[0].as_ref().is_some_and(|outcome| matches!(
            outcome,
            BackendTaskDispatchResult::Completed(Ok(()))
        )));
        assert!(outcomes[5].as_ref().is_some_and(|outcome| matches!(
            outcome,
            BackendTaskDispatchResult::Completed(Ok(()))
        )));
        assert!(outcomes[1].is_none());
        assert!(outcomes[2].is_none());
        assert!(outcomes[3].is_none());
        assert!(outcomes[4].is_none());

        executor.clear();
    }

    #[test]
    fn quarantine_is_serialized_per_backend_instance() {
        let executor = BackendExecutor::new();
        let stops = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(StopCountingBackend::new(Arc::clone(&stops))) as Arc<dyn PowBackend>;

        executor.quarantine_backend(9, Arc::clone(&backend));
        executor.quarantine_backend(9, backend);

        thread::sleep(Duration::from_millis(140));
        assert_eq!(stops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn prune_drops_quarantine_entries_for_removed_backends() {
        let executor = BackendExecutor::new();
        let active_backend = Arc::new(NoopBackend) as Arc<dyn PowBackend>;
        let removed_backend = Arc::new(NoopBackend) as Arc<dyn PowBackend>;
        let active_key = backend_worker_key(1, &active_backend);
        let removed_key = backend_worker_key(2, &removed_backend);

        executor
            .quarantined
            .lock()
            .expect("quarantine lock should be available")
            .extend([active_key, removed_key]);

        let backends = vec![BackendSlot {
            id: 1,
            backend: Arc::clone(&active_backend),
            lanes: 1,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];
        executor.prune(&backends);

        let quarantined = executor
            .quarantined
            .lock()
            .expect("quarantine lock should be available");
        assert!(quarantined.contains(&active_key));
        assert!(!quarantined.contains(&removed_key));
    }

    #[test]
    fn clear_resets_quarantine_registry() {
        let executor = BackendExecutor::new();
        let backend = Arc::new(NoopBackend) as Arc<dyn PowBackend>;
        let key = backend_worker_key(7, &backend);

        executor
            .quarantined
            .lock()
            .expect("quarantine lock should be available")
            .insert(key);

        executor.clear();

        assert!(executor
            .quarantined
            .lock()
            .expect("quarantine lock should be available")
            .is_empty());
    }

    struct StopInterruptBackend {
        stops: Arc<AtomicUsize>,
        interrupts: Arc<AtomicUsize>,
    }

    impl StopInterruptBackend {
        fn new(stops: Arc<AtomicUsize>, interrupts: Arc<AtomicUsize>) -> Self {
            Self { stops, interrupts }
        }
    }

    impl PowBackend for StopInterruptBackend {
        fn name(&self) -> &'static str {
            "stop-interrupt"
        }

        fn lanes(&self) -> usize {
            1
        }

        fn set_instance_id(&self, _id: BackendInstanceId) {}

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            Ok(())
        }

        fn stop(&self) {
            self.stops.fetch_add(1, Ordering::Relaxed);
            thread::sleep(Duration::from_millis(40));
        }

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            Ok(())
        }

        fn cancel_work(&self) -> Result<()> {
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            Ok(())
        }

        fn request_timeout_interrupt(&self) -> Result<()> {
            self.interrupts.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn quarantine_falls_back_to_direct_stop_when_worker_queue_stays_saturated() {
        let stops = Arc::new(AtomicUsize::new(0));
        let interrupts = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(StopInterruptBackend::new(
            Arc::clone(&stops),
            Arc::clone(&interrupts),
        )) as Arc<dyn PowBackend>;
        let (worker_tx, _worker_rx) = bounded::<BackendWorkerCommand>(1);
        let (outcome_tx, _outcome_rx) = bounded::<BackendTaskOutcome>(1);
        let pending_control = Arc::new(AtomicUsize::new(0));
        worker_tx
            .send(BackendWorkerCommand::Run {
                task: BackendTask {
                    idx: 0,
                    backend_id: 5,
                    backend: "stop-counter",
                    backend_handle: Arc::clone(&backend),
                    kind: BackendTaskKind::Cancel,
                    timeout: Duration::from_secs(1),
                },
                deadline: Instant::now() + Duration::from_secs(1),
                outcome_tx,
                pending_control,
            })
            .expect("prefill backend worker queue should succeed");

        perform_quarantine_stop(5, "stop-counter", backend, Some(worker_tx));

        assert_eq!(stops.load(Ordering::Relaxed), 1);
        assert!(interrupts.load(Ordering::Relaxed) >= 1);
    }

    struct CountingBackend {
        assign_calls: AtomicUsize,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self {
                assign_calls: AtomicUsize::new(0),
            }
        }
    }

    impl PowBackend for CountingBackend {
        fn name(&self) -> &'static str {
            "counting"
        }

        fn lanes(&self) -> usize {
            1
        }

        fn set_instance_id(&self, _id: BackendInstanceId) {}

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            Ok(())
        }

        fn stop(&self) {}

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            self.assign_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn cancel_work(&self) -> Result<()> {
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn run_backend_call_rejects_expired_deadline_before_invoking_backend() {
        let backend = Arc::new(CountingBackend::new());
        let backend_dyn = Arc::clone(&backend) as Arc<dyn PowBackend>;
        let work = WorkAssignment {
            template: Arc::new(WorkTemplate {
                work_id: 1,
                epoch: 1,
                header_base: Arc::from(vec![0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                stop_at: Instant::now() + Duration::from_secs(1),
            }),
            nonce_chunk: NonceChunk {
                start_nonce: 0,
                nonce_count: 1,
            },
        };

        let pending_control = AtomicUsize::new(0);
        let err = run_backend_call(
            backend_dyn,
            BackendTaskKind::Assign(work),
            Instant::now() - Duration::from_millis(1),
            &pending_control,
        )
        .expect_err("expired deadline should fail fast");
        assert!(format!("{err:#}").contains("deadline elapsed"));
        assert_eq!(backend.assign_calls.load(Ordering::Relaxed), 0);
    }

    struct SlowAssignBackend {
        interrupts: Arc<AtomicUsize>,
        delay: Duration,
    }

    impl SlowAssignBackend {
        fn new(interrupts: Arc<AtomicUsize>, delay: Duration) -> Self {
            Self { interrupts, delay }
        }
    }

    impl PowBackend for SlowAssignBackend {
        fn name(&self) -> &'static str {
            "slow-assign"
        }

        fn lanes(&self) -> usize {
            1
        }

        fn set_instance_id(&self, _id: BackendInstanceId) {}

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            Ok(())
        }

        fn stop(&self) {}

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            thread::sleep(self.delay);
            Ok(())
        }

        fn cancel_work(&self) -> Result<()> {
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            Ok(())
        }

        fn request_timeout_interrupt(&self) -> Result<()> {
            self.interrupts.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn dispatch_ignores_late_outcomes_after_timeout() {
        let executor = BackendExecutor::new();
        let interrupts = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(SlowAssignBackend::new(
            Arc::clone(&interrupts),
            Duration::from_millis(40),
        )) as Arc<dyn PowBackend>;

        let work = WorkAssignment {
            template: Arc::new(WorkTemplate {
                work_id: 1,
                epoch: 1,
                header_base: Arc::from(vec![0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                stop_at: Instant::now() + Duration::from_secs(1),
            }),
            nonce_chunk: NonceChunk {
                start_nonce: 0,
                nonce_count: 1,
            },
        };

        let outcomes = executor.dispatch_backend_tasks(vec![BackendTask {
            idx: 0,
            backend_id: 77,
            backend: "slow-assign",
            backend_handle: backend,
            kind: BackendTaskKind::Assign(work),
            timeout: Duration::from_millis(5),
        }]);

        assert!(
            matches!(
                outcomes[0],
                Some(BackendTaskDispatchResult::TimedOut(
                    BackendTaskTimeoutKind::Execution
                ))
            ),
            "timed-out task should be classified as execution timeout"
        );
        assert!(
            interrupts.load(Ordering::Relaxed) > 0,
            "timeout path should request backend interrupt"
        );
    }

    #[test]
    fn dispatch_classifies_enqueue_timeout_separately_from_execution_timeout() {
        let executor = BackendExecutor::new();
        let interrupts = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(SlowAssignBackend::new(
            Arc::clone(&interrupts),
            Duration::from_millis(80),
        )) as Arc<dyn PowBackend>;

        let make_task = || BackendTask {
            idx: 0,
            backend_id: 78,
            backend: "slow-assign",
            backend_handle: Arc::clone(&backend),
            kind: BackendTaskKind::Assign(WorkAssignment {
                template: Arc::new(WorkTemplate {
                    work_id: 1,
                    epoch: 1,
                    header_base: Arc::from(vec![0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN]),
                    target: [0xFF; 32],
                    stop_at: Instant::now() + Duration::from_secs(1),
                }),
                nonce_chunk: NonceChunk {
                    start_nonce: 0,
                    nonce_count: 1,
                },
            }),
            timeout: Duration::from_millis(5),
        };

        let _ = executor.dispatch_backend_tasks(vec![make_task()]);
        let _ = executor.dispatch_backend_tasks(vec![make_task()]);

        let mut enqueue_timeout_task = make_task();
        enqueue_timeout_task.timeout = Duration::from_millis(1);
        let outcomes = executor.dispatch_backend_tasks(vec![enqueue_timeout_task]);
        assert!(
            matches!(
                outcomes[0],
                Some(BackendTaskDispatchResult::TimedOut(
                    BackendTaskTimeoutKind::Enqueue
                ))
            ),
            "queue-saturation timeout should be classified as enqueue timeout"
        );
    }

    #[test]
    fn dispatch_requests_interrupt_for_enqueue_timeouts() {
        let executor = BackendExecutor::new();
        let interrupts = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(SlowAssignBackend::new(
            Arc::clone(&interrupts),
            Duration::from_millis(80),
        )) as Arc<dyn PowBackend>;

        let make_work = || WorkAssignment {
            template: Arc::new(WorkTemplate {
                work_id: 1,
                epoch: 1,
                header_base: Arc::from(vec![0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                stop_at: Instant::now() + Duration::from_secs(1),
            }),
            nonce_chunk: NonceChunk {
                start_nonce: 0,
                nonce_count: 1,
            },
        };

        let worker_tx = executor
            .worker_sender_for_backend(79, "slow-assign", &backend)
            .expect("backend worker should spawn");
        let (prefill_outcome_tx, _prefill_outcome_rx) =
            crossbeam_channel::unbounded::<BackendTaskOutcome>();

        worker_tx
            .assignment_tx
            .send(BackendWorkerCommand::Run {
                task: BackendTask {
                    idx: 10,
                    backend_id: 79,
                    backend: "slow-assign",
                    backend_handle: Arc::clone(&backend),
                    kind: BackendTaskKind::Assign(make_work()),
                    timeout: Duration::from_secs(1),
                },
                deadline: Instant::now() + Duration::from_secs(1),
                outcome_tx: prefill_outcome_tx.clone(),
                pending_control: Arc::clone(&worker_tx.pending_control),
            })
            .expect("first prefill command should enqueue");
        worker_tx
            .assignment_tx
            .send(BackendWorkerCommand::Run {
                task: BackendTask {
                    idx: 11,
                    backend_id: 79,
                    backend: "slow-assign",
                    backend_handle: Arc::clone(&backend),
                    kind: BackendTaskKind::Assign(make_work()),
                    timeout: Duration::from_secs(1),
                },
                deadline: Instant::now() + Duration::from_secs(1),
                outcome_tx: prefill_outcome_tx,
                pending_control: Arc::clone(&worker_tx.pending_control),
            })
            .expect("second prefill command should enqueue while first is running");

        let outcomes = executor.dispatch_backend_tasks(vec![BackendTask {
            idx: 0,
            backend_id: 79,
            backend: "slow-assign",
            backend_handle: Arc::clone(&backend),
            kind: BackendTaskKind::Assign(make_work()),
            timeout: Duration::from_millis(1),
        }]);

        assert!(
            matches!(
                outcomes[0],
                Some(BackendTaskDispatchResult::TimedOut(
                    BackendTaskTimeoutKind::Enqueue
                ))
            ),
            "dispatch should classify queue saturation as enqueue timeout"
        );
        assert!(
            interrupts.load(Ordering::Relaxed) > 0,
            "enqueue timeout should request backend interrupt"
        );
    }

    #[test]
    fn control_dispatch_uses_control_lane_under_assignment_queue_saturation() {
        let executor = BackendExecutor::new();
        let interrupts = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(SlowAssignBackend::new(
            Arc::clone(&interrupts),
            Duration::from_millis(40),
        )) as Arc<dyn PowBackend>;

        let make_work = || WorkAssignment {
            template: Arc::new(WorkTemplate {
                work_id: 1,
                epoch: 1,
                header_base: Arc::from(vec![0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                stop_at: Instant::now() + Duration::from_secs(1),
            }),
            nonce_chunk: NonceChunk {
                start_nonce: 0,
                nonce_count: 1,
            },
        };

        let worker_tx = executor
            .worker_sender_for_backend(80, "slow-assign", &backend)
            .expect("backend worker should spawn");
        let (prefill_outcome_tx, _prefill_outcome_rx) =
            crossbeam_channel::unbounded::<BackendTaskOutcome>();

        worker_tx
            .assignment_tx
            .send(BackendWorkerCommand::Run {
                task: BackendTask {
                    idx: 10,
                    backend_id: 80,
                    backend: "slow-assign",
                    backend_handle: Arc::clone(&backend),
                    kind: BackendTaskKind::Assign(make_work()),
                    timeout: Duration::from_secs(1),
                },
                deadline: Instant::now() + Duration::from_secs(1),
                outcome_tx: prefill_outcome_tx.clone(),
                pending_control: Arc::clone(&worker_tx.pending_control),
            })
            .expect("first prefill command should enqueue");
        worker_tx
            .assignment_tx
            .send(BackendWorkerCommand::Run {
                task: BackendTask {
                    idx: 11,
                    backend_id: 80,
                    backend: "slow-assign",
                    backend_handle: Arc::clone(&backend),
                    kind: BackendTaskKind::Assign(make_work()),
                    timeout: Duration::from_secs(1),
                },
                deadline: Instant::now() + Duration::from_secs(1),
                outcome_tx: prefill_outcome_tx,
                pending_control: Arc::clone(&worker_tx.pending_control),
            })
            .expect("second prefill command should enqueue while first is running");

        let outcomes = executor.dispatch_backend_tasks(vec![BackendTask {
            idx: 0,
            backend_id: 80,
            backend: "slow-assign",
            backend_handle: Arc::clone(&backend),
            kind: BackendTaskKind::Cancel,
            timeout: Duration::from_millis(20),
        }]);

        assert!(
            matches!(
                outcomes[0],
                Some(BackendTaskDispatchResult::TimedOut(
                    BackendTaskTimeoutKind::Execution
                ))
            ),
            "control dispatch should avoid enqueue timeout even when assignment work is running"
        );

        let telemetry = executor.take_backend_telemetry(80, &backend);
        assert_eq!(telemetry.control_execution_timeouts, 1);
        assert_eq!(telemetry.control_enqueue_timeouts, 0);
        assert!(
            interrupts.load(Ordering::Relaxed) > 0,
            "timed-out control dispatch should request backend interrupt"
        );
    }

    #[test]
    fn take_backend_telemetry_reports_and_resets_timeout_counters() {
        let executor = BackendExecutor::new();
        let interrupts = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(SlowAssignBackend::new(
            Arc::clone(&interrupts),
            Duration::from_millis(40),
        )) as Arc<dyn PowBackend>;

        let work = WorkAssignment {
            template: Arc::new(WorkTemplate {
                work_id: 1,
                epoch: 1,
                header_base: Arc::from(vec![0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                stop_at: Instant::now() + Duration::from_secs(1),
            }),
            nonce_chunk: NonceChunk {
                start_nonce: 0,
                nonce_count: 1,
            },
        };

        let outcomes = executor.dispatch_backend_tasks(vec![BackendTask {
            idx: 0,
            backend_id: 81,
            backend: "slow-assign",
            backend_handle: Arc::clone(&backend),
            kind: BackendTaskKind::Assign(work),
            timeout: Duration::from_millis(5),
        }]);

        assert!(matches!(
            outcomes[0],
            Some(BackendTaskDispatchResult::TimedOut(
                BackendTaskTimeoutKind::Execution
            ))
        ));

        let telemetry = executor.take_backend_telemetry(81, &backend);
        assert_eq!(telemetry.assignment_execution_timeouts, 1);
        assert_eq!(telemetry.assignment_enqueue_timeouts, 0);
        assert_eq!(telemetry.control_enqueue_timeouts, 0);
        assert_eq!(telemetry.control_execution_timeouts, 0);

        let reset = executor.take_backend_telemetry(81, &backend);
        assert_eq!(reset.assignment_execution_timeouts, 0);
        assert_eq!(reset.assignment_enqueue_timeouts, 0);
        assert_eq!(reset.control_enqueue_timeouts, 0);
        assert_eq!(reset.control_execution_timeouts, 0);
        assert_eq!(reset.assignment_timeout_strikes, 0);
    }

    struct PendingThenCompleteBackend {
        pending: AtomicUsize,
        wait_calls: AtomicUsize,
    }

    impl PendingThenCompleteBackend {
        fn new(pending: usize) -> Self {
            Self {
                pending: AtomicUsize::new(pending),
                wait_calls: AtomicUsize::new(0),
            }
        }
    }

    impl PowBackend for PendingThenCompleteBackend {
        fn name(&self) -> &'static str {
            "pending-assign"
        }

        fn lanes(&self) -> usize {
            1
        }

        fn set_instance_id(&self, _id: BackendInstanceId) {}

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            Ok(())
        }

        fn stop(&self) {}

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            Ok(())
        }

        fn cancel_work(&self) -> Result<()> {
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            Ok(())
        }

        fn assign_work_batch_nonblocking(
            &self,
            _work: &[WorkAssignment],
        ) -> Result<BackendCallStatus> {
            let mut pending = self.pending.load(Ordering::Acquire);
            loop {
                if pending == 0 {
                    return Ok(BackendCallStatus::Complete);
                }
                match self.pending.compare_exchange(
                    pending,
                    pending - 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Ok(BackendCallStatus::Pending),
                    Err(observed) => pending = observed,
                }
            }
        }

        fn wait_for_nonblocking_progress(&self, _wait_for: Duration) -> Result<()> {
            self.wait_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn supports_true_nonblocking(&self) -> bool {
            true
        }

        fn capabilities(&self) -> crate::backend::BackendCapabilities {
            crate::backend::BackendCapabilities {
                execution_model: crate::backend::BackendExecutionModel::Nonblocking,
                nonblocking_poll_min: Some(Duration::from_micros(50)),
                nonblocking_poll_max: Some(Duration::from_millis(1)),
                ..crate::backend::BackendCapabilities::default()
            }
        }
    }

    #[test]
    fn dispatch_retries_pending_nonblocking_assignment_until_complete() {
        let executor = BackendExecutor::new();
        let backend = Arc::new(PendingThenCompleteBackend::new(3));
        let backend_dyn = Arc::clone(&backend) as Arc<dyn PowBackend>;

        let work = WorkAssignment {
            template: Arc::new(WorkTemplate {
                work_id: 1,
                epoch: 1,
                header_base: Arc::from(vec![0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                stop_at: Instant::now() + Duration::from_secs(1),
            }),
            nonce_chunk: NonceChunk {
                start_nonce: 0,
                nonce_count: 1,
            },
        };

        let outcomes = executor.dispatch_backend_tasks(vec![BackendTask {
            idx: 0,
            backend_id: 1,
            backend: "pending-assign",
            backend_handle: backend_dyn,
            kind: BackendTaskKind::Assign(work),
            timeout: Duration::from_millis(25),
        }]);

        assert!(outcomes[0].as_ref().is_some_and(|outcome| matches!(
            outcome,
            BackendTaskDispatchResult::Completed(Ok(()))
        )));
        assert!(
            backend.wait_calls.load(Ordering::Relaxed) > 0,
            "pending path should use backend wait hook"
        );
    }
}
