use std::collections::{BTreeMap, BTreeSet};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use crossbeam_channel::{bounded, RecvTimeoutError, SendTimeoutError, Sender, TrySendError};

use crate::backend::{
    normalize_backend_capabilities, AssignmentSemantics, BackendCallStatus, BackendCapabilities,
    BackendInstanceId, PowBackend, WorkAssignment,
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
}

pub(super) struct BackendTask {
    pub idx: usize,
    pub backend_id: BackendInstanceId,
    pub backend: &'static str,
    pub backend_handle: Arc<dyn PowBackend>,
    pub kind: BackendTaskKind,
}

pub(super) struct BackendTaskOutcome {
    pub idx: usize,
    pub result: Result<()>,
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
    tx: Sender<BackendWorkerCommand>,
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

const DEFAULT_ASSIGNMENT_TIMEOUT_STRIKES_BEFORE_QUARANTINE: u32 = 3;
const BACKEND_WORKER_QUEUE_CAPACITY_MAX: usize = 64;
const BACKEND_STOP_ACK_TIMEOUT: Duration = Duration::from_secs(1);
const BACKEND_STOP_ACK_GRACE_TIMEOUT: Duration = Duration::from_secs(2);
const BACKEND_STOP_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(100);
const BACKEND_STOP_FALLBACK_TIMEOUT: Duration = Duration::from_secs(2);
const BACKEND_TASK_ENQUEUE_RETRY_MIN: Duration = Duration::from_micros(50);
const BACKEND_TASK_ENQUEUE_RETRY_MAX: Duration = Duration::from_millis(1);

#[derive(Clone)]
pub(super) struct BackendExecutor {
    workers: Arc<Mutex<BTreeMap<BackendWorkerKey, BackendWorker>>>,
    quarantined: Arc<Mutex<BTreeSet<BackendWorkerKey>>>,
    assignment_timeout_strikes: Arc<Mutex<BTreeMap<BackendWorkerKey, u32>>>,
    assignment_timeout_threshold: Arc<AtomicU32>,
}

impl BackendExecutor {
    pub(super) fn new() -> Self {
        Self {
            workers: Arc::new(Mutex::new(BTreeMap::new())),
            quarantined: Arc::new(Mutex::new(BTreeSet::new())),
            assignment_timeout_strikes: Arc::new(Mutex::new(BTreeMap::new())),
            assignment_timeout_threshold: Arc::new(AtomicU32::new(
                DEFAULT_ASSIGNMENT_TIMEOUT_STRIKES_BEFORE_QUARANTINE,
            )),
        }
    }

    pub(super) fn set_assignment_timeout_threshold(&self, threshold: u32) {
        self.assignment_timeout_threshold
            .store(threshold.max(1), Ordering::Release);
    }

    fn assignment_timeout_threshold(&self) -> u32 {
        self.assignment_timeout_threshold
            .load(Ordering::Acquire)
            .max(1)
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

fn backend_worker_queue_capacity(backend_handle: &Arc<dyn PowBackend>) -> usize {
    let capabilities = normalized_backend_capabilities(backend_handle);
    let requested = capabilities.max_inflight_assignments.max(1) as usize;
    let queue_depth = if capabilities.assignment_semantics == AssignmentSemantics::Append {
        requested
    } else {
        1
    };
    queue_depth.clamp(1, BACKEND_WORKER_QUEUE_CAPACITY_MAX)
}

fn spawn_backend_worker(
    backend_id: BackendInstanceId,
    backend: &'static str,
    backend_ptr: usize,
    queue_capacity: usize,
) -> Option<BackendWorker> {
    let (cmd_tx, cmd_rx) = bounded::<BackendWorkerCommand>(queue_capacity.max(1));
    let thread_name = format!("seine-backend-{backend}-{backend_id}-{backend_ptr:x}");
    let spawn_result = thread::Builder::new().name(thread_name).spawn(move || {
        while let Ok(command) = cmd_rx.recv() {
            match command {
                BackendWorkerCommand::Run {
                    task,
                    deadline,
                    outcome_tx,
                } => run_backend_task(task, deadline, outcome_tx),
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
                    break;
                }
            }
        }
    });

    if spawn_result.is_err() {
        return None;
    }
    Some(BackendWorker { tx: cmd_tx })
}

impl BackendExecutor {
    fn worker_sender_for_backend(
        &self,
        backend_id: BackendInstanceId,
        backend: &'static str,
        backend_handle: &Arc<dyn PowBackend>,
    ) -> Option<Sender<BackendWorkerCommand>> {
        let key = backend_worker_key(backend_id, backend_handle);
        let mut registry = self.workers.lock().ok()?;
        if let Some(worker) = registry.get(&key) {
            return Some(worker.tx.clone());
        }
        let queue_capacity = backend_worker_queue_capacity(backend_handle);
        let worker = spawn_backend_worker(backend_id, backend, key.backend_ptr, queue_capacity)?;
        let sender = worker.tx.clone();
        registry.insert(key, worker);
        Some(sender)
    }

    pub(super) fn clear(&self) {
        if let Ok(mut registry) = self.workers.lock() {
            registry.clear();
        }
        if let Ok(mut inflight) = self.quarantined.lock() {
            inflight.clear();
        }
        if let Ok(mut strikes) = self.assignment_timeout_strikes.lock() {
            strikes.clear();
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
        if let Ok(mut strikes) = self.assignment_timeout_strikes.lock() {
            strikes.retain(|backend_key, _| active_keys.contains(backend_key));
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
        if let Ok(mut strikes) = self.assignment_timeout_strikes.lock() {
            strikes.remove(&key);
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

        if let Ok(mut strikes) = self.assignment_timeout_strikes.lock() {
            strikes.remove(&key);
        }

        let worker_tx = self
            .workers
            .lock()
            .ok()
            .and_then(|mut registry| registry.remove(&key))
            .map(|worker| worker.tx);
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
        let key = backend_worker_key(backend_id, backend_handle);
        if let Ok(mut strikes) = self.assignment_timeout_strikes.lock() {
            strikes.remove(&key);
        }
    }

    pub(super) fn note_assignment_timeout(
        &self,
        backend_id: BackendInstanceId,
        backend_handle: &Arc<dyn PowBackend>,
    ) -> AssignmentTimeoutDecision {
        let key = backend_worker_key(backend_id, backend_handle);
        let threshold = self.assignment_timeout_threshold();
        let mut strikes_map = match self.assignment_timeout_strikes.lock() {
            Ok(map) => map,
            Err(_) => {
                return AssignmentTimeoutDecision {
                    strikes: threshold,
                    threshold,
                    should_quarantine: true,
                };
            }
        };
        let strikes = strikes_map
            .get(&key)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        let should_quarantine = strikes >= threshold;
        if should_quarantine {
            strikes_map.remove(&key);
        } else {
            strikes_map.insert(key, strikes);
        }

        AssignmentTimeoutDecision {
            strikes,
            threshold,
            should_quarantine,
        }
    }

    pub(super) fn dispatch_backend_tasks(
        &self,
        tasks: Vec<BackendTask>,
        timeout: Duration,
    ) -> Vec<Option<BackendTaskDispatchResult>> {
        if tasks.is_empty() {
            return Vec::new();
        }

        #[derive(Clone)]
        struct TimeoutTaskContext {
            backend_id: BackendInstanceId,
            backend: &'static str,
            action: &'static str,
            backend_handle: Arc<dyn PowBackend>,
            deadline: Instant,
        }

        let timeout = timeout.max(Duration::from_millis(1));
        let expected_indices = tasks.iter().map(|task| task.idx).collect::<BTreeSet<_>>();
        let expected = expected_indices.len();
        let outcomes_len = expected_indices
            .iter()
            .next_back()
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        let mut outcomes: Vec<Option<BackendTaskDispatchResult>> =
            std::iter::repeat_with(|| None).take(outcomes_len).collect();
        let mut task_contexts = BTreeMap::<usize, TimeoutTaskContext>::new();
        let (outcome_tx, outcome_rx) = bounded::<BackendTaskOutcome>(expected.max(1));
        let mut pending = BTreeSet::new();

        for task in tasks {
            let backend_id = task.backend_id;
            let backend = task.backend;
            let action = task.kind.action_label();
            let backend_handle = Arc::clone(&task.backend_handle);
            let idx = task.idx;
            let task_deadline = Instant::now()
                .checked_add(timeout)
                .unwrap_or_else(Instant::now);
            task_contexts.insert(
                idx,
                TimeoutTaskContext {
                    backend_id,
                    backend,
                    action,
                    backend_handle: Arc::clone(&backend_handle),
                    deadline: task_deadline,
                },
            );
            let Some(worker_tx) =
                self.worker_sender_for_backend(backend_id, backend, &backend_handle)
            else {
                outcomes[idx] = Some(BackendTaskDispatchResult::Completed(Err(anyhow!(
                    "{action} dispatch failed: could not spawn worker for {backend}#{backend_id}"
                ))));
                continue;
            };

            let command = BackendWorkerCommand::Run {
                task,
                deadline: task_deadline,
                outcome_tx: outcome_tx.clone(),
            };
            match enqueue_backend_command_until_deadline(&worker_tx, command, task_deadline) {
                EnqueueCommandStatus::Enqueued => {
                    pending.insert(idx);
                }
                EnqueueCommandStatus::Disconnected => {
                    self.remove_backend_worker(backend_id, &backend_handle);
                    outcomes[idx] = Some(BackendTaskDispatchResult::Completed(Err(anyhow!(
                        "{action} dispatch failed: worker channel closed for {backend}#{backend_id}"
                    ))));
                    self.quarantine_backend(backend_id, backend_handle);
                }
                EnqueueCommandStatus::DeadlineElapsed => {
                    outcomes[idx] = Some(BackendTaskDispatchResult::TimedOut(
                        BackendTaskTimeoutKind::Enqueue,
                    ));
                }
            }
        }
        drop(outcome_tx);

        while !pending.is_empty() {
            let now = Instant::now();

            let mut next_deadline: Option<Instant> = None;
            let mut expired = Vec::new();
            for idx in &pending {
                let Some(context) = task_contexts.get(idx) else {
                    expired.push(*idx);
                    continue;
                };
                if now >= context.deadline {
                    expired.push(*idx);
                } else {
                    next_deadline = Some(
                        next_deadline
                            .map_or(context.deadline, |current| current.min(context.deadline)),
                    );
                }
            }
            for idx in expired {
                pending.remove(&idx);
                if outcomes[idx].is_none() {
                    outcomes[idx] = Some(BackendTaskDispatchResult::TimedOut(
                        BackendTaskTimeoutKind::Execution,
                    ));
                }
            }
            let Some(next_deadline) = next_deadline else {
                break;
            };

            let wait_for = next_deadline
                .saturating_duration_since(now)
                .max(Duration::from_millis(1));
            match outcome_rx.recv_timeout(wait_for) {
                Ok(outcome) => {
                    let outcome_idx = outcome.idx;
                    if pending.contains(&outcome_idx) && outcomes[outcome_idx].is_none() {
                        pending.remove(&outcome_idx);
                        outcomes[outcome_idx] =
                            Some(BackendTaskDispatchResult::Completed(outcome.result));
                    }
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }

            while let Ok(outcome) = outcome_rx.try_recv() {
                let outcome_idx = outcome.idx;
                if pending.contains(&outcome_idx) && outcomes[outcome_idx].is_none() {
                    pending.remove(&outcome_idx);
                    outcomes[outcome_idx] =
                        Some(BackendTaskDispatchResult::Completed(outcome.result));
                }
            }
        }

        while let Ok(outcome) = outcome_rx.try_recv() {
            let outcome_idx = outcome.idx;
            if pending.contains(&outcome_idx) && outcomes[outcome_idx].is_none() {
                pending.remove(&outcome_idx);
                outcomes[outcome_idx] = Some(BackendTaskDispatchResult::Completed(outcome.result));
            }
        }

        for idx in &expected_indices {
            let Some(context) = task_contexts.get(idx) else {
                continue;
            };
            let Some(timeout_kind) = (match outcomes[*idx] {
                Some(BackendTaskDispatchResult::TimedOut(kind)) => Some(kind),
                _ => None,
            }) else {
                continue;
            };
            let timeout_label = match timeout_kind {
                BackendTaskTimeoutKind::Enqueue => "enqueue",
                BackendTaskTimeoutKind::Execution => "execution",
            };
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

fn run_backend_task(task: BackendTask, deadline: Instant, outcome_tx: Sender<BackendTaskOutcome>) {
    let BackendTask {
        idx,
        backend_id,
        backend,
        backend_handle,
        kind,
        ..
    } = task;
    let action_label = kind.action_label();
    let result = run_backend_call(Arc::clone(&backend_handle), kind, deadline)
        .map_err(|err| anyhow!("{action_label} failed for {backend}#{backend_id}: {err:#}"));
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
                        "backend stop command enqueue timed out for {backend}#{backend_id}; requesting interrupt and retrying stop enqueue"
                    ),
                );
                request_backend_stop_interrupt(backend_id, backend, &backend_handle);

                match enqueue_stop_command(BACKEND_STOP_ACK_GRACE_TIMEOUT) {
                    Ok(done_rx) => {
                        wait_for_stop_ack(backend_id, backend, &backend_handle, done_rx);
                    }
                    Err(SendTimeoutError::Timeout(_)) => {
                        warn(
                            "BACKEND",
                            format!(
                                "backend stop command is still saturated for {backend}#{backend_id}; handing off deferred stop waiter"
                            ),
                        );
                        if !spawn_deferred_stop_handoff(
                            backend_id,
                            backend,
                            Arc::clone(&backend_handle),
                            worker_tx.clone(),
                        ) {
                            should_run_fallback_stop = true;
                        }
                    }
                    Err(SendTimeoutError::Disconnected(_)) => {
                        should_run_fallback_stop = true;
                    }
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

fn spawn_deferred_stop_handoff(
    backend_id: BackendInstanceId,
    backend: &'static str,
    backend_handle: Arc<dyn PowBackend>,
    worker_tx: Sender<BackendWorkerCommand>,
) -> bool {
    let thread_name = format!("seine-backend-stop-handoff-{backend}-{backend_id}");
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let (done_tx, done_rx) = bounded::<()>(1);
            let stop_command = BackendWorkerCommand::Stop {
                backend_id,
                backend,
                backend_handle: Arc::clone(&backend_handle),
                done_tx,
            };
            let deadline = Instant::now()
                .checked_add(BACKEND_STOP_ACK_GRACE_TIMEOUT)
                .unwrap_or_else(Instant::now);
            match enqueue_backend_command_until_deadline(&worker_tx, stop_command, deadline) {
                EnqueueCommandStatus::Enqueued => {
                    wait_for_stop_ack(backend_id, backend, &backend_handle, done_rx)
                }
                EnqueueCommandStatus::DeadlineElapsed => {
                    warn(
                        "BACKEND",
                        format!(
                            "backend stop handoff enqueue timed out for {backend}#{backend_id}; running fallback stop"
                        ),
                    );
                    request_backend_stop_interrupt(backend_id, backend, &backend_handle);
                    run_synchronous_stop_fallback(backend_id, backend, backend_handle);
                }
                EnqueueCommandStatus::Disconnected => {
                    run_synchronous_stop_fallback(backend_id, backend, backend_handle);
                }
            }
        })
        .is_ok()
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
                    format!("backend stop fallback timed out for {backend}#{backend_id}; detached"),
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
) -> Result<()> {
    if Instant::now() >= deadline {
        return Err(anyhow!("task deadline elapsed before backend call"));
    }
    let (min_backoff, max_backoff) = backend_nonblocking_backoff_bounds(&backend_handle);
    match panic::catch_unwind(AssertUnwindSafe(|| match kind {
        BackendTaskKind::Assign(work) => run_nonblocking_until_deadline(
            deadline,
            || backend_handle.assign_work_batch_nonblocking(std::slice::from_ref(&work)),
            |wait| backend_handle.wait_for_nonblocking_progress(wait),
            min_backoff,
            max_backoff,
            "assignment deadline elapsed before backend accepted work",
        ),
        BackendTaskKind::AssignBatch(batch) => run_nonblocking_until_deadline(
            deadline,
            || backend_handle.assign_work_batch_nonblocking(&batch),
            |wait| backend_handle.wait_for_nonblocking_progress(wait),
            min_backoff,
            max_backoff,
            "assignment deadline elapsed before backend accepted work",
        ),
        BackendTaskKind::Cancel => run_nonblocking_until_deadline(
            deadline,
            || backend_handle.cancel_work_nonblocking(),
            |wait| backend_handle.wait_for_nonblocking_progress(wait),
            min_backoff,
            max_backoff,
            "cancel deadline elapsed before backend acknowledged cancel",
        ),
        BackendTaskKind::Fence => run_nonblocking_until_deadline(
            deadline,
            || backend_handle.fence_nonblocking(),
            |wait| backend_handle.wait_for_nonblocking_progress(wait),
            min_backoff,
            max_backoff,
            "fence deadline elapsed before backend acknowledged fence",
        ),
    })) {
        Ok(result) => result,
        Err(_) => Err(anyhow!("backend task panicked")),
    }
}

fn run_nonblocking_until_deadline<F, W>(
    deadline: Instant,
    mut op: F,
    mut wait_for_progress: W,
    min_backoff: Duration,
    max_backoff: Duration,
    timeout_message: &'static str,
) -> Result<()>
where
    F: FnMut() -> Result<BackendCallStatus>,
    W: FnMut(Duration) -> Result<()>,
{
    let mut backoff = min_backoff.max(Duration::from_micros(10));
    loop {
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
    }

    struct CapabilityBackend {
        max_inflight_assignments: u32,
        assignment_semantics: AssignmentSemantics,
        supports_batching: bool,
        nonblocking_poll_min: Option<Duration>,
        nonblocking_poll_max: Option<Duration>,
    }

    impl CapabilityBackend {
        fn new(max_inflight_assignments: u32) -> Self {
            Self {
                max_inflight_assignments,
                assignment_semantics: AssignmentSemantics::Replace,
                supports_batching: false,
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

        fn supports_assignment_batching(&self) -> bool {
            self.supports_batching
        }

        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities {
                max_inflight_assignments: self.max_inflight_assignments,
                assignment_semantics: self.assignment_semantics,
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
    fn nonblocking_backoff_bounds_normalize_inverted_hints() {
        let backend = Arc::new(CapabilityBackend::new(1).with_nonblocking_poll(
            Some(Duration::from_millis(7)),
            Some(Duration::from_micros(10)),
        )) as Arc<dyn PowBackend>;

        let (min_backoff, max_backoff) = backend_nonblocking_backoff_bounds(&backend);
        assert_eq!(min_backoff, Duration::from_millis(7));
        assert_eq!(max_backoff, Duration::from_millis(7));
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
            },
            BackendTask {
                idx: 5,
                backend_id: 2,
                backend: "noop",
                backend_handle: backend,
                kind: BackendTaskKind::Fence,
            },
        ];

        let started = Instant::now();
        let outcomes = executor.dispatch_backend_tasks(tasks, Duration::from_millis(250));
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

        fn request_timeout_interrupt(&self) -> Result<()> {
            self.interrupts.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn quarantine_avoids_synchronous_stop_when_stop_enqueue_times_out() {
        let stops = Arc::new(AtomicUsize::new(0));
        let interrupts = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(StopInterruptBackend::new(
            Arc::clone(&stops),
            Arc::clone(&interrupts),
        )) as Arc<dyn PowBackend>;
        let (worker_tx, _worker_rx) = bounded::<BackendWorkerCommand>(1);
        let (outcome_tx, _outcome_rx) = bounded::<BackendTaskOutcome>(1);
        worker_tx
            .send(BackendWorkerCommand::Run {
                task: BackendTask {
                    idx: 0,
                    backend_id: 5,
                    backend: "stop-counter",
                    backend_handle: Arc::clone(&backend),
                    kind: BackendTaskKind::Cancel,
                },
                deadline: Instant::now() + Duration::from_secs(1),
                outcome_tx,
            })
            .expect("prefill backend worker queue should succeed");

        perform_quarantine_stop(5, "stop-counter", backend, Some(worker_tx));

        assert_eq!(stops.load(Ordering::Relaxed), 0);
        assert!(interrupts.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn deferred_stop_handoff_enqueues_stop_after_queue_capacity_returns() {
        let stops = Arc::new(AtomicUsize::new(0));
        let interrupts = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(StopInterruptBackend::new(
            Arc::clone(&stops),
            Arc::clone(&interrupts),
        )) as Arc<dyn PowBackend>;
        let (worker_tx, worker_rx) = bounded::<BackendWorkerCommand>(1);
        let (outcome_tx, _outcome_rx) = bounded::<BackendTaskOutcome>(1);

        worker_tx
            .send(BackendWorkerCommand::Run {
                task: BackendTask {
                    idx: 0,
                    backend_id: 5,
                    backend: "stop-counter",
                    backend_handle: Arc::clone(&backend),
                    kind: BackendTaskKind::Cancel,
                },
                deadline: Instant::now() + Duration::from_secs(1),
                outcome_tx,
            })
            .expect("prefill backend worker queue should succeed");

        assert!(spawn_deferred_stop_handoff(
            5,
            "stop-counter",
            Arc::clone(&backend),
            worker_tx,
        ));

        let first = worker_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("first command should be present");
        assert!(matches!(first, BackendWorkerCommand::Run { .. }));

        let stop = worker_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("stop handoff should enqueue once capacity is available");
        match stop {
            BackendWorkerCommand::Stop {
                backend_id,
                backend,
                backend_handle,
                done_tx,
            } => {
                assert_eq!(backend_id, 5);
                assert_eq!(backend, "stop-counter");
                backend_handle.stop();
                let _ = done_tx.send(());
            }
            BackendWorkerCommand::Run { .. } => panic!("expected stop command"),
        }

        assert_eq!(stops.load(Ordering::Relaxed), 1);
        assert_eq!(interrupts.load(Ordering::Relaxed), 0);
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

        let err = run_backend_call(
            backend_dyn,
            BackendTaskKind::Assign(work),
            Instant::now() - Duration::from_millis(1),
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

        let outcomes = executor.dispatch_backend_tasks(
            vec![BackendTask {
                idx: 0,
                backend_id: 77,
                backend: "slow-assign",
                backend_handle: backend,
                kind: BackendTaskKind::Assign(work),
            }],
            Duration::from_millis(5),
        );

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
        };

        let _ = executor.dispatch_backend_tasks(vec![make_task()], Duration::from_millis(5));
        let _ = executor.dispatch_backend_tasks(vec![make_task()], Duration::from_millis(5));

        let outcomes = executor.dispatch_backend_tasks(vec![make_task()], Duration::from_millis(1));
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
            .send(BackendWorkerCommand::Run {
                task: BackendTask {
                    idx: 10,
                    backend_id: 79,
                    backend: "slow-assign",
                    backend_handle: Arc::clone(&backend),
                    kind: BackendTaskKind::Assign(make_work()),
                },
                deadline: Instant::now() + Duration::from_secs(1),
                outcome_tx: prefill_outcome_tx.clone(),
            })
            .expect("first prefill command should enqueue");
        worker_tx
            .send(BackendWorkerCommand::Run {
                task: BackendTask {
                    idx: 11,
                    backend_id: 79,
                    backend: "slow-assign",
                    backend_handle: Arc::clone(&backend),
                    kind: BackendTaskKind::Assign(make_work()),
                },
                deadline: Instant::now() + Duration::from_secs(1),
                outcome_tx: prefill_outcome_tx,
            })
            .expect("second prefill command should enqueue while first is running");

        let outcomes = executor.dispatch_backend_tasks(
            vec![BackendTask {
                idx: 0,
                backend_id: 79,
                backend: "slow-assign",
                backend_handle: Arc::clone(&backend),
                kind: BackendTaskKind::Assign(make_work()),
            }],
            Duration::from_millis(1),
        );

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

        let outcomes = executor.dispatch_backend_tasks(
            vec![BackendTask {
                idx: 0,
                backend_id: 1,
                backend: "pending-assign",
                backend_handle: backend_dyn,
                kind: BackendTaskKind::Assign(work),
            }],
            Duration::from_millis(25),
        );

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
