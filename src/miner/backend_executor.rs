use std::collections::{BTreeMap, BTreeSet};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use crossbeam_channel::{bounded, RecvTimeoutError, Sender, TrySendError};

use crate::backend::{BackendInstanceId, PowBackend, WorkAssignment};

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

enum BackendWorkerCommand {
    Run {
        task: BackendTask,
        timeout: Duration,
        outcome_tx: Sender<BackendTaskOutcome>,
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

static BACKEND_WORKERS: OnceLock<Mutex<BTreeMap<BackendWorkerKey, BackendWorker>>> =
    OnceLock::new();

fn worker_registry() -> &'static Mutex<BTreeMap<BackendWorkerKey, BackendWorker>> {
    BACKEND_WORKERS.get_or_init(|| Mutex::new(BTreeMap::new()))
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

fn spawn_backend_worker(
    backend_id: BackendInstanceId,
    backend: &'static str,
    backend_ptr: usize,
) -> Option<BackendWorker> {
    let (cmd_tx, cmd_rx) = bounded::<BackendWorkerCommand>(1);
    let thread_name = format!("seine-backend-{backend}-{backend_id}-{backend_ptr:x}");
    let spawn_result = thread::Builder::new().name(thread_name).spawn(move || {
        while let Ok(command) = cmd_rx.recv() {
            run_backend_command(command);
        }
    });

    if spawn_result.is_err() {
        return None;
    }
    Some(BackendWorker { tx: cmd_tx })
}

fn worker_sender_for_backend(
    backend_id: BackendInstanceId,
    backend: &'static str,
    backend_handle: &Arc<dyn PowBackend>,
) -> Option<Sender<BackendWorkerCommand>> {
    let key = backend_worker_key(backend_id, backend_handle);
    let mut registry = worker_registry().lock().ok()?;
    if let Some(worker) = registry.get(&key) {
        return Some(worker.tx.clone());
    }
    let worker = spawn_backend_worker(backend_id, backend, key.backend_ptr)?;
    let sender = worker.tx.clone();
    registry.insert(key, worker);
    Some(sender)
}

pub(super) fn clear_backend_workers() {
    if let Ok(mut registry) = worker_registry().lock() {
        registry.clear();
    }
}

pub(super) fn prune_backend_workers(backends: &[BackendSlot]) {
    let active_keys = backends
        .iter()
        .map(|slot| backend_worker_key(slot.id, &slot.backend))
        .collect::<BTreeSet<_>>();
    if let Ok(mut registry) = worker_registry().lock() {
        registry.retain(|backend_key, _| active_keys.contains(backend_key));
    }
}

pub(super) fn remove_backend_worker(
    backend_id: BackendInstanceId,
    backend_handle: &Arc<dyn PowBackend>,
) {
    let key = backend_worker_key(backend_id, backend_handle);
    if let Ok(mut registry) = worker_registry().lock() {
        registry.remove(&key);
    }
}

pub(super) fn quarantine_backend(backend: Arc<dyn PowBackend>) {
    let detached = Arc::clone(&backend);
    if thread::Builder::new()
        .name("seine-backend-quarantine".to_string())
        .spawn(move || detached.stop())
        .is_err()
    {
        warn(
            "BACKEND",
            "failed to spawn backend quarantine worker; running synchronous stop fallback",
        );
        if panic::catch_unwind(AssertUnwindSafe(|| backend.stop())).is_err() {
            warn(
                "BACKEND",
                "backend stop panicked during synchronous quarantine fallback",
            );
        }
    }
}

pub(super) fn dispatch_backend_tasks(
    tasks: Vec<BackendTask>,
    timeout: Duration,
) -> Vec<Option<BackendTaskOutcome>> {
    if tasks.is_empty() {
        return Vec::new();
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
    let mut outcomes: Vec<Option<BackendTaskOutcome>> =
        std::iter::repeat_with(|| None).take(outcomes_len).collect();
    let (outcome_tx, outcome_rx) = bounded::<BackendTaskOutcome>(expected.max(1));

    for task in tasks {
        let backend_id = task.backend_id;
        let backend = task.backend;
        let action = task.kind.action_label();
        let backend_handle = Arc::clone(&task.backend_handle);
        let idx = task.idx;
        let Some(worker_tx) = worker_sender_for_backend(backend_id, backend, &backend_handle)
        else {
            let _ = outcome_tx.send(BackendTaskOutcome {
                idx,
                result: Err(anyhow!(
                    "{action} dispatch failed: could not spawn worker for {backend}#{backend_id}"
                )),
            });
            continue;
        };

        let command = BackendWorkerCommand::Run {
            task,
            timeout,
            outcome_tx: outcome_tx.clone(),
        };
        match worker_tx.try_send(command) {
            Ok(()) => {}
            Err(TrySendError::Disconnected(_)) => {
                remove_backend_worker(backend_id, &backend_handle);
                let _ = outcome_tx.send(BackendTaskOutcome {
                    idx,
                    result: Err(anyhow!(
                        "{action} dispatch failed: worker channel closed for {backend}#{backend_id}"
                    )),
                });
                quarantine_backend(backend_handle);
            }
            Err(TrySendError::Full(_)) => {
                remove_backend_worker(backend_id, &backend_handle);
                let _ = outcome_tx.send(BackendTaskOutcome {
                    idx,
                    result: Err(anyhow!(
                        "{action} dispatch failed: queue saturated for {backend}#{backend_id}"
                    )),
                });
                quarantine_backend(backend_handle);
            }
        }
    }
    drop(outcome_tx);

    let deadline = Instant::now() + timeout;
    let mut received = 0usize;
    while received < expected {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        match outcome_rx.recv_timeout(deadline.saturating_duration_since(now)) {
            Ok(outcome) => {
                let outcome_idx = outcome.idx;
                if expected_indices.contains(&outcome_idx) && outcomes[outcome_idx].is_none() {
                    received = received.saturating_add(1);
                }
                outcomes[outcome_idx] = Some(outcome);
            }
            Err(RecvTimeoutError::Timeout) => break,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    while let Ok(outcome) = outcome_rx.try_recv() {
        let outcome_idx = outcome.idx;
        if expected_indices.contains(&outcome_idx) && outcomes[outcome_idx].is_none() {
            received = received.saturating_add(1);
        }
        outcomes[outcome_idx] = Some(outcome);
    }

    outcomes
}

fn run_backend_command(command: BackendWorkerCommand) {
    let BackendWorkerCommand::Run {
        task,
        timeout,
        outcome_tx,
    } = command;

    let BackendTask {
        idx,
        backend_id,
        backend,
        backend_handle,
        kind,
        ..
    } = task;
    let deadline = Instant::now() + timeout.max(Duration::from_millis(1));
    let action_label = kind.action_label();
    let result = run_backend_call(Arc::clone(&backend_handle), kind, deadline)
        .map_err(|err| anyhow!("{action_label} failed for {backend}#{backend_id}: {err:#}"));
    let send_result = outcome_tx.send(BackendTaskOutcome { idx, result });
    if send_result.is_err() {
        quarantine_backend(backend_handle);
    }
}

fn run_backend_call(
    backend_handle: Arc<dyn PowBackend>,
    kind: BackendTaskKind,
    deadline: Instant,
) -> Result<()> {
    match panic::catch_unwind(AssertUnwindSafe(|| match kind {
        BackendTaskKind::Assign(work) => backend_handle.assign_work_with_deadline(&work, deadline),
        BackendTaskKind::AssignBatch(batch) => {
            backend_handle.assign_work_batch_with_deadline(&batch, deadline)
        }
        BackendTaskKind::Cancel => backend_handle.cancel_work_with_deadline(deadline),
        BackendTaskKind::Fence => backend_handle.fence_with_deadline(deadline),
    })) {
        Ok(result) => result,
        Err(_) => Err(anyhow!("backend task panicked")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::Sender;

    use crate::backend::{BackendEvent, PowBackend};

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

    #[test]
    fn dispatch_handles_sparse_indices_without_waiting_for_missing_slots() {
        clear_backend_workers();

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
        let outcomes = dispatch_backend_tasks(tasks, Duration::from_millis(250));
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "sparse indices should not block waiting for missing outcomes"
        );
        assert_eq!(outcomes.len(), 6);
        assert!(outcomes[0]
            .as_ref()
            .is_some_and(|outcome| outcome.result.is_ok()));
        assert!(outcomes[5]
            .as_ref()
            .is_some_and(|outcome| outcome.result.is_ok()));
        assert!(outcomes[1].is_none());
        assert!(outcomes[2].is_none());
        assert!(outcomes[3].is_none());
        assert!(outcomes[4].is_none());

        clear_backend_workers();
    }
}
