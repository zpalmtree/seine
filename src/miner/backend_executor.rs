use std::collections::{BTreeMap, BTreeSet};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use crossbeam_channel::{bounded, unbounded, RecvTimeoutError, Sender};

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

static BACKEND_WORKERS: OnceLock<Mutex<BTreeMap<BackendInstanceId, BackendWorker>>> =
    OnceLock::new();

fn worker_registry() -> &'static Mutex<BTreeMap<BackendInstanceId, BackendWorker>> {
    BACKEND_WORKERS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn spawn_backend_worker(
    backend_id: BackendInstanceId,
    backend: &'static str,
) -> Option<BackendWorker> {
    let (cmd_tx, cmd_rx) = unbounded::<BackendWorkerCommand>();
    let thread_name = format!("seine-backend-{backend}-{backend_id}");
    let spawn_result = thread::Builder::new().name(thread_name).spawn(move || {
        while let Ok(command) = cmd_rx.recv() {
            match command {
                BackendWorkerCommand::Run {
                    task,
                    timeout,
                    outcome_tx,
                } => run_backend_task(task, timeout, outcome_tx),
            }
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
) -> Option<Sender<BackendWorkerCommand>> {
    let mut registry = worker_registry().lock().ok()?;
    if let Some(worker) = registry.get(&backend_id) {
        return Some(worker.tx.clone());
    }
    let worker = spawn_backend_worker(backend_id, backend)?;
    let sender = worker.tx.clone();
    registry.insert(backend_id, worker);
    Some(sender)
}

pub(super) fn clear_backend_workers() {
    if let Ok(mut registry) = worker_registry().lock() {
        registry.clear();
    }
}

pub(super) fn prune_backend_workers(backends: &[BackendSlot]) {
    let active_ids = backends.iter().map(|slot| slot.id).collect::<BTreeSet<_>>();
    if let Ok(mut registry) = worker_registry().lock() {
        registry.retain(|backend_id, _| active_ids.contains(backend_id));
    }
}

pub(super) fn remove_backend_worker(backend_id: BackendInstanceId) {
    if let Ok(mut registry) = worker_registry().lock() {
        registry.remove(&backend_id);
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
            "failed to spawn backend quarantine worker; skipping synchronous stop to avoid runtime stall",
        );
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
    let expected = tasks
        .iter()
        .map(|task| task.idx)
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    let mut outcomes: Vec<Option<BackendTaskOutcome>> =
        std::iter::repeat_with(|| None).take(expected).collect();
    let (outcome_tx, outcome_rx) = bounded::<BackendTaskOutcome>(expected.max(1));

    for task in tasks {
        let backend_id = task.backend_id;
        let backend = task.backend;
        let action = task.kind.action_label();
        let backend_handle = Arc::clone(&task.backend_handle);
        let idx = task.idx;
        let Some(worker_tx) = worker_sender_for_backend(backend_id, backend) else {
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
        if worker_tx.send(command).is_err() {
            if let Ok(mut registry) = worker_registry().lock() {
                registry.remove(&backend_id);
            }
            let _ = outcome_tx.send(BackendTaskOutcome {
                idx,
                result: Err(anyhow!(
                    "{action} dispatch failed: worker channel closed for {backend}#{backend_id}"
                )),
            });
            quarantine_backend(backend_handle);
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
                if outcomes[outcome_idx].is_none() {
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
        if outcomes[outcome_idx].is_none() {
            received = received.saturating_add(1);
        }
        outcomes[outcome_idx] = Some(outcome);
    }

    outcomes
}

fn run_backend_task(task: BackendTask, timeout: Duration, outcome_tx: Sender<BackendTaskOutcome>) {
    let BackendTask {
        idx,
        backend_id,
        backend,
        backend_handle,
        kind,
        ..
    } = task;
    let started = Instant::now();
    let deadline = started + timeout.max(Duration::from_millis(1));
    let action_label = kind.action_label();
    let (result_tx, result_rx) = bounded::<Result<()>>(1);
    let call_backend = Arc::clone(&backend_handle);
    let call_kind = kind;
    let call_thread_name = format!("seine-backend-call-{backend}-{backend_id}");
    let result = match thread::Builder::new()
        .name(call_thread_name)
        .spawn(move || {
            let result = run_backend_call(call_backend, call_kind, deadline);
            let _ = result_tx.send(result);
        }) {
        Ok(call_handle) => match result_rx.recv_timeout(timeout) {
            Ok(result) => {
                let _ = call_handle.join();
                result
            }
            Err(RecvTimeoutError::Timeout) => {
                // Detached by design: backend may be stuck in an uninterruptible call.
                drop(call_handle);
                Err(anyhow!(
                    "{} call exceeded {}ms deadline; backend quarantined",
                    action_label,
                    timeout.as_millis()
                ))
            }
            Err(RecvTimeoutError::Disconnected) => {
                if call_handle.join().is_err() {
                    Err(anyhow!("{} task panicked", action_label))
                } else {
                    Err(anyhow!(
                        "{} worker exited without returning an outcome",
                        action_label
                    ))
                }
            }
        },
        Err(_) => Err(anyhow!(
            "{} dispatch failed: could not spawn call worker for {}#{}",
            action_label,
            backend,
            backend_id
        )),
    };
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
