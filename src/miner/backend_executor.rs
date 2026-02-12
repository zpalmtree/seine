use std::collections::{BTreeMap, BTreeSet};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
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
        deadline: Instant,
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
        let worker = spawn_backend_worker(backend_id, backend, key.backend_ptr)?;
        let sender = worker.tx.clone();
        registry.insert(key, worker);
        Some(sender)
    }

    pub(super) fn clear(&self) {
        if let Ok(mut registry) = self.workers.lock() {
            registry.clear();
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

        let detached = Arc::clone(&backend);
        let quarantined = Arc::clone(&self.quarantined);
        if thread::Builder::new()
            .name("seine-backend-quarantine".to_string())
            .spawn(move || {
                if panic::catch_unwind(AssertUnwindSafe(|| detached.stop())).is_err() {
                    warn("BACKEND", "backend stop panicked during async quarantine");
                }
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
            if panic::catch_unwind(AssertUnwindSafe(|| backend.stop())).is_err() {
                warn(
                    "BACKEND",
                    "backend stop panicked during synchronous quarantine fallback",
                );
            }
            if let Ok(mut inflight) = self.quarantined.lock() {
                inflight.remove(&key);
            }
        }
    }

    pub(super) fn dispatch_backend_tasks(
        &self,
        tasks: Vec<BackendTask>,
        timeout: Duration,
    ) -> Vec<Option<BackendTaskOutcome>> {
        if tasks.is_empty() {
            return Vec::new();
        }

        #[derive(Clone)]
        struct TimeoutTaskContext {
            backend_id: BackendInstanceId,
            backend: &'static str,
            action: &'static str,
            backend_handle: Arc<dyn PowBackend>,
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
        let mut task_contexts = BTreeMap::<usize, TimeoutTaskContext>::new();
        let (outcome_tx, outcome_rx) = bounded::<BackendTaskOutcome>(expected.max(1));
        let mut recv_deadline = Instant::now();

        for task in tasks {
            let backend_id = task.backend_id;
            let backend = task.backend;
            let action = task.kind.action_label();
            let backend_handle = Arc::clone(&task.backend_handle);
            let idx = task.idx;
            task_contexts.insert(
                idx,
                TimeoutTaskContext {
                    backend_id,
                    backend,
                    action,
                    backend_handle: Arc::clone(&backend_handle),
                },
            );
            let task_deadline = Instant::now()
                .checked_add(timeout)
                .unwrap_or_else(Instant::now);
            if task_deadline > recv_deadline {
                recv_deadline = task_deadline;
            }
            let Some(worker_tx) =
                self.worker_sender_for_backend(backend_id, backend, &backend_handle)
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
                deadline: task_deadline,
                outcome_tx: outcome_tx.clone(),
            };
            match worker_tx.try_send(command) {
                Ok(()) => {}
                Err(TrySendError::Disconnected(_)) => {
                    self.remove_backend_worker(backend_id, &backend_handle);
                    let _ = outcome_tx.send(BackendTaskOutcome {
                        idx,
                        result: Err(anyhow!(
                            "{action} dispatch failed: worker channel closed for {backend}#{backend_id}"
                        )),
                    });
                    self.quarantine_backend(backend_id, backend_handle);
                }
                Err(TrySendError::Full(_)) => {
                    self.remove_backend_worker(backend_id, &backend_handle);
                    let _ = outcome_tx.send(BackendTaskOutcome {
                        idx,
                        result: Err(anyhow!(
                            "{action} dispatch failed: queue saturated for {backend}#{backend_id}"
                        )),
                    });
                    self.quarantine_backend(backend_id, backend_handle);
                }
            }
        }
        drop(outcome_tx);

        let mut received = 0usize;
        while received < expected {
            let now = Instant::now();
            if now >= recv_deadline {
                break;
            }
            match outcome_rx.recv_timeout(recv_deadline.saturating_duration_since(now)) {
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

        for idx in &expected_indices {
            if outcomes[*idx].is_some() {
                continue;
            }
            let Some(context) = task_contexts.get(idx) else {
                continue;
            };
            if let Err(err) = context.backend_handle.request_timeout_interrupt() {
                warn(
                    "BACKEND",
                    format!(
                        "{} timeout interrupt failed for {}#{}: {err:#}",
                        context.action, context.backend, context.backend_id
                    ),
                );
            }
        }

        outcomes
    }
}

fn run_backend_command(command: BackendWorkerCommand) {
    let BackendWorkerCommand::Run {
        task,
        deadline,
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
    let action_label = kind.action_label();
    let result = run_backend_call(Arc::clone(&backend_handle), kind, deadline)
        .map_err(|err| anyhow!("{action_label} failed for {backend}#{backend_id}: {err:#}"));
    let send_result = outcome_tx.send(BackendTaskOutcome { idx, result });
    if send_result.is_err() {
        let _ = backend_handle.request_timeout_interrupt();
    }
}

fn run_backend_call(
    backend_handle: Arc<dyn PowBackend>,
    kind: BackendTaskKind,
    deadline: Instant,
) -> Result<()> {
    if Instant::now() >= deadline {
        return Err(anyhow!("task deadline elapsed before backend call"));
    }
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::backend::{BackendEvent, NonceChunk, PowBackend, WorkTemplate};

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
}
