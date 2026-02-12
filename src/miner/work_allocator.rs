use std::collections::BTreeMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use crossbeam_channel::{bounded, unbounded, RecvTimeoutError, Sender};

use crate::backend::{BackendInstanceId, NonceChunk, WorkAssignment, WorkTemplate};

use super::ui::warn;
use super::{backend_capabilities, backend_names, total_lanes, BackendSlot, DistributeWorkOptions};

struct DispatchTask {
    idx: usize,
    slot: BackendSlot,
    batch: Vec<WorkAssignment>,
}

struct DispatchOutcome {
    idx: usize,
    slot: BackendSlot,
    backend_id: BackendInstanceId,
    backend: &'static str,
    elapsed: Duration,
    result: Result<()>,
}

struct DispatchFailure {
    backend_id: BackendInstanceId,
    backend: &'static str,
    reason: String,
}

enum AssignWorkerCommand {
    Run {
        task: DispatchTask,
        timeout: Duration,
        outcome_tx: Sender<DispatchOutcome>,
    },
}

#[derive(Clone)]
struct AssignWorker {
    tx: Sender<AssignWorkerCommand>,
}

static ASSIGN_WORKERS: OnceLock<Mutex<BTreeMap<BackendInstanceId, AssignWorker>>> = OnceLock::new();

fn assign_worker_registry() -> &'static Mutex<BTreeMap<BackendInstanceId, AssignWorker>> {
    ASSIGN_WORKERS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn spawn_assign_worker(
    backend_id: BackendInstanceId,
    backend: &'static str,
) -> Option<AssignWorker> {
    let (cmd_tx, cmd_rx) = unbounded::<AssignWorkerCommand>();
    let thread_name = format!("seine-assign-{backend}-{backend_id}");
    let spawn_result = thread::Builder::new().name(thread_name).spawn(move || {
        while let Ok(command) = cmd_rx.recv() {
            match command {
                AssignWorkerCommand::Run {
                    task,
                    timeout,
                    outcome_tx,
                } => run_assignment_task(task, timeout, outcome_tx),
            }
        }
    });

    if spawn_result.is_err() {
        return None;
    }
    Some(AssignWorker { tx: cmd_tx })
}

fn worker_sender_for_backend(
    backend_id: BackendInstanceId,
    backend: &'static str,
) -> Option<Sender<AssignWorkerCommand>> {
    let mut registry = assign_worker_registry().lock().ok()?;
    if let Some(worker) = registry.get(&backend_id) {
        return Some(worker.tx.clone());
    }
    let worker = spawn_assign_worker(backend_id, backend)?;
    let sender = worker.tx.clone();
    registry.insert(backend_id, worker);
    Some(sender)
}

fn dispatch_to_assign_worker(
    task: DispatchTask,
    timeout: Duration,
    outcome_tx: Sender<DispatchOutcome>,
) -> std::result::Result<(), DispatchTask> {
    let backend_id = task.slot.id;
    let backend = task.slot.backend.name();
    let Some(worker_tx) = worker_sender_for_backend(backend_id, backend) else {
        return Err(task);
    };

    let command = AssignWorkerCommand::Run {
        task,
        timeout,
        outcome_tx,
    };
    match worker_tx.send(command) {
        Ok(()) => Ok(()),
        Err(send_err) => {
            if let Ok(mut registry) = assign_worker_registry().lock() {
                registry.remove(&backend_id);
            }
            match send_err.0 {
                AssignWorkerCommand::Run { task, .. } => Err(task),
            }
        }
    }
}

pub(super) fn distribute_work(
    backends: &mut Vec<BackendSlot>,
    options: DistributeWorkOptions<'_>,
) -> Result<u64> {
    let mut attempt_start_nonce = options.reservation.start_nonce;
    let mut total_span_consumed = 0u64;
    loop {
        if backends.is_empty() {
            bail!("all mining backends are unavailable");
        }

        let nonce_counts = compute_backend_nonce_counts(
            backends,
            options.reservation.max_iters_per_lane,
            options.backend_weights,
        );
        let total_lanes = total_lanes(backends);
        let attempt_span = nonce_counts.iter().copied().sum::<u64>().max(total_lanes);
        total_span_consumed = total_span_consumed.wrapping_add(attempt_span);
        let template = Arc::new(WorkTemplate {
            work_id: options.work_id,
            epoch: options.epoch,
            header_base: Arc::clone(&options.header_base),
            target: options.target,
            stop_at: options.stop_at,
        });

        let mut chunk_start = attempt_start_nonce;
        let mut dispatch_tasks = Vec::with_capacity(backends.len());
        for (idx, slot) in std::mem::take(backends).into_iter().enumerate() {
            let nonce_count = *nonce_counts.get(idx).unwrap_or(&slot.lanes.max(1));
            let batch = build_assignment_batch(
                Arc::clone(&template),
                chunk_start,
                nonce_count,
                backend_capabilities(&slot).max_inflight_assignments,
            );

            dispatch_tasks.push(DispatchTask { idx, slot, batch });
            chunk_start = chunk_start.wrapping_add(nonce_count);
        }

        let (survivors, failures) =
            dispatch_assignment_tasks(dispatch_tasks, options.assignment_timeout);
        *backends = survivors;

        if failures.is_empty() {
            return Ok(total_span_consumed.saturating_sub(options.reservation.reserved_span));
        }

        for failure in failures {
            warn(
                "BACKEND",
                format!(
                    "{}#{} failed work assignment: {}",
                    failure.backend, failure.backend_id, failure.reason
                ),
            );
            warn(
                "BACKEND",
                format!(
                    "quarantined {}#{} due to assignment failure",
                    failure.backend, failure.backend_id
                ),
            );
        }

        if backends.is_empty() {
            bail!("all mining backends are unavailable after assignment failure");
        }

        attempt_start_nonce = attempt_start_nonce.wrapping_add(attempt_span);
        warn(
            "BACKEND",
            format!(
                "retrying work assignment with remaining={} start_nonce={}",
                backend_names(backends),
                attempt_start_nonce
            ),
        );
    }
}

fn dispatch_assignment_tasks(
    tasks: Vec<DispatchTask>,
    timeout: Duration,
) -> (Vec<BackendSlot>, Vec<DispatchFailure>) {
    if tasks.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let timeout = timeout.max(Duration::from_millis(1));
    let expected = tasks.len();
    let mut metadata_by_idx = vec![(0u64, "unknown"); expected];
    let mut outcomes: Vec<Option<DispatchOutcome>> =
        std::iter::repeat_with(|| None).take(expected).collect();
    let (outcome_tx, outcome_rx) = bounded::<DispatchOutcome>(expected.max(1));

    for task in tasks {
        let idx = task.idx;
        let backend_id = task.slot.id;
        let backend = task.slot.backend.name();
        if idx < metadata_by_idx.len() {
            metadata_by_idx[idx] = (backend_id, backend);
        }
        if let Err(task) = dispatch_to_assign_worker(task, timeout, outcome_tx.clone()) {
            run_assignment_task(task, timeout, outcome_tx.clone());
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

    let mut survivors = Vec::new();
    let mut failures = Vec::new();

    for (idx, outcome_slot) in outcomes.iter_mut().enumerate().take(expected) {
        match outcome_slot.take() {
            Some(mut outcome) => {
                if outcome.elapsed > timeout {
                    outcome.slot.backend.stop();
                    failures.push(DispatchFailure {
                        backend_id: outcome.backend_id,
                        backend: outcome.backend,
                        reason: format!(
                            "assignment timed out after {}ms (limit={}ms)",
                            outcome.elapsed.as_millis(),
                            timeout.as_millis()
                        ),
                    });
                    continue;
                }

                match outcome.result {
                    Ok(()) => survivors.push((idx, outcome.slot)),
                    Err(err) => {
                        outcome.slot.backend.stop();
                        failures.push(DispatchFailure {
                            backend_id: outcome.backend_id,
                            backend: outcome.backend,
                            reason: format!("{err:#}"),
                        });
                    }
                }
            }
            None => {
                let (backend_id, backend) =
                    metadata_by_idx.get(idx).copied().unwrap_or((0, "unknown"));
                failures.push(DispatchFailure {
                    backend_id,
                    backend,
                    reason: format!(
                        "assignment timed out after {}ms; backend detached",
                        timeout.as_millis()
                    ),
                });
            }
        }
    }

    survivors.sort_by_key(|(idx, _)| *idx);
    (
        survivors
            .into_iter()
            .map(|(_, slot)| slot)
            .collect::<Vec<_>>(),
        failures,
    )
}

fn run_assignment_task(
    task: DispatchTask,
    timeout: Duration,
    outcome_tx: crossbeam_channel::Sender<DispatchOutcome>,
) {
    let DispatchTask { idx, slot, batch } = task;
    let backend_id = slot.id;
    let backend = slot.backend.name();
    let started = Instant::now();
    let deadline = started + timeout;
    let result = match panic::catch_unwind(AssertUnwindSafe(|| {
        slot.backend
            .assign_work_batch_with_deadline(&batch, deadline)
    })) {
        Ok(result) => result,
        Err(_) => Err(anyhow!("assignment task panicked")),
    };
    let elapsed = started.elapsed();

    let send_result = outcome_tx.send(DispatchOutcome {
        idx,
        slot,
        backend_id,
        backend,
        elapsed,
        result,
    });
    if let Err(send_err) = send_result {
        let mut dropped = send_err.0;
        dropped.slot.backend.stop();
    }
}

pub(super) fn compute_backend_nonce_counts(
    backends: &[BackendSlot],
    max_iters_per_lane: u64,
    backend_weights: Option<&BTreeMap<BackendInstanceId, f64>>,
) -> Vec<u64> {
    if backends.is_empty() {
        return Vec::new();
    }

    let base_counts: Vec<u64> = backends.iter().map(|slot| slot.lanes.max(1)).collect();
    let preferred_counts: Vec<u64> = backends
        .iter()
        .map(|slot| {
            let lanes = slot.lanes.max(1);
            let iters = backend_iters_per_lane(slot, max_iters_per_lane);
            lanes.saturating_mul(iters).max(lanes)
        })
        .collect();
    if backend_weights.is_none() {
        return preferred_counts;
    }

    let total_lanes = total_lanes(backends);
    let total_span = preferred_counts
        .iter()
        .fold(0u64, |acc, count| acc.saturating_add(*count))
        .max(total_lanes);
    let min_total = base_counts
        .iter()
        .fold(0u64, |acc, count| acc.saturating_add(*count));
    if total_span <= min_total {
        return base_counts;
    }

    let weights_map = backend_weights.expect("weights should be present");
    let mut weights = Vec::with_capacity(backends.len());
    let mut sum_weights = 0.0f64;
    for (idx, slot) in backends.iter().enumerate() {
        let fallback = preferred_counts
            .get(idx)
            .copied()
            .unwrap_or_else(|| slot.lanes.max(1)) as f64;
        let weight = weights_map
            .get(&slot.id)
            .copied()
            .filter(|w| w.is_finite() && *w > 0.0)
            .unwrap_or(fallback);
        sum_weights += weight;
        weights.push(weight);
    }
    if !sum_weights.is_finite() || sum_weights <= 0.0 {
        return base_counts;
    }

    let remaining = total_span - min_total;
    let mut extra = vec![0u64; backends.len()];
    let mut floors = 0u64;
    let mut remainders: Vec<(usize, f64, BackendInstanceId)> = Vec::with_capacity(backends.len());
    for (idx, weight) in weights.iter().enumerate() {
        let raw = (remaining as f64) * (*weight / sum_weights);
        let floor = raw.floor() as u64;
        floors = floors.saturating_add(floor);
        extra[idx] = floor;
        remainders.push((idx, raw - floor as f64, backends[idx].id));
    }

    let mut leftover = remaining.saturating_sub(floors);
    if leftover > 0 {
        remainders.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.2.cmp(&b.2))
        });
        for (idx, _, _) in remainders {
            if leftover == 0 {
                break;
            }
            extra[idx] = extra[idx].saturating_add(1);
            leftover -= 1;
        }
    }

    base_counts
        .iter()
        .zip(extra)
        .map(|(base, add)| base.saturating_add(add))
        .collect()
}

pub(super) fn backend_iters_per_lane(slot: &BackendSlot, default_iters_per_lane: u64) -> u64 {
    let default_iters_per_lane = default_iters_per_lane.max(1);
    backend_capabilities(slot)
        .preferred_iters_per_lane
        .unwrap_or(default_iters_per_lane)
        .max(1)
}

fn build_assignment_batch(
    template: Arc<WorkTemplate>,
    start_nonce: u64,
    nonce_count: u64,
    max_inflight_assignments: u32,
) -> Vec<WorkAssignment> {
    let nonce_count = nonce_count.max(1);
    let parts = (max_inflight_assignments.max(1) as u64).min(nonce_count);
    let chunks = split_nonce_chunks(start_nonce, nonce_count, parts);
    let mut batch = Vec::with_capacity(chunks.len());
    for nonce_chunk in chunks {
        batch.push(WorkAssignment {
            template: Arc::clone(&template),
            nonce_chunk,
        });
    }
    batch
}

fn split_nonce_chunks(start_nonce: u64, nonce_count: u64, parts: u64) -> Vec<NonceChunk> {
    let nonce_count = nonce_count.max(1);
    let parts = parts.max(1).min(nonce_count);
    let base = nonce_count / parts;
    let remainder = nonce_count % parts;

    let mut chunks = Vec::with_capacity(parts as usize);
    let mut next_start = start_nonce;
    for idx in 0..parts {
        let size = base + u64::from(idx < remainder);
        if size == 0 {
            continue;
        }
        chunks.push(NonceChunk {
            start_nonce: next_start,
            nonce_count: size,
        });
        next_start = next_start.wrapping_add(size);
    }

    if chunks.is_empty() {
        chunks.push(NonceChunk {
            start_nonce,
            nonce_count,
        });
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::split_nonce_chunks;

    #[test]
    fn split_nonce_chunks_covers_full_span_without_overlap() {
        let chunks = split_nonce_chunks(100, 10, 3);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].start_nonce, 100);
        assert_eq!(chunks[0].nonce_count, 4);
        assert_eq!(chunks[1].start_nonce, 104);
        assert_eq!(chunks[1].nonce_count, 3);
        assert_eq!(chunks[2].start_nonce, 107);
        assert_eq!(chunks[2].nonce_count, 3);

        let total: u64 = chunks.iter().map(|chunk| chunk.nonce_count).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn split_nonce_chunks_caps_parts_to_nonce_count() {
        let chunks = split_nonce_chunks(5, 2, 8);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].nonce_count, 1);
        assert_eq!(chunks[1].nonce_count, 1);
    }
}
