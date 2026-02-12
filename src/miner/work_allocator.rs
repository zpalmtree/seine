use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};

use crate::backend::{BackendInstanceId, NonceChunk, PowBackend, WorkAssignment, WorkTemplate};

use super::backend_executor::{self, BackendTask, BackendTaskKind};
use super::ui::warn;
use super::{backend_capabilities, backend_names, total_lanes, BackendSlot, DistributeWorkOptions};

enum DispatchAssignments {
    Single(WorkAssignment),
    Batch(Vec<WorkAssignment>),
}

struct DispatchTask {
    idx: usize,
    backend_id: BackendInstanceId,
    backend: &'static str,
    backend_handle: Arc<dyn PowBackend>,
    assignments: DispatchAssignments,
}

struct DispatchFailure {
    backend_id: BackendInstanceId,
    backend: &'static str,
    reason: String,
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
        let mut slots_by_idx = Vec::with_capacity(backends.len());
        for (idx, slot) in std::mem::take(backends).into_iter().enumerate() {
            let nonce_count = *nonce_counts.get(idx).unwrap_or(&slot.lanes.max(1));
            let capabilities = backend_capabilities(&slot);
            let assignments = build_assignment_batch(
                Arc::clone(&template),
                chunk_start,
                nonce_count,
                capabilities.max_inflight_assignments,
            );

            dispatch_tasks.push(DispatchTask {
                idx,
                backend_id: slot.id,
                backend: slot.backend.name(),
                backend_handle: Arc::clone(&slot.backend),
                assignments,
            });
            slots_by_idx.push(Some(slot));
            chunk_start = chunk_start.wrapping_add(nonce_count);
        }

        let (survivors, failures) =
            dispatch_assignment_tasks(dispatch_tasks, slots_by_idx, options.assignment_timeout);
        *backends = survivors;
        backend_executor::prune_backend_workers(backends);

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
    mut slots_by_idx: Vec<Option<BackendSlot>>,
    timeout: Duration,
) -> (Vec<BackendSlot>, Vec<DispatchFailure>) {
    if tasks.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let timeout = timeout.max(Duration::from_millis(1));
    let expected = tasks.len().max(slots_by_idx.len());
    if slots_by_idx.len() < expected {
        slots_by_idx.resize_with(expected, || None);
    }
    let backend_tasks = tasks
        .into_iter()
        .map(|task| BackendTask {
            idx: task.idx,
            backend_id: task.backend_id,
            backend: task.backend,
            backend_handle: task.backend_handle,
            kind: match task.assignments {
                DispatchAssignments::Single(work) => BackendTaskKind::Assign(work),
                DispatchAssignments::Batch(batch) => BackendTaskKind::AssignBatch(batch),
            },
        })
        .collect();
    let mut outcomes = backend_executor::dispatch_backend_tasks(backend_tasks, timeout);
    if outcomes.len() < expected {
        outcomes.resize_with(expected, || None);
    }

    let mut survivors = Vec::new();
    let mut failures = Vec::new();

    for (idx, outcome_slot) in outcomes.iter_mut().enumerate().take(expected) {
        let Some(slot) = slots_by_idx.get_mut(idx).and_then(Option::take) else {
            continue;
        };
        let backend_id = slot.id;
        let backend = slot.backend.name();
        match outcome_slot.take() {
            Some(outcome) => match outcome.result {
                Ok(()) => survivors.push((idx, slot)),
                Err(err) => {
                    backend_executor::quarantine_backend(Arc::clone(&slot.backend));
                    backend_executor::remove_backend_worker(backend_id);
                    failures.push(DispatchFailure {
                        backend_id,
                        backend,
                        reason: format!("{err:#}"),
                    });
                }
            },
            None => {
                backend_executor::quarantine_backend(Arc::clone(&slot.backend));
                backend_executor::remove_backend_worker(backend_id);
                failures.push(DispatchFailure {
                    backend_id,
                    backend,
                    reason: format!(
                        "assignment timed out after {}ms; backend quarantined",
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

pub(super) fn compute_backend_nonce_counts(
    backends: &[BackendSlot],
    max_iters_per_lane: u64,
    backend_weights: Option<&BTreeMap<BackendInstanceId, f64>>,
) -> Vec<u64> {
    if backends.is_empty() {
        return Vec::new();
    }

    let base_counts: Vec<u64> = backends.iter().map(|slot| slot.lanes.max(1)).collect();
    let allocation_counts: Vec<u64> = backends
        .iter()
        .map(|slot| {
            let lanes = slot.lanes.max(1);
            let iters = backend_allocation_iters_per_lane(slot, max_iters_per_lane);
            lanes.saturating_mul(iters).max(lanes)
        })
        .collect();
    if backend_weights.is_none() {
        return allocation_counts;
    }

    let total_lanes = total_lanes(backends);
    let total_span = allocation_counts
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
        let fallback = allocation_counts
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
    backend_allocation_iters_per_lane(slot, default_iters_per_lane)
}

pub(super) fn backend_allocation_iters_per_lane(
    slot: &BackendSlot,
    default_iters_per_lane: u64,
) -> u64 {
    let default_iters_per_lane = default_iters_per_lane.max(1);
    backend_capabilities(slot)
        .preferred_allocation_iters_per_lane
        .unwrap_or(default_iters_per_lane)
        .max(1)
}

pub(super) fn backend_dispatch_iters_per_lane(
    slot: &BackendSlot,
    default_iters_per_lane: u64,
) -> u64 {
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
) -> DispatchAssignments {
    let nonce_count = nonce_count.max(1);
    let parts = (max_inflight_assignments.max(1) as u64).min(nonce_count);
    if parts == 1 {
        return DispatchAssignments::Single(WorkAssignment {
            template,
            nonce_chunk: NonceChunk {
                start_nonce,
                nonce_count,
            },
        });
    }

    let chunks = split_nonce_chunks(start_nonce, nonce_count, parts);
    let mut batch = Vec::with_capacity(chunks.len());
    for nonce_chunk in chunks {
        batch.push(WorkAssignment {
            template: Arc::clone(&template),
            nonce_chunk,
        });
    }
    DispatchAssignments::Batch(batch)
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
