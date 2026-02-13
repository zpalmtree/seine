use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};

use crate::backend::{
    AssignmentSemantics, BackendInstanceId, NonceChunk, PowBackend, WorkAssignment, WorkTemplate,
};

use super::backend_executor::{
    self, BackendTask, BackendTaskDispatchResult, BackendTaskKind, BackendTaskTimeoutKind,
};
use super::ui::warn;
use super::{backend_capabilities, backend_names, total_lanes, BackendSlot, DistributeWorkOptions};

const ADAPTIVE_WEIGHT_FLOOR_PER_LANE_FRAC: f64 = 0.01;
const ADAPTIVE_NEW_BACKEND_BOOST_PER_LANE_FRAC: f64 = 0.10;

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
    span_end_offset: u64,
    assignment_timeout: Duration,
    assignment_timeout_strikes: u32,
}

struct DispatchFailure {
    backend_id: BackendInstanceId,
    backend: &'static str,
    reason: String,
    quarantined: bool,
}

pub(super) fn distribute_work(
    backends: &mut Vec<BackendSlot>,
    options: DistributeWorkOptions<'_>,
    backend_executor: &backend_executor::BackendExecutor,
) -> Result<u64> {
    let mut attempt_start_nonce = options.reservation.start_nonce;
    let mut additional_span_consumed = 0u64;
    let mut remaining_reserved_span = options.reservation.reserved_span;
    let mut non_quarantined_retry_used = false;
    loop {
        if backends.is_empty() {
            bail!("all mining backends are unavailable");
        }
        if std::time::Instant::now() >= options.stop_at {
            return Ok(additional_span_consumed);
        }

        let nonce_counts = compute_backend_nonce_counts(
            backends,
            options.reservation.max_iters_per_lane,
            options.backend_weights,
        );
        let total_lanes = total_lanes(backends);
        let attempt_span = nonce_counts.iter().copied().sum::<u64>().max(total_lanes);
        let template = Arc::new(WorkTemplate {
            work_id: options.work_id,
            epoch: options.epoch,
            header_base: Arc::clone(&options.header_base),
            target: options.target,
            stop_at: options.stop_at,
        });

        let mut chunk_start = attempt_start_nonce;
        let mut chunk_offset = 0u64;
        let mut dispatch_tasks = Vec::with_capacity(backends.len());
        let mut slots_by_idx = Vec::with_capacity(backends.len());
        for (idx, slot) in std::mem::take(backends).into_iter().enumerate() {
            let nonce_count = *nonce_counts.get(idx).unwrap_or(&slot.lanes.max(1));
            let capabilities = backend_capabilities(&slot);
            let dispatch_iters_per_lane =
                backend_dispatch_iters_per_lane(&slot, options.reservation.max_iters_per_lane);
            let preferred_dispatch_nonce_count = slot
                .lanes
                .max(1)
                .saturating_mul(dispatch_iters_per_lane)
                .max(slot.lanes.max(1));
            let assignments = build_assignment_batch(
                Arc::clone(&template),
                chunk_start,
                nonce_count,
                capabilities.max_inflight_assignments,
                preferred_dispatch_nonce_count,
            );

            chunk_offset = chunk_offset.saturating_add(nonce_count);
            dispatch_tasks.push(DispatchTask {
                idx,
                backend_id: slot.id,
                backend: slot.backend.name(),
                backend_handle: Arc::clone(&slot.backend),
                assignments,
                span_end_offset: chunk_offset,
                assignment_timeout: slot.runtime_policy.assignment_timeout,
                assignment_timeout_strikes: slot.runtime_policy.assignment_timeout_strikes,
            });
            slots_by_idx.push(Some(slot));
            chunk_start = chunk_start.wrapping_add(nonce_count);
        }

        let (survivors, failures, attempt_consumed_span) =
            dispatch_assignment_tasks(dispatch_tasks, slots_by_idx, backend_executor);
        *backends = survivors;
        backend_executor.prune(backends);

        let covered_by_reserved = remaining_reserved_span.min(attempt_consumed_span);
        remaining_reserved_span = remaining_reserved_span.saturating_sub(covered_by_reserved);
        additional_span_consumed = additional_span_consumed
            .saturating_add(attempt_consumed_span.saturating_sub(covered_by_reserved));

        if failures.is_empty() {
            return Ok(additional_span_consumed);
        }

        let has_non_quarantined_failures = failures.iter().any(|failure| !failure.quarantined);
        for failure in failures {
            warn(
                "BACKEND",
                format!(
                    "{}#{} failed work assignment: {}",
                    failure.backend, failure.backend_id, failure.reason
                ),
            );
            if failure.quarantined {
                warn(
                    "BACKEND",
                    format!(
                        "quarantined {}#{} due to assignment failure",
                        failure.backend, failure.backend_id
                    ),
                );
            } else {
                warn(
                    "BACKEND",
                    format!(
                        "keeping {}#{} active; continuing current assignment window",
                        failure.backend, failure.backend_id
                    ),
                );
            }
        }

        if backends.is_empty() {
            bail!("all mining backends are unavailable after assignment failure");
        }

        if has_non_quarantined_failures {
            if non_quarantined_retry_used || std::time::Instant::now() >= options.stop_at {
                warn(
                    "BACKEND",
                    "assignment timeout without quarantine; deferring redistribution to next round",
                );
                return Ok(additional_span_consumed);
            }

            non_quarantined_retry_used = true;
            attempt_start_nonce = attempt_start_nonce.wrapping_add(attempt_consumed_span);
            warn(
                "BACKEND",
                format!(
                    "assignment timeout without quarantine; redistributing immediately with remaining={} start_nonce={} consumed_span={} attempted_span={}",
                    backend_names(backends),
                    attempt_start_nonce,
                    attempt_consumed_span,
                    attempt_span,
                ),
            );
            continue;
        }

        attempt_start_nonce = attempt_start_nonce.wrapping_add(attempt_consumed_span);
        warn(
            "BACKEND",
            format!(
                "retrying work assignment with remaining={} start_nonce={} consumed_span={} attempted_span={}",
                backend_names(backends),
                attempt_start_nonce,
                attempt_consumed_span,
                attempt_span,
            ),
        );
    }
}

fn dispatch_assignment_tasks(
    tasks: Vec<DispatchTask>,
    mut slots_by_idx: Vec<Option<BackendSlot>>,
    backend_executor: &backend_executor::BackendExecutor,
) -> (Vec<BackendSlot>, Vec<DispatchFailure>, u64) {
    if tasks.is_empty() {
        return (Vec::new(), Vec::new(), 0);
    }

    let max_task_idx = tasks.iter().map(|task| task.idx).max();
    let expected = slots_by_idx
        .len()
        .max(max_task_idx.map_or(0, |idx| idx.saturating_add(1)));
    if slots_by_idx.len() < expected {
        slots_by_idx.resize_with(expected, || None);
    }

    let default_timeout = Duration::from_millis(1);
    let mut span_end_offsets = vec![0u64; expected];
    let mut task_timeouts = vec![default_timeout; expected];
    let mut task_timeout_strikes = vec![1u32; expected];
    let backend_tasks = tasks
        .into_iter()
        .map(|task| {
            let DispatchTask {
                idx,
                backend_id,
                backend,
                backend_handle,
                assignments,
                span_end_offset,
                assignment_timeout,
                assignment_timeout_strikes,
            } = task;
            let timeout = assignment_timeout.max(default_timeout);
            let timeout_strikes = assignment_timeout_strikes.max(1);
            if idx < expected {
                span_end_offsets[idx] = span_end_offset;
                task_timeouts[idx] = timeout;
                task_timeout_strikes[idx] = timeout_strikes;
            }
            BackendTask {
                idx,
                backend_id,
                backend,
                backend_handle,
                kind: match assignments {
                    DispatchAssignments::Single(work) => BackendTaskKind::Assign(work),
                    DispatchAssignments::Batch(batch) => BackendTaskKind::AssignBatch(batch),
                },
                timeout,
            }
        })
        .collect();
    let mut outcomes = backend_executor.dispatch_backend_tasks(backend_tasks);
    if outcomes.len() < expected {
        outcomes.resize_with(expected, || None);
    }

    let mut survivors = Vec::new();
    let mut failures = Vec::new();
    let mut consumed_span = 0u64;

    for (idx, outcome_slot) in outcomes.iter_mut().enumerate().take(expected) {
        let Some(slot) = slots_by_idx.get_mut(idx).and_then(Option::take) else {
            continue;
        };
        let span_end_offset = span_end_offsets.get(idx).copied().unwrap_or(0);
        let timeout = task_timeouts.get(idx).copied().unwrap_or(default_timeout);
        let timeout_strikes = task_timeout_strikes.get(idx).copied().unwrap_or(1);
        let backend_id = slot.id;
        let backend = slot.backend.name();
        match outcome_slot.take() {
            Some(BackendTaskDispatchResult::Completed(Ok(()))) => {
                consumed_span = consumed_span.max(span_end_offset);
                backend_executor.note_assignment_success(backend_id, &slot.backend);
                survivors.push((idx, slot));
            }
            Some(BackendTaskDispatchResult::Completed(Err(err))) => {
                consumed_span = consumed_span.max(span_end_offset);
                backend_executor.quarantine_backend(backend_id, Arc::clone(&slot.backend));
                backend_executor.remove_backend_worker(backend_id, &slot.backend);
                failures.push(DispatchFailure {
                    backend_id,
                    backend,
                    reason: format!("{err:#}"),
                    quarantined: true,
                });
            }
            Some(BackendTaskDispatchResult::TimedOut(BackendTaskTimeoutKind::Execution)) | None => {
                consumed_span = consumed_span.max(span_end_offset);
                let timeout_decision = backend_executor.note_assignment_timeout_with_threshold(
                    backend_id,
                    &slot.backend,
                    timeout_strikes,
                );
                let append_semantics =
                    backend_capabilities(&slot).assignment_semantics == AssignmentSemantics::Append;
                if timeout_decision.should_quarantine || append_semantics {
                    backend_executor.quarantine_backend(backend_id, Arc::clone(&slot.backend));
                    backend_executor.remove_backend_worker(backend_id, &slot.backend);
                    let reason = if append_semantics && !timeout_decision.should_quarantine {
                        format!(
                            "assignment timed out after {}ms (strike {}/{}); append-assignment backend quarantined to prevent queue carry-over",
                            timeout.as_millis(),
                            timeout_decision.strikes,
                            timeout_decision.threshold,
                        )
                    } else {
                        format!(
                            "assignment timed out after {}ms (strike {}/{}); backend quarantined",
                            timeout.as_millis(),
                            timeout_decision.strikes,
                            timeout_decision.threshold,
                        )
                    };
                    failures.push(DispatchFailure {
                        backend_id,
                        backend,
                        reason,
                        quarantined: true,
                    });
                } else {
                    failures.push(DispatchFailure {
                        backend_id,
                        backend,
                        reason: format!(
                            "assignment timed out after {}ms (execution strike {}/{})",
                            timeout.as_millis(),
                            timeout_decision.strikes,
                            timeout_decision.threshold,
                        ),
                        quarantined: false,
                    });
                    survivors.push((idx, slot));
                }
            }
            Some(BackendTaskDispatchResult::TimedOut(BackendTaskTimeoutKind::Enqueue)) => {
                let timeout_decision = backend_executor.note_assignment_timeout_with_threshold(
                    backend_id,
                    &slot.backend,
                    timeout_strikes,
                );
                let append_semantics =
                    backend_capabilities(&slot).assignment_semantics == AssignmentSemantics::Append;
                if timeout_decision.should_quarantine || append_semantics {
                    backend_executor.quarantine_backend(backend_id, Arc::clone(&slot.backend));
                    backend_executor.remove_backend_worker(backend_id, &slot.backend);
                    let reason = if append_semantics && !timeout_decision.should_quarantine {
                        format!(
                            "assignment dispatch queue remained saturated for {}ms before enqueue (strike {}/{}); append-assignment backend quarantined to prevent queue carry-over",
                            timeout.as_millis(),
                            timeout_decision.strikes,
                            timeout_decision.threshold,
                        )
                    } else {
                        format!(
                            "assignment dispatch queue remained saturated for {}ms before enqueue (strike {}/{}); backend quarantined",
                            timeout.as_millis(),
                            timeout_decision.strikes,
                            timeout_decision.threshold,
                        )
                    };
                    failures.push(DispatchFailure {
                        backend_id,
                        backend,
                        reason,
                        quarantined: true,
                    });
                } else {
                    failures.push(DispatchFailure {
                        backend_id,
                        backend,
                        reason: format!(
                            "assignment dispatch queue remained saturated for {}ms before enqueue (enqueue strike {}/{})",
                            timeout.as_millis(),
                            timeout_decision.strikes,
                            timeout_decision.threshold,
                        ),
                        quarantined: false,
                    });
                    survivors.push((idx, slot));
                }
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
        consumed_span,
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
    let mut known_weight_sum = 0.0f64;
    let mut known_lane_sum = 0.0f64;
    for slot in backends {
        if let Some(observed_weight) = weights_map
            .get(&slot.id)
            .copied()
            .filter(|weight| weight.is_finite() && *weight > 0.0)
        {
            known_weight_sum += observed_weight;
            known_lane_sum += slot.lanes.max(1) as f64;
        }
    }
    let per_lane_reference = if known_lane_sum > 0.0 {
        known_weight_sum / known_lane_sum
    } else {
        let fallback_lanes = base_counts.iter().copied().sum::<u64>().max(1) as f64;
        (allocation_counts.iter().copied().sum::<u64>() as f64) / fallback_lanes
    };

    let mut weights = Vec::with_capacity(backends.len());
    let mut sum_weights = 0.0f64;
    for (idx, slot) in backends.iter().enumerate() {
        let lanes = slot.lanes.max(1) as f64;
        let fallback = allocation_counts
            .get(idx)
            .copied()
            .unwrap_or_else(|| slot.lanes.max(1)) as f64;
        let observed = weights_map
            .get(&slot.id)
            .copied()
            .filter(|weight| weight.is_finite() && *weight > 0.0);
        let mut weight = observed.unwrap_or(fallback);
        if per_lane_reference.is_finite() && per_lane_reference > 0.0 {
            // Keep exploration signal alive so newly added/recovered devices can be remeasured.
            let mut exploration_floor =
                lanes * per_lane_reference * ADAPTIVE_WEIGHT_FLOOR_PER_LANE_FRAC;
            if observed.is_none() {
                exploration_floor = exploration_floor
                    .max(lanes * per_lane_reference * ADAPTIVE_NEW_BACKEND_BOOST_PER_LANE_FRAC);
            }
            weight = weight.max(exploration_floor);
        }
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
    preferred_dispatch_nonce_count: u64,
) -> DispatchAssignments {
    let nonce_count = nonce_count.max(1);
    let preferred = preferred_dispatch_nonce_count.max(1);
    let desired_parts = nonce_count
        .saturating_add(preferred.saturating_sub(1))
        .saturating_div(preferred)
        .max(1);
    let parts = desired_parts
        .min(max_inflight_assignments.max(1) as u64)
        .min(nonce_count);
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
