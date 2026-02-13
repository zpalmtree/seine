use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

use crate::backend::MiningSolution;
use serde_json::Value;

use super::submit::SubmitTemplate;
use super::ui::warn;

pub(super) const RECENT_TEMPLATE_CACHE_MIN: usize = 16;
pub(super) const RECENT_TEMPLATE_CACHE_MAX: usize = 512;
const RECENT_TEMPLATE_CACHE_HEADROOM_ROUNDS: usize = 4;
pub(super) const RECENT_TEMPLATE_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;
pub(super) const RECENT_SUBMITTED_SOLUTIONS_CAPACITY: usize = 4096;
pub(super) const DEFERRED_SOLUTIONS_CAPACITY: usize = 8192;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DeferredQueuePushOutcome {
    pub inserted: bool,
    pub dropped: u64,
}

pub(super) struct RecentTemplateEntry {
    pub(super) epoch: u64,
    pub(super) recorded_at: Instant,
    pub(super) submit_template: SubmitTemplate,
    pub(super) estimated_bytes: usize,
}

#[cfg(test)]
pub(super) fn push_deferred_solution(
    deferred_solutions: &mut Vec<MiningSolution>,
    solution: MiningSolution,
) {
    if deferred_solutions
        .iter()
        .any(|queued| queued.epoch == solution.epoch && queued.nonce == solution.nonce)
    {
        return;
    }

    if deferred_solutions.len() >= DEFERRED_SOLUTIONS_CAPACITY {
        let drop_count = deferred_solutions
            .len()
            .saturating_sub(DEFERRED_SOLUTIONS_CAPACITY)
            .saturating_add(1);
        deferred_solutions.drain(0..drop_count);
        warn(
            "BACKEND",
            format!(
                "deferred solution queue reached capacity ({}); dropped {} oldest queued solution(s)",
                DEFERRED_SOLUTIONS_CAPACITY, drop_count
            ),
        );
    }

    deferred_solutions.push(solution);
}

pub(super) fn push_deferred_solution_indexed(
    deferred_solutions: &mut Vec<MiningSolution>,
    deferred_solution_keys: &mut HashSet<(u64, u64)>,
    solution: MiningSolution,
) -> DeferredQueuePushOutcome {
    let key = (solution.epoch, solution.nonce);
    if !deferred_solution_keys.insert(key) {
        return DeferredQueuePushOutcome::default();
    }

    let mut dropped = 0u64;
    if deferred_solutions.len() >= DEFERRED_SOLUTIONS_CAPACITY {
        let drop_count = deferred_solutions
            .len()
            .saturating_sub(DEFERRED_SOLUTIONS_CAPACITY)
            .saturating_add(1);
        for dropped in deferred_solutions.drain(0..drop_count) {
            deferred_solution_keys.remove(&(dropped.epoch, dropped.nonce));
        }
        warn(
            "BACKEND",
            format!(
                "deferred solution queue reached capacity ({}); dropped {} oldest queued solution(s)",
                DEFERRED_SOLUTIONS_CAPACITY, drop_count
            ),
        );
        dropped = drop_count as u64;
    }

    deferred_solutions.push(solution);
    DeferredQueuePushOutcome {
        inserted: true,
        dropped,
    }
}

#[cfg(test)]
pub(super) fn dedupe_queued_solutions(queued: Vec<MiningSolution>) -> Vec<MiningSolution> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(queued.len());
    for solution in queued {
        if seen.insert((solution.epoch, solution.nonce)) {
            deduped.push(solution);
        }
    }
    deduped
}

#[cfg(test)]
pub(super) fn drop_solution_from_deferred(
    deferred_solutions: &mut Vec<MiningSolution>,
    primary_submitted: Option<&MiningSolution>,
) {
    let Some(solution) = primary_submitted else {
        return;
    };

    deferred_solutions.retain(|candidate| {
        !(candidate.epoch == solution.epoch && candidate.nonce == solution.nonce)
    });
}

pub(super) fn drop_solution_from_deferred_indexed(
    deferred_solutions: &mut Vec<MiningSolution>,
    deferred_solution_keys: &mut HashSet<(u64, u64)>,
    primary_submitted: Option<&MiningSolution>,
) {
    let Some(solution) = primary_submitted else {
        return;
    };
    let key = (solution.epoch, solution.nonce);
    if !deferred_solution_keys.remove(&key) {
        return;
    }
    deferred_solutions.retain(|candidate| (candidate.epoch, candidate.nonce) != key);
}

pub(super) fn take_deferred_solutions_indexed(
    deferred_solutions: &mut Vec<MiningSolution>,
    deferred_solution_keys: &mut HashSet<(u64, u64)>,
) -> Vec<MiningSolution> {
    if deferred_solutions.is_empty() {
        deferred_solution_keys.clear();
        return Vec::new();
    }
    deferred_solution_keys.clear();
    std::mem::take(deferred_solutions)
}

pub(super) fn submit_template_for_solution_epoch(
    current_epoch: u64,
    current_submit_template: &SubmitTemplate,
    recent_templates: &VecDeque<RecentTemplateEntry>,
    solution_epoch: u64,
) -> Option<SubmitTemplate> {
    if solution_epoch == current_epoch {
        return Some(current_submit_template.clone());
    }

    for entry in recent_templates.iter().rev() {
        if entry.epoch == solution_epoch {
            return Some(entry.submit_template.clone());
        }
    }

    None
}

pub(super) fn remember_recent_template(
    recent_templates: &mut VecDeque<RecentTemplateEntry>,
    recent_templates_bytes: &mut usize,
    epoch: u64,
    submit_template: SubmitTemplate,
    retention: Duration,
    max_entries: usize,
    max_bytes: usize,
) {
    let retention = retention.max(Duration::from_millis(1));
    let max_entries = max_entries.max(1);
    let max_bytes = max_bytes.max(1);
    let now = Instant::now();
    let estimated_bytes = submit_template_estimated_bytes(&submit_template).max(1);
    recent_templates.push_back(RecentTemplateEntry {
        epoch,
        recorded_at: now,
        submit_template,
        estimated_bytes,
    });
    *recent_templates_bytes = recent_templates_bytes.saturating_add(estimated_bytes);

    while let Some(front) = recent_templates.front() {
        let over_capacity = recent_templates.len() > max_entries;
        let over_bytes = *recent_templates_bytes > max_bytes;
        let stale_by_age = now.saturating_duration_since(front.recorded_at) > retention;
        if !over_capacity && !stale_by_age && !over_bytes {
            break;
        }
        if let Some(removed) = recent_templates.pop_front() {
            *recent_templates_bytes =
                recent_templates_bytes.saturating_sub(removed.estimated_bytes);
        }
    }
}

fn submit_template_estimated_bytes(template: &SubmitTemplate) -> usize {
    match template {
        SubmitTemplate::Compact { template_id } => template_id.len().saturating_add(32),
        SubmitTemplate::FullBlock { block } => estimate_template_block_bytes(block.as_ref()),
    }
}

fn estimate_template_block_bytes(block: &crate::types::TemplateBlock) -> usize {
    let mut bytes = 128usize;
    bytes = bytes.saturating_add(
        estimate_optional_u64(block.header.nonce)
            .saturating_add(estimate_optional_u64(block.header.nonce_upper))
            .saturating_add(estimate_optional_u64(block.header.height))
            .saturating_add(estimate_optional_u64(block.header.height_upper))
            .saturating_add(estimate_optional_u64(block.header.difficulty))
            .saturating_add(estimate_optional_u64(block.header.difficulty_upper)),
    );
    bytes = bytes.saturating_add(estimate_json_map_bytes(&block.header.extra));
    bytes.saturating_add(estimate_json_map_bytes(&block.extra))
}

fn estimate_optional_u64(value: Option<u64>) -> usize {
    value.map_or(0, |v| v.to_string().len().saturating_add(8))
}

fn estimate_json_map_bytes(map: &serde_json::Map<String, Value>) -> usize {
    let mut bytes = 2usize;
    for (key, value) in map {
        bytes = bytes
            .saturating_add(key.len())
            .saturating_add(3)
            .saturating_add(estimate_json_value_bytes(value));
    }
    bytes
}

fn estimate_json_value_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(_) => 5,
        Value::Number(number) => number.to_string().len(),
        Value::String(text) => text.len().saturating_add(2),
        Value::Array(values) => values
            .iter()
            .fold(2usize, |acc, item| {
                acc.saturating_add(estimate_json_value_bytes(item).saturating_add(1))
            })
            .saturating_sub(usize::from(!values.is_empty())),
        Value::Object(map) => estimate_json_map_bytes(map),
    }
}

pub(super) fn already_submitted_solution(
    submitted_solution_keys: &HashSet<(u64, u64)>,
    solution: &MiningSolution,
) -> bool {
    submitted_solution_keys.contains(&(solution.epoch, solution.nonce))
}

pub(super) fn remember_submitted_solution(
    submitted_solution_order: &mut VecDeque<(u64, u64)>,
    submitted_solution_keys: &mut HashSet<(u64, u64)>,
    solution: &MiningSolution,
) {
    let key = (solution.epoch, solution.nonce);
    if !submitted_solution_keys.insert(key) {
        return;
    }

    submitted_solution_order.push_back(key);
    while submitted_solution_order.len() > RECENT_SUBMITTED_SOLUTIONS_CAPACITY {
        if let Some(expired) = submitted_solution_order.pop_front() {
            submitted_solution_keys.remove(&expired);
        }
    }
}

pub(super) fn recent_template_retention_from_timeouts(
    refresh_interval: Duration,
    backend_control_timeout: Duration,
    backend_assign_timeout: Duration,
    prefetch_wait: Duration,
) -> Duration {
    let refresh_interval = refresh_interval.max(Duration::from_millis(1));
    refresh_interval
        .saturating_add(backend_control_timeout)
        .saturating_add(backend_assign_timeout)
        .saturating_add(prefetch_wait)
}

pub(super) fn recent_template_cache_size_from_timeouts(
    refresh_interval: Duration,
    backend_control_timeout: Duration,
    backend_assign_timeout: Duration,
    prefetch_wait: Duration,
) -> usize {
    let refresh_millis = refresh_interval.max(Duration::from_millis(1)).as_millis();
    let grace_millis = backend_control_timeout
        .as_millis()
        .saturating_add(backend_assign_timeout.as_millis())
        .saturating_add(prefetch_wait.as_millis())
        .saturating_add(refresh_millis);
    let rounds_needed = grace_millis
        .saturating_div(refresh_millis)
        .saturating_add(RECENT_TEMPLATE_CACHE_HEADROOM_ROUNDS as u128);
    rounds_needed.clamp(
        RECENT_TEMPLATE_CACHE_MIN as u128,
        RECENT_TEMPLATE_CACHE_MAX as u128,
    ) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn solution(epoch: u64, nonce: u64) -> MiningSolution {
        MiningSolution {
            epoch,
            nonce,
            backend_id: 1,
            backend: "cpu",
        }
    }

    #[test]
    fn indexed_push_is_deduped_in_constant_time() {
        let mut queue = Vec::new();
        let mut keys = HashSet::new();
        let first = push_deferred_solution_indexed(&mut queue, &mut keys, solution(7, 11));
        let duplicate = push_deferred_solution_indexed(&mut queue, &mut keys, solution(7, 11));

        assert_eq!(queue.len(), 1);
        assert_eq!(keys.len(), 1);
        assert!(first.inserted);
        assert!(!duplicate.inserted);
        assert_eq!(duplicate.dropped, 0);
    }

    #[test]
    fn indexed_push_reports_evictions_when_bounded_queue_wraps() {
        let mut queue = Vec::new();
        let mut keys = HashSet::new();

        for idx in 0..DEFERRED_SOLUTIONS_CAPACITY {
            let outcome = push_deferred_solution_indexed(
                &mut queue,
                &mut keys,
                solution(idx as u64, idx as u64),
            );
            assert!(outcome.inserted);
            assert_eq!(outcome.dropped, 0);
        }

        let outcome = push_deferred_solution_indexed(
            &mut queue,
            &mut keys,
            solution(DEFERRED_SOLUTIONS_CAPACITY as u64 + 1, 99),
        );
        assert!(outcome.inserted);
        assert_eq!(outcome.dropped, 1);
        assert_eq!(queue.len(), DEFERRED_SOLUTIONS_CAPACITY);
        assert_eq!(keys.len(), DEFERRED_SOLUTIONS_CAPACITY);
        assert_eq!(queue.first().map(|entry| entry.epoch), Some(1));
    }

    #[test]
    fn indexed_drop_keeps_set_and_queue_in_sync() {
        let mut queue = Vec::new();
        let mut keys = HashSet::new();
        push_deferred_solution_indexed(&mut queue, &mut keys, solution(7, 11));
        let submitted = solution(7, 11);
        drop_solution_from_deferred_indexed(&mut queue, &mut keys, Some(&submitted));

        assert!(queue.is_empty());
        assert!(keys.is_empty());
    }

    #[test]
    fn indexed_take_clears_key_index() {
        let mut queue = Vec::new();
        let mut keys = HashSet::new();
        push_deferred_solution_indexed(&mut queue, &mut keys, solution(1, 1));
        let drained = take_deferred_solutions_indexed(&mut queue, &mut keys);

        assert_eq!(drained.len(), 1);
        assert!(queue.is_empty());
        assert!(keys.is_empty());
    }
}
