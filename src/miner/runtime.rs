use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::config::WorkAllocation;

use super::stats::Stats;
use super::BackendSlot;

const MIN_ADAPTIVE_WEIGHT: f64 = 1e-9;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum RoundEndReason {
    Refresh,
    Solved,
    StaleTip,
    Shutdown,
}

pub(super) fn maybe_print_stats(
    stats: &Stats,
    last_stats_print: &mut Instant,
    stats_interval: Duration,
    enabled: bool,
) {
    if !enabled {
        return;
    }
    if last_stats_print.elapsed() >= stats_interval {
        stats.print();
        *last_stats_print = Instant::now();
    }
}

pub(super) fn seed_backend_weights(backends: &[BackendSlot]) -> BTreeMap<u64, f64> {
    backends
        .iter()
        .map(|slot| (slot.id, slot.lanes.max(1) as f64))
        .collect()
}

pub(super) fn work_distribution_weights(
    mode: WorkAllocation,
    backend_weights: &BTreeMap<u64, f64>,
) -> Option<&BTreeMap<u64, f64>> {
    match mode {
        WorkAllocation::Static => None,
        WorkAllocation::Adaptive => Some(backend_weights),
    }
}

pub(super) struct WeightUpdateInputs<'a> {
    pub backends: &'a [BackendSlot],
    pub round_backend_hashes: &'a BTreeMap<u64, u64>,
    pub round_elapsed_secs: f64,
    pub mode: WorkAllocation,
    pub round_end_reason: RoundEndReason,
    pub refresh_interval: Duration,
}

pub(super) fn update_backend_weights(
    backend_weights: &mut BTreeMap<u64, f64>,
    inputs: WeightUpdateInputs<'_>,
) {
    if inputs.mode == WorkAllocation::Static {
        backend_weights.clear();
        for slot in inputs.backends {
            backend_weights.insert(slot.id, slot.lanes.max(1) as f64);
        }
        return;
    }

    let elapsed = inputs.round_elapsed_secs.max(0.001);
    if elapsed < 0.050 {
        return;
    }

    let base_alpha = match inputs.round_end_reason {
        RoundEndReason::Refresh => 0.35f64,
        RoundEndReason::Solved => 0.18f64,
        RoundEndReason::StaleTip => 0.12f64,
        RoundEndReason::Shutdown => 0.0f64,
    };
    if base_alpha <= 0.0 {
        return;
    }

    // Scale update strength by round coverage to keep short churny rounds useful but bounded.
    let refresh_secs = inputs.refresh_interval.as_secs_f64().max(0.001);
    let coverage = (elapsed / refresh_secs).clamp(0.1, 1.0);
    let alpha = (base_alpha * coverage).clamp(0.02, 0.35);

    backend_weights
        .retain(|backend_id, _| inputs.backends.iter().any(|slot| slot.id == *backend_id));

    for slot in inputs.backends {
        let baseline = slot.lanes.max(1) as f64;
        let prior = backend_weights.get(&slot.id).copied().unwrap_or(baseline);
        let observed_hashes = inputs
            .round_backend_hashes
            .get(&slot.id)
            .copied()
            .unwrap_or(0);
        // Use round wall-clock elapsed time to avoid assignment-boundary attribution skew.
        let observed_secs = elapsed;
        let observed_hps = observed_hashes as f64 / observed_secs;
        let next = if observed_hps > 0.0 {
            ((1.0 - alpha) * prior) + (alpha * observed_hps)
        } else {
            (0.9 * prior) + (0.1 * baseline)
        };
        backend_weights.insert(slot.id, next.max(MIN_ADAPTIVE_WEIGHT));
    }
}
