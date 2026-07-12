use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use super::ui::{info, warn};

#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    pub elapsed_secs: f64,
    pub hashes: u64,
    pub templates: u64,
    pub submitted: u64,
    pub stale_shares: u64,
    pub accepted: u64,
    pub deferred: u64,
    pub dropped: u64,
    pub hps: f64,
}

pub struct Stats {
    started_at: Instant,
    hashes: AtomicU64,
    templates: AtomicU64,
    submitted: AtomicU64,
    stale_shares: AtomicU64,
    accepted: AtomicU64,
    deferred: AtomicU64,
    dropped: AtomicU64,
    expected_backends: Mutex<Vec<String>>,
    degraded_warning: Mutex<Option<String>>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            hashes: AtomicU64::new(0),
            templates: AtomicU64::new(0),
            submitted: AtomicU64::new(0),
            stale_shares: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            deferred: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            expected_backends: Mutex::new(Vec::new()),
            degraded_warning: Mutex::new(None),
        }
    }

    /// Registers backends that this run is expected to keep active. Labels
    /// already registered are kept; the set only grows (e.g. when a deferred
    /// NVIDIA backend comes online mid-run).
    pub fn register_expected_backends(&self, labels: &[String]) {
        if let Ok(mut expected) = self.expected_backends.lock() {
            for label in labels {
                if !expected.iter().any(|existing| existing == label) {
                    expected.push(label.clone());
                }
            }
        }
    }

    /// Recomputes the recurring degraded-run warning from the currently active
    /// backend labels versus every backend registered as expected.
    pub fn update_backend_degradation(&self, active: &[String]) {
        let warning = self
            .expected_backends
            .lock()
            .ok()
            .and_then(|expected| degraded_backends_warning(active, &expected, "quarantined"));
        if let Ok(mut slot) = self.degraded_warning.lock() {
            *slot = warning;
        }
    }

    pub fn degraded_warning(&self) -> Option<String> {
        self.degraded_warning
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
    }

    pub fn add_hashes(&self, hashes: u64) {
        if hashes > 0 {
            self.hashes.fetch_add(hashes, Ordering::Relaxed);
        }
    }

    pub fn bump_templates(&self) {
        self.templates.fetch_add(1, Ordering::Relaxed);
    }

    pub fn bump_submitted(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn bump_stale_shares(&self) {
        self.stale_shares.fetch_add(1, Ordering::Relaxed);
    }

    pub fn bump_accepted(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_deferred(&self, count: u64) {
        if count > 0 {
            self.deferred.fetch_add(count, Ordering::Relaxed);
        }
    }

    pub fn add_dropped(&self, count: u64) {
        if count > 0 {
            self.dropped.fetch_add(count, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        let elapsed_secs = self.started_at.elapsed().as_secs_f64().max(0.001);
        let hashes = self.hashes.load(Ordering::Relaxed);
        let templates = self.templates.load(Ordering::Relaxed);
        let submitted = self.submitted.load(Ordering::Relaxed);
        let stale_shares = self.stale_shares.load(Ordering::Relaxed);
        let accepted = self.accepted.load(Ordering::Relaxed);
        let deferred = self.deferred.load(Ordering::Relaxed);
        let dropped = self.dropped.load(Ordering::Relaxed);
        let hps = hashes as f64 / elapsed_secs;

        StatsSnapshot {
            elapsed_secs,
            hashes,
            templates,
            submitted,
            stale_shares,
            accepted,
            deferred,
            dropped,
            hps,
        }
    }

    pub fn print(&self) {
        let snapshot = self.snapshot();

        info(
            "STATS",
            format!(
                "elapsed={:.1}s hashes={} rate={} templates={} submitted={} stale={} accepted={} deferred={} dropped={}",
                snapshot.elapsed_secs,
                snapshot.hashes,
                format_hashrate(snapshot.hps),
                snapshot.templates,
                snapshot.submitted,
                snapshot.stale_shares,
                snapshot.accepted,
                snapshot.deferred,
                snapshot.dropped,
            ),
        );
        if let Some(warning) = self.degraded_warning() {
            warn("STATS", warning);
        }
    }
}

/// Formats the prominent degraded-run warning, or `None` when every expected
/// backend is still active. `active` and `expected` are backend labels
/// (matching is exact); `missing_reason` describes why backends are absent,
/// e.g. "quarantined" for the mining runtime or "missing" for benchmarks.
pub fn degraded_backends_warning(
    active: &[String],
    expected: &[String],
    missing_reason: &str,
) -> Option<String> {
    if expected.is_empty() {
        return None;
    }
    let mut remaining = active.to_vec();
    let mut missing = Vec::new();
    for label in expected {
        if let Some(position) = remaining.iter().position(|entry| entry == label) {
            remaining.swap_remove(position);
        } else {
            missing.push(label.clone());
        }
    }
    if missing.is_empty() {
        return None;
    }
    Some(format!(
        "running degraded: {}/{} backends active ({}: {})",
        expected.len() - missing.len(),
        expected.len(),
        missing_reason,
        missing.join(",")
    ))
}

pub fn format_hashrate(hps: f64) -> String {
    format_hashrate_with_decimals(hps, 3)
}

pub fn format_hashrate_ui(hps: f64) -> String {
    format_hashrate_with_decimals(hps, 2)
}

fn format_hashrate_with_decimals(hps: f64, decimals: usize) -> String {
    if hps >= 1_000_000_000.0 {
        return format!("{:.*} GH/s", decimals, hps / 1_000_000_000.0);
    }
    if hps >= 1_000_000.0 {
        return format!("{:.*} MH/s", decimals, hps / 1_000_000.0);
    }
    if hps >= 1_000.0 {
        return format!("{:.*} KH/s", decimals, hps / 1_000.0);
    }
    format!("{hps:.decimals$} H/s")
}

pub fn median(sorted: &[f64]) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_handles_even_and_odd() {
        assert_eq!(median(&[]), 0.0);
        assert_eq!(median(&[5.0]), 5.0);
        assert_eq!(median(&[1.0, 3.0, 5.0]), 3.0);
        assert_eq!(median(&[1.0, 3.0, 5.0, 7.0]), 4.0);
    }

    #[test]
    fn format_hashrate_units() {
        assert_eq!(format_hashrate(5.0), "5.000 H/s");
        assert_eq!(format_hashrate(5_000.0), "5.000 KH/s");
        assert_eq!(format_hashrate(5_000_000.0), "5.000 MH/s");
    }

    #[test]
    fn format_hashrate_ui_units() {
        assert_eq!(format_hashrate_ui(5.0), "5.00 H/s");
        assert_eq!(format_hashrate_ui(5_000.0), "5.00 KH/s");
        assert_eq!(format_hashrate_ui(5_000_000.0), "5.00 MH/s");
    }

    fn labels(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn degraded_backends_warning_is_silent_when_all_expected_backends_are_active() {
        assert_eq!(
            degraded_backends_warning(
                &labels(&["cpu#1", "nvidia#2"]),
                &labels(&["cpu#1", "nvidia#2"]),
                "quarantined",
            ),
            None
        );
        assert_eq!(
            degraded_backends_warning(&labels(&[]), &labels(&[]), "quarantined"),
            None
        );
        // Extra active backends never count as degradation.
        assert_eq!(
            degraded_backends_warning(
                &labels(&["cpu#1", "nvidia#2"]),
                &labels(&["cpu#1"]),
                "quarantined",
            ),
            None
        );
    }

    #[test]
    fn degraded_backends_warning_reports_missing_backends() {
        assert_eq!(
            degraded_backends_warning(
                &labels(&["nvidia#2"]),
                &labels(&["cpu#1", "nvidia#2"]),
                "quarantined",
            )
            .as_deref(),
            Some("running degraded: 1/2 backends active (quarantined: cpu#1)")
        );
        assert_eq!(
            degraded_backends_warning(&labels(&[]), &labels(&["cpu#1", "nvidia#2"]), "quarantined")
                .as_deref(),
            Some("running degraded: 0/2 backends active (quarantined: cpu#1,nvidia#2)")
        );
    }

    #[test]
    fn degraded_backends_warning_handles_duplicate_kind_labels() {
        // Benchmarks compare requested backend kinds, which may repeat.
        assert_eq!(
            degraded_backends_warning(&labels(&["cpu"]), &labels(&["cpu", "cpu"]), "missing")
                .as_deref(),
            Some("running degraded: 1/2 backends active (missing: cpu)")
        );
    }

    #[test]
    fn stats_tracks_degraded_backends_from_expected_set() {
        let stats = Stats::new();
        stats.register_expected_backends(&labels(&["cpu#1", "nvidia#2"]));
        // Re-registering must not duplicate entries.
        stats.register_expected_backends(&labels(&["nvidia#2"]));

        stats.update_backend_degradation(&labels(&["cpu#1", "nvidia#2"]));
        assert_eq!(stats.degraded_warning(), None);

        stats.update_backend_degradation(&labels(&["nvidia#2"]));
        assert_eq!(
            stats.degraded_warning().as_deref(),
            Some("running degraded: 1/2 backends active (quarantined: cpu#1)")
        );

        stats.update_backend_degradation(&labels(&["cpu#1", "nvidia#2"]));
        assert_eq!(stats.degraded_warning(), None);
    }
}
