use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::ui::info;

#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    pub elapsed_secs: f64,
    pub hashes: u64,
    pub templates: u64,
    pub submitted: u64,
    pub accepted: u64,
    pub hps: f64,
}

pub struct Stats {
    started_at: Instant,
    hashes: AtomicU64,
    templates: AtomicU64,
    submitted: AtomicU64,
    accepted: AtomicU64,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            hashes: AtomicU64::new(0),
            templates: AtomicU64::new(0),
            submitted: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
        }
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

    pub fn bump_accepted(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        let elapsed_secs = self.started_at.elapsed().as_secs_f64().max(0.001);
        let hashes = self.hashes.load(Ordering::Relaxed);
        let templates = self.templates.load(Ordering::Relaxed);
        let submitted = self.submitted.load(Ordering::Relaxed);
        let accepted = self.accepted.load(Ordering::Relaxed);
        let hps = hashes as f64 / elapsed_secs;

        StatsSnapshot {
            elapsed_secs,
            hashes,
            templates,
            submitted,
            accepted,
            hps,
        }
    }

    pub fn print(&self) {
        let snapshot = self.snapshot();

        info(
            "STATS",
            format!(
                "elapsed={:.1}s hashes={} rate={} templates={} submitted={} accepted={}",
                snapshot.elapsed_secs,
                snapshot.hashes,
                format_hashrate(snapshot.hps),
                snapshot.templates,
                snapshot.submitted,
                snapshot.accepted,
            ),
        );
    }
}

pub fn format_hashrate(hps: f64) -> String {
    if hps >= 1_000_000_000.0 {
        return format!("{:.3} GH/s", hps / 1_000_000_000.0);
    }
    if hps >= 1_000_000.0 {
        return format!("{:.3} MH/s", hps / 1_000_000.0);
    }
    if hps >= 1_000.0 {
        return format!("{:.3} KH/s", hps / 1_000.0);
    }
    format!("{hps:.3} H/s")
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
}
