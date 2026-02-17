use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

use crate::backend::BackendInstanceId;

const CURRENT_WINDOW_SECS: f64 = 5.0;
const MAX_SAMPLES: usize = 600;
const MIN_WINDOW_SECS: f64 = 2.0;

#[derive(Clone)]
struct HashrateSnapshot {
    time: Instant,
    total: u64,
    per_device: BTreeMap<BackendInstanceId, u64>,
}

pub struct HashrateTracker {
    samples: VecDeque<HashrateSnapshot>,
    session_start: Option<Instant>,
    device_session_start: BTreeMap<BackendInstanceId, Instant>,
    completed_rounds_device_hashes: BTreeMap<BackendInstanceId, u64>,
    last_round_start: Option<Instant>,
    last_round_device_hashes: BTreeMap<BackendInstanceId, u64>,
}

pub struct HashrateRates {
    pub current_total: f64,
    pub average_total: f64,
    pub current_per_device: BTreeMap<BackendInstanceId, f64>,
    pub average_per_device: BTreeMap<BackendInstanceId, f64>,
}

impl HashrateTracker {
    pub fn new() -> Self {
        Self {
            samples: VecDeque::with_capacity(MAX_SAMPLES),
            session_start: None,
            device_session_start: BTreeMap::new(),
            completed_rounds_device_hashes: BTreeMap::new(),
            last_round_start: None,
            last_round_device_hashes: BTreeMap::new(),
        }
    }

    /// Record a sample. Call once per second from the TUI update loop.
    ///
    /// - `total_hashes`: cumulative session-wide hash count (from Stats).
    /// - `round_start`: the Instant when the current round began.
    /// - `round_device_hashes`: per-device hashes accumulated *this round only*.
    pub fn record(
        &mut self,
        total_hashes: u64,
        round_start: Instant,
        round_device_hashes: &BTreeMap<BackendInstanceId, u64>,
    ) {
        // Detect round transition: if round_start changed, bank the previous round's hashes.
        if let Some(prev_start) = self.last_round_start {
            if round_start != prev_start {
                for (&id, &hashes) in &self.last_round_device_hashes {
                    *self.completed_rounds_device_hashes.entry(id).or_insert(0) += hashes;
                }
            }
        }
        self.last_round_start = Some(round_start);
        self.last_round_device_hashes = round_device_hashes.clone();

        // Build cumulative per-device = completed_rounds + current_round
        let mut cumulative_device = self.completed_rounds_device_hashes.clone();
        for (&id, &hashes) in round_device_hashes {
            *cumulative_device.entry(id).or_insert(0) += hashes;
        }

        let now = Instant::now();
        if self.session_start.is_none() {
            self.session_start = Some(now);
        }
        for (&id, &hashes) in &cumulative_device {
            if hashes > 0 {
                self.device_session_start.entry(id).or_insert(now);
            }
        }

        let snapshot = HashrateSnapshot {
            time: now,
            total: total_hashes,
            per_device: cumulative_device,
        };

        if self.samples.len() >= MAX_SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back(snapshot);
    }

    /// Compute hashrates: current uses a 30s rolling window, average is session-lifetime.
    pub fn rates(&self) -> HashrateRates {
        let now = Instant::now();
        let (average_total, average_per_device) = self.session_rates(now);
        HashrateRates {
            current_total: self.window_rate_total(now, CURRENT_WINDOW_SECS),
            average_total,
            current_per_device: self.window_rates_per_device(now, CURRENT_WINDOW_SECS),
            average_per_device,
        }
    }

    fn session_rates(&self, now: Instant) -> (f64, BTreeMap<BackendInstanceId, f64>) {
        let latest = match self.samples.back() {
            Some(s) => s,
            None => return (0.0, BTreeMap::new()),
        };
        let session_start = match self.session_start {
            Some(t) => t,
            None => return (0.0, BTreeMap::new()),
        };
        let dt = now.duration_since(session_start).as_secs_f64();
        if dt < MIN_WINDOW_SECS {
            return (0.0, BTreeMap::new());
        }

        let total = latest.total as f64 / dt;

        let mut per_device = BTreeMap::new();
        for (&id, &hashes) in &latest.per_device {
            let device_start = self
                .device_session_start
                .get(&id)
                .copied()
                .unwrap_or(session_start);
            let device_dt = now.duration_since(device_start).as_secs_f64();
            if device_dt < MIN_WINDOW_SECS {
                continue;
            }
            per_device.insert(id, hashes as f64 / device_dt);
        }

        (total, per_device)
    }

    fn window_rate_total(&self, now: Instant, window_secs: f64) -> f64 {
        let latest = match self.samples.back() {
            Some(s) => s,
            None => return 0.0,
        };
        let cutoff = now - std::time::Duration::from_secs_f64(window_secs);
        let oldest = self
            .samples
            .iter()
            .find(|s| s.time >= cutoff)
            .unwrap_or(latest);

        let dt = latest.time.duration_since(oldest.time).as_secs_f64();
        if dt < MIN_WINDOW_SECS {
            return 0.0;
        }
        let dh = latest.total.saturating_sub(oldest.total);
        dh as f64 / dt
    }

    fn window_rates_per_device(
        &self,
        now: Instant,
        window_secs: f64,
    ) -> BTreeMap<BackendInstanceId, f64> {
        let latest = match self.samples.back() {
            Some(s) => s,
            None => return BTreeMap::new(),
        };
        let cutoff = now - std::time::Duration::from_secs_f64(window_secs);
        let oldest = self
            .samples
            .iter()
            .find(|s| s.time >= cutoff)
            .unwrap_or(latest);

        let dt = latest.time.duration_since(oldest.time).as_secs_f64();
        if dt < MIN_WINDOW_SECS {
            return BTreeMap::new();
        }

        let mut rates = BTreeMap::new();
        for (&id, &latest_hashes) in &latest.per_device {
            let oldest_hashes = oldest.per_device.get(&id).copied().unwrap_or(0);
            let dh = latest_hashes.saturating_sub(oldest_hashes);
            rates.insert(id, dh as f64 / dt);
        }
        rates
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn returns_zero_before_enough_samples() {
        let tracker = HashrateTracker::new();
        let rates = tracker.rates();
        assert_eq!(rates.current_total, 0.0);
        assert_eq!(rates.average_total, 0.0);
    }

    #[test]
    fn tracks_round_transitions() {
        let mut tracker = HashrateTracker::new();
        let start1 = Instant::now();
        let mut device_hashes = BTreeMap::new();
        device_hashes.insert(1u64, 100u64);

        tracker.record(100, start1, &device_hashes);

        // Simulate round transition
        let start2 = Instant::now();
        let mut device_hashes2 = BTreeMap::new();
        device_hashes2.insert(1u64, 50u64);

        tracker.record(150, start2, &device_hashes2);

        // After round transition, completed_rounds should have 100 from round 1
        assert_eq!(
            tracker.completed_rounds_device_hashes.get(&1u64).copied(),
            Some(100)
        );
    }

    #[test]
    fn computes_rate_with_enough_data() {
        let mut tracker = HashrateTracker::new();
        let round_start = Instant::now();
        let device_hashes = BTreeMap::new();

        tracker.record(0, round_start, &device_hashes);
        thread::sleep(Duration::from_millis(2100));
        tracker.record(1000, round_start, &device_hashes);

        let rates = tracker.rates();
        // ~1000 hashes / ~2.1s ≈ ~476 H/s, just check it's nonzero
        assert!(rates.current_total > 0.0);
        assert!(rates.average_total > 0.0);
    }

    #[test]
    fn late_joining_device_average_is_not_diluted_by_session_start() {
        let mut tracker = HashrateTracker::new();
        let round_start = Instant::now();

        // Establish miner session start with CPU-only hashes.
        let mut hashes = BTreeMap::new();
        hashes.insert(1u64, 200u64);
        tracker.record(200, round_start, &hashes);
        thread::sleep(Duration::from_millis(2100));
        tracker.record(400, round_start, &hashes);
        let baseline = tracker.rates();
        let cpu_avg = baseline
            .average_per_device
            .get(&1u64)
            .copied()
            .unwrap_or(0.0);
        assert!(cpu_avg > 0.0);

        // NVIDIA appears later with a burst of hashes.
        thread::sleep(Duration::from_millis(2100));
        hashes.insert(2u64, 200u64);
        tracker.record(600, round_start, &hashes);
        thread::sleep(Duration::from_millis(2100));
        hashes.insert(2u64, 400u64);
        tracker.record(800, round_start, &hashes);

        let rates = tracker.rates();
        let nvidia_avg = rates.average_per_device.get(&2u64).copied().unwrap_or(0.0);
        let cpu_avg_after = rates.average_per_device.get(&1u64).copied().unwrap_or(0.0);

        assert!(nvidia_avg > 70.0);
        assert!(cpu_avg_after > 0.0);
    }
}
