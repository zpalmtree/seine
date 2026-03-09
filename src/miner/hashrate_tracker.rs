use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use crate::backend::BackendInstanceId;

const CURRENT_WINDOW_SECS: f64 = 5.0;
const MAX_SAMPLES: usize = 600;
const MIN_WINDOW_SECS: f64 = 2.0;
/// Maximum lookback for bursty devices (e.g. GPU) whose hash count is flat
/// within the standard window.
const DEVICE_MAX_LOOKBACK_SECS: f64 = 60.0;

#[derive(Clone)]
struct HashrateSnapshot {
    time: Instant,
    total: u64,
    per_device: BTreeMap<BackendInstanceId, u64>,
}

#[derive(Clone, Copy)]
struct SessionClockStart {
    time: Instant,
    paused_baseline: Duration,
}

pub struct HashrateTracker {
    samples: VecDeque<HashrateSnapshot>,
    session_start: Option<SessionClockStart>,
    device_session_start: BTreeMap<BackendInstanceId, SessionClockStart>,
    paused_since: Option<Instant>,
    paused_total: Duration,
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
            paused_since: None,
            paused_total: Duration::ZERO,
            completed_rounds_device_hashes: BTreeMap::new(),
            last_round_start: None,
            last_round_device_hashes: BTreeMap::new(),
        }
    }

    pub fn set_paused(&mut self, paused: bool) {
        let now = Instant::now();
        if paused {
            if self.paused_since.is_none() {
                self.paused_since = Some(now);
            }
            return;
        }

        if let Some(paused_since) = self.paused_since.take() {
            self.paused_total += now.duration_since(paused_since);
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
        if self.session_start.is_none() && total_hashes > 0 {
            let start_time = self
                .samples
                .back()
                .and_then(|sample| (sample.total < total_hashes).then_some(sample.time))
                .unwrap_or(now);
            self.session_start = Some(SessionClockStart {
                time: start_time,
                paused_baseline: self.paused_duration_at(start_time),
            });
        }
        for (&id, &hashes) in &cumulative_device {
            if hashes == 0 || self.device_session_start.contains_key(&id) {
                continue;
            }

            let start_time = self
                .samples
                .back()
                .and_then(|sample| {
                    let previous_hashes = sample.per_device.get(&id).copied().unwrap_or(0);
                    (previous_hashes < hashes).then_some(sample.time)
                })
                .unwrap_or(now);

            self.device_session_start.insert(
                id,
                SessionClockStart {
                    time: start_time,
                    paused_baseline: self.paused_duration_at(start_time),
                },
            );
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

    /// Compute hashrates: current uses a rolling window, average uses active (unpaused) time.
    pub fn rates(&self) -> HashrateRates {
        self.rates_with_current_windows(&BTreeMap::new())
    }

    /// Compute hashrates with optional per-device current-rate window overrides.
    ///
    /// Devices omitted from `current_window_secs` keep the default current window.
    pub fn rates_with_current_windows(
        &self,
        current_window_secs: &BTreeMap<BackendInstanceId, f64>,
    ) -> HashrateRates {
        let now = Instant::now();
        let (average_total, average_per_device) = self.session_rates(now);
        let current_per_device = self.window_rates_per_device(now, current_window_secs);
        let current_total = if current_window_secs.is_empty() || current_per_device.is_empty() {
            self.window_rate_total(now, CURRENT_WINDOW_SECS)
        } else {
            current_per_device.values().sum()
        };
        HashrateRates {
            current_total,
            average_total,
            current_per_device,
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
        let dt = self.active_elapsed(now, session_start).as_secs_f64();
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
            let device_dt = self.active_elapsed(now, device_start).as_secs_f64();
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
        current_window_secs: &BTreeMap<BackendInstanceId, f64>,
    ) -> BTreeMap<BackendInstanceId, f64> {
        let latest = match self.samples.back() {
            Some(s) => s,
            None => return BTreeMap::new(),
        };

        let mut rates = BTreeMap::new();
        for &id in latest.per_device.keys() {
            let window_secs = current_window_secs
                .get(&id)
                .copied()
                .unwrap_or(CURRENT_WINDOW_SECS)
                .max(MIN_WINDOW_SECS);
            if let Some(rate) = self.window_rate_for_device(now, latest, id, window_secs) {
                rates.insert(id, rate);
            }
        }
        rates
    }

    fn window_rate_for_device(
        &self,
        now: Instant,
        latest: &HashrateSnapshot,
        id: BackendInstanceId,
        window_secs: f64,
    ) -> Option<f64> {
        let latest_hashes = latest.per_device.get(&id).copied().unwrap_or(0);
        if latest_hashes == 0 {
            return None;
        }

        let cutoff = now - Duration::from_secs_f64(window_secs);
        let oldest = self
            .samples
            .iter()
            .find(|s| s.time >= cutoff)
            .unwrap_or(latest);
        let dt = latest.time.duration_since(oldest.time).as_secs_f64();
        if dt < MIN_WINDOW_SECS {
            return None;
        }

        let oldest_hashes = oldest.per_device.get(&id).copied().unwrap_or(0);
        let dh = latest_hashes.saturating_sub(oldest_hashes);
        if dh > 0 {
            return Some(dh as f64 / dt);
        }

        // Device had no new hashes in the selected window. This is common for
        // bursty accelerators that only publish work completion after a deeper
        // launch finishes, so extend the lookback to avoid showing 0 H/s.
        self.extended_device_rate(now, latest, id)
    }

    /// Walk backwards through the sample buffer for `id` to find the most
    /// recent snapshot where its cumulative hash count was lower than in
    /// `latest`.  Returns the amortized rate from that point up to `now`.
    fn extended_device_rate(
        &self,
        now: Instant,
        latest: &HashrateSnapshot,
        id: BackendInstanceId,
    ) -> Option<f64> {
        let latest_hashes = latest.per_device.get(&id).copied().unwrap_or(0);
        for sample in self.samples.iter().rev().skip(1) {
            let dt = now.duration_since(sample.time).as_secs_f64();
            if dt > DEVICE_MAX_LOOKBACK_SECS {
                return None;
            }
            let sample_hashes = sample.per_device.get(&id).copied().unwrap_or(0);
            if sample_hashes < latest_hashes && dt >= MIN_WINDOW_SECS {
                let dh = latest_hashes.saturating_sub(sample_hashes);
                return Some(dh as f64 / dt);
            }
        }
        None
    }

    fn paused_duration_at(&self, now: Instant) -> Duration {
        match self.paused_since {
            Some(paused_since) if now >= paused_since => {
                self.paused_total + now.duration_since(paused_since)
            }
            _ => self.paused_total,
        }
    }

    fn active_elapsed(&self, now: Instant, start: SessionClockStart) -> Duration {
        let paused = self.paused_duration_at(now);
        let paused_since_start = paused.saturating_sub(start.paused_baseline);
        now.duration_since(start.time)
            .saturating_sub(paused_since_start)
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

        tracker.record(1, round_start, &device_hashes);
        thread::sleep(Duration::from_millis(2100));
        tracker.record(1000, round_start, &device_hashes);

        let rates = tracker.rates();
        // ~999 hashes / ~2.1s ≈ ~476 H/s, just check it's nonzero
        assert!(rates.current_total > 0.0);
        assert!(rates.average_total > 0.0);
    }

    #[test]
    fn average_excludes_paused_time() {
        let mut tracker = HashrateTracker::new();
        let round_start = Instant::now();
        let mut device_hashes = BTreeMap::new();
        device_hashes.insert(1u64, 100u64);

        tracker.record(100, round_start, &device_hashes);
        thread::sleep(Duration::from_millis(2100));

        device_hashes.insert(1u64, 200u64);
        tracker.record(200, round_start, &device_hashes);
        let baseline_avg = tracker.rates().average_total;
        assert!(baseline_avg > 0.0);

        tracker.set_paused(true);
        thread::sleep(Duration::from_millis(2100));
        tracker.set_paused(false);

        thread::sleep(Duration::from_millis(2100));
        device_hashes.insert(1u64, 400u64);
        tracker.record(400, round_start, &device_hashes);

        let avg_after_pause = tracker.rates().average_total;
        let paused_time_diluted = 400.0 / 6.3;
        assert!(
            avg_after_pause > baseline_avg * 0.85,
            "paused time should not heavily dilute average: baseline={baseline_avg} after={avg_after_pause}"
        );
        assert!(
            avg_after_pause > paused_time_diluted * 1.2,
            "average should stay above paused-time-diluted estimate: diluted={paused_time_diluted} after={avg_after_pause}"
        );
    }

    #[test]
    fn average_anchor_uses_prior_sample_boundary() {
        let mut tracker = HashrateTracker::new();
        let round_start = Instant::now();
        let mut device_hashes = BTreeMap::new();
        device_hashes.insert(1u64, 0u64);

        tracker.record(0, round_start, &device_hashes);
        thread::sleep(Duration::from_millis(1100));

        device_hashes.insert(1u64, 100u64);
        tracker.record(100, round_start, &device_hashes);
        thread::sleep(Duration::from_millis(1100));

        device_hashes.insert(1u64, 200u64);
        tracker.record(200, round_start, &device_hashes);

        let rates = tracker.rates();
        // With boundary anchoring, average should be near 200 hashes / ~2.2s.
        // Without anchoring it would be closer to 200 / ~1.1s (inflated).
        assert!(
            rates.average_total < 130.0,
            "average appears inflated; got {}",
            rates.average_total
        );
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

    #[test]
    fn bursty_device_shows_nonzero_current_rate_between_bursts() {
        // Simulates GPU + CPU: GPU produces hashes in bursts while CPU
        // produces continuously.  Between GPU bursts, the GPU's per-device
        // current rate should use extended lookback instead of showing 0.
        let mut tracker = HashrateTracker::new();
        let round_start = Instant::now();

        // t=0: CPU-only, GPU not yet producing.
        let mut device_hashes = BTreeMap::new();
        device_hashes.insert(1u64, 100u64); // CPU
        device_hashes.insert(2u64, 0u64); // GPU idle
        tracker.record(100, round_start, &device_hashes);

        thread::sleep(Duration::from_millis(1100));

        // t≈1.1s: GPU burst of 500 hashes arrives.
        device_hashes.insert(1u64, 200u64);
        device_hashes.insert(2u64, 500u64);
        tracker.record(700, round_start, &device_hashes);

        thread::sleep(Duration::from_millis(1100));

        // t≈2.2s: CPU progressed, GPU still idle (same cumulative).
        device_hashes.insert(1u64, 300u64);
        // GPU stays at 500
        tracker.record(800, round_start, &device_hashes);

        thread::sleep(Duration::from_millis(1100));

        // t≈3.3s: CPU progressed more, GPU still idle.
        device_hashes.insert(1u64, 400u64);
        tracker.record(900, round_start, &device_hashes);

        thread::sleep(Duration::from_millis(1100));

        // t≈4.4s: CPU progressed more, GPU still idle.
        device_hashes.insert(1u64, 500u64);
        tracker.record(1000, round_start, &device_hashes);

        thread::sleep(Duration::from_millis(1100));

        // t≈5.5s: CPU progressed more, GPU still idle.
        // Now the 5-second window (t≈0.5 to t≈5.5) has GPU flat at 500
        // across most samples.
        device_hashes.insert(1u64, 600u64);
        tracker.record(1100, round_start, &device_hashes);

        let rates = tracker.rates();
        let gpu_current = rates.current_per_device.get(&2u64).copied().unwrap_or(0.0);
        let cpu_current = rates.current_per_device.get(&1u64).copied().unwrap_or(0.0);

        // CPU should show a normal rate from the 5-second window.
        assert!(
            cpu_current > 0.0,
            "CPU current rate should be non-zero, got {cpu_current}"
        );
        // GPU should show a non-zero amortized rate via extended lookback
        // rather than 0.
        assert!(
            gpu_current > 0.0,
            "GPU current rate should be non-zero via extended lookback, got {gpu_current}"
        );
    }

    #[test]
    fn custom_device_window_can_smooth_bursty_current_rate() {
        let now = Instant::now();
        let samples = [
            (12_300_u64, 0_u64),
            (8_200_u64, 40_u64),
            (4_900_u64, 40_u64),
            (100_u64, 80_u64),
            (0_u64, 120_u64),
        ]
        .into_iter()
        .map(|(millis_ago, hashes)| {
            let mut per_device = BTreeMap::new();
            per_device.insert(7_u64, hashes);
            HashrateSnapshot {
                time: now - Duration::from_millis(millis_ago),
                total: hashes,
                per_device,
            }
        })
        .collect();

        let tracker = HashrateTracker {
            samples,
            session_start: None,
            device_session_start: BTreeMap::new(),
            paused_since: None,
            paused_total: Duration::ZERO,
            completed_rounds_device_hashes: BTreeMap::new(),
            last_round_start: None,
            last_round_device_hashes: BTreeMap::new(),
        };

        let raw = tracker.rates();
        let mut current_window_secs = BTreeMap::new();
        current_window_secs.insert(7_u64, 15.0);
        let smoothed = tracker.rates_with_current_windows(&current_window_secs);

        let raw_device = raw.current_per_device.get(&7_u64).copied().unwrap_or(0.0);
        let smooth_device = smoothed
            .current_per_device
            .get(&7_u64)
            .copied()
            .unwrap_or(0.0);

        assert!(
            raw_device > 15.0,
            "expected short window spike, got {raw_device}"
        );
        assert!(
            (smooth_device - 10.0).abs() < 0.3,
            "expected longer window to smooth near 10 H/s, got {smooth_device}"
        );
        assert!(
            (smoothed.current_total - smooth_device).abs() < f64::EPSILON,
            "single-device total should match device current"
        );
    }
}
