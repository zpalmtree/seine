use std::time::Duration;

/// Hardcoded dev fee wallet address (base58-encoded spend+view pubkeys).
pub const DEV_ADDRESS: &str =
    "SEiNEceuDyfEY3GKDQAYVuK6382K2Ln2gQSv83ySAFkKhraBxhHnTf2P6PsF8CXtreqywg4T1qwBAjQo3L2VYDYWtsbb9";

/// Dev fee percentage of total mining time.
pub const DEV_FEE_PERCENT: f64 = 2.5;
pub const BNTPOOL_DEV_FEE_PERCENT: f64 = 1.0;
const BNTPOOL_HOST: &str = "bntpool.com";

/// Mine for the user this long before the first dev round.
const GRACE_PERIOD: Duration = Duration::from_secs(10 * 60);

pub struct DevFeeTracker {
    fee_fraction: f64,
    total_elapsed: Duration,
    dev_elapsed: Duration,
    is_dev_round: bool,
}

impl DevFeeTracker {
    pub fn new() -> Self {
        Self::with_percent(DEV_FEE_PERCENT)
    }

    pub fn with_percent(percent: f64) -> Self {
        let fee_fraction = (percent / 100.0).clamp(0.0, 1.0);
        Self {
            fee_fraction,
            total_elapsed: Duration::ZERO,
            dev_elapsed: Duration::ZERO,
            is_dev_round: false,
        }
    }

    /// Returns true when dev fee mining is owed.
    /// Dev time owed is computed only from elapsed time after the grace period,
    /// so the miner runs purely for the user during early startup.
    fn should_mine_dev(&self) -> bool {
        if self.fee_fraction <= 0.0 {
            return false;
        }
        if self.total_elapsed < GRACE_PERIOD {
            return false;
        }
        let billable = (self.total_elapsed - GRACE_PERIOD).as_secs_f64();
        let owed = billable * self.fee_fraction;
        self.dev_elapsed.as_secs_f64() < owed
    }

    /// Call at the start of each round. Sets whether this round mines for dev.
    /// Returns true if the mode changed from the previous round.
    pub fn begin_round(&mut self) -> bool {
        let was_dev = self.is_dev_round;
        self.is_dev_round = self.should_mine_dev();
        self.is_dev_round != was_dev
    }

    pub fn disable(&mut self) {
        self.fee_fraction = 0.0;
        self.is_dev_round = false;
    }

    /// Call at the end of each round to accumulate elapsed time.
    pub fn end_round(&mut self, elapsed: Duration) {
        self.total_elapsed += elapsed;
        if self.is_dev_round {
            self.dev_elapsed += elapsed;
        }
    }

    pub fn is_dev_round(&self) -> bool {
        self.is_dev_round
    }

    /// Returns `Some(DEV_ADDRESS)` during dev rounds, `None` during user rounds.
    pub fn address(&self) -> Option<&'static str> {
        if self.is_dev_round {
            Some(DEV_ADDRESS)
        } else {
            None
        }
    }
}

pub fn effective_pool_dev_fee_percent(pool_url: &str) -> f64 {
    if pool_url_targets_bntpool(pool_url) {
        BNTPOOL_DEV_FEE_PERCENT
    } else {
        DEV_FEE_PERCENT
    }
}

fn pool_url_targets_bntpool(pool_url: &str) -> bool {
    let Some(host) = pool_url_host(pool_url) else {
        return false;
    };
    host == BNTPOOL_HOST || host.ends_with(".bntpool.com")
}

fn pool_url_host(pool_url: &str) -> Option<String> {
    let trimmed = pool_url.trim();
    if trimmed.is_empty() {
        return None;
    }
    let rest = trimmed
        .strip_prefix("stratum+tcp://")
        .or_else(|| trimmed.strip_prefix("stratum+ssl://"))
        .or_else(|| trimmed.strip_prefix("stratum+tls://"))
        .unwrap_or(trimmed);
    let authority = rest.split('/').next().unwrap_or(rest).trim();
    if authority.is_empty() {
        return None;
    }

    let host = if authority.starts_with('[') {
        let close_idx = authority.find(']')?;
        authority[..=close_idx].to_string()
    } else {
        authority
            .split(':')
            .next()
            .unwrap_or(authority)
            .trim()
            .to_string()
    };

    let host = host.trim_matches(['[', ']']).trim().to_ascii_lowercase();
    if host.is_empty() {
        None
    } else {
        Some(host)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_dev_during_grace_period() {
        let mut tracker = DevFeeTracker::new();

        // Simulate many rounds within the grace period
        for _ in 0..20 {
            tracker.begin_round();
            assert!(!tracker.is_dev_round());
            tracker.end_round(Duration::from_secs(20));
        }
        // 400s total, still under 10min grace
        assert!(!tracker.is_dev_round());
    }

    #[test]
    fn dev_triggers_after_grace_period() {
        let mut tracker = DevFeeTracker::new();

        // Exhaust grace period
        tracker.begin_round();
        tracker.end_round(GRACE_PERIOD);

        // One more user round to create billable time
        tracker.begin_round();
        assert!(!tracker.is_dev_round()); // billable=0, owed=0
        tracker.end_round(Duration::from_secs(20));

        // Now billable=20s, owed=0.5s, dev_elapsed=0 => dev round
        tracker.begin_round();
        assert!(tracker.is_dev_round());
    }

    #[test]
    fn returns_to_user_after_dev_round() {
        let mut tracker = DevFeeTracker::new();

        // Past grace period + one user round
        tracker.begin_round();
        tracker.end_round(GRACE_PERIOD + Duration::from_secs(20));

        // Dev round
        tracker.begin_round();
        assert!(tracker.is_dev_round());
        tracker.end_round(Duration::from_secs(20));

        // dev_elapsed=20 >> owed => back to user
        tracker.begin_round();
        assert!(!tracker.is_dev_round());
    }

    #[test]
    fn begin_round_reports_mode_change() {
        let mut tracker = DevFeeTracker::new();

        // Past grace + user round
        tracker.begin_round();
        tracker.end_round(GRACE_PERIOD + Duration::from_secs(20));

        let changed = tracker.begin_round();
        assert!(changed); // switched to dev
    }

    #[test]
    fn address_returns_dev_address_during_dev_round() {
        let mut tracker = DevFeeTracker::new();
        tracker.begin_round();
        assert!(tracker.address().is_none());

        tracker.end_round(GRACE_PERIOD + Duration::from_secs(20));
        tracker.begin_round();
        assert_eq!(tracker.address(), Some(DEV_ADDRESS));
    }

    #[test]
    fn disable_turns_off_current_and_future_dev_rounds() {
        let mut tracker = DevFeeTracker::new();
        tracker.end_round(GRACE_PERIOD + Duration::from_secs(20));
        tracker.begin_round();
        assert!(tracker.is_dev_round());

        tracker.disable();
        assert!(!tracker.is_dev_round());
        assert!(tracker.address().is_none());

        let changed = tracker.begin_round();
        assert!(!changed);
        assert!(!tracker.is_dev_round());
    }

    #[test]
    fn aggregate_converges_to_fee_percent() {
        let mut tracker = DevFeeTracker::new();
        let round_dur = Duration::from_secs(20);

        // Run 1000 rounds (~5.5 hours)
        for _ in 0..1000 {
            tracker.begin_round();
            tracker.end_round(round_dur);
        }

        let total = tracker.total_elapsed.as_secs_f64();
        let dev = tracker.dev_elapsed.as_secs_f64();
        let billable = (tracker.total_elapsed - GRACE_PERIOD).as_secs_f64();
        let actual_pct = dev / billable * 100.0;

        assert!(total > 0.0);
        // Should be within 0.5% of target (full-round granularity causes small variance)
        assert!(
            (actual_pct - DEV_FEE_PERCENT).abs() < 0.5,
            "expected ~{DEV_FEE_PERCENT}%, got {actual_pct:.2}%"
        );
    }

    #[test]
    fn custom_percent_converges() {
        let mut tracker = DevFeeTracker::with_percent(BNTPOOL_DEV_FEE_PERCENT);
        let round_dur = Duration::from_secs(20);

        for _ in 0..1_000 {
            tracker.begin_round();
            tracker.end_round(round_dur);
        }

        let billable = (tracker.total_elapsed - GRACE_PERIOD).as_secs_f64();
        let actual_pct = tracker.dev_elapsed.as_secs_f64() / billable * 100.0;
        assert!(
            (actual_pct - BNTPOOL_DEV_FEE_PERCENT).abs() < 0.5,
            "expected ~{BNTPOOL_DEV_FEE_PERCENT}%, got {actual_pct:.2}%"
        );
    }

    #[test]
    fn effective_pool_dev_fee_is_discounted_for_bntpool_hosts() {
        assert_eq!(
            effective_pool_dev_fee_percent("stratum+tcp://bntpool.com:3333"),
            BNTPOOL_DEV_FEE_PERCENT
        );
        assert_eq!(
            effective_pool_dev_fee_percent("stratum+tcp://eu.bntpool.com:3333"),
            BNTPOOL_DEV_FEE_PERCENT
        );
        assert_eq!(
            effective_pool_dev_fee_percent("stratum+tcp://BNTPOOL.COM:3333"),
            BNTPOOL_DEV_FEE_PERCENT
        );
        assert_eq!(
            effective_pool_dev_fee_percent("stratum+tcp://example.com:3333"),
            DEV_FEE_PERCENT
        );
    }
}
