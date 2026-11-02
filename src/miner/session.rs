use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use super::hashrate_tracker::HashrateTracker;
use super::stats::Stats;
use super::tui::TuiState;

/// Lifetime data for one logical mining session. Worker replacements share it;
/// an explicit stop/start or API restart gets a new instance.
pub(crate) struct SessionHistory {
    pub(super) stats: Stats,
    pub(crate) started_at_unix_ms: u64,
    previous_tui: Mutex<Option<TuiState>>,
    hashrate_tracker: Mutex<Option<HashrateTracker>>,
}

impl fmt::Debug for SessionHistory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionHistory")
            .field("stats", &self.stats.snapshot())
            .finish_non_exhaustive()
    }
}

impl SessionHistory {
    pub(crate) fn new() -> Self {
        Self {
            stats: Stats::new(),
            started_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            previous_tui: Mutex::new(None),
            hashrate_tracker: Mutex::new(None),
        }
    }

    pub(super) fn attach_tui(&self, state: &TuiState) {
        let mut previous = self.previous_tui.lock().unwrap_or_else(|e| e.into_inner());
        let mut next = state.lock().unwrap_or_else(|e| e.into_inner());
        let same_state = previous.as_ref().is_some_and(|old| Arc::ptr_eq(old, state));
        // A long-lived dashboard state keeps its log; re-anchor it to this session.
        let shift = self
            .stats
            .started_at()
            .saturating_duration_since(next.started_at)
            .as_secs_f64();
        if !same_state && previous.is_none() {
            for entry in &mut next.log_entries {
                entry.elapsed_secs -= shift;
            }
            next.avg_hashrate = "0.000 H/s".to_string();
            next.block_found_ticks.clear();
        }
        next.started_at = self.stats.started_at();
        let snapshot = self.stats.snapshot();
        next.total_hashes = snapshot.hashes;
        next.templates = snapshot.templates;
        next.submitted = snapshot.submitted;
        next.stale_shares = snapshot.stale_shares;
        next.accepted = snapshot.accepted;
        next.share_retries = snapshot.share_retries;
        if let Some(old) = previous.as_ref().filter(|_| !same_state) {
            let old = old.lock().unwrap_or_else(|e| e.into_inner());
            next.avg_hashrate.clone_from(&old.avg_hashrate);
            next.block_found_ticks.clone_from(&old.block_found_ticks);
            let startup_logs = std::mem::take(&mut next.log_entries);
            next.log_entries.clone_from(&old.log_entries);
            for mut entry in startup_logs {
                entry.elapsed_secs = snapshot.elapsed_secs;
                next.push_log(entry);
            }
        }
        *previous = Some(Arc::clone(state));
    }

    pub(super) fn take_hashrate_tracker(&self) -> HashrateTracker {
        self.hashrate_tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .unwrap_or_else(HashrateTracker::new)
    }

    pub(super) fn save_hashrate_tracker(&self, tracker: HashrateTracker) {
        *self
            .hashrate_tracker
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(tracker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::miner::tui::{new_tui_state, LogEntry, LogLevel};

    #[test]
    fn long_lived_dashboard_state_survives_replacement_without_deadlock() {
        let history = SessionHistory::new();
        let state = new_tui_state();
        state
            .lock()
            .unwrap()
            .push_log(LogEntry::new(0.0, LogLevel::Info, "CPU", "startup line"));
        history.attach_tui(&state);
        history.stats.add_hashes(10);
        state.lock().unwrap().avg_hashrate = "9.00 H/s".into();
        history.attach_tui(&state);
        let state = state.lock().unwrap();
        assert_eq!(state.total_hashes, 10);
        assert_eq!(state.avg_hashrate, "9.00 H/s");
        assert_eq!(state.log_entries.len(), 1);
    }

    #[test]
    fn replacement_preserves_totals_uptime_and_history_including_teardown_updates() {
        let history = Arc::new(SessionHistory::new());
        let first = new_tui_state();
        history.attach_tui(&first);
        history.stats.add_hashes(100);
        history.stats.bump_templates();
        history.stats.bump_submitted();
        history.stats.bump_accepted();
        history.stats.bump_stale_shares();
        history.stats.add_deferred(2);
        history.stats.add_dropped(3);
        {
            let mut first = first.lock().unwrap();
            first.avg_hashrate = "12.50 H/s".into();
            first.round_hashrate = "20.00 H/s".into();
            first.push_block_found_tick(5);
            first.push_log(LogEntry {
                elapsed_secs: 5.0,
                level: LogLevel::Mined,
                tag: "MINED".into(),
                message: "accepted block".into(),
                count: 1,
            });
            first.thread_change_pending = true;
        }
        // The old workers may still flush hashes/acceptances after a restart
        // request. Sharing the counters keeps these final updates as well.
        let replacement_history = Arc::clone(&history);
        history.stats.add_hashes(25);
        history.stats.bump_accepted();
        let second = new_tui_state();
        replacement_history.attach_tui(&second);
        {
            let second = second.lock().unwrap();
            assert_eq!(second.started_at, first.lock().unwrap().started_at);
            assert_eq!(second.total_hashes, 125);
            assert_eq!(second.templates, 1);
            assert_eq!(second.submitted, 1);
            assert_eq!(second.accepted, 2);
            assert_eq!(second.stale_shares, 1);
            assert_eq!(second.avg_hashrate, "12.50 H/s");
            assert_eq!(second.round_hashrate, "0.000 H/s");
            assert_eq!(second.block_found_ticks.front(), Some(&5));
            assert_eq!(second.log_entries.back().unwrap().message, "accepted block");
            assert!(!second.thread_change_pending);
        }
        let snapshot = replacement_history.stats.snapshot();
        assert_eq!((snapshot.deferred, snapshot.dropped), (2, 3));
        replacement_history.stats.add_hashes(50);
        let third = new_tui_state();
        replacement_history.attach_tui(&third);
        assert_eq!(third.lock().unwrap().total_hashes, 175);
        assert_eq!(
            third.lock().unwrap().started_at,
            first.lock().unwrap().started_at
        );

        let fresh = SessionHistory::new();
        let fresh_tui = new_tui_state();
        fresh.attach_tui(&fresh_tui);
        assert_eq!(fresh_tui.lock().unwrap().total_hashes, 0);
        assert!(fresh_tui.lock().unwrap().log_entries.is_empty());
    }
}
