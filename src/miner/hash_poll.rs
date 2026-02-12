use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::backend::{BackendInstanceId, BackendTelemetry};

use super::{backend_capabilities, BackendSlot};

pub(super) type BackendPollState = BTreeMap<BackendInstanceId, (Duration, Instant)>;

pub(super) struct BackendPollSample {
    pub backend_id: BackendInstanceId,
    pub hashes: u64,
    pub telemetry: BackendTelemetry,
}

pub(super) fn backend_poll_interval(slot: &BackendSlot, configured: Duration) -> Duration {
    let configured = configured.max(Duration::from_millis(1));
    let hinted = backend_capabilities(slot)
        .preferred_hash_poll_interval
        .filter(|hint| *hint > Duration::from_millis(0))
        .map(|hint| hint.max(Duration::from_millis(1)));
    hinted.map_or(configured, |hint| configured.min(hint))
}

pub(super) fn build_backend_poll_state(
    backends: &[BackendSlot],
    configured: Duration,
) -> BackendPollState {
    let now = Instant::now();
    let mut state = BackendPollState::new();
    for slot in backends {
        let interval = backend_poll_interval(slot, configured);
        state.insert(slot.id, (interval, now + interval));
    }
    state
}

pub(super) fn next_backend_poll_deadline(poll_state: &BackendPollState) -> Instant {
    poll_state
        .values()
        .map(|(_, next_poll)| *next_poll)
        .min()
        .unwrap_or_else(Instant::now)
}

pub(super) fn collect_due_backend_samples<F>(
    backends: &[BackendSlot],
    configured: Duration,
    poll_state: &mut BackendPollState,
    mut on_sample: F,
) where
    F: FnMut(BackendPollSample),
{
    let now = Instant::now();
    for slot in backends {
        let (interval, next_poll) = poll_state.entry(slot.id).or_insert_with(|| {
            let interval = backend_poll_interval(slot, configured);
            (interval, now + interval)
        });
        if now < *next_poll {
            continue;
        }

        on_sample(BackendPollSample {
            backend_id: slot.id,
            hashes: slot.backend.take_hashes(),
            telemetry: slot.backend.take_telemetry(),
        });
        *next_poll = now + *interval;
    }
}
