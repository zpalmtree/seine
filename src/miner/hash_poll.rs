use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::backend::{BackendInstanceId, BackendTelemetry};

use super::{backend_capabilities, BackendSlot};

pub(super) type BackendPollState = BTreeMap<BackendInstanceId, (Duration, Instant)>;

pub(super) struct BackendPollSample<'a> {
    pub slot: &'a BackendSlot,
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

pub(super) fn collect_due_backend_samples<'a, F>(
    backends: &'a [BackendSlot],
    configured: Duration,
    poll_state: &mut BackendPollState,
    mut on_sample: F,
) where
    F: FnMut(BackendPollSample<'a>),
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
            slot,
            hashes: slot.backend.take_hashes(),
            telemetry: slot.backend.take_telemetry(),
        });
        *next_poll = now + *interval;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use anyhow::Result;
    use crossbeam_channel::Sender;

    use crate::backend::{
        BackendCapabilities, BackendEvent, BackendInstanceId, PowBackend, WorkAssignment,
    };

    struct MockBackend {
        hashes: AtomicU64,
        preferred_hash_poll_interval: Option<Duration>,
    }

    impl MockBackend {
        fn new(hashes: u64, preferred_hash_poll_interval: Option<Duration>) -> Self {
            Self {
                hashes: AtomicU64::new(hashes),
                preferred_hash_poll_interval,
            }
        }
    }

    impl PowBackend for MockBackend {
        fn name(&self) -> &'static str {
            "mock"
        }

        fn lanes(&self) -> usize {
            1
        }

        fn set_instance_id(&self, _id: BackendInstanceId) {}

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            Ok(())
        }

        fn stop(&self) {}

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            Ok(())
        }

        fn take_hashes(&self) -> u64 {
            self.hashes.swap(0, Ordering::AcqRel)
        }

        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities {
                preferred_hash_poll_interval: self.preferred_hash_poll_interval,
                ..BackendCapabilities::default()
            }
        }
    }

    fn slot(id: BackendInstanceId, backend: Arc<dyn PowBackend>) -> BackendSlot {
        BackendSlot {
            id,
            backend,
            lanes: 1,
        }
    }

    #[test]
    fn backend_poll_interval_uses_backend_hint_when_lower() {
        let slot = slot(
            1,
            Arc::new(MockBackend::new(0, Some(Duration::from_millis(50)))),
        );

        let interval = backend_poll_interval(&slot, Duration::from_millis(200));
        assert_eq!(interval, Duration::from_millis(50));
    }

    #[test]
    fn collect_due_backend_samples_only_polls_due_backends() {
        let backends = vec![
            slot(1, Arc::new(MockBackend::new(11, None))),
            slot(2, Arc::new(MockBackend::new(22, None))),
        ];
        let mut poll_state = build_backend_poll_state(&backends, Duration::from_millis(200));
        poll_state.insert(
            1,
            (
                Duration::from_millis(200),
                Instant::now() - Duration::from_millis(1),
            ),
        );
        poll_state.insert(
            2,
            (
                Duration::from_millis(200),
                Instant::now() + Duration::from_secs(30),
            ),
        );

        let mut sampled_backend_ids = Vec::new();
        let mut sampled_hashes = 0u64;
        collect_due_backend_samples(
            &backends,
            Duration::from_millis(200),
            &mut poll_state,
            |sample| {
                sampled_backend_ids.push(sample.slot.id);
                sampled_hashes = sampled_hashes.saturating_add(sample.hashes);
            },
        );

        assert_eq!(sampled_backend_ids, vec![1]);
        assert_eq!(sampled_hashes, 11);
    }
}
