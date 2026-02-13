use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use crossbeam_channel::Receiver;

use crate::backend::{BackendEvent, BackendInstanceId};

use super::backend_executor::BackendExecutor;
use super::hash_poll::{next_backend_poll_deadline, BackendPollState};
use super::{collect_round_backend_samples, BackendRoundTelemetry, BackendSlot, MIN_EVENT_WAIT};

pub(super) struct RoundDriverStep {
    pub collected_hashes: u64,
    pub event: Option<BackendEvent>,
}

pub(super) struct RoundDriverInput<'a> {
    pub backends: &'a [BackendSlot],
    pub backend_events: &'a Receiver<BackendEvent>,
    pub backend_executor: &'a BackendExecutor,
    pub configured_hash_poll_interval: Duration,
    pub poll_state: &'a mut BackendPollState,
    pub round_backend_hashes: &'a mut BTreeMap<BackendInstanceId, u64>,
    pub round_backend_telemetry: &'a mut BTreeMap<BackendInstanceId, BackendRoundTelemetry>,
    pub stop_at: Instant,
    pub extra_deadline: Option<Instant>,
}

pub(super) fn drive_round_step(input: RoundDriverInput<'_>) -> Result<RoundDriverStep> {
    let collected_hashes = collect_round_backend_samples(
        input.backends,
        input.backend_executor,
        input.configured_hash_poll_interval,
        input.poll_state,
        input.round_backend_hashes,
        input.round_backend_telemetry,
    );

    let now = Instant::now();
    let next_hash_poll_at = next_backend_poll_deadline(input.poll_state);
    let mut wait_until = input.stop_at.min(next_hash_poll_at);
    if let Some(extra_deadline) = input.extra_deadline {
        wait_until = wait_until.min(extra_deadline);
    }
    let wait_for = wait_until
        .saturating_duration_since(now)
        .max(MIN_EVENT_WAIT);

    let mut event = None;
    crossbeam_channel::select! {
        recv(input.backend_events) -> backend_event => {
            event = Some(backend_event.map_err(|_| anyhow!("backend event channel closed"))?);
        }
        default(wait_for) => {}
    }

    Ok(RoundDriverStep {
        collected_hashes,
        event,
    })
}
