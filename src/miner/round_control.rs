use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;

use crate::backend::BackendInstanceId;
use crate::config::WorkAllocation;

use super::scheduler::NonceScheduler;
use super::ui::warn;
use super::{
    cancel_backend_slots, distribute_work, total_lanes, BackendSlot, DistributeWorkOptions,
    RuntimeBackendEventAction, RuntimeMode,
};

pub(super) struct TopologyRedistributionOptions<'a> {
    pub epoch: u64,
    pub work_id: u64,
    pub header_base: Arc<[u8]>,
    pub target: [u8; 32],
    pub stop_at: Instant,
    pub mode: RuntimeMode,
    pub work_allocation: WorkAllocation,
    pub reason: &'static str,
    pub backend_weights: Option<&'a BTreeMap<BackendInstanceId, f64>>,
    pub nonce_scheduler: &'a mut NonceScheduler,
    pub backend_executor: &'a super::backend_executor::BackendExecutor,
    pub log_tag: &'static str,
}

pub(super) fn redistribute_for_topology_change(
    backends: &mut Vec<BackendSlot>,
    options: TopologyRedistributionOptions<'_>,
) -> Result<()> {
    if backends.is_empty() {
        return Ok(());
    }

    if super::backends_have_append_assignment_semantics(backends)
        && cancel_backend_slots(backends, options.mode, options.backend_executor)?
            == RuntimeBackendEventAction::TopologyChanged
        && backends.is_empty()
    {
        return Ok(());
    }

    let reservation = options.nonce_scheduler.reserve(total_lanes(backends));
    warn(
        options.log_tag,
        format!(
            "{}; redistributing e={} id={} backends={}",
            options.reason,
            options.epoch,
            options.work_id,
            super::backend_names(backends),
        ),
    );

    let distribution_weights = match options.work_allocation {
        WorkAllocation::Static => None,
        WorkAllocation::Adaptive => options.backend_weights,
    };
    let additional_span = distribute_work(
        backends,
        DistributeWorkOptions {
            epoch: options.epoch,
            work_id: options.work_id,
            header_base: options.header_base,
            target: options.target,
            reservation,
            stop_at: options.stop_at,
            backend_weights: distribution_weights,
        },
        options.backend_executor,
    )?;
    options
        .nonce_scheduler
        .consume_additional_span(additional_span);
    Ok(())
}
