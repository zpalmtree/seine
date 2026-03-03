use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use crossbeam_channel::Receiver;

use crate::backend::MiningSolution;
use crate::config::{Config, WorkAllocation};
use crate::pool::{PoolClient, PoolEvent, PoolJob};
use crate::types::{decode_hex, difficulty_to_target, parse_target};

use super::mining::MiningRuntimeBackends;
use super::mining_tui::{
    init_tui_display, render_tui_now, set_tui_pending_nvidia, set_tui_state_label,
    set_tui_wallet_overview, update_tui, RoundUiView, TuiDisplay,
};
use super::runtime::{maybe_print_stats, seed_backend_weights, work_distribution_weights};
use super::scheduler::NonceReservation;
use super::stats::Stats;
use super::tui::TuiState;
use super::ui::{error, info, success, warn};
use super::{
    collect_backend_hashes, distribute_work, next_work_id, total_lanes, BackendRoundTelemetry,
    BackendSlot, DistributeWorkOptions, RuntimeMode, TEMPLATE_RETRY_DELAY,
};

const POOL_WAIT_POLL: Duration = Duration::from_millis(200);
const POOL_EVENT_IDLE_SLEEP: Duration = Duration::from_millis(5);
const POOL_JOB_STOP_AT_HORIZON: Duration = Duration::from_secs(365 * 24 * 60 * 60);
const POOL_MAX_PENDING_SUBMITS: usize = 1;

struct ActivePoolJob {
    job: PoolJob,
    header_base: Arc<[u8]>,
    target: [u8; 32],
    share_difficulty: Option<u64>,
    next_nonce: u64,
    epoch: u64,
    height: String,
    round_start: Instant,
    round_hashes: u64,
    round_backend_hashes: BTreeMap<u64, u64>,
    round_backend_telemetry: BTreeMap<u64, BackendRoundTelemetry>,
    submitted_nonces: HashSet<u64>,
    pending_submit_nonces: HashSet<u64>,
}

impl ActivePoolJob {
    fn new(job: PoolJob, epoch: u64, header_base: Arc<[u8]>, target: [u8; 32]) -> Self {
        let next_nonce = job.nonce_start;
        Self {
            height: job.height.to_string(),
            header_base,
            target,
            share_difficulty: None,
            next_nonce,
            job,
            epoch,
            round_start: Instant::now(),
            round_hashes: 0,
            round_backend_hashes: BTreeMap::new(),
            round_backend_telemetry: BTreeMap::new(),
            submitted_nonces: HashSet::new(),
            pending_submit_nonces: HashSet::new(),
        }
    }
}

enum PoolShareSubmitOutcome {
    Submitted,
    Backpressured,
    Duplicate,
    StaleEpoch,
    QueueFailed,
}

pub(super) fn run_pool_mining_loop(
    cfg: &Config,
    shutdown: Arc<AtomicBool>,
    runtime_backends: MiningRuntimeBackends<'_>,
    tui_state: Option<TuiState>,
    deferred_backends: Option<(Receiver<super::BackendSlot>, u64)>,
) -> Result<()> {
    let MiningRuntimeBackends {
        backends,
        backend_events,
        backend_executor,
    } = runtime_backends;
    let (deferred_rx, mut deferred_remaining) = match deferred_backends {
        Some((rx, count)) => (Some(rx), count),
        None => (None, 0),
    };

    let pool_url = cfg
        .pool_url
        .clone()
        .ok_or_else(|| anyhow!("pool mode requires a configured pool URL"))?;
    let address = cfg
        .mining_address
        .clone()
        .ok_or_else(|| anyhow!("pool mode requires a configured address"))?;
    let worker = cfg
        .pool_worker
        .clone()
        .ok_or_else(|| anyhow!("pool mode requires a configured pool worker"))?;

    let stats = Stats::new();
    let mut last_stats_print = Instant::now();
    let mut backend_weights = seed_backend_weights(backends);
    let mut work_id_cursor = 1u64;
    let mut epoch = 0u64;
    let mut last_hash_poll = Instant::now();
    let mut tui = init_tui_display(tui_state, Arc::clone(&shutdown));
    if tui.is_some() {
        super::maybe_warn_linux_hugepages_setup(cfg, RuntimeMode::Mining);
        set_tui_wallet_overview(&mut tui, &address, "---", "---");
        set_tui_state_label(&mut tui, "pool-connecting");
    }

    let pool_client = PoolClient::connect(
        &pool_url,
        address.clone(),
        worker.clone(),
        Arc::clone(&shutdown),
    )?;
    let compact_address = compact_pool_address_for_log(&address);
    info(
        "POOL",
        format!("connecting to {pool_url} as {compact_address}.{worker}"),
    );

    let mut active_job: Option<ActivePoolJob> = None;
    let mut pending_nvidia_logged = false;

    while !shutdown.load(Ordering::Relaxed) {
        if backends.is_empty() {
            bail!("all mining backends are unavailable");
        }

        if let Some(ref deferred) = deferred_rx {
            loop {
                match deferred.try_recv() {
                    Ok(slot) => {
                        deferred_remaining = deferred_remaining.saturating_sub(1);
                        let slot_name = format!("{}#{}", slot.backend.name(), slot.id);
                        info(
                            "BACKEND",
                            format!(
                                "{slot_name}: online (initialized in background, {} lanes)",
                                slot.lanes
                            ),
                        );
                        backend_weights.insert(slot.id, slot.lanes.max(1) as f64);
                        backends.push(slot);
                        if active_job.is_some() {
                            warn("POOL", "new backend will start on the next pool job update");
                        }
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => break,
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        deferred_remaining = 0;
                        break;
                    }
                }
            }
            if deferred_remaining > 0 && !pending_nvidia_logged {
                info(
                    "BACKEND",
                    format!("nvidia initialization in progress: {deferred_remaining} pending"),
                );
                pending_nvidia_logged = true;
            }
            if deferred_remaining == 0 {
                pending_nvidia_logged = false;
            }
            set_tui_pending_nvidia(&mut tui, deferred_remaining);
        }

        let mut processed_pool_event = false;
        for event in pool_client.drain_events() {
            processed_pool_event = true;
            handle_pool_event(
                event,
                cfg,
                &pool_client,
                &mut work_id_cursor,
                &mut epoch,
                backends,
                backend_executor,
                &mut backend_weights,
                &mut active_job,
                &stats,
                &mut tui,
            )?;
        }

        if active_job.is_none() && !processed_pool_event {
            set_tui_state_label(&mut tui, "waiting-pool-job");
            render_tui_now(&mut tui);
            if let Some(event) = pool_client.recv_event_timeout(POOL_WAIT_POLL) {
                handle_pool_event(
                    event,
                    cfg,
                    &pool_client,
                    &mut work_id_cursor,
                    &mut epoch,
                    backends,
                    backend_executor,
                    &mut backend_weights,
                    &mut active_job,
                    &stats,
                    &mut tui,
                )?;
                continue;
            }
        }

        loop {
            match backend_events.try_recv() {
                Ok(event) => {
                    let current_epoch = active_job.as_ref().map(|job| job.epoch).unwrap_or(0);
                    let (action, maybe_solution) = super::handle_runtime_backend_event(
                        event,
                        current_epoch,
                        backends,
                        RuntimeMode::Mining,
                        backend_executor,
                    )?;
                    if action == super::RuntimeBackendEventAction::TopologyChanged {
                        warn(
                            "BACKEND",
                            "backend topology changed; rebalancing on next pool job",
                        );
                    }
                    if let Some(solution) = maybe_solution {
                        let submit_outcome =
                            submit_pool_solution(&pool_client, &mut active_job, &solution, &stats);
                        if matches!(
                            submit_outcome,
                            PoolShareSubmitOutcome::Submitted
                                | PoolShareSubmitOutcome::Backpressured
                        ) {
                            if let Some(job) = active_job.as_mut() {
                                if job.next_nonce <= job.job.nonce_end {
                                    if let Err(err) = assign_pool_continuation(
                                        cfg,
                                        &mut work_id_cursor,
                                        &mut epoch,
                                        backends,
                                        backend_executor,
                                        &mut backend_weights,
                                        job,
                                    ) {
                                        warn(
                                            "POOL",
                                            format!("failed to continue job after share: {err:#}"),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => break,
            }
        }

        if last_hash_poll.elapsed() >= cfg.hash_poll_interval {
            if let Some(active_job) = active_job.as_mut() {
                collect_backend_hashes(
                    backends,
                    backend_executor,
                    Some(&stats),
                    &mut active_job.round_hashes,
                    Some(&mut active_job.round_backend_hashes),
                    Some(&mut active_job.round_backend_telemetry),
                );
                update_tui(
                    &mut tui,
                    &stats,
                    RoundUiView {
                        backends,
                        round_backend_hashes: &active_job.round_backend_hashes,
                        round_start: active_job.round_start,
                        height: &active_job.height,
                        network_hashrate: "---",
                        epoch: active_job.epoch,
                        state_label: "working",
                    },
                );
            }
            last_hash_poll = Instant::now();
        }

        maybe_print_stats(
            &stats,
            &mut last_stats_print,
            cfg.stats_interval,
            tui.is_none(),
        );

        if !processed_pool_event {
            std::thread::sleep(POOL_EVENT_IDLE_SLEEP);
        }
    }

    if !backends.is_empty() {
        info("MINER", "shutting down: cancelling pool work...");
        if let Err(err) =
            super::cancel_backend_slots(backends, RuntimeMode::Mining, backend_executor)
        {
            warn("BACKEND", format!("pool cancel failed: {err:#}"));
        }
        let _ = super::quiesce_backend_slots(backends, RuntimeMode::Mining, backend_executor);
    }

    stats.print();
    info("MINER", "stopped");
    Ok(())
}

fn handle_pool_event(
    event: PoolEvent,
    cfg: &Config,
    pool_client: &PoolClient,
    work_id_cursor: &mut u64,
    epoch_cursor: &mut u64,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
    backend_weights: &mut BTreeMap<u64, f64>,
    active_job: &mut Option<ActivePoolJob>,
    stats: &Stats,
    tui: &mut Option<TuiDisplay>,
) -> Result<()> {
    match event {
        PoolEvent::Connected => {
            success("POOL", "connected");
            set_tui_state_label(tui, "pool-connected");
        }
        PoolEvent::Disconnected(message) => {
            warn("POOL", message);
            set_tui_state_label(tui, "pool-disconnected");
            std::thread::sleep(TEMPLATE_RETRY_DELAY);
        }
        PoolEvent::LoginAccepted(ack) => {
            let required = if ack.required_capabilities.is_empty() {
                "none".to_string()
            } else {
                ack.required_capabilities.join(",")
            };
            success(
                "POOL",
                format!(
                    "login accepted (protocol v{}, required={required})",
                    ack.protocol_version
                ),
            );
            set_tui_state_label(tui, "pool-authenticated");
        }
        PoolEvent::LoginRejected(message) => {
            error("POOL", format!("login rejected: {message}"));
            set_tui_state_label(tui, "pool-login-rejected");
        }
        PoolEvent::SubmitAck(ack) => {
            let current_job_id = active_job.as_ref().map(|job| job.job.job_id.as_str());
            let ack_for_current_job = current_job_id.is_some_and(|job_id| job_id == ack.job_id);
            if ack_for_current_job {
                if let Some(job) = active_job.as_mut() {
                    job.pending_submit_nonces.remove(&ack.nonce);
                }
            }
            if ack.accepted {
                stats.bump_accepted();
                success("POOL", format!("share accepted nonce={}", ack.nonce));
            } else {
                stats.bump_stale_shares();
                let reason = ack.error.as_deref().unwrap_or("unknown");
                warn(
                    "POOL",
                    format!("share rejected nonce={} ({reason})", ack.nonce),
                );
            }
            if let Some(difficulty) = ack.difficulty {
                apply_submit_ack_difficulty(
                    cfg,
                    work_id_cursor,
                    epoch_cursor,
                    backends,
                    backend_executor,
                    backend_weights,
                    active_job,
                    difficulty,
                )?;
            }
        }
        PoolEvent::Job(job) => {
            assign_pool_job(
                cfg,
                pool_client,
                job,
                work_id_cursor,
                epoch_cursor,
                backends,
                backend_executor,
                backend_weights,
                active_job,
                stats,
                tui,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_submit_ack_difficulty(
    cfg: &Config,
    work_id_cursor: &mut u64,
    epoch_cursor: &mut u64,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
    backend_weights: &mut BTreeMap<u64, f64>,
    active_job: &mut Option<ActivePoolJob>,
    difficulty: u64,
) -> Result<()> {
    let Some(job) = active_job.as_mut() else {
        return Ok(());
    };
    let difficulty = difficulty.max(1);
    if job.share_difficulty == Some(difficulty) {
        return Ok(());
    }

    let new_target = difficulty_to_target(difficulty);
    let old_difficulty = job.share_difficulty;
    job.share_difficulty = Some(difficulty);
    if new_target == job.target {
        return Ok(());
    }
    job.target = new_target;
    let old_label = old_difficulty
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    info(
        "POOL",
        format!(
            "applied vardiff update difficulty {old_label} -> {difficulty}; refreshing active work"
        ),
    );
    assign_pool_continuation(
        cfg,
        work_id_cursor,
        epoch_cursor,
        backends,
        backend_executor,
        backend_weights,
        job,
    )
}

#[allow(clippy::too_many_arguments)]
fn assign_pool_job(
    cfg: &Config,
    _pool_client: &PoolClient,
    job: PoolJob,
    work_id_cursor: &mut u64,
    epoch_cursor: &mut u64,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
    backend_weights: &mut BTreeMap<u64, f64>,
    active_job: &mut Option<ActivePoolJob>,
    stats: &Stats,
    tui: &mut Option<TuiDisplay>,
) -> Result<()> {
    let nonce_count = job.nonce_count();
    if nonce_count == 0 {
        warn("POOL", "ignoring job with empty nonce range");
        return Ok(());
    }

    let header_base = match decode_hex(&job.header_base, "pool.header_base") {
        Ok(value) => Arc::<[u8]>::from(value),
        Err(err) => {
            warn("POOL", format!("invalid job header_base: {err:#}"));
            return Ok(());
        }
    };
    let target = match parse_target(&job.target) {
        Ok(target) => target,
        Err(err) => {
            warn("POOL", format!("invalid job target: {err:#}"));
            return Ok(());
        }
    };

    if let Err(err) = super::cancel_backend_slots(backends, RuntimeMode::Mining, backend_executor) {
        warn(
            "BACKEND",
            format!("failed cancelling prior pool work: {err:#}"),
        );
    }

    stats.bump_templates();
    let height = job.height;
    let job_id = job.job_id.clone();
    *active_job = Some(ActivePoolJob::new(job, 0, header_base, target));

    info(
        "POOL",
        format!("new job {job_id} height={height} nonce_count={nonce_count}"),
    );
    if let Some(job) = active_job.as_mut() {
        assign_pool_continuation(
            cfg,
            work_id_cursor,
            epoch_cursor,
            backends,
            backend_executor,
            backend_weights,
            job,
        )?;
    }
    set_tui_state_label(tui, "working");
    render_tui_now(tui);

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn assign_pool_continuation(
    cfg: &Config,
    work_id_cursor: &mut u64,
    epoch_cursor: &mut u64,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
    backend_weights: &mut BTreeMap<u64, f64>,
    active_job: &mut ActivePoolJob,
) -> Result<()> {
    if active_job.next_nonce > active_job.job.nonce_end {
        return Ok(());
    }

    *epoch_cursor = epoch_cursor.wrapping_add(1).max(1);
    let epoch = *epoch_cursor;
    active_job.epoch = epoch;
    let work_id = next_work_id(work_id_cursor);

    let remaining_span = active_job
        .job
        .nonce_end
        .saturating_sub(active_job.next_nonce)
        .saturating_add(1);
    let lanes = total_lanes(backends).max(1);
    let max_iters_per_lane = div_ceil_u64(remaining_span, lanes).max(1);
    let reservation = NonceReservation {
        start_nonce: active_job.next_nonce,
        max_iters_per_lane,
        reserved_span: remaining_span,
    };
    let stop_at = Instant::now() + POOL_JOB_STOP_AT_HORIZON;
    let additional_span = distribute_work(
        backends,
        DistributeWorkOptions {
            epoch,
            work_id,
            header_base: Arc::clone(&active_job.header_base),
            target: active_job.target,
            reservation,
            stop_at,
            backend_weights: match cfg.work_allocation {
                WorkAllocation::Static => None,
                WorkAllocation::Adaptive => {
                    work_distribution_weights(cfg.work_allocation, backend_weights)
                }
            },
        },
        backend_executor,
    )?;
    if additional_span > 0 {
        warn(
            "POOL",
            format!(
                "pool assignment consumed extra nonce span outside reserved window ({} nonces)",
                additional_span
            ),
        );
    }

    Ok(())
}

fn submit_pool_solution(
    pool_client: &PoolClient,
    active_job: &mut Option<ActivePoolJob>,
    solution: &MiningSolution,
    stats: &Stats,
) -> PoolShareSubmitOutcome {
    let Some(job) = active_job.as_mut() else {
        return PoolShareSubmitOutcome::StaleEpoch;
    };
    if solution.epoch != job.epoch {
        return PoolShareSubmitOutcome::StaleEpoch;
    }
    if job.pending_submit_nonces.len() >= POOL_MAX_PENDING_SUBMITS {
        job.next_nonce = job.next_nonce.max(solution.nonce.saturating_add(1));
        return PoolShareSubmitOutcome::Backpressured;
    }
    if !job.submitted_nonces.insert(solution.nonce) {
        return PoolShareSubmitOutcome::Duplicate;
    }

    if pool_client
        .submit_share(job.job.job_id.clone(), solution.nonce, solution.hash)
        .is_ok()
    {
        stats.bump_submitted();
        info("POOL", format!("share submitted nonce={}", solution.nonce));
        job.pending_submit_nonces.insert(solution.nonce);
        job.next_nonce = job.next_nonce.max(solution.nonce.saturating_add(1));
        PoolShareSubmitOutcome::Submitted
    } else {
        job.submitted_nonces.remove(&solution.nonce);
        warn(
            "POOL",
            format!("failed to queue pool submit for nonce={}", solution.nonce),
        );
        PoolShareSubmitOutcome::QueueFailed
    }
}

fn div_ceil_u64(value: u64, divisor: u64) -> u64 {
    let divisor = divisor.max(1);
    (value.saturating_add(divisor - 1)) / divisor
}

fn compact_pool_address_for_log(address: &str) -> String {
    let trimmed = address.trim();
    const KEEP: usize = 6;
    if trimmed.len() <= KEEP * 2 + 3 {
        return trimmed.to_string();
    }
    format!(
        "{}...{}",
        &trimmed[..KEEP],
        &trimmed[trimmed.len() - KEEP..]
    )
}
