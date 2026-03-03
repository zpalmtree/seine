use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use crossbeam_channel::Receiver;
use reqwest::blocking::Client as HttpClient;
use serde_json::Value;

use crate::backend::MiningSolution;
use crate::config::{Config, WorkAllocation};
use crate::dev_fee::{DevFeeTracker, DEV_ADDRESS, DEV_FEE_PERCENT};
use crate::pool::{PoolClient, PoolEvent, PoolJob};
use crate::types::{decode_hex, difficulty_to_target, parse_target};

use super::mining::MiningRuntimeBackends;
use super::mining_tui::{
    init_tui_display, render_tui_now, set_tui_dev_fee_active, set_tui_pending_nvidia,
    set_tui_state_label, set_tui_wallet_overview, update_tui, RoundUiView, TuiDisplay,
};
use super::runtime::{maybe_print_stats, seed_backend_weights, work_distribution_weights};
use super::scheduler::NonceReservation;
use super::stats::{format_hashrate_ui, Stats};
use super::tui::TuiState;
use super::ui::{error, info, notify_dev_fee_mode, success, warn};
use super::{
    collect_backend_hashes, distribute_work, next_work_id, total_lanes, BackendRoundTelemetry,
    BackendSlot, DistributeWorkOptions, RuntimeMode, TEMPLATE_RETRY_DELAY,
};

const POOL_WAIT_POLL: Duration = Duration::from_millis(200);
const POOL_EVENT_IDLE_SLEEP: Duration = Duration::from_millis(5);
const POOL_JOB_STOP_AT_HORIZON: Duration = Duration::from_secs(365 * 24 * 60 * 60);
const POOL_MAX_PENDING_SUBMITS: usize = 1;
const POOL_TELEMETRY_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const POOL_TELEMETRY_TIMEOUT: Duration = Duration::from_millis(750);
const POOL_API_DEFAULT_PORT: u16 = 24783;
const ATOMIC_UNITS_PER_BNT: u64 = 100_000_000;
const BNT_DISPLAY_DECIMALS: usize = 4;
const BNT_DISPLAY_SCALE_ATOMIC_UNITS: u64 = 10_000;
const DEV_POOL_URL: &str = "stratum+tcp://localhost:3333";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PoolConnectionMode {
    User,
    Dev,
}

impl PoolConnectionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Dev => "dev",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PoolConnectionConfig {
    mode: PoolConnectionMode,
    pool_url: String,
    address: String,
    worker: String,
}

#[derive(Debug)]
struct PoolUiTelemetryClient {
    stats_url: String,
    miner_url: String,
    http: HttpClient,
}

struct PoolSession {
    connection: PoolConnectionConfig,
    client: PoolClient,
    telemetry: Option<PoolUiTelemetryClient>,
    latest_job: Option<PoolJob>,
    connected: bool,
}

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
        let share_difficulty = job.difficulty;
        Self {
            height: job.height.to_string(),
            header_base,
            target,
            share_difficulty,
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

fn build_pool_connection_configs(
    cfg: &Config,
) -> Result<(PoolConnectionConfig, PoolConnectionConfig)> {
    let user_pool_url = cfg
        .pool_url
        .clone()
        .ok_or_else(|| anyhow!("pool mode requires a configured pool URL"))?;
    let user_address = cfg
        .mining_address
        .clone()
        .ok_or_else(|| anyhow!("pool mode requires a configured address"))?;
    let user_worker = cfg
        .pool_worker
        .clone()
        .ok_or_else(|| anyhow!("pool mode requires a configured pool worker"))?;

    let user = PoolConnectionConfig {
        mode: PoolConnectionMode::User,
        pool_url: user_pool_url,
        address: user_address,
        worker: user_worker,
    };
    let dev = PoolConnectionConfig {
        mode: PoolConnectionMode::Dev,
        pool_url: DEV_POOL_URL.to_string(),
        address: DEV_ADDRESS.to_string(),
        worker: cfg.dev_fee_pool_worker.clone(),
    };
    Ok((user, dev))
}

fn connect_pool_session(
    connection: &PoolConnectionConfig,
    shutdown: Arc<AtomicBool>,
    tui: &mut Option<TuiDisplay>,
    update_ui: bool,
) -> Result<(PoolClient, Option<PoolUiTelemetryClient>)> {
    if update_ui {
        set_tui_state_label(tui, "pool-connecting");
        render_tui_now(tui);
    }

    let pool_client = PoolClient::connect(
        &connection.pool_url,
        connection.address.clone(),
        connection.worker.clone(),
        shutdown,
    )?;
    match connection.mode {
        PoolConnectionMode::Dev => info("CONN", "connecting (dev session)"),
        PoolConnectionMode::User => {
            let compact_address = compact_pool_address_for_log(&connection.address);
            info(
                "CONN",
                format!(
                    "connecting ({}) to {} as {}.{}",
                    connection.mode.as_str(),
                    connection.pool_url,
                    compact_address,
                    connection.worker
                ),
            );
        }
    }
    let telemetry = PoolUiTelemetryClient::new(&connection.pool_url, &connection.address);
    if telemetry.is_none() {
        warn(
            "STATS",
            format!(
                "pool telemetry unavailable for {} session: could not derive pool API URL",
                connection.mode.as_str()
            ),
        );
    }
    if update_ui {
        set_tui_wallet_overview(tui, &connection.address, "---", "---");
    }
    Ok((pool_client, telemetry))
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

    let (user_connection, dev_connection) = build_pool_connection_configs(cfg)?;
    info("DEV FEE", "dev fee session configured");

    let stats = Stats::new();
    let mut last_stats_print = Instant::now();
    let mut backend_weights = seed_backend_weights(backends);
    let mut work_id_cursor = 1u64;
    let mut epoch = 0u64;
    let mut last_hash_poll = Instant::now();
    let mut tui = init_tui_display(tui_state, Arc::clone(&shutdown));
    let mut dev_fee_tracker = DevFeeTracker::new();
    let mut dev_fee_round_started = Instant::now();
    let _ = dev_fee_tracker.begin_round();
    let mut active_mode = if dev_fee_tracker.is_dev_round() {
        PoolConnectionMode::Dev
    } else {
        PoolConnectionMode::User
    };

    if tui.is_some() {
        super::maybe_warn_linux_hugepages_setup(cfg, RuntimeMode::Mining);
        set_tui_dev_fee_active(&mut tui, active_mode == PoolConnectionMode::Dev);
        let active_address = if active_mode == PoolConnectionMode::Dev {
            dev_connection.address.as_str()
        } else {
            user_connection.address.as_str()
        };
        set_tui_wallet_overview(&mut tui, active_address, "---", "---");
    }
    info("MINER", format!("dev fee: {:.1}%", DEV_FEE_PERCENT));

    let (user_client, user_telemetry) = connect_pool_session(
        &user_connection,
        Arc::clone(&shutdown),
        &mut tui,
        active_mode == PoolConnectionMode::User,
    )?;
    let (dev_client, dev_telemetry) = connect_pool_session(
        &dev_connection,
        Arc::clone(&shutdown),
        &mut tui,
        active_mode == PoolConnectionMode::Dev,
    )?;
    let mut user_session = PoolSession {
        connection: user_connection,
        client: user_client,
        telemetry: user_telemetry,
        latest_job: None,
        connected: false,
    };
    let mut dev_session = PoolSession {
        connection: dev_connection,
        client: dev_client,
        telemetry: dev_telemetry,
        latest_job: None,
        connected: false,
    };

    let mut active_job: Option<ActivePoolJob> = None;
    let mut pending_nvidia_logged = false;
    let mut pool_network_hashrate = "unknown".to_string();
    let mut next_pool_telemetry_refresh = Instant::now();
    let mut pool_telemetry_warning_logged = false;

    while !shutdown.load(Ordering::Relaxed) {
        if backends.is_empty() {
            bail!("all mining backends are unavailable");
        }

        if dev_fee_round_started.elapsed() >= cfg.refresh_interval {
            dev_fee_tracker.end_round(dev_fee_round_started.elapsed());
            dev_fee_round_started = Instant::now();

            let mode_changed = dev_fee_tracker.begin_round();
            let next_is_dev_round = dev_fee_tracker.is_dev_round();
            if mode_changed {
                notify_dev_fee_mode(next_is_dev_round);
                set_tui_dev_fee_active(&mut tui, next_is_dev_round);

                let next_mode = if next_is_dev_round {
                    PoolConnectionMode::Dev
                } else {
                    PoolConnectionMode::User
                };
                if next_mode != active_mode {
                    if next_mode == PoolConnectionMode::Dev {
                        for event in dev_session.client.drain_events() {
                            handle_inactive_pool_event(
                                event,
                                PoolConnectionMode::Dev,
                                &mut dev_session.latest_job,
                                &mut dev_session.connected,
                                &stats,
                            );
                        }
                    } else {
                        for event in user_session.client.drain_events() {
                            handle_inactive_pool_event(
                                event,
                                PoolConnectionMode::User,
                                &mut user_session.latest_job,
                                &mut user_session.connected,
                                &stats,
                            );
                        }
                    }
                    info(
                        "CONN",
                        format!(
                            "switching pool session: {} -> {}",
                            active_mode.as_str(),
                            next_mode.as_str()
                        ),
                    );
                    if let Err(err) =
                        super::cancel_backend_slots(backends, RuntimeMode::Mining, backend_executor)
                    {
                        warn(
                            "BACKEND",
                            format!("pool cancel failed during switch: {err:#}"),
                        );
                    }
                    let _ = super::quiesce_backend_slots(
                        backends,
                        RuntimeMode::Mining,
                        backend_executor,
                    );
                    active_job = None;
                    active_mode = next_mode;

                    let (active_address, cached_job) = if active_mode == PoolConnectionMode::Dev {
                        (
                            dev_session.connection.address.clone(),
                            if dev_session.connected {
                                dev_session.latest_job.clone()
                            } else {
                                None
                            },
                        )
                    } else {
                        (
                            user_session.connection.address.clone(),
                            if user_session.connected {
                                user_session.latest_job.clone()
                            } else {
                                None
                            },
                        )
                    };
                    set_tui_wallet_overview(&mut tui, &active_address, "---", "---");

                    if let Some(job) = cached_job {
                        let active_client = if active_mode == PoolConnectionMode::Dev {
                            &dev_session.client
                        } else {
                            &user_session.client
                        };
                        assign_pool_job(
                            cfg,
                            active_client,
                            job,
                            false,
                            &mut work_id_cursor,
                            &mut epoch,
                            backends,
                            backend_executor,
                            &mut backend_weights,
                            &mut active_job,
                            &stats,
                            &mut tui,
                        )?;
                    } else {
                        set_tui_state_label(&mut tui, "waiting-pool-job");
                        render_tui_now(&mut tui);
                    }

                    pool_network_hashrate = "unknown".to_string();
                    next_pool_telemetry_refresh = Instant::now();
                    pool_telemetry_warning_logged = false;
                }
            }
        }

        let (active_address, active_telemetry) = if active_mode == PoolConnectionMode::Dev {
            (
                dev_session.connection.address.as_str(),
                dev_session.telemetry.as_ref(),
            )
        } else {
            (
                user_session.connection.address.as_str(),
                user_session.telemetry.as_ref(),
            )
        };
        if let Some(telemetry) = active_telemetry {
            if Instant::now() >= next_pool_telemetry_refresh {
                let mut errors = Vec::new();
                match telemetry.fetch_pool_hashrate() {
                    Ok(hashrate) => {
                        pool_network_hashrate = hashrate;
                    }
                    Err(err) => {
                        errors.push(format!("global hashrate: {err:#}"));
                    }
                }
                match telemetry.fetch_pool_balances() {
                    Ok((pending, paid)) => {
                        set_tui_wallet_overview(&mut tui, active_address, &pending, &paid);
                    }
                    Err(err) => {
                        errors.push(format!("balance: {err:#}"));
                    }
                }

                if errors.is_empty() {
                    if pool_telemetry_warning_logged {
                        info("STATS", "pool telemetry recovered");
                        pool_telemetry_warning_logged = false;
                    }
                } else if !pool_telemetry_warning_logged {
                    warn("STATS", "failed to fetch pool stats");
                    pool_telemetry_warning_logged = true;
                }

                next_pool_telemetry_refresh = Instant::now() + POOL_TELEMETRY_REFRESH_INTERVAL;
            }
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
                            warn("JOB", "new backend will start on the next pool job update");
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
        for event in user_session.client.drain_events() {
            if active_mode == PoolConnectionMode::User {
                processed_pool_event = true;
                handle_active_pool_event(
                    event,
                    PoolConnectionMode::User,
                    cfg,
                    &user_session.client,
                    &mut work_id_cursor,
                    &mut epoch,
                    backends,
                    backend_executor,
                    &mut backend_weights,
                    &mut active_job,
                    &mut user_session.latest_job,
                    &mut user_session.connected,
                    &stats,
                    &mut tui,
                )?;
            } else {
                handle_inactive_pool_event(
                    event,
                    PoolConnectionMode::User,
                    &mut user_session.latest_job,
                    &mut user_session.connected,
                    &stats,
                );
            }
        }
        for event in dev_session.client.drain_events() {
            if active_mode == PoolConnectionMode::Dev {
                processed_pool_event = true;
                handle_active_pool_event(
                    event,
                    PoolConnectionMode::Dev,
                    cfg,
                    &dev_session.client,
                    &mut work_id_cursor,
                    &mut epoch,
                    backends,
                    backend_executor,
                    &mut backend_weights,
                    &mut active_job,
                    &mut dev_session.latest_job,
                    &mut dev_session.connected,
                    &stats,
                    &mut tui,
                )?;
            } else {
                handle_inactive_pool_event(
                    event,
                    PoolConnectionMode::Dev,
                    &mut dev_session.latest_job,
                    &mut dev_session.connected,
                    &stats,
                );
            }
        }

        if active_job.is_none() && !processed_pool_event {
            set_tui_state_label(&mut tui, "waiting-pool-job");
            render_tui_now(&mut tui);
            if active_mode == PoolConnectionMode::User {
                if let Some(event) = user_session.client.recv_event_timeout(POOL_WAIT_POLL) {
                    handle_active_pool_event(
                        event,
                        PoolConnectionMode::User,
                        cfg,
                        &user_session.client,
                        &mut work_id_cursor,
                        &mut epoch,
                        backends,
                        backend_executor,
                        &mut backend_weights,
                        &mut active_job,
                        &mut user_session.latest_job,
                        &mut user_session.connected,
                        &stats,
                        &mut tui,
                    )?;
                    continue;
                }
            } else if let Some(event) = dev_session.client.recv_event_timeout(POOL_WAIT_POLL) {
                handle_active_pool_event(
                    event,
                    PoolConnectionMode::Dev,
                    cfg,
                    &dev_session.client,
                    &mut work_id_cursor,
                    &mut epoch,
                    backends,
                    backend_executor,
                    &mut backend_weights,
                    &mut active_job,
                    &mut dev_session.latest_job,
                    &mut dev_session.connected,
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
                        let active_client = if active_mode == PoolConnectionMode::Dev {
                            &dev_session.client
                        } else {
                            &user_session.client
                        };
                        let submit_outcome =
                            submit_pool_solution(active_client, &mut active_job, &solution, &stats);
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
                                            "JOB",
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
                        network_hashrate: &pool_network_hashrate,
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

fn handle_active_pool_event(
    event: PoolEvent,
    mode: PoolConnectionMode,
    cfg: &Config,
    pool_client: &PoolClient,
    work_id_cursor: &mut u64,
    epoch_cursor: &mut u64,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
    backend_weights: &mut BTreeMap<u64, f64>,
    active_job: &mut Option<ActivePoolJob>,
    latest_job: &mut Option<PoolJob>,
    connected: &mut bool,
    stats: &Stats,
    tui: &mut Option<TuiDisplay>,
) -> Result<()> {
    match event {
        PoolEvent::Connected => {
            *connected = true;
            if mode == PoolConnectionMode::Dev {
                success("CONN", "dev session connected");
            } else {
                success("CONN", "connected");
            }
            set_tui_state_label(tui, "pool-connected");
        }
        PoolEvent::Disconnected(message) => {
            if mode == PoolConnectionMode::Dev {
                warn("CONN", "dev session disconnected");
            } else {
                warn("CONN", message);
            }
            *latest_job = None;
            *connected = false;
            if active_job.is_some() {
                if let Err(err) =
                    super::cancel_backend_slots(backends, RuntimeMode::Mining, backend_executor)
                {
                    warn(
                        "BACKEND",
                        format!("pool cancel failed after disconnect: {err:#}"),
                    );
                }
                let _ =
                    super::quiesce_backend_slots(backends, RuntimeMode::Mining, backend_executor);
                *active_job = None;
            }
            set_tui_state_label(tui, "pool-disconnected");
            std::thread::sleep(TEMPLATE_RETRY_DELAY);
        }
        PoolEvent::LoginAccepted(ack) => {
            if mode == PoolConnectionMode::Dev {
                success("AUTH", "dev session login accepted");
            } else {
                let required = if ack.required_capabilities.is_empty() {
                    "none".to_string()
                } else {
                    ack.required_capabilities.join(",")
                };
                success(
                    "AUTH",
                    format!(
                        "login accepted (protocol v{}, required={required})",
                        ack.protocol_version
                    ),
                );
            }
            set_tui_state_label(tui, "pool-authenticated");
        }
        PoolEvent::LoginRejected(message) => {
            if mode == PoolConnectionMode::Dev {
                error("AUTH", "dev session login rejected");
            } else {
                error("AUTH", format!("login rejected: {message}"));
            }
            *latest_job = None;
            *connected = false;
            if active_job.is_some() {
                if let Err(err) =
                    super::cancel_backend_slots(backends, RuntimeMode::Mining, backend_executor)
                {
                    warn(
                        "BACKEND",
                        format!("pool cancel failed after login reject: {err:#}"),
                    );
                }
                let _ =
                    super::quiesce_backend_slots(backends, RuntimeMode::Mining, backend_executor);
                *active_job = None;
            }
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
                success("SHARE", "accepted");
                if let Some(display) = tui.as_mut() {
                    display.mark_block_found();
                }
            } else {
                stats.bump_stale_shares();
                let reason = ack.error.as_deref().unwrap_or("unknown");
                warn("SHARE", format!("rejected ({reason})"));
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
            *latest_job = Some(job.clone());
            assign_pool_job(
                cfg,
                pool_client,
                job,
                true,
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

fn handle_inactive_pool_event(
    event: PoolEvent,
    mode: PoolConnectionMode,
    latest_job: &mut Option<PoolJob>,
    connected: &mut bool,
    stats: &Stats,
) {
    match event {
        PoolEvent::Connected => {
            if !*connected {
                info("CONN", format!("{} session connected", mode.as_str()));
            }
            *connected = true;
        }
        PoolEvent::Disconnected(message) => {
            *latest_job = None;
            if *connected {
                if mode == PoolConnectionMode::Dev {
                    warn("CONN", "dev session disconnected");
                } else {
                    warn("CONN", format!("{} session {message}", mode.as_str()));
                }
            }
            *connected = false;
        }
        PoolEvent::LoginAccepted(ack) => {
            if mode == PoolConnectionMode::Dev {
                success("AUTH", "dev session login accepted");
            } else {
                let required = if ack.required_capabilities.is_empty() {
                    "none".to_string()
                } else {
                    ack.required_capabilities.join(",")
                };
                success(
                    "AUTH",
                    format!(
                        "{} session login accepted (protocol v{}, required={required})",
                        mode.as_str(),
                        ack.protocol_version
                    ),
                );
            }
        }
        PoolEvent::LoginRejected(message) => {
            *latest_job = None;
            *connected = false;
            if mode == PoolConnectionMode::Dev {
                error("AUTH", "dev session login rejected");
            } else {
                error(
                    "AUTH",
                    format!("{} session login rejected: {message}", mode.as_str()),
                );
            }
        }
        PoolEvent::SubmitAck(ack) => {
            if ack.accepted {
                stats.bump_accepted();
                success("SHARE", format!("accepted ({})", mode.as_str()));
            } else {
                stats.bump_stale_shares();
                let reason = ack.error.as_deref().unwrap_or("unknown");
                warn("SHARE", format!("rejected ({reason}) ({})", mode.as_str()));
            }
        }
        PoolEvent::Job(job) => {
            *latest_job = Some(job);
        }
    }
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
    if let Some(old) = old_difficulty {
        if difficulty > old {
            success("VARDIFF", format!("difficulty {old} -> {difficulty}"));
        } else if difficulty < old {
            info("VARDIFF", format!("difficulty {old} -> {difficulty}"));
        }
    } else {
        info("VARDIFF", format!("difficulty set to {difficulty}"));
    }
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
    cancel_prior_work: bool,
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
        warn("JOB", "ignoring job with empty nonce range");
        return Ok(());
    }

    let header_base = match decode_hex(&job.header_base, "pool.header_base") {
        Ok(value) => Arc::<[u8]>::from(value),
        Err(err) => {
            warn("JOB", format!("invalid job header_base: {err:#}"));
            return Ok(());
        }
    };
    let target = match parse_target(&job.target) {
        Ok(target) => target,
        Err(err) => {
            warn("JOB", format!("invalid job target: {err:#}"));
            return Ok(());
        }
    };

    if cancel_prior_work {
        if let Err(err) =
            super::cancel_backend_slots(backends, RuntimeMode::Mining, backend_executor)
        {
            warn(
                "BACKEND",
                format!("failed cancelling prior pool work: {err:#}"),
            );
        }
    }

    stats.bump_templates();
    let height = job.height;
    let difficulty_label = job
        .difficulty
        .map(|difficulty| difficulty.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    *active_job = Some(ActivePoolJob::new(job, 0, header_base, target));

    info(
        "JOB",
        format!("new job height={height} difficulty={difficulty_label}"),
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
            "JOB",
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
        info("SHARE", "submitted");
        job.pending_submit_nonces.insert(solution.nonce);
        job.next_nonce = job.next_nonce.max(solution.nonce.saturating_add(1));
        PoolShareSubmitOutcome::Submitted
    } else {
        job.submitted_nonces.remove(&solution.nonce);
        warn("SHARE", "failed to queue submit");
        PoolShareSubmitOutcome::QueueFailed
    }
}

impl PoolUiTelemetryClient {
    fn new(pool_url: &str, address: &str) -> Option<Self> {
        let base_url = pool_api_base_url_from_pool_url(pool_url)?;
        let http = HttpClient::builder()
            .timeout(POOL_TELEMETRY_TIMEOUT)
            .build()
            .ok()?;
        Some(Self {
            stats_url: format!("{base_url}/api/stats"),
            miner_url: format!("{base_url}/api/miner/{address}"),
            http,
        })
    }

    fn fetch_pool_hashrate(&self) -> Result<String> {
        let body: Value = self
            .http
            .get(&self.stats_url)
            .send()
            .and_then(|resp| resp.json())
            .map_err(|err| anyhow!("GET {} failed: {err}", self.stats_url))?;

        let hashrate = body
            .pointer("/chain/network_hashrate")
            .and_then(value_as_f64)
            .or_else(|| body.pointer("/network_hashrate").and_then(value_as_f64))
            .ok_or_else(|| anyhow!("missing network hashrate field"))?;
        if !hashrate.is_finite() || hashrate < 0.0 {
            bail!("invalid network hashrate value");
        }
        Ok(format_hashrate_ui(hashrate))
    }

    fn fetch_pool_balances(&self) -> Result<(String, String)> {
        let body: Value = self
            .http
            .get(&self.miner_url)
            .send()
            .and_then(|resp| resp.json())
            .map_err(|err| anyhow!("GET {} failed: {err}", self.miner_url))?;

        let pending = body
            .pointer("/balance/pending")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("missing field balance.pending"))?;
        let paid = body
            .pointer("/balance/paid")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("missing field balance.paid"))?;
        Ok((
            format_atomic_units_bnt(pending),
            format_atomic_units_bnt(paid),
        ))
    }
}

fn div_ceil_u64(value: u64, divisor: u64) -> u64 {
    let divisor = divisor.max(1);
    (value.saturating_add(divisor - 1)) / divisor
}

fn pool_api_base_url_from_pool_url(pool_url: &str) -> Option<String> {
    let trimmed = pool_url.trim();
    if trimmed.is_empty() {
        return None;
    }
    let authority = trimmed
        .strip_prefix("stratum+tcp://")
        .unwrap_or(trimmed)
        .split('/')
        .next()
        .unwrap_or(trimmed)
        .trim();
    if authority.is_empty() {
        return None;
    }

    let host = if authority.starts_with('[') {
        let closing = authority.find(']')?;
        authority[..=closing].to_string()
    } else {
        authority
            .split(':')
            .next()
            .unwrap_or(authority)
            .trim()
            .to_string()
    };
    if host.is_empty() {
        return None;
    }

    Some(format!("http://{host}:{POOL_API_DEFAULT_PORT}"))
}

fn format_atomic_units_bnt(amount_atomic: u64) -> String {
    let rounded_atomic = amount_atomic.saturating_add(BNT_DISPLAY_SCALE_ATOMIC_UNITS / 2)
        / BNT_DISPLAY_SCALE_ATOMIC_UNITS
        * BNT_DISPLAY_SCALE_ATOMIC_UNITS;
    let whole = rounded_atomic / ATOMIC_UNITS_PER_BNT;
    let fractional = (rounded_atomic % ATOMIC_UNITS_PER_BNT) / BNT_DISPLAY_SCALE_ATOMIC_UNITS;
    let whole = format_u64_with_commas(whole);
    if fractional == 0 {
        return format!("{whole} BNT");
    }

    let mut frac = format!("{fractional:0width$}", width = BNT_DISPLAY_DECIMALS);
    while frac.ends_with('0') {
        frac.pop();
    }
    format!("{whole}.{frac} BNT")
}

fn format_u64_with_commas(value: u64) -> String {
    if value < 1_000 {
        return value.to_string();
    }

    let mut digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    while digits.len() > 3 {
        let chunk = digits.split_off(digits.len() - 3);
        if out.is_empty() {
            out = chunk;
        } else {
            out = format!("{chunk},{out}");
        }
    }
    if out.is_empty() {
        digits
    } else {
        format!("{digits},{out}")
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_u64().map(|v| v as f64))
        .or_else(|| value.as_i64().filter(|v| *v >= 0).map(|v| v as f64))
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
