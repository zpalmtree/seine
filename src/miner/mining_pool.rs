use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use crossbeam_channel::{unbounded, Receiver};
use reqwest::blocking::Client as HttpClient;
use serde_json::Value;

use crate::backend::{DynamicShareTarget, MiningSolution, ShareBindingId};
use crate::config::{Config, WorkAllocation};
use crate::daemon_api::{is_unauthorized_error, ApiClient};
use crate::dev_fee::{effective_pool_dev_fee_percent, DevFeeTracker, DEV_ADDRESS, DEV_FEE_PERCENT};
use crate::pool::{
    PoolClient, PoolEvent, PoolJob, POOL_NOTIFICATION_MINER_BLOCK_FOUND,
    POOL_NOTIFICATION_POOL_BLOCK_SOLVED,
};
use crate::types::{decode_hex, difficulty_to_target, parse_target};

use super::auth::{refresh_api_token_from_cookie, TokenRefreshOutcome};
use super::mining::MiningRuntimeBackends;
use super::mining_tui::{
    init_tui_display, render_tui_now, set_tui_dev_fee_active, set_tui_pending_nvidia,
    set_tui_pool_balance_overview, set_tui_state_label, set_tui_wallet_overview, update_tui,
    RoundUiView, TuiDisplay,
};
use super::runtime::{maybe_print_stats, seed_backend_weights, work_distribution_weights};
use super::scheduler::NonceReservation;
use super::stats::{format_hashrate_ui, Stats};
use super::tui::TuiState;
use super::ui::{error, info, notify_dev_fee_mode, success, warn};
use super::{
    distribute_work, next_work_id, pending_backend_slot_label, solution_backend_label, total_lanes,
    BackendRoundTelemetry, BackendSlot, DistributeWorkOptions, RuntimeMode, TEMPLATE_RETRY_DELAY,
};

const POOL_WAIT_POLL: Duration = Duration::from_millis(200);
const POOL_EVENT_IDLE_SLEEP: Duration = Duration::from_millis(5);
const POOL_JOB_STOP_AT_HORIZON: Duration = Duration::from_secs(365 * 24 * 60 * 60);
const POOL_MAX_INFLIGHT_SUBMITS: usize = 16;
const POOL_MAX_DEFERRED_SUBMITS: usize = 4096;
const POOL_SUBMIT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const POOL_SUBMIT_ACK_WARN_THRESHOLD: usize = 4;
const POOL_SUBMIT_LOOKUP_RETENTION: Duration = Duration::from_secs(120);
const POOL_SUBMIT_LOOKUP_CAPACITY: usize = 8192;
const POOL_DESYNC_STALE_ACK_WINDOW: Duration = Duration::from_secs(60);
const POOL_DESYNC_STALE_ACK_THRESHOLD: usize = 3;
const POOL_DESYNC_STALE_FOR_THRESHOLD: Duration = Duration::from_secs(15);
const POOL_TELEMETRY_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const POOL_TELEMETRY_TIMEOUT: Duration = Duration::from_secs(5);
const POOL_TIMING_SLOW_CANCEL_THRESHOLD: Duration = Duration::from_secs(1);
const POOL_TIMING_SLOW_ASSIGN_THRESHOLD: Duration = Duration::from_secs(1);
const POOL_TIMING_SLOW_QUEUE_THRESHOLD: Duration = Duration::from_secs(3);
const POOL_TIMING_SLOW_ACK_THRESHOLD: Duration = Duration::from_secs(3);
const ATOMIC_UNITS_PER_BNT: u64 = 100_000_000;
const BNT_DISPLAY_DECIMALS: usize = 4;
const BNT_DISPLAY_SCALE_ATOMIC_UNITS: u64 = 10_000;
const DEV_POOL_URL: &str = "stratum+tcp://bntpool.com:3333";

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

    fn is_user(self) -> bool {
        matches!(self, Self::User)
    }
}

fn should_apply_submit_ack_difficulty_immediately(_mode: PoolConnectionMode) -> bool {
    // Pool vardiff changes supersede the current assignment and arrive with a new job_id.
    // Applying the difficulty from the ACK before the replacement job arrives can keep
    // work/backlog attached to an already-stale assignment.
    false
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PoolConnectionConfig {
    mode: PoolConnectionMode,
    pool_url: String,
    address: String,
    worker: String,
}

#[derive(Debug, Clone)]
struct PoolUiTelemetryClient {
    endpoints: Vec<PoolTelemetryEndpoint>,
    http: HttpClient,
}

#[derive(Debug)]
struct PoolTelemetryRefreshResult {
    request_id: u64,
    hashrate: Option<String>,
    balances: Option<(String, String)>,
    local_wallet: Option<LocalDaemonWalletSnapshot>,
    errors: Vec<String>,
}

#[derive(Debug, Clone)]
struct PoolTelemetryEndpoint {
    stats_url: String,
    miner_balance_url: String,
    miner_url: String,
}

#[derive(Debug, Clone)]
struct LocalDaemonWalletSnapshot {
    address: String,
    pending: String,
    unlocked: String,
}

struct PoolSession {
    connection: PoolConnectionConfig,
    client: PoolClient,
    telemetry: Option<PoolUiTelemetryClient>,
    latest_job: Option<PoolJob>,
    connected: bool,
    resume_job: Option<ActivePoolJob>,
    recent_submit_lookup: RecentPendingSubmitLookup,
}

struct ActivePoolJob {
    job: PoolJob,
    header_base: Arc<[u8]>,
    template_id: Option<String>,
    network_target: Option<[u8; 32]>,
    target: [u8; 32],
    dynamic_share_target: Arc<DynamicShareTarget>,
    share_difficulty: Option<u64>,
    next_share_binding_id: ShareBindingId,
    share_bindings: HashMap<ShareBindingId, PoolShareBinding>,
    next_nonce: u64,
    dispatch_nonce: u64,
    epoch: u64,
    height: String,
    round_start: Instant,
    round_hashes: u64,
    round_backend_hashes: BTreeMap<u64, u64>,
    round_backend_telemetry: BTreeMap<u64, BackendRoundTelemetry>,
    submitted_nonces: HashSet<u64>,
    pending_submit_nonces: HashMap<(String, u64), PendingPoolSubmit>,
    deferred_submits: VecDeque<DeferredPoolSubmit>,
    recent_stale_submit_acks: VecDeque<Instant>,
    job_received_at: Instant,
    last_assignment_timing: Option<PoolAssignmentTiming>,
}

#[derive(Debug, Clone)]
struct PoolShareBinding {
    job_id: String,
}

#[derive(Debug, Clone)]
struct DeferredPoolSubmit {
    share_binding_id: ShareBindingId,
    nonce: u64,
    claimed_hash: Option<[u8; 32]>,
    found_at: Instant,
    backend_label: String,
}

#[derive(Debug, Clone)]
struct PendingPoolSubmit {
    found_at: Instant,
    submitted_at: Instant,
    backend_label: String,
}

#[derive(Default)]
struct RecentPendingSubmitLookup {
    entries: HashMap<(String, u64), PendingPoolSubmit>,
    order: VecDeque<(String, u64, Instant)>,
}

impl RecentPendingSubmitLookup {
    fn insert(&mut self, job_id: &str, nonce: u64, pending_submit: PendingPoolSubmit) {
        let submitted_at = pending_submit.submitted_at;
        let key = (job_id.to_string(), nonce);
        self.entries.insert(key.clone(), pending_submit);
        self.order.push_back((key.0, key.1, submitted_at));
        self.prune(Instant::now());
    }

    fn remove(&mut self, job_id: &str, nonce: u64) -> Option<PendingPoolSubmit> {
        self.prune(Instant::now());
        self.entries.remove(&(job_id.to_string(), nonce))
    }

    fn prune(&mut self, now: Instant) {
        let cutoff = now.checked_sub(POOL_SUBMIT_LOOKUP_RETENTION).unwrap_or(now);
        while let Some((job_id, nonce, submitted_at)) = self.order.front().cloned() {
            let over_capacity = self.entries.len() > POOL_SUBMIT_LOOKUP_CAPACITY;
            if !over_capacity && submitted_at > cutoff {
                break;
            }

            self.order.pop_front();
            let key = (job_id, nonce);
            let should_remove = self
                .entries
                .get(&key)
                .is_some_and(|pending| pending.submitted_at == submitted_at);
            if should_remove {
                self.entries.remove(&key);
            }
        }

        if self.entries.is_empty() {
            self.order.clear();
        }
    }
}

#[derive(Debug, Clone)]
struct PoolAssignmentTiming {
    reason: &'static str,
    cancel_latency: Option<Duration>,
    assign_latency: Duration,
    completed_at: Instant,
}

impl ActivePoolJob {
    fn new(
        job: PoolJob,
        epoch: u64,
        header_base: Arc<[u8]>,
        target: [u8; 32],
        network_target: Option<[u8; 32]>,
    ) -> Self {
        let now = Instant::now();
        let next_nonce = job.nonce_start;
        let dispatch_nonce = next_nonce;
        let share_difficulty = job.difficulty;
        let current_share_binding_id = 1;
        let dynamic_share_target =
            Arc::new(DynamicShareTarget::new(target, current_share_binding_id));
        let template_id = job.template_id.clone();
        let mut share_bindings = HashMap::new();
        share_bindings.insert(
            current_share_binding_id,
            PoolShareBinding {
                job_id: job.job_id.clone(),
            },
        );
        Self {
            height: job.height.to_string(),
            header_base,
            template_id,
            network_target,
            target,
            dynamic_share_target,
            share_difficulty,
            next_share_binding_id: current_share_binding_id.saturating_add(1),
            share_bindings,
            next_nonce,
            dispatch_nonce,
            job,
            epoch,
            round_start: now,
            round_hashes: 0,
            round_backend_hashes: BTreeMap::new(),
            round_backend_telemetry: BTreeMap::new(),
            submitted_nonces: HashSet::new(),
            pending_submit_nonces: HashMap::new(),
            deferred_submits: VecDeque::new(),
            recent_stale_submit_acks: VecDeque::new(),
            job_received_at: now,
            last_assignment_timing: None,
        }
    }

    fn has_share_binding_job_id(&self, job_id: &str) -> bool {
        self.share_bindings
            .values()
            .any(|binding| binding.job_id == job_id)
    }

    fn share_binding(&self, share_binding_id: ShareBindingId) -> Option<&PoolShareBinding> {
        self.share_bindings.get(&share_binding_id)
    }

    fn register_share_binding(
        &mut self,
        job_id: String,
        target: [u8; 32],
    ) -> ShareBindingId {
        let share_binding_id = self.next_share_binding_id.max(1);
        self.next_share_binding_id = share_binding_id.saturating_add(1);
        self.share_bindings
            .insert(share_binding_id, PoolShareBinding { job_id });
        self.dynamic_share_target.update(target, share_binding_id);
        share_binding_id
    }
}

fn same_pool_template_identity(
    active_job: &ActivePoolJob,
    incoming_job: &PoolJob,
    header_base: &[u8],
    network_target: Option<&[u8; 32]>,
) -> bool {
    if active_job.job.height != incoming_job.height {
        return false;
    }
    if active_job.job.nonce_start != incoming_job.nonce_start
        || active_job.job.nonce_end != incoming_job.nonce_end
    {
        return false;
    }
    if active_job.header_base.as_ref() != header_base {
        return false;
    }
    match (active_job.network_target.as_ref(), network_target) {
        (Some(current), Some(incoming)) if current != incoming => return false,
        _ => {}
    }
    match (&active_job.template_id, &incoming_job.template_id) {
        (Some(current), Some(incoming)) if current != incoming => return false,
        _ => {}
    }
    true
}

fn rebind_active_pool_job(
    mode: PoolConnectionMode,
    active_job: &mut ActivePoolJob,
    job: PoolJob,
    target: [u8; 32],
    network_target: Option<[u8; 32]>,
) {
    let now = Instant::now();
    let previous_difficulty = active_job.share_difficulty;
    active_job.template_id = job.template_id.clone();
    active_job.network_target = network_target;
    active_job.target = target;
    active_job.share_difficulty = job.difficulty;
    active_job.height = job.height.to_string();
    active_job.job_received_at = now;
    active_job.register_share_binding(job.job_id.clone(), target);
    active_job.job = job.clone();

    if !mode.is_user() {
        return;
    }
    let difficulty_label = job
        .difficulty
        .map(|difficulty| difficulty.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    match (previous_difficulty, job.difficulty) {
        (Some(old), Some(new)) if old != new => {
            if new > old {
                success("VARDIFF", format!("difficulty {old} -> {new}"));
            } else {
                info("VARDIFF", format!("difficulty {old} -> {new}"));
            }
        }
        _ => {}
    }
    info(
        "JOB",
        format!("updated job height={} difficulty={difficulty_label}", job.height),
    );
}

enum PoolShareSubmitOutcome {
    Submitted,
    Deferred,
    Duplicate,
    StaleEpoch,
    QueueFailed,
}

fn should_resume_pool_assignment(job: &ActivePoolJob, has_pending_work: bool) -> bool {
    !has_pending_work && job.next_nonce <= job.job.nonce_end
}

fn advance_pool_nonce_cursor(job: &mut ActivePoolJob, solved_nonce: u64) {
    let window_end_exclusive = job.job.nonce_end.saturating_add(1);
    let next_nonce = solved_nonce.saturating_add(1).min(window_end_exclusive);
    if next_nonce > job.next_nonce {
        job.next_nonce = next_nonce;
    }
}

fn parse_pool_stale_for(reason: &str) -> Option<Duration> {
    let marker = "stale_for=";
    let start = reason.find(marker)? + marker.len();
    let remainder = &reason[start..];
    let end = remainder.find('s')?;
    let seconds = remainder[..end].parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    Some(Duration::from_secs_f64(seconds))
}

fn note_current_job_stale_submit_rejection(
    job: &mut ActivePoolJob,
    reason: &str,
    now: Instant,
) -> Option<String> {
    let lower = reason.to_ascii_lowercase();
    if !lower.contains("stale job") {
        job.recent_stale_submit_acks.clear();
        return None;
    }
    if lower.contains("assignment not found") {
        return Some("pool no longer recognizes the current job assignment".to_string());
    }
    if let Some(stale_for) = parse_pool_stale_for(reason) {
        if stale_for >= POOL_DESYNC_STALE_FOR_THRESHOLD {
            return Some(format!(
                "pool reported stale_for={:.3}s for the current job",
                stale_for.as_secs_f64()
            ));
        }
    }

    let cutoff = now.checked_sub(POOL_DESYNC_STALE_ACK_WINDOW).unwrap_or(now);
    while job
        .recent_stale_submit_acks
        .front()
        .is_some_and(|entry| *entry < cutoff)
    {
        job.recent_stale_submit_acks.pop_front();
    }
    job.recent_stale_submit_acks.push_back(now);
    if job.recent_stale_submit_acks.len() >= POOL_DESYNC_STALE_ACK_THRESHOLD {
        return Some(format!(
            "received {} stale submit rejections within {}s",
            job.recent_stale_submit_acks.len(),
            POOL_DESYNC_STALE_ACK_WINDOW.as_secs()
        ));
    }
    None
}

fn format_pool_duration(duration: Duration) -> String {
    format!("{:.3}s", duration.as_secs_f64())
}

fn record_pool_assignment_timing(
    mode: PoolConnectionMode,
    job: &mut ActivePoolJob,
    reason: &'static str,
    cancel_latency: Option<Duration>,
    assign_latency: Duration,
) {
    let completed_at = Instant::now();
    job.last_assignment_timing = Some(PoolAssignmentTiming {
        reason,
        cancel_latency,
        assign_latency,
        completed_at,
    });

    if !mode.is_user() {
        return;
    }
    let slow_cancel =
        cancel_latency.is_some_and(|value| value >= POOL_TIMING_SLOW_CANCEL_THRESHOLD);
    let slow_assign = assign_latency >= POOL_TIMING_SLOW_ASSIGN_THRESHOLD;
    if slow_cancel || slow_assign {
        warn(
            "POOL",
            format!(
                "timing | switch={reason} cancel={} assign={}",
                cancel_latency
                    .map(format_pool_duration)
                    .unwrap_or_else(|| "-".to_string()),
                format_pool_duration(assign_latency),
            ),
        );
    }
}

fn maybe_log_pool_submit_timing(
    mode: PoolConnectionMode,
    status: &str,
    nonce: u64,
    pending_submit: Option<&PendingPoolSubmit>,
    acknowledged_at: Instant,
) {
    let Some(pending_submit) = pending_submit else {
        return;
    };
    let queued_for = pending_submit
        .submitted_at
        .saturating_duration_since(pending_submit.found_at);
    let submit_to_ack = acknowledged_at.saturating_duration_since(pending_submit.submitted_at);
    if queued_for < POOL_TIMING_SLOW_QUEUE_THRESHOLD
        && submit_to_ack < POOL_TIMING_SLOW_ACK_THRESHOLD
    {
        return;
    }
    let total = acknowledged_at.saturating_duration_since(pending_submit.found_at);
    let message = format!(
        "timing | share={status} nonce={nonce} backend={} queued={} submit_to_ack={} total={}",
        pending_submit.backend_label,
        format_pool_duration(queued_for),
        format_pool_duration(submit_to_ack),
        format_pool_duration(total),
    );
    if mode.is_user() {
        if status == "accepted" {
            info("POOL", message);
        } else {
            warn("POOL", message);
        }
    }
}

fn take_submit_timing_for_ack(
    ack_job_id: &str,
    nonce: u64,
    ack_for_active_job: bool,
    current_pending_submit_nonces: Option<&mut HashMap<(String, u64), PendingPoolSubmit>>,
    recent_submit_lookup: &mut RecentPendingSubmitLookup,
) -> (bool, Option<PendingPoolSubmit>) {
    let mut submit_timing = if ack_for_active_job {
        current_pending_submit_nonces
            .and_then(|pending| pending.remove(&(ack_job_id.to_string(), nonce)))
    } else {
        None
    };

    let tracked_submit = recent_submit_lookup.remove(ack_job_id, nonce);
    if submit_timing.is_none() {
        submit_timing = tracked_submit;
    }

    (ack_for_active_job, submit_timing)
}

fn log_pool_issue_timing(
    mode: PoolConnectionMode,
    context: &str,
    reason: &str,
    active_job: Option<&ActivePoolJob>,
    pending_submit: Option<PendingPoolSubmit>,
    now: Instant,
) {
    if !mode.is_user() {
        return;
    }

    let mut parts = Vec::new();
    parts.push(format!("timing | context={context}"));
    parts.push(format!("reason={reason}"));
    if let Some(pending_submit) = pending_submit.as_ref() {
        parts.push(format!("backend={}", pending_submit.backend_label));
    }
    if let Some(job) = active_job {
        parts.push(format!("job_height={}", job.height));
        parts.push(format!(
            "job_age={}",
            format_pool_duration(now.saturating_duration_since(job.job_received_at))
        ));
        parts.push(format!("pending={}", job.pending_submit_nonces.len()));
        parts.push(format!("deferred={}", job.deferred_submits.len()));
        if let Some(last_assignment) = &job.last_assignment_timing {
            parts.push(format!("last_switch={}", last_assignment.reason));
            parts.push(format!(
                "last_cancel={}",
                last_assignment
                    .cancel_latency
                    .map(format_pool_duration)
                    .unwrap_or_else(|| "-".to_string())
            ));
            parts.push(format!(
                "last_assign={}",
                format_pool_duration(last_assignment.assign_latency)
            ));
            parts.push(format!(
                "last_switch_age={}",
                format_pool_duration(now.saturating_duration_since(last_assignment.completed_at))
            ));
        }
    }
    if let Some(pending_submit) = pending_submit {
        let queued_for = pending_submit
            .submitted_at
            .saturating_duration_since(pending_submit.found_at);
        let submit_to_ack = now.saturating_duration_since(pending_submit.submitted_at);
        let total = now.saturating_duration_since(pending_submit.found_at);
        parts.push(format!("share_queued={}", format_pool_duration(queued_for)));
        parts.push(format!(
            "share_submit_to_ack={}",
            format_pool_duration(submit_to_ack)
        ));
        parts.push(format!("share_total={}", format_pool_duration(total)));
    }

    warn("POOL", parts.join(" "));
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
    if connection.mode.is_user() {
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
    let telemetry = PoolUiTelemetryClient::new(&connection.pool_url, &connection.address);
    if telemetry.is_none() && connection.mode.is_user() {
        warn(
            "STATS",
            format!(
                "pool telemetry unavailable for {} session: could not derive pool API URL",
                connection.mode.as_str()
            ),
        );
    }
    if update_ui {
        set_tui_pool_balance_overview(tui, &connection.address, "---", "---");
    }
    Ok((pool_client, telemetry))
}

pub(super) fn run_pool_mining_loop(
    cfg: &Config,
    shutdown: Arc<AtomicBool>,
    runtime_backends: MiningRuntimeBackends<'_>,
    local_daemon_client: Option<ApiClient>,
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

    let stats = Stats::new();
    let mut last_stats_print = Instant::now();
    let mut backend_weights = seed_backend_weights(backends);
    let mut work_id_cursor = 1u64;
    let mut epoch = 0u64;
    let mut last_hash_poll = Instant::now();
    let mut tui = init_tui_display(tui_state, Arc::clone(&shutdown));
    let dev_fee_percent = effective_pool_dev_fee_percent(&user_connection.pool_url);
    if (dev_fee_percent - DEV_FEE_PERCENT).abs() > f64::EPSILON {
        success(
            "POOL",
            format!(
                "bntpool.com detected; dev fee reduced to {:.1}% (default {:.1}%)",
                dev_fee_percent, DEV_FEE_PERCENT
            ),
        );
    }
    let mut dev_fee_tracker = DevFeeTracker::with_percent(dev_fee_percent);
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
        set_tui_pool_balance_overview(&mut tui, &user_connection.address, "---", "---");
    }
    let (user_client, user_telemetry) =
        connect_pool_session(&user_connection, Arc::clone(&shutdown), &mut tui, false)?;
    let (dev_client, dev_telemetry) =
        connect_pool_session(&dev_connection, Arc::clone(&shutdown), &mut tui, false)?;
    let mut user_session = PoolSession {
        connection: user_connection,
        client: user_client,
        telemetry: user_telemetry,
        latest_job: None,
        connected: false,
        resume_job: None,
        recent_submit_lookup: RecentPendingSubmitLookup::default(),
    };
    let mut dev_session = PoolSession {
        connection: dev_connection,
        client: dev_client,
        telemetry: dev_telemetry,
        latest_job: None,
        connected: false,
        resume_job: None,
        recent_submit_lookup: RecentPendingSubmitLookup::default(),
    };

    let mut active_job: Option<ActivePoolJob> = None;
    let mut pending_nvidia_logged = false;
    let mut pool_network_hashrate = "unknown".to_string();
    let mut next_pool_telemetry_refresh = Instant::now();
    let mut pool_telemetry_warning_logged = false;
    let (telemetry_result_tx, telemetry_result_rx) = unbounded::<PoolTelemetryRefreshResult>();
    let mut telemetry_next_request_id = 0u64;
    let mut telemetry_inflight_request_id: Option<u64> = None;
    let mut local_wallet_address_logged: Option<String> = None;
    let mut local_wallet_mismatch_logged = false;

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
                                &dev_session.client,
                                &mut dev_session.latest_job,
                                &mut dev_session.connected,
                                &mut dev_session.resume_job,
                                &mut dev_session.recent_submit_lookup,
                                &stats,
                            );
                        }
                    } else {
                        for event in user_session.client.drain_events() {
                            handle_inactive_pool_event(
                                event,
                                PoolConnectionMode::User,
                                &user_session.client,
                                &mut user_session.latest_job,
                                &mut user_session.connected,
                                &mut user_session.resume_job,
                                &mut user_session.recent_submit_lookup,
                                &stats,
                            );
                        }
                    }
                    if active_mode == PoolConnectionMode::Dev {
                        dev_session.resume_job = active_job.take();
                    } else {
                        user_session.resume_job = active_job.take();
                    }
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
                    active_mode = next_mode;

                    let (cached_job, mut resumed_job) = if active_mode == PoolConnectionMode::Dev {
                        (
                            if dev_session.connected {
                                dev_session.latest_job.clone()
                            } else {
                                None
                            },
                            if dev_session.connected {
                                dev_session.resume_job.take()
                            } else {
                                None
                            },
                        )
                    } else {
                        (
                            if user_session.connected {
                                user_session.latest_job.clone()
                            } else {
                                None
                            },
                            if user_session.connected {
                                user_session.resume_job.take()
                            } else {
                                None
                            },
                        )
                    };

                    if let Some(resume) = resumed_job.take() {
                        if cached_job
                            .as_ref()
                            .is_some_and(|cached| cached.job_id == resume.job.job_id)
                        {
                            active_job = Some(resume);
                            if let Some(job) = active_job.as_mut() {
                                let assign_started = Instant::now();
                                restart_pool_assignment(
                                    cfg,
                                    &mut work_id_cursor,
                                    &mut epoch,
                                    backends,
                                    backend_executor,
                                    &mut backend_weights,
                                    job,
                                )?;
                                record_pool_assignment_timing(
                                    active_mode,
                                    job,
                                    "resume",
                                    None,
                                    assign_started.elapsed(),
                                );
                            }
                            set_tui_state_label(&mut tui, "working");
                            render_tui_now(&mut tui);
                        }
                    }

                    if active_job.is_none() {
                        if let Some(job) = cached_job {
                            let active_client = if active_mode == PoolConnectionMode::Dev {
                                &dev_session.client
                            } else {
                                &user_session.client
                            };
                            assign_pool_job(
                                active_mode,
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
                    }

                    pool_network_hashrate = "unknown".to_string();
                    next_pool_telemetry_refresh = Instant::now();
                    pool_telemetry_warning_logged = false;
                    telemetry_inflight_request_id = None;
                }
            }
        }

        let user_address = user_session.connection.address.as_str();
        let user_telemetry = user_session.telemetry.as_ref();
        while let Ok(result) = telemetry_result_rx.try_recv() {
            if telemetry_inflight_request_id == Some(result.request_id) {
                telemetry_inflight_request_id = None;
            }
            if let Some(hashrate) = result.hashrate {
                pool_network_hashrate = hashrate;
            }
            if let Some(snapshot) = result.local_wallet {
                set_tui_wallet_overview(
                    &mut tui,
                    &snapshot.address,
                    &snapshot.pending,
                    &snapshot.unlocked,
                );
                if local_wallet_address_logged.as_deref() != Some(snapshot.address.as_str()) {
                    info(
                        "WALLET",
                        format!("using local daemon wallet balance: {}", snapshot.address),
                    );
                    local_wallet_address_logged = Some(snapshot.address.clone());
                    local_wallet_mismatch_logged = false;
                }
                if snapshot.address != user_address && !local_wallet_mismatch_logged {
                    warn(
                        "WALLET",
                        "local daemon wallet differs from pool payout address; wallet panel shows the local daemon wallet",
                    );
                    local_wallet_mismatch_logged = true;
                }
            } else if let Some((pending, paid)) = result.balances {
                set_tui_pool_balance_overview(&mut tui, user_address, &pending, &paid);
            }

            if result.errors.is_empty() {
                if pool_telemetry_warning_logged {
                    info("STATS", "pool telemetry recovered");
                    pool_telemetry_warning_logged = false;
                }
            } else if !pool_telemetry_warning_logged {
                let detail = result
                    .errors
                    .first()
                    .map(String::as_str)
                    .unwrap_or("unknown telemetry error");
                warn("STATS", format!("failed to fetch pool stats: {detail}"));
                pool_telemetry_warning_logged = true;
            }
        }
        if user_telemetry.is_some() || local_daemon_client.is_some() {
            if Instant::now() >= next_pool_telemetry_refresh
                && telemetry_inflight_request_id.is_none()
            {
                telemetry_next_request_id = telemetry_next_request_id.wrapping_add(1).max(1);
                let request_id = telemetry_next_request_id;
                let telemetry = user_telemetry.cloned();
                let local_daemon_client = local_daemon_client.clone();
                let local_daemon_cookie_path = cfg.token_cookie_path.clone();
                let tx = telemetry_result_tx.clone();
                let spawn_result = std::thread::Builder::new()
                    .name("pool-telemetry-user".to_string())
                    .spawn(move || {
                        let mut errors = Vec::new();
                        let hashrate = if let Some(telemetry) = telemetry.as_ref() {
                            match telemetry.fetch_pool_hashrate() {
                                Ok(value) => Some(value),
                                Err(err) => {
                                    errors.push(format!("global hashrate: {err:#}"));
                                    None
                                }
                            }
                        } else {
                            None
                        };
                        let balances = if let Some(telemetry) = telemetry.as_ref() {
                            match telemetry.fetch_pool_balances() {
                                Ok(value) => Some(value),
                                Err(err) => {
                                    errors.push(format!("balance: {err:#}"));
                                    None
                                }
                            }
                        } else {
                            None
                        };
                        let local_wallet = local_daemon_client.as_ref().and_then(|client| {
                            fetch_local_daemon_wallet_snapshot(
                                client,
                                local_daemon_cookie_path.as_ref(),
                            )
                            .ok()
                        });
                        let _ = tx.send(PoolTelemetryRefreshResult {
                            request_id,
                            hashrate,
                            balances,
                            local_wallet,
                            errors,
                        });
                    });
                if spawn_result.is_ok() {
                    telemetry_inflight_request_id = Some(request_id);
                } else if !pool_telemetry_warning_logged {
                    warn("STATS", "failed to start pool telemetry refresh");
                    pool_telemetry_warning_logged = true;
                }
                next_pool_telemetry_refresh = Instant::now() + POOL_TELEMETRY_REFRESH_INTERVAL;
            }
        }

        if let Some(ref deferred) = deferred_rx {
            let mut deferred_backend_activated = false;
            loop {
                match deferred.try_recv() {
                    Ok(slot) => {
                        deferred_remaining = deferred_remaining.saturating_sub(1);
                        let slot_name = pending_backend_slot_label(backends, &slot);
                        info(
                            "BACKEND",
                            format!(
                                "{slot_name}: online (initialized in background, {} lanes)",
                                slot.lanes
                            ),
                        );
                        backend_weights.insert(slot.id, slot.lanes.max(1) as f64);
                        backends.push(slot);
                        deferred_backend_activated = true;
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
            if deferred_backend_activated {
                maybe_hot_rebalance_active_pool_job(
                    active_mode,
                    cfg,
                    &mut work_id_cursor,
                    &mut epoch,
                    backends,
                    backend_executor,
                    &mut backend_weights,
                    &mut active_job,
                    "deferred backend activated",
                )?;
            }
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
                    &mut user_session.recent_submit_lookup,
                    &mut user_session.latest_job,
                    &mut user_session.connected,
                    &stats,
                    &mut tui,
                )?;
            } else {
                handle_inactive_pool_event(
                    event,
                    PoolConnectionMode::User,
                    &user_session.client,
                    &mut user_session.latest_job,
                    &mut user_session.connected,
                    &mut user_session.resume_job,
                    &mut user_session.recent_submit_lookup,
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
                    &mut dev_session.recent_submit_lookup,
                    &mut dev_session.latest_job,
                    &mut dev_session.connected,
                    &stats,
                    &mut tui,
                )?;
            } else {
                handle_inactive_pool_event(
                    event,
                    PoolConnectionMode::Dev,
                    &dev_session.client,
                    &mut dev_session.latest_job,
                    &mut dev_session.connected,
                    &mut dev_session.resume_job,
                    &mut dev_session.recent_submit_lookup,
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
                        &mut user_session.recent_submit_lookup,
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
                    &mut dev_session.recent_submit_lookup,
                    &mut dev_session.latest_job,
                    &mut dev_session.connected,
                    &stats,
                    &mut tui,
                )?;
                continue;
            }
        }

        let mut topology_changed = false;
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
                        topology_changed = true;
                    }
                    if let Some(solution) = maybe_solution {
                        let backend_label = solution_backend_label(backends, &solution);
                        let submit_outcome = match active_mode {
                            PoolConnectionMode::Dev => submit_pool_solution(
                                active_mode,
                                &dev_session.client,
                                &mut active_job,
                                &solution,
                                &backend_label,
                                &mut dev_session.recent_submit_lookup,
                                &stats,
                            ),
                            PoolConnectionMode::User => submit_pool_solution(
                                active_mode,
                                &user_session.client,
                                &mut active_job,
                                &solution,
                                &backend_label,
                                &mut user_session.recent_submit_lookup,
                                &stats,
                            ),
                        };
                        if matches!(submit_outcome, PoolShareSubmitOutcome::StaleEpoch) {
                            continue;
                        }
                    }
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => break,
            }
        }

        if let Some(job) = active_job.as_mut() {
            match active_mode {
                PoolConnectionMode::Dev => service_pool_submit_backlog(
                    active_mode,
                    &dev_session.client,
                    job,
                    &mut dev_session.recent_submit_lookup,
                    &stats,
                ),
                PoolConnectionMode::User => service_pool_submit_backlog(
                    active_mode,
                    &user_session.client,
                    job,
                    &mut user_session.recent_submit_lookup,
                    &stats,
                ),
            }
        }
        if let Some(job) = user_session.resume_job.as_mut() {
            service_pool_submit_backlog(
                PoolConnectionMode::User,
                &user_session.client,
                job,
                &mut user_session.recent_submit_lookup,
                &stats,
            );
        }
        if let Some(job) = dev_session.resume_job.as_mut() {
            service_pool_submit_backlog(
                PoolConnectionMode::Dev,
                &dev_session.client,
                job,
                &mut dev_session.recent_submit_lookup,
                &stats,
            );
        }

        if topology_changed {
            maybe_hot_rebalance_active_pool_job(
                active_mode,
                cfg,
                &mut work_id_cursor,
                &mut epoch,
                backends,
                backend_executor,
                &mut backend_weights,
                &mut active_job,
                "backend topology changed",
            )?;
        }

        let mut should_resume_assignment = false;
        if last_hash_poll.elapsed() >= cfg.hash_poll_interval {
            if let Some(active_job) = active_job.as_mut() {
                let has_pending_work =
                    collect_pool_backend_samples(backends, backend_executor, &stats, active_job);
                should_resume_assignment =
                    should_resume_pool_assignment(active_job, has_pending_work);
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
        if should_resume_assignment {
            if let Some(job) = active_job.as_mut() {
                let assign_started = Instant::now();
                restart_pool_assignment(
                    cfg,
                    &mut work_id_cursor,
                    &mut epoch,
                    backends,
                    backend_executor,
                    &mut backend_weights,
                    job,
                )?;
                record_pool_assignment_timing(
                    active_mode,
                    job,
                    "resume-backlog",
                    None,
                    assign_started.elapsed(),
                );
            }
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

fn collect_pool_backend_samples(
    backends: &[BackendSlot],
    backend_executor: &super::backend_executor::BackendExecutor,
    stats: &Stats,
    active_job: &mut ActivePoolJob,
) -> bool {
    let runtime_telemetry = backend_executor.take_backend_telemetry_ordered(backends.iter());
    let mut collected = 0u64;
    let mut has_pending_work = false;

    for (slot, runtime) in backends.iter().zip(runtime_telemetry.into_iter()) {
        let backend_id = slot.id;
        let hashes = slot.backend.take_hashes();
        let telemetry = slot.backend.take_telemetry();
        has_pending_work |= telemetry.pending_work > 0 || telemetry.active_lanes > 0;
        super::merge_backend_telemetry(
            &mut active_job.round_backend_telemetry,
            backend_id,
            telemetry,
        );
        super::merge_backend_telemetry(
            &mut active_job.round_backend_telemetry,
            backend_id,
            runtime,
        );
        if hashes > 0 {
            collected = collected.saturating_add(hashes);
            let entry = active_job
                .round_backend_hashes
                .entry(backend_id)
                .or_insert(0);
            *entry = entry.saturating_add(hashes);
        }
    }

    if collected > 0 {
        stats.add_hashes(collected);
        active_job.round_hashes = active_job.round_hashes.saturating_add(collected);
    }

    has_pending_work
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
    recent_submit_lookup: &mut RecentPendingSubmitLookup,
    latest_job: &mut Option<PoolJob>,
    connected: &mut bool,
    stats: &Stats,
    tui: &mut Option<TuiDisplay>,
) -> Result<()> {
    match event {
        PoolEvent::Connected => {
            *connected = true;
            if mode.is_user() {
                success("CONN", "connected");
            }
            set_tui_state_label(tui, "pool-connected");
        }
        PoolEvent::Disconnected(message) => {
            if mode.is_user() {
                warn("CONN", &message);
            }
            *latest_job = None;
            *connected = false;
            if let Some(job) = active_job.as_ref() {
                log_pool_issue_timing(
                    mode,
                    "disconnect",
                    &message,
                    Some(job),
                    None,
                    Instant::now(),
                );
            }
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
            if mode.is_user() {
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
            if mode.is_user() {
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
        PoolEvent::Notification(notification) => {
            if mode.is_user() {
                match notification.kind.as_str() {
                    POOL_NOTIFICATION_POOL_BLOCK_SOLVED => {
                        success("POOL", notification.message);
                    }
                    POOL_NOTIFICATION_MINER_BLOCK_FOUND => {
                        success("ACCEPT", notification.message);
                        if let Some(display) = tui.as_mut() {
                            display.mark_block_found();
                        }
                    }
                    _ => {
                        info("POOL", notification.message);
                    }
                }
            }
        }
        PoolEvent::SubmitAck(ack) => {
            let acknowledged_at = Instant::now();
            let ack_for_current_job = active_job
                .as_ref()
                .is_some_and(|job| job.has_share_binding_job_id(ack.job_id.as_str()));
            let (ack_for_current_job, submit_timing) = take_submit_timing_for_ack(
                ack.job_id.as_str(),
                ack.nonce,
                ack_for_current_job,
                active_job.as_mut().map(|job| &mut job.pending_submit_nonces),
                recent_submit_lookup,
            );
            let mut desync_reason = None::<String>;
            if ack.accepted {
                stats.bump_accepted();
                let backend_suffix = submit_timing
                    .as_ref()
                    .map(|pending| format!(" (backend={})", pending.backend_label))
                    .unwrap_or_default();
                if mode.is_user() {
                    success("SHARE", format!("accepted{backend_suffix}"));
                }
                if let Some(display) = tui.as_mut() {
                    display.mark_block_found();
                }
                if ack_for_current_job {
                    if let Some(job) = active_job.as_mut() {
                        job.recent_stale_submit_acks.clear();
                    }
                }
                maybe_log_pool_submit_timing(
                    mode,
                    "accepted",
                    ack.nonce,
                    submit_timing.as_ref(),
                    acknowledged_at,
                );
            } else {
                stats.bump_stale_shares();
                let reason = ack.error.as_deref().unwrap_or("unknown");
                let backend_suffix = submit_timing
                    .as_ref()
                    .map(|pending| format!(" (backend={})", pending.backend_label))
                    .unwrap_or_default();
                if mode.is_user() {
                    warn("SHARE", format!("rejected ({reason}){backend_suffix}"));
                }
                maybe_log_pool_submit_timing(
                    mode,
                    "rejected",
                    ack.nonce,
                    submit_timing.as_ref(),
                    acknowledged_at,
                );
                if ack_for_current_job {
                    if let Some(job) = active_job.as_mut() {
                        desync_reason =
                            note_current_job_stale_submit_rejection(job, reason, acknowledged_at);
                    }
                }
            }
            if let Some(reason) = desync_reason {
                force_pool_session_resync(
                    mode,
                    pool_client,
                    latest_job,
                    connected,
                    backends,
                    backend_executor,
                    active_job,
                    tui,
                    &reason,
                    submit_timing.clone(),
                )?;
                return Ok(());
            }
            if let Some(difficulty) = ack.difficulty {
                if should_apply_submit_ack_difficulty_immediately(mode) {
                    apply_submit_ack_difficulty(
                        mode,
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
            if ack_for_current_job {
                if let Some(job) = active_job.as_mut() {
                    service_pool_submit_backlog(
                        mode,
                        pool_client,
                        job,
                        recent_submit_lookup,
                        stats,
                    );
                }
            }
        }
        PoolEvent::Job(job) => {
            *latest_job = Some(job.clone());
            assign_pool_job(
                mode,
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

fn force_pool_session_resync(
    mode: PoolConnectionMode,
    pool_client: &PoolClient,
    latest_job: &mut Option<PoolJob>,
    connected: &mut bool,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
    active_job: &mut Option<ActivePoolJob>,
    tui: &mut Option<TuiDisplay>,
    reason: &str,
    pending_submit: Option<PendingPoolSubmit>,
) -> Result<()> {
    let now = Instant::now();
    if mode.is_user() {
        warn(
            "CONN",
            format!("detected pool desync ({reason}); reconnecting"),
        );
    }
    log_pool_issue_timing(
        mode,
        "resync",
        reason,
        active_job.as_ref(),
        pending_submit,
        now,
    );
    if active_job.is_some() {
        if let Err(err) =
            super::cancel_backend_slots(backends, RuntimeMode::Mining, backend_executor)
        {
            warn(
                "BACKEND",
                format!("pool cancel failed during resync: {err:#}"),
            );
        }
        let _ = super::quiesce_backend_slots(backends, RuntimeMode::Mining, backend_executor);
    }
    *active_job = None;
    *latest_job = None;
    *connected = false;
    pool_client.request_reconnect();
    set_tui_state_label(tui, "pool-resync");
    render_tui_now(tui);
    Ok(())
}

fn handle_inactive_pool_event(
    event: PoolEvent,
    mode: PoolConnectionMode,
    pool_client: &PoolClient,
    latest_job: &mut Option<PoolJob>,
    connected: &mut bool,
    inactive_job: &mut Option<ActivePoolJob>,
    recent_submit_lookup: &mut RecentPendingSubmitLookup,
    stats: &Stats,
) {
    match event {
        PoolEvent::Connected => {
            if !*connected && mode.is_user() {
                info("CONN", format!("{} session connected", mode.as_str()));
            }
            *connected = true;
        }
        PoolEvent::Disconnected(message) => {
            *latest_job = None;
            if *connected && mode.is_user() {
                warn("CONN", format!("{} session {message}", mode.as_str()));
            }
            *connected = false;
            *inactive_job = None;
        }
        PoolEvent::LoginAccepted(ack) => {
            if mode.is_user() {
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
            *inactive_job = None;
            if mode.is_user() {
                error(
                    "AUTH",
                    format!("{} session login rejected: {message}", mode.as_str()),
                );
            }
        }
        PoolEvent::Notification(notification) => {
            if mode.is_user() {
                match notification.kind.as_str() {
                    POOL_NOTIFICATION_POOL_BLOCK_SOLVED => {
                        success("POOL", notification.message);
                    }
                    POOL_NOTIFICATION_MINER_BLOCK_FOUND => {
                        success("ACCEPT", notification.message);
                    }
                    _ => {
                        info("POOL", notification.message);
                    }
                }
            }
        }
        PoolEvent::SubmitAck(ack) => {
            let ack_for_current_job = inactive_job
                .as_ref()
                .is_some_and(|job| job.has_share_binding_job_id(ack.job_id.as_str()));
            let (ack_for_current_job, submit_timing) = take_submit_timing_for_ack(
                ack.job_id.as_str(),
                ack.nonce,
                ack_for_current_job,
                inactive_job.as_mut().map(|job| &mut job.pending_submit_nonces),
                recent_submit_lookup,
            );
            if ack.accepted {
                stats.bump_accepted();
                let backend_suffix = submit_timing
                    .as_ref()
                    .map(|pending| format!(" (backend={})", pending.backend_label))
                    .unwrap_or_default();
                if mode.is_user() {
                    success(
                        "SHARE",
                        format!("accepted ({}){backend_suffix}", mode.as_str()),
                    );
                }
            } else {
                stats.bump_stale_shares();
                let reason = ack.error.as_deref().unwrap_or("unknown");
                let backend_suffix = submit_timing
                    .as_ref()
                    .map(|pending| format!(" (backend={})", pending.backend_label))
                    .unwrap_or_default();
                if mode.is_user() {
                    warn(
                        "SHARE",
                        format!("rejected ({reason}) ({}){backend_suffix}", mode.as_str()),
                    );
                }
            }
            if ack_for_current_job {
                if let Some(job) = inactive_job.as_mut() {
                    if let Some(difficulty) = ack.difficulty {
                        if should_apply_submit_ack_difficulty_immediately(mode) {
                            let difficulty = difficulty.max(1);
                            job.share_difficulty = Some(difficulty);
                            job.target = difficulty_to_target(difficulty);
                        }
                    }
                    service_pool_submit_backlog(
                        mode,
                        pool_client,
                        job,
                        recent_submit_lookup,
                        stats,
                    );
                }
            }
        }
        PoolEvent::Job(job) => {
            if inactive_job
                .as_ref()
                .is_some_and(|active| active.job.job_id != job.job_id)
            {
                *inactive_job = None;
            }
            *latest_job = Some(job);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_submit_ack_difficulty(
    mode: PoolConnectionMode,
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
    if mode.is_user() {
        if let Some(old) = old_difficulty {
            if difficulty > old {
                success("VARDIFF", format!("difficulty {old} -> {difficulty}"));
            } else if difficulty < old {
                info("VARDIFF", format!("difficulty {old} -> {difficulty}"));
            }
        } else {
            info("VARDIFF", format!("difficulty set to {difficulty}"));
        }
    }
    let assign_started = Instant::now();
    restart_pool_assignment(
        cfg,
        work_id_cursor,
        epoch_cursor,
        backends,
        backend_executor,
        backend_weights,
        job,
    )?;
    record_pool_assignment_timing(
        mode,
        job,
        "difficulty-update",
        None,
        assign_started.elapsed(),
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn assign_pool_job(
    mode: PoolConnectionMode,
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
        if mode.is_user() {
            warn("JOB", "ignoring job with empty nonce range");
        }
        return Ok(());
    }

    let header_base = match decode_hex(&job.header_base, "pool.header_base") {
        Ok(value) => Arc::<[u8]>::from(value),
        Err(err) => {
            if mode.is_user() {
                warn("JOB", format!("invalid job header_base: {err:#}"));
            }
            return Ok(());
        }
    };
    let target = match parse_target(&job.target) {
        Ok(target) => target,
        Err(err) => {
            if mode.is_user() {
                warn("JOB", format!("invalid job target: {err:#}"));
            }
            return Ok(());
        }
    };
    let network_target = match job.network_target.as_deref() {
        Some(value) => match parse_target(value) {
            Ok(target) => Some(target),
            Err(err) => {
                if mode.is_user() {
                    warn("JOB", format!("invalid job network_target: {err:#}"));
                }
                return Ok(());
            }
        },
        None => None,
    };

    if let Some(current) = active_job.as_mut() {
        if same_pool_template_identity(current, &job, header_base.as_ref(), network_target.as_ref())
        {
            rebind_active_pool_job(mode, current, job, target, network_target);
            set_tui_state_label(tui, "working");
            render_tui_now(tui);
            return Ok(());
        }
    }

    let mut cancel_latency = None;
    if cancel_prior_work {
        let cancel_started = Instant::now();
        if let Err(err) =
            super::cancel_backend_slots(backends, RuntimeMode::Mining, backend_executor)
        {
            warn(
                "BACKEND",
                format!("failed cancelling prior pool work: {err:#}"),
            );
        }
        cancel_latency = Some(cancel_started.elapsed());
    }

    stats.bump_templates();
    let height = job.height;
    let difficulty_label = job
        .difficulty
        .map(|difficulty| difficulty.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    *active_job = Some(ActivePoolJob::new(job, 0, header_base, target, network_target));

    if mode.is_user() {
        info(
            "JOB",
            format!("new job height={height} difficulty={difficulty_label}"),
        );
    }
    if let Some(job) = active_job.as_mut() {
        let assign_started = Instant::now();
        restart_pool_assignment(
            cfg,
            work_id_cursor,
            epoch_cursor,
            backends,
            backend_executor,
            backend_weights,
            job,
        )?;
        record_pool_assignment_timing(
            mode,
            job,
            "new-job",
            cancel_latency,
            assign_started.elapsed(),
        );
    }
    set_tui_state_label(tui, "working");
    render_tui_now(tui);

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn restart_pool_assignment(
    cfg: &Config,
    work_id_cursor: &mut u64,
    epoch_cursor: &mut u64,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
    backend_weights: &mut BTreeMap<u64, f64>,
    active_job: &mut ActivePoolJob,
) -> Result<()> {
    active_job.dispatch_nonce = active_job.next_nonce;
    assign_pool_continuation(
        cfg,
        work_id_cursor,
        epoch_cursor,
        backends,
        backend_executor,
        backend_weights,
        active_job,
    )
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
    if active_job.dispatch_nonce > active_job.job.nonce_end {
        return Ok(());
    }

    *epoch_cursor = epoch_cursor.wrapping_add(1).max(1);
    let epoch = *epoch_cursor;
    active_job.epoch = epoch;
    let work_id = next_work_id(work_id_cursor);

    let remaining_span = active_job
        .job
        .nonce_end
        .saturating_sub(active_job.dispatch_nonce)
        .saturating_add(1);
    let lanes = total_lanes(backends).max(1);
    let max_iters_per_lane = div_ceil_u64(remaining_span, lanes).max(1);
    let reservation = NonceReservation {
        start_nonce: active_job.dispatch_nonce,
        max_iters_per_lane,
        reserved_span: remaining_span,
    };
    let stop_at = Instant::now() + POOL_JOB_STOP_AT_HORIZON;
    let distribution = distribute_work(
        backends,
        DistributeWorkOptions {
            epoch,
            work_id,
            header_base: Arc::clone(&active_job.header_base),
            target: active_job.target,
            dynamic_share_target: Some(Arc::clone(&active_job.dynamic_share_target)),
            pause_on_solution: false,
            reservation,
            stop_at,
            backend_weights: match cfg.work_allocation {
                WorkAllocation::Static => None,
                WorkAllocation::Adaptive => {
                    work_distribution_weights(cfg.work_allocation, backend_weights)
                }
            },
            strict_reservation: true,
        },
        backend_executor,
    )?;
    active_job.dispatch_nonce = active_job
        .dispatch_nonce
        .saturating_add(distribution.consumed_span);
    if distribution.additional_span_consumed > 0 {
        let message = format!(
            "pool assignment consumed extra nonce span outside reserved window ({} nonces)",
            distribution.additional_span_consumed
        );
        if distribution.additional_span_consumed < lanes {
            info("JOB", message);
        } else {
            warn("JOB", message);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn maybe_hot_rebalance_active_pool_job(
    mode: PoolConnectionMode,
    cfg: &Config,
    work_id_cursor: &mut u64,
    epoch_cursor: &mut u64,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
    backend_weights: &mut BTreeMap<u64, f64>,
    active_job: &mut Option<ActivePoolJob>,
    reason: &str,
) -> Result<()> {
    let Some(job) = active_job.as_ref() else {
        return Ok(());
    };
    if job.next_nonce > job.job.nonce_end || backends.is_empty() {
        return Ok(());
    }

    if super::backends_have_append_assignment_semantics(backends) {
        if let Err(err) =
            super::cancel_backend_slots(backends, RuntimeMode::Mining, backend_executor)
        {
            warn(
                "BACKEND",
                format!("failed to cancel append-assignment backends for hot rebalance: {err:#}"),
            );
            return Ok(());
        }
        if let Err(err) =
            super::quiesce_backend_slots(backends, RuntimeMode::Mining, backend_executor)
        {
            warn(
                "BACKEND",
                format!("failed to quiesce append-assignment backends for hot rebalance: {err:#}"),
            );
            return Ok(());
        }
        if backends.is_empty() {
            return Ok(());
        }
    }

    if let Some(job) = active_job.as_mut() {
        if job.next_nonce <= job.job.nonce_end {
            let assign_started = Instant::now();
            restart_pool_assignment(
                cfg,
                work_id_cursor,
                epoch_cursor,
                backends,
                backend_executor,
                backend_weights,
                job,
            )?;
            record_pool_assignment_timing(
                mode,
                job,
                "hot-rebalance",
                None,
                assign_started.elapsed(),
            );
            info("JOB", format!("{reason}; hot-rebalanced current pool job"));
        }
    }

    Ok(())
}

fn service_pool_submit_backlog(
    mode: PoolConnectionMode,
    pool_client: &PoolClient,
    job: &mut ActivePoolJob,
    recent_submit_lookup: &mut RecentPendingSubmitLookup,
    stats: &Stats,
) {
    service_pool_submit_backlog_with_submitter(
        mode,
        job,
        stats,
        |job_id, nonce, claimed_hash| pool_client.submit_share(job_id.to_string(), nonce, claimed_hash),
        recent_submit_lookup,
    );
}

fn service_pool_submit_backlog_with_submitter<F>(
    mode: PoolConnectionMode,
    job: &mut ActivePoolJob,
    stats: &Stats,
    mut submitter: F,
    recent_submit_lookup: &mut RecentPendingSubmitLookup,
) where
    F: FnMut(&str, u64, Option<[u8; 32]>) -> Result<()>,
{
    let now = Instant::now();
    let timed_out = reap_timed_out_pending_submits(job, now);
    if timed_out > 0 && mode.is_user() {
        let message = format!(
            "{timed_out} pending submit acknowledgement(s) exceeded {}s; keeping dedupe state, releasing inflight slots, and continuing to match late ACKs",
            POOL_SUBMIT_ACK_TIMEOUT.as_secs()
        );
        if timed_out >= POOL_SUBMIT_ACK_WARN_THRESHOLD || !job.deferred_submits.is_empty() {
            warn("SHARE", message);
        } else {
            info("SHARE", message);
        }
    }

    let (flushed, submit_failed) =
        flush_deferred_pool_submits(job, stats, &mut submitter, recent_submit_lookup);
    if flushed > 0 && mode.is_user() {
        info("SHARE", format!("submitted {flushed} deferred share(s)"));
    }
    if submit_failed && mode.is_user() {
        warn("SHARE", "failed to queue deferred submit");
    }
}

fn reap_timed_out_pending_submits(job: &mut ActivePoolJob, now: Instant) -> usize {
    let mut timed_out = Vec::new();
    for (key, pending_submit) in &job.pending_submit_nonces {
        if now.saturating_duration_since(pending_submit.submitted_at) >= POOL_SUBMIT_ACK_TIMEOUT {
            timed_out.push(key.clone());
        }
    }
    for key in &timed_out {
        job.pending_submit_nonces.remove(key);
    }
    timed_out.len()
}

fn enqueue_deferred_pool_submit(
    mode: PoolConnectionMode,
    job: &mut ActivePoolJob,
    share_binding_id: ShareBindingId,
    nonce: u64,
    claimed_hash: Option<[u8; 32]>,
    found_at: Instant,
    backend_label: &str,
    stats: &Stats,
) -> bool {
    if job.deferred_submits.len() >= POOL_MAX_DEFERRED_SUBMITS {
        stats.add_dropped(1);
        if mode.is_user() {
            warn(
                "SHARE",
                format!(
                    "deferred submit queue full ({}); dropping share nonce={nonce} (backend={backend_label})",
                    POOL_MAX_DEFERRED_SUBMITS,
                ),
            );
        }
        return false;
    }

    job.deferred_submits.push_back(DeferredPoolSubmit {
        share_binding_id,
        nonce,
        claimed_hash,
        found_at,
        backend_label: backend_label.to_string(),
    });
    stats.add_deferred(1);
    true
}

fn flush_deferred_pool_submits<F>(
    job: &mut ActivePoolJob,
    stats: &Stats,
    mut submitter: F,
    recent_submit_lookup: &mut RecentPendingSubmitLookup,
) -> (u64, bool)
where
    F: FnMut(&str, u64, Option<[u8; 32]>) -> Result<()>,
{
    let mut flushed = 0u64;
    let mut submit_failed = false;
    while job.pending_submit_nonces.len() < POOL_MAX_INFLIGHT_SUBMITS {
        let Some(deferred) = job.deferred_submits.pop_front() else {
            break;
        };
        let Some(binding) = job.share_binding(deferred.share_binding_id).cloned() else {
            stats.add_dropped(1);
            continue;
        };
        match submitter(binding.job_id.as_str(), deferred.nonce, deferred.claimed_hash) {
            Ok(()) => {
                job.pending_submit_nonces.insert(
                    (binding.job_id.clone(), deferred.nonce),
                    PendingPoolSubmit {
                        found_at: deferred.found_at,
                        submitted_at: Instant::now(),
                        backend_label: deferred.backend_label,
                    },
                );
                if let Some(pending_submit) = job
                    .pending_submit_nonces
                    .get(&(binding.job_id.clone(), deferred.nonce))
                {
                    recent_submit_lookup.insert(
                        binding.job_id.as_str(),
                        deferred.nonce,
                        pending_submit.clone(),
                    );
                }
                stats.bump_submitted();
                flushed = flushed.saturating_add(1);
            }
            Err(_) => {
                job.deferred_submits.push_front(deferred);
                submit_failed = true;
                break;
            }
        }
    }
    (flushed, submit_failed)
}

fn submit_pool_solution(
    mode: PoolConnectionMode,
    pool_client: &PoolClient,
    active_job: &mut Option<ActivePoolJob>,
    solution: &MiningSolution,
    backend_label: &str,
    recent_submit_lookup: &mut RecentPendingSubmitLookup,
    stats: &Stats,
) -> PoolShareSubmitOutcome {
    submit_pool_solution_with_submitter(
        mode,
        active_job,
        solution,
        backend_label,
        stats,
        |job_id, nonce, claimed_hash| {
            pool_client.submit_share(job_id.to_string(), nonce, claimed_hash)
        },
        recent_submit_lookup,
    )
}

fn submit_pool_solution_with_submitter<F>(
    mode: PoolConnectionMode,
    active_job: &mut Option<ActivePoolJob>,
    solution: &MiningSolution,
    backend_label: &str,
    stats: &Stats,
    mut submitter: F,
    recent_submit_lookup: &mut RecentPendingSubmitLookup,
) -> PoolShareSubmitOutcome
where
    F: FnMut(&str, u64, Option<[u8; 32]>) -> Result<()>,
{
    let Some(job) = active_job.as_mut() else {
        return PoolShareSubmitOutcome::StaleEpoch;
    };
    let found_at = Instant::now();
    if solution.epoch != job.epoch {
        return PoolShareSubmitOutcome::StaleEpoch;
    }
    let Some(binding) = job.share_binding(solution.share_binding_id).cloned() else {
        return PoolShareSubmitOutcome::StaleEpoch;
    };
    advance_pool_nonce_cursor(job, solution.nonce);

    service_pool_submit_backlog_with_submitter(
        mode,
        job,
        stats,
        |job_id, nonce, claimed_hash| submitter(job_id, nonce, claimed_hash),
        recent_submit_lookup,
    );
    if !job.submitted_nonces.insert(solution.nonce) {
        return PoolShareSubmitOutcome::Duplicate;
    }

    if job.pending_submit_nonces.len() >= POOL_MAX_INFLIGHT_SUBMITS {
        if !enqueue_deferred_pool_submit(
            mode,
            job,
            solution.share_binding_id,
            solution.nonce,
            solution.hash,
            found_at,
            backend_label,
            stats,
        ) {
            // Keep dedupe marker even when dropped to prevent duplicate share spam.
        }
        return PoolShareSubmitOutcome::Deferred;
    }

    if submitter(binding.job_id.as_str(), solution.nonce, solution.hash).is_ok() {
        stats.bump_submitted();
        if mode.is_user() {
            info("SHARE", format!("submitted (backend={backend_label})"));
        }
        job.pending_submit_nonces.insert(
            (binding.job_id.clone(), solution.nonce),
            PendingPoolSubmit {
                found_at,
                submitted_at: Instant::now(),
                backend_label: backend_label.to_string(),
            },
        );
        if let Some(pending_submit) = job
            .pending_submit_nonces
            .get(&(binding.job_id.clone(), solution.nonce))
        {
            recent_submit_lookup.insert(
                binding.job_id.as_str(),
                solution.nonce,
                pending_submit.clone(),
            );
        }
        PoolShareSubmitOutcome::Submitted
    } else {
        job.submitted_nonces.remove(&solution.nonce);
        if mode.is_user() {
            warn(
                "SHARE",
                format!("failed to queue submit (backend={backend_label})"),
            );
        }
        PoolShareSubmitOutcome::QueueFailed
    }
}

impl PoolUiTelemetryClient {
    fn new(pool_url: &str, address: &str) -> Option<Self> {
        let base_urls = pool_api_base_urls_from_pool_url(pool_url);
        if base_urls.is_empty() {
            return None;
        }
        let http = HttpClient::builder()
            .timeout(POOL_TELEMETRY_TIMEOUT)
            .build()
            .ok()?;
        let endpoints = base_urls
            .into_iter()
            .map(|base_url| PoolTelemetryEndpoint {
                stats_url: format!("{base_url}/api/stats"),
                miner_balance_url: format!(
                    "{base_url}/api/miner/{address}/balance?include_pending_estimate=false"
                ),
                miner_url: format!("{base_url}/api/miner/{address}?include_pending_estimate=false"),
            })
            .collect::<Vec<_>>();
        Some(Self { endpoints, http })
    }

    fn fetch_pool_hashrate(&self) -> Result<String> {
        let mut errors = Vec::new();
        for endpoint in &self.endpoints {
            let body: Value = match self
                .http
                .get(&endpoint.stats_url)
                .send()
                .and_then(|resp| resp.json())
            {
                Ok(body) => body,
                Err(err) => {
                    errors.push(format!("GET {} failed: {err}", endpoint.stats_url));
                    continue;
                }
            };

            let hashrate = body
                .pointer("/chain/network_hashrate")
                .and_then(value_as_f64)
                .or_else(|| body.pointer("/network_hashrate").and_then(value_as_f64));
            let Some(hashrate) = hashrate else {
                errors.push(format!(
                    "GET {} missing network hashrate field",
                    endpoint.stats_url
                ));
                continue;
            };
            if !hashrate.is_finite() || hashrate < 0.0 {
                errors.push(format!(
                    "GET {} invalid network hashrate value",
                    endpoint.stats_url
                ));
                continue;
            }

            return Ok(format_hashrate_ui(hashrate));
        }

        bail!("pool hashrate telemetry unavailable: {}", errors.join("; "));
    }

    fn fetch_pool_balances(&self) -> Result<(String, String)> {
        let mut errors = Vec::new();
        for endpoint in &self.endpoints {
            match self.fetch_balance_from_url(&endpoint.miner_balance_url) {
                Ok(value) => return Ok(value),
                Err(err) => errors.push(format!(
                    "GET {} failed: {err:#}",
                    endpoint.miner_balance_url
                )),
            }

            match self.fetch_balance_from_url(&endpoint.miner_url) {
                Ok(value) => return Ok(value),
                Err(err) => errors.push(format!("GET {} failed: {err:#}", endpoint.miner_url)),
            }
        }

        bail!("pool balance telemetry unavailable: {}", errors.join("; "))
    }

    fn fetch_balance_from_url(&self, url: &str) -> Result<(String, String)> {
        let body: Value = self.http.get(url).send().and_then(|resp| resp.json())?;
        let pending = body
            .pointer("/balance/pending_confirmed")
            .and_then(Value::as_u64)
            .or_else(|| body.pointer("/balance/pending").and_then(Value::as_u64));
        let paid = body.pointer("/balance/paid").and_then(Value::as_u64);
        let (Some(pending), Some(paid)) = (pending, paid) else {
            bail!("missing balance fields");
        };
        Ok((
            format_atomic_units_bnt(pending),
            format_atomic_units_bnt(paid),
        ))
    }
}

fn fetch_local_daemon_wallet_snapshot(
    client: &ApiClient,
    cookie_path: Option<&PathBuf>,
) -> Result<LocalDaemonWalletSnapshot> {
    match fetch_local_daemon_wallet_snapshot_once(client) {
        Ok(snapshot) => Ok(snapshot),
        Err(err) if is_unauthorized_error(&err) => {
            match refresh_api_token_from_cookie(client, cookie_path.map(PathBuf::as_path)) {
                TokenRefreshOutcome::Refreshed | TokenRefreshOutcome::Unchanged => {
                    fetch_local_daemon_wallet_snapshot_once(client)
                }
                TokenRefreshOutcome::Unavailable => Err(err),
                TokenRefreshOutcome::Failed(message) => {
                    bail!("failed to refresh local daemon auth from cookie: {message}");
                }
            }
        }
        Err(err) => Err(err),
    }
}

fn fetch_local_daemon_wallet_snapshot_once(
    client: &ApiClient,
) -> Result<LocalDaemonWalletSnapshot> {
    let address = client.get_wallet_address()?.address;
    let balance = client.get_wallet_balance()?;
    Ok(LocalDaemonWalletSnapshot {
        address,
        pending: format_atomic_units_bnt(balance.pending),
        unlocked: format_atomic_units_bnt(balance.spendable),
    })
}

fn div_ceil_u64(value: u64, divisor: u64) -> u64 {
    let divisor = divisor.max(1);
    (value.saturating_add(divisor - 1)) / divisor
}

fn pool_api_base_urls_from_pool_url(pool_url: &str) -> Vec<String> {
    let trimmed = pool_url.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let (transport, rest) = if let Some(rest) = trimmed.strip_prefix("stratum+tcp://") {
        ("stratum+tcp", rest)
    } else if let Some(rest) = trimmed.strip_prefix("stratum+ssl://") {
        ("stratum+ssl", rest)
    } else if let Some(rest) = trimmed.strip_prefix("stratum+tls://") {
        ("stratum+tls", rest)
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        ("https", rest)
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        ("http", rest)
    } else {
        ("unknown", trimmed)
    };
    let authority = rest.split('/').next().unwrap_or(rest).trim();
    if authority.is_empty() {
        return Vec::new();
    }

    let host = if authority.starts_with('[') {
        let Some(closing) = authority.find(']') else {
            return Vec::new();
        };
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
        return Vec::new();
    }

    let mut out = Vec::new();
    match transport {
        "https" => push_unique(&mut out, format!("https://{authority}")),
        "http" => push_unique(&mut out, format!("http://{authority}")),
        _ => {
            push_unique(&mut out, format!("https://{host}"));
            if !telemetry_https_only_host(&host) {
                push_unique(&mut out, format!("http://{host}"));
            }
        }
    }
    out
}

fn telemetry_https_only_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("bntpool.com")
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
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
    let chars = trimmed.chars().collect::<Vec<_>>();
    if chars.len() <= KEEP * 2 + 3 {
        return trimmed.to_string();
    }
    let head = chars.iter().take(KEEP).collect::<String>();
    let tail = chars
        .iter()
        .skip(chars.len().saturating_sub(KEEP))
        .collect::<String>();
    format!("{head}...{tail}")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::backend::MiningSolution;
    use crate::daemon_api::ApiClient;
    use crate::miner::stats::Stats;
    use httpmock::prelude::*;
    use serde_json::json;

    use super::{
        advance_pool_nonce_cursor, compact_pool_address_for_log,
        fetch_local_daemon_wallet_snapshot, note_current_job_stale_submit_rejection,
        parse_pool_stale_for, pool_api_base_urls_from_pool_url,
        service_pool_submit_backlog_with_submitter, should_apply_submit_ack_difficulty_immediately,
        should_resume_pool_assignment, submit_pool_solution_with_submitter,
        take_submit_timing_for_ack, ActivePoolJob, PendingPoolSubmit, PoolConnectionMode, PoolJob,
        PoolShareSubmitOutcome, PoolUiTelemetryClient, RecentPendingSubmitLookup,
        POOL_DESYNC_STALE_ACK_THRESHOLD, POOL_DESYNC_STALE_ACK_WINDOW, POOL_MAX_INFLIGHT_SUBMITS,
        POOL_SUBMIT_ACK_TIMEOUT,
    };

    fn test_daemon_client(server: &MockServer, token: &str) -> ApiClient {
        ApiClient::new(
            server.url("").trim_end_matches('/').to_string(),
            token.to_string(),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .expect("test daemon client should be created")
    }

    fn unique_temp_dir() -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("seine-pool-test-{nanos}"))
    }

    fn test_active_job(epoch: u64) -> ActivePoolJob {
        ActivePoolJob::new(
            PoolJob {
                job_id: "job-1".to_string(),
                template_id: Some("tmpl-1".to_string()),
                header_base: "00".to_string(),
                target: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    .to_string(),
                difficulty: Some(1),
                network_target: Some(
                    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                        .to_string(),
                ),
                height: 1,
                nonce_start: 0,
                nonce_end: 1_000,
            },
            epoch,
            Arc::<[u8]>::from(vec![0u8; 1]),
            [0xFF; 32],
            Some([0xFF; 32]),
        )
    }

    fn test_solution(epoch: u64, nonce: u64) -> MiningSolution {
        MiningSolution {
            epoch,
            nonce,
            hash: Some([0xAA; 32]),
            share_binding_id: 1,
            backend_id: 1,
            backend: "cpu",
        }
    }

    #[test]
    fn telemetry_base_urls_use_https_only_for_known_public_pool() {
        let urls = pool_api_base_urls_from_pool_url("stratum+tcp://bntpool.com:3333");
        assert_eq!(urls, vec!["https://bntpool.com".to_string()]);
    }

    #[test]
    fn telemetry_base_urls_keep_http_fallback_for_generic_stratum_endpoint() {
        let urls = pool_api_base_urls_from_pool_url("stratum+tcp://example.com:3333");
        assert_eq!(
            urls,
            vec![
                "https://example.com".to_string(),
                "http://example.com".to_string()
            ]
        );
    }

    #[test]
    fn telemetry_base_urls_preserve_https_origin() {
        let urls = pool_api_base_urls_from_pool_url("https://example.com:8443/path");
        assert_eq!(urls, vec!["https://example.com:8443".to_string()]);
    }

    #[test]
    fn telemetry_balance_url_skips_pending_estimate_query() {
        let client = PoolUiTelemetryClient::new("stratum+tcp://bntpool.com:3333", "addr")
            .expect("telemetry client should be created");
        assert_eq!(
            client.endpoints[0].miner_balance_url,
            "https://bntpool.com/api/miner/addr/balance?include_pending_estimate=false"
        );
    }

    #[test]
    fn telemetry_miner_fallback_url_skips_pending_estimate_query() {
        let client = PoolUiTelemetryClient::new("stratum+tcp://bntpool.com:3333", "addr")
            .expect("telemetry client should be created");
        assert_eq!(
            client.endpoints[0].miner_url,
            "https://bntpool.com/api/miner/addr?include_pending_estimate=false"
        );
    }

    #[test]
    fn local_daemon_wallet_snapshot_reads_address_and_balance() {
        let server = MockServer::start();
        let addr_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/api/wallet/address")
                .header("authorization", "Bearer testtoken");
            then.status(200)
                .json_body(json!({ "address": "Pwallet123" }));
        });
        let balance_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/api/wallet/balance")
                .header("authorization", "Bearer testtoken");
            then.status(200).json_body(json!({
                "pending": 450_000_000u64,
                "spendable": 123_000_000u64
            }));
        });

        let client = test_daemon_client(&server, "testtoken");
        let snapshot =
            fetch_local_daemon_wallet_snapshot(&client, None).expect("wallet snapshot expected");

        assert_eq!(snapshot.address, "Pwallet123");
        assert_eq!(snapshot.pending, "4.5 BNT");
        assert_eq!(snapshot.unlocked, "1.23 BNT");
        addr_mock.assert_hits(1);
        balance_mock.assert_hits(1);
    }

    #[test]
    fn local_daemon_wallet_snapshot_refreshes_cookie_after_unauthorized() {
        let server = MockServer::start();
        let stale_addr = server.mock(|when, then| {
            when.method(GET)
                .path("/api/wallet/address")
                .header("authorization", "Bearer stale-token");
            then.status(401)
                .json_body(json!({ "error": "unauthorized" }));
        });
        let fresh_addr = server.mock(|when, then| {
            when.method(GET)
                .path("/api/wallet/address")
                .header("authorization", "Bearer fresh-token");
            then.status(200)
                .json_body(json!({ "address": "Pwallet456" }));
        });
        let fresh_balance = server.mock(|when, then| {
            when.method(GET)
                .path("/api/wallet/balance")
                .header("authorization", "Bearer fresh-token");
            then.status(200).json_body(json!({
                "pending": 10_000u64,
                "spendable": 250_000_000u64
            }));
        });

        let dir = unique_temp_dir();
        fs::create_dir_all(&dir).expect("temp dir should be created");
        let cookie_path = dir.join("api.cookie");
        fs::write(&cookie_path, "fresh-token\n").expect("cookie should be written");

        let client = test_daemon_client(&server, "stale-token");
        let snapshot = fetch_local_daemon_wallet_snapshot(&client, Some(&cookie_path))
            .expect("wallet snapshot should refresh auth");

        assert_eq!(snapshot.address, "Pwallet456");
        assert_eq!(snapshot.pending, "0.0001 BNT");
        assert_eq!(snapshot.unlocked, "2.5 BNT");
        stale_addr.assert_hits(1);
        fresh_addr.assert_hits(1);
        fresh_balance.assert_hits(1);

        let _ = fs::remove_file(cookie_path);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn compact_pool_address_handles_unicode_without_panicking() {
        let input = "矿工地址测试1234567890abcdef";
        let compacted = compact_pool_address_for_log(input);
        assert!(compacted.contains("..."));
        assert!(compacted.starts_with("矿工地址测"));
    }

    #[test]
    fn compact_pool_address_keeps_short_values_as_is() {
        let input = "短地址";
        assert_eq!(compact_pool_address_for_log(input), input);
    }

    #[test]
    fn pool_assignment_resume_waits_for_idle_and_remaining_nonce_range() {
        let mut job = test_active_job(13);
        assert!(should_resume_pool_assignment(&job, false));
        assert!(!should_resume_pool_assignment(&job, true));

        job.dispatch_nonce = job.job.nonce_end.saturating_add(1);
        assert!(
            should_resume_pool_assignment(&job, false),
            "resume must key off next_nonce because dispatch_nonce can already cover the full window"
        );

        job.next_nonce = job.job.nonce_end.saturating_add(1);
        assert!(!should_resume_pool_assignment(&job, false));
    }

    #[test]
    fn nonce_cursor_advances_to_solution_nonce_plus_one() {
        let mut job = test_active_job(21);
        job.next_nonce = 10;
        job.dispatch_nonce = 99;
        advance_pool_nonce_cursor(&mut job, 42);
        assert_eq!(job.next_nonce, 43);
        assert_eq!(job.dispatch_nonce, 99);

        // Cursor should never move backward.
        advance_pool_nonce_cursor(&mut job, 11);
        assert_eq!(job.next_nonce, 43);
        assert_eq!(job.dispatch_nonce, 99);

        // Cap at assigned nonce range end + 1.
        job.job.nonce_end = 50;
        advance_pool_nonce_cursor(&mut job, 99);
        assert_eq!(job.next_nonce, 51);
        assert_eq!(job.dispatch_nonce, 99);
    }

    #[test]
    fn submit_pool_solution_defers_when_inflight_limit_is_saturated() {
        let stats = Stats::new();
        let mut active_job = Some(test_active_job(7));
        let mut recent_submit_lookup = RecentPendingSubmitLookup::default();
        {
            let now = Instant::now();
            let job = active_job.as_mut().expect("active job should exist");
            for nonce in 0..POOL_MAX_INFLIGHT_SUBMITS as u64 {
                job.pending_submit_nonces.insert(
                    ("job-1".to_string(), nonce),
                    PendingPoolSubmit {
                        found_at: now,
                        submitted_at: now,
                        backend_label: "cpu#1".to_string(),
                    },
                );
            }
        }

        let mut submit_calls = 0u64;
        let outcome = submit_pool_solution_with_submitter(
            PoolConnectionMode::Dev,
            &mut active_job,
            &test_solution(7, 99),
            "cpu#1",
            &stats,
            |_job_id, _nonce, _hash| {
                submit_calls = submit_calls.saturating_add(1);
                Ok(())
            },
            &mut recent_submit_lookup,
        );

        assert!(matches!(outcome, PoolShareSubmitOutcome::Deferred));
        assert_eq!(submit_calls, 0);
        let job = active_job.expect("active job should remain available");
        assert!(job.submitted_nonces.contains(&99));
        assert_eq!(job.deferred_submits.len(), 1);
        assert_eq!(job.pending_submit_nonces.len(), POOL_MAX_INFLIGHT_SUBMITS);
        assert_eq!(job.deferred_submits[0].backend_label, "cpu#1");
    }

    #[test]
    fn submit_pool_solution_deduplicates_nonce_and_keeps_cursor_progress() {
        let stats = Stats::new();
        let mut active_job = Some(test_active_job(9));
        let mut recent_submit_lookup = RecentPendingSubmitLookup::default();
        let initial_next_nonce = active_job
            .as_ref()
            .expect("active job should exist")
            .next_nonce;
        let mut submitted_nonces = Vec::new();

        let first = submit_pool_solution_with_submitter(
            PoolConnectionMode::Dev,
            &mut active_job,
            &test_solution(9, 42),
            "cpu#1",
            &stats,
            |_job_id, nonce, _hash| {
                submitted_nonces.push(nonce);
                Ok(())
            },
            &mut recent_submit_lookup,
        );
        let second = submit_pool_solution_with_submitter(
            PoolConnectionMode::Dev,
            &mut active_job,
            &test_solution(9, 42),
            "cpu#1",
            &stats,
            |_job_id, nonce, _hash| {
                submitted_nonces.push(nonce);
                Ok(())
            },
            &mut recent_submit_lookup,
        );

        assert!(matches!(first, PoolShareSubmitOutcome::Submitted));
        assert!(matches!(second, PoolShareSubmitOutcome::Duplicate));
        assert_eq!(submitted_nonces, vec![42]);
        let job = active_job.expect("active job should remain available");
        assert_eq!(
            job.next_nonce,
            initial_next_nonce.max(43),
            "cursor should advance once and remain stable on duplicate"
        );
        assert_eq!(
            job.dispatch_nonce, initial_next_nonce,
            "share submission must not rewind or advance the dispatch cursor"
        );
        assert!(job
            .pending_submit_nonces
            .contains_key(&("job-1".to_string(), 42)));
        assert_eq!(
            job.pending_submit_nonces
                .get(&("job-1".to_string(), 42))
                .expect("pending share should keep backend label")
                .backend_label,
            "cpu#1"
        );
        assert_eq!(
            recent_submit_lookup
                .remove("job-1", 42)
                .expect("recent submit lookup should retain backend label")
                .backend_label,
            "cpu#1"
        );
    }

    #[test]
    fn timed_out_pending_submit_releases_slot_without_clearing_dedupe() {
        let stats = Stats::new();
        let mut job = test_active_job(11);
        let mut recent_submit_lookup = RecentPendingSubmitLookup::default();
        let now = Instant::now();
        let timed_out_nonce = 7u64;
        let healthy_nonce = 8u64;
        let deferred_nonce = 77u64;

        job.submitted_nonces.insert(timed_out_nonce);
        job.submitted_nonces.insert(healthy_nonce);
        job.submitted_nonces.insert(deferred_nonce);
        job.pending_submit_nonces.insert(
            ("job-1".to_string(), timed_out_nonce),
            PendingPoolSubmit {
                found_at: now - (POOL_SUBMIT_ACK_TIMEOUT + Duration::from_secs(2)),
                submitted_at: now - (POOL_SUBMIT_ACK_TIMEOUT + Duration::from_secs(1)),
                backend_label: "cpu#1".to_string(),
            },
        );
        job.pending_submit_nonces.insert(
            ("job-1".to_string(), healthy_nonce),
            PendingPoolSubmit {
                found_at: now,
                submitted_at: now,
                backend_label: "cpu#2".to_string(),
            },
        );
        job.deferred_submits.push_back(super::DeferredPoolSubmit {
            share_binding_id: 1,
            nonce: deferred_nonce,
            claimed_hash: Some([0xAB; 32]),
            found_at: now,
            backend_label: "cpu#3".to_string(),
        });

        let mut submitted = Vec::new();
        service_pool_submit_backlog_with_submitter(
            PoolConnectionMode::Dev,
            &mut job,
            &stats,
            |_job_id, nonce, _hash| {
                submitted.push(nonce);
                Ok(())
            },
            &mut recent_submit_lookup,
        );

        assert_eq!(submitted, vec![deferred_nonce]);
        assert!(!job
            .pending_submit_nonces
            .contains_key(&("job-1".to_string(), timed_out_nonce)));
        assert!(job
            .pending_submit_nonces
            .contains_key(&("job-1".to_string(), healthy_nonce)));
        assert!(job
            .pending_submit_nonces
            .contains_key(&("job-1".to_string(), deferred_nonce)));
        assert!(job.submitted_nonces.contains(&timed_out_nonce));
        assert_eq!(
            job.pending_submit_nonces
                .get(&("job-1".to_string(), deferred_nonce))
                .expect("deferred share should move into pending state")
                .backend_label,
            "cpu#3"
        );
        assert_eq!(
            recent_submit_lookup
                .remove("job-1", deferred_nonce)
                .expect("deferred share should stay discoverable after flush")
                .backend_label,
            "cpu#3"
        );
    }

    #[test]
    fn take_submit_timing_for_ack_recovers_backend_after_job_switch() {
        let now = Instant::now();
        let mut recent_submit_lookup = RecentPendingSubmitLookup::default();
        recent_submit_lookup.insert(
            "job-1",
            42,
            PendingPoolSubmit {
                found_at: now,
                submitted_at: now,
                backend_label: "nvidia#1".to_string(),
            },
        );
        let mut current_pending_submit_nonces = HashMap::new();

        let (ack_for_current_job, submit_timing) = take_submit_timing_for_ack(
            "job-1",
            42,
            false,
            Some(&mut current_pending_submit_nonces),
            &mut recent_submit_lookup,
        );

        assert!(!ack_for_current_job);
        assert_eq!(
            submit_timing
                .expect("late ack should recover backend metadata from lookup")
                .backend_label,
            "nvidia#1"
        );
    }

    #[test]
    fn take_submit_timing_for_ack_recovers_backend_after_pending_timeout() {
        let now = Instant::now();
        let mut recent_submit_lookup = RecentPendingSubmitLookup::default();
        recent_submit_lookup.insert(
            "job-1",
            7,
            PendingPoolSubmit {
                found_at: now,
                submitted_at: now,
                backend_label: "cpu#1".to_string(),
            },
        );
        let mut current_pending_submit_nonces = HashMap::new();

        let (ack_for_current_job, submit_timing) = take_submit_timing_for_ack(
            "job-1",
            7,
            true,
            Some(&mut current_pending_submit_nonces),
            &mut recent_submit_lookup,
        );

        assert!(ack_for_current_job);
        assert_eq!(
            submit_timing
                .expect("timed-out ack should still recover backend metadata")
                .backend_label,
            "cpu#1"
        );
    }

    #[test]
    fn submit_ack_difficulty_waits_for_replacement_job() {
        assert!(!should_apply_submit_ack_difficulty_immediately(
            PoolConnectionMode::Dev
        ));
        assert!(!should_apply_submit_ack_difficulty_immediately(
            PoolConnectionMode::User
        ));
    }

    #[test]
    fn parse_pool_stale_for_extracts_seconds() {
        let stale_for = parse_pool_stale_for(
            "stale job: template abc exceeded stale grace (current=def, stale_for=42.125s, stale_grace=8.000s)",
        )
        .expect("stale_for should parse");
        assert!((stale_for.as_secs_f64() - 42.125).abs() < 0.000_001);
        assert!(parse_pool_stale_for("stale job: assignment not found").is_none());
    }

    #[test]
    fn stale_rejection_triggers_resync_when_assignment_is_missing() {
        let now = Instant::now();
        let mut job = test_active_job(1);
        let reason = note_current_job_stale_submit_rejection(
            &mut job,
            "stale job: assignment not found",
            now,
        )
        .expect("assignment miss should trigger resync");
        assert!(reason.contains("no longer recognizes"));
        assert!(job.recent_stale_submit_acks.is_empty());
    }

    #[test]
    fn stale_rejection_triggers_resync_on_large_stale_for() {
        let now = Instant::now();
        let mut job = test_active_job(1);
        let reason = note_current_job_stale_submit_rejection(
            &mut job,
            "stale job: assignment superseded by newer difficulty (stale_for=19.500s, stale_grace=8.000s)",
            now,
        )
        .expect("large stale_for should trigger resync");
        assert!(reason.contains("stale_for=19.500s"));
    }

    #[test]
    fn stale_rejection_triggers_resync_after_burst() {
        let base = Instant::now();
        let mut job = test_active_job(1);
        let reason_text =
            "stale job: assignment superseded by newer difficulty (stale_for=6.000s, stale_grace=8.000s)";

        for idx in 0..POOL_DESYNC_STALE_ACK_THRESHOLD - 1 {
            let reason = note_current_job_stale_submit_rejection(
                &mut job,
                reason_text,
                base + Duration::from_secs(idx as u64),
            );
            assert!(reason.is_none(), "threshold should not trigger early");
        }

        let reason = note_current_job_stale_submit_rejection(
            &mut job,
            reason_text,
            base + Duration::from_secs((POOL_DESYNC_STALE_ACK_THRESHOLD - 1) as u64),
        )
        .expect("burst threshold should trigger resync");
        assert!(reason.contains(&POOL_DESYNC_STALE_ACK_WINDOW.as_secs().to_string()));
    }
}
