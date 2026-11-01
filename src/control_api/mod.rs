use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use async_stream::stream;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

use crate::config::{
    read_token_from_cookie_file, BackendKind, BackendSpec, Config, CpuAffinityMode, CpuPageMode,
    CpuPerformanceProfile, MiningMode, UiMode, WorkAllocation,
};
use crate::dev_fee::{DEV_ADDRESS, DEV_FEE_PERCENT};
use crate::miner::session::SessionHistory;
use crate::miner::thread_control::{ThreadChangeRequest, ThreadControl};
use crate::miner::ui::{set_log_sink, UiLogEvent};
use crate::shortname::validate_destination;

const API_EVENT_CHANNEL_CAPACITY: usize = 4096;
const RECENT_LOG_CAPACITY: usize = 200;
const API_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize)]
struct ApiErrorBody {
    code: &'static str,
    message: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "bad_request",
            message: message.into(),
        }
    }

    fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "forbidden",
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(ApiErrorBody {
                code: self.code,
                message: self.message,
            }),
        )
            .into_response()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum MinerLifecycle {
    Idle,
    Starting,
    Running,
    Stopping,
    Error,
}

#[derive(Debug, Clone, Serialize)]
struct StreamEnvelope {
    seq: u64,
    ts_unix_ms: u64,
    event: String,
    data: Value,
}

#[derive(Debug, Clone, Default, Serialize)]
struct RuntimeMetrics {
    start_requests: u64,
    stop_requests: u64,
    restart_requests: u64,
    live_config_patches: u64,
    wallet_unlock_requests: u64,
    log_events: u64,
    accepted_blocks: u64,
    submitted_blocks: u64,
    stale_submits: u64,
    backend_quarantines: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ApiConfigView {
    api_server_enabled: bool,
    service_mode: bool,
    api_bind: String,
    api_allow_unsafe_bind: bool,
    api_cors: String,
    api_key_required: bool,
    api_cookie_path: Option<String>,
    mode: String,
    api_url: String,
    pool_url: Option<String>,
    pool_worker: Option<String>,
    has_token: bool,
    cookie_path: Option<String>,
    wallet_password_present: bool,
    mining_address: Option<String>,
    backend_specs: Vec<BackendSpecView>,
    threads: usize,
    cpu_profile: String,
    cpu_affinity: String,
    cpu_page_mode: String,
    refresh_secs: u64,
    request_timeout_secs: u64,
    events_stream_timeout_secs: u64,
    events_idle_timeout_secs: u64,
    stats_secs: u64,
    hash_poll_ms: u64,
    strict_round_accounting: bool,
    start_nonce: u64,
    nonce_iters_per_lane: u64,
    work_allocation: String,
    sub_round_rebalance_ms: Option<u64>,
    sse_enabled: bool,
    refresh_on_same_height: bool,
    ui_mode: String,
    cpu_autotune_threads: bool,
    cpu_autotune_min_threads: usize,
    cpu_autotune_max_threads: Option<usize>,
    cpu_autotune_secs: u64,
    nvidia_autotune_secs: u64,
    nvidia_autotune_samples: u32,
    nvidia_max_rregcount: Option<u32>,
    nvidia_max_lanes: Option<usize>,
    nvidia_dispatch_iters_per_lane: Option<u64>,
    nvidia_allocation_iters_per_lane: Option<u64>,
    nvidia_hashes_per_launch_per_lane: u32,
    nvidia_fused_target_check: bool,
    nvidia_adaptive_launch_depth: bool,
    nvidia_enforce_template_stop: bool,
    metal_max_lanes: Option<usize>,
    metal_hashes_per_launch_per_lane: u32,
    backend_assign_timeout_ms: u64,
    backend_assign_timeout_strikes: u32,
    backend_control_timeout_ms: u64,
    allow_best_effort_deadlines: bool,
    prefetch_wait_ms: u64,
    tip_listener_join_wait_ms: u64,
    submit_join_wait_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
struct BackendSpecView {
    kind: String,
    device_index: Option<u32>,
    cpu_threads: Option<usize>,
    cpu_affinity: Option<String>,
    assign_timeout_ms_override: Option<u64>,
    control_timeout_ms_override: Option<u64>,
    assign_timeout_strikes_override: Option<u32>,
}

impl From<&Config> for ApiConfigView {
    fn from(cfg: &Config) -> Self {
        Self {
            api_server_enabled: cfg.api_server_enabled,
            service_mode: cfg.service_mode,
            api_bind: cfg.api_bind.clone(),
            api_allow_unsafe_bind: cfg.api_allow_unsafe_bind,
            api_cors: cfg.api_cors.clone(),
            api_key_required: true,
            api_cookie_path: cfg
                .api_token
                .is_none()
                .then(|| cfg.api_cookie_path.display().to_string()),
            mode: mining_mode_to_str(cfg.mode).to_string(),
            api_url: cfg.api_url.clone(),
            pool_url: cfg.pool_url.clone(),
            pool_worker: cfg.pool_worker.clone(),
            has_token: cfg.token.is_some(),
            cookie_path: cfg
                .token_cookie_path
                .as_ref()
                .map(|path| path.display().to_string()),
            wallet_password_present: cfg.wallet_password.is_some(),
            mining_address: cfg.mining_address.clone(),
            backend_specs: cfg
                .backend_specs
                .iter()
                .map(|spec| BackendSpecView {
                    kind: format!("{:?}", spec.kind).to_ascii_lowercase(),
                    device_index: spec.device_index,
                    cpu_threads: spec.cpu_threads,
                    cpu_affinity: spec
                        .cpu_affinity
                        .map(|mode| cpu_affinity_to_str(mode).to_string()),
                    assign_timeout_ms_override: spec
                        .assign_timeout_override
                        .map(|value| value.as_millis().min(u64::MAX as u128) as u64),
                    control_timeout_ms_override: spec
                        .control_timeout_override
                        .map(|value| value.as_millis().min(u64::MAX as u128) as u64),
                    assign_timeout_strikes_override: spec.assign_timeout_strikes_override,
                })
                .collect(),
            threads: cfg.threads,
            cpu_profile: cpu_profile_to_str(cfg.cpu_profile).to_string(),
            cpu_affinity: cpu_affinity_to_str(cfg.cpu_affinity).to_string(),
            cpu_page_mode: cfg.cpu_page_mode.as_str().to_string(),
            refresh_secs: cfg.refresh_interval.as_secs(),
            request_timeout_secs: cfg.request_timeout.as_secs(),
            events_stream_timeout_secs: cfg.events_stream_timeout.as_secs(),
            events_idle_timeout_secs: cfg.events_idle_timeout.as_secs(),
            stats_secs: cfg.stats_interval.as_secs(),
            hash_poll_ms: cfg.hash_poll_interval.as_millis().min(u64::MAX as u128) as u64,
            strict_round_accounting: cfg.strict_round_accounting,
            start_nonce: cfg.start_nonce,
            nonce_iters_per_lane: cfg.nonce_iters_per_lane,
            work_allocation: work_allocation_to_str(cfg.work_allocation).to_string(),
            sub_round_rebalance_ms: cfg
                .sub_round_rebalance_interval
                .map(|value| value.as_millis().min(u64::MAX as u128) as u64),
            sse_enabled: cfg.sse_enabled,
            refresh_on_same_height: cfg.refresh_on_same_height,
            ui_mode: ui_mode_to_str(cfg.ui_mode).to_string(),
            cpu_autotune_threads: cfg.cpu_autotune_threads,
            cpu_autotune_min_threads: cfg.cpu_autotune_min_threads,
            cpu_autotune_max_threads: cfg.cpu_autotune_max_threads,
            cpu_autotune_secs: cfg.cpu_autotune_secs,
            nvidia_autotune_secs: cfg.nvidia_autotune_secs,
            nvidia_autotune_samples: cfg.nvidia_autotune_samples,
            nvidia_max_rregcount: cfg.nvidia_max_rregcount,
            nvidia_max_lanes: cfg.nvidia_max_lanes,
            nvidia_dispatch_iters_per_lane: cfg.nvidia_dispatch_iters_per_lane,
            nvidia_allocation_iters_per_lane: cfg.nvidia_allocation_iters_per_lane,
            nvidia_hashes_per_launch_per_lane: cfg.nvidia_hashes_per_launch_per_lane,
            nvidia_fused_target_check: cfg.nvidia_fused_target_check,
            nvidia_adaptive_launch_depth: cfg.nvidia_adaptive_launch_depth,
            nvidia_enforce_template_stop: cfg.nvidia_enforce_template_stop,
            metal_max_lanes: cfg.metal_max_lanes,
            metal_hashes_per_launch_per_lane: cfg.metal_hashes_per_launch_per_lane,
            backend_assign_timeout_ms: cfg.backend_assign_timeout.as_millis().min(u64::MAX as u128)
                as u64,
            backend_assign_timeout_strikes: cfg.backend_assign_timeout_strikes,
            backend_control_timeout_ms: cfg
                .backend_control_timeout
                .as_millis()
                .min(u64::MAX as u128) as u64,
            allow_best_effort_deadlines: cfg.allow_best_effort_deadlines,
            prefetch_wait_ms: cfg.prefetch_wait.as_millis().min(u64::MAX as u128) as u64,
            tip_listener_join_wait_ms: cfg.tip_listener_join_wait.as_millis().min(u64::MAX as u128)
                as u64,
            submit_join_wait_ms: cfg.submit_join_wait.as_millis().min(u64::MAX as u128) as u64,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeStateSnapshot {
    lifecycle: MinerLifecycle,
    run_id: u64,
    running: bool,
    started_at_unix_ms: Option<u64>,
    stopped_at_unix_ms: Option<u64>,
    last_error: Option<String>,
    pending_wallet_password: bool,
    pending_nvidia_count: u64,
    dev_fee_percent: f64,
    dev_fee_mode: String,
    dev_fee_address: &'static str,
    effective_config: Option<ApiConfigView>,
    backend_phases: BTreeMap<String, String>,
    metrics: RuntimeMetrics,
    recent_logs: Vec<UiLogEventSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
struct UiLogEventSnapshot {
    elapsed_secs: f64,
    level: String,
    tag: String,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
struct ActionResponse {
    ok: bool,
    message: String,
    requires_restart: bool,
    state: RuntimeStateSnapshot,
}

#[derive(Debug, Clone)]
struct SupervisorState {
    lifecycle: MinerLifecycle,
    run_id: u64,
    started_at_unix_ms: Option<u64>,
    stopped_at_unix_ms: Option<u64>,
    last_error: Option<String>,
    session_shutdown: Option<Arc<AtomicBool>>,
    stop_origin: Arc<crate::diagnostics::StopOrigin>,
    session_history: Option<Arc<SessionHistory>>,
    effective_config: Option<Config>,
    staged_config: Option<Config>,
    pending_wallet_password: Option<String>,
    pending_nvidia_count: u64,
    dev_fee_mode: String,
    backend_phases: BTreeMap<String, String>,
    metrics: RuntimeMetrics,
    recent_logs: VecDeque<UiLogEventSnapshot>,
}

impl Default for SupervisorState {
    fn default() -> Self {
        Self {
            lifecycle: MinerLifecycle::Idle,
            run_id: 0,
            started_at_unix_ms: None,
            stopped_at_unix_ms: None,
            last_error: None,
            session_shutdown: None,
            stop_origin: Arc::default(),
            session_history: None,
            effective_config: None,
            staged_config: None,
            pending_wallet_password: None,
            pending_nvidia_count: 0,
            dev_fee_mode: "user".to_string(),
            backend_phases: BTreeMap::new(),
            metrics: RuntimeMetrics::default(),
            recent_logs: VecDeque::with_capacity(RECENT_LOG_CAPACITY),
        }
    }
}

struct MinerSupervisor {
    base_config: Config,
    process_shutdown: Arc<AtomicBool>,
    state: Arc<Mutex<SupervisorState>>,
    events_tx: broadcast::Sender<StreamEnvelope>,
    seq: Arc<AtomicU64>,
    // Serialize user operations so a queued TUI restart cannot stop an API-started session.
    operations: Mutex<()>,
    thread_changes_tx: crossbeam_channel::Sender<ThreadChangeRequest>,
    thread_changes_rx: crossbeam_channel::Receiver<ThreadChangeRequest>,
}

impl MinerSupervisor {
    fn new(base_config: Config, process_shutdown: Arc<AtomicBool>) -> Self {
        let (events_tx, _) = broadcast::channel(API_EVENT_CHANNEL_CAPACITY.max(64));
        let (thread_changes_tx, thread_changes_rx) = crossbeam_channel::bounded(1);
        Self {
            base_config,
            process_shutdown,
            state: Arc::new(Mutex::new(SupervisorState::default())),
            events_tx,
            seq: Arc::new(AtomicU64::new(0)),
            operations: Mutex::new(()),
            thread_changes_tx,
            thread_changes_rx,
        }
    }

    fn base_config(&self) -> &Config {
        &self.base_config
    }

    fn subscribe(&self) -> broadcast::Receiver<StreamEnvelope> {
        self.events_tx.subscribe()
    }

    fn snapshot(&self) -> RuntimeStateSnapshot {
        let state = self
            .state
            .lock()
            .expect("supervisor state lock poisoned during snapshot");
        RuntimeStateSnapshot {
            lifecycle: state.lifecycle,
            run_id: state.run_id,
            running: matches!(
                state.lifecycle,
                MinerLifecycle::Running | MinerLifecycle::Starting
            ),
            started_at_unix_ms: state.started_at_unix_ms,
            stopped_at_unix_ms: state.stopped_at_unix_ms,
            last_error: state.last_error.clone(),
            pending_wallet_password: state.pending_wallet_password.is_some(),
            pending_nvidia_count: state.pending_nvidia_count,
            dev_fee_percent: DEV_FEE_PERCENT,
            dev_fee_mode: state.dev_fee_mode.clone(),
            dev_fee_address: DEV_ADDRESS,
            effective_config: state
                .effective_config
                .as_ref()
                .or(state.staged_config.as_ref())
                .map(ApiConfigView::from),
            backend_phases: state.backend_phases.clone(),
            metrics: state.metrics.clone(),
            recent_logs: state.recent_logs.iter().cloned().collect(),
        }
    }

    fn ingest_log(&self, log: UiLogEvent) {
        let snapshot = UiLogEventSnapshot {
            elapsed_secs: log.elapsed_secs,
            level: log.level.to_string(),
            tag: log.tag.clone(),
            message: log.message.clone(),
        };

        let mut extra_events = Vec::<(&'static str, Value)>::new();
        {
            let mut state = self
                .state
                .lock()
                .expect("supervisor state lock poisoned while ingesting log");
            state.metrics.log_events = state.metrics.log_events.saturating_add(1);
            if state.recent_logs.len() >= RECENT_LOG_CAPACITY {
                state.recent_logs.pop_front();
            }
            state.recent_logs.push_back(snapshot.clone());

            if log.tag == "DEV FEE" {
                if log.message.contains("mining for dev") {
                    state.dev_fee_mode = "dev".to_string();
                    extra_events.push(("devfee.mode", json!({"mode": "dev"})));
                } else if log.message.contains("mining for user") {
                    state.dev_fee_mode = "user".to_string();
                    extra_events.push(("devfee.mode", json!({"mode": "user"})));
                }
            }

            if log.tag == "BACKEND" {
                if let Some(pending) = parse_pending_nvidia_count(&log.message) {
                    state.pending_nvidia_count = pending;
                    extra_events.push((
                        "nvidia.init.progress",
                        json!({
                            "pending_count": pending,
                            "message": log.message,
                        }),
                    ));
                }

                if let Some((backend_label, phase)) = parse_backend_phase(&log.message) {
                    state
                        .backend_phases
                        .insert(backend_label.clone(), phase.to_string());
                    extra_events.push((
                        "backend.phase",
                        json!({"backend": backend_label, "phase": phase}),
                    ));
                }

                if log.message.contains("quarantined") {
                    state.metrics.backend_quarantines =
                        state.metrics.backend_quarantines.saturating_add(1);
                }
            }

            if log.tag == "WALLET" && log.message.contains("ACTION REQUIRED") {
                extra_events.push(("wallet.required", json!({"message": log.message})));
            }
            if log.tag == "WALLET" && log.message.contains("loaded via") {
                extra_events.push(("wallet.loaded", json!({"message": log.message})));
            }
            if log.tag == "SOLVE" {
                extra_events.push(("solution.found", json!({"message": log.message})));
            }
            if log.tag == "SUBMIT" {
                state.metrics.submitted_blocks = state.metrics.submitted_blocks.saturating_add(1);
                if log.message.contains("stale") {
                    state.metrics.stale_submits = state.metrics.stale_submits.saturating_add(1);
                }
                extra_events.push(("submit.result", json!({"message": log.message})));
            }
            if log.tag == "ACCEPT" && log.message.contains("accepted") {
                state.metrics.accepted_blocks = state.metrics.accepted_blocks.saturating_add(1);
                extra_events.push((
                    "submit.result",
                    json!({"message": log.message, "accepted": true}),
                ));
            }
            if log.message.contains("autotune |") {
                extra_events.push((
                    "autotune.progress",
                    json!({"tag": log.tag, "message": log.message}),
                ));
                if log.message.contains("autotune | done") {
                    extra_events.push((
                        "autotune.completed",
                        json!({"tag": log.tag, "message": log.message}),
                    ));
                }
            }
        }

        self.emit_event(
            "miner.log",
            json!({
                "elapsed_secs": snapshot.elapsed_secs,
                "level": snapshot.level,
                "tag": snapshot.tag,
                "message": snapshot.message,
            }),
        );

        for (kind, payload) in extra_events {
            self.emit_event(kind, payload);
        }
    }

    fn emit_state_changed(&self, previous: MinerLifecycle, current: MinerLifecycle, reason: &str) {
        self.emit_event(
            "state.changed",
            json!({
                "previous": previous,
                "current": current,
                "reason": reason,
            }),
        );
    }

    fn emit_event(&self, event: &str, data: Value) {
        emit_broadcast_event(&self.events_tx, &self.seq, event, data);
    }

    fn start(&self, patch: StartRequest, allow_wallet_prompt: bool) -> Result<ActionResponse> {
        let _operation = self
            .operations
            .lock()
            .map_err(|_| anyhow!("supervisor operation lock poisoned"))?;
        self.start_session(patch, allow_wallet_prompt, None)
    }

    fn start_session(
        &self,
        patch: StartRequest,
        allow_wallet_prompt: bool,
        preserved_history: Option<Arc<SessionHistory>>,
    ) -> Result<ActionResponse> {
        if self.process_shutdown.load(Ordering::SeqCst) {
            bail!("miner is shutting down");
        }
        let (pending_password, mut cfg) = {
            let state = self
                .state
                .lock()
                .map_err(|_| anyhow!("supervisor state lock poisoned"))?;
            if matches!(
                state.lifecycle,
                MinerLifecycle::Starting | MinerLifecycle::Running | MinerLifecycle::Stopping
            ) {
                if state.lifecycle == MinerLifecycle::Stopping {
                    bail!("miner is stopping");
                }
                bail!("miner is already running");
            }
            let cfg = state
                .staged_config
                .clone()
                .unwrap_or_else(|| self.base_config.clone());
            (state.pending_wallet_password.clone(), cfg)
        };

        if let Some(password) = pending_password {
            cfg.wallet_password = Some(password);
        }

        apply_start_patch(&mut cfg, &patch)?;
        cfg.allow_wallet_prompt = allow_wallet_prompt;
        cfg.bench = false;
        cfg.service_mode = false;
        match cfg.mode {
            MiningMode::Daemon => {
                if cfg
                    .token
                    .as_ref()
                    .map_or(true, |token| token.trim().is_empty())
                {
                    bail!(
                        "daemon mode requires an API token; provide --token/--cookie at startup or include token/cookie_path in start payload"
                    );
                }
            }
            MiningMode::Pool => {
                if cfg
                    .mining_address
                    .as_ref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    bail!(
                        "pool mode requires mining_address; provide --address at startup or include mining_address in start payload"
                    );
                }
                if cfg
                    .pool_url
                    .as_ref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    bail!(
                        "pool mode requires pool_url; provide --pool-url at startup or include pool_url in start payload"
                    );
                }
                if cfg
                    .pool_worker
                    .as_ref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    bail!(
                        "pool mode requires pool_worker; provide --pool-worker at startup or include pool_worker in start payload"
                    );
                }
            }
        }

        let history = preserved_history.unwrap_or_else(|| Arc::new(SessionHistory::new()));
        let run_shutdown = Arc::new(AtomicBool::new(false));
        let stop_origin = Arc::new(crate::diagnostics::StopOrigin::default());
        let run_shutdown_for_worker = Arc::clone(&run_shutdown);
        let cfg_for_worker = cfg.clone();

        let run_id = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow!("supervisor state lock poisoned"))?;
            if matches!(
                state.lifecycle,
                MinerLifecycle::Starting | MinerLifecycle::Running | MinerLifecycle::Stopping
            ) {
                if state.lifecycle == MinerLifecycle::Stopping {
                    bail!("miner is stopping");
                }
                bail!("miner is already running");
            }
            let previous = state.lifecycle;
            state.metrics.start_requests = state.metrics.start_requests.saturating_add(1);
            state.lifecycle = MinerLifecycle::Starting;
            state.run_id = state.run_id.saturating_add(1).max(1);
            let run_id = state.run_id;
            state.started_at_unix_ms = Some(history.started_at_unix_ms);
            state.session_history = Some(Arc::clone(&history));
            state.stopped_at_unix_ms = None;
            state.last_error = None;
            state.session_shutdown = Some(Arc::clone(&run_shutdown));
            state.stop_origin = Arc::clone(&stop_origin);
            state.effective_config = Some(cfg.clone());
            state.backend_phases.clear();
            state.pending_nvidia_count = 0;
            self.emit_state_changed(previous, state.lifecycle, "start requested");
            run_id
        };

        let state_handle = Arc::clone(&self.state);
        let events_tx = self.events_tx.clone();
        let seq = Arc::clone(&self.seq);
        let thread_control = ThreadControl {
            run_id,
            sender: self.thread_changes_tx.clone(),
            memory: None,
            max_threads: cfg.cpu_auto_threads_cap,
            current_threads: cfg.threads,
            quit_requested: Arc::new(AtomicBool::new(false)),
        };

        let spawn_result = thread::Builder::new()
            .name("seine-api-miner-session".to_string())
            .spawn(move || {
                {
                    if let Ok(mut state) = state_handle.lock() {
                        if state.run_id == run_id && state.lifecycle == MinerLifecycle::Starting {
                            let previous = state.lifecycle;
                            state.lifecycle = MinerLifecycle::Running;
                            emit_broadcast_event(
                                &events_tx,
                                &seq,
                                "state.changed",
                                json!({
                                    "previous": previous,
                                    "current": state.lifecycle,
                                    "reason": "worker started",
                                }),
                            );
                        }
                    }
                }

                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    crate::miner::run_with_thread_control(
                        &cfg_for_worker,
                        Arc::clone(&run_shutdown_for_worker),
                        Some(thread_control),
                        history,
                        stop_origin,
                    )
                }))
                .unwrap_or_else(|_| Err(anyhow!("miner session panicked")));

                if let Ok(mut state) = state_handle.lock() {
                    if state.run_id != run_id {
                        return;
                    }
                    crate::miner::live_stats::clear();
                    state.session_shutdown = None;
                    state.stopped_at_unix_ms = Some(now_unix_millis());
                    let previous = state.lifecycle;
                    match result {
                        Ok(()) => {
                            state.lifecycle = MinerLifecycle::Idle;
                            state.last_error = None;
                        }
                        Err(err) => {
                            state.lifecycle = MinerLifecycle::Error;
                            state.last_error = Some(format!("{err:#}"));
                        }
                    }
                    if state.staged_config.is_none() {
                        state.effective_config = None;
                    }
                    let next = state.lifecycle;
                    let event_payload = json!({
                        "previous": previous,
                        "current": next,
                        "reason": "worker exited"
                    });
                    emit_broadcast_event(&events_tx, &seq, "state.changed", event_payload);
                }
            })
            .context("failed to spawn miner session thread");
        if let Err(err) = spawn_result {
            if let Ok(mut state) = self.state.lock() {
                if state.run_id == run_id {
                    let previous = state.lifecycle;
                    state.lifecycle = MinerLifecycle::Error;
                    state.last_error = Some(format!("{err:#}"));
                    state.stopped_at_unix_ms = Some(now_unix_millis());
                    state.session_shutdown = None;
                    if state.staged_config.is_none() {
                        state.effective_config = None;
                    }
                    self.emit_state_changed(previous, state.lifecycle, "worker spawn failed");
                }
            }
            return Err(err);
        }

        Ok(ActionResponse {
            ok: true,
            message: "miner start requested".to_string(),
            requires_restart: false,
            state: self.snapshot(),
        })
    }

    fn stop(&self, request: StopRequest) -> ActionResponse {
        self.stop_with_origin(request, Some(seine_telemetry::ShutdownRequest::ApiStop))
    }

    fn stop_with_origin(
        &self,
        request: StopRequest,
        origin: Option<seine_telemetry::ShutdownRequest>,
    ) -> ActionResponse {
        let _operation = self
            .operations
            .lock()
            .expect("supervisor operation lock poisoned");
        self.stop_session(request, origin)
    }

    fn stop_session(
        &self,
        request: StopRequest,
        origin: Option<seine_telemetry::ShutdownRequest>,
    ) -> ActionResponse {
        let wait = request
            .wait_ms
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_secs(10));

        let mut requested = false;
        let previous;
        {
            let mut state = self
                .state
                .lock()
                .expect("supervisor state lock poisoned while stopping");
            state.metrics.stop_requests = state.metrics.stop_requests.saturating_add(1);
            previous = state.lifecycle;
            if let Some(flag) = state.session_shutdown.as_ref() {
                if let Some(origin) = origin {
                    state.stop_origin.request(origin);
                }
                flag.store(true, Ordering::SeqCst);
                requested = true;
                state.lifecycle = MinerLifecycle::Stopping;
            }
        }

        if requested {
            self.emit_state_changed(previous, MinerLifecycle::Stopping, "stop requested");
            let deadline = Instant::now() + wait.max(Duration::from_millis(1));
            while Instant::now() < deadline {
                let done = {
                    let state = self
                        .state
                        .lock()
                        .expect("supervisor state lock poisoned while waiting for stop");
                    !matches!(
                        state.lifecycle,
                        MinerLifecycle::Stopping | MinerLifecycle::Running
                    )
                };
                if done {
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
        }

        ActionResponse {
            ok: true,
            message: if requested {
                "miner stop requested".to_string()
            } else {
                "miner is already stopped".to_string()
            },
            requires_restart: false,
            state: self.snapshot(),
        }
    }

    fn restart(&self, patch: StartRequest, allow_wallet_prompt: bool) -> Result<ActionResponse> {
        let _operation = self
            .operations
            .lock()
            .map_err(|_| anyhow!("supervisor operation lock poisoned"))?;
        self.restart_session(patch, allow_wallet_prompt, None)
    }

    fn restart_session(
        &self,
        patch: StartRequest,
        allow_wallet_prompt: bool,
        quit_requested: Option<&AtomicBool>,
    ) -> Result<ActionResponse> {
        let preserved_history = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow!("supervisor state lock poisoned"))?;
            state.metrics.restart_requests = state.metrics.restart_requests.saturating_add(1);
            // Only a thread adjustment continues the logical mining session.
            // An explicit API restart still starts a fresh session.
            quit_requested.and_then(|_| state.session_history.clone())
        };
        let _ = self.stop_session(
            StopRequest {
                wait_ms: Some(15_000),
            },
            Some(if quit_requested.is_some() {
                seine_telemetry::ShutdownRequest::TuiThreadChange
            } else {
                seine_telemetry::ShutdownRequest::ApiRestart
            }),
        );
        if quit_requested.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            return Ok(ActionResponse {
                ok: true,
                message: "thread restart canceled by quit".to_string(),
                requires_restart: false,
                state: self.snapshot(),
            });
        }
        self.start_session(patch, allow_wallet_prompt, preserved_history)
    }

    fn apply_pending_thread_change(&self) -> Result<()> {
        let Ok(request) = self.thread_changes_rx.try_recv() else {
            return Ok(());
        };
        let _operation = self
            .operations
            .lock()
            .map_err(|_| anyhow!("supervisor operation lock poisoned"))?;
        if self.process_shutdown.load(Ordering::SeqCst)
            || request.quit_requested.load(Ordering::SeqCst)
        {
            return Ok(());
        }
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow!("supervisor state lock poisoned"))?;
            // A delayed keypress from a previous session must never restart a new one.
            if state.run_id != request.run_id || state.lifecycle != MinerLifecycle::Running {
                return Ok(());
            }
            let mut cfg = state
                .staged_config
                .as_ref()
                .or(state.effective_config.as_ref())
                .cloned()
                .ok_or_else(|| anyhow!("no active mining configuration"))?;
            let admission = request
                .memory
                .as_ref()
                .ok_or_else(|| anyhow!("RAM check unavailable; try again."))
                .and_then(|guard| guard.validate(request.threads))
                .and_then(|()| {
                    apply_runtime_thread_count(&mut cfg, request.threads, request.max_threads)
                });
            if let Err(err) = admission {
                if let Some(reply) = &request.rejection {
                    let _ = reply.try_send(format!("{err:#}"));
                }
                return Err(err);
            }
            state.staged_config = Some(cfg);
        }
        crate::miner::ui::info(
            "THREADS",
            format!(
                "restarting mining with {} threads per CPU backend; session statistics preserved",
                request.threads
            ),
        );
        self.restart_session(StartRequest::default(), true, Some(&request.quit_requested))?;
        Ok(())
    }

    fn set_wallet_password(&self, password: String) -> ActionResponse {
        let mut state = self
            .state
            .lock()
            .expect("supervisor state lock poisoned while setting wallet password");
        state.metrics.wallet_unlock_requests =
            state.metrics.wallet_unlock_requests.saturating_add(1);
        state.pending_wallet_password = Some(password);
        drop(state);
        self.emit_event("wallet.loaded", json!({"source": "api"}));

        ActionResponse {
            ok: true,
            message: "wallet password stored for future start attempts".to_string(),
            requires_restart: false,
            state: self.snapshot(),
        }
    }

    fn patch_live_config(&self, patch: StartRequest) -> Result<ActionResponse> {
        let _operation = self
            .operations
            .lock()
            .map_err(|_| anyhow!("supervisor operation lock poisoned"))?;
        let mut cfg = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow!("supervisor state lock poisoned"))?;
            state.metrics.live_config_patches = state.metrics.live_config_patches.saturating_add(1);
            state
                .staged_config
                .clone()
                .or_else(|| state.effective_config.clone())
                .unwrap_or_else(|| self.base_config.clone())
        };
        apply_start_patch(&mut cfg, &patch)?;
        let requires_restart = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow!("supervisor state lock poisoned"))?;
            state.staged_config = Some(cfg.clone());
            let active = matches!(
                state.lifecycle,
                MinerLifecycle::Running | MinerLifecycle::Starting | MinerLifecycle::Stopping
            );
            if !active {
                state.effective_config = Some(cfg);
            }
            active
        };

        let message = if requires_restart {
            "runtime live patch queued for next start; restart required for current backend session"
                .to_string()
        } else {
            "config patch applied to next session".to_string()
        };

        Ok(ActionResponse {
            ok: true,
            message,
            requires_restart,
            state: self.snapshot(),
        })
    }

    fn initiate_global_shutdown(&self) {
        self.process_shutdown.store(true, Ordering::SeqCst);
        let state = self
            .state
            .lock()
            .expect("supervisor state lock poisoned while initiating global shutdown");
        if let Some(flag) = state.session_shutdown.as_ref() {
            flag.store(true, Ordering::SeqCst);
        }
    }
}

#[derive(Clone)]
struct AppState {
    supervisor: Arc<MinerSupervisor>,
    started_unix_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct BackendSpecPatch {
    kind: String,
    device_index: Option<u32>,
    cpu_threads: Option<usize>,
    cpu_affinity: Option<String>,
    assign_timeout_ms_override: Option<u64>,
    control_timeout_ms_override: Option<u64>,
    assign_timeout_strikes_override: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct StartRequest {
    mode: Option<String>,
    api_url: Option<String>,
    token: Option<String>,
    cookie_path: Option<String>,
    wallet_password: Option<String>,
    wallet_password_file: Option<String>,
    mining_address: Option<String>,
    pool_url: Option<String>,
    pool_worker: Option<String>,
    backend_specs: Option<Vec<BackendSpecPatch>>,
    threads: Option<usize>,
    refresh_secs: Option<u64>,
    request_timeout_secs: Option<u64>,
    events_stream_timeout_secs: Option<u64>,
    events_idle_timeout_secs: Option<u64>,
    stats_secs: Option<u64>,
    hash_poll_ms: Option<u64>,
    cpu_hash_batch_size: Option<u64>,
    cpu_control_check_interval_hashes: Option<u64>,
    cpu_hash_flush_ms: Option<u64>,
    cpu_event_dispatch_capacity: Option<usize>,
    cpu_profile: Option<String>,
    cpu_affinity: Option<String>,
    cpu_page_mode: Option<String>,
    cpu_autotune_threads: Option<bool>,
    cpu_autotune_min_threads: Option<usize>,
    cpu_autotune_max_threads: Option<usize>,
    cpu_autotune_secs: Option<u64>,
    nvidia_autotune_secs: Option<u64>,
    nvidia_autotune_samples: Option<u32>,
    nvidia_max_rregcount: Option<u32>,
    nvidia_max_lanes: Option<usize>,
    nvidia_dispatch_iters_per_lane: Option<u64>,
    nvidia_allocation_iters_per_lane: Option<u64>,
    nvidia_hashes_per_launch_per_lane: Option<u32>,
    nvidia_fused_target_check: Option<bool>,
    nvidia_adaptive_launch_depth: Option<bool>,
    nvidia_enforce_template_stop: Option<bool>,
    metal_max_lanes: Option<usize>,
    metal_hashes_per_launch_per_lane: Option<u32>,
    backend_assign_timeout_ms: Option<u64>,
    backend_assign_timeout_strikes: Option<u32>,
    backend_control_timeout_ms: Option<u64>,
    allow_best_effort_deadlines: Option<bool>,
    prefetch_wait_ms: Option<u64>,
    tip_listener_join_wait_ms: Option<u64>,
    submit_join_wait_ms: Option<u64>,
    strict_round_accounting: Option<bool>,
    start_nonce: Option<u64>,
    nonce_iters_per_lane: Option<u64>,
    work_allocation: Option<String>,
    sub_round_rebalance_ms: Option<u64>,
    sse_enabled: Option<bool>,
    refresh_on_same_height: Option<bool>,
    ui_mode: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct StopRequest {
    wait_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct WalletUnlockRequest {
    password: String,
}

#[derive(Debug, Clone, Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    started_unix_ms: u64,
    now_unix_ms: u64,
}

/// `GET /v1/miner/stats`: live hashrate and share counters for monitoring.
#[derive(Debug, Clone, Serialize)]
struct MinerStatsResponse {
    version: &'static str,
    lifecycle: MinerLifecycle,
    running: bool,
    /// `null` until the first sample of a mining session.
    mining: Option<crate::miner::live_stats::LiveMiningStats>,
}

#[derive(Debug, Clone, Serialize)]
struct ConfigEnvelope {
    config: ApiConfigView,
}

#[derive(Debug, Clone, Serialize)]
struct BackendsResponse {
    pending_nvidia_count: u64,
    backend_phases: BTreeMap<String, String>,
    configured_backends: Vec<BackendSpecView>,
    available_devices: Vec<AvailableDeviceView>,
}

/// A GPU currently reported by device enumeration, so API clients can build
/// explicit per-device `backend_specs` without guessing indices.
#[derive(Debug, Clone, Serialize)]
struct AvailableDeviceView {
    vendor: &'static str,
    index: u32,
    name: String,
    memory_total_mib: u64,
}

fn detect_available_devices() -> Vec<AvailableDeviceView> {
    let mut devices = Vec::new();
    if let Ok(detected) = crate::config::detect_nvidia_devices() {
        devices.extend(detected.into_iter().map(|device| AvailableDeviceView {
            vendor: "nvidia",
            index: device.index,
            name: device.name,
            memory_total_mib: device.memory_total_mib,
        }));
    }
    if let Ok(detected) = crate::config::detect_amd_devices(&mut Vec::new(), true, false) {
        devices.extend(detected.into_iter().map(|device| AvailableDeviceView {
            vendor: "amd",
            index: device.index,
            name: device.name,
            memory_total_mib: device.memory_total_mib,
        }));
    }
    devices
}

pub fn run(cfg: Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime for control API")?;

    runtime.block_on(async move { run_async(cfg, shutdown).await })
}

/// The same session supervisor as the API, without opening an HTTP listener.
pub(crate) fn run_interactive(cfg: Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    let supervisor = Arc::new(MinerSupervisor::new(cfg, Arc::clone(&shutdown)));
    let log_supervisor = Arc::clone(&supervisor);
    set_log_sink(Some(Arc::new(move |event| {
        log_supervisor.ingest_log(event)
    })));
    let result = (|| {
        supervisor.start(StartRequest::default(), true)?;
        loop {
            if shutdown.load(Ordering::SeqCst) {
                supervisor.stop_with_origin(
                    StopRequest {
                        wait_ms: Some(60_000),
                    },
                    crate::shutdown::requested(),
                );
                return Ok(());
            }
            if let Err(err) = supervisor.apply_pending_thread_change() {
                crate::miner::ui::warn("THREADS", format!("could not change threads: {err:#}"));
            }
            // Avoid copying the log history and API config on every UI control tick.
            let (lifecycle, last_error) = {
                let state = supervisor
                    .state
                    .lock()
                    .map_err(|_| anyhow!("supervisor state lock poisoned"))?;
                (state.lifecycle, state.last_error.clone())
            };
            match lifecycle {
                MinerLifecycle::Idle => return Ok(()),
                MinerLifecycle::Error => bail!(
                    "{}",
                    last_error.unwrap_or_else(|| "mining failed".to_string())
                ),
                _ => thread::sleep(Duration::from_millis(50)),
            }
        }
    })();
    set_log_sink(None);
    result
}

struct ThreadControlWorker {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl ThreadControlWorker {
    fn spawn(supervisor: Arc<MinerSupervisor>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            while !worker_stop.load(Ordering::Relaxed)
                && !supervisor.process_shutdown.load(Ordering::Relaxed)
            {
                if let Err(err) = supervisor.apply_pending_thread_change() {
                    crate::miner::ui::warn("THREADS", format!("could not change threads: {err:#}"));
                }
                thread::sleep(Duration::from_millis(50));
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for ThreadControlWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn apply_runtime_thread_count(cfg: &mut Config, threads: usize, limit: usize) -> Result<()> {
    // This channel is internal to the TUI. Use the live bound supplied by the
    // mining controller instead of the supervisor's pre-start RAM estimate.
    if threads == 0 || threads > limit {
        bail!("thread request exceeds the current runtime limit");
    }
    let mut next = cfg.clone();
    next.cpu_auto_threads_cap = limit;
    apply_thread_count(&mut next, threads)?;
    *cfg = next;
    Ok(())
}

fn apply_thread_count(cfg: &mut Config, threads: usize) -> Result<()> {
    let cpu_specs: Vec<_> = cfg
        .backend_specs
        .iter()
        .filter(|spec| spec.kind == BackendKind::Cpu)
        .collect();
    if cpu_specs.is_empty() {
        bail!("no CPU backend is active");
    }
    let limit = cfg.cpu_auto_threads_cap.max(
        cpu_specs
            .iter()
            .map(|spec| spec.cpu_threads.unwrap_or(cfg.threads))
            .min()
            .unwrap_or(1),
    );
    if threads == 0 || threads > limit {
        bail!("threads must be between 1 and {limit}");
    }
    apply_start_patch(
        cfg,
        &StartRequest {
            threads: Some(threads),
            cpu_autotune_threads: Some(false),
            ..StartRequest::default()
        },
    )
}

async fn run_async(cfg: Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    crate::miner::live_stats::enable();
    let supervisor = Arc::new(MinerSupervisor::new(cfg.clone(), Arc::clone(&shutdown)));
    let log_supervisor = Arc::clone(&supervisor);
    set_log_sink(Some(Arc::new(move |event| {
        log_supervisor.ingest_log(event);
    })));

    let api_token = match cfg.api_token.clone() {
        Some(token) => token,
        None => {
            let token = generate_api_token()?;
            write_api_cookie(&cfg.api_cookie_path, &token)?;
            crate::console_output::eprintln!(
                "control API token written to {}",
                cfg.api_cookie_path.display()
            );
            token
        }
    };
    let guard = Arc::new(ApiGuard {
        token: api_token,
        require_loopback_host: is_loopback_bind_addr(&cfg.api_bind),
    });

    if cfg.api_server_enabled && !cfg.service_mode {
        let _ = supervisor.start(StartRequest::default(), true);
    }

    let app_state = AppState {
        supervisor: Arc::clone(&supervisor),
        started_unix_ms: now_unix_millis(),
    };

    let mut app = Router::new()
        .route("/v1/health", get(get_health))
        .route("/v1/runtime/state", get(get_runtime_state))
        .route("/v1/runtime/config/defaults", get(get_runtime_defaults))
        .route("/v1/runtime/config/effective", get(get_runtime_effective))
        .route("/v1/miner/start", post(post_miner_start))
        .route("/v1/miner/stop", post(post_miner_stop))
        .route("/v1/miner/restart", post(post_miner_restart))
        .route("/v1/miner/live-config", patch(patch_live_config))
        .route("/v1/miner/stats", get(get_miner_stats))
        .route("/v1/backends", get(get_backends))
        .route("/v1/wallet/unlock", post(post_wallet_unlock))
        .route("/v1/events/stream", get(get_events_stream))
        .route("/metrics", get(get_metrics))
        .with_state(app_state)
        .layer(middleware::from_fn_with_state(guard, require_api_auth));

    if let Some(cors) = build_cors_layer(&cfg.api_cors)? {
        app = app.layer(cors);
    }

    let bind_addr = parse_api_bind_addr(&cfg.api_bind)?;
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("failed to bind control API listener at {bind_addr}"))?;

    let thread_control_worker = ThreadControlWorker::spawn(Arc::clone(&supervisor));

    let result = serve_control_api(listener, app, Arc::clone(&supervisor)).await;

    if cfg.api_token.is_none() {
        let _ = std::fs::remove_file(&cfg.api_cookie_path);
    }
    drop(thread_control_worker);
    let _ = supervisor.stop_with_origin(
        StopRequest {
            wait_ms: Some(15_000),
        },
        crate::shutdown::requested(),
    );
    set_log_sink(None);

    result.context("control API server exited unexpectedly")?;
    Ok(())
}

async fn serve_control_api(
    listener: tokio::net::TcpListener,
    app: Router,
    supervisor: Arc<MinerSupervisor>,
) -> std::io::Result<()> {
    let shutdown = Arc::clone(&supervisor.process_shutdown);
    let server_shutdown = Arc::clone(&shutdown);
    let server = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(wait_for_shutdown(server_shutdown))
            .await
    };
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => {
            supervisor.initiate_global_shutdown();
            result
        }
        _ = wait_for_shutdown(shutdown) => {
            // Mining must stop before waiting for HTTP clients. A stream or
            // stalled request must not delay the session's cleanup signal.
            supervisor.initiate_global_shutdown();
            tokio::time::timeout(API_SHUTDOWN_DRAIN_TIMEOUT, &mut server)
                .await
                .unwrap_or(Ok(()))
        }
    }
}

async fn wait_for_shutdown(shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

struct ApiGuard {
    token: String,
    require_loopback_host: bool,
}

/// Every route except `/v1/health` needs `Authorization: Bearer <token>`. On a loopback
/// bind, requests must also name a loopback Host so DNS-rebinding pages are refused.
async fn require_api_auth(
    State(guard): State<Arc<ApiGuard>>,
    request: Request,
    next: Next,
) -> Response {
    if guard.require_loopback_host && !host_header_is_loopback(request.headers()) {
        return ApiError::forbidden("control API only accepts loopback Host headers")
            .into_response();
    }
    let public = request.method() == Method::GET && request.uri().path() == "/v1/health";
    if !public && !bearer_token_matches(request.headers(), &guard.token) {
        return ApiError::unauthorized(
            "missing or invalid bearer token; read it from the control API cookie file",
        )
        .into_response();
    }
    next.run(request).await
}

fn bearer_token_matches(headers: &HeaderMap, expected: &str) -> bool {
    let Some(provided) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return false;
    };
    let (provided, expected) = (provided.trim().as_bytes(), expected.as_bytes());
    provided.len() == expected.len()
        && provided
            .iter()
            .zip(expected)
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            == 0
}

fn host_header_is_loopback(headers: &HeaderMap) -> bool {
    // Browsers always send Host; non-browser clients that omit it are not a rebinding risk.
    let Some(host) = headers.get(header::HOST) else {
        return true;
    };
    let Ok(host) = host.to_str() else {
        return false;
    };
    let host = host.trim();
    let name = if let Some(rest) = host.strip_prefix('[') {
        rest.split(']').next().unwrap_or_default()
    } else {
        host.rsplit_once(':').map_or(host, |(name, _)| name)
    };
    name.eq_ignore_ascii_case("localhost")
        || name
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn is_loopback_bind_addr(bind: &str) -> bool {
    parse_api_bind_addr(bind).is_ok_and(|addr| addr.ip().is_loopback())
}

fn generate_api_token() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes)
        .map_err(|err| anyhow!("failed to generate control API token: {err}"))?;
    Ok(hex::encode(bytes))
}

fn write_api_cookie(path: &std::path::Path, token: &str) -> Result<()> {
    use std::io::Write;

    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let _ = std::fs::remove_file(path);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("failed to create control API cookie {}", path.display()))?;
    file.write_all(token.as_bytes())
        .with_context(|| format!("failed to write control API cookie {}", path.display()))?;
    Ok(())
}

fn build_cors_layer(cors_value: &str) -> Result<Option<CorsLayer>> {
    let trimmed = cors_value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed == "*" {
        return Ok(Some(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        ));
    }

    let value = axum::http::HeaderValue::from_str(trimmed)
        .with_context(|| format!("invalid CORS origin '{trimmed}'"))?;
    Ok(Some(
        CorsLayer::new()
            .allow_origin(value)
            .allow_methods(Any)
            .allow_headers(Any),
    ))
}

fn parse_api_bind_addr(bind: &str) -> Result<SocketAddr> {
    if let Ok(addr) = SocketAddr::from_str(bind.trim()) {
        return Ok(addr);
    }

    let bind = bind.trim();
    if let Some((host, port)) = bind.rsplit_once(':') {
        let port = port
            .parse::<u16>()
            .with_context(|| format!("invalid api bind port in '{bind}'"))?;
        if host.eq_ignore_ascii_case("localhost") {
            return Ok(SocketAddr::from(([127, 0, 0, 1], port)));
        }
    }

    bail!("invalid api-bind '{bind}' (expected host:port)")
}

fn normalize_pool_url(value: &str) -> Result<String> {
    crate::pool::normalize_pool_urls(value).context("invalid pool_url")
}

fn classify_start_or_restart_error(err: anyhow::Error, operation: &str) -> ApiError {
    let message = format!("{err:#}");
    if message.contains("already running") || message.contains("is stopping") {
        return ApiError::conflict("already_running", message);
    }
    if message.contains("failed to spawn miner session thread") {
        return ApiError::internal(format!("failed to {operation} miner: {message}"));
    }
    ApiError::bad_request(message)
}

async fn get_health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: crate::version::release_version(),
        started_unix_ms: state.started_unix_ms,
        now_unix_ms: now_unix_millis(),
    })
}

async fn get_runtime_state(State(state): State<AppState>) -> Json<RuntimeStateSnapshot> {
    Json(state.supervisor.snapshot())
}

async fn get_miner_stats(State(state): State<AppState>) -> Json<MinerStatsResponse> {
    let (lifecycle, running) = state
        .supervisor
        .state
        .lock()
        .map(|guard| (guard.lifecycle, guard.lifecycle == MinerLifecycle::Running))
        .unwrap_or((MinerLifecycle::Error, false));
    Json(MinerStatsResponse {
        version: crate::version::release_version(),
        lifecycle,
        running,
        mining: crate::miner::live_stats::snapshot(),
    })
}

async fn get_runtime_defaults(State(state): State<AppState>) -> Json<ConfigEnvelope> {
    Json(ConfigEnvelope {
        config: ApiConfigView::from(state.supervisor.base_config()),
    })
}

async fn get_runtime_effective(State(state): State<AppState>) -> Json<ConfigEnvelope> {
    let snapshot = state.supervisor.snapshot();
    let config = snapshot
        .effective_config
        .unwrap_or_else(|| ApiConfigView::from(state.supervisor.base_config()));
    Json(ConfigEnvelope { config })
}

async fn post_miner_start(
    State(state): State<AppState>,
    payload: Option<Json<StartRequest>>,
) -> Result<Json<ActionResponse>, ApiError> {
    let request = payload.map(|value| value.0).unwrap_or_default();
    state
        .supervisor
        .start(request, false)
        .map(Json)
        .map_err(|err| classify_start_or_restart_error(err, "start"))
}

async fn post_miner_stop(
    State(state): State<AppState>,
    payload: Option<Json<StopRequest>>,
) -> Result<Json<ActionResponse>, ApiError> {
    Ok(Json(
        state
            .supervisor
            .stop(payload.map(|value| value.0).unwrap_or_default()),
    ))
}

async fn post_miner_restart(
    State(state): State<AppState>,
    payload: Option<Json<StartRequest>>,
) -> Result<Json<ActionResponse>, ApiError> {
    let request = payload.map(|value| value.0).unwrap_or_default();
    state
        .supervisor
        .restart(request, false)
        .map(Json)
        .map_err(|err| classify_start_or_restart_error(err, "restart"))
}

async fn patch_live_config(
    State(state): State<AppState>,
    Json(patch): Json<StartRequest>,
) -> Result<Json<ActionResponse>, ApiError> {
    state
        .supervisor
        .patch_live_config(patch)
        .map(Json)
        .map_err(|err| ApiError::bad_request(format!("invalid patch: {err:#}")))
}

async fn get_backends(State(state): State<AppState>) -> Json<BackendsResponse> {
    let snapshot = state.supervisor.snapshot();
    let configured_backends = snapshot
        .effective_config
        .as_ref()
        .map(|view| view.backend_specs.clone())
        .unwrap_or_else(|| ApiConfigView::from(state.supervisor.base_config()).backend_specs);

    // Enumeration shells out (nvidia-smi) or calls into HIP; keep it off the
    // async worker threads since this endpoint is polled by dashboards.
    let available_devices = tokio::task::spawn_blocking(detect_available_devices)
        .await
        .unwrap_or_default();

    Json(BackendsResponse {
        pending_nvidia_count: snapshot.pending_nvidia_count,
        backend_phases: snapshot.backend_phases,
        configured_backends,
        available_devices,
    })
}

async fn post_wallet_unlock(
    State(state): State<AppState>,
    Json(request): Json<WalletUnlockRequest>,
) -> Result<Json<ActionResponse>, ApiError> {
    if request.password.trim().is_empty() {
        return Err(ApiError::bad_request("password must not be empty"));
    }
    Ok(Json(state.supervisor.set_wallet_password(request.password)))
}

async fn get_events_stream(State(state): State<AppState>) -> impl IntoResponse {
    let mut rx = state.supervisor.subscribe();
    let shutdown = Arc::clone(&state.supervisor.process_shutdown);

    let stream = stream! {
        let stopping = wait_for_shutdown(shutdown);
        tokio::pin!(stopping);
        loop {
            let event = tokio::select! {
                biased;
                _ = &mut stopping => break,
                event = rx.recv() => event,
            };
            match event {
                Ok(event) => {
                    let payload = serde_json::to_string(&event.data).unwrap_or_else(|_| "{}".to_string());
                    let sse_event = Event::default()
                        .id(event.seq.to_string())
                        .event(event.event)
                        .data(payload);
                    yield Ok::<Event, std::convert::Infallible>(sse_event);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    };

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    )
}

async fn get_metrics(State(state): State<AppState>) -> String {
    let snapshot = state.supervisor.snapshot();
    let lifecycle = format!("{:?}", snapshot.lifecycle).to_ascii_lowercase();
    let running = if snapshot.running { 1 } else { 0 };
    let mut output = String::new();
    output.push_str("# TYPE seine_miner_running gauge\n");
    output.push_str(&format!("seine_miner_running {}\n", running));
    output.push_str("# TYPE seine_miner_lifecycle_info gauge\n");
    output.push_str(&format!(
        "seine_miner_lifecycle_info{{state=\"{}\"}} 1\n",
        lifecycle
    ));
    output.push_str("# TYPE seine_miner_pending_nvidia gauge\n");
    output.push_str(&format!(
        "seine_miner_pending_nvidia {}\n",
        snapshot.pending_nvidia_count
    ));
    output.push_str("# TYPE seine_miner_start_requests_total counter\n");
    output.push_str(&format!(
        "seine_miner_start_requests_total {}\n",
        snapshot.metrics.start_requests
    ));
    output.push_str("# TYPE seine_miner_stop_requests_total counter\n");
    output.push_str(&format!(
        "seine_miner_stop_requests_total {}\n",
        snapshot.metrics.stop_requests
    ));
    output.push_str("# TYPE seine_miner_restart_requests_total counter\n");
    output.push_str(&format!(
        "seine_miner_restart_requests_total {}\n",
        snapshot.metrics.restart_requests
    ));
    output.push_str("# TYPE seine_miner_live_config_patches_total counter\n");
    output.push_str(&format!(
        "seine_miner_live_config_patches_total {}\n",
        snapshot.metrics.live_config_patches
    ));
    output.push_str("# TYPE seine_miner_wallet_unlock_requests_total counter\n");
    output.push_str(&format!(
        "seine_miner_wallet_unlock_requests_total {}\n",
        snapshot.metrics.wallet_unlock_requests
    ));
    output.push_str("# TYPE seine_miner_log_events_total counter\n");
    output.push_str(&format!(
        "seine_miner_log_events_total {}\n",
        snapshot.metrics.log_events
    ));
    output.push_str("# TYPE seine_miner_submitted_blocks_total counter\n");
    output.push_str(&format!(
        "seine_miner_submitted_blocks_total {}\n",
        snapshot.metrics.submitted_blocks
    ));
    output.push_str("# TYPE seine_miner_accepted_blocks_total counter\n");
    output.push_str(&format!(
        "seine_miner_accepted_blocks_total {}\n",
        snapshot.metrics.accepted_blocks
    ));
    output.push_str("# TYPE seine_miner_stale_submits_total counter\n");
    output.push_str(&format!(
        "seine_miner_stale_submits_total {}\n",
        snapshot.metrics.stale_submits
    ));
    output.push_str("# TYPE seine_miner_backend_quarantines_total counter\n");
    output.push_str(&format!(
        "seine_miner_backend_quarantines_total {}\n",
        snapshot.metrics.backend_quarantines
    ));
    crate::miner::block_latency::render_prometheus(&mut output);
    output
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

fn emit_broadcast_event(
    events_tx: &broadcast::Sender<StreamEnvelope>,
    seq: &Arc<AtomicU64>,
    event: &str,
    data: Value,
) {
    let next_seq = seq.fetch_add(1, Ordering::AcqRel).saturating_add(1);
    let _ = events_tx.send(StreamEnvelope {
        seq: next_seq,
        ts_unix_ms: now_unix_millis(),
        event: event.to_string(),
        data,
    });
}

fn parse_pending_nvidia_count(message: &str) -> Option<u64> {
    if !message.starts_with("nvidia initialization in progress:") {
        return None;
    }
    let tail = message.split(':').nth(1)?.trim();
    let digits: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse::<u64>().ok()
}

fn parse_backend_phase(message: &str) -> Option<(String, &'static str)> {
    if let Some((label, _)) = message.split_once(": online") {
        return Some((label.trim().to_string(), "ready"));
    }
    if let Some((label, _)) = message.split_once(": initializing CUDA engine") {
        return Some((label.trim().to_string(), "initializing"));
    }
    if message.contains("awaiting activation") {
        if let Some((label, _)) = message.split_once(": initialized") {
            return Some((label.trim().to_string(), "initialized"));
        }
    }
    if let Some(rest) = message.strip_prefix("quarantined ") {
        let backend = rest
            .split_whitespace()
            .next()
            .unwrap_or("backend")
            .trim_matches(|c: char| matches!(c, ';' | ',' | ':'))
            .to_string();
        if !backend.is_empty() {
            return Some((backend, "quarantined"));
        }
    }
    None
}

fn apply_start_patch(cfg: &mut Config, patch: &StartRequest) -> Result<()> {
    if patch.token.is_some() && patch.cookie_path.is_some() {
        bail!("token and cookie_path are mutually exclusive");
    }

    if let Some(mode) = patch.mode.as_deref() {
        cfg.mode = parse_mining_mode(mode)?;
    }
    if let Some(api_url) = patch.api_url.as_ref() {
        let api_url = api_url.trim();
        if api_url.is_empty() {
            bail!("api_url cannot be empty");
        }
        cfg.api_url = api_url.trim_end_matches('/').to_string();
    }
    if let Some(token) = patch.token.as_ref() {
        let token = token.trim();
        if token.is_empty() {
            bail!("token cannot be empty");
        }
        cfg.token = Some(token.to_string());
        cfg.token_cookie_path = None;
    }
    if let Some(cookie_path) = patch.cookie_path.as_ref() {
        let cookie_path = cookie_path.trim();
        if cookie_path.is_empty() {
            bail!("cookie_path cannot be empty");
        }
        let path = std::path::PathBuf::from(cookie_path);
        let token = read_token_from_cookie_file(&path)?;
        cfg.token = Some(token);
        cfg.token_cookie_path = Some(path);
    }
    if let Some(password) = patch.wallet_password.as_ref() {
        cfg.wallet_password = Some(password.clone());
    }
    if let Some(path) = patch.wallet_password_file.as_ref() {
        cfg.wallet_password_file = Some(path.into());
    }
    if let Some(mining_address) = patch.mining_address.as_ref() {
        let mining_address = mining_address.trim();
        if mining_address.is_empty() {
            bail!("mining_address cannot be empty");
        }
        validate_destination(mining_address)
            .map_err(|err| anyhow!("invalid mining_address: {err}"))?;
        cfg.mining_address = Some(mining_address.to_string());
    }
    if let Some(pool_url) = patch.pool_url.as_ref() {
        cfg.pool_url = Some(normalize_pool_url(pool_url)?);
    }
    if let Some(pool_worker) = patch.pool_worker.as_ref() {
        let pool_worker = pool_worker.trim();
        if pool_worker.is_empty() {
            bail!("pool_worker cannot be empty");
        }
        cfg.pool_worker = Some(pool_worker.to_string());
    }
    if let Some(specs) = patch.backend_specs.as_ref() {
        cfg.backend_specs = parse_backend_specs_patch(specs)?;
        if patch.cpu_autotune_threads.is_none()
            && cfg
                .backend_specs
                .iter()
                .any(|s| s.kind == BackendKind::Cpu && s.cpu_threads.is_some())
        {
            cfg.cpu_autotune_threads = false;
        }
    }
    if let Some(threads) = patch.threads {
        if threads == 0 {
            bail!("threads must be >= 1");
        }
        cfg.threads = threads;
        cfg.cpu_manual_memory_check = true;
        if patch.cpu_autotune_threads.is_none() {
            cfg.cpu_autotune_threads = false;
        }
        for spec in cfg
            .backend_specs
            .iter_mut()
            .filter(|spec| format!("{:?}", spec.kind).eq_ignore_ascii_case("cpu"))
        {
            spec.cpu_threads = Some(threads);
        }
    }
    if let Some(value) = patch.refresh_secs {
        if value == 0 {
            bail!("refresh_secs must be >= 1");
        }
        cfg.refresh_interval = Duration::from_secs(value);
    }
    if let Some(value) = patch.request_timeout_secs {
        if value == 0 {
            bail!("request_timeout_secs must be >= 1");
        }
        cfg.request_timeout = Duration::from_secs(value);
    }
    if let Some(value) = patch.events_stream_timeout_secs {
        if value == 0 {
            bail!("events_stream_timeout_secs must be >= 1");
        }
        cfg.events_stream_timeout = Duration::from_secs(value);
    }
    if let Some(value) = patch.events_idle_timeout_secs {
        if value == 0 {
            bail!("events_idle_timeout_secs must be >= 1");
        }
        cfg.events_idle_timeout = Duration::from_secs(value);
    }
    if let Some(value) = patch.stats_secs {
        if value == 0 {
            bail!("stats_secs must be >= 1");
        }
        cfg.stats_interval = Duration::from_secs(value);
    }
    if let Some(value) = patch.hash_poll_ms {
        if value == 0 {
            bail!("hash_poll_ms must be >= 1");
        }
        cfg.hash_poll_interval = Duration::from_millis(value);
    }
    if let Some(value) = patch.cpu_hash_batch_size {
        if value == 0 {
            bail!("cpu_hash_batch_size must be >= 1");
        }
        cfg.cpu_hash_batch_size = value;
    }
    if let Some(value) = patch.cpu_control_check_interval_hashes {
        if value == 0 {
            bail!("cpu_control_check_interval_hashes must be >= 1");
        }
        cfg.cpu_control_check_interval_hashes = value;
    }
    if let Some(value) = patch.cpu_hash_flush_ms {
        if value == 0 {
            bail!("cpu_hash_flush_ms must be >= 1");
        }
        cfg.cpu_hash_flush_interval = Duration::from_millis(value);
    }
    if let Some(value) = patch.cpu_event_dispatch_capacity {
        if value == 0 {
            bail!("cpu_event_dispatch_capacity must be >= 1");
        }
        cfg.cpu_event_dispatch_capacity = value;
    }
    if let Some(value) = patch.cpu_profile.as_deref() {
        cfg.cpu_profile = parse_cpu_profile(value)?;
    }
    if let Some(value) = patch.cpu_affinity.as_deref() {
        cfg.cpu_affinity = parse_cpu_affinity(value)?;
    }
    if let Some(value) = patch.cpu_page_mode.as_deref() {
        cfg.cpu_page_mode = parse_cpu_page_mode(value)?;
    }
    if let Some(value) = patch.cpu_autotune_threads {
        cfg.cpu_autotune_threads = value;
    }
    if let Some(value) = patch.cpu_autotune_min_threads {
        if value == 0 {
            bail!("cpu_autotune_min_threads must be >= 1");
        }
        cfg.cpu_autotune_min_threads = value;
    }
    if let Some(value) = patch.cpu_autotune_max_threads {
        if value == 0 {
            bail!("cpu_autotune_max_threads must be >= 1");
        }
        cfg.cpu_autotune_max_threads = Some(value);
    }
    if let Some(value) = patch.cpu_autotune_secs {
        if value == 0 {
            bail!("cpu_autotune_secs must be >= 1");
        }
        cfg.cpu_autotune_secs = value;
    }
    if let Some(value) = patch.nvidia_autotune_secs {
        if value == 0 {
            bail!("nvidia_autotune_secs must be >= 1");
        }
        cfg.nvidia_autotune_secs = value;
    }
    if let Some(value) = patch.nvidia_autotune_samples {
        if value == 0 {
            bail!("nvidia_autotune_samples must be >= 1");
        }
        cfg.nvidia_autotune_samples = value;
    }
    if let Some(value) = patch.nvidia_max_rregcount {
        if value == 0 {
            bail!("nvidia_max_rregcount must be >= 1");
        }
        cfg.nvidia_max_rregcount = Some(value);
    }
    if let Some(value) = patch.nvidia_max_lanes {
        if value == 0 {
            bail!("nvidia_max_lanes must be >= 1");
        }
        cfg.nvidia_max_lanes = Some(value);
    }
    if let Some(value) = patch.nvidia_dispatch_iters_per_lane {
        if value == 0 {
            bail!("nvidia_dispatch_iters_per_lane must be >= 1");
        }
        cfg.nvidia_dispatch_iters_per_lane = Some(value);
    }
    if let Some(value) = patch.nvidia_allocation_iters_per_lane {
        if value == 0 {
            bail!("nvidia_allocation_iters_per_lane must be >= 1");
        }
        cfg.nvidia_allocation_iters_per_lane = Some(value);
    }
    if let Some(value) = patch.nvidia_hashes_per_launch_per_lane {
        if value == 0 {
            bail!("nvidia_hashes_per_launch_per_lane must be >= 1");
        }
        cfg.nvidia_hashes_per_launch_per_lane = value;
        cfg.nvidia_hashes_per_launch_per_lane_was_set = true;
    }
    if let Some(value) = patch.nvidia_fused_target_check {
        cfg.nvidia_fused_target_check = value;
    }
    if let Some(value) = patch.nvidia_adaptive_launch_depth {
        cfg.nvidia_adaptive_launch_depth = value;
    }
    if let Some(value) = patch.nvidia_enforce_template_stop {
        cfg.nvidia_enforce_template_stop = value;
    }
    if let Some(value) = patch.metal_max_lanes {
        if value == 0 {
            bail!("metal_max_lanes must be >= 1");
        }
        cfg.metal_max_lanes = Some(value);
    }
    if let Some(value) = patch.metal_hashes_per_launch_per_lane {
        if value == 0 {
            bail!("metal_hashes_per_launch_per_lane must be >= 1");
        }
        cfg.metal_hashes_per_launch_per_lane = value;
    }
    if let Some(value) = patch.backend_assign_timeout_ms {
        if value == 0 {
            bail!("backend_assign_timeout_ms must be >= 1");
        }
        cfg.backend_assign_timeout = Duration::from_millis(value);
    }
    if let Some(value) = patch.backend_assign_timeout_strikes {
        if value == 0 {
            bail!("backend_assign_timeout_strikes must be >= 1");
        }
        cfg.backend_assign_timeout_strikes = value;
    }
    if let Some(value) = patch.backend_control_timeout_ms {
        if value == 0 {
            bail!("backend_control_timeout_ms must be >= 1");
        }
        cfg.backend_control_timeout = Duration::from_millis(value);
    }
    if let Some(value) = patch.allow_best_effort_deadlines {
        cfg.allow_best_effort_deadlines = value;
    }
    if let Some(value) = patch.prefetch_wait_ms {
        if value == 0 {
            bail!("prefetch_wait_ms must be >= 1");
        }
        cfg.prefetch_wait = Duration::from_millis(value);
    }
    if let Some(value) = patch.tip_listener_join_wait_ms {
        if value == 0 {
            bail!("tip_listener_join_wait_ms must be >= 1");
        }
        cfg.tip_listener_join_wait = Duration::from_millis(value);
    }
    if let Some(value) = patch.submit_join_wait_ms {
        if value == 0 {
            bail!("submit_join_wait_ms must be >= 1");
        }
        cfg.submit_join_wait = Duration::from_millis(value);
    }
    if let Some(value) = patch.strict_round_accounting {
        cfg.strict_round_accounting = value;
        cfg.nvidia_enforce_template_stop = value;
    }
    if let Some(value) = patch.start_nonce {
        cfg.start_nonce = value;
    }
    if let Some(value) = patch.nonce_iters_per_lane {
        if value == 0 {
            bail!("nonce_iters_per_lane must be >= 1");
        }
        cfg.nonce_iters_per_lane = value;
    }
    if let Some(value) = patch.work_allocation.as_deref() {
        cfg.work_allocation = parse_work_allocation(value)?;
    }
    if let Some(value) = patch.sub_round_rebalance_ms {
        cfg.sub_round_rebalance_interval = if value == 0 {
            None
        } else {
            Some(Duration::from_millis(value))
        };
    }
    if let Some(value) = patch.sse_enabled {
        cfg.sse_enabled = value;
    }
    if let Some(value) = patch.refresh_on_same_height {
        cfg.refresh_on_same_height = value;
    }
    if let Some(value) = patch.ui_mode.as_deref() {
        cfg.ui_mode = parse_ui_mode(value)?;
    }

    if cfg!(target_os = "macos")
        && cfg.cpu_page_mode.requires_explicit_large_pages()
        && cfg
            .backend_specs
            .iter()
            .any(|spec| spec.kind == BackendKind::Cpu)
    {
        bail!(
            "cpu_page_mode {} is unsupported on macOS; use auto or regular",
            cfg.cpu_page_mode.as_str()
        );
    }

    Ok(())
}

fn parse_backend_specs_patch(specs: &[BackendSpecPatch]) -> Result<Vec<BackendSpec>> {
    parse_backend_specs_patch_with_detection(specs, detected_gpu_indexes)
}

/// Device indices usable for auto-expanding a GPU spec submitted without a
/// `device_index`. Detection failures degrade to an empty list, which keeps
/// the pre-expansion behavior (a single unpinned instance).
fn detected_gpu_indexes(kind: BackendKind) -> Vec<u32> {
    let detection = match kind {
        BackendKind::Nvidia => crate::config::detect_nvidia_devices(),
        BackendKind::Amd => crate::config::detect_amd_devices(&mut Vec::new(), true, true),
        BackendKind::Cpu | BackendKind::Metal => return Vec::new(),
    };
    detection
        .map(|devices| devices.iter().map(|device| device.index).collect())
        .unwrap_or_default()
}

fn parse_backend_specs_patch_with_detection(
    specs: &[BackendSpecPatch],
    detect: impl Fn(BackendKind) -> Vec<u32>,
) -> Result<Vec<BackendSpec>> {
    if specs.is_empty() {
        bail!("backend_specs must include at least one backend");
    }

    // Detection shells out (nvidia-smi) or dlopens (HIP); run at most once per
    // kind even when a payload repeats bare GPU specs.
    let mut detected_cache: HashMap<BackendKind, Vec<u32>> = HashMap::new();
    let mut parsed = Vec::with_capacity(specs.len());
    for (idx, patch) in specs.iter().enumerate() {
        let kind = parse_backend_kind(&patch.kind)
            .with_context(|| format!("backend_specs[{idx}].kind is invalid"))?;
        if patch.cpu_threads == Some(0) {
            bail!("backend_specs[{idx}].cpu_threads must be >= 1");
        }
        if patch.assign_timeout_ms_override == Some(0) {
            bail!("backend_specs[{idx}].assign_timeout_ms_override must be >= 1");
        }
        if patch.control_timeout_ms_override == Some(0) {
            bail!("backend_specs[{idx}].control_timeout_ms_override must be >= 1");
        }
        if patch.assign_timeout_strikes_override == Some(0) {
            bail!("backend_specs[{idx}].assign_timeout_strikes_override must be >= 1");
        }

        if kind != BackendKind::Nvidia && kind != BackendKind::Amd && patch.device_index.is_some() {
            bail!("backend_specs[{idx}].device_index is only valid for nvidia/amd backends");
        }

        let cpu_threads = if kind == BackendKind::Cpu {
            patch.cpu_threads
        } else {
            if patch.cpu_threads.is_some() {
                bail!("backend_specs[{idx}].cpu_threads is only valid for cpu backends");
            }
            None
        };
        let cpu_affinity = if kind == BackendKind::Cpu {
            match patch.cpu_affinity.as_deref() {
                Some(raw) => Some(
                    parse_cpu_affinity(raw)
                        .with_context(|| format!("backend_specs[{idx}].cpu_affinity is invalid"))?,
                ),
                None => None,
            }
        } else {
            if patch.cpu_affinity.is_some() {
                bail!("backend_specs[{idx}].cpu_affinity is only valid for cpu backends");
            }
            None
        };

        let spec = BackendSpec {
            kind,
            device_index: patch.device_index,
            cpu_threads,
            cpu_affinity,
            assign_timeout_override: patch.assign_timeout_ms_override.map(Duration::from_millis),
            control_timeout_override: patch.control_timeout_ms_override.map(Duration::from_millis),
            assign_timeout_strikes_override: patch.assign_timeout_strikes_override,
        };

        let is_bare_gpu_spec =
            matches!(kind, BackendKind::Nvidia | BackendKind::Amd) && patch.device_index.is_none();
        if is_bare_gpu_spec {
            let detected = detected_cache
                .entry(kind)
                .or_insert_with(|| detect(kind))
                .clone();
            if detected.is_empty() {
                parsed.push(spec);
            } else {
                for device_index in detected {
                    parsed.push(BackendSpec {
                        device_index: Some(device_index),
                        ..spec
                    });
                }
            }
        } else {
            parsed.push(spec);
        }
    }

    Ok(parsed)
}

fn parse_backend_kind(value: &str) -> Result<BackendKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "cpu" => Ok(BackendKind::Cpu),
        "nvidia" => Ok(BackendKind::Nvidia),
        "metal" => Ok(BackendKind::Metal),
        "amd" => Ok(BackendKind::Amd),
        other => bail!("invalid backend kind '{other}' (expected: cpu|nvidia|metal|amd)"),
    }
}

fn parse_work_allocation(value: &str) -> Result<WorkAllocation> {
    match value.trim().to_ascii_lowercase().as_str() {
        "adaptive" => Ok(WorkAllocation::Adaptive),
        "static" => Ok(WorkAllocation::Static),
        other => bail!("invalid work_allocation '{other}' (expected: adaptive|static)"),
    }
}

fn parse_cpu_profile(value: &str) -> Result<CpuPerformanceProfile> {
    match value.trim().to_ascii_lowercase().as_str() {
        "balanced" => Ok(CpuPerformanceProfile::Balanced),
        "throughput" => Ok(CpuPerformanceProfile::Throughput),
        "efficiency" => Ok(CpuPerformanceProfile::Efficiency),
        other => bail!("invalid cpu_profile '{other}' (expected: balanced|throughput|efficiency)"),
    }
}

fn parse_cpu_affinity(value: &str) -> Result<CpuAffinityMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "os" => Ok(CpuAffinityMode::Os),
        "off" => Ok(CpuAffinityMode::Off),
        "auto" => Ok(CpuAffinityMode::Auto),
        "pcore-only" => Ok(CpuAffinityMode::PcoreOnly),
        other => bail!("invalid cpu_affinity '{other}' (expected: off|auto|os|pcore-only)"),
    }
}

fn parse_cpu_page_mode(value: &str) -> Result<CpuPageMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "auto" => Ok(CpuPageMode::Auto),
        "regular" => Ok(CpuPageMode::Regular),
        "large" => Ok(CpuPageMode::Large),
        "large-1g" => Ok(CpuPageMode::Large1G),
        other => bail!("invalid cpu_page_mode '{other}' (expected: auto|regular|large|large-1g)"),
    }
}

fn parse_mining_mode(value: &str) -> Result<MiningMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "pool" => Ok(MiningMode::Pool),
        "daemon" => Ok(MiningMode::Daemon),
        other => bail!("invalid mode '{other}' (expected: pool|daemon)"),
    }
}

fn parse_ui_mode(value: &str) -> Result<UiMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "auto" => Ok(UiMode::Auto),
        "tui" => Ok(UiMode::Tui),
        "plain" => Ok(UiMode::Plain),
        other => bail!("invalid ui_mode '{other}' (expected: auto|tui|plain)"),
    }
}

fn work_allocation_to_str(value: WorkAllocation) -> &'static str {
    match value {
        WorkAllocation::Static => "static",
        WorkAllocation::Adaptive => "adaptive",
    }
}

fn cpu_profile_to_str(value: CpuPerformanceProfile) -> &'static str {
    match value {
        CpuPerformanceProfile::Balanced => "balanced",
        CpuPerformanceProfile::Throughput => "throughput",
        CpuPerformanceProfile::Efficiency => "efficiency",
    }
}

fn cpu_affinity_to_str(value: CpuAffinityMode) -> &'static str {
    match value {
        CpuAffinityMode::Os => "os",
        CpuAffinityMode::Off => "off",
        CpuAffinityMode::Auto => "auto",
        CpuAffinityMode::PcoreOnly => "pcore-only",
    }
}

fn mining_mode_to_str(value: MiningMode) -> &'static str {
    match value {
        MiningMode::Pool => "pool",
        MiningMode::Daemon => "daemon",
    }
}

fn ui_mode_to_str(value: UiMode) -> &'static str {
    match value {
        UiMode::Auto => "auto",
        UiMode::Tui => "tui",
        UiMode::Plain => "plain",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[tokio::test]
    async fn miner_stats_reports_lifecycle_and_version_while_idle() {
        let supervisor = Arc::new(MinerSupervisor::new(
            crate::miner::tests::test_config(),
            Arc::new(AtomicBool::new(false)),
        ));
        let Json(stats) = get_miner_stats(State(AppState {
            supervisor,
            started_unix_ms: now_unix_millis(),
        }))
        .await;
        let value = serde_json::to_value(&stats).unwrap();
        assert_eq!(value["lifecycle"], "idle");
        assert_eq!(value["running"], false);
        assert_eq!(value["version"], crate::version::release_version());
        assert!(value.get("mining").is_some(), "mining is always present");
    }

    /// The HiveOS and mmpOS package tests serve this fixture from a fake
    /// control API, so it must stay in the exact shape seine serializes.
    #[test]
    fn miner_stats_fixture_matches_serialized_shape() {
        use crate::miner::live_stats::{LiveDeviceStats, LiveMiningStats};

        let device =
            |label: &str, kind: &str, index, pci: Option<&str>, lanes, rate, avg| LiveDeviceStats {
                label: label.to_string(),
                kind: kind.to_string(),
                device_index: index,
                pci_address: pci.map(str::to_string),
                lanes,
                hashrate_hps: rate,
                average_hashrate_hps: avg,
            };
        let response = MinerStatsResponse {
            version: "0.2.46",
            lifecycle: MinerLifecycle::Running,
            running: true,
            mining: Some(LiveMiningStats {
                state: "working".to_string(),
                uptime_secs: 3605.25,
                updated_at_unix_ms: 1_767_225_600_000,
                hashrate_hps: 1250.5,
                average_hashrate_hps: 1240.0,
                total_hashes: 4_470_000,
                submitted: 52,
                accepted: 50,
                rejected: 2,
                devices: vec![
                    device(
                        "nvidia#1",
                        "nvidia",
                        Some(0),
                        Some("0000:01:00.0"),
                        11,
                        600.25,
                        590.0,
                    ),
                    device("nvidia#2", "nvidia", Some(1), None, 3, 150.125, 150.0),
                    device(
                        "amd#1",
                        "amd",
                        Some(0),
                        Some("0000:0a:00.0"),
                        7,
                        300.0,
                        300.0,
                    ),
                    device("cpu#1", "cpu", None, None, 2, 200.125, 200.0),
                ],
            }),
        };
        let fixture: Value = serde_json::from_str(include_str!(
            "../../packaging/hiveos/testdata/miner-stats.json"
        ))
        .unwrap();
        assert_eq!(serde_json::to_value(&response).unwrap(), fixture);
    }

    #[tokio::test]
    async fn process_shutdown_stops_mining_and_closes_an_open_event_stream() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let supervisor = Arc::new(MinerSupervisor::new(
            crate::miner::tests::test_config(),
            Arc::clone(&shutdown),
        ));
        let session_shutdown = Arc::new(AtomicBool::new(false));
        supervisor.state.lock().unwrap().session_shutdown = Some(Arc::clone(&session_shutdown));
        let app = Router::new()
            .route("/v1/events/stream", get(get_events_stream))
            .with_state(AppState {
                supervisor: Arc::clone(&supervisor),
                started_unix_ms: now_unix_millis(),
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/events/stream", listener.local_addr().unwrap());
        let mut server = tokio::spawn(serve_control_api(listener, app, Arc::clone(&supervisor)));
        let mut response = tokio::time::timeout(
            Duration::from_secs(2),
            reqwest::Client::new().get(endpoint).send(),
        )
        .await
        .expect("SSE response headers")
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );
        supervisor.emit_event("fixture", json!({ "ready": true }));
        let event = tokio::time::timeout(Duration::from_secs(1), async {
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\n\n") {
                bytes.extend_from_slice(&response.chunk().await.unwrap().expect("live stream"));
            }
            String::from_utf8(bytes).unwrap()
        })
        .await
        .expect("live event delivery");
        assert!(event.contains("event: fixture"));
        assert!(event.contains(r#"data: {"ready":true}"#));
        assert!(!session_shutdown.load(Ordering::SeqCst));
        shutdown.store(true, Ordering::SeqCst);
        let drained = tokio::time::timeout(Duration::from_secs(1), &mut server).await;
        let session_stopped = session_shutdown.load(Ordering::SeqCst);
        let stream_closed =
            tokio::time::timeout(Duration::from_millis(200), response.chunk()).await;
        drop(response);
        if drained.is_err() {
            server.abort();
            let _ = server.await;
        }
        assert!(
            session_stopped,
            "shutdown must reach mining before HTTP drain completes"
        );
        drained
            .expect("an open SSE client must not hold shutdown open")
            .unwrap()
            .unwrap();
        assert!(
            stream_closed.unwrap().unwrap().is_none(),
            "the event stream must end cleanly"
        );
    }

    #[tokio::test]
    async fn process_shutdown_bounds_http_drain_and_signals_mining_first() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let supervisor = Arc::new(MinerSupervisor::new(
            crate::miner::tests::test_config(),
            Arc::clone(&shutdown),
        ));
        let session_shutdown = Arc::new(AtomicBool::new(false));
        supervisor.state.lock().unwrap().session_shutdown = Some(Arc::clone(&session_shutdown));
        let entered = Arc::new(tokio::sync::Notify::new());
        let handler_entered = Arc::clone(&entered);
        let app = Router::new().route(
            "/stalled",
            get(move || {
                let entered = Arc::clone(&handler_entered);
                async move {
                    entered.notify_one();
                    std::future::pending::<&'static str>().await
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/stalled", listener.local_addr().unwrap());
        let mut server = tokio::spawn(serve_control_api(listener, app, supervisor));
        let client = tokio::spawn(async move { reqwest::Client::new().get(endpoint).send().await });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .expect("request reached the stalled handler");
        shutdown.store(true, Ordering::SeqCst);
        let mining_signalled = tokio::time::timeout(Duration::from_millis(500), async {
            while !session_shutdown.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        let waiting_for_http = !server.is_finished();
        let drained = tokio::time::timeout(
            API_SHUTDOWN_DRAIN_TIMEOUT + Duration::from_secs(1),
            &mut server,
        )
        .await;
        if drained.is_err() {
            server.abort();
            let _ = server.await;
        }
        client.abort();
        let _ = client.await;
        assert!(
            mining_signalled,
            "mining must stop before the HTTP drain deadline"
        );
        assert!(
            waiting_for_http,
            "the fixture must keep an HTTP request pending"
        );
        drained
            .expect("HTTP drain must be bounded")
            .unwrap()
            .unwrap();
    }

    fn headers(pairs: &[(header::HeaderName, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(name.clone(), value.parse().unwrap());
        }
        map
    }

    #[test]
    fn api_stop_records_origin_but_internal_teardown_does_not_invent_one() {
        let _lock = crate::diagnostics::TEST_LOCK.lock().unwrap();
        for api_request in [true, false] {
            let guard = crate::diagnostics::install(crate::diagnostics::test_context());
            let supervisor = MinerSupervisor::new(
                crate::miner::tests::test_config(),
                Arc::new(AtomicBool::new(false)),
            );
            let shutdown = Arc::new(AtomicBool::new(false));
            {
                let mut state = supervisor.state.lock().unwrap();
                state.session_shutdown = Some(shutdown.clone());
                state.lifecycle = MinerLifecycle::Running;
                state.stop_origin.attach(&guard.0);
            }
            let request = StopRequest { wait_ms: Some(1) };
            if api_request {
                supervisor.stop(request);
            } else {
                supervisor.stop_with_origin(request, None);
            }
            assert!(shutdown.load(Ordering::SeqCst));
            assert_eq!(
                guard.0.checkpoint().shutdown_request,
                api_request.then_some(seine_telemetry::ShutdownRequest::ApiStop)
            );
        }
    }

    #[test]
    fn snapshot_reports_flat_dev_fee_for_every_pool_and_solo() {
        for (mode, pool_url) in [
            (MiningMode::Pool, Some("stratum+tcp://bntpool.com:3333")),
            (MiningMode::Pool, Some("stratum+tcp://eu.bntpool.com:3333")),
            (MiningMode::Pool, Some("stratum+tcp://example.com:3333")),
            (MiningMode::Daemon, None),
        ] {
            let mut cfg = crate::miner::tests::test_config();
            cfg.mode = mode;
            cfg.pool_url = pool_url.map(str::to_string);
            let supervisor = MinerSupervisor::new(cfg.clone(), Arc::new(AtomicBool::new(false)));
            assert_eq!(supervisor.snapshot().dev_fee_percent, 2.0);
            supervisor.state.lock().unwrap().effective_config = Some(cfg);
            assert_eq!(supervisor.snapshot().dev_fee_percent, 2.0);
        }
    }

    #[test]
    fn bearer_token_must_match_exactly() {
        let ok = headers(&[(header::AUTHORIZATION, "Bearer secret-token")]);
        assert!(bearer_token_matches(&ok, "secret-token"));
        assert!(!bearer_token_matches(&ok, "secret-tokeN"));
        assert!(!bearer_token_matches(&ok, "secret-token-longer"));
        assert!(!bearer_token_matches(&headers(&[]), "secret-token"));
        let basic = headers(&[(header::AUTHORIZATION, "Basic secret-token")]);
        assert!(!bearer_token_matches(&basic, "secret-token"));
    }

    #[test]
    fn host_header_check_rejects_rebinding_names() {
        for host in [
            "127.0.0.1:9977",
            "localhost:9977",
            "LOCALHOST",
            "[::1]:9977",
            "127.1.2.3",
        ] {
            assert!(
                host_header_is_loopback(&headers(&[(header::HOST, host)])),
                "{host}"
            );
        }
        for host in [
            "evil.example:9977",
            "127.0.0.1.evil.example",
            "10.0.0.5:9977",
            "[::2]:9977",
        ] {
            assert!(
                !host_header_is_loopback(&headers(&[(header::HOST, host)])),
                "{host}"
            );
        }
    }

    #[tokio::test]
    async fn router_requires_token_and_loopback_host() {
        use tower::ServiceExt;

        let guard = Arc::new(ApiGuard {
            token: "t0ken".to_string(),
            require_loopback_host: true,
        });
        let app = Router::new()
            .route("/v1/health", get(|| async { "ok" }))
            .route("/v1/miner/live-config", patch(|| async { "patched" }))
            .layer(middleware::from_fn_with_state(guard, require_api_auth));
        let send = |method: Method, path: &str, host: &str, auth: Option<&str>| {
            let mut builder = axum::http::Request::builder()
                .method(method)
                .uri(path)
                .header(header::HOST, host);
            if let Some(auth) = auth {
                builder = builder.header(header::AUTHORIZATION, auth);
            }
            app.clone()
                .oneshot(builder.body(axum::body::Body::empty()).unwrap())
        };

        let status = |res: Result<Response, std::convert::Infallible>| res.unwrap().status();
        assert_eq!(
            status(send(Method::GET, "/v1/health", "127.0.0.1:9977", None).await),
            StatusCode::OK
        );
        assert_eq!(
            status(
                send(
                    Method::PATCH,
                    "/v1/miner/live-config",
                    "127.0.0.1:9977",
                    None
                )
                .await
            ),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(
                send(
                    Method::PATCH,
                    "/v1/miner/live-config",
                    "127.0.0.1:9977",
                    Some("Bearer nope")
                )
                .await
            ),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(
                send(
                    Method::PATCH,
                    "/v1/miner/live-config",
                    "127.0.0.1:9977",
                    Some("Bearer t0ken")
                )
                .await
            ),
            StatusCode::OK
        );
        assert_eq!(
            status(
                send(
                    Method::PATCH,
                    "/v1/miner/live-config",
                    "evil.example:9977",
                    Some("Bearer t0ken")
                )
                .await
            ),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status(send(Method::GET, "/v1/health", "evil.example:9977", None).await),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn cors_is_off_unless_configured() {
        assert!(build_cors_layer("").unwrap().is_none());
        assert!(build_cors_layer("  ").unwrap().is_none());
        assert!(build_cors_layer("http://localhost:3000").unwrap().is_some());
    }

    #[test]
    fn api_thread_override_disables_adaptation_unless_explicitly_requested() {
        for forced in [None, Some(true)] {
            let mut cfg = crate::miner::tests::test_config();
            cfg.cpu_autotune_threads = true;
            apply_start_patch(
                &mut cfg,
                &StartRequest {
                    threads: Some(2),
                    cpu_autotune_threads: forced,
                    ..StartRequest::default()
                },
            )
            .unwrap();
            assert_eq!(cfg.cpu_autotune_threads, forced.unwrap_or(false));
            assert_eq!(cfg.threads, 2);
        }
    }

    #[test]
    fn runtime_thread_limit_overrides_stale_startup_ram_estimate_in_both_directions() {
        let mut cfg = crate::miner::tests::test_config();
        cfg.cpu_autotune_threads = true;
        cfg.cpu_auto_threads_cap = 1;
        apply_runtime_thread_count(&mut cfg, 3, 4).unwrap();
        assert_eq!(cfg.threads, 3);
        assert_eq!(cfg.cpu_auto_threads_cap, 4);
        assert!(!cfg.cpu_autotune_threads);
        assert!(apply_runtime_thread_count(&mut cfg, 3, 2).is_err());
        assert_eq!(
            cfg.cpu_auto_threads_cap, 4,
            "rejected request cannot mutate config"
        );
        apply_runtime_thread_count(&mut cfg, 1, 2).unwrap();
        assert_eq!(cfg.threads, 1);
        assert_eq!(cfg.cpu_auto_threads_cap, 2);
    }

    #[test]
    fn manual_threads_disable_autotune_and_preserve_other_settings() {
        let mut cfg = crate::miner::tests::test_config();
        cfg.cpu_auto_threads_cap = 8;
        cfg.cpu_autotune_threads = true;
        let mut gpu = cfg.backend_specs[0];
        gpu.kind = BackendKind::Nvidia;
        gpu.cpu_threads = None;
        gpu.device_index = Some(3);
        cfg.backend_specs.push(gpu);
        let endpoint = cfg.api_url.clone();
        apply_thread_count(&mut cfg, 4).unwrap();
        assert_eq!(cfg.threads, 4);
        assert_eq!(cfg.backend_specs[0].cpu_threads, Some(4));
        assert!(cfg.cpu_manual_memory_check);
        assert_eq!(cfg.backend_specs[1], gpu);
        assert_eq!(cfg.api_url, endpoint);
        assert!(!cfg.cpu_autotune_threads);
    }

    #[test]
    fn manual_thread_limits_reject_invalid_counts_without_mutation() {
        let mut cfg = crate::miner::tests::test_config();
        cfg.cpu_auto_threads_cap = 4;
        for count in [0, 5, usize::MAX] {
            assert!(apply_thread_count(&mut cfg, count).is_err());
            assert_eq!(cfg.threads, 1);
            assert_eq!(cfg.backend_specs[0].cpu_threads, Some(1));
        }
        // An explicitly oversized configuration can be reduced without
        // increasing the other CPU backend to that oversized count.
        cfg.backend_specs[0].cpu_threads = Some(8);
        let mut second = cfg.backend_specs[0];
        second.cpu_threads = Some(2);
        cfg.backend_specs.push(second);
        assert!(apply_thread_count(&mut cfg, 8).is_err());
        apply_thread_count(&mut cfg, 3).unwrap();
        assert!(cfg
            .backend_specs
            .iter()
            .all(|spec| spec.cpu_threads == Some(3)));
        cfg.backend_specs.clear();
        assert!(apply_thread_count(&mut cfg, 1).is_err());
    }

    #[test]
    fn rejected_thread_request_keeps_session_and_config_and_replies() {
        let mut cfg = crate::miner::tests::test_config();
        // Deterministic zero headroom without allocating RAM or relying on this host.
        cfg.cpu_memory_reserve_percent = Some(100);
        let supervisor = MinerSupervisor::new(cfg.clone(), Arc::new(AtomicBool::new(false)));
        let session_shutdown = Arc::new(AtomicBool::new(false));
        {
            let mut state = supervisor.state.lock().unwrap();
            state.lifecycle = MinerLifecycle::Running;
            state.run_id = 2;
            state.effective_config = Some(cfg.clone());
            state.session_shutdown = Some(Arc::clone(&session_shutdown));
        }
        let (reply, received) = crossbeam_channel::bounded(1);
        supervisor
            .thread_changes_tx
            .send(ThreadChangeRequest {
                max_threads: 8,
                run_id: 2,
                threads: 2,
                memory: Some(Arc::new(
                    crate::miner::thread_control::ThreadMemoryGuard::new(&cfg, vec![]),
                )),
                rejection: Some(reply),
                quit_requested: Arc::new(AtomicBool::new(false)),
            })
            .unwrap();
        assert!(supervisor.apply_pending_thread_change().is_err());
        assert!(received.try_recv().unwrap().contains("Not enough RAM"));
        assert!(!session_shutdown.load(Ordering::SeqCst));
        let state = supervisor.state.lock().unwrap();
        assert!(state.staged_config.is_none());
        assert_eq!(
            state.effective_config.as_ref().unwrap().threads,
            cfg.threads
        );
        assert_eq!(state.metrics.restart_requests, 0);
    }

    #[test]
    fn stale_thread_requests_cannot_stop_or_modify_a_new_session() {
        let cfg = crate::miner::tests::test_config();
        let supervisor = MinerSupervisor::new(cfg.clone(), Arc::new(AtomicBool::new(false)));
        let session_shutdown = Arc::new(AtomicBool::new(false));
        {
            let mut state = supervisor.state.lock().unwrap();
            state.lifecycle = MinerLifecycle::Running;
            state.run_id = 2;
            state.effective_config = Some(cfg);
            state.session_shutdown = Some(Arc::clone(&session_shutdown));
        }
        supervisor
            .thread_changes_tx
            .send(ThreadChangeRequest {
                max_threads: 8,
                memory: None,
                rejection: None,
                run_id: 1,
                threads: 1,
                quit_requested: Arc::new(AtomicBool::new(false)),
            })
            .unwrap();
        supervisor.apply_pending_thread_change().unwrap();
        assert!(!session_shutdown.load(Ordering::SeqCst));
        let state = supervisor.state.lock().unwrap();
        assert!(state.staged_config.is_none());
        assert_eq!(state.metrics.restart_requests, 0);
    }

    #[test]
    fn shutdown_prevents_a_queued_thread_restart() {
        let shutdown = Arc::new(AtomicBool::new(true));
        let supervisor = MinerSupervisor::new(crate::miner::tests::test_config(), shutdown);
        supervisor
            .thread_changes_tx
            .send(ThreadChangeRequest {
                max_threads: 8,
                memory: None,
                rejection: None,
                run_id: 0,
                threads: 1,
                quit_requested: Arc::new(AtomicBool::new(false)),
            })
            .unwrap();
        supervisor.apply_pending_thread_change().unwrap();
        assert!(supervisor.start(StartRequest::default(), false).is_err());
        assert_eq!(supervisor.snapshot().run_id, 0);
    }

    #[test]
    fn quit_during_teardown_cancels_the_replacement_session() {
        let supervisor = MinerSupervisor::new(
            crate::miner::tests::test_config(),
            Arc::new(AtomicBool::new(false)),
        );
        let session_shutdown = Arc::new(AtomicBool::new(false));
        let quit = Arc::new(AtomicBool::new(false));
        {
            let mut state = supervisor.state.lock().unwrap();
            state.lifecycle = MinerLifecycle::Running;
            state.run_id = 1;
            state.session_shutdown = Some(Arc::clone(&session_shutdown));
        }
        let state = Arc::clone(&supervisor.state);
        let quit_during_stop = Arc::clone(&quit);
        let worker = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            while !session_shutdown.load(Ordering::SeqCst) {
                assert!(Instant::now() < deadline, "stop was not requested");
                thread::sleep(Duration::from_millis(1));
            }
            quit_during_stop.store(true, Ordering::SeqCst);
            let mut state = state.lock().unwrap();
            state.lifecycle = MinerLifecycle::Idle;
            state.session_shutdown = None;
        });
        supervisor
            .restart_session(StartRequest::default(), true, Some(&quit))
            .unwrap();
        worker.join().unwrap();
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.run_id, 1);
        assert_eq!(snapshot.lifecycle, MinerLifecycle::Idle);
    }

    #[test]
    fn parse_backend_phase_extracts_quarantined_backend_label() {
        let parsed = parse_backend_phase("quarantined cpu#1; continuing with nvidia#2");
        assert_eq!(parsed, Some(("cpu#1".to_string(), "quarantined")));
    }

    #[test]
    fn parse_backend_specs_patch_parses_and_validates_fields() {
        let parsed = parse_backend_specs_patch(&[
            BackendSpecPatch {
                kind: "cpu".to_string(),
                cpu_threads: Some(4),
                cpu_affinity: Some("auto".to_string()),
                ..BackendSpecPatch::default()
            },
            BackendSpecPatch {
                kind: "nvidia".to_string(),
                device_index: Some(1),
                assign_timeout_ms_override: Some(1500),
                control_timeout_ms_override: Some(9000),
                assign_timeout_strikes_override: Some(2),
                ..BackendSpecPatch::default()
            },
        ])
        .expect("backend specs should parse");

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].kind, BackendKind::Cpu);
        assert_eq!(parsed[0].cpu_threads, Some(4));
        assert_eq!(parsed[1].kind, BackendKind::Nvidia);
        assert_eq!(parsed[1].device_index, Some(1));
        assert_eq!(
            parsed[1]
                .assign_timeout_override
                .map(|value| value.as_millis()),
            Some(1500)
        );
    }

    #[test]
    fn bare_nvidia_patch_expands_to_all_detected_devices() {
        let parsed = parse_backend_specs_patch_with_detection(
            &[BackendSpecPatch {
                kind: "nvidia".to_string(),
                assign_timeout_ms_override: Some(1500),
                ..BackendSpecPatch::default()
            }],
            |kind| {
                assert_eq!(kind, BackendKind::Nvidia);
                vec![0, 1]
            },
        )
        .expect("bare nvidia spec should expand");

        assert_eq!(parsed.len(), 2);
        for (spec, expected_index) in parsed.iter().zip([0, 1]) {
            assert_eq!(spec.kind, BackendKind::Nvidia);
            assert_eq!(spec.device_index, Some(expected_index));
            assert_eq!(
                spec.assign_timeout_override.map(|value| value.as_millis()),
                Some(1500)
            );
        }
    }

    #[test]
    fn bare_gpu_patch_without_detected_devices_stays_single_unpinned_spec() {
        let parsed = parse_backend_specs_patch_with_detection(
            &[BackendSpecPatch {
                kind: "amd".to_string(),
                ..BackendSpecPatch::default()
            }],
            |_| Vec::new(),
        )
        .expect("bare amd spec should fall back to a single spec");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].kind, BackendKind::Amd);
        assert_eq!(parsed[0].device_index, None);
    }

    #[test]
    fn explicit_device_index_patch_is_never_expanded() {
        let parsed = parse_backend_specs_patch_with_detection(
            &[BackendSpecPatch {
                kind: "nvidia".to_string(),
                device_index: Some(1),
                ..BackendSpecPatch::default()
            }],
            |_| panic!("detection must not run for explicit device_index"),
        )
        .expect("explicit device index should parse unchanged");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].device_index, Some(1));
    }

    #[test]
    fn parse_cpu_page_mode_accepts_public_contract_values() {
        assert_eq!(parse_cpu_page_mode("auto").unwrap(), CpuPageMode::Auto);
        assert_eq!(
            parse_cpu_page_mode("regular").unwrap(),
            CpuPageMode::Regular
        );
        assert_eq!(parse_cpu_page_mode("large").unwrap(), CpuPageMode::Large);
        assert_eq!(
            parse_cpu_page_mode("large-1g").unwrap(),
            CpuPageMode::Large1G
        );
        assert!(parse_cpu_page_mode("transparent").is_err());
        assert!(parse_cpu_page_mode("large1g").is_err());
    }

    #[test]
    fn classify_start_or_restart_error_maps_conflict() {
        let err = anyhow!("miner is already running");
        let api_err = classify_start_or_restart_error(err, "restart");
        assert_eq!(api_err.status, StatusCode::CONFLICT);
        assert_eq!(api_err.code, "already_running");
    }

    #[test]
    fn classify_start_or_restart_error_maps_spawn_failure_to_internal() {
        let err = anyhow!("failed to spawn miner session thread");
        let api_err = classify_start_or_restart_error(err, "start");
        assert_eq!(api_err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(api_err.code, "internal_error");
    }

    #[test]
    fn classify_start_or_restart_error_maps_validation_failures_to_bad_request() {
        let err = anyhow!("missing API token; provide --token or --cookie");
        let api_err = classify_start_or_restart_error(err, "restart");
        assert_eq!(api_err.status, StatusCode::BAD_REQUEST);
        assert_eq!(api_err.code, "bad_request");
    }
}
