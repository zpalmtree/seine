use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use async_stream::stream;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

use crate::config::{
    read_token_from_cookie_file, BackendKind, BackendSpec, Config, CpuAffinityMode,
    CpuPerformanceProfile, UiMode, WorkAllocation,
};
use crate::dev_fee::{DEV_ADDRESS, DEV_FEE_PERCENT};
use crate::miner::ui::{set_log_sink, UiLogEvent};

const API_EVENT_CHANNEL_CAPACITY: usize = 4096;
const RECENT_LOG_CAPACITY: usize = 200;

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
    api_url: String,
    has_token: bool,
    cookie_path: Option<String>,
    wallet_password_present: bool,
    mining_address: Option<String>,
    backend_specs: Vec<BackendSpecView>,
    threads: usize,
    cpu_profile: String,
    cpu_affinity: String,
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
            api_key_required: false,
            api_url: cfg.api_url.clone(),
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
}

impl MinerSupervisor {
    fn new(base_config: Config, process_shutdown: Arc<AtomicBool>) -> Self {
        let (events_tx, _) = broadcast::channel(API_EVENT_CHANNEL_CAPACITY.max(64));
        Self {
            base_config,
            process_shutdown,
            state: Arc::new(Mutex::new(SupervisorState::default())),
            events_tx,
            seq: Arc::new(AtomicU64::new(0)),
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
        if cfg
            .token
            .as_ref()
            .map_or(true, |token| token.trim().is_empty())
        {
            bail!(
                "missing API token; provide --token/--cookie at startup or include token/cookie_path in start payload"
            );
        }

        let run_shutdown = Arc::new(AtomicBool::new(false));
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
            state.started_at_unix_ms = Some(now_unix_millis());
            state.stopped_at_unix_ms = None;
            state.last_error = None;
            state.session_shutdown = Some(Arc::clone(&run_shutdown));
            state.effective_config = Some(cfg.clone());
            state.backend_phases.clear();
            state.pending_nvidia_count = 0;
            self.emit_state_changed(previous, state.lifecycle, "start requested");
            run_id
        };

        let state_handle = Arc::clone(&self.state);
        let events_tx = self.events_tx.clone();
        let seq = Arc::clone(&self.seq);

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

                let result =
                    crate::miner::run(&cfg_for_worker, Arc::clone(&run_shutdown_for_worker));

                if let Ok(mut state) = state_handle.lock() {
                    if state.run_id != run_id {
                        return;
                    }
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
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow!("supervisor state lock poisoned"))?;
            state.metrics.restart_requests = state.metrics.restart_requests.saturating_add(1);
        }
        let _ = self.stop(StopRequest {
            wait_ms: Some(15_000),
        });
        self.start(patch, allow_wallet_prompt)
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
    api_url: Option<String>,
    token: Option<String>,
    cookie_path: Option<String>,
    wallet_password: Option<String>,
    wallet_password_file: Option<String>,
    mining_address: Option<String>,
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

#[derive(Debug, Clone, Serialize)]
struct ConfigEnvelope {
    config: ApiConfigView,
}

#[derive(Debug, Clone, Serialize)]
struct BackendsResponse {
    pending_nvidia_count: u64,
    backend_phases: BTreeMap<String, String>,
    configured_backends: Vec<BackendSpecView>,
}

pub fn run(cfg: Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime for control API")?;

    runtime.block_on(async move { run_async(cfg, shutdown).await })
}

async fn run_async(cfg: Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    let supervisor = Arc::new(MinerSupervisor::new(cfg.clone(), Arc::clone(&shutdown)));
    let log_supervisor = Arc::clone(&supervisor);
    set_log_sink(Some(Arc::new(move |event| {
        log_supervisor.ingest_log(event);
    })));

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
        .route("/v1/backends", get(get_backends))
        .route("/v1/wallet/unlock", post(post_wallet_unlock))
        .route("/v1/events/stream", get(get_events_stream))
        .route("/metrics", get(get_metrics))
        .with_state(app_state);

    app = app.layer(build_cors_layer(&cfg.api_cors)?);

    let bind_addr = parse_api_bind_addr(&cfg.api_bind)?;
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("failed to bind control API listener at {bind_addr}"))?;

    let server =
        axum::serve(listener, app).with_graceful_shutdown(wait_for_shutdown(Arc::clone(&shutdown)));

    let result = server.await;

    supervisor.initiate_global_shutdown();
    let _ = supervisor.stop(StopRequest {
        wait_ms: Some(15_000),
    });
    set_log_sink(None);

    result.context("control API server exited unexpectedly")?;
    Ok(())
}

async fn wait_for_shutdown(shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn build_cors_layer(cors_value: &str) -> Result<CorsLayer> {
    let trimmed = cors_value.trim();
    if trimmed == "*" {
        return Ok(CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any));
    }

    let value = axum::http::HeaderValue::from_str(trimmed)
        .with_context(|| format!("invalid CORS origin '{trimmed}'"))?;
    Ok(CorsLayer::new()
        .allow_origin(value)
        .allow_methods(Any)
        .allow_headers(Any))
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

    Json(BackendsResponse {
        pending_nvidia_count: snapshot.pending_nvidia_count,
        backend_phases: snapshot.backend_phases,
        configured_backends,
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

    let stream = stream! {
        loop {
            match rx.recv().await {
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
        cfg.mining_address = Some(mining_address.to_string());
    }
    if let Some(specs) = patch.backend_specs.as_ref() {
        cfg.backend_specs = parse_backend_specs_patch(specs)?;
    }
    if let Some(threads) = patch.threads {
        if threads == 0 {
            bail!("threads must be >= 1");
        }
        cfg.threads = threads;
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

    Ok(())
}

fn parse_backend_specs_patch(specs: &[BackendSpecPatch]) -> Result<Vec<BackendSpec>> {
    if specs.is_empty() {
        bail!("backend_specs must include at least one backend");
    }

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

        if kind != BackendKind::Nvidia && patch.device_index.is_some() {
            bail!("backend_specs[{idx}].device_index is only valid for nvidia backends");
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

        parsed.push(BackendSpec {
            kind,
            device_index: patch.device_index,
            cpu_threads,
            cpu_affinity,
            assign_timeout_override: patch.assign_timeout_ms_override.map(Duration::from_millis),
            control_timeout_override: patch.control_timeout_ms_override.map(Duration::from_millis),
            assign_timeout_strikes_override: patch.assign_timeout_strikes_override,
        });
    }

    Ok(parsed)
}

fn parse_backend_kind(value: &str) -> Result<BackendKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "cpu" => Ok(BackendKind::Cpu),
        "nvidia" => Ok(BackendKind::Nvidia),
        "metal" => Ok(BackendKind::Metal),
        other => bail!("invalid backend kind '{other}' (expected: cpu|nvidia|metal)"),
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
        "off" => Ok(CpuAffinityMode::Off),
        "auto" => Ok(CpuAffinityMode::Auto),
        "pcore-only" => Ok(CpuAffinityMode::PcoreOnly),
        other => bail!("invalid cpu_affinity '{other}' (expected: off|auto|pcore-only)"),
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
        CpuAffinityMode::Off => "off",
        CpuAffinityMode::Auto => "auto",
        CpuAffinityMode::PcoreOnly => "pcore-only",
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
