use std::collections::{BTreeMap, HashSet, VecDeque};
use std::env;
use std::fs;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use blocknet_pow_spec::POW_HEADER_BASE_LEN;
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::is_raw_mode_enabled;

use crate::api::{
    is_no_wallet_loaded_error, is_unauthorized_error, is_wallet_already_loaded_error, ApiClient,
};
use crate::backend::{BackendEvent, MiningSolution};
use crate::config::Config;
use crate::types::{
    decode_hex, parse_target, set_block_nonce, template_difficulty, template_height,
    BlockTemplateResponse,
};

use super::auth::{refresh_api_token_from_cookie, TokenRefreshOutcome};
use super::round_control::{redistribute_for_topology_change, TopologyRedistributionOptions};
use super::runtime::{
    maybe_print_stats, seed_backend_weights, update_backend_weights, work_distribution_weights,
    RoundEndReason, WeightUpdateInputs,
};
use super::scheduler::NonceScheduler;
use super::stats::{format_hashrate, Stats};
use super::template_prefetch::TemplatePrefetch;
pub(super) use super::tip::{spawn_tip_listener, TipListener, TipSignal};
use super::tui::{TuiRenderer, TuiState};
use super::ui::{error, info, mined, set_tui_state, success, warn};
use super::{
    cancel_backend_slots, collect_backend_hashes, distribute_work, format_round_backend_hashrate,
    format_round_backend_telemetry, next_event_wait, next_work_id, quiesce_backend_slots,
    total_lanes, BackendRoundTelemetry, BackendSlot, RuntimeBackendEventAction, RuntimeMode,
    TEMPLATE_RETRY_DELAY,
};

type BackendEventAction = RuntimeBackendEventAction;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum WalletPasswordSource {
    CliFlag,
    PasswordFile,
    Environment,
    Prompt,
}

impl WalletPasswordSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::CliFlag => "--wallet-password",
            Self::PasswordFile => "--wallet-password-file",
            Self::Environment => "SEINE_WALLET_PASSWORD",
            Self::Prompt => "terminal prompt",
        }
    }
}

const TUI_RENDER_INTERVAL: Duration = Duration::from_secs(1);
const TUI_QUIT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const TUI_RENDER_SIGNAL_CAPACITY: usize = 8;
const RETRY_LOG_INTERVAL: Duration = Duration::from_secs(10);
const RECENT_TEMPLATE_CACHE_MIN: usize = 16;
const RECENT_TEMPLATE_CACHE_MAX: usize = 512;
const RECENT_TEMPLATE_CACHE_HEADROOM_ROUNDS: usize = 4;
static PROMPT_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Default)]
struct RetryTracker {
    failures: u64,
    last_log_at: Option<Instant>,
    outage_logged: bool,
}

impl RetryTracker {
    fn note_failure(&mut self, tag: &str, first: &str, repeat: &str, immediate_first: bool) {
        let now = Instant::now();
        self.failures = self.failures.saturating_add(1);

        let should_log = if self.failures == 1 {
            immediate_first
        } else {
            self.last_log_at
                .is_none_or(|last| now.saturating_duration_since(last) >= RETRY_LOG_INTERVAL)
        };

        if should_log {
            let message = if self.failures == 1 { first } else { repeat };
            warn(tag, message);
            self.last_log_at = Some(now);
            self.outage_logged = true;
        }
    }

    fn note_recovered(&mut self, tag: &str, message: &str) {
        if self.failures == 0 {
            return;
        }
        if !self.outage_logged {
            *self = Self::default();
            return;
        }
        success(tag, message);
        *self = Self::default();
    }
}

struct MiningControlPlane<'a> {
    client: &'a ApiClient,
    cfg: &'a Config,
    shutdown: Arc<AtomicBool>,
    tip_signal: Option<&'a TipSignal>,
    prefetch: Option<TemplatePrefetch>,
}

struct TuiDisplay {
    state: TuiState,
    last_render_request: Instant,
    last_state_label: String,
    render_signal: Sender<RenderSignal>,
    render_stop: Arc<AtomicBool>,
    render_worker: Option<JoinHandle<()>>,
    quit_watcher_stop: Arc<AtomicBool>,
    quit_watcher: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone, Copy)]
enum RenderSignal {
    RenderNow,
}

struct RoundUiView<'a> {
    backends: &'a [BackendSlot],
    round_backend_hashes: &'a BTreeMap<u64, u64>,
    round_hashes: u64,
    round_start: Instant,
    height: &'a str,
    difficulty: &'a str,
    epoch: u64,
    state_label: &'a str,
}

struct RoundLoopState {
    solved: Option<MiningSolution>,
    stale_tip_event: bool,
    round_hashes: u64,
    round_backend_hashes: BTreeMap<u64, u64>,
    round_backend_telemetry: BTreeMap<u64, BackendRoundTelemetry>,
}

impl TuiDisplay {
    fn new(tui_state: TuiState, shutdown: Arc<AtomicBool>) -> Result<Self> {
        let (render_signal, render_stop, render_worker) =
            spawn_tui_render_worker(Arc::clone(&tui_state), Arc::clone(&shutdown))?;
        let (quit_watcher_stop, quit_watcher) = spawn_tui_quit_watcher(shutdown);
        let display = Self {
            state: tui_state,
            last_render_request: Instant::now() - TUI_RENDER_INTERVAL,
            last_state_label: String::new(),
            render_signal,
            render_stop,
            render_worker: Some(render_worker),
            quit_watcher_stop,
            quit_watcher: Some(quit_watcher),
        };
        display.request_render();
        Ok(display)
    }

    fn update(&mut self, stats: &Stats, view: RoundUiView<'_>) {
        let state_changed = self.last_state_label != view.state_label;
        if !state_changed && self.last_render_request.elapsed() < TUI_RENDER_INTERVAL {
            return;
        }

        let snapshot = stats.snapshot();
        let round_elapsed = view.round_start.elapsed().as_secs_f64().max(0.001);
        let round_rate = format_hashrate(view.round_hashes as f64 / round_elapsed);
        let backend_rate =
            format_round_backend_hashrate(view.backends, view.round_backend_hashes, round_elapsed);

        if let Ok(mut s) = self.state.lock() {
            s.height = view.height.to_string();
            s.difficulty = view.difficulty.to_string();
            s.epoch = view.epoch;
            s.state = view.state_label.to_string();
            s.round_hashrate = round_rate;
            s.avg_hashrate = format_hashrate(snapshot.hps);
            s.total_hashes = snapshot.hashes;
            s.templates = snapshot.templates;
            s.submitted = snapshot.submitted;
            s.accepted = snapshot.accepted;
            s.backend_rates = backend_rate;
        }

        self.request_render();
        self.last_render_request = Instant::now();
        self.last_state_label = view.state_label.to_string();
    }

    fn mark_block_found(&mut self) {
        if let Ok(mut s) = self.state.lock() {
            let elapsed = s.started_at.elapsed().as_secs();
            s.push_block_found_tick(elapsed);
        }
        self.request_render();
    }

    fn render_now(&mut self) {
        self.request_render();
        self.last_render_request = Instant::now();
    }

    fn set_state_and_render(&mut self, state_label: &str) {
        if let Ok(mut s) = self.state.lock() {
            s.state = state_label.to_string();
        }
        self.last_state_label = state_label.to_string();
        self.render_now();
    }

    fn request_render(&self) {
        match self.render_signal.try_send(RenderSignal::RenderNow) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

impl Drop for TuiDisplay {
    fn drop(&mut self) {
        self.render_stop.store(true, Ordering::SeqCst);
        let _ = self.render_signal.try_send(RenderSignal::RenderNow);
        if let Some(handle) = self.render_worker.take() {
            let _ = handle.join();
        }

        self.quit_watcher_stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.quit_watcher.take() {
            let _ = handle.join();
        }
    }
}

fn init_tui_display(tui_state: Option<TuiState>, shutdown: Arc<AtomicBool>) -> Option<TuiDisplay> {
    let state = tui_state?;
    match TuiDisplay::new(Arc::clone(&state), shutdown) {
        Ok(display) => {
            set_tui_state(state);
            Some(display)
        }
        Err(err) => {
            warn(
                "TUI",
                format!("disabled after init failure; continuing in plain mode ({err:#})"),
            );
            None
        }
    }
}

fn update_tui(tui: &mut Option<TuiDisplay>, stats: &Stats, view: RoundUiView<'_>) {
    if let Some(display) = tui.as_mut() {
        display.update(stats, view);
    }
}

fn render_tui_now(tui: &mut Option<TuiDisplay>) {
    if let Some(display) = tui.as_mut() {
        display.render_now();
    }
}

fn set_tui_state_label(tui: &mut Option<TuiDisplay>, state_label: &str) {
    if let Some(display) = tui.as_mut() {
        display.set_state_and_render(state_label);
    }
}

fn spawn_tui_render_worker(
    state: TuiState,
    shutdown: Arc<AtomicBool>,
) -> Result<(Sender<RenderSignal>, Arc<AtomicBool>, JoinHandle<()>)> {
    let (render_signal_tx, render_signal_rx) = bounded(TUI_RENDER_SIGNAL_CAPACITY.max(1));
    let render_stop = Arc::new(AtomicBool::new(false));
    let render_stop_flag = Arc::clone(&render_stop);
    let (ready_tx, ready_rx) = bounded::<Result<()>>(1);

    let render_worker = thread::spawn(move || {
        let mut renderer = match TuiRenderer::new() {
            Ok(renderer) => {
                let _ = ready_tx.send(Ok(()));
                renderer
            }
            Err(err) => {
                let _ = ready_tx.send(Err(anyhow!("TUI renderer init failed: {err}")));
                return;
            }
        };

        if let Ok(locked) = state.lock() {
            let _ = renderer.render(&locked);
        }

        while !render_stop_flag.load(Ordering::Relaxed) && !shutdown.load(Ordering::Relaxed) {
            match render_signal_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(RenderSignal::RenderNow) => {
                    if let Ok(locked) = state.lock() {
                        let _ = renderer.render(&locked);
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
    });

    match ready_rx.recv() {
        Ok(Ok(())) => Ok((render_signal_tx, render_stop, render_worker)),
        Ok(Err(err)) => {
            render_stop.store(true, Ordering::SeqCst);
            let _ = render_worker.join();
            Err(err)
        }
        Err(_) => {
            render_stop.store(true, Ordering::SeqCst);
            let _ = render_worker.join();
            Err(anyhow!(
                "TUI renderer thread terminated before initialization"
            ))
        }
    }
}

fn spawn_tui_quit_watcher(shutdown: Arc<AtomicBool>) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_flag = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        while !stop_flag.load(Ordering::Relaxed) && !shutdown.load(Ordering::Relaxed) {
            if PROMPT_ACTIVE.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(50));
                continue;
            }
            let has_event = event::poll(TUI_QUIT_POLL_INTERVAL).unwrap_or(false);
            if !has_event {
                continue;
            }
            if let Ok(Event::Key(key)) = event::read() {
                if key.code == KeyCode::Char('q')
                    || (key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL))
                {
                    shutdown.store(true, Ordering::SeqCst);
                    break;
                }
            }
        }
    });
    (stop, handle)
}

fn current_tip_sequence(tip_signal: Option<&TipSignal>) -> u64 {
    tip_signal.map(TipSignal::snapshot_sequence).unwrap_or(0)
}

impl<'a> MiningControlPlane<'a> {
    fn new(
        client: &'a ApiClient,
        cfg: &'a Config,
        shutdown: Arc<AtomicBool>,
        tip_signal: Option<&'a TipSignal>,
    ) -> Self {
        Self {
            client,
            cfg,
            prefetch: Some(TemplatePrefetch::spawn(
                client.clone(),
                cfg.clone(),
                Arc::clone(&shutdown),
            )),
            shutdown,
            tip_signal,
        }
    }

    fn fetch_initial_template(
        &mut self,
        tui: &mut Option<TuiDisplay>,
    ) -> Option<BlockTemplateResponse> {
        fetch_template_with_retry(self.client, self.cfg, self.shutdown.as_ref(), tui)
    }

    fn spawn_prefetch_if_needed(&mut self) {
        if let Some(prefetch) = self.prefetch.as_mut() {
            prefetch.request_if_idle(current_tip_sequence(self.tip_signal));
        }
    }

    fn resolve_next_template(
        &mut self,
        tui: &mut Option<TuiDisplay>,
    ) -> Option<BlockTemplateResponse> {
        resolve_next_template(
            &mut self.prefetch,
            self.client,
            self.cfg,
            &self.shutdown,
            self.tip_signal,
            tui,
        )
    }

    fn submit_solution(
        &self,
        template: &BlockTemplateResponse,
        solution: MiningSolution,
        stats: &Stats,
        tui: &mut Option<TuiDisplay>,
    ) {
        let template_id = template.template_id.clone();
        let mut solved_block = template.block.clone();
        set_block_nonce(&mut solved_block, solution.nonce);

        stats.bump_submitted();
        match self
            .client
            .submit_block(&solved_block, template_id.as_deref(), solution.nonce)
        {
            Ok(resp) => {
                if resp.accepted {
                    stats.bump_accepted();
                    if let Some(display) = tui.as_mut() {
                        display.mark_block_found();
                    }
                    let height = resp
                        .height
                        .map(|h| h.to_string())
                        .unwrap_or_else(|| "unknown".to_string());
                    let hash = resp.hash.unwrap_or_else(|| "unknown".to_string());
                    mined("SUBMIT", format!("block accepted at height {height}"));
                    mined("SUBMIT", format!("hash {}", compact_hash(&hash)));
                } else {
                    warn("SUBMIT", "rejected by daemon");
                }
            }
            Err(err) => {
                error("SUBMIT", format!("submit failed: {err:#}"));
            }
        }
    }

    fn finish(mut self) {
        if let Some(mut prefetch_task) = self.prefetch.take() {
            if self.shutdown.load(Ordering::Relaxed) {
                prefetch_task.detach();
            } else if !prefetch_task.shutdown_for(self.cfg.prefetch_wait) {
                warn(
                    "TEMPLATE",
                    format!(
                        "prefetch worker shutdown exceeded {}ms; detached",
                        self.cfg.prefetch_wait.as_millis()
                    ),
                );
            }
        }
    }
}

pub(super) struct MiningRuntimeBackends<'a> {
    pub backends: &'a mut Vec<BackendSlot>,
    pub backend_events: &'a Receiver<BackendEvent>,
    pub backend_executor: &'a super::backend_executor::BackendExecutor,
}

pub(super) fn run_mining_loop(
    cfg: &Config,
    client: &ApiClient,
    shutdown: Arc<AtomicBool>,
    runtime_backends: MiningRuntimeBackends<'_>,
    tui_state: Option<TuiState>,
    tip_signal: Option<&TipSignal>,
) -> Result<()> {
    let MiningRuntimeBackends {
        backends,
        backend_events,
        backend_executor,
    } = runtime_backends;
    let stats = Stats::new();
    let mut nonce_scheduler = NonceScheduler::new(cfg.start_nonce, cfg.nonce_iters_per_lane);
    let mut work_id_cursor = 1u64;
    let mut epoch = 0u64;
    let mut last_stats_print = Instant::now();
    let mut tui = init_tui_display(tui_state, Arc::clone(&shutdown));
    let mut backend_weights = seed_backend_weights(backends);
    let mut control_plane = MiningControlPlane::new(client, cfg, Arc::clone(&shutdown), tip_signal);
    let recent_template_cache_size = recent_template_cache_size(cfg);
    let mut recent_templates = VecDeque::<(u64, BlockTemplateResponse)>::new();
    let mut deferred_solutions = Vec::<MiningSolution>::new();

    let mut template = match control_plane.fetch_initial_template(&mut tui) {
        Some(t) => t,
        None => {
            stats.print();
            info("MINER", "stopped");
            return Ok(());
        }
    };
    success("MINER", "connected and mining");
    info(
        "MINER",
        format!(
            "template-history | {} epochs (timeout-aware)",
            recent_template_cache_size
        ),
    );

    while !shutdown.load(Ordering::Relaxed) {
        if backends.is_empty() {
            bail!("all mining backends are unavailable");
        }

        let header_base = match decode_hex(&template.header_base, "header_base") {
            Ok(v) => v,
            Err(err) => {
                warn("TEMPLATE", format!("decode error: {err:#}"));
                if !sleep_with_shutdown(shutdown.as_ref(), TEMPLATE_RETRY_DELAY) {
                    break;
                }
                let Some(next_template) = control_plane.resolve_next_template(&mut tui) else {
                    break;
                };
                template = next_template;
                continue;
            }
        };

        if header_base.len() != POW_HEADER_BASE_LEN {
            warn(
                "TEMPLATE",
                format!(
                    "header_base length mismatch: expected {} bytes, got {}",
                    POW_HEADER_BASE_LEN,
                    header_base.len()
                ),
            );
            if !sleep_with_shutdown(shutdown.as_ref(), TEMPLATE_RETRY_DELAY) {
                break;
            }
            let Some(next_template) = control_plane.resolve_next_template(&mut tui) else {
                break;
            };
            template = next_template;
            continue;
        }

        let target = match parse_target(&template.target) {
            Ok(t) => t,
            Err(err) => {
                warn("TEMPLATE", format!("target parse error: {err:#}"));
                if !sleep_with_shutdown(shutdown.as_ref(), TEMPLATE_RETRY_DELAY) {
                    break;
                }
                let Some(next_template) = control_plane.resolve_next_template(&mut tui) else {
                    break;
                };
                template = next_template;
                continue;
            }
        };
        let header_base: Arc<[u8]> = Arc::from(header_base);

        let template_height_u64 = template_height(&template.block);
        if let Some(height) = template_height_u64 {
            if let Some(signal) = tip_signal {
                signal.set_current_template_height(height);
            }
        }
        let height = template_height_u64
            .map(|h| h.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let difficulty = template_difficulty(&template.block)
            .map(|d| d.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        epoch = epoch.wrapping_add(1).max(1);
        let work_id = next_work_id(&mut work_id_cursor);
        let reservation = nonce_scheduler.reserve(total_lanes(backends));
        let stop_at = Instant::now() + cfg.refresh_interval;

        stats.bump_templates();

        let additional_span = distribute_work(
            backends,
            super::DistributeWorkOptions {
                epoch,
                work_id,
                header_base: Arc::clone(&header_base),
                target,
                reservation,
                stop_at,
                assignment_timeout: cfg.backend_assign_timeout,
                backend_weights: work_distribution_weights(cfg.work_allocation, &backend_weights),
            },
            backend_executor,
        )?;
        nonce_scheduler.consume_additional_span(additional_span);
        control_plane.spawn_prefetch_if_needed();

        let round_start = Instant::now();
        let mut round_runtime = RoundRuntime {
            cfg,
            shutdown: shutdown.as_ref(),
            backends,
            backend_events,
            tip_signal,
            backend_executor,
            stats: &stats,
            tui: &mut tui,
            last_stats_print: &mut last_stats_print,
            nonce_scheduler: &mut nonce_scheduler,
            backend_weights: &backend_weights,
            deferred_solutions: &mut deferred_solutions,
        };
        let mut round_state = round_runtime.run(RoundInput {
            epoch,
            work_id,
            stop_at,
            round_start,
            height: &height,
            difficulty: &difficulty,
            header_base: &header_base,
            target,
        })?;
        let mut submitted_solution = None;
        let mut pending_solution = round_state.solved.take();
        let _ = drain_mining_backend_events(
            backend_events,
            epoch,
            &mut pending_solution,
            &mut deferred_solutions,
            backends,
            backend_executor,
        )?;
        let solved_found = pending_solution.is_some();
        let append_semantics_active = super::backends_have_append_assignment_semantics(backends);

        if cfg.strict_round_accounting {
            let _ = quiesce_backend_slots(
                backends,
                RuntimeMode::Mining,
                cfg.backend_control_timeout,
                backend_executor,
            )?;
        } else if should_cancel_relaxed_round(
            round_state.stale_tip_event,
            solved_found,
            append_semantics_active,
        ) {
            let _ = cancel_backend_slots(
                backends,
                RuntimeMode::Mining,
                cfg.backend_control_timeout,
                backend_executor,
            )?;
        }
        let _ = drain_mining_backend_events(
            backend_events,
            epoch,
            &mut pending_solution,
            &mut deferred_solutions,
            backends,
            backend_executor,
        )?;
        if let Some(solution) = pending_solution.take() {
            control_plane.submit_solution(&template, solution.clone(), &stats, &mut tui);
            submitted_solution = Some(solution);
        }
        drop_solution_from_deferred(&mut deferred_solutions, submitted_solution.as_ref());
        submit_deferred_solutions(
            &control_plane,
            epoch,
            &template,
            &recent_templates,
            &mut deferred_solutions,
            &stats,
            &mut tui,
        );
        collect_backend_hashes(
            backends,
            Some(&stats),
            &mut round_state.round_hashes,
            Some(&mut round_state.round_backend_hashes),
            Some(&mut round_state.round_backend_telemetry),
        );
        round_state.stale_tip_event |= tip_signal.is_some_and(TipSignal::take_stale);
        let round_end_reason = if shutdown.load(Ordering::Relaxed) {
            RoundEndReason::Shutdown
        } else if submitted_solution.is_some() {
            RoundEndReason::Solved
        } else if round_state.stale_tip_event {
            RoundEndReason::StaleTip
        } else {
            RoundEndReason::Refresh
        };
        let round_elapsed_secs = round_start.elapsed().as_secs_f64();
        update_backend_weights(
            &mut backend_weights,
            WeightUpdateInputs {
                backends,
                round_backend_hashes: &round_state.round_backend_hashes,
                round_backend_telemetry: Some(&round_state.round_backend_telemetry),
                round_elapsed_secs,
                mode: cfg.work_allocation,
                round_end_reason,
                refresh_interval: cfg.refresh_interval,
            },
        );
        let telemetry_line =
            format_round_backend_telemetry(backends, &round_state.round_backend_telemetry);
        if telemetry_line != "none" {
            info("BACKEND", format!("telemetry | {telemetry_line}"));
        }

        if let Some(solution) = submitted_solution {
            update_tui(
                &mut tui,
                &stats,
                RoundUiView {
                    backends,
                    round_backend_hashes: &round_state.round_backend_hashes,
                    round_hashes: round_state.round_hashes,
                    round_start,
                    height: &height,
                    difficulty: &difficulty,
                    epoch,
                    state_label: "solved",
                },
            );
            mined(
                "SOLVE",
                format!(
                    "solution found! elapsed={:.2}s backend={}#{}",
                    round_start.elapsed().as_secs_f64(),
                    solution.backend,
                    solution.backend_id,
                ),
            );
        } else if round_state.stale_tip_event {
            update_tui(
                &mut tui,
                &stats,
                RoundUiView {
                    backends,
                    round_backend_hashes: &round_state.round_backend_hashes,
                    round_hashes: round_state.round_hashes,
                    round_start,
                    height: &height,
                    difficulty: &difficulty,
                    epoch,
                    state_label: "stale-refresh",
                },
            );
        } else {
            update_tui(
                &mut tui,
                &stats,
                RoundUiView {
                    backends,
                    round_backend_hashes: &round_state.round_backend_hashes,
                    round_hashes: round_state.round_hashes,
                    round_start,
                    height: &height,
                    difficulty: &difficulty,
                    epoch,
                    state_label: "refresh",
                },
            );
        }

        maybe_print_stats(
            &stats,
            &mut last_stats_print,
            cfg.stats_interval,
            tui.is_none(),
        );

        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let Some(next_template) = control_plane.resolve_next_template(&mut tui) else {
            break;
        };
        remember_recent_template(
            &mut recent_templates,
            epoch,
            template,
            recent_template_cache_size,
        );
        template = next_template;
    }

    if !backends.is_empty() {
        match quiesce_backend_slots(
            backends,
            RuntimeMode::Mining,
            cfg.backend_control_timeout,
            backend_executor,
        ) {
            Ok(_) => {}
            Err(err) => warn("BACKEND", format!("final backend quiesce failed: {err:#}")),
        }
        let mut final_pending_solution = None;
        match drain_mining_backend_events(
            backend_events,
            epoch,
            &mut final_pending_solution,
            &mut deferred_solutions,
            backends,
            backend_executor,
        ) {
            Ok(_) => {}
            Err(err) => warn(
                "BACKEND",
                format!("final backend event drain failed: {err:#}"),
            ),
        }
        if let Some(solution) = final_pending_solution.as_ref() {
            control_plane.submit_solution(&template, solution.clone(), &stats, &mut tui);
        }
        drop_solution_from_deferred(&mut deferred_solutions, final_pending_solution.as_ref());
        submit_deferred_solutions(
            &control_plane,
            epoch,
            &template,
            &recent_templates,
            &mut deferred_solutions,
            &stats,
            &mut tui,
        );
    }

    control_plane.finish();

    stats.print();
    info("MINER", "stopped");
    Ok(())
}

fn should_cancel_relaxed_round(
    stale_tip_event: bool,
    solved_found: bool,
    append_semantics_active: bool,
) -> bool {
    stale_tip_event || solved_found || append_semantics_active
}

type BackendPollState = BTreeMap<u64, (Duration, Instant)>;

fn backend_poll_interval(slot: &BackendSlot, configured: Duration) -> Duration {
    let configured = configured.max(Duration::from_millis(1));
    let hinted = super::backend_capabilities(slot)
        .preferred_hash_poll_interval
        .filter(|hint| *hint > Duration::from_millis(0))
        .map(|hint| hint.max(Duration::from_millis(1)));
    hinted.map_or(configured, |hint| configured.min(hint))
}

fn build_backend_poll_state(backends: &[BackendSlot], configured: Duration) -> BackendPollState {
    let now = Instant::now();
    let mut state = BackendPollState::new();
    for slot in backends {
        let interval = backend_poll_interval(slot, configured);
        state.insert(slot.id, (interval, now + interval));
    }
    state
}

fn next_backend_poll_deadline(poll_state: &BackendPollState) -> Instant {
    poll_state
        .values()
        .map(|(_, next_poll)| *next_poll)
        .min()
        .unwrap_or_else(Instant::now)
}

fn collect_due_backend_hashes(
    backends: &[BackendSlot],
    configured: Duration,
    poll_state: &mut BackendPollState,
    stats: Option<&Stats>,
    round_hashes: &mut u64,
    round_backend_hashes: &mut BTreeMap<u64, u64>,
    round_backend_telemetry: &mut BTreeMap<u64, BackendRoundTelemetry>,
) {
    let now = Instant::now();
    let mut collected = 0u64;

    for slot in backends {
        let (interval, next_poll) = poll_state.entry(slot.id).or_insert_with(|| {
            let interval = backend_poll_interval(slot, configured);
            (interval, now + interval)
        });
        if now < *next_poll {
            continue;
        }

        let slot_hashes = slot.backend.take_hashes();
        let telemetry = slot.backend.take_telemetry();
        super::merge_backend_telemetry(round_backend_telemetry, slot.id, telemetry);

        if slot_hashes > 0 {
            collected = collected.saturating_add(slot_hashes);
            let entry = round_backend_hashes.entry(slot.id).or_insert(0);
            *entry = entry.saturating_add(slot_hashes);
        }

        *next_poll = now + *interval;
    }

    if collected == 0 {
        return;
    }
    if let Some(stats) = stats {
        stats.add_hashes(collected);
    }
    *round_hashes = round_hashes.saturating_add(collected);
}

struct RoundInput<'a> {
    epoch: u64,
    work_id: u64,
    stop_at: Instant,
    round_start: Instant,
    height: &'a str,
    difficulty: &'a str,
    header_base: &'a Arc<[u8]>,
    target: [u8; 32],
}

struct RoundRuntime<'a> {
    cfg: &'a Config,
    shutdown: &'a AtomicBool,
    backends: &'a mut Vec<BackendSlot>,
    backend_events: &'a Receiver<BackendEvent>,
    tip_signal: Option<&'a TipSignal>,
    backend_executor: &'a super::backend_executor::BackendExecutor,
    stats: &'a Stats,
    tui: &'a mut Option<TuiDisplay>,
    last_stats_print: &'a mut Instant,
    nonce_scheduler: &'a mut NonceScheduler,
    backend_weights: &'a BTreeMap<u64, f64>,
    deferred_solutions: &'a mut Vec<MiningSolution>,
}

impl<'a> RoundRuntime<'a> {
    fn run(&mut self, input: RoundInput<'_>) -> Result<RoundLoopState> {
        let mut solved: Option<MiningSolution> = None;
        let mut stale_tip_event = false;
        let mut round_hashes = 0u64;
        let mut round_backend_hashes = BTreeMap::new();
        let mut round_backend_telemetry = BTreeMap::new();
        let mut topology_changed = false;
        let mut backend_poll_state =
            build_backend_poll_state(self.backends, self.cfg.hash_poll_interval);
        update_tui(
            self.tui,
            self.stats,
            RoundUiView {
                backends: self.backends,
                round_backend_hashes: &round_backend_hashes,
                round_hashes,
                round_start: input.round_start,
                height: input.height,
                difficulty: input.difficulty,
                epoch: input.epoch,
                state_label: "working",
            },
        );

        while !self.shutdown.load(Ordering::Relaxed)
            && Instant::now() < input.stop_at
            && solved.is_none()
            && !stale_tip_event
        {
            let hashes_before = round_hashes;
            collect_due_backend_hashes(
                self.backends,
                self.cfg.hash_poll_interval,
                &mut backend_poll_state,
                Some(self.stats),
                &mut round_hashes,
                &mut round_backend_hashes,
                &mut round_backend_telemetry,
            );
            if round_hashes != hashes_before {
                update_tui(
                    self.tui,
                    self.stats,
                    RoundUiView {
                        backends: self.backends,
                        round_backend_hashes: &round_backend_hashes,
                        round_hashes,
                        round_start: input.round_start,
                        height: input.height,
                        difficulty: input.difficulty,
                        epoch: input.epoch,
                        state_label: "working",
                    },
                );
            }

            maybe_print_stats(
                self.stats,
                self.last_stats_print,
                self.cfg.stats_interval,
                self.tui.is_none(),
            );

            if self.tip_signal.is_some_and(TipSignal::take_stale) {
                stale_tip_event = true;
                update_tui(
                    self.tui,
                    self.stats,
                    RoundUiView {
                        backends: self.backends,
                        round_backend_hashes: &round_backend_hashes,
                        round_hashes,
                        round_start: input.round_start,
                        height: input.height,
                        difficulty: input.difficulty,
                        epoch: input.epoch,
                        state_label: "stale-tip",
                    },
                );
                continue;
            }

            let next_hash_poll_at = next_backend_poll_deadline(&backend_poll_state);
            let wait_for = next_event_wait(
                input.stop_at,
                *self.last_stats_print,
                self.cfg.stats_interval,
                next_hash_poll_at,
                self.tui.is_none(),
            );

            crossbeam_channel::select! {
                recv(self.backend_events) -> event => {
                    let event = event.map_err(|_| anyhow!("backend event channel closed"))?;
                    if handle_mining_backend_event(
                        event,
                        input.epoch,
                        &mut solved,
                        self.deferred_solutions,
                        self.backends,
                        self.backend_executor,
                    )? == BackendEventAction::TopologyChanged
                    {
                        topology_changed = true;
                    }
                }
                default(wait_for) => {}
            }

            if topology_changed
                && !self.shutdown.load(Ordering::Relaxed)
                && solved.is_none()
                && !stale_tip_event
                && Instant::now() < input.stop_at
                && !self.backends.is_empty()
            {
                redistribute_for_topology_change(
                    self.backends,
                    TopologyRedistributionOptions {
                        epoch: input.epoch,
                        work_id: input.work_id,
                        header_base: Arc::clone(input.header_base),
                        target: input.target,
                        stop_at: input.stop_at,
                        assignment_timeout: self.cfg.backend_assign_timeout,
                        control_timeout: self.cfg.backend_control_timeout,
                        mode: RuntimeMode::Mining,
                        work_allocation: self.cfg.work_allocation,
                        backend_weights: Some(self.backend_weights),
                        nonce_scheduler: self.nonce_scheduler,
                        backend_executor: self.backend_executor,
                        log_tag: "BACKEND",
                    },
                )?;
                if self.backends.is_empty() {
                    break;
                }
                backend_poll_state =
                    build_backend_poll_state(self.backends, self.cfg.hash_poll_interval);
                topology_changed = false;
                update_tui(
                    self.tui,
                    self.stats,
                    RoundUiView {
                        backends: self.backends,
                        round_backend_hashes: &round_backend_hashes,
                        round_hashes,
                        round_start: input.round_start,
                        height: input.height,
                        difficulty: input.difficulty,
                        epoch: input.epoch,
                        state_label: "rebalanced",
                    },
                );
            }
        }

        Ok(RoundLoopState {
            solved,
            stale_tip_event,
            round_hashes,
            round_backend_hashes,
            round_backend_telemetry,
        })
    }
}

fn resolve_next_template(
    prefetch: &mut Option<TemplatePrefetch>,
    client: &ApiClient,
    cfg: &Config,
    shutdown: &Arc<AtomicBool>,
    tip_signal: Option<&TipSignal>,
    tui: &mut Option<TuiDisplay>,
) -> Option<BlockTemplateResponse> {
    if shutdown.load(Ordering::Relaxed) {
        if let Some(task) = prefetch.take() {
            task.detach();
        }
        return None;
    }

    if prefetch.is_none() {
        *prefetch = Some(TemplatePrefetch::spawn(
            client.clone(),
            cfg.clone(),
            Arc::clone(shutdown),
        ));
    }

    if let Some(task) = prefetch.as_mut() {
        let prefetch_budget = cfg.prefetch_wait.max(Duration::from_millis(1));
        let prefetch_started = Instant::now();
        loop {
            let latest_tip_sequence = current_tip_sequence(tip_signal);
            task.request_if_idle(latest_tip_sequence);

            let remaining = prefetch_budget
                .saturating_sub(prefetch_started.elapsed())
                .max(Duration::from_millis(1));
            let Some((tip_sequence, template)) = task.wait_for_result(remaining) else {
                break;
            };
            let latest_after_wait = current_tip_sequence(tip_signal);
            if tip_sequence == latest_after_wait {
                if let Some(template) = template {
                    return Some(template);
                }
                break;
            }

            task.request_if_idle(latest_after_wait);
            if prefetch_started.elapsed() >= prefetch_budget {
                break;
            }
        }
    }

    fetch_template_with_retry(client, cfg, shutdown.as_ref(), tui)
}

fn handle_mining_backend_event(
    event: BackendEvent,
    epoch: u64,
    solved: &mut Option<MiningSolution>,
    deferred_solutions: &mut Vec<MiningSolution>,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
) -> Result<BackendEventAction> {
    let (action, maybe_solution) = super::handle_runtime_backend_event(
        event,
        epoch,
        backends,
        RuntimeMode::Mining,
        backend_executor,
    )?;
    if let Some(solution) = maybe_solution {
        if solution.epoch == epoch {
            if solved.is_none() {
                *solved = Some(solution);
            } else {
                deferred_solutions.push(solution);
            }
        } else if solution.epoch < epoch {
            deferred_solutions.push(solution);
        } else {
            warn(
                "BACKEND",
                format!(
                    "ignoring future solution from {}#{} epoch={} current_epoch={}",
                    solution.backend, solution.backend_id, solution.epoch, epoch
                ),
            );
        }
    }
    Ok(action)
}

fn drain_mining_backend_events(
    backend_events: &Receiver<BackendEvent>,
    epoch: u64,
    solved: &mut Option<MiningSolution>,
    deferred_solutions: &mut Vec<MiningSolution>,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
) -> Result<BackendEventAction> {
    let (action, solutions) = super::drain_runtime_backend_events(
        backend_events,
        epoch,
        backends,
        RuntimeMode::Mining,
        backend_executor,
    )?;
    for solution in solutions {
        if solution.epoch == epoch {
            if solved.is_none() {
                *solved = Some(solution);
            } else {
                deferred_solutions.push(solution);
            }
        } else if solution.epoch < epoch {
            deferred_solutions.push(solution);
        } else {
            warn(
                "BACKEND",
                format!(
                    "ignoring future solution from {}#{} epoch={} current_epoch={}",
                    solution.backend, solution.backend_id, solution.epoch, epoch
                ),
            );
        }
    }
    Ok(action)
}

fn submit_deferred_solutions(
    control_plane: &MiningControlPlane<'_>,
    current_epoch: u64,
    current_template: &BlockTemplateResponse,
    recent_templates: &VecDeque<(u64, BlockTemplateResponse)>,
    deferred_solutions: &mut Vec<MiningSolution>,
    stats: &Stats,
    tui: &mut Option<TuiDisplay>,
) {
    if deferred_solutions.is_empty() {
        return;
    }

    let mut queued = Vec::new();
    std::mem::swap(&mut queued, deferred_solutions);
    for solution in dedupe_queued_solutions(queued) {
        let Some(template) = template_for_solution_epoch(
            current_epoch,
            current_template,
            recent_templates,
            solution.epoch,
        ) else {
            warn(
                "BACKEND",
                format!(
                    "dropping stale solution from {}#{} epoch={} (current_epoch={})",
                    solution.backend, solution.backend_id, solution.epoch, current_epoch
                ),
            );
            continue;
        };
        control_plane.submit_solution(template, solution, stats, tui);
    }
}

fn dedupe_queued_solutions(queued: Vec<MiningSolution>) -> Vec<MiningSolution> {
    let mut seen = HashSet::new();

    let mut deduped = Vec::with_capacity(queued.len());
    for solution in queued {
        if seen.insert((solution.epoch, solution.nonce)) {
            deduped.push(solution);
        }
    }
    deduped
}

fn drop_solution_from_deferred(
    deferred_solutions: &mut Vec<MiningSolution>,
    primary_submitted: Option<&MiningSolution>,
) {
    let Some(solution) = primary_submitted else {
        return;
    };
    deferred_solutions.retain(|candidate| {
        !(candidate.epoch == solution.epoch && candidate.nonce == solution.nonce)
    });
}

fn template_for_solution_epoch<'a>(
    current_epoch: u64,
    current_template: &'a BlockTemplateResponse,
    recent_templates: &'a VecDeque<(u64, BlockTemplateResponse)>,
    solution_epoch: u64,
) -> Option<&'a BlockTemplateResponse> {
    if solution_epoch == current_epoch {
        return Some(current_template);
    }
    for (epoch, template) in recent_templates.iter().rev() {
        if *epoch == solution_epoch {
            return Some(template);
        }
    }
    None
}

fn remember_recent_template(
    recent_templates: &mut VecDeque<(u64, BlockTemplateResponse)>,
    epoch: u64,
    template: BlockTemplateResponse,
    max_entries: usize,
) {
    let max_entries = max_entries.max(1);
    recent_templates.push_back((epoch, template));
    while recent_templates.len() > max_entries {
        recent_templates.pop_front();
    }
}

fn recent_template_cache_size(cfg: &Config) -> usize {
    recent_template_cache_size_from_timeouts(
        cfg.refresh_interval,
        cfg.backend_control_timeout,
        cfg.backend_assign_timeout,
        cfg.prefetch_wait,
    )
}

fn recent_template_cache_size_from_timeouts(
    refresh_interval: Duration,
    backend_control_timeout: Duration,
    backend_assign_timeout: Duration,
    prefetch_wait: Duration,
) -> usize {
    let refresh_millis = refresh_interval.max(Duration::from_millis(1)).as_millis();
    let grace_millis = backend_control_timeout
        .as_millis()
        .saturating_add(backend_assign_timeout.as_millis())
        .saturating_add(prefetch_wait.as_millis())
        .saturating_add(refresh_millis);
    let rounds_needed = grace_millis
        .saturating_div(refresh_millis)
        .saturating_add(RECENT_TEMPLATE_CACHE_HEADROOM_ROUNDS as u128);
    rounds_needed.clamp(
        RECENT_TEMPLATE_CACHE_MIN as u128,
        RECENT_TEMPLATE_CACHE_MAX as u128,
    ) as usize
}

fn fetch_template_with_retry(
    client: &ApiClient,
    cfg: &Config,
    shutdown: &AtomicBool,
    tui: &mut Option<TuiDisplay>,
) -> Option<BlockTemplateResponse> {
    let mut retry = RetryTracker::default();

    while !shutdown.load(Ordering::Relaxed) {
        match client.get_block_template() {
            Ok(template) => {
                retry.note_recovered("NETWORK", "blocktemplate fetch recovered");
                render_tui_now(tui);
                return Some(template);
            }
            Err(err) if is_no_wallet_loaded_error(&err) => {
                set_tui_state_label(tui, "awaiting-wallet");
                warn(
                    "WALLET",
                    "blocktemplate requires loaded wallet; attempting automatic load",
                );
                render_tui_now(tui);
                match auto_load_wallet(client, cfg, shutdown, tui) {
                    Ok(true) => continue,
                    Ok(false) => {
                        warn(
                            "WALLET",
                            "unable to auto-load wallet; use --wallet-password, --wallet-password-file, SEINE_WALLET_PASSWORD, or interactive prompt",
                        );
                        render_tui_now(tui);
                        return None;
                    }
                    Err(load_err) => {
                        error(
                            "WALLET",
                            format!("automatic wallet load failed: {load_err:#}"),
                        );
                        render_tui_now(tui);
                        return None;
                    }
                }
            }
            Err(err) if is_unauthorized_error(&err) => {
                match refresh_api_token_from_cookie(client, cfg.token_cookie_path.as_deref()) {
                    TokenRefreshOutcome::Refreshed => {
                        success("AUTH", "auth refreshed from cookie");
                        render_tui_now(tui);
                        continue;
                    }
                    TokenRefreshOutcome::Unchanged => {
                        retry.note_failure(
                            "AUTH",
                            "auth expired; waiting for new cookie token",
                            "auth still expired; waiting for new cookie token",
                            true,
                        );
                    }
                    TokenRefreshOutcome::Unavailable => {
                        retry.note_failure(
                            "AUTH",
                            "auth failed; static --token cannot auto-refresh",
                            "still waiting for manual token refresh",
                            true,
                        );
                    }
                    TokenRefreshOutcome::Failed(msg) => {
                        retry.note_failure(
                            "AUTH",
                            &msg,
                            "failed to refresh auth token from cookie",
                            true,
                        );
                    }
                }
                render_tui_now(tui);
                if !sleep_with_shutdown(shutdown, TEMPLATE_RETRY_DELAY) {
                    break;
                }
            }
            Err(_) => {
                retry.note_failure(
                    "NETWORK",
                    "failed to fetch blocktemplate; retrying",
                    "still failing to fetch blocktemplate; retrying",
                    true,
                );
                render_tui_now(tui);
                if !sleep_with_shutdown(shutdown, TEMPLATE_RETRY_DELAY) {
                    break;
                }
            }
        }
    }

    None
}

fn compact_hash(hash: &str) -> String {
    let value = hash.trim();
    let len = value.chars().count();
    if len <= 8 {
        return value.to_string();
    }

    let prefix: String = value.chars().take(4).collect();
    let suffix: String = value
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{prefix}...{suffix}")
}

fn sleep_with_shutdown(shutdown: &AtomicBool, duration: Duration) -> bool {
    let deadline = Instant::now() + duration;
    while !shutdown.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        let sleep_for = deadline
            .saturating_duration_since(now)
            .min(Duration::from_millis(100));
        thread::sleep(sleep_for);
    }
    false
}

fn auto_load_wallet(
    client: &ApiClient,
    cfg: &Config,
    shutdown: &AtomicBool,
    tui: &mut Option<TuiDisplay>,
) -> Result<bool> {
    const MAX_PROMPT_ATTEMPTS: u32 = 3;

    let (mut password, source) = match resolve_wallet_password(cfg)? {
        Some((password, source)) => (password, source),
        None => {
            let Some(password) = prompt_wallet_password(tui)? else {
                return Ok(false);
            };
            (password, WalletPasswordSource::Prompt)
        }
    };
    let mut prompt_attempt = 1u32;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(false);
        }

        match client.load_wallet(&password) {
            Ok(()) => {
                success("WALLET", format!("loaded via {}", source.as_str()));
                password.clear();
                return Ok(true);
            }
            Err(err) if is_wallet_already_loaded_error(&err) => {
                info("WALLET", "already loaded");
                password.clear();
                return Ok(true);
            }
            Err(err)
                if source == WalletPasswordSource::Prompt
                    && prompt_attempt < MAX_PROMPT_ATTEMPTS =>
            {
                warn("WALLET", format!("load failed: {err:#}"));
                render_tui_now(tui);
                prompt_attempt += 1;
                let Some(next_password) = prompt_wallet_password(tui)? else {
                    return Ok(false);
                };
                password.clear();
                password = next_password;
                continue;
            }
            Err(err) => {
                password.clear();
                return Err(err).with_context(|| {
                    format!("wallet load request failed using {}", source.as_str())
                });
            }
        }
    }
}

fn resolve_wallet_password(cfg: &Config) -> Result<Option<(String, WalletPasswordSource)>> {
    if let Some(password) = &cfg.wallet_password {
        if password.is_empty() {
            bail!("--wallet-password is empty");
        }
        return Ok(Some((password.clone(), WalletPasswordSource::CliFlag)));
    }

    if let Some(path) = &cfg.wallet_password_file {
        let password = read_password_file(path)?;
        if password.is_empty() {
            bail!("wallet password file is empty: {}", path.display());
        }
        return Ok(Some((password, WalletPasswordSource::PasswordFile)));
    }

    if let Ok(password) = env::var("SEINE_WALLET_PASSWORD") {
        if !password.is_empty() {
            return Ok(Some((password, WalletPasswordSource::Environment)));
        }
    }

    if let Ok(password) = env::var("BNMINER_WALLET_PASSWORD") {
        if !password.is_empty() {
            return Ok(Some((password, WalletPasswordSource::Environment)));
        }
    }

    Ok(None)
}

fn read_password_file(path: &std::path::Path) -> Result<String> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read wallet password file at {}", path.display()))?;
    Ok(raw.trim_end_matches(['\r', '\n']).to_string())
}

fn prompt_wallet_password(tui: &mut Option<TuiDisplay>) -> Result<Option<String>> {
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return Ok(None);
    }

    PROMPT_ACTIVE.store(true, Ordering::Release);
    struct PromptGuard;
    impl Drop for PromptGuard {
        fn drop(&mut self) {
            PROMPT_ACTIVE.store(false, Ordering::Release);
        }
    }
    let _prompt_guard = PromptGuard;

    let raw_mode = is_raw_mode_enabled().unwrap_or(false);

    set_tui_state_label(tui, "awaiting-wallet");
    error(
        "WALLET",
        "ACTION REQUIRED: wallet password needed to continue mining",
    );
    warn(
        "WALLET",
        "password input is hidden; type password and press Enter",
    );
    render_tui_now(tui);
    if raw_mode {
        return prompt_wallet_password_raw_mode();
    }

    let password = rpassword::prompt_password("wallet password (input hidden): ")
        .context("failed to read wallet password from terminal")?;
    if password.is_empty() {
        return Ok(None);
    }
    Ok(Some(password))
}

fn prompt_wallet_password_raw_mode() -> Result<Option<String>> {
    let mut password = String::new();

    loop {
        let event = event::read().context("failed to read wallet password key event")?;
        let Event::Key(key) = event else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }

        match key.code {
            KeyCode::Enter => break,
            KeyCode::Esc => return Ok(None),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Ok(None);
            }
            KeyCode::Backspace => {
                password.pop();
            }
            KeyCode::Char(ch) => {
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                {
                    password.push(ch);
                }
            }
            _ => {}
        }
    }

    if password.is_empty() {
        return Ok(None);
    }
    Ok(Some(password))
}

#[cfg(test)]
mod tests {
    use crossbeam_channel::Sender;
    use serde_json::json;

    use super::*;
    use crate::backend::{BackendInstanceId, PowBackend, WorkAssignment};
    use crate::config::WorkAllocation;
    use anyhow::Result;

    struct NoopBackend {
        name: &'static str,
    }

    impl NoopBackend {
        fn new(name: &'static str) -> Self {
            Self { name }
        }
    }

    impl PowBackend for NoopBackend {
        fn name(&self) -> &'static str {
            self.name
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
    }

    #[test]
    fn stale_solution_is_ignored_for_current_epoch() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let mut solved = None;
        let mut deferred = Vec::new();
        let mut backends = Vec::new();

        let action = handle_mining_backend_event(
            BackendEvent::Solution(MiningSolution {
                epoch: 41,
                nonce: 9,
                backend_id: 1,
                backend: "cpu",
            }),
            42,
            &mut solved,
            &mut deferred,
            &mut backends,
            &backend_executor,
        )
        .expect("stale solution handling should succeed");

        assert_eq!(action, BackendEventAction::None);
        assert!(solved.is_none());
        assert!(deferred.is_empty());
    }

    #[test]
    fn stale_solution_from_active_backend_is_deferred() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let mut solved = None;
        let mut deferred = Vec::new();
        let mut backends = vec![BackendSlot {
            id: 1,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
        }];

        let action = handle_mining_backend_event(
            BackendEvent::Solution(MiningSolution {
                epoch: 41,
                nonce: 9,
                backend_id: 1,
                backend: "cpu",
            }),
            42,
            &mut solved,
            &mut deferred,
            &mut backends,
            &backend_executor,
        )
        .expect("stale solution should be deferred");

        assert_eq!(action, BackendEventAction::None);
        assert!(solved.is_none());
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].epoch, 41);
    }

    #[test]
    fn same_epoch_solution_is_deferred_when_one_is_already_selected() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let mut solved = Some(MiningSolution {
            epoch: 42,
            nonce: 7,
            backend_id: 1,
            backend: "cpu",
        });
        let mut deferred = Vec::new();
        let mut backends = vec![BackendSlot {
            id: 1,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
        }];

        let action = handle_mining_backend_event(
            BackendEvent::Solution(MiningSolution {
                epoch: 42,
                nonce: 11,
                backend_id: 1,
                backend: "cpu",
            }),
            42,
            &mut solved,
            &mut deferred,
            &mut backends,
            &backend_executor,
        )
        .expect("extra same-epoch solution should be deferred");

        assert_eq!(action, BackendEventAction::None);
        assert_eq!(solved.as_ref().map(|solution| solution.nonce), Some(7));
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].epoch, 42);
        assert_eq!(deferred[0].nonce, 11);
    }

    #[test]
    fn drain_mining_backend_events_keeps_all_solutions() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let (event_tx, event_rx) = crossbeam_channel::bounded(8);
        let mut solved = None;
        let mut deferred = Vec::new();
        let mut backends = vec![BackendSlot {
            id: 1,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
        }];

        event_tx
            .send(BackendEvent::Solution(MiningSolution {
                epoch: 42,
                nonce: 3,
                backend_id: 1,
                backend: "cpu",
            }))
            .expect("enqueue current solution");
        event_tx
            .send(BackendEvent::Solution(MiningSolution {
                epoch: 41,
                nonce: 5,
                backend_id: 1,
                backend: "cpu",
            }))
            .expect("enqueue stale solution");
        event_tx
            .send(BackendEvent::Solution(MiningSolution {
                epoch: 42,
                nonce: 9,
                backend_id: 1,
                backend: "cpu",
            }))
            .expect("enqueue additional current solution");

        let action = drain_mining_backend_events(
            &event_rx,
            42,
            &mut solved,
            &mut deferred,
            &mut backends,
            &backend_executor,
        )
        .expect("drain should succeed");

        assert_eq!(action, BackendEventAction::None);
        assert_eq!(solved.as_ref().map(|solution| solution.nonce), Some(3));
        assert_eq!(deferred.len(), 2);
        assert!(deferred
            .iter()
            .any(|solution| solution.epoch == 41 && solution.nonce == 5));
        assert!(deferred
            .iter()
            .any(|solution| solution.epoch == 42 && solution.nonce == 9));
    }

    #[test]
    fn dedupe_queued_solutions_skips_repeated_solutions() {
        let queued = vec![
            MiningSolution {
                epoch: 4,
                nonce: 7,
                backend_id: 1,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 5,
                nonce: 9,
                backend_id: 1,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 4,
                nonce: 7,
                backend_id: 1,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 4,
                nonce: 7,
                backend_id: 2,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 4,
                nonce: 7,
                backend_id: 1,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 4,
                nonce: 7,
                backend_id: 1,
                backend: "cpu",
            },
        ];

        let deduped = dedupe_queued_solutions(queued);
        assert_eq!(deduped.len(), 2);
        assert!(deduped
            .iter()
            .any(|solution| solution.epoch == 4 && solution.nonce == 7));
        assert!(deduped
            .iter()
            .any(|solution| solution.epoch == 5 && solution.nonce == 9));
    }

    #[test]
    fn drop_solution_from_deferred_filters_primary_solution() {
        let primary = MiningSolution {
            epoch: 5,
            nonce: 42,
            backend_id: 1,
            backend: "cpu",
        };
        let mut deferred = vec![
            MiningSolution {
                epoch: 5,
                nonce: 42,
                backend_id: 1,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 5,
                nonce: 42,
                backend_id: 2,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 5,
                nonce: 9,
                backend_id: 1,
                backend: "cpu",
            },
        ];

        drop_solution_from_deferred(&mut deferred, Some(&primary));
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].nonce, 9);
    }

    #[test]
    fn template_selection_matches_current_or_previous_epoch() {
        let current = sample_template("curr");
        let previous = sample_template("prev");
        let mut recent = VecDeque::new();
        recent.push_back((9, previous));

        assert!(template_for_solution_epoch(10, &current, &recent, 10).is_some());
        assert!(template_for_solution_epoch(10, &current, &recent, 9).is_some());
        assert!(template_for_solution_epoch(10, &current, &recent, 8).is_none());
    }

    #[test]
    fn remember_recent_template_keeps_bounded_history() {
        let max_entries = 6usize;
        let mut recent = VecDeque::new();
        for epoch in 1..=(max_entries as u64 + 2) {
            remember_recent_template(&mut recent, epoch, sample_template("tmpl"), max_entries);
        }

        assert_eq!(recent.len(), max_entries);
        assert_eq!(recent.front().map(|entry| entry.0), Some(3));
        assert_eq!(
            recent.back().map(|entry| entry.0),
            Some(max_entries as u64 + 2)
        );
    }

    #[test]
    fn recent_template_cache_size_uses_timeout_window_and_bounds() {
        let min_entries = recent_template_cache_size_from_timeouts(
            Duration::from_secs(20),
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_millis(250),
        );
        assert_eq!(min_entries, RECENT_TEMPLATE_CACHE_MIN);

        let scaled_entries = recent_template_cache_size_from_timeouts(
            Duration::from_secs(1),
            Duration::from_secs(120),
            Duration::from_secs(2),
            Duration::from_millis(500),
        );
        assert!(scaled_entries > RECENT_TEMPLATE_CACHE_MIN);

        let capped_entries = recent_template_cache_size_from_timeouts(
            Duration::from_millis(1),
            Duration::from_secs(3_600),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        assert_eq!(capped_entries, RECENT_TEMPLATE_CACHE_MAX);
    }

    #[test]
    fn relaxed_round_cancel_triggers_on_solved_or_stale_tip() {
        assert!(should_cancel_relaxed_round(false, true, false));
        assert!(should_cancel_relaxed_round(true, false, false));
        assert!(should_cancel_relaxed_round(false, false, true));
        assert!(!should_cancel_relaxed_round(false, false, false));
    }

    #[test]
    fn backend_error_reports_topology_change_when_backend_is_removed() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let mut solved = None;
        let mut deferred = Vec::new();
        let mut backends = vec![
            BackendSlot {
                id: 1,
                backend: Arc::new(NoopBackend::new("cpu")),
                lanes: 1,
            },
            BackendSlot {
                id: 2,
                backend: Arc::new(NoopBackend::new("cpu")),
                lanes: 1,
            },
        ];

        let action = handle_mining_backend_event(
            BackendEvent::Error {
                backend_id: 1,
                backend: "cpu",
                message: "test failure".to_string(),
            },
            1,
            &mut solved,
            &mut deferred,
            &mut backends,
            &backend_executor,
        )
        .expect("backend removal should be handled");

        assert_eq!(action, BackendEventAction::TopologyChanged);
        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].id, 2);
    }

    fn sample_template(template_id: &str) -> BlockTemplateResponse {
        BlockTemplateResponse {
            block: serde_json::from_value(json!({
                "header": {"nonce": 0u64},
                "txns": []
            }))
            .expect("sample template block should deserialize"),
            target: "00".repeat(32),
            header_base: "11".repeat(92),
            template_id: Some(template_id.to_string()),
        }
    }

    #[test]
    fn sleep_with_shutdown_stops_early_when_shutdown_requested() {
        let shutdown = AtomicBool::new(true);
        let start = Instant::now();
        assert!(!sleep_with_shutdown(&shutdown, Duration::from_secs(1)));
        assert!(start.elapsed() < Duration::from_millis(20));
    }

    #[test]
    fn adaptive_weight_update_tracks_observed_throughput() {
        let backends = vec![
            BackendSlot {
                id: 1,
                backend: Arc::new(NoopBackend::new("cpu")),
                lanes: 1,
            },
            BackendSlot {
                id: 2,
                backend: Arc::new(NoopBackend::new("cpu")),
                lanes: 1,
            },
        ];
        let mut weights = seed_backend_weights(&backends);
        let mut round_hashes = BTreeMap::new();
        round_hashes.insert(1, 10_000);
        round_hashes.insert(2, 1_000);

        update_backend_weights(
            &mut weights,
            WeightUpdateInputs {
                backends: &backends,
                round_backend_hashes: &round_hashes,
                round_backend_telemetry: None,
                round_elapsed_secs: 1.0,
                mode: WorkAllocation::Adaptive,
                round_end_reason: RoundEndReason::Refresh,
                refresh_interval: Duration::from_secs(1),
            },
        );

        assert!(weights.get(&1).copied().unwrap_or(0.0) > weights.get(&2).copied().unwrap_or(0.0));
    }

    #[test]
    fn adaptive_weight_update_uses_solved_rounds_with_lower_gain() {
        let backends = vec![BackendSlot {
            id: 7,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
        }];
        let mut weights = seed_backend_weights(&backends);
        let mut round_hashes = BTreeMap::new();
        round_hashes.insert(7, 10_000);

        update_backend_weights(
            &mut weights,
            WeightUpdateInputs {
                backends: &backends,
                round_backend_hashes: &round_hashes,
                round_backend_telemetry: None,
                round_elapsed_secs: 1.0,
                mode: WorkAllocation::Adaptive,
                round_end_reason: RoundEndReason::Solved,
                refresh_interval: Duration::from_secs(1),
            },
        );

        assert!(weights.get(&7).copied().unwrap_or(0.0) > 1.0);
    }

    #[test]
    fn adaptive_weight_update_keeps_sub_one_throughput_signal() {
        let backends = vec![BackendSlot {
            id: 5,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
        }];
        let mut weights = seed_backend_weights(&backends);
        let mut round_hashes = BTreeMap::new();
        round_hashes.insert(5, 1);

        update_backend_weights(
            &mut weights,
            WeightUpdateInputs {
                backends: &backends,
                round_backend_hashes: &round_hashes,
                round_backend_telemetry: None,
                round_elapsed_secs: 100.0,
                mode: WorkAllocation::Adaptive,
                round_end_reason: RoundEndReason::Refresh,
                refresh_interval: Duration::from_secs(1),
            },
        );

        let updated = weights.get(&5).copied().unwrap_or_default();
        assert!(updated > 0.0);
        assert!(updated < 1.0);
    }

    #[test]
    fn adaptive_weight_update_prefers_active_assignment_time_when_available() {
        let backends = vec![BackendSlot {
            id: 3,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
        }];
        let mut weights = seed_backend_weights(&backends);
        let mut round_hashes = BTreeMap::new();
        round_hashes.insert(3, 100);
        let mut round_telemetry = BTreeMap::new();
        round_telemetry.insert(
            3,
            super::BackendRoundTelemetry {
                completed_assignment_micros: 100_000,
                ..super::BackendRoundTelemetry::default()
            },
        );

        update_backend_weights(
            &mut weights,
            WeightUpdateInputs {
                backends: &backends,
                round_backend_hashes: &round_hashes,
                round_backend_telemetry: Some(&round_telemetry),
                round_elapsed_secs: 1.0,
                mode: WorkAllocation::Adaptive,
                round_end_reason: RoundEndReason::Refresh,
                refresh_interval: Duration::from_secs(1),
            },
        );

        assert!(
            weights.get(&3).copied().unwrap_or_default() > 100.0,
            "active assignment timing should produce a stronger throughput signal than wall-clock"
        );
    }

    #[test]
    fn static_weight_update_resets_to_lane_weights() {
        let backends = vec![BackendSlot {
            id: 9,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 3,
        }];
        let mut weights = BTreeMap::new();
        weights.insert(9, 999.0);
        let round_hashes = BTreeMap::new();

        update_backend_weights(
            &mut weights,
            WeightUpdateInputs {
                backends: &backends,
                round_backend_hashes: &round_hashes,
                round_backend_telemetry: None,
                round_elapsed_secs: 1.0,
                mode: WorkAllocation::Static,
                round_end_reason: RoundEndReason::Refresh,
                refresh_interval: Duration::from_secs(1),
            },
        );

        assert_eq!(weights.get(&9).copied(), Some(3.0));
    }

    #[test]
    fn compact_hash_uses_prefix_and_suffix() {
        assert_eq!(compact_hash("abcdef12"), "abcdef12");
        assert_eq!(compact_hash("abc"), "abc");
        assert_eq!(compact_hash("a1b2c3d4e5f6"), "a1b2...e5f6");
    }
}
