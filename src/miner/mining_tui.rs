use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use crossbeam_channel::{bounded, Sender, TrySendError};
use crossterm::event::{self, Event};

use super::hashrate_tracker::HashrateTracker;
use super::session::SessionHistory;
use super::stats::{format_hashrate_ui, Stats};
use super::tui::{
    new_tui_state, restore_terminal, DeviceHashrate, LogLevel, TuiRenderer, TuiState,
};
use super::ui::{clear_tui_state, current_tui_state, set_tui_state, warn};
use super::{backend_descriptions, backend_display_names, BackendSlot};

const NVIDIA_CURRENT_WINDOW_SECS: f64 = 30.0;
const TUI_RENDER_INTERVAL: Duration = Duration::from_secs(1);
const TUI_QUIT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const TUI_RENDER_SIGNAL_CAPACITY: usize = 8;

const EXIT_SUMMARY_MAX_PROBLEMS: usize = 5;
const EXIT_SUMMARY_PROBLEM_MAX_AGE_SECS: f64 = 300.0;

static PROMPT_ACTIVE: AtomicBool = AtomicBool::new(false);
static ACTIVE_DASHBOARD: Mutex<Weak<Dashboard>> = Mutex::new(Weak::new());

pub(super) struct PromptSessionGuard;

impl Drop for PromptSessionGuard {
    fn drop(&mut self) {
        PROMPT_ACTIVE.store(false, Ordering::Release);
    }
}

pub(super) fn begin_prompt_session() -> PromptSessionGuard {
    PROMPT_ACTIVE.store(true, Ordering::Release);
    PromptSessionGuard
}

/// The live terminal screen: draws whichever state is current in `ui` and owns
/// the keyboard. It can outlive any one mining run, so startup, thread-change
/// restarts and shutdown all stay on the same screen.
pub(crate) struct Dashboard {
    render_signal: Sender<RenderSignal>,
    render_stop: Arc<AtomicBool>,
    render_worker: Option<JoinHandle<()>>,
    quit_watcher_stop: Arc<AtomicBool>,
    quit_watcher: Option<JoinHandle<()>>,
    exit_summary: bool,
}

impl Dashboard {
    fn spawn(shutdown: Arc<AtomicBool>, exit_summary: bool) -> Result<Arc<Self>> {
        let (render_signal, render_stop, render_worker) =
            spawn_tui_render_worker(Arc::clone(&shutdown))?;
        let (quit_watcher_stop, quit_watcher) =
            spawn_tui_quit_watcher(shutdown, render_signal.clone());
        let dashboard = Arc::new(Self {
            render_signal,
            render_stop,
            render_worker: Some(render_worker),
            quit_watcher_stop,
            quit_watcher: Some(quit_watcher),
            exit_summary,
        });
        *ACTIVE_DASHBOARD.lock().unwrap_or_else(|e| e.into_inner()) = Arc::downgrade(&dashboard);
        Ok(dashboard)
    }
}

impl Drop for Dashboard {
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
        if let Some(state) = current_tui_state() {
            clear_tui_state(&state);
            if self.exit_summary {
                print_exit_summary(&state);
            }
        }
    }
}

/// Opens the dashboard before hardware detection and tuning, so the whole
/// startup happens on it. Returns None to continue in plain mode.
pub(crate) fn start_dashboard(
    shutdown: Arc<AtomicBool>,
    startup: &crate::config::StartupUi,
) -> Option<Arc<Dashboard>> {
    let state = new_tui_state();
    if let Ok(mut s) = state.lock() {
        s.mode = startup.mode_label.to_string();
        s.api_url = startup.pool_url.clone().unwrap_or_default();
        s.pool_worker = startup.pool_worker.clone().unwrap_or_default();
        s.version = crate::version::ui_display_version();
        s.state = "checking hardware".to_string();
        s.before_first_run = true;
    }
    set_tui_state(Arc::clone(&state));
    match Dashboard::spawn(shutdown, true) {
        Ok(dashboard) => Some(dashboard),
        Err(err) => {
            clear_tui_state(&state);
            warn(
                "TUI",
                format!("disabled after init failure; continuing in plain mode ({err:#})"),
            );
            None
        }
    }
}

/// What the alternate screen took with it: recent problems and how the run went.
fn print_exit_summary(state: &TuiState) {
    use crate::console_output::{eprintln, println};
    let Ok(state) = state.lock() else {
        return;
    };
    let now = state.started_at.elapsed().as_secs_f64();
    let problems: Vec<_> = state
        .log_entries
        .iter()
        .rev()
        .filter(|entry| {
            matches!(entry.level, LogLevel::Warn | LogLevel::Error)
                && now - entry.elapsed_secs <= EXIT_SUMMARY_PROBLEM_MAX_AGE_SECS
        })
        .take(EXIT_SUMMARY_MAX_PROBLEMS)
        .collect();
    for entry in problems.iter().rev() {
        eprintln!("{}: {}", entry.tag, entry.message);
    }
    // A run that never got going has only its error to report.
    if !state.before_first_run {
        println!("{}", exit_summary_line(&state));
    }
}

fn exit_summary_line(state: &super::tui::TuiStateInner) -> String {
    let mut line = format!("seine stopped after {}", state.uptime());
    if state.total_hashes > 0 {
        let noun = match (state.mode == "pool", state.accepted == 1) {
            (true, true) => "share",
            (true, false) => "shares",
            (false, true) => "block",
            (false, false) => "blocks",
        };
        line.push_str(&format!(
            ": {} average, {} {noun} accepted",
            state.avg_hashrate, state.accepted
        ));
    }
    line
}

pub(super) struct TuiDisplay {
    state: TuiState,
    last_render_request: Instant,
    last_state_label: String,
    render_signal: Sender<RenderSignal>,
    _dashboard: Arc<Dashboard>,
    hashrate_tracker: HashrateTracker,
    history: Arc<SessionHistory>,
}

#[derive(Debug, Clone, Copy)]
enum RenderSignal {
    RenderNow,
}

pub(super) struct RoundUiView<'a> {
    pub backends: &'a [BackendSlot],
    pub round_backend_hashes: &'a BTreeMap<u64, u64>,
    pub round_start: Instant,
    pub height: &'a str,
    pub network_hashrate: &'a str,
    pub epoch: u64,
    pub state_label: &'a str,
}

impl TuiDisplay {
    fn new(
        tui_state: TuiState,
        shutdown: Arc<AtomicBool>,
        history: Arc<SessionHistory>,
    ) -> Result<Self> {
        let active = ACTIVE_DASHBOARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade();
        let dashboard = match active {
            Some(dashboard) => dashboard,
            None => Dashboard::spawn(shutdown, false)?,
        };
        let display = Self {
            state: tui_state,
            last_render_request: Instant::now() - TUI_RENDER_INTERVAL,
            last_state_label: String::new(),
            render_signal: dashboard.render_signal.clone(),
            _dashboard: dashboard,
            hashrate_tracker: history.take_hashrate_tracker(),
            history,
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
        let paused = should_zero_current_hashrate(view.state_label);
        self.hashrate_tracker.set_paused(paused);

        // Record sample in the sliding-window tracker
        self.hashrate_tracker
            .record(snapshot.hashes, view.round_start, view.round_backend_hashes);
        let current_window_secs = current_hashrate_window_overrides(view.backends);
        let rates = self
            .hashrate_tracker
            .rates_with_current_windows(&current_window_secs);

        // Build per-device hashrate display data
        let display_names = backend_display_names(view.backends);
        let mut device_hashrates = Vec::with_capacity(view.backends.len());
        for slot in view.backends {
            let id = slot.id;
            let current_rate = rates.current_per_device.get(&id).copied().unwrap_or(0.0);
            let avg_rate = rates.average_per_device.get(&id).copied().unwrap_or(0.0);
            let name = display_names
                .get(&id)
                .cloned()
                .unwrap_or_else(|| slot.backend.name().to_string());
            device_hashrates.push(DeviceHashrate {
                name,
                current: format_hashrate_ui(current_rate),
                average: format_hashrate_ui(avg_rate),
            });
        }

        if let Ok(mut s) = self.state.lock() {
            s.height = view.height.to_string();
            s.network_hashrate = view.network_hashrate.to_string();
            s.epoch = view.epoch;
            s.state = view.state_label.to_string();
            s.round_hashrate = format_hashrate_ui(rates.current_total);
            s.avg_hashrate = format_hashrate_ui(rates.average_total);
            s.total_hashes = snapshot.hashes;
            s.templates = snapshot.templates;
            s.submitted = snapshot.submitted;
            s.stale_shares = snapshot.stale_shares;
            s.accepted = snapshot.accepted;
            s.share_retries = snapshot.share_retries;
            s.device_hashrates = device_hashrates;
            s.backends_desc = backend_descriptions(view.backends);
        }

        self.request_render();
        self.last_render_request = Instant::now();
        self.last_state_label = view.state_label.to_string();
    }

    pub(super) fn mark_block_found(&mut self) {
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
        let paused = should_zero_current_hashrate(state_label);
        self.hashrate_tracker.set_paused(paused);
        if let Ok(mut s) = self.state.lock() {
            s.state = state_label.to_string();
            if paused {
                let zero = format_hashrate_ui(0.0);
                s.round_hashrate = zero.clone();
                for device in &mut s.device_hashrates {
                    device.current = zero.clone();
                }
            }
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

pub(super) fn should_zero_current_hashrate(state_label: &str) -> bool {
    matches!(
        state_label,
        "wallet-required"
            | "wallet-password-required"
            | "daemon-syncing"
            | "daemon-unavailable"
            | "invalid-address"
    )
}

pub(super) fn current_hashrate_window_overrides(backends: &[BackendSlot]) -> BTreeMap<u64, f64> {
    let mut overrides = BTreeMap::new();
    for slot in backends {
        if let Some(window_secs) = current_hashrate_window_secs_for_backend(slot.backend.name()) {
            // NVIDIA deep-launch batches complete in bursts, so a longer
            // current-rate window keeps the TUI from oscillating between
            // adjacent samples while leaving other backends responsive.
            overrides.insert(slot.id, window_secs);
        }
    }
    overrides
}

fn current_hashrate_window_secs_for_backend(backend_name: &str) -> Option<f64> {
    match backend_name {
        "nvidia" => Some(NVIDIA_CURRENT_WINDOW_SECS),
        _ => None,
    }
}

impl Drop for TuiDisplay {
    fn drop(&mut self) {
        crate::diagnostics::cleanup_stage(seine_telemetry::CleanupStage::StoppingTui);
        let mut tracker = std::mem::replace(&mut self.hashrate_tracker, HashrateTracker::new());
        tracker.pause_for_restart();
        self.history.save_hashrate_tracker(tracker);
    }
}

pub(super) fn init_tui_display(
    tui_state: Option<TuiState>,
    shutdown: Arc<AtomicBool>,
    history: Arc<SessionHistory>,
) -> Option<TuiDisplay> {
    let state = tui_state?;
    match TuiDisplay::new(Arc::clone(&state), shutdown, history) {
        Ok(display) => {
            set_tui_state(state);
            display.request_render();
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

pub(super) fn update_tui(tui: &mut Option<TuiDisplay>, stats: &Stats, view: RoundUiView<'_>) {
    super::live_stats::record(stats, &view);
    if let Some(display) = tui.as_mut() {
        display.update(stats, view);
    }
}

pub(super) fn render_tui_now(tui: &mut Option<TuiDisplay>) {
    if let Some(display) = tui.as_mut() {
        display.render_now();
    }
}

pub(super) fn set_tui_dev_fee_active(tui: &mut Option<TuiDisplay>, active: bool) {
    if let Some(display) = tui.as_mut() {
        if let Ok(mut s) = display.state.lock() {
            s.dev_fee_active = active;
        }
        display.request_render();
    }
}

pub(super) fn set_tui_state_label(tui: &mut Option<TuiDisplay>, state_label: &str) {
    super::live_stats::set_state(state_label);
    if let Some(display) = tui.as_mut() {
        display.set_state_and_render(state_label);
    }
}

pub(super) fn set_tui_blocktemplate_retrying(tui: &mut Option<TuiDisplay>, retrying: bool) {
    if let Some(display) = tui.as_mut() {
        if let Ok(mut s) = display.state.lock() {
            if retrying {
                if s.blocktemplate_retry_since.is_none() {
                    s.blocktemplate_retry_since = Some(Instant::now());
                }
            } else {
                s.blocktemplate_retry_since = None;
            }
        }
        display.request_render();
    }
}

pub(super) fn set_tui_pending_gpus(tui: &mut Option<TuiDisplay>, pending: &[&'static str]) {
    if let Some(display) = tui.as_mut() {
        if let Ok(mut s) = display.state.lock() {
            if !pending.is_empty() && s.pending_gpus_since.is_none() {
                s.pending_gpus_since = Some(Instant::now());
            } else if pending.is_empty() {
                s.pending_gpus_since = None;
            }
            if s.pending_gpus != pending {
                s.pending_gpus = pending.to_vec();
            }
        }
    }
}

pub(super) fn set_tui_pool_payout_minimum(tui: &mut Option<TuiDisplay>, minimum: Option<u64>) {
    if let Some(display) = tui.as_mut() {
        if let Ok(mut s) = display.state.lock() {
            s.pool_payout_minimum = minimum
                .filter(|amount| *amount > 0)
                .map(|amount| format!("{} BNT", crate::payout_destination::format_amount(amount)));
        }
        display.request_render();
    }
}

pub(super) fn set_tui_pool_balance_overview(
    tui: &mut Option<TuiDisplay>,
    address: &str,
    pending: &str,
    paid: &str,
) {
    if let Some(display) = tui.as_mut() {
        if let Ok(mut s) = display.state.lock() {
            s.pool_address = address.to_string();
            s.pool_pending = pending.to_string();
            s.pool_paid = paid.to_string();
        }
        display.request_render();
    }
}

pub(super) fn set_tui_wallet_overview(
    tui: &mut Option<TuiDisplay>,
    address: &str,
    pending: &str,
    unlocked: &str,
) {
    if let Some(display) = tui.as_mut() {
        if let Ok(mut s) = display.state.lock() {
            s.wallet_visible = true;
            s.wallet_address = address.to_string();
            s.wallet_pending = pending.to_string();
            s.wallet_unlocked = unlocked.to_string();
        }
        display.request_render();
    }
}

fn spawn_tui_render_worker(
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

        let mut last_render: Option<Instant> = None;
        let mut last_log_seq = 0;
        while !render_stop_flag.load(Ordering::Relaxed) {
            let requested = match render_signal_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(RenderSignal::RenderNow) => true,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => false,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };
            let Some(state) = current_tui_state() else {
                continue;
            };
            let Ok(mut locked) = state.lock() else {
                continue;
            };
            if shutdown.load(Ordering::Relaxed) {
                locked.stopping = true;
            }
            let thread_reply = locked.poll_thread_reply();
            let notice_expired = locked.expire_thread_notice(Instant::now());
            // Startup has no mining loop asking for frames, so new log lines
            // and the clock drive redraws too.
            let due = last_render.is_none_or(|at| at.elapsed() >= TUI_RENDER_INTERVAL);
            if requested || thread_reply || notice_expired || due || locked.log_seq != last_log_seq
            {
                last_log_seq = locked.log_seq;
                let _ = renderer.render(&locked);
                last_render = Some(Instant::now());
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

fn spawn_tui_quit_watcher(
    shutdown: Arc<AtomicBool>,
    render_signal: Sender<RenderSignal>,
) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_flag = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        while !stop_flag.load(Ordering::Relaxed) {
            if PROMPT_ACTIVE.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(50));
                continue;
            }
            let has_event = event::poll(TUI_QUIT_POLL_INTERVAL).unwrap_or(false);
            if !has_event {
                continue;
            }
            if let Ok(Event::Key(key)) = event::read() {
                let quit = current_tui_state()
                    .and_then(|state| state.lock().ok().map(|mut s| s.handle_key(key)))
                    .unwrap_or(false);
                let _ = render_signal.try_send(RenderSignal::RenderNow);
                // A second quit skips cleanup a stuck backend may be holding up.
                if quit && shutdown.swap(true, Ordering::SeqCst) {
                    restore_terminal();
                    crate::backend::amd::on_process_exit();
                    std::process::exit(130);
                }
            }
        }
    });
    (stop, handle)
}

#[cfg(test)]
mod tests {
    use super::{
        current_hashrate_window_secs_for_backend, exit_summary_line, should_zero_current_hashrate,
    };
    use crate::miner::tui::TuiStateInner;

    #[test]
    fn exit_summary_reports_results_only_after_mining() {
        let mut state = TuiStateInner::new();
        state.mode = "pool".into();
        let line = exit_summary_line(&state);
        assert!(line.starts_with("seine stopped after "));
        assert!(!line.contains("average"));

        state.total_hashes = 100;
        state.avg_hashrate = "1.200 kH/s".into();
        state.accepted = 3;
        assert!(exit_summary_line(&state).ends_with(": 1.200 kH/s average, 3 shares accepted"));
        state.mode = "daemon".into();
        assert!(exit_summary_line(&state).ends_with("3 blocks accepted"));
        state.accepted = 1;
        assert!(exit_summary_line(&state).ends_with("1 block accepted"));
        state.mode = "pool".into();
        assert!(exit_summary_line(&state).ends_with("1 share accepted"));
    }

    #[test]
    fn zeroes_current_hashrate_for_non_mining_states() {
        for state in [
            "wallet-required",
            "wallet-password-required",
            "daemon-syncing",
            "daemon-unavailable",
            "invalid-address",
        ] {
            assert!(should_zero_current_hashrate(state));
        }
        assert!(!should_zero_current_hashrate("working"));
        assert!(!should_zero_current_hashrate("refresh"));
    }

    #[test]
    fn nvidia_uses_longer_current_hashrate_window() {
        assert_eq!(
            current_hashrate_window_secs_for_backend("nvidia"),
            Some(30.0)
        );
        assert_eq!(current_hashrate_window_secs_for_backend("cpu"), None);
    }
}
