use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use crossbeam_channel::{bounded, Sender, TrySendError};
use crossterm::event::{self, Event, KeyCode, KeyModifiers};

use super::hashrate_tracker::HashrateTracker;
use super::stats::{format_hashrate, Stats};
use super::tui::{DeviceHashrate, TuiRenderer, TuiState};
use super::ui::{set_tui_state, warn};
use super::{backend_names_by_id, BackendSlot};

const TUI_RENDER_INTERVAL: Duration = Duration::from_secs(1);
const TUI_QUIT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const TUI_RENDER_SIGNAL_CAPACITY: usize = 8;

static PROMPT_ACTIVE: AtomicBool = AtomicBool::new(false);

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

pub(super) struct TuiDisplay {
    state: TuiState,
    last_render_request: Instant,
    last_state_label: String,
    render_signal: Sender<RenderSignal>,
    render_stop: Arc<AtomicBool>,
    render_worker: Option<JoinHandle<()>>,
    quit_watcher_stop: Arc<AtomicBool>,
    quit_watcher: Option<JoinHandle<()>>,
    hashrate_tracker: HashrateTracker,
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
    pub difficulty: &'a str,
    pub epoch: u64,
    pub state_label: &'a str,
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
            hashrate_tracker: HashrateTracker::new(),
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

        // Record sample in the sliding-window tracker
        self.hashrate_tracker
            .record(snapshot.hashes, view.round_start, view.round_backend_hashes);
        let rates = self.hashrate_tracker.rates();

        // Build per-device hashrate display data
        let names = backend_names_by_id(view.backends);
        let mut device_hashrates = Vec::new();
        for (&id, &current_rate) in &rates.current_per_device {
            let avg_rate = rates.average_per_device.get(&id).copied().unwrap_or(0.0);
            let name = names
                .get(&id)
                .map(|n| format!("{n}#{id}"))
                .unwrap_or_else(|| format!("?#{id}"));
            device_hashrates.push(DeviceHashrate {
                name,
                current: format_hashrate(current_rate),
                average: format_hashrate(avg_rate),
            });
        }

        if let Ok(mut s) = self.state.lock() {
            s.height = view.height.to_string();
            s.difficulty = view.difficulty.to_string();
            s.epoch = view.epoch;
            s.state = view.state_label.to_string();
            s.round_hashrate = format_hashrate(rates.current_total);
            s.avg_hashrate = format_hashrate(rates.average_total);
            s.total_hashes = snapshot.hashes;
            s.templates = snapshot.templates;
            s.submitted = snapshot.submitted;
            s.accepted = snapshot.accepted;
            s.device_hashrates = device_hashrates;
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

pub(super) fn init_tui_display(
    tui_state: Option<TuiState>,
    shutdown: Arc<AtomicBool>,
) -> Option<TuiDisplay> {
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

pub(super) fn update_tui(tui: &mut Option<TuiDisplay>, stats: &Stats, view: RoundUiView<'_>) {
    if let Some(display) = tui.as_mut() {
        display.update(stats, view);
    }
}

pub(super) fn render_tui_now(tui: &mut Option<TuiDisplay>) {
    if let Some(display) = tui.as_mut() {
        display.render_now();
    }
}

pub(super) fn set_tui_state_label(tui: &mut Option<TuiDisplay>, state_label: &str) {
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
