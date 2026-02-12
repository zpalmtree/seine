use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, IsTerminal};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use blocknet_pow_spec::POW_HEADER_BASE_LEN;
use crossbeam_channel::{after, Receiver};
use serde_json::Value;

use crate::api::{is_no_wallet_loaded_error, is_wallet_already_loaded_error, ApiClient};
use crate::backend::{BackendEvent, MiningSolution};
use crate::config::Config;
use crate::types::{
    decode_hex, parse_target, set_block_nonce, template_difficulty, template_height,
    BlockTemplateResponse,
};

use super::scheduler::NonceScheduler;
use super::stats::{format_hashrate, Stats};
use super::tui::{TuiRenderer, TuiState};
use super::ui::{active_tui_state, error, info, success, warn};
use super::{
    collect_backend_hashes, distribute_work, format_round_backend_hashrate, next_event_wait,
    next_work_id, quiesce_backend_slots, total_lanes, BackendSlot, RuntimeBackendEventAction,
    RuntimeMode, TEMPLATE_RETRY_DELAY,
};

pub(super) struct TipSignal {
    stale: Arc<AtomicBool>,
    current_template_height: Arc<AtomicU64>,
    last_new_block: Arc<Mutex<Option<LastNewBlock>>>,
}

struct LastNewBlock {
    hash: String,
    height: Option<u64>,
}

impl TipSignal {
    fn new() -> Self {
        Self {
            stale: Arc::new(AtomicBool::new(false)),
            current_template_height: Arc::new(AtomicU64::new(0)),
            last_new_block: Arc::new(Mutex::new(None)),
        }
    }

    fn take_stale(&self) -> bool {
        self.stale.swap(false, Ordering::AcqRel)
    }

    fn set_current_template_height(&self, height: u64) {
        self.current_template_height
            .store(height, Ordering::Release);
        let should_clear_stale = if let Ok(last_event) = self.last_new_block.lock() {
            matches!(
                last_event.as_ref(),
                Some(last) if last.height.is_some_and(|h| h.saturating_add(1) < height)
            )
        } else {
            false
        };
        if should_clear_stale {
            self.stale.store(false, Ordering::Release);
        }
    }

    fn mark_stale_for_new_block(&self, hash: &str, event_height: Option<u64>) {
        if let Some(height) = event_height {
            let template_height = self.current_template_height.load(Ordering::Acquire);
            if template_height != 0 && height.saturating_add(1) < template_height {
                return;
            }
        }

        let mut changed = false;
        if let Ok(mut last_event) = self.last_new_block.lock() {
            if !matches!(
                last_event.as_ref(),
                Some(last) if last.hash == hash && last.height == event_height
            ) {
                *last_event = Some(LastNewBlock {
                    hash: hash.to_string(),
                    height: event_height,
                });
                changed = true;
            }
        } else {
            // If lock state is poisoned, err on the side of refreshing work.
            changed = true;
        }
        if changed {
            self.stale.store(true, Ordering::Release);
        }
    }

    fn mark_stale_on_unparsed_event(&self) {
        self.stale.store(true, Ordering::Release);
    }
}

pub(super) struct TipListener {
    signal: TipSignal,
    handle: Option<JoinHandle<()>>,
}

impl TipListener {
    pub(super) fn signal(&self) -> &TipSignal {
        &self.signal
    }

    pub(super) fn join(mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

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

struct TuiDisplay {
    renderer: TuiRenderer,
    state: TuiState,
    last_render: Instant,
    last_state_label: String,
}

impl TuiDisplay {
    fn new() -> Result<Self> {
        let tui_state = active_tui_state()
            .ok_or_else(|| anyhow!("TUI state not initialized"))?;
        let renderer = TuiRenderer::new()
            .map_err(|err| anyhow!("TUI renderer init failed: {err}"))?;
        Ok(Self {
            renderer,
            state: Arc::clone(tui_state),
            last_render: Instant::now() - TUI_RENDER_INTERVAL,
            last_state_label: String::new(),
        })
    }

    fn update(
        &mut self,
        stats: &Stats,
        backends: &[BackendSlot],
        round_backend_hashes: &BTreeMap<u64, u64>,
        round_hashes: u64,
        round_start: Instant,
        height: &str,
        difficulty: &str,
        epoch: u64,
        state_label: &str,
    ) {
        let state_changed = self.last_state_label != state_label;
        if !state_changed && self.last_render.elapsed() < TUI_RENDER_INTERVAL {
            return;
        }

        let snapshot = stats.snapshot();
        let round_elapsed = round_start.elapsed().as_secs_f64().max(0.001);
        let round_rate = format_hashrate(round_hashes as f64 / round_elapsed);
        let backend_rate =
            format_round_backend_hashrate(backends, round_backend_hashes, round_elapsed);

        if let Ok(mut s) = self.state.lock() {
            s.height = height.to_string();
            s.difficulty = difficulty.to_string();
            s.epoch = epoch;
            s.state = state_label.to_string();
            s.round_hashrate = round_rate;
            s.avg_hashrate = format_hashrate(snapshot.hps);
            s.total_hashes = snapshot.hashes;
            s.templates = snapshot.templates;
            s.submitted = snapshot.submitted;
            s.accepted = snapshot.accepted;
            s.backend_rates = backend_rate;
        }

        if let Ok(locked) = self.state.lock() {
            let _ = self.renderer.render(&locked);
        }
        self.last_render = Instant::now();
        self.last_state_label = state_label.to_string();
    }

    fn poll_quit(&self) -> bool {
        self.renderer.poll_quit()
    }
}

pub(super) fn run_mining_loop(
    cfg: &Config,
    client: &ApiClient,
    shutdown: Arc<AtomicBool>,
    backends: &mut Vec<BackendSlot>,
    backend_events: &Receiver<BackendEvent>,
    tip_signal: Option<&TipSignal>,
) -> Result<()> {
    let stats = Stats::new();
    let mut nonce_scheduler = NonceScheduler::new(cfg.start_nonce, cfg.nonce_iters_per_lane);
    let mut work_id_cursor = 1u64;
    let mut epoch = 0u64;
    let mut last_stats_print = Instant::now();
    let mut prefetch: Option<TemplatePrefetch> = None;
    let mut tui = TuiDisplay::new()?;

    let mut template = match fetch_template_with_retry(client, cfg, shutdown.as_ref()) {
        Some(t) => t,
        None => {
            stats.print();
            info("MINER", "stopped");
            return Ok(());
        }
    };
    success("MINER", "connected and mining");

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
                let Some(next_template) =
                    resolve_next_template(&mut prefetch, client, cfg, &shutdown)
                else {
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
            let Some(next_template) = resolve_next_template(&mut prefetch, client, cfg, &shutdown)
            else {
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
                let Some(next_template) =
                    resolve_next_template(&mut prefetch, client, cfg, &shutdown)
                else {
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

        distribute_work(
            backends,
            epoch,
            work_id,
            Arc::clone(&header_base),
            target,
            reservation,
            stop_at,
        )?;
        if prefetch.is_none() {
            prefetch = Some(TemplatePrefetch::spawn(
                client.clone(),
                cfg.clone(),
                Arc::clone(&shutdown),
            ));
        }

        let round_start = Instant::now();
        let mut solved: Option<MiningSolution> = None;
        let mut stale_tip_event = false;
        let mut round_hashes = 0u64;
        let mut round_backend_hashes = BTreeMap::new();
        let mut topology_changed = false;
        let mut next_hash_poll_at = Instant::now();
        tui.update(
            &stats,
            backends,
            &round_backend_hashes,
            round_hashes,
            round_start,
            &height,
            &difficulty,
            epoch,
            "working",
        );

        while !shutdown.load(Ordering::Relaxed)
            && Instant::now() < stop_at
            && solved.is_none()
            && !stale_tip_event
        {
            let now = Instant::now();
            if now >= next_hash_poll_at {
                collect_backend_hashes(
                    backends,
                    Some(&stats),
                    &mut round_hashes,
                    Some(&mut round_backend_hashes),
                );
                next_hash_poll_at = now + cfg.hash_poll_interval;
                tui.update(
                    &stats,
                    backends,
                    &round_backend_hashes,
                    round_hashes,
                    round_start,
                    &height,
                    &difficulty,
                    epoch,
                    "working",
                );
                if tui.poll_quit() {
                    shutdown.store(true, Ordering::SeqCst);
                    break;
                }
            }

            if last_stats_print.elapsed() >= cfg.stats_interval {
                last_stats_print = Instant::now();
            }

            if tip_signal.is_some_and(TipSignal::take_stale) {
                stale_tip_event = true;
                tui.update(
                    &stats,
                    backends,
                    &round_backend_hashes,
                    round_hashes,
                    round_start,
                    &height,
                    &difficulty,
                    epoch,
                    "stale-tip",
                );
                continue;
            }

            let wait_for = next_event_wait(
                stop_at,
                last_stats_print,
                cfg.stats_interval,
                next_hash_poll_at,
            );

            crossbeam_channel::select! {
                recv(backend_events) -> event => {
                    let event = event.map_err(|_| anyhow!("backend event channel closed"))?;
                    if handle_mining_backend_event(event, epoch, &mut solved, backends)?
                        == BackendEventAction::TopologyChanged
                    {
                        topology_changed = true;
                    }
                }
                recv(after(wait_for)) -> _ => {}
            }

            if topology_changed
                && !shutdown.load(Ordering::Relaxed)
                && solved.is_none()
                && !stale_tip_event
                && Instant::now() < stop_at
                && !backends.is_empty()
            {
                let reservation = nonce_scheduler.reserve(total_lanes(backends));
                warn(
                    "BACKEND",
                    format!(
                        "topology change; redistributing e={} id={} backends={}",
                        epoch,
                        work_id,
                        super::backend_names(backends),
                    ),
                );
                distribute_work(
                    backends,
                    epoch,
                    work_id,
                    Arc::clone(&header_base),
                    target,
                    reservation,
                    stop_at,
                )?;
                next_hash_poll_at = Instant::now();
                topology_changed = false;
                tui.update(
                    &stats,
                    backends,
                    &round_backend_hashes,
                    round_hashes,
                    round_start,
                    &height,
                    &difficulty,
                    epoch,
                    "rebalanced",
                );
            }
        }

        if cfg.strict_round_accounting {
            quiesce_backend_slots(backends)?;
        }
        let _ = drain_mining_backend_events(backend_events, epoch, &mut solved, backends)?;
        collect_backend_hashes(
            backends,
            Some(&stats),
            &mut round_hashes,
            Some(&mut round_backend_hashes),
        );
        stale_tip_event |= tip_signal.is_some_and(TipSignal::take_stale);

        if let Some(solution) = solved {
            tui.update(
                &stats,
                backends,
                &round_backend_hashes,
                round_hashes,
                round_start,
                &height,
                &difficulty,
                epoch,
                "solved",
            );
            success(
                "SOLVE",
                format!(
                    "backend={}#{} nonce={} elapsed={:.2}s",
                    solution.backend,
                    solution.backend_id,
                    solution.nonce,
                    round_start.elapsed().as_secs_f64(),
                ),
            );

            let template_id = template.template_id.clone();
            let mut solved_block = template.block;
            set_block_nonce(&mut solved_block, solution.nonce);

            stats.bump_submitted();

            match client.submit_block(&solved_block, template_id.as_deref(), solution.nonce) {
                Ok(resp) => {
                    if resp.accepted {
                        stats.bump_accepted();
                        success(
                            "SUBMIT",
                            format!(
                                "accepted height={} hash={}",
                                resp.height
                                    .map(|h| h.to_string())
                                    .unwrap_or_else(|| "unknown".to_string()),
                                resp.hash.unwrap_or_else(|| "unknown".to_string())
                            ),
                        );
                    } else {
                        warn("SUBMIT", "rejected by daemon");
                    }
                }
                Err(err) => {
                    error("SUBMIT", format!("submit failed: {err:#}"));
                }
            }
        } else if stale_tip_event {
            tui.update(
                &stats,
                backends,
                &round_backend_hashes,
                round_hashes,
                round_start,
                &height,
                &difficulty,
                epoch,
                "stale-refresh",
            );
        } else {
            tui.update(
                &stats,
                backends,
                &round_backend_hashes,
                round_hashes,
                round_start,
                &height,
                &difficulty,
                epoch,
                "refresh",
            );
        }

        if last_stats_print.elapsed() >= cfg.stats_interval {
            last_stats_print = Instant::now();
        }

        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let Some(next_template) = resolve_next_template(&mut prefetch, client, cfg, &shutdown)
        else {
            break;
        };
        template = next_template;
    }

    if let Some(prefetch_task) = prefetch {
        let _ = prefetch_task.join();
    }

    stats.print();
    info("MINER", "stopped");
    Ok(())
}


struct TemplatePrefetch {
    handle: JoinHandle<Option<BlockTemplateResponse>>,
}

impl TemplatePrefetch {
    fn spawn(client: ApiClient, cfg: Config, shutdown: Arc<AtomicBool>) -> Self {
        let handle =
            thread::spawn(move || fetch_template_with_retry(&client, &cfg, shutdown.as_ref()));
        Self { handle }
    }

    fn join(self) -> Option<BlockTemplateResponse> {
        match self.handle.join() {
            Ok(template) => template,
            Err(_) => {
                error("TEMPLATE", "prefetch thread panicked");
                None
            }
        }
    }
}

fn resolve_next_template(
    prefetch: &mut Option<TemplatePrefetch>,
    client: &ApiClient,
    cfg: &Config,
    shutdown: &Arc<AtomicBool>,
) -> Option<BlockTemplateResponse> {
    if let Some(task) = prefetch.take() {
        if let Some(template) = task.join() {
            return Some(template);
        }
    }

    fetch_template_with_retry(client, cfg, shutdown.as_ref())
}

fn handle_mining_backend_event(
    event: BackendEvent,
    epoch: u64,
    solved: &mut Option<MiningSolution>,
    backends: &mut Vec<BackendSlot>,
) -> Result<BackendEventAction> {
    let (action, maybe_solution) =
        super::handle_runtime_backend_event(event, epoch, backends, RuntimeMode::Mining)?;
    if solved.is_none() {
        *solved = maybe_solution;
    }
    Ok(action)
}

fn drain_mining_backend_events(
    backend_events: &Receiver<BackendEvent>,
    epoch: u64,
    solved: &mut Option<MiningSolution>,
    backends: &mut Vec<BackendSlot>,
) -> Result<BackendEventAction> {
    let (action, maybe_solution) =
        super::drain_runtime_backend_events(backend_events, epoch, backends, RuntimeMode::Mining)?;
    if solved.is_none() {
        *solved = maybe_solution;
    }
    Ok(action)
}

pub(super) fn spawn_tip_listener(client: ApiClient, shutdown: Arc<AtomicBool>) -> TipListener {
    let tip_signal = TipSignal::new();
    let signal = TipSignal {
        stale: Arc::clone(&tip_signal.stale),
        current_template_height: Arc::clone(&tip_signal.current_template_height),
        last_new_block: Arc::clone(&tip_signal.last_new_block),
    };

    let handle = thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            match client.open_events_stream() {
                Ok(resp) => {
                    if let Err(err) = stream_tip_events(resp, &signal, &shutdown) {
                        if !shutdown.load(Ordering::Relaxed) && !is_stream_timeout_error(&err) {
                            warn("EVENTS", format!("stream dropped: {err:#}"));
                        }
                    }
                }
                Err(err) => {
                    if !shutdown.load(Ordering::Relaxed) && !is_stream_timeout_error(&err) {
                        warn("EVENTS", format!("failed to open stream: {err:#}"));
                    }
                }
            }

            if !shutdown.load(Ordering::Relaxed) {
                thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    });

    TipListener {
        signal: tip_signal,
        handle: Some(handle),
    }
}

fn stream_tip_events(
    resp: reqwest::blocking::Response,
    signal: &TipSignal,
    shutdown: &AtomicBool,
) -> Result<()> {
    let mut event_name = String::new();
    let reader = BufReader::new(resp);

    for line_result in reader.lines() {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let line = line_result.context("failed reading SSE event stream")?;
        process_sse_line(&line, &mut event_name, signal);
    }

    Ok(())
}

fn process_sse_line(line: &str, event_name: &mut String, signal: &TipSignal) {
    if let Some(name) = line.strip_prefix("event:") {
        *event_name = name.trim().to_string();
        return;
    }

    if line.is_empty() {
        event_name.clear();
        return;
    }

    if event_name != "new_block" || !line.starts_with("data:") {
        return;
    }

    let payload = line
        .strip_prefix("data:")
        .map(str::trim)
        .unwrap_or_default();
    if let Some(event) = extract_new_block_event(payload) {
        signal.mark_stale_for_new_block(&event.hash, event.height);
        return;
    }

    // If parsing fails, preserve old behavior and refresh once.
    signal.mark_stale_on_unparsed_event();
}

struct NewBlockEvent {
    hash: String,
    height: Option<u64>,
}

fn extract_new_block_event(payload: &str) -> Option<NewBlockEvent> {
    let value: Value = serde_json::from_str(payload).ok()?;
    let hash = value
        .get("hash")
        .and_then(Value::as_str)
        .map(str::to_string)?;
    let height = value.get("height").and_then(Value::as_u64);
    Some(NewBlockEvent { hash, height })
}

fn is_stream_timeout_error(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(req_err) = cause.downcast_ref::<reqwest::Error>() {
            if req_err.is_timeout() {
                return true;
            }
        }
        if let Some(io_err) = cause.downcast_ref::<std::io::Error>() {
            if io_err.kind() == std::io::ErrorKind::TimedOut {
                return true;
            }
        }
    }
    false
}

fn fetch_template_with_retry(
    client: &ApiClient,
    cfg: &Config,
    shutdown: &AtomicBool,
) -> Option<BlockTemplateResponse> {
    while !shutdown.load(Ordering::Relaxed) {
        match client.get_block_template() {
            Ok(template) => return Some(template),
            Err(err) if is_no_wallet_loaded_error(&err) => {
                warn(
                    "WALLET",
                    "blocktemplate requires loaded wallet; attempting automatic load",
                );
                match auto_load_wallet(client, cfg, shutdown) {
                    Ok(true) => continue,
                    Ok(false) => {
                        warn(
                            "WALLET",
                            "unable to auto-load wallet; use --wallet-password, --wallet-password-file, SEINE_WALLET_PASSWORD, or interactive prompt",
                        );
                        return None;
                    }
                    Err(load_err) => {
                        error(
                            "WALLET",
                            format!("automatic wallet load failed: {load_err:#}"),
                        );
                        return None;
                    }
                }
            }
            Err(err) => {
                warn("NETWORK", format!("blocktemplate request failed: {err:#}"));
                if !sleep_with_shutdown(shutdown, TEMPLATE_RETRY_DELAY) {
                    break;
                }
            }
        }
    }

    None
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

fn auto_load_wallet(client: &ApiClient, cfg: &Config, shutdown: &AtomicBool) -> Result<bool> {
    const MAX_PROMPT_ATTEMPTS: u32 = 3;

    let Some((mut password, source)) = resolve_wallet_password(cfg)? else {
        return Ok(false);
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
                prompt_attempt += 1;
                let Some(next_password) = prompt_wallet_password()? else {
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

    if let Some(password) = prompt_wallet_password()? {
        return Ok(Some((password, WalletPasswordSource::Prompt)));
    }

    Ok(None)
}

fn read_password_file(path: &std::path::Path) -> Result<String> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read wallet password file at {}", path.display()))?;
    Ok(raw.trim_end_matches(['\r', '\n']).to_string())
}

fn prompt_wallet_password() -> Result<Option<String>> {
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return Ok(None);
    }

    let password = rpassword::prompt_password("wallet password: ")
        .context("failed to read wallet password from terminal")?;
    if password.is_empty() {
        return Ok(None);
    }
    Ok(Some(password))
}

#[cfg(test)]
mod tests {
    use crossbeam_channel::Sender;

    use super::*;
    use crate::backend::{BackendInstanceId, PowBackend, WorkAssignment};
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

        fn set_instance_id(&mut self, _id: BackendInstanceId) {}

        fn set_event_sink(&mut self, _sink: Sender<BackendEvent>) {}

        fn start(&mut self) -> Result<()> {
            Ok(())
        }

        fn stop(&mut self) {}

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn stale_solution_is_ignored_for_current_epoch() {
        let mut solved = None;
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
            &mut backends,
        )
        .expect("stale solution handling should succeed");

        assert_eq!(action, BackendEventAction::None);
        assert!(solved.is_none());
    }

    #[test]
    fn backend_error_reports_topology_change_when_backend_is_removed() {
        let mut solved = None;
        let mut backends = vec![
            BackendSlot {
                id: 1,
                backend: Box::new(NoopBackend::new("cpu")),
                lane_offset: 0,
                lanes: 1,
            },
            BackendSlot {
                id: 2,
                backend: Box::new(NoopBackend::new("cpu")),
                lane_offset: 1,
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
            &mut backends,
        )
        .expect("backend removal should be handled");

        assert_eq!(action, BackendEventAction::TopologyChanged);
        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].id, 2);
    }

    #[test]
    fn sleep_with_shutdown_stops_early_when_shutdown_requested() {
        let shutdown = AtomicBool::new(true);
        let start = Instant::now();
        assert!(!sleep_with_shutdown(&shutdown, Duration::from_secs(1)));
        assert!(start.elapsed() < Duration::from_millis(20));
    }

    #[test]
    fn duplicate_new_block_hashes_are_coalesced() {
        let signal = TipSignal::new();
        let mut event_name = String::new();

        process_sse_line("event: new_block", &mut event_name, &signal);
        process_sse_line(
            "data: {\"hash\":\"abc\",\"height\":1}",
            &mut event_name,
            &signal,
        );
        assert!(signal.take_stale());

        process_sse_line("event: new_block", &mut event_name, &signal);
        process_sse_line(
            "data: {\"hash\":\"abc\",\"height\":1}",
            &mut event_name,
            &signal,
        );
        assert!(!signal.take_stale());
    }

    #[test]
    fn duplicate_new_block_hashes_across_reconnects_are_coalesced() {
        let signal = TipSignal::new();
        let mut first_stream_event = String::new();

        process_sse_line("event: new_block", &mut first_stream_event, &signal);
        process_sse_line(
            "data: {\"hash\":\"abc\",\"height\":1}",
            &mut first_stream_event,
            &signal,
        );
        assert!(signal.take_stale());

        let mut second_stream_event = String::new();
        process_sse_line("event: new_block", &mut second_stream_event, &signal);
        process_sse_line(
            "data: {\"hash\":\"abc\",\"height\":1}",
            &mut second_stream_event,
            &signal,
        );
        assert!(!signal.take_stale());
    }

    #[test]
    fn historical_new_block_events_are_ignored() {
        let signal = TipSignal::new();
        signal.set_current_template_height(1761);
        let mut event_name = String::new();

        process_sse_line("event: new_block", &mut event_name, &signal);
        process_sse_line(
            "data: {\"hash\":\"old\",\"height\":1759}",
            &mut event_name,
            &signal,
        );
        assert!(!signal.take_stale());

        process_sse_line("event: new_block", &mut event_name, &signal);
        process_sse_line(
            "data: {\"hash\":\"tip\",\"height\":1760}",
            &mut event_name,
            &signal,
        );
        assert!(signal.take_stale());
    }

    #[test]
    fn setting_template_height_clears_only_historical_stale_state() {
        let signal = TipSignal::new();
        let mut event_name = String::new();

        process_sse_line("event: new_block", &mut event_name, &signal);
        process_sse_line(
            "data: {\"hash\":\"old\",\"height\":1750}",
            &mut event_name,
            &signal,
        );
        assert!(signal.stale.load(Ordering::Acquire));

        signal.set_current_template_height(1762);
        assert!(!signal.take_stale());
    }

    #[test]
    fn new_block_hash_change_triggers_refresh() {
        let signal = TipSignal::new();
        let mut event_name = String::new();

        process_sse_line("event: new_block", &mut event_name, &signal);
        process_sse_line(
            "data: {\"hash\":\"abc\",\"height\":1}",
            &mut event_name,
            &signal,
        );
        let _ = signal.take_stale();

        process_sse_line("event: new_block", &mut event_name, &signal);
        process_sse_line(
            "data: {\"hash\":\"def\",\"height\":2}",
            &mut event_name,
            &signal,
        );
        assert!(signal.take_stale());
    }
}
