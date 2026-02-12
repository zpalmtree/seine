use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, IsTerminal};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
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
use super::stats::Stats;
use super::{
    collect_backend_hashes, distribute_work, format_round_backend_hashrate, next_event_wait,
    next_work_id, quiesce_backend_slots, remove_backend_by_id, total_lanes, BackendSlot,
    TEMPLATE_RETRY_DELAY,
};

pub(super) struct TipSignal {
    stale: Arc<AtomicBool>,
}

impl TipSignal {
    fn new() -> Self {
        Self {
            stale: Arc::new(AtomicBool::new(false)),
        }
    }

    fn take_stale(&self) -> bool {
        self.stale.swap(false, Ordering::AcqRel)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum BackendEventAction {
    None,
    TopologyChanged,
}

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
            Self::Environment => "BNMINER_WALLET_PASSWORD",
            Self::Prompt => "terminal prompt",
        }
    }
}

pub(super) fn run_mining_loop(
    cfg: &Config,
    client: &ApiClient,
    shutdown: &AtomicBool,
    backends: &mut Vec<BackendSlot>,
    backend_events: &Receiver<BackendEvent>,
    tip_signal: Option<&TipSignal>,
) -> Result<()> {
    let stats = Stats::new();
    let mut nonce_scheduler = NonceScheduler::new(cfg.start_nonce, cfg.nonce_iters_per_lane);
    let mut work_id_cursor = 1u64;
    let mut epoch = 0u64;
    let mut last_stats_print = Instant::now();

    while !shutdown.load(Ordering::Relaxed) {
        if backends.is_empty() {
            bail!("all mining backends are unavailable");
        }

        let template = match fetch_template_with_retry(client, cfg, shutdown) {
            Some(t) => t,
            None => break,
        };

        let header_base = match decode_hex(&template.header_base, "header_base") {
            Ok(v) => v,
            Err(err) => {
                eprintln!("template decode error: {err:#}");
                continue;
            }
        };

        if header_base.len() != POW_HEADER_BASE_LEN {
            eprintln!(
                "template header_base length mismatch: expected {} bytes, got {}",
                POW_HEADER_BASE_LEN,
                header_base.len()
            );
            continue;
        }

        let target = match parse_target(&template.target) {
            Ok(t) => t,
            Err(err) => {
                eprintln!("target parse error: {err:#}");
                continue;
            }
        };
        let header_base: Arc<[u8]> = Arc::from(header_base);

        let height = template_height(&template.block)
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

        println!(
            "[template] height={} difficulty={} epoch={} work_id={} nonce_seed={} refresh={}s",
            height,
            difficulty,
            epoch,
            work_id,
            reservation.start_nonce,
            cfg.refresh_interval.as_secs(),
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

        let round_start = Instant::now();
        let mut solved: Option<MiningSolution> = None;
        let mut stale_tip_event = false;
        let mut round_hashes = 0u64;
        let mut round_backend_hashes = BTreeMap::new();
        let mut topology_changed = false;

        while !shutdown.load(Ordering::Relaxed)
            && Instant::now() < stop_at
            && solved.is_none()
            && !stale_tip_event
        {
            collect_backend_hashes(
                backends,
                Some(&stats),
                &mut round_hashes,
                Some(&mut round_backend_hashes),
            );

            if last_stats_print.elapsed() >= cfg.stats_interval {
                stats.print();
                last_stats_print = Instant::now();
            }

            if tip_signal.is_some_and(TipSignal::take_stale) {
                stale_tip_event = true;
                continue;
            }

            let wait_for = next_event_wait(stop_at, last_stats_print, cfg.stats_interval);

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
                eprintln!(
                    "[backend] topology changed; redistributing work epoch={} work_id={} nonce_seed={} backends={}",
                    epoch,
                    work_id,
                    reservation.start_nonce,
                    super::backend_names(backends),
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
                topology_changed = false;
            }
        }

        quiesce_backend_slots(backends)?;
        let _ = drain_mining_backend_events(backend_events, epoch, &mut solved, backends)?;
        collect_backend_hashes(
            backends,
            Some(&stats),
            &mut round_hashes,
            Some(&mut round_backend_hashes),
        );
        stale_tip_event |= tip_signal.is_some_and(TipSignal::take_stale);
        println!(
            "[template] backend_throughput {}",
            format_round_backend_hashrate(
                backends,
                &round_backend_hashes,
                round_start.elapsed().as_secs_f64()
            )
        );

        if let Some(solution) = solved {
            println!(
                "[solution] backend={}#{} nonce={} elapsed={:.2}s",
                solution.backend,
                solution.backend_id,
                solution.nonce,
                round_start.elapsed().as_secs_f64(),
            );

            let template_id = template.template_id.clone();
            let mut solved_block = template.block;
            set_block_nonce(&mut solved_block, solution.nonce);

            stats.bump_submitted();

            match client.submit_block(&solved_block, template_id.as_deref(), solution.nonce) {
                Ok(resp) => {
                    if resp.accepted {
                        stats.bump_accepted();
                        println!(
                            "[submit] accepted=true height={} hash={}",
                            resp.height
                                .map(|h| h.to_string())
                                .unwrap_or_else(|| "unknown".to_string()),
                            resp.hash.unwrap_or_else(|| "unknown".to_string())
                        );
                    } else {
                        println!("[submit] accepted=false");
                    }
                }
                Err(err) => {
                    eprintln!("submit failed: {err:#}");
                }
            }
        } else if stale_tip_event {
            println!("[template] stale tip event received; refreshing template immediately");
        } else {
            println!("[template] no solution before refresh or shutdown");
        }

        if last_stats_print.elapsed() >= cfg.stats_interval {
            stats.print();
            last_stats_print = Instant::now();
        }
    }

    stats.print();
    println!("bnminer stopped");
    Ok(())
}

fn handle_mining_backend_event(
    event: BackendEvent,
    epoch: u64,
    solved: &mut Option<MiningSolution>,
    backends: &mut Vec<BackendSlot>,
) -> Result<BackendEventAction> {
    match event {
        BackendEvent::Solution(solution) => {
            let backend_active = backends.iter().any(|slot| slot.id == solution.backend_id);
            if solution.epoch == epoch && backend_active {
                *solved = Some(solution);
            }
            Ok(BackendEventAction::None)
        }
        BackendEvent::Error {
            backend_id,
            backend,
            message,
        } => {
            eprintln!("[backend] {backend}#{backend_id} runtime error: {message}");
            let removed = remove_backend_by_id(backends, backend_id);
            if removed {
                let remaining = super::backend_names(backends);
                if backends.is_empty() {
                    bail!("all mining backends are unavailable after failure in '{backend}#{backend_id}'");
                }
                eprintln!(
                    "[backend] quarantined {backend}#{backend_id}; continuing with {remaining}"
                );
                Ok(BackendEventAction::TopologyChanged)
            } else {
                eprintln!(
                    "[backend] ignoring error from unavailable backend '{backend}#{backend_id}'"
                );
                Ok(BackendEventAction::None)
            }
        }
    }
}

fn drain_mining_backend_events(
    backend_events: &Receiver<BackendEvent>,
    epoch: u64,
    solved: &mut Option<MiningSolution>,
    backends: &mut Vec<BackendSlot>,
) -> Result<BackendEventAction> {
    let mut action = BackendEventAction::None;
    while let Ok(event) = backend_events.try_recv() {
        if handle_mining_backend_event(event, epoch, solved, backends)?
            == BackendEventAction::TopologyChanged
        {
            action = BackendEventAction::TopologyChanged;
        }
    }
    Ok(action)
}

pub(super) fn spawn_tip_listener(client: ApiClient, shutdown: Arc<AtomicBool>) -> TipSignal {
    let tip_signal = TipSignal::new();
    let signal = Arc::clone(&tip_signal.stale);

    thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            match client.open_events_stream() {
                Ok(resp) => {
                    if let Err(err) = stream_tip_events(resp, &signal, &shutdown) {
                        if !shutdown.load(Ordering::Relaxed) {
                            eprintln!("events stream dropped: {err:#}");
                        }
                    }
                }
                Err(err) => {
                    if !shutdown.load(Ordering::Relaxed) {
                        eprintln!("failed to open events stream: {err:#}");
                    }
                }
            }

            if !shutdown.load(Ordering::Relaxed) {
                thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    });

    tip_signal
}

fn stream_tip_events(
    resp: reqwest::blocking::Response,
    stale: &AtomicBool,
    shutdown: &AtomicBool,
) -> Result<()> {
    let mut event_name = String::new();
    let mut last_new_block_hash: Option<String> = None;
    let reader = BufReader::new(resp);

    for line_result in reader.lines() {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let line = line_result.context("failed reading SSE event stream")?;
        process_sse_line(&line, &mut event_name, &mut last_new_block_hash, stale);
    }

    Ok(())
}

fn process_sse_line(
    line: &str,
    event_name: &mut String,
    last_new_block_hash: &mut Option<String>,
    stale: &AtomicBool,
) {
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
    if let Some(hash) = extract_new_block_hash(payload) {
        if last_new_block_hash.as_deref() != Some(hash.as_str()) {
            stale.store(true, Ordering::Release);
        }
        *last_new_block_hash = Some(hash);
        return;
    }

    // If parsing fails, preserve old behavior and refresh once.
    stale.store(true, Ordering::Release);
    *last_new_block_hash = None;
}

fn extract_new_block_hash(payload: &str) -> Option<String> {
    let value: Value = serde_json::from_str(payload).ok()?;
    value
        .get("hash")
        .and_then(Value::as_str)
        .map(str::to_string)
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
                eprintln!(
                    "[wallet] blocktemplate requires a loaded wallet; attempting automatic wallet load"
                );
                match auto_load_wallet(client, cfg, shutdown) {
                    Ok(true) => continue,
                    Ok(false) => {
                        eprintln!(
                            "[wallet] unable to load wallet automatically. Provide --wallet-password, --wallet-password-file, BNMINER_WALLET_PASSWORD, or run bnminer in a terminal for prompt-based loading."
                        );
                        return None;
                    }
                    Err(load_err) => {
                        eprintln!("[wallet] automatic wallet load failed: {load_err:#}");
                        return None;
                    }
                }
            }
            Err(err) => {
                eprintln!("blocktemplate request failed: {err:#}");
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
                println!("[wallet] loaded wallet via {}", source.as_str());
                password.clear();
                return Ok(true);
            }
            Err(err) if is_wallet_already_loaded_error(&err) => {
                println!("[wallet] wallet already loaded");
                password.clear();
                return Ok(true);
            }
            Err(err)
                if source == WalletPasswordSource::Prompt
                    && prompt_attempt < MAX_PROMPT_ATTEMPTS =>
            {
                eprintln!("[wallet] load failed: {err:#}");
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
        let stale = AtomicBool::new(false);
        let mut event_name = String::new();
        let mut last_hash = None;

        process_sse_line("event: new_block", &mut event_name, &mut last_hash, &stale);
        process_sse_line(
            "data: {\"hash\":\"abc\",\"height\":1}",
            &mut event_name,
            &mut last_hash,
            &stale,
        );
        assert!(stale.swap(false, Ordering::AcqRel));

        process_sse_line("event: new_block", &mut event_name, &mut last_hash, &stale);
        process_sse_line(
            "data: {\"hash\":\"abc\",\"height\":1}",
            &mut event_name,
            &mut last_hash,
            &stale,
        );
        assert!(!stale.swap(false, Ordering::AcqRel));
    }

    #[test]
    fn new_block_hash_change_triggers_refresh() {
        let stale = AtomicBool::new(false);
        let mut event_name = String::new();
        let mut last_hash = None;

        process_sse_line("event: new_block", &mut event_name, &mut last_hash, &stale);
        process_sse_line(
            "data: {\"hash\":\"abc\",\"height\":1}",
            &mut event_name,
            &mut last_hash,
            &stale,
        );
        let _ = stale.swap(false, Ordering::AcqRel);

        process_sse_line("event: new_block", &mut event_name, &mut last_hash, &stale);
        process_sse_line(
            "data: {\"hash\":\"def\",\"height\":2}",
            &mut event_name,
            &mut last_hash,
            &stale,
        );
        assert!(stale.swap(false, Ordering::AcqRel));
    }
}
