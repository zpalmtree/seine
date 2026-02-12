use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use blocknet_pow_spec::POW_HEADER_BASE_LEN;
use crossbeam_channel::{after, Receiver};

use crate::api::ApiClient;
use crate::backend::{BackendEvent, MiningSolution};
use crate::config::Config;
use crate::types::{
    decode_hex, parse_target, set_block_nonce, template_difficulty, template_height,
    BlockTemplateResponse,
};

use super::scheduler::NonceScheduler;
use super::stats::Stats;
use super::{
    collect_backend_hashes, distribute_work, next_event_wait, next_work_id, quiesce_backend_slots,
    remove_backend_by_name, total_lanes, BackendSlot, TEMPLATE_RETRY_DELAY,
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

        let template = match fetch_template_with_retry(client, shutdown) {
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
            Arc::from(header_base),
            target,
            reservation,
            stop_at,
        )?;

        let round_start = Instant::now();
        let mut solved: Option<MiningSolution> = None;
        let mut stale_tip_event = false;
        let mut round_hashes = 0u64;

        while !shutdown.load(Ordering::Relaxed)
            && Instant::now() < stop_at
            && solved.is_none()
            && !stale_tip_event
        {
            collect_backend_hashes(backends, Some(&stats), &mut round_hashes);

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
                    handle_mining_backend_event(event, epoch, &mut solved, backends)?;
                }
                recv(after(wait_for)) -> _ => {}
            }
        }

        quiesce_backend_slots(backends)?;
        drain_mining_backend_events(backend_events, epoch, &mut solved, backends)?;
        collect_backend_hashes(backends, Some(&stats), &mut round_hashes);
        stale_tip_event |= tip_signal.is_some_and(TipSignal::take_stale);

        if let Some(solution) = solved {
            println!(
                "[solution] backend={} nonce={} elapsed={:.2}s",
                solution.backend,
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
) -> Result<()> {
    match event {
        BackendEvent::Solution(solution) => {
            let backend_active = backends
                .iter()
                .any(|slot| slot.backend.name() == solution.backend);
            if solution.epoch == epoch && backend_active {
                *solved = Some(solution);
            }
        }
        BackendEvent::Error { backend, message } => {
            eprintln!("[backend] {backend} runtime error: {message}");
            let removed = remove_backend_by_name(backends, backend);
            if removed {
                let remaining = super::backend_names(backends);
                if backends.is_empty() {
                    bail!("all mining backends are unavailable after failure in '{backend}'");
                }
                eprintln!("[backend] quarantined {backend}; continuing with {remaining}");
            } else {
                eprintln!("[backend] ignoring error from unavailable backend '{backend}'");
            }
        }
    }
    Ok(())
}

fn drain_mining_backend_events(
    backend_events: &Receiver<BackendEvent>,
    epoch: u64,
    solved: &mut Option<MiningSolution>,
    backends: &mut Vec<BackendSlot>,
) -> Result<()> {
    while let Ok(event) = backend_events.try_recv() {
        handle_mining_backend_event(event, epoch, solved, backends)?;
    }
    Ok(())
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
    let reader = BufReader::new(resp);

    for line_result in reader.lines() {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let line = line_result.context("failed reading SSE event stream")?;

        if let Some(name) = line.strip_prefix("event:") {
            event_name = name.trim().to_string();
            continue;
        }

        if line.is_empty() {
            event_name.clear();
            continue;
        }

        if event_name == "new_block" && line.starts_with("data:") {
            stale.store(true, Ordering::Release);
        }
    }

    Ok(())
}

fn fetch_template_with_retry(
    client: &ApiClient,
    shutdown: &AtomicBool,
) -> Option<BlockTemplateResponse> {
    while !shutdown.load(Ordering::Relaxed) {
        match client.get_block_template() {
            Ok(template) => return Some(template),
            Err(err) => {
                eprintln!("blocktemplate request failed: {err:#}");
                thread::sleep(TEMPLATE_RETRY_DELAY);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_solution_is_ignored_for_current_epoch() {
        let mut solved = None;
        let mut backends = Vec::new();

        handle_mining_backend_event(
            BackendEvent::Solution(MiningSolution {
                epoch: 41,
                nonce: 9,
                backend: "cpu".to_string(),
            }),
            42,
            &mut solved,
            &mut backends,
        )
        .expect("stale solution handling should succeed");

        assert!(solved.is_none());
    }
}
