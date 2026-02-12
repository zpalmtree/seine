mod bench;
mod mining;
mod scheduler;
mod stats;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use crossbeam_channel::{unbounded, Receiver};

use crate::api::ApiClient;
use crate::backend::cpu::CpuBackend;
use crate::backend::nvidia::NvidiaBackend;
use crate::backend::{BackendEvent, MiningWork, PowBackend, WORK_ID_MAX};
use crate::config::{BackendKind, Config};
use scheduler::NonceReservation;
use stats::Stats;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const TEMPLATE_RETRY_DELAY: Duration = Duration::from_secs(2);
const MIN_EVENT_WAIT: Duration = Duration::from_millis(1);
const HASH_POLL_INTERVAL: Duration = Duration::from_millis(200);

struct BackendSlot {
    backend: Box<dyn PowBackend>,
    lane_offset: u64,
    lanes: u64,
}

pub fn run(cfg: &Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    if cfg.bench {
        return bench::run_benchmark(cfg, shutdown.as_ref());
    }

    let token = cfg
        .token
        .clone()
        .ok_or_else(|| anyhow!("missing API token in mining mode"))?;
    let client = ApiClient::new(cfg.api_url.clone(), token, REQUEST_TIMEOUT)?;

    let backend_instances = build_backend_instances(cfg);
    let (mut backends, backend_events) = activate_backends(backend_instances)?;
    let total_lanes = total_lanes(&backends);

    println!(
        "starting bnminer | backends={} | cpu_threads={} (~{}GB RAM for CPU lanes) | lanes={} | nonce_iters_per_lane={} | api={} | sse={}",
        backend_names(&backends),
        cfg.threads,
        cfg.threads * 2,
        total_lanes,
        cfg.nonce_iters_per_lane,
        cfg.api_url,
        if cfg.sse_enabled { "on" } else { "off" }
    );

    let tip_events = if cfg.sse_enabled {
        Some(mining::spawn_tip_listener(
            client.clone(),
            Arc::clone(&shutdown),
        ))
    } else {
        None
    };

    let result = mining::run_mining_loop(
        cfg,
        &client,
        shutdown.as_ref(),
        &mut backends,
        &backend_events,
        tip_events.as_ref(),
    );

    stop_backend_slots(&mut backends);
    result
}

fn build_backend_instances(cfg: &Config) -> Vec<Box<dyn PowBackend>> {
    cfg.backends
        .iter()
        .map(|backend_kind| match backend_kind {
            BackendKind::Cpu => Box::new(CpuBackend::new(cfg.threads)) as Box<dyn PowBackend>,
            BackendKind::Nvidia => Box::new(NvidiaBackend::new()) as Box<dyn PowBackend>,
        })
        .collect()
}

fn activate_backends(
    mut backends: Vec<Box<dyn PowBackend>>,
) -> Result<(Vec<BackendSlot>, Receiver<BackendEvent>)> {
    let mut active = Vec::new();
    let (event_tx, event_rx) = unbounded::<BackendEvent>();

    for mut backend in backends.drain(..) {
        let backend_name = backend.name();
        backend.set_event_sink(event_tx.clone());
        match backend.start() {
            Ok(()) => {
                let lanes = backend.lanes() as u64;
                if lanes == 0 {
                    eprintln!("[backend] skipping {}: reported zero lanes", backend_name);
                    backend.stop();
                    continue;
                }
                active.push(BackendSlot {
                    backend,
                    lane_offset: 0,
                    lanes,
                });
            }
            Err(err) => {
                eprintln!("[backend] {} unavailable: {err:#}", backend_name);
            }
        }
    }

    if active.is_empty() {
        bail!("no mining backend could be started");
    }

    let mut lane_offset = 0u64;
    for slot in &mut active {
        slot.lane_offset = lane_offset;
        lane_offset = lane_offset.wrapping_add(slot.lanes);
    }

    Ok((active, event_rx))
}

fn start_backend_slots(backends: &mut [BackendSlot]) -> Result<()> {
    for slot in backends {
        slot.backend
            .start()
            .with_context(|| format!("failed to start backend {}", slot.backend.name()))?;
    }
    Ok(())
}

fn stop_backend_slots(backends: &mut [BackendSlot]) {
    for slot in backends {
        slot.backend.stop();
    }
}

fn distribute_work(
    backends: &[BackendSlot],
    epoch: u64,
    work_id: u64,
    header_base: Arc<[u8]>,
    target: [u8; 32],
    reservation: NonceReservation,
    stop_at: Instant,
) -> Result<()> {
    let stride = total_lanes(backends);
    for slot in backends {
        let work = MiningWork {
            work_id,
            epoch,
            header_base: Arc::clone(&header_base),
            target,
            start_nonce: reservation.start_nonce,
            lane_offset: slot.lane_offset,
            global_stride: stride,
            max_iters_per_lane: reservation.max_iters_per_lane,
            stop_at,
        };
        slot.backend
            .set_work(work)
            .with_context(|| format!("failed to set work for backend {}", slot.backend.name()))?;
    }
    Ok(())
}

fn collect_backend_hashes(
    backends: &[BackendSlot],
    epoch: u64,
    stats: Option<&Stats>,
    round_hashes: &mut u64,
) {
    let mut collected = 0u64;
    for slot in backends {
        collected = collected.saturating_add(slot.backend.take_hashes(epoch));
    }

    if collected == 0 {
        return;
    }

    if let Some(stats) = stats {
        stats.add_hashes(collected);
    }
    *round_hashes = round_hashes.saturating_add(collected);
}

fn backend_name_list(backends: &[BackendSlot]) -> Vec<String> {
    backends
        .iter()
        .map(|slot| slot.backend.name().to_string())
        .collect()
}

fn backend_names(backends: &[BackendSlot]) -> String {
    backend_name_list(backends).join(",")
}

fn total_lanes(backends: &[BackendSlot]) -> u64 {
    backends.iter().map(|slot| slot.lanes).sum::<u64>().max(1)
}

fn next_event_wait(
    stop_at: Instant,
    last_stats_print: Instant,
    stats_interval: Duration,
) -> Duration {
    let now = Instant::now();
    let until_stop = stop_at.saturating_duration_since(now);
    let next_stats_at = last_stats_print + stats_interval;
    let until_stats = next_stats_at.saturating_duration_since(now);
    until_stop
        .min(until_stats)
        .min(HASH_POLL_INTERVAL)
        .max(MIN_EVENT_WAIT)
}

fn next_work_id(next_id: &mut u64) -> u64 {
    if *next_id == 0 || *next_id > WORK_ID_MAX {
        *next_id = 1;
    }
    let id = *next_id;
    *next_id = if id >= WORK_ID_MAX { 1 } else { id + 1 };
    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use blocknet_pow_spec::POW_HEADER_BASE_LEN;

    #[test]
    fn next_work_id_wraps_within_valid_range() {
        let mut id = WORK_ID_MAX;
        assert_eq!(next_work_id(&mut id), WORK_ID_MAX);
        assert_eq!(next_work_id(&mut id), 1);
    }

    #[test]
    fn total_lanes_never_zero() {
        assert_eq!(total_lanes(&[]), 1);
    }

    #[test]
    fn header_base_len_matches_pow_spec() {
        assert_eq!(POW_HEADER_BASE_LEN, 92);
    }
}
