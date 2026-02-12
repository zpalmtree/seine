use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use blocknet_pow_spec::POW_HEADER_BASE_LEN;
use crossbeam_channel::{after, Receiver};
use serde::{Deserialize, Serialize};
use sysinfo::System;

use crate::backend::{BackendEvent, BackendInstanceId, PowBackend};
use crate::config::{BenchKind, Config};

use super::scheduler::NonceScheduler;
use super::stats::{format_hashrate, median};
use super::{
    activate_backends, backend_name_list, backend_names, collect_backend_hashes, distribute_work,
    format_round_backend_hashrate, next_work_id, quiesce_backend_slots, start_backend_slots,
    stop_backend_slots, total_lanes, BackendSlot, RuntimeBackendEventAction, RuntimeMode,
    MIN_EVENT_WAIT,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchBackendRun {
    backend_id: BackendInstanceId,
    backend: String,
    hashes: u64,
    hps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchRun {
    round: u32,
    hashes: u64,
    elapsed_secs: f64,
    #[serde(default)]
    fence_secs: f64,
    hps: f64,
    #[serde(default)]
    backend_runs: Vec<BenchBackendRun>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchReport {
    #[serde(default)]
    environment: BenchEnvironment,
    bench_kind: String,
    backends: Vec<String>,
    total_lanes: u64,
    cpu_threads: usize,
    bench_secs: u64,
    rounds: u32,
    avg_hps: f64,
    median_hps: f64,
    min_hps: f64,
    max_hps: f64,
    runs: Vec<BenchRun>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BenchEnvironment {
    timestamp_unix_secs: u64,
    bnminer_version: String,
    git_commit: Option<String>,
    target_triple: String,
    hostname: Option<String>,
    os: Option<String>,
    kernel_version: Option<String>,
    cpu_arch: Option<String>,
    cpu_brand: Option<String>,
    logical_cores: usize,
    physical_cores: Option<usize>,
    total_memory_bytes: u64,
    available_memory_bytes: u64,
    cgroup_total_memory_bytes: Option<u64>,
    cgroup_free_memory_bytes: Option<u64>,
}

type BackendEventAction = RuntimeBackendEventAction;

pub(super) fn run_benchmark(cfg: &Config, shutdown: &AtomicBool) -> Result<()> {
    let instances = super::build_backend_instances(cfg);

    match cfg.bench_kind {
        BenchKind::Kernel => run_kernel_benchmark(cfg, shutdown, instances),
        BenchKind::Backend => run_worker_benchmark(cfg, shutdown, instances, false),
        BenchKind::EndToEnd => run_worker_benchmark(cfg, shutdown, instances, true),
    }
}

fn run_kernel_benchmark(
    cfg: &Config,
    shutdown: &AtomicBool,
    backends: Vec<Box<dyn PowBackend>>,
) -> Result<()> {
    let mut iter = backends.into_iter();
    let backend = iter
        .next()
        .ok_or_else(|| anyhow!("kernel benchmark requires at least one backend"))?;

    if iter.next().is_some() {
        bail!("kernel benchmark requires exactly one backend");
    }

    println!(
        "benchmark mode | kind=kernel | backend={} | rounds={} | seconds_per_round={}",
        backend.name(),
        cfg.bench_rounds,
        cfg.bench_secs
    );

    let mut runs = Vec::with_capacity(cfg.bench_rounds as usize);
    let environment = benchmark_environment();
    for round in 0..cfg.bench_rounds {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let round_start = Instant::now();
        let hashes = backend.kernel_bench(cfg.bench_secs, shutdown)?;
        let elapsed = round_start.elapsed().as_secs_f64().max(0.001);
        let hps = hashes as f64 / elapsed;

        println!(
            "[bench] round {}/{} | hashes={} | elapsed={:.2}s | {}",
            round + 1,
            cfg.bench_rounds,
            hashes,
            elapsed,
            format_hashrate(hps),
        );

        runs.push(BenchRun {
            round: round + 1,
            hashes,
            elapsed_secs: elapsed,
            fence_secs: 0.0,
            hps,
            backend_runs: Vec::new(),
        });
    }

    summarize_benchmark(
        cfg,
        BenchReport {
            environment,
            bench_kind: "kernel".to_string(),
            backends: vec![backend.name().to_string()],
            total_lanes: backend.lanes() as u64,
            cpu_threads: cfg.threads,
            bench_secs: cfg.bench_secs,
            rounds: runs.len() as u32,
            avg_hps: 0.0,
            median_hps: 0.0,
            min_hps: 0.0,
            max_hps: 0.0,
            runs,
        },
    )
}

fn run_worker_benchmark(
    cfg: &Config,
    shutdown: &AtomicBool,
    instances: Vec<Box<dyn PowBackend>>,
    restart_each_round: bool,
) -> Result<()> {
    let (mut backends, backend_events) = activate_backends(instances, cfg.backend_event_capacity)?;
    let bench_kind = if restart_each_round {
        "end_to_end"
    } else {
        "backend"
    };

    println!(
        "benchmark mode | kind={} | backends={} | lanes={} | rounds={} | seconds_per_round={} | hash_poll={}ms | accounting={} | measurement_fence=on",
        bench_kind,
        backend_names(&backends),
        total_lanes(&backends),
        cfg.bench_rounds,
        cfg.bench_secs,
        cfg.hash_poll_interval.as_millis(),
        if cfg.strict_round_accounting {
            "strict"
        } else {
            "relaxed"
        }
    );

    if restart_each_round {
        stop_backend_slots(&mut backends);
    }

    let result = run_worker_benchmark_inner(
        cfg,
        shutdown,
        &mut backends,
        &backend_events,
        restart_each_round,
    );
    stop_backend_slots(&mut backends);
    result
}

fn run_worker_benchmark_inner(
    cfg: &Config,
    shutdown: &AtomicBool,
    backends: &mut Vec<BackendSlot>,
    backend_events: &Receiver<BackendEvent>,
    restart_each_round: bool,
) -> Result<()> {
    let impossible_target = [0u8; 32];
    let mut runs = Vec::with_capacity(cfg.bench_rounds as usize);
    let environment = benchmark_environment();
    let mut epoch = 0u64;
    let mut work_id_cursor = 1u64;
    let mut scheduler = NonceScheduler::new(cfg.start_nonce, cfg.nonce_iters_per_lane);

    for round in 0..cfg.bench_rounds {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        if backends.is_empty() {
            bail!("all benchmark backends are unavailable");
        }

        if restart_each_round {
            start_backend_slots(backends)?;
        }

        epoch = epoch.wrapping_add(1).max(1);
        let work_id = next_work_id(&mut work_id_cursor);
        let round_start = Instant::now();
        let stop_at = round_start + Duration::from_secs(cfg.bench_secs);
        let header_base = benchmark_header_base(round);
        let reservation = scheduler.reserve(total_lanes(backends));

        distribute_work(
            backends,
            epoch,
            work_id,
            std::sync::Arc::clone(&header_base),
            impossible_target,
            reservation,
            stop_at,
        )?;

        let mut round_hashes = 0u64;
        let mut round_backend_hashes = BTreeMap::new();
        let mut topology_changed = false;
        let mut next_hash_poll_at = Instant::now();
        while Instant::now() < stop_at && !shutdown.load(Ordering::Relaxed) {
            let now = Instant::now();
            if now >= next_hash_poll_at {
                collect_backend_hashes(
                    backends,
                    None,
                    &mut round_hashes,
                    Some(&mut round_backend_hashes),
                );
                next_hash_poll_at = now + cfg.hash_poll_interval;
            }
            let now = Instant::now();
            let wait_for = stop_at
                .saturating_duration_since(now)
                .min(next_hash_poll_at.saturating_duration_since(now))
                .max(MIN_EVENT_WAIT);

            crossbeam_channel::select! {
                recv(backend_events) -> event => {
                    let event = event.map_err(|_| anyhow!("backend event channel closed"))?;
                    if handle_benchmark_backend_event(event, epoch, backends)?
                        == BackendEventAction::TopologyChanged
                    {
                        topology_changed = true;
                    }
                }
                recv(after(wait_for)) -> _ => {}
            }

            if topology_changed
                && !shutdown.load(Ordering::Relaxed)
                && Instant::now() < stop_at
                && !backends.is_empty()
            {
                let reservation = scheduler.reserve(total_lanes(backends));
                eprintln!(
                    "[bench] topology changed; redistributing work epoch={} work_id={} nonce_seed={} backends={}",
                    epoch,
                    work_id,
                    reservation.start_nonce,
                    backend_names(backends),
                );
                distribute_work(
                    backends,
                    epoch,
                    work_id,
                    std::sync::Arc::clone(&header_base),
                    impossible_target,
                    reservation,
                    stop_at,
                )?;
                next_hash_poll_at = Instant::now();
                topology_changed = false;
            }
        }

        let counted_until = std::cmp::min(Instant::now(), stop_at);
        let counted_elapsed = counted_until
            .saturating_duration_since(round_start)
            .as_secs_f64()
            .max(0.001);
        let fence_start = Instant::now();
        quiesce_backend_slots(backends)?;
        let fence_elapsed = fence_start.elapsed().as_secs_f64();
        collect_backend_hashes(
            backends,
            None,
            &mut round_hashes,
            Some(&mut round_backend_hashes),
        );
        drain_benchmark_backend_events(backend_events, epoch, backends)?;

        let hps = round_hashes as f64 / counted_elapsed;
        let backend_runs =
            build_backend_round_stats(backends, &round_backend_hashes, counted_elapsed);

        println!(
            "[bench] round {}/{} | hashes={} | counted={:.2}s | fence={:.3}s | {} | per_backend={}",
            round + 1,
            cfg.bench_rounds,
            round_hashes,
            counted_elapsed,
            fence_elapsed,
            format_hashrate(hps),
            format_round_backend_hashrate(backends, &round_backend_hashes, counted_elapsed),
        );

        runs.push(BenchRun {
            round: round + 1,
            hashes: round_hashes,
            elapsed_secs: counted_elapsed,
            fence_secs: fence_elapsed,
            hps,
            backend_runs,
        });

        if restart_each_round {
            stop_backend_slots(backends);
        }
    }

    summarize_benchmark(
        cfg,
        BenchReport {
            environment,
            bench_kind: if restart_each_round {
                "end_to_end".to_string()
            } else {
                "backend".to_string()
            },
            backends: backend_name_list(backends),
            total_lanes: total_lanes(backends),
            cpu_threads: cfg.threads,
            bench_secs: cfg.bench_secs,
            rounds: runs.len() as u32,
            avg_hps: 0.0,
            median_hps: 0.0,
            min_hps: 0.0,
            max_hps: 0.0,
            runs,
        },
    )
}

fn summarize_benchmark(cfg: &Config, mut report: BenchReport) -> Result<()> {
    if report.runs.is_empty() {
        println!("benchmark aborted before first round");
        return Ok(());
    }

    let mut sorted_hps: Vec<f64> = report.runs.iter().map(|r| r.hps).collect();
    sorted_hps.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    report.avg_hps = sorted_hps.iter().sum::<f64>() / sorted_hps.len() as f64;
    report.median_hps = median(&sorted_hps);
    report.min_hps = *sorted_hps.first().unwrap_or(&0.0);
    report.max_hps = *sorted_hps.last().unwrap_or(&0.0);

    println!(
        "[bench] summary | avg={} | median={} | min={} | max={}",
        format_hashrate(report.avg_hps),
        format_hashrate(report.median_hps),
        format_hashrate(report.min_hps),
        format_hashrate(report.max_hps),
    );

    if let Some(path) = &cfg.bench_baseline {
        let baseline_text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read baseline file {}", path.display()))?;
        let baseline: BenchReport = serde_json::from_str(&baseline_text)
            .with_context(|| format!("failed to parse baseline JSON {}", path.display()))?;
        if baseline.avg_hps > 0.0 {
            let delta_pct = ((report.avg_hps - baseline.avg_hps) / baseline.avg_hps) * 100.0;
            println!(
                "[bench] baseline compare | baseline_avg={} | delta={:+.2}%",
                format_hashrate(baseline.avg_hps),
                delta_pct
            );
        }
    }

    if let Some(path) = &cfg.bench_output {
        let json = serde_json::to_string_pretty(&report)
            .context("failed to serialize benchmark report")?;
        std::fs::write(path, json)
            .with_context(|| format!("failed to write benchmark report {}", path.display()))?;
        println!("[bench] wrote report to {}", path.display());
    }

    Ok(())
}

fn benchmark_header_base(round: u32) -> std::sync::Arc<[u8]> {
    let mut data = [0u8; POW_HEADER_BASE_LEN];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i as u8)
            .wrapping_mul(37)
            .wrapping_add(11)
            .wrapping_add((round % 251) as u8);
    }
    std::sync::Arc::from(data.to_vec())
}

fn benchmark_environment() -> BenchEnvironment {
    let mut sys = System::new();
    sys.refresh_memory();
    sys.refresh_cpu_all();
    let cgroup = sys.cgroup_limits();

    BenchEnvironment {
        timestamp_unix_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        bnminer_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: std::env::var("BNMINER_GIT_COMMIT")
            .ok()
            .or_else(|| option_env!("BNMINER_GIT_COMMIT").map(str::to_string)),
        target_triple: format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH),
        hostname: System::host_name(),
        os: System::long_os_version().or_else(System::name),
        kernel_version: System::kernel_version(),
        cpu_arch: System::cpu_arch(),
        cpu_brand: sys.cpus().first().map(|cpu| cpu.brand().to_string()),
        logical_cores: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        physical_cores: sys.physical_core_count(),
        total_memory_bytes: sys.total_memory(),
        available_memory_bytes: sys.available_memory(),
        cgroup_total_memory_bytes: cgroup
            .as_ref()
            .map(|limits| limits.total_memory)
            .filter(|value| *value > 0),
        cgroup_free_memory_bytes: cgroup
            .as_ref()
            .map(|limits| limits.free_memory)
            .filter(|value| *value > 0),
    }
}

fn build_backend_round_stats(
    backends: &[BackendSlot],
    round_backend_hashes: &BTreeMap<BackendInstanceId, u64>,
    elapsed_secs: f64,
) -> Vec<BenchBackendRun> {
    let elapsed_secs = elapsed_secs.max(0.001);
    let mut runs = Vec::with_capacity(round_backend_hashes.len());
    for (backend_id, hashes) in round_backend_hashes {
        let backend = backends
            .iter()
            .find(|slot| slot.id == *backend_id)
            .map(|slot| slot.backend.name().to_string())
            .unwrap_or_else(|| "unknown".to_string());
        runs.push(BenchBackendRun {
            backend_id: *backend_id,
            backend,
            hashes: *hashes,
            hps: *hashes as f64 / elapsed_secs,
        });
    }
    runs
}

fn handle_benchmark_backend_event(
    event: BackendEvent,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
) -> Result<BackendEventAction> {
    let (action, _) =
        super::handle_runtime_backend_event(event, epoch, backends, RuntimeMode::Bench)?;
    Ok(action)
}

fn drain_benchmark_backend_events(
    backend_events: &Receiver<BackendEvent>,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
) -> Result<BackendEventAction> {
    let (action, _) =
        super::drain_runtime_backend_events(backend_events, epoch, backends, RuntimeMode::Bench)?;
    Ok(action)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_ignores_stale_solution_events() {
        let mut backends = Vec::new();
        let action = handle_benchmark_backend_event(
            BackendEvent::Solution(crate::backend::MiningSolution {
                epoch: 99,
                nonce: 123,
                backend_id: 1,
                backend: "cpu",
            }),
            100,
            &mut backends,
        )
        .expect("stale benchmark solution event should be ignored");
        assert_eq!(action, BackendEventAction::None);
    }
}
