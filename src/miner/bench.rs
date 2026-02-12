use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use blocknet_pow_spec::POW_HEADER_BASE_LEN;
use crossbeam_channel::{after, Receiver};
use serde::{Deserialize, Serialize};

use crate::backend::{BackendEvent, PowBackend};
use crate::config::{BenchKind, Config};

use super::scheduler::NonceScheduler;
use super::stats::{format_hashrate, median};
use super::{
    activate_backends, backend_name_list, backend_names, collect_backend_hashes, distribute_work,
    next_work_id, quiesce_backend_slots, remove_backend_by_name, start_backend_slots,
    stop_backend_slots, total_lanes, BackendSlot, HASH_POLL_INTERVAL, MIN_EVENT_WAIT,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchRun {
    round: u32,
    hashes: u64,
    elapsed_secs: f64,
    hps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchReport {
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
            hps,
        });
    }

    summarize_benchmark(
        cfg,
        BenchReport {
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
    let (mut backends, backend_events) = activate_backends(instances)?;
    let bench_kind = if restart_each_round {
        "end_to_end"
    } else {
        "backend"
    };

    println!(
        "benchmark mode | kind={} | backends={} | lanes={} | rounds={} | seconds_per_round={}",
        bench_kind,
        backend_names(&backends),
        total_lanes(&backends),
        cfg.bench_rounds,
        cfg.bench_secs
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
            header_base,
            impossible_target,
            reservation,
            stop_at,
        )?;

        let mut round_hashes = 0u64;
        while Instant::now() < stop_at && !shutdown.load(Ordering::Relaxed) {
            collect_backend_hashes(backends, None, &mut round_hashes);
            let wait_for = stop_at
                .saturating_duration_since(Instant::now())
                .min(HASH_POLL_INTERVAL)
                .max(MIN_EVENT_WAIT);

            crossbeam_channel::select! {
                recv(backend_events) -> event => {
                    let event = event.map_err(|_| anyhow!("backend event channel closed"))?;
                    handle_benchmark_backend_event(event, epoch, backends)?;
                }
                recv(after(wait_for)) -> _ => {}
            }
        }

        quiesce_backend_slots(backends)?;
        collect_backend_hashes(backends, None, &mut round_hashes);
        drain_benchmark_backend_events(backend_events, epoch, backends)?;

        let elapsed = round_start.elapsed().as_secs_f64().max(0.001);
        let hps = round_hashes as f64 / elapsed;

        println!(
            "[bench] round {}/{} | hashes={} | elapsed={:.2}s | {}",
            round + 1,
            cfg.bench_rounds,
            round_hashes,
            elapsed,
            format_hashrate(hps),
        );

        runs.push(BenchRun {
            round: round + 1,
            hashes: round_hashes,
            elapsed_secs: elapsed,
            hps,
        });

        if restart_each_round {
            stop_backend_slots(backends);
        }
    }

    summarize_benchmark(
        cfg,
        BenchReport {
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

fn handle_benchmark_backend_event(
    event: BackendEvent,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
) -> Result<()> {
    match event {
        BackendEvent::Solution(solution) => {
            let backend_active = backends
                .iter()
                .any(|slot| slot.backend.name() == solution.backend);
            if solution.epoch == epoch && backend_active {
                println!(
                    "[bench] unexpected solution found by {} at nonce={}",
                    solution.backend, solution.nonce
                );
            }
        }
        BackendEvent::Error { backend, message } => {
            eprintln!("[bench] backend '{backend}' runtime error: {message}");
            let removed = remove_backend_by_name(backends, backend);
            if removed {
                if backends.is_empty() {
                    bail!("all benchmark backends are unavailable after failure in '{backend}'");
                }
                eprintln!(
                    "[bench] quarantined {backend}; remaining backends={}",
                    backend_names(backends)
                );
            }
        }
    }
    Ok(())
}

fn drain_benchmark_backend_events(
    backend_events: &Receiver<BackendEvent>,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
) -> Result<()> {
    while let Ok(event) = backend_events.try_recv() {
        handle_benchmark_backend_event(event, epoch, backends)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_ignores_stale_solution_events() {
        let mut backends = Vec::new();
        handle_benchmark_backend_event(
            BackendEvent::Solution(crate::backend::MiningSolution {
                epoch: 99,
                nonce: 123,
                backend: "cpu".to_string(),
            }),
            100,
            &mut backends,
        )
        .expect("stale benchmark solution event should be ignored");
    }
}
