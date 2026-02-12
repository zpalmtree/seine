use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use blocknet_pow_spec::POW_HEADER_BASE_LEN;
use crossbeam_channel::Receiver;
use serde::{Deserialize, Serialize};
use sysinfo::System;

use crate::backend::{BackendEvent, BackendInstanceId, PowBackend};
use crate::config::{BenchKind, Config};

use super::scheduler::NonceScheduler;
use super::stats::{format_hashrate, median};
use super::ui::{info, startup_banner, success, warn};
use super::{
    activate_backends, backend_name_list, backend_names, collect_backend_hashes, distribute_work,
    format_round_backend_hashrate, format_round_backend_telemetry, next_work_id,
    quiesce_backend_slots, start_backend_slots, stop_backend_slots, total_lanes,
    BackendRoundTelemetry, BackendSlot, RuntimeBackendEventAction, RuntimeMode, MIN_EVENT_WAIT,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchBackendRun {
    backend_id: BackendInstanceId,
    backend: String,
    hashes: u64,
    hps: f64,
    #[serde(default)]
    peak_active_lanes: u64,
    #[serde(default)]
    peak_pending_work: u64,
    #[serde(default)]
    dropped_events: u64,
    #[serde(default)]
    completed_assignments: u64,
    #[serde(default)]
    completed_assignment_hashes: u64,
    #[serde(default)]
    completed_assignment_secs: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchRun {
    round: u32,
    hashes: u64,
    #[serde(default)]
    counted_hashes: u64,
    #[serde(default)]
    late_hashes: u64,
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
    #[serde(default)]
    preemption: Vec<String>,
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
    #[serde(alias = "bnminer_version")]
    seine_version: String,
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

    let lines = vec![
        ("Mode", "benchmark".to_string()),
        ("Kind", "kernel".to_string()),
        ("Backend", backend.name().to_string()),
        ("Preemption", backend.preemption_granularity().describe()),
        ("Rounds", cfg.bench_rounds.to_string()),
        ("Seconds/Round", cfg.bench_secs.to_string()),
        (
            "Regress Gate",
            cfg.bench_fail_below_pct
                .map(|pct| format!("-{pct:.2}%"))
                .unwrap_or_else(|| "off".to_string()),
        ),
    ];
    startup_banner(&lines);

    let mut runs = Vec::with_capacity(cfg.bench_rounds as usize);
    let environment = benchmark_environment();
    let bench_backend = backend
        .bench_backend()
        .ok_or_else(|| anyhow!("kernel benchmark is not implemented for {}", backend.name()))?;
    for round in 0..cfg.bench_rounds {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let round_start = Instant::now();
        let hashes = bench_backend.kernel_bench(cfg.bench_secs, shutdown)?;
        let elapsed = round_start.elapsed().as_secs_f64().max(0.001);
        let hps = hashes as f64 / elapsed;

        info(
            "BENCH",
            format!(
                "round {}/{} | hashes={} | elapsed={:.2}s | {}",
                round + 1,
                cfg.bench_rounds,
                hashes,
                elapsed,
                format_hashrate(hps),
            ),
        );

        runs.push(BenchRun {
            round: round + 1,
            hashes,
            counted_hashes: hashes,
            late_hashes: 0,
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
            preemption: vec![format!(
                "{}={}",
                backend.name(),
                backend.preemption_granularity().describe()
            )],
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

    let lines = vec![
        ("Mode", "benchmark".to_string()),
        ("Kind", bench_kind.to_string()),
        ("Backends", backend_names(&backends)),
        ("Preemption", super::backend_preemption_profiles(&backends)),
        ("Lanes", total_lanes(&backends).to_string()),
        ("Rounds", cfg.bench_rounds.to_string()),
        ("Seconds/Round", cfg.bench_secs.to_string()),
        (
            "Hash Poll",
            format!("{}ms", cfg.hash_poll_interval.as_millis()),
        ),
        (
            "Accounting",
            if cfg.strict_round_accounting {
                "strict"
            } else {
                "relaxed"
            }
            .to_string(),
        ),
        ("Measurement", "counted window + end fence".to_string()),
        (
            "Regress Gate",
            cfg.bench_fail_below_pct
                .map(|pct| format!("-{pct:.2}%"))
                .unwrap_or_else(|| "off".to_string()),
        ),
    ];
    startup_banner(&lines);

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

        let additional_span = distribute_work(
            backends,
            super::DistributeWorkOptions {
                epoch,
                work_id,
                header_base: std::sync::Arc::clone(&header_base),
                target: impossible_target,
                reservation,
                stop_at,
                backend_weights: None,
            },
        )?;
        scheduler.consume_additional_span(additional_span);

        let mut round_hashes = 0u64;
        let mut round_backend_hashes = BTreeMap::new();
        let mut round_backend_telemetry = BTreeMap::new();
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
                    Some(&mut round_backend_telemetry),
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
                default(wait_for) => {}
            }

            if topology_changed
                && !shutdown.load(Ordering::Relaxed)
                && Instant::now() < stop_at
                && !backends.is_empty()
            {
                let reservation = scheduler.reserve(total_lanes(backends));
                warn(
                    "BENCH",
                    format!(
                        "topology change; redistributing e={} id={} backends={}",
                        epoch,
                        work_id,
                        backend_names(backends),
                    ),
                );
                let additional_span = distribute_work(
                    backends,
                    super::DistributeWorkOptions {
                        epoch,
                        work_id,
                        header_base: std::sync::Arc::clone(&header_base),
                        target: impossible_target,
                        reservation,
                        stop_at,
                        backend_weights: None,
                    },
                )?;
                scheduler.consume_additional_span(additional_span);
                next_hash_poll_at = Instant::now();
                topology_changed = false;
            }
        }

        collect_backend_hashes(
            backends,
            None,
            &mut round_hashes,
            Some(&mut round_backend_hashes),
            Some(&mut round_backend_telemetry),
        );

        let counted_hashes = round_hashes;
        let counted_until = std::cmp::min(Instant::now(), stop_at);
        let counted_elapsed = counted_until
            .saturating_duration_since(round_start)
            .as_secs_f64()
            .max(0.001);
        let fence_start = Instant::now();
        quiesce_backend_slots(backends)?;
        let fence_elapsed = fence_start.elapsed().as_secs_f64();
        let mut late_hashes = 0u64;
        let mut late_backend_hashes = BTreeMap::new();
        let mut late_backend_telemetry = BTreeMap::new();
        collect_backend_hashes(
            backends,
            None,
            &mut late_hashes,
            Some(&mut late_backend_hashes),
            Some(&mut late_backend_telemetry),
        );
        drain_benchmark_backend_events(backend_events, epoch, backends)?;

        for (backend_id, hashes) in late_backend_hashes {
            let entry = round_backend_hashes.entry(backend_id).or_insert(0);
            *entry = entry.saturating_add(hashes);
        }
        for (backend_id, telemetry) in late_backend_telemetry {
            merge_round_telemetry(&mut round_backend_telemetry, backend_id, telemetry);
        }

        let round_hashes = counted_hashes.saturating_add(late_hashes);
        let measured_elapsed = (counted_elapsed + fence_elapsed).max(0.001);
        let hps = round_hashes as f64 / measured_elapsed;
        let backend_runs = build_backend_round_stats(
            backends,
            &round_backend_hashes,
            &round_backend_telemetry,
            measured_elapsed,
        );

        info(
            "BENCH",
            format!(
                "round {}/{} hashes={} counted={} late={} window={:.2}s fence={:.3}s rate={} backends={}",
                round + 1,
                cfg.bench_rounds,
                round_hashes,
                counted_hashes,
                late_hashes,
                counted_elapsed,
                fence_elapsed,
                format_hashrate(hps),
                format_round_backend_hashrate(backends, &round_backend_hashes, measured_elapsed),
            ),
        );
        let telemetry_line = format_round_backend_telemetry(backends, &round_backend_telemetry);
        if telemetry_line != "none" {
            info("BENCH", format!("telemetry | {telemetry_line}"));
        }

        runs.push(BenchRun {
            round: round + 1,
            hashes: round_hashes,
            counted_hashes,
            late_hashes,
            elapsed_secs: measured_elapsed,
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
            preemption: backends
                .iter()
                .map(|slot| {
                    format!(
                        "{}#{}={}",
                        slot.backend.name(),
                        slot.id,
                        slot.backend.preemption_granularity().describe()
                    )
                })
                .collect(),
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
        warn("BENCH", "aborted before first round");
        return Ok(());
    }

    let mut sorted_hps: Vec<f64> = report.runs.iter().map(|r| r.hps).collect();
    sorted_hps.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    report.avg_hps = sorted_hps.iter().sum::<f64>() / sorted_hps.len() as f64;
    report.median_hps = median(&sorted_hps);
    report.min_hps = *sorted_hps.first().unwrap_or(&0.0);
    report.max_hps = *sorted_hps.last().unwrap_or(&0.0);

    success(
        "BENCH",
        format!(
            "summary | avg={} | median={} | min={} | max={}",
            format_hashrate(report.avg_hps),
            format_hashrate(report.median_hps),
            format_hashrate(report.min_hps),
            format_hashrate(report.max_hps),
        ),
    );

    let mut baseline_delta_pct = None;
    if let Some(path) = &cfg.bench_baseline {
        let baseline_text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read baseline file {}", path.display()))?;
        let baseline: BenchReport = serde_json::from_str(&baseline_text)
            .with_context(|| format!("failed to parse baseline JSON {}", path.display()))?;
        let compatibility_issues = baseline_compatibility_issues(&report, &baseline);
        if !compatibility_issues.is_empty() {
            let message = format!(
                "baseline is not comparable ({})",
                compatibility_issues.join("; ")
            );
            if cfg.bench_fail_below_pct.is_some() {
                bail!("{message}");
            }
            warn("BENCH", message);
        } else if baseline.avg_hps > 0.0 {
            let delta_pct = ((report.avg_hps - baseline.avg_hps) / baseline.avg_hps) * 100.0;
            baseline_delta_pct = Some(delta_pct);
            info(
                "BENCH",
                format!(
                    "baseline compare | baseline_avg={} | delta={:+.2}%",
                    format_hashrate(baseline.avg_hps),
                    delta_pct
                ),
            );
        } else if cfg.bench_fail_below_pct.is_some() {
            bail!("baseline avg_hps must be > 0 for regression gating");
        }
    }

    if let Some(path) = &cfg.bench_output {
        let json = serde_json::to_string_pretty(&report)
            .context("failed to serialize benchmark report")?;
        std::fs::write(path, json)
            .with_context(|| format!("failed to write benchmark report {}", path.display()))?;
        success("BENCH", format!("wrote report to {}", path.display()));
    }

    if let Some(threshold_pct) = cfg.bench_fail_below_pct {
        let delta_pct = baseline_delta_pct.ok_or_else(|| {
            anyhow!("--bench-fail-below-pct requires a baseline report with avg_hps > 0")
        })?;
        if delta_pct < -threshold_pct {
            bail!(
                "benchmark regression gate failed: delta={:+.2}% is below allowed -{:.2}%",
                delta_pct,
                threshold_pct
            );
        }
        success(
            "BENCH",
            format!(
                "regression gate passed | delta={:+.2}% | threshold=-{:.2}%",
                delta_pct, threshold_pct
            ),
        );
    }

    Ok(())
}

fn baseline_compatibility_issues(current: &BenchReport, baseline: &BenchReport) -> Vec<String> {
    let mut issues = Vec::new();

    if baseline.bench_kind != current.bench_kind {
        issues.push(format!(
            "kind mismatch baseline={} current={}",
            baseline.bench_kind, current.bench_kind
        ));
    }
    if baseline.backends != current.backends {
        issues.push(format!(
            "backend mismatch baseline={} current={}",
            baseline.backends.join(","),
            current.backends.join(",")
        ));
    }
    if baseline.preemption != current.preemption {
        issues.push(format!(
            "preemption mismatch baseline={} current={}",
            baseline.preemption.join(","),
            current.preemption.join(",")
        ));
    }
    if baseline.total_lanes != current.total_lanes {
        issues.push(format!(
            "lanes mismatch baseline={} current={}",
            baseline.total_lanes, current.total_lanes
        ));
    }
    if baseline.cpu_threads != current.cpu_threads {
        issues.push(format!(
            "cpu_threads mismatch baseline={} current={}",
            baseline.cpu_threads, current.cpu_threads
        ));
    }
    if baseline.bench_secs != current.bench_secs {
        issues.push(format!(
            "bench_secs mismatch baseline={} current={}",
            baseline.bench_secs, current.bench_secs
        ));
    }

    if !baseline.environment.target_triple.is_empty()
        && !current.environment.target_triple.is_empty()
        && baseline.environment.target_triple != current.environment.target_triple
    {
        issues.push(format!(
            "target mismatch baseline={} current={}",
            baseline.environment.target_triple, current.environment.target_triple
        ));
    }
    if baseline.environment.cpu_brand.is_some()
        && current.environment.cpu_brand.is_some()
        && baseline.environment.cpu_brand != current.environment.cpu_brand
    {
        issues.push(format!(
            "cpu mismatch baseline={} current={}",
            baseline
                .environment
                .cpu_brand
                .as_deref()
                .unwrap_or("unknown"),
            current
                .environment
                .cpu_brand
                .as_deref()
                .unwrap_or("unknown")
        ));
    }

    issues
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
        seine_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: std::env::var("SEINE_GIT_COMMIT")
            .ok()
            .or_else(|| std::env::var("BNMINER_GIT_COMMIT").ok())
            .or_else(|| option_env!("SEINE_GIT_COMMIT").map(str::to_string))
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

fn merge_round_telemetry(
    round_backend_telemetry: &mut BTreeMap<BackendInstanceId, BackendRoundTelemetry>,
    backend_id: BackendInstanceId,
    telemetry: BackendRoundTelemetry,
) {
    let entry = round_backend_telemetry.entry(backend_id).or_default();
    entry.dropped_events = entry
        .dropped_events
        .saturating_add(telemetry.dropped_events);
    entry.completed_assignments = entry
        .completed_assignments
        .saturating_add(telemetry.completed_assignments);
    entry.completed_assignment_hashes = entry
        .completed_assignment_hashes
        .saturating_add(telemetry.completed_assignment_hashes);
    entry.completed_assignment_micros = entry
        .completed_assignment_micros
        .saturating_add(telemetry.completed_assignment_micros);
    entry.peak_active_lanes = entry.peak_active_lanes.max(telemetry.peak_active_lanes);
    entry.peak_pending_work = entry.peak_pending_work.max(telemetry.peak_pending_work);
}

fn build_backend_round_stats(
    backends: &[BackendSlot],
    round_backend_hashes: &BTreeMap<BackendInstanceId, u64>,
    round_backend_telemetry: &BTreeMap<BackendInstanceId, BackendRoundTelemetry>,
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
        let telemetry = round_backend_telemetry
            .get(backend_id)
            .copied()
            .unwrap_or_default();
        runs.push(BenchBackendRun {
            backend_id: *backend_id,
            backend,
            hashes: *hashes,
            hps: *hashes as f64 / elapsed_secs,
            peak_active_lanes: telemetry.peak_active_lanes,
            peak_pending_work: telemetry.peak_pending_work,
            dropped_events: telemetry.dropped_events,
            completed_assignments: telemetry.completed_assignments,
            completed_assignment_hashes: telemetry.completed_assignment_hashes,
            completed_assignment_secs: telemetry.completed_assignment_micros as f64 / 1_000_000.0,
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

    fn sample_report() -> BenchReport {
        BenchReport {
            environment: BenchEnvironment {
                target_triple: "linux/x86_64".to_string(),
                cpu_brand: Some("test-cpu".to_string()),
                ..BenchEnvironment::default()
            },
            bench_kind: "backend".to_string(),
            backends: vec!["cpu#1".to_string()],
            preemption: vec!["cpu#1=per-hash".to_string()],
            total_lanes: 1,
            cpu_threads: 1,
            bench_secs: 10,
            rounds: 1,
            avg_hps: 10.0,
            median_hps: 10.0,
            min_hps: 10.0,
            max_hps: 10.0,
            runs: vec![BenchRun {
                round: 1,
                hashes: 10,
                counted_hashes: 10,
                late_hashes: 0,
                elapsed_secs: 1.0,
                fence_secs: 0.0,
                hps: 10.0,
                backend_runs: Vec::new(),
            }],
        }
    }

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

    #[test]
    fn baseline_compatibility_detects_mismatched_kind() {
        let current = sample_report();
        let mut baseline = sample_report();
        baseline.bench_kind = "kernel".to_string();

        let issues = baseline_compatibility_issues(&current, &baseline);
        assert!(!issues.is_empty());
        assert!(issues.iter().any(|issue| issue.contains("kind mismatch")));
    }
}
