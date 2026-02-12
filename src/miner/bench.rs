use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use blocknet_pow_spec::{
    POW_HEADER_BASE_LEN, POW_ITERATIONS, POW_MEMORY_KB, POW_OUTPUT_LEN, POW_PARALLELISM,
};
use crossbeam_channel::Receiver;
use serde::{Deserialize, Serialize};
use sysinfo::System;

use crate::backend::{BackendEvent, BackendInstanceId, DeadlineSupport, PowBackend};
use crate::config::{BenchBaselinePolicy, BenchKind, Config, CpuAffinityMode, WorkAllocation};

use super::runtime::{
    seed_backend_weights, update_backend_weights, work_distribution_weights, RoundEndReason,
    WeightUpdateInputs,
};
use super::scheduler::NonceScheduler;
use super::stats::{format_hashrate, median};
use super::ui::{info, startup_banner, success, warn};
use super::{
    activate_backends, collect_backend_hashes, distribute_work, format_round_backend_telemetry,
    next_work_id, quiesce_backend_slots, start_backend_slots, stop_backend_slots, total_lanes,
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
    peak_inflight_assignment_hashes: u64,
    #[serde(default)]
    peak_inflight_assignment_secs: f64,
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
    schema_version: u32,
    #[serde(default)]
    environment: BenchEnvironment,
    #[serde(default)]
    config_fingerprint: BenchConfigFingerprint,
    #[serde(default)]
    pow_fingerprint: BenchPowFingerprint,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
struct BenchConfigFingerprint {
    backend_event_capacity: usize,
    hash_poll_ms: u64,
    backend_assign_timeout_ms: u64,
    backend_control_timeout_ms: u64,
    allow_best_effort_deadlines: bool,
    prefetch_wait_ms: u64,
    tip_listener_join_wait_ms: u64,
    strict_round_accounting: bool,
    refresh_secs: u64,
    nonce_iters_per_lane: u64,
    start_nonce: u64,
    work_allocation: String,
    cpu_affinity: String,
    events_idle_timeout_secs: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
struct BenchPowFingerprint {
    memory_kb: u32,
    iterations: u32,
    parallelism: u32,
    output_len: usize,
    header_base_len: usize,
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

#[derive(Debug, Clone)]
struct WorkerBenchmarkIdentity {
    backend_ids: BTreeSet<BackendInstanceId>,
    backend_lanes: BTreeMap<BackendInstanceId, u64>,
    backends: Vec<String>,
    preemption: Vec<String>,
    total_lanes: u64,
}

type BackendEventAction = RuntimeBackendEventAction;
const BENCH_REPORT_SCHEMA_VERSION: u32 = 2;
type BackendPollState = BTreeMap<BackendInstanceId, (Duration, Instant)>;

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

fn collect_due_backend_hashes(
    backends: &[BackendSlot],
    configured: Duration,
    poll_state: &mut BackendPollState,
    round_hashes: &mut u64,
    round_backend_hashes: &mut BTreeMap<BackendInstanceId, u64>,
    round_backend_telemetry: &mut BTreeMap<BackendInstanceId, BackendRoundTelemetry>,
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
        merge_round_telemetry(
            round_backend_telemetry,
            slot.id,
            BackendRoundTelemetry {
                dropped_events: telemetry.dropped_events,
                completed_assignments: telemetry.completed_assignments,
                completed_assignment_hashes: telemetry.completed_assignment_hashes,
                completed_assignment_micros: telemetry.completed_assignment_micros,
                peak_active_lanes: telemetry.active_lanes,
                peak_pending_work: telemetry.pending_work,
                peak_inflight_assignment_hashes: telemetry.inflight_assignment_hashes,
                peak_inflight_assignment_micros: telemetry.inflight_assignment_micros,
            },
        );

        if slot_hashes > 0 {
            collected = collected.saturating_add(slot_hashes);
            let entry = round_backend_hashes.entry(slot.id).or_insert(0);
            *entry = entry.saturating_add(slot_hashes);
        }

        *next_poll = now + *interval;
    }

    *round_hashes = round_hashes.saturating_add(collected);
}

fn next_backend_poll_deadline(
    poll_state: &BackendPollState,
) -> Instant {
    poll_state
        .values()
        .map(|(_, next_poll)| *next_poll)
        .min()
        .unwrap_or_else(Instant::now)
}

pub(super) fn run_benchmark(cfg: &Config, shutdown: &AtomicBool) -> Result<()> {
    let instances = super::build_backend_instances(cfg);
    let backend_executor = super::backend_executor::BackendExecutor::new();

    match cfg.bench_kind {
        BenchKind::Kernel => run_kernel_benchmark(cfg, shutdown, instances),
        BenchKind::Backend => {
            run_worker_benchmark(cfg, shutdown, instances, false, &backend_executor)
        }
        BenchKind::EndToEnd => {
            run_worker_benchmark(cfg, shutdown, instances, true, &backend_executor)
        }
    }
}

fn run_kernel_benchmark(
    cfg: &Config,
    shutdown: &AtomicBool,
    backends: Vec<Arc<dyn PowBackend>>,
) -> Result<()> {
    let mut iter = backends.into_iter();
    let backend = iter
        .next()
        .ok_or_else(|| anyhow!("kernel benchmark requires at least one backend"))?;

    if iter.next().is_some() {
        bail!("kernel benchmark requires exactly one backend");
    }

    if !cfg.allow_best_effort_deadlines
        && backend.capabilities().deadline_support == DeadlineSupport::BestEffort
    {
        bail!(
            "backend {} reports best-effort deadlines; pass --allow-best-effort-deadlines to run kernel benchmark anyway",
            backend.name()
        );
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
        (
            "Baseline Policy",
            match cfg.bench_baseline_policy {
                BenchBaselinePolicy::Strict => "strict".to_string(),
                BenchBaselinePolicy::IgnoreEnvironment => "ignore-environment".to_string(),
            },
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
            schema_version: BENCH_REPORT_SCHEMA_VERSION,
            environment,
            config_fingerprint: benchmark_config_fingerprint(cfg),
            pow_fingerprint: benchmark_pow_fingerprint(),
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

fn worker_benchmark_identity(backends: &[BackendSlot]) -> WorkerBenchmarkIdentity {
    WorkerBenchmarkIdentity {
        backend_ids: backends.iter().map(|slot| slot.id).collect(),
        backend_lanes: backends.iter().map(|slot| (slot.id, slot.lanes)).collect(),
        backends: backends
            .iter()
            .map(|slot| format!("{}#{}", slot.backend.name(), slot.id))
            .collect(),
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
    }
}

fn ensure_worker_topology_identity(
    backends: &[BackendSlot],
    identity: &WorkerBenchmarkIdentity,
    context: &str,
) -> Result<()> {
    let current_ids = backends.iter().map(|slot| slot.id).collect::<BTreeSet<_>>();
    let current_lanes = backends
        .iter()
        .map(|slot| (slot.id, slot.lanes))
        .collect::<BTreeMap<_, _>>();
    let current_backends = backends
        .iter()
        .map(|slot| format!("{}#{}", slot.backend.name(), slot.id))
        .collect::<Vec<_>>();
    let current_preemption = backends
        .iter()
        .map(|slot| {
            format!(
                "{}#{}={}",
                slot.backend.name(),
                slot.id,
                slot.backend.preemption_granularity().describe()
            )
        })
        .collect::<Vec<_>>();

    if current_ids == identity.backend_ids
        && current_lanes == identity.backend_lanes
        && current_backends == identity.backends
        && current_preemption == identity.preemption
    {
        return Ok(());
    }
    bail!(
        "benchmark aborted: backend topology changed during {context} (expected_backends={} current_backends={} expected_lanes={:?} current_lanes={:?} expected_preemption={} current_preemption={})",
        identity.backends.join(","),
        current_backends.join(","),
        identity.backend_lanes,
        current_lanes,
        identity.preemption.join(","),
        current_preemption.join(",")
    )
}

fn run_worker_benchmark(
    cfg: &Config,
    shutdown: &AtomicBool,
    instances: Vec<Arc<dyn PowBackend>>,
    restart_each_round: bool,
    backend_executor: &super::backend_executor::BackendExecutor,
) -> Result<()> {
    let (mut backends, backend_events) = activate_backends(instances, cfg.backend_event_capacity)?;
    super::enforce_deadline_policy(
        &mut backends,
        cfg.allow_best_effort_deadlines,
        backend_executor,
    )?;
    let identity = worker_benchmark_identity(&backends);
    let bench_kind = if restart_each_round {
        "end_to_end"
    } else {
        "backend"
    };
    let effective_hash_poll =
        super::effective_hash_poll_interval(&backends, cfg.hash_poll_interval);

    let lines = vec![
        ("Mode", "benchmark".to_string()),
        ("Kind", bench_kind.to_string()),
        ("Backends", identity.backends.join(",")),
        ("Preemption", identity.preemption.join(", ")),
        ("Lanes", identity.total_lanes.to_string()),
        ("Rounds", cfg.bench_rounds.to_string()),
        ("Seconds/Round", cfg.bench_secs.to_string()),
        (
            "Hash Poll",
            format!(
                "configured={}ms effective={}ms",
                cfg.hash_poll_interval.as_millis(),
                effective_hash_poll.as_millis()
            ),
        ),
        (
            "Assign Timeout",
            format!("{}ms", cfg.backend_assign_timeout.as_millis()),
        ),
        (
            "Control Timeout",
            format!("{}ms", cfg.backend_control_timeout.as_millis()),
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
        (
            "Baseline Policy",
            match cfg.bench_baseline_policy {
                BenchBaselinePolicy::Strict => "strict".to_string(),
                BenchBaselinePolicy::IgnoreEnvironment => "ignore-environment".to_string(),
            },
        ),
    ];
    startup_banner(&lines);

    if restart_each_round {
        stop_backend_slots(&mut backends, backend_executor);
    }

    let result = run_worker_benchmark_inner(
        cfg,
        shutdown,
        &mut backends,
        &backend_events,
        restart_each_round,
        &identity,
        backend_executor,
    );
    stop_backend_slots(&mut backends, backend_executor);
    result
}

fn run_worker_benchmark_inner(
    cfg: &Config,
    shutdown: &AtomicBool,
    backends: &mut Vec<BackendSlot>,
    backend_events: &Receiver<BackendEvent>,
    restart_each_round: bool,
    identity: &WorkerBenchmarkIdentity,
    backend_executor: &super::backend_executor::BackendExecutor,
) -> Result<()> {
    let impossible_target = [0u8; 32];
    let mut runs = Vec::with_capacity(cfg.bench_rounds as usize);
    let environment = benchmark_environment();
    let mut epoch = 0u64;
    let mut work_id_cursor = 1u64;
    let mut scheduler = NonceScheduler::new(cfg.start_nonce, cfg.nonce_iters_per_lane);
    let mut backend_weights = seed_backend_weights(backends);
    ensure_worker_topology_identity(backends, identity, "benchmark setup")?;

    for round in 0..cfg.bench_rounds {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        if backends.is_empty() {
            bail!("all benchmark backends are unavailable");
        }

        if restart_each_round {
            start_backend_slots(backends)?;
            ensure_worker_topology_identity(backends, identity, "backend restart")?;
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
                assignment_timeout: cfg.backend_assign_timeout,
                backend_weights: work_distribution_weights(cfg.work_allocation, &backend_weights),
            },
            backend_executor,
        )?;
        scheduler.consume_additional_span(additional_span);

        let mut round_hashes = 0u64;
        let mut round_backend_hashes = BTreeMap::new();
        let mut round_backend_telemetry = BTreeMap::new();
        let mut backend_poll_state = build_backend_poll_state(backends, cfg.hash_poll_interval);
        while Instant::now() < stop_at && !shutdown.load(Ordering::Relaxed) {
            collect_due_backend_hashes(
                backends,
                cfg.hash_poll_interval,
                &mut backend_poll_state,
                &mut round_hashes,
                &mut round_backend_hashes,
                &mut round_backend_telemetry,
            );
            let now = Instant::now();
            let next_hash_poll_at = next_backend_poll_deadline(&backend_poll_state);
            let wait_for = stop_at
                .saturating_duration_since(now)
                .min(next_hash_poll_at.saturating_duration_since(now))
                .max(MIN_EVENT_WAIT);

            crossbeam_channel::select! {
                recv(backend_events) -> event => {
                    let event = event.map_err(|_| anyhow!("backend event channel closed"))?;
                    if handle_benchmark_backend_event(event, epoch, backends, backend_executor)?
                        == BackendEventAction::TopologyChanged
                    {
                        ensure_worker_topology_identity(
                            backends,
                            identity,
                            &format!("round {}", round + 1),
                        )?;
                    }
                }
                default(wait_for) => {}
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
        if quiesce_backend_slots(
            backends,
            RuntimeMode::Bench,
            cfg.backend_control_timeout,
            backend_executor,
        )?
            == BackendEventAction::TopologyChanged
        {
            ensure_worker_topology_identity(
                backends,
                identity,
                &format!("round {} fence", round + 1),
            )?;
        }
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
        if drain_benchmark_backend_events(backend_events, epoch, backends, backend_executor)?
            == BackendEventAction::TopologyChanged
        {
            ensure_worker_topology_identity(
                backends,
                identity,
                &format!("round {} event drain", round + 1),
            )?;
        }

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
                format_bench_backend_hashrate(backends, &round_backend_hashes, measured_elapsed),
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

        let round_end_reason = if shutdown.load(Ordering::Relaxed) {
            RoundEndReason::Shutdown
        } else {
            RoundEndReason::Refresh
        };
        update_backend_weights(
            &mut backend_weights,
            WeightUpdateInputs {
                backends,
                round_backend_hashes: &round_backend_hashes,
                round_backend_telemetry: Some(&round_backend_telemetry),
                round_elapsed_secs: measured_elapsed,
                mode: cfg.work_allocation,
                round_end_reason,
                refresh_interval: cfg.refresh_interval,
            },
        );

        if restart_each_round {
            stop_backend_slots(backends, backend_executor);
        }
    }

    summarize_benchmark(
        cfg,
        BenchReport {
            schema_version: BENCH_REPORT_SCHEMA_VERSION,
            environment,
            config_fingerprint: benchmark_config_fingerprint(cfg),
            pow_fingerprint: benchmark_pow_fingerprint(),
            bench_kind: if restart_each_round {
                "end_to_end".to_string()
            } else {
                "backend".to_string()
            },
            backends: identity.backends.clone(),
            preemption: identity.preemption.clone(),
            total_lanes: identity.total_lanes,
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
        let compatibility_issues =
            baseline_compatibility_issues(&report, &baseline, cfg.bench_baseline_policy);
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

fn baseline_compatibility_issues(
    current: &BenchReport,
    baseline: &BenchReport,
    policy: BenchBaselinePolicy,
) -> Vec<String> {
    let mut issues = Vec::new();

    if baseline.schema_version != current.schema_version {
        issues.push(format!(
            "schema mismatch baseline={} current={}",
            baseline.schema_version, current.schema_version
        ));
    }

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

    if baseline.schema_version >= BENCH_REPORT_SCHEMA_VERSION
        && current.schema_version >= BENCH_REPORT_SCHEMA_VERSION
    {
        if baseline.config_fingerprint.backend_event_capacity
            != current.config_fingerprint.backend_event_capacity
        {
            issues.push(format!(
                "backend_event_capacity mismatch baseline={} current={}",
                baseline.config_fingerprint.backend_event_capacity,
                current.config_fingerprint.backend_event_capacity
            ));
        }
        if baseline.config_fingerprint.hash_poll_ms != current.config_fingerprint.hash_poll_ms {
            issues.push(format!(
                "hash_poll_ms mismatch baseline={} current={}",
                baseline.config_fingerprint.hash_poll_ms, current.config_fingerprint.hash_poll_ms
            ));
        }
        if baseline.config_fingerprint.backend_assign_timeout_ms
            != current.config_fingerprint.backend_assign_timeout_ms
        {
            issues.push(format!(
                "backend_assign_timeout_ms mismatch baseline={} current={}",
                baseline.config_fingerprint.backend_assign_timeout_ms,
                current.config_fingerprint.backend_assign_timeout_ms
            ));
        }
        if baseline.config_fingerprint.backend_control_timeout_ms
            != current.config_fingerprint.backend_control_timeout_ms
        {
            issues.push(format!(
                "backend_control_timeout_ms mismatch baseline={} current={}",
                baseline.config_fingerprint.backend_control_timeout_ms,
                current.config_fingerprint.backend_control_timeout_ms
            ));
        }
        if baseline.config_fingerprint.allow_best_effort_deadlines
            != current.config_fingerprint.allow_best_effort_deadlines
        {
            issues.push(format!(
                "allow_best_effort_deadlines mismatch baseline={} current={}",
                baseline.config_fingerprint.allow_best_effort_deadlines,
                current.config_fingerprint.allow_best_effort_deadlines
            ));
        }
        if baseline.config_fingerprint.prefetch_wait_ms
            != current.config_fingerprint.prefetch_wait_ms
        {
            issues.push(format!(
                "prefetch_wait_ms mismatch baseline={} current={}",
                baseline.config_fingerprint.prefetch_wait_ms,
                current.config_fingerprint.prefetch_wait_ms
            ));
        }
        if baseline.config_fingerprint.tip_listener_join_wait_ms
            != current.config_fingerprint.tip_listener_join_wait_ms
        {
            issues.push(format!(
                "tip_listener_join_wait_ms mismatch baseline={} current={}",
                baseline.config_fingerprint.tip_listener_join_wait_ms,
                current.config_fingerprint.tip_listener_join_wait_ms
            ));
        }
        if baseline.config_fingerprint.strict_round_accounting
            != current.config_fingerprint.strict_round_accounting
        {
            issues.push(format!(
                "strict_round_accounting mismatch baseline={} current={}",
                baseline.config_fingerprint.strict_round_accounting,
                current.config_fingerprint.strict_round_accounting
            ));
        }
        if baseline.config_fingerprint.refresh_secs != current.config_fingerprint.refresh_secs {
            issues.push(format!(
                "refresh_secs mismatch baseline={} current={}",
                baseline.config_fingerprint.refresh_secs, current.config_fingerprint.refresh_secs
            ));
        }
        if baseline.config_fingerprint.nonce_iters_per_lane
            != current.config_fingerprint.nonce_iters_per_lane
        {
            issues.push(format!(
                "nonce_iters_per_lane mismatch baseline={} current={}",
                baseline.config_fingerprint.nonce_iters_per_lane,
                current.config_fingerprint.nonce_iters_per_lane
            ));
        }
        if baseline.config_fingerprint.start_nonce != current.config_fingerprint.start_nonce {
            issues.push(format!(
                "start_nonce mismatch baseline={} current={}",
                baseline.config_fingerprint.start_nonce, current.config_fingerprint.start_nonce
            ));
        }
        if baseline.config_fingerprint.work_allocation != current.config_fingerprint.work_allocation
        {
            issues.push(format!(
                "work_allocation mismatch baseline={} current={}",
                baseline.config_fingerprint.work_allocation,
                current.config_fingerprint.work_allocation
            ));
        }
        if baseline.config_fingerprint.cpu_affinity != current.config_fingerprint.cpu_affinity {
            issues.push(format!(
                "cpu_affinity mismatch baseline={} current={}",
                baseline.config_fingerprint.cpu_affinity, current.config_fingerprint.cpu_affinity
            ));
        }
        if baseline.config_fingerprint.events_idle_timeout_secs
            != current.config_fingerprint.events_idle_timeout_secs
        {
            issues.push(format!(
                "events_idle_timeout_secs mismatch baseline={} current={}",
                baseline.config_fingerprint.events_idle_timeout_secs,
                current.config_fingerprint.events_idle_timeout_secs
            ));
        }
        if baseline.pow_fingerprint != current.pow_fingerprint {
            issues.push("pow parameter fingerprint mismatch".to_string());
        }
    } else {
        issues.push("baseline benchmark report schema is too old".to_string());
    }

    if policy == BenchBaselinePolicy::Strict {
        if !baseline.environment.seine_version.is_empty()
            && !current.environment.seine_version.is_empty()
            && baseline.environment.seine_version != current.environment.seine_version
        {
            issues.push(format!(
                "version mismatch baseline={} current={}",
                baseline.environment.seine_version, current.environment.seine_version
            ));
        }
        if baseline.environment.git_commit.is_some()
            && current.environment.git_commit.is_some()
            && baseline.environment.git_commit != current.environment.git_commit
        {
            issues.push(format!(
                "git mismatch baseline={} current={}",
                baseline
                    .environment
                    .git_commit
                    .as_deref()
                    .unwrap_or("unknown"),
                current
                    .environment
                    .git_commit
                    .as_deref()
                    .unwrap_or("unknown")
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
        if baseline.environment.cpu_arch.is_some()
            && current.environment.cpu_arch.is_some()
            && baseline.environment.cpu_arch != current.environment.cpu_arch
        {
            issues.push(format!(
                "cpu_arch mismatch baseline={} current={}",
                baseline
                    .environment
                    .cpu_arch
                    .as_deref()
                    .unwrap_or("unknown"),
                current.environment.cpu_arch.as_deref().unwrap_or("unknown")
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
        if baseline.environment.logical_cores > 0
            && current.environment.logical_cores > 0
            && baseline.environment.logical_cores != current.environment.logical_cores
        {
            issues.push(format!(
                "logical_cores mismatch baseline={} current={}",
                baseline.environment.logical_cores, current.environment.logical_cores
            ));
        }
        if baseline.environment.physical_cores.is_some()
            && current.environment.physical_cores.is_some()
            && baseline.environment.physical_cores != current.environment.physical_cores
        {
            issues.push(format!(
                "physical_cores mismatch baseline={} current={}",
                baseline.environment.physical_cores.unwrap_or(0),
                current.environment.physical_cores.unwrap_or(0)
            ));
        }
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

fn benchmark_config_fingerprint(cfg: &Config) -> BenchConfigFingerprint {
    BenchConfigFingerprint {
        backend_event_capacity: cfg.backend_event_capacity,
        hash_poll_ms: cfg.hash_poll_interval.as_millis() as u64,
        backend_assign_timeout_ms: cfg.backend_assign_timeout.as_millis() as u64,
        backend_control_timeout_ms: cfg.backend_control_timeout.as_millis() as u64,
        allow_best_effort_deadlines: cfg.allow_best_effort_deadlines,
        prefetch_wait_ms: cfg.prefetch_wait.as_millis() as u64,
        tip_listener_join_wait_ms: cfg.tip_listener_join_wait.as_millis() as u64,
        strict_round_accounting: cfg.strict_round_accounting,
        refresh_secs: cfg.refresh_interval.as_secs(),
        nonce_iters_per_lane: cfg.nonce_iters_per_lane,
        start_nonce: cfg.start_nonce,
        work_allocation: work_allocation_label(cfg.work_allocation).to_string(),
        cpu_affinity: cpu_affinity_label(cfg.cpu_affinity).to_string(),
        events_idle_timeout_secs: cfg.events_idle_timeout.as_secs(),
    }
}

fn benchmark_pow_fingerprint() -> BenchPowFingerprint {
    BenchPowFingerprint {
        memory_kb: POW_MEMORY_KB,
        iterations: POW_ITERATIONS,
        parallelism: POW_PARALLELISM,
        output_len: POW_OUTPUT_LEN,
        header_base_len: POW_HEADER_BASE_LEN,
    }
}

fn work_allocation_label(mode: WorkAllocation) -> &'static str {
    match mode {
        WorkAllocation::Static => "static",
        WorkAllocation::Adaptive => "adaptive",
    }
}

fn cpu_affinity_label(mode: CpuAffinityMode) -> &'static str {
    match mode {
        CpuAffinityMode::Off => "off",
        CpuAffinityMode::Auto => "auto",
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
    entry.peak_inflight_assignment_hashes = entry
        .peak_inflight_assignment_hashes
        .max(telemetry.peak_inflight_assignment_hashes);
    entry.peak_inflight_assignment_micros = entry
        .peak_inflight_assignment_micros
        .max(telemetry.peak_inflight_assignment_micros);
}

fn build_backend_round_stats(
    backends: &[BackendSlot],
    round_backend_hashes: &BTreeMap<BackendInstanceId, u64>,
    round_backend_telemetry: &BTreeMap<BackendInstanceId, BackendRoundTelemetry>,
    elapsed_secs: f64,
) -> Vec<BenchBackendRun> {
    let elapsed_secs = elapsed_secs.max(0.001);
    let mut runs = Vec::with_capacity(backends.len().max(round_backend_hashes.len()));
    let mut seen = BTreeSet::new();

    for slot in backends {
        let backend_id = slot.id;
        seen.insert(backend_id);
        let hashes = round_backend_hashes.get(&backend_id).copied().unwrap_or(0);
        let telemetry = round_backend_telemetry
            .get(&backend_id)
            .copied()
            .unwrap_or_default();
        runs.push(BenchBackendRun {
            backend_id,
            backend: slot.backend.name().to_string(),
            hashes,
            hps: hashes as f64 / elapsed_secs,
            peak_active_lanes: telemetry.peak_active_lanes,
            peak_pending_work: telemetry.peak_pending_work,
            peak_inflight_assignment_hashes: telemetry.peak_inflight_assignment_hashes,
            peak_inflight_assignment_secs: telemetry.peak_inflight_assignment_micros as f64
                / 1_000_000.0,
            dropped_events: telemetry.dropped_events,
            completed_assignments: telemetry.completed_assignments,
            completed_assignment_hashes: telemetry.completed_assignment_hashes,
            completed_assignment_secs: telemetry.completed_assignment_micros as f64 / 1_000_000.0,
        });
    }

    for (backend_id, hashes) in round_backend_hashes {
        if seen.contains(backend_id) {
            continue;
        }
        let backend = "unknown".to_string();
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
            peak_inflight_assignment_hashes: telemetry.peak_inflight_assignment_hashes,
            peak_inflight_assignment_secs: telemetry.peak_inflight_assignment_micros as f64
                / 1_000_000.0,
            dropped_events: telemetry.dropped_events,
            completed_assignments: telemetry.completed_assignments,
            completed_assignment_hashes: telemetry.completed_assignment_hashes,
            completed_assignment_secs: telemetry.completed_assignment_micros as f64 / 1_000_000.0,
        });
    }
    runs
}

fn format_bench_backend_hashrate(
    backends: &[BackendSlot],
    round_backend_hashes: &BTreeMap<BackendInstanceId, u64>,
    elapsed_secs: f64,
) -> String {
    if backends.is_empty() {
        return "none".to_string();
    }
    let elapsed_secs = elapsed_secs.max(0.001);
    backends
        .iter()
        .map(|slot| {
            let hashes = round_backend_hashes.get(&slot.id).copied().unwrap_or(0);
            let hps = hashes as f64 / elapsed_secs;
            format!(
                "{}#{}={}",
                slot.backend.name(),
                slot.id,
                format_hashrate(hps)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn handle_benchmark_backend_event(
    event: BackendEvent,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
) -> Result<BackendEventAction> {
    let (action, _) = super::handle_runtime_backend_event(
        event,
        epoch,
        backends,
        RuntimeMode::Bench,
        backend_executor,
    )?;
    Ok(action)
}

fn drain_benchmark_backend_events(
    backend_events: &Receiver<BackendEvent>,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
) -> Result<BackendEventAction> {
    let (action, _) = super::drain_runtime_backend_events(
        backend_events,
        epoch,
        backends,
        RuntimeMode::Bench,
        backend_executor,
    )?;
    Ok(action)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use anyhow::Result;
    use crossbeam_channel::Sender;

    use crate::backend::{BackendEvent, BackendInstanceId, PowBackend, WorkAssignment};

    struct NoopBackend;

    impl PowBackend for NoopBackend {
        fn name(&self) -> &'static str {
            "noop"
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

    fn sample_report() -> BenchReport {
        BenchReport {
            schema_version: BENCH_REPORT_SCHEMA_VERSION,
            environment: BenchEnvironment {
                seine_version: "0.1.0".to_string(),
                target_triple: "linux/x86_64".to_string(),
                cpu_brand: Some("test-cpu".to_string()),
                cpu_arch: Some("x86_64".to_string()),
                logical_cores: 8,
                physical_cores: Some(4),
                ..BenchEnvironment::default()
            },
            config_fingerprint: BenchConfigFingerprint {
                backend_event_capacity: 1024,
                hash_poll_ms: 200,
                backend_assign_timeout_ms: 1000,
                backend_control_timeout_ms: 60_000,
                allow_best_effort_deadlines: false,
                prefetch_wait_ms: 250,
                tip_listener_join_wait_ms: 250,
                strict_round_accounting: true,
                refresh_secs: 20,
                nonce_iters_per_lane: 1u64 << 36,
                start_nonce: 7,
                work_allocation: "adaptive".to_string(),
                cpu_affinity: "auto".to_string(),
                events_idle_timeout_secs: 90,
            },
            pow_fingerprint: BenchPowFingerprint {
                memory_kb: POW_MEMORY_KB,
                iterations: POW_ITERATIONS,
                parallelism: POW_PARALLELISM,
                output_len: POW_OUTPUT_LEN,
                header_base_len: POW_HEADER_BASE_LEN,
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
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
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
            &backend_executor,
        )
        .expect("stale benchmark solution event should be ignored");
        assert_eq!(action, BackendEventAction::None);
    }

    #[test]
    fn baseline_compatibility_detects_mismatched_kind() {
        let current = sample_report();
        let mut baseline = sample_report();
        baseline.bench_kind = "kernel".to_string();

        let issues =
            baseline_compatibility_issues(&current, &baseline, BenchBaselinePolicy::Strict);
        assert!(!issues.is_empty());
        assert!(issues.iter().any(|issue| issue.contains("kind mismatch")));
    }

    #[test]
    fn baseline_compatibility_detects_schema_mismatch() {
        let current = sample_report();
        let mut baseline = sample_report();
        baseline.schema_version = 1;

        let issues =
            baseline_compatibility_issues(&current, &baseline, BenchBaselinePolicy::Strict);
        assert!(issues.iter().any(|issue| issue.contains("schema mismatch")));
    }

    #[test]
    fn baseline_policy_can_ignore_environment_mismatch() {
        let current = sample_report();
        let mut baseline = sample_report();
        baseline.environment.git_commit = Some("a".to_string());
        let mut current_with_git = current.clone();
        current_with_git.environment.git_commit = Some("b".to_string());

        let strict_issues = baseline_compatibility_issues(
            &current_with_git,
            &baseline,
            BenchBaselinePolicy::Strict,
        );
        assert!(strict_issues
            .iter()
            .any(|issue| issue.contains("git mismatch")));

        let relaxed_issues = baseline_compatibility_issues(
            &current_with_git,
            &baseline,
            BenchBaselinePolicy::IgnoreEnvironment,
        );
        assert!(!relaxed_issues
            .iter()
            .any(|issue| issue.contains("git mismatch")));
    }

    #[test]
    fn backend_round_stats_include_zero_hash_backends() {
        let backends = vec![BackendSlot {
            id: 7,
            backend: Arc::new(NoopBackend),
            lanes: 1,
        }];
        let round_backend_hashes = BTreeMap::new();
        let round_backend_telemetry = BTreeMap::new();

        let runs = build_backend_round_stats(
            &backends,
            &round_backend_hashes,
            &round_backend_telemetry,
            1.0,
        );
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].backend_id, 7);
        assert_eq!(runs[0].backend, "noop");
        assert_eq!(runs[0].hashes, 0);
    }

    #[test]
    fn bench_hashrate_formatter_includes_zero_hash_backends() {
        let backends = vec![BackendSlot {
            id: 7,
            backend: Arc::new(NoopBackend),
            lanes: 1,
        }];
        let round_backend_hashes = BTreeMap::new();

        let rendered = format_bench_backend_hashrate(&backends, &round_backend_hashes, 1.0);
        assert!(rendered.contains("noop#7=0.000 H/s"), "{rendered}");
    }

    #[test]
    fn worker_topology_identity_tracks_initial_backend_set() {
        let backends = vec![
            BackendSlot {
                id: 2,
                backend: Arc::new(NoopBackend),
                lanes: 1,
            },
            BackendSlot {
                id: 9,
                backend: Arc::new(NoopBackend),
                lanes: 2,
            },
        ];

        let identity = worker_benchmark_identity(&backends);
        assert_eq!(
            identity.backends,
            vec!["noop#2".to_string(), "noop#9".to_string()]
        );
        assert_eq!(
            identity.preemption,
            vec!["noop#2=unknown", "noop#9=unknown"]
        );
        assert_eq!(identity.total_lanes, 3);
        assert_eq!(
            identity.backend_ids,
            [2u64, 9u64].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn topology_identity_validation_fails_when_backend_is_removed() {
        let expected = vec![
            BackendSlot {
                id: 2,
                backend: Arc::new(NoopBackend),
                lanes: 1,
            },
            BackendSlot {
                id: 9,
                backend: Arc::new(NoopBackend),
                lanes: 1,
            },
        ];
        let current = vec![BackendSlot {
            id: 2,
            backend: Arc::new(NoopBackend),
            lanes: 1,
        }];
        let identity = worker_benchmark_identity(&expected);

        let err = ensure_worker_topology_identity(&current, &identity, "round 1")
            .expect_err("topology mismatch should fail benchmark");
        assert!(format!("{err:#}").contains("topology changed"));
    }

    #[test]
    fn topology_identity_validation_fails_when_lane_shape_changes() {
        let expected = vec![BackendSlot {
            id: 2,
            backend: Arc::new(NoopBackend),
            lanes: 2,
        }];
        let current = vec![BackendSlot {
            id: 2,
            backend: Arc::new(NoopBackend),
            lanes: 1,
        }];
        let identity = worker_benchmark_identity(&expected);

        let err = ensure_worker_topology_identity(&current, &identity, "round 1")
            .expect_err("lane mismatch should fail benchmark");
        let rendered = format!("{err:#}");
        assert!(rendered.contains("expected_lanes"));
        assert!(rendered.contains("current_lanes"));
    }
}
