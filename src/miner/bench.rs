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

use crate::backend::{
    BackendEvent, BackendInstanceId, DeadlineSupport, KernelBenchSample, PowBackend,
};
use crate::config::{
    BenchBaselinePolicy, BenchKind, Config, CpuAffinityMode, CpuPerformanceProfile, WorkAllocation,
};

use super::hash_poll::build_backend_poll_state;
use super::runtime::{
    seed_backend_weights, update_backend_weights, work_distribution_weights, RoundEndReason,
    WeightUpdateInputs,
};
use super::scheduler::NonceScheduler;
use super::stats::{degraded_backends_warning, format_hashrate, median};
use super::ui::{info, startup_banner, success, warn};
use super::{
    activate_backends, collect_backend_hashes, distribute_work, format_round_backend_telemetry,
    next_work_id, quiesce_backend_slots, start_backend_slots, stop_backend_slots, total_lanes,
    BackendRoundTelemetry, BackendSlot, RuntimeBackendEventAction, RuntimeMode,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    memory_explicit_large_workers: u64,
    #[serde(default)]
    memory_explicit_large_1g_workers: u64,
    #[serde(default)]
    memory_transparent_huge_workers: u64,
    #[serde(default)]
    memory_regular_workers: u64,
    #[serde(default)]
    memory_heap_workers: u64,
    #[serde(default)]
    memory_explicit_large_bytes: u64,
    #[serde(default)]
    memory_explicit_large_1g_bytes: u64,
    #[serde(default)]
    memory_transparent_huge_bytes: u64,
    #[serde(default)]
    memory_regular_bytes: u64,
    #[serde(default)]
    memory_heap_bytes: u64,
    #[serde(default)]
    memory_allocation_failures: u64,
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
    #[serde(default)]
    assignment_enqueue_timeouts: u64,
    #[serde(default)]
    assignment_execution_timeouts: u64,
    #[serde(default)]
    control_enqueue_timeouts: u64,
    #[serde(default)]
    control_execution_timeouts: u64,
    #[serde(default)]
    peak_assignment_timeout_strikes: u32,
    #[serde(default)]
    assignment_enqueue_latency_samples: u64,
    #[serde(default)]
    assignment_enqueue_latency_p95_micros: u64,
    #[serde(default)]
    assignment_enqueue_latency_max_micros: u64,
    #[serde(default)]
    assignment_execution_latency_samples: u64,
    #[serde(default)]
    assignment_execution_latency_p95_micros: u64,
    #[serde(default)]
    assignment_execution_latency_max_micros: u64,
    #[serde(default)]
    control_enqueue_latency_samples: u64,
    #[serde(default)]
    control_enqueue_latency_p95_micros: u64,
    #[serde(default)]
    control_enqueue_latency_max_micros: u64,
    #[serde(default)]
    control_execution_latency_samples: u64,
    #[serde(default)]
    control_execution_latency_p95_micros: u64,
    #[serde(default)]
    control_execution_latency_max_micros: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchRun {
    round: u32,
    hashes: u64,
    #[serde(default)]
    counted_hashes: u64,
    #[serde(default)]
    late_hashes: u64,
    #[serde(default)]
    late_hash_pct: f64,
    /// Backward-compatible rate denominator. See `actual_elapsed_secs` and `wall_secs`.
    elapsed_secs: f64,
    #[serde(default)]
    configured_secs: f64,
    #[serde(default)]
    actual_elapsed_secs: f64,
    #[serde(default)]
    window_overrun_secs: f64,
    #[serde(default)]
    wall_secs: f64,
    #[serde(default)]
    startup_secs: f64,
    #[serde(default)]
    teardown_secs: f64,
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
    #[serde(default)]
    warmup_rounds: u32,
    avg_hps: f64,
    median_hps: f64,
    min_hps: f64,
    max_hps: f64,
    #[serde(default)]
    total_hashes: u64,
    #[serde(default)]
    total_counted_hashes: u64,
    #[serde(default)]
    total_late_hashes: u64,
    #[serde(default)]
    late_hash_pct: f64,
    runs: Vec<BenchRun>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
struct BenchBackendRuntimeFingerprint {
    backend_id: BackendInstanceId,
    backend: String,
    assign_timeout_ms: u64,
    assign_timeout_strikes: u32,
    control_timeout_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
struct BenchConfigFingerprint {
    backend_event_capacity: usize,
    hash_poll_ms: u64,
    cpu_profile: String,
    cpu_page_mode: String,
    cpu_hash_batch_size: u64,
    cpu_control_check_interval_hashes: u64,
    cpu_hash_flush_ms: u64,
    cpu_event_dispatch_capacity: usize,
    cpu_autotune_threads: bool,
    cpu_autotune_min_threads: usize,
    cpu_autotune_max_threads: Option<usize>,
    cpu_autotune_secs: u64,
    cpu_auto_threads_cap: usize,
    nvidia_autotune_secs: u64,
    nvidia_autotune_samples: u32,
    nvidia_max_rregcount: Option<u32>,
    nvidia_max_lanes: Option<usize>,
    nvidia_dispatch_iters_per_lane: Option<u64>,
    nvidia_allocation_iters_per_lane: Option<u64>,
    nvidia_hashes_per_launch_per_lane: u32,
    nvidia_hashes_per_launch_per_lane_was_set: bool,
    nvidia_fused_target_check: bool,
    nvidia_adaptive_launch_depth: bool,
    nvidia_enforce_template_stop: bool,
    backend_assign_timeout_ms: u64,
    backend_assign_timeout_strikes: u32,
    backend_control_timeout_ms: u64,
    bench_warmup_rounds: u32,
    allow_best_effort_deadlines: bool,
    prefetch_wait_ms: u64,
    tip_listener_join_wait_ms: u64,
    strict_round_accounting: bool,
    refresh_secs: u64,
    nonce_iters_per_lane: u64,
    start_nonce: u64,
    work_allocation: String,
    cpu_affinity: String,
    cpu_affinity_strategy: String,
    events_idle_timeout_secs: u64,
    backend_runtime: Vec<BenchBackendRuntimeFingerprint>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
struct BenchPowFingerprint {
    memory_kb: u32,
    iterations: u32,
    parallelism: u32,
    output_len: usize,
    header_base_len: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct BenchEnvironment {
    timestamp_unix_secs: u64,
    #[serde(alias = "bnminer_version")]
    seine_version: String,
    git_commit: Option<String>,
    target_triple: String,
    runtime_environment: String,
    wsl_distro: Option<String>,
    build_host: String,
    build_profile: String,
    build_opt_level: String,
    build_rustflags: String,
    build_features: String,
    rustc_version: String,
    source_fingerprint: String,
    build_fingerprint: String,
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
    initial_startup_secs: f64,
}

type BackendEventAction = RuntimeBackendEventAction;
const BENCH_REPORT_SCHEMA_VERSION: u32 = 12;
const BENCH_REPORT_COMPAT_MIN_SCHEMA_VERSION: u32 = 12;
const BENCH_SHORT_WINDOW_WARN_SECS: u64 = 10;
const BENCH_FENCE_JITTER_WARN_SECS: f64 = 0.250;
const BENCH_FENCE_JITTER_WARN_RATIO: f64 = 0.50;
const BENCH_CONTROL_LATENCY_WARN_MICROS: u64 = 300_000;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum KernelBenchMode {
    Steady,
    Effective,
}

#[derive(Debug, Clone, Copy)]
struct BenchTiming {
    configured_secs: f64,
    actual_elapsed_secs: f64,
    window_overrun_secs: f64,
    wall_secs: f64,
    rate_elapsed_secs: f64,
}

fn normalize_bench_timing(
    configured_secs: f64,
    actual_elapsed_secs: f64,
    wall_secs: f64,
    lifecycle_rate: bool,
) -> BenchTiming {
    let configured_secs = configured_secs.max(0.001);
    let actual_elapsed_secs = actual_elapsed_secs.max(0.001);
    let wall_secs = wall_secs.max(actual_elapsed_secs);
    BenchTiming {
        configured_secs,
        actual_elapsed_secs,
        window_overrun_secs: (actual_elapsed_secs - configured_secs).max(0.0),
        wall_secs,
        rate_elapsed_secs: if lifecycle_rate {
            wall_secs
        } else {
            actual_elapsed_secs
        },
    }
}

impl KernelBenchMode {
    fn kind_label(self) -> &'static str {
        match self {
            Self::Steady => "kernel",
            Self::Effective => "kernel-effective",
        }
    }

    fn report_kind_label(self) -> &'static str {
        match self {
            Self::Steady => "kernel",
            Self::Effective => "kernel_effective",
        }
    }

    fn measurement_label(self) -> &'static str {
        match self {
            Self::Steady => "steady window (bench-secs)",
            Self::Effective => "wall-time window + target/eval path",
        }
    }
}

pub(super) fn run_benchmark(cfg: &Config, shutdown: &AtomicBool) -> Result<()> {
    let runtime_cfg = super::prepare_runtime_config(cfg, shutdown, RuntimeMode::Bench)?;
    let cfg = &runtime_cfg;
    let instances = super::build_backend_instances(cfg);
    let backend_executor = super::backend_executor::BackendExecutor::new();

    match cfg.bench_kind {
        BenchKind::Kernel => run_kernel_benchmark(
            cfg,
            shutdown,
            instances.into_iter().map(|(_, backend)| backend).collect(),
            KernelBenchMode::Steady,
        ),
        BenchKind::KernelEffective => run_kernel_benchmark(
            cfg,
            shutdown,
            instances.into_iter().map(|(_, backend)| backend).collect(),
            KernelBenchMode::Effective,
        ),
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
    mode: KernelBenchMode,
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
        ("Kind", mode.kind_label().to_string()),
        ("Measurement", mode.measurement_label().to_string()),
        ("Backend", backend.name().to_string()),
        ("Preemption", backend.preemption_granularity().describe()),
        ("Rounds", cfg.bench_rounds.to_string()),
        ("Warmup Rounds", cfg.bench_warmup_rounds.to_string()),
        ("Seconds/Round", cfg.bench_secs.to_string()),
        ("CPU Page Mode", cfg.cpu_page_mode.as_str().to_string()),
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
    let total_rounds = cfg.bench_warmup_rounds.saturating_add(cfg.bench_rounds);
    let round_samples = match mode {
        KernelBenchMode::Steady => {
            bench_backend.kernel_bench_samples(total_rounds, cfg.bench_secs, shutdown)?
        }
        KernelBenchMode::Effective => {
            bench_backend.kernel_bench_effective_samples(total_rounds, cfg.bench_secs, shutdown)?
        }
    };
    let kernel_telemetry = backend.take_telemetry();
    let mut measured_round = 0u32;
    for (round, sample) in round_samples
        .into_iter()
        .take(total_rounds as usize)
        .enumerate()
    {
        let round = round as u32;
        let is_warmup = round < cfg.bench_warmup_rounds;
        let KernelBenchSample {
            hashes,
            elapsed_secs,
            wall_elapsed_secs,
        } = sample;
        let timing = normalize_bench_timing(
            cfg.bench_secs.max(1) as f64,
            elapsed_secs,
            wall_elapsed_secs,
            false,
        );
        // Whole hash batches can complete after the requested deadline. Dividing by the
        // configured window inflates H/s, so every kernel mode uses its measured duration.
        let hps = hashes as f64 / timing.rate_elapsed_secs;

        if is_warmup {
            info(
                "BENCH",
                format!(
                    "warmup {}/{} | hashes={} | configured={:.2}s actual={:.2}s overrun={:.3}s wall={:.2}s | {}",
                    round + 1,
                    cfg.bench_warmup_rounds,
                    hashes,
                    timing.configured_secs,
                    timing.actual_elapsed_secs,
                    timing.window_overrun_secs,
                    timing.wall_secs,
                    format_hashrate(hps),
                ),
            );
            continue;
        }

        measured_round = measured_round.saturating_add(1);
        info(
            "BENCH",
            format!(
                "round {}/{} | hashes={} | configured={:.2}s actual={:.2}s overrun={:.3}s wall={:.2}s | {}",
                measured_round,
                cfg.bench_rounds,
                hashes,
                timing.configured_secs,
                timing.actual_elapsed_secs,
                timing.window_overrun_secs,
                timing.wall_secs,
                format_hashrate(hps),
            ),
        );

        runs.push(BenchRun {
            round: measured_round,
            hashes,
            counted_hashes: hashes,
            late_hashes: 0,
            late_hash_pct: 0.0,
            elapsed_secs: timing.rate_elapsed_secs,
            configured_secs: timing.configured_secs,
            actual_elapsed_secs: timing.actual_elapsed_secs,
            window_overrun_secs: timing.window_overrun_secs,
            wall_secs: timing.wall_secs,
            startup_secs: 0.0,
            teardown_secs: 0.0,
            fence_secs: 0.0,
            hps,
            backend_runs: vec![BenchBackendRun {
                backend_id: 0,
                backend: backend.name().to_string(),
                hashes,
                hps,
                memory_explicit_large_workers: kernel_telemetry.memory_explicit_large_workers,
                memory_explicit_large_1g_workers: kernel_telemetry.memory_explicit_large_1g_workers,
                memory_transparent_huge_workers: kernel_telemetry.memory_transparent_huge_workers,
                memory_regular_workers: kernel_telemetry.memory_regular_workers,
                memory_heap_workers: kernel_telemetry.memory_heap_workers,
                memory_explicit_large_bytes: kernel_telemetry.memory_explicit_large_bytes,
                memory_explicit_large_1g_bytes: kernel_telemetry.memory_explicit_large_1g_bytes,
                memory_transparent_huge_bytes: kernel_telemetry.memory_transparent_huge_bytes,
                memory_regular_bytes: kernel_telemetry.memory_regular_bytes,
                memory_heap_bytes: kernel_telemetry.memory_heap_bytes,
                memory_allocation_failures: kernel_telemetry.memory_allocation_failures,
                ..BenchBackendRun::default()
            }],
        });
    }

    summarize_benchmark(
        cfg,
        BenchReport {
            schema_version: BENCH_REPORT_SCHEMA_VERSION,
            environment,
            config_fingerprint: benchmark_config_fingerprint(cfg, None),
            pow_fingerprint: benchmark_pow_fingerprint(),
            bench_kind: mode.report_kind_label().to_string(),
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
            warmup_rounds: cfg.bench_warmup_rounds,
            avg_hps: 0.0,
            median_hps: 0.0,
            min_hps: 0.0,
            max_hps: 0.0,
            total_hashes: 0,
            total_counted_hashes: 0,
            total_late_hashes: 0,
            late_hash_pct: 0.0,
            runs,
        },
    )
}

fn worker_benchmark_identity(
    backends: &[BackendSlot],
    initial_startup_secs: f64,
) -> WorkerBenchmarkIdentity {
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
        initial_startup_secs,
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
    instances: Vec<(crate::config::BackendSpec, Arc<dyn PowBackend>)>,
    restart_each_round: bool,
    backend_executor: &super::backend_executor::BackendExecutor,
) -> Result<()> {
    if let Some(hint) = cfg.nvidia_hint {
        info("HINT", hint);
    }
    let initial_startup_started = Instant::now();
    let requested_backend_kinds: Vec<String> = instances
        .iter()
        .map(|(_, backend)| backend.name().to_string())
        .collect();
    let (mut backends, backend_events) = activate_backends(
        instances,
        cfg.backend_event_capacity,
        cfg,
        shutdown,
        Vec::new(),
    )?;
    super::enforce_deadline_policy(
        &mut backends,
        cfg.allow_best_effort_deadlines,
        RuntimeMode::Bench,
        backend_executor,
    )?;
    // The benchmark proceeds with the surviving backends; make a missing
    // requested backend impossible to overlook in the results.
    let degraded_warning = degraded_backends_warning(
        &backends
            .iter()
            .map(|slot| slot.backend.name().to_string())
            .collect::<Vec<_>>(),
        &requested_backend_kinds,
        "missing",
    );
    if let Some(warning) = degraded_warning.as_deref() {
        warn("BENCH", warning);
    }
    let initial_startup_secs = initial_startup_started.elapsed().as_secs_f64();
    let identity = worker_benchmark_identity(&backends, initial_startup_secs);
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
        ("Warmup Rounds", cfg.bench_warmup_rounds.to_string()),
        ("Seconds/Round", cfg.bench_secs.to_string()),
        ("CPU Page Mode", cfg.cpu_page_mode.as_str().to_string()),
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
            "Assign Strikes",
            cfg.backend_assign_timeout_strikes.to_string(),
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
        (
            "Measurement",
            if restart_each_round {
                "backend start + counted window/fence + teardown"
            } else {
                "counted window + end fence"
            }
            .to_string(),
        ),
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

    let result = run_worker_benchmark_inner(
        cfg,
        shutdown,
        &mut backends,
        &backend_events,
        restart_each_round,
        &identity,
        backend_executor,
        degraded_warning.as_deref(),
    );
    stop_backend_slots(
        &mut backends,
        backend_executor,
        cfg.backend_control_timeout,
        "BENCH",
    );
    result
}

#[allow(clippy::too_many_arguments)]
fn run_worker_benchmark_inner(
    cfg: &Config,
    shutdown: &AtomicBool,
    backends: &mut Vec<BackendSlot>,
    backend_events: &Receiver<BackendEvent>,
    restart_each_round: bool,
    identity: &WorkerBenchmarkIdentity,
    backend_executor: &super::backend_executor::BackendExecutor,
    degraded_warning: Option<&str>,
) -> Result<()> {
    let impossible_target = [0u8; 32];
    let mut runs = Vec::with_capacity(cfg.bench_rounds as usize);
    let environment = benchmark_environment();
    let mut epoch = 0u64;
    let mut work_id_cursor = 1u64;
    let mut scheduler = NonceScheduler::new(cfg.start_nonce, cfg.nonce_iters_per_lane);
    let mut backend_weights = seed_backend_weights(backends);
    ensure_worker_topology_identity(backends, identity, "benchmark setup")?;
    let total_rounds = cfg.bench_warmup_rounds.saturating_add(cfg.bench_rounds);
    let mut measured_round = 0u32;

    for round in 0..total_rounds {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        if backends.is_empty() {
            bail!("all benchmark backends are unavailable");
        }
        let is_warmup = round < cfg.bench_warmup_rounds;
        let phase_label = if is_warmup {
            format!("warmup {}", round + 1)
        } else {
            format!("round {}", measured_round + 1)
        };

        let first_round_uses_initial_startup = restart_each_round && round == 0;
        let startup_secs = if first_round_uses_initial_startup {
            identity.initial_startup_secs
        } else if restart_each_round {
            let startup_started = Instant::now();
            start_backend_slots(backends, backend_executor, cfg.backend_control_timeout)?;
            ensure_worker_topology_identity(backends, identity, "backend restart")?;
            startup_started.elapsed().as_secs_f64()
        } else {
            0.0
        };

        epoch = epoch.wrapping_add(1).max(1);
        let work_id = next_work_id(&mut work_id_cursor);
        let round_start = Instant::now();
        let stop_at = round_start + Duration::from_secs(cfg.bench_secs);
        let header_base = benchmark_header_base(round);
        let reservation = scheduler.reserve(total_lanes(backends));

        let distribution = distribute_work(
            backends,
            super::DistributeWorkOptions {
                epoch,
                work_id,
                header_base: std::sync::Arc::clone(&header_base),
                target: impossible_target,
                dynamic_share_target: None,
                pause_on_solution: true,
                reservation,
                stop_at,
                backend_weights: work_distribution_weights(cfg.work_allocation, &backend_weights),
                strict_reservation: false,
            },
            backend_executor,
        )?;
        scheduler.consume_additional_span(distribution.additional_span_consumed);

        let mut round_hashes = 0u64;
        let mut round_backend_hashes = BTreeMap::new();
        let mut round_backend_telemetry = BTreeMap::new();
        let mut backend_poll_state = build_backend_poll_state(backends, cfg.hash_poll_interval);
        let mut round_driver = super::round_driver::RoundDriverContext {
            backends,
            backend_events,
            backend_executor,
            configured_hash_poll_interval: cfg.hash_poll_interval,
            poll_state: &mut backend_poll_state,
            round_backend_hashes: &mut round_backend_hashes,
            round_backend_telemetry: &mut round_backend_telemetry,
        };
        super::round_driver::run_round_window(
            &mut round_driver,
            shutdown,
            stop_at,
            || None,
            |_driver| Ok(super::round_driver::RoundWindowControl::Continue),
            |driver, step| {
                round_hashes = round_hashes.saturating_add(step.collected_hashes);
                if let Some(event) = step.event {
                    if handle_benchmark_backend_event(
                        event,
                        epoch,
                        driver.backends,
                        backend_executor,
                    )? == BackendEventAction::TopologyChanged
                    {
                        ensure_worker_topology_identity(driver.backends, identity, &phase_label)?;
                    }
                }
                Ok(super::round_driver::RoundWindowControl::Continue)
            },
        )?;

        collect_backend_hashes(
            backends,
            backend_executor,
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
        if quiesce_backend_slots(backends, RuntimeMode::Bench, backend_executor)?
            == BackendEventAction::TopologyChanged
        {
            ensure_worker_topology_identity(backends, identity, &format!("{phase_label} fence"))?;
        }
        let fence_elapsed = fence_start.elapsed().as_secs_f64();
        let mut late_hashes = 0u64;
        let mut late_backend_hashes = BTreeMap::new();
        let mut late_backend_telemetry = BTreeMap::new();
        collect_backend_hashes(
            backends,
            backend_executor,
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
                &format!("{phase_label} event drain"),
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
        let late_hash_pct = late_hash_share_pct(late_hashes, round_hashes);
        let actual_elapsed_secs = (counted_elapsed + fence_elapsed).max(0.001);

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
                round_elapsed_secs: actual_elapsed_secs,
                mode: cfg.work_allocation,
                round_end_reason,
                refresh_interval: cfg.refresh_interval,
            },
        );

        let teardown_started = Instant::now();
        let teardown_secs = if restart_each_round {
            stop_backend_slots(
                backends,
                backend_executor,
                cfg.backend_control_timeout,
                "BENCH",
            );
            teardown_started.elapsed().as_secs_f64()
        } else {
            0.0
        };
        let raw_wall_secs = if restart_each_round {
            startup_secs + actual_elapsed_secs + teardown_secs
        } else {
            actual_elapsed_secs
        };
        // Backend mode reports steady effective throughput. End-to-end mode is a lifecycle
        // benchmark, so backend startup and teardown are part of its rate denominator.
        let timing = normalize_bench_timing(
            cfg.bench_secs.max(1) as f64,
            actual_elapsed_secs,
            raw_wall_secs,
            restart_each_round,
        );
        let hps = counted_hashes as f64 / timing.rate_elapsed_secs;
        let backend_runs = build_backend_round_stats(
            backends,
            &round_backend_hashes,
            &round_backend_telemetry,
            timing.rate_elapsed_secs,
        );
        let backend_rates = format_bench_backend_hashrate(
            backends,
            &round_backend_hashes,
            timing.rate_elapsed_secs,
        );
        let telemetry_line = format_round_backend_telemetry(backends, &round_backend_telemetry);

        if is_warmup {
            info(
                "BENCH",
                format!(
                    "warmup {}/{} hashes={} counted={} late={} late_pct={:.2}% configured={:.2}s actual={:.2}s fence={:.3}s startup={:.3}s teardown={:.3}s wall={:.2}s rate={} backends={}",
                    round + 1,
                    cfg.bench_warmup_rounds,
                    round_hashes,
                    counted_hashes,
                    late_hashes,
                    late_hash_pct,
                    timing.configured_secs,
                    timing.actual_elapsed_secs,
                    fence_elapsed,
                    startup_secs,
                    teardown_secs,
                    timing.wall_secs,
                    format_hashrate(hps),
                    backend_rates,
                ),
            );
            if let Some(warning) = degraded_warning {
                warn("BENCH", warning);
            }
            if let Some(telemetry_line) = &telemetry_line {
                info("BENCH", format!("warmup telemetry | {telemetry_line}"));
            }
        } else {
            measured_round = measured_round.saturating_add(1);
            info(
                "BENCH",
                format!(
                    "round {}/{} hashes={} counted={} late={} late_pct={:.2}% configured={:.2}s actual={:.2}s fence={:.3}s startup={:.3}s teardown={:.3}s wall={:.2}s rate={} backends={}",
                    measured_round,
                    cfg.bench_rounds,
                    round_hashes,
                    counted_hashes,
                    late_hashes,
                    late_hash_pct,
                    timing.configured_secs,
                    timing.actual_elapsed_secs,
                    fence_elapsed,
                    startup_secs,
                    teardown_secs,
                    timing.wall_secs,
                    format_hashrate(hps),
                    backend_rates,
                ),
            );
            if let Some(warning) = degraded_warning {
                warn("BENCH", warning);
            }
            if let Some(telemetry_line) = &telemetry_line {
                info("BENCH", format!("telemetry | {telemetry_line}"));
            }

            runs.push(BenchRun {
                round: measured_round,
                hashes: round_hashes,
                counted_hashes,
                late_hashes,
                late_hash_pct,
                elapsed_secs: timing.rate_elapsed_secs,
                configured_secs: timing.configured_secs,
                actual_elapsed_secs: timing.actual_elapsed_secs,
                window_overrun_secs: timing.window_overrun_secs,
                wall_secs: timing.wall_secs,
                startup_secs,
                teardown_secs,
                fence_secs: fence_elapsed,
                hps,
                backend_runs,
            });
        }
    }

    summarize_benchmark(
        cfg,
        BenchReport {
            schema_version: BENCH_REPORT_SCHEMA_VERSION,
            environment,
            config_fingerprint: benchmark_config_fingerprint(cfg, Some(backends)),
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
            warmup_rounds: cfg.bench_warmup_rounds,
            avg_hps: 0.0,
            median_hps: 0.0,
            min_hps: 0.0,
            max_hps: 0.0,
            total_hashes: 0,
            total_counted_hashes: 0,
            total_late_hashes: 0,
            late_hash_pct: 0.0,
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
    report.total_counted_hashes = report
        .runs
        .iter()
        .map(|run| run.counted_hashes)
        .fold(0u64, u64::saturating_add);
    report.total_late_hashes = report
        .runs
        .iter()
        .map(|run| run.late_hashes)
        .fold(0u64, u64::saturating_add);
    report.total_hashes = report
        .total_counted_hashes
        .saturating_add(report.total_late_hashes);
    report.late_hash_pct = late_hash_share_pct(report.total_late_hashes, report.total_hashes);

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
    info(
        "BENCH",
        format!(
            "accounting | hashes={} counted={} late={} late_pct={:.2}%",
            report.total_hashes,
            report.total_counted_hashes,
            report.total_late_hashes,
            report.late_hash_pct,
        ),
    );
    emit_benchmark_diagnostics(cfg, &report);

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

fn emit_benchmark_diagnostics(cfg: &Config, report: &BenchReport) {
    if report.runs.is_empty() {
        return;
    }

    let mut fence_samples: Vec<f64> = report
        .runs
        .iter()
        .map(|run| run.fence_secs.max(0.0))
        .collect();
    fence_samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let fence_avg = fence_samples.iter().sum::<f64>() / fence_samples.len() as f64;
    let fence_median = median(&fence_samples);
    let fence_min = *fence_samples.first().unwrap_or(&0.0);
    let fence_max = *fence_samples.last().unwrap_or(&0.0);
    let fence_spread = fence_max - fence_min;
    info(
        "BENCH",
        format!(
            "fence | avg={:.3}s median={:.3}s min={:.3}s max={:.3}s",
            fence_avg, fence_median, fence_min, fence_max
        ),
    );

    let worker_mode = matches!(cfg.bench_kind, BenchKind::Backend | BenchKind::EndToEnd);
    let short_window = cfg.bench_secs < BENCH_SHORT_WINDOW_WARN_SECS;
    let jitter_warn = fence_spread >= BENCH_FENCE_JITTER_WARN_SECS
        || (fence_avg > 0.0 && (fence_spread / fence_avg) >= BENCH_FENCE_JITTER_WARN_RATIO);
    if worker_mode && (short_window || jitter_warn) {
        warn(
            "BENCH",
            format!(
                "short-window/jitter warning | bench-secs={} fence_spread={:.3}s; prefer --bench-secs >= {} and --bench-warmup-rounds 1 for stable A/B comparisons",
                cfg.bench_secs,
                fence_spread,
                BENCH_SHORT_WINDOW_WARN_SECS
            ),
        );
    }

    let mut control_enqueue_p95_max = 0u64;
    let mut control_execution_p95_max = 0u64;
    let mut has_nvidia_backend = false;
    for run in &report.runs {
        for backend_run in &run.backend_runs {
            has_nvidia_backend |= backend_run.backend == "nvidia";
            control_enqueue_p95_max =
                control_enqueue_p95_max.max(backend_run.control_enqueue_latency_p95_micros);
            control_execution_p95_max =
                control_execution_p95_max.max(backend_run.control_execution_latency_p95_micros);
        }
    }
    if control_enqueue_p95_max > 0 || control_execution_p95_max > 0 {
        info(
            "BENCH",
            format!(
                "control-latency | enqueue_p95_max={}ms execution_p95_max={}ms",
                control_enqueue_p95_max / 1_000,
                control_execution_p95_max / 1_000
            ),
        );
    }
    if has_nvidia_backend
        && (control_enqueue_p95_max >= BENCH_CONTROL_LATENCY_WARN_MICROS
            || control_execution_p95_max >= BENCH_CONTROL_LATENCY_WARN_MICROS)
    {
        warn(
            "BENCH",
            "control-latency warning | high control latency can inflate fence overhead; consider lower launch depth (for example --nvidia-hashes-per-launch-per-lane 1 or 2) while keeping adaptive depth enabled",
        );
    }
}

fn baseline_compatibility_issues(
    current: &BenchReport,
    baseline: &BenchReport,
    policy: BenchBaselinePolicy,
) -> Vec<String> {
    let mut issues = Vec::new();
    let schema_compatible = baseline.schema_version >= BENCH_REPORT_COMPAT_MIN_SCHEMA_VERSION
        && baseline.schema_version <= BENCH_REPORT_SCHEMA_VERSION
        && current.schema_version >= BENCH_REPORT_COMPAT_MIN_SCHEMA_VERSION;

    if !schema_compatible {
        issues.push(format!(
            "schema mismatch baseline={} current={} (compatible baseline schemas {}-{})",
            baseline.schema_version,
            current.schema_version,
            BENCH_REPORT_COMPAT_MIN_SCHEMA_VERSION,
            BENCH_REPORT_SCHEMA_VERSION
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

    if schema_compatible {
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
        if baseline.schema_version >= 7 && current.schema_version >= 7 {
            if baseline.config_fingerprint.cpu_profile != current.config_fingerprint.cpu_profile {
                issues.push(format!(
                    "cpu_profile mismatch baseline={} current={}",
                    baseline.config_fingerprint.cpu_profile, current.config_fingerprint.cpu_profile
                ));
            }
            if baseline.config_fingerprint.cpu_autotune_threads
                != current.config_fingerprint.cpu_autotune_threads
            {
                issues.push(format!(
                    "cpu_autotune_threads mismatch baseline={} current={}",
                    baseline.config_fingerprint.cpu_autotune_threads,
                    current.config_fingerprint.cpu_autotune_threads
                ));
            }
            if baseline.config_fingerprint.cpu_autotune_min_threads
                != current.config_fingerprint.cpu_autotune_min_threads
            {
                issues.push(format!(
                    "cpu_autotune_min_threads mismatch baseline={} current={}",
                    baseline.config_fingerprint.cpu_autotune_min_threads,
                    current.config_fingerprint.cpu_autotune_min_threads
                ));
            }
            if baseline.config_fingerprint.cpu_autotune_max_threads
                != current.config_fingerprint.cpu_autotune_max_threads
            {
                issues.push(format!(
                    "cpu_autotune_max_threads mismatch baseline={:?} current={:?}",
                    baseline.config_fingerprint.cpu_autotune_max_threads,
                    current.config_fingerprint.cpu_autotune_max_threads
                ));
            }
            if baseline.config_fingerprint.cpu_autotune_secs
                != current.config_fingerprint.cpu_autotune_secs
            {
                issues.push(format!(
                    "cpu_autotune_secs mismatch baseline={} current={}",
                    baseline.config_fingerprint.cpu_autotune_secs,
                    current.config_fingerprint.cpu_autotune_secs
                ));
            }
            if baseline.config_fingerprint.cpu_auto_threads_cap
                != current.config_fingerprint.cpu_auto_threads_cap
            {
                issues.push(format!(
                    "cpu_auto_threads_cap mismatch baseline={} current={}",
                    baseline.config_fingerprint.cpu_auto_threads_cap,
                    current.config_fingerprint.cpu_auto_threads_cap
                ));
            }
        }
        if baseline.schema_version >= 12 && current.schema_version >= 12 {
            if baseline.config_fingerprint.cpu_page_mode != current.config_fingerprint.cpu_page_mode
            {
                issues.push(format!(
                    "cpu_page_mode mismatch baseline={} current={}",
                    baseline.config_fingerprint.cpu_page_mode,
                    current.config_fingerprint.cpu_page_mode
                ));
            }
        }
        if baseline.schema_version >= 8 && current.schema_version >= 8 {
            if baseline.config_fingerprint.nvidia_autotune_secs
                != current.config_fingerprint.nvidia_autotune_secs
            {
                issues.push(format!(
                    "nvidia_autotune_secs mismatch baseline={} current={}",
                    baseline.config_fingerprint.nvidia_autotune_secs,
                    current.config_fingerprint.nvidia_autotune_secs
                ));
            }
            if baseline.config_fingerprint.nvidia_autotune_samples
                != current.config_fingerprint.nvidia_autotune_samples
            {
                issues.push(format!(
                    "nvidia_autotune_samples mismatch baseline={} current={}",
                    baseline.config_fingerprint.nvidia_autotune_samples,
                    current.config_fingerprint.nvidia_autotune_samples
                ));
            }
            if baseline.config_fingerprint.nvidia_max_rregcount
                != current.config_fingerprint.nvidia_max_rregcount
            {
                issues.push(format!(
                    "nvidia_max_rregcount mismatch baseline={:?} current={:?}",
                    baseline.config_fingerprint.nvidia_max_rregcount,
                    current.config_fingerprint.nvidia_max_rregcount
                ));
            }
            if baseline.config_fingerprint.nvidia_max_lanes
                != current.config_fingerprint.nvidia_max_lanes
            {
                issues.push(format!(
                    "nvidia_max_lanes mismatch baseline={:?} current={:?}",
                    baseline.config_fingerprint.nvidia_max_lanes,
                    current.config_fingerprint.nvidia_max_lanes
                ));
            }
            if baseline.config_fingerprint.nvidia_dispatch_iters_per_lane
                != current.config_fingerprint.nvidia_dispatch_iters_per_lane
            {
                issues.push(format!(
                    "nvidia_dispatch_iters_per_lane mismatch baseline={:?} current={:?}",
                    baseline.config_fingerprint.nvidia_dispatch_iters_per_lane,
                    current.config_fingerprint.nvidia_dispatch_iters_per_lane
                ));
            }
            if baseline.config_fingerprint.nvidia_allocation_iters_per_lane
                != current.config_fingerprint.nvidia_allocation_iters_per_lane
            {
                issues.push(format!(
                    "nvidia_allocation_iters_per_lane mismatch baseline={:?} current={:?}",
                    baseline.config_fingerprint.nvidia_allocation_iters_per_lane,
                    current.config_fingerprint.nvidia_allocation_iters_per_lane
                ));
            }
            if baseline
                .config_fingerprint
                .nvidia_hashes_per_launch_per_lane
                != current.config_fingerprint.nvidia_hashes_per_launch_per_lane
            {
                issues.push(format!(
                    "nvidia_hashes_per_launch_per_lane mismatch baseline={} current={}",
                    baseline
                        .config_fingerprint
                        .nvidia_hashes_per_launch_per_lane,
                    current.config_fingerprint.nvidia_hashes_per_launch_per_lane
                ));
            }
        }
        if baseline.schema_version >= 9 && current.schema_version >= 9 {
            if baseline.config_fingerprint.nvidia_adaptive_launch_depth
                != current.config_fingerprint.nvidia_adaptive_launch_depth
            {
                issues.push(format!(
                    "nvidia_adaptive_launch_depth mismatch baseline={} current={}",
                    baseline.config_fingerprint.nvidia_adaptive_launch_depth,
                    current.config_fingerprint.nvidia_adaptive_launch_depth
                ));
            }
            if baseline.config_fingerprint.nvidia_enforce_template_stop
                != current.config_fingerprint.nvidia_enforce_template_stop
            {
                issues.push(format!(
                    "nvidia_enforce_template_stop mismatch baseline={} current={}",
                    baseline.config_fingerprint.nvidia_enforce_template_stop,
                    current.config_fingerprint.nvidia_enforce_template_stop
                ));
            }
        }
        if baseline.schema_version >= 10 && current.schema_version >= 10 {
            if baseline.config_fingerprint.nvidia_fused_target_check
                != current.config_fingerprint.nvidia_fused_target_check
            {
                issues.push(format!(
                    "nvidia_fused_target_check mismatch baseline={} current={}",
                    baseline.config_fingerprint.nvidia_fused_target_check,
                    current.config_fingerprint.nvidia_fused_target_check
                ));
            }
        }
        if baseline.schema_version >= 11
            && current.schema_version >= 11
            && baseline
                .config_fingerprint
                .nvidia_hashes_per_launch_per_lane_was_set
                != current
                    .config_fingerprint
                    .nvidia_hashes_per_launch_per_lane_was_set
        {
            issues.push(format!(
                "nvidia_hashes_per_launch_per_lane_was_set mismatch baseline={} current={}",
                baseline
                    .config_fingerprint
                    .nvidia_hashes_per_launch_per_lane_was_set,
                current
                    .config_fingerprint
                    .nvidia_hashes_per_launch_per_lane_was_set
            ));
        }
        if baseline.schema_version >= 6 && current.schema_version >= 6 {
            if baseline.config_fingerprint.cpu_hash_batch_size
                != current.config_fingerprint.cpu_hash_batch_size
            {
                issues.push(format!(
                    "cpu_hash_batch_size mismatch baseline={} current={}",
                    baseline.config_fingerprint.cpu_hash_batch_size,
                    current.config_fingerprint.cpu_hash_batch_size
                ));
            }
            if baseline
                .config_fingerprint
                .cpu_control_check_interval_hashes
                != current.config_fingerprint.cpu_control_check_interval_hashes
            {
                issues.push(format!(
                    "cpu_control_check_interval_hashes mismatch baseline={} current={}",
                    baseline
                        .config_fingerprint
                        .cpu_control_check_interval_hashes,
                    current.config_fingerprint.cpu_control_check_interval_hashes
                ));
            }
            if baseline.config_fingerprint.cpu_hash_flush_ms
                != current.config_fingerprint.cpu_hash_flush_ms
            {
                issues.push(format!(
                    "cpu_hash_flush_ms mismatch baseline={} current={}",
                    baseline.config_fingerprint.cpu_hash_flush_ms,
                    current.config_fingerprint.cpu_hash_flush_ms
                ));
            }
            if baseline.config_fingerprint.cpu_event_dispatch_capacity
                != current.config_fingerprint.cpu_event_dispatch_capacity
            {
                issues.push(format!(
                    "cpu_event_dispatch_capacity mismatch baseline={} current={}",
                    baseline.config_fingerprint.cpu_event_dispatch_capacity,
                    current.config_fingerprint.cpu_event_dispatch_capacity
                ));
            }
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
        if baseline.config_fingerprint.backend_assign_timeout_strikes
            != current.config_fingerprint.backend_assign_timeout_strikes
        {
            issues.push(format!(
                "backend_assign_timeout_strikes mismatch baseline={} current={}",
                baseline.config_fingerprint.backend_assign_timeout_strikes,
                current.config_fingerprint.backend_assign_timeout_strikes
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
        if baseline.config_fingerprint.bench_warmup_rounds
            != current.config_fingerprint.bench_warmup_rounds
        {
            issues.push(format!(
                "bench_warmup_rounds mismatch baseline={} current={}",
                baseline.config_fingerprint.bench_warmup_rounds,
                current.config_fingerprint.bench_warmup_rounds
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
        if !baseline.config_fingerprint.cpu_affinity_strategy.is_empty()
            && !current.config_fingerprint.cpu_affinity_strategy.is_empty()
            && baseline.config_fingerprint.cpu_affinity_strategy
                != current.config_fingerprint.cpu_affinity_strategy
        {
            issues.push(format!(
                "cpu_affinity_strategy mismatch baseline={} current={}",
                baseline.config_fingerprint.cpu_affinity_strategy,
                current.config_fingerprint.cpu_affinity_strategy
            ));
        }
        if baseline.schema_version >= 5
            && current.schema_version >= 5
            && baseline.config_fingerprint.backend_runtime
                != current.config_fingerprint.backend_runtime
        {
            issues.push(format!(
                "backend_runtime mismatch baseline={} current={}",
                format_backend_runtime_fingerprint(&baseline.config_fingerprint.backend_runtime),
                format_backend_runtime_fingerprint(&current.config_fingerprint.backend_runtime)
            ));
        }
        if baseline.pow_fingerprint != current.pow_fingerprint {
            issues.push("pow parameter fingerprint mismatch".to_string());
        }
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
        if baseline.schema_version >= 11 && current.schema_version >= 11 {
            if baseline.environment.runtime_environment != current.environment.runtime_environment {
                issues.push(format!(
                    "runtime environment mismatch baseline={} current={}",
                    baseline.environment.runtime_environment,
                    current.environment.runtime_environment
                ));
            }
            if baseline.environment.kernel_version != current.environment.kernel_version {
                issues.push(format!(
                    "kernel mismatch baseline={} current={}",
                    baseline
                        .environment
                        .kernel_version
                        .as_deref()
                        .unwrap_or("unknown"),
                    current
                        .environment
                        .kernel_version
                        .as_deref()
                        .unwrap_or("unknown")
                ));
            }
            if baseline.environment.build_fingerprint != current.environment.build_fingerprint {
                issues.push("build/toolchain fingerprint mismatch".to_string());
            }
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

fn format_backend_runtime_fingerprint(runtime: &[BenchBackendRuntimeFingerprint]) -> String {
    runtime
        .iter()
        .map(|entry| {
            format!(
                "{}#{}(assign={}ms,strikes={},control={}ms)",
                entry.backend,
                entry.backend_id,
                entry.assign_timeout_ms,
                entry.assign_timeout_strikes,
                entry.control_timeout_ms
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn late_hash_share_pct(late_hashes: u64, total_hashes: u64) -> f64 {
    if total_hashes == 0 {
        0.0
    } else {
        (late_hashes as f64 * 100.0) / total_hashes as f64
    }
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
        target_triple: crate::runtime_identity::build_target().to_string(),
        runtime_environment: crate::runtime_identity::runtime_environment(),
        wsl_distro: crate::runtime_identity::wsl_distro(),
        build_host: crate::runtime_identity::build_host().to_string(),
        build_profile: crate::runtime_identity::build_profile().to_string(),
        build_opt_level: crate::runtime_identity::build_opt_level().to_string(),
        build_rustflags: crate::runtime_identity::build_rustflags().to_string(),
        build_features: crate::runtime_identity::build_features().to_string(),
        rustc_version: crate::runtime_identity::rustc_version().to_string(),
        source_fingerprint: crate::runtime_identity::source_fingerprint().to_string(),
        build_fingerprint: crate::runtime_identity::build_fingerprint(),
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

fn benchmark_config_fingerprint(
    cfg: &Config,
    backends: Option<&[BackendSlot]>,
) -> BenchConfigFingerprint {
    let backend_runtime = backends
        .map(|slots| {
            slots
                .iter()
                .map(|slot| BenchBackendRuntimeFingerprint {
                    backend_id: slot.id,
                    backend: slot.backend.name().to_string(),
                    assign_timeout_ms: slot.runtime_policy.assignment_timeout.as_millis() as u64,
                    assign_timeout_strikes: slot.runtime_policy.assignment_timeout_strikes,
                    control_timeout_ms: slot.runtime_policy.control_timeout.as_millis() as u64,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    BenchConfigFingerprint {
        backend_event_capacity: cfg.backend_event_capacity,
        hash_poll_ms: cfg.hash_poll_interval.as_millis() as u64,
        cpu_profile: cpu_profile_label(cfg.cpu_profile).to_string(),
        cpu_page_mode: cfg.cpu_page_mode.as_str().to_string(),
        cpu_hash_batch_size: cfg.cpu_hash_batch_size,
        cpu_control_check_interval_hashes: cfg.cpu_control_check_interval_hashes,
        cpu_hash_flush_ms: cfg.cpu_hash_flush_interval.as_millis() as u64,
        cpu_event_dispatch_capacity: cfg.cpu_event_dispatch_capacity,
        cpu_autotune_threads: cfg.cpu_autotune_threads,
        cpu_autotune_min_threads: cfg.cpu_autotune_min_threads,
        cpu_autotune_max_threads: cfg.cpu_autotune_max_threads,
        cpu_autotune_secs: cfg.cpu_autotune_secs,
        cpu_auto_threads_cap: cfg.cpu_auto_threads_cap,
        nvidia_autotune_secs: cfg.nvidia_autotune_secs,
        nvidia_autotune_samples: cfg.nvidia_autotune_samples,
        nvidia_max_rregcount: cfg.nvidia_max_rregcount,
        nvidia_max_lanes: cfg.nvidia_max_lanes,
        nvidia_dispatch_iters_per_lane: cfg.nvidia_dispatch_iters_per_lane,
        nvidia_allocation_iters_per_lane: cfg.nvidia_allocation_iters_per_lane,
        nvidia_hashes_per_launch_per_lane: cfg.nvidia_hashes_per_launch_per_lane,
        nvidia_hashes_per_launch_per_lane_was_set: cfg.nvidia_hashes_per_launch_per_lane_was_set,
        nvidia_fused_target_check: cfg.nvidia_fused_target_check,
        nvidia_adaptive_launch_depth: cfg.nvidia_adaptive_launch_depth,
        nvidia_enforce_template_stop: cfg.nvidia_enforce_template_stop,
        backend_assign_timeout_ms: cfg.backend_assign_timeout.as_millis() as u64,
        backend_assign_timeout_strikes: cfg.backend_assign_timeout_strikes,
        backend_control_timeout_ms: cfg.backend_control_timeout.as_millis() as u64,
        bench_warmup_rounds: cfg.bench_warmup_rounds,
        allow_best_effort_deadlines: cfg.allow_best_effort_deadlines,
        prefetch_wait_ms: cfg.prefetch_wait.as_millis() as u64,
        tip_listener_join_wait_ms: cfg.tip_listener_join_wait.as_millis() as u64,
        strict_round_accounting: cfg.strict_round_accounting,
        refresh_secs: cfg.refresh_interval.as_secs(),
        nonce_iters_per_lane: cfg.nonce_iters_per_lane,
        start_nonce: cfg.start_nonce,
        work_allocation: work_allocation_label(cfg.work_allocation).to_string(),
        cpu_affinity: cpu_affinity_label(cfg.cpu_affinity).to_string(),
        cpu_affinity_strategy: cpu_affinity_strategy_label(cfg.cpu_affinity).to_string(),
        events_idle_timeout_secs: cfg.events_idle_timeout.as_secs(),
        backend_runtime,
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
        CpuAffinityMode::PcoreOnly => "pcore-only",
    }
}

fn cpu_affinity_strategy_label(mode: CpuAffinityMode) -> &'static str {
    if mode == CpuAffinityMode::Off {
        return "off";
    }
    #[cfg(target_os = "windows")]
    {
        "windows-physical-first-if-complete"
    }
    #[cfg(target_os = "linux")]
    {
        "linux-os-order"
    }
    #[cfg(target_os = "macos")]
    {
        match mode {
            CpuAffinityMode::PcoreOnly => "macos-limited-affinity-tags-high-qos",
            CpuAffinityMode::Auto => "macos-affinity-tags-high-qos",
            CpuAffinityMode::Off => "off",
        }
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        "os-order"
    }
}

fn cpu_profile_label(profile: CpuPerformanceProfile) -> &'static str {
    match profile {
        CpuPerformanceProfile::Balanced => "balanced",
        CpuPerformanceProfile::Throughput => "throughput",
        CpuPerformanceProfile::Efficiency => "efficiency",
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
    entry.memory_explicit_large_workers = entry
        .memory_explicit_large_workers
        .max(telemetry.memory_explicit_large_workers);
    entry.memory_explicit_large_1g_workers = entry
        .memory_explicit_large_1g_workers
        .max(telemetry.memory_explicit_large_1g_workers);
    entry.memory_transparent_huge_workers = entry
        .memory_transparent_huge_workers
        .max(telemetry.memory_transparent_huge_workers);
    entry.memory_regular_workers = entry
        .memory_regular_workers
        .max(telemetry.memory_regular_workers);
    entry.memory_heap_workers = entry.memory_heap_workers.max(telemetry.memory_heap_workers);
    entry.memory_explicit_large_bytes = entry
        .memory_explicit_large_bytes
        .max(telemetry.memory_explicit_large_bytes);
    entry.memory_explicit_large_1g_bytes = entry
        .memory_explicit_large_1g_bytes
        .max(telemetry.memory_explicit_large_1g_bytes);
    entry.memory_transparent_huge_bytes = entry
        .memory_transparent_huge_bytes
        .max(telemetry.memory_transparent_huge_bytes);
    entry.memory_regular_bytes = entry
        .memory_regular_bytes
        .max(telemetry.memory_regular_bytes);
    entry.memory_heap_bytes = entry.memory_heap_bytes.max(telemetry.memory_heap_bytes);
    entry.memory_allocation_failures = entry
        .memory_allocation_failures
        .saturating_add(telemetry.memory_allocation_failures);
    entry.peak_inflight_assignment_hashes = entry
        .peak_inflight_assignment_hashes
        .max(telemetry.peak_inflight_assignment_hashes);
    entry.peak_inflight_assignment_micros = entry
        .peak_inflight_assignment_micros
        .max(telemetry.peak_inflight_assignment_micros);
    entry.assignment_enqueue_timeouts = entry
        .assignment_enqueue_timeouts
        .saturating_add(telemetry.assignment_enqueue_timeouts);
    entry.assignment_execution_timeouts = entry
        .assignment_execution_timeouts
        .saturating_add(telemetry.assignment_execution_timeouts);
    entry.control_enqueue_timeouts = entry
        .control_enqueue_timeouts
        .saturating_add(telemetry.control_enqueue_timeouts);
    entry.control_execution_timeouts = entry
        .control_execution_timeouts
        .saturating_add(telemetry.control_execution_timeouts);
    entry.peak_assignment_timeout_strikes = entry
        .peak_assignment_timeout_strikes
        .max(telemetry.peak_assignment_timeout_strikes);
    entry.assignment_enqueue_latency_samples = entry
        .assignment_enqueue_latency_samples
        .saturating_add(telemetry.assignment_enqueue_latency_samples);
    entry.assignment_enqueue_latency_p95_micros = entry
        .assignment_enqueue_latency_p95_micros
        .max(telemetry.assignment_enqueue_latency_p95_micros);
    entry.assignment_enqueue_latency_max_micros = entry
        .assignment_enqueue_latency_max_micros
        .max(telemetry.assignment_enqueue_latency_max_micros);
    entry.assignment_execution_latency_samples = entry
        .assignment_execution_latency_samples
        .saturating_add(telemetry.assignment_execution_latency_samples);
    entry.assignment_execution_latency_p95_micros = entry
        .assignment_execution_latency_p95_micros
        .max(telemetry.assignment_execution_latency_p95_micros);
    entry.assignment_execution_latency_max_micros = entry
        .assignment_execution_latency_max_micros
        .max(telemetry.assignment_execution_latency_max_micros);
    entry.control_enqueue_latency_samples = entry
        .control_enqueue_latency_samples
        .saturating_add(telemetry.control_enqueue_latency_samples);
    entry.control_enqueue_latency_p95_micros = entry
        .control_enqueue_latency_p95_micros
        .max(telemetry.control_enqueue_latency_p95_micros);
    entry.control_enqueue_latency_max_micros = entry
        .control_enqueue_latency_max_micros
        .max(telemetry.control_enqueue_latency_max_micros);
    entry.control_execution_latency_samples = entry
        .control_execution_latency_samples
        .saturating_add(telemetry.control_execution_latency_samples);
    entry.control_execution_latency_p95_micros = entry
        .control_execution_latency_p95_micros
        .max(telemetry.control_execution_latency_p95_micros);
    entry.control_execution_latency_max_micros = entry
        .control_execution_latency_max_micros
        .max(telemetry.control_execution_latency_max_micros);
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
            memory_explicit_large_workers: telemetry.memory_explicit_large_workers,
            memory_explicit_large_1g_workers: telemetry.memory_explicit_large_1g_workers,
            memory_transparent_huge_workers: telemetry.memory_transparent_huge_workers,
            memory_regular_workers: telemetry.memory_regular_workers,
            memory_heap_workers: telemetry.memory_heap_workers,
            memory_explicit_large_bytes: telemetry.memory_explicit_large_bytes,
            memory_explicit_large_1g_bytes: telemetry.memory_explicit_large_1g_bytes,
            memory_transparent_huge_bytes: telemetry.memory_transparent_huge_bytes,
            memory_regular_bytes: telemetry.memory_regular_bytes,
            memory_heap_bytes: telemetry.memory_heap_bytes,
            memory_allocation_failures: telemetry.memory_allocation_failures,
            peak_inflight_assignment_hashes: telemetry.peak_inflight_assignment_hashes,
            peak_inflight_assignment_secs: telemetry.peak_inflight_assignment_micros as f64
                / 1_000_000.0,
            dropped_events: telemetry.dropped_events,
            completed_assignments: telemetry.completed_assignments,
            completed_assignment_hashes: telemetry.completed_assignment_hashes,
            completed_assignment_secs: telemetry.completed_assignment_micros as f64 / 1_000_000.0,
            assignment_enqueue_timeouts: telemetry.assignment_enqueue_timeouts,
            assignment_execution_timeouts: telemetry.assignment_execution_timeouts,
            control_enqueue_timeouts: telemetry.control_enqueue_timeouts,
            control_execution_timeouts: telemetry.control_execution_timeouts,
            peak_assignment_timeout_strikes: telemetry.peak_assignment_timeout_strikes,
            assignment_enqueue_latency_samples: telemetry.assignment_enqueue_latency_samples,
            assignment_enqueue_latency_p95_micros: telemetry.assignment_enqueue_latency_p95_micros,
            assignment_enqueue_latency_max_micros: telemetry.assignment_enqueue_latency_max_micros,
            assignment_execution_latency_samples: telemetry.assignment_execution_latency_samples,
            assignment_execution_latency_p95_micros: telemetry
                .assignment_execution_latency_p95_micros,
            assignment_execution_latency_max_micros: telemetry
                .assignment_execution_latency_max_micros,
            control_enqueue_latency_samples: telemetry.control_enqueue_latency_samples,
            control_enqueue_latency_p95_micros: telemetry.control_enqueue_latency_p95_micros,
            control_enqueue_latency_max_micros: telemetry.control_enqueue_latency_max_micros,
            control_execution_latency_samples: telemetry.control_execution_latency_samples,
            control_execution_latency_p95_micros: telemetry.control_execution_latency_p95_micros,
            control_execution_latency_max_micros: telemetry.control_execution_latency_max_micros,
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
            memory_explicit_large_workers: telemetry.memory_explicit_large_workers,
            memory_explicit_large_1g_workers: telemetry.memory_explicit_large_1g_workers,
            memory_transparent_huge_workers: telemetry.memory_transparent_huge_workers,
            memory_regular_workers: telemetry.memory_regular_workers,
            memory_heap_workers: telemetry.memory_heap_workers,
            memory_explicit_large_bytes: telemetry.memory_explicit_large_bytes,
            memory_explicit_large_1g_bytes: telemetry.memory_explicit_large_1g_bytes,
            memory_transparent_huge_bytes: telemetry.memory_transparent_huge_bytes,
            memory_regular_bytes: telemetry.memory_regular_bytes,
            memory_heap_bytes: telemetry.memory_heap_bytes,
            memory_allocation_failures: telemetry.memory_allocation_failures,
            peak_inflight_assignment_hashes: telemetry.peak_inflight_assignment_hashes,
            peak_inflight_assignment_secs: telemetry.peak_inflight_assignment_micros as f64
                / 1_000_000.0,
            dropped_events: telemetry.dropped_events,
            completed_assignments: telemetry.completed_assignments,
            completed_assignment_hashes: telemetry.completed_assignment_hashes,
            completed_assignment_secs: telemetry.completed_assignment_micros as f64 / 1_000_000.0,
            assignment_enqueue_timeouts: telemetry.assignment_enqueue_timeouts,
            assignment_execution_timeouts: telemetry.assignment_execution_timeouts,
            control_enqueue_timeouts: telemetry.control_enqueue_timeouts,
            control_execution_timeouts: telemetry.control_execution_timeouts,
            peak_assignment_timeout_strikes: telemetry.peak_assignment_timeout_strikes,
            assignment_enqueue_latency_samples: telemetry.assignment_enqueue_latency_samples,
            assignment_enqueue_latency_p95_micros: telemetry.assignment_enqueue_latency_p95_micros,
            assignment_enqueue_latency_max_micros: telemetry.assignment_enqueue_latency_max_micros,
            assignment_execution_latency_samples: telemetry.assignment_execution_latency_samples,
            assignment_execution_latency_p95_micros: telemetry
                .assignment_execution_latency_p95_micros,
            assignment_execution_latency_max_micros: telemetry
                .assignment_execution_latency_max_micros,
            control_enqueue_latency_samples: telemetry.control_enqueue_latency_samples,
            control_enqueue_latency_p95_micros: telemetry.control_enqueue_latency_p95_micros,
            control_enqueue_latency_max_micros: telemetry.control_enqueue_latency_max_micros,
            control_execution_latency_samples: telemetry.control_execution_latency_samples,
            control_execution_latency_p95_micros: telemetry.control_execution_latency_p95_micros,
            control_execution_latency_max_micros: telemetry.control_execution_latency_max_micros,
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
    use serde_json::json;

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

        fn cancel_work(&self) -> Result<()> {
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            Ok(())
        }
    }

    fn sample_report() -> BenchReport {
        BenchReport {
            schema_version: BENCH_REPORT_SCHEMA_VERSION,
            environment: BenchEnvironment {
                seine_version: "0.1.0".to_string(),
                target_triple: "linux/x86_64".to_string(),
                runtime_environment: "native".to_string(),
                build_host: "x86_64-unknown-linux-gnu".to_string(),
                build_profile: "release".to_string(),
                build_opt_level: "3".to_string(),
                rustc_version: "rustc test".to_string(),
                source_fingerprint: "test-source".to_string(),
                build_fingerprint: "test-build".to_string(),
                cpu_brand: Some("test-cpu".to_string()),
                cpu_arch: Some("x86_64".to_string()),
                logical_cores: 8,
                physical_cores: Some(4),
                ..BenchEnvironment::default()
            },
            config_fingerprint: BenchConfigFingerprint {
                backend_event_capacity: 1024,
                hash_poll_ms: 200,
                cpu_profile: "balanced".to_string(),
                cpu_page_mode: "auto".to_string(),
                cpu_hash_batch_size: 64,
                cpu_control_check_interval_hashes: 256,
                cpu_hash_flush_ms: 50,
                cpu_event_dispatch_capacity: 256,
                cpu_autotune_threads: false,
                cpu_autotune_min_threads: 1,
                cpu_autotune_max_threads: None,
                cpu_autotune_secs: 2,
                cpu_auto_threads_cap: 1,
                nvidia_autotune_secs: 5,
                nvidia_autotune_samples: 2,
                nvidia_max_rregcount: None,
                nvidia_max_lanes: None,
                nvidia_dispatch_iters_per_lane: None,
                nvidia_allocation_iters_per_lane: None,
                nvidia_hashes_per_launch_per_lane: 2,
                nvidia_hashes_per_launch_per_lane_was_set: false,
                nvidia_fused_target_check: false,
                nvidia_adaptive_launch_depth: true,
                nvidia_enforce_template_stop: false,
                backend_assign_timeout_ms: 1000,
                backend_assign_timeout_strikes: 1,
                backend_control_timeout_ms: 60_000,
                bench_warmup_rounds: 0,
                allow_best_effort_deadlines: false,
                prefetch_wait_ms: 250,
                tip_listener_join_wait_ms: 250,
                strict_round_accounting: true,
                refresh_secs: 20,
                nonce_iters_per_lane: 1u64 << 36,
                start_nonce: 7,
                work_allocation: "adaptive".to_string(),
                cpu_affinity: "auto".to_string(),
                cpu_affinity_strategy: "linux-os-order".to_string(),
                events_idle_timeout_secs: 90,
                backend_runtime: vec![BenchBackendRuntimeFingerprint {
                    backend_id: 1,
                    backend: "cpu".to_string(),
                    assign_timeout_ms: 1000,
                    assign_timeout_strikes: 1,
                    control_timeout_ms: 60_000,
                }],
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
            warmup_rounds: 0,
            avg_hps: 10.0,
            median_hps: 10.0,
            min_hps: 10.0,
            max_hps: 10.0,
            total_hashes: 10,
            total_counted_hashes: 10,
            total_late_hashes: 0,
            late_hash_pct: 0.0,
            runs: vec![BenchRun {
                round: 1,
                hashes: 10,
                counted_hashes: 10,
                late_hashes: 0,
                late_hash_pct: 0.0,
                elapsed_secs: 1.0,
                configured_secs: 1.0,
                actual_elapsed_secs: 1.0,
                window_overrun_secs: 0.0,
                wall_secs: 1.0,
                startup_secs: 0.0,
                teardown_secs: 0.0,
                fence_secs: 0.0,
                hps: 10.0,
                backend_runs: Vec::new(),
            }],
        }
    }

    #[test]
    fn affinity_strategy_records_platform_policy() {
        assert_eq!(cpu_affinity_strategy_label(CpuAffinityMode::Off), "off");
        #[cfg(target_os = "linux")]
        assert_eq!(
            cpu_affinity_strategy_label(CpuAffinityMode::Auto),
            "linux-os-order"
        );
        #[cfg(target_os = "windows")]
        assert_eq!(
            cpu_affinity_strategy_label(CpuAffinityMode::Auto),
            "windows-physical-first-if-complete"
        );
        #[cfg(target_os = "macos")]
        assert_eq!(
            cpu_affinity_strategy_label(CpuAffinityMode::PcoreOnly),
            "macos-limited-affinity-tags-high-qos"
        );
    }

    #[test]
    fn benchmark_ignores_stale_solution_events() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let mut backends = Vec::new();
        let action = handle_benchmark_backend_event(
            BackendEvent::Solution(crate::backend::MiningSolution {
                epoch: 99,
                nonce: 123,
                hash: None,
                share_binding_id: 0,
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
        baseline.schema_version = BENCH_REPORT_SCHEMA_VERSION.saturating_add(1);

        let issues =
            baseline_compatibility_issues(&current, &baseline, BenchBaselinePolicy::Strict);
        assert!(issues.iter().any(|issue| issue.contains("schema mismatch")));
    }

    #[test]
    fn baseline_compatibility_detects_cpu_page_mode_mismatch() {
        let current = sample_report();
        let mut baseline = sample_report();
        baseline.config_fingerprint.cpu_page_mode = "regular".to_string();

        let issues =
            baseline_compatibility_issues(&current, &baseline, BenchBaselinePolicy::Strict);
        assert!(issues
            .iter()
            .any(|issue| issue.contains("cpu_page_mode mismatch")));
    }

    #[test]
    fn baseline_compatibility_rejects_pre_timing_fix_schema() {
        let current = sample_report();
        let mut baseline = sample_report();
        baseline.schema_version = BENCH_REPORT_SCHEMA_VERSION - 1;

        let issues =
            baseline_compatibility_issues(&current, &baseline, BenchBaselinePolicy::Strict);
        assert!(issues.iter().any(|issue| issue.contains("schema mismatch")));
    }

    #[test]
    fn baseline_parsing_allows_old_fields_but_rejects_comparison() {
        let current = sample_report();
        let mut baseline_value =
            serde_json::to_value(sample_report()).expect("sample report should serialize to JSON");
        baseline_value["schema_version"] = json!(BENCH_REPORT_SCHEMA_VERSION - 1);
        baseline_value
            .as_object_mut()
            .expect("baseline report should be a JSON object")
            .remove("warmup_rounds");
        baseline_value["config_fingerprint"]
            .as_object_mut()
            .expect("config fingerprint should be a JSON object")
            .remove("bench_warmup_rounds");
        baseline_value["runs"][0]
            .as_object_mut()
            .expect("benchmark run should be a JSON object")
            .remove("actual_elapsed_secs");

        let baseline: BenchReport = serde_json::from_value(baseline_value)
            .expect("old-style baseline report should deserialize");
        assert_eq!(baseline.warmup_rounds, 0);
        assert_eq!(baseline.config_fingerprint.bench_warmup_rounds, 0);
        assert_eq!(baseline.runs[0].actual_elapsed_secs, 0.0);

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
    fn strict_baseline_detects_runtime_and_build_identity_mismatch() {
        let mut current = sample_report();
        let baseline = sample_report();
        current.environment.runtime_environment = "wsl2".to_string();
        current.environment.build_fingerprint = "different-build".to_string();

        let issues =
            baseline_compatibility_issues(&current, &baseline, BenchBaselinePolicy::Strict);
        assert!(issues
            .iter()
            .any(|issue| issue.contains("runtime environment mismatch")));
        assert!(issues
            .iter()
            .any(|issue| issue.contains("build/toolchain fingerprint mismatch")));

        let relaxed = baseline_compatibility_issues(
            &current,
            &baseline,
            BenchBaselinePolicy::IgnoreEnvironment,
        );
        assert!(!relaxed
            .iter()
            .any(|issue| issue.contains("runtime environment mismatch")));
    }

    #[test]
    fn kernel_timing_uses_actual_elapsed_and_tracks_overrun() {
        let timing = normalize_bench_timing(10.0, 12.5, 13.0, false);
        assert_eq!(timing.configured_secs, 10.0);
        assert_eq!(timing.actual_elapsed_secs, 12.5);
        assert_eq!(timing.window_overrun_secs, 2.5);
        assert_eq!(timing.wall_secs, 13.0);
        assert_eq!(timing.rate_elapsed_secs, 12.5);
    }

    #[test]
    fn lifecycle_timing_uses_full_wall_elapsed() {
        let timing = normalize_bench_timing(10.0, 10.5, 14.0, true);
        assert_eq!(timing.actual_elapsed_secs, 10.5);
        assert_eq!(timing.wall_secs, 14.0);
        assert_eq!(timing.rate_elapsed_secs, 14.0);
    }
    #[test]
    fn baseline_compatibility_detects_warmup_round_mismatch() {
        let current = sample_report();
        let mut baseline = sample_report();
        baseline.config_fingerprint.bench_warmup_rounds = 2;

        let issues =
            baseline_compatibility_issues(&current, &baseline, BenchBaselinePolicy::Strict);
        assert!(issues
            .iter()
            .any(|issue| issue.contains("bench_warmup_rounds mismatch")));
    }

    #[test]
    fn baseline_compatibility_detects_backend_runtime_profile_mismatch() {
        let current = sample_report();
        let mut baseline = sample_report();
        baseline.config_fingerprint.backend_runtime[0].assign_timeout_ms = 500;

        let issues =
            baseline_compatibility_issues(&current, &baseline, BenchBaselinePolicy::Strict);
        assert!(issues
            .iter()
            .any(|issue| issue.contains("backend_runtime mismatch")));
    }

    #[test]
    fn baseline_compatibility_detects_cpu_tuning_mismatch_for_schema_v6() {
        let current = sample_report();
        let mut baseline = sample_report();
        baseline.config_fingerprint.cpu_hash_batch_size = 32;

        let issues =
            baseline_compatibility_issues(&current, &baseline, BenchBaselinePolicy::Strict);
        assert!(issues
            .iter()
            .any(|issue| issue.contains("cpu_hash_batch_size mismatch")));
    }

    #[test]
    fn baseline_compatibility_ignores_context_only_timeout_fields() {
        let current = sample_report();
        let mut baseline = sample_report();
        baseline.config_fingerprint.prefetch_wait_ms = 999;
        baseline.config_fingerprint.tip_listener_join_wait_ms = 888;
        baseline.config_fingerprint.events_idle_timeout_secs = 777;

        let issues =
            baseline_compatibility_issues(&current, &baseline, BenchBaselinePolicy::Strict);
        assert!(!issues
            .iter()
            .any(|issue| issue.contains("prefetch_wait_ms")));
        assert!(!issues
            .iter()
            .any(|issue| issue.contains("tip_listener_join_wait_ms")));
        assert!(!issues
            .iter()
            .any(|issue| issue.contains("events_idle_timeout_secs")));
    }
    #[test]
    fn backend_round_stats_include_zero_hash_backends() {
        let backends = vec![BackendSlot {
            id: 7,
            backend: Arc::new(NoopBackend),
            lanes: 1,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
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
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
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
                runtime_policy: crate::miner::BackendRuntimePolicy::default(),
                capabilities: crate::backend::BackendCapabilities::default(),
            },
            BackendSlot {
                id: 9,
                backend: Arc::new(NoopBackend),
                lanes: 2,
                runtime_policy: crate::miner::BackendRuntimePolicy::default(),
                capabilities: crate::backend::BackendCapabilities::default(),
            },
        ];

        let identity = worker_benchmark_identity(&backends, 0.0);
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
                runtime_policy: crate::miner::BackendRuntimePolicy::default(),
                capabilities: crate::backend::BackendCapabilities::default(),
            },
            BackendSlot {
                id: 9,
                backend: Arc::new(NoopBackend),
                lanes: 1,
                runtime_policy: crate::miner::BackendRuntimePolicy::default(),
                capabilities: crate::backend::BackendCapabilities::default(),
            },
        ];
        let current = vec![BackendSlot {
            id: 2,
            backend: Arc::new(NoopBackend),
            lanes: 1,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];
        let identity = worker_benchmark_identity(&expected, 0.0);

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
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];
        let current = vec![BackendSlot {
            id: 2,
            backend: Arc::new(NoopBackend),
            lanes: 1,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];
        let identity = worker_benchmark_identity(&expected, 0.0);

        let err = ensure_worker_topology_identity(&current, &identity, "round 1")
            .expect_err("lane mismatch should fail benchmark");
        let rendered = format!("{err:#}");
        assert!(rendered.contains("expected_lanes"));
        assert!(rendered.contains("current_lanes"));
    }
}
