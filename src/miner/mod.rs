mod auth;
mod backend_control;
mod backend_executor;
mod bench;
mod hash_poll;
mod hashrate_tracker;
mod mining;
mod mining_tui;
mod round_control;
mod round_driver;
mod runtime;
mod scheduler;
mod solution_cache;
mod stats;
mod submit;
mod template_prefetch;
mod tip;
mod tui;
mod ui;
mod wallet;
mod work_allocator;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Result};
use blocknet_pow_spec::CPU_LANE_MEMORY_BYTES;
use crossbeam_channel::{bounded, Receiver, Sender};
use serde::{Deserialize, Serialize};

use crate::api::ApiClient;
use crate::backend::cpu::{CpuBackend, CpuBackendTuning};
use crate::backend::metal::MetalBackend;
use crate::backend::nvidia::{NvidiaBackend, NvidiaBackendTuningOptions};
use crate::backend::{
    normalize_backend_capabilities, BackendCapabilities, BackendEvent, BackendExecutionModel,
    BackendInstanceId, BackendTelemetry, BenchBackend, DeadlineSupport, PowBackend,
    PreemptionGranularity, WORK_ID_MAX,
};
use crate::config::{
    BackendKind, BackendSpec, Config, CpuPerformanceProfile, UiMode, WorkAllocation,
};
use scheduler::NonceReservation;
use stats::{format_hashrate, Stats};
use tui::{new_tui_state, TuiState};
use ui::{info, warn};

const TEMPLATE_RETRY_DELAY: Duration = Duration::from_secs(2);
const MIN_EVENT_WAIT: Duration = Duration::from_millis(1);
const BACKEND_EVENT_SOURCE_CAPACITY_MAX: usize = 256;
const CPU_AUTOTUNE_RECORD_SCHEMA_VERSION: u32 = 3;
const CPU_AUTOTUNE_LINEAR_SCAN_MAX_CANDIDATES: usize = 8;
const CPU_AUTOTUNE_FINAL_SWEEP_RADIUS: usize = 2;
const CPU_AUTOTUNE_TARGET_HASHES_PER_CANDIDATE: u64 = 30;
const CPU_AUTOTUNE_MAX_SAMPLE_SECS_PER_CANDIDATE: u64 = 60;
const CPU_AUTOTUNE_BALANCED_PEAK_FLOOR_FRAC: f64 = 0.95;
const CPU_AUTOTUNE_EFFICIENCY_PEAK_FLOOR_FRAC: f64 = 0.75;

#[derive(Debug, Clone, Copy)]
struct BackendRuntimePolicy {
    assignment_timeout: Duration,
    assignment_timeout_strikes: u32,
    control_timeout: Duration,
}

impl Default for BackendRuntimePolicy {
    fn default() -> Self {
        Self {
            assignment_timeout: Duration::from_millis(1_000),
            assignment_timeout_strikes: 3,
            control_timeout: Duration::from_millis(60_000),
        }
    }
}

struct BackendSlot {
    id: BackendInstanceId,
    backend: Arc<dyn PowBackend>,
    lanes: u64,
    runtime_policy: BackendRuntimePolicy,
    capabilities: BackendCapabilities,
}

struct DistributeWorkOptions<'a> {
    epoch: u64,
    work_id: u64,
    header_base: Arc<[u8]>,
    target: [u8; 32],
    reservation: NonceReservation,
    stop_at: Instant,
    backend_weights: Option<&'a BTreeMap<BackendInstanceId, f64>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct BackendRoundTelemetry {
    dropped_events: u64,
    completed_assignments: u64,
    completed_assignment_hashes: u64,
    completed_assignment_micros: u64,
    peak_active_lanes: u64,
    peak_pending_work: u64,
    peak_inflight_assignment_hashes: u64,
    peak_inflight_assignment_micros: u64,
    assignment_enqueue_timeouts: u64,
    assignment_execution_timeouts: u64,
    control_enqueue_timeouts: u64,
    control_execution_timeouts: u64,
    peak_assignment_timeout_strikes: u32,
    assignment_enqueue_latency_samples: u64,
    assignment_enqueue_latency_p95_micros: u64,
    assignment_enqueue_latency_max_micros: u64,
    assignment_execution_latency_samples: u64,
    assignment_execution_latency_p95_micros: u64,
    assignment_execution_latency_max_micros: u64,
    control_enqueue_latency_samples: u64,
    control_enqueue_latency_p95_micros: u64,
    control_enqueue_latency_max_micros: u64,
    control_execution_latency_samples: u64,
    control_execution_latency_p95_micros: u64,
    control_execution_latency_max_micros: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CpuAutotuneRecord {
    schema_version: u32,
    profile: String,
    affinity: String,
    cpu_instances: usize,
    min_threads: usize,
    max_threads: usize,
    selected_threads: usize,
    measured_hps: f64,
    autotune_secs: u64,
    timestamp_unix_secs: u64,
}

#[derive(Debug, Clone, Copy)]
struct CpuAutotuneMeasurement {
    hashes: u64,
    wall_secs: f64,
    hps: f64,
}

#[derive(Debug, Clone, Copy)]
struct CpuAutotuneSelection {
    selected_threads: usize,
    selected_hps: f64,
    peak_threads: usize,
    peak_hps: f64,
    peak_floor_frac: f64,
}

pub fn run(cfg: &Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    if cfg.bench {
        return bench::run_benchmark(cfg, shutdown.as_ref());
    }
    let runtime_cfg = prepare_runtime_config(cfg, shutdown.as_ref(), RuntimeMode::Mining)?;
    if shutdown.load(Ordering::SeqCst) {
        return Ok(());
    }
    let cfg = &runtime_cfg;

    let backend_executor = backend_executor::BackendExecutor::new();

    let token = cfg
        .token
        .clone()
        .ok_or_else(|| anyhow!("missing API token in mining mode"))?;
    let client = ApiClient::new(
        cfg.api_url.clone(),
        token,
        cfg.request_timeout,
        cfg.events_stream_timeout,
        cfg.events_idle_timeout,
    )?;

    let backend_instances = build_backend_instances(cfg);
    let (mut backends, backend_events) =
        activate_backends(backend_instances, cfg.backend_event_capacity, cfg, shutdown.as_ref())?;
    enforce_deadline_policy(
        &mut backends,
        cfg.allow_best_effort_deadlines,
        RuntimeMode::Mining,
        &backend_executor,
    )?;
    let total_lanes = total_lanes(&backends);
    let cpu_lanes = cpu_lane_count(&backends);
    let cpu_ram_gib =
        (cpu_lanes as f64 * CPU_LANE_MEMORY_BYTES as f64) / (1024.0 * 1024.0 * 1024.0);

    let tui_state = if should_enable_tui(cfg) {
        Some(build_tui_state(cfg, &backends))
    } else {
        None
    };

    if let Some(hint) = cfg.nvidia_hint {
        info("HINT", hint);
    }

    info(
        "MINER",
        format!(
            "effective | api={} auth={} backends={} cpu_threads={} ui={}",
            cfg.api_url,
            auth_source_label(cfg),
            backend_names(&backends),
            cpu_threads_summary(&backends),
            if tui_state.is_some() { "tui" } else { "plain" }
        ),
    );

    info(
        "MINER",
        format!(
            "starting | {} | {} lanes | ~{:.1} GiB RAM",
            backend_names(&backends),
            total_lanes,
            cpu_ram_gib
        ),
    );
    info(
        "MINER",
        format!("preemption | {}", backend_preemption_profiles(&backends)),
    );
    info(
        "MINER",
        format!(
            "allocation | {}",
            match cfg.work_allocation {
                WorkAllocation::Static => "static-lanes",
                WorkAllocation::Adaptive => "adaptive-weighted",
            }
        ),
    );
    if cfg.work_allocation == WorkAllocation::Static && cfg.sub_round_rebalance_interval.is_some() {
        warn(
            "MINER",
            "--sub-round-rebalance-ms is set but --work-allocation=static; in-round rebalance is disabled",
        );
    }
    info(
        "MINER",
        format!(
            "chunking | {}",
            backend_chunk_profiles(&backends, cfg.nonce_iters_per_lane)
        ),
    );
    let append_semantics = backends
        .iter()
        .filter_map(|slot| {
            let semantics = backend_capabilities(slot).assignment_semantics;
            if semantics == crate::backend::AssignmentSemantics::Append {
                Some(format!("{}#{}", slot.backend.name(), slot.id))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if !append_semantics.is_empty() {
        warn(
            "MINER",
            format!(
                "append assignment semantics reported by {}; relaxed mode will force round-boundary cancels",
                append_semantics.join(", ")
            ),
        );
    }
    info(
        "MINER",
        format!(
            "hash-poll | configured={}ms effective={}ms",
            cfg.hash_poll_interval.as_millis(),
            effective_hash_poll_interval(&backends, cfg.hash_poll_interval).as_millis(),
        ),
    );
    info(
        "MINER",
        format!(
            "cpu-profile | {} | auto-cap={} | autotune={} range={}..{} secs={}",
            cpu_profile_label(cfg.cpu_profile),
            cfg.cpu_auto_threads_cap,
            if cfg.cpu_autotune_threads {
                "on"
            } else {
                "off"
            },
            cfg.cpu_autotune_min_threads,
            cfg.cpu_autotune_max_threads
                .unwrap_or(cfg.cpu_auto_threads_cap.max(1)),
            cfg.cpu_autotune_secs,
        ),
    );
    info(
        "MINER",
        format!(
            "cpu-tuning | hash_batch={} control_check={} hash_flush={}ms event_q={}",
            cfg.cpu_hash_batch_size,
            cfg.cpu_control_check_interval_hashes,
            cfg.cpu_hash_flush_interval.as_millis(),
            cfg.cpu_event_dispatch_capacity,
        ),
    );
    let sub_round_rebalance = cfg
        .sub_round_rebalance_interval
        .map(|interval| format!("{}ms", interval.as_millis()))
        .unwrap_or_else(|| "off".to_string());
    info(
        "MINER",
        format!(
            "timeouts | assign={}ms assign_strikes={} control={}ms prefetch_wait={}ms tip_join_wait={}ms submit_join_wait={}ms sub_round_rebalance={}",
            cfg.backend_assign_timeout.as_millis(),
            cfg.backend_assign_timeout_strikes,
            cfg.backend_control_timeout.as_millis(),
            cfg.prefetch_wait.as_millis(),
            cfg.tip_listener_join_wait.as_millis(),
            cfg.submit_join_wait.as_millis(),
            sub_round_rebalance,
        ),
    );
    info(
        "MINER",
        format!("timeout-policy | {}", backend_timeout_profiles(&backends)),
    );
    if cfg.strict_round_accounting {
        let constrained: Vec<String> = backends
            .iter()
            .filter_map(|slot| {
                let granularity = slot.backend.preemption_granularity();
                if matches!(granularity, PreemptionGranularity::Hashes(1)) {
                    None
                } else {
                    Some(format!(
                        "{}#{}={}",
                        slot.backend.name(),
                        slot.id,
                        granularity.describe()
                    ))
                }
            })
            .collect();
        if !constrained.is_empty() {
            warn(
                "MINER",
                format!(
                    "strict accounting fence latency depends on backend preemption ({})",
                    constrained.join(", ")
                ),
            );
        }
        let best_effort_deadlines: Vec<String> = backends
            .iter()
            .filter_map(|slot| {
                let capabilities = backend_capabilities(slot);
                if capabilities.deadline_support == DeadlineSupport::BestEffort {
                    Some(format!("{}#{}", slot.backend.name(), slot.id))
                } else {
                    None
                }
            })
            .collect();
        if !best_effort_deadlines.is_empty() {
            warn(
                "MINER",
                format!(
                    "deadline enforcement is best-effort for {}",
                    best_effort_deadlines.join(", ")
                ),
            );
        }
    }

    let tip_listener = if cfg.sse_enabled {
        Some(mining::spawn_tip_listener(
            client.clone(),
            Arc::clone(&shutdown),
            cfg.refresh_on_same_height,
            cfg.token_cookie_path.clone(),
        ))
    } else {
        None
    };

    let result = mining::run_mining_loop(
        cfg,
        &client,
        Arc::clone(&shutdown),
        mining::MiningRuntimeBackends {
            backends: &mut backends,
            backend_events: &backend_events,
            backend_executor: &backend_executor,
        },
        tui_state,
        tip_listener.as_ref().map(mining::TipListener::signal),
    );

    let shutdown_requested = shutdown.load(Ordering::SeqCst);
    info("MINER", "shutting down: stopping backends...");
    let stop_t = Instant::now();
    stop_backend_slots(
        &mut backends,
        &backend_executor,
        cfg.backend_control_timeout,
        "BACKEND",
    );
    info(
        "MINER",
        format!("backends stopped in {:.1}s", stop_t.elapsed().as_secs_f64()),
    );
    shutdown.store(true, Ordering::SeqCst);
    if let Some(listener) = tip_listener {
        if shutdown_requested {
            listener.detach();
        } else if !listener.join_for(cfg.tip_listener_join_wait) {
            warn(
                "EVENTS",
                format!(
                    "tip listener shutdown exceeded {}ms; detached",
                    cfg.tip_listener_join_wait.as_millis()
                ),
            );
        }
    }
    result
}

fn should_enable_tui(cfg: &Config) -> bool {
    match cfg.ui_mode {
        UiMode::Plain => false,
        UiMode::Tui => true,
        UiMode::Auto => std::io::stdout().is_terminal() && std::io::stderr().is_terminal(),
    }
}

fn build_tui_state(cfg: &Config, backends: &[BackendSlot]) -> TuiState {
    let tui_state = new_tui_state();
    if let Ok(mut s) = tui_state.lock() {
        s.api_url = cfg.api_url.clone();
        s.threads = cfg.threads;
        s.refresh_secs = cfg.refresh_interval.as_secs();
        s.sse_enabled = cfg.sse_enabled;
        s.backends_desc = backend_descriptions(backends);
        s.accounting = if cfg.strict_round_accounting {
            "strict".to_string()
        } else {
            "relaxed".to_string()
        };
        s.version = format!("v{}", env!("CARGO_PKG_VERSION"));
        if let Some(hint) = cfg.nvidia_hint {
            s.push_log(tui::LogEntry {
                elapsed_secs: 0.0,
                level: tui::LogLevel::Info,
                tag: "HINT".to_string(),
                message: hint.to_string(),
            });
        }
    }
    tui_state
}

pub(super) fn prepare_runtime_config(
    cfg: &Config,
    shutdown: &AtomicBool,
    mode: RuntimeMode,
) -> Result<Config> {
    let mut runtime_cfg = cfg.clone();
    maybe_autotune_cpu_threads(&mut runtime_cfg, shutdown, mode)?;
    Ok(runtime_cfg)
}

fn maybe_autotune_cpu_threads(
    cfg: &mut Config,
    shutdown: &AtomicBool,
    mode: RuntimeMode,
) -> Result<()> {
    if !cfg.cpu_autotune_threads {
        return Ok(());
    }

    let tag = runtime_mode_tag(mode);
    let cpu_instance_count = cfg
        .backend_specs
        .iter()
        .filter(|spec| spec.kind == BackendKind::Cpu)
        .count();
    if cpu_instance_count == 0 {
        info(tag, "cpu-autotune | skipped (no CPU backend instances)");
        return Ok(());
    }

    let safe_threads_cap = cfg.cpu_auto_threads_cap.max(1);
    let mut min_threads = cfg.cpu_autotune_min_threads.max(1);
    if min_threads > safe_threads_cap {
        warn(
            tag,
            format!(
                "cpu-autotune | clamping min threads {} -> {} (safe cap)",
                min_threads, safe_threads_cap
            ),
        );
        min_threads = safe_threads_cap;
    }

    let requested_max_threads = cfg.cpu_autotune_max_threads.unwrap_or(safe_threads_cap);
    let mut max_threads = requested_max_threads.max(min_threads);
    if max_threads > safe_threads_cap {
        warn(
            tag,
            format!(
                "cpu-autotune | clamping max threads {} -> {} (safe cap)",
                max_threads, safe_threads_cap
            ),
        );
        max_threads = safe_threads_cap;
    }

    let autotune_secs = cfg.cpu_autotune_secs.max(1);
    if let Some(record) = load_cpu_autotune_record(
        cfg,
        tag,
        cpu_instance_count,
        min_threads,
        max_threads,
        autotune_secs,
    ) {
        apply_cpu_threads(cfg, record.selected_threads);
        info(
            tag,
            format!(
                "cpu-autotune | using cached result: {} threads at {} (from {})",
                record.selected_threads,
                format_hashrate(record.measured_hps),
                cfg.cpu_autotune_config_path.display()
            ),
        );
        return Ok(());
    }

    info(
        tag,
        format!(
            "cpu-autotune | finding the fastest thread count for your CPU (profile={}, testing {}..{} threads, ~{}s per candidate)",
            cpu_profile_label(cfg.cpu_profile),
            min_threads,
            max_threads,
            autotune_secs,
        ),
    );
    info(
        tag,
        "cpu-autotune | this only runs once — results are cached for future launches",
    );

    let candidate_count = max_threads.saturating_sub(min_threads).saturating_add(1);
    let mut measurements = BTreeMap::<usize, CpuAutotuneMeasurement>::new();
    let mut interrupted = false;

    if candidate_count <= CPU_AUTOTUNE_LINEAR_SCAN_MAX_CANDIDATES {
        info(
            tag,
            format!("cpu-autotune | testing all {candidate_count} thread counts in range"),
        );
        for (step, threads) in (min_threads..=max_threads).enumerate() {
            info(
                tag,
                format!(
                    "cpu-autotune | [{}/{}] benchmarking {} threads ...",
                    step + 1,
                    candidate_count,
                    threads
                ),
            );
            if shutdown.load(Ordering::Relaxed)
                || !measure_cpu_autotune_candidate(
                    cfg,
                    tag,
                    shutdown,
                    threads,
                    autotune_secs,
                    &mut measurements,
                )?
            {
                interrupted = true;
                break;
            }
        }
    } else {
        info(
            tag,
            format!(
                "cpu-autotune | {candidate_count} thread counts to search — using binary search to narrow down quickly",
            ),
        );
        info(
            tag,
            "cpu-autotune | phase 1/2: narrowing down the optimal range ...",
        );
        let mut lo = min_threads;
        let mut hi = max_threads;
        let mut binary_step: usize = 0;
        while lo < hi {
            if shutdown.load(Ordering::Relaxed) {
                interrupted = true;
                break;
            }
            binary_step += 1;
            let mid = lo + (hi - lo) / 2;
            let right = (mid + 1).min(max_threads);
            info(
                tag,
                format!(
                    "cpu-autotune | phase 1 step {binary_step}: comparing {mid} vs {right} threads (range {}..{}) ...",
                    lo, hi
                ),
            );
            if !measure_cpu_autotune_candidate(
                cfg,
                tag,
                shutdown,
                mid,
                autotune_secs,
                &mut measurements,
            )? || !measure_cpu_autotune_candidate(
                cfg,
                tag,
                shutdown,
                right,
                autotune_secs,
                &mut measurements,
            )? {
                interrupted = true;
                break;
            }
            let mid_hps = measurements.get(&mid).map(|m| m.hps).unwrap_or(0.0);
            let right_hps = measurements.get(&right).map(|m| m.hps).unwrap_or(0.0);
            if right_hps >= mid_hps {
                lo = right;
            } else {
                hi = mid;
            }
        }

        if !interrupted {
            let mut final_candidates = BTreeSet::new();
            final_candidates.insert(min_threads);
            final_candidates.insert(max_threads);
            final_candidates.insert(lo);
            let sweep_min = lo
                .saturating_sub(CPU_AUTOTUNE_FINAL_SWEEP_RADIUS)
                .max(min_threads);
            let sweep_max = lo
                .saturating_add(CPU_AUTOTUNE_FINAL_SWEEP_RADIUS)
                .min(max_threads);
            for threads in sweep_min..=sweep_max {
                final_candidates.insert(threads);
            }

            // Remove already-measured candidates so the step counter is accurate.
            let final_candidates: Vec<usize> = final_candidates
                .into_iter()
                .filter(|t| !measurements.contains_key(t))
                .collect();
            if !final_candidates.is_empty() {
                info(
                    tag,
                    format!(
                        "cpu-autotune | phase 2/2: fine-tuning around {} threads ({} candidates left) ...",
                        lo,
                        final_candidates.len()
                    ),
                );
            }

            for (step, threads) in final_candidates.iter().enumerate() {
                info(
                    tag,
                    format!(
                        "cpu-autotune | [{}/{}] benchmarking {} threads ...",
                        step + 1,
                        final_candidates.len(),
                        threads
                    ),
                );
                if shutdown.load(Ordering::Relaxed)
                    || !measure_cpu_autotune_candidate(
                        cfg,
                        tag,
                        shutdown,
                        *threads,
                        autotune_secs,
                        &mut measurements,
                    )?
                {
                    interrupted = true;
                    break;
                }
            }
        }
    }

    if measurements.is_empty() {
        warn(
            tag,
            "cpu-autotune | no measurements collected; keeping configured thread count",
        );
        return Ok(());
    }

    if interrupted {
        warn(tag, "cpu-autotune | interrupted by shutdown request");
        return Ok(());
    }

    let selection = select_cpu_autotune_candidate(cfg.cpu_profile, &measurements).unwrap_or(
        CpuAutotuneSelection {
            selected_threads: cfg.threads.max(1),
            selected_hps: 0.0,
            peak_threads: cfg.threads.max(1),
            peak_hps: 0.0,
            peak_floor_frac: 1.0,
        },
    );
    apply_cpu_threads(cfg, selection.selected_threads);
    if selection.selected_threads != selection.peak_threads {
        let saved_threads = selection
            .peak_threads
            .saturating_sub(selection.selected_threads);
        let saved_ram_gib =
            (saved_threads as f64 * CPU_LANE_MEMORY_BYTES as f64) / (1024.0 * 1024.0 * 1024.0);
        info(
            tag,
            format!(
                "cpu-autotune | done — selected {} threads at {} (peak {} threads at {}; profile={} keeps >= {:.0}% peak, saves ~{:.1} GiB RAM per CPU instance)",
                selection.selected_threads,
                format_hashrate(selection.selected_hps),
                selection.peak_threads,
                format_hashrate(selection.peak_hps),
                cpu_profile_label(cfg.cpu_profile),
                selection.peak_floor_frac * 100.0,
                saved_ram_gib,
            ),
        );
    } else {
        info(
            tag,
            format!(
                "cpu-autotune | done — best: {} threads at {} per CPU instance",
                selection.selected_threads,
                format_hashrate(selection.selected_hps)
            ),
        );
    }
    if let Err(err) = persist_cpu_autotune_record(
        cfg,
        cpu_instance_count,
        min_threads,
        max_threads,
        selection.selected_threads,
        selection.selected_hps,
        autotune_secs,
    ) {
        warn(
            tag,
            format!(
                "cpu-autotune | failed to persist {} ({err:#})",
                cfg.cpu_autotune_config_path.display()
            ),
        );
    } else {
        info(
            tag,
            format!(
                "cpu-autotune | wrote {}",
                cfg.cpu_autotune_config_path.display()
            ),
        );
    }
    Ok(())
}

fn cpu_autotune_peak_floor_frac(profile: CpuPerformanceProfile) -> f64 {
    match profile {
        CpuPerformanceProfile::Throughput => 1.0,
        CpuPerformanceProfile::Balanced => CPU_AUTOTUNE_BALANCED_PEAK_FLOOR_FRAC,
        CpuPerformanceProfile::Efficiency => CPU_AUTOTUNE_EFFICIENCY_PEAK_FLOOR_FRAC,
    }
}

fn select_peak_autotune_candidate(
    measurements: &BTreeMap<usize, CpuAutotuneMeasurement>,
) -> Option<(usize, CpuAutotuneMeasurement)> {
    measurements
        .iter()
        .max_by(|(left_threads, left), (right_threads, right)| {
            // Prefer lower thread counts when measured H/s ties.
            left.hps
                .total_cmp(&right.hps)
                .then_with(|| right_threads.cmp(left_threads))
        })
        .map(|(threads, measurement)| (*threads, *measurement))
}

fn select_cpu_autotune_candidate(
    profile: CpuPerformanceProfile,
    measurements: &BTreeMap<usize, CpuAutotuneMeasurement>,
) -> Option<CpuAutotuneSelection> {
    let (peak_threads, peak) = select_peak_autotune_candidate(measurements)?;
    let peak_floor_frac = cpu_autotune_peak_floor_frac(profile).clamp(0.0, 1.0);
    let min_acceptable_hps = peak.hps * peak_floor_frac;

    let (selected_threads, selected) = if peak_floor_frac >= 1.0 || peak.hps <= 0.0 {
        (peak_threads, peak)
    } else {
        measurements
            .iter()
            .find(|(_, measurement)| measurement.hps + f64::EPSILON >= min_acceptable_hps)
            .map(|(threads, measurement)| (*threads, *measurement))
            .unwrap_or((peak_threads, peak))
    };

    Some(CpuAutotuneSelection {
        selected_threads,
        selected_hps: selected.hps,
        peak_threads,
        peak_hps: peak.hps,
        peak_floor_frac,
    })
}

fn measure_cpu_autotune_candidate(
    cfg: &Config,
    tag: &str,
    shutdown: &AtomicBool,
    threads: usize,
    window_secs: u64,
    measurements: &mut BTreeMap<usize, CpuAutotuneMeasurement>,
) -> Result<bool> {
    if measurements.contains_key(&threads) {
        return Ok(true);
    }

    let Some(measurement) = bench_cpu_autotune_candidate(cfg, shutdown, threads, window_secs)?
    else {
        return Ok(false);
    };
    info(
        tag,
        format!(
            "cpu-autotune |   -> {} threads: {} ({} hashes in {:.1}s)",
            threads,
            format_hashrate(measurement.hps),
            measurement.hashes,
            measurement.wall_secs,
        ),
    );
    measurements.insert(threads, measurement);
    Ok(true)
}

fn bench_cpu_autotune_candidate(
    cfg: &Config,
    shutdown: &AtomicBool,
    threads: usize,
    window_secs: u64,
) -> Result<Option<CpuAutotuneMeasurement>> {
    if shutdown.load(Ordering::Relaxed) {
        return Ok(None);
    }

    let backend = CpuBackend::with_tuning(
        threads,
        cfg.cpu_affinity,
        CpuBackendTuning {
            hash_batch_size: cfg.cpu_hash_batch_size,
            control_check_interval_hashes: cfg.cpu_control_check_interval_hashes,
            hash_flush_interval: cfg.cpu_hash_flush_interval,
            event_dispatch_capacity: cfg.cpu_event_dispatch_capacity,
        },
    );

    let sample_window_secs = window_secs.max(1);
    let mut sampled_secs = 0u64;
    let mut sampled_hashes = 0u64;
    let mut wall_secs = 0.0f64;

    while sampled_secs < CPU_AUTOTUNE_MAX_SAMPLE_SECS_PER_CANDIDATE
        && sampled_hashes < CPU_AUTOTUNE_TARGET_HASHES_PER_CANDIDATE
    {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let remaining_secs = CPU_AUTOTUNE_MAX_SAMPLE_SECS_PER_CANDIDATE - sampled_secs;
        let chunk_secs = sample_window_secs.min(remaining_secs).max(1);
        let started_at = Instant::now();
        let chunk_hashes = BenchBackend::kernel_bench(&backend, chunk_secs, shutdown)?;
        wall_secs += started_at.elapsed().as_secs_f64();
        sampled_secs = sampled_secs.saturating_add(chunk_secs);
        sampled_hashes = sampled_hashes.saturating_add(chunk_hashes);
    }

    if sampled_secs == 0 {
        return Ok(None);
    }

    // Use elapsed wall time for throughput so short windows don't overcount hashes
    // that finish after the target window boundary.
    let hps = (sampled_hashes as f64) / wall_secs.max(f64::EPSILON);
    Ok(Some(CpuAutotuneMeasurement {
        hashes: sampled_hashes,
        wall_secs,
        hps,
    }))
}

fn apply_cpu_threads(cfg: &mut Config, threads: usize) {
    let threads = threads.max(1);
    cfg.threads = threads;
    for spec in cfg
        .backend_specs
        .iter_mut()
        .filter(|spec| spec.kind == BackendKind::Cpu)
    {
        spec.cpu_threads = Some(threads);
    }
}

fn load_cpu_autotune_record(
    cfg: &Config,
    tag: &str,
    cpu_instance_count: usize,
    min_threads: usize,
    max_threads: usize,
    autotune_secs: u64,
) -> Option<CpuAutotuneRecord> {
    let raw = match fs::read_to_string(&cfg.cpu_autotune_config_path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        Err(err) => {
            warn(
                tag,
                format!(
                    "cpu-autotune | failed reading {} ({err})",
                    cfg.cpu_autotune_config_path.display()
                ),
            );
            return None;
        }
    };

    let record = match serde_json::from_str::<CpuAutotuneRecord>(&raw) {
        Ok(record) => record,
        Err(err) => {
            warn(
                tag,
                format!(
                    "cpu-autotune | invalid config {} ({err})",
                    cfg.cpu_autotune_config_path.display()
                ),
            );
            return None;
        }
    };

    if record.schema_version != CPU_AUTOTUNE_RECORD_SCHEMA_VERSION
        || record.profile != cpu_profile_label(cfg.cpu_profile)
        || record.affinity != cpu_affinity_label(cfg.cpu_affinity)
        || record.cpu_instances != cpu_instance_count
        || record.autotune_secs != autotune_secs
        || record.selected_threads < min_threads
        || record.selected_threads > max_threads
    {
        return None;
    }

    Some(record)
}

fn persist_cpu_autotune_record(
    cfg: &Config,
    cpu_instance_count: usize,
    min_threads: usize,
    max_threads: usize,
    selected_threads: usize,
    measured_hps: f64,
    autotune_secs: u64,
) -> Result<()> {
    if let Some(parent) = cfg.cpu_autotune_config_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let record = CpuAutotuneRecord {
        schema_version: CPU_AUTOTUNE_RECORD_SCHEMA_VERSION,
        profile: cpu_profile_label(cfg.cpu_profile).to_string(),
        affinity: cpu_affinity_label(cfg.cpu_affinity).to_string(),
        cpu_instances: cpu_instance_count,
        min_threads,
        max_threads,
        selected_threads,
        measured_hps,
        autotune_secs,
        timestamp_unix_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0),
    };
    let payload = serde_json::to_string_pretty(&record)?;
    fs::write(&cfg.cpu_autotune_config_path, payload)?;
    Ok(())
}

fn runtime_mode_tag(mode: RuntimeMode) -> &'static str {
    match mode {
        RuntimeMode::Mining => "MINER",
        RuntimeMode::Bench => "BENCH",
    }
}

fn cpu_profile_label(profile: CpuPerformanceProfile) -> &'static str {
    match profile {
        CpuPerformanceProfile::Balanced => "balanced",
        CpuPerformanceProfile::Throughput => "throughput",
        CpuPerformanceProfile::Efficiency => "efficiency",
    }
}

fn cpu_affinity_label(affinity: crate::config::CpuAffinityMode) -> &'static str {
    match affinity {
        crate::config::CpuAffinityMode::Off => "off",
        crate::config::CpuAffinityMode::Auto => "auto",
    }
}

fn build_backend_instances(cfg: &Config) -> Vec<(BackendSpec, Arc<dyn PowBackend>)> {
    cfg.backend_specs
        .iter()
        .copied()
        .map(|backend_spec| {
            let backend = match backend_spec.kind {
                BackendKind::Cpu => Arc::new(CpuBackend::with_tuning(
                    backend_spec.cpu_threads.unwrap_or(cfg.threads),
                    backend_spec.cpu_affinity.unwrap_or(cfg.cpu_affinity),
                    CpuBackendTuning {
                        hash_batch_size: cfg.cpu_hash_batch_size,
                        control_check_interval_hashes: cfg.cpu_control_check_interval_hashes,
                        hash_flush_interval: cfg.cpu_hash_flush_interval,
                        event_dispatch_capacity: cfg.cpu_event_dispatch_capacity,
                    },
                )) as Arc<dyn PowBackend>,
                BackendKind::Nvidia => Arc::new(NvidiaBackend::new(
                    backend_spec.device_index,
                    cfg.nvidia_autotune_config_path.clone(),
                    cfg.nvidia_autotune_secs,
                    NvidiaBackendTuningOptions {
                        max_rregcount_override: cfg.nvidia_max_rregcount,
                        max_lanes_override: cfg.nvidia_max_lanes,
                        autotune_samples: cfg.nvidia_autotune_samples,
                        dispatch_iters_per_lane: cfg.nvidia_dispatch_iters_per_lane,
                        allocation_iters_per_lane: cfg.nvidia_allocation_iters_per_lane,
                        hashes_per_launch_per_lane: cfg.nvidia_hashes_per_launch_per_lane,
                        fused_target_check: cfg.nvidia_fused_target_check,
                        adaptive_launch_depth: cfg.nvidia_adaptive_launch_depth,
                        enforce_template_stop: cfg.nvidia_enforce_template_stop,
                    },
                )) as Arc<dyn PowBackend>,
                BackendKind::Metal => Arc::new(MetalBackend::new(
                    cfg.metal_max_lanes,
                    cfg.metal_hashes_per_launch_per_lane,
                )) as Arc<dyn PowBackend>,
            };
            (backend_spec, backend)
        })
        .collect()
}

fn activate_backends(
    mut backends: Vec<(BackendSpec, Arc<dyn PowBackend>)>,
    event_capacity: usize,
    cfg: &Config,
    shutdown: &AtomicBool,
) -> Result<(Vec<BackendSlot>, Receiver<BackendEvent>)> {
    let mut active = Vec::new();
    let mut backend_event_sources = Vec::new();
    let (event_tx, event_rx) = bounded::<BackendEvent>(event_capacity.max(1));
    let mut next_backend_id: BackendInstanceId = 1;
    let per_backend_event_capacity = event_capacity.clamp(8, BACKEND_EVENT_SOURCE_CAPACITY_MAX);

    for (backend_spec, backend) in backends.drain(..) {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let backend_id = next_backend_id;
        next_backend_id = next_backend_id.saturating_add(1);
        let backend_name = backend.name();
        backend.set_instance_id(backend_id);
        let (backend_event_tx, backend_event_rx) =
            bounded::<BackendEvent>(per_backend_event_capacity);
        backend.set_event_sink(backend_event_tx);
        let backend_start_detail = match backend_name {
            "cpu" => {
                let lanes = backend.lanes();
                let ram_gib =
                    (lanes as f64 * CPU_LANE_MEMORY_BYTES as f64) / (1024.0 * 1024.0 * 1024.0);
                format!(
                    "{backend_name}: initializing {lanes} workers (~{ram_gib:.1} GiB RAM, precomputing refs)...",
                )
            }
            "nvidia" => format!(
                "{backend_name}: compiling CUDA kernel and allocating device memory (one-time, may take ~2 min)...",
            ),
            _ => format!("{backend_name}: initializing..."),
        };
        info("BACKEND", &backend_start_detail);
        let start_t = Instant::now();

        // Run backend.start() on a separate thread so we can poll the
        // shutdown flag while FFI calls (e.g. NVRTC compilation) block.
        let start_result = {
            let backend_ref = Arc::clone(&backend);
            let (tx, rx) = bounded::<Result<()>>(1);
            std::thread::Builder::new()
                .name(format!("{backend_name}-init"))
                .spawn(move || {
                    let _ = tx.send(backend_ref.start());
                })
                .expect("failed to spawn backend init thread");
            loop {
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(result) => break result,
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        if shutdown.load(Ordering::Relaxed) {
                            break Err(anyhow!("interrupted by user"));
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                        break Err(anyhow!("{backend_name} init thread panicked"));
                    }
                }
            }
        };
        match start_result {
            Ok(()) => {
                info(
                    "BACKEND",
                    format!(
                        "{backend_name}: ready in {:.1}s",
                        start_t.elapsed().as_secs_f64()
                    ),
                );
                let capabilities = match backend_capabilities_for_start(
                    backend.as_ref(),
                    backend_name,
                    backend_id,
                ) {
                    Ok(capabilities) => capabilities,
                    Err(err) => {
                        warn(
                                "BACKEND",
                                format!(
                                    "{backend_name}#{backend_id} capability contract violation: {err:#}"
                                ),
                            );
                        backend.stop();
                        continue;
                    }
                };
                if capabilities.max_inflight_assignments > 1
                    && !backend.supports_assignment_batching()
                {
                    warn(
                        "BACKEND",
                        format!(
                            "{}#{} reports inflight={} but does not support batched assignment dispatch; clamping runtime inflight to 1",
                            backend_name,
                            backend_id,
                            capabilities.max_inflight_assignments
                        ),
                    );
                }
                let lanes = backend.lanes() as u64;
                if lanes == 0 {
                    warn(
                        "BACKEND",
                        format!(
                            "skipping {}#{}: reported zero lanes",
                            backend_name, backend_id
                        ),
                    );
                    backend.stop();
                    continue;
                }
                let runtime_policy = backend_runtime_policy(cfg, &backend_spec, capabilities);
                active.push(BackendSlot {
                    id: backend_id,
                    backend,
                    lanes,
                    runtime_policy,
                    capabilities,
                });
                backend_event_sources.push(backend_event_rx);
            }
            Err(err) => {
                warn(
                    "BACKEND",
                    format!("{backend_name}#{backend_id} unavailable: {err:#}"),
                );
                if backend_name == "nvidia" {
                    warn(
                        "BACKEND",
                        "NVIDIA mining requires: (1) NVIDIA GPU drivers and \
                         (2) CUDA Toolkit — https://developer.nvidia.com/cuda-downloads",
                    );
                }
                backend.stop();
            }
        }
    }

    if active.is_empty() {
        bail!("no mining backend could be started");
    }

    spawn_backend_event_fan_in(backend_event_sources, event_tx)?;

    Ok((active, event_rx))
}

fn spawn_backend_event_fan_in(
    sources: Vec<Receiver<BackendEvent>>,
    sink: Sender<BackendEvent>,
) -> Result<()> {
    if sources.is_empty() {
        return Ok(());
    }
    let thread_name = "seine-backend-events-fanin".to_string();
    std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let mut receivers = sources;
            while !receivers.is_empty() {
                let mut select = crossbeam_channel::Select::new();
                for receiver in &receivers {
                    select.recv(receiver);
                }
                let operation = select.select();
                let idx = operation.index();
                match operation.recv(&receivers[idx]) {
                    Ok(event) => {
                        if sink.send(event).is_err() {
                            return;
                        }
                    }
                    Err(_) => {
                        receivers.swap_remove(idx);
                    }
                }
            }
        })
        .map_err(|err| anyhow!("failed to spawn backend event fan-in thread: {err}"))?;
    Ok(())
}

fn start_backend_slots(
    backends: &mut Vec<BackendSlot>,
    backend_executor: &backend_executor::BackendExecutor,
    stop_timeout: Duration,
) -> Result<()> {
    let mut failed_indices = Vec::new();
    for (idx, slot) in backends.iter_mut().enumerate() {
        if let Err(err) = slot.backend.start() {
            warn(
                "BENCH",
                format!(
                    "backend {}#{} failed to restart: {err:#}",
                    slot.backend.name(),
                    slot.id
                ),
            );
            failed_indices.push(idx);
        }
    }

    for idx in failed_indices.into_iter().rev() {
        let slot = backends.remove(idx);
        let backend_name = slot.backend.name();
        let backend_id = slot.id;
        backend_executor.quarantine_backend(backend_id, Arc::clone(&slot.backend));
        warn(
            "BENCH",
            format!("quarantined {backend_name}#{backend_id} after restart failure"),
        );
    }

    if !backend_executor.wait_for_quarantine_drain(stop_timeout.max(Duration::from_millis(100))) {
        warn(
            "BENCH",
            format!(
                "backend restart cleanup exceeded {}ms; detached",
                stop_timeout.max(Duration::from_millis(100)).as_millis()
            ),
        );
    }

    if backends.is_empty() {
        bail!("all benchmark backends are unavailable after restart failure");
    }
    Ok(())
}

fn stop_backend_slots(
    backends: &mut [BackendSlot],
    backend_executor: &backend_executor::BackendExecutor,
    stop_timeout: Duration,
    log_tag: &'static str,
) {
    for slot in backends.iter() {
        backend_executor.quarantine_backend(slot.id, Arc::clone(&slot.backend));
    }

    let wait_timeout = stop_timeout.max(Duration::from_millis(100));
    if !backend_executor.wait_for_quarantine_drain(wait_timeout) {
        warn(
            log_tag,
            format!(
                "backend stop did not quiesce within {}ms; detached",
                wait_timeout.as_millis()
            ),
        );
    }
    backend_executor.clear();
}

fn distribute_work(
    backends: &mut Vec<BackendSlot>,
    options: DistributeWorkOptions<'_>,
    backend_executor: &backend_executor::BackendExecutor,
) -> Result<u64> {
    work_allocator::distribute_work(backends, options, backend_executor)
}

#[cfg(test)]
fn compute_backend_nonce_counts(
    backends: &[BackendSlot],
    max_iters_per_lane: u64,
    backend_weights: Option<&BTreeMap<BackendInstanceId, f64>>,
) -> Vec<u64> {
    work_allocator::compute_backend_nonce_counts(backends, max_iters_per_lane, backend_weights)
}

fn backend_iters_per_lane(slot: &BackendSlot, default_iters_per_lane: u64) -> u64 {
    work_allocator::backend_iters_per_lane(slot, default_iters_per_lane)
}

fn backend_dispatch_iters_per_lane(slot: &BackendSlot, default_iters_per_lane: u64) -> u64 {
    work_allocator::backend_dispatch_iters_per_lane(slot, default_iters_per_lane)
}

fn collect_backend_hashes(
    backends: &[BackendSlot],
    backend_executor: &backend_executor::BackendExecutor,
    stats: Option<&Stats>,
    round_hashes: &mut u64,
    mut round_backend_hashes: Option<&mut BTreeMap<BackendInstanceId, u64>>,
    mut round_backend_telemetry: Option<&mut BTreeMap<BackendInstanceId, BackendRoundTelemetry>>,
) {
    let mut collected = 0u64;
    let runtime_telemetry = backend_executor.take_backend_telemetry_ordered(backends.iter());
    for (slot, runtime) in backends.iter().zip(runtime_telemetry.into_iter()) {
        let slot_hashes = slot.backend.take_hashes();
        let telemetry = slot.backend.take_telemetry();
        if let Some(per_backend_telemetry) = round_backend_telemetry.as_deref_mut() {
            merge_backend_telemetry(per_backend_telemetry, slot.id, telemetry);
            merge_backend_telemetry(per_backend_telemetry, slot.id, runtime);
        }

        if slot_hashes == 0 {
            continue;
        }

        collected = collected.saturating_add(slot_hashes);
        if let Some(per_backend) = round_backend_hashes.as_deref_mut() {
            let entry = per_backend.entry(slot.id).or_insert(0);
            *entry = entry.saturating_add(slot_hashes);
        }
    }

    if collected == 0 {
        return;
    }

    if let Some(stats) = stats {
        stats.add_hashes(collected);
    }
    *round_hashes = round_hashes.saturating_add(collected);
}

fn collect_round_backend_samples(
    backends: &[BackendSlot],
    backend_executor: &backend_executor::BackendExecutor,
    configured_hash_poll_interval: Duration,
    poll_state: &mut hash_poll::BackendPollState,
    round_backend_hashes: &mut BTreeMap<BackendInstanceId, u64>,
    round_backend_telemetry: &mut BTreeMap<BackendInstanceId, BackendRoundTelemetry>,
) -> u64 {
    let due_slots =
        hash_poll::due_backend_slots(backends, configured_hash_poll_interval, poll_state);
    if due_slots.is_empty() {
        return 0;
    }

    let mut collected = 0u64;
    let runtime_telemetry =
        backend_executor.take_backend_telemetry_ordered(due_slots.iter().copied());
    for (slot, runtime) in due_slots.into_iter().zip(runtime_telemetry.into_iter()) {
        let backend_id = slot.id;
        let hashes = slot.backend.take_hashes();
        let telemetry = slot.backend.take_telemetry();
        merge_backend_telemetry(round_backend_telemetry, backend_id, telemetry);
        merge_backend_telemetry(round_backend_telemetry, backend_id, runtime);
        if hashes > 0 {
            collected = collected.saturating_add(hashes);
            let entry = round_backend_hashes.entry(backend_id).or_insert(0);
            *entry = entry.saturating_add(hashes);
        }
    }
    collected
}

fn merge_backend_telemetry(
    per_backend_telemetry: &mut BTreeMap<BackendInstanceId, BackendRoundTelemetry>,
    backend_id: BackendInstanceId,
    telemetry: BackendTelemetry,
) {
    let telemetry = backend_round_telemetry_delta(telemetry);
    if telemetry.peak_active_lanes == 0
        && telemetry.peak_pending_work == 0
        && telemetry.dropped_events == 0
        && telemetry.completed_assignments == 0
        && telemetry.completed_assignment_hashes == 0
        && telemetry.completed_assignment_micros == 0
        && telemetry.peak_inflight_assignment_hashes == 0
        && telemetry.peak_inflight_assignment_micros == 0
        && telemetry.assignment_enqueue_timeouts == 0
        && telemetry.assignment_execution_timeouts == 0
        && telemetry.control_enqueue_timeouts == 0
        && telemetry.control_execution_timeouts == 0
        && telemetry.peak_assignment_timeout_strikes == 0
        && telemetry.assignment_enqueue_latency_samples == 0
        && telemetry.assignment_enqueue_latency_p95_micros == 0
        && telemetry.assignment_enqueue_latency_max_micros == 0
        && telemetry.assignment_execution_latency_samples == 0
        && telemetry.assignment_execution_latency_p95_micros == 0
        && telemetry.assignment_execution_latency_max_micros == 0
        && telemetry.control_enqueue_latency_samples == 0
        && telemetry.control_enqueue_latency_p95_micros == 0
        && telemetry.control_enqueue_latency_max_micros == 0
        && telemetry.control_execution_latency_samples == 0
        && telemetry.control_execution_latency_p95_micros == 0
        && telemetry.control_execution_latency_max_micros == 0
    {
        return;
    }

    let entry = per_backend_telemetry.entry(backend_id).or_default();
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

pub(super) fn backend_round_telemetry_delta(telemetry: BackendTelemetry) -> BackendRoundTelemetry {
    BackendRoundTelemetry {
        dropped_events: telemetry.dropped_events,
        completed_assignments: telemetry.completed_assignments,
        completed_assignment_hashes: telemetry.completed_assignment_hashes,
        completed_assignment_micros: telemetry.completed_assignment_micros,
        peak_active_lanes: telemetry.active_lanes,
        peak_pending_work: telemetry.pending_work,
        peak_inflight_assignment_hashes: telemetry.inflight_assignment_hashes,
        peak_inflight_assignment_micros: telemetry.inflight_assignment_micros,
        assignment_enqueue_timeouts: telemetry.assignment_enqueue_timeouts,
        assignment_execution_timeouts: telemetry.assignment_execution_timeouts,
        control_enqueue_timeouts: telemetry.control_enqueue_timeouts,
        control_execution_timeouts: telemetry.control_execution_timeouts,
        peak_assignment_timeout_strikes: telemetry.assignment_timeout_strikes,
        assignment_enqueue_latency_samples: telemetry.assignment_enqueue_latency_samples,
        assignment_enqueue_latency_p95_micros: telemetry.assignment_enqueue_latency_p95_micros,
        assignment_enqueue_latency_max_micros: telemetry.assignment_enqueue_latency_max_micros,
        assignment_execution_latency_samples: telemetry.assignment_execution_latency_samples,
        assignment_execution_latency_p95_micros: telemetry.assignment_execution_latency_p95_micros,
        assignment_execution_latency_max_micros: telemetry.assignment_execution_latency_max_micros,
        control_enqueue_latency_samples: telemetry.control_enqueue_latency_samples,
        control_enqueue_latency_p95_micros: telemetry.control_enqueue_latency_p95_micros,
        control_enqueue_latency_max_micros: telemetry.control_enqueue_latency_max_micros,
        control_execution_latency_samples: telemetry.control_execution_latency_samples,
        control_execution_latency_p95_micros: telemetry.control_execution_latency_p95_micros,
        control_execution_latency_max_micros: telemetry.control_execution_latency_max_micros,
    }
}

fn cancel_backend_slots(
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
    backend_executor: &backend_executor::BackendExecutor,
) -> Result<RuntimeBackendEventAction> {
    backend_control::cancel_backend_slots(backends, mode, backend_executor)
}

fn quiesce_backend_slots(
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
    backend_executor: &backend_executor::BackendExecutor,
) -> Result<RuntimeBackendEventAction> {
    backend_control::quiesce_backend_slots(backends, mode, backend_executor)
}

fn remove_backend_by_id(
    backends: &mut Vec<BackendSlot>,
    backend_id: BackendInstanceId,
    backend_executor: &backend_executor::BackendExecutor,
) -> bool {
    let Some(idx) = backends.iter().position(|slot| slot.id == backend_id) else {
        return false;
    };

    let slot = backends.remove(idx);
    backend_executor.quarantine_backend(slot.id, Arc::clone(&slot.backend));
    backend_executor.remove_backend_worker(slot.id, &slot.backend);
    backend_executor.prune(backends);
    true
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum RuntimeMode {
    Mining,
    Bench,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RuntimeBackendEventAction {
    None,
    TopologyChanged,
}

fn enforce_deadline_policy(
    backends: &mut Vec<BackendSlot>,
    allow_best_effort_deadlines: bool,
    mode: RuntimeMode,
    backend_executor: &backend_executor::BackendExecutor,
) -> Result<()> {
    if !allow_best_effort_deadlines {
        let mut best_effort = Vec::new();
        let mut survivors = Vec::with_capacity(backends.len());
        for slot in std::mem::take(backends) {
            let capabilities = backend_capabilities(&slot);
            if capabilities.deadline_support == DeadlineSupport::BestEffort {
                best_effort.push(slot);
            } else {
                survivors.push(slot);
            }
        }
        *backends = survivors;

        for slot in best_effort {
            let backend_name = slot.backend.name();
            let backend_id = slot.id;
            backend_executor.quarantine_backend(backend_id, Arc::clone(&slot.backend));
            backend_executor.remove_backend_worker(backend_id, &slot.backend);
            warn(
                "BACKEND",
                format!(
                    "quarantined {backend_name}#{backend_id}: best-effort deadlines disabled (pass --allow-best-effort-deadlines to allow)"
                ),
            );
        }

        backend_executor.prune(backends);
        if backends.is_empty() {
            bail!(
                "all active backends report best-effort deadlines; pass --allow-best-effort-deadlines to continue"
            );
        }
    }

    if backends.is_empty() {
        return Ok(());
    }

    let action = quiesce_backend_slots(backends, mode, backend_executor)?;
    if action == RuntimeBackendEventAction::TopologyChanged {
        warn(
            "BACKEND",
            format!(
                "deadline probe quarantined backend(s); remaining={}",
                backend_names(backends)
            ),
        );
    }
    Ok(())
}

fn handle_runtime_backend_event(
    event: BackendEvent,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
    backend_executor: &backend_executor::BackendExecutor,
) -> Result<(
    RuntimeBackendEventAction,
    Option<crate::backend::MiningSolution>,
)> {
    backend_control::handle_runtime_backend_event(event, epoch, backends, mode, backend_executor)
}

fn drain_runtime_backend_events(
    backend_events: &Receiver<BackendEvent>,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
    backend_executor: &backend_executor::BackendExecutor,
) -> Result<(
    RuntimeBackendEventAction,
    Vec<crate::backend::MiningSolution>,
)> {
    backend_control::drain_runtime_backend_events(
        backend_events,
        epoch,
        backends,
        mode,
        backend_executor,
    )
}

fn backend_name_list(backends: &[BackendSlot]) -> Vec<String> {
    backends
        .iter()
        .map(|slot| format!("{}#{}", slot.backend.name(), slot.id))
        .collect()
}

fn auth_source_label(cfg: &Config) -> String {
    if let Some(path) = &cfg.token_cookie_path {
        format!("cookie:{}", path.display())
    } else {
        "token:static".to_string()
    }
}

fn cpu_threads_summary(backends: &[BackendSlot]) -> String {
    let cpu_threads: Vec<String> = backends
        .iter()
        .filter(|slot| slot.backend.name() == "cpu")
        .map(|slot| format!("cpu#{}={}", slot.id, slot.lanes))
        .collect();
    if cpu_threads.is_empty() {
        "none".to_string()
    } else {
        cpu_threads.join(",")
    }
}

fn backend_names(backends: &[BackendSlot]) -> String {
    backend_name_list(backends).join(",")
}

fn backend_descriptions(backends: &[BackendSlot]) -> String {
    backends
        .iter()
        .map(|slot| {
            let name = format!("{}#{}", slot.backend.name(), slot.id);
            if slot.backend.name() == "cpu" {
                let ram_gib =
                    (slot.lanes as f64 * CPU_LANE_MEMORY_BYTES as f64) / (1024.0 * 1024.0 * 1024.0);
                format!("{} ({} lanes, ~{:.1} GiB RAM)", name, slot.lanes, ram_gib)
            } else if let Some(mem_bytes) = slot.backend.device_memory_bytes() {
                let mem_gib = mem_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                format!("{} ({} lanes, {:.1} GiB VRAM)", name, slot.lanes, mem_gib)
            } else {
                format!("{} ({} lanes)", name, slot.lanes)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn backend_preemption_profiles(backends: &[BackendSlot]) -> String {
    backends
        .iter()
        .map(|slot| {
            format!(
                "{}#{}={}",
                slot.backend.name(),
                slot.id,
                slot.backend.preemption_granularity().describe()
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn backend_chunk_profiles(backends: &[BackendSlot], default_iters_per_lane: u64) -> String {
    backends
        .iter()
        .map(|slot| {
            let allocation_hint = backend_iters_per_lane(slot, default_iters_per_lane);
            let dispatch_hint = backend_dispatch_iters_per_lane(slot, default_iters_per_lane);
            let capabilities = backend_capabilities(slot);
            let inflight = capabilities.max_inflight_assignments.max(1);
            let poll_hint = capabilities
                .preferred_hash_poll_interval
                .map(|d| format!("{}ms", d.as_millis()))
                .unwrap_or_else(|| "none".to_string());
            let deadline_support = capabilities.deadline_support.describe();
            let assignment_semantics = capabilities.assignment_semantics.describe();
            let execution_model = capabilities.execution_model.describe();
            let worker_queue_depth =
                backend_executor::effective_backend_worker_queue_capacity(capabilities);
            let nonblocking_poll = match (
                capabilities.nonblocking_poll_min,
                capabilities.nonblocking_poll_max,
            ) {
                (Some(min_poll), Some(max_poll)) => {
                    format!("{}us..{}us", min_poll.as_micros(), max_poll.as_micros())
                }
                _ => "default".to_string(),
            };
            format!(
                "{}#{}=alloc:{} dispatch:{} iters/lane inflight={} worker_q={} poll_hint={} nb_poll={} deadline={} assign={} exec={} assign_timeout={}ms control_timeout={}ms assign_strikes={}",
                slot.backend.name(),
                slot.id,
                allocation_hint,
                dispatch_hint,
                inflight,
                worker_queue_depth,
                poll_hint,
                nonblocking_poll,
                deadline_support,
                assignment_semantics,
                execution_model,
                slot.runtime_policy.assignment_timeout.as_millis(),
                slot.runtime_policy.control_timeout.as_millis(),
                slot.runtime_policy.assignment_timeout_strikes,
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn backend_timeout_profiles(backends: &[BackendSlot]) -> String {
    backends
        .iter()
        .map(|slot| {
            format!(
                "{}#{}=assign:{}ms strikes:{} control:{}ms",
                slot.backend.name(),
                slot.id,
                slot.runtime_policy.assignment_timeout.as_millis(),
                slot.runtime_policy.assignment_timeout_strikes,
                slot.runtime_policy.control_timeout.as_millis(),
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn effective_hash_poll_interval(backends: &[BackendSlot], configured: Duration) -> Duration {
    let mut effective = configured.max(Duration::from_millis(1));
    for slot in backends {
        if let Some(hint) = backend_capabilities(slot).preferred_hash_poll_interval {
            if hint > Duration::from_millis(0) {
                effective = effective.min(hint);
            }
        }
    }
    effective
}

fn backend_capabilities_for_start(
    backend: &dyn PowBackend,
    backend_name: &'static str,
    backend_id: BackendInstanceId,
) -> Result<BackendCapabilities> {
    let capabilities = normalize_backend_capabilities(
        backend.capabilities(),
        backend.supports_assignment_batching(),
    );
    if capabilities.execution_model == BackendExecutionModel::Nonblocking
        && !backend.supports_true_nonblocking()
    {
        bail!(
            "{backend_name}#{backend_id} reports nonblocking execution model without true nonblocking hook support"
        );
    }
    Ok(capabilities)
}

fn backend_runtime_policy(
    cfg: &Config,
    backend_spec: &BackendSpec,
    capabilities: BackendCapabilities,
) -> BackendRuntimePolicy {
    let assignment_timeout = backend_spec
        .assign_timeout_override
        .or(capabilities.preferred_assignment_timeout)
        .unwrap_or(cfg.backend_assign_timeout)
        .max(Duration::from_millis(1));
    let control_timeout = backend_spec
        .control_timeout_override
        .or(capabilities.preferred_control_timeout)
        .unwrap_or(cfg.backend_control_timeout)
        .max(Duration::from_millis(1));
    let assignment_timeout_strikes = backend_spec
        .assign_timeout_strikes_override
        .or(capabilities.preferred_assignment_timeout_strikes)
        .unwrap_or(cfg.backend_assign_timeout_strikes)
        .max(1);
    BackendRuntimePolicy {
        assignment_timeout,
        assignment_timeout_strikes,
        control_timeout,
    }
}

fn backend_capabilities(slot: &BackendSlot) -> BackendCapabilities {
    slot.capabilities
}

fn cpu_lane_count(backends: &[BackendSlot]) -> u64 {
    backends
        .iter()
        .filter(|slot| slot.backend.name() == "cpu")
        .map(|slot| slot.lanes)
        .sum()
}

fn backends_have_append_assignment_semantics(backends: &[BackendSlot]) -> bool {
    backends.iter().any(|slot| {
        backend_capabilities(slot).assignment_semantics
            == crate::backend::AssignmentSemantics::Append
    })
}

fn backend_names_by_id(backends: &[BackendSlot]) -> BTreeMap<BackendInstanceId, &'static str> {
    backends
        .iter()
        .map(|slot| (slot.id, slot.backend.name()))
        .collect()
}

fn format_round_backend_telemetry(
    backends: &[BackendSlot],
    round_backend_telemetry: &BTreeMap<BackendInstanceId, BackendRoundTelemetry>,
) -> String {
    let backend_names = backend_names_by_id(backends);
    let mut parts = Vec::new();
    for (backend_id, telemetry) in round_backend_telemetry {
        if telemetry.dropped_events == 0
            && telemetry.completed_assignments == 0
            && telemetry.peak_active_lanes == 0
            && telemetry.peak_pending_work == 0
            && telemetry.peak_inflight_assignment_hashes == 0
            && telemetry.peak_inflight_assignment_micros == 0
            && telemetry.assignment_enqueue_timeouts == 0
            && telemetry.assignment_execution_timeouts == 0
            && telemetry.control_enqueue_timeouts == 0
            && telemetry.control_execution_timeouts == 0
            && telemetry.peak_assignment_timeout_strikes == 0
            && telemetry.assignment_enqueue_latency_samples == 0
            && telemetry.assignment_enqueue_latency_p95_micros == 0
            && telemetry.assignment_enqueue_latency_max_micros == 0
            && telemetry.assignment_execution_latency_samples == 0
            && telemetry.assignment_execution_latency_p95_micros == 0
            && telemetry.assignment_execution_latency_max_micros == 0
            && telemetry.control_enqueue_latency_samples == 0
            && telemetry.control_enqueue_latency_p95_micros == 0
            && telemetry.control_enqueue_latency_max_micros == 0
            && telemetry.control_execution_latency_samples == 0
            && telemetry.control_execution_latency_p95_micros == 0
            && telemetry.control_execution_latency_max_micros == 0
        {
            continue;
        }
        let backend_name = backend_names.get(backend_id).copied().unwrap_or("unknown");
        parts.push(format!(
            "{backend_name}#{backend_id}:active_peak={} pending_peak={} inflight_hashes_peak={} inflight_secs_peak={:.3} drops={} assignments={} assignment_hashes={} assignment_secs={:.3} assign_timeout_enq={} assign_timeout_exec={} control_timeout_enq={} control_timeout_exec={} assign_timeout_strike_peak={} assign_enq_lat_samples={} assign_enq_lat_p95_us={} assign_enq_lat_max_us={} assign_exec_lat_samples={} assign_exec_lat_p95_us={} assign_exec_lat_max_us={} control_enq_lat_samples={} control_enq_lat_p95_us={} control_enq_lat_max_us={} control_exec_lat_samples={} control_exec_lat_p95_us={} control_exec_lat_max_us={}",
            telemetry.peak_active_lanes,
            telemetry.peak_pending_work,
            telemetry.peak_inflight_assignment_hashes,
            telemetry.peak_inflight_assignment_micros as f64 / 1_000_000.0,
            telemetry.dropped_events,
            telemetry.completed_assignments,
            telemetry.completed_assignment_hashes,
            telemetry.completed_assignment_micros as f64 / 1_000_000.0,
            telemetry.assignment_enqueue_timeouts,
            telemetry.assignment_execution_timeouts,
            telemetry.control_enqueue_timeouts,
            telemetry.control_execution_timeouts,
            telemetry.peak_assignment_timeout_strikes,
            telemetry.assignment_enqueue_latency_samples,
            telemetry.assignment_enqueue_latency_p95_micros,
            telemetry.assignment_enqueue_latency_max_micros,
            telemetry.assignment_execution_latency_samples,
            telemetry.assignment_execution_latency_p95_micros,
            telemetry.assignment_execution_latency_max_micros,
            telemetry.control_enqueue_latency_samples,
            telemetry.control_enqueue_latency_p95_micros,
            telemetry.control_enqueue_latency_max_micros,
            telemetry.control_execution_latency_samples,
            telemetry.control_execution_latency_p95_micros,
            telemetry.control_execution_latency_max_micros,
        ));
    }

    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join(", ")
    }
}

fn total_lanes(backends: &[BackendSlot]) -> u64 {
    backends.iter().map(|slot| slot.lanes).sum::<u64>().max(1)
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
    use crate::backend::{NonceChunk, WorkAssignment, WorkTemplate};
    use anyhow::anyhow;
    use blocknet_pow_spec::POW_HEADER_BASE_LEN;
    use crossbeam_channel::Sender;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MockState {
        fail_start: AtomicBool,
        fail_next_assign: AtomicBool,
        panic_next_assign: AtomicBool,
        assign_calls: AtomicUsize,
        assign_batch_calls: AtomicUsize,
        last_batch_len: AtomicUsize,
        stop_calls: AtomicUsize,
        last_chunk: Mutex<Option<NonceChunk>>,
        chunks: Mutex<Vec<NonceChunk>>,
    }

    struct MockBackend {
        name: &'static str,
        lanes: usize,
        state: Arc<MockState>,
        preferred_iters_per_lane: Option<u64>,
        preferred_allocation_iters_per_lane: Option<u64>,
        preferred_hash_poll_interval: Option<Duration>,
        max_inflight_assignments: u32,
        preferred_worker_queue_depth: Option<u32>,
        supports_assignment_batching: bool,
        assign_delay: Option<Duration>,
        deadline_support: crate::backend::DeadlineSupport,
        assignment_semantics: crate::backend::AssignmentSemantics,
        execution_model: crate::backend::BackendExecutionModel,
        supports_true_nonblocking: bool,
    }

    impl MockBackend {
        fn new(name: &'static str, lanes: usize, state: Arc<MockState>) -> Self {
            Self {
                name,
                lanes,
                state,
                preferred_iters_per_lane: None,
                preferred_allocation_iters_per_lane: None,
                preferred_hash_poll_interval: None,
                max_inflight_assignments: 1,
                preferred_worker_queue_depth: None,
                supports_assignment_batching: false,
                assign_delay: None,
                deadline_support: crate::backend::DeadlineSupport::Cooperative,
                assignment_semantics: crate::backend::AssignmentSemantics::Replace,
                execution_model: crate::backend::BackendExecutionModel::Blocking,
                supports_true_nonblocking: false,
            }
        }

        fn with_preferred_iters_per_lane(mut self, preferred_iters_per_lane: u64) -> Self {
            let preferred = preferred_iters_per_lane.max(1);
            self.preferred_iters_per_lane = Some(preferred);
            self.preferred_allocation_iters_per_lane = Some(preferred);
            self
        }

        fn with_preferred_dispatch_iters_per_lane(mut self, preferred_iters_per_lane: u64) -> Self {
            self.preferred_iters_per_lane = Some(preferred_iters_per_lane.max(1));
            self
        }

        fn with_preferred_allocation_iters_per_lane(
            mut self,
            preferred_iters_per_lane: u64,
        ) -> Self {
            self.preferred_allocation_iters_per_lane = Some(preferred_iters_per_lane.max(1));
            self
        }

        fn with_preferred_hash_poll_interval(mut self, interval: Duration) -> Self {
            self.preferred_hash_poll_interval = Some(interval);
            self
        }

        fn with_max_inflight_assignments(mut self, max_inflight_assignments: u32) -> Self {
            self.max_inflight_assignments = max_inflight_assignments.max(1);
            self.supports_assignment_batching = self.max_inflight_assignments > 1;
            self
        }

        fn with_preferred_worker_queue_depth(mut self, preferred_worker_queue_depth: u32) -> Self {
            self.preferred_worker_queue_depth = Some(preferred_worker_queue_depth.max(1));
            self
        }

        fn with_assign_delay(mut self, assign_delay: Duration) -> Self {
            self.assign_delay = Some(assign_delay);
            self
        }

        fn with_deadline_support(
            mut self,
            deadline_support: crate::backend::DeadlineSupport,
        ) -> Self {
            self.deadline_support = deadline_support;
            self
        }

        fn with_assignment_semantics(
            mut self,
            assignment_semantics: crate::backend::AssignmentSemantics,
        ) -> Self {
            self.assignment_semantics = assignment_semantics;
            self
        }

        fn with_execution_model(
            mut self,
            execution_model: crate::backend::BackendExecutionModel,
        ) -> Self {
            self.execution_model = execution_model;
            self
        }

        fn with_true_nonblocking_support(mut self, enabled: bool) -> Self {
            self.supports_true_nonblocking = enabled;
            self
        }
    }

    impl PowBackend for MockBackend {
        fn name(&self) -> &'static str {
            self.name
        }

        fn lanes(&self) -> usize {
            self.lanes
        }

        fn set_instance_id(&self, _id: BackendInstanceId) {}

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            if self.state.fail_start.load(Ordering::Acquire) {
                return Err(anyhow!("injected start failure"));
            }
            Ok(())
        }

        fn stop(&self) {
            self.state.stop_calls.fetch_add(1, Ordering::Relaxed);
        }

        fn assign_work(&self, work: WorkAssignment) -> Result<()> {
            if let Some(delay) = self.assign_delay {
                std::thread::sleep(delay);
            }
            self.state.assign_calls.fetch_add(1, Ordering::Relaxed);
            if self.state.panic_next_assign.swap(false, Ordering::AcqRel) {
                panic!("injected assign panic");
            }
            if self.state.fail_next_assign.swap(false, Ordering::AcqRel) {
                return Err(anyhow!("injected assign failure"));
            }
            self.state
                .chunks
                .lock()
                .expect("mock chunk history lock should not be poisoned")
                .push(work.nonce_chunk);
            *self
                .state
                .last_chunk
                .lock()
                .expect("mock chunk lock should not be poisoned") = Some(work.nonce_chunk);
            Ok(())
        }

        fn assign_work_batch(&self, work: &[WorkAssignment]) -> Result<()> {
            self.state
                .assign_batch_calls
                .fetch_add(1, Ordering::Relaxed);
            self.state
                .last_batch_len
                .store(work.len(), Ordering::Relaxed);
            for assignment in work {
                self.assign_work(assignment.clone())?;
            }
            Ok(())
        }

        fn cancel_work(&self) -> Result<()> {
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            Ok(())
        }

        fn supports_assignment_batching(&self) -> bool {
            self.supports_assignment_batching
        }

        fn supports_true_nonblocking(&self) -> bool {
            self.supports_true_nonblocking
        }

        fn capabilities(&self) -> crate::backend::BackendCapabilities {
            crate::backend::BackendCapabilities {
                preferred_iters_per_lane: self.preferred_iters_per_lane,
                preferred_allocation_iters_per_lane: self.preferred_allocation_iters_per_lane,
                preferred_hash_poll_interval: self.preferred_hash_poll_interval,
                preferred_assignment_timeout: None,
                preferred_control_timeout: None,
                preferred_assignment_timeout_strikes: None,
                preferred_worker_queue_depth: self.preferred_worker_queue_depth,
                max_inflight_assignments: self.max_inflight_assignments.max(1),
                deadline_support: self.deadline_support,
                assignment_semantics: self.assignment_semantics,
                execution_model: self.execution_model,
                nonblocking_poll_min: None,
                nonblocking_poll_max: None,
            }
        }
    }

    fn slot(id: BackendInstanceId, lanes: u64, backend: Arc<dyn PowBackend>) -> BackendSlot {
        let mut capabilities = normalize_backend_capabilities(
            backend.capabilities(),
            backend.supports_assignment_batching(),
        );
        if capabilities.execution_model == BackendExecutionModel::Nonblocking
            && !backend.supports_true_nonblocking()
        {
            capabilities.execution_model = BackendExecutionModel::Blocking;
        }
        BackendSlot {
            id,
            backend,
            lanes,
            runtime_policy: BackendRuntimePolicy::default(),
            capabilities,
        }
    }

    fn wait_for_stop_call(state: &MockState, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if state.stop_calls.load(Ordering::Relaxed) > 0 {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_stop_counter(counter: &AtomicUsize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if counter.load(Ordering::Relaxed) > 0 {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn test_config() -> Config {
        Config {
            api_url: "http://127.0.0.1:8332".to_string(),
            token: Some("test-token".to_string()),
            token_cookie_path: None,
            wallet_password: None,
            wallet_password_file: None,
            backend_specs: vec![BackendSpec {
                kind: BackendKind::Cpu,
                device_index: None,
                cpu_threads: Some(1),
                cpu_affinity: Some(crate::config::CpuAffinityMode::Off),
                assign_timeout_override: None,
                control_timeout_override: None,
                assign_timeout_strikes_override: None,
            }],
            threads: 1,
            cpu_auto_threads_cap: 1,
            cpu_affinity: crate::config::CpuAffinityMode::Off,
            cpu_profile: crate::config::CpuPerformanceProfile::Balanced,
            refresh_interval: Duration::from_secs(20),
            request_timeout: Duration::from_secs(10),
            events_stream_timeout: Duration::from_secs(10),
            events_idle_timeout: Duration::from_secs(90),
            stats_interval: Duration::from_secs(10),
            backend_event_capacity: 1024,
            hash_poll_interval: Duration::from_millis(200),
            cpu_hash_batch_size: 64,
            cpu_control_check_interval_hashes: 1,
            cpu_hash_flush_interval: Duration::from_millis(50),
            cpu_event_dispatch_capacity: 256,
            cpu_autotune_threads: false,
            cpu_autotune_min_threads: 1,
            cpu_autotune_max_threads: None,
            cpu_autotune_secs: 2,
            cpu_autotune_config_path: std::path::PathBuf::from("./data/seine.cpu-autotune.json"),
            nvidia_autotune_secs: 2,
            nvidia_autotune_samples: 2,
            nvidia_autotune_config_path: std::path::PathBuf::from(
                "./data/seine.nvidia-autotune.json",
            ),
            nvidia_max_rregcount: None,
            nvidia_max_lanes: None,
            nvidia_dispatch_iters_per_lane: None,
            nvidia_allocation_iters_per_lane: None,
            nvidia_hashes_per_launch_per_lane: 2,
            nvidia_fused_target_check: false,
            nvidia_adaptive_launch_depth: true,
            nvidia_enforce_template_stop: false,
            metal_max_lanes: None,
            metal_hashes_per_launch_per_lane: 2,
            backend_assign_timeout: Duration::from_millis(1_000),
            backend_assign_timeout_strikes: 3,
            backend_control_timeout: Duration::from_millis(60_000),
            allow_best_effort_deadlines: false,
            prefetch_wait: Duration::from_millis(250),
            tip_listener_join_wait: Duration::from_millis(250),
            submit_join_wait: Duration::from_millis(2_000),
            strict_round_accounting: false,
            start_nonce: 0,
            nonce_iters_per_lane: 1 << 20,
            work_allocation: WorkAllocation::Adaptive,
            sub_round_rebalance_interval: None,
            sse_enabled: true,
            refresh_on_same_height: false,
            ui_mode: crate::config::UiMode::Plain,
            bench: false,
            bench_kind: crate::config::BenchKind::Backend,
            bench_secs: 20,
            bench_rounds: 3,
            bench_warmup_rounds: 0,
            bench_output: None,
            bench_baseline: None,
            bench_fail_below_pct: None,
            bench_baseline_policy: crate::config::BenchBaselinePolicy::Strict,
            nvidia_hint: None,
        }
    }

    fn autotune_measurements(values: &[(usize, f64)]) -> BTreeMap<usize, CpuAutotuneMeasurement> {
        values
            .iter()
            .map(|(threads, hps)| {
                (
                    *threads,
                    CpuAutotuneMeasurement {
                        hashes: 0,
                        wall_secs: 0.0,
                        hps: *hps,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn autotune_selection_throughput_prefers_peak_and_lower_tie_threads() {
        let selection = select_cpu_autotune_candidate(
            CpuPerformanceProfile::Throughput,
            &autotune_measurements(&[(3, 3.0), (4, 3.0), (5, 2.9)]),
        )
        .expect("selection should exist");

        assert_eq!(selection.peak_threads, 3);
        assert_eq!(selection.selected_threads, 3);
        assert_eq!(selection.peak_floor_frac, 1.0);
    }

    #[test]
    fn autotune_selection_balanced_biases_lower_when_near_peak() {
        // 95% of peak (3.3) is 3.135, so 4 threads (3.2) is the first candidate that qualifies.
        let selection = select_cpu_autotune_candidate(
            CpuPerformanceProfile::Balanced,
            &autotune_measurements(&[(3, 3.0), (4, 3.2), (22, 3.3)]),
        )
        .expect("selection should exist");

        assert_eq!(selection.peak_threads, 22);
        assert_eq!(selection.selected_threads, 4);
        assert!((selection.peak_floor_frac - CPU_AUTOTUNE_BALANCED_PEAK_FLOOR_FRAC).abs() < 1e-9);
    }

    #[test]
    fn autotune_selection_efficiency_uses_stronger_bias_floor() {
        let selection = select_cpu_autotune_candidate(
            CpuPerformanceProfile::Efficiency,
            &autotune_measurements(&[(2, 2.1), (3, 2.3), (10, 3.0)]),
        )
        .expect("selection should exist");

        // 75% of peak (3.0) is 2.25, so 3 threads is the first candidate that qualifies.
        assert_eq!(selection.peak_threads, 10);
        assert_eq!(selection.selected_threads, 3);
        assert!((selection.peak_floor_frac - CPU_AUTOTUNE_EFFICIENCY_PEAK_FLOOR_FRAC).abs() < 1e-9);
    }

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

    #[test]
    fn backend_capabilities_for_start_rejects_inconsistent_nonblocking_contract() {
        let backend: Arc<dyn PowBackend> = Arc::new(
            MockBackend::new("mock", 1, Arc::new(MockState::default()))
                .with_execution_model(crate::backend::BackendExecutionModel::Nonblocking)
                .with_true_nonblocking_support(false),
        );

        let result = backend_capabilities_for_start(backend.as_ref(), "mock", 77);
        assert!(result.is_err());
    }

    #[test]
    fn backend_capabilities_downgrades_nonblocking_without_runtime_support() {
        let backend: Arc<dyn PowBackend> = Arc::new(
            MockBackend::new("mock", 1, Arc::new(MockState::default()))
                .with_execution_model(crate::backend::BackendExecutionModel::Nonblocking)
                .with_true_nonblocking_support(false),
        );
        let slot = slot(88, 1, backend);

        let capabilities = backend_capabilities(&slot);
        assert_eq!(
            capabilities.execution_model,
            crate::backend::BackendExecutionModel::Blocking
        );
    }

    #[test]
    fn backend_capabilities_preserves_nonblocking_when_runtime_supports_it() {
        let backend: Arc<dyn PowBackend> = Arc::new(
            MockBackend::new("mock", 1, Arc::new(MockState::default()))
                .with_execution_model(crate::backend::BackendExecutionModel::Nonblocking)
                .with_true_nonblocking_support(true),
        );
        let slot = slot(89, 1, backend);

        let capabilities = backend_capabilities(&slot);
        assert_eq!(
            capabilities.execution_model,
            crate::backend::BackendExecutionModel::Nonblocking
        );
    }

    #[test]
    fn remove_backend_by_id_only_removes_target_instance() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let first_state = Arc::new(MockState::default());
        let second_state = Arc::new(MockState::default());

        let mut backends = vec![
            slot(
                11,
                1,
                Arc::new(MockBackend::new("nvidia", 1, Arc::clone(&first_state))),
            ),
            slot(
                22,
                1,
                Arc::new(MockBackend::new("nvidia", 1, Arc::clone(&second_state))),
            ),
        ];
        assert!(remove_backend_by_id(&mut backends, 22, &backend_executor));
        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].id, 11);
        assert_eq!(first_state.stop_calls.load(Ordering::Relaxed), 0);
        assert!(
            wait_for_stop_call(&second_state, Duration::from_millis(250)),
            "removed backend should be stopped asynchronously"
        );
    }

    #[test]
    fn distribute_work_quarantines_assignment_failures_and_reassigns_lanes() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let first_state = Arc::new(MockState::default());
        let failed_state = Arc::new(MockState::default());
        let third_state = Arc::new(MockState::default());
        failed_state.fail_next_assign.store(true, Ordering::Relaxed);

        let mut backends = vec![
            slot(
                1,
                2,
                Arc::new(MockBackend::new("cpu", 2, Arc::clone(&first_state))),
            ),
            slot(
                2,
                1,
                Arc::new(MockBackend::new("nvidia", 1, Arc::clone(&failed_state))),
            ),
            slot(
                3,
                1,
                Arc::new(MockBackend::new("nvidia", 1, Arc::clone(&third_state))),
            ),
        ];
        let additional_span = distribute_work(
            &mut backends,
            DistributeWorkOptions {
                epoch: 1,
                work_id: 1,
                header_base: Arc::from(vec![7u8; POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                reservation: NonceReservation {
                    start_nonce: 100,
                    max_iters_per_lane: 10,
                    reserved_span: 40,
                },
                stop_at: Instant::now() + Duration::from_secs(1),
                backend_weights: None,
            },
            &backend_executor,
        )
        .expect("distribution should continue after quarantining failing backend");
        assert_eq!(additional_span, 30);

        assert_eq!(backends.len(), 2);
        assert_eq!(backends[0].id, 1);
        assert_eq!(backends[1].id, 3);

        let first_chunk = first_state
            .last_chunk
            .lock()
            .expect("first chunk lock should not be poisoned")
            .expect("first backend should receive work");
        let third_chunk = third_state
            .last_chunk
            .lock()
            .expect("third chunk lock should not be poisoned")
            .expect("third backend should receive work");

        // Retry after assignment failure moves to a fresh nonce reservation window.
        assert_eq!(first_chunk.start_nonce, 140);
        assert_eq!(first_chunk.nonce_count, 20);
        assert_eq!(third_chunk.start_nonce, 160);
        assert_eq!(third_chunk.nonce_count, 10);
        assert!(
            wait_for_stop_call(&failed_state, Duration::from_millis(250)),
            "failing backend should be quarantined"
        );
    }

    #[test]
    fn distribute_work_uses_backend_inflight_batching_hint() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let state = Arc::new(MockState::default());
        let mut backends = vec![slot(
            7,
            1,
            Arc::new(
                MockBackend::new("nvidia", 1, Arc::clone(&state))
                    .with_preferred_allocation_iters_per_lane(12)
                    .with_preferred_dispatch_iters_per_lane(3)
                    .with_max_inflight_assignments(4),
            ),
        )];

        let additional_span = distribute_work(
            &mut backends,
            DistributeWorkOptions {
                epoch: 1,
                work_id: 1,
                header_base: Arc::from(vec![7u8; POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                reservation: NonceReservation {
                    start_nonce: 200,
                    max_iters_per_lane: 12,
                    reserved_span: 12,
                },
                stop_at: Instant::now() + Duration::from_secs(1),
                backend_weights: None,
            },
            &backend_executor,
        )
        .expect("distribution should succeed");

        assert_eq!(additional_span, 0);
        assert_eq!(state.assign_batch_calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.last_batch_len.load(Ordering::Relaxed), 4);

        let chunks = state
            .chunks
            .lock()
            .expect("chunk history lock should not be poisoned")
            .clone();
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].start_nonce, 200);
        assert_eq!(chunks[0].nonce_count, 3);
        assert_eq!(chunks[1].start_nonce, 203);
        assert_eq!(chunks[1].nonce_count, 3);
        assert_eq!(chunks[2].start_nonce, 206);
        assert_eq!(chunks[2].nonce_count, 3);
        assert_eq!(chunks[3].start_nonce, 209);
        assert_eq!(chunks[3].nonce_count, 3);
    }

    #[test]
    fn backend_chunk_profiles_uses_effective_worker_queue_depth() {
        let backends = vec![slot(
            77,
            1,
            Arc::new(
                MockBackend::new("nvidia", 1, Arc::new(MockState::default()))
                    .with_preferred_worker_queue_depth(10_000)
                    .with_assignment_semantics(crate::backend::AssignmentSemantics::Replace),
            ),
        )];

        let profiles = backend_chunk_profiles(&backends, 1 << 20);
        assert!(
            profiles.contains("worker_q=64"),
            "expected clamped queue depth in profile output, got: {profiles}"
        );
    }

    #[test]
    fn distribute_work_timeout_cleanup_stops_backend_after_repeated_timeouts() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let slow_state = Arc::new(MockState::default());
        let mut backends = vec![slot(
            9,
            1,
            Arc::new(
                MockBackend::new("cpu", 1, Arc::clone(&slow_state))
                    .with_assign_delay(Duration::from_millis(50)),
            ),
        )];
        backends[0].runtime_policy.assignment_timeout = Duration::from_millis(5);

        let first_round = distribute_work(
            &mut backends,
            DistributeWorkOptions {
                epoch: 1,
                work_id: 1,
                header_base: Arc::from(vec![7u8; POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                reservation: NonceReservation {
                    start_nonce: 10,
                    max_iters_per_lane: 1,
                    reserved_span: 1,
                },
                stop_at: Instant::now() + Duration::from_secs(1),
                backend_weights: None,
            },
            &backend_executor,
        )
        .expect("first timeout window should keep backend active");
        assert!(first_round >= 1);

        std::thread::sleep(Duration::from_millis(60));

        let err = distribute_work(
            &mut backends,
            DistributeWorkOptions {
                epoch: 2,
                work_id: 2,
                header_base: Arc::from(vec![7u8; POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                reservation: NonceReservation {
                    start_nonce: 20,
                    max_iters_per_lane: 1,
                    reserved_span: 1,
                },
                stop_at: Instant::now() + Duration::from_secs(1),
                backend_weights: None,
            },
            &backend_executor,
        )
        .expect_err("repeated timeout strikes should quarantine the only backend");
        assert!(format!("{err:#}").contains("all mining backends are unavailable"));

        assert!(
            wait_for_stop_call(&slow_state, Duration::from_millis(250)),
            "timed-out backend should be stopped once quarantined"
        );
    }

    #[test]
    fn distribute_work_timeout_without_quarantine_retries_once_then_keeps_backend_active() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let slow_state = Arc::new(MockState::default());
        let mut backends = vec![slot(
            9,
            1,
            Arc::new(
                MockBackend::new("cpu", 1, Arc::clone(&slow_state))
                    .with_assign_delay(Duration::from_millis(50)),
            ),
        )];
        backends[0].runtime_policy.assignment_timeout = Duration::from_millis(5);

        let additional_span = distribute_work(
            &mut backends,
            DistributeWorkOptions {
                epoch: 1,
                work_id: 1,
                header_base: Arc::from(vec![7u8; POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                reservation: NonceReservation {
                    start_nonce: 10,
                    max_iters_per_lane: 1,
                    reserved_span: 1,
                },
                stop_at: Instant::now() + Duration::from_secs(1),
                backend_weights: None,
            },
            &backend_executor,
        )
        .expect("non-quarantined timeout should keep backend active");

        assert!(additional_span >= 1);
        assert_eq!(backends.len(), 1);
        std::thread::sleep(Duration::from_millis(120));
        assert!(
            slow_state.assign_calls.load(Ordering::Relaxed) <= 2,
            "timeout path should not spin unbounded retries in one distribution call"
        );
    }

    #[test]
    fn distribute_work_timeout_quarantines_append_semantics_backend_immediately() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let slow_state = Arc::new(MockState::default());
        let mut backends = vec![slot(
            12,
            1,
            Arc::new(
                MockBackend::new("nvidia", 1, Arc::clone(&slow_state))
                    .with_assign_delay(Duration::from_millis(50))
                    .with_assignment_semantics(crate::backend::AssignmentSemantics::Append),
            ),
        )];
        backends[0].runtime_policy.assignment_timeout = Duration::from_millis(5);

        let err = distribute_work(
            &mut backends,
            DistributeWorkOptions {
                epoch: 1,
                work_id: 1,
                header_base: Arc::from(vec![7u8; POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                reservation: NonceReservation {
                    start_nonce: 10,
                    max_iters_per_lane: 1,
                    reserved_span: 1,
                },
                stop_at: Instant::now() + Duration::from_secs(1),
                backend_weights: None,
            },
            &backend_executor,
        )
        .expect_err("append semantics backend should be quarantined after first timeout");
        assert!(format!("{err:#}").contains("all mining backends are unavailable"));

        assert!(
            wait_for_stop_call(&slow_state, Duration::from_millis(250)),
            "timed-out append backend should be stopped once late dispatch completes"
        );
    }

    #[test]
    fn distribute_work_quarantines_assignment_panics_and_reassigns_lanes() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let first_state = Arc::new(MockState::default());
        let panic_state = Arc::new(MockState::default());
        let third_state = Arc::new(MockState::default());
        panic_state.panic_next_assign.store(true, Ordering::Relaxed);

        let mut backends = vec![
            slot(
                1,
                2,
                Arc::new(MockBackend::new("cpu", 2, Arc::clone(&first_state))),
            ),
            slot(
                2,
                1,
                Arc::new(MockBackend::new("nvidia", 1, Arc::clone(&panic_state))),
            ),
            slot(
                3,
                1,
                Arc::new(MockBackend::new("nvidia", 1, Arc::clone(&third_state))),
            ),
        ];

        let additional_span = distribute_work(
            &mut backends,
            DistributeWorkOptions {
                epoch: 1,
                work_id: 1,
                header_base: Arc::from(vec![7u8; POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                reservation: NonceReservation {
                    start_nonce: 100,
                    max_iters_per_lane: 10,
                    reserved_span: 40,
                },
                stop_at: Instant::now() + Duration::from_secs(1),
                backend_weights: None,
            },
            &backend_executor,
        )
        .expect("distribution should continue after quarantining panicing backend");

        assert_eq!(additional_span, 30);
        assert_eq!(backends.len(), 2);
        assert_eq!(backends[0].id, 1);
        assert_eq!(backends[1].id, 3);
        assert!(
            wait_for_stop_call(&panic_state, Duration::from_millis(250)),
            "panicking backend should be quarantined"
        );
    }

    #[test]
    fn activate_backends_stops_backend_when_start_fails() {
        let failed_state = Arc::new(MockState::default());
        let healthy_state = Arc::new(MockState::default());
        failed_state.fail_start.store(true, Ordering::Relaxed);

        let cfg = test_config();
        let backend_spec = BackendSpec {
            kind: BackendKind::Cpu,
            device_index: None,
            cpu_threads: Some(1),
            cpu_affinity: Some(crate::config::CpuAffinityMode::Off),
            assign_timeout_override: None,
            control_timeout_override: None,
            assign_timeout_strikes_override: None,
        };
        let instances: Vec<(BackendSpec, Arc<dyn PowBackend>)> = vec![
            (
                backend_spec,
                Arc::new(MockBackend::new("cpu", 1, Arc::clone(&failed_state))),
            ),
            (
                backend_spec,
                Arc::new(MockBackend::new("cpu", 1, Arc::clone(&healthy_state))),
            ),
        ];

        let no_shutdown = AtomicBool::new(false);
        let (active, _events) = activate_backends(instances, 16, &cfg, &no_shutdown)
            .expect("activation should continue with the healthy backend");

        assert_eq!(active.len(), 1);
        assert!(
            wait_for_stop_call(&failed_state, Duration::from_millis(250)),
            "failing backend should be stopped after restart quarantine"
        );
        assert_eq!(healthy_state.stop_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn start_backend_slots_quarantines_failed_restart() {
        let failed_state = Arc::new(MockState::default());
        let healthy_state = Arc::new(MockState::default());
        failed_state.fail_start.store(true, Ordering::Relaxed);

        let mut backends = vec![
            slot(
                1,
                1,
                Arc::new(MockBackend::new("cpu", 1, Arc::clone(&failed_state))),
            ),
            slot(
                2,
                1,
                Arc::new(MockBackend::new("cpu", 1, Arc::clone(&healthy_state))),
            ),
        ];

        let backend_executor = backend_executor::BackendExecutor::new();
        start_backend_slots(&mut backends, &backend_executor, Duration::from_secs(1))
            .expect("restart should continue with healthy backend");

        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].id, 2);
        assert!(
            wait_for_stop_call(&failed_state, Duration::from_millis(250)),
            "failing backend should be stopped after restart quarantine"
        );
        assert_eq!(healthy_state.stop_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn nonce_counts_static_match_lane_quota() {
        let state_a = Arc::new(MockState::default());
        let state_b = Arc::new(MockState::default());
        let backends = vec![
            slot(
                1,
                2,
                Arc::new(MockBackend::new("cpu", 2, Arc::clone(&state_a))),
            ),
            slot(
                2,
                1,
                Arc::new(MockBackend::new("cpu", 1, Arc::clone(&state_b))),
            ),
        ];

        let counts = compute_backend_nonce_counts(&backends, 10, None);
        assert_eq!(counts, vec![20, 10]);
    }

    #[test]
    fn nonce_counts_static_respect_backend_preferred_iters() {
        let state_a = Arc::new(MockState::default());
        let state_b = Arc::new(MockState::default());
        let backends = vec![
            slot(
                1,
                2,
                Arc::new(
                    MockBackend::new("cpu", 2, Arc::clone(&state_a))
                        .with_preferred_iters_per_lane(4),
                ),
            ),
            slot(
                2,
                1,
                Arc::new(
                    MockBackend::new("nvidia", 1, Arc::clone(&state_b))
                        .with_preferred_iters_per_lane(16),
                ),
            ),
        ];

        let counts = compute_backend_nonce_counts(&backends, 10, None);
        assert_eq!(counts, vec![8, 16]);
    }

    #[test]
    fn nonce_counts_adaptive_follow_weights_without_overlap() {
        let state_a = Arc::new(MockState::default());
        let state_b = Arc::new(MockState::default());
        let backends = vec![
            slot(
                10,
                2,
                Arc::new(MockBackend::new("cpu", 2, Arc::clone(&state_a))),
            ),
            slot(
                20,
                2,
                Arc::new(MockBackend::new("cpu", 2, Arc::clone(&state_b))),
            ),
        ];
        let mut weights = BTreeMap::new();
        weights.insert(10, 9.0);
        weights.insert(20, 1.0);

        let counts = compute_backend_nonce_counts(&backends, 10, Some(&weights));
        assert_eq!(counts.iter().copied().sum::<u64>(), 40);
        assert!(counts[0] > counts[1]);
        assert!(counts[0] >= 2);
        assert!(counts[1] >= 2);
    }

    #[test]
    fn nonce_counts_adaptive_boosts_new_backend_exploration_share() {
        let state_a = Arc::new(MockState::default());
        let state_b = Arc::new(MockState::default());
        let backends = vec![
            slot(
                10,
                1,
                Arc::new(MockBackend::new("cpu", 1, Arc::clone(&state_a))),
            ),
            slot(
                20,
                1,
                Arc::new(MockBackend::new("cpu", 1, Arc::clone(&state_b))),
            ),
        ];
        let mut weights = BTreeMap::new();
        weights.insert(10, 1_000_000.0);

        let counts = compute_backend_nonce_counts(&backends, 100, Some(&weights));
        assert_eq!(counts.iter().copied().sum::<u64>(), 200);
        assert!(
            counts[1] > 2,
            "new backend should receive exploration work beyond lane minimum: {:?}",
            counts
        );
    }

    #[test]
    fn effective_hash_poll_interval_uses_backend_hint() {
        let state_a = Arc::new(MockState::default());
        let state_b = Arc::new(MockState::default());
        let backends = vec![
            slot(
                1,
                1,
                Arc::new(
                    MockBackend::new("cpu", 1, Arc::clone(&state_a))
                        .with_preferred_hash_poll_interval(Duration::from_millis(50)),
                ),
            ),
            slot(
                2,
                1,
                Arc::new(MockBackend::new("cpu", 1, Arc::clone(&state_b))),
            ),
        ];

        let effective = effective_hash_poll_interval(&backends, Duration::from_millis(200));
        assert_eq!(effective, Duration::from_millis(50));
    }

    #[test]
    fn collect_round_backend_samples_keeps_runtime_telemetry_until_backend_is_due() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let state_a = Arc::new(MockState::default());
        let state_b = Arc::new(MockState::default());
        let backend_a: Arc<dyn PowBackend> = Arc::new(
            MockBackend::new("cpu-a", 1, Arc::clone(&state_a))
                .with_assign_delay(Duration::from_millis(40)),
        );
        let backend_b: Arc<dyn PowBackend> = Arc::new(
            MockBackend::new("cpu-b", 1, Arc::clone(&state_b))
                .with_assign_delay(Duration::from_millis(40)),
        );
        let backends = vec![
            slot(1, 1, Arc::clone(&backend_a)),
            slot(2, 1, Arc::clone(&backend_b)),
        ];

        let make_work = || WorkAssignment {
            template: Arc::new(WorkTemplate {
                work_id: 1,
                epoch: 1,
                header_base: Arc::from(vec![7u8; POW_HEADER_BASE_LEN]),
                target: [0xFF; 32],
                stop_at: Instant::now() + Duration::from_secs(1),
            }),
            nonce_chunk: NonceChunk {
                start_nonce: 0,
                nonce_count: 1,
            },
        };

        let outcomes = backend_executor.dispatch_backend_tasks(vec![
            backend_executor::BackendTask {
                idx: 0,
                backend_id: 1,
                backend: "cpu-a",
                backend_handle: Arc::clone(&backend_a),
                kind: backend_executor::BackendTaskKind::Assign(make_work()),
                timeout: Duration::from_millis(5),
            },
            backend_executor::BackendTask {
                idx: 1,
                backend_id: 2,
                backend: "cpu-b",
                backend_handle: Arc::clone(&backend_b),
                kind: backend_executor::BackendTaskKind::Assign(make_work()),
                timeout: Duration::from_millis(5),
            },
        ]);
        assert!(matches!(
            outcomes[0],
            Some(backend_executor::BackendTaskDispatchResult::TimedOut(
                backend_executor::BackendTaskTimeoutKind::Execution
            ))
        ));
        assert!(matches!(
            outcomes[1],
            Some(backend_executor::BackendTaskDispatchResult::TimedOut(
                backend_executor::BackendTaskTimeoutKind::Execution
            ))
        ));

        let poll_interval = Duration::from_millis(200);
        let mut poll_state = hash_poll::build_backend_poll_state(&backends, poll_interval);
        poll_state.insert(
            1,
            (poll_interval, Instant::now() - Duration::from_millis(1)),
        );
        poll_state.insert(2, (poll_interval, Instant::now() + Duration::from_secs(30)));

        let mut round_backend_hashes = BTreeMap::new();
        let mut round_backend_telemetry = BTreeMap::new();
        let _ = collect_round_backend_samples(
            &backends,
            &backend_executor,
            poll_interval,
            &mut poll_state,
            &mut round_backend_hashes,
            &mut round_backend_telemetry,
        );
        assert_eq!(
            round_backend_telemetry
                .get(&1)
                .map(|telemetry| telemetry.assignment_execution_timeouts),
            Some(1)
        );
        assert!(
            !round_backend_telemetry.contains_key(&2),
            "backend 2 should not be merged until it is due for polling"
        );

        poll_state.insert(
            2,
            (poll_interval, Instant::now() - Duration::from_millis(1)),
        );
        let _ = collect_round_backend_samples(
            &backends,
            &backend_executor,
            poll_interval,
            &mut poll_state,
            &mut round_backend_hashes,
            &mut round_backend_telemetry,
        );
        assert_eq!(
            round_backend_telemetry
                .get(&2)
                .map(|telemetry| telemetry.assignment_execution_timeouts),
            Some(1),
            "runtime telemetry for non-due backends must remain until they are sampled"
        );
    }

    #[test]
    fn deadline_policy_rejects_best_effort_when_not_allowed() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let state = Arc::new(MockState::default());
        let mut backends = vec![slot(
            1,
            1,
            Arc::new(
                MockBackend::new("nvidia", 1, Arc::clone(&state))
                    .with_deadline_support(crate::backend::DeadlineSupport::BestEffort),
            ),
        )];

        let err =
            enforce_deadline_policy(&mut backends, false, RuntimeMode::Mining, &backend_executor)
                .expect_err("best-effort backend should be rejected by default");
        assert!(format!("{err:#}").contains("--allow-best-effort-deadlines"));
    }

    #[test]
    fn deadline_policy_quarantines_best_effort_and_keeps_cooperative_backends() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let best_effort_state = Arc::new(MockState::default());
        let cooperative_state = Arc::new(MockState::default());
        let mut backends = vec![
            slot(
                1,
                1,
                Arc::new(
                    MockBackend::new("nvidia", 1, Arc::clone(&best_effort_state))
                        .with_deadline_support(crate::backend::DeadlineSupport::BestEffort),
                ),
            ),
            slot(
                2,
                1,
                Arc::new(
                    MockBackend::new("cpu", 1, Arc::clone(&cooperative_state))
                        .with_deadline_support(crate::backend::DeadlineSupport::Cooperative),
                ),
            ),
        ];

        enforce_deadline_policy(&mut backends, false, RuntimeMode::Mining, &backend_executor)
            .expect("cooperative backend should remain available");

        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].id, 2);
        assert!(
            wait_for_stop_call(&best_effort_state, Duration::from_millis(250)),
            "best-effort backend should be quarantined asynchronously"
        );
        assert_eq!(cooperative_state.stop_calls.load(Ordering::Relaxed), 0);
    }

    #[derive(Default)]
    struct QuiesceTrace {
        events: Mutex<Vec<String>>,
    }

    struct QuiesceMockBackend {
        name: &'static str,
        trace: Arc<QuiesceTrace>,
    }

    impl QuiesceMockBackend {
        fn new(name: &'static str, trace: Arc<QuiesceTrace>) -> Self {
            Self { name, trace }
        }

        fn record(&self, action: &str) {
            if let Ok(mut events) = self.trace.events.lock() {
                events.push(format!("{action}:{}", self.name));
            }
        }
    }

    impl PowBackend for QuiesceMockBackend {
        fn name(&self) -> &'static str {
            self.name
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
            self.record("cancel");
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            self.record("fence");
            Ok(())
        }
    }

    #[test]
    fn quiesce_cancels_all_backends_before_fencing_any_backend() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let trace = Arc::new(QuiesceTrace::default());
        let mut backends = vec![
            slot(
                1,
                1,
                Arc::new(QuiesceMockBackend::new("cpu-a", Arc::clone(&trace))),
            ),
            slot(
                2,
                1,
                Arc::new(QuiesceMockBackend::new("cpu-b", Arc::clone(&trace))),
            ),
        ];

        quiesce_backend_slots(&mut backends, RuntimeMode::Mining, &backend_executor)
            .expect("quiesce should succeed");

        let events = trace
            .events
            .lock()
            .expect("trace lock should not be poisoned")
            .clone();
        let cancel_count = events
            .iter()
            .filter(|entry| entry.starts_with("cancel:"))
            .count();
        let fence_count = events
            .iter()
            .filter(|entry| entry.starts_with("fence:"))
            .count();
        assert!(cancel_count >= 2);
        assert_eq!(fence_count, 2);
        assert_eq!(events.len(), cancel_count + fence_count);

        let first_fence_idx = events
            .iter()
            .position(|entry| entry.starts_with("fence:"))
            .expect("fence events should exist");
        assert!(
            events
                .iter()
                .take(first_fence_idx)
                .all(|entry| entry.starts_with("cancel:")),
            "all pre-fence actions should be cancels: {events:?}"
        );
    }

    struct ControlFailBackend {
        name: &'static str,
        fail_cancel: bool,
        fail_fence: bool,
        stop_calls: Arc<AtomicUsize>,
    }

    impl ControlFailBackend {
        fn new(
            name: &'static str,
            fail_cancel: bool,
            fail_fence: bool,
            stop_calls: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                name,
                fail_cancel,
                fail_fence,
                stop_calls,
            }
        }
    }

    impl PowBackend for ControlFailBackend {
        fn name(&self) -> &'static str {
            self.name
        }

        fn lanes(&self) -> usize {
            1
        }

        fn set_instance_id(&self, _id: BackendInstanceId) {}

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            Ok(())
        }

        fn stop(&self) {
            self.stop_calls.fetch_add(1, Ordering::Relaxed);
        }

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            Ok(())
        }

        fn cancel_work(&self) -> Result<()> {
            if self.fail_cancel {
                Err(anyhow!("injected cancel failure"))
            } else {
                Ok(())
            }
        }

        fn fence(&self) -> Result<()> {
            if self.fail_fence {
                Err(anyhow!("injected fence failure"))
            } else {
                Ok(())
            }
        }
    }

    struct ControlPanicBackend {
        name: &'static str,
        panic_cancel: bool,
        panic_fence: bool,
        stop_calls: Arc<AtomicUsize>,
    }

    impl ControlPanicBackend {
        fn new(
            name: &'static str,
            panic_cancel: bool,
            panic_fence: bool,
            stop_calls: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                name,
                panic_cancel,
                panic_fence,
                stop_calls,
            }
        }
    }

    impl PowBackend for ControlPanicBackend {
        fn name(&self) -> &'static str {
            self.name
        }

        fn lanes(&self) -> usize {
            1
        }

        fn set_instance_id(&self, _id: BackendInstanceId) {}

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            Ok(())
        }

        fn stop(&self) {
            self.stop_calls.fetch_add(1, Ordering::Relaxed);
        }

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            Ok(())
        }

        fn cancel_work(&self) -> Result<()> {
            if self.panic_cancel {
                panic!("injected cancel panic")
            } else {
                Ok(())
            }
        }

        fn fence(&self) -> Result<()> {
            if self.panic_fence {
                panic!("injected fence panic")
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn quiesce_quarantines_only_failing_backend() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let fail_stop_calls = Arc::new(AtomicUsize::new(0));
        let ok_stop_calls = Arc::new(AtomicUsize::new(0));
        let mut backends = vec![
            slot(
                1,
                1,
                Arc::new(ControlFailBackend::new(
                    "cpu-a",
                    false,
                    true,
                    Arc::clone(&fail_stop_calls),
                )),
            ),
            slot(
                2,
                1,
                Arc::new(ControlFailBackend::new(
                    "cpu-b",
                    false,
                    false,
                    Arc::clone(&ok_stop_calls),
                )),
            ),
        ];

        let action = quiesce_backend_slots(&mut backends, RuntimeMode::Mining, &backend_executor)
            .expect("quiesce should quarantine only the failing backend");

        assert_eq!(action, RuntimeBackendEventAction::TopologyChanged);
        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].id, 2);
        assert!(
            wait_for_stop_counter(&fail_stop_calls, Duration::from_millis(250)),
            "failing backend should be stopped asynchronously"
        );
        assert_eq!(ok_stop_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn quiesce_quarantines_backend_when_control_panics() {
        let backend_executor = backend_executor::BackendExecutor::new();
        let panic_stop_calls = Arc::new(AtomicUsize::new(0));
        let ok_stop_calls = Arc::new(AtomicUsize::new(0));
        let mut backends = vec![
            slot(
                1,
                1,
                Arc::new(ControlPanicBackend::new(
                    "cpu-a",
                    false,
                    true,
                    Arc::clone(&panic_stop_calls),
                )),
            ),
            slot(
                2,
                1,
                Arc::new(ControlPanicBackend::new(
                    "cpu-b",
                    false,
                    false,
                    Arc::clone(&ok_stop_calls),
                )),
            ),
        ];

        let action = quiesce_backend_slots(&mut backends, RuntimeMode::Mining, &backend_executor)
            .expect("quiesce should quarantine panicing backend");

        assert_eq!(action, RuntimeBackendEventAction::TopologyChanged);
        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].id, 2);
        assert!(
            wait_for_stop_counter(&panic_stop_calls, Duration::from_millis(250)),
            "panicing backend should be stopped asynchronously"
        );
        assert_eq!(ok_stop_calls.load(Ordering::Relaxed), 0);
    }
}
