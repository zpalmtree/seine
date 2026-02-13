mod auth;
mod backend_control;
mod backend_executor;
mod bench;
mod hash_poll;
mod mining;
mod mining_tui;
mod round_control;
mod runtime;
mod scheduler;
mod stats;
mod template_prefetch;
mod tip;
mod tui;
mod ui;
mod wallet;
mod work_allocator;

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use blocknet_pow_spec::CPU_LANE_MEMORY_BYTES;
use crossbeam_channel::{bounded, Receiver};

use crate::api::ApiClient;
use crate::backend::cpu::CpuBackend;
use crate::backend::nvidia::NvidiaBackend;
use crate::backend::{
    BackendCapabilities, BackendEvent, BackendInstanceId, BackendTelemetry, DeadlineSupport,
    PowBackend, PreemptionGranularity, WORK_ID_MAX,
};
use crate::config::{BackendKind, Config, UiMode, WorkAllocation};
use scheduler::NonceReservation;
use stats::{format_hashrate, Stats};
use tui::{new_tui_state, TuiState};
use ui::{info, warn};

const TEMPLATE_RETRY_DELAY: Duration = Duration::from_secs(2);
const MIN_EVENT_WAIT: Duration = Duration::from_millis(1);

struct BackendSlot {
    id: BackendInstanceId,
    backend: Arc<dyn PowBackend>,
    lanes: u64,
}

struct DistributeWorkOptions<'a> {
    epoch: u64,
    work_id: u64,
    header_base: Arc<[u8]>,
    target: [u8; 32],
    reservation: NonceReservation,
    stop_at: Instant,
    assignment_timeout: Duration,
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
}

pub fn run(cfg: &Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    if cfg.bench {
        return bench::run_benchmark(cfg, shutdown.as_ref());
    }

    let backend_executor = backend_executor::BackendExecutor::new();
    backend_executor.set_assignment_timeout_threshold(cfg.backend_assign_timeout_strikes);

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
        activate_backends(backend_instances, cfg.backend_event_capacity)?;
    enforce_deadline_policy(
        &mut backends,
        cfg.allow_best_effort_deadlines,
        cfg.backend_control_timeout,
        RuntimeMode::Mining,
        &backend_executor,
    )?;
    let total_lanes = total_lanes(&backends);
    let cpu_lanes = cpu_lane_count(&backends);
    let cpu_ram_gib =
        (cpu_lanes as f64 * CPU_LANE_MEMORY_BYTES as f64) / (1024.0 * 1024.0 * 1024.0);

    let tui_state = if should_enable_tui(cfg) {
        Some(build_tui_state(cfg, &backends, total_lanes, cpu_ram_gib))
    } else {
        None
    };

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
            "timeouts | assign={}ms assign_strikes={} control={}ms prefetch_wait={}ms tip_join_wait={}ms submit_join_wait={}ms",
            cfg.backend_assign_timeout.as_millis(),
            cfg.backend_assign_timeout_strikes,
            cfg.backend_control_timeout.as_millis(),
            cfg.prefetch_wait.as_millis(),
            cfg.tip_listener_join_wait.as_millis(),
            cfg.submit_join_wait.as_millis(),
        ),
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
    stop_backend_slots(&mut backends, &backend_executor);
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

fn build_tui_state(
    cfg: &Config,
    backends: &[BackendSlot],
    total_lanes: u64,
    cpu_ram_gib: f64,
) -> TuiState {
    let tui_state = new_tui_state();
    if let Ok(mut s) = tui_state.lock() {
        s.api_url = cfg.api_url.clone();
        s.threads = cfg.threads;
        s.refresh_secs = cfg.refresh_interval.as_secs();
        s.sse_enabled = cfg.sse_enabled;
        s.backends_desc = format!(
            "{} ({} lanes, ~{:.1} GiB RAM)",
            backend_names(backends),
            total_lanes,
            cpu_ram_gib
        );
        s.accounting = if cfg.strict_round_accounting {
            "strict".to_string()
        } else {
            "relaxed".to_string()
        };
        s.version = format!("v{}", env!("CARGO_PKG_VERSION"));
    }
    tui_state
}

fn build_backend_instances(cfg: &Config) -> Vec<Arc<dyn PowBackend>> {
    cfg.backend_specs
        .iter()
        .map(|backend_spec| match backend_spec.kind {
            BackendKind::Cpu => {
                Arc::new(CpuBackend::new(cfg.threads, cfg.cpu_affinity)) as Arc<dyn PowBackend>
            }
            BackendKind::Nvidia => {
                Arc::new(NvidiaBackend::new(backend_spec.device_index)) as Arc<dyn PowBackend>
            }
        })
        .collect()
}

fn activate_backends(
    mut backends: Vec<Arc<dyn PowBackend>>,
    event_capacity: usize,
) -> Result<(Vec<BackendSlot>, Receiver<BackendEvent>)> {
    let mut active = Vec::new();
    let (event_tx, event_rx) = bounded::<BackendEvent>(event_capacity.max(1));
    let mut next_backend_id: BackendInstanceId = 1;

    for backend in backends.drain(..) {
        let backend_id = next_backend_id;
        next_backend_id = next_backend_id.saturating_add(1);
        let backend_name = backend.name();
        backend.set_instance_id(backend_id);
        backend.set_event_sink(event_tx.clone());
        match backend.start() {
            Ok(()) => {
                let capabilities =
                    backend_capabilities_for_start(backend.as_ref(), backend_name, backend_id);
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
                active.push(BackendSlot {
                    id: backend_id,
                    backend,
                    lanes,
                });
            }
            Err(err) => {
                warn(
                    "BACKEND",
                    format!("{backend_name}#{backend_id} unavailable: {err:#}"),
                );
                backend.stop();
            }
        }
    }

    if active.is_empty() {
        bail!("no mining backend could be started");
    }

    Ok((active, event_rx))
}

fn start_backend_slots(backends: &mut Vec<BackendSlot>) -> Result<()> {
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
        slot.backend.stop();
        warn(
            "BENCH",
            format!("quarantined {backend_name}#{backend_id} after restart failure"),
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
) {
    for slot in backends {
        slot.backend.stop();
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
    stats: Option<&Stats>,
    round_hashes: &mut u64,
    mut round_backend_hashes: Option<&mut BTreeMap<BackendInstanceId, u64>>,
    mut round_backend_telemetry: Option<&mut BTreeMap<BackendInstanceId, BackendRoundTelemetry>>,
) {
    let mut collected = 0u64;
    for slot in backends {
        let slot_hashes = slot.backend.take_hashes();
        let telemetry = slot.backend.take_telemetry();
        if let Some(per_backend_telemetry) = round_backend_telemetry.as_deref_mut() {
            merge_backend_telemetry(per_backend_telemetry, slot.id, telemetry);
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
    configured_hash_poll_interval: Duration,
    poll_state: &mut hash_poll::BackendPollState,
    round_backend_hashes: &mut BTreeMap<BackendInstanceId, u64>,
    round_backend_telemetry: &mut BTreeMap<BackendInstanceId, BackendRoundTelemetry>,
) -> u64 {
    let mut collected = 0u64;
    hash_poll::collect_due_backend_samples(
        backends,
        configured_hash_poll_interval,
        poll_state,
        |sample| {
            merge_backend_telemetry(round_backend_telemetry, sample.backend_id, sample.telemetry);
            if sample.hashes > 0 {
                collected = collected.saturating_add(sample.hashes);
                let entry = round_backend_hashes.entry(sample.backend_id).or_insert(0);
                *entry = entry.saturating_add(sample.hashes);
            }
        },
    );
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
    }
}

fn cancel_backend_slots(
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
    control_timeout: Duration,
    backend_executor: &backend_executor::BackendExecutor,
) -> Result<RuntimeBackendEventAction> {
    backend_control::cancel_backend_slots(backends, mode, control_timeout, backend_executor)
}

fn quiesce_backend_slots(
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
    control_timeout: Duration,
    backend_executor: &backend_executor::BackendExecutor,
) -> Result<RuntimeBackendEventAction> {
    backend_control::quiesce_backend_slots(backends, mode, control_timeout, backend_executor)
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
enum RuntimeMode {
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
    control_timeout: Duration,
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

    let probe_timeout = control_timeout.max(Duration::from_millis(1));
    let action = quiesce_backend_slots(backends, mode, probe_timeout, backend_executor)?;
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

fn backend_names(backends: &[BackendSlot]) -> String {
    backend_name_list(backends).join(",")
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
            let worker_queue_depth = if capabilities.assignment_semantics
                == crate::backend::AssignmentSemantics::Append
            {
                inflight
            } else {
                1
            };
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
                "{}#{}=alloc:{} dispatch:{} iters/lane inflight={} worker_q={} poll_hint={} nb_poll={} deadline={} assign={}",
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
    _backend_name: &'static str,
    _backend_id: BackendInstanceId,
) -> BackendCapabilities {
    normalize_backend_capabilities(
        backend.capabilities(),
        backend.supports_assignment_batching(),
    )
}

fn backend_capabilities(slot: &BackendSlot) -> BackendCapabilities {
    normalize_backend_capabilities(
        slot.backend.capabilities(),
        slot.backend.supports_assignment_batching(),
    )
}

fn normalize_backend_capabilities(
    mut capabilities: BackendCapabilities,
    supports_assignment_batching: bool,
) -> BackendCapabilities {
    capabilities.max_inflight_assignments = capabilities.max_inflight_assignments.max(1);
    if capabilities.max_inflight_assignments > 1 && !supports_assignment_batching {
        capabilities.max_inflight_assignments = 1;
    }

    capabilities.nonblocking_poll_min = capabilities
        .nonblocking_poll_min
        .map(|min_poll| min_poll.max(Duration::from_micros(10)));
    capabilities.nonblocking_poll_max = capabilities
        .nonblocking_poll_max
        .map(|max_poll| max_poll.max(Duration::from_micros(10)));
    if let (Some(min_poll), Some(max_poll)) = (
        capabilities.nonblocking_poll_min,
        capabilities.nonblocking_poll_max,
    ) {
        if max_poll < min_poll {
            capabilities.nonblocking_poll_max = Some(min_poll);
        }
    }
    capabilities
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

fn format_round_backend_hashrate(
    backends: &[BackendSlot],
    round_backend_hashes: &BTreeMap<BackendInstanceId, u64>,
    elapsed_secs: f64,
) -> String {
    let elapsed_secs = elapsed_secs.max(0.001);
    let mut parts = Vec::new();
    for (backend_id, hashes) in round_backend_hashes {
        if *hashes == 0 {
            continue;
        }
        let backend_name = backends
            .iter()
            .find(|slot| slot.id == *backend_id)
            .map(|slot| slot.backend.name())
            .unwrap_or("unknown");
        let hps = (*hashes as f64) / elapsed_secs;
        parts.push(format!(
            "{backend_name}#{backend_id}={}",
            format_hashrate(hps)
        ));
    }

    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join(", ")
    }
}

fn format_round_backend_telemetry(
    backends: &[BackendSlot],
    round_backend_telemetry: &BTreeMap<BackendInstanceId, BackendRoundTelemetry>,
) -> String {
    let mut parts = Vec::new();
    for (backend_id, telemetry) in round_backend_telemetry {
        if telemetry.dropped_events == 0
            && telemetry.completed_assignments == 0
            && telemetry.peak_active_lanes == 0
            && telemetry.peak_pending_work == 0
            && telemetry.peak_inflight_assignment_hashes == 0
            && telemetry.peak_inflight_assignment_micros == 0
        {
            continue;
        }
        let backend_name = backends
            .iter()
            .find(|slot| slot.id == *backend_id)
            .map(|slot| slot.backend.name())
            .unwrap_or("unknown");
        parts.push(format!(
            "{backend_name}#{backend_id}:active_peak={} pending_peak={} inflight_hashes_peak={} inflight_secs_peak={:.3} drops={} assignments={} assignment_hashes={} assignment_secs={:.3}",
            telemetry.peak_active_lanes,
            telemetry.peak_pending_work,
            telemetry.peak_inflight_assignment_hashes,
            telemetry.peak_inflight_assignment_micros as f64 / 1_000_000.0,
            telemetry.dropped_events,
            telemetry.completed_assignments,
            telemetry.completed_assignment_hashes,
            telemetry.completed_assignment_micros as f64 / 1_000_000.0,
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

fn next_event_wait(
    stop_at: Instant,
    last_stats_print: Instant,
    stats_interval: Duration,
    next_hash_poll_at: Instant,
    stats_enabled: bool,
) -> Duration {
    let now = Instant::now();
    let until_stop = stop_at.saturating_duration_since(now);
    let until_stats = if stats_enabled {
        let next_stats_at = last_stats_print + stats_interval;
        next_stats_at.saturating_duration_since(now)
    } else {
        until_stop
    };
    let until_hash_poll = next_hash_poll_at.saturating_duration_since(now);
    until_stop
        .min(until_stats)
        .min(until_hash_poll)
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
    use crate::backend::{NonceChunk, WorkAssignment};
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
        supports_assignment_batching: bool,
        assign_delay: Option<Duration>,
        deadline_support: crate::backend::DeadlineSupport,
        assignment_semantics: crate::backend::AssignmentSemantics,
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
                supports_assignment_batching: false,
                assign_delay: None,
                deadline_support: crate::backend::DeadlineSupport::Cooperative,
                assignment_semantics: crate::backend::AssignmentSemantics::Replace,
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

        fn supports_assignment_batching(&self) -> bool {
            self.supports_assignment_batching
        }

        fn capabilities(&self) -> crate::backend::BackendCapabilities {
            crate::backend::BackendCapabilities {
                preferred_iters_per_lane: self.preferred_iters_per_lane,
                preferred_allocation_iters_per_lane: self.preferred_allocation_iters_per_lane,
                preferred_hash_poll_interval: self.preferred_hash_poll_interval,
                max_inflight_assignments: self.max_inflight_assignments.max(1),
                deadline_support: self.deadline_support,
                assignment_semantics: self.assignment_semantics,
                nonblocking_poll_min: None,
                nonblocking_poll_max: None,
            }
        }
    }

    fn slot(id: BackendInstanceId, lanes: u64, backend: Arc<dyn PowBackend>) -> BackendSlot {
        BackendSlot { id, backend, lanes }
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
                assignment_timeout: Duration::from_secs(1),
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
                assignment_timeout: Duration::from_secs(1),
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
                assignment_timeout: Duration::from_millis(5),
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
                assignment_timeout: Duration::from_millis(5),
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
                assignment_timeout: Duration::from_millis(5),
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
                assignment_timeout: Duration::from_millis(5),
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
                assignment_timeout: Duration::from_secs(1),
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

        let instances: Vec<Arc<dyn PowBackend>> = vec![
            Arc::new(MockBackend::new("cpu", 1, Arc::clone(&failed_state))),
            Arc::new(MockBackend::new("cpu", 1, Arc::clone(&healthy_state))),
        ];

        let (active, _events) = activate_backends(instances, 16)
            .expect("activation should continue with the healthy backend");

        assert_eq!(active.len(), 1);
        assert_eq!(failed_state.stop_calls.load(Ordering::Relaxed), 1);
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

        start_backend_slots(&mut backends).expect("restart should continue with healthy backend");

        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].id, 2);
        assert_eq!(failed_state.stop_calls.load(Ordering::Relaxed), 1);
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

        let err = enforce_deadline_policy(
            &mut backends,
            false,
            Duration::from_secs(1),
            RuntimeMode::Mining,
            &backend_executor,
        )
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

        enforce_deadline_policy(
            &mut backends,
            false,
            Duration::from_secs(1),
            RuntimeMode::Mining,
            &backend_executor,
        )
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

        quiesce_backend_slots(
            &mut backends,
            RuntimeMode::Mining,
            Duration::from_secs(1),
            &backend_executor,
        )
        .expect("quiesce should succeed");

        let events = trace
            .events
            .lock()
            .expect("trace lock should not be poisoned")
            .clone();
        assert_eq!(events.len(), 4);
        assert_eq!(
            events
                .iter()
                .filter(|entry| entry.starts_with("cancel:"))
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|entry| entry.starts_with("fence:"))
                .count(),
            2
        );

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

        let action = quiesce_backend_slots(
            &mut backends,
            RuntimeMode::Mining,
            Duration::from_secs(1),
            &backend_executor,
        )
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

        let action = quiesce_backend_slots(
            &mut backends,
            RuntimeMode::Mining,
            Duration::from_secs(1),
            &backend_executor,
        )
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
