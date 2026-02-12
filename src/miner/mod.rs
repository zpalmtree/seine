mod bench;
mod mining;
mod scheduler;
mod stats;
mod tip;
mod tui;
mod ui;

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use blocknet_pow_spec::CPU_LANE_MEMORY_BYTES;
use crossbeam_channel::{bounded, Receiver};

use crate::api::ApiClient;
use crate::backend::cpu::CpuBackend;
use crate::backend::nvidia::NvidiaBackend;
use crate::backend::{
    BackendEvent, BackendInstanceId, BackendTelemetry, NonceChunk, PowBackend,
    PreemptionGranularity, WorkAssignment, WorkTemplate, WORK_ID_MAX,
};
use crate::config::{BackendKind, Config, UiMode, WorkAllocation};
use scheduler::NonceReservation;
use stats::{format_hashrate, Stats};
use tui::{new_tui_state, TuiState};
use ui::{error, info, warn};

const TEMPLATE_RETRY_DELAY: Duration = Duration::from_secs(2);
const MIN_EVENT_WAIT: Duration = Duration::from_millis(1);

struct BackendSlot {
    id: BackendInstanceId,
    backend: Box<dyn PowBackend>,
    lanes: u64,
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
struct BackendRoundTelemetry {
    dropped_events: u64,
    completed_assignments: u64,
    completed_assignment_hashes: u64,
    completed_assignment_micros: u64,
    peak_active_lanes: u64,
    peak_pending_work: u64,
}

pub fn run(cfg: &Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    if cfg.bench {
        return bench::run_benchmark(cfg, shutdown.as_ref());
    }

    let token = cfg
        .token
        .clone()
        .ok_or_else(|| anyhow!("missing API token in mining mode"))?;
    let client = ApiClient::new(
        cfg.api_url.clone(),
        token,
        cfg.request_timeout,
        cfg.events_stream_timeout,
    )?;

    let backend_instances = build_backend_instances(cfg);
    let (mut backends, backend_events) =
        activate_backends(backend_instances, cfg.backend_event_capacity)?;
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
        &mut backends,
        &backend_events,
        tui_state,
        tip_listener.as_ref().map(mining::TipListener::signal),
    );

    let shutdown_requested = shutdown.load(Ordering::SeqCst);
    stop_backend_slots(&mut backends);
    shutdown.store(true, Ordering::SeqCst);
    if let Some(listener) = tip_listener {
        if shutdown_requested {
            listener.detach();
        } else {
            listener.join();
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

fn build_backend_instances(cfg: &Config) -> Vec<Box<dyn PowBackend>> {
    cfg.backends
        .iter()
        .map(|backend_kind| match backend_kind {
            BackendKind::Cpu => {
                Box::new(CpuBackend::new(cfg.threads, cfg.cpu_affinity)) as Box<dyn PowBackend>
            }
            BackendKind::Nvidia => Box::new(NvidiaBackend::new()) as Box<dyn PowBackend>,
        })
        .collect()
}

fn activate_backends(
    mut backends: Vec<Box<dyn PowBackend>>,
    event_capacity: usize,
) -> Result<(Vec<BackendSlot>, Receiver<BackendEvent>)> {
    let mut active = Vec::new();
    let (event_tx, event_rx) = bounded::<BackendEvent>(event_capacity.max(1));
    let mut next_backend_id: BackendInstanceId = 1;

    for mut backend in backends.drain(..) {
        let backend_id = next_backend_id;
        next_backend_id = next_backend_id.saturating_add(1);
        let backend_name = backend.name();
        backend.set_instance_id(backend_id);
        backend.set_event_sink(event_tx.clone());
        match backend.start() {
            Ok(()) => {
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

fn start_backend_slots(backends: &mut [BackendSlot]) -> Result<()> {
    for slot in backends {
        slot.backend.start().with_context(|| {
            format!(
                "failed to start backend {}#{}",
                slot.backend.name(),
                slot.id
            )
        })?;
    }
    Ok(())
}

fn stop_backend_slots(backends: &mut [BackendSlot]) {
    for slot in backends {
        slot.backend.stop();
    }
}

fn distribute_work(
    backends: &mut Vec<BackendSlot>,
    options: DistributeWorkOptions<'_>,
) -> Result<u64> {
    let mut attempt_start_nonce = options.reservation.start_nonce;
    let mut total_span_consumed = 0u64;
    loop {
        if backends.is_empty() {
            bail!("all mining backends are unavailable");
        }

        let nonce_counts = compute_backend_nonce_counts(
            backends,
            options.reservation.max_iters_per_lane,
            options.backend_weights,
        );
        let total_lanes = total_lanes(backends);
        let attempt_span = nonce_counts.iter().copied().sum::<u64>().max(total_lanes);
        total_span_consumed = total_span_consumed.wrapping_add(attempt_span);
        let template = Arc::new(WorkTemplate {
            work_id: options.work_id,
            epoch: options.epoch,
            header_base: Arc::clone(&options.header_base),
            target: options.target,
            stop_at: options.stop_at,
        });
        let mut failed_indices = Vec::new();
        let mut chunk_start = attempt_start_nonce;

        for (idx, slot) in backends.iter().enumerate() {
            let nonce_count = *nonce_counts.get(idx).unwrap_or(&slot.lanes.max(1));
            let work = WorkAssignment {
                template: Arc::clone(&template),
                nonce_chunk: NonceChunk {
                    start_nonce: chunk_start,
                    nonce_count,
                },
            };
            if let Err(err) = slot.backend.assign_work(work) {
                warn(
                    "BACKEND",
                    format!(
                        "{}#{} failed work assignment: {err:#}",
                        slot.backend.name(),
                        slot.id
                    ),
                );
                failed_indices.push(idx);
            }
            chunk_start = chunk_start.wrapping_add(nonce_count);
        }

        if failed_indices.is_empty() {
            return Ok(total_span_consumed.saturating_sub(options.reservation.reserved_span));
        }

        for idx in failed_indices.into_iter().rev() {
            let mut slot = backends.remove(idx);
            let backend_name = slot.backend.name();
            let backend_id = slot.id;
            slot.backend.stop();
            warn(
                "BACKEND",
                format!(
                    "quarantined {}#{} due to assignment failure",
                    backend_name, backend_id
                ),
            );
        }

        if backends.is_empty() {
            bail!("all mining backends are unavailable after assignment failure");
        }

        attempt_start_nonce = attempt_start_nonce.wrapping_add(attempt_span);
        warn(
            "BACKEND",
            format!(
                "retrying work assignment with remaining={} start_nonce={}",
                backend_names(backends),
                attempt_start_nonce
            ),
        );
    }
}

fn compute_backend_nonce_counts(
    backends: &[BackendSlot],
    max_iters_per_lane: u64,
    backend_weights: Option<&BTreeMap<BackendInstanceId, f64>>,
) -> Vec<u64> {
    if backends.is_empty() {
        return Vec::new();
    }

    let base_counts: Vec<u64> = backends.iter().map(|slot| slot.lanes.max(1)).collect();
    if backend_weights.is_none() {
        return base_counts
            .iter()
            .map(|lanes| lanes.saturating_mul(max_iters_per_lane).max(*lanes))
            .collect();
    }

    let total_lanes = total_lanes(backends);
    let total_span = total_lanes
        .saturating_mul(max_iters_per_lane)
        .max(total_lanes);
    let min_total: u64 = base_counts.iter().copied().sum();
    if total_span <= min_total {
        return base_counts;
    }

    let weights_map = backend_weights.expect("weights should be present");
    let mut weights = Vec::with_capacity(backends.len());
    let mut sum_weights = 0.0f64;
    for slot in backends {
        let fallback = slot.lanes.max(1) as f64;
        let weight = weights_map
            .get(&slot.id)
            .copied()
            .filter(|w| w.is_finite() && *w > 0.0)
            .unwrap_or(fallback);
        sum_weights += weight;
        weights.push(weight);
    }
    if !sum_weights.is_finite() || sum_weights <= 0.0 {
        return base_counts;
    }

    let remaining = total_span - min_total;
    let mut extra = vec![0u64; backends.len()];
    let mut floors = 0u64;
    let mut remainders: Vec<(usize, f64, BackendInstanceId)> = Vec::with_capacity(backends.len());
    for (idx, weight) in weights.iter().enumerate() {
        let raw = (remaining as f64) * (*weight / sum_weights);
        let floor = raw.floor() as u64;
        floors = floors.saturating_add(floor);
        extra[idx] = floor;
        remainders.push((idx, raw - floor as f64, backends[idx].id));
    }

    let mut leftover = remaining.saturating_sub(floors);
    if leftover > 0 {
        remainders.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.2.cmp(&b.2))
        });
        for (idx, _, _) in remainders {
            if leftover == 0 {
                break;
            }
            extra[idx] = extra[idx].saturating_add(1);
            leftover -= 1;
        }
    }

    base_counts
        .iter()
        .zip(extra)
        .map(|(base, add)| base.saturating_add(add))
        .collect()
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

fn merge_backend_telemetry(
    per_backend_telemetry: &mut BTreeMap<BackendInstanceId, BackendRoundTelemetry>,
    backend_id: BackendInstanceId,
    telemetry: BackendTelemetry,
) {
    if telemetry.active_lanes == 0
        && telemetry.pending_work == 0
        && telemetry.dropped_events == 0
        && telemetry.completed_assignments == 0
        && telemetry.completed_assignment_hashes == 0
        && telemetry.completed_assignment_micros == 0
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
    entry.peak_active_lanes = entry.peak_active_lanes.max(telemetry.active_lanes);
    entry.peak_pending_work = entry.peak_pending_work.max(telemetry.pending_work);
}

fn quiesce_backend_slots(backends: &[BackendSlot]) -> Result<()> {
    for slot in backends {
        slot.backend.quiesce().with_context(|| {
            format!(
                "failed to quiesce backend {}#{}",
                slot.backend.name(),
                slot.id
            )
        })?;
    }
    Ok(())
}

fn remove_backend_by_id(backends: &mut Vec<BackendSlot>, backend_id: BackendInstanceId) -> bool {
    let Some(idx) = backends.iter().position(|slot| slot.id == backend_id) else {
        return false;
    };

    let mut slot = backends.remove(idx);
    slot.backend.stop();
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

fn handle_runtime_backend_event(
    event: BackendEvent,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
) -> Result<(
    RuntimeBackendEventAction,
    Option<crate::backend::MiningSolution>,
)> {
    match event {
        BackendEvent::Solution(solution) => {
            let backend_active = backends.iter().any(|slot| slot.id == solution.backend_id);
            if solution.epoch == epoch && backend_active {
                match mode {
                    RuntimeMode::Mining => {
                        return Ok((RuntimeBackendEventAction::None, Some(solution)));
                    }
                    RuntimeMode::Bench => {
                        info(
                            "BENCH",
                            format!(
                                "unexpected solution from {}#{} at nonce={}",
                                solution.backend, solution.backend_id, solution.nonce
                            ),
                        );
                    }
                }
            }
            Ok((RuntimeBackendEventAction::None, None))
        }
        BackendEvent::Error {
            backend_id,
            backend,
            message,
        } => {
            match mode {
                RuntimeMode::Mining => {
                    error(
                        "BACKEND",
                        format!("{backend}#{backend_id} runtime error: {message}"),
                    );
                }
                RuntimeMode::Bench => {
                    error(
                        "BENCH",
                        format!("backend '{backend}#{backend_id}' runtime error: {message}"),
                    );
                }
            }

            let removed = remove_backend_by_id(backends, backend_id);
            if removed {
                if backends.is_empty() {
                    match mode {
                        RuntimeMode::Mining => {
                            bail!(
                                "all mining backends are unavailable after failure in '{backend}#{backend_id}'"
                            );
                        }
                        RuntimeMode::Bench => {
                            bail!(
                                "all benchmark backends are unavailable after failure in '{backend}#{backend_id}'"
                            );
                        }
                    }
                }

                match mode {
                    RuntimeMode::Mining => {
                        warn(
                            "BACKEND",
                            format!(
                                "quarantined {backend}#{backend_id}; continuing with {}",
                                backend_names(backends)
                            ),
                        );
                    }
                    RuntimeMode::Bench => {
                        warn(
                            "BENCH",
                            format!(
                                "quarantined {backend}#{backend_id}; remaining backends={}",
                                backend_names(backends)
                            ),
                        );
                    }
                }
                Ok((RuntimeBackendEventAction::TopologyChanged, None))
            } else {
                if mode == RuntimeMode::Mining {
                    warn(
                        "BACKEND",
                        format!("ignoring error from unavailable backend '{backend}#{backend_id}'"),
                    );
                }
                Ok((RuntimeBackendEventAction::None, None))
            }
        }
    }
}

fn drain_runtime_backend_events(
    backend_events: &Receiver<BackendEvent>,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
) -> Result<(
    RuntimeBackendEventAction,
    Option<crate::backend::MiningSolution>,
)> {
    let mut action = RuntimeBackendEventAction::None;
    let mut solution = None;
    while let Ok(event) = backend_events.try_recv() {
        let (event_action, maybe_solution) =
            handle_runtime_backend_event(event, epoch, backends, mode)?;
        if event_action == RuntimeBackendEventAction::TopologyChanged {
            action = RuntimeBackendEventAction::TopologyChanged;
        }
        if solution.is_none() {
            solution = maybe_solution;
        }
    }
    Ok((action, solution))
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

fn cpu_lane_count(backends: &[BackendSlot]) -> u64 {
    backends
        .iter()
        .filter(|slot| slot.backend.name() == "cpu")
        .map(|slot| slot.lanes)
        .sum()
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
        {
            continue;
        }
        let backend_name = backends
            .iter()
            .find(|slot| slot.id == *backend_id)
            .map(|slot| slot.backend.name())
            .unwrap_or("unknown");
        parts.push(format!(
            "{backend_name}#{backend_id}:active_peak={} pending_peak={} drops={} assignments={} assignment_hashes={} assignment_secs={:.3}",
            telemetry.peak_active_lanes,
            telemetry.peak_pending_work,
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
) -> Duration {
    let now = Instant::now();
    let until_stop = stop_at.saturating_duration_since(now);
    let next_stats_at = last_stats_print + stats_interval;
    let until_stats = next_stats_at.saturating_duration_since(now);
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
    use anyhow::anyhow;
    use blocknet_pow_spec::POW_HEADER_BASE_LEN;
    use crossbeam_channel::Sender;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MockState {
        fail_start: AtomicBool,
        fail_next_assign: AtomicBool,
        assign_calls: AtomicUsize,
        stop_calls: AtomicUsize,
        last_chunk: Mutex<Option<NonceChunk>>,
    }

    struct MockBackend {
        name: &'static str,
        lanes: usize,
        state: Arc<MockState>,
    }

    impl MockBackend {
        fn new(name: &'static str, lanes: usize, state: Arc<MockState>) -> Self {
            Self { name, lanes, state }
        }
    }

    impl PowBackend for MockBackend {
        fn name(&self) -> &'static str {
            self.name
        }

        fn lanes(&self) -> usize {
            self.lanes
        }

        fn set_instance_id(&mut self, _id: BackendInstanceId) {}

        fn set_event_sink(&mut self, _sink: Sender<BackendEvent>) {}

        fn start(&mut self) -> Result<()> {
            if self.state.fail_start.load(Ordering::Acquire) {
                return Err(anyhow!("injected start failure"));
            }
            Ok(())
        }

        fn stop(&mut self) {
            self.state.stop_calls.fetch_add(1, Ordering::Relaxed);
        }

        fn assign_work(&self, work: WorkAssignment) -> Result<()> {
            self.state.assign_calls.fetch_add(1, Ordering::Relaxed);
            if self.state.fail_next_assign.swap(false, Ordering::AcqRel) {
                return Err(anyhow!("injected assign failure"));
            }
            *self
                .state
                .last_chunk
                .lock()
                .expect("mock chunk lock should not be poisoned") = Some(work.nonce_chunk);
            Ok(())
        }
    }

    fn slot(id: BackendInstanceId, lanes: u64, backend: Box<dyn PowBackend>) -> BackendSlot {
        BackendSlot { id, backend, lanes }
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
        let first_state = Arc::new(MockState::default());
        let second_state = Arc::new(MockState::default());

        let mut backends = vec![
            slot(
                11,
                1,
                Box::new(MockBackend::new("nvidia", 1, Arc::clone(&first_state))),
            ),
            slot(
                22,
                1,
                Box::new(MockBackend::new("nvidia", 1, Arc::clone(&second_state))),
            ),
        ];
        assert!(remove_backend_by_id(&mut backends, 22));
        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].id, 11);
        assert_eq!(first_state.stop_calls.load(Ordering::Relaxed), 0);
        assert_eq!(second_state.stop_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn distribute_work_quarantines_assignment_failures_and_reassigns_lanes() {
        let first_state = Arc::new(MockState::default());
        let failed_state = Arc::new(MockState::default());
        let third_state = Arc::new(MockState::default());
        failed_state.fail_next_assign.store(true, Ordering::Relaxed);

        let mut backends = vec![
            slot(
                1,
                2,
                Box::new(MockBackend::new("cpu", 2, Arc::clone(&first_state))),
            ),
            slot(
                2,
                1,
                Box::new(MockBackend::new("nvidia", 1, Arc::clone(&failed_state))),
            ),
            slot(
                3,
                1,
                Box::new(MockBackend::new("nvidia", 1, Arc::clone(&third_state))),
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
        assert_eq!(failed_state.stop_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn activate_backends_stops_backend_when_start_fails() {
        let failed_state = Arc::new(MockState::default());
        let healthy_state = Arc::new(MockState::default());
        failed_state.fail_start.store(true, Ordering::Relaxed);

        let instances: Vec<Box<dyn PowBackend>> = vec![
            Box::new(MockBackend::new("cpu", 1, Arc::clone(&failed_state))),
            Box::new(MockBackend::new("cpu", 1, Arc::clone(&healthy_state))),
        ];

        let (active, _events) = activate_backends(instances, 16)
            .expect("activation should continue with the healthy backend");

        assert_eq!(active.len(), 1);
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
                Box::new(MockBackend::new("cpu", 2, Arc::clone(&state_a))),
            ),
            slot(
                2,
                1,
                Box::new(MockBackend::new("cpu", 1, Arc::clone(&state_b))),
            ),
        ];

        let counts = compute_backend_nonce_counts(&backends, 10, None);
        assert_eq!(counts, vec![20, 10]);
    }

    #[test]
    fn nonce_counts_adaptive_follow_weights_without_overlap() {
        let state_a = Arc::new(MockState::default());
        let state_b = Arc::new(MockState::default());
        let backends = vec![
            slot(
                10,
                2,
                Box::new(MockBackend::new("cpu", 2, Arc::clone(&state_a))),
            ),
            slot(
                20,
                2,
                Box::new(MockBackend::new("cpu", 2, Arc::clone(&state_b))),
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
}
