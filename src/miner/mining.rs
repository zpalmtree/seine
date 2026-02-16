use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use blocknet_pow_spec::POW_HEADER_BASE_LEN;
use crossbeam_channel::Receiver;

use crate::api::ApiClient;
use crate::backend::{BackendEvent, MiningSolution};
use crate::config::{Config, WorkAllocation};
use crate::dev_fee::DevFeeTracker;
use crate::types::{
    decode_hex, parse_target, template_difficulty, template_height, BlockTemplateResponse,
};

use super::hash_poll::build_backend_poll_state;
use super::mining_tui::{
    init_tui_display, render_tui_now, set_tui_state_label, update_tui, RoundUiView, TuiDisplay,
};
use super::round_control::{redistribute_for_topology_change, TopologyRedistributionOptions};
use super::runtime::{
    maybe_print_stats, seed_backend_weights, update_backend_weights, work_distribution_weights,
    RoundEndReason, WeightUpdateInputs,
};
use super::scheduler::NonceScheduler;
use super::solution_cache::{
    already_submitted_solution, drop_solution_from_deferred_indexed,
    push_deferred_solution_indexed, recent_template_cache_size_from_timeouts,
    recent_template_retention_from_timeouts, remember_recent_template, remember_submitted_solution,
    submit_template_for_solution_epoch, take_deferred_solutions_indexed, RecentTemplateEntry,
    RECENT_TEMPLATE_CACHE_MAX_BYTES,
};
#[cfg(test)]
use super::solution_cache::{
    dedupe_queued_solutions, drop_solution_from_deferred, push_deferred_solution,
    DEFERRED_SOLUTIONS_CAPACITY, RECENT_SUBMITTED_SOLUTIONS_CAPACITY, RECENT_TEMPLATE_CACHE_MAX,
    RECENT_TEMPLATE_CACHE_MIN,
};
use super::stats::Stats;
#[cfg(test)]
use super::submit::process_submit_request;
use super::submit::{
    SubmitEnqueueOutcome, SubmitOutcome, SubmitRequest, SubmitResult, SubmitTemplate, SubmitWorker,
};
use super::template_prefetch::{fetch_template_once, PrefetchOutcome, TemplatePrefetch};
pub(super) use super::tip::{spawn_tip_listener, TipListener, TipSignal};
use super::tui::TuiState;
use super::ui::{error, info, mined, success, warn};
use super::wallet::auto_load_wallet;
use super::{
    cancel_backend_slots, collect_backend_hashes, distribute_work, format_round_backend_telemetry,
    next_work_id, quiesce_backend_slots, total_lanes, BackendRoundTelemetry, BackendSlot,
    RuntimeBackendEventAction, RuntimeMode, TEMPLATE_RETRY_DELAY,
};

type BackendEventAction = RuntimeBackendEventAction;

const RETRY_LOG_INTERVAL: Duration = Duration::from_secs(10);
const SUBMIT_BACKLOG_CAPACITY: usize = 512;
const SUBMIT_BACKLOG_HARD_CAPACITY: usize = 4096;
const SUBMIT_BACKLOG_FLUSH_WAIT: Duration = Duration::from_millis(10);
const SUBMIT_BACKLOG_BACKPRESSURE_LOG_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
struct RetryTracker {
    failures: u64,
    last_log_at: Option<Instant>,
    outage_logged: bool,
}

impl RetryTracker {
    fn note_failure(&mut self, tag: &str, first: &str, repeat: &str, immediate_first: bool) {
        let now = Instant::now();
        self.failures = self.failures.saturating_add(1);

        let should_log = if self.failures == 1 {
            immediate_first
        } else {
            self.last_log_at
                .is_none_or(|last| now.saturating_duration_since(last) >= RETRY_LOG_INTERVAL)
        };

        if should_log {
            let message = if self.failures == 1 { first } else { repeat };
            warn(tag, message);
            self.last_log_at = Some(now);
            self.outage_logged = true;
        }
    }

    fn note_recovered(&mut self, tag: &str, message: &str) {
        if self.failures == 0 {
            return;
        }
        if !self.outage_logged {
            *self = Self::default();
            return;
        }
        success(tag, message);
        *self = Self::default();
    }
}

struct MiningControlPlane<'a> {
    client: &'a ApiClient,
    cfg: &'a Config,
    shutdown: Arc<AtomicBool>,
    tip_signal: Option<&'a TipSignal>,
    prefetch: Option<TemplatePrefetch>,
    submit_worker: Option<SubmitWorker>,
    submit_backlog: VecDeque<SubmitRequest>,
    pending_submit_results: Vec<SubmitResult>,
    submit_backlog_high_watermark_logged: bool,
    submit_backlog_last_saturation_log: Option<Instant>,
    dev_fee_address: Option<&'static str>,
}

struct RoundLoopState {
    solved: Option<MiningSolution>,
    stale_tip_event: bool,
    round_hashes: u64,
    round_backend_hashes: BTreeMap<u64, u64>,
    round_backend_telemetry: BTreeMap<u64, BackendRoundTelemetry>,
}

struct PreparedTemplate {
    header_base: Arc<[u8]>,
    target: [u8; 32],
    height: String,
    difficulty: String,
}

fn current_tip_sequence(tip_signal: Option<&TipSignal>) -> u64 {
    tip_signal.map(TipSignal::snapshot_sequence).unwrap_or(0)
}

impl<'a> MiningControlPlane<'a> {
    fn new(
        client: &'a ApiClient,
        cfg: &'a Config,
        shutdown: Arc<AtomicBool>,
        tip_signal: Option<&'a TipSignal>,
    ) -> Self {
        Self {
            client,
            cfg,
            prefetch: Some(TemplatePrefetch::spawn(
                client.clone(),
                cfg.clone(),
                Arc::clone(&shutdown),
            )),
            submit_worker: Some(SubmitWorker::spawn(
                client.clone(),
                Arc::clone(&shutdown),
                cfg.token_cookie_path.clone(),
            )),
            submit_backlog: VecDeque::with_capacity(SUBMIT_BACKLOG_CAPACITY),
            pending_submit_results: Vec::new(),
            submit_backlog_high_watermark_logged: false,
            submit_backlog_last_saturation_log: None,
            shutdown,
            tip_signal,
            dev_fee_address: None,
        }
    }

    fn ensure_submit_worker(&mut self) {
        if self.submit_worker.is_some() {
            return;
        }
        self.submit_worker = Some(SubmitWorker::spawn(
            self.client.clone(),
            Arc::clone(&self.shutdown),
            self.cfg.token_cookie_path.clone(),
        ));
    }

    fn enqueue_submit_request(
        &mut self,
        request: SubmitRequest,
        stats: &Stats,
        tui: &mut Option<TuiDisplay>,
    ) -> bool {
        // Keep the mining control loop responsive: attempt one flush pass and avoid blocking if
        // submit workers are saturated.
        self.flush_submit_backlog();
        self.collect_submit_worker_results(stats, tui);

        if self.submit_backlog.len() >= SUBMIT_BACKLOG_CAPACITY
            && !self.submit_backlog_high_watermark_logged
        {
            warn(
                "SUBMIT",
                format!(
                    "submit backlog exceeded soft limit ({}); retaining queued requests while worker catches up",
                    SUBMIT_BACKLOG_CAPACITY
                ),
            );
            self.submit_backlog_high_watermark_logged = true;
        }

        self.enforce_submit_backpressure(stats, tui);
        self.try_push_submit_backlog(request)
    }

    fn maybe_log_submit_backpressure(&mut self, message: String) {
        let now = Instant::now();
        if self.submit_backlog_last_saturation_log.is_none_or(|last| {
            now.saturating_duration_since(last) >= SUBMIT_BACKLOG_BACKPRESSURE_LOG_INTERVAL
        }) {
            warn("SUBMIT", message);
            self.submit_backlog_last_saturation_log = Some(now);
        }
    }

    fn enforce_submit_backpressure(&mut self, stats: &Stats, tui: &mut Option<TuiDisplay>) {
        if self.submit_backlog.len() < SUBMIT_BACKLOG_HARD_CAPACITY {
            return;
        }

        self.maybe_log_submit_backpressure(format!(
            "submit backlog reached hard threshold ({}); skipping enqueue wait to keep mining loop responsive (queued={})",
            SUBMIT_BACKLOG_HARD_CAPACITY,
            self.submit_backlog.len()
        ));
        self.flush_submit_backlog();
        self.collect_submit_worker_results(stats, tui);

        if self.submit_backlog.len() >= SUBMIT_BACKLOG_HARD_CAPACITY {
            self.maybe_log_submit_backpressure(format!(
                "submit backlog remains saturated; new submit requests will be deferred until queue drains (queued={})",
                self.submit_backlog.len()
            ));
        }
    }

    fn try_push_submit_backlog(&mut self, request: SubmitRequest) -> bool {
        if self.submit_backlog.len() >= SUBMIT_BACKLOG_HARD_CAPACITY {
            self.maybe_log_submit_backpressure(format!(
                "submit backlog hard cap reached ({}); deferring new submit request",
                SUBMIT_BACKLOG_HARD_CAPACITY
            ));
            return false;
        }
        self.submit_backlog.push_back(request);
        true
    }

    fn collect_submit_worker_results(&mut self, stats: &Stats, tui: &mut Option<TuiDisplay>) {
        if let Some(worker) = self.submit_worker.as_ref() {
            self.pending_submit_results
                .extend(worker.drain_results(stats, tui));
        }
    }

    fn flush_submit_backlog(&mut self) {
        if self.submit_backlog.is_empty() {
            return;
        }

        self.ensure_submit_worker();
        while let Some(request) = self.submit_backlog.pop_front() {
            let Some(worker) = self.submit_worker.as_ref() else {
                self.submit_backlog.push_front(request);
                break;
            };

            match worker.submit(request) {
                SubmitEnqueueOutcome::Queued => {}
                SubmitEnqueueOutcome::Full(request) => {
                    self.submit_backlog.push_front(request);
                    break;
                }
                SubmitEnqueueOutcome::Closed(request) => {
                    warn("SUBMIT", "submit worker disconnected; respawning");
                    self.submit_worker = None;
                    self.submit_backlog.push_front(request);
                    self.ensure_submit_worker();
                    break;
                }
            }
        }

        if self.submit_backlog.len() < SUBMIT_BACKLOG_CAPACITY {
            self.submit_backlog_high_watermark_logged = false;
        }
        if self.submit_backlog.len() < SUBMIT_BACKLOG_HARD_CAPACITY {
            self.submit_backlog_last_saturation_log = None;
        }
    }

    fn flush_submit_backlog_for(
        &mut self,
        max_wait: Duration,
        stats: &Stats,
        tui: &mut Option<TuiDisplay>,
    ) {
        let max_wait = max_wait.max(Duration::from_millis(1));
        let deadline = Instant::now() + max_wait;

        while !self.submit_backlog.is_empty() && Instant::now() < deadline {
            self.flush_submit_backlog();
            self.collect_submit_worker_results(stats, tui);
            if self.submit_backlog.is_empty() {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            thread::sleep(remaining.min(SUBMIT_BACKLOG_FLUSH_WAIT));
        }
    }

    fn fetch_initial_template(
        &mut self,
        tui: &mut Option<TuiDisplay>,
    ) -> Option<BlockTemplateResponse> {
        self.resolve_next_template(tui)
    }

    fn spawn_prefetch_if_needed(&mut self) {
        if let Some(prefetch) = self.prefetch.as_mut() {
            prefetch.request_if_idle(current_tip_sequence(self.tip_signal), self.dev_fee_address);
        }
    }

    fn resolve_next_template(
        &mut self,
        tui: &mut Option<TuiDisplay>,
    ) -> Option<BlockTemplateResponse> {
        resolve_next_template(
            &mut self.prefetch,
            self.client,
            self.cfg,
            &self.shutdown,
            self.tip_signal,
            tui,
            self.dev_fee_address,
        )
    }

    fn set_dev_fee_address(&mut self, address: Option<&'static str>) {
        self.dev_fee_address = address;
    }

    fn submit_template(
        &mut self,
        template: SubmitTemplate,
        solution: MiningSolution,
        stats: &Stats,
        tui: &mut Option<TuiDisplay>,
    ) -> bool {
        let is_dev_fee = self.dev_fee_address.is_some();
        if !self.enqueue_submit_request(SubmitRequest { template, solution, is_dev_fee }, stats, tui) {
            return false;
        }
        stats.bump_submitted();
        self.flush_submit_backlog();
        self.collect_submit_worker_results(stats, tui);
        true
    }

    fn drain_submit_results(
        &mut self,
        stats: &Stats,
        tui: &mut Option<TuiDisplay>,
    ) -> Vec<SubmitResult> {
        self.flush_submit_backlog();
        self.collect_submit_worker_results(stats, tui);
        self.flush_submit_backlog();
        self.collect_submit_worker_results(stats, tui);
        std::mem::take(&mut self.pending_submit_results)
    }

    fn finish(mut self, stats: &Stats, tui: &mut Option<TuiDisplay>) -> Vec<SubmitResult> {
        if self.shutdown.load(Ordering::Relaxed) {
            let dropped = self.submit_backlog.len() as u64;
            stats.add_dropped(dropped);
            self.submit_backlog.clear();
        } else {
            self.flush_submit_backlog_for(self.cfg.submit_join_wait, stats, tui);
            if !self.submit_backlog.is_empty() {
                let dropped = self.submit_backlog.len() as u64;
                stats.add_dropped(dropped);
                warn(
                    "SUBMIT",
                    format!(
                        "dropping {} queued submit request(s) during shutdown",
                        self.submit_backlog.len()
                    ),
                );
                self.submit_backlog.clear();
            }
        }

        if let Some(mut submit_worker) = self.submit_worker.take() {
            if self.shutdown.load(Ordering::Relaxed) {
                submit_worker.detach();
            } else {
                if !submit_worker.shutdown_for(self.cfg.submit_join_wait) {
                    warn(
                        "SUBMIT",
                        format!(
                            "submit worker shutdown exceeded {}ms; detached",
                            self.cfg.submit_join_wait.as_millis()
                        ),
                    );
                }
                self.pending_submit_results
                    .extend(submit_worker.drain_results(stats, tui));
            }
        }

        if let Some(mut prefetch_task) = self.prefetch.take() {
            if self.shutdown.load(Ordering::Relaxed) {
                prefetch_task.detach();
            } else if !prefetch_task.shutdown_for(self.cfg.prefetch_wait) {
                warn(
                    "TEMPLATE",
                    format!(
                        "prefetch worker shutdown exceeded {}ms; detached",
                        self.cfg.prefetch_wait.as_millis()
                    ),
                );
            }
        }
        std::mem::take(&mut self.pending_submit_results)
    }
}

pub(super) struct MiningRuntimeBackends<'a> {
    pub backends: &'a mut Vec<BackendSlot>,
    pub backend_events: &'a Receiver<BackendEvent>,
    pub backend_executor: &'a super::backend_executor::BackendExecutor,
}

fn prepare_round_template(
    template: &mut BlockTemplateResponse,
    control_plane: &mut MiningControlPlane<'_>,
    shutdown: &AtomicBool,
    tip_signal: Option<&TipSignal>,
    tui: &mut Option<TuiDisplay>,
) -> Option<PreparedTemplate> {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return None;
        }

        let header_base = match decode_hex(&template.header_base, "header_base") {
            Ok(v) => v,
            Err(err) => {
                warn("TEMPLATE", format!("decode error: {err:#}"));
                if !sleep_with_shutdown(shutdown, TEMPLATE_RETRY_DELAY) {
                    return None;
                }
                let next_template = control_plane.resolve_next_template(tui)?;
                *template = next_template;
                continue;
            }
        };

        if header_base.len() != POW_HEADER_BASE_LEN {
            warn(
                "TEMPLATE",
                format!(
                    "header_base length mismatch: expected {} bytes, got {}",
                    POW_HEADER_BASE_LEN,
                    header_base.len()
                ),
            );
            if !sleep_with_shutdown(shutdown, TEMPLATE_RETRY_DELAY) {
                return None;
            }
            let next_template = control_plane.resolve_next_template(tui)?;
            *template = next_template;
            continue;
        }

        let target = match parse_target(&template.target) {
            Ok(target) => target,
            Err(err) => {
                warn("TEMPLATE", format!("target parse error: {err:#}"));
                if !sleep_with_shutdown(shutdown, TEMPLATE_RETRY_DELAY) {
                    return None;
                }
                let next_template = control_plane.resolve_next_template(tui)?;
                *template = next_template;
                continue;
            }
        };
        let header_base: Arc<[u8]> = Arc::from(header_base);

        let template_height_u64 = template_height(&template.block);
        if let Some(height) = template_height_u64 {
            if let Some(signal) = tip_signal {
                signal.set_current_template_height(height);
            }
        }
        let height = template_height_u64
            .map(|h| h.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let difficulty = template_difficulty(&template.block)
            .map(|d| d.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        return Some(PreparedTemplate {
            header_base,
            target,
            height,
            difficulty,
        });
    }
}

struct DispatchRoundInputs<'a> {
    cfg: &'a Config,
    epoch: u64,
    work_id: u64,
    header_base: &'a Arc<[u8]>,
    target: [u8; 32],
    reservation: super::scheduler::NonceReservation,
    stop_at: Instant,
    backend_weights: &'a BTreeMap<u64, f64>,
}

fn dispatch_round_assignments(
    inputs: DispatchRoundInputs<'_>,
    backends: &mut Vec<BackendSlot>,
    nonce_scheduler: &mut NonceScheduler,
    backend_executor: &super::backend_executor::BackendExecutor,
) -> Result<()> {
    let additional_span = distribute_work(
        backends,
        super::DistributeWorkOptions {
            epoch: inputs.epoch,
            work_id: inputs.work_id,
            header_base: Arc::clone(inputs.header_base),
            target: inputs.target,
            reservation: inputs.reservation,
            stop_at: inputs.stop_at,
            backend_weights: work_distribution_weights(
                inputs.cfg.work_allocation,
                inputs.backend_weights,
            ),
        },
        backend_executor,
    )?;
    nonce_scheduler.consume_additional_span(additional_span);
    Ok(())
}

struct ExecuteRoundPhase<'a, 'cp> {
    cfg: &'a Config,
    shutdown: &'a Arc<AtomicBool>,
    tip_signal: Option<&'a TipSignal>,
    epoch: u64,
    work_id: u64,
    stop_at: Instant,
    round_start: Instant,
    height: &'a str,
    difficulty: &'a str,
    header_base: &'a Arc<[u8]>,
    target: [u8; 32],
    current_template: &'a BlockTemplateResponse,
    control_plane: &'a mut MiningControlPlane<'cp>,
    backends: &'a mut Vec<BackendSlot>,
    backend_events: &'a Receiver<BackendEvent>,
    backend_executor: &'a super::backend_executor::BackendExecutor,
    stats: &'a Stats,
    tui: &'a mut Option<TuiDisplay>,
    last_stats_print: &'a mut Instant,
    nonce_scheduler: &'a mut NonceScheduler,
    backend_weights: &'a mut BTreeMap<u64, f64>,
    recent_templates: &'a VecDeque<RecentTemplateEntry>,
    deferred_solutions: &'a mut VecDeque<MiningSolution>,
    deferred_solution_keys: &'a mut HashSet<(u64, u64)>,
    submitted_solution_order: &'a mut VecDeque<(u64, u64)>,
    submitted_solution_keys: &'a mut HashSet<(u64, u64)>,
    inflight_solution_keys: &'a mut HashSet<(u64, u64)>,
}

fn execute_round_phase(phase: ExecuteRoundPhase<'_, '_>) -> Result<()> {
    let ExecuteRoundPhase {
        cfg,
        shutdown,
        tip_signal,
        epoch,
        work_id,
        stop_at,
        round_start,
        height,
        difficulty,
        header_base,
        target,
        current_template,
        control_plane,
        backends,
        backend_events,
        backend_executor,
        stats,
        tui,
        last_stats_print,
        nonce_scheduler,
        backend_weights,
        recent_templates,
        deferred_solutions,
        deferred_solution_keys,
        submitted_solution_order,
        submitted_solution_keys,
        inflight_solution_keys,
    } = phase;

    let mut round_runtime = RoundRuntime {
        cfg,
        shutdown: shutdown.as_ref(),
        backends,
        backend_events,
        tip_signal,
        backend_executor,
        stats,
        tui,
        last_stats_print,
        nonce_scheduler,
        backend_weights,
        deferred_solutions,
        deferred_solution_keys,
    };
    let mut round_state = round_runtime.run(RoundInput {
        epoch,
        work_id,
        stop_at,
        round_start,
        height,
        difficulty,
        header_base,
        target,
    })?;

    let mut submitted_solution = None;
    let mut current_submit_template: Option<SubmitTemplate> = None;
    let mut pending_solution = round_state.solved.take();
    {
        let mut deferred_state = DeferredQueueState {
            deferred_solutions,
            deferred_solution_keys,
            stats,
        };
        let _ = drain_mining_backend_events(
            backend_events,
            epoch,
            &mut pending_solution,
            &mut deferred_state,
            backends,
            backend_executor,
        )?;
    }
    let solved_found = pending_solution.is_some();
    let append_semantics_active = super::backends_have_append_assignment_semantics(backends);

    if cfg.strict_round_accounting {
        let _ = quiesce_backend_slots(backends, RuntimeMode::Mining, backend_executor)?;
    } else if should_cancel_relaxed_round(
        round_state.stale_tip_event,
        solved_found,
        append_semantics_active,
    ) {
        let _ = cancel_backend_slots(backends, RuntimeMode::Mining, backend_executor)?;
    }
    {
        let mut deferred_state = DeferredQueueState {
            deferred_solutions,
            deferred_solution_keys,
            stats,
        };
        let _ = drain_mining_backend_events(
            backend_events,
            epoch,
            &mut pending_solution,
            &mut deferred_state,
            backends,
            backend_executor,
        )?;
    }
    let mut enqueued_solution = None;
    if let Some(solution) = pending_solution.take() {
        let key = (solution.epoch, solution.nonce);
        if already_submitted_solution(submitted_solution_keys, &solution)
            || inflight_solution_keys.contains(&key)
        {
            warn(
                "SUBMIT",
                format!(
                    "skipping duplicate solution epoch={} nonce={}",
                    solution.epoch, solution.nonce
                ),
            );
        } else {
            let submit_template = current_submit_template
                .get_or_insert_with(|| SubmitTemplate::from_template(current_template))
                .clone();
            if control_plane.submit_template(submit_template, solution.clone(), stats, tui) {
                inflight_solution_keys.insert(key);
                enqueued_solution = Some(solution.clone());
            } else {
                warn(
                    "SUBMIT",
                    format!(
                        "submit queue saturated; deferring solution epoch={} nonce={}",
                        solution.epoch, solution.nonce
                    ),
                );
                defer_solution_indexed(
                    deferred_solutions,
                    deferred_solution_keys,
                    solution.clone(),
                    stats,
                );
            }
        }
        submitted_solution = Some(solution);
    }
    drop_solution_from_deferred_indexed(
        deferred_solutions,
        deferred_solution_keys,
        enqueued_solution.as_ref(),
    );
    if !deferred_solutions.is_empty() {
        let submit_template = current_submit_template
            .get_or_insert_with(|| SubmitTemplate::from_template(current_template));
        submit_deferred_solutions(
            control_plane,
            epoch,
            submit_template,
            DeferredSubmitState {
                recent_templates,
                deferred_solutions,
                deferred_solution_keys,
                submitted_solution_keys,
                inflight_solution_keys,
                stats,
                tui,
            },
        );
    }
    process_submit_results(
        control_plane.drain_submit_results(stats, tui),
        deferred_solutions,
        deferred_solution_keys,
        submitted_solution_order,
        submitted_solution_keys,
        inflight_solution_keys,
        stats,
    );
    collect_backend_hashes(
        backends,
        backend_executor,
        Some(stats),
        &mut round_state.round_hashes,
        Some(&mut round_state.round_backend_hashes),
        Some(&mut round_state.round_backend_telemetry),
    );
    round_state.stale_tip_event |= tip_signal.is_some_and(TipSignal::take_stale);
    let round_end_reason = if shutdown.load(Ordering::Relaxed) {
        RoundEndReason::Shutdown
    } else if submitted_solution.is_some() {
        RoundEndReason::Solved
    } else if round_state.stale_tip_event {
        RoundEndReason::StaleTip
    } else {
        RoundEndReason::Refresh
    };
    let round_elapsed_secs = round_start.elapsed().as_secs_f64();
    update_backend_weights(
        backend_weights,
        WeightUpdateInputs {
            backends,
            round_backend_hashes: &round_state.round_backend_hashes,
            round_backend_telemetry: Some(&round_state.round_backend_telemetry),
            round_elapsed_secs,
            mode: cfg.work_allocation,
            round_end_reason,
            refresh_interval: cfg.refresh_interval,
        },
    );
    let telemetry_line =
        format_round_backend_telemetry(backends, &round_state.round_backend_telemetry);
    if telemetry_line != "none" {
        info("BACKEND", format!("telemetry | {telemetry_line}"));
    }

    if let Some(solution) = submitted_solution {
        update_tui(
            tui,
            stats,
            RoundUiView {
                backends,
                round_backend_hashes: &round_state.round_backend_hashes,

                round_start,
                height,
                difficulty,
                epoch,
                state_label: "solved",
            },
        );
        mined(
            "SOLVE",
            format!(
                "solution found! elapsed={:.2}s backend={}#{}",
                round_start.elapsed().as_secs_f64(),
                solution.backend,
                solution.backend_id,
            ),
        );
    } else if round_state.stale_tip_event {
        update_tui(
            tui,
            stats,
            RoundUiView {
                backends,
                round_backend_hashes: &round_state.round_backend_hashes,

                round_start,
                height,
                difficulty,
                epoch,
                state_label: "stale-refresh",
            },
        );
    } else {
        update_tui(
            tui,
            stats,
            RoundUiView {
                backends,
                round_backend_hashes: &round_state.round_backend_hashes,

                round_start,
                height,
                difficulty,
                epoch,
                state_label: "refresh",
            },
        );
    }

    maybe_print_stats(stats, last_stats_print, cfg.stats_interval, tui.is_none());

    Ok(())
}

pub(super) fn run_mining_loop(
    cfg: &Config,
    client: &ApiClient,
    shutdown: Arc<AtomicBool>,
    runtime_backends: MiningRuntimeBackends<'_>,
    tui_state: Option<TuiState>,
    tip_signal: Option<&TipSignal>,
) -> Result<()> {
    let MiningRuntimeBackends {
        backends,
        backend_events,
        backend_executor,
    } = runtime_backends;
    let stats = Stats::new();
    let mut nonce_scheduler = NonceScheduler::new(cfg.start_nonce, cfg.nonce_iters_per_lane);
    let mut work_id_cursor = 1u64;
    let mut epoch = 0u64;
    let mut last_stats_print = Instant::now();
    let mut tui = init_tui_display(tui_state, Arc::clone(&shutdown));
    let mut backend_weights = seed_backend_weights(backends);
    let mut control_plane = MiningControlPlane::new(client, cfg, Arc::clone(&shutdown), tip_signal);
    let mut dev_fee_tracker = DevFeeTracker::new();
    info(
        "MINER",
        format!(
            "dev fee: {:.1}%",
            crate::dev_fee::DEV_FEE_PERCENT
        ),
    );
    let recent_template_retention = recent_template_retention_for_backends(cfg, backends);
    let recent_template_cache_size = recent_template_cache_size_for_backends(cfg, backends);
    let recent_template_cache_max_bytes = recent_template_cache_max_bytes();
    let mut recent_templates = VecDeque::<RecentTemplateEntry>::new();
    let mut recent_templates_bytes = 0usize;
    let mut deferred_solutions = VecDeque::<MiningSolution>::new();
    let mut deferred_solution_keys = HashSet::<(u64, u64)>::new();
    let mut submitted_solution_keys = HashSet::<(u64, u64)>::new();
    let mut submitted_solution_order = VecDeque::<(u64, u64)>::new();
    let mut inflight_solution_keys = HashSet::<(u64, u64)>::new();

    let mut template = match control_plane.fetch_initial_template(&mut tui) {
        Some(t) => t,
        None => {
            stats.print();
            info("MINER", "stopped");
            return Ok(());
        }
    };
    success("MINER", "connected and mining");
    info(
        "MINER",
        format!(
            "template-history | max={} retention={}s cap={}MiB",
            recent_template_cache_size,
            recent_template_retention.as_secs_f64(),
            recent_template_cache_max_bytes / (1024 * 1024)
        ),
    );

    while !shutdown.load(Ordering::Relaxed) {
        if backends.is_empty() {
            bail!("all mining backends are unavailable");
        }

        let mode_changed = dev_fee_tracker.begin_round();
        control_plane.set_dev_fee_address(dev_fee_tracker.address());
        if mode_changed {
            if dev_fee_tracker.is_dev_round() {
                info("DEV FEE", "mining for dev");
            } else {
                info("DEV FEE", "mining for user");
            }
            // Discard any prefetched template (fetched with wrong address) and get fresh one
            let Some(fresh) = control_plane.resolve_next_template(&mut tui) else {
                break;
            };
            template = fresh;
        }

        process_submit_results(
            control_plane.drain_submit_results(&stats, &mut tui),
            &mut deferred_solutions,
            &mut deferred_solution_keys,
            &mut submitted_solution_order,
            &mut submitted_solution_keys,
            &mut inflight_solution_keys,
            &stats,
        );

        let Some(prepared_template) = prepare_round_template(
            &mut template,
            &mut control_plane,
            shutdown.as_ref(),
            tip_signal,
            &mut tui,
        ) else {
            break;
        };
        let PreparedTemplate {
            header_base,
            target,
            height,
            difficulty,
        } = prepared_template;

        epoch = epoch.wrapping_add(1).max(1);
        let work_id = next_work_id(&mut work_id_cursor);
        let reservation = nonce_scheduler.reserve(total_lanes(backends));
        let round_start = Instant::now();
        let stop_at = round_start + cfg.refresh_interval;

        stats.bump_templates();

        dispatch_round_assignments(
            DispatchRoundInputs {
                cfg,
                epoch,
                work_id,
                header_base: &header_base,
                target,
                reservation,
                stop_at,
                backend_weights: &backend_weights,
            },
            backends,
            &mut nonce_scheduler,
            backend_executor,
        )?;
        control_plane.spawn_prefetch_if_needed();

        execute_round_phase(ExecuteRoundPhase {
            cfg,
            shutdown: &shutdown,
            tip_signal,
            epoch,
            work_id,
            stop_at,
            round_start,
            height: &height,
            difficulty: &difficulty,
            header_base: &header_base,
            target,
            current_template: &template,
            control_plane: &mut control_plane,
            backends,
            backend_events,
            backend_executor,
            stats: &stats,
            tui: &mut tui,
            last_stats_print: &mut last_stats_print,
            nonce_scheduler: &mut nonce_scheduler,
            backend_weights: &mut backend_weights,
            recent_templates: &recent_templates,
            deferred_solutions: &mut deferred_solutions,
            deferred_solution_keys: &mut deferred_solution_keys,
            submitted_solution_order: &mut submitted_solution_order,
            submitted_solution_keys: &mut submitted_solution_keys,
            inflight_solution_keys: &mut inflight_solution_keys,
        })?;

        dev_fee_tracker.end_round(round_start.elapsed());

        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let Some(next_template) = control_plane.resolve_next_template(&mut tui) else {
            break;
        };
        remember_recent_template(
            &mut recent_templates,
            &mut recent_templates_bytes,
            epoch,
            SubmitTemplate::from_template(&template),
            recent_template_retention,
            recent_template_cache_size,
            recent_template_cache_max_bytes,
        );
        template = next_template;
    }

    if !backends.is_empty() {
        let final_submit_template = SubmitTemplate::from_template(&template);
        info("MINER", "shutting down: quiescing backends...");
        match quiesce_backend_slots(backends, RuntimeMode::Mining, backend_executor) {
            Ok(_) => {}
            Err(err) => warn("BACKEND", format!("final backend quiesce failed: {err:#}")),
        }
        info("MINER", "shutting down: draining final events...");
        let mut final_pending_solution = None;
        {
            let mut deferred_state = DeferredQueueState {
                deferred_solutions: &mut deferred_solutions,
                deferred_solution_keys: &mut deferred_solution_keys,
                stats: &stats,
            };
            match drain_mining_backend_events(
                backend_events,
                epoch,
                &mut final_pending_solution,
                &mut deferred_state,
                backends,
                backend_executor,
            ) {
                Ok(_) => {}
                Err(err) => warn(
                    "BACKEND",
                    format!("final backend event drain failed: {err:#}"),
                ),
            }
        }
        let mut final_enqueued_solution = None;
        if let Some(solution) = final_pending_solution.as_ref() {
            let key = (solution.epoch, solution.nonce);
            if already_submitted_solution(&submitted_solution_keys, solution)
                || inflight_solution_keys.contains(&key)
            {
                warn(
                    "SUBMIT",
                    format!(
                        "skipping duplicate solution epoch={} nonce={}",
                        solution.epoch, solution.nonce
                    ),
                );
            } else if control_plane.submit_template(
                final_submit_template.clone(),
                solution.clone(),
                &stats,
                &mut tui,
            ) {
                inflight_solution_keys.insert(key);
                final_enqueued_solution = final_pending_solution.clone();
            } else {
                warn(
                    "SUBMIT",
                    format!(
                        "submit queue saturated; deferring solution epoch={} nonce={}",
                        solution.epoch, solution.nonce
                    ),
                );
                defer_solution_indexed(
                    &mut deferred_solutions,
                    &mut deferred_solution_keys,
                    solution.clone(),
                    &stats,
                );
            }
        }
        drop_solution_from_deferred_indexed(
            &mut deferred_solutions,
            &mut deferred_solution_keys,
            final_enqueued_solution.as_ref(),
        );
        submit_deferred_solutions(
            &mut control_plane,
            epoch,
            &final_submit_template,
            DeferredSubmitState {
                recent_templates: &recent_templates,
                deferred_solutions: &mut deferred_solutions,
                deferred_solution_keys: &mut deferred_solution_keys,
                inflight_solution_keys: &mut inflight_solution_keys,
                submitted_solution_keys: &mut submitted_solution_keys,
                stats: &stats,
                tui: &mut tui,
            },
        );
    }

    process_submit_results(
        control_plane.drain_submit_results(&stats, &mut tui),
        &mut deferred_solutions,
        &mut deferred_solution_keys,
        &mut submitted_solution_order,
        &mut submitted_solution_keys,
        &mut inflight_solution_keys,
        &stats,
    );
    info("MINER", "shutting down: flushing pending submits...");
    process_submit_results(
        control_plane.finish(&stats, &mut tui),
        &mut deferred_solutions,
        &mut deferred_solution_keys,
        &mut submitted_solution_order,
        &mut submitted_solution_keys,
        &mut inflight_solution_keys,
        &stats,
    );

    stats.print();
    info("MINER", "stopped");
    Ok(())
}

fn should_cancel_relaxed_round(
    stale_tip_event: bool,
    solved_found: bool,
    append_semantics_active: bool,
) -> bool {
    stale_tip_event || solved_found || append_semantics_active
}

fn should_trigger_sub_round_rebalance(
    cfg: &Config,
    backends: &[BackendSlot],
    round_hashes: u64,
    last_sub_round_rebalance_hashes: u64,
    next_sub_round_rebalance_at: Option<Instant>,
    now: Instant,
    stop_at: Instant,
) -> bool {
    if cfg.work_allocation != WorkAllocation::Adaptive {
        return false;
    }
    if cfg.sub_round_rebalance_interval.is_none() {
        return false;
    }
    if backends.len() <= 1 {
        return false;
    }
    if round_hashes <= last_sub_round_rebalance_hashes {
        return false;
    }
    if now >= stop_at {
        return false;
    }

    next_sub_round_rebalance_at.is_some_and(|deadline| now >= deadline)
}

fn backend_hash_deltas_since(
    backends: &[BackendSlot],
    round_backend_hashes: &BTreeMap<u64, u64>,
    baseline_hashes: &BTreeMap<u64, u64>,
) -> BTreeMap<u64, u64> {
    let mut deltas = BTreeMap::new();
    for slot in backends {
        let current = round_backend_hashes.get(&slot.id).copied().unwrap_or(0);
        let baseline = baseline_hashes.get(&slot.id).copied().unwrap_or(0);
        deltas.insert(slot.id, current.saturating_sub(baseline));
    }
    deltas
}

struct RoundInput<'a> {
    epoch: u64,
    work_id: u64,
    stop_at: Instant,
    round_start: Instant,
    height: &'a str,
    difficulty: &'a str,
    header_base: &'a Arc<[u8]>,
    target: [u8; 32],
}

struct RoundRuntime<'a> {
    cfg: &'a Config,
    shutdown: &'a AtomicBool,
    backends: &'a mut Vec<BackendSlot>,
    backend_events: &'a Receiver<BackendEvent>,
    tip_signal: Option<&'a TipSignal>,
    backend_executor: &'a super::backend_executor::BackendExecutor,
    stats: &'a Stats,
    tui: &'a mut Option<TuiDisplay>,
    last_stats_print: &'a mut Instant,
    nonce_scheduler: &'a mut NonceScheduler,
    backend_weights: &'a mut BTreeMap<u64, f64>,
    deferred_solutions: &'a mut VecDeque<MiningSolution>,
    deferred_solution_keys: &'a mut HashSet<(u64, u64)>,
}

struct RoundProgressState {
    solved: Option<MiningSolution>,
    stale_tip_event: bool,
    round_hashes: u64,
    round_backend_hashes: BTreeMap<u64, u64>,
    round_backend_telemetry: BTreeMap<u64, BackendRoundTelemetry>,
}

impl RoundProgressState {
    fn new() -> Self {
        Self {
            solved: None,
            stale_tip_event: false,
            round_hashes: 0,
            round_backend_hashes: BTreeMap::new(),
            round_backend_telemetry: BTreeMap::new(),
        }
    }

    fn into_round_loop_state(self) -> RoundLoopState {
        RoundLoopState {
            solved: self.solved,
            stale_tip_event: self.stale_tip_event,
            round_hashes: self.round_hashes,
            round_backend_hashes: self.round_backend_hashes,
            round_backend_telemetry: self.round_backend_telemetry,
        }
    }
}

struct RoundRebalanceState {
    topology_changed: bool,
    backend_poll_state: super::hash_poll::BackendPollState,
    rebalance_interval: Option<Duration>,
    next_sub_round_rebalance_at: Option<Instant>,
    last_sub_round_rebalance_hashes: u64,
    last_sub_round_rebalance_at: Instant,
    last_sub_round_backend_hashes: BTreeMap<u64, u64>,
}

impl RoundRebalanceState {
    fn new(backends: &[BackendSlot], hash_poll_interval: Duration, round_start: Instant) -> Self {
        Self {
            topology_changed: false,
            backend_poll_state: build_backend_poll_state(backends, hash_poll_interval),
            rebalance_interval: None,
            next_sub_round_rebalance_at: None,
            last_sub_round_rebalance_hashes: 0,
            last_sub_round_rebalance_at: round_start,
            last_sub_round_backend_hashes: BTreeMap::new(),
        }
    }

    fn configure_rebalance_interval(&mut self, interval: Option<Duration>, round_start: Instant) {
        self.rebalance_interval = interval;
        self.next_sub_round_rebalance_at = self
            .rebalance_interval
            .and_then(|rebalance| round_start.checked_add(rebalance));
    }

    fn reset_poll_state(&mut self, backends: &[BackendSlot], hash_poll_interval: Duration) {
        self.backend_poll_state = build_backend_poll_state(backends, hash_poll_interval);
    }

    fn note_rebalanced(&mut self, at: Instant, progress: &RoundProgressState) {
        self.topology_changed = false;
        self.last_sub_round_rebalance_hashes = progress.round_hashes;
        self.last_sub_round_rebalance_at = at;
        self.last_sub_round_backend_hashes = progress.round_backend_hashes.clone();
        self.next_sub_round_rebalance_at = self
            .rebalance_interval
            .and_then(|interval| at.checked_add(interval));
    }
}

impl<'a> RoundRuntime<'a> {
    fn run(&mut self, input: RoundInput<'_>) -> Result<RoundLoopState> {
        let mut progress = RoundProgressState::new();
        let mut rebalance = RoundRebalanceState::new(
            self.backends,
            self.cfg.hash_poll_interval,
            input.round_start,
        );
        rebalance.configure_rebalance_interval(
            self.cfg
                .sub_round_rebalance_interval
                .map(|interval| interval.max(Duration::from_millis(1))),
            input.round_start,
        );

        self.update_round_tui(&input, &progress, "working");

        while self.should_continue_round_loop(&input, &progress) {
            if self.mark_round_stale_if_needed(&input, &mut progress) {
                continue;
            }

            let step = self.drive_round_step(&input, &mut progress, &mut rebalance)?;
            self.apply_collected_hashes(&input, &mut progress, step.collected_hashes);

            if self.mark_round_stale_if_needed(&input, &mut progress) {
                continue;
            }

            maybe_print_stats(
                self.stats,
                self.last_stats_print,
                self.cfg.stats_interval,
                self.tui.is_none(),
            );

            self.handle_round_event(&input, &mut progress, &mut rebalance, step.event)?;
            self.maybe_rebalance_for_topology_change(&input, &mut progress, &mut rebalance)?;
            self.maybe_rebalance_in_round(&input, &mut progress, &mut rebalance)?;
        }

        Ok(progress.into_round_loop_state())
    }

    fn should_continue_round_loop(
        &self,
        input: &RoundInput<'_>,
        progress: &RoundProgressState,
    ) -> bool {
        !self.shutdown.load(Ordering::Relaxed)
            && !self.backends.is_empty()
            && Instant::now() < input.stop_at
            && progress.solved.is_none()
            && !progress.stale_tip_event
    }

    fn update_round_tui(
        &mut self,
        input: &RoundInput<'_>,
        progress: &RoundProgressState,
        state_label: &'static str,
    ) {
        update_tui(
            self.tui,
            self.stats,
            RoundUiView {
                backends: self.backends,
                round_backend_hashes: &progress.round_backend_hashes,
                round_start: input.round_start,
                height: input.height,
                difficulty: input.difficulty,
                epoch: input.epoch,
                state_label,
            },
        );
    }

    fn mark_round_stale_if_needed(
        &mut self,
        input: &RoundInput<'_>,
        progress: &mut RoundProgressState,
    ) -> bool {
        if !self.tip_signal.is_some_and(TipSignal::take_stale) {
            return false;
        }
        progress.stale_tip_event = true;
        self.update_round_tui(input, progress, "stale-tip");
        true
    }

    fn drive_round_step(
        &mut self,
        input: &RoundInput<'_>,
        progress: &mut RoundProgressState,
        rebalance: &mut RoundRebalanceState,
    ) -> Result<super::round_driver::RoundDriverStep> {
        let stats_deadline = if self.tui.is_none() {
            Some(*self.last_stats_print + self.cfg.stats_interval)
        } else {
            None
        };
        super::round_driver::drive_round_step(super::round_driver::RoundDriverInput {
            backends: self.backends,
            backend_events: self.backend_events,
            backend_executor: self.backend_executor,
            configured_hash_poll_interval: self.cfg.hash_poll_interval,
            poll_state: &mut rebalance.backend_poll_state,
            round_backend_hashes: &mut progress.round_backend_hashes,
            round_backend_telemetry: &mut progress.round_backend_telemetry,
            stop_at: input.stop_at,
            extra_deadline: stats_deadline,
        })
    }

    fn apply_collected_hashes(
        &mut self,
        input: &RoundInput<'_>,
        progress: &mut RoundProgressState,
        collected: u64,
    ) {
        if collected == 0 {
            return;
        }
        self.stats.add_hashes(collected);
        progress.round_hashes = progress.round_hashes.saturating_add(collected);
        self.update_round_tui(input, progress, "working");
    }

    fn handle_round_event(
        &mut self,
        input: &RoundInput<'_>,
        progress: &mut RoundProgressState,
        rebalance: &mut RoundRebalanceState,
        event: Option<BackendEvent>,
    ) -> Result<()> {
        let Some(event) = event else {
            return Ok(());
        };
        let mut deferred_state = DeferredQueueState {
            deferred_solutions: self.deferred_solutions,
            deferred_solution_keys: self.deferred_solution_keys,
            stats: self.stats,
        };
        if handle_mining_backend_event(
            event,
            input.epoch,
            &mut progress.solved,
            &mut deferred_state,
            self.backends,
            self.backend_executor,
        )? == BackendEventAction::TopologyChanged
        {
            rebalance.topology_changed = true;
        }
        Ok(())
    }

    fn maybe_rebalance_for_topology_change(
        &mut self,
        input: &RoundInput<'_>,
        progress: &mut RoundProgressState,
        rebalance: &mut RoundRebalanceState,
    ) -> Result<()> {
        if !rebalance.topology_changed
            || self.shutdown.load(Ordering::Relaxed)
            || progress.solved.is_some()
            || progress.stale_tip_event
            || Instant::now() >= input.stop_at
            || self.backends.is_empty()
        {
            return Ok(());
        }

        redistribute_for_topology_change(
            self.backends,
            TopologyRedistributionOptions {
                epoch: input.epoch,
                work_id: input.work_id,
                header_base: Arc::clone(input.header_base),
                target: input.target,
                stop_at: input.stop_at,
                mode: RuntimeMode::Mining,
                work_allocation: self.cfg.work_allocation,
                reason: "topology change",
                backend_weights: Some(self.backend_weights),
                nonce_scheduler: self.nonce_scheduler,
                backend_executor: self.backend_executor,
                log_tag: "BACKEND",
            },
        )?;
        if self.backends.is_empty() {
            return Ok(());
        }

        let rebalance_now = Instant::now();
        rebalance.reset_poll_state(self.backends, self.cfg.hash_poll_interval);
        rebalance.note_rebalanced(rebalance_now, progress);
        self.update_round_tui(input, progress, "rebalanced");
        Ok(())
    }

    fn maybe_rebalance_in_round(
        &mut self,
        input: &RoundInput<'_>,
        progress: &mut RoundProgressState,
        rebalance: &mut RoundRebalanceState,
    ) -> Result<()> {
        let now = Instant::now();
        if !should_trigger_sub_round_rebalance(
            self.cfg,
            self.backends,
            progress.round_hashes,
            rebalance.last_sub_round_rebalance_hashes,
            rebalance.next_sub_round_rebalance_at,
            now,
            input.stop_at,
        ) {
            return Ok(());
        }

        let rebalance_hash_deltas = backend_hash_deltas_since(
            self.backends,
            &progress.round_backend_hashes,
            &rebalance.last_sub_round_backend_hashes,
        );
        let rebalance_elapsed_secs = now
            .saturating_duration_since(rebalance.last_sub_round_rebalance_at)
            .as_secs_f64();
        update_backend_weights(
            self.backend_weights,
            WeightUpdateInputs {
                backends: self.backends,
                round_backend_hashes: &rebalance_hash_deltas,
                round_backend_telemetry: None,
                round_elapsed_secs: rebalance_elapsed_secs,
                mode: self.cfg.work_allocation,
                round_end_reason: RoundEndReason::Refresh,
                refresh_interval: self.cfg.refresh_interval,
            },
        );

        redistribute_for_topology_change(
            self.backends,
            TopologyRedistributionOptions {
                epoch: input.epoch,
                work_id: input.work_id,
                header_base: Arc::clone(input.header_base),
                target: input.target,
                stop_at: input.stop_at,
                mode: RuntimeMode::Mining,
                work_allocation: self.cfg.work_allocation,
                reason: "in-round performance rebalance",
                backend_weights: Some(self.backend_weights),
                nonce_scheduler: self.nonce_scheduler,
                backend_executor: self.backend_executor,
                log_tag: "BACKEND",
            },
        )?;
        if self.backends.is_empty() {
            return Ok(());
        }

        rebalance.reset_poll_state(self.backends, self.cfg.hash_poll_interval);
        rebalance.note_rebalanced(now, progress);
        self.update_round_tui(input, progress, "rebalanced");
        Ok(())
    }
}

fn resolve_next_template(
    prefetch: &mut Option<TemplatePrefetch>,
    client: &ApiClient,
    cfg: &Config,
    shutdown: &Arc<AtomicBool>,
    tip_signal: Option<&TipSignal>,
    tui: &mut Option<TuiDisplay>,
    address: Option<&str>,
) -> Option<BlockTemplateResponse> {
    if shutdown.load(Ordering::Relaxed) {
        if let Some(task) = prefetch.take() {
            task.detach();
        }
        return None;
    }

    if prefetch.is_none() {
        *prefetch = Some(TemplatePrefetch::spawn(
            client.clone(),
            cfg.clone(),
            Arc::clone(shutdown),
        ));
    }

    let mut network_retry = RetryTracker::default();
    let mut auth_retry = RetryTracker::default();

    while !shutdown.load(Ordering::Relaxed) {
        if prefetch.as_ref().is_some_and(TemplatePrefetch::is_closed) {
            *prefetch = Some(TemplatePrefetch::spawn(
                client.clone(),
                cfg.clone(),
                Arc::clone(shutdown),
            ));
        }

        let latest_tip_sequence = current_tip_sequence(tip_signal);
        let wait = cfg.prefetch_wait.max(Duration::from_millis(1));
        let (prefetch_result, prefetch_closed) = {
            let task = prefetch.as_mut()?;
            task.request_if_idle(latest_tip_sequence, address);
            let result = task.wait_for_result(wait);
            let closed = task.is_closed();
            (result, closed)
        };
        let outcome = if let Some((tip_sequence, outcome)) = prefetch_result {
            let latest_after_wait = current_tip_sequence(tip_signal);
            if tip_sequence < latest_after_wait {
                if let Some(task) = prefetch.as_mut() {
                    task.request_if_idle(latest_after_wait, address);
                }
                continue;
            }
            outcome
        } else {
            if prefetch_closed {
                *prefetch = Some(TemplatePrefetch::spawn(
                    client.clone(),
                    cfg.clone(),
                    Arc::clone(shutdown),
                ));
                warn("TEMPLATE", "prefetch worker disconnected; respawned");
                continue;
            }
            render_tui_now(tui);
            fetch_template_once(client, cfg, shutdown.as_ref(), address)
        };

        match outcome {
            PrefetchOutcome::Template(template) => {
                network_retry.note_recovered("NETWORK", "blocktemplate fetch recovered");
                auth_retry.note_recovered("AUTH", "auth refreshed from cookie");
                render_tui_now(tui);
                return Some(*template);
            }
            PrefetchOutcome::NoWalletLoaded => {
                set_tui_state_label(tui, "awaiting-wallet");
                warn(
                    "WALLET",
                    "blocktemplate requires loaded wallet; attempting automatic load",
                );
                render_tui_now(tui);
                match auto_load_wallet(client, cfg, shutdown, tui) {
                    Ok(true) => continue,
                    Ok(false) => {
                        warn(
                            "WALLET",
                            "unable to auto-load wallet; use --wallet-password, --wallet-password-file, SEINE_WALLET_PASSWORD, or interactive prompt",
                        );
                        render_tui_now(tui);
                        return None;
                    }
                    Err(load_err) => {
                        error(
                            "WALLET",
                            format!("automatic wallet load failed: {load_err:#}"),
                        );
                        render_tui_now(tui);
                        return None;
                    }
                }
            }
            PrefetchOutcome::Unauthorized => {
                if cfg.token_cookie_path.is_some() {
                    auth_retry.note_failure(
                        "AUTH",
                        "auth expired; waiting for new cookie token",
                        "auth still expired; waiting for new cookie token",
                        true,
                    );
                } else {
                    auth_retry.note_failure(
                        "AUTH",
                        "auth failed; static --token cannot auto-refresh",
                        "still waiting for manual token refresh",
                        true,
                    );
                }
                render_tui_now(tui);
                if !sleep_with_shutdown(shutdown.as_ref(), TEMPLATE_RETRY_DELAY) {
                    return None;
                }
            }
            PrefetchOutcome::Unavailable => {
                network_retry.note_failure(
                    "NETWORK",
                    "failed to fetch blocktemplate; retrying",
                    "still failing to fetch blocktemplate; retrying",
                    true,
                );
                render_tui_now(tui);
                if !sleep_with_shutdown(shutdown.as_ref(), TEMPLATE_RETRY_DELAY) {
                    return None;
                }
            }
        }
    }

    None
}
struct DeferredQueueState<'a> {
    deferred_solutions: &'a mut VecDeque<MiningSolution>,
    deferred_solution_keys: &'a mut HashSet<(u64, u64)>,
    stats: &'a Stats,
}

fn handle_mining_backend_event(
    event: BackendEvent,
    epoch: u64,
    solved: &mut Option<MiningSolution>,
    deferred_state: &mut DeferredQueueState<'_>,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
) -> Result<BackendEventAction> {
    let (action, maybe_solution) = super::handle_runtime_backend_event(
        event,
        epoch,
        backends,
        RuntimeMode::Mining,
        backend_executor,
    )?;
    if let Some(solution) = maybe_solution {
        route_mining_solution(solution, epoch, solved, deferred_state);
    }
    Ok(action)
}

fn drain_mining_backend_events(
    backend_events: &Receiver<BackendEvent>,
    epoch: u64,
    solved: &mut Option<MiningSolution>,
    deferred_state: &mut DeferredQueueState<'_>,
    backends: &mut Vec<BackendSlot>,
    backend_executor: &super::backend_executor::BackendExecutor,
) -> Result<BackendEventAction> {
    let (action, solutions) = super::drain_runtime_backend_events(
        backend_events,
        epoch,
        backends,
        RuntimeMode::Mining,
        backend_executor,
    )?;
    for solution in solutions {
        route_mining_solution(solution, epoch, solved, deferred_state);
    }
    Ok(action)
}

fn route_mining_solution(
    solution: MiningSolution,
    epoch: u64,
    solved: &mut Option<MiningSolution>,
    deferred_state: &mut DeferredQueueState<'_>,
) {
    if solution.epoch == epoch {
        if solved.is_none() {
            *solved = Some(solution);
        } else {
            defer_solution_indexed(
                deferred_state.deferred_solutions,
                deferred_state.deferred_solution_keys,
                solution,
                deferred_state.stats,
            );
        }
    } else if solution.epoch < epoch {
        defer_solution_indexed(
            deferred_state.deferred_solutions,
            deferred_state.deferred_solution_keys,
            solution,
            deferred_state.stats,
        );
    } else {
        deferred_state.stats.add_dropped(1);
        warn(
            "BACKEND",
            format!(
                "ignoring future solution from {}#{} epoch={} current_epoch={}",
                solution.backend, solution.backend_id, solution.epoch, epoch
            ),
        );
    }
}

fn process_submit_results(
    results: Vec<SubmitResult>,
    deferred_solutions: &mut VecDeque<MiningSolution>,
    deferred_solution_keys: &mut HashSet<(u64, u64)>,
    submitted_solution_order: &mut VecDeque<(u64, u64)>,
    submitted_solution_keys: &mut HashSet<(u64, u64)>,
    inflight_solution_keys: &mut HashSet<(u64, u64)>,
    stats: &Stats,
) {
    let mut accepted_epochs = HashSet::new();
    for result in results {
        let key = (result.solution.epoch, result.solution.nonce);
        inflight_solution_keys.remove(&key);
        match result.outcome {
            SubmitOutcome::Response(resp) => {
                if resp.accepted {
                    accepted_epochs.insert(result.solution.epoch);
                }
                remember_submitted_solution(
                    submitted_solution_order,
                    submitted_solution_keys,
                    &result.solution,
                );
                drop_solution_from_deferred_indexed(
                    deferred_solutions,
                    deferred_solution_keys,
                    Some(&result.solution),
                );
            }
            SubmitOutcome::StaleHeightError { .. } | SubmitOutcome::TerminalError(_) => {
                remember_submitted_solution(
                    submitted_solution_order,
                    submitted_solution_keys,
                    &result.solution,
                );
                drop_solution_from_deferred_indexed(
                    deferred_solutions,
                    deferred_solution_keys,
                    Some(&result.solution),
                );
            }
            SubmitOutcome::RetryableError(_) => {
                if !already_submitted_solution(submitted_solution_keys, &result.solution)
                    && !inflight_solution_keys.contains(&key)
                {
                    defer_solution_indexed(
                        deferred_solutions,
                        deferred_solution_keys,
                        result.solution,
                        stats,
                    );
                }
            }
        }
    }

    for epoch in accepted_epochs {
        let dropped = drop_deferred_solutions_for_epoch_indexed(
            deferred_solutions,
            deferred_solution_keys,
            epoch,
        );
        if dropped > 0 {
            stats.add_dropped(dropped);
            info(
                "SUBMIT",
                format!(
                    "dropped {} queued same-epoch solution(s) after accepted block epoch={}",
                    dropped, epoch
                ),
            );
        }
    }
}

fn defer_solution_indexed(
    deferred_solutions: &mut VecDeque<MiningSolution>,
    deferred_solution_keys: &mut HashSet<(u64, u64)>,
    solution: MiningSolution,
    stats: &Stats,
) {
    let outcome =
        push_deferred_solution_indexed(deferred_solutions, deferred_solution_keys, solution);
    if outcome.inserted {
        stats.add_deferred(1);
    }
    stats.add_dropped(outcome.dropped);
}

fn drop_deferred_solutions_for_epoch_indexed(
    deferred_solutions: &mut VecDeque<MiningSolution>,
    deferred_solution_keys: &mut HashSet<(u64, u64)>,
    epoch: u64,
) -> u64 {
    if deferred_solutions.is_empty() || deferred_solution_keys.is_empty() {
        return 0;
    }

    let mut dropped = 0u64;
    deferred_solutions.retain(|solution| {
        if solution.epoch != epoch {
            return true;
        }
        let key = (solution.epoch, solution.nonce);
        if deferred_solution_keys.remove(&key) {
            dropped = dropped.saturating_add(1);
        }
        false
    });
    if deferred_solutions.is_empty() {
        deferred_solution_keys.clear();
    }
    dropped
}

struct DeferredSubmitState<'a> {
    recent_templates: &'a VecDeque<RecentTemplateEntry>,
    deferred_solutions: &'a mut VecDeque<MiningSolution>,
    deferred_solution_keys: &'a mut HashSet<(u64, u64)>,
    submitted_solution_keys: &'a mut HashSet<(u64, u64)>,
    inflight_solution_keys: &'a mut HashSet<(u64, u64)>,
    stats: &'a Stats,
    tui: &'a mut Option<TuiDisplay>,
}

fn submit_deferred_solutions(
    control_plane: &mut MiningControlPlane<'_>,
    current_epoch: u64,
    current_submit_template: &SubmitTemplate,
    state: DeferredSubmitState<'_>,
) {
    if state.deferred_solutions.is_empty() {
        return;
    }

    let queued =
        take_deferred_solutions_indexed(state.deferred_solutions, state.deferred_solution_keys);
    for solution in queued {
        let key = (solution.epoch, solution.nonce);
        if already_submitted_solution(state.submitted_solution_keys, &solution)
            || state.inflight_solution_keys.contains(&key)
        {
            continue;
        }
        let Some(submit_template) = submit_template_for_solution_epoch(
            current_epoch,
            current_submit_template,
            state.recent_templates,
            solution.epoch,
        ) else {
            warn(
                "BACKEND",
                format!(
                    "dropping stale solution from {}#{} epoch={} (current_epoch={})",
                    solution.backend, solution.backend_id, solution.epoch, current_epoch
                ),
            );
            state.stats.add_dropped(1);
            continue;
        };
        if control_plane.submit_template(submit_template, solution.clone(), state.stats, state.tui)
        {
            state.inflight_solution_keys.insert(key);
        } else {
            defer_solution_indexed(
                state.deferred_solutions,
                state.deferred_solution_keys,
                solution,
                state.stats,
            );
        }
    }
}

fn effective_backend_timeouts_for_template_cache(
    cfg: &Config,
    backends: &[BackendSlot],
) -> (Duration, Duration) {
    let mut control_timeout = cfg.backend_control_timeout;
    let mut assign_timeout = cfg.backend_assign_timeout;
    for slot in backends {
        control_timeout = control_timeout.max(slot.runtime_policy.control_timeout);
        assign_timeout = assign_timeout.max(slot.runtime_policy.assignment_timeout);
    }
    (control_timeout, assign_timeout)
}

fn recent_template_cache_size_for_backends(cfg: &Config, backends: &[BackendSlot]) -> usize {
    let (backend_control_timeout, backend_assign_timeout) =
        effective_backend_timeouts_for_template_cache(cfg, backends);
    recent_template_cache_size_from_timeouts(
        cfg.refresh_interval,
        backend_control_timeout,
        backend_assign_timeout,
        cfg.prefetch_wait,
    )
}

fn recent_template_retention_for_backends(cfg: &Config, backends: &[BackendSlot]) -> Duration {
    let (backend_control_timeout, backend_assign_timeout) =
        effective_backend_timeouts_for_template_cache(cfg, backends);
    recent_template_retention_from_timeouts(
        cfg.refresh_interval,
        backend_control_timeout,
        backend_assign_timeout,
        cfg.prefetch_wait,
    )
}

fn recent_template_cache_max_bytes() -> usize {
    RECENT_TEMPLATE_CACHE_MAX_BYTES
}

#[cfg(test)]
fn compact_hash(hash: &str) -> String {
    let value = hash.trim();
    let len = value.chars().count();
    if len <= 8 {
        return value.to_string();
    }

    let prefix: String = value.chars().take(4).collect();
    let suffix: String = value
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{prefix}...{suffix}")
}

fn sleep_with_shutdown(shutdown: &AtomicBool, duration: Duration) -> bool {
    let deadline = Instant::now() + duration;
    while !shutdown.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        let sleep_for = deadline
            .saturating_duration_since(now)
            .min(Duration::from_millis(100));
        thread::sleep(sleep_for);
    }
    false
}

#[cfg(test)]
mod tests {
    use crossbeam_channel::Sender;
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::backend::{BackendInstanceId, PowBackend, WorkAssignment};
    use crate::config::WorkAllocation;
    use anyhow::Result;

    struct NoopBackend {
        name: &'static str,
    }

    impl NoopBackend {
        fn new(name: &'static str) -> Self {
            Self { name }
        }
    }

    impl PowBackend for NoopBackend {
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
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            Ok(())
        }
    }

    fn submit_test_client(server: &MockServer) -> ApiClient {
        ApiClient::new(
            server.url("").trim_end_matches('/').to_string(),
            "test-token".to_string(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .expect("submit test client should be created")
    }

    #[test]
    fn submit_unauthorized_without_refresh_source_fails_without_retry() {
        let server = MockServer::start();
        let submit_mock = server.mock(|when, then| {
            when.method(POST).path("/api/mining/submitblock");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({"error": "unauthorized"}));
        });

        let client = submit_test_client(&server);
        let request = SubmitRequest {
            template: SubmitTemplate::Compact {
                template_id: "tmpl-unauth".to_string(),
            },
            solution: MiningSolution {
                epoch: 1,
                nonce: 99,
                backend_id: 1,
                backend: "cpu",
            },
            is_dev_fee: false,
        };
        let shutdown = AtomicBool::new(false);

        let result = process_submit_request(&client, request, &shutdown, None);

        assert_eq!(result.attempts, 1);
        match result.outcome {
            SubmitOutcome::TerminalError(message) => {
                assert!(message.contains("no cookie refresh source is available"));
            }
            SubmitOutcome::StaleHeightError {
                message,
                expected_height: _,
                got_height: _,
            } => {
                panic!("expected terminal submit failure, got stale-height error: {message}");
            }
            SubmitOutcome::RetryableError(message) => {
                panic!("expected terminal submit failure, got retryable error: {message}");
            }
            SubmitOutcome::Response(_) => panic!("unauthorized submit should fail"),
        }
        submit_mock.assert_hits(1);
    }

    #[test]
    fn stale_solution_from_unavailable_backend_is_deferred() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let stats = Stats::new();
        let mut solved = None;
        let mut deferred = VecDeque::new();
        let mut deferred_keys = HashSet::new();
        let mut backends = Vec::new();

        let action = {
            let mut deferred_state = DeferredQueueState {
                deferred_solutions: &mut deferred,
                deferred_solution_keys: &mut deferred_keys,
                stats: &stats,
            };
            handle_mining_backend_event(
                BackendEvent::Solution(MiningSolution {
                    epoch: 41,
                    nonce: 9,
                    backend_id: 1,
                    backend: "cpu",
                }),
                42,
                &mut solved,
                &mut deferred_state,
                &mut backends,
                &backend_executor,
            )
        }
        .expect("stale solution handling should succeed");

        assert_eq!(action, BackendEventAction::None);
        assert!(solved.is_none());
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].epoch, 41);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.deferred, 1);
        assert_eq!(snapshot.dropped, 0);
    }

    #[test]
    fn stale_solution_from_active_backend_is_deferred() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let stats = Stats::new();
        let mut solved = None;
        let mut deferred = VecDeque::new();
        let mut deferred_keys = HashSet::new();
        let mut backends = vec![BackendSlot {
            id: 1,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];

        let action = {
            let mut deferred_state = DeferredQueueState {
                deferred_solutions: &mut deferred,
                deferred_solution_keys: &mut deferred_keys,
                stats: &stats,
            };
            handle_mining_backend_event(
                BackendEvent::Solution(MiningSolution {
                    epoch: 41,
                    nonce: 9,
                    backend_id: 1,
                    backend: "cpu",
                }),
                42,
                &mut solved,
                &mut deferred_state,
                &mut backends,
                &backend_executor,
            )
        }
        .expect("stale solution should be deferred");

        assert_eq!(action, BackendEventAction::None);
        assert!(solved.is_none());
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].epoch, 41);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.deferred, 1);
        assert_eq!(snapshot.dropped, 0);
    }

    #[test]
    fn future_solution_is_dropped_and_accounted() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let stats = Stats::new();
        let mut solved = None;
        let mut deferred = VecDeque::new();
        let mut deferred_keys = HashSet::new();
        let mut backends = Vec::new();

        let action = {
            let mut deferred_state = DeferredQueueState {
                deferred_solutions: &mut deferred,
                deferred_solution_keys: &mut deferred_keys,
                stats: &stats,
            };
            handle_mining_backend_event(
                BackendEvent::Solution(MiningSolution {
                    epoch: 43,
                    nonce: 9,
                    backend_id: 1,
                    backend: "cpu",
                }),
                42,
                &mut solved,
                &mut deferred_state,
                &mut backends,
                &backend_executor,
            )
        }
        .expect("future solution should be dropped");

        assert_eq!(action, BackendEventAction::None);
        assert!(solved.is_none());
        assert!(deferred.is_empty());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.deferred, 0);
        assert_eq!(snapshot.dropped, 1);
    }

    #[test]
    fn same_epoch_solution_is_deferred_when_one_is_already_selected() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let stats = Stats::new();
        let mut solved = Some(MiningSolution {
            epoch: 42,
            nonce: 7,
            backend_id: 1,
            backend: "cpu",
        });
        let mut deferred = VecDeque::new();
        let mut deferred_keys = HashSet::new();
        let mut backends = vec![BackendSlot {
            id: 1,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];

        let action = {
            let mut deferred_state = DeferredQueueState {
                deferred_solutions: &mut deferred,
                deferred_solution_keys: &mut deferred_keys,
                stats: &stats,
            };
            handle_mining_backend_event(
                BackendEvent::Solution(MiningSolution {
                    epoch: 42,
                    nonce: 11,
                    backend_id: 1,
                    backend: "cpu",
                }),
                42,
                &mut solved,
                &mut deferred_state,
                &mut backends,
                &backend_executor,
            )
        }
        .expect("extra same-epoch solution should be deferred");

        assert_eq!(action, BackendEventAction::None);
        assert_eq!(solved.as_ref().map(|solution| solution.nonce), Some(7));
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].epoch, 42);
        assert_eq!(deferred[0].nonce, 11);
    }

    #[test]
    fn drain_mining_backend_events_keeps_all_solutions() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let stats = Stats::new();
        let (event_tx, event_rx) = crossbeam_channel::bounded(8);
        let mut solved = None;
        let mut deferred = VecDeque::new();
        let mut deferred_keys = HashSet::new();
        let mut backends = vec![BackendSlot {
            id: 1,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];

        event_tx
            .send(BackendEvent::Solution(MiningSolution {
                epoch: 42,
                nonce: 3,
                backend_id: 1,
                backend: "cpu",
            }))
            .expect("enqueue current solution");
        event_tx
            .send(BackendEvent::Solution(MiningSolution {
                epoch: 41,
                nonce: 5,
                backend_id: 1,
                backend: "cpu",
            }))
            .expect("enqueue stale solution");
        event_tx
            .send(BackendEvent::Solution(MiningSolution {
                epoch: 42,
                nonce: 9,
                backend_id: 1,
                backend: "cpu",
            }))
            .expect("enqueue additional current solution");

        let action = {
            let mut deferred_state = DeferredQueueState {
                deferred_solutions: &mut deferred,
                deferred_solution_keys: &mut deferred_keys,
                stats: &stats,
            };
            drain_mining_backend_events(
                &event_rx,
                42,
                &mut solved,
                &mut deferred_state,
                &mut backends,
                &backend_executor,
            )
        }
        .expect("drain should succeed");

        assert_eq!(action, BackendEventAction::None);
        assert_eq!(solved.as_ref().map(|solution| solution.nonce), Some(3));
        assert_eq!(deferred.len(), 2);
        assert!(deferred
            .iter()
            .any(|solution| solution.epoch == 41 && solution.nonce == 5));
        assert!(deferred
            .iter()
            .any(|solution| solution.epoch == 42 && solution.nonce == 9));
    }

    #[test]
    fn dedupe_queued_solutions_skips_repeated_solutions() {
        let queued = vec![
            MiningSolution {
                epoch: 4,
                nonce: 7,
                backend_id: 1,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 5,
                nonce: 9,
                backend_id: 1,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 4,
                nonce: 7,
                backend_id: 1,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 4,
                nonce: 7,
                backend_id: 2,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 4,
                nonce: 7,
                backend_id: 1,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 4,
                nonce: 7,
                backend_id: 1,
                backend: "cpu",
            },
        ];

        let deduped = dedupe_queued_solutions(queued);
        assert_eq!(deduped.len(), 2);
        assert!(deduped
            .iter()
            .any(|solution| solution.epoch == 4 && solution.nonce == 7));
        assert!(deduped
            .iter()
            .any(|solution| solution.epoch == 5 && solution.nonce == 9));
    }

    #[test]
    fn drop_solution_from_deferred_filters_primary_solution() {
        let primary = MiningSolution {
            epoch: 5,
            nonce: 42,
            backend_id: 1,
            backend: "cpu",
        };
        let mut deferred = vec![
            MiningSolution {
                epoch: 5,
                nonce: 42,
                backend_id: 1,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 5,
                nonce: 42,
                backend_id: 2,
                backend: "cpu",
            },
            MiningSolution {
                epoch: 5,
                nonce: 9,
                backend_id: 1,
                backend: "cpu",
            },
        ];

        drop_solution_from_deferred(&mut deferred, Some(&primary));
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].nonce, 9);
    }

    #[test]
    fn deferred_solution_queue_is_bounded() {
        let mut deferred = Vec::new();
        for idx in 0..(DEFERRED_SOLUTIONS_CAPACITY as u64 + 3) {
            push_deferred_solution(
                &mut deferred,
                MiningSolution {
                    epoch: idx,
                    nonce: idx,
                    backend_id: 1,
                    backend: "cpu",
                },
            );
        }

        assert_eq!(deferred.len(), DEFERRED_SOLUTIONS_CAPACITY);
        assert_eq!(deferred[0].epoch, 3);
    }

    #[test]
    fn deferred_solution_queue_dedupes_epoch_and_nonce() {
        let mut deferred = Vec::new();
        push_deferred_solution(
            &mut deferred,
            MiningSolution {
                epoch: 10,
                nonce: 77,
                backend_id: 1,
                backend: "cpu",
            },
        );
        push_deferred_solution(
            &mut deferred,
            MiningSolution {
                epoch: 10,
                nonce: 77,
                backend_id: 9,
                backend: "nvidia",
            },
        );

        assert_eq!(deferred.len(), 1);
    }

    #[test]
    fn accepted_submit_drops_queued_same_epoch_solutions() {
        let stats = Stats::new();
        let mut deferred_solutions = VecDeque::new();
        let mut deferred_solution_keys = HashSet::new();
        defer_solution_indexed(
            &mut deferred_solutions,
            &mut deferred_solution_keys,
            MiningSolution {
                epoch: 42,
                nonce: 11,
                backend_id: 1,
                backend: "cpu",
            },
            &stats,
        );
        defer_solution_indexed(
            &mut deferred_solutions,
            &mut deferred_solution_keys,
            MiningSolution {
                epoch: 42,
                nonce: 13,
                backend_id: 1,
                backend: "cpu",
            },
            &stats,
        );
        defer_solution_indexed(
            &mut deferred_solutions,
            &mut deferred_solution_keys,
            MiningSolution {
                epoch: 43,
                nonce: 17,
                backend_id: 1,
                backend: "cpu",
            },
            &stats,
        );

        let mut submitted_solution_order = VecDeque::new();
        let mut submitted_solution_keys = HashSet::new();
        let mut inflight_solution_keys = HashSet::new();
        inflight_solution_keys.insert((42, 7));
        let results = vec![SubmitResult {
            solution: MiningSolution {
                epoch: 42,
                nonce: 7,
                backend_id: 1,
                backend: "cpu",
            },
            outcome: SubmitOutcome::Response(crate::types::SubmitBlockResponse {
                accepted: true,
                hash: None,
                height: Some(7),
            }),
            attempts: 1,
            is_dev_fee: false,
        }];

        process_submit_results(
            results,
            &mut deferred_solutions,
            &mut deferred_solution_keys,
            &mut submitted_solution_order,
            &mut submitted_solution_keys,
            &mut inflight_solution_keys,
            &stats,
        );

        assert!(deferred_solutions
            .iter()
            .all(|solution| solution.epoch != 42));
        assert!(deferred_solutions
            .iter()
            .any(|solution| solution.epoch == 43));
        assert!(deferred_solution_keys.contains(&(43, 17)));
        assert!(!deferred_solution_keys.contains(&(42, 11)));
        assert!(!deferred_solution_keys.contains(&(42, 13)));
        assert!(!inflight_solution_keys.contains(&(42, 7)));
    }

    #[test]
    fn template_selection_matches_current_or_previous_epoch() {
        let current = SubmitTemplate::from_template(&sample_template("curr"));
        let previous = SubmitTemplate::from_template(&sample_template("prev"));
        let mut recent = VecDeque::new();
        recent.push_back(RecentTemplateEntry {
            epoch: 9,
            recorded_at: Instant::now(),
            submit_template: previous,
            estimated_bytes: 128,
        });

        assert!(submit_template_for_solution_epoch(10, &current, &recent, 10).is_some());
        assert!(submit_template_for_solution_epoch(10, &current, &recent, 9).is_some());
        assert!(submit_template_for_solution_epoch(10, &current, &recent, 8).is_none());
    }

    #[test]
    fn remember_recent_template_keeps_bounded_history() {
        let max_entries = 6usize;
        let mut recent = VecDeque::new();
        let mut bytes = 0usize;
        for epoch in 1..=(max_entries as u64 + 2) {
            remember_recent_template(
                &mut recent,
                &mut bytes,
                epoch,
                SubmitTemplate::from_template(&sample_template("tmpl")),
                Duration::from_secs(60),
                max_entries,
                usize::MAX,
            );
        }

        assert_eq!(recent.len(), max_entries);
        assert_eq!(recent.front().map(|entry| entry.epoch), Some(3));
        assert_eq!(
            recent.back().map(|entry| entry.epoch),
            Some(max_entries as u64 + 2)
        );
    }

    #[test]
    fn remember_recent_template_evicts_by_age() {
        let mut recent = VecDeque::new();
        let mut bytes = 256usize;
        recent.push_back(RecentTemplateEntry {
            epoch: 1,
            recorded_at: Instant::now() - Duration::from_secs(10),
            submit_template: SubmitTemplate::from_template(&sample_template("old")),
            estimated_bytes: 256,
        });
        remember_recent_template(
            &mut recent,
            &mut bytes,
            2,
            SubmitTemplate::from_template(&sample_template("new")),
            Duration::from_secs(1),
            64,
            usize::MAX,
        );

        assert_eq!(recent.len(), 1);
        assert_eq!(recent.front().map(|entry| entry.epoch), Some(2));
    }

    #[test]
    fn remember_recent_template_evicts_by_memory_cap() {
        let mut recent = VecDeque::new();
        let mut bytes = 0usize;
        let max_bytes = 150usize;

        remember_recent_template(
            &mut recent,
            &mut bytes,
            1,
            SubmitTemplate::Compact {
                template_id: "x".repeat(80),
            },
            Duration::from_secs(60),
            64,
            max_bytes,
        );
        remember_recent_template(
            &mut recent,
            &mut bytes,
            2,
            SubmitTemplate::Compact {
                template_id: "y".repeat(80),
            },
            Duration::from_secs(60),
            64,
            max_bytes,
        );

        assert!(bytes <= max_bytes);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent.front().map(|entry| entry.epoch), Some(2));
    }

    #[test]
    fn recent_template_cache_size_uses_timeout_window_and_bounds() {
        let min_entries = recent_template_cache_size_from_timeouts(
            Duration::from_secs(20),
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_millis(250),
        );
        assert_eq!(min_entries, RECENT_TEMPLATE_CACHE_MIN);

        let scaled_entries = recent_template_cache_size_from_timeouts(
            Duration::from_secs(1),
            Duration::from_secs(120),
            Duration::from_secs(2),
            Duration::from_millis(500),
        );
        assert!(scaled_entries > RECENT_TEMPLATE_CACHE_MIN);

        let capped_entries = recent_template_cache_size_from_timeouts(
            Duration::from_millis(1),
            Duration::from_secs(3_600),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        assert_eq!(capped_entries, RECENT_TEMPLATE_CACHE_MAX);
    }

    #[test]
    fn submitted_solution_cache_is_cross_backend_and_bounded() {
        let mut order = VecDeque::new();
        let mut keys = HashSet::new();
        let solution = MiningSolution {
            epoch: 9,
            nonce: 42,
            backend_id: 1,
            backend: "cpu",
        };
        let cross_backend = MiningSolution {
            epoch: 9,
            nonce: 42,
            backend_id: 2,
            backend: "cpu",
        };

        remember_submitted_solution(&mut order, &mut keys, &solution);
        assert!(already_submitted_solution(&keys, &solution));
        assert!(already_submitted_solution(&keys, &cross_backend));

        for idx in 0..(RECENT_SUBMITTED_SOLUTIONS_CAPACITY + 1) {
            let entry = MiningSolution {
                epoch: 100 + idx as u64,
                nonce: idx as u64,
                backend_id: 1,
                backend: "cpu",
            };
            remember_submitted_solution(&mut order, &mut keys, &entry);
        }

        assert_eq!(order.len(), RECENT_SUBMITTED_SOLUTIONS_CAPACITY);
        assert_eq!(keys.len(), RECENT_SUBMITTED_SOLUTIONS_CAPACITY);
        assert!(!already_submitted_solution(&keys, &solution));
    }

    #[test]
    fn relaxed_round_cancel_triggers_on_solved_or_stale_tip() {
        assert!(should_cancel_relaxed_round(false, true, false));
        assert!(should_cancel_relaxed_round(true, false, false));
        assert!(should_cancel_relaxed_round(false, false, true));
        assert!(!should_cancel_relaxed_round(false, false, false));
    }

    #[test]
    fn backend_error_reports_topology_change_when_backend_is_removed() {
        let backend_executor = super::super::backend_executor::BackendExecutor::new();
        let stats = Stats::new();
        let mut solved = None;
        let mut deferred = VecDeque::new();
        let mut deferred_keys = HashSet::new();
        let mut backends = vec![
            BackendSlot {
                id: 1,
                backend: Arc::new(NoopBackend::new("cpu")),
                lanes: 1,
                runtime_policy: crate::miner::BackendRuntimePolicy::default(),
                capabilities: crate::backend::BackendCapabilities::default(),
            },
            BackendSlot {
                id: 2,
                backend: Arc::new(NoopBackend::new("cpu")),
                lanes: 1,
                runtime_policy: crate::miner::BackendRuntimePolicy::default(),
                capabilities: crate::backend::BackendCapabilities::default(),
            },
        ];

        let action = {
            let mut deferred_state = DeferredQueueState {
                deferred_solutions: &mut deferred,
                deferred_solution_keys: &mut deferred_keys,
                stats: &stats,
            };
            handle_mining_backend_event(
                BackendEvent::Error {
                    backend_id: 1,
                    backend: "cpu",
                    message: "test failure".to_string(),
                },
                1,
                &mut solved,
                &mut deferred_state,
                &mut backends,
                &backend_executor,
            )
        }
        .expect("backend removal should be handled");

        assert_eq!(action, BackendEventAction::TopologyChanged);
        assert_eq!(backends.len(), 1);
        assert_eq!(backends[0].id, 2);
    }

    fn sample_template(template_id: &str) -> BlockTemplateResponse {
        BlockTemplateResponse {
            block: serde_json::from_value(json!({
                "header": {"nonce": 0u64},
                "txns": []
            }))
            .expect("sample template block should deserialize"),
            target: "00".repeat(32),
            header_base: "11".repeat(92),
            template_id: Some(template_id.to_string()),
        }
    }

    #[test]
    fn sleep_with_shutdown_stops_early_when_shutdown_requested() {
        let shutdown = AtomicBool::new(true);
        let start = Instant::now();
        assert!(!sleep_with_shutdown(&shutdown, Duration::from_secs(1)));
        assert!(start.elapsed() < Duration::from_millis(20));
    }

    #[test]
    fn adaptive_weight_update_tracks_observed_throughput() {
        let backends = vec![
            BackendSlot {
                id: 1,
                backend: Arc::new(NoopBackend::new("cpu")),
                lanes: 1,
                runtime_policy: crate::miner::BackendRuntimePolicy::default(),
                capabilities: crate::backend::BackendCapabilities::default(),
            },
            BackendSlot {
                id: 2,
                backend: Arc::new(NoopBackend::new("cpu")),
                lanes: 1,
                runtime_policy: crate::miner::BackendRuntimePolicy::default(),
                capabilities: crate::backend::BackendCapabilities::default(),
            },
        ];
        let mut weights = seed_backend_weights(&backends);
        let mut round_hashes = BTreeMap::new();
        round_hashes.insert(1, 10_000);
        round_hashes.insert(2, 1_000);

        update_backend_weights(
            &mut weights,
            WeightUpdateInputs {
                backends: &backends,
                round_backend_hashes: &round_hashes,
                round_backend_telemetry: None,
                round_elapsed_secs: 1.0,
                mode: WorkAllocation::Adaptive,
                round_end_reason: RoundEndReason::Refresh,
                refresh_interval: Duration::from_secs(1),
            },
        );

        assert!(weights.get(&1).copied().unwrap_or(0.0) > weights.get(&2).copied().unwrap_or(0.0));
    }

    #[test]
    fn adaptive_weight_update_uses_solved_rounds_with_lower_gain() {
        let backends = vec![BackendSlot {
            id: 7,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];
        let mut weights = seed_backend_weights(&backends);
        let mut round_hashes = BTreeMap::new();
        round_hashes.insert(7, 10_000);

        update_backend_weights(
            &mut weights,
            WeightUpdateInputs {
                backends: &backends,
                round_backend_hashes: &round_hashes,
                round_backend_telemetry: None,
                round_elapsed_secs: 1.0,
                mode: WorkAllocation::Adaptive,
                round_end_reason: RoundEndReason::Solved,
                refresh_interval: Duration::from_secs(1),
            },
        );

        assert!(weights.get(&7).copied().unwrap_or(0.0) > 1.0);
    }

    #[test]
    fn adaptive_weight_update_keeps_sub_one_throughput_signal() {
        let backends = vec![BackendSlot {
            id: 5,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];
        let mut weights = seed_backend_weights(&backends);
        let mut round_hashes = BTreeMap::new();
        round_hashes.insert(5, 1);

        update_backend_weights(
            &mut weights,
            WeightUpdateInputs {
                backends: &backends,
                round_backend_hashes: &round_hashes,
                round_backend_telemetry: None,
                round_elapsed_secs: 100.0,
                mode: WorkAllocation::Adaptive,
                round_end_reason: RoundEndReason::Refresh,
                refresh_interval: Duration::from_secs(1),
            },
        );

        let updated = weights.get(&5).copied().unwrap_or_default();
        assert!(updated > 0.0);
        assert!(updated < 1.0);
    }

    #[test]
    fn adaptive_weight_update_incorporates_short_rounds() {
        let backends = vec![BackendSlot {
            id: 15,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];
        let mut weights = seed_backend_weights(&backends);
        let mut round_hashes = BTreeMap::new();
        round_hashes.insert(15, 5_000);

        update_backend_weights(
            &mut weights,
            WeightUpdateInputs {
                backends: &backends,
                round_backend_hashes: &round_hashes,
                round_backend_telemetry: None,
                round_elapsed_secs: 0.020,
                mode: WorkAllocation::Adaptive,
                round_end_reason: RoundEndReason::Refresh,
                refresh_interval: Duration::from_secs(1),
            },
        );

        assert!(
            weights.get(&15).copied().unwrap_or_default() > 1.0,
            "short rounds should still contribute throughput signal"
        );
    }

    #[test]
    fn adaptive_weight_update_prefers_active_assignment_time_when_available() {
        let backends = vec![BackendSlot {
            id: 3,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 1,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];
        let mut weights = seed_backend_weights(&backends);
        let mut round_hashes = BTreeMap::new();
        round_hashes.insert(3, 100);
        let mut round_telemetry = BTreeMap::new();
        round_telemetry.insert(
            3,
            super::BackendRoundTelemetry {
                completed_assignment_micros: 100_000,
                ..super::BackendRoundTelemetry::default()
            },
        );

        update_backend_weights(
            &mut weights,
            WeightUpdateInputs {
                backends: &backends,
                round_backend_hashes: &round_hashes,
                round_backend_telemetry: Some(&round_telemetry),
                round_elapsed_secs: 1.0,
                mode: WorkAllocation::Adaptive,
                round_end_reason: RoundEndReason::Refresh,
                refresh_interval: Duration::from_secs(1),
            },
        );

        assert!(
            weights.get(&3).copied().unwrap_or_default() > 100.0,
            "active assignment timing should produce a stronger throughput signal than wall-clock"
        );
    }

    #[test]
    fn static_weight_update_resets_to_lane_weights() {
        let backends = vec![BackendSlot {
            id: 9,
            backend: Arc::new(NoopBackend::new("cpu")),
            lanes: 3,
            runtime_policy: crate::miner::BackendRuntimePolicy::default(),
            capabilities: crate::backend::BackendCapabilities::default(),
        }];
        let mut weights = BTreeMap::new();
        weights.insert(9, 999.0);
        let round_hashes = BTreeMap::new();

        update_backend_weights(
            &mut weights,
            WeightUpdateInputs {
                backends: &backends,
                round_backend_hashes: &round_hashes,
                round_backend_telemetry: None,
                round_elapsed_secs: 1.0,
                mode: WorkAllocation::Static,
                round_end_reason: RoundEndReason::Refresh,
                refresh_interval: Duration::from_secs(1),
            },
        );

        assert_eq!(weights.get(&9).copied(), Some(3.0));
    }

    #[test]
    fn compact_hash_uses_prefix_and_suffix() {
        assert_eq!(compact_hash("abcdef12"), "abcdef12");
        assert_eq!(compact_hash("abc"), "abc");
        assert_eq!(compact_hash("a1b2c3d4e5f6"), "a1b2...e5f6");
    }
}
