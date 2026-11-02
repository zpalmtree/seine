//! Optional, bounded evidence on the tuning thread. No PoW or network I/O.
use crate::backend::cpu::{CpuAdaptiveSnapshot, Placement, PlacementSampler};
use crate::config::{Config, CpuAffinityMode, CpuAutotuneResources, CpuPageMode};
use crate::hardware_telemetry::Recorder;
use blocknet_pow_kernel::CpuKernel;
use seine_telemetry::tuning::*;
use std::time::{Duration, Instant};

struct Window {
    started: u64,
    hashes: u64,
    context: TrialContext,
    changed: bool,
    last_hashes: u64,
    last_seen: u64,
}

struct Trace {
    outcome: TrialOutcome,
    window: Option<Window>,
    context_changed: bool,
}

pub(super) struct Monitor {
    recorder: Recorder,
    // Align the tuner's existing whole-second clock to this report's run.
    // Do not change the clock used for tuning decisions to collect telemetry.
    origin_ms: u64,
    members: Vec<(TrialAffinity, TrialPageMode)>,
    context: TrialContext,
    ready: bool,
    hashes: u64,
    trace: Option<Trace>,
    pending: Option<(TrialOutcome, TrialArm)>,
    placement: PlacementSampler,
    last_observed: Option<Instant>,
}

fn same_resources(a: &TrialResources, b: &TrialResources) -> bool {
    a.allowed_logical_cpus == b.allowed_logical_cpus
        && a.available_parallelism == b.available_parallelism
        && a.effective_memory_mib == b.effective_memory_mib
        && a.placement_fingerprint == b.placement_fingerprint
}

fn same_context(a: &TrialContext, b: &TrialContext) -> bool {
    a.members == b.members && same_resources(&a.resources, &b.resources)
}

impl Monitor {
    fn enabled(&mut self) -> bool {
        if self.recorder.trials_enabled() {
            return true;
        }
        self.trace = None;
        self.pending = None;
        false
    }

    #[cfg(test)]
    pub(super) fn test_without_placement(recorder: Recorder, cfg: &Config) -> Self {
        let mut monitor = Self::new(recorder, cfg);
        monitor.placement = PlacementSampler::with_probe(Placement::default);
        monitor
    }

    pub(super) fn new(recorder: Recorder, cfg: &Config) -> Self {
        let members = cfg
            .backend_specs
            .iter()
            .filter(|s| s.kind == super::BackendKind::Cpu)
            .map(|s| {
                let affinity = match s.cpu_affinity.unwrap_or(cfg.cpu_affinity) {
                    CpuAffinityMode::Auto => TrialAffinity::Auto,
                    CpuAffinityMode::Os => TrialAffinity::Os,
                    CpuAffinityMode::Off => TrialAffinity::Off,
                    CpuAffinityMode::PcoreOnly => TrialAffinity::PcoreOnly,
                };
                let pages = match cfg.cpu_page_mode {
                    CpuPageMode::Auto => TrialPageMode::Auto,
                    CpuPageMode::Regular => TrialPageMode::Regular,
                    CpuPageMode::Large => TrialPageMode::Large,
                    CpuPageMode::Large1G => TrialPageMode::Large1g,
                };
                (affinity, pages)
            })
            .collect();
        Self {
            origin_ms: recorder.trial_elapsed_ms(),
            recorder,
            members,
            context: TrialContext {
                members: Vec::new(),
                resources: TrialResources {
                    allowed_logical_cpus: None,
                    available_parallelism: None,
                    effective_memory_mib: None,
                    available_memory_mib: None,
                    placement_fingerprint: None,
                },
            },
            ready: false,
            hashes: 0,
            trace: None,
            pending: None,
            placement: PlacementSampler::default(),
            last_observed: None,
        }
    }

    fn time(&self, now: u64) -> u64 {
        self.origin_ms.saturating_add(now.saturating_mul(1000))
    }

    pub(super) fn observe(
        &mut self,
        now: u64,
        snapshots: &[CpuAdaptiveSnapshot],
        resources: CpuAutotuneResources,
    ) {
        if !self.enabled() {
            return;
        }
        // The controller ticks once a second. Do no topology IO while idle;
        // begin() takes its own fresh observation before opening a trial.
        let placement = if self.trace.is_some() || self.pending.is_some() {
            self.placement.sample()
        } else {
            Placement::default()
        };
        self.observe_placement(now, snapshots, resources, placement);
    }

    fn observe_placement(
        &mut self,
        now: u64,
        snapshots: &[CpuAdaptiveSnapshot],
        resources: CpuAutotuneResources,
        placement: Placement,
    ) {
        self.context = TrialContext {
            members: snapshots
                .iter()
                .enumerate()
                .filter(|(_, s)| s.running)
                .filter_map(|(slot, s)| {
                    let (affinity, page_mode) = *self.members.get(slot)?;
                    Some(TrialMember {
                        slot: slot.try_into().ok()?,
                        workers: s.threads.try_into().ok()?,
                        kernel: s.kernel.to_string(),
                        backing_mib: s.backing_bytes.map(|b| b / (1024 * 1024)),
                        affinity,
                        page_mode,
                    })
                })
                .collect(),
            resources: TrialResources {
                allowed_logical_cpus: placement.allowed_logical_cpus,
                available_parallelism: u32::try_from(resources.parallelism).ok().filter(|n| *n > 0),
                effective_memory_mib: resources.total_bytes.map(|b| b / (1024 * 1024)),
                available_memory_mib: resources.available_bytes.map(|b| b / (1024 * 1024)),
                placement_fingerprint: placement.fingerprint,
            },
        };
        self.ready = !self.context.members.is_empty()
            && snapshots
                .iter()
                .filter(|s| s.running)
                .all(|s| s.kernel_ready);
        self.hashes = snapshots
            .iter()
            .filter(|s| s.running)
            .fold(0u64, |n, s| n.saturating_add(s.hashes));
        let observed_at = Instant::now();
        let missed = self
            .last_observed
            .is_some_and(|last| observed_at.duration_since(last) > Duration::from_secs(3));
        self.last_observed = Some(observed_at);
        if let Some(trace) = &mut self.trace {
            trace.context_changed |= missed
                || !same_resources(
                    &trace.outcome.context_before.resources,
                    &self.context.resources,
                );
            if trace.context_changed {
                for sample in &mut trace.outcome.samples {
                    sample.changed = true;
                }
            }
        }
        if let Some(trace) = &mut self.trace {
            if let Some(window) = &mut trace.window {
                window.changed |= !same_context(&window.context, &self.context)
                    || self.hashes < window.hashes
                    || !self.ready
                    || trace.context_changed;
                if !window.changed {
                    window.last_hashes = self.hashes;
                    window.last_seen = now;
                }
            }
        }
        if let Some((mut outcome, target)) = self.pending.take() {
            if missed || !same_resources(&outcome.context_before.resources, &self.context.resources)
            {
                for sample in &mut outcome.samples {
                    sample.changed = true;
                }
            }
            let expected = match target {
                TrialArm::Baseline => &outcome.baseline,
                TrialArm::Candidate => &outcome.candidate,
            };
            if self.ready && self.context.members == *expected {
                self.emit(outcome, now);
            } else if !self.context.members.is_empty()
                && self
                    .context
                    .members
                    .iter()
                    .map(|m| (m.slot, m.workers, m.backing_mib))
                    .collect::<Vec<_>>()
                    != expected
                        .iter()
                        .map(|m| (m.slot, m.workers, m.backing_mib))
                        .collect::<Vec<_>>()
            {
                let mut aborted = outcome;
                aborted.decision = TrialDecision::Aborted;
                aborted.reason = TrialReason::ContextChanged;
                self.emit(aborted, now);
            } else {
                self.pending = Some((outcome, target));
            }
        }
    }

    pub(super) fn begin(
        &mut self,
        now: u64,
        base: CpuKernel,
        candidate: CpuKernel,
        policy: TrialPolicy,
    ) {
        if !self.enabled() {
            return;
        }
        // An unexpectedly replaced trace is evidence too; never report success.
        self.abort(now, TrialReason::ContextChanged);
        let placement = self.placement.sample();
        self.context.resources.allowed_logical_cpus = placement.allowed_logical_cpus;
        self.context.resources.placement_fingerprint = placement.fingerprint;
        self.last_observed = Some(Instant::now());
        let mut baseline = self.context.members.clone();
        let mut challenger = baseline.clone();
        for m in &mut baseline {
            m.kernel = base.to_string();
        }
        for m in &mut challenger {
            m.kernel = candidate.to_string();
        }
        self.trace = Some(Trace {
            window: None,
            context_changed: false,
            outcome: TrialOutcome {
                kind: TrialKind::CpuKernel,
                policy,
                started_ms: self.time(now),
                finished_ms: self.time(now),
                baseline,
                candidate: challenger,
                context_before: self.context.clone(),
                context_after: self.context.clone(),
                samples: Vec::with_capacity(MAX_SAMPLES),
                decision: TrialDecision::Aborted,
                reason: TrialReason::ContextChanged,
                applied: None,
                cost: TrialCost {
                    switching_ms: None,
                    idle_ms: None,
                },
            },
        });
    }

    /// Begin a comparison of two complete settings (thread count and kernel).
    pub(super) fn begin_settings(
        &mut self,
        now: u64,
        base: super::adaptive_search::Setting,
        candidate: super::adaptive_search::Setting,
        policy: TrialPolicy,
    ) {
        self.begin(now, base.kernel, candidate.kernel, policy);
        if let Some(trace) = &mut self.trace {
            let kind = if base.threads == candidate.threads {
                TrialKind::CpuKernel
            } else if base.kernel == candidate.kernel {
                TrialKind::CpuWorkers
            } else {
                TrialKind::CpuJoint
            };
            trace.outcome.kind = kind;
            let set = |members: &mut Vec<TrialMember>, threads: usize| {
                for member in members {
                    let lanes = u64::from(member.workers).max(1);
                    let per_lane = member.backing_mib.map(|mib| mib / lanes);
                    member.workers = threads.try_into().unwrap_or(member.workers);
                    member.backing_mib = per_lane.map(|mib| mib * threads as u64);
                }
            };
            set(&mut trace.outcome.baseline, base.threads);
            set(&mut trace.outcome.candidate, candidate.threads);
        }
    }

    /// Report a finished comparison at once; the next one may start immediately.
    pub(super) fn finish_applied(
        &mut self,
        now: u64,
        decision: TrialDecision,
        reason: TrialReason,
        applied: TrialArm,
    ) {
        if let Some(mut trace) = self.trace.take() {
            trace.outcome.decision = decision;
            trace.outcome.reason = reason;
            trace.outcome.finished_ms = self.time(now);
            trace.outcome.context_after = self.context.clone();
            trace.outcome.applied = Some(applied);
            self.recorder.record_trial(trace.outcome);
        }
    }

    pub(super) fn window(&mut self, now: u64) {
        if let Some(trace) = &mut self.trace {
            trace.window = Some(Window {
                started: now,
                hashes: self.hashes,
                context: self.context.clone(),
                changed: trace.context_changed,
                last_hashes: self.hashes,
                last_seen: now,
            });
        }
    }

    pub(super) fn discard_window(&mut self) {
        if let Some(trace) = &mut self.trace {
            trace.window = None;
        }
    }

    pub(super) fn sample(&mut self, hashes: u64, seconds: u64) {
        if let Some(trace) = &mut self.trace {
            let changed = trace.window.take().is_some_and(|w| w.changed) || trace.context_changed;
            if trace.outcome.samples.len() < MAX_SAMPLES {
                trace.outcome.samples.push(TrialSample {
                    arm: if trace.outcome.samples.len() % 3 == 1 {
                        TrialArm::Candidate
                    } else {
                        TrialArm::Baseline
                    },
                    hashes,
                    elapsed_ms: seconds.saturating_mul(1000),
                    settling_ms: None,
                    context: self.context.clone(),
                    changed,
                });
            }
        }
    }

    #[cfg(test)]
    pub(super) fn finish(
        &mut self,
        now: u64,
        decision: TrialDecision,
        reason: TrialReason,
        target: TrialArm,
    ) {
        if let Some(mut trace) = self.trace.take() {
            trace.outcome.decision = decision;
            trace.outcome.reason = reason;
            let expected = match target {
                TrialArm::Baseline => &trace.outcome.baseline,
                TrialArm::Candidate => &trace.outcome.candidate,
            };
            if self.ready && self.context.members == *expected {
                self.emit(trace.outcome, now);
            } else {
                self.pending = Some((trace.outcome, target));
            }
        }
    }

    pub(super) fn abort(&mut self, now: u64, reason: TrialReason) {
        if let Some(trace) = &mut self.trace {
            if let Some(window) = trace.window.take() {
                if window.last_seen > window.started && trace.outcome.samples.len() < MAX_SAMPLES {
                    trace.outcome.samples.push(TrialSample {
                        arm: if trace.outcome.samples.len() % 3 == 1 {
                            TrialArm::Candidate
                        } else {
                            TrialArm::Baseline
                        },
                        hashes: window.last_hashes - window.hashes,
                        elapsed_ms: (window.last_seen - window.started).saturating_mul(1000),
                        settling_ms: None,
                        context: window.context,
                        changed: window.changed,
                    });
                }
            }
        }
        let outcome = self
            .trace
            .take()
            .map(|t| t.outcome)
            .or_else(|| self.pending.take().map(|p| p.0));
        if let Some(mut outcome) = outcome {
            outcome.decision = TrialDecision::Aborted;
            outcome.reason = reason;
            self.emit(outcome, now);
        }
    }

    pub(super) fn abort_observed(
        &mut self,
        now: u64,
        snapshots: &[CpuAdaptiveSnapshot],
        resources: CpuAutotuneResources,
        reason: TrialReason,
    ) {
        if !self.enabled() {
            return;
        }
        // A control failure can follow a successful request to another CPU
        // backend. Refresh actual settings without acknowledging a pending win.
        let pending = self.pending.take();
        let placement = self.placement.sample();
        self.observe_placement(now, snapshots, resources, placement);
        self.pending = pending;
        self.abort(now, reason);
    }

    fn emit(&self, mut outcome: TrialOutcome, now: u64) {
        outcome.finished_ms = self.time(now);
        outcome.context_after = self.context.clone();
        outcome.applied = if self.ready && outcome.context_after.members == outcome.baseline {
            Some(TrialArm::Baseline)
        } else if self.ready && outcome.context_after.members == outcome.candidate {
            Some(TrialArm::Candidate)
        } else {
            None
        };
        self.recorder.record_trial(outcome);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known() -> Placement {
        Placement {
            allowed_logical_cpus: Some(4),
            fingerprint: Some("a".repeat(64)),
        }
    }

    fn resources() -> CpuAutotuneResources {
        CpuAutotuneResources {
            parallelism: 4,
            total_bytes: Some(8 * 1024 * 1024 * 1024),
            available_bytes: Some(6 * 1024 * 1024 * 1024),
        }
    }

    fn snapshot() -> CpuAdaptiveSnapshot {
        CpuAdaptiveSnapshot {
            kernel: CpuKernel::Avx2,
            kernel_generation: 1,
            kernel_ready: true,
            backing_bytes: [0, 0, 0, 2 * 1024 * 1024 * 1024, 0],
            running: true,
            threads: 1,
            committed_bytes: 2 * 1024 * 1024 * 1024,
            resident_bytes: 2 * 1024 * 1024 * 1024,
            hashes: 0,
            hashing: true,
            allocation_failures: 0,
            warmed: 1,
        }
    }

    fn setup() -> (Monitor, Recorder, CpuAdaptiveSnapshot, TrialPolicy) {
        let recorder = Recorder::test_recorder();
        let mut monitor = Monitor::new(recorder.clone(), &super::super::tests::test_config());
        monitor.placement = PlacementSampler::with_probe(known);
        let snapshot = snapshot();
        monitor.observe(0, &[snapshot], resources());
        let fixture: seine_telemetry::Report =
            serde_json::from_str(include_str!("../../telemetry/tuning-fixture.json")).unwrap();
        let policy = fixture.tuning.unwrap().trials.remove(0).outcome.policy;
        (monitor, recorder, snapshot, policy)
    }

    #[test]
    fn a_thread_count_comparison_reports_as_a_worker_trial() {
        use super::super::adaptive_search::Setting;
        let (mut m, recorder, mut s, policy) = setup();
        let base = Setting {
            threads: 1,
            kernel: CpuKernel::Avx2,
        };
        let candidate = Setting {
            threads: 2,
            kernel: CpuKernel::Avx2,
        };
        m.begin_settings(0, base, candidate, policy);
        let mut now = 1;
        for arm in [1usize, 2, 1] {
            s.threads = arm;
            s.warmed = arm;
            s.resident_bytes = arm as u64 * 2 * 1024 * 1024 * 1024;
            s.committed_bytes = s.resident_bytes;
            s.backing_bytes[3] = s.resident_bytes;
            m.observe(now, &[s], resources());
            m.window(now);
            now += 10;
            s.hashes += 100;
            m.observe(now, &[s], resources());
            m.sample(100, 10);
            now += 1;
        }
        m.finish_applied(
            now,
            TrialDecision::NoConfirmedGain,
            TrialReason::Completed,
            TrialArm::Baseline,
        );
        let report = recorder.test_tuning_report().unwrap();
        assert_eq!(report.trials.len(), 1, "{report:?}");
        let outcome = &report.trials[0].outcome;
        assert_eq!(outcome.kind, TrialKind::CpuWorkers);
        assert_eq!(outcome.baseline[0].workers, 1);
        assert_eq!(outcome.candidate[0].workers, 2);
        assert_eq!(outcome.samples.len(), 3);
        assert_eq!(outcome.applied, Some(TrialArm::Baseline));
    }

    #[test]
    fn fresh_known_context_survives_real_report_contract_without_changing_decision() {
        let (mut m, recorder, mut s, policy) = setup();
        assert!(m.context.resources.placement_fingerprint.is_none()); // Idle never probes.
        m.begin(0, CpuKernel::Avx2, CpuKernel::Avx512, policy);
        assert_eq!(
            m.context.resources.placement_fingerprint,
            known().fingerprint
        );
        let mut now = 1;
        for arm in [false, true, false, false, true, false] {
            s.kernel = if arm {
                CpuKernel::Avx512
            } else {
                CpuKernel::Avx2
            };
            m.observe(now, &[s], resources());
            m.window(now);
            now += 30;
            let hashes = if arm { 3300 } else { 3000 };
            s.hashes += hashes;
            m.observe(now, &[s], resources());
            m.sample(hashes, 30);
            now += 1;
        }
        m.finish(
            now,
            TrialDecision::Gain,
            TrialReason::Completed,
            TrialArm::Candidate,
        );
        assert!(recorder.test_tuning_report().is_none());
        s.kernel = CpuKernel::Avx512;
        m.observe(now + 1, &[s], resources());
        let tuning = recorder.test_tuning_report().unwrap();
        assert_eq!(tuning.dropped_trials, 0);
        let trial = &tuning.trials[0].outcome;
        assert_eq!(trial.decision, TrialDecision::Gain);
        assert_eq!(trial.applied, Some(TrialArm::Candidate));
        assert_eq!(trial.samples.len(), 6);
        assert!(trial.samples.iter().all(|s| !s.changed));
        assert_eq!(trial.context_before.resources.allowed_logical_cpus, Some(4));
        assert!(trial.cost.idle_ms.is_none());
        let mut report: seine_telemetry::Report =
            serde_json::from_str(include_str!("../../telemetry/tuning-fixture.json")).unwrap();
        report.tuning = Some(tuning);
        assert!(report.validate());
        // Optional test-only artifact for exercising collector/SQL boundaries.
        if let Ok(path) = std::env::var("SEINE_TRIAL_TEST_OUTPUT") {
            std::fs::write(path, serde_json::to_vec(&report).unwrap()).unwrap();
        }
    }

    #[test]
    fn transient_mid_window_and_between_window_changes_stay_disqualified() {
        for changed in [
            Placement {
                allowed_logical_cpus: Some(4),
                fingerprint: Some("b".repeat(64)),
            },
            Placement {
                allowed_logical_cpus: Some(2),
                fingerprint: Some("a".repeat(64)),
            },
            Placement::default(),
        ] {
            for between in [false, true] {
                let (mut m, _, s, policy) = setup();
                m.begin(0, CpuKernel::Avx2, CpuKernel::Avx512, policy);
                m.window(1);
                if between {
                    m.sample(3000, 30);
                }
                m.observe_placement(32, &[s], resources(), changed.clone());
                m.observe_placement(33, &[s], resources(), known());
                if between {
                    m.window(34);
                }
                m.sample(3000, 30);
                let trace = m.trace.as_ref().unwrap();
                assert!(trace.outcome.samples.iter().all(|s| s.changed));
                assert!(trace.window.is_none());
            }
        }
    }

    #[test]
    fn resource_changes_and_missed_observations_invalidate_but_free_memory_does_not() {
        let (mut m, _, s, policy) = setup();
        m.begin(0, CpuKernel::Avx2, CpuKernel::Avx512, policy);
        m.window(1);
        let mut r = resources();
        r.available_bytes = Some(1024 * 1024 * 1024);
        m.observe_placement(2, &[s], r, known());
        assert!(!m.trace.as_ref().unwrap().context_changed);
        r.total_bytes = Some(4 * 1024 * 1024 * 1024);
        m.observe_placement(3, &[s], r, known());
        assert!(m.trace.as_ref().unwrap().context_changed);
        let (mut m, _, s, policy) = setup();
        m.begin(0, CpuKernel::Avx2, CpuKernel::Avx512, policy);
        m.last_observed = Some(Instant::now() - Duration::from_secs(4));
        m.observe_placement(4, &[s], resources(), known());
        assert!(m.trace.as_ref().unwrap().context_changed);
    }

    #[test]
    fn pending_acknowledgment_retains_a_transient_context_change() {
        let (mut m, recorder, mut s, policy) = setup();
        m.begin(0, CpuKernel::Avx2, CpuKernel::Avx512, policy);
        for _ in 0..6 {
            m.sample(3000, 30);
        }
        m.finish(
            200,
            TrialDecision::Gain,
            TrialReason::Completed,
            TrialArm::Candidate,
        );
        assert!(m.pending.is_some());
        s.kernel_ready = false;
        m.observe_placement(201, &[s], resources(), Placement::default());
        s.kernel_ready = true;
        s.kernel = CpuKernel::Avx512;
        m.observe_placement(202, &[s], resources(), known());
        let report = recorder.test_tuning_report().unwrap();
        assert_eq!(report.dropped_trials, 0);
        assert!(report.trials[0].outcome.samples.iter().all(|s| s.changed));
    }

    #[test]
    fn idle_observation_does_not_poll_topology() {
        let (mut m, _, s, _) = setup();
        m.placement = PlacementSampler::with_probe(|| panic!("idle must not poll"));
        m.observe(1, &[s], resources());
    }

    #[test]
    fn opt_out_or_worker_exit_stops_placement_probes_and_clears_pending_trace() {
        let (mut m, recorder, s, policy) = setup();
        m.begin(0, CpuKernel::Avx2, CpuKernel::Avx512, policy.clone());
        recorder.test_disable_trials();
        m.placement = PlacementSampler::with_probe(|| panic!("disabled must not poll"));
        m.observe(1, &[s], resources());
        m.begin(2, CpuKernel::Avx2, CpuKernel::Avx512, policy);
        m.abort_observed(3, &[s], resources(), TrialReason::UserInterrupted);
        assert!(m.trace.is_none());
        assert!(m.pending.is_none());
        assert!(recorder.test_tuning_report().is_none());
    }
}
