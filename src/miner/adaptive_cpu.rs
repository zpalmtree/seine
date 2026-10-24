//! Productive CPU tuning: startup uses a seed, and A/B/A trials run on real work.
//! RAM limits constrain the operating count without replacing a remembered peak.
use super::*;
use crate::config::{cpu_autotune_capacity, cpu_autotune_worker_ceiling};
use blocknet_pow_kernel::CpuKernel;
use std::thread::{self, JoinHandle};

const RECOVERY_SECS: u64 = 30;
const RETRY_SECS: u64 = 300;
const MAX_DRIFT: f64 = 0.08;
const JOINT_TIMEOUT_SECS: u64 = 15 * 60;
const SWITCH_TIMEOUT_SECS: u64 = 10 * 60;

pub(super) fn limits(cfg: &Config, owned_bytes: u64) -> (usize, usize, usize) {
    let (_, available) = cpu_autotune_capacity(cfg, owned_bytes);
    let (min, max) = worker_bounds(cfg, cpu_autotune_worker_ceiling(cfg));
    (
        min,
        max,
        available
            .min(max)
            .min(cfg.cpu_autotune_worker_slots.unwrap_or(max)),
    )
}

fn worker_bounds(cfg: &Config, cpu_ceiling: usize) -> (usize, usize) {
    // Parked slots and their nonce stride must survive a raised cgroup RAM
    // ceiling. Only active arenas are bounded by the current memory budget.
    let max = cfg
        .cpu_autotune_max_threads
        .unwrap_or(cpu_ceiling)
        .min(cpu_ceiling)
        .max(1);
    (cfg.cpu_autotune_min_threads.max(1).min(max), max)
}

fn instances(cfg: &Config) -> usize {
    cfg.backend_specs
        .iter()
        .filter(|s| s.kind == BackendKind::Cpu)
        .count()
}

fn compatible(record: &CpuAutotuneRecord, cfg: &Config, host: &CpuAutotuneHostFingerprint) -> bool {
    // Schema 10 already has warmed scores. Old records remain useful as seeds.
    (10..=CPU_AUTOTUNE_RECORD_SCHEMA_VERSION).contains(&record.schema_version)
        && record.host_fingerprint == *host
        && record.affinity == cpu_affinity_label(cfg.cpu_affinity)
        && record.page_mode == cfg.cpu_page_mode.as_str()
        && record.profile == cpu_profile_label(cfg.cpu_profile)
        && record.cpu_instances == instances(cfg)
        && record.selected_threads > 0
        && record.measured_hps.is_finite()
        && record.measured_hps >= 0.0
}

fn read_seed(cfg: &Config) -> Option<CpuAutotuneRecord> {
    let record: CpuAutotuneRecord =
        serde_json::from_slice(&fs::read(&cfg.cpu_autotune_config_path).ok()?).ok()?;
    compatible(&record, cfg, &cpu_autotune_host_fingerprint()).then_some(record)
}

fn seed_threads(
    record: Option<&CpuAutotuneRecord>,
    min: usize,
    max: usize,
    available: usize,
    memory_capacity: usize,
) -> usize {
    if available == 0 {
        return 0;
    }
    let floor = min.min(available);
    record
        .map(|r| r.selected_threads)
        .unwrap_or((max.min(memory_capacity) / 2).max(1))
        .clamp(floor, max.min(available))
}

// Threads already running (including the API's Tokio worker pool) are charged
// in pids.current. Reserve only later work. DNS/blocking and GPU driver threads
// are demand-driven, so the margin is best effort; startup also handles spawn
// failure. Keep this accounting alongside the mining startup order.
fn startup_task_reserve(cfg: &Config) -> usize {
    let cpu = instances(cfg);
    let backends = cfg.backend_specs.len();
    let common = cpu + 2 * backends + 2; // event forwarders, executors, init/cleanup, fan-in, tuner
    let network = match cfg.mode {
        // User AND dev sessions: two Stratum + two HTTP runtimes + fetch worker.
        MiningMode::Pool => 5 + 3, // three DNS/blocking slots
        MiningMode::Daemon => 2 + usize::from(cfg.sse_enabled) + 2, // prefetch, submit, SSE, DNS
    };
    common + network
        + if cfg.token.is_some() { 2 } else { 0 } // JSON and SSE HTTP runtimes
        + if should_enable_tui(cfg) { 2 } else { 0 }
        + usize::from(cfg.api_server_enabled || cfg.service_mode) // control worker may race prepare
        + 2 * (backends - cpu) // additional accelerator/driver startup margin
}

fn provisioned_slots(max: usize, count: usize, headroom: Option<usize>, reserve: usize) -> usize {
    headroom
        .map_or(max, |remaining| {
            remaining.saturating_sub(reserve) / count.max(1)
        })
        .min(max)
}

pub(super) fn manual_limit(cfg: &Config, backends: &[Arc<CpuBackend>]) -> usize {
    let running: Vec<_> = backends
        .iter()
        .filter(|b| b.adaptive_snapshot().running)
        .collect();
    let slots = running
        .iter()
        .map(|b| b.worker_capacity())
        .min()
        .unwrap_or(0);
    let owned = running
        .iter()
        .map(|b| b.adaptive_snapshot().committed_bytes)
        .sum();
    let current = running.iter().map(|b| b.enabled_lanes()).min().unwrap_or(0);
    let (_, available) = cpu_autotune_capacity(cfg, owned);
    available.min(slots).max(current)
}

pub(super) fn prepare(cfg: &mut Config) -> Result<()> {
    if instances(cfg) == 0 {
        return Ok(());
    }
    let (memory_capacity, available) = cpu_autotune_capacity(cfg, 0);
    prepare_with_resources(
        cfg,
        cpu_autotune_worker_ceiling(cfg),
        memory_capacity,
        available,
        crate::config::cpu_task_headroom(),
    )
}

fn prepare_with_resources(
    cfg: &mut Config,
    cpu_ceiling: usize,
    memory_capacity: usize,
    available: usize,
    headroom: Option<usize>,
) -> Result<()> {
    let (min, max) = worker_bounds(cfg, cpu_ceiling);
    let slots = provisioned_slots(max, instances(cfg), headroom, startup_task_reserve(cfg));
    if slots == 0 {
        bail!("insufficient task/thread headroom for automatic CPU mining and its control/network threads; increase the process task limit or stop other tasks in this cgroup");
    }
    let available = available.min(max).min(slots);
    let seed = read_seed(cfg);
    let threads = seed_threads(seed.as_ref(), min, max, available, memory_capacity);
    // Zero is intentional here. A parked CPU backend stays connected and can
    // recover when a whole arena plus the configured reserve fits again.
    cfg.threads = threads;
    cfg.cpu_autotune_worker_slots = Some(slots);
    // This field also bounds manual controls/offline tuning; never replace its
    // RAM ceiling with the larger parked-slot or CPU-only search maximum.
    cfg.cpu_auto_threads_cap = memory_capacity.min(slots);
    for spec in cfg
        .backend_specs
        .iter_mut()
        .filter(|s| s.kind == BackendKind::Cpu)
    {
        spec.cpu_threads = Some(threads);
    }
    let reason = if available == 0 {
        "waiting for RAM"
    } else if seed.is_some() {
        "cached starting point"
    } else {
        "initial estimate"
    };
    info("MINER", format!("cpu-autotune | starting with {threads} threads ({reason}); tuning while mining, capacity={max}, worker slots={slots}, currently available={available}"));
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum Phase {
    Baseline,
    Probe {
        base: usize,
        before: CpuAutotuneMeasurement,
    },
    Confirm {
        base: usize,
        before: CpuAutotuneMeasurement,
        candidate: usize,
        candidate_sample: CpuAutotuneMeasurement,
    },
}

#[derive(Debug, Clone)]
struct JointComparison {
    base: usize,
    candidate: usize,
    base_kernel: CpuKernel,
    candidate_kernel: Option<CpuKernel>,
    base_backing: Vec<[u64; 5]>,
    candidate_backing: Option<Vec<[u64; 5]>>,
    started: u64,
    confirmed_peak: f64,
    // A changed kernel must also win a second fresh count/configuration A/B/A.
    repeat: bool,
}

#[derive(Debug)]
struct Policy {
    current: usize,
    min: usize,
    max: usize,
    profile: CpuPerformanceProfile,
    phase: Phase,
    up: bool,
    attempts: u64,
    ramp: bool,
    next_up_probe: Option<usize>,
    unsuccessful: usize,
    next_trial: u64,
    allocation_retry_at: u64,
    headroom_since: u64,
    previous_cap: usize,
    cap_observed: bool,
    recovery_pending: bool,
    window_start: u64,
    window_hashes: u64,
    window_secs: u64,
    sampling: bool,
    idle_since: Option<u64>,
    reference_hps: Option<f64>,
    peak_hps: f64,
    joint: Option<JointComparison>,
    restoring: Option<(CpuKernel, u64)>,
}

#[derive(Debug)]
struct Decision {
    threads: usize,
    reason: &'static str,
    learned: Option<Vec<(usize, CpuAutotuneMeasurement)>>,
}

impl Policy {
    fn new(cfg: &Config, current: usize, min: usize, max: usize) -> Self {
        Self {
            current,
            min,
            max,
            profile: cfg.cpu_profile,
            phase: Phase::Baseline,
            up: true,
            attempts: 0,
            ramp: true,
            next_up_probe: None,
            unsuccessful: 0,
            next_trial: 0,
            allocation_retry_at: 0,
            headroom_since: 0,
            previous_cap: current,
            cap_observed: false,
            recovery_pending: false,
            window_start: 0,
            window_hashes: 0,
            window_secs: cfg.cpu_autotune_secs.saturating_mul(5).max(30),
            sampling: false,
            idle_since: None,
            reference_hps: None,
            peak_hps: 0.0,
            joint: None,
            restoring: None,
        }
    }

    fn change(&mut self, threads: usize, reason: &'static str) -> Decision {
        self.current = threads;
        self.recovery_pending = false;
        self.sampling = false;
        self.idle_since = None;
        Decision {
            threads,
            reason,
            learned: None,
        }
    }

    fn reset(&mut self, now: u64, delay: u64) {
        self.phase = Phase::Baseline;
        self.next_up_probe = None;
        self.sampling = false;
        self.idle_since = None;
        self.next_trial = now.saturating_add(delay);
    }

    fn observe_cap(&mut self, now: u64, cap: usize) {
        if cap <= self.current {
            self.recovery_pending = false;
        } else if self.cap_observed && cap > self.previous_cap && self.previous_cap <= self.current
        {
            self.recovery_pending = true;
        }
        self.cap_observed = true;
        if cap != self.previous_cap {
            if cap <= self.current || self.previous_cap <= self.current {
                self.headroom_since = now;
            }
            if cap > self.previous_cap {
                self.up = true;
                self.ramp = true;
                self.next_trial = self.next_trial.min(now.saturating_add(RECOVERY_SECS));
            }
            self.previous_cap = cap;
        }
    }

    fn restore_kernel(
        &mut self,
        now: u64,
        kernel: CpuKernel,
        snapshots: &[crate::backend::cpu::CpuAdaptiveSnapshot],
        action: &mut super::adaptive_kernel::Action,
    ) {
        if snapshots
            .iter()
            .filter(|s| s.running)
            .any(|s| s.kernel != kernel || !s.kernel_ready)
        {
            action.kernel = Some(kernel);
            self.restoring = Some((kernel, now));
            self.sampling = false;
            self.idle_since = None;
        }
    }

    fn cancel_joint(
        &mut self,
        kernels: &mut super::adaptive_kernel::Tuner,
        now: u64,
        snapshots: &[crate::backend::cpu::CpuAdaptiveSnapshot],
        cap: usize,
        actual: usize,
        allocation_limited: bool,
    ) -> (super::adaptive_kernel::Action, Option<Decision>) {
        let joint = self.joint.take().expect("joint comparison exists");
        let delay = if allocation_limited {
            RETRY_SECS
        } else {
            RECOVERY_SECS
        };
        self.restoring = None;
        self.reset(now, delay);
        if allocation_limited {
            self.allocation_retry_at = now + RETRY_SECS;
        }
        let mut action = kernels.abort(now);
        // The kernel tuner's baseline belongs to the candidate count. Restore
        // the original configuration's kernel on every outer cancellation.
        action.kernel = Some(joint.base_kernel);
        action.hold_counts = false;
        self.restore_kernel(now, joint.base_kernel, snapshots, &mut action);
        let count = if allocation_limited {
            actual.min(cap)
        } else {
            joint.base.min(cap)
        };
        (
            action,
            Some(self.change(
                count,
                "joint comparison interrupted; restoring safe baseline",
            )),
        )
    }

    fn tick_joint(
        &mut self,
        kernels: &mut super::adaptive_kernel::Tuner,
        now: u64,
        snapshots: &[crate::backend::cpu::CpuAdaptiveSnapshot],
        cap: usize,
        allocation_failed: bool,
    ) -> (super::adaptive_kernel::Action, Option<Decision>) {
        let cap = cap.min(self.max);
        let running: Vec<_> = snapshots.iter().filter(|s| s.running).collect();
        let actual = running.iter().map(|s| s.threads).min().unwrap_or(0);
        let allocation_limited = allocation_failed || actual < self.current;
        let hashes = running.iter().map(|s| s.hashes).sum();
        let ready = !running.is_empty()
            && running.iter().all(|s| {
                s.hashing
                    && s.warmed == s.threads
                    && s.resident_bytes == s.threads as u64 * CPU_LANE_MEMORY_BYTES
            });
        self.observe_cap(now, cap);
        let recovering_capacity = self.recovery_pending && now >= self.allocation_retry_at;
        let recovery_due = recovering_capacity
            && now >= self.next_trial
            && now.saturating_sub(self.headroom_since) >= RECOVERY_SECS;
        let safety_change = (recovery_due && kernels.is_comparing())
            || cap < actual
            || actual != self.current
            || allocation_failed
            || running.len() != snapshots.len()
            || actual == 0;
        let backing: Vec<_> = snapshots.iter().map(|s| s.backing_bytes).collect();
        let joint_invalid = self.joint.as_ref().is_some_and(|j| {
            j.base.max(j.candidate) > cap
                || now.saturating_sub(j.started)
                    >= JOINT_TIMEOUT_SECS
                        .max(self.window_secs.saturating_mul(6).saturating_add(600))
                || (ready
                    && !matches!(self.phase, Phase::Probe { .. })
                    && backing != j.base_backing)
        });
        let switch_timeout = self
            .restoring
            .is_some_and(|(_, start)| now.saturating_sub(start) >= SWITCH_TIMEOUT_SECS);
        if self.joint.is_some() && (safety_change || joint_invalid || switch_timeout) {
            return self.cancel_joint(kernels, now, snapshots, cap, actual, allocation_limited);
        }
        if safety_change || switch_timeout {
            self.restoring = None;
        }
        if let Some((kernel, _)) = self.restoring {
            if ready && running.iter().all(|s| s.kernel == kernel && s.kernel_ready) {
                self.restoring = None;
            } else {
                self.sampling = false;
                return (
                    super::adaptive_kernel::Action {
                        hold_counts: true,
                        ..Default::default()
                    },
                    None,
                );
            }
        }
        let phase_before = self.phase;
        let mut action = if safety_change || switch_timeout {
            kernels.abort(now)
        } else {
            kernels.tick(
                now,
                snapshots,
                ready,
                (matches!(self.phase, Phase::Baseline)
                    && self.joint.is_none()
                    && !recovering_capacity)
                    || matches!(self.phase, Phase::Probe { .. }),
            )
        };
        if action.interrupted && self.joint.is_some() {
            return self.cancel_joint(kernels, now, snapshots, cap, actual, allocation_limited);
        }
        if action.hold_counts {
            self.sampling = false;
            self.idle_since = None;
        }
        if action.reset_scores {
            if matches!(self.phase, Phase::Probe { .. }) {
                // Keep the original count/kernel baseline while selecting the
                // candidate's path. Only this window's partial sample is stale.
                self.sampling = false;
                self.idle_since = None;
            } else {
                self.kernel_changed(now);
            }
        }
        let mut decision = if action.hold_counts && !safety_change {
            None
        } else {
            self.tick(
                now,
                hashes,
                ready && running.iter().all(|s| s.kernel_ready) && action.kernel.is_none(),
                cap,
                allocation_failed,
                actual,
            )
        };
        match (phase_before, self.phase) {
            (Phase::Baseline, Phase::Probe { base, .. }) => {
                if let Some(joint) = self.joint.clone() {
                    // Second outer A/B/A: use the already-confirmed candidate
                    // kernel, then wait for acknowledgements before sampling.
                    self.restore_kernel(
                        now,
                        joint.candidate_kernel.unwrap_or(joint.base_kernel),
                        snapshots,
                        &mut action,
                    );
                } else if let Some(first) = running.first() {
                    self.joint = Some(JointComparison {
                        base,
                        candidate: self.current,
                        base_kernel: first.kernel,
                        candidate_kernel: None,
                        base_backing: backing.clone(),
                        candidate_backing: None,
                        started: now,
                        confirmed_peak: self.peak_hps,
                        repeat: false,
                    });
                }
            }
            (Phase::Probe { .. }, Phase::Confirm { .. }) => {
                if let (Some(mut joint), Some(first)) = (self.joint.clone(), running.first()) {
                    if joint.repeat
                        && (joint.candidate_kernel != Some(first.kernel)
                            || joint.candidate_backing.as_ref() != Some(&backing))
                    {
                        return self.cancel_joint(kernels, now, snapshots, cap, actual, false);
                    }
                    joint.candidate_kernel = Some(first.kernel);
                    joint.candidate_backing = Some(backing);
                    let base_kernel = joint.base_kernel;
                    self.joint = Some(joint);
                    self.restore_kernel(now, base_kernel, snapshots, &mut action);
                }
            }
            (Phase::Confirm { .. }, Phase::Baseline) => {
                if let Some(mut joint) = self.joint.take() {
                    let accepted = decision
                        .as_ref()
                        .is_some_and(|d| d.learned.is_some() && d.threads == joint.candidate);
                    let candidate_kernel = joint.candidate_kernel.unwrap_or(joint.base_kernel);
                    if accepted && candidate_kernel != joint.base_kernel && !joint.repeat {
                        joint.repeat = true;
                        let base = joint.base;
                        // The first outer gain is provisional. It must not raise
                        // the profile floor if the fresh confirmation fails.
                        self.peak_hps = joint.confirmed_peak;
                        self.joint = Some(joint);
                        self.reset(now, 0);
                        self.reference_hps = None;
                        decision =
                            Some(self.change(base, "repeating joint configuration confirmation"));
                    } else {
                        let selected = if accepted {
                            candidate_kernel
                        } else {
                            joint.base_kernel
                        };
                        self.restore_kernel(now, selected, snapshots, &mut action);
                    }
                }
            }
            (Phase::Probe { .. }, Phase::Baseline) if self.joint.is_some() => {
                return self.cancel_joint(kernels, now, snapshots, cap, actual, allocation_limited);
            }
            _ => {}
        }
        (action, decision)
    }

    fn kernel_changed(&mut self, now: u64) {
        // Each count window must use one path, but the profile floor is a joint
        // throughput frontier. Retain that peak and the discovery/retention
        // state across confirmed path changes; resetting them can cycle between
        // a low-count kernel and a high-count kernel under completely steady load.
        // RAM recovery/backoff timers also remain in force.
        self.reset(now, 0);
        self.reference_hps = None;
    }

    fn tick(
        &mut self,
        now: u64,
        hashes: u64,
        ready: bool,
        cap: usize,
        allocation_failed: bool,
        actual: usize,
    ) -> Option<Decision> {
        let cap = cap.min(self.max);
        self.observe_cap(now, cap);
        if allocation_failed || actual < self.current {
            self.reset(now, RETRY_SECS);
            self.allocation_retry_at = now.saturating_add(RETRY_SECS);
            return Some(self.change(actual.min(cap), "allocation limited; retry later"));
        }
        if self.current > cap {
            self.reset(now, RECOVERY_SECS);
            return Some(self.change(cap, "releasing RAM"));
        }
        let comparison_max = match self.phase {
            Phase::Baseline => self.current,
            Phase::Probe { base, .. } => base.max(self.current),
            Phase::Confirm {
                base, candidate, ..
            } => base.max(candidate),
        };
        if comparison_max > cap {
            self.reset(now, RECOVERY_SECS);
            return Some(self.change(
                self.current.min(cap),
                "RAM limit changed; discarding comparison",
            ));
        }
        if self.current == 0 {
            if cap > 0
                && now >= self.next_trial
                && now >= self.allocation_retry_at
                && now.saturating_sub(self.headroom_since) >= RECOVERY_SECS
            {
                self.reset(now, 0);
                return Some(self.change(1, "RAM available; resuming"));
            }
            return None;
        }
        let floor = self.min.min(cap);
        if self.current < floor
            && now >= self.next_trial
            && now >= self.allocation_retry_at
            && now.saturating_sub(self.headroom_since) >= RECOVERY_SECS
        {
            self.reset(now, 0);
            return Some(self.change(floor, "restoring configured minimum after RAM recovery"));
        }
        if !ready {
            if self.sampling {
                // Brief tip changes are part of effective mining throughput.
                // Keep their wall time; a sustained disconnect invalidates the
                // comparison instead of keeping an old baseline indefinitely.
                let idle = self.idle_since.get_or_insert(now);
                if now.saturating_sub(*idle) < 3 {
                    return None;
                }
                let base = match self.phase {
                    Phase::Probe { base, .. } | Phase::Confirm { base, .. } => Some(base),
                    Phase::Baseline => None,
                };
                if let Some(base) = base {
                    self.reset(now, RECOVERY_SECS);
                    return Some(
                        self.change(base.min(cap), "work interrupted; discarding comparison"),
                    );
                }
            }
            self.sampling = false;
            // Interruptions invalidate an A/B/A comparison, but allow a new
            // worker to finish warming before its probe window begins.
            return None;
        }
        self.idle_since = None;
        if !self.sampling {
            self.window_start = now;
            self.window_hashes = hashes;
            self.sampling = true;
            return None;
        }
        let elapsed = now.saturating_sub(self.window_start);
        let completed = hashes.saturating_sub(self.window_hashes);
        if elapsed < self.window_secs || completed < CPU_AUTOTUNE_TARGET_HASHES_PER_CANDIDATE {
            return None;
        }
        let hps = completed as f64 / elapsed.max(1) as f64;
        let sample = CpuAutotuneMeasurement {
            hashes: completed,
            elapsed_secs: elapsed as f64,
            wall_secs: elapsed as f64,
            hps,
        };
        self.sampling = false;
        match self.phase {
            Phase::Baseline => {
                if let Some(old) = self.reference_hps {
                    let ratio = hps / old;
                    if (ratio - 1.0).abs() > MAX_DRIFT {
                        self.next_trial = now;
                        self.up = true;
                        self.ramp = true;
                        self.peak_hps = hps;
                    } else {
                        // Track gradual clock/load drift at the same operating
                        // count without compounding the profile's tolerance.
                        self.peak_hps *= ratio;
                    }
                }
                self.peak_hps = self.peak_hps.max(hps);
                self.reference_hps = Some(hps);
                if now < self.next_trial || now < self.allocation_retry_at {
                    return None;
                }
                let floor = self.min.min(cap);
                // Mostly local probes, with occasional wider probes to escape
                // a plateau. All probes still hash useful pool/daemon work.
                let step = if (self.ramp && self.up) || self.attempts % 6 == 5 {
                    (self.current / 2).max(1)
                } else {
                    1
                };
                let can_grow = now.saturating_sub(self.headroom_since) >= RECOVERY_SECS;
                let farther_probe = self
                    .next_up_probe
                    .take()
                    .filter(|&n| can_grow && n > self.current && n <= cap);
                let candidate = if let Some(joint) = self.joint.as_ref().filter(|j| j.repeat) {
                    joint.candidate
                } else if let Some(threads) = farther_probe {
                    threads
                } else if self.up && can_grow && self.current < cap {
                    self.current.saturating_add(step).min(cap)
                } else if !self.up && self.current > floor {
                    self.current.saturating_sub(step).max(floor)
                } else if can_grow && self.current < cap {
                    self.current + 1
                } else if self.current > floor {
                    self.current - 1
                } else {
                    self.next_trial = now + RETRY_SECS;
                    return Some(Decision {
                        threads: self.current,
                        reason: "only feasible count; continuing to watch resources",
                        learned: Some(vec![(self.current, sample)]),
                    });
                };
                self.attempts += 1;
                self.phase = Phase::Probe {
                    base: self.current,
                    before: sample,
                };
                Some(self.change(candidate, "measuring useful mining work"))
            }
            Phase::Probe { base, before } => {
                self.phase = Phase::Confirm {
                    base,
                    before,
                    candidate: self.current,
                    candidate_sample: sample,
                };
                Some(self.change(base.min(cap), "checking baseline again"))
            }
            Phase::Confirm {
                base,
                before,
                candidate,
                candidate_sample,
            } => {
                let stable = (hps / before.hps - 1.0).abs() <= MAX_DRIFT;
                let baseline = merge_cpu_autotune_measurements(before, sample);
                let previous_peak = self.peak_hps;
                if stable {
                    self.peak_hps = self.peak_hps.max(baseline.hps).max(candidate_sample.hps);
                } else {
                    self.peak_hps = hps;
                }
                let quantization = 1.0 / before.elapsed_secs
                    + 1.0 / sample.elapsed_secs
                    + 1.0 / candidate_sample.elapsed_secs;
                let better = stable
                    && candidate <= cap
                    && if candidate < base {
                        candidate_sample.hps
                            >= self.peak_hps * cpu_autotune_peak_floor_frac(self.profile)
                    } else {
                        candidate_sample.hps > baseline.hps * 1.02 + quantization
                            // Discover the peak initially, then retain fewer
                            // workers when they already satisfy the profile.
                            // Probes still update the peak before this choice.
                            && (self.ramp
                                || baseline.hps
                                    < self.peak_hps * cpu_autotune_peak_floor_frac(self.profile))
                    };
                // A new peak can justify probing farther even if this candidate
                // does not justify a larger operating count. This discovers
                // recovered capacity when load changes leave the baseline's
                // own rate unchanged, without recommitting known higher counts.
                let next_up_probe = (stable
                    && !better
                    && candidate > base
                    && candidate < cap
                    && candidate_sample.hps > previous_peak * 1.02 + quantization)
                    .then(|| candidate + 1);
                self.up = if !stable {
                    true
                } else if better {
                    candidate > base
                } else {
                    candidate < base
                };
                if better || next_up_probe.is_some() {
                    self.unsuccessful = 0;
                } else {
                    self.unsuccessful += 1;
                    self.ramp = !stable;
                }
                self.reset(
                    now,
                    if self.unsuccessful < 2 {
                        RECOVERY_SECS
                    } else {
                        RETRY_SECS
                    },
                );
                self.next_up_probe = next_up_probe;
                let mut decision = self.change(
                    if better { candidate } else { base.min(cap) },
                    if !stable {
                        "load changed; discarding comparison"
                    } else if better {
                        "confirmed better setting"
                    } else {
                        "keeping confirmed setting"
                    },
                );
                self.reference_hps = Some(if better { candidate_sample.hps } else { hps });
                if stable {
                    decision.learned = Some(vec![(base, baseline), (candidate, candidate_sample)]);
                }
                Some(decision)
            }
        }
    }
}

pub(super) struct Controller {
    stop: Sender<()>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct MemoryPressure {
    #[cfg(target_os = "linux")]
    previous: Option<(Instant, u64, u64)>,
}

#[derive(Default)]
struct PressureBackoff {
    consecutive: u8,
}

impl PressureBackoff {
    fn cap(&mut self, pressure: bool, available: usize, current: usize, max: usize) -> usize {
        // First-touch/reclaim can briefly stall while ample clean cache remains
        // available. Require both sustained stalls and tight arena headroom.
        self.consecutive = if pressure && available <= current {
            self.consecutive.saturating_add(1)
        } else {
            0
        };
        if self.consecutive >= 3 {
            available.min(max).min(current.saturating_sub(1))
        } else {
            available.min(max)
        }
    }
}

impl MemoryPressure {
    fn elevated(&mut self) -> bool {
        #[cfg(target_os = "linux")]
        {
            let counters = read_pressure_totals(
                crate::config::linux_cgroup_v2_path().as_deref(),
                std::path::Path::new("/proc/pressure/memory"),
            );
            let Some((some, full)) = counters else {
                self.previous = None;
                return false;
            };
            let now = Instant::now();
            let previous = self.previous.replace((now, some, full));
            previous.is_some_and(|(at, old_some, old_full)| {
                let micros = now.saturating_duration_since(at).as_micros() as u64;
                micros > 0
                    && (some.saturating_sub(old_some) > micros / 20
                        || full.saturating_sub(old_full) > micros / 100)
            })
        }
        #[cfg(target_os = "windows")]
        unsafe {
            use windows_sys::Win32::Foundation::CloseHandle;
            use windows_sys::Win32::System::Memory::{
                CreateMemoryResourceNotification, LowMemoryResourceNotification,
                QueryMemoryResourceNotification,
            };
            let handle = CreateMemoryResourceNotification(LowMemoryResourceNotification);
            if handle.is_null() {
                return false;
            }
            let mut low = 0;
            let ok = QueryMemoryResourceNotification(handle, &mut low);
            CloseHandle(handle);
            ok != 0 && low != 0
        }
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        {
            false
        }
    }
}

#[cfg(any(target_os = "linux", test))]
fn read_pressure_totals(
    cgroup: Option<&std::path::Path>,
    system: &std::path::Path,
) -> Option<(u64, u64)> {
    // A small cgroup budget can coexist with abundant host RAM. System-wide
    // stalls from unrelated tasks must not force this healthy group to shrink.
    cgroup
        .and_then(|dir| fs::read_to_string(dir.join("memory.pressure")).ok())
        .and_then(|raw| pressure_totals(&raw))
        .or_else(|| {
            fs::read_to_string(system)
                .ok()
                .and_then(|raw| pressure_totals(&raw))
        })
}

#[cfg(any(target_os = "linux", test))]
fn pressure_totals(raw: &str) -> Option<(u64, u64)> {
    let total = |kind| {
        raw.lines()
            .find(|line| line.split_whitespace().next() == Some(kind))?
            .split_whitespace()
            .find_map(|word| word.strip_prefix("total="))?
            .parse()
            .ok()
    };
    Some((total("some")?, total("full")?))
}

impl Controller {
    pub(super) fn start(
        cfg: Config,
        backends: Vec<Arc<CpuBackend>>,
        tui: Option<TuiState>,
    ) -> Result<Self> {
        let (stop, rx) = bounded(1);
        let handle = if backends.is_empty() {
            None
        } else {
            Some(
                thread::Builder::new()
                    .name("cpu-autotune".into())
                    .spawn(move || {
                        let (min, max, available) = limits(&cfg, 0);
                        let slots = backends.iter().filter(|b| b.adaptive_snapshot().running)
                            .map(|b| b.worker_capacity()).min().unwrap_or(0);
                        let current = backends.iter().filter(|b| b.adaptive_snapshot().running)
                            .map(|b| b.enabled_lanes()).min().unwrap_or(0);
                        let mut policy = Policy::new(&cfg, current, min, max);
                        policy.observe_cap(0, available.min(slots));
                        let mut record = read_seed(&cfg);
                        discard_cached_scores(&mut record);
                        let host = cpu_autotune_host_fingerprint();
                        let mut kernels = super::adaptive_kernel::Tuner::new(&cfg, host.clone());
                        let started = Instant::now();
                        let mut failures = 0;
                        let mut pressure = MemoryPressure::default();
                        let mut pressure_backoff = PressureBackoff::default();
                        loop {
                            match rx.recv_timeout(Duration::from_secs(1)) {
                                Ok(()) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                                    break
                                }
                                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                            }
                            let all_snapshots: Vec<_> =
                                backends.iter().map(|b| b.adaptive_snapshot()).collect();
                            // A quarantined backend is never restarted by the tuner.
                            let snapshots: Vec<_> =
                                all_snapshots.iter().filter(|s| s.running).collect();
                            if snapshots.is_empty() {
                                break;
                            }
                            let slots = backends.iter().zip(&all_snapshots)
                                .filter(|(_, snapshot)| snapshot.running)
                                .map(|(backend, _)| backend.worker_capacity()).min().unwrap_or(0);
                            let actual = snapshots.iter().map(|s| s.threads).min().unwrap_or(0);
                            let owned = snapshots.iter().map(|s| s.committed_bytes).sum();
                            let count_failures =
                                snapshots.iter().map(|s| s.allocation_failures).sum();
                            let (_, available) = cpu_autotune_capacity(&cfg, owned);
                            let cap =
                                pressure_backoff.cap(pressure.elevated(), available.min(slots), actual, max);
                            let now = started.elapsed().as_secs();
                            let (action, decision) = policy.tick_joint(
                                &mut kernels, now, &all_snapshots, cap, count_failures > failures,
                            );
                            if let Some(kernel) = action.kernel {
                                for (backend, snapshot) in backends.iter().zip(&all_snapshots) {
                                    if snapshot.running {
                                        if let Err(err) = backend.set_active_kernel(kernel) {
                                            warn("MINER", format!("cpu-autotune | kernel switch failed: {err:#}"));
                                        }
                                    }
                                }
                            }
                            if let Some(message) = action.message {
                                info("MINER", format!("cpu-autotune | {message}"));
                            }
                            if action.reset_scores {
                                discard_cached_scores(&mut record);
                            }
                            if let Some(decision) = decision {
                                for (backend, snapshot) in backends.iter().zip(&all_snapshots) {
                                    if snapshot.running {
                                        backend.set_active_threads(decision.threads);
                                    }
                                }
                                info(
                                    "MINER",
                                    format!(
                                        "cpu-autotune | {}{} threads per CPU backend: {} (available={available}, effective cap={cap})",
                                        if decision.learned.is_some() {
                                            "done — "
                                        } else {
                                            ""
                                        },
                                        decision.threads,
                                        decision.reason
                                    ),
                                );
                                if let Some(values) = decision
                                    .learned
                                    .filter(|_| snapshots.len() == backends.len())
                                {
                                    let result = save_observation(
                                        &cfg,
                                        &host,
                                        record.as_ref(),
                                        decision.threads,
                                        cap,
                                        max,
                                        &values,
                                    );
                                    match result {
                                        Ok(saved) => record = Some(saved),
                                        Err(err) => warn(
                                            "MINER",
                                            format!(
                                                "cpu-autotune | could not save progress: {err:#}"
                                            ),
                                        ),
                                    }
                                }
                            }
                            failures = count_failures;
                            if let Some(tui) = &tui {
                                if let Ok(mut state) = tui.lock() {
                                    state.threads = policy.current * snapshots.len();
                                    if let Some(control) = &mut state.thread_control {
                                        control.current_threads = policy.current;
                                        control.max_threads = cap.max(policy.current);
                                    }
                                }
                            }
                        }
                    })?,
            )
        };
        Ok(Self { stop, handle })
    }

    pub(super) fn stop(&mut self) {
        let _ = self.stop.try_send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Controller {
    fn drop(&mut self) {
        self.stop();
    }
}

fn discard_cached_scores(record: &mut Option<CpuAutotuneRecord>) {
    if let Some(record) = record {
        // A temporarily RAM-limited run must retain the higher count as a seed,
        // even though measurements from another path cannot be reused.
        record.measured_hps = 0.0;
        record.measurements.clear();
        record.mining_measurement = false;
    }
}

fn save_observation(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    previous: Option<&CpuAutotuneRecord>,
    selected: usize,
    available: usize,
    max: usize,
    values: &[(usize, CpuAutotuneMeasurement)],
) -> Result<CpuAutotuneRecord> {
    let memory_limited = available < max;
    let retained = previous
        .filter(|r| memory_limited && r.selected_threads > available && r.selected_threads <= max);
    let selected_threads = retained.map_or(selected, |r| r.selected_threads);
    let divisor = instances(cfg).max(1) as f64;
    // A retained kernel winner is a count hint, not a measured mining score.
    // Zero denotes an as-yet unmeasured selected count in this mining record.
    let measured_hps = retained
        .filter(|r| r.mining_measurement)
        .map(|r| r.measured_hps)
        .unwrap_or_else(|| {
            values
                .iter()
                .find(|(n, _)| *n == selected_threads)
                .map_or(0.0, |(_, sample)| sample.hps / divisor)
        });
    let mut measurements = previous
        .filter(|r| r.mining_measurement)
        .map(|r| r.measurements.clone())
        .unwrap_or_default();
    for &(n, sample) in values {
        measurements.insert(n, sample);
    }
    let record = CpuAutotuneRecord {
        schema_version: CPU_AUTOTUNE_RECORD_SCHEMA_VERSION,
        profile: cpu_profile_label(cfg.cpu_profile).into(),
        affinity: cpu_affinity_label(cfg.cpu_affinity).into(),
        page_mode: cfg.cpu_page_mode.as_str().into(),
        cpu_instances: instances(cfg),
        min_threads: cfg.cpu_autotune_min_threads,
        max_threads: max,
        selected_threads,
        measured_hps,
        autotune_secs: cfg.cpu_autotune_secs,
        host_fingerprint: host.clone(),
        timestamp_unix_secs: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        measurements,
        memory_limited,
        mining_measurement: true,
    };
    if let Some(parent) = cfg
        .cpu_autotune_config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let temp = cfg
        .cpu_autotune_config_path
        .with_extension(format!("{}.tmp", std::process::id()));
    fs::write(&temp, serde_json::to_vec_pretty(&record)?)?;
    if let Err(err) = fs::rename(&temp, &cfg.cpu_autotune_config_path) {
        let _ = fs::remove_file(&temp);
        return Err(err.into());
    }
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preparation_keeps_ram_manual_limit_separate_and_rejects_exhausted_pid_budget() {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_config_path =
            std::env::temp_dir().join(format!("seine-no-seed-{}", std::process::id()));
        let reserve = startup_task_reserve(&cfg);
        prepare_with_resources(&mut cfg, 16, 1, 1, Some(reserve + 4)).unwrap();
        assert_eq!(cfg.cpu_autotune_worker_slots, Some(4));
        assert_eq!(cfg.cpu_auto_threads_cap, 1);
        assert_eq!(cfg.threads, 1);
        assert_eq!(cfg.backend_specs[0].cpu_threads, Some(1));
        assert!(prepare_with_resources(&mut cfg, 16, 1, 1, Some(reserve)).is_err());
        assert_eq!(cfg.cpu_autotune_worker_slots, Some(4), "failure is atomic");
        prepare_with_resources(&mut cfg, 16, 16, 16, None).unwrap();
        assert_eq!(
            cfg.cpu_autotune_worker_slots,
            Some(16),
            "new session can reprovision"
        );
        assert_eq!(cfg.cpu_auto_threads_cap, 16);
        assert_eq!(cfg.threads, 8);
    }

    #[test]
    #[ignore = "requires an owned task-limited cgroup and a real 2 GiB arena; run optimized"]
    fn pid_budget_startup_probe() {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_threads = true;
        cfg.cpu_page_mode = CpuPageMode::Regular;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        let (memory_capacity, available) = cpu_autotune_capacity(&cfg, 0);
        assert_eq!(available, 1, "fixture must allow exactly one active arena");
        let legacy = std::env::var_os("SEINE_PID_PROBE_LEGACY").is_some();
        let capacity = if legacy {
            memory_capacity
        } else {
            cpu_autotune_worker_ceiling(&cfg)
        };
        // Deliberately bypass PID preflight: this isolates the race/OS-limit
        // fallback. Full mining needs more tasks than this backend-only fixture.
        let backend = CpuBackend::adaptive(
            1,
            capacity,
            cfg.cpu_affinity,
            CpuBackendTuning {
                page_mode: CpuPageMode::Regular,
                ..CpuBackendTuning::default()
            },
        );
        println!("PID_PROBE legacy={legacy} seed=1 capacity={capacity} preflight=bypassed");
        let result = backend.start();
        println!(
            "PID_PROBE startup={result:?} provisioned={} stride={}",
            backend.worker_capacity(),
            backend.lanes()
        );
        backend.stop();
        result.expect("bounded one-arena backend should start");
    }

    #[test]
    fn pid_budget_shares_headroom_without_changing_cache_or_manual_bounds() {
        assert_eq!(provisioned_slots(16, 2, Some(24), 8), 8);
        assert_eq!(provisioned_slots(16, 2, Some(9), 8), 0);
        assert_eq!(provisioned_slots(16, 1, Some(4), 8), 0);
        assert_eq!(provisioned_slots(4, 1, Some(24), 8), 4);
        assert_eq!(provisioned_slots(16, 2, None, 8), 16);
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_auto_threads_cap = 1; // RAM-safe manual ceiling
        cfg.cpu_autotune_worker_slots = Some(3);
        assert_eq!(
            worker_bounds(&cfg, 16).1,
            16,
            "PID slots are not the cache maximum"
        );
        let cached = record(&cfg, 12);
        assert_eq!(seed_threads(Some(&cached), 1, 16, 3, 16), 3);
        assert_eq!(cfg.cpu_auto_threads_cap, 1);
        assert_eq!(
            seed_threads(Some(&cached), 1, 16, 16, 16),
            12,
            "next unrestricted run recovers seed"
        );
    }

    #[test]
    fn count_trials_never_exceed_provisioned_slots_even_with_abundant_ram() {
        let mut p = policy(1);
        let mut hashes = 0;
        for now in 0..3000 {
            hashes += p.current as u64 * 10;
            let _ = p.tick(now, hashes, true, 2, false, p.current);
            assert!(p.current <= 2);
        }
        assert_eq!(
            p.max, 16,
            "resource cap does not replace cache/search maximum"
        );
        assert_eq!(p.current, 2);
    }

    #[test]
    fn startup_task_reserve_accounts_for_both_pool_sessions_and_existing_api_pool() {
        let mut cfg = super::super::tests::test_config();
        cfg.mode = MiningMode::Pool;
        cfg.token = None;
        cfg.ui_mode = UiMode::Plain;
        cfg.api_server_enabled = false;
        cfg.service_mode = false;
        assert_eq!(startup_task_reserve(&cfg), 13); // 5 common + 5 pool + 3 DNS
        cfg.api_server_enabled = true;
        assert_eq!(
            startup_task_reserve(&cfg),
            14,
            "API workers already charged in pids.current"
        );
        cfg.token = Some("test".into());
        assert_eq!(startup_task_reserve(&cfg), 16);
        cfg.ui_mode = UiMode::Tui;
        assert_eq!(startup_task_reserve(&cfg), 18);
        cfg.backend_specs.push(cfg.backend_specs[0]);
        assert_eq!(
            startup_task_reserve(&cfg),
            21,
            "per-instance costs deducted before division"
        );
    }

    #[test]
    #[ignore = "runs productive online tuning for up to five minutes with real 2 GiB arenas; run optimized"]
    fn productive_controller_real_smoke() {
        use crate::backend::{NonceChunk, WorkAssignment, WorkTemplate};
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_threads = true;
        cfg.cpu_autotune_max_threads = Some(2);
        cfg.cpu_autotune_secs = 6;
        cfg.cpu_page_mode = CpuPageMode::Regular;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        let dir = std::env::temp_dir().join(format!(
            "seine-live-tune-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        cfg.cpu_autotune_config_path = dir.join("tuning.json");
        let start = Instant::now();
        prepare(&mut cfg).unwrap();
        assert_eq!(
            limits(&cfg, 0).2,
            2,
            "live comparison smoke requires capacity for two 2 GiB arenas plus reserve/overhead"
        );
        let backend = Arc::new(CpuBackend::adaptive(
            cfg.threads,
            2,
            cfg.cpu_affinity,
            CpuBackendTuning {
                page_mode: CpuPageMode::Regular,
                ..CpuBackendTuning::default()
            },
        ));
        backend.start().unwrap();
        let mut controller =
            Controller::start(cfg.clone(), vec![Arc::clone(&backend)], None).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            backend
                .assign_work(WorkAssignment {
                    template: Arc::new(WorkTemplate {
                        work_id: 1,
                        epoch: 1,
                        header_base: Arc::from(vec![0; blocknet_pow_spec::POW_HEADER_BASE_LEN]),
                        target: [0; 32],
                        dynamic_share_target: None,
                        pause_on_solution: false,
                        stop_at: Instant::now() + Duration::from_secs(310),
                    }),
                    nonce_chunk: NonceChunk {
                        start_nonce: 0,
                        nonce_count: 1_000_000,
                    },
                })
                .unwrap();
            while !cfg.cpu_autotune_config_path.exists() {
                assert!(
                    start.elapsed() < Duration::from_secs(300),
                    "controller did not confirm a stable comparison"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
            let record = read_seed(&cfg).expect("persisted mining record is compatible");
            assert!(record.mining_measurement);
            assert_eq!(record.measurements.len(), 2);
            assert!(record
                .measurements
                .values()
                .all(|m| m.hashes >= 20 && m.elapsed_secs >= 30.0));
            let elapsed = start.elapsed().as_secs_f64();
            let completed = backend.adaptive_snapshot().hashes;
            // Model a restart. A second miner correctly sees the first miner's
            // still-resident arenas as unavailable and would clamp its seed.
            controller.stop();
            backend.stop();
            let mut cached = cfg.clone();
            let cached_start = Instant::now();
            prepare(&mut cached).unwrap();
            assert_eq!(cached.threads, record.selected_threads);
            println!("productive_controller_real_smoke elapsed_secs={elapsed:.3} completed_hashes={completed} selected_threads={} cached_prepare_secs={:.4} measurements={:?}", record.selected_threads, cached_start.elapsed().as_secs_f64(), record.measurements);
        }));
        controller.stop();
        backend.stop();
        let _ = fs::remove_dir_all(&dir);
        result.unwrap();
    }

    fn sample(hps: f64) -> CpuAutotuneMeasurement {
        CpuAutotuneMeasurement {
            hashes: (hps * 30.0) as u64,
            elapsed_secs: 30.0,
            wall_secs: 30.0,
            hps,
        }
    }

    fn policy(current: usize) -> Policy {
        Policy::new(&super::super::tests::test_config(), current, 1, 16)
    }

    #[test]
    fn pressure_releases_workers_and_recovery_requires_stable_headroom() {
        let mut p = policy(8);
        let d = p.tick(1, 0, true, 2, false, 8).unwrap();
        assert_eq!(d.threads, 2);
        assert!(d.learned.is_none());
        assert_eq!(p.tick(2, 0, true, 0, false, 2).unwrap().threads, 0);
        assert!(p.tick(3, 0, false, 8, false, 0).is_none());
        assert!(p.tick(32, 0, false, 8, false, 0).is_none());
        assert_eq!(p.tick(33, 0, false, 8, false, 0).unwrap().threads, 1);
    }

    #[test]
    fn allocation_failure_cools_down_even_when_os_reports_headroom() {
        let mut p = policy(4);
        assert_eq!(p.tick(1, 0, false, 16, true, 0).unwrap().threads, 0);
        assert!(p.tick(299, 0, false, 16, false, 0).is_none());
        assert_eq!(p.tick(301, 0, false, 16, false, 0).unwrap().threads, 1);
    }

    #[test]
    fn fluctuating_ram_does_not_shorten_allocation_failure_backoff() {
        for initial in [0, 1] {
            let mut p = policy(4);
            assert_eq!(
                p.tick(1, 0, false, 16, true, initial).unwrap().threads,
                initial
            );
            p.tick(2, 0, false, 2, false, initial);
            p.tick(3, 0, false, 16, false, initial);
            p.min = 4;
            assert!(p.tick(299, 0, false, 16, false, initial).is_none());
            assert_eq!(
                p.tick(301, 0, false, 16, false, initial).unwrap().threads,
                if initial == 0 { 1 } else { 4 }
            );
        }
    }

    #[test]
    fn pressure_backoff_requires_sustained_stalls_and_tight_headroom() {
        let mut p = PressureBackoff::default();
        // A user ceiling of two must not conceal room for ten arenas.
        for _ in 0..10 {
            assert_eq!(p.cap(true, 10, 2, 2), 2);
        }
        for _ in 0..10 {
            assert_eq!(p.cap(true, 3, 2, 8), 3);
        }
        assert_eq!(p.cap(true, 2, 2, 8), 2);
        assert_eq!(p.cap(true, 2, 2, 8), 2);
        assert_eq!(p.cap(false, 2, 2, 8), 2);
        assert_eq!(p.cap(true, 2, 2, 8), 2);
        assert_eq!(p.cap(true, 2, 2, 8), 2);
        assert_eq!(p.cap(true, 2, 2, 8), 1);
        assert_eq!(p.cap(false, 10, 1, 8), 8);
        // The byte budget can shed workers immediately, including all workers.
        assert_eq!(p.cap(false, 1, 8, 16), 1);
        assert_eq!(p.cap(false, 0, 1, 16), 0);
    }

    #[test]
    fn recovered_ram_restores_configured_minimum_before_performance_trials() {
        let mut p = policy(1);
        p.min = 4;
        assert!(p.tick(0, 0, false, 8, false, 1).is_none());
        assert!(p.tick(29, 0, false, 8, false, 1).is_none());
        assert_eq!(p.tick(30, 0, false, 8, false, 1).unwrap().threads, 4);
    }

    #[test]
    fn changed_load_discards_aba_result() {
        let mut p = policy(4);
        p.phase = Phase::Confirm {
            base: 4,
            before: sample(100.0),
            candidate: 6,
            candidate_sample: sample(150.0),
        };
        p.tick(0, 0, true, 16, false, 4);
        let d = p.tick(30, 1800, true, 16, false, 4).unwrap();
        assert_eq!(d.threads, 4);
        assert!(d.learned.is_none());
    }

    #[test]
    fn network_gap_and_new_memory_cap_invalidate_comparisons() {
        for memory_drop in [false, true] {
            let mut p = policy(4);
            p.phase = Phase::Probe {
                base: 8,
                before: sample(100.0),
            };
            p.tick(0, 0, true, 16, false, 4);
            if !memory_drop {
                assert!(p.tick(1, 10, false, 16, false, 4).is_none());
            }
            let d = p
                .tick(
                    4,
                    10,
                    memory_drop,
                    if memory_drop { 6 } else { 16 },
                    false,
                    4,
                )
                .unwrap();
            assert!(d.learned.is_none());
            assert!(matches!(p.phase, Phase::Baseline));
            assert!(d.threads <= if memory_drop { 6 } else { 16 });
        }
    }

    #[test]
    fn productive_trials_find_peak_and_revisit_after_load_changes() {
        let mut p = policy(4);
        let mut hashes = 0;
        let mut learned_high = false;
        let mut learned_low = false;
        let mut recovered = false;
        for now in 0..7200 {
            let peak: usize = if (2400..4800).contains(&now) { 2 } else { 8 };
            let hps = (p.current.min(peak) * 20)
                .saturating_sub(p.current.saturating_sub(peak) * 3)
                .max(1);
            hashes += hps as u64;
            if let Some(d) = p.tick(now, hashes, true, 16, false, p.current) {
                if d.learned.is_some() {
                    learned_high |= now < 2400 && d.threads == 8;
                    learned_low |= (2400..4800).contains(&now) && d.threads == 2;
                    recovered |= now >= 4800 && d.threads == 8;
                }
            }
        }
        assert!(learned_high, "must explore above startup count");
        assert!(learned_low, "must revisit under sustained competing load");
        assert!(
            recovered,
            "a busy interval must not permanently cap throughput"
        );
    }

    #[test]
    fn constrained_start_provisions_stable_slots_and_keeps_user_ceiling() {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_max_threads = None;
        cfg.cpu_autotune_min_threads = 1;
        let (min, max) = worker_bounds(&cfg, 16);
        assert_eq!((min, max), (1, 16));
        assert_eq!(seed_threads(None, min, max, 1, 1), 1);
        assert_eq!(seed_threads(None, min, max, 8, 8), 4);
        cfg.cpu_autotune_max_threads = Some(4);
        let (min, max) = worker_bounds(&cfg, 16);
        let initial = seed_threads(None, min, max, 1, 1);
        let backend =
            CpuBackend::adaptive(initial, max, cfg.cpu_affinity, CpuBackendTuning::default());
        assert_eq!(backend.adaptive_snapshot().threads, 1);
        assert_eq!(backend.adaptive_snapshot().resident_bytes, 0);
        backend.set_active_threads(2);
        assert_eq!(backend.adaptive_snapshot().threads, 2);
        backend.set_active_threads(usize::MAX);
        assert_eq!(backend.adaptive_snapshot().threads, 4);
        assert_eq!(
            backend.adaptive_snapshot().resident_bytes,
            0,
            "parked slots allocate no arenas"
        );
        cfg.cpu_autotune_min_threads = 3;
        assert_eq!(worker_bounds(&cfg, 16), (3, 4));
        assert_eq!(seed_threads(None, 3, 4, 1, 1), 1);
        assert_eq!(seed_threads(None, 3, 4, 4, 4), 3);
    }

    #[test]
    fn recovered_ram_interrupts_constrained_kernel_trial_before_count_growth() {
        use crate::backend::cpu::CpuAdaptiveSnapshot;
        use blocknet_pow_kernel::CpuKernel;
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_config_path = std::env::temp_dir().join(format!(
            "seine-capacity-recovery-{}.json",
            std::process::id()
        ));
        let path = std::path::PathBuf::from(format!(
            "{}.kernels.json",
            cfg.cpu_autotune_config_path.display()
        ));
        let _ = fs::remove_file(&path);
        let mut kernels =
            super::super::adaptive_kernel::Tuner::new(&cfg, CpuAutotuneHostFingerprint::default());
        let mut policy = Policy::new(&cfg, 1, 1, 4);
        let base = CpuKernel::preferred();
        let can_trial = CpuKernel::ALL
            .into_iter()
            .filter(|k| k.is_supported() && !matches!(k.as_str(), "scalar" | "avx512-256"))
            .count()
            >= 2;
        let mut s = CpuAdaptiveSnapshot {
            kernel: base,
            kernel_generation: 1,
            kernel_ready: true,
            backing_bytes: [0, 0, 0, CPU_LANE_MEMORY_BYTES, 0],
            running: true,
            threads: 1,
            committed_bytes: CPU_LANE_MEMORY_BYTES,
            resident_bytes: CPU_LANE_MEMORY_BYTES,
            hashes: 0,
            hashing: true,
            allocation_failures: 0,
            warmed: 1,
        };
        for now in 0..34 {
            s.hashes += 100;
            let (action, _) = policy.tick_joint(&mut kernels, now, &[s], 1, false);
            if let Some(kernel) = action.kernel {
                s.kernel = kernel;
                s.kernel_generation += 1;
            }
        }
        if can_trial {
            assert_ne!(s.kernel, base);
        }
        let (action, _) = policy.tick_joint(&mut kernels, 34, &[s], 4, false);
        assert_eq!(action.hold_counts, can_trial);
        for now in 35..64 {
            s.hashes += 100;
            let (action, _) = policy.tick_joint(&mut kernels, now, &[s], 4, false);
            if let Some(kernel) = action.kernel {
                s.kernel = kernel;
                s.kernel_generation += 1;
            }
        }
        let (action, _) = policy.tick_joint(&mut kernels, 64, &[s], 4, false);
        assert!(!action.hold_counts);
        assert!(!kernels.is_comparing());
        if can_trial {
            assert_eq!(action.kernel, Some(base));
        }
        if let Some(kernel) = action.kernel {
            s.kernel = kernel;
            s.kernel_generation += 1;
        }
        let mut recovered = false;
        for now in 65..155 {
            s.hashes += 100 * policy.current as u64;
            let (_, decision) = policy.tick_joint(&mut kernels, now, &[s], 4, false);
            if let Some(decision) = decision {
                assert!(decision.threads <= 4);
                recovered |= decision.threads > 1;
            }
            s.threads = policy.current;
            s.warmed = policy.current;
            s.resident_bytes = policy.current as u64 * CPU_LANE_MEMORY_BYTES;
            s.committed_bytes = s.resident_bytes;
            s.backing_bytes[3] = s.resident_bytes;
        }
        assert!(
            recovered,
            "startup memory ceiling must not permanently cap workers"
        );
        let _ = fs::remove_file(path);
    }

    fn recovery_fixture(
        label: &str,
    ) -> (
        std::path::PathBuf,
        super::super::adaptive_kernel::Tuner,
        Policy,
        crate::backend::cpu::CpuAdaptiveSnapshot,
    ) {
        use crate::backend::cpu::CpuAdaptiveSnapshot;
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_config_path = std::env::temp_dir().join(format!(
            "seine-recovery-{label}-{}.json",
            std::process::id()
        ));
        let path = std::path::PathBuf::from(format!(
            "{}.kernels.json",
            cfg.cpu_autotune_config_path.display()
        ));
        let _ = fs::remove_file(&path);
        let kernels =
            super::super::adaptive_kernel::Tuner::new(&cfg, CpuAutotuneHostFingerprint::default());
        let policy = Policy::new(&cfg, 1, 1, 4);
        let snapshot = CpuAdaptiveSnapshot {
            kernel: blocknet_pow_kernel::CpuKernel::preferred(),
            kernel_generation: 1,
            kernel_ready: true,
            backing_bytes: [0, 0, 0, CPU_LANE_MEMORY_BYTES, 0],
            running: true,
            threads: 1,
            committed_bytes: CPU_LANE_MEMORY_BYTES,
            resident_bytes: CPU_LANE_MEMORY_BYTES,
            hashes: 0,
            hashing: true,
            allocation_failures: 0,
            warmed: 1,
        };
        (path, kernels, policy, snapshot)
    }

    #[test]
    fn recovery_without_kernel_trial_does_not_start_one_before_count_growth() {
        let (path, mut kernels, mut policy, mut s) = recovery_fixture("idle");
        policy.observe_cap(0, 1);
        let mut recovered = false;
        for now in 1..90 {
            s.hashes += 100;
            let (action, decision) = policy.tick_joint(&mut kernels, now, &[s], 4, false);
            assert!(
                !action.hold_counts,
                "fresh kernel trial delayed pending count recovery"
            );
            assert!(!kernels.is_comparing());
            if let Some(decision) = decision {
                if decision.threads > 1 {
                    assert!(now >= RECOVERY_SECS);
                    recovered = true;
                    break;
                }
            }
        }
        assert!(recovered);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn transient_headroom_does_not_abort_active_kernel_trial() {
        let (path, mut kernels, mut policy, mut s) = recovery_fixture("spike");
        let (start, _) = policy.tick_joint(&mut kernels, 0, &[s], 1, false);
        if !start.hold_counts {
            return;
        }
        for (now, cap) in [(1, 1), (2, 2), (3, 1)] {
            s.hashes += 100;
            let (action, decision) = policy.tick_joint(&mut kernels, now, &[s], cap, false);
            assert!(action.hold_counts);
            assert!(decision.is_none());
            assert!(kernels.is_comparing());
        }
        assert!(!policy.recovery_pending);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn pending_recovery_honors_allocation_cooldown_then_resumes() {
        let (path, mut kernels, mut policy, mut s) = recovery_fixture("allocation");
        policy.allocation_retry_at = 200;
        let mut recovered = false;
        for now in 0..270 {
            s.hashes += 100;
            let cap = if now < 2 { 1 } else { 4 };
            let (action, decision) = policy.tick_joint(&mut kernels, now, &[s], cap, false);
            if let Some(kernel) = action.kernel {
                s.kernel = kernel;
                s.kernel_generation += 1;
            }
            if let Some(decision) = decision {
                if decision.threads > 1 {
                    assert!(now >= 200, "allocation retry cooldown was bypassed");
                    recovered = true;
                    break;
                }
            }
        }
        assert!(recovered);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn joint_tuner_crosses_count_kernel_valley() {
        simulate_crossover(false, false);
    }

    fn joint_fixture(
        name: &str,
        base: usize,
        candidate: usize,
    ) -> (
        Policy,
        super::super::adaptive_kernel::Tuner,
        crate::backend::cpu::CpuAdaptiveSnapshot,
    ) {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_config_path =
            std::env::temp_dir().join(format!("seine-joint-{name}-{}.json", std::process::id()));
        let mut p = Policy::new(&cfg, candidate, 1, 16);
        p.phase = Phase::Probe {
            base,
            before: sample(10.0),
        };
        p.joint = Some(JointComparison {
            base,
            candidate,
            base_kernel: CpuKernel::Avx512,
            candidate_kernel: None,
            base_backing: vec![[0, 0, 0, base as u64 * CPU_LANE_MEMORY_BYTES, 0]],
            candidate_backing: None,
            started: 0,
            confirmed_peak: 10.0,
            repeat: false,
        });
        let kernels =
            super::super::adaptive_kernel::Tuner::new(&cfg, CpuAutotuneHostFingerprint::default());
        let s = crate::backend::cpu::CpuAdaptiveSnapshot {
            kernel: CpuKernel::Avx2,
            kernel_generation: 1,
            kernel_ready: true,
            backing_bytes: [0, 0, 0, candidate as u64 * CPU_LANE_MEMORY_BYTES, 0],
            running: true,
            threads: candidate,
            committed_bytes: candidate as u64 * CPU_LANE_MEMORY_BYTES,
            resident_bytes: candidate as u64 * CPU_LANE_MEMORY_BYTES,
            hashes: 1000,
            hashing: true,
            allocation_failures: 0,
            warmed: candidate,
        };
        (p, kernels, s)
    }

    #[test]
    fn joint_pressure_and_allocation_failure_restore_original_kernel_without_learning() {
        for (base, candidate, cap, failed, actual, expected) in [
            (8, 12, 4, false, 12, 4),
            (12, 8, 10, false, 8, 10),
            (8, 12, 16, true, 3, 3),
            (8, 12, 0, false, 12, 0),
        ] {
            let (mut p, mut kernels, mut s) = joint_fixture("pressure", base, candidate);
            s.threads = actual;
            let (action, decision) = p.tick_joint(&mut kernels, 100, &[s], cap, failed);
            let d = decision.unwrap();
            assert_eq!(action.kernel, Some(CpuKernel::Avx512));
            assert_eq!(d.threads, expected);
            assert!(d.learned.is_none());
            assert!(p.joint.is_none());
            assert!(!action.hold_counts);
            if failed {
                assert_eq!(p.allocation_retry_at, 100 + RETRY_SECS);
            }
        }
    }

    #[test]
    fn joint_baseline_waits_for_its_kernel_ack_and_rejects_changed_backing() {
        let (mut p, mut kernels, mut s) = joint_fixture("ack", 8, 12);
        p.phase = Phase::Confirm {
            base: 8,
            before: sample(10.0),
            candidate: 12,
            candidate_sample: sample(11.0),
        };
        p.current = 8;
        p.restoring = Some((CpuKernel::Avx512, 1));
        s.threads = 8;
        s.warmed = 8;
        s.resident_bytes = 8 * CPU_LANE_MEMORY_BYTES;
        s.committed_bytes = s.resident_bytes;
        s.backing_bytes[3] = s.resident_bytes;
        let (a, d) = p.tick_joint(&mut kernels, 2, &[s], 16, false);
        assert!(a.hold_counts && d.is_none() && !p.sampling);
        s.kernel = CpuKernel::Avx512;
        s.kernel_ready = false;
        assert!(p.tick_joint(&mut kernels, 3, &[s], 16, false).0.hold_counts);
        s.kernel_ready = true;
        assert!(p.tick_joint(&mut kernels, 4, &[s], 16, false).1.is_none());
        assert!(p.sampling);
        s.backing_bytes.swap(0, 3);
        let (_, d) = p.tick_joint(&mut kernels, 5, &[s], 16, false);
        assert!(d.unwrap().learned.is_none());
        assert!(p.joint.is_none());
    }

    #[test]
    fn joint_trial_timeout_and_restart_do_not_retain_partial_confirmation() {
        let (mut p, mut kernels, s) = joint_fixture("timeout", 8, 12);
        p.joint.as_mut().unwrap().repeat = true;
        let (a, d) = p.tick_joint(&mut kernels, JOINT_TIMEOUT_SECS, &[s], 16, false);
        assert_eq!(a.kernel, Some(CpuKernel::Avx512));
        assert_eq!(d.as_ref().unwrap().threads, 8);
        assert!(d.unwrap().learned.is_none());
        assert!(p.joint.is_none());
        let restarted = Policy::new(&super::super::tests::test_config(), 8, 1, 16);
        assert!(restarted.joint.is_none() && restarted.restoring.is_none());
    }

    #[test]
    fn joint_deadline_allows_explicitly_long_count_windows() {
        let (mut p, mut kernels, s) = joint_fixture("long-window", 8, 12);
        p.window_secs = 300;
        let (_, d) = p.tick_joint(&mut kernels, JOINT_TIMEOUT_SECS, &[s], 16, false);
        assert!(p.joint.is_some());
        assert!(d.is_none());
    }

    #[test]
    fn interrupted_candidate_kernel_trial_cancels_outer_comparison() {
        if !CpuKernel::Avx512.is_supported() || !CpuKernel::Avx2.is_supported() {
            return;
        }
        let (mut p, mut kernels, mut s) = joint_fixture("disconnect", 8, 12);
        // The inner trial's baseline is AVX2; the outer baseline is AVX-512.
        assert!(p.tick_joint(&mut kernels, 1, &[s], 16, false).0.hold_counts);
        s.hashing = false;
        assert!(p.tick_joint(&mut kernels, 2, &[s], 16, false).1.is_none());
        let (a, d) = p.tick_joint(&mut kernels, 5, &[s], 16, false);
        assert_eq!(a.kernel, Some(CpuKernel::Avx512));
        assert_eq!(d.as_ref().unwrap().threads, 8);
        assert!(d.unwrap().learned.is_none());
        assert!(p.joint.is_none());
    }

    #[test]
    fn joint_tuner_rejects_a_gain_that_disappears_on_repeat() {
        simulate_crossover(true, false);
    }

    #[test]
    fn joint_restart_reuses_kernel_evidence_but_repeats_count_comparisons() {
        simulate_crossover(false, true);
    }

    fn simulate_crossover(fade_on_repeat: bool, restart_on_repeat: bool) {
        use crate::backend::cpu::CpuAdaptiveSnapshot;
        use blocknet_pow_kernel::CpuKernel;
        if !CpuKernel::Avx512.is_supported() || !CpuKernel::Avx2.is_supported() {
            return;
        }
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_config_path = std::env::temp_dir().join(format!(
            "seine-joint-valley-{fade_on_repeat}-{restart_on_repeat}-{}.json",
            std::process::id()
        ));
        let path = std::path::PathBuf::from(format!(
            "{}.kernels.json",
            cfg.cpu_autotune_config_path.display()
        ));
        let _ = fs::remove_file(&path);
        let mut kernels =
            super::super::adaptive_kernel::Tuner::new(&cfg, CpuAutotuneHostFingerprint::default());
        let mut p = Policy::new(&cfg, 8, 1, 12);
        let mut s = CpuAdaptiveSnapshot {
            kernel: CpuKernel::Avx512,
            kernel_generation: 1,
            kernel_ready: true,
            backing_bytes: [0, 0, 0, 8 * CPU_LANE_MEMORY_BYTES, 0],
            running: true,
            threads: 8,
            committed_bytes: 8 * CPU_LANE_MEMORY_BYTES,
            resident_bytes: 8 * CPU_LANE_MEMORY_BYTES,
            hashes: 0,
            hashing: true,
            allocation_failures: 0,
            warmed: 8,
        };
        let mut accepted = false;
        let mut visited_candidate_kernel = false;
        let mut repeats = 0;
        let mut baseline_returns = 0;
        let mut restarted = false;
        let mut hash_remainder = 0;
        for now in 0..3600 {
            // Hundredths of H/s model the C5 crossover: full512 peaks
            // at eight; twelve loses on full512 but wins with established AVX2.
            if matches!(p.phase, Phase::Confirm { .. }) && p.sampling {
                assert_eq!(
                    s.kernel,
                    CpuKernel::Avx512,
                    "confirm with original baseline kernel"
                );
            }
            let phase_before = p.phase;
            let rate = if fade_on_repeat
                && repeats > 0
                && s.threads == 12
                && s.kernel == CpuKernel::Avx2
            {
                950
            } else if s.kernel == CpuKernel::Avx512 {
                if s.threads <= 8 {
                    s.threads as u64 * 121 + 4
                } else {
                    972 - (s.threads as u64 - 8) * 11
                }
            } else {
                [
                    0, 108, 216, 324, 432, 540, 648, 756, 865, 907, 946, 990, 1023,
                ][s.threads]
            };
            hash_remainder += rate;
            s.hashes += hash_remainder / 100;
            hash_remainder %= 100;
            visited_candidate_kernel |= s.threads == 12 && s.kernel == CpuKernel::Avx2;
            let (action, decision) = p.tick_joint(&mut kernels, now, &[s], 12, false);
            if let Some(kernel) = action.kernel {
                if kernel != s.kernel {
                    s.kernel_generation += 1;
                }
                s.kernel = kernel;
            }
            if matches!(phase_before, Phase::Probe { .. })
                && matches!(p.phase, Phase::Confirm { .. })
            {
                baseline_returns += 1;
            }
            if let Some(d) = decision {
                if d.reason == "repeating joint configuration confirmation" {
                    assert!(
                        d.learned.is_none(),
                        "first joint gain must not reach count cache"
                    );
                    repeats += 1;
                    if restart_on_repeat && !restarted {
                        // A restart can reuse the fully confirmed kernel cache,
                        // but has no partially confirmed count winner to resume.
                        assert!(path.exists());
                        kernels = super::super::adaptive_kernel::Tuner::new(
                            &cfg,
                            CpuAutotuneHostFingerprint::default(),
                        );
                        p = Policy::new(&cfg, 8, 1, 12);
                        s.kernel = CpuKernel::Avx512;
                        s.kernel_generation = 1;
                        s.hashes = 0;
                        hash_remainder = 0;
                        restarted = true;
                    }
                }
                accepted |= d.learned.is_some() && d.threads == 12 && s.kernel == CpuKernel::Avx2;
                if repeats > 0 && d.learned.is_some() {
                    break;
                }
            }
            s.threads = p.current;
            s.warmed = p.current;
            s.resident_bytes = p.current as u64 * CPU_LANE_MEMORY_BYTES;
            s.committed_bytes = s.resident_bytes;
            s.backing_bytes[3] = s.resident_bytes;
        }
        let _ = fs::remove_file(path);
        assert!(
            visited_candidate_kernel,
            "candidate count was rejected without testing its alternate kernel"
        );
        assert_eq!(repeats, 1 + usize::from(restart_on_repeat));
        assert_eq!(
            baseline_returns,
            if restart_on_repeat { 3 } else { 2 },
            "two complete configuration comparisons required"
        );
        assert_eq!(
            accepted, !fade_on_repeat,
            "second comparison controls promotion"
        );
        assert_eq!(p.current, if fade_on_repeat { 8 } else { 12 });
        let expected_peak = if fade_on_repeat { 9.72 } else { 10.23 };
        assert!((p.peak_hps - expected_peak).abs() < 0.04);
        assert_eq!(
            s.kernel,
            if fade_on_repeat {
                CpuKernel::Avx512
            } else {
                CpuKernel::Avx2
            }
        );
    }

    #[test]
    fn joint_tuner_finds_count_dependent_kernel_and_preserves_memory_safety() {
        use crate::backend::cpu::CpuAdaptiveSnapshot;
        use blocknet_pow_kernel::CpuKernel;
        if !CpuKernel::Avx512.is_supported() || !CpuKernel::Avx2.is_supported() {
            return;
        }
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_config_path =
            std::env::temp_dir().join(format!("seine-joint-test-{}.json", std::process::id()));
        let path = std::path::PathBuf::from(format!(
            "{}.kernels.json",
            cfg.cpu_autotune_config_path.display()
        ));
        let _ = fs::remove_file(&path);
        let mut kernels =
            super::super::adaptive_kernel::Tuner::new(&cfg, CpuAutotuneHostFingerprint::default());
        let mut p = Policy::new(&cfg, 1, 1, 4);
        p.profile = CpuPerformanceProfile::Throughput;
        let mut snapshot = CpuAdaptiveSnapshot {
            kernel: CpuKernel::Avx512,
            kernel_generation: 1,
            kernel_ready: true,
            backing_bytes: [0, 0, 0, CPU_LANE_MEMORY_BYTES, 0],
            running: true,
            threads: 1,
            committed_bytes: CPU_LANE_MEMORY_BYTES,
            resident_bytes: CPU_LANE_MEMORY_BYTES,
            hashes: 0,
            hashing: true,
            allocation_failures: 0,
            warmed: 1,
        };
        let mut learned = Vec::new();
        for now in 0..7200 {
            let rates = if snapshot.kernel == CpuKernel::Avx512 {
                [0, 110, 200, 215, 210]
            } else {
                [0, 100, 220, 240, 230]
            };
            snapshot.hashes += rates[p.current];
            let (action, decision) = p.tick_joint(&mut kernels, now, &[snapshot], 4, false);
            if action.hold_counts {
                assert!(decision.is_none());
            }
            if let Some(kernel) = action.kernel {
                if kernel != snapshot.kernel {
                    snapshot.kernel_generation += 1;
                }
                snapshot.kernel = kernel;
            }
            if let Some(decision) = decision {
                if now > 3600 && decision.learned.is_some() {
                    learned.push((decision.threads, snapshot.kernel));
                }
            }
            snapshot.threads = p.current;
            snapshot.warmed = p.current;
            snapshot.resident_bytes = p.current as u64 * CPU_LANE_MEMORY_BYTES;
            snapshot.committed_bytes = snapshot.resident_bytes;
            snapshot.backing_bytes[3] = snapshot.resident_bytes;
        }
        assert!(!learned.is_empty());
        assert!(
            learned
                .iter()
                .all(|&(count, kernel)| count == 3 && kernel == CpuKernel::Avx2),
            "{learned:?}"
        );
        // Force a new backing context so a kernel trial is active when RAM vanishes.
        snapshot.backing_bytes.swap(0, 3);
        let (action, _) = p.tick_joint(&mut kernels, 7201, &[snapshot], 4, false);
        assert!(action.hold_counts);
        let (action, decision) = p.tick_joint(&mut kernels, 7202, &[snapshot], 0, false);
        assert!(!action.hold_counts);
        assert_eq!(decision.unwrap().threads, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn joint_efficiency_does_not_cycle_between_count_specific_kernels() {
        use crate::backend::cpu::CpuAdaptiveSnapshot;
        use blocknet_pow_kernel::CpuKernel;
        if !CpuKernel::Avx512.is_supported() || !CpuKernel::Avx2.is_supported() {
            return;
        }
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_config_path = std::env::temp_dir().join(format!(
            "seine-joint-efficiency-{}.json",
            std::process::id()
        ));
        let path = std::path::PathBuf::from(format!(
            "{}.kernels.json",
            cfg.cpu_autotune_config_path.display()
        ));
        let _ = fs::remove_file(&path);
        let mut kernels =
            super::super::adaptive_kernel::Tuner::new(&cfg, CpuAutotuneHostFingerprint::default());
        let mut p = Policy::new(&cfg, 1, 1, 8);
        p.profile = CpuPerformanceProfile::Efficiency;
        let mut s = CpuAdaptiveSnapshot {
            kernel: CpuKernel::Avx512,
            kernel_generation: 1,
            kernel_ready: true,
            backing_bytes: [0, 0, 0, CPU_LANE_MEMORY_BYTES, 0],
            running: true,
            threads: 1,
            committed_bytes: CPU_LANE_MEMORY_BYTES,
            resident_bytes: CPU_LANE_MEMORY_BYTES,
            hashes: 0,
            hashing: true,
            allocation_failures: 0,
            warmed: 1,
        };
        let mut confirmations = 0;
        for now in 0..14_400 {
            let rates = if s.kernel == CpuKernel::Avx512 {
                [0, 35, 70, 105, 140, 160, 170, 175, 180]
            } else {
                [0, 30, 60, 90, 120, 150, 170, 190, 200]
            };
            s.hashes += rates[p.current];
            let (action, decision) = p.tick_joint(&mut kernels, now, &[s], 8, false);
            if let Some(kernel) = action.kernel {
                if kernel != s.kernel {
                    s.kernel_generation += 1;
                }
                s.kernel = kernel;
            }
            if let Some(d) = decision {
                if now > 7200 && d.learned.is_some() {
                    confirmations += 1;
                    assert_eq!(
                        d.threads, 5,
                        "stable count at {now}s, kernel={}, peak={}",
                        s.kernel, p.peak_hps
                    );
                    assert_eq!(s.kernel, CpuKernel::Avx512);
                }
            }
            s.threads = p.current;
            s.warmed = p.current;
            s.resident_bytes = p.current as u64 * CPU_LANE_MEMORY_BYTES;
            s.committed_bytes = s.resident_bytes;
            s.backing_bytes[3] = s.resident_bytes;
        }
        assert!(confirmations >= 10);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn kernel_score_invalidation_retains_ram_limited_startup_hint() {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_config_path =
            std::env::temp_dir().join(format!("seine-joint-seed-test-{}.json", std::process::id()));
        let mut old = record(&cfg, 8);
        old.mining_measurement = true;
        old.measured_hps = 200.0;
        old.measurements.insert(8, sample(200.0));
        let mut previous = Some(old);
        discard_cached_scores(&mut previous);
        let saved = save_observation(
            &cfg,
            &CpuAutotuneHostFingerprint::default(),
            previous.as_ref(),
            3,
            3,
            8,
            &[(3, sample(100.0))],
        )
        .unwrap();
        assert_eq!(saved.selected_threads, 8);
        assert_eq!(saved.measured_hps, 0.0);
        assert!(!saved.measurements.contains_key(&8));
        assert!(saved.measurements.contains_key(&3));
        let _ = fs::remove_file(cfg.cpu_autotune_config_path);
    }

    #[test]
    fn kernel_change_preserves_joint_profile_frontier_and_memory_backoff() {
        let mut p = policy(4);
        p.reference_hps = Some(100.0);
        p.peak_hps = 110.0;
        p.ramp = false;
        p.next_up_probe = Some(8);
        p.allocation_retry_at = 500;
        p.headroom_since = 123;
        p.kernel_changed(200);
        assert_eq!(p.reference_hps, None);
        assert_eq!(p.peak_hps, 110.0);
        assert!(!p.ramp);
        assert_eq!(p.next_up_probe, None);
        assert_eq!(p.allocation_retry_at, 500);
        assert_eq!(p.headroom_since, 123);
    }

    #[test]
    fn efficiency_keeps_qualifying_count_across_sustained_rechecks() {
        let mut p = policy(4);
        p.max = 8;
        p.profile = CpuPerformanceProfile::Efficiency;
        let rates = [0, 24, 48, 72, 96, 112, 124, 132, 136];
        let mut hashes = 0;
        let mut confirmations = 0;
        for now in 0..14_400 {
            hashes += rates[p.current];
            if let Some(d) = p.tick(now, hashes, true, 8, false, p.current) {
                if now >= 1800 && d.learned.is_some() {
                    confirmations += 1;
                    assert_eq!(d.threads, 5, "steady-state confirmation at {now}s");
                }
            }
        }
        assert!(
            confirmations >= 20,
            "exercise repeated rechecks, not one endpoint"
        );
    }

    #[test]
    fn efficiency_rediscovers_peak_after_cpu_load_changes() {
        let mut p = policy(4);
        p.max = 8;
        p.profile = CpuPerformanceProfile::Efficiency;
        let normal = [0, 24, 48, 72, 96, 112, 124, 132, 136];
        let loaded = [0, 24, 48, 64, 60, 56, 52, 48, 44];
        let mut hashes = 0;
        let mut confirmations = [0; 3];
        for now in 0..14_400 {
            let stage = (now / 4800) as usize;
            hashes += if stage == 1 {
                loaded[p.current]
            } else {
                normal[p.current]
            };
            if let Some(d) = p.tick(now, hashes, true, 8, false, p.current) {
                if now % 4800 >= 1800 && d.learned.is_some() {
                    confirmations[stage] += 1;
                    assert_eq!(
                        d.threads,
                        if stage == 1 { 2 } else { 5 },
                        "stage {stage}, {now}s"
                    );
                }
            }
        }
        assert!(confirmations.iter().all(|&count| count >= 5));
    }

    #[test]
    fn efficiency_rediscovers_peak_after_ram_capacity_returns() {
        let mut p = policy(2);
        p.max = 8;
        p.profile = CpuPerformanceProfile::Efficiency;
        let rates = [0, 24, 48, 72, 96, 112, 124, 132, 136];
        let mut hashes = 0;
        let mut recovered = 0;
        for now in 0..8000 {
            let cap = if now < 4000 { 2 } else { 8 };
            hashes += rates[p.current];
            if let Some(d) = p.tick(now, hashes, true, cap, false, p.current) {
                if now >= 6000 && d.learned.is_some() {
                    recovered += 1;
                    assert_eq!(d.threads, 5, "recovered capacity at {now}s");
                }
            }
        }
        assert!(recovered >= 5);
    }

    #[test]
    fn efficiency_tolerance_does_not_compound_across_downward_steps() {
        let mut p = policy(8);
        p.profile = CpuPerformanceProfile::Efficiency;
        let mut hashes = 0;
        let mut last_confirmed = 8;
        for now in 0..5000 {
            hashes += p.current.min(8) as u64 * 20;
            if let Some(d) = p.tick(now, hashes, true, 16, false, p.current) {
                if d.learned.is_some() {
                    last_confirmed = d.threads;
                }
            }
        }
        assert_eq!(
            last_confirmed, 6,
            "retain 75% of the measured peak, not 75% per step"
        );
    }

    #[test]
    fn pressure_prefers_current_cgroup_and_falls_back_when_unavailable() {
        let dir = std::env::temp_dir().join(format!(
            "seine-pressure-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let system = dir.join("system.pressure");
        let group = dir.join("memory.pressure");
        fs::write(&system, "some total=100000\nfull total=10000\n").unwrap();
        fs::write(&group, "some total=10\nfull total=1\n").unwrap();
        assert_eq!(read_pressure_totals(Some(&dir), &system), Some((10, 1)));
        assert_eq!(read_pressure_totals(None, &system), Some((100000, 10000)));
        fs::write(&group, "unavailable").unwrap();
        assert_eq!(
            read_pressure_totals(Some(&dir), &system),
            Some((100000, 10000))
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn pressure_parser_requires_both_stall_totals() {
        assert_eq!(
            pressure_totals("some avg10=0.10 avg60=0.10 total=1000\nfull avg10=0 total=20\n"),
            Some((1000, 20))
        );
        assert_eq!(pressure_totals("some total=1000\n"), None);
        assert_eq!(pressure_totals("some total=bad\nfull total=20\n"), None);
    }

    #[test]
    fn cached_start_respects_current_ram_and_explicit_bounds() {
        let cfg = super::super::tests::test_config();
        let r = record(&cfg, 12);
        assert_eq!(seed_threads(Some(&r), 1, 16, 16, 16), 12);
        assert_eq!(seed_threads(Some(&r), 1, 16, 3, 16), 3);
        assert_eq!(seed_threads(Some(&r), 1, 16, 0, 16), 0);
        assert_eq!(seed_threads(Some(&r), 1, 6, 16, 6), 6);
        assert_eq!(seed_threads(None, 8, 16, 3, 16), 3);
        assert_eq!(seed_threads(None, 1, 16, 16, 16), 8);
    }

    fn record(cfg: &Config, selected_threads: usize) -> CpuAutotuneRecord {
        CpuAutotuneRecord {
            schema_version: 10,
            profile: cpu_profile_label(cfg.cpu_profile).into(),
            affinity: cpu_affinity_label(cfg.cpu_affinity).into(),
            page_mode: cfg.cpu_page_mode.as_str().into(),
            cpu_instances: instances(cfg),
            min_threads: 1,
            max_threads: 16,
            selected_threads,
            measured_hps: 10.0,
            autotune_secs: 6,
            host_fingerprint: CpuAutotuneHostFingerprint::default(),
            timestamp_unix_secs: 1,
            measurements: BTreeMap::new(),
            memory_limited: false,
            mining_measurement: false,
        }
    }

    #[test]
    fn seed_compatibility_ignores_age_and_transient_bounds_but_keeps_hardware_contract() {
        let mut cfg = super::super::tests::test_config();
        let r = record(&cfg, 12);
        cfg.cpu_auto_threads_cap = 3;
        cfg.cpu_autotune_max_threads = Some(6);
        assert!(compatible(&r, &cfg, &r.host_fingerprint));
        cfg.cpu_page_mode = CpuPageMode::Regular;
        assert!(!compatible(&r, &cfg, &r.host_fingerprint));
        cfg.cpu_page_mode = CpuPageMode::Auto;
        let mut other_host = r.host_fingerprint.clone();
        other_host.affinity_topology = "changed".into();
        assert!(!compatible(&r, &cfg, &other_host));
    }

    #[test]
    fn constrained_progress_keeps_previous_higher_setting_on_disk() {
        let mut cfg = super::super::tests::test_config();
        let path = std::env::temp_dir().join(format!(
            "seine-adaptive-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        cfg.cpu_autotune_config_path = path.clone();
        let previous = record(&cfg, 12);
        let saved = save_observation(
            &cfg,
            &previous.host_fingerprint,
            Some(&previous),
            3,
            4,
            16,
            &[(3, sample(3.0)), (4, sample(2.9))],
        )
        .unwrap();
        let parsed: CpuAutotuneRecord = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        fs::remove_file(&path).unwrap();
        assert_eq!(saved.selected_threads, 12);
        assert_eq!(parsed.selected_threads, 12);
        assert!(parsed.memory_limited);
        assert_eq!(
            parsed.measured_hps, 0.0,
            "do not relabel a kernel score as a mining score"
        );
        assert!(parsed.measurements.contains_key(&3));
        assert_eq!(seed_threads(Some(&parsed), 1, 16, 16, 16), 12);
    }
}
