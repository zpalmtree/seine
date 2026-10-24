//! Bounded kernel trials on productive work at baseline and proposed worker counts.
//! Count sampling pauses while a kernel comparison is in progress.
//! Only established paths participate. Experimental kernels remain benchmark-only.
use super::*;
use crate::backend::cpu::CpuAdaptiveSnapshot;
use blocknet_pow_kernel::CpuKernel;

const RECHECK_SECS: u64 = 30 * 60;
const RETRY_SECS: u64 = 5 * 60;
const MAX_TRIAL_SECS: u64 = 10 * 60;
const MAX_DRIFT: f64 = 0.05;

// Each architecture has at most one challenger. ARM starts on the portable
// NEON path; SHA3 is eligible only after runtime feature detection and must
// win two productive-work comparisons before it becomes the cached choice.
fn candidates() -> Vec<CpuKernel> {
    [
        CpuKernel::Avx512,
        CpuKernel::Avx2,
        CpuKernel::Neon,
        CpuKernel::NeonSha3,
    ]
    .into_iter()
    .filter(|k| k.is_supported())
    .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Context {
    // Indices in the controller's stable CPU backend list. Different survivors
    // may have different affinity even with identical worker/backing vectors.
    members: Vec<usize>,
    workers: Vec<usize>,
    backing: Vec<[u64; 5]>,
}

impl Context {
    fn same_workers(&self, other: &Self) -> bool {
        self.members == other.members && self.workers == other.workers
    }

    fn from_snapshots(snapshots: &[CpuAdaptiveSnapshot]) -> Self {
        Self {
            members: snapshots
                .iter()
                .enumerate()
                .filter_map(|(index, s)| s.running.then_some(index))
                .collect(),
            workers: snapshots
                .iter()
                .filter(|s| s.running)
                .map(|s| s.threads)
                .collect(),
            backing: snapshots
                .iter()
                .filter(|s| s.running)
                .map(|s| s.backing_bytes)
                .collect(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Winner {
    context: Context,
    kernel: String,
    timestamp: u64,
    base: String,
    candidate: String,
    samples: Vec<Sample>,
}

#[derive(Serialize, Deserialize)]
struct Cache {
    schema: u32,
    host: CpuAutotuneHostFingerprint,
    configuration: String,
    winners: Vec<Winner>,
}

impl Cache {
    fn compatible(&self, host: &CpuAutotuneHostFingerprint, configuration: &str) -> bool {
        self.schema == 2 && self.host == *host && self.configuration == configuration
    }

    fn winner(&self, context: &Context, now: u64) -> Option<CpuKernel> {
        self.winners.iter().rev().find_map(|winner| {
            let kernel = winner.kernel.parse::<CpuKernel>().ok()?;
            (winner.context == *context
                && winner.timestamp <= now
                && now - winner.timestamp < CPU_AUTOTUNE_RECORD_MAX_AGE_SECS
                && candidates().contains(&kernel)
                && winner.samples.iter().all(|s| s.secs > 0 && s.hashes > 0)
                && if winner.kernel == winner.base {
                    match winner.samples.as_slice() {
                        [a, b, c] => comparison(*a, *b, *c) == Comparison::NoConfirmedGain,
                        [a, b, c, d, e, f] => {
                            improves(*a, *b, *c)
                                && comparison(*d, *e, *f) == Comparison::NoConfirmedGain
                        }
                        _ => false,
                    }
                } else {
                    winner.kernel == winner.candidate
                        && winner.samples.len() == 6
                        && improves(winner.samples[0], winner.samples[1], winner.samples[2])
                        && improves(winner.samples[3], winner.samples[4], winner.samples[5])
                })
            .then_some(kernel)
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct Sample {
    hashes: u64,
    secs: u64,
}

impl Sample {
    fn hps(self) -> f64 {
        self.hashes as f64 / self.secs.max(1) as f64
    }
}

// Two A/B/A comparisons are required; each has a fresh baseline. One favourable
// window cannot promote a path. Baseline drift and hash quantization are gated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Comparison {
    Gain,
    NoConfirmedGain,
    Inconclusive,
}

fn improves(before: Sample, candidate: Sample, after: Sample) -> bool {
    comparison(before, candidate, after) == Comparison::Gain
}

fn comparison(before: Sample, candidate: Sample, after: Sample) -> Comparison {
    if [before, candidate, after]
        .iter()
        .any(|s| s.secs == 0 || s.hashes == 0)
    {
        return Comparison::Inconclusive;
    }
    if (after.hps() / before.hps() - 1.0).abs() > MAX_DRIFT {
        return Comparison::Inconclusive;
    }
    let baseline =
        (before.hashes as f64 + after.hashes as f64) / (before.secs as f64 + after.secs as f64);
    let quantization =
        1.0 / before.secs as f64 + 1.0 / candidate.secs as f64 + 1.0 / after.secs as f64;
    if candidate.hps() > baseline * 1.02 + quantization {
        Comparison::Gain
    } else {
        Comparison::NoConfirmedGain
    }
}

struct Trial {
    context: Context,
    base: CpuKernel,
    candidate: CpuKernel,
    started: u64,
    samples: Vec<Sample>,
    window: Option<(u64, u64, Vec<u64>)>,
    idle_since: Option<u64>,
}

impl Trial {
    fn requested(&self) -> CpuKernel {
        if self.samples.len() % 3 == 1 {
            self.candidate
        } else {
            self.base
        }
    }
}

#[derive(Default)]
pub(super) struct Action {
    pub hold_counts: bool,
    pub kernel: Option<CpuKernel>,
    pub reset_scores: bool,
    /// An outer count/configuration comparison must discard interrupted or
    /// inconclusive kernel trials, even if its original path is restored.
    pub interrupted: bool,
    pub message: Option<String>,
}

pub(super) struct Tuner {
    cache: Cache,
    path: std::path::PathBuf,
    trial: Option<Trial>,
    checked: Option<Context>,
    next_check: u64,
    // Interrupted counts back off independently. Backing changes at the same
    // count must not evade that delay; winner caches still require exact backing.
    retries: Vec<(Context, u64)>,
    window_secs: u64,
    // Cached/final winners must finish switching before thread sampling resumes.
    settling: Option<CpuKernel>,
    settle_started: u64,
}

impl Tuner {
    pub(super) fn new(cfg: &Config, host: CpuAutotuneHostFingerprint) -> Self {
        let configuration = format!(
            "profile={:?};pages={:?};affinity={:?};cpu={:?};batch={};control={};flush={:?}",
            cfg.cpu_profile,
            cfg.cpu_page_mode,
            cfg.cpu_affinity,
            cfg.backend_specs
                .iter()
                .filter(|s| s.kind == BackendKind::Cpu)
                .map(|s| s.cpu_affinity.unwrap_or(cfg.cpu_affinity))
                .collect::<Vec<_>>(),
            cfg.cpu_hash_batch_size,
            cfg.cpu_control_check_interval_hashes,
            cfg.cpu_hash_flush_interval,
        );
        let mut name = cfg.cpu_autotune_config_path.as_os_str().to_owned();
        name.push(".kernels.json");
        let path = std::path::PathBuf::from(name);
        let cache = fs::read(&path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<Cache>(&raw).ok())
            .filter(|record| record.compatible(&host, &configuration))
            .unwrap_or(Cache {
                schema: 2,
                host,
                configuration,
                winners: Vec::new(),
            });
        Self {
            cache,
            path,
            trial: None,
            checked: None,
            next_check: 0,
            retries: Vec::new(),
            window_secs: cfg.cpu_autotune_secs.saturating_mul(5).clamp(30, 60),
            settling: None,
            settle_started: 0,
        }
    }

    fn save(&mut self, trial: Trial, kernel: CpuKernel) {
        let context = trial.context;
        self.cache
            .winners
            .retain(|winner| winner.context != context);
        self.cache.winners.push(Winner {
            context,
            kernel: kernel.to_string(),
            timestamp: unix_secs(),
            base: trial.base.to_string(),
            candidate: trial.candidate.to_string(),
            samples: trial.samples,
        });
        // Bound disk use even if memory backing varies continually.
        if self.cache.winners.len() > 64 {
            self.cache.winners.remove(0);
        }
        let result = (|| -> Result<()> {
            if let Some(parent) = self.path.parent().filter(|p| !p.as_os_str().is_empty()) {
                fs::create_dir_all(parent)?;
            }
            let temp = self
                .path
                .with_extension(format!("{}.tmp", std::process::id()));
            fs::write(&temp, serde_json::to_vec_pretty(&self.cache)?)?;
            if let Err(err) = fs::rename(&temp, &self.path) {
                let _ = fs::remove_file(&temp);
                return Err(err.into());
            }
            Ok(())
        })();
        if let Err(err) = result {
            warn(
                "MINER",
                format!("cpu-autotune | could not save kernel selection: {err:#}"),
            );
        }
    }

    pub(super) fn is_comparing(&self) -> bool {
        self.trial.is_some() || self.settling.is_some()
    }

    pub(super) fn forget_previous_members(&mut self, now: u64) {
        // The caller already aborted/restored the old comparison. Its retry
        // delay must not let a new membership's count probes bypass kernel
        // selection and incorrectly reject a count on its incumbent path.
        debug_assert!(!self.is_comparing());
        self.checked = None;
        self.retries.clear();
        self.next_check = now;
    }

    fn defer_retry(&mut self, now: u64, context: Context) {
        self.retries
            .retain(|(old, until)| *until > now && !old.same_workers(&context));
        self.retries.push((context, now.saturating_add(RETRY_SECS)));
        if self.retries.len() > 64 {
            self.retries.remove(0);
        }
    }

    pub(super) fn retry_pending(&self, now: u64, snapshots: &[CpuAdaptiveSnapshot]) -> bool {
        !self.retries.is_empty()
            && self.context_retry_pending(now, &Context::from_snapshots(snapshots))
    }

    fn context_retry_pending(&self, now: u64, context: &Context) -> bool {
        self.retries
            .iter()
            .any(|(old, until)| now < *until && old.same_workers(context))
    }

    pub(super) fn abort(&mut self, now: u64) -> Action {
        let trial = self.trial.take();
        let context = trial
            .as_ref()
            .map(|trial| trial.context.clone())
            .or_else(|| self.settling.and(self.checked.clone()));
        let base = trial.map(|trial| trial.base);
        if let Some(context) = context {
            self.defer_retry(now, context);
            // Do not let an interrupted new context inherit an older, longer
            // recheck. An idle cancellation has no comparison to reschedule.
            self.next_check = now.saturating_add(RETRY_SECS);
        }
        self.settling = None;
        Action {
            kernel: base,
            reset_scores: false,
            interrupted: true,
            message: base.map(|_| "kernel comparison interrupted; keeping previous path".into()),
            ..Action::default()
        }
    }

    pub(super) fn tick(
        &mut self,
        now: u64,
        snapshots: &[CpuAdaptiveSnapshot],
        ready: bool,
        allow_start: bool,
    ) -> Action {
        let context = Context::from_snapshots(snapshots);
        let retry_pending = self.context_retry_pending(now, &context);
        let active: Vec<_> = snapshots.iter().filter(|s| s.running).copied().collect();
        let snapshots = active.as_slice();
        if snapshots.is_empty() || candidates().len() < 2 {
            return Action::default();
        }
        let hashes: u64 = snapshots.iter().map(|s| s.hashes).sum();
        if let Some(kernel) = self.settling {
            if ready
                && snapshots
                    .iter()
                    .all(|s| s.kernel == kernel && s.kernel_ready)
            {
                self.settling = None;
                return Action::default();
            }
            if now.saturating_sub(self.settle_started) >= MAX_TRIAL_SECS {
                return self.abort(now);
            }
            return Action {
                hold_counts: true,
                ..Action::default()
            };
        }
        if let Some(trial) = &self.trial {
            if context != trial.context || now.saturating_sub(trial.started) >= MAX_TRIAL_SECS {
                return self.abort(now);
            }
        }
        if self.trial.is_none() {
            if retry_pending || !allow_start || !ready || !snapshots.iter().all(|s| s.kernel_ready)
            {
                return Action::default();
            }
            let changed = self.checked.as_ref() != Some(&context);
            if now < self.next_check && !changed {
                return Action::default();
            }
            // When a worker count/backing changes, reuse only that exact context.
            if changed {
                if let Some(kernel) = self.cache.winner(&context, unix_secs()) {
                    self.checked = Some(context);
                    self.next_check = now + RECHECK_SECS;
                    if snapshots.iter().all(|s| s.kernel == kernel) {
                        return Action::default();
                    }
                    self.settling = Some(kernel);
                    self.settle_started = now;
                    return Action {
                        hold_counts: true, kernel: Some(kernel), reset_scores: true, interrupted: false,
                        message: Some(format!("restoring confirmed {kernel} path for this worker count and memory backing")),
                    };
                }
            }
            let base = snapshots[0].kernel;
            if !snapshots.iter().all(|s| s.kernel == base) {
                return Action::default();
            }
            let Some(candidate) = candidates().into_iter().find(|&k| k != base) else {
                return Action::default();
            };
            self.checked = Some(context.clone());
            self.trial = Some(Trial {
                context,
                base,
                candidate,
                started: now,
                samples: Vec::new(),
                window: None,
                idle_since: None,
            });
            return Action {
                hold_counts: true,
                message: Some(format!("comparing {base} and {candidate} while mining")),
                ..Action::default()
            };
        }
        let trial = self.trial.as_mut().unwrap();
        let requested = trial.requested();
        if !ready {
            let idle = trial.idle_since.get_or_insert(now);
            if now.saturating_sub(*idle) >= 3 {
                return self.abort(now);
            }
            return Action {
                hold_counts: true,
                ..Action::default()
            };
        }
        trial.idle_since = None;
        if !snapshots
            .iter()
            .all(|s| s.kernel == requested && s.kernel_ready)
        {
            trial.window = None;
            return Action {
                hold_counts: true,
                ..Action::default()
            };
        }
        let generations: Vec<_> = snapshots.iter().map(|s| s.kernel_generation).collect();
        let Some((start, first, observed_generations)) = &trial.window else {
            trial.window = Some((now, hashes, generations));
            return Action {
                hold_counts: true,
                ..Action::default()
            };
        };
        if observed_generations != &generations || hashes < *first {
            return self.abort(now);
        }
        let sample = Sample {
            hashes: hashes - *first,
            secs: now.saturating_sub(*start),
        };
        if sample.secs < self.window_secs || sample.hashes < 32 {
            return Action {
                hold_counts: true,
                ..Action::default()
            };
        }
        trial.window = None;
        trial.samples.push(sample);
        if trial.samples.len() == 3
            && !improves(trial.samples[0], trial.samples[1], trial.samples[2])
        {
            // Reject an inconclusive/noisy candidate without spending a second cycle.
            let trial = self.trial.take().unwrap();
            let stable = comparison(trial.samples[0], trial.samples[1], trial.samples[2])
                == Comparison::NoConfirmedGain;
            self.next_check = now + if stable { RECHECK_SECS } else { RETRY_SECS };
            if stable {
                let base = trial.base;
                self.save(trial, base);
            } else {
                self.defer_retry(now, trial.context);
            }
            return Action {
                interrupted: !stable,
                message: Some("keeping current kernel; no confirmed gain".into()),
                ..Action::default()
            };
        }
        if trial.samples.len() == 6 {
            let trial = self.trial.take().unwrap();
            let outcome = comparison(trial.samples[3], trial.samples[4], trial.samples[5]);
            let promoted = outcome == Comparison::Gain;
            let conclusive = outcome != Comparison::Inconclusive;
            let kernel = if promoted {
                trial.candidate
            } else {
                trial.base
            };
            if conclusive {
                self.save(trial, kernel);
            } else {
                self.defer_retry(now, trial.context);
            }
            self.next_check = now + if conclusive { RECHECK_SECS } else { RETRY_SECS };
            self.settling = Some(kernel);
            self.settle_started = now;
            return Action {
                hold_counts: true,
                kernel: Some(kernel),
                reset_scores: promoted,
                interrupted: !conclusive,
                message: Some(if promoted {
                    format!("confirmed faster {kernel} path in two comparisons")
                } else if conclusive {
                    format!("keeping {kernel}; gain did not repeat")
                } else {
                    format!("keeping {kernel}; confirmation was noisy, retrying sooner")
                }),
            };
        }
        Action {
            hold_counts: true,
            kernel: Some(trial.requested()),
            ..Action::default()
        }
    }
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(hps: u64) -> Sample {
        Sample {
            hashes: hps * 30,
            secs: 30,
        }
    }

    fn snapshot() -> CpuAdaptiveSnapshot {
        CpuAdaptiveSnapshot {
            kernel: CpuKernel::preferred(),
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
        }
    }

    fn tuner() -> Tuner {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_config_path = std::env::temp_dir().join(format!(
            "seine-kernel-test-{}-{:?}.json",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut t = Tuner::new(&cfg, CpuAutotuneHostFingerprint::default());
        t.cache.winners.clear();
        t
    }

    #[test]
    fn candidates_are_supported_and_experimental_x86_path_stays_manual() {
        let paths = candidates();
        assert!(paths.len() <= 2);
        assert!(paths.iter().all(|path| path.is_supported()));
        assert!(!paths.contains(&CpuKernel::Avx512256));
        assert_eq!(
            paths.contains(&CpuKernel::NeonSha3),
            CpuKernel::NeonSha3.is_supported()
        );
    }

    #[test]
    fn noise_and_low_hash_count_cannot_promote_kernel() {
        assert!(improves(sample(100), sample(110), sample(100)));
        assert!(!improves(sample(100), sample(102), sample(100)));
        assert!(!improves(sample(100), sample(120), sample(110)));
        assert!(!improves(
            Sample {
                hashes: 1,
                secs: 30
            },
            Sample {
                hashes: 2,
                secs: 30
            },
            Sample {
                hashes: 1,
                secs: 30
            }
        ));
    }

    #[test]
    fn two_confirmations_and_switch_acknowledgements_are_required() {
        if candidates().len() < 2 {
            return;
        }
        let mut t = tuner();
        let mut s = snapshot();
        let challenger = candidates().into_iter().find(|&k| k != s.kernel).unwrap();
        let mut now = 0;
        assert!(t.tick(now, &[s], true, true).hold_counts);
        for (index, rate) in [100, 110, 100, 100, 110, 100].into_iter().enumerate() {
            now += 1;
            s.kernel_ready = false;
            assert!(t.tick(now, &[s], true, true).hold_counts);
            assert!(t.trial.as_ref().unwrap().window.is_none());
            s.kernel_ready = true;
            t.tick(now, &[s], true, true);
            now += 30;
            s.hashes += rate * 30;
            let action = t.tick(now, &[s], true, true);
            if let Some(kernel) = action.kernel {
                s.kernel = kernel;
                s.kernel_generation += 1;
            }
            if index < 5 {
                assert!(t.cache.winners.is_empty());
            }
        }
        assert_eq!(s.kernel, challenger);
        assert_eq!(t.cache.winners.len(), 1);
        assert!(!t.tick(now + 1, &[s], true, true).hold_counts);
        assert!(!t.tick(now + 2, &[s], true, true).hold_counts);
        let _ = fs::remove_file(&t.path);
    }

    #[test]
    fn noisy_second_confirmation_is_not_cached_as_a_loss() {
        if candidates().len() < 2 {
            return;
        }
        for (rates, conclusive) in [
            ([100, 110, 100, 100, 120, 110], false),
            ([100, 110, 100, 100, 95, 100], true),
        ] {
            let mut t = tuner();
            let mut s = snapshot();
            let base = s.kernel;
            let mut now = 0;
            assert!(t.tick(now, &[s], true, true).hold_counts);
            for rate in rates {
                now += 1;
                t.tick(now, &[s], true, true);
                now += 30;
                s.hashes += rate * 30;
                let action = t.tick(now, &[s], true, true);
                if let Some(kernel) = action.kernel {
                    s.kernel = kernel;
                    s.kernel_generation += 1;
                }
            }
            assert_eq!(s.kernel, base);
            assert_eq!(t.cache.winners.len(), usize::from(conclusive));
            assert_eq!(
                t.next_check,
                now + if conclusive { RECHECK_SECS } else { RETRY_SECS }
            );
            assert!(!t.tick(now + 1, &[s], true, true).hold_counts);
            let _ = fs::remove_file(&t.path);
        }
    }

    #[test]
    fn aborted_count_does_not_delay_a_different_count_or_forget_its_own_backoff() {
        if candidates().len() < 2 {
            return;
        }
        let mut t = tuner();
        let mut first = snapshot();
        first.threads = 9;
        first.warmed = 9;
        first.committed_bytes = 9 * CPU_LANE_MEMORY_BYTES;
        first.resident_bytes = first.committed_bytes;
        first.backing_bytes[3] = first.committed_bytes;
        assert!(t.tick(0, &[first], true, true).hold_counts);
        t.abort(10);
        let mut second = first;
        second.threads = 12;
        second.warmed = 12;
        second.committed_bytes = 12 * CPU_LANE_MEMORY_BYTES;
        second.resident_bytes = second.committed_bytes;
        second.backing_bytes[3] = second.committed_bytes;
        assert!(
            t.tick(11, &[second], true, true).hold_counts,
            "a new count must compare kernels before count selection"
        );
        t.abort(12);
        assert!(!t.tick(13, &[first], true, true).hold_counts);
        // Changing backing at the interrupted count must not evade its backoff.
        first.backing_bytes = [first.resident_bytes, 0, 0, 0, 0];
        assert!(!t.tick(14, &[first], true, true).hold_counts);
        assert!(t.tick(10 + RETRY_SECS, &[first], true, true).hold_counts);
    }

    #[test]
    fn idle_abort_does_not_extend_last_checked_counts_retry() {
        if candidates().len() < 2 {
            return;
        }
        let mut t = tuner();
        let s = snapshot();
        assert!(t.tick(0, &[s], true, true).hold_counts);
        t.abort(1);
        t.abort(100);
        assert!(t.tick(301, &[s], true, true).hold_counts);
    }

    #[test]
    fn inconclusive_count_keeps_backoff_after_another_count_is_visited() {
        if candidates().len() < 2 {
            return;
        }
        let mut t = tuner();
        let mut first = snapshot();
        let mut now = 0;
        assert!(t.tick(now, &[first], true, true).hold_counts);
        for rate in [100, 120, 110] {
            now += 1;
            t.tick(now, &[first], true, true);
            now += 30;
            first.hashes += rate * 30;
            if let Some(kernel) = t.tick(now, &[first], true, true).kernel {
                first.kernel = kernel;
                first.kernel_generation += 1;
            }
        }
        assert!(t.trial.is_none());
        assert!(t.cache.winners.is_empty(), "drift is not a cached loss");
        let retry = now + RETRY_SECS;
        let mut second = first;
        second.threads = 2;
        second.warmed = 2;
        second.committed_bytes *= 2;
        second.resident_bytes *= 2;
        second.backing_bytes[3] *= 2;
        assert!(t.tick(now + 1, &[second], true, true).hold_counts);
        t.abort(now + 2);
        assert!(!t.tick(now + 3, &[first], true, true).hold_counts);
        assert!(t.tick(retry, &[first], true, true).hold_counts);
    }

    #[test]
    fn low_count_timeout_in_new_context_does_not_inherit_old_recheck() {
        if candidates().len() < 2 {
            return;
        }
        let mut t = tuner();
        let mut s = snapshot();
        t.checked = Some(Context::from_snapshots(&[s]));
        t.next_check = RECHECK_SECS;
        s.threads = 2;
        s.warmed = 2;
        s.committed_bytes *= 2;
        s.resident_bytes *= 2;
        s.backing_bytes[3] *= 2;
        assert!(t.tick(1, &[s], true, true).hold_counts);
        t.tick(2, &[s], true, true);
        s.hashes = 31;
        assert!(t.tick(MAX_TRIAL_SECS - 1, &[s], true, true).hold_counts);
        assert!(t.trial.as_ref().unwrap().samples.is_empty());
        let now = MAX_TRIAL_SECS + 1;
        assert_eq!(t.tick(now, &[s], true, true).kernel, Some(s.kernel));
        assert!(t.trial.is_none());
        assert!(t.cache.winners.is_empty());
        assert_eq!(t.next_check, now + RETRY_SECS);
        assert!(!t.tick(now + RETRY_SECS - 1, &[s], true, true).hold_counts);
        assert!(t.tick(now + RETRY_SECS, &[s], true, true).hold_counts);
    }

    #[test]
    fn interrupts_and_changed_backing_restore_baseline_and_back_off() {
        if candidates().len() < 2 {
            return;
        }
        for backing_change in [true, false] {
            let mut t = tuner();
            let mut s = snapshot();
            t.tick(0, &[s], true, true);
            t.tick(1, &[s], true, true);
            s.hashes = 3000;
            let action = t.tick(31, &[s], true, true);
            s.kernel = action.kernel.unwrap();
            if backing_change {
                s.backing_bytes = [CPU_LANE_MEMORY_BYTES, 0, 0, 0, 0];
            } else {
                t.tick(32, &[s], false, true);
            }
            let action = t.tick(35, &[s], backing_change, true);
            assert_eq!(action.kernel, Some(CpuKernel::preferred()));
            assert!(t.trial.is_none());
            assert!(t.cache.winners.is_empty());
            assert!(!t.tick(36, &[s], true, true).hold_counts);
        }
    }

    #[test]
    fn stopped_backend_does_not_block_acknowledgements_or_contribute_hashes() {
        if candidates().len() < 2 {
            return;
        }
        let mut t = tuner();
        let mut s = snapshot();
        let mut stopped = s;
        stopped.running = false;
        stopped.hashing = false;
        stopped.kernel_ready = false;
        stopped.hashes = 1_000_000;
        assert!(t.tick(0, &[stopped, s], true, true).hold_counts);
        assert!(t.tick(1, &[stopped, s], true, true).hold_counts);
        s.hashes = 3000;
        stopped.hashes += 50_000;
        let action = t.tick(31, &[stopped, s], true, true);
        assert!(action.kernel.is_some());
        let trial = t.trial.as_ref().unwrap();
        assert_eq!(trial.context.members, vec![1]);
        assert_eq!(trial.context.workers, vec![1]);
        assert_eq!(trial.samples[0].hashes, 3000);
        assert_eq!(trial.samples[0].secs, 30);
    }

    #[test]
    fn cache_is_scoped_to_members_count_backing_host_build_and_supported_paths() {
        let host = CpuAutotuneHostFingerprint::default();
        let context = Context::from_snapshots(&[snapshot()]);
        let mut cache = Cache {
            schema: 2,
            host: host.clone(),
            configuration: "test".into(),
            winners: vec![Winner {
                context: context.clone(),
                kernel: CpuKernel::preferred().to_string(),
                base: CpuKernel::preferred().to_string(),
                candidate: "avx2".into(),
                samples: vec![sample(100); 3],
                timestamp: 100,
            }],
        };
        assert!(cache.compatible(&host, "test"));
        cache.schema = 1;
        assert!(!cache.compatible(&host, "test"));
        cache.schema = 2;
        let mut other = host.clone();
        other.build_fingerprint = "changed".into();
        assert!(!cache.compatible(&other, "test"));
        other = host.clone();
        other.affinity_topology = "changed".into();
        assert!(!cache.compatible(&other, "test"));
        assert!(!cache.compatible(&host, "other"));
        let mut changed = context.clone();
        changed.members[0] = 1;
        assert!(cache.winner(&changed, 100).is_none());
        changed = context.clone();
        changed.workers[0] += 1;
        assert!(cache.winner(&changed, 100).is_none());
        changed = context.clone();
        changed.backing[0].swap(0, 3);
        assert!(cache.winner(&changed, 100).is_none());
        assert!(cache.winner(&context, 99).is_none());
        assert!(cache
            .winner(&context, 100 + CPU_AUTOTUNE_RECORD_MAX_AGE_SECS)
            .is_none());
        assert_eq!(cache.winner(&context, 100), Some(CpuKernel::preferred()));
        cache.winners[0].samples = [100, 110, 100, 100, 120, 110]
            .into_iter()
            .map(sample)
            .collect();
        assert!(cache.winner(&context, 100).is_none());
        cache.winners[0].samples = [100, 110, 100, 100, 95, 100]
            .into_iter()
            .map(sample)
            .collect();
        assert_eq!(cache.winner(&context, 100), Some(CpuKernel::preferred()));
        cache.winners[0].kernel = "avx512-256".into();
        assert!(cache.winner(&context, 100).is_none());
    }

    #[test]
    fn caller_can_block_new_kernel_probe_and_trial_has_hard_deadline() {
        if candidates().len() < 2 {
            return;
        }
        let mut t = tuner();
        let s = snapshot();
        assert!(!t.tick(0, &[s], true, false).hold_counts);
        assert!(t.trial.is_none());
        assert!(t.tick(1, &[s], true, true).hold_counts);
        let action = t.tick(1 + MAX_TRIAL_SECS, &[s], true, true);
        assert_eq!(action.kernel, Some(CpuKernel::preferred()));
        assert!(t.trial.is_none());
    }
}
