//! Bounded kernel trials on productive work, serialized with worker-count trials.
//! Architecture eligibility permits a trial; only repeated gains select a path.
use super::*;
use crate::backend::cpu::CpuAdaptiveSnapshot;
use blocknet_pow_kernel::CpuKernel;

const RECHECK_SECS: u64 = 30 * 60;
const RETRY_SECS: u64 = 5 * 60;
const MAX_TRIAL_SECS: u64 = 10 * 60;
const MAX_DRIFT: f64 = 0.05;

const CACHE_SCHEMA: u32 = 2;

fn candidates() -> Vec<CpuKernel> {
    candidates_for(
        &crate::runtime_identity::cpu_identity(),
        &CpuKernel::ALL
            .into_iter()
            .filter(|k| k.is_supported())
            .collect::<Vec<_>>(),
    )
}

fn candidates_for(
    identity: &crate::runtime_identity::CpuIdentity,
    supported: &[CpuKernel],
) -> Vec<CpuKernel> {
    let mut paths: Vec<_> = [CpuKernel::Avx512, CpuKernel::Avx2, CpuKernel::Neon]
        .into_iter()
        .filter(|k| supported.contains(k))
        .collect();
    // The repeated high-count win is on Turin (family 1Ah, model 02h).
    // Identity limits automatic experiments; the independent ISA/OS support
    // check remains mandatory. Intel and desktop Zen 5 keep their current set.
    if identity.vendor.as_deref() == Some("AuthenticAMD")
        && identity.family == Some(0x1a)
        && identity.model == Some(0x02)
        && supported.contains(&CpuKernel::Avx512256Stream)
    {
        paths.push(CpuKernel::Avx512256Stream);
    }
    paths
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Context {
    workers: Vec<usize>,
    backing: Vec<[u64; 5]>,
}

impl Context {
    fn from_snapshots(snapshots: &[CpuAdaptiveSnapshot]) -> Self {
        Self {
            workers: snapshots.iter().map(|s| s.threads).collect(),
            backing: snapshots.iter().map(|s| s.backing_bytes).collect(),
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
        self.schema == CACHE_SCHEMA && self.host == *host && self.configuration == configuration
    }

    fn winner(&self, context: &Context, now: u64, paths: &[CpuKernel]) -> Option<CpuKernel> {
        self.winners.iter().rev().find_map(|winner| {
            let kernel = winner.kernel.parse::<CpuKernel>().ok()?;
            (winner.context == *context
                && winner.timestamp <= now
                && now - winner.timestamp < CPU_AUTOTUNE_RECORD_MAX_AGE_SECS
                && paths.contains(&kernel)
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
    pub message: Option<String>,
}

pub(super) struct Tuner {
    cache: Cache,
    path: std::path::PathBuf,
    trial: Option<Trial>,
    checked: Option<Context>,
    next_check: u64,
    retry_at: u64,
    window_secs: u64,
    candidates: Vec<CpuKernel>,
    // Remaining challengers for this count/backing. Cache only a complete sweep.
    pending: Vec<CpuKernel>,
    // Cached/final winners must finish switching before thread sampling resumes.
    settling: Option<CpuKernel>,
    settle_started: u64,
}

impl Tuner {
    pub(super) fn new(cfg: &Config, host: CpuAutotuneHostFingerprint) -> Self {
        let candidates = candidates();
        let configuration = format!(
            "profile={:?};pages={:?};affinity={:?};cpu={:?};batch={};control={};flush={:?};kernels={:?}",
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
            candidates,
        );
        let mut name = cfg.cpu_autotune_config_path.as_os_str().to_owned();
        name.push(".kernels.json");
        let path = std::path::PathBuf::from(name);
        let cache = fs::read(&path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<Cache>(&raw).ok())
            .filter(|record| record.compatible(&host, &configuration))
            .unwrap_or(Cache {
                schema: CACHE_SCHEMA,
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
            retry_at: 0,
            window_secs: cfg.cpu_autotune_secs.saturating_mul(5).clamp(30, 60),
            candidates,
            pending: Vec::new(),
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

    fn finish_comparison(&mut self, trial: Trial, kernel: CpuKernel, now: u64) {
        if self.pending.is_empty() {
            self.save(trial, kernel);
            self.next_check = now + RECHECK_SECS;
        } else {
            // Keep count trials paused until every eligible challenger has had
            // its own A/B/A comparison. A partial sweep must not hide the third
            // path behind a cached winner or a thirty-minute recheck delay.
            self.next_check = now;
        }
    }

    pub(super) fn is_comparing(&self) -> bool {
        self.trial.is_some() || self.settling.is_some()
    }

    pub(super) fn abort(&mut self, now: u64) -> Action {
        let base = self.trial.take().map(|trial| trial.base);
        self.settling = None;
        self.pending.clear();
        self.retry_at = now + RETRY_SECS;
        // A new context can begin before the previous context's recheck date.
        // Its interrupted trial must not inherit that stale, longer deadline.
        self.next_check = self.retry_at;
        Action {
            kernel: base,
            reset_scores: false,
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
        if snapshots.is_empty() || self.candidates.len() < 2 {
            return Action::default();
        }
        let context = Context::from_snapshots(snapshots);
        let hashes: u64 = snapshots.iter().map(|s| s.hashes).sum();
        if let Some(kernel) = self.settling {
            if ready
                && snapshots
                    .iter()
                    .all(|s| s.kernel == kernel && s.kernel_ready)
            {
                self.settling = None;
                return Action {
                    hold_counts: !self.pending.is_empty(),
                    ..Action::default()
                };
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
            if now < self.retry_at
                || !allow_start
                || !ready
                || !snapshots.iter().all(|s| s.kernel_ready)
            {
                return Action::default();
            }
            let changed = self.checked.as_ref() != Some(&context);
            if now < self.next_check && !changed {
                return Action::default();
            }
            // When a worker count/backing changes, reuse only that exact context.
            if changed {
                self.pending.clear();
                if let Some(kernel) = self.cache.winner(&context, unix_secs(), &self.candidates) {
                    self.checked = Some(context);
                    self.next_check = now + RECHECK_SECS;
                    if snapshots.iter().all(|s| s.kernel == kernel) {
                        return Action::default();
                    }
                    self.settling = Some(kernel);
                    self.settle_started = now;
                    return Action {
                        hold_counts: true, kernel: Some(kernel), reset_scores: true,
                        message: Some(format!("restoring confirmed {kernel} path for this worker count and memory backing")),
                    };
                }
            }
            let base = snapshots[0].kernel;
            if !snapshots.iter().all(|s| s.kernel == base) {
                return Action::default();
            }
            if self.pending.is_empty() {
                self.pending = self
                    .candidates
                    .iter()
                    .copied()
                    .filter(|&k| k != base)
                    .collect();
            }
            if self.pending.is_empty() {
                return Action::default();
            }
            let candidate = self.pending.remove(0);
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
            if stable {
                let base = trial.base;
                self.finish_comparison(trial, base, now);
            } else {
                self.pending.clear();
                self.next_check = now + RETRY_SECS;
            }
            return Action {
                hold_counts: !self.pending.is_empty(),
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
                self.finish_comparison(trial, kernel, now);
            } else {
                self.pending.clear();
                self.next_check = now + RETRY_SECS;
            }
            self.settling = Some(kernel);
            self.settle_started = now;
            return Action {
                hold_counts: true,
                kernel: Some(kernel),
                reset_scores: promoted,
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
        t.candidates.retain(|k| *k != CpuKernel::Avx512256Stream);
        t
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
    fn cache_is_scoped_to_count_backing_host_build_and_supported_paths() {
        let host = CpuAutotuneHostFingerprint::default();
        let context = Context::from_snapshots(&[snapshot()]);
        let mut cache = Cache {
            schema: CACHE_SCHEMA,
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
        cache.schema = CACHE_SCHEMA;
        let mut other = host.clone();
        other.build_fingerprint = "changed".into();
        assert!(!cache.compatible(&other, "test"));
        other = host.clone();
        other.affinity_topology = "changed".into();
        assert!(!cache.compatible(&other, "test"));
        assert!(!cache.compatible(&host, "other"));
        let mut changed = context.clone();
        changed.workers[0] += 1;
        assert!(cache.winner(&changed, 100, &candidates()).is_none());
        changed = context.clone();
        changed.backing[0].swap(0, 3);
        assert!(cache.winner(&changed, 100, &candidates()).is_none());
        assert!(cache.winner(&context, 99, &candidates()).is_none());
        assert!(cache
            .winner(
                &context,
                100 + CPU_AUTOTUNE_RECORD_MAX_AGE_SECS,
                &candidates()
            )
            .is_none());
        assert_eq!(
            cache.winner(&context, 100, &candidates()),
            Some(CpuKernel::preferred())
        );
        cache.winners[0].samples = [100, 110, 100, 100, 120, 110]
            .into_iter()
            .map(sample)
            .collect();
        assert!(cache.winner(&context, 100, &candidates()).is_none());
        cache.winners[0].samples = [100, 110, 100, 100, 95, 100]
            .into_iter()
            .map(sample)
            .collect();
        assert_eq!(
            cache.winner(&context, 100, &candidates()),
            Some(CpuKernel::preferred())
        );
        cache.winners[0].kernel = "avx512-256".into();
        assert!(cache.winner(&context, 100, &candidates()).is_none());
    }

    #[test]
    fn count_probe_blocks_kernel_probe_and_trial_has_hard_deadline() {
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
    #[test]
    fn streaming_trials_require_validated_identity_and_supported_instructions() {
        use crate::runtime_identity::CpuIdentity;
        let supported = [
            CpuKernel::Avx512,
            CpuKernel::Avx2,
            CpuKernel::Avx512256Stream,
        ];
        let turin = CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(26),
            model: Some(2),
            stepping: Some(1),
        };
        assert_eq!(candidates_for(&turin, &supported), supported);
        assert_eq!(candidates_for(&turin, &supported[..2]), supported[..2]);
        for identity in [
            CpuIdentity {
                vendor: Some("GenuineIntel".into()),
                ..turin.clone()
            },
            CpuIdentity {
                family: Some(25),
                model: Some(17),
                ..turin.clone()
            },
            CpuIdentity {
                model: Some(68),
                ..turin.clone()
            },
            CpuIdentity::default(),
        ] {
            assert_eq!(candidates_for(&identity, &supported), supported[..2]);
        }
        assert_eq!(
            candidates_for(&turin, &[CpuKernel::Avx2]),
            [CpuKernel::Avx2]
        );
    }

    fn three_path_tuner() -> Tuner {
        let mut t = tuner();
        // Simulate eligible host paths, independent of this test runner's ISA.
        t.candidates = vec![
            CpuKernel::Avx512,
            CpuKernel::Avx2,
            CpuKernel::Avx512256Stream,
        ];
        t
    }

    fn run_comparison(
        t: &mut Tuner,
        s: &mut CpuAdaptiveSnapshot,
        now: &mut u64,
        rates: &[u64],
    ) -> Action {
        *now += 1;
        // A previously promoted path must settle before the next comparison.
        if t.settling.is_some() {
            let action = t.tick(*now, &[*s], true, true);
            assert_eq!(action.hold_counts, !t.pending.is_empty());
            *now += 1;
        }
        assert!(t.tick(*now, &[*s], true, true).hold_counts);
        let mut action = Action::default();
        for rate in rates {
            *now += 1;
            t.tick(*now, &[*s], true, true);
            *now += 30;
            s.hashes += rate * 30;
            action = t.tick(*now, &[*s], true, true);
            if let Some(kernel) = action.kernel {
                s.kernel = kernel;
                s.kernel_generation += 1;
            }
        }
        action
    }

    #[test]
    fn third_path_is_reached_after_a_loss_and_cached_only_after_confirmation() {
        let mut t = three_path_tuner();
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512;
        let mut now = 0;
        let a = run_comparison(&mut t, &mut s, &mut now, &[100, 95, 100]);
        assert!(a.hold_counts);
        assert_eq!(t.pending, [CpuKernel::Avx512256Stream]);
        assert!(t.cache.winners.is_empty());
        run_comparison(&mut t, &mut s, &mut now, &[100, 125, 100, 100, 125, 100]);
        assert_eq!(s.kernel, CpuKernel::Avx512256Stream);
        assert_eq!(t.cache.winners.len(), 1);
        let context = Context::from_snapshots(&[s]);
        assert_eq!(
            t.cache.winner(&context, unix_secs(), &t.candidates),
            Some(s.kernel)
        );
        assert_eq!(t.next_check, now + RECHECK_SECS);
        let _ = fs::remove_file(&t.path);
    }

    #[test]
    fn next_challenger_uses_the_promoted_incumbent_and_keeps_it_if_faster() {
        let mut t = three_path_tuner();
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512;
        let mut now = 0;
        run_comparison(&mut t, &mut s, &mut now, &[100, 110, 100, 100, 110, 100]);
        assert_eq!(s.kernel, CpuKernel::Avx2);
        assert!(t.cache.winners.is_empty());
        run_comparison(&mut t, &mut s, &mut now, &[110, 105, 110]);
        assert_eq!(s.kernel, CpuKernel::Avx2);
        assert_eq!(t.cache.winners.len(), 1);
        assert_eq!(t.cache.winners[0].base, "avx2");
        assert_eq!(t.cache.winners[0].candidate, "avx512-256-stream");
        let _ = fs::remove_file(&t.path);
    }

    #[test]
    fn changed_context_discards_pending_challengers_and_cannot_reuse_streaming_winner() {
        let mut t = three_path_tuner();
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512;
        let mut now = 0;
        run_comparison(&mut t, &mut s, &mut now, &[100, 95, 100]);
        s.backing_bytes.swap(0, 3);
        t.tick(now + 1, &[s], true, true);
        assert_eq!(t.trial.as_ref().unwrap().candidate, CpuKernel::Avx2);
        assert!(t.cache.winners.is_empty());
        t.abort(now + 2);
        assert!(t.pending.is_empty());
    }

    #[test]
    fn a_streaming_winner_can_switch_back_when_the_worker_count_changes() {
        let mut t = three_path_tuner();
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512;
        s.threads = 24;
        s.warmed = 24;
        s.backing_bytes[3] = 24 * CPU_LANE_MEMORY_BYTES;
        s.resident_bytes = s.backing_bytes[3];
        s.committed_bytes = s.resident_bytes;
        let mut now = 0;
        run_comparison(&mut t, &mut s, &mut now, &[100, 95, 100]);
        run_comparison(&mut t, &mut s, &mut now, &[100, 125, 100, 100, 125, 100]);
        assert_eq!(s.kernel, CpuKernel::Avx512256Stream);
        assert_eq!(t.cache.winners.len(), 1);
        s.threads = 1;
        s.warmed = 1;
        s.backing_bytes[3] = CPU_LANE_MEMORY_BYTES;
        s.resident_bytes = s.backing_bytes[3];
        s.committed_bytes = s.resident_bytes;
        run_comparison(&mut t, &mut s, &mut now, &[100, 110, 100, 100, 110, 100]);
        assert_eq!(s.kernel, CpuKernel::Avx512);
        run_comparison(&mut t, &mut s, &mut now, &[110, 105, 110]);
        assert_eq!(s.kernel, CpuKernel::Avx512);
        assert!(t.pending.is_empty());
        let _ = fs::remove_file(&t.path);
    }

    #[test]
    fn noisy_third_path_does_not_cache_a_partial_sweep() {
        let mut t = three_path_tuner();
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512;
        let mut now = 0;
        run_comparison(&mut t, &mut s, &mut now, &[100, 110, 100, 100, 110, 100]);
        run_comparison(&mut t, &mut s, &mut now, &[110, 130, 125]);
        assert_eq!(s.kernel, CpuKernel::Avx2);
        assert!(t.cache.winners.is_empty());
        assert!(t.pending.is_empty());
        assert_eq!(t.next_check, now + RETRY_SECS);
        assert!(!t.tick(now + RETRY_SECS - 1, &[s], true, true).hold_counts);
        assert!(t.tick(now + RETRY_SECS, &[s], true, true).hold_counts);
    }
    #[test]
    #[ignore = "24 real 2 GiB workers with reserved local HugeTLB on validated EPYC; up to 15 minutes"]
    fn epyc_streaming_productive_selection() {
        use crate::backend::cpu::{CpuBackend, CpuBackendTuning};
        use crate::backend::{NonceChunk, PowBackend, WorkAssignment, WorkTemplate};
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        assert!(
            candidates().contains(&CpuKernel::Avx512256Stream),
            "requires validated EPYC identity and ISA"
        );
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_config_path =
            std::env::temp_dir().join(format!("seine-epyc-productive-{}", std::process::id()));
        cfg.cpu_autotune_secs = 6;
        let mut t = Tuner::new(&cfg, cpu_autotune_host_fingerprint());
        t.cache.winners.clear();
        let backend = CpuBackend::adaptive(
            24,
            24,
            crate::config::CpuAffinityMode::Auto,
            CpuBackendTuning {
                page_mode: crate::config::CpuPageMode::Large,
                ..CpuBackendTuning::default()
            },
        );
        backend.start().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let started = Instant::now();
            let budget = Duration::from_secs(900);
            backend
                .assign_work(WorkAssignment {
                    template: Arc::new(WorkTemplate {
                        work_id: 1,
                        epoch: 1,
                        header_base: Arc::from(vec![0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN]),
                        target: [0; 32],
                        dynamic_share_target: None,
                        pause_on_solution: false,
                        stop_at: started + budget,
                    }),
                    nonce_chunk: NonceChunk {
                        start_nonce: 0,
                        nonce_count: 1_000_000,
                    },
                })
                .unwrap();
            loop {
                assert!(
                    started.elapsed() < budget,
                    "automatic streaming comparison did not finish"
                );
                let snapshot = backend.adaptive_snapshot();
                assert_eq!(snapshot.allocation_failures, 0);
                let ready = snapshot.hashing
                    && snapshot.warmed == 24
                    && snapshot.resident_bytes == 24 * CPU_LANE_MEMORY_BYTES;
                let action = t.tick(started.elapsed().as_secs(), &[snapshot], ready, true);
                if let Some(message) = action.message {
                    println!("{message}");
                }
                if let Some(kernel) = action.kernel {
                    backend.set_active_kernel(kernel).unwrap();
                }
                if !t.cache.winners.is_empty() && snapshot.kernel_ready && t.settling.is_none() {
                    assert_eq!(snapshot.kernel, CpuKernel::Avx512256Stream);
                    assert_eq!(snapshot.backing_bytes[0], 24 * CPU_LANE_MEMORY_BYTES);
                    assert!(snapshot.hashes > 0);
                    assert_eq!(
                        t.cache.winner(
                            &Context::from_snapshots(&[snapshot]),
                            unix_secs(),
                            &t.candidates
                        ),
                        Some(snapshot.kernel)
                    );
                    println!(
                        "EPYC_PRODUCTIVE_PASS hashes={} kernel={} workers={} backing={:?}",
                        snapshot.hashes, snapshot.kernel, snapshot.threads, snapshot.backing_bytes
                    );
                    println!(
                        "EPYC_COMPARISON {}",
                        serde_json::to_string(&t.cache.winners[0]).unwrap()
                    );
                    break;
                }
                std::thread::sleep(Duration::from_secs(1));
            }
        }));
        backend.stop();
        let _ = fs::remove_file(&t.path);
        assert_eq!(backend.adaptive_snapshot().resident_bytes, 0);
        result.unwrap();
    }
}
