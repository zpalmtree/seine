//! Bounded CPU tuning evidence. Runtime producers emit once before consuming a
//! trial; the recorder supplies run/trial identity. No logs, paths, or endpoints.
use serde::{Deserialize, Serialize};

pub const MAX_TRIALS: usize = 8;
pub const MAX_SAMPLES: usize = 6;
pub const MAX_MEMBERS: usize = 8;
pub const MAX_TUNING_BYTES: usize = 16 * 1024;
pub const MAX_CURVE_POINTS: usize = 16;
const MAX_UPTIME_MS: u64 = 315_360_000_000;
const MAX_TRIAL_MS: u64 = 86_400_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialKind {
    CpuKernel,
    CpuWorkers,
    CpuJoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialArm {
    Baseline,
    Candidate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialDecision {
    Gain,
    NoConfirmedGain,
    Inconclusive,
    Aborted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialReason {
    Completed,
    InsufficientHashes,
    BaselineDrift,
    ContextChanged,
    ExecutionFailure,
    UserInterrupted,
    MiningIdle,
    TimeBudget,
    UnsupportedCandidate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialQuantization {
    None,
    ThreeWindowHash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TrialAffinity {
    Auto,
    Os,
    Off,
    PcoreOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TrialPageMode {
    Auto,
    Regular,
    Large,
    #[serde(rename = "large-1g")]
    Large1g,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrialMember {
    /// Stable index within this process's CPU backend list, not a hardware ID.
    pub slot: u16,
    pub workers: u32,
    /// Public kernel identifier, never a message or cache path.
    pub kernel: String,
    /// Explicit 2 MiB, explicit 1 GiB, THP, ordinary, heap backing, in MiB.
    pub backing_mib: [u64; 5],
    pub affinity: TrialAffinity,
    pub page_mode: TrialPageMode,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrialResources {
    pub allowed_logical_cpus: Option<u32>,
    pub available_parallelism: Option<u32>,
    pub effective_memory_mib: Option<u64>,
    /// Diagnostic snapshot only: ordinary fluctuations do not split cohorts.
    pub available_memory_mib: Option<u64>,
    /// Hex digest of allowed CPU/NUMA/L3 placement and affinity inputs only.
    /// Never hash a hostname, wallet, command line, path, or entire Config.
    pub placement_fingerprint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrialContext {
    pub members: Vec<TrialMember>,
    pub resources: TrialResources,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrialSample {
    pub arm: TrialArm,
    pub hashes: u64,
    /// Measured counted-hash window, excluding settling and switching.
    pub elapsed_ms: u64,
    pub settling_ms: Option<u64>,
    /// Actual observed context, not the requested candidate.
    pub context: TrialContext,
    /// Settings/resources changed or a sample was missed during this window.
    pub changed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrialPolicy {
    pub revision: u32,
    pub minimum_hashes: u64,
    pub minimum_sample_ms: u64,
    /// Optional shorter window with a higher hash threshold. Both fields must
    /// be present together; each counted window may satisfy either whole rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short_sample_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short_minimum_hashes: Option<u64>,
    pub quantization: TrialQuantization,
    pub maximum_baseline_drift_bps: u32,
    pub required_gain_bps: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrialCost {
    /// Non-overlapping portions of finished_ms - started_ms. Hashing windows
    /// remain productive work; elapsed time is not automatically lost mining.
    pub switching_ms: Option<u64>,
    /// Observed no-job idle only. None means unmeasured, never assumed zero.
    pub idle_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrialOutcome {
    pub kind: TrialKind,
    pub policy: TrialPolicy,
    /// Relative to the miner run, never a wall-clock/user timestamp.
    pub started_ms: u64,
    pub finished_ms: u64,
    pub baseline: Vec<TrialMember>,
    pub candidate: Vec<TrialMember>,
    pub context_before: TrialContext,
    pub context_after: TrialContext,
    /// Ordered A/B/A windows, at most two cycles. Aborts may be incomplete.
    pub samples: Vec<TrialSample>,
    pub decision: TrialDecision,
    pub reason: TrialReason,
    /// None means no active setting survived, not a default baseline win.
    pub applied: Option<TrialArm>,
    pub cost: TrialCost,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedTrial {
    pub trial_id: u64,
    pub outcome: TrialOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TuningReport {
    pub run_id: String,
    pub trials: Vec<RecordedTrial>,
    /// Queue contention, overflow, invalid records, or payload-budget omissions.
    pub dropped_trials: u64,
    /// The last result the search settled on since the previous report (schema 15).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled: Option<SettledTuning>,
}

/// Where the search started from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedSource {
    Estimate,
    Family,
    Measured,
}

/// One setting the search measured.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CurvePoint {
    pub workers: u32,
    pub kernel: String,
    pub hps: f64,
}

/// What the search settled on, and what it took to get there (schema 15).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettledTuning {
    pub workers: u32,
    pub kernel: String,
    pub hps: f64,
    /// From the start of the run to this result.
    pub settled_ms: u64,
    pub comparisons: u32,
    /// Time spent inside comparisons, a subset of `settled_ms`.
    pub comparing_ms: u64,
    pub ram_limited: bool,
    pub seed: SeedSource,
    /// A new build's changes were being tested.
    pub after_update: bool,
    pub curve: Vec<CurvePoint>,
}

fn valid_kernel_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 48
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn valid_hps(hps: f64) -> bool {
    hps.is_finite() && (0.0..=1e9).contains(&hps)
}

impl SettledTuning {
    pub fn validate(&self) -> bool {
        (1..=65536).contains(&self.workers)
            && valid_kernel_name(&self.kernel)
            && valid_hps(self.hps)
            && self.settled_ms <= MAX_UPTIME_MS
            && self.comparing_ms <= self.settled_ms
            && self.comparisons <= 1_000_000
            && self.curve.len() <= MAX_CURVE_POINTS
            && self.curve.iter().all(|point| {
                (1..=65536).contains(&point.workers)
                    && valid_kernel_name(&point.kernel)
                    && valid_hps(point.hps)
            })
    }
}

fn members_valid(members: &[TrialMember]) -> bool {
    !members.is_empty()
        && members.len() <= MAX_MEMBERS
        && members.windows(2).all(|m| m[0].slot < m[1].slot)
        && members.iter().all(|m| {
            m.slot < 128
                && (1..=65536).contains(&m.workers)
                && !m.kernel.is_empty()
                && m.kernel.len() <= 64
                && m.kernel
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && m.backing_mib.iter().all(|n| *n <= 1_073_741_824)
        })
}

impl TrialContext {
    fn validate(&self) -> bool {
        self.validate_members(false)
    }

    fn validate_members(&self, allow_empty: bool) -> bool {
        let r = &self.resources;
        (members_valid(&self.members) || (allow_empty && self.members.is_empty()))
            && r.allowed_logical_cpus
                .is_none_or(|n| (1..=65536).contains(&n))
            && r.available_parallelism
                .is_none_or(|n| (1..=65536).contains(&n))
            && r.effective_memory_mib
                .is_none_or(|n| n > 0 && n <= 1_073_741_824)
            && r.available_memory_mib.is_none_or(|n| n <= 1_073_741_824)
            && r.placement_fingerprint.as_ref().is_none_or(|s| {
                (16..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_hexdigit())
            })
    }
}

impl TrialOutcome {
    pub fn validate(&self) -> bool {
        let duration = self.finished_ms.saturating_sub(self.started_ms);
        let complete = matches!(
            self.decision,
            TrialDecision::Gain | TrialDecision::NoConfirmedGain
        );
        self.finished_ms >= self.started_ms
            && self.finished_ms <= MAX_UPTIME_MS
            && duration <= MAX_TRIAL_MS
            && self.policy.revision > 0
            && self.policy.minimum_hashes <= 1_000_000_000_000_000
            && self.policy.minimum_sample_ms <= MAX_TRIAL_MS
            && match (
                self.policy.short_sample_ms,
                self.policy.short_minimum_hashes,
            ) {
                (None, None) => true,
                (Some(ms), Some(hashes)) => {
                    ms > 0
                        && ms <= self.policy.minimum_sample_ms
                        && hashes > 0
                        && hashes >= self.policy.minimum_hashes
                        && hashes <= 1_000_000_000_000_000
                }
                _ => false,
            }
            && self.policy.maximum_baseline_drift_bps <= 10000
            && self.policy.required_gain_bps <= 10000
            && members_valid(&self.baseline)
            && members_valid(&self.candidate)
            && self.context_before.validate()
            && self
                .context_after
                .validate_members(self.applied.is_none() && self.decision == TrialDecision::Aborted)
            && self.samples.len() <= MAX_SAMPLES
            && (!complete
                || (matches!(self.samples.len(), 3 | 6) && self.reason == TrialReason::Completed))
            && (complete || self.reason != TrialReason::Completed)
            && self.samples.iter().all(|s| {
                s.elapsed_ms > 0
                    && s.elapsed_ms <= duration
                    && s.settling_ms.is_none_or(|ms| ms <= duration)
                    && s.hashes <= 1_000_000_000_000_000
                    && s.context.validate()
            })
            && self.cost.switching_ms.is_none_or(|ms| ms <= duration)
            && self.cost.idle_ms.is_none_or(|ms| ms <= duration)
            && self.samples.iter().fold(
                self.cost
                    .switching_ms
                    .unwrap_or(0)
                    .saturating_add(self.cost.idle_ms.unwrap_or(0)),
                |total, s| {
                    total
                        .saturating_add(s.elapsed_ms)
                        .saturating_add(s.settling_ms.unwrap_or(0))
                },
            ) <= duration
    }
}

impl TuningReport {
    pub fn validate(&self) -> bool {
        crate::valid_id(&self.run_id)
            && self.trials.len() <= MAX_TRIALS
            && self.dropped_trials <= 1_000_000_000_000
            && self.settled.as_ref().is_none_or(SettledTuning::validate)
            && self
                .trials
                .iter()
                .all(|t| t.trial_id > 0 && t.trial_id <= i64::MAX as u64 && t.outcome.validate())
            && self
                .trials
                .windows(2)
                .all(|t| t[0].trial_id < t[1].trial_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> crate::Report {
        serde_json::from_str(include_str!("../tuning-fixture.json")).unwrap()
    }

    #[test]
    fn strict_trial_allowlist_and_schema_boundary() {
        let report = fixture();
        assert!(report.validate());
        let mut old = report.clone();
        old.schema_version = 5;
        assert!(!old.validate());
        old.tuning = None;
        assert!(old.validate());
        let mut missing_clock = report.clone();
        missing_clock.diagnostics = None;
        assert!(!missing_clock.validate());
        let mut future = report.clone();
        future.diagnostics.as_mut().unwrap().uptime_secs = 1;
        assert!(!future.validate());
        for field in ["wallet", "hostname", "log", "command_line", "path"] {
            let mut value = serde_json::to_value(&report).unwrap();
            value["tuning"]["trials"][0]["outcome"][field] = "PRIVATE".into();
            assert!(serde_json::from_value::<crate::Report>(value).is_err());
        }
        for field in ["decision", "reason", "kind"] {
            let mut value = serde_json::to_value(&report).unwrap();
            value["tuning"]["trials"][0]["outcome"][field] = "PRIVATE".into();
            assert!(serde_json::from_value::<crate::Report>(value).is_err());
        }
        let mut invalid = fixture();
        invalid.tuning.as_mut().unwrap().trials[0].outcome.samples[0]
            .context
            .resources
            .placement_fingerprint = Some("/home/private".into());
        assert!(!invalid.validate());
    }

    #[test]
    fn bounded_counts_order_timing_and_context() {
        let report = fixture();
        let tuning = report.tuning.as_ref().unwrap();
        let trial = &tuning.trials[0].outcome;
        for invalid in [0, i64::MAX as u64 + 1] {
            let mut t = tuning.clone();
            t.trials[0].trial_id = invalid;
            assert!(!t.validate());
        }
        let mut t = tuning.clone();
        t.trials.push(t.trials[0].clone());
        assert!(!t.validate());
        let mut t = trial.clone();
        t.samples = vec![t.samples[0].clone(); 7];
        assert!(!t.validate());
        let mut t = trial.clone();
        t.finished_ms = t.started_ms;
        assert!(!t.validate());
        let mut t = trial.clone();
        t.samples[0].elapsed_ms = 0;
        assert!(!t.validate());
        let mut t = trial.clone();
        t.cost.idle_ms = Some(t.finished_ms);
        assert!(!t.validate());
        let mut t = trial.clone();
        t.baseline[0].kernel = "/private/path".into();
        assert!(!t.validate());
        let mut t = trial.clone();
        t.context_before
            .members
            .push(t.context_before.members[0].clone());
        assert!(!t.validate());
    }

    #[test]
    fn incomplete_and_changed_trials_are_retained_without_being_completed() {
        let mut t = fixture().tuning.unwrap().trials.remove(0).outcome;
        t.samples.clear();
        t.decision = TrialDecision::Aborted;
        t.reason = TrialReason::ExecutionFailure;
        t.applied = None;
        t.context_after.members.clear();
        t.cost.idle_ms = None;
        t.cost.switching_ms = None;
        assert!(t.validate());
        t.reason = TrialReason::ContextChanged;
        assert!(t.validate());
        t.decision = TrialDecision::Gain;
        assert!(!t.validate());
        let mut t = fixture().tuning.unwrap().trials.remove(0).outcome;
        t.samples[1].changed = true;
        t.decision = TrialDecision::Inconclusive;
        t.reason = TrialReason::ContextChanged;
        assert!(t.validate()); // Analysis, not ingestion, excludes it from hints.
    }

    #[test]
    fn alternate_window_rule_is_optional_paired_and_bounded() {
        let mut report = fixture();
        let t = &mut report.tuning.as_mut().unwrap().trials[0].outcome;
        assert!(t.policy.short_sample_ms.is_none()); // Original schema-6 fixture.
        assert!(t.validate());
        t.policy.minimum_sample_ms = 30_000;
        t.policy.minimum_hashes = 32;
        t.policy.short_sample_ms = Some(10_000);
        assert!(!t.validate());
        t.policy.short_minimum_hashes = Some(256);
        assert!(t.validate());
        t.policy.short_sample_ms = None;
        assert!(!t.validate());
        t.policy.short_sample_ms = Some(0);
        assert!(!t.validate());
        t.policy.short_sample_ms = Some(30_001);
        assert!(!t.validate());
        t.policy.short_sample_ms = Some(10_000);
        t.policy.short_minimum_hashes = Some(31);
        assert!(!t.validate());
        t.policy.short_minimum_hashes = Some(1_000_000_000_000_001);
        assert!(!t.validate());
    }
}

#[cfg(test)]
mod settled_tests {
    use super::*;

    fn settled() -> SettledTuning {
        SettledTuning {
            workers: 12,
            kernel: "avx2-stream".into(),
            hps: 10.4,
            settled_ms: 400_000,
            comparisons: 5,
            comparing_ms: 300_000,
            ram_limited: false,
            seed: SeedSource::Family,
            after_update: false,
            curve: vec![CurvePoint {
                workers: 12,
                kernel: "avx2-stream".into(),
                hps: 10.4,
            }],
        }
    }

    #[test]
    fn a_settled_result_validates_and_rejects_nonsense() {
        assert!(settled().validate());
        let mut bad = settled();
        bad.hps = f64::NAN;
        assert!(!bad.validate());
        let mut bad = settled();
        bad.workers = 0;
        assert!(!bad.validate());
        let mut bad = settled();
        bad.kernel = "Not A Kernel".into();
        assert!(!bad.validate());
        let mut bad = settled();
        bad.comparing_ms = bad.settled_ms + 1;
        assert!(!bad.validate());
        let mut bad = settled();
        bad.curve = vec![bad.curve[0].clone(); MAX_CURVE_POINTS + 1];
        assert!(!bad.validate());
    }

    #[test]
    fn a_settled_result_is_optional_on_the_wire() {
        let report: TuningReport = serde_json::from_str(
            r#"{"run_id":"11111111111111111111111111111111","trials":[],"dropped_trials":0}"#,
        )
        .unwrap();
        assert!(report.settled.is_none());
        assert!(!serde_json::to_string(&report).unwrap().contains("settled"));
    }
}
