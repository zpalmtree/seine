//! Thread-count and kernel search on real mining work.
//!
//! Every comparison runs incumbent, candidate, incumbent so drift cancels, and all
//! windows hash useful work. Once settled, nothing runs until something changes.
use super::*;
use crate::backend::cpu::CpuAdaptiveSnapshot;
use blocknet_pow_kernel::CpuKernel;
use std::collections::{BTreeMap, VecDeque};

/// Two settings within this share of each other count as equal; a tie goes to
/// fewer threads.
pub(super) const TIE_BAND: f64 = 0.0025;
/// Above this a first comparison decides without a second cycle.
const CLEAR_GAIN: f64 = 0.02;
/// A candidate running this far behind ends its window early.
const CLEAR_LOSS: f64 = 0.85;
const NOISE_FLOOR: f64 = 0.001;
/// Hashes in flight at a window's two edges, per square root of the worker count.
const UNEVEN_FINISH: f64 = 0.5;
/// A win must clear the band by this many standard errors: more threads cost RAM
/// for as long as the miner runs.
const WIN_SIGMAS: f64 = 2.5;
/// Win margin once the same two settings have been measured again.
const POOLED_WIN_SIGMAS: f64 = 2.0;
/// Standard errors below the estimate that must still sit inside the tie band before
/// a lower count is accepted.
const FEWER_SIGMAS: f64 = 1.5;

const FIRST_WINDOW_SECS: u64 = 15;
const LONGEST_FIRST_WINDOW_SECS: u64 = 60;
const LONGEST_WINDOW_SECS: u64 = 60;
const LONGEST_WINDOW_SECS_SLOW: u64 = 120;
const RETEST_WINDOW_SECS: u64 = 150;
const RETEST_WINDOW_SECS_SLOW: u64 = 300;
const MIN_HASHES: u64 = 64;
const SHORTEST_HASHES: u64 = 16;
const CLEAR_LOSS_HASHES: u64 = 32;
const TRIAL_TIMEOUT_SECS: u64 = 30 * 60;
const SEARCH_BUDGET_SECS: u64 = 30 * 60;
const SEARCH_BUDGET_SECS_SLOW: u64 = 60 * 60;
const SLOW_RATE: f64 = 4.0;

const HEADROOM_HOLD_SECS: u64 = 30;
const CONSTRAINED_SEARCH_WAIT_SECS: u64 = 300;
const ALLOCATION_RETRY_SECS: u64 = 300;
const ALLOCATION_RETRY_MAX_SECS: u64 = 2400;

const WATCH_WINDOW_SECS: u64 = 300;
/// Sustained H/s change that means the machine is not the one we tuned on.
const SHIFT: f64 = 0.07;
const SHIFT_WINDOWS: u8 = 2;
const SHIFT_RETRY_SECS: u64 = 3600;
const SHIFT_RETRY_MAX_SECS: u64 = 24 * 3600;
const RECOVERY_PROBE_SECS: u64 = 600;
const RECOVERY_PROBE_MAX_SECS: u64 = 6 * 3600;
const UNDECIDED_RETRY_SECS: u64 = 30 * 60;
const UNDECIDED_LOOKS: u8 = 5;
const UNDECIDED_PENDING: usize = 3;
const MAX_REQUEUED: u8 = 8;
/// A shift in large-page coverage this small does not change what a window measures.
const BACKING_SHIFT: f64 = 0.1;
/// Across a run: a machine too noisy to resolve small differences stops trying.
const MAX_RETESTS: u32 = 24;
/// Above this standard error a final result is noise, not evidence of a tie.
const CONTAMINATED_ERROR: f64 = 0.01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Setting {
    pub threads: usize,
    pub kernel: CpuKernel,
}

/// How much the starting point can be trusted, which sets how far the search looks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SeedQuality {
    /// Nothing known about this machine beyond what fits in RAM.
    Estimate,
    /// A rule for this CPU family.
    Family,
    /// Field data for this CPU, or this machine's own earlier result.
    Measured,
}

/// What this build changed since the saved result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct BuildFacts {
    /// Hash of everything that affects hashing speed.
    pub hot_fingerprint: String,
    /// Kernel name to revision, raised when a kernel is improved.
    pub kernel_revisions: BTreeMap<String, u32>,
}

/// One remembered kernel result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(super) struct KernelResult {
    pub kernel: String,
    pub revision: u32,
    pub threads: usize,
    /// Rate relative to the kernel it was compared with.
    pub ratio: f64,
}

/// A comparison still too close to call when the result was saved.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(super) struct SavedRecheck {
    pub threads: usize,
    pub kernel: String,
    pub looks: u8,
    pub against_threads: usize,
    pub against_kernel: String,
    pub gain: f64,
    /// Zero when the evidence so far no longer applies.
    pub error: f64,
}

/// The result kept between runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(super) struct Saved {
    pub threads: usize,
    pub kernel: String,
    pub hps: f64,
    /// The RAM limit in force when this was decided, if it held the count back.
    pub ram_limit: Option<usize>,
    pub hot_fingerprint: String,
    pub kernel_revisions: BTreeMap<String, u32>,
    pub kernels: Vec<KernelResult>,
    #[serde(default)]
    pub rechecks: Vec<SavedRecheck>,
}

pub(super) struct Params {
    pub seed: Setting,
    pub quality: SeedQuality,
    pub min: usize,
    pub max: usize,
    /// Share of the peak that a lower count may give up (the profile's floor).
    pub down_band: f64,
    pub candidates: Vec<CpuKernel>,
    pub saved: Option<Saved>,
    pub build: BuildFacts,
}

#[derive(Debug, Default)]
pub(super) struct Output {
    pub threads: Option<usize>,
    pub kernel: Option<CpuKernel>,
    /// One plain sentence for the dashboard log.
    pub notes: Vec<Note>,
    /// Detail for plain logs and the control API.
    pub details: Vec<String>,
    pub events: Vec<TrialEvent>,
    /// A new result to keep.
    pub save: bool,
    pub settled_now: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct Note {
    pub text: String,
    pub success: bool,
}

/// Progress of the running comparison, for telemetry.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum TrialEvent {
    Begin {
        base: Setting,
        candidate: Setting,
    },
    Open,
    Discard,
    Close {
        hashes: u64,
        secs: u64,
    },
    End {
        verdict: Verdict,
        applied_candidate: bool,
    },
    Abort {
        idle: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct SettledSummary {
    pub threads: usize,
    pub kernel: CpuKernel,
    pub hps: f64,
    pub comparisons: u32,
    pub comparing_secs: u64,
    pub ram_limited: bool,
    pub quality: SeedQuality,
    pub after_update: bool,
    pub curve: Vec<(usize, &'static str, f64)>,
}

/// What is being measured right now, which explains a thread count that moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Comparing {
    Threads(usize, usize),
    Kernels,
    Nothing,
}

/// What the status line shows.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Status {
    /// A saved or built-in setting is being checked before it is trusted.
    Checking,
    Tuning {
        threads: usize,
        kernel: CpuKernel,
        done: u32,
        left: u32,
        secs_left: u64,
        testing_update: bool,
        comparing: Comparing,
    },
    /// A settled miner taking another look at a result it could not call.
    Rechecking {
        current: usize,
        candidate: usize,
    },
    Settled {
        threads: usize,
        kernel: CpuKernel,
        hps: f64,
    },
    /// Running below the best count because RAM is short.
    LimitedByRam {
        threads: usize,
        best: usize,
    },
    WaitingForRam,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Verdict {
    Better,
    Worse,
    Tie,
    Undecided,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Sample {
    pub hashes: u64,
    pub secs: u64,
    pub threads: usize,
}

impl Sample {
    fn rate(self) -> f64 {
        self.hashes as f64 / self.secs.max(1) as f64
    }

    /// Relative error from workers finishing at different moments in the window.
    fn error(self) -> f64 {
        UNEVEN_FINISH * (self.threads.max(1) as f64).sqrt() / self.hashes.max(1) as f64
    }
}

/// Gain of the candidate over the incumbent and its standard error, from complete
/// incumbent, candidate, incumbent cycles.
pub(super) fn estimate(windows: &[Sample]) -> (f64, f64) {
    let (mut weighted, mut weights) = (0.0, 0.0);
    let mut gains = Vec::with_capacity(2);
    let mut variances = Vec::with_capacity(2);
    for cycle in windows.chunks_exact(3) {
        let [first, candidate, last] = [cycle[0], cycle[1], cycle[2]];
        let base = (first.hashes + last.hashes) as f64 / (first.secs + last.secs).max(1) as f64;
        if base <= 0.0 {
            continue;
        }
        let gain = candidate.rate() / base - 1.0;
        // Half the before/after difference is how far drift could reach.
        let drift = (last.rate() - first.rate()) / (last.rate() + first.rate()).max(1e-9);
        let variance = candidate.error().powi(2)
            + (first.error().powi(2) + last.error().powi(2)) / 4.0
            + drift * drift
            + NOISE_FLOOR * NOISE_FLOOR;
        weighted += gain / variance;
        weights += 1.0 / variance;
        gains.push(gain);
        variances.push(variance);
    }
    if weights == 0.0 {
        return (0.0, f64::INFINITY);
    }
    let mut variance = 1.0 / weights;
    // Two cycles that disagree by more than their own errors explain cannot be
    // more certain than that disagreement.
    if let ([first, second], [a, b]) = (gains.as_slice(), variances.as_slice()) {
        variance = variance.max(((first - second).powi(2) - a - b) / 4.0);
    }
    (weighted / weights, variance.sqrt())
}

/// Decide from complete cycles. `down_band` is how far below the incumbent still
/// counts as equal. On the last cycle an unproven result inside the band reports as a
/// tie; a lower count also has to pass `fewer_holds`.
pub(super) fn judge(gain: f64, error: f64, down_band: f64, final_cycle: bool) -> Verdict {
    let (lower, upper) = (gain - WIN_SIGMAS * error, gain + 2.0 * error);
    let win_at = if final_cycle {
        TIE_BAND
    } else {
        CLEAR_GAIN.max(TIE_BAND)
    };
    if lower > win_at {
        Verdict::Better
    } else if upper < -down_band {
        Verdict::Worse
    } else if (gain - 2.0 * error > -down_band && upper < TIE_BAND)
        || (final_cycle && error <= CONTAMINATED_ERROR && gain > -down_band && gain < TIE_BAND)
    {
        Verdict::Tie
    } else {
        Verdict::Undecided
    }
}

/// Fewer threads need evidence that little is given up, not only a lack of proof
/// against them.
fn fewer_holds(gain: f64, error: f64, down_band: f64) -> bool {
    error.is_finite() && gain - FEWER_SIGMAS * error > -down_band
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Purpose {
    Kernel,
    Up,
    Down,
    UpRefine,
    Restore,
}

#[derive(Debug, Clone, Copy)]
struct WindowNeed {
    secs: u64,
    hashes: u64,
    longest: u64,
    shortest_hashes: u64,
}

/// Evidence pooled from earlier comparisons of the same two settings.
#[derive(Debug, Clone, Copy, Default)]
struct Pooled {
    weighted: f64,
    weights: f64,
    looks: u8,
    /// The setting this evidence was measured against.
    against: Option<Setting>,
}

impl Pooled {
    /// Evidence after `looks` comparisons; `gain` and `error` already include earlier ones.
    fn summary(looks: u8, gain: f64, error: f64, against: Setting) -> Self {
        let mut pooled = Self {
            looks,
            against: Some(against),
            ..Self::default()
        };
        if error.is_finite() && error > 0.0 {
            pooled.weights = 1.0 / (error * error);
            pooled.weighted = gain * pooled.weights;
        }
        pooled
    }

    fn add(self, gain: f64, error: f64) -> Self {
        if !error.is_finite() || error <= 0.0 {
            return self;
        }
        let weight = 1.0 / (error * error);
        Self {
            weighted: self.weighted + gain * weight,
            weights: self.weights + weight,
            looks: self.looks + 1,
            against: self.against,
        }
    }

    fn estimate(self) -> (f64, f64) {
        if self.weights == 0.0 {
            return (0.0, f64::INFINITY);
        }
        (self.weighted / self.weights, (1.0 / self.weights).sqrt())
    }
}

#[derive(Debug)]
struct Trial {
    purpose: Purpose,
    base: Setting,
    candidate: Setting,
    windows: Vec<Sample>,
    open: Option<(u64, u64)>,
    generations: Vec<u64>,
    /// Page backing of the incumbent: its windows only compare if this holds.
    base_backing: Option<Vec<[u64; 5]>>,
    idle_since: Option<u64>,
    started: u64,
    need: WindowNeed,
    prior: Pooled,
    retries: u8,
}

impl Trial {
    fn arm(&self) -> Setting {
        if self.windows.len() % 3 == 1 {
            self.candidate
        } else {
            self.base
        }
    }

    fn peak_threads(&self) -> usize {
        self.base.threads.max(self.candidate.threads)
    }
}

fn first_window(threads: usize) -> WindowNeed {
    WindowNeed {
        secs: FIRST_WINDOW_SECS,
        hashes: MIN_HASHES.max(8 * threads as u64),
        longest: LONGEST_FIRST_WINDOW_SECS,
        shortest_hashes: SHORTEST_HASHES,
    }
}

/// Window length for a second cycle: enough hashes to reach the nearest decision
/// boundary from the first cycle's estimate.
fn second_window(gain: f64, threads: usize, rate: f64, floor_secs: u64) -> WindowNeed {
    let distance = (gain.abs() - TIE_BAND).abs().max(TIE_BAND);
    let error = distance / 2.45;
    let hashes = (UNEVEN_FINISH * (threads.max(1) as f64).sqrt() / error).ceil() as u64;
    let longest = if rate < SLOW_RATE {
        LONGEST_WINDOW_SECS_SLOW
    } else {
        LONGEST_WINDOW_SECS
    };
    let secs = (hashes as f64 / rate.max(0.01)).ceil() as u64;
    WindowNeed {
        secs: secs.max(floor_secs).min(longest.max(floor_secs)),
        hashes: SHORTEST_HASHES,
        longest,
        shortest_hashes: SHORTEST_HASHES,
    }
}

/// Whether two views of page backing, per backend, can be compared. Coverage moves a
/// little as the kernel collapses pages; a real change moves the share of large pages.
fn same_backing(a: &[[u64; 5]], b: &[[u64; 5]]) -> bool {
    let large_share = |bytes: &[u64; 5]| {
        let total: u64 = bytes.iter().sum();
        (bytes[0] + bytes[1] + bytes[2]) as f64 / total.max(1) as f64
    };
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(a, b)| (large_share(a) - large_share(b)).abs() <= BACKING_SHIFT)
}

/// A re-test already knows it is close: go straight to the longest window.
fn long_window(threads: usize, rate: f64) -> WindowNeed {
    let longest = if rate < SLOW_RATE {
        RETEST_WINDOW_SECS_SLOW
    } else {
        RETEST_WINDOW_SECS
    };
    WindowNeed {
        secs: longest,
        hashes: SHORTEST_HASHES.max(threads as u64),
        longest,
        shortest_hashes: SHORTEST_HASHES,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// Starting from a saved result: watch the rate before trusting it.
    Checking,
    Kernels,
    CountUp,
    Refine,
    KernelsAtFinal,
    Settled,
}

struct View {
    members: Vec<usize>,
    threads: usize,
    hashes: u64,
    hashing: bool,
    warm: bool,
    kernel: Option<CpuKernel>,
    kernel_ready: bool,
    backing: Vec<[u64; 5]>,
    generations: Vec<u64>,
}

impl View {
    fn of(snapshots: &[CpuAdaptiveSnapshot]) -> Self {
        let running: Vec<_> = snapshots.iter().filter(|s| s.running).collect();
        let first = running.first();
        Self {
            members: snapshots
                .iter()
                .enumerate()
                .filter_map(|(index, s)| s.running.then_some(index))
                .collect(),
            threads: running.iter().map(|s| s.threads).min().unwrap_or(0),
            hashes: running.iter().map(|s| s.hashes).sum(),
            hashing: !running.is_empty() && running.iter().all(|s| s.hashing),
            warm: !running.is_empty()
                && running.iter().all(|s| {
                    s.warmed == s.threads
                        && s.threads > 0
                        && s.resident_bytes == s.threads as u64 * CPU_LANE_MEMORY_BYTES
                }),
            kernel: first
                .filter(|f| running.iter().all(|s| s.kernel == f.kernel))
                .map(|f| f.kernel),
            kernel_ready: running.iter().all(|s| s.kernel_ready),
            backing: running.iter().map(|s| s.backing_bytes).collect(),
            generations: running.iter().map(|s| s.kernel_generation).collect(),
        }
    }
}

struct Watch {
    open: Option<(u64, u64)>,
    reference: Option<f64>,
    low: u8,
    high: u8,
    next_allowed: u64,
    retry_secs: u64,
}

pub(super) struct Search {
    min: usize,
    max: usize,
    down_band: f64,
    candidates: Vec<CpuKernel>,
    build: BuildFacts,
    quality: SeedQuality,

    stage: Stage,
    peak: Setting,
    /// Smallest count that tied the peak.
    keep: usize,
    /// Below this count the rate is lower, measured or assumed from the seed.
    lo: usize,
    lo_measured: bool,
    /// At or above this count nothing more is gained.
    hi: usize,
    step: usize,
    stalls: u8,
    /// Highest count measured against the current peak.
    tested_above: usize,
    /// Still looking for a higher count.
    climbing: bool,
    /// The next upward look goes straight to everything that fits.
    top_first: bool,
    upper_ties: u8,
    requeued: u8,
    queue: VecDeque<CpuKernel>,
    kernels_tested_at: usize,
    rounds: u8,
    trial: Option<Trial>,

    target: Setting,
    commanded: Setting,
    started_search: u64,
    deadline: Option<u64>,
    done: u32,
    testing_update: bool,

    last_rate: f64,
    results: Vec<KernelResult>,
    /// Faster than the current setting by an amount we could not confirm.
    undecided: VecDeque<(Setting, Pooled)>,
    undecided_at: u64,
    /// The highest count proven worse, or the floor below `min`.
    worse_at: usize,
    retests: u32,

    watch: Watch,
    /// The count to return to after other programs stop competing for the CPU.
    unloaded_best: Option<usize>,
    recovery_at: u64,
    recovery_secs: u64,

    settled_hps: f64,
    ram_limit: Option<usize>,
    allocation_limit: Option<usize>,
    allocation_retry_at: u64,
    allocation_backoff: u64,
    headroom_since: Option<u64>,
    last_limit: usize,
    constrained_since: Option<u64>,
    limited_note: bool,
    members: Option<Vec<usize>>,
    announced: Option<Setting>,
    /// A count picked by hand is not a result to start from next time.
    manual: bool,
    parallelism: Option<usize>,
    search_reason: Option<&'static str>,
    shift_noted: bool,
    /// Hashes and seconds measured at each setting tried.
    measured: BTreeMap<(usize, &'static str), (u64, u64)>,
    comparing_secs: u64,
    settled_after_update: bool,
    events: Vec<TrialEvent>,
    pending_notes: Vec<Note>,
    pending_details: Vec<String>,
}

impl Search {
    pub(super) fn new(params: Params) -> Self {
        let Params {
            seed,
            quality,
            min,
            max,
            down_band,
            candidates,
            saved,
            build,
        } = params;
        let mut search = Self {
            min: min.max(1).min(max.max(1)),
            max: max.max(1),
            down_band: down_band.max(TIE_BAND),
            candidates,
            build,
            quality,
            stage: Stage::Kernels,
            peak: seed,
            keep: seed.threads,
            lo: min.saturating_sub(1),
            lo_measured: false,
            hi: max.max(1),
            step: 1,
            stalls: 0,
            tested_above: 0,
            climbing: true,
            top_first: false,
            upper_ties: 0,
            requeued: 0,
            queue: VecDeque::new(),
            kernels_tested_at: seed.threads,
            rounds: 0,
            trial: None,
            target: seed,
            commanded: seed,
            started_search: 0,
            deadline: None,
            done: 0,
            testing_update: false,
            last_rate: 0.0,
            results: Vec::new(),
            undecided: VecDeque::new(),
            undecided_at: 0,
            worse_at: min.max(1).min(max.max(1)) - 1,
            retests: 0,
            watch: Watch {
                open: None,
                reference: None,
                low: 0,
                high: 0,
                next_allowed: 0,
                retry_secs: SHIFT_RETRY_SECS,
            },
            unloaded_best: None,
            recovery_at: 0,
            recovery_secs: RECOVERY_PROBE_SECS,
            settled_hps: 0.0,
            ram_limit: None,
            allocation_limit: None,
            allocation_retry_at: 0,
            allocation_backoff: ALLOCATION_RETRY_SECS,
            headroom_since: None,
            last_limit: max.max(1),
            constrained_since: None,
            limited_note: false,
            members: None,
            announced: None,
            manual: false,
            parallelism: None,
            search_reason: None,
            shift_noted: false,
            measured: BTreeMap::new(),
            comparing_secs: 0,
            settled_after_update: false,
            events: Vec::new(),
            pending_notes: Vec::new(),
            pending_details: Vec::new(),
        };
        search.step = search.first_step(seed.threads);
        match saved {
            Some(saved) if saved.kernel.parse::<CpuKernel>() == Ok(seed.kernel) => {
                search.resume(saved);
            }
            _ => search.begin_fresh(seed),
        }
        search
    }

    fn first_step(&self, threads: usize) -> usize {
        let divisor = match self.quality {
            SeedQuality::Estimate => 2,
            SeedQuality::Family => 4,
            SeedQuality::Measured => 8,
        };
        (threads / divisor).max(if threads >= 8 { 2 } else { 1 })
    }

    fn begin_fresh(&mut self, seed: Setting) {
        self.lo = match self.quality {
            // Nothing below half the known-good count is worth a test.
            SeedQuality::Estimate => self.min - 1,
            SeedQuality::Family | SeedQuality::Measured => {
                (self.min - 1).max(seed.threads.saturating_sub(2 * self.step))
            }
        };
        self.queue = self.challengers(seed.kernel, seed.threads);
        self.stage = Stage::Kernels;
        self.top_first = self.quality != SeedQuality::Measured;
        self.search_reason =
            Some("finding the best settings: the thread count will change for a few minutes");
    }

    /// Start from an earlier result and test only what this build changed.
    fn resume(&mut self, saved: Saved) {
        // The saved count may not fit in RAM right now; it stays the count to return to.
        let saved_threads = saved.threads.clamp(self.min, self.max);
        self.peak.threads = saved_threads;
        self.keep = saved_threads;
        self.settled_hps = saved.hps;
        self.watch.reference = (saved.hps > 0.0).then_some(saved.hps);
        self.ram_limit = saved.ram_limit;
        let hot_changed = saved.hot_fingerprint != self.build.hot_fingerprint;
        let revision = |kernel: &str| self.build.kernel_revisions.get(kernel).copied();
        // A result for a kernel this build changed says nothing about it now.
        let unchanged: Vec<KernelResult> = saved
            .kernels
            .iter()
            .filter(|r| revision(&r.kernel) == Some(r.revision))
            .cloned()
            .collect();
        let new_or_improved: Vec<CpuKernel> = self
            .candidates
            .iter()
            .copied()
            .filter(|k| *k != self.peak.kernel)
            .filter(|k| !unchanged.iter().any(|r| r.kernel == k.as_str()))
            .collect();
        let incumbent = self.peak.kernel;
        let incumbent_improved = saved.kernel_revisions.get(incumbent.as_str())
            != self.build.kernel_revisions.get(incumbent.as_str());
        self.results = unchanged;
        // Rechecks a restart interrupted carry on, unless this build changed the answer.
        if !hot_changed && !incumbent_improved {
            for recheck in &saved.rechecks {
                let Ok(kernel) = recheck.kernel.parse::<CpuKernel>() else {
                    continue;
                };
                let Ok(against_kernel) = recheck.against_kernel.parse::<CpuKernel>() else {
                    continue;
                };
                let same_kernel = kernel == self.peak.kernel;
                if !(same_kernel || self.candidates.contains(&kernel))
                    || !(self.min..=self.max).contains(&recheck.threads)
                    || recheck.looks >= UNDECIDED_LOOKS
                    || self.undecided.len() >= UNDECIDED_PENDING
                {
                    continue;
                }
                let against = Setting {
                    threads: recheck.against_threads,
                    kernel: against_kernel,
                };
                self.undecided.push_back((
                    Setting {
                        threads: recheck.threads,
                        kernel,
                    },
                    Pooled::summary(recheck.looks, recheck.gain, recheck.error, against),
                ));
            }
            self.schedule_retests(0);
        }
        self.queue = new_or_improved.into_iter().collect();
        let tested_anything_new = !self.queue.is_empty();
        if hot_changed || incumbent_improved {
            // Other changes can move any result: re-check the closest rival once
            // and see whether the best count moved.
            if let Some(rival) = self.closest_rival(incumbent) {
                if !self.queue.contains(&rival) {
                    self.queue.push_back(rival);
                }
            }
        }
        let step = (self.peak.threads / 8).max(2);
        self.step = step;
        self.lo = (self.min - 1).max(self.peak.threads.saturating_sub(step));
        self.lo_measured = false;
        self.hi = self.max;
        // One look upward unless it wins; a stable result is not re-searched.
        // An improved kernel can move the best count further, so it gets two.
        self.stalls = u8::from(!incumbent_improved);
        self.tested_above = self.peak.threads;
        self.announced = Some(self.peak);
        self.testing_update = tested_anything_new || hot_changed || incumbent_improved;
        if self.testing_update {
            self.search_reason = Some(
                "new seine version: testing what changed, the thread count may change for a few minutes",
            );
        }
        self.stage = if self.testing_update {
            Stage::Kernels
        } else {
            Stage::Checking
        };
        self.rounds = 1;
    }

    fn closest_rival(&self, kernel: CpuKernel) -> Option<CpuKernel> {
        self.results
            .iter()
            .filter(|r| r.kernel != kernel.as_str())
            .max_by(|a, b| a.ratio.total_cmp(&b.ratio))
            .and_then(|r| r.kernel.parse().ok())
            .filter(|k| self.candidates.contains(k))
    }

    /// Kernels still worth trying at this count, most promising first.
    fn challengers(&self, current: CpuKernel, threads: usize) -> VecDeque<CpuKernel> {
        self.candidates
            .iter()
            .copied()
            .filter(|k| *k != current)
            .filter(|k| !self.known_result(*k, threads))
            .collect()
    }

    /// A result stands at counts within a factor of two of where it was measured.
    fn known_result(&self, kernel: CpuKernel, threads: usize) -> bool {
        self.results.iter().any(|r| {
            r.kernel == kernel.as_str()
                && threads <= r.threads.saturating_mul(2)
                && threads.saturating_mul(2) >= r.threads
        })
    }

    fn revision_of(&self, kernel: CpuKernel) -> u32 {
        self.build
            .kernel_revisions
            .get(kernel.as_str())
            .copied()
            .unwrap_or(0)
    }

    fn remember(&mut self, kernel: CpuKernel, threads: usize, ratio: f64) {
        let revision = self.revision_of(kernel);
        self.results.retain(|r| r.kernel != kernel.as_str());
        self.results.push(KernelResult {
            kernel: kernel.to_string(),
            revision,
            threads,
            ratio,
        });
    }

    fn operating(&self) -> Setting {
        Setting {
            threads: self.keep,
            kernel: self.peak.kernel,
        }
    }

    fn limit(&self, now: u64, cap: usize) -> usize {
        let cap = cap.min(self.max);
        match self.allocation_limit {
            Some(limit) if now < self.allocation_retry_at => cap.min(limit),
            _ => cap,
        }
    }

    pub(super) fn status(&self, cap: usize) -> Status {
        if self.target.threads == 0 {
            return Status::WaitingForRam;
        }
        let best = self.unloaded_best.unwrap_or(self.keep);
        if self.trial.is_none() && self.target.threads < best && cap.min(self.last_limit) < best {
            return Status::LimitedByRam {
                threads: self.target.threads,
                best,
            };
        }
        let comparing = match &self.trial {
            Some(trial) if trial.base.threads != trial.candidate.threads => {
                Comparing::Threads(trial.base.threads, trial.candidate.threads)
            }
            Some(_) => Comparing::Kernels,
            None => Comparing::Nothing,
        };
        match self.stage {
            Stage::Settled => match self.trial.as_ref() {
                Some(trial) => Status::Rechecking {
                    current: trial.base.threads,
                    candidate: trial.candidate.threads,
                },
                None => Status::Settled {
                    threads: self.target.threads,
                    kernel: self.target.kernel,
                    hps: self.settled_hps,
                },
            },
            Stage::Checking => Status::Checking,
            _ => {
                let left = self.estimate_left();
                // Use this machine's own pace once there is one to go by.
                let per_comparison = if self.done > 0 {
                    (self.comparing_secs / self.done as u64).max(30)
                } else {
                    90
                };
                Status::Tuning {
                    threads: self.peak.threads,
                    kernel: self.peak.kernel,
                    done: self.done,
                    left,
                    secs_left: left as u64 * per_comparison,
                    testing_update: self.testing_update,
                    comparing,
                }
            }
        }
    }

    fn estimate_left(&self) -> u32 {
        let gap = |a: usize, b: usize| {
            let mut gap = a.saturating_sub(b);
            let mut steps = 0;
            while gap > 1 {
                gap /= 2;
                steps += 1;
            }
            steps
        };
        let count = match self.stage {
            Stage::Kernels | Stage::CountUp => {
                1 + gap(self.peak.threads, self.lo) + gap(self.hi, self.peak.threads)
            }
            Stage::Refine => gap(self.keep, self.lo) + gap(self.hi, self.peak.threads),
            _ => 0,
        };
        self.queue.len() as u32 + count + u32::from(self.trial.is_some())
    }

    /// The CPUs this process may use changed: the best count may have moved.
    pub(super) fn observe_parallelism(&mut self, now: u64, parallelism: usize) {
        if parallelism == 0 {
            return;
        }
        let changed = self
            .parallelism
            .replace(parallelism)
            .is_some_and(|before| before != parallelism);
        if changed && matches!(self.stage, Stage::Settled | Stage::Checking) && self.min < self.max
        {
            self.pending_details
                .push("available CPUs changed; checking the count".into());
            self.search_reason = Some("available CPUs changed: rechecking the thread count");
            self.watch.open = None;
            self.watch.reference = None;
            self.restart_count_check(now);
        }
    }

    /// A validated kernel for this machine, moved to before any comparison.
    pub(super) fn adopt_start(&mut self, kernel: CpuKernel) {
        if self.trial.is_none() && kernel != self.peak.kernel {
            self.peak.kernel = kernel;
            self.target.kernel = kernel;
            self.queue = self.challengers(kernel, self.peak.threads);
        }
    }

    /// The user picked a count: only kernels may still change.
    pub(super) fn set_manual(&mut self, threads: usize) {
        self.cancel_trial();
        self.manual = true;
        self.min = threads;
        self.max = threads;
        self.peak.threads = threads;
        self.keep = threads;
        self.lo = threads - 1;
        self.hi = threads;
        self.target = self.operating();
        self.unloaded_best = None;
        self.undecided.clear();
        self.ram_limit = None;
        self.watch.open = None;
        self.watch.reference = None;
        self.queue = self.challengers(self.peak.kernel, threads);
        self.kernels_tested_at = threads;
        self.stage = Stage::Kernels;
    }

    pub(super) fn saved(&self) -> Saved {
        let running = self.trial.iter().filter_map(|trial| {
            (trial.purpose == Purpose::Restore && trial.prior.looks > 0)
                .then_some((trial.candidate, trial.prior))
        });
        let rechecks = self
            .undecided
            .iter()
            .copied()
            .chain(running)
            .filter_map(|(candidate, pooled)| {
                let against = pooled.against?;
                let (gain, error) = if pooled.weights > 0.0 {
                    pooled.estimate()
                } else {
                    (0.0, 0.0)
                };
                Some(SavedRecheck {
                    threads: candidate.threads,
                    kernel: candidate.kernel.to_string(),
                    looks: pooled.looks,
                    against_threads: against.threads,
                    against_kernel: against.kernel.to_string(),
                    gain,
                    error,
                })
            })
            .collect();
        Saved {
            threads: self.keep,
            kernel: self.peak.kernel.to_string(),
            hps: self.settled_hps,
            ram_limit: self.ram_limit,
            hot_fingerprint: self.build.hot_fingerprint.clone(),
            kernel_revisions: self.build.kernel_revisions.clone(),
            kernels: self.results.clone(),
            rechecks,
        }
    }

    /// What this search has settled on and what it measured to get there.
    pub(super) fn settled_summary(&self) -> SettledSummary {
        SettledSummary {
            threads: self.keep,
            kernel: self.peak.kernel,
            hps: self.settled_hps.max(self.last_rate),
            comparisons: self.done,
            comparing_secs: self.comparing_secs,
            ram_limited: self.ram_limit.is_some(),
            quality: self.quality,
            after_update: self.settled_after_update,
            curve: self
                .measured
                .iter()
                .rev()
                .take(16)
                .map(|(&(threads, kernel), &(hashes, secs))| {
                    (threads, kernel, hashes as f64 / secs.max(1) as f64)
                })
                .collect(),
        }
    }

    pub(super) fn target_threads(&self) -> usize {
        self.target.threads
    }

    #[cfg(test)]
    pub(super) fn tick(
        &mut self,
        now: u64,
        snapshots: &[CpuAdaptiveSnapshot],
        cap: usize,
        allocation_failed: bool,
    ) -> Output {
        self.tick_rig(now, snapshots, cap, allocation_failed, None)
    }

    /// `rig_hashes` counts every backend on rigs with GPUs, so a thread count that
    /// slows a GPU does not win on CPU hashes alone.
    pub(super) fn tick_rig(
        &mut self,
        now: u64,
        snapshots: &[CpuAdaptiveSnapshot],
        cap: usize,
        allocation_failed: bool,
        rig_hashes: Option<u64>,
    ) -> Output {
        let mut view = View::of(snapshots);
        if let Some(hashes) = rig_hashes {
            view.hashes = hashes;
        }
        let mut out = Output::default();
        self.observe_members(now, &view);
        self.observe_capacity(now, &view, cap, allocation_failed);
        if let Some(rate) = self.advance_trial(now, &view, &mut out) {
            self.last_rate = rate;
        }
        if self.trial.is_none() && self.target.threads > 0 {
            match self.stage {
                Stage::Settled => self.watch_settled(now, &view, cap, &mut out),
                Stage::Checking => self.watch_checking(now, &view, &mut out),
                _ => self.plan(now, &view, cap, &mut out),
            }
        }
        if self.target.threads != self.commanded.threads {
            out.threads = Some(self.target.threads);
            self.commanded.threads = self.target.threads;
        }
        if self.target.kernel != self.commanded.kernel {
            out.kernel = Some(self.target.kernel);
            self.commanded.kernel = self.target.kernel;
        }
        out.events.append(&mut self.events);
        out.notes.append(&mut self.pending_notes);
        out.details.append(&mut self.pending_details);
        out
    }

    fn observe_members(&mut self, now: u64, view: &View) {
        let changed = self.members.as_ref().is_some_and(|m| *m != view.members);
        self.members = Some(view.members.clone());
        if changed {
            // Rates from different backend sets are not comparable.
            self.cancel_trial();
            self.watch.open = None;
            self.watch.reference = None;
            self.pending_details
                .push("active CPU backends changed; restarting comparisons".into());
            if self.stage == Stage::Settled || self.stage == Stage::Checking {
                self.restart_count_check(now);
            }
        }
    }

    fn cancel_trial(&mut self) {
        if let Some(trial) = self.trial.take() {
            self.events.push(TrialEvent::Abort { idle: false });
            // An interrupted comparison says nothing: a kernel keeps its turn.
            if trial.purpose == Purpose::Kernel && self.requeued < MAX_REQUEUED {
                self.requeued += 1;
                self.queue.push_front(trial.candidate.kernel);
            }
        }
    }

    fn restart_count_check(&mut self, now: u64) {
        self.started_search = now;
        self.deadline = None;
        self.rounds = 1;
        let step = (self.keep / 8).max(2);
        self.step = step;
        self.peak.threads = self.keep;
        self.lo = (self.min - 1).max(self.keep.saturating_sub(step));
        self.lo_measured = false;
        self.worse_at = self.min - 1;
        self.hi = self.max;
        self.climbing = true;
        self.stalls = 1;
        self.tested_above = self.keep;
        self.stage = Stage::CountUp;
    }

    fn observe_capacity(&mut self, now: u64, view: &View, cap: usize, allocation_failed: bool) {
        let limited = allocation_failed || view.threads < self.commanded.threads;
        if limited {
            self.allocation_limit = Some(view.threads);
            self.allocation_retry_at = now + self.allocation_backoff;
            self.allocation_backoff = (self.allocation_backoff * 2).min(ALLOCATION_RETRY_MAX_SECS);
            self.cancel_trial();
            self.hi = self.hi.min(view.threads.max(1));
            self.target.threads = view.threads.min(cap);
            self.commanded.threads = view.threads;
            self.note_ram_limit();
        }
        let limit = self.limit(now, cap);
        self.last_limit = limit;
        if self
            .trial
            .as_ref()
            .is_some_and(|t| t.peak_threads() > limit)
        {
            // The candidate no longer fits. Remember the room we actually have and
            // back off before trying to grow again.
            let (candidate, base) = {
                let trial = self.trial.as_ref().unwrap();
                (trial.candidate.threads, trial.base.threads)
            };
            self.cancel_trial();
            if candidate > limit && base <= limit {
                self.allocation_retry_at = now + self.allocation_backoff;
                self.allocation_limit = Some(limit);
                self.allocation_backoff =
                    (self.allocation_backoff * 2).min(ALLOCATION_RETRY_MAX_SECS);
            }
            self.pending_details
                .push("RAM limit changed; discarding comparison".into());
            self.target.threads = self.keep.min(limit);
            if self.target.threads < self.keep {
                self.note_ram_limit();
            }
        }
        if self.target.threads > limit {
            self.target.threads = limit;
            self.note_ram_limit();
        }
        // Short of RAM for the best known count: wait for it, then search with what
        // there is if it stays short.
        if limit < self.keep {
            let since = *self.constrained_since.get_or_insert(now);
            if !matches!(self.stage, Stage::Settled | Stage::Checking)
                && limit > 0
                && now.saturating_sub(since) >= CONSTRAINED_SEARCH_WAIT_SECS
            {
                self.peak.threads = self.peak.threads.min(limit);
                self.keep = limit;
                self.hi = self.hi.min(limit);
                self.lo = self.lo.min(limit.saturating_sub(1));
            }
        } else {
            self.constrained_since = None;
        }
        if limit > self.target.threads {
            self.headroom_since.get_or_insert(now);
        } else {
            self.headroom_since = None;
        }
        if self.target.threads == 0 {
            if self.ready_to_grow(now) && limit > 0 {
                self.target.threads = self.keep.min(limit).max(1);
                self.limited_note = false;
                self.pending_notes.push(Note {
                    text: format!("resumed on {} threads: RAM is free", self.target.threads),
                    success: false,
                });
            }
        } else if self.trial.is_none()
            && self.target.threads < self.keep
            && self.ready_to_grow(now)
            && limit > self.target.threads
        {
            self.target.threads = self.keep.min(limit);
            self.pending_details
                .push(format!("{} threads: RAM is free", self.target.threads));
            if self.limited_note && self.target.threads == self.keep {
                self.limited_note = false;
                self.pending_notes.push(Note {
                    text: format!("back to {} threads: RAM is free", self.target.threads),
                    success: false,
                });
            }
        }
    }

    fn ready_to_grow(&self, now: u64) -> bool {
        now >= self.allocation_retry_at
            && self
                .headroom_since
                .is_some_and(|since| now.saturating_sub(since) >= HEADROOM_HOLD_SECS)
    }

    fn note_ram_limit(&mut self) {
        self.pending_details
            .push(format!("{} threads: RAM limit", self.target.threads));
        if !self.limited_note {
            self.limited_note = true;
            self.pending_notes.push(Note {
                text: if self.target.threads == 0 {
                    "paused until enough RAM is free for one thread".to_string()
                } else {
                    format!(
                        "lowered to {} threads: RAM is low; going back when it is free",
                        self.target.threads
                    )
                },
                success: false,
            });
        }
    }

    /// Returns the last measured rate when a window closes.
    fn advance_trial(&mut self, now: u64, view: &View, out: &mut Output) -> Option<f64> {
        let trial = self.trial.as_mut()?;
        if now.saturating_sub(trial.started) >= TRIAL_TIMEOUT_SECS {
            self.events.push(TrialEvent::Abort { idle: true });
            self.finish_trial(now, Verdict::Undecided, 0.0, f64::INFINITY, out);
            self.events.retain(|e| !matches!(e, TrialEvent::End { .. }));
            return None;
        }
        let arm = trial.arm();
        self.target = arm;
        let on_arm = view.threads == arm.threads
            && view.kernel == Some(arm.kernel)
            && view.kernel_ready
            && view.warm;
        if !on_arm {
            if trial.open.take().is_some() {
                self.events.push(TrialEvent::Discard);
            }
            return None;
        }
        if !view.hashing {
            // Brief gaps between jobs count; a longer one ends the window.
            let idle = trial.idle_since.get_or_insert(now);
            if now.saturating_sub(*idle) >= 3 && trial.open.take().is_some() {
                self.events.push(TrialEvent::Discard);
            }
            return None;
        }
        trial.idle_since = None;
        let Some((start, first)) = trial.open else {
            if trial.windows.len() % 3 != 1 {
                match &trial.base_backing {
                    Some(backing) if !same_backing(backing, &view.backing) => {
                        // Different pages mean a different rate: nothing to compare.
                        self.pending_details
                            .push("memory backing changed; discarding comparison".into());
                        self.cancel_trial();
                        self.target = self.operating();
                        return None;
                    }
                    Some(_) => {}
                    None => trial.base_backing = Some(view.backing.clone()),
                }
            }
            trial.open = Some((now, view.hashes));
            trial.generations = view.generations.clone();
            self.events.push(TrialEvent::Open);
            return None;
        };
        if view.hashes < first || trial.generations != view.generations {
            trial.open = None;
            self.events.push(TrialEvent::Discard);
            return None;
        }
        let secs = now.saturating_sub(start);
        let hashes = view.hashes - first;
        // Only a first look stops early: a close result stays close through a load spike.
        let may_cut = trial.windows.len() == 1 && trial.prior.looks == 0;
        let long_enough = (secs >= trial.need.secs && hashes >= trial.need.hashes)
            || (secs >= trial.need.longest && hashes >= trial.need.shortest_hashes);
        // Enough hashes that the shortfall cannot be workers finishing unevenly.
        let clearly_losing = may_cut
            && secs >= FIRST_WINDOW_SECS / 2
            && hashes >= CLEAR_LOSS_HASHES.max(4 * arm.threads as u64)
            && trial.windows.last().is_some_and(|before| {
                let unevenness = 3.0 * (arm.threads as f64).sqrt() / hashes as f64;
                (hashes as f64 / secs.max(1) as f64) * (1.0 + unevenness)
                    < before.rate() * CLEAR_LOSS
            });
        if !long_enough && !clearly_losing {
            return None;
        }
        let sample = Sample {
            hashes,
            secs: secs.max(1),
            threads: arm.threads,
        };
        trial.windows.push(sample);
        trial.open = None;
        let measured = self
            .measured
            .entry((arm.threads, arm.kernel.as_str()))
            .or_default();
        measured.0 += sample.hashes;
        measured.1 += sample.secs;
        self.events.push(TrialEvent::Close {
            hashes: sample.hashes,
            secs: sample.secs,
        });
        let rate = sample.rate();
        if trial.windows.len() % 3 == 0 {
            let (gain, error) = estimate(&trial.windows);
            let prior = trial.prior;
            let (gain, error) = if prior.looks > 0 {
                prior.add(gain, error).estimate()
            } else {
                (gain, error)
            };
            let final_cycle = trial.windows.len() >= 6;
            let down_band = if trial.candidate.threads < trial.base.threads {
                self.down_band
            } else {
                TIE_BAND
            };
            let mut verdict = judge(gain, error, down_band, final_cycle);
            if final_cycle
                && prior.looks > 0
                && verdict == Verdict::Undecided
                && gain - POOLED_WIN_SIGMAS * error > TIE_BAND
            {
                verdict = Verdict::Better;
            }
            let contaminated = final_cycle
                && matches!(verdict, Verdict::Undecided | Verdict::Tie)
                && error > CONTAMINATED_ERROR
                && trial.retries == 0;
            if contaminated {
                // Something else ran during the windows; this says nothing either way.
                let hottest = trial.peak_threads();
                let (base, candidate) = (trial.base, trial.candidate);
                trial.retries += 1;
                trial.windows.clear();
                trial.need = first_window(hottest);
                trial.base_backing = None;
                self.events.push(TrialEvent::Abort { idle: false });
                self.events.push(TrialEvent::Begin { base, candidate });
            } else if verdict != Verdict::Undecided || final_cycle {
                self.finish_trial(now, verdict, gain, error, out);
            } else {
                let trial = self.trial.as_mut().unwrap();
                if trial.prior.looks == 0 {
                    let hottest = trial.peak_threads();
                    let floor = trial.need.secs.max(FIRST_WINDOW_SECS);
                    trial.need = second_window(gain, hottest, rate, floor);
                }
            }
        }
        Some(rate)
    }

    fn finish_trial(
        &mut self,
        now: u64,
        verdict: Verdict,
        gain: f64,
        error: f64,
        out: &mut Output,
    ) {
        let Some(trial) = self.trial.take() else {
            return;
        };
        self.done += 1;
        self.comparing_secs += now.saturating_sub(trial.started);
        out.details.push(format!(
            "{} threads {} vs {} threads {}: {:+.2}% (+/-{:.2}%), {verdict:?}",
            trial.base.threads,
            trial.base.kernel,
            trial.candidate.threads,
            trial.candidate.kernel,
            gain * 100.0,
            (2.0 * error * 100.0).min(999.0),
        ));
        // Keep a settled-rate reference from the incumbent's own windows.
        let base_rate = {
            let (hashes, secs) = trial
                .windows
                .iter()
                .enumerate()
                .filter(|(index, _)| index % 3 != 1)
                .fold((0u64, 0u64), |(h, s), (_, w)| (h + w.hashes, s + w.secs));
            hashes as f64 / secs.max(1) as f64
        };
        if base_rate > 0.0 {
            self.last_rate = base_rate;
        }
        let fewer = trial.candidate.threads < trial.base.threads;
        let band = if fewer { self.down_band } else { TIE_BAND };
        let close = matches!(verdict, Verdict::Undecided | Verdict::Tie);
        let looks = trial.prior.looks + 1;
        let evidence = Pooled::summary(looks, gain, error, trial.base);
        match trial.purpose {
            Purpose::Kernel => {
                let ratio = 1.0 + gain;
                if verdict == Verdict::Better {
                    // The old incumbent is now the rival to remember.
                    self.remember(trial.base.kernel, trial.base.threads, 1.0 / ratio);
                    self.peak.kernel = trial.candidate.kernel;
                    self.results
                        .retain(|r| r.kernel != trial.candidate.kernel.as_str());
                } else if error.is_finite() {
                    self.remember(trial.candidate.kernel, trial.candidate.threads, ratio);
                }
            }
            Purpose::Up => match verdict {
                Verdict::Better => {
                    // Only a clear win says the counts below it are out of the running.
                    if gain - WIN_SIGMAS * error > CLEAR_GAIN {
                        self.lo = trial.base.threads;
                        self.lo_measured = true;
                    }
                    self.peak.threads = trial.candidate.threads;
                    self.keep = self.peak.threads;
                    self.stalls = 0;
                    self.hi = self.max;
                    self.step = self.step.saturating_mul(2);
                }
                Verdict::Worse => {
                    self.hi = trial.candidate.threads.min(self.hi);
                    self.climbing = false;
                }
                _ => {
                    if verdict == Verdict::Tie {
                        self.upper_ties += 1;
                    }
                    // A small step may not show a real slope: try a bigger one
                    // before giving up, and refine below the first one that held.
                    self.hi = trial.candidate.threads.min(self.hi);
                    if gain <= TIE_BAND {
                        self.stalls += 1;
                    }
                    self.step = self.step.saturating_mul(2);
                    if self.stalls >= 2 {
                        self.climbing = false;
                    }
                }
            },
            Purpose::Down => match verdict {
                Verdict::Better => {
                    self.hi = trial.base.threads;
                    self.peak.threads = trial.candidate.threads;
                    self.keep = trial.candidate.threads;
                    self.extend_down();
                }
                Verdict::Tie | Verdict::Undecided if fewer_holds(gain, error, self.down_band) => {
                    // Measured faster, if unproven: later counts answer to this one.
                    if gain > TIE_BAND {
                        self.hi = trial.base.threads;
                        self.peak.threads = trial.candidate.threads;
                    }
                    self.keep = trial.candidate.threads;
                    self.look_between(now, trial.candidate, trial.base.threads);
                    self.extend_down();
                }
                _ => {
                    self.lo = trial.candidate.threads;
                    self.lo_measured = true;
                    if verdict == Verdict::Worse {
                        self.worse_at = self.worse_at.max(trial.candidate.threads);
                    }
                    // Possibly as fast with fewer threads: worth another look later.
                    if close && error.is_finite() && gain + error > -self.down_band {
                        self.queue_retest(now, trial.candidate, evidence, false);
                    }
                }
            },
            Purpose::UpRefine => match verdict {
                Verdict::Better => {
                    self.lo = trial.base.threads;
                    self.peak.threads = trial.candidate.threads;
                    self.keep = trial.candidate.threads;
                    self.upper_ties = 0;
                }
                Verdict::Tie => {
                    self.hi = trial.candidate.threads;
                    self.upper_ties += 1;
                    // Level at two places above the peak: nothing to find between.
                    if self.upper_ties >= 2 {
                        self.hi = self.peak.threads;
                    }
                }
                _ => self.hi = trial.candidate.threads,
            },
            Purpose::Restore => {
                // A re-test of a close result proves nothing through heavy noise.
                let trusted = trial.prior.looks == 0 || error <= CONTAMINATED_ERROR;
                let accepted = trusted
                    && (verdict == Verdict::Better || (fewer && fewer_holds(gain, error, band)));
                if accepted {
                    self.keep = trial.candidate.threads;
                    if fewer {
                        self.look_between(now, trial.candidate, trial.base.threads);
                    }
                    self.peak.kernel = trial.candidate.kernel;
                    if !fewer || gain > TIE_BAND {
                        self.peak.threads = trial.candidate.threads;
                    }
                    self.unloaded_best = None;
                    self.recovery_secs = RECOVERY_PROBE_SECS;
                    if trial.candidate.threads > trial.base.threads && self.stage == Stage::Settled
                    {
                        // The count was too low: see whether the climb goes on.
                        self.started_search = now;
                        self.deadline = None;
                        self.lo = trial.base.threads;
                        self.lo_measured = true;
                        self.hi = self.max;
                        self.step = (self.keep / 8).max(2);
                        self.stalls = 1;
                        self.tested_above = self.keep;
                        self.climbing = true;
                        self.stage = Stage::CountUp;
                    } else if fewer && self.stage == Stage::Settled {
                        // Fewer threads held up: lower counts may too.
                        out.save = !self.manual;
                        self.started_search = now;
                        self.deadline = None;
                        self.step = (self.keep / 8).max(1);
                        self.lo = self.worse_at.max(self.keep.saturating_sub(self.step));
                        self.lo_measured = self.lo == self.worse_at;
                        self.stage = Stage::Refine;
                    }
                } else if verdict == Verdict::Tie {
                    self.unloaded_best = None;
                }
            }
        }
        // Fewer threads were accepted on evidence that also allows a real loss: the
        // count given up stays on the list.
        if trial.purpose == Purpose::Down
            && self.keep == trial.candidate.threads
            && error.is_finite()
            && gain - 2.0 * error < -TIE_BAND
        {
            let reversed = Pooled::summary(looks, -gain, error, trial.candidate);
            self.queue_retest(now, trial.base, reversed, false);
        }
        // A gain worth having, or fewer threads at no cost, is still possible. The
        // candidate runs about as fast, so keep adding evidence, spaced out, a few times.
        let unresolved = close
            && error.is_finite()
            && self.operating() != trial.candidate
            && if fewer {
                trial.purpose == Purpose::Restore && gain + 2.0 * error > -band
            } else {
                gain + 2.0 * error > TIE_BAND
            };
        if unresolved {
            // A kernel can change every count's result, so it goes first.
            let first = trial.candidate.kernel != trial.base.kernel;
            self.queue_retest(now, trial.candidate, evidence, first);
        }
        self.target = self.operating();
        self.events.push(TrialEvent::End {
            verdict,
            applied_candidate: self.operating() == trial.candidate,
        });
        if self.stage == Stage::Settled {
            // A single check while settled; the new result becomes the reference.
            self.watch.open = None;
            self.watch.reference = None;
            out.save = !self.manual;
            self.schedule_retests(now);
        }
    }

    fn queue_retest(&mut self, now: u64, candidate: Setting, evidence: Pooled, first: bool) {
        self.undecided.retain(|(pending, _)| *pending != candidate);
        // A long look still this noisy says more looks would not settle it.
        let (_, error) = evidence.estimate();
        if (evidence.looks >= 2 && error > CONTAMINATED_ERROR)
            || evidence.looks >= UNDECIDED_LOOKS
            || self.undecided.len() >= UNDECIDED_PENDING
        {
            return;
        }
        if first {
            self.undecided.push_front((candidate, evidence));
        } else {
            self.undecided.push_back((candidate, evidence));
        }
        self.undecided_at = now + UNDECIDED_RETRY_SECS;
    }

    /// Equal rates either side of a gap can hide a better count inside it.
    fn look_between(&mut self, now: u64, lower: Setting, upper: usize) {
        let middle = (lower.threads + upper) / 2;
        if middle > lower.threads {
            let evidence = Pooled {
                looks: 1,
                against: Some(lower),
                ..Pooled::default()
            };
            let candidate = Setting {
                threads: middle,
                kernel: lower.kernel,
            };
            self.queue_retest(now, candidate, evidence, true);
        }
    }

    /// A result still in doubt gets its second look straight away. Later looks are
    /// spaced out, so they do not all see the same conditions.
    fn schedule_retests(&mut self, now: u64) {
        let first_look_due = self.undecided.iter().any(|(_, pooled)| pooled.looks <= 1);
        self.undecided_at = if first_look_due {
            now
        } else {
            now + UNDECIDED_RETRY_SECS
        };
    }

    /// The assumed bottom was never measured: if the count keeps doing as well
    /// close to it, look further down.
    fn extend_down(&mut self) {
        let gap = (self.peak.threads / 32).max(1);
        if !self.lo_measured && self.keep.saturating_sub(self.lo) <= gap {
            self.step = self.step.saturating_mul(2);
            self.lo = self.keep.saturating_sub(self.step).max(self.min - 1);
        }
    }

    /// One log line when a search starts, so a moving thread count has an explanation.
    fn announce_search(&mut self) {
        if let Some(text) = self.search_reason.take() {
            if self.min < self.max {
                self.pending_notes.push(Note {
                    text: text.to_string(),
                    success: false,
                });
            }
        }
    }

    fn start_trial(&mut self, now: u64, purpose: Purpose, base: Setting, candidate: Setting) {
        if purpose != Purpose::Restore {
            self.announce_search();
        }
        self.pending_details.push(format!(
            "comparing {} threads {} with {} threads {} while mining",
            base.threads, base.kernel, candidate.threads, candidate.kernel
        ));
        self.target = base;
        self.events.push(TrialEvent::Begin { base, candidate });
        self.trial = Some(Trial {
            purpose,
            base,
            candidate,
            windows: Vec::with_capacity(6),
            open: None,
            generations: Vec::new(),
            base_backing: None,
            idle_since: None,
            started: now,
            need: first_window(base.threads.max(candidate.threads)),
            prior: Pooled::default(),
            retries: 0,
        });
    }

    /// Decide what to test next during the search.
    fn plan(&mut self, now: u64, view: &View, cap: usize, out: &mut Output) {
        // No comparisons while the best count is out of reach or not yet restored.
        if self.limit(now, cap) < self.keep || self.target.threads < self.keep {
            return;
        }
        if view.threads != self.target.threads
            || view.kernel != Some(self.target.kernel)
            || !view.kernel_ready
        {
            return;
        }
        if self.deadline.is_none() && view.warm {
            self.started_search = now;
        }
        let budget = if self.last_rate > 0.0 && self.last_rate < SLOW_RATE {
            SEARCH_BUDGET_SECS_SLOW
        } else {
            SEARCH_BUDGET_SECS
        };
        let deadline = *self.deadline.get_or_insert(self.started_search + budget);
        if now >= deadline {
            self.pending_details
                .push("search budget used; keeping the best setting so far".into());
            let limit = self.limit(now, cap);
            self.settle(now, out, limit);
            return;
        }
        let limit = self.limit(now, cap);
        loop {
            match self.stage {
                Stage::Kernels | Stage::KernelsAtFinal => {
                    if let Some(kernel) = self.queue.pop_front() {
                        if kernel == self.peak.kernel {
                            continue;
                        }
                        let base = self.operating();
                        let candidate = Setting {
                            threads: base.threads,
                            kernel,
                        };
                        self.start_trial(now, Purpose::Kernel, base, candidate);
                        return;
                    }
                    self.kernels_tested_at = self.keep;
                    if self.stage == Stage::KernelsAtFinal {
                        self.settle(now, out, limit);
                        return;
                    }
                    self.stage = Stage::CountUp;
                }
                Stage::CountUp => {
                    let top = limit.min(self.max);
                    if self.climbing && self.peak.threads < top {
                        // A big step shows clearly what small ones cannot, and most
                        // machines end at or near everything that fits.
                        let candidate = if self.top_first {
                            top
                        } else {
                            self.peak.threads.saturating_add(self.step).min(top)
                        };
                        self.top_first = false;
                        if candidate > self.tested_above {
                            self.tested_above = candidate;
                            self.start_count(now, Purpose::Up, candidate);
                            return;
                        }
                    }
                    self.stage = Stage::Refine;
                }
                Stage::Refine => {
                    let gap = (self.peak.threads / 32).max(1);
                    let top = self.hi.min(limit);
                    if top.saturating_sub(self.peak.threads) > gap {
                        let candidate = (self.peak.threads + top) / 2;
                        if candidate > self.peak.threads {
                            self.start_count(now, Purpose::UpRefine, candidate);
                            return;
                        }
                    }
                    if self.keep > self.min && self.keep.saturating_sub(self.lo) > gap {
                        let candidate = (self.lo + self.keep).div_ceil(2).max(self.min);
                        if candidate < self.keep {
                            self.start_count(now, Purpose::Down, candidate);
                            return;
                        }
                    }
                    let moved =
                        self.keep.abs_diff(self.kernels_tested_at) * 4 > self.kernels_tested_at;
                    if moved && self.rounds == 0 {
                        self.rounds = 1;
                        self.queue = self.close_rivals();
                        self.stage = Stage::KernelsAtFinal;
                        continue;
                    }
                    self.settle(now, out, limit);
                    return;
                }
                Stage::Settled | Stage::Checking => return,
            }
        }
    }

    /// Kernels close enough that the count may have changed the order.
    fn close_rivals(&self) -> VecDeque<CpuKernel> {
        self.results
            .iter()
            .filter(|r| (r.ratio - 1.0).abs() < 0.10)
            .filter_map(|r| r.kernel.parse().ok())
            .filter(|k| *k != self.peak.kernel && self.candidates.contains(k))
            .collect()
    }

    fn start_count(&mut self, now: u64, purpose: Purpose, candidate: usize) {
        let base = Setting {
            threads: self.peak.threads,
            kernel: self.peak.kernel,
        };
        self.start_trial(
            now,
            purpose,
            base,
            Setting {
                threads: candidate,
                kernel: self.peak.kernel,
            },
        );
    }

    fn settle(&mut self, now: u64, out: &mut Output, limit: usize) {
        self.stage = Stage::Settled;
        self.target = self.operating();
        self.deadline = None;
        self.watch.open = None;
        self.watch.reference = None;
        self.watch.low = 0;
        self.watch.high = 0;
        self.watch.next_allowed = now + SHIFT_RETRY_SECS;
        // RAM held the count back only if the peak sits at a limit below the maximum.
        self.ram_limit = (self.peak.threads >= limit && limit < self.max).then_some(limit);
        if self.unloaded_best.is_some_and(|best| best <= self.keep) {
            self.unloaded_best = None;
        }
        out.settled_now = true;
        out.save = !self.manual;
        out.details.push(format!(
            "done — {} threads on {} after {} comparisons",
            self.keep, self.peak.kernel, self.done
        ));
        let announced = self.announced;
        self.announced = Some(self.operating());
        if announced != self.announced {
            self.pending_notes.push(Note {
                text: format!("tuned to {} threads", self.keep),
                success: true,
            });
        }
        self.settled_after_update = self.testing_update;
        self.testing_update = false;
        self.schedule_retests(now);
    }

    /// A saved result needs no comparisons if the machine still performs like it.
    fn watch_checking(&mut self, now: u64, view: &View, out: &mut Output) {
        match self.watch_window(now, view) {
            Some(rate) => {
                let Some(reference) = self.watch.reference else {
                    self.settled_hps = rate;
                    self.stage = Stage::Settled;
                    out.save = !self.manual;
                    return;
                };
                if (rate / reference - 1.0).abs() <= SHIFT {
                    self.settled_hps = rate;
                    self.watch.reference = Some(rate);
                    self.stage = Stage::Settled;
                    self.watch.next_allowed = now + SHIFT_RETRY_SECS;
                    self.pending_details
                        .push("saved setting still performs as expected".into());
                } else {
                    self.pending_details
                        .push("H/s differs from the saved result; checking the count".into());
                    self.search_reason =
                        Some("hashrate differs from last time: rechecking the thread count");
                    self.watch.reference = None;
                    self.restart_count_check(now);
                }
            }
            None => {}
        }
    }

    /// Measure at the operating setting while nothing else happens.
    fn watch_window(&mut self, now: u64, view: &View) -> Option<f64> {
        if !view.warm
            || !view.hashing
            || !view.kernel_ready
            || view.threads != self.target.threads
            || view.kernel != Some(self.target.kernel)
        {
            self.watch.open = None;
            return None;
        }
        let Some((start, first)) = self.watch.open else {
            self.watch.open = Some((now, view.hashes));
            return None;
        };
        let secs = now.saturating_sub(start);
        if secs < WATCH_WINDOW_SECS || view.hashes < first {
            return None;
        }
        self.watch.open = Some((now, view.hashes));
        Some((view.hashes - first) as f64 / secs.max(1) as f64)
    }

    fn watch_settled(&mut self, now: u64, view: &View, cap: usize, out: &mut Output) {
        let limit = self.limit(now, cap);
        // RAM that was holding the count back has come free.
        if self.ram_limit.is_some_and(|held| limit > held)
            && self.keep < self.max
            && self.ready_to_grow(now)
        {
            self.ram_limit = None;
            self.pending_details
                .push("more RAM is free; checking a higher count".into());
            self.search_reason = Some("more RAM is free: trying more threads");
            self.started_search = now;
            self.deadline = None;
            self.lo = self.keep;
            self.peak.threads = self.keep;
            self.step = (self.keep / 2).max(1);
            self.hi = limit;
            self.tested_above = self.keep;
            self.stalls = 0;
            self.climbing = true;
            self.stage = Stage::CountUp;
            return;
        }
        if self.target.threads < self.keep {
            // Limited by RAM: no measurements mean anything until it returns.
            self.watch.open = None;
            return;
        }
        // Competing programs pushed us down: look for the way back, rarely.
        if let Some(best) = self.unloaded_best {
            if best > self.keep && limit >= best && now >= self.recovery_at {
                self.recovery_at = now + self.recovery_secs;
                self.recovery_secs = (self.recovery_secs * 2).min(RECOVERY_PROBE_MAX_SECS);
                let base = self.operating();
                self.start_trial(
                    now,
                    Purpose::Restore,
                    base,
                    Setting {
                        threads: best,
                        kernel: base.kernel,
                    },
                );
                return;
            }
        }
        if now >= self.undecided_at && self.retests < MAX_RETESTS {
            let fits = |(candidate, _): &(Setting, Pooled)| candidate.threads <= limit;
            if let Some(position) = self.undecided.iter().position(fits) {
                let (candidate, pooled) = self.undecided.remove(position).unwrap();
                // Lower counts answer to the peak, so small steps cannot add up.
                let base = if candidate.threads < self.keep
                    && candidate.kernel == self.peak.kernel
                    && self.peak.threads <= limit
                {
                    self.peak
                } else {
                    self.operating()
                };
                self.undecided_at = now + UNDECIDED_RETRY_SECS;
                if candidate != base {
                    self.retests += 1;
                    self.start_trial(now, Purpose::Restore, base, candidate);
                    if let Some(trial) = &mut self.trial {
                        // Evidence against another setting says nothing about this one.
                        trial.prior = if pooled.against == Some(base) {
                            pooled
                        } else {
                            Pooled {
                                looks: pooled.looks,
                                against: Some(base),
                                ..Pooled::default()
                            }
                        };
                        let rate = if self.settled_hps > 0.0 {
                            self.settled_hps
                        } else {
                            self.last_rate
                        };
                        trial.need = long_window(candidate.threads.max(base.threads), rate);
                    }
                    return;
                }
            }
        }
        let Some(rate) = self.watch_window(now, view) else {
            return;
        };
        let Some(reference) = self.watch.reference else {
            self.watch.reference = Some(rate);
            self.settled_hps = rate;
            out.save = !self.manual;
            return;
        };
        if rate < reference * (1.0 - SHIFT) {
            self.watch.low += 1;
            self.watch.high = 0;
        } else if rate > reference * (1.0 + SHIFT) {
            self.watch.high += 1;
            self.watch.low = 0;
        } else {
            self.watch.low = 0;
            self.watch.high = 0;
            let tracked = 0.9 * reference + 0.1 * rate;
            self.watch.reference = Some(tracked);
            self.settled_hps = tracked;
            return;
        }
        if self.watch.low >= SHIFT_WINDOWS || self.watch.high >= SHIFT_WINDOWS {
            let dropped = self.watch.low >= SHIFT_WINDOWS;
            self.watch.low = 0;
            self.watch.high = 0;
            self.watch.reference = Some(rate);
            self.settled_hps = rate;
            if now < self.watch.next_allowed {
                return;
            }
            self.watch.next_allowed = now + self.watch.retry_secs;
            self.watch.retry_secs = (self.watch.retry_secs * 2).min(SHIFT_RETRY_MAX_SECS);
            self.pending_details.push(format!(
                "H/s {} and stayed there; checking whether the best count moved",
                if dropped { "fell" } else { "rose" }
            ));
            // Worth saying once. On a busy machine this repeats, and the status row
            // already explains each comparison.
            if !self.shift_noted {
                self.shift_noted = true;
                self.search_reason = Some(if dropped {
                    "hashrate fell: rechecking the thread count"
                } else {
                    "hashrate rose: rechecking the thread count"
                });
            }
            if dropped {
                // Another program may want the CPU: fewer threads could now be best.
                self.unloaded_best.get_or_insert(self.keep);
                self.recovery_at = now + RECOVERY_PROBE_SECS;
                self.recovery_secs = RECOVERY_PROBE_SECS * 2;
            }
            self.restart_count_check(now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const AVX2: CpuKernel = CpuKernel::Avx2;
    const STREAM: CpuKernel = CpuKernel::Avx2Stream;

    fn setting(threads: usize, kernel: CpuKernel) -> Setting {
        Setting { threads, kernel }
    }

    fn params(threads: usize, max: usize) -> Params {
        Params {
            seed: setting(threads, STREAM),
            quality: SeedQuality::Estimate,
            min: 1,
            max,
            down_band: TIE_BAND,
            candidates: vec![STREAM],
            saved: None,
            build: BuildFacts::default(),
        }
    }

    fn snapshot(threads: usize, kernel: CpuKernel, hashes: u64) -> CpuAdaptiveSnapshot {
        let bytes = threads as u64 * CPU_LANE_MEMORY_BYTES;
        CpuAdaptiveSnapshot {
            kernel,
            kernel_generation: 1,
            kernel_ready: true,
            backing_bytes: [0, 0, 0, bytes, 0],
            running: true,
            threads,
            committed_bytes: bytes,
            resident_bytes: bytes,
            hashes,
            hashing: threads > 0,
            allocation_failures: 0,
            warmed: threads,
        }
    }

    fn sample(hashes: u64, secs: u64) -> Sample {
        Sample {
            hashes,
            secs,
            threads: 8,
        }
    }

    /// Hash at a fixed rate per thread and obey the search's commands.
    fn run(search: &mut Search, from: u64, to: u64, threads: &mut usize, cap: usize) -> Vec<Note> {
        let mut notes = Vec::new();
        let mut hashes = from * 10;
        for now in from..to {
            hashes += *threads as u64;
            let out = search.tick(now, &[snapshot(*threads, STREAM, hashes)], cap, false);
            if let Some(next) = out.threads {
                *threads = next;
            }
            notes.extend(out.notes);
        }
        notes
    }

    #[test]
    fn fewer_threads_need_evidence_that_little_is_lost() {
        // Inside the band, but the measurement allows a real loss.
        assert!(!fewer_holds(-0.0024, 0.0075, TIE_BAND));
        // Measured faster by more than the doubt.
        assert!(fewer_holds(0.0124, 0.0058, TIE_BAND));
        // Equal, measured well.
        assert!(fewer_holds(0.0, 0.001, TIE_BAND));
        assert!(!fewer_holds(0.0, f64::INFINITY, TIE_BAND));
    }

    #[test]
    fn a_close_result_gets_its_second_look_at_once_and_later_ones_spaced_out() {
        let mut search = Search::new(params(10, 12));
        search.keep = 10;
        search.peak = setting(10, STREAM);
        search.target = setting(10, STREAM);
        let close = Pooled::summary(1, 0.003, 0.004, setting(10, STREAM));
        search.queue_retest(100, setting(12, STREAM), close, false);
        search.schedule_retests(100);
        assert_eq!(search.undecided_at, 100);
        let looked_again = Pooled::summary(2, 0.003, 0.003, setting(10, STREAM));
        search.queue_retest(100, setting(12, STREAM), looked_again, false);
        search.schedule_retests(100);
        assert_eq!(search.undecided_at, 100 + UNDECIDED_RETRY_SECS);
    }

    #[test]
    fn a_result_only_stops_being_retested_after_its_last_look() {
        let mut search = Search::new(params(10, 12));
        let evidence = |looks| Pooled::summary(looks, 0.003, 0.003, setting(10, STREAM));
        search.queue_retest(0, setting(12, STREAM), evidence(UNDECIDED_LOOKS - 1), false);
        assert_eq!(search.undecided.len(), 1);
        search.queue_retest(0, setting(12, STREAM), evidence(UNDECIDED_LOOKS), false);
        assert!(search.undecided.is_empty());
    }

    #[test]
    fn a_clear_win_decides_after_one_cycle_and_a_loss_needs_no_more() {
        let windows = [sample(1000, 20), sample(1200, 20), sample(1000, 20)];
        let (gain, error) = estimate(&windows);
        assert!((gain - 0.2).abs() < 1e-9);
        assert_eq!(judge(gain, error, TIE_BAND, false), Verdict::Better);
        let windows = [sample(1000, 20), sample(800, 20), sample(1000, 20)];
        let (gain, error) = estimate(&windows);
        assert_eq!(judge(gain, error, TIE_BAND, false), Verdict::Worse);
    }

    #[test]
    fn a_small_gain_needs_a_second_cycle_and_enough_hashes() {
        let windows = [sample(10_000, 60), sample(10_100, 60), sample(10_000, 60)];
        let (gain, error) = estimate(&windows);
        assert_eq!(judge(gain, error, TIE_BAND, false), Verdict::Undecided);
        let windows = [sample(100, 20), sample(101, 20), sample(100, 20)];
        let (gain, error) = estimate(&windows);
        assert_ne!(judge(gain, error, TIE_BAND, true), Verdict::Better);
    }

    #[test]
    fn drift_between_incumbent_windows_blocks_a_verdict() {
        let windows = [sample(1000, 20), sample(1100, 20), sample(800, 20)];
        let (gain, error) = estimate(&windows);
        assert!(gain > 0.0);
        assert_ne!(judge(gain, error, TIE_BAND, false), Verdict::Better);
    }

    #[test]
    fn identical_settings_are_a_tie_once_measured_well() {
        let windows = [
            sample(200_000, 60),
            sample(200_000, 60),
            sample(200_000, 60),
        ];
        let (gain, error) = estimate(&windows);
        assert_eq!(judge(gain, error, TIE_BAND, true), Verdict::Tie);
    }

    #[test]
    fn efficiency_profile_accepts_fewer_threads_within_its_floor() {
        let windows = [sample(10_000, 60), sample(8_500, 60), sample(10_000, 60)];
        let (gain, error) = estimate(&windows);
        assert_eq!(judge(gain, error, 0.25, true), Verdict::Tie);
        assert_eq!(judge(gain, error, TIE_BAND, true), Verdict::Worse);
    }

    #[test]
    fn ram_shortage_sheds_at_once_and_returns_after_a_hold() {
        let mut search = Search::new(params(8, 8));
        let mut threads = 8;
        run(&mut search, 0, 2, &mut threads, 8);
        let notes = run(&mut search, 2, 3, &mut threads, 3);
        assert_eq!(threads, 3);
        assert!(notes[0].text.contains("RAM is low"), "{notes:?}");
        // RAM returns: not before the hold, then all the way back.
        run(&mut search, 3, 3 + HEADROOM_HOLD_SECS - 1, &mut threads, 8);
        assert_eq!(threads, 3);
        let notes = run(&mut search, 3 + HEADROOM_HOLD_SECS - 1, 40, &mut threads, 8);
        assert_eq!(threads, 8);
        assert!(notes[0].text.contains("RAM is free"), "{notes:?}");
    }

    #[test]
    fn no_ram_parks_every_worker_and_resumes() {
        let mut search = Search::new(params(4, 4));
        let mut threads = 4;
        run(&mut search, 0, 2, &mut threads, 4);
        let notes = run(&mut search, 2, 3, &mut threads, 0);
        assert_eq!(threads, 0);
        assert!(notes[0].text.contains("paused"), "{notes:?}");
        assert_eq!(search.status(0), Status::WaitingForRam);
        run(&mut search, 3, 40, &mut threads, 4);
        assert!(threads > 0);
        // Whatever comparisons follow, it ends back on every thread.
        run(&mut search, 40, 1800, &mut threads, 4);
        assert_eq!(threads, 4);
    }

    #[test]
    fn an_allocation_failure_backs_off_even_when_ram_looks_free() {
        let mut search = Search::new(params(4, 8));
        let snap = |threads, hashes| snapshot(threads, STREAM, hashes);
        // Grant a candidate, then fail its allocation.
        let mut threads = 4;
        let mut hashes = 0;
        let mut now = 0;
        while threads == 4 && now < 400 {
            hashes += 8;
            let out = search.tick(now, &[snap(threads, hashes)], 8, false);
            if let Some(next) = out.threads {
                threads = next;
            }
            now += 1;
        }
        assert!(threads > 4);
        let out = search.tick(now, &[snap(4, hashes)], 8, true);
        assert!(out.notes.iter().any(|n| n.text.contains("RAM is low")));
        threads = 4;
        // Free RAM alone must not trigger another attempt during the backoff.
        for at in now + 1..now + ALLOCATION_RETRY_SECS - 1 {
            hashes += 8;
            let out = search.tick(at, &[snap(threads, hashes)], 8, false);
            if let Some(next) = out.threads {
                assert!(next <= 4, "grew to {next} at {at}");
                threads = next;
            }
        }
    }

    #[test]
    fn a_comparison_ends_when_its_candidate_no_longer_fits() {
        let mut search = Search::new(params(4, 8));
        let mut threads = 4;
        let mut hashes = 0;
        let mut now = 0;
        while threads == 4 && now < 400 {
            hashes += 8;
            let out = search.tick(now, &[snapshot(threads, STREAM, hashes)], 8, false);
            if let Some(next) = out.threads {
                threads = next;
            }
            now += 1;
        }
        assert!(threads > 4);
        let out = search.tick(now, &[snapshot(threads, STREAM, hashes)], 4, false);
        assert_eq!(out.threads, Some(4));
        assert!(out
            .events
            .iter()
            .any(|e| matches!(e, TrialEvent::Abort { .. })));
    }

    fn saved(threads: usize, kernel: CpuKernel, revisions: &[(&str, u32)]) -> Saved {
        Saved {
            threads,
            kernel: kernel.to_string(),
            hps: 8.0,
            ram_limit: None,
            hot_fingerprint: "hot".into(),
            kernel_revisions: revisions.iter().map(|(k, r)| (k.to_string(), *r)).collect(),
            kernels: vec![KernelResult {
                kernel: AVX2.to_string(),
                revision: 1,
                threads,
                ratio: 0.7,
            }],
            rechecks: Vec::new(),
        }
    }

    fn resume_params(
        saved: Saved,
        hot: &str,
        revisions: &[(&str, u32)],
        candidates: Vec<CpuKernel>,
    ) -> Params {
        Params {
            seed: setting(saved.threads, saved.kernel.parse().unwrap()),
            quality: SeedQuality::Measured,
            min: 1,
            max: 16,
            down_band: TIE_BAND,
            candidates,
            saved: Some(saved),
            build: BuildFacts {
                hot_fingerprint: hot.into(),
                kernel_revisions: revisions.iter().map(|(k, r)| (k.to_string(), *r)).collect(),
            },
        }
    }

    fn first_comparison(params: Params) -> Option<Setting> {
        let threads = params.seed.threads;
        let kernel = params.seed.kernel;
        let mut search = Search::new(params);
        (0..700).find_map(|now| {
            let out = search.tick(now, &[snapshot(threads, kernel, now * 8)], 16, false);
            out.events.iter().find_map(|e| match e {
                TrialEvent::Begin { candidate, .. } => Some(*candidate),
                _ => None,
            })
        })
    }

    #[test]
    fn a_build_that_changes_nothing_relevant_runs_no_comparison() {
        let revisions = [("avx2", 1), ("avx2-stream", 1)];
        let params = resume_params(
            saved(8, STREAM, &revisions),
            "hot",
            &revisions,
            vec![AVX2, STREAM],
        );
        assert_eq!(first_comparison(params), None);
    }

    #[test]
    fn a_restart_carries_on_with_results_still_in_doubt() {
        let revisions = [("avx2", 1), ("avx2-stream", 1)];
        let mut first = saved(12, STREAM, &revisions);
        let mut run = Search::new(resume_params(
            first.clone(),
            "hot",
            &revisions,
            vec![AVX2, STREAM],
        ));
        let evidence = Pooled::summary(1, 0.004, 0.003, setting(12, STREAM));
        run.queue_retest(0, setting(14, STREAM), evidence, false);
        first.rechecks = run.saved().rechecks;
        let text = serde_json::to_string(&first).unwrap();
        let again: Saved = serde_json::from_str(&text).unwrap();
        let resumed = Search::new(resume_params(again, "hot", &revisions, vec![AVX2, STREAM]));
        assert_eq!(resumed.undecided.len(), 1);
        assert_eq!(resumed.undecided[0].0, setting(14, STREAM));
        let (gain, error) = resumed.undecided[0].1.estimate();
        assert!((gain - 0.004).abs() < 1e-9 && (error - 0.003).abs() < 1e-9);
        assert_eq!(resumed.undecided_at, 0);
        // A build that changes hashing has its own checks to run.
        let changed = Search::new(resume_params(
            first,
            "other",
            &revisions,
            vec![AVX2, STREAM],
        ));
        assert!(changed.undecided.is_empty());
    }

    #[test]
    fn a_new_kernel_is_compared_at_the_saved_count() {
        let before = [("avx2", 1), ("avx2-stream", 1)];
        let after = [("avx2", 1), ("avx2-stream", 1), ("avx512-stream", 1)];
        let params = resume_params(
            saved(8, STREAM, &before),
            "hot",
            &after,
            vec![AVX2, STREAM, CpuKernel::Avx512Stream],
        );
        assert_eq!(
            first_comparison(params),
            Some(setting(8, CpuKernel::Avx512Stream))
        );
    }

    #[test]
    fn an_improved_kernel_is_compared_even_if_it_lost_before() {
        let before = [("avx2", 1), ("avx2-stream", 1)];
        let after = [("avx2", 2), ("avx2-stream", 1)];
        let params = resume_params(saved(8, STREAM, &before), "new", &after, vec![AVX2, STREAM]);
        assert_eq!(first_comparison(params), Some(setting(8, AVX2)));
    }

    #[test]
    fn another_tweak_rechecks_the_closest_rival_and_the_count() {
        let revisions = [("avx2", 1), ("avx2-stream", 1)];
        let params = resume_params(
            saved(8, STREAM, &revisions),
            "new",
            &revisions,
            vec![AVX2, STREAM],
        );
        assert_eq!(first_comparison(params), Some(setting(8, AVX2)));
    }

    #[test]
    fn a_saved_count_that_does_not_fit_stays_the_count_to_return_to() {
        let revisions = [("avx2-stream", 1)];
        let mut params = resume_params(
            saved(12, STREAM, &revisions),
            "hot",
            &revisions,
            vec![STREAM],
        );
        params.seed.threads = 6;
        let mut search = Search::new(params);
        let mut threads = 6;
        run(&mut search, 0, 5, &mut threads, 6);
        assert_eq!(threads, 6);
        assert_eq!(
            search.status(6),
            Status::LimitedByRam {
                threads: 6,
                best: 12
            }
        );
        run(&mut search, 5, 50, &mut threads, 16);
        assert_eq!(threads, 12);
    }

    #[test]
    fn a_rig_is_judged_on_every_backend_not_cpu_hashes_alone() {
        // More CPU threads raise CPU hashes but slow the GPU by more.
        let rig_rate = |threads: u64| match threads {
            0..=6 => 60 + 8 * threads,
            _ => 108 - 3 * (threads - 6),
        };
        let mut search = Search::new(params(6, 8));
        let (mut threads, mut cpu, mut total) = (6usize, 0u64, 0u64);
        for now in 0..1800 {
            cpu += threads as u64;
            total += rig_rate(threads as u64) * 4;
            let out = search.tick_rig(
                now,
                &[snapshot(threads, STREAM, cpu)],
                8,
                false,
                Some(total),
            );
            if let Some(next) = out.threads {
                threads = next;
            }
        }
        assert_eq!(threads, 6);
    }

    #[test]
    fn a_change_in_available_cpus_rechecks_a_settled_count() {
        let revisions = [("avx2-stream", 1)];
        let mut search = Search::new(resume_params(
            saved(8, STREAM, &revisions),
            "hot",
            &revisions,
            vec![STREAM],
        ));
        search.observe_parallelism(0, 16);
        let mut threads = 8;
        let mut hashes = 0;
        let mut began = |search: &mut Search, from: u64, to: u64| {
            let mut any = false;
            for now in from..to {
                hashes += threads as u64;
                let out = search.tick(now, &[snapshot(threads, STREAM, hashes)], 16, false);
                if let Some(next) = out.threads {
                    threads = next;
                }
                any |= out
                    .events
                    .iter()
                    .any(|e| matches!(e, TrialEvent::Begin { .. }));
            }
            any
        };
        assert!(
            !began(&mut search, 0, 700),
            "no comparison while nothing changes"
        );
        search.observe_parallelism(700, 16);
        assert!(
            !began(&mut search, 700, 800),
            "the same value is not a change"
        );
        search.observe_parallelism(800, 8);
        assert!(began(&mut search, 800, 900));
    }

    /// Run until a comparison of thread counts is under way.
    fn until_comparing_threads(search: &mut Search) -> (Status, Vec<String>) {
        let mut threads = search.target_threads();
        let mut notes = Vec::new();
        let mut hashes = 0;
        for now in 0..2000 {
            hashes += threads as u64;
            let out = search.tick(now, &[snapshot(threads, STREAM, hashes)], 16, false);
            notes.extend(out.notes.into_iter().map(|n| n.text));
            if let Some(next) = out.threads {
                threads = next;
            }
            let status = search.status(16);
            if let Status::Tuning {
                comparing: Comparing::Threads(..),
                ..
            } = status
            {
                return (status, notes);
            }
        }
        panic!("no thread comparison");
    }

    #[test]
    fn a_moving_thread_count_is_explained_once_in_the_log_and_always_in_the_status() {
        let (status, notes) = until_comparing_threads(&mut Search::new(params(8, 16)));
        assert!(matches!(
            status,
            Status::Tuning {
                comparing: Comparing::Threads(8, _),
                ..
            }
        ));
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert!(notes[0].contains("thread count"), "{notes:?}");
    }

    #[test]
    fn a_pinned_count_gets_no_note_about_moving_threads() {
        let mut params = params(4, 4);
        params.min = 4;
        params.candidates = vec![STREAM, AVX2];
        let mut search = Search::new(params);
        for now in 0..30 {
            let out = search.tick(now, &[snapshot(4, STREAM, now * 8)], 4, false);
            assert!(out.notes.is_empty(), "{:?}", out.notes);
        }
    }

    #[test]
    fn the_same_result_is_announced_only_once() {
        let mut search = Search::new(params(8, 8));
        let mut threads = 8;
        let mut hashes = 0;
        let mut tuned = 0;
        let mut settled = 0;
        for now in 0..6000 {
            // Something changes twice, and the search comes back to the same answer.
            if now == 2000 || now == 4000 {
                search.observe_parallelism(now, 8);
                search.observe_parallelism(now, 4);
            }
            hashes += threads as u64;
            let out = search.tick(now, &[snapshot(threads, STREAM, hashes)], 8, false);
            if let Some(next) = out.threads {
                threads = next;
            }
            settled += u32::from(out.settled_now);
            tuned += out
                .notes
                .iter()
                .filter(|n| n.text.starts_with("tuned to"))
                .count();
        }
        assert!(settled >= 2, "settled {settled} times");
        assert_eq!(tuned, 1);
    }

    #[test]
    fn small_shifts_in_large_page_coverage_do_not_end_a_comparison() {
        let gib = 1024 * 1024 * 1024u64;
        let mostly = [[0, 0, 15 * gib, gib, 0]];
        let nearly_all = [[0, 0, 16 * gib, 0, 0]];
        let regular = [[0, 0, 0, 16 * gib, 0]];
        assert!(same_backing(&mostly, &nearly_all));
        assert!(!same_backing(&nearly_all, &regular));
        assert!(!same_backing(&mostly, &[mostly[0], mostly[0]]));
    }

    #[test]
    fn a_kernel_interrupted_by_a_ram_change_keeps_its_turn() {
        let mut params = params(4, 4);
        params.candidates = vec![STREAM, AVX2];
        let mut search = Search::new(params);
        let mut began = 0;
        let mut hashes = 0;
        for now in 0..40 {
            hashes += 4;
            // A RAM drop cancels the running comparison; RAM then returns.
            let cap = if now == 5 { 2 } else { 4 };
            let threads = if (6..38).contains(&now) { 2 } else { 4 };
            let out = search.tick(now, &[snapshot(threads, STREAM, hashes)], cap, false);
            began += out
                .events
                .iter()
                .filter(|e| matches!(e, TrialEvent::Begin { candidate, .. } if candidate.kernel == AVX2))
                .count();
        }
        assert!(began >= 1);
        assert!(search.queue.contains(&AVX2) || search.trial.is_some());
    }

    #[test]
    fn settling_below_the_ram_limit_is_not_waiting_for_more_ram() {
        // The peak sits at the limit but fewer threads tied it: nothing was held back
        // until the limit itself rises.
        let mut search = Search::new(params(8, 16));
        search.peak.threads = 14;
        search.keep = 13;
        let mut out = Output::default();
        search.settle(0, &mut out, 14);
        assert_eq!(search.ram_limit, Some(14));
        let mut threads = 13;
        let mut hashes = 0;
        for now in 0..400 {
            hashes += threads as u64;
            let out = search.tick(now, &[snapshot(threads, STREAM, hashes)], 14, false);
            assert!(!out
                .events
                .iter()
                .any(|e| matches!(e, TrialEvent::Begin { .. })));
            if let Some(next) = out.threads {
                threads = next;
            }
        }
        let mut began = false;
        for now in 400..500 {
            hashes += threads as u64;
            let out = search.tick(now, &[snapshot(threads, STREAM, hashes)], 16, false);
            began |= out
                .events
                .iter()
                .any(|e| matches!(e, TrialEvent::Begin { .. }));
            if let Some(next) = out.threads {
                threads = next;
            }
        }
        assert!(began, "the limit rose, so a higher count is worth a look");
    }

    #[test]
    fn a_count_set_from_the_dashboard_is_kept_but_never_saved() {
        let mut search = Search::new(params(8, 16));
        search.set_manual(3);
        let mut threads = 8;
        let mut hashes = 0;
        for now in 0..1200 {
            hashes += threads as u64;
            let out = search.tick(now, &[snapshot(threads, STREAM, hashes)], 16, false);
            if let Some(next) = out.threads {
                threads = next;
            }
            assert!(!out.save, "saved at {now}");
        }
        assert_eq!(threads, 3);
        assert_eq!(search.saved().threads, 3);
    }

    #[test]
    fn a_pinned_count_never_changes_but_kernels_still_compare() {
        let mut params = params(4, 4);
        params.min = 4;
        params.candidates = vec![STREAM, AVX2];
        assert_eq!(first_comparison(params), Some(setting(4, AVX2)));
    }
}
