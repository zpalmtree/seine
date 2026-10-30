//! Bounded kernel trials on productive work, serialized with worker-count trials.
//! Architecture eligibility permits a trial; only repeated gains select a path.
use super::*;
use crate::backend::cpu::CpuAdaptiveSnapshot;
use blocknet_pow_kernel::CpuKernel;
use seine_telemetry::tuning::{
    TrialArm, TrialDecision, TrialPolicy, TrialQuantization, TrialReason,
};

const RECHECK_SECS: u64 = 30 * 60;
const RETRY_SECS: u64 = 5 * 60;
const MAX_TRIAL_SECS: u64 = 10 * 60;
const MAX_DRIFT: f64 = 0.05;

const CACHE_SCHEMA: u32 = 3;

// Plain AVX-512/AVX2 stay a safety-net comparison, deferred for the first half hour
// when mining started on a streaming kernel.
const GENERIC_TRIAL_DEFER_SECS: u64 = 1800;

fn defer_generic_trial(
    startup: Option<CpuKernel>,
    base: CpuKernel,
    candidate: CpuKernel,
    now: u64,
) -> bool {
    now < GENERIC_TRIAL_DEFER_SECS
        && startup == Some(base)
        && matches!(
            base,
            CpuKernel::Avx512256Stream
                | CpuKernel::Avx512256StreamL2
                | CpuKernel::Avx512StreamL2
                | CpuKernel::Avx512Stream
                | CpuKernel::Avx2Stream
        )
        && matches!(candidate, CpuKernel::Avx512 | CpuKernel::Avx2)
}

// Shorter samples only for Turin (family 1Ah model 02h).
// Slow samples and explicitly lengthened trials retain the existing window.
fn short_trials_for(
    identity: &crate::runtime_identity::CpuIdentity,
    candidates: &[CpuKernel],
    configured_secs: u64,
) -> bool {
    configured_secs == 6
        && identity.vendor.as_deref() == Some("AuthenticAMD")
        && identity.family == Some(0x1a)
        && identity.model == Some(0x02)
        && candidates.contains(&CpuKernel::Avx512256Stream)
}

fn sample_ready(sample: Sample, window_secs: u64, short_trials: bool) -> bool {
    (sample.secs >= window_secs && sample.hashes >= 32)
        || (short_trials && sample.secs >= 10 && sample.hashes >= 256)
}

// ARM starts on the portable NEON path; SHA3 is eligible only after runtime
// feature detection and must win two productive-work comparisons before it
// becomes the cached choice.
fn supported_kernels() -> Vec<CpuKernel> {
    CpuKernel::ALL
        .into_iter()
        .filter(|k| k.is_supported())
        .collect()
}

#[cfg(test)]
fn candidates() -> Vec<CpuKernel> {
    // Synthetic policy fixtures explicitly use the identity-neutral set.
    candidates_for(
        &crate::runtime_identity::CpuIdentity::default(),
        &supported_kernels(),
    )
}

fn candidates_for(
    identity: &crate::runtime_identity::CpuIdentity,
    supported: &[CpuKernel],
) -> Vec<CpuKernel> {
    let mut paths: Vec<_> = [
        CpuKernel::Avx512,
        CpuKernel::Avx2,
        CpuKernel::Neon,
        CpuKernel::NeonSha3,
    ]
    .into_iter()
    .filter(|k| supported.contains(k))
    .collect();
    // Gate streaming by microarchitecture rather than exact model so every Zen 4/5
    // part (Raphael, Turin Dense, masked cloud CPUIDs) is eligible. The independent
    // ISA/OS support check remains mandatory, and repeated-win trials can still
    // replace it.
    if let Some(stream) = cohort_stream_kernel(identity, supported) {
        if stream == CpuKernel::Avx512StreamSkip {
            // The plain path stays a challenger so the tuner can turn skipping off.
            paths.push(stream);
            paths.push(CpuKernel::Avx512Stream);
        } else if stream == CpuKernel::Avx512256Stream && identity.model == Some(0x02) {
            // Turin: test the high-concurrency challenger before AVX2.
            // Ordering changes discovery cost, never the repeated-win gate.
            let before_avx2 = paths
                .iter()
                .position(|k| *k == CpuKernel::Avx2)
                .unwrap_or(paths.len());
            paths.insert(before_avx2, stream);
        } else {
            paths.push(stream);
        }
    } else if let Some(stream) = matches!(
        identity.vendor.as_deref(),
        Some("AuthenticAMD" | "GenuineIntel")
    )
    .then(|| {
        [CpuKernel::Avx512256Stream, CpuKernel::Avx2Stream]
            .into_iter()
            .find(|k| supported.contains(k))
    })
    .flatten()
    {
        // Hosts without a cohort default (Intel, masked-CPUID AMD VMs, Zen 1) keep
        // their start, but may discover streaming through the repeated-win gate.
        paths.push(stream);
    }
    paths
}

/// The streaming kernel for an AMD microarchitecture cohort, if validated.
/// Zen 4 (family 19h with AVX-512) and Zen 5 (family 1Ah) use the AVX-512 path.
/// Zen 3 (family 19h without AVX-512) and Zen 2 (family 17h, model 30h and up)
/// use the AVX2 path. Zen 1/Zen+ (17h below 30h) has no default, and the AVX2
/// path is enabled only on Linux and Windows. Callers always intersect with
/// runtime-detected kernels, so ISA support is never assumed.
const AVX2_STREAM_OS: bool = cfg!(any(target_os = "linux", target_os = "windows", test));

fn cohort_stream_kernel(
    identity: &crate::runtime_identity::CpuIdentity,
    supported: &[CpuKernel],
) -> Option<CpuKernel> {
    // Intel Sapphire Rapids (06h/8Fh) and Emerald Rapids (06h/CFh, same Raptor
    // Cove core) use full-width streaming with the register-resident compress.
    if identity.vendor.as_deref() == Some("GenuineIntel") {
        if identity.family != Some(6) {
            return None;
        }
        // Alder Lake and Raptor Lake desktops (AVX2 only) use the AVX2 streaming path.
        if matches!(identity.model, Some(0x97 | 0x9a | 0xb7 | 0xba | 0xbf)) {
            return (AVX2_STREAM_OS && supported.contains(&CpuKernel::Avx2Stream))
                .then_some(CpuKernel::Avx2Stream);
        }
        if !matches!(identity.model, Some(0x8f | 0xcf)) {
            return None;
        }
        return [CpuKernel::Avx512Stream, CpuKernel::Avx512256Stream]
            .into_iter()
            .find(|k| supported.contains(k));
    }
    if identity.vendor.as_deref() != Some("AuthenticAMD") {
        return None;
    }
    match identity.family {
        // Zen 5 server-class models (00h-1Fh: Turin, Turin Dense, masked cloud
        // CPUIDs) use full-width compression. Client Zen 5 (20h+) stays 256-bit.
        // Zen 5c (16 cores per L3) starts with skipped stores; the plain path is
        // compared against it while mining.
        Some(0x1a)
            if identity.model.is_some_and(|m| m < 0x20)
                && identity.l3_cores.is_some_and(|cores| cores >= 16)
                && supported.contains(&CpuKernel::Avx512StreamSkip) =>
        {
            Some(CpuKernel::Avx512StreamSkip)
        }
        Some(0x1a)
            if identity.model.is_some_and(|m| m < 0x20)
                && supported.contains(&CpuKernel::Avx512Stream) =>
        {
            Some(CpuKernel::Avx512Stream)
        }
        // Zen 4: far prefetch with full-width compression.
        Some(0x19) if supported.contains(&CpuKernel::Avx512StreamL2) => {
            Some(CpuKernel::Avx512StreamL2)
        }
        Some(0x19) if supported.contains(&CpuKernel::Avx512256StreamL2) => {
            Some(CpuKernel::Avx512256StreamL2)
        }
        Some(0x19 | 0x1a) if supported.contains(&CpuKernel::Avx512256Stream) => {
            Some(CpuKernel::Avx512256Stream)
        }
        Some(0x19) if AVX2_STREAM_OS && supported.contains(&CpuKernel::Avx2Stream) => {
            Some(CpuKernel::Avx2Stream)
        }
        Some(0x17)
            if AVX2_STREAM_OS
                && identity.model.is_some_and(|m| m >= 0x30)
                && supported.contains(&CpuKernel::Avx2Stream) =>
        {
            Some(CpuKernel::Avx2Stream)
        }
        // No AMD CPU with AVX2 reports family 06h: these are hypervisor CPU models
        // that hide the real Zen family and AVX-512. Use the AVX2 streaming path.
        Some(6) if AVX2_STREAM_OS && supported.contains(&CpuKernel::Avx2Stream) => {
            Some(CpuKernel::Avx2Stream)
        }
        _ => None,
    }
}

// A/B/A trials on a busy desktop rarely clear the baseline-drift gate, so start
// Zen 4/5 contexts on streaming at this many workers or more; the other candidates
// still get the usual two comparisons against it.
const STARTUP_STREAM_MIN_WORKERS: usize = 8;

fn startup_kernel_for(
    identity: &crate::runtime_identity::CpuIdentity,
    candidates: &[CpuKernel],
) -> Option<CpuKernel> {
    cohort_stream_kernel(identity, candidates)
}

/// The cohort kernel regardless of worker count, for the worker-count search:
/// every candidate count must be measured with the kernel that will mine.
pub(super) fn cohort_kernel() -> Option<CpuKernel> {
    let identity = crate::runtime_identity::cpu_identity();
    cohort_stream_kernel(&identity, &candidates_for(&identity, &supported_kernels()))
}

/// Whether a fixed `--threads` count on this host should still run the kernel
/// comparisons at that count: where no cohort default exists but a streaming
/// kernel is supported, and where the default is the skipped-store path. Other
/// hosts with a cohort default run no trials at an explicit count.
pub(crate) fn fixed_count_trials_wanted() -> bool {
    fixed_count_trials_wanted_for(
        &crate::runtime_identity::cpu_identity(),
        &supported_kernels(),
    )
}

/// Also true where the cohort default is the skipped-store path, which is always
/// compared with the plain path, including at an explicit `--threads` count.
fn fixed_count_trials_wanted_for(
    identity: &crate::runtime_identity::CpuIdentity,
    supported: &[CpuKernel],
) -> bool {
    match cohort_stream_kernel(identity, supported) {
        Some(CpuKernel::Avx512StreamSkip) => true,
        Some(_) => false,
        None => candidates_for(identity, supported)
            .iter()
            .any(|kernel| matches!(kernel, CpuKernel::Avx512256Stream | CpuKernel::Avx2Stream)),
    }
}

/// Side-cache slots for this host (see `SideCacheSchedule` in the kernel crate),
/// or 0. On for Zen 5 servers, Zen 5 desktops below the full thread count, and
/// Intel Sapphire/Emerald Rapids at the full count; off elsewhere.
pub(crate) fn cohort_side_cache_blocks(
    workers: usize,
    physical_cores: Option<usize>,
    logical_cpus: usize,
) -> usize {
    if !cfg!(target_os = "linux") || !CpuKernel::Avx512Stream.is_supported() {
        return 0;
    }
    let identity = crate::runtime_identity::cpu_identity();
    match identity.vendor.as_deref() {
        // Zen 5 server-class models (00h-1Fh).
        Some("AuthenticAMD")
            if identity.family == Some(0x1a)
                && identity.model.is_some_and(|model| model < 0x20) =>
        {
            let _ = physical_cores;
            512
        }
        // Zen 5 desktop, below the full thread count.
        Some("AuthenticAMD") if identity.family == Some(0x1a) && workers < logical_cpus => 512,
        Some("GenuineIntel")
            if identity.family == Some(6)
                && matches!(identity.model, Some(0x8f | 0xcf))
                && workers >= logical_cpus.max(2) =>
        {
            1024
        }
        _ => 0,
    }
}

/// Skipped-store permille for this host (see `set_skip_store_permille` in the
/// kernel crate), or 0. On for Zen 5 desktops at 8 or more workers on Linux and
/// Windows, and on Linux for Zen 4c EPYC at 128 or more, Zen 3 EPYC at 24 or more,
/// and Zen 4 and Zen 3 desktops, Zen 2 EPYC and Threadripper 3000 at 12 or more;
/// off elsewhere.
pub(crate) fn cohort_skip_store_permille(workers: usize) -> u16 {
    let linux = cfg!(target_os = "linux");
    if !(linux || cfg!(windows)) {
        return 0;
    }
    let avx512 = CpuKernel::Avx512Stream.is_supported();
    let identity = crate::runtime_identity::cpu_identity();
    match identity.vendor.as_deref() {
        // Raphael and Dragon Range (Ryzen 7000 desktop and HX laptop), Linux.
        Some("AuthenticAMD")
            if linux
                && avx512
                && identity.family == Some(0x19)
                && identity.model == Some(0x61)
                && workers >= 12 =>
        {
            600
        }
        // Granite Ridge and Fire Range (Ryzen 9000 desktop and HX laptop), Linux and Windows.
        Some("AuthenticAMD")
            if avx512
                && identity.family == Some(0x1a)
                && identity.model == Some(0x44)
                && workers >= 8 =>
        {
            600
        }
        // Bergamo and Siena (Zen 4c EPYC), Linux.
        Some("AuthenticAMD")
            if linux
                && avx512
                && identity.family == Some(0x19)
                && identity
                    .model
                    .is_some_and(|model| (0xa0..=0xaf).contains(&model))
                && workers >= 128 =>
        {
            600
        }
        // Milan and Milan-X (Zen 3 EPYC), Linux.
        Some("AuthenticAMD")
            if linux
                && CpuKernel::Avx2Stream.is_supported()
                && identity.family == Some(0x19)
                && identity.model == Some(0x01)
                && workers >= 24 =>
        {
            600
        }
        // Vermeer (Ryzen 5000 desktop), Linux.
        Some("AuthenticAMD")
            if linux
                && CpuKernel::Avx2Stream.is_supported()
                && identity.family == Some(0x19)
                && identity.model == Some(0x21)
                && workers >= 12 =>
        {
            600
        }
        // Rome and Castle Peak (Zen 2 EPYC and Threadripper 3000), Linux.
        Some("AuthenticAMD")
            if linux
                && CpuKernel::Avx2Stream.is_supported()
                && identity.family == Some(0x17)
                && identity.model == Some(0x31)
                && workers >= 12 =>
        {
            600
        }
        _ => 0,
    }
}

/// Kernel for fixed worker counts (explicit `--threads`), which run no trials.
/// Uses the same cohort default the adaptive path starts from.
pub(super) fn static_kernel_for(workers: usize) -> Option<CpuKernel> {
    static_kernel_for_identity(
        &crate::runtime_identity::cpu_identity(),
        &supported_kernels(),
        workers,
    )
}

fn static_kernel_for_identity(
    identity: &crate::runtime_identity::CpuIdentity,
    supported: &[CpuKernel],
    workers: usize,
) -> Option<CpuKernel> {
    (workers >= STARTUP_STREAM_MIN_WORKERS)
        .then(|| startup_kernel_for(identity, &candidates_for(identity, supported)))
        .flatten()
}

// Host eligibility must not depend on the initial worker count: a cached count
// can differ on the next launch without changing the kernel policy. The actual
// startup action remains restricted to the 32-worker/full-THP context
// below. Normal repeated comparisons can still replace the starting kernel.
fn genoa_stream_startup(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    candidates: &[CpuKernel],
) -> bool {
    host.os == "linux"
        && candidates.contains(&CpuKernel::Avx512256Stream)
        && super::adaptive_cpu::full_count_cold_start(
            cfg,
            host,
            identity,
            32,
            cfg.cpu_auto_threads_cap,
        )
}

// Streaming startup for this exact host context.
// Actual full THP backing is checked separately before applying the startup hint.
fn sapphire_stream_startup(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    supported: &[CpuKernel],
) -> bool {
    supported.contains(&CpuKernel::Avx512256Stream)
        && super::adaptive_cpu::sapphire_full_count_cold_start(
            cfg,
            host,
            identity,
            32,
            cfg.cpu_auto_threads_cap,
        )
}

// Turin cold-start context.
// Actual full THP backing is checked separately before applying the startup hint.
fn turin_stream_startup(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    supported: &[CpuKernel],
) -> bool {
    supported.contains(&CpuKernel::Avx512256Stream)
        && super::adaptive_cpu::turin_full_count_cold_start(
            cfg,
            host,
            identity,
            32,
            cfg.cpu_auto_threads_cap,
        )
}

fn turin_demand_startup(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    supported: &[CpuKernel],
) -> bool {
    supported.contains(&CpuKernel::Avx512256StreamR64D3)
        && turin_stream_startup(cfg, host, identity, supported)
}

fn turin_prefetch_startup(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    supported: &[CpuKernel],
) -> bool {
    supported.contains(&CpuKernel::Avx512256StreamR64D4)
        && turin_demand_startup(cfg, host, identity, supported)
}

// Depth six over depth four on the same Turin context.
fn turin_depth6_startup(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    supported: &[CpuKernel],
) -> bool {
    supported.contains(&CpuKernel::Avx512256StreamR64D6)
        && turin_prefetch_startup(cfg, host, identity, supported)
}

fn genoa_stream_context(context: &Context) -> bool {
    context.members.len() == 1
        && context.workers == [32]
        && context.backing == [[0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0, 0]]
}

// Discovering 32 workers on Windows takes many minutes, so start there while
// retaining exact backing, ISA, resource and repeated-comparison gates.
fn windows_genoa_stream_startup(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    candidates: &[CpuKernel],
) -> bool {
    host.os == "windows"
        && candidates.contains(&CpuKernel::Avx512256Stream)
        && super::adaptive_cpu::full_count_cold_start(
            cfg,
            host,
            identity,
            32,
            cfg.cpu_auto_threads_cap,
        )
}

fn windows_genoa_stream_context(context: &Context) -> bool {
    context.members.len() == 1
        && context.workers == [32]
        && context.backing == [[0, 0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0]]
}

// Shorter samples only for these Zen 4 contexts.
// Require enough completed hashes for each ten-second sample, retaining the
// A/B/A drift gate and two-cycle promotion rule.
fn genoa_short_trials_for(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    candidates: &[CpuKernel],
) -> bool {
    host.os == "linux"
        && cfg.cpu_autotune_secs == 6
        && candidates.contains(&CpuKernel::Avx512256Stream)
        && super::adaptive_cpu::full_count_cold_start(
            cfg,
            host,
            identity,
            cfg.cpu_auto_threads_cap,
            cfg.cpu_auto_threads_cap,
        )
}

fn genoa_short_context(context: &Context) -> bool {
    let [workers] = context.workers.as_slice() else {
        return false;
    };
    context.members == [0]
        && (30..=32).contains(workers)
        && context.backing == [[0, 0, *workers as u64 * CPU_LANE_MEMORY_BYTES, 0, 0]]
}

// Prioritize streaming on the first sweep only.
// A saved winner makes subsequent periodic sweeps exhaustive again.
fn genoa_frontier_context(context: &Context) -> bool {
    genoa_short_context(context) && matches!(context.workers.as_slice(), [31 | 32])
}

// Prioritize streaming at 31/32 workers with ordinary pages on Windows.
// Subsequent periodic sweeps remain
// exhaustive once this context has a saved winner.
fn windows_genoa_frontier_for(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    candidates: &[CpuKernel],
) -> bool {
    cfg.cpu_autotune_secs == 6
        && candidates.contains(&CpuKernel::Avx512256StreamR8D2)
        && windows_genoa_stream_startup(cfg, host, identity, candidates)
}

fn windows_genoa_frontier_context(context: &Context) -> bool {
    let [workers] = context.workers.as_slice() else {
        return false;
    };
    context.members == [0]
        && matches!(*workers, 31 | 32)
        && context.backing == [[0, 0, 0, *workers as u64 * CPU_LANE_MEMORY_BYTES, 0]]
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
    // Challengers that lost decisively; skipped at nearby worker counts.
    #[serde(default)]
    decisive: Vec<DecisiveLoss>,
}

// A challenger at or below this share of the incumbent's rate in a stable A/B/A
// is not retried at nearby counts, so known losers are not re-run at every count
// probe.
const DECISIVE_LOSS_RATIO: f64 = 0.85;

#[derive(Clone, Serialize, Deserialize)]
struct DecisiveLoss {
    base: String,
    loser: String,
    workers: usize,
    timestamp: u64,
}

impl DecisiveLoss {
    fn applies(&self, base: CpuKernel, loser: CpuKernel, workers: usize, wall_now: u64) -> bool {
        self.base == base.as_str()
            && self.loser == loser.as_str()
            && workers <= self.workers.saturating_mul(2)
            && workers.saturating_mul(2) >= self.workers
            && self.timestamp <= wall_now
            && wall_now - self.timestamp < CPU_AUTOTUNE_RECORD_MAX_AGE_SECS
    }
}

impl Cache {
    fn compatible(&self, host: &CpuAutotuneHostFingerprint, configuration: &str) -> bool {
        self.schema == CACHE_SCHEMA && self.host == *host && self.configuration == configuration
    }

    fn winner(&self, context: &Context, now: u64, paths: &[CpuKernel]) -> Option<CpuKernel> {
        self.winners.iter().rev().find_map(|winner| {
            let kernel = winner.kernel.parse::<CpuKernel>().ok()?;
            let base = winner.base.parse::<CpuKernel>().ok()?;
            let candidate = winner.candidate.parse::<CpuKernel>().ok()?;
            let compare = |a, b, c| comparison_for(base, candidate, a, b, c);
            (winner.context == *context
                && winner.timestamp <= now
                && now - winner.timestamp < CPU_AUTOTUNE_RECORD_MAX_AGE_SECS
                && paths.contains(&kernel)
                && winner.samples.iter().all(|s| s.secs > 0 && s.hashes > 0)
                && if winner.kernel == winner.base {
                    match winner.samples.as_slice() {
                        [a, b, c] => compare(*a, *b, *c) == Comparison::NoConfirmedGain,
                        [a, b, c, d, e, f] => {
                            compare(*a, *b, *c) == Comparison::Gain
                                && compare(*d, *e, *f) == Comparison::NoConfirmedGain
                        }
                        _ => false,
                    }
                } else {
                    winner.kernel == winner.candidate
                        && winner.samples.len() == 6
                        && compare(winner.samples[0], winner.samples[1], winner.samples[2])
                            == Comparison::Gain
                        && compare(winner.samples[3], winner.samples[4], winner.samples[5])
                            == Comparison::Gain
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

// Rates behind each decision, so productive trials can be audited from logs.
fn rates(samples: &[Sample], base: CpuKernel, candidate: CpuKernel) -> String {
    samples
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let kernel = if i % 3 == 1 { candidate } else { base };
            format!("{kernel} {:.2}", s.hps())
        })
        .collect::<Vec<_>>()
        .join(" / ")
}

#[cfg(test)]
fn improves(before: Sample, candidate: Sample, after: Sample) -> bool {
    comparison(before, candidate, after) == Comparison::Gain
}

fn turin_small_gain_pair(base: CpuKernel, candidate: CpuKernel) -> bool {
    let measured_path = |kernel| {
        matches!(
            kernel,
            CpuKernel::Avx512256Stream
                | CpuKernel::Avx512256StreamR64D3
                | CpuKernel::Avx512256StreamR64D4
                | CpuKernel::Avx512256StreamR64D6
        )
    };
    base != candidate && measured_path(base) && measured_path(candidate)
}

fn turin_pair_resolution(base: CpuKernel, candidate: CpuKernel) -> (u64, u64) {
    if [base, candidate].contains(&CpuKernel::Avx512256StreamR64D6) {
        // Resolve a small gain above three-window hash quantization.
        (45, 2048)
    } else {
        (30, 1024)
    }
}

fn comparison_for(
    base: CpuKernel,
    path: CpuKernel,
    before: Sample,
    candidate: Sample,
    after: Sample,
) -> Comparison {
    if turin_small_gain_pair(base, path) {
        // Fence/prefetch gains are below the generic 2% switching margin.
        // Require longer, well-resolved samples and tighter baseline stability;
        // both fresh A/B/A cycles still have to win before promotion/caching.
        let (seconds, hashes) = turin_pair_resolution(base, path);
        if [before, candidate, after]
            .iter()
            .any(|s| s.secs < seconds || s.hashes < hashes)
        {
            return Comparison::Inconclusive;
        }
        comparison_with_limits(before, candidate, after, 0.0, 0.01)
    } else {
        comparison(before, candidate, after)
    }
}

fn comparison(before: Sample, candidate: Sample, after: Sample) -> Comparison {
    comparison_with_limits(before, candidate, after, 0.02, MAX_DRIFT)
}

fn comparison_with_limits(
    before: Sample,
    candidate: Sample,
    after: Sample,
    margin: f64,
    max_drift: f64,
) -> Comparison {
    if [before, candidate, after]
        .iter()
        .any(|s| s.secs == 0 || s.hashes == 0)
    {
        return Comparison::Inconclusive;
    }
    if (after.hps() / before.hps() - 1.0).abs() > max_drift {
        return Comparison::Inconclusive;
    }
    let baseline =
        (before.hashes as f64 + after.hashes as f64) / (before.secs as f64 + after.secs as f64);
    let quantization =
        1.0 / before.secs as f64 + 1.0 / candidate.secs as f64 + 1.0 / after.secs as f64;
    if candidate.hps() > baseline * (1.0 + margin) + quantization {
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
    pub interrupted: bool,
    pub message: Option<String>,
}

pub(super) struct Tuner {
    pub(super) monitor: Option<super::adaptive_telemetry::Monitor>,
    cache: Cache,
    // An exact prior policy may seed mining, but cannot certify a new candidate.
    startup_cache: Option<Cache>,
    path: std::path::PathBuf,
    trial: Option<Trial>,
    checked: Option<Context>,
    next_check: u64,
    retries: Vec<(Context, u64)>,
    window_secs: u64,
    short_trials: bool,
    genoa_short_trials: bool,
    genoa_startup_frontier: bool,
    candidates: Vec<CpuKernel>,
    // Remaining challengers for this count/backing. Cache only a complete sweep.
    pending: Vec<CpuKernel>,
    // Cached/final winners must finish switching before thread sampling resumes.
    settling: Option<CpuKernel>,
    settle_started: u64,
    // Identity-validated starting path for uncached contexts, if any.
    startup: Option<CpuKernel>,
    genoa_startup: bool,
    windows_genoa_startup: bool,
    sapphire_startup: bool,
    turin_startup: bool,
}

impl Tuner {
    pub(super) fn new(cfg: &Config, host: CpuAutotuneHostFingerprint) -> Self {
        Self::for_identity(cfg, host, &crate::runtime_identity::cpu_identity())
    }

    #[cfg(test)]
    pub(super) fn identity_neutral(cfg: &Config, host: CpuAutotuneHostFingerprint) -> Self {
        Self::for_identity(cfg, host, &crate::runtime_identity::CpuIdentity::default())
    }

    fn for_identity(
        cfg: &Config,
        host: CpuAutotuneHostFingerprint,
        identity: &crate::runtime_identity::CpuIdentity,
    ) -> Self {
        // Native hardware tests must exercise the shipping identity and ISA
        // gates. Only synthetic fixtures opt into an identity-neutral tuner.
        let supported = supported_kernels();
        let mut candidates = candidates_for(identity, &supported);
        // This exact Zen 4 context keeps its own startup, frontier and sample
        // policies, which assume the 256-bit kernel.
        if super::adaptive_cpu::full_count_cold_start(cfg, &host, identity, 32, 32) {
            for kernel in &mut candidates {
                if matches!(
                    *kernel,
                    CpuKernel::Avx512256StreamL2 | CpuKernel::Avx512StreamL2
                ) {
                    *kernel = CpuKernel::Avx512256Stream;
                }
            }
        }
        let sapphire_startup = sapphire_stream_startup(cfg, &host, identity, &supported);
        if sapphire_startup && !candidates.contains(&CpuKernel::Avx512256Stream) {
            candidates.push(CpuKernel::Avx512256Stream);
        }
        let turin_startup = turin_stream_startup(cfg, &host, identity, &supported);
        // 32-core Linux Turin: demand fences keep the R64/D3
        // lookahead. Actual startup still requires full THP backing.
        let turin_demand = turin_demand_startup(cfg, &host, identity, &supported);
        if turin_demand {
            candidates.insert(0, CpuKernel::Avx512256StreamR64D3);
        }
        // Depth four over depth three on the same context.
        // Keep the previous path available; actual startup still requires THP.
        let turin_prefetch = turin_prefetch_startup(cfg, &host, identity, &supported);
        if turin_prefetch {
            candidates.insert(0, CpuKernel::Avx512256StreamR64D4);
        }
        let turin_depth6 = turin_depth6_startup(cfg, &host, identity, &supported);
        if turin_depth6 {
            candidates.insert(0, CpuKernel::Avx512256StreamR64D6);
        }
        let genoa_startup = genoa_stream_startup(cfg, &host, identity, &candidates);
        let windows_genoa_startup = windows_genoa_stream_startup(cfg, &host, identity, &candidates);
        // Retain the old streaming path as a challenger.
        // Only this exact host context gets the automatic candidate; the startup
        // action still checks exact worker count and platform-specific backing.
        let ring_startup = (genoa_startup || windows_genoa_startup)
            && supported.contains(&CpuKernel::Avx512256StreamR8D2);
        if ring_startup {
            candidates.insert(0, CpuKernel::Avx512256StreamR8D2);
        }
        let startup = if ring_startup {
            Some(CpuKernel::Avx512256StreamR8D2)
        } else if turin_depth6 {
            Some(CpuKernel::Avx512256StreamR64D6)
        } else if turin_prefetch {
            Some(CpuKernel::Avx512256StreamR64D4)
        } else if turin_demand {
            Some(CpuKernel::Avx512256StreamR64D3)
        } else if genoa_startup || windows_genoa_startup || sapphire_startup || turin_startup {
            Some(CpuKernel::Avx512256Stream)
        } else {
            startup_kernel_for(identity, &candidates)
        };
        let short_trials = short_trials_for(identity, &candidates, cfg.cpu_autotune_secs);
        let genoa_short_trials = genoa_short_trials_for(cfg, &host, identity, &candidates);
        let genoa_startup_frontier = (genoa_short_trials
            && candidates.contains(&CpuKernel::Avx512256StreamR8D2))
            || windows_genoa_frontier_for(cfg, &host, identity, &candidates);
        let configuration_for = |paths: &[CpuKernel], use_genoa_short: bool, frontier: bool| {
            let mut configuration = format!(
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
            paths,
        );
            if short_trials {
                configuration.push_str(";sample_policy=turin-10s-256-v1");
            }
            if use_genoa_short {
                configuration.push_str(";sample_policy=genoa-thp-30-32-10s-256-v1");
            }
            if frontier {
                configuration.push_str(if windows_genoa_startup {
                    ";frontier_policy=genoa-windows-regular31-32-stream-first-v1"
                } else {
                    ";frontier_policy=genoa-thp31-32-stream-first-v1"
                });
            }
            if turin_startup {
                configuration.push_str(";startup=turin9r45-32-thp-v1");
            }
            if sapphire_startup {
                configuration.push_str(";startup=sapphire8488c-32-thp-v1");
            }
            if genoa_startup {
                configuration.push_str(";startup=genoa-32-thp-v1");
            }
            if windows_genoa_startup {
                configuration.push_str(";startup=genoa-windows-32-regular-v1");
            }
            configuration
        };
        let configuration =
            configuration_for(&candidates, genoa_short_trials, genoa_startup_frontier);
        let prior_startup_configuration =
            turin_startup.then(|| configuration.replace(";startup=turin9r45-32-thp-v1", ""));
        let prior_configuration = (ring_startup || turin_demand).then(|| {
            let prior_paths: Vec<_> = candidates
                .iter()
                .copied()
                .filter(|k| {
                    !matches!(
                        k,
                        CpuKernel::Avx512256StreamR8D2
                            | CpuKernel::Avx512256StreamR64D3
                            | CpuKernel::Avx512256StreamR64D4
                            | CpuKernel::Avx512256StreamR64D6
                    )
                })
                .collect();
            configuration_for(&prior_paths, false, false)
        });
        let prior_depth6_configuration = turin_depth6.then(|| {
            let paths: Vec<_> = candidates
                .iter()
                .copied()
                .filter(|k| *k != CpuKernel::Avx512256StreamR64D6)
                .collect();
            configuration_for(&paths, genoa_short_trials, genoa_startup_frontier)
        });
        let prior_prefetch_configuration = turin_prefetch.then(|| {
            let paths: Vec<_> = candidates
                .iter()
                .copied()
                .filter(|k| {
                    !matches!(
                        k,
                        CpuKernel::Avx512256StreamR64D4 | CpuKernel::Avx512256StreamR64D6
                    )
                })
                .collect();
            configuration_for(&paths, genoa_short_trials, genoa_startup_frontier)
        });
        let prior_sample_configuration =
            genoa_short_trials.then(|| configuration_for(&candidates, false, false));
        let prior_frontier_configuration = genoa_startup_frontier
            .then(|| configuration_for(&candidates, genoa_short_trials, false));
        let mut name = cfg.cpu_autotune_config_path.as_os_str().to_owned();
        name.push(".kernels.json");
        let path = std::path::PathBuf::from(name);
        let mut startup_cache = None;
        let cache = fs::read(&path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<Cache>(&raw).ok())
            .and_then(|mut record| {
                if record.compatible(&host, &configuration) {
                    Some(record)
                } else if prior_startup_configuration
                    .as_ref()
                    .is_some_and(|prior| record.compatible(&host, prior))
                    || prior_sample_configuration
                        .as_ref()
                        .is_some_and(|prior| record.compatible(&host, prior))
                    || prior_frontier_configuration
                        .as_ref()
                        .is_some_and(|prior| record.compatible(&host, prior))
                {
                    // A shorter future sample policy does not invalidate an
                    // already confirmed winner for this identical candidate set.
                    // Context, age and comparison proof are checked on lookup.
                    record.configuration = configuration.clone();
                    Some(record)
                } else {
                    if prior_depth6_configuration
                        .as_ref()
                        .is_some_and(|prior| record.compatible(&host, prior))
                        || prior_prefetch_configuration
                            .as_ref()
                            .is_some_and(|prior| record.compatible(&host, prior))
                        || prior_configuration.as_ref().is_some_and(|prior| {
                            record.compatible(&host, prior)
                                || (turin_demand
                                    && record.compatible(
                                        &host,
                                        &prior.replace(";startup=turin9r45-32-thp-v1", ""),
                                    ))
                        })
                    {
                        startup_cache = Some(record);
                    }
                    None
                }
            })
            .unwrap_or(Cache {
                schema: CACHE_SCHEMA,
                host,
                configuration,
                winners: Vec::new(),
                decisive: Vec::new(),
            });
        Self {
            cache,
            startup_cache,
            path,
            trial: None,
            checked: None,
            next_check: 0,
            retries: Vec::new(),
            monitor: None,
            window_secs: cfg.cpu_autotune_secs.saturating_mul(5).clamp(30, 60),
            short_trials,
            genoa_short_trials,
            genoa_startup_frontier,
            candidates,
            pending: Vec::new(),
            settling: None,
            settle_started: 0,
            startup,
            genoa_startup,
            windows_genoa_startup,
            sapphire_startup,
            turin_startup,
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

    pub(super) fn joint_budget_secs(&self) -> u64 {
        // Two A/B/A cycles per challenger, one retry cooldown and switch slack.
        // Outer count windows are budgeted separately by the count policy.
        // A D6 incumbent can require longer samples against every challenger.
        let minimum_window = if self.candidates.contains(&CpuKernel::Avx512256StreamR64D6) {
            45
        } else {
            30
        };
        self.window_secs
            .max(minimum_window)
            .saturating_mul(6)
            .saturating_mul(self.candidates.len().saturating_sub(1) as u64)
            .saturating_add(RETRY_SECS)
            .saturating_add(120)
    }

    #[cfg(test)]
    pub(super) fn test_candidates(&mut self, paths: Vec<CpuKernel>) {
        self.candidates = paths;
    }

    pub(super) fn is_comparing(&self) -> bool {
        self.trial.is_some() || self.settling.is_some() || !self.pending.is_empty()
    }

    pub(super) fn forget_previous_members(&mut self, now: u64) {
        // The caller already aborted/restored the old comparison. Its retry
        // delay must not let a new membership's count probes bypass kernel
        // selection and incorrectly reject a count on its incumbent path.
        debug_assert!(!self.is_comparing());
        self.pending.clear();
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

    pub(super) fn retry_at_for_threads(
        &self,
        now: u64,
        snapshots: &[CpuAdaptiveSnapshot],
        threads: usize,
    ) -> Option<u64> {
        // Admission only: no prospective page backing is assumed. The existing
        // retry key intentionally uses stable membership and worker counts.
        let mut context = Context::from_snapshots(snapshots);
        context.workers.fill(threads);
        self.retries
            .iter()
            .filter(|(old, until)| now < *until && old.same_workers(&context))
            .map(|(_, until)| *until)
            .max()
    }

    fn context_retry_pending(&self, now: u64, context: &Context) -> bool {
        self.retries
            .iter()
            .any(|(old, until)| now < *until && old.same_workers(context))
    }

    pub(super) fn abort(&mut self, now: u64) -> Action {
        self.abort_reason(now, TrialReason::ContextChanged)
    }

    fn abort_reason(&mut self, now: u64, reason: TrialReason) -> Action {
        if let Some(monitor) = &mut self.monitor {
            monitor.abort(now, reason);
        }
        let trial = self.trial.take();
        let context = trial
            .as_ref()
            .map(|trial| trial.context.clone())
            .or_else(|| {
                (self.settling.is_some() || !self.pending.is_empty())
                    .then(|| self.checked.clone())
                    .flatten()
            });
        let base = trial.map(|trial| trial.base);
        if let Some(context) = context {
            self.defer_retry(now, context);
            // Do not let an interrupted new context inherit an older, longer
            // recheck. An idle cancellation has no comparison to reschedule.
            self.next_check = now.saturating_add(RETRY_SECS);
        }
        self.settling = None;
        self.pending.clear();
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
        if snapshots.is_empty() || self.candidates.len() < 2 {
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
                return Action {
                    hold_counts: !self.pending.is_empty(),
                    ..Action::default()
                };
            }
            if now.saturating_sub(self.settle_started) >= MAX_TRIAL_SECS {
                return self.abort_reason(now, TrialReason::TimeBudget);
            }
            return Action {
                hold_counts: true,
                ..Action::default()
            };
        }
        if let Some(trial) = &self.trial {
            if context != trial.context {
                return self.abort(now);
            }
            if now.saturating_sub(trial.started) >= MAX_TRIAL_SECS {
                return self.abort_reason(now, TrialReason::TimeBudget);
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
                        hold_counts: true, kernel: Some(kernel), reset_scores: true, interrupted: false,
                        message: Some(format!("restoring confirmed {kernel} path for this worker count and memory backing")),
                    };
                }
                // Adding a candidate does not invalidate the previously verified
                // winner as a starting point. Only the new path needs a comparison;
                // retain all host, settings, context, age and sample-proof checks.
                let prior_paths: Vec<_> = self
                    .candidates
                    .iter()
                    .copied()
                    .filter(|k| Some(*k) != self.startup)
                    .collect();
                if let Some(kernel) = self
                    .startup_cache
                    .as_ref()
                    .and_then(|prior| prior.winner(&context, unix_secs(), &prior_paths))
                {
                    self.checked = Some(context);
                    self.next_check = now;
                    self.pending = self.startup.into_iter().collect();
                    let switching = !snapshots.iter().all(|s| s.kernel == kernel);
                    if switching {
                        self.settling = Some(kernel);
                        self.settle_started = now;
                    }
                    return Action {
                        hold_counts: true, kernel: switching.then_some(kernel),
                        reset_scores: switching, interrupted: false,
                        message: Some(format!("starting from saved {kernel} path; comparing the newly available kernel while mining")),
                    };
                }
                if let Some(kernel) = self.startup.filter(|k| {
                    self.candidates.contains(k)
                        && if self.genoa_startup || self.sapphire_startup || self.turin_startup {
                            genoa_stream_context(&context)
                        } else if self.windows_genoa_startup {
                            windows_genoa_stream_context(&context)
                        } else {
                            context
                                .workers
                                .iter()
                                .all(|&w| w >= STARTUP_STREAM_MIN_WORKERS)
                        }
                        && !snapshots.iter().all(|s| s.kernel == *k)
                }) {
                    // Compare the remaining paths against it promptly; only a
                    // completed sweep is cached.
                    self.checked = Some(context);
                    self.next_check = now;
                    self.settling = Some(kernel);
                    self.settle_started = now;
                    return Action {
                        hold_counts: true,
                        kernel: Some(kernel),
                        reset_scores: true,
                        interrupted: false,
                        message: Some(if self.genoa_startup || self.windows_genoa_startup {
                            format!("starting validated {kernel} path for matched EPYC context; comparing other paths against it")
                        } else {
                            format!("starting {kernel} path validated for this CPU; comparing other paths against it")
                        }),
                    };
                }
            }
            let base = snapshots[0].kernel;
            if !snapshots.iter().all(|s| s.kernel == base) {
                return Action::default();
            }
            if self.pending.is_empty() {
                let frontier = self.genoa_startup_frontier
                    && if self.windows_genoa_startup {
                        windows_genoa_frontier_context(&context)
                    } else {
                        genoa_frontier_context(&context)
                    }
                    && !self.cache.winners.iter().any(|w| w.context == context);
                let workers: usize = context.workers.iter().sum();
                let decisive = &self.cache.decisive;
                let wall = unix_secs(); // decisive timestamps are wall-clock, like winners
                self.pending = self
                    .candidates
                    .iter()
                    .copied()
                    .filter(|&k| k != base)
                    .filter(|&k| !decisive.iter().any(|d| d.applies(base, k, workers, wall)))
                    .filter(|&k| !defer_generic_trial(self.startup, base, k, now))
                    .filter(|&k| {
                        !frontier
                            || matches!(
                                k,
                                CpuKernel::Avx512256Stream | CpuKernel::Avx512256StreamR8D2
                            )
                    })
                    .collect();
            }
            if self.pending.is_empty() {
                return Action::default();
            }
            let candidate = self.pending.remove(0);
            if let Some(monitor) = &mut self.monitor {
                let fence = turin_small_gain_pair(base, candidate);
                let (seconds, hashes) = turin_pair_resolution(base, candidate);
                let short = !fence
                    && (self.short_trials
                        || (self.genoa_short_trials && genoa_short_context(&context)));
                monitor.begin(
                    now,
                    base,
                    candidate,
                    TrialPolicy {
                        revision: 1,
                        minimum_sample_ms: if fence {
                            self.window_secs.max(seconds)
                        } else {
                            self.window_secs
                        } * 1000,
                        minimum_hashes: if fence { hashes } else { 32 },
                        short_sample_ms: short.then_some(10_000),
                        short_minimum_hashes: short.then_some(256),
                        maximum_baseline_drift_bps: if fence { 100 } else { 500 },
                        required_gain_bps: if fence { 0 } else { 200 },
                        quantization: TrialQuantization::ThreeWindowHash,
                    },
                );
            }
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
                return self.abort_reason(now, TrialReason::MiningIdle);
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
            if let Some(monitor) = &mut self.monitor {
                monitor.discard_window();
            }
            return Action {
                hold_counts: true,
                ..Action::default()
            };
        }
        let generations: Vec<_> = snapshots.iter().map(|s| s.kernel_generation).collect();
        let Some((start, first, observed_generations)) = &trial.window else {
            trial.window = Some((now, hashes, generations));
            if let Some(monitor) = &mut self.monitor {
                monitor.window(now);
            }
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
        let short =
            self.short_trials || (self.genoa_short_trials && genoa_short_context(&trial.context));
        let fence_pair = turin_small_gain_pair(trial.base, trial.candidate);
        let (pair_seconds, pair_hashes) = turin_pair_resolution(trial.base, trial.candidate);
        if !sample_ready(sample, self.window_secs, short && !fence_pair)
            || (fence_pair && (sample.secs < pair_seconds || sample.hashes < pair_hashes))
        {
            return Action {
                hold_counts: true,
                ..Action::default()
            };
        }
        trial.window = None;
        if let Some(monitor) = &mut self.monitor {
            monitor.sample(sample.hashes, sample.secs);
        }
        trial.samples.push(sample);
        if trial.samples.len() == 3
            && comparison_for(
                trial.base,
                trial.candidate,
                trial.samples[0],
                trial.samples[1],
                trial.samples[2],
            ) != Comparison::Gain
        {
            // Reject an inconclusive/noisy candidate without spending a second cycle.
            let trial = self.trial.take().unwrap();
            let stable = comparison_for(
                trial.base,
                trial.candidate,
                trial.samples[0],
                trial.samples[1],
                trial.samples[2],
            ) == Comparison::NoConfirmedGain;
            let measured = rates(&trial.samples, trial.base, trial.candidate);
            if let Some(monitor) = &mut self.monitor {
                monitor.finish(
                    now,
                    if stable {
                        TrialDecision::NoConfirmedGain
                    } else {
                        TrialDecision::Inconclusive
                    },
                    if stable {
                        TrialReason::Completed
                    } else {
                        TrialReason::BaselineDrift
                    },
                    TrialArm::Baseline,
                );
            }
            if stable {
                let base = trial.base;
                let [before, candidate, after] =
                    [trial.samples[0], trial.samples[1], trial.samples[2]];
                if candidate.hps() <= before.hps().min(after.hps()) * DECISIVE_LOSS_RATIO {
                    let workers = trial.context.workers.iter().sum();
                    self.cache.decisive.retain(|d| {
                        !(d.base == base.as_str() && d.loser == trial.candidate.as_str())
                    });
                    self.cache.decisive.push(DecisiveLoss {
                        base: base.to_string(),
                        loser: trial.candidate.to_string(),
                        workers,
                        timestamp: unix_secs(),
                    });
                }
                self.finish_comparison(trial, base, now);
            } else {
                self.pending.clear();
                self.next_check = now + RETRY_SECS;
                self.defer_retry(now, trial.context);
            }
            return Action {
                hold_counts: !self.pending.is_empty(),
                interrupted: !stable,
                message: Some(if stable {
                    format!("keeping current kernel; no confirmed gain ({measured} H/s)")
                } else {
                    format!("keeping current kernel; baseline drifted, retrying sooner ({measured} H/s)")
                }),
                ..Action::default()
            };
        }
        if trial.samples.len() == 6 {
            let trial = self.trial.take().unwrap();
            let outcome = comparison_for(
                trial.base,
                trial.candidate,
                trial.samples[3],
                trial.samples[4],
                trial.samples[5],
            );
            let measured = rates(&trial.samples, trial.base, trial.candidate);
            let promoted = outcome == Comparison::Gain;
            let conclusive = outcome != Comparison::Inconclusive;
            if let Some(monitor) = &mut self.monitor {
                monitor.finish(
                    now,
                    if promoted {
                        TrialDecision::Gain
                    } else if conclusive {
                        TrialDecision::NoConfirmedGain
                    } else {
                        TrialDecision::Inconclusive
                    },
                    if conclusive {
                        TrialReason::Completed
                    } else {
                        TrialReason::BaselineDrift
                    },
                    if promoted {
                        TrialArm::Candidate
                    } else {
                        TrialArm::Baseline
                    },
                );
            }
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
                self.defer_retry(now, trial.context);
            }
            self.settling = Some(kernel);
            self.settle_started = now;
            return Action {
                hold_counts: true,
                kernel: Some(kernel),
                reset_scores: promoted,
                interrupted: !conclusive,
                message: Some(if promoted {
                    format!("confirmed faster {kernel} path in two comparisons ({measured} H/s)")
                } else if conclusive {
                    format!("keeping {kernel}; gain did not repeat ({measured} H/s)")
                } else {
                    format!("keeping {kernel}; confirmation was noisy, retrying sooner ({measured} H/s)")
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

    #[test]
    fn dense_zen5_starts_on_skip_kernel_with_plain_challenger_and_fixed_count_trials() {
        use crate::runtime_identity::CpuIdentity;
        let all: Vec<CpuKernel> = CpuKernel::ALL.to_vec();
        let dense = CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x1a),
            model: Some(0x11),
            l3_cores: Some(16),
            ..CpuIdentity::default()
        };
        let candidates = candidates_for(&dense, &all);
        assert_eq!(
            startup_kernel_for(&dense, &candidates),
            Some(CpuKernel::Avx512StreamSkip)
        );
        assert!(candidates.contains(&CpuKernel::Avx512Stream));
        assert!(fixed_count_trials_wanted_for(&dense, &all));
        assert_eq!(
            static_kernel_for_identity(&dense, &all, 192),
            Some(CpuKernel::Avx512StreamSkip)
        );

        // Regular Zen 5 EPYC (8 cores per L3), Zen 5 desktops and unknown L3 keep
        // the plain path and no fixed-count trials.
        for other in [
            CpuIdentity {
                l3_cores: Some(8),
                ..dense.clone()
            },
            CpuIdentity {
                l3_cores: None,
                ..dense.clone()
            },
            CpuIdentity {
                model: Some(0x44),
                ..dense.clone()
            },
        ] {
            let candidates = candidates_for(&other, &all);
            assert_ne!(
                startup_kernel_for(&other, &candidates),
                Some(CpuKernel::Avx512StreamSkip)
            );
            assert!(!candidates.contains(&CpuKernel::Avx512StreamSkip));
        }
        let regular = CpuIdentity {
            l3_cores: Some(8),
            ..dense.clone()
        };
        assert!(!fixed_count_trials_wanted_for(&regular, &all));

        // Without AVX-512 the skip path is never chosen.
        let no_avx512: Vec<CpuKernel> = all
            .iter()
            .copied()
            .filter(|k| !k.as_str().starts_with("avx512"))
            .collect();
        assert_ne!(
            startup_kernel_for(&dense, &candidates_for(&dense, &no_avx512)),
            Some(CpuKernel::Avx512StreamSkip)
        );
    }

    #[test]
    fn short_trial_policy_is_limited_to_turin_default_timing_and_supported_path() {
        use crate::runtime_identity::CpuIdentity;
        let turin = CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x1a),
            model: Some(0x02),
            ..CpuIdentity::default()
        };
        let paths = [CpuKernel::Avx512, CpuKernel::Avx512256Stream];
        assert!(short_trials_for(&turin, &paths, 6));
        assert!(!short_trials_for(&turin, &paths, 12));
        assert!(!short_trials_for(&turin, &[CpuKernel::Avx2], 6));
        for other in [
            CpuIdentity {
                model: Some(0x44),
                ..turin.clone()
            },
            CpuIdentity {
                family: Some(0x19),
                model: Some(0x11),
                ..turin.clone()
            },
            CpuIdentity {
                vendor: Some("GenuineIntel".into()),
                ..turin.clone()
            },
            CpuIdentity::default(),
        ] {
            assert!(!short_trials_for(&other, &paths, 6));
        }
    }

    #[test]
    fn fast_samples_need_time_and_count_while_slow_samples_keep_original_rule() {
        for (secs, hashes, short, expected) in [
            (9, 500, true, false),
            (10, 255, true, false),
            (10, 256, true, true),
            (10, 500, false, false),
            (29, 255, true, false),
            (30, 31, true, false),
            (30, 32, true, true),
            (30, 32, false, true),
        ] {
            assert_eq!(sample_ready(Sample { secs, hashes }, 30, short), expected);
        }
    }

    #[test]
    fn short_samples_still_need_two_promotions_and_kernel_acknowledgements() {
        let mut t = tuner();
        t.short_trials = true;
        t.candidates = vec![CpuKernel::Avx512, CpuKernel::Avx2];
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512;
        let mut now = 0;
        assert!(t.tick(now, &[s], true, true).hold_counts);
        for (index, rate) in [100, 120, 100, 100, 120, 100].into_iter().enumerate() {
            now += 1;
            s.kernel_ready = false;
            assert!(t.tick(now, &[s], true, true).hold_counts);
            assert!(t.trial.as_ref().unwrap().window.is_none());
            s.kernel_ready = true;
            t.tick(now, &[s], true, true);
            now += 10;
            s.hashes += rate * 10;
            let action = t.tick(now, &[s], true, true);
            if let Some(kernel) = action.kernel {
                s.kernel = kernel;
                s.kernel_generation += 1;
            }
            if index < 5 {
                assert!(t.cache.winners.is_empty());
            }
        }
        assert_eq!(s.kernel, CpuKernel::Avx2);
        assert_eq!(t.cache.winners.len(), 1);
        assert!(t.cache.winners[0]
            .samples
            .iter()
            .all(|sample| sample.secs == 10));
        let _ = fs::remove_file(&t.path);
    }

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
        let mut t = Tuner::identity_neutral(&cfg, CpuAutotuneHostFingerprint::default());
        t.cache.winners.clear();
        t.candidates.retain(|k| *k != CpuKernel::Avx512256Stream);
        // Independent of whether this test runner is the validated Ryzen.
        t.startup = None;
        t
    }

    fn monitored_tick(t: &mut Tuner, now: u64, s: CpuAdaptiveSnapshot) -> Action {
        if let Some(monitor) = &mut t.monitor {
            monitor.observe(
                now,
                &[s],
                crate::config::CpuAutotuneResources {
                    parallelism: 4,
                    total_bytes: Some(8 * CPU_LANE_MEMORY_BYTES),
                    available_bytes: Some(6 * CPU_LANE_MEMORY_BYTES),
                },
            );
        }
        t.tick(now, &[s], true, true)
    }

    #[test]
    fn telemetry_preserves_decisions_and_emits_gain_only_after_switch_acknowledgement() {
        let mut decisions = Vec::new();
        for enabled in [false, true] {
            let recorder = crate::hardware_telemetry::Recorder::test_recorder();
            let mut t = tuner();
            t.candidates = vec![CpuKernel::Avx512, CpuKernel::Avx2];
            t.short_trials = true;
            if enabled {
                t.monitor = Some(
                    super::super::adaptive_telemetry::Monitor::test_without_placement(
                        recorder.clone(),
                        &super::super::tests::test_config(),
                    ),
                );
            }
            let mut s = snapshot();
            s.kernel = CpuKernel::Avx512;
            let mut now = 0;
            let mut actions = Vec::new();
            monitored_tick(&mut t, now, s);
            for rate in [100, 120, 100, 100, 120, 100] {
                now += 1;
                s.kernel_ready = false;
                monitored_tick(&mut t, now, s);
                s.kernel_ready = true;
                monitored_tick(&mut t, now, s);
                now += 10;
                s.hashes += rate * 10;
                let action = monitored_tick(&mut t, now, s);
                actions.push((
                    action.kernel,
                    action.hold_counts,
                    action.interrupted,
                    action.reset_scores,
                ));
                if let Some(kernel) = action.kernel {
                    s.kernel = kernel;
                    s.kernel_generation += 1;
                }
                assert!(recorder.test_tuning_report().is_none());
            }
            s.kernel_ready = false;
            monitored_tick(&mut t, now + 1, s);
            assert!(recorder.test_tuning_report().is_none());
            s.kernel_ready = true;
            monitored_tick(&mut t, now + 2, s);
            if enabled {
                let report = recorder.test_tuning_report().unwrap();
                assert_eq!(report.dropped_trials, 0);
                assert_eq!(report.trials.len(), 1);
                let outcome = &report.trials[0].outcome;
                assert!(outcome.validate());
                assert_eq!(outcome.decision, TrialDecision::Gain);
                assert_eq!(outcome.applied, Some(TrialArm::Candidate));
                assert_eq!(outcome.context_after.members, outcome.candidate);
                assert_eq!(outcome.samples.len(), 6);
                assert!(outcome
                    .samples
                    .iter()
                    .all(|s| !s.changed && s.elapsed_ms == 10_000));
                assert_eq!(outcome.policy.short_sample_ms, Some(10_000));
                assert_eq!(outcome.policy.short_minimum_hashes, Some(256));
                assert_eq!(outcome.policy.minimum_sample_ms, 30_000);
                assert_eq!(outcome.policy.minimum_hashes, 32);
                assert_eq!(outcome.cost.idle_ms, None);
                assert!(recorder.test_tuning_report().is_none());
            } else {
                assert!(recorder.test_tuning_report().is_none());
            }
            decisions.push(actions);
            let _ = fs::remove_file(&t.path);
        }
        assert_eq!(decisions[0], decisions[1]);
    }

    #[test]
    fn telemetry_does_not_report_unacknowledged_promotion_as_applied() {
        for reason in [
            TrialReason::ExecutionFailure,
            TrialReason::TimeBudget,
            TrialReason::UserInterrupted,
        ] {
            let recorder = crate::hardware_telemetry::Recorder::test_recorder();
            let mut t = tuner();
            t.candidates = vec![CpuKernel::Avx512, CpuKernel::Avx2];
            t.monitor = Some(
                super::super::adaptive_telemetry::Monitor::test_without_placement(
                    recorder.clone(),
                    &super::super::tests::test_config(),
                ),
            );
            let mut s = snapshot();
            s.kernel = CpuKernel::Avx512;
            let mut now = 0;
            monitored_tick(&mut t, now, s);
            for rate in [100, 120, 100, 100, 120, 100] {
                now += 1;
                monitored_tick(&mut t, now, s);
                now += 30;
                s.hashes += rate * 30;
                let action = monitored_tick(&mut t, now, s);
                if let Some(kernel) = action.kernel {
                    s.kernel = kernel;
                    s.kernel_generation += 1;
                }
            }
            assert!(recorder.test_tuning_report().is_none());
            // The final request has not reached the workers. The last observed
            // configuration is still the final baseline measurement.
            t.monitor.as_mut().unwrap().abort(now, reason);
            let report = recorder.test_tuning_report().unwrap();
            assert_eq!(report.dropped_trials, 0);
            let outcome = &report.trials[0].outcome;
            assert!(outcome.validate());
            assert_eq!(outcome.samples.len(), 6);
            assert_eq!(outcome.decision, TrialDecision::Aborted);
            assert_eq!(outcome.reason, reason);
            assert_eq!(outcome.applied, Some(TrialArm::Baseline));
            t.monitor.as_mut().unwrap().abort(now + 1, reason);
            assert!(recorder.test_tuning_report().is_none());
            let _ = fs::remove_file(&t.path);
        }
    }

    #[test]
    fn telemetry_failure_observes_partially_switched_cpu_backends() {
        let recorder = crate::hardware_telemetry::Recorder::test_recorder();
        let mut cfg = super::super::tests::test_config();
        cfg.backend_specs.push(cfg.backend_specs[0].clone());
        let mut monitor = super::super::adaptive_telemetry::Monitor::test_without_placement(
            recorder.clone(),
            &cfg,
        );
        let mut snapshots = [snapshot(), snapshot()];
        for s in &mut snapshots {
            s.kernel = CpuKernel::Avx512;
        }
        let resources = crate::config::CpuAutotuneResources {
            parallelism: 4,
            total_bytes: Some(8 * CPU_LANE_MEMORY_BYTES),
            available_bytes: Some(6 * CPU_LANE_MEMORY_BYTES),
        };
        monitor.observe(0, &snapshots, resources);
        monitor.begin(
            0,
            CpuKernel::Avx512,
            CpuKernel::Avx2,
            TrialPolicy {
                revision: 1,
                minimum_hashes: 32,
                minimum_sample_ms: 30_000,
                short_sample_ms: None,
                short_minimum_hashes: None,
                quantization: TrialQuantization::ThreeWindowHash,
                maximum_baseline_drift_bps: 500,
                required_gain_bps: 200,
            },
        );
        snapshots[0].kernel = CpuKernel::Avx2;
        snapshots[0].kernel_ready = false;
        monitor.abort_observed(1, &snapshots, resources, TrialReason::ExecutionFailure);
        let report = recorder.test_tuning_report().unwrap();
        assert_eq!(report.dropped_trials, 0);
        let outcome = &report.trials[0].outcome;
        assert!(outcome.validate());
        assert_eq!(outcome.applied, None);
        assert_eq!(outcome.reason, TrialReason::ExecutionFailure);
        assert_eq!(
            outcome.context_after.members[0].kernel,
            CpuKernel::Avx2.to_string()
        );
        assert_eq!(
            outcome.context_after.members[1].kernel,
            CpuKernel::Avx512.to_string()
        );
    }

    #[test]
    fn telemetry_keeps_negative_and_drifted_comparisons() {
        for (rates, decision, reason) in [
            (
                [100, 90, 100],
                TrialDecision::NoConfirmedGain,
                TrialReason::Completed,
            ),
            (
                [100, 110, 120],
                TrialDecision::Inconclusive,
                TrialReason::BaselineDrift,
            ),
        ] {
            let recorder = crate::hardware_telemetry::Recorder::test_recorder();
            let mut t = tuner();
            t.candidates = vec![CpuKernel::Avx512, CpuKernel::Avx2];
            t.monitor = Some(
                super::super::adaptive_telemetry::Monitor::test_without_placement(
                    recorder.clone(),
                    &super::super::tests::test_config(),
                ),
            );
            let mut s = snapshot();
            s.kernel = CpuKernel::Avx512;
            let mut now = 0;
            monitored_tick(&mut t, now, s);
            for rate in rates {
                now += 1;
                monitored_tick(&mut t, now, s);
                now += 30;
                s.hashes += rate * 30;
                let action = monitored_tick(&mut t, now, s);
                if let Some(kernel) = action.kernel {
                    s.kernel = kernel;
                    s.kernel_generation += 1;
                }
            }
            let report = recorder.test_tuning_report().unwrap();
            assert_eq!(report.dropped_trials, 0);
            let result = &report.trials[0].outcome;
            assert!(result.validate());
            assert_eq!(result.decision, decision);
            assert_eq!(result.reason, reason);
            assert_eq!(result.applied, Some(TrialArm::Baseline));
            assert_eq!(
                result.samples.iter().map(|s| s.hashes).collect::<Vec<_>>(),
                rates.map(|r| r * 30)
            );
            let _ = fs::remove_file(&t.path);
        }
    }

    #[test]
    fn telemetry_preserves_partial_sample_when_last_backend_disappears() {
        let recorder = crate::hardware_telemetry::Recorder::test_recorder();
        let mut t = tuner();
        t.candidates = vec![CpuKernel::Avx512, CpuKernel::Avx2];
        t.monitor = Some(
            super::super::adaptive_telemetry::Monitor::test_without_placement(
                recorder.clone(),
                &super::super::tests::test_config(),
            ),
        );
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512;
        monitored_tick(&mut t, 0, s);
        monitored_tick(&mut t, 1, s);
        s.hashes = 77;
        monitored_tick(&mut t, 4, s);
        s.running = false;
        monitored_tick(&mut t, 5, s);
        t.monitor
            .as_mut()
            .unwrap()
            .abort(5, TrialReason::ExecutionFailure);
        let report = recorder.test_tuning_report().unwrap();
        assert_eq!(report.dropped_trials, 0);
        let result = &report.trials[0].outcome;
        assert!(result.validate());
        assert_eq!(result.decision, TrialDecision::Aborted);
        assert_eq!(result.reason, TrialReason::ExecutionFailure);
        assert_eq!(result.applied, None);
        assert!(result.context_after.members.is_empty());
        assert_eq!(result.samples.len(), 1);
        assert_eq!(result.samples[0].hashes, 77);
        assert_eq!(result.samples[0].elapsed_ms, 3000);
        assert!(result.samples[0].changed);
        t.monitor
            .as_mut()
            .unwrap()
            .abort(6, TrialReason::UserInterrupted);
        assert!(recorder.test_tuning_report().is_none());
        let _ = fs::remove_file(&t.path);
    }

    #[test]
    fn native_constructor_uses_runtime_identity_in_hardware_tests() {
        let cfg = super::super::tests::test_config();
        let identity = crate::runtime_identity::cpu_identity();
        let supported: Vec<_> = CpuKernel::ALL
            .into_iter()
            .filter(|k| k.is_supported())
            .collect();
        let expected = candidates_for(&identity, &supported);
        let t = Tuner::new(&cfg, CpuAutotuneHostFingerprint::default());
        assert_eq!(t.candidates, expected);
        assert_eq!(t.startup, startup_kernel_for(&identity, &expected));
    }

    #[test]
    fn injected_identity_controls_candidates_startup_and_cache_fingerprint() {
        use crate::runtime_identity::CpuIdentity;
        let cfg = super::super::tests::test_config();
        let host = CpuAutotuneHostFingerprint::default();
        let neutral = Tuner::identity_neutral(&cfg, host.clone());
        assert_eq!(neutral.candidates, candidates());
        assert_eq!(neutral.startup, None);
        let turin = CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x1a),
            model: Some(0x02),
            ..CpuIdentity::default()
        };
        // Zen 5 server-class models use full-width streaming, client Zen 5 the
        // 256-bit path and Zen 4 full width with a far L2 prefetch.
        for (identity, stream) in [
            (turin.clone(), Some(CpuKernel::Avx512Stream)),
            (
                CpuIdentity {
                    model: Some(0x44),
                    ..turin.clone()
                },
                Some(CpuKernel::Avx512256Stream),
            ),
            (
                CpuIdentity {
                    family: Some(0x19),
                    model: Some(0x11),
                    ..turin.clone()
                },
                Some(CpuKernel::Avx512StreamL2),
            ),
            (
                CpuIdentity {
                    vendor: Some("GenuineIntel".into()),
                    ..turin
                },
                None,
            ),
        ] {
            let t = Tuner::for_identity(&cfg, host.clone(), &identity);
            let stream = stream.filter(|k| k.is_supported());
            assert_eq!(
                stream.is_some_and(|k| t.candidates.contains(&k)),
                stream.is_some()
            );
            assert_eq!(t.startup, stream);
            assert_eq!(
                t.cache.configuration != neutral.cache.configuration,
                t.candidates != neutral.candidates
            );
            assert!(t.candidates.iter().all(|k| k.is_supported()));
        }
    }

    #[test]
    fn candidates_are_supported_and_experimental_x86_path_stays_manual() {
        let paths = candidates();
        // Only the identity-gated streaming path may add a third candidate.
        let streaming = usize::from(paths.contains(&CpuKernel::Avx512256Stream));
        assert!(paths.len() <= 2 + streaming);
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
    fn demand_fence_small_gain_requires_resolved_stable_samples() {
        let base = CpuKernel::Avx512256Stream;
        let candidate = CpuKernel::Avx512256StreamR64D3;
        let a = Sample {
            hashes: 1500,
            secs: 30,
        };
        let b = Sample {
            hashes: 1515,
            secs: 30,
        };
        assert_eq!(comparison(a, b, a), Comparison::NoConfirmedGain);
        assert_eq!(comparison_for(base, candidate, a, b, a), Comparison::Gain);
        assert_eq!(
            comparison_for(base, candidate, a, Sample { hashes: 1501, ..a }, a),
            Comparison::NoConfirmedGain
        );
        assert_eq!(
            comparison_for(base, candidate, a, b, Sample { hashes: 1530, ..a }),
            Comparison::Inconclusive
        );
        assert_eq!(
            comparison_for(
                base,
                candidate,
                Sample {
                    hashes: 500,
                    secs: 10
                },
                b,
                a
            ),
            Comparison::Inconclusive
        );
    }

    #[test]
    fn demand_fence_one_percent_promotes_after_two_cycles_and_survives_cache() {
        if candidates().len() < 2 {
            return;
        }
        let mut t = tuner();
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512256Stream;
        t.candidates = vec![s.kernel, CpuKernel::Avx512256StreamR64D3];
        t.short_trials = true;
        let challenger = CpuKernel::Avx512256StreamR64D3;
        let mut now = 0;
        assert!(t.tick(now, &[s], true, true).hold_counts);
        for (index, rate) in [100, 101, 100, 100, 101, 100].into_iter().enumerate() {
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
        let context = Context::from_snapshots(&[s]);
        assert_eq!(
            t.cache.winner(&context, unix_secs(), &t.candidates),
            Some(challenger)
        );
        assert!(!t.tick(now + 1, &[s], true, true).hold_counts);
        assert!(!t.tick(now + 2, &[s], true, true).hold_counts);
        let _ = fs::remove_file(&t.path);
    }

    #[test]
    fn prefetch_small_gain_promotes_after_two_cycles_and_survives_cache() {
        if candidates().len() < 2 {
            return;
        }
        let mut t = tuner();
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512256StreamR64D3;
        t.candidates = vec![s.kernel, CpuKernel::Avx512256StreamR64D4];
        t.short_trials = true;
        let challenger = CpuKernel::Avx512256StreamR64D4;
        let mut now = 0;
        assert!(t.tick(now, &[s], true, true).hold_counts);
        for (index, rate) in [1500, 1505, 1500, 1500, 1505, 1500].into_iter().enumerate() {
            now += 1;
            s.kernel_ready = false;
            assert!(t.tick(now, &[s], true, true).hold_counts);
            assert!(t.trial.as_ref().unwrap().window.is_none());
            s.kernel_ready = true;
            t.tick(now, &[s], true, true);
            now += 30;
            s.hashes += rate;
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
        let context = Context::from_snapshots(&[s]);
        assert_eq!(
            t.cache.winner(&context, unix_secs(), &t.candidates),
            Some(challenger)
        );
        assert!(!t.tick(now + 1, &[s], true, true).hold_counts);
        assert!(!t.tick(now + 2, &[s], true, true).hold_counts);
        let _ = fs::remove_file(&t.path);
    }

    #[test]
    fn depth6_small_gain_rejects_unresolved_samples_and_baseline_drift() {
        let a = CpuKernel::Avx512256StreamR64D4;
        let b = CpuKernel::Avx512256StreamR64D6;
        let before = Sample {
            hashes: 2250,
            secs: 45,
        };
        let candidate = Sample {
            hashes: 2254,
            secs: 45,
        };
        assert_eq!(
            comparison_for(a, b, before, candidate, before),
            Comparison::Gain
        );
        assert_eq!(
            comparison_for(
                a,
                b,
                before,
                Sample {
                    hashes: 2253,
                    secs: 45
                },
                before
            ),
            Comparison::NoConfirmedGain
        );
        assert_eq!(
            comparison_for(
                a,
                b,
                before,
                candidate,
                Sample {
                    hashes: 2275,
                    secs: 45
                }
            ),
            Comparison::Inconclusive
        );
        for unresolved in [
            Sample {
                hashes: 2250,
                secs: 44,
            },
            Sample {
                hashes: 2047,
                secs: 45,
            },
        ] {
            assert_eq!(
                comparison_for(a, b, unresolved, candidate, before),
                Comparison::Inconclusive
            );
        }
        assert_eq!(
            turin_pair_resolution(a, CpuKernel::Avx512256StreamR64D3),
            (30, 1024)
        );
    }

    #[test]
    fn depth6_small_gain_promotes_after_two_cycles_and_survives_cache() {
        if candidates().len() < 2 {
            return;
        }
        let mut t = tuner();
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512256StreamR64D4;
        t.candidates = vec![s.kernel, CpuKernel::Avx512256StreamR64D6];
        t.short_trials = true;
        let challenger = CpuKernel::Avx512256StreamR64D6;
        let mut now = 0;
        assert!(t.tick(now, &[s], true, true).hold_counts);
        for (index, rate) in [2250, 2254, 2250, 2250, 2254, 2250].into_iter().enumerate() {
            now += 1;
            s.kernel_ready = false;
            assert!(t.tick(now, &[s], true, true).hold_counts);
            assert!(t.trial.as_ref().unwrap().window.is_none());
            s.kernel_ready = true;
            t.tick(now, &[s], true, true);
            now += 45;
            s.hashes += rate;
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
        let context = Context::from_snapshots(&[s]);
        assert_eq!(
            t.cache.winner(&context, unix_secs(), &t.candidates),
            Some(challenger)
        );
        assert!(!t.tick(now + 1, &[s], true, true).hold_counts);
        assert!(!t.tick(now + 2, &[s], true, true).hold_counts);
        let _ = fs::remove_file(&t.path);
    }

    #[test]
    fn joint_budget_covers_depth6_sweep_and_a_retry() {
        let mut t = tuner();
        t.window_secs = 30;
        t.candidates = vec![
            CpuKernel::Avx512256StreamR64D6,
            CpuKernel::Avx512256StreamR64D4,
            CpuKernel::Avx512256StreamR64D3,
            CpuKernel::Avx512256Stream,
            CpuKernel::Avx512,
            CpuKernel::Avx2,
        ];
        // Three small-gain opponents need 45s windows; generic opponents
        // still take 30s. Do not abort the count trial before these can finish.
        let sweep = 3 * 6 * 45 + 2 * 6 * 30;
        assert!(t.joint_budget_secs() >= sweep + RETRY_SECS + 120);
        t.window_secs = 60;
        assert!(t.joint_budget_secs() >= 5 * 6 * 60 + RETRY_SECS + 120);
        let _ = fs::remove_file(&t.path);
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
            decisive: Vec::new(),
        };
        assert!(cache.compatible(&host, "test"));
        cache.schema = 1;
        assert!(!cache.compatible(&host, "test"));
        cache.schema = CACHE_SCHEMA;
        assert!(cache.compatible(&host, "test"));
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
            l3_cores: None,
        };
        assert_eq!(
            candidates_for(&turin, &supported),
            [
                CpuKernel::Avx512,
                CpuKernel::Avx512256Stream,
                CpuKernel::Avx2
            ]
        );
        assert_eq!(candidates_for(&turin, &supported[..2]), supported[..2]);
        let ryzen = CpuIdentity {
            model: Some(0x44),
            ..turin.clone()
        };
        assert_eq!(candidates_for(&ryzen, &supported), supported);
        assert_eq!(candidates_for(&ryzen, &supported[..2]), supported[..2]);
        let genoa = CpuIdentity {
            family: Some(0x19),
            model: Some(0x11),
            ..turin.clone()
        };
        assert_eq!(candidates_for(&genoa, &supported), supported);
        assert_eq!(candidates_for(&genoa, &supported[..2]), supported[..2]);
        assert_eq!(
            startup_kernel_for(&genoa, &supported),
            Some(CpuKernel::Avx512256Stream)
        );
        // Zen 4/5 parts beyond the measured models share the cohort policy:
        // Raphael (19h/61h), other Genoa models, Turin Dense, masked cloud CPUIDs.
        for model in [0x61, 0x10, 0x12] {
            let zen4 = CpuIdentity {
                family: Some(25),
                model: Some(model),
                ..turin.clone()
            };
            assert_eq!(candidates_for(&zen4, &supported), supported);
        }
        for model in [0x70, 0x11, 0x00] {
            let zen5 = CpuIdentity {
                model: Some(model),
                ..turin.clone()
            };
            assert_eq!(candidates_for(&zen5, &supported), supported);
        }
        // Unvalidated x86 hosts keep their start but may discover streaming
        // through repeated trials; identity-neutral fixtures stay unchanged.
        for identity in [
            CpuIdentity {
                vendor: Some("GenuineIntel".into()),
                ..turin.clone()
            },
            CpuIdentity {
                family: Some(0x17),
                model: Some(0x31),
                ..turin.clone()
            },
        ] {
            assert_eq!(candidates_for(&identity, &supported), supported);
            assert_eq!(startup_kernel_for(&identity, &supported), None);
        }
        assert_eq!(
            candidates_for(&CpuIdentity::default(), &supported),
            supported[..2]
        );
        // Alder Lake and Raptor Lake desktops (AVX2 only) start on AVX2 streaming;
        // other Intel client models keep their start.
        let avx2 = [CpuKernel::Avx2, CpuKernel::Avx2Stream];
        for (model, expected) in [
            (0x97, Some(CpuKernel::Avx2Stream)),
            (0xbf, Some(CpuKernel::Avx2Stream)),
            (0x9e, None),
            (0xa5, None),
        ] {
            let intel = CpuIdentity {
                vendor: Some("GenuineIntel".into()),
                family: Some(6),
                model: Some(model),
                ..turin.clone()
            };
            assert_eq!(
                startup_kernel_for(&intel, &avx2),
                expected,
                "model {model:#x}"
            );
        }
        // Zen 4 prefers full width with the far L2 prefetch, else the 256-bit one.
        let zen4 = CpuIdentity {
            family: Some(0x19),
            model: Some(0x11),
            ..turin.clone()
        };
        let both_l2 = [CpuKernel::Avx512256StreamL2, CpuKernel::Avx512StreamL2];
        assert_eq!(
            startup_kernel_for(&zen4, &both_l2),
            Some(CpuKernel::Avx512StreamL2)
        );
        assert_eq!(
            startup_kernel_for(&zen4, &both_l2[..1]),
            Some(CpuKernel::Avx512256StreamL2)
        );
        // A masked-CPUID AMD VM (family 06h) starts AVX2-only hosts on streaming.
        let masked = CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(6),
            model: Some(63),
            ..CpuIdentity::default()
        };
        let avx2_paths = [CpuKernel::Avx2, CpuKernel::Avx2Stream];
        assert_eq!(
            startup_kernel_for(&masked, &avx2_paths),
            Some(CpuKernel::Avx2Stream)
        );
        assert_eq!(
            static_kernel_for_identity(&masked, &avx2_paths, 14),
            Some(CpuKernel::Avx2Stream)
        );
        // Sapphire and Emerald Rapids start on streaming (full width when
        // available); other Intel models do not.
        for (model, stream) in [(0x8f, true), (0xcf, true), (0xad, false), (0x55, false)] {
            let xeon = CpuIdentity {
                vendor: Some("GenuineIntel".into()),
                family: Some(6),
                model: Some(model),
                ..CpuIdentity::default()
            };
            assert_eq!(candidates_for(&xeon, &supported), supported);
            assert_eq!(
                startup_kernel_for(&xeon, &supported),
                stream.then_some(CpuKernel::Avx512256Stream)
            );
            let with_full = [supported.as_slice(), &[CpuKernel::Avx512Stream]].concat();
            assert_eq!(
                startup_kernel_for(&xeon, &with_full),
                stream.then_some(CpuKernel::Avx512Stream)
            );
        }
        assert_eq!(
            candidates_for(&turin, &[CpuKernel::Avx2]),
            [CpuKernel::Avx2]
        );
    }

    #[test]
    fn fixed_counts_use_the_cohort_default_kernel() {
        use crate::runtime_identity::CpuIdentity;
        let all = [
            CpuKernel::Avx512,
            CpuKernel::Avx2,
            CpuKernel::Avx512256Stream,
        ];
        let masked_zen5 = CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x1a),
            model: Some(0),
            stepping: None,
            l3_cores: None,
        };
        let raphael = CpuIdentity {
            family: Some(0x19),
            model: Some(0x61),
            ..masked_zen5.clone()
        };
        for identity in [&masked_zen5, &raphael] {
            assert_eq!(
                static_kernel_for_identity(identity, &all, 192),
                Some(CpuKernel::Avx512256Stream)
            );
            assert_eq!(
                static_kernel_for_identity(identity, &all, 8),
                Some(CpuKernel::Avx512256Stream)
            );
            assert_eq!(static_kernel_for_identity(identity, &all, 7), None);
        }
        // Zen 3 shares family 19h but has no AVX-512, so no streaming path.
        let milan = CpuIdentity {
            model: Some(0x01),
            ..raphael.clone()
        };
        assert_eq!(static_kernel_for_identity(&milan, &all[..2], 48), None);
        // Zen 3 uses the AVX2 streaming path once it is runtime-supported.
        let avx2_only = [CpuKernel::Avx2, CpuKernel::Avx2Stream];
        assert_eq!(
            static_kernel_for_identity(&milan, &avx2_only, 12),
            Some(CpuKernel::Avx2Stream)
        );
        assert_eq!(
            candidates_for(&milan, &avx2_only),
            [CpuKernel::Avx2, CpuKernel::Avx2Stream]
        );
        // Zen 2 (family 17h, model 30h and up) shares the AVX2 streaming path.
        let rome = CpuIdentity {
            family: Some(0x17),
            model: Some(0x31),
            ..raphael.clone()
        };
        assert_eq!(
            static_kernel_for_identity(&rome, &avx2_only, 32),
            Some(CpuKernel::Avx2Stream)
        );
        // Zen 1/Zen+ (17h below 30h) is not yet validated natively.
        let naples = CpuIdentity {
            model: Some(0x01),
            ..rome.clone()
        };
        assert_eq!(static_kernel_for_identity(&naples, &avx2_only, 32), None);
        assert_eq!(
            candidates_for(&naples, &avx2_only),
            [CpuKernel::Avx2, CpuKernel::Avx2Stream]
        );
        let intel = CpuIdentity {
            vendor: Some("GenuineIntel".into()),
            ..masked_zen5.clone()
        };
        assert_eq!(static_kernel_for_identity(&intel, &all, 64), None);
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
    fn decisive_losers_are_not_retried_at_nearby_counts() {
        let mut t = three_path_tuner();
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512256Stream;
        let mut now = 0;
        // AVX-512 loses decisively (75%); AVX2 only mildly (95%).
        run_comparison(&mut t, &mut s, &mut now, &[100, 75, 100]);
        run_comparison(&mut t, &mut s, &mut now, &[100, 95, 100]);
        assert_eq!(s.kernel, CpuKernel::Avx512256Stream);
        assert_eq!(t.cache.decisive.len(), 1);
        assert_eq!(t.cache.decisive[0].loser, CpuKernel::Avx512.as_str());

        // A nearby count only re-checks the mild loser.
        s.threads = 2;
        s.backing_bytes = [0, 0, 0, 2 * CPU_LANE_MEMORY_BYTES, 0];
        s.committed_bytes = 2 * CPU_LANE_MEMORY_BYTES;
        s.resident_bytes = 2 * CPU_LANE_MEMORY_BYTES;
        t.next_check = 0;
        now += 1;
        t.tick(now, &[s], true, true);
        let trial = t.trial.as_ref().expect("the mild loser is still compared");
        assert_eq!(trial.candidate, CpuKernel::Avx2);
        assert!(
            t.pending.is_empty(),
            "decisive loser skipped: {:?}",
            t.pending
        );

        // A far count (more than double) gets the full sweep again.
        let wall = unix_secs();
        assert!(t.cache.decisive[0].applies(
            CpuKernel::Avx512256Stream,
            CpuKernel::Avx512,
            2,
            wall
        ));
        assert!(!t.cache.decisive[0].applies(
            CpuKernel::Avx512256Stream,
            CpuKernel::Avx512,
            3,
            wall
        ));
        let _ = fs::remove_file(&t.path);
    }

    #[test]
    fn turin_streaming_first_still_confirms_twice_and_checks_avx2_before_caching() {
        let mut t = three_path_tuner();
        t.candidates = candidates_for(
            &crate::runtime_identity::CpuIdentity {
                vendor: Some("AuthenticAMD".into()),
                family: Some(0x1a),
                model: Some(0x02),
                stepping: Some(1),
                l3_cores: None,
            },
            &t.candidates,
        );
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512;
        let mut now = 0;
        run_comparison(&mut t, &mut s, &mut now, &[100, 125, 100]);
        let trial = t
            .trial
            .as_ref()
            .expect("second confirmation remains required");
        assert_eq!(trial.base, CpuKernel::Avx512);
        assert_eq!(trial.candidate, CpuKernel::Avx512256Stream);
        assert_eq!(trial.samples.len(), 3);
        assert!(t.cache.winners.is_empty());
        // Complete the same active trial without starting a new comparison.
        for rate in [100, 125, 100] {
            now += 1;
            t.tick(now, &[s], true, true);
            now += 30;
            s.hashes += rate * 30;
            if let Some(kernel) = t.tick(now, &[s], true, true).kernel {
                s.kernel = kernel;
                s.kernel_generation += 1;
            }
        }
        assert_eq!(s.kernel, CpuKernel::Avx512256Stream);
        assert_eq!(t.pending, [CpuKernel::Avx2]);
        assert!(t.cache.winners.is_empty());
        run_comparison(&mut t, &mut s, &mut now, &[125, 95, 125]);
        assert_eq!(t.cache.winners.len(), 1);
        assert_eq!(t.cache.winners[0].kernel, "avx512-256-stream");
        assert_eq!(t.cache.winners[0].candidate, "avx2");
        assert!(t.pending.is_empty());
        let _ = fs::remove_file(&t.path);
    }

    #[test]
    fn genoa_ring_startup_is_host_scoped_and_requires_measured_backing() {
        if !CpuKernel::Avx512256StreamR8D2.is_supported() {
            return;
        }
        let mut cfg = super::super::tests::test_config();
        cfg.threads = 32;
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        cfg.cpu_autotune_config_path = std::env::temp_dir().join(format!(
            "seine-genoa-ring-policy-{}.json",
            std::process::id()
        ));
        let host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("AMD EPYC 9R14".into()),
            logical_cores: 32,
            physical_cores: Some(32),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "linux".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x11),
            ..Default::default()
        };
        let mut exact = Tuner::for_identity(&cfg, host.clone(), &identity);
        let _ = fs::remove_file(&exact.path);
        assert!(exact.candidates.contains(&CpuKernel::Avx512256Stream));
        assert!(exact.candidates.contains(&CpuKernel::Avx512256StreamR8D2));
        assert_eq!(exact.startup, Some(CpuKernel::Avx512256StreamR8D2));
        let mut state = snapshot();
        state.threads = 32;
        state.kernel = CpuKernel::Avx512;
        state.backing_bytes = [0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0, 0];
        assert_eq!(
            exact.tick(1, &[state], true, true).kernel,
            Some(CpuKernel::Avx512256StreamR8D2)
        );
        for (os, environment, brand) in [
            ("linux", "wsl2", "AMD EPYC 9R14"),
            ("linux", "native", "AMD EPYC 9355P"),
        ] {
            let mut other = host.clone();
            other.os = os.into();
            other.runtime_environment = environment.into();
            other.cpu_brand = Some(brand.into());
            let tuner = Tuner::for_identity(&cfg, other, &identity);
            assert!(!tuner.candidates.contains(&CpuKernel::Avx512256StreamR8D2));
            assert_ne!(tuner.startup, Some(CpuKernel::Avx512256StreamR8D2));
        }
        let mut fallback = Tuner::for_identity(&cfg, host, &identity);
        state.backing_bytes = [0, 0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0];
        assert_ne!(
            fallback.tick(1, &[state], true, true).kernel,
            Some(CpuKernel::Avx512256StreamR8D2)
        );
        let _ = fs::remove_file(&exact.path);
    }

    #[test]
    fn windows_ring_candidate_starts_only_on_regular_backing() {
        if !CpuKernel::Avx512256StreamR8D2.is_supported() {
            return;
        }
        let mut cfg = super::super::tests::test_config();
        cfg.threads = 32;
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        cfg.cpu_autotune_config_path =
            std::env::temp_dir().join(format!("seine-win-ring-{}.json", std::process::id()));
        let host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("AMD EPYC 9R14".into()),
            logical_cores: 32,
            physical_cores: Some(32),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "windows".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x11),
            ..Default::default()
        };
        for regular in [true, false] {
            let mut tuner = Tuner::for_identity(&cfg, host.clone(), &identity);
            let _ = fs::remove_file(&tuner.path);
            assert!(tuner.candidates.contains(&CpuKernel::Avx512256StreamR8D2));
            let mut state = snapshot();
            state.threads = 32;
            state.kernel = CpuKernel::Avx512;
            state.backing_bytes = [0; 5];
            state.backing_bytes[if regular { 3 } else { 2 }] = 32 * CPU_LANE_MEMORY_BYTES;
            let action = tuner.tick(1, &[state], true, true);
            assert_eq!(
                action.kernel,
                regular.then_some(CpuKernel::Avx512256StreamR8D2)
            );
        }
        cfg.cpu_page_mode = crate::config::CpuPageMode::Large;
        assert!(!Tuner::for_identity(&cfg, host, &identity)
            .candidates
            .contains(&CpuKernel::Avx512256StreamR8D2));
    }

    #[test]
    fn ring_candidate_expansion_restores_prior_winner_before_new_trial() {
        if !CpuKernel::Avx512256StreamR8D2.is_supported() {
            return;
        }
        let mut cfg = super::super::tests::test_config();
        cfg.threads = 31;
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        cfg.cpu_autotune_config_path = std::env::temp_dir().join(format!(
            "seine-ring-upgrade-seed-{}.json",
            std::process::id()
        ));
        let host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("AMD EPYC 9R14".into()),
            logical_cores: 32,
            physical_cores: Some(32),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "linux".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x11),
            ..Default::default()
        };
        let fresh = Tuner::for_identity(&cfg, host.clone(), &identity);
        let _ = fs::remove_file(&fresh.path);
        let context = Context {
            members: vec![0],
            workers: vec![31],
            backing: vec![[0, 0, 31 * CPU_LANE_MEMORY_BYTES, 0, 0]],
        };
        let mut prior = Cache {
            schema: CACHE_SCHEMA,
            host: host.clone(),
            configuration: fresh
                .cache
                .configuration
                .replace("kernels=[Avx512256StreamR8D2, ", "kernels=[")
                .replace(";sample_policy=genoa-thp-30-32-10s-256-v1", "")
                .replace(";frontier_policy=genoa-thp31-32-stream-first-v1", ""),
            winners: vec![Winner {
                context: context.clone(),
                kernel: "avx512-256-stream".into(),
                timestamp: unix_secs(),
                base: "avx512-256-stream".into(),
                candidate: "avx2".into(),
                samples: vec![sample(100); 3],
            }],
            decisive: Vec::new(),
        };
        assert_ne!(prior.configuration, fresh.cache.configuration);
        fs::write(&fresh.path, serde_json::to_vec(&prior).unwrap()).unwrap();
        let mut upgraded = Tuner::for_identity(&cfg, host.clone(), &identity);
        let mut state = snapshot();
        state.threads = 31;
        state.backing_bytes = context.backing[0];
        state.kernel = CpuKernel::Avx512;
        let first = upgraded.tick(1, &[state], true, true);
        assert_eq!(first.kernel, Some(CpuKernel::Avx512256Stream));
        assert!(first.hold_counts && upgraded.cache.winners.is_empty());
        state.kernel = CpuKernel::Avx512256Stream;
        state.kernel_generation += 1;
        upgraded.tick(2, &[state], true, true);
        upgraded.tick(3, &[state], true, true);
        assert_eq!(
            upgraded.trial.as_ref().unwrap().candidate,
            CpuKernel::Avx512256StreamR8D2
        );
        assert!(
            upgraded.pending.is_empty(),
            "do not repeat already confirmed old alternatives"
        );
        // A different setting or expired evidence cannot become an upgrade seed.
        for stale in [false, true] {
            prior.configuration = if stale {
                fresh
                    .cache
                    .configuration
                    .replace("kernels=[Avx512256StreamR8D2, ", "kernels=[")
                    .replace(";sample_policy=genoa-thp-30-32-10s-256-v1", "")
                    .replace(";frontier_policy=genoa-thp31-32-stream-first-v1", "")
            } else {
                "other-settings".into()
            };
            prior.winners[0].timestamp = if stale { 0 } else { unix_secs() };
            fs::write(&fresh.path, serde_json::to_vec(&prior).unwrap()).unwrap();
            let mut rejected = Tuner::for_identity(&cfg, host.clone(), &identity);
            state.kernel = CpuKernel::Avx512;
            assert_ne!(
                rejected.tick(1, &[state], true, true).kernel,
                Some(CpuKernel::Avx512256Stream)
            );
        }
        let _ = fs::remove_file(&fresh.path);
    }

    #[test]
    fn genoa_kernel_cache_survives_a_different_confirmed_starting_count() {
        if !CpuKernel::Avx512256Stream.is_supported() {
            return;
        }
        let mut cfg = super::super::tests::test_config();
        cfg.threads = 32;
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        cfg.cpu_autotune_config_path = std::env::temp_dir().join(format!(
            "seine-genoa-cache-seed-{}.json",
            std::process::id()
        ));
        let host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("AMD EPYC 9R14".into()),
            logical_cores: 32,
            physical_cores: Some(32),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "linux".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x11),
            ..Default::default()
        };
        let mut first = Tuner::for_identity(&cfg, host.clone(), &identity);
        let _ = fs::remove_file(&first.path);
        let context = Context {
            members: vec![0],
            workers: vec![31],
            backing: vec![[0, 0, 31 * CPU_LANE_MEMORY_BYTES, 0, 0]],
        };
        first.cache.winners = vec![Winner {
            context: context.clone(),
            kernel: "avx512-256-stream".into(),
            timestamp: unix_secs(),
            base: "avx512-256-stream".into(),
            candidate: "avx2".into(),
            samples: vec![sample(100); 3],
        }];
        let current_configuration = first.cache.configuration.clone();
        // Both an already-shortened policy and the preceding longer-window
        // policy must retain the known winner when a restart uses 31 workers.
        for prior_policy in 0..3 {
            first.cache.configuration = if prior_policy == 2 {
                current_configuration
                    .replace(";sample_policy=genoa-thp-30-32-10s-256-v1", "")
                    .replace(";frontier_policy=genoa-thp31-32-stream-first-v1", "")
            } else if prior_policy == 1 {
                current_configuration.replace(";frontier_policy=genoa-thp31-32-stream-first-v1", "")
            } else {
                current_configuration.clone()
            };
            fs::write(&first.path, serde_json::to_vec(&first.cache).unwrap()).unwrap();
            cfg.threads = 31;
            let mut restored = Tuner::for_identity(&cfg, host.clone(), &identity);
            assert_eq!(restored.cache.configuration, current_configuration);
            let mut state = snapshot();
            state.threads = 31;
            state.backing_bytes = context.backing[0];
            state.kernel = CpuKernel::Avx512;
            let action = restored.tick(1, &[state], true, true);
            assert_eq!(action.kernel, Some(CpuKernel::Avx512256Stream));
            assert!(action.hold_counts);
            assert!(
                restored.pending.is_empty(),
                "confirmed paths need no repeat sweep"
            );
        }
        let _ = fs::remove_file(&first.path);

        // No cached winner means a 31-worker context still needs ordinary
        // comparisons: the measured automatic streaming start remains 32 only.
        let mut fresh = Tuner::for_identity(&cfg, host, &identity);
        let mut s = snapshot();
        s.threads = 31;
        s.backing_bytes = context.backing[0];
        s.kernel = CpuKernel::Avx512;
        let action = fresh.tick(1, &[s], true, true);
        assert_eq!(action.kernel, None);
        assert!(action.hold_counts);
        assert!(!genoa_stream_context(&context));
    }

    #[test]
    fn genoa_frontier_is_initial_and_keeps_periodic_full_search() {
        for (workers, huge, enabled, expected) in [
            (32, true, true, CpuKernel::Avx512256Stream),
            (31, true, true, CpuKernel::Avx512256Stream),
            (30, true, true, CpuKernel::Avx512),
            (32, false, true, CpuKernel::Avx512),
            (32, true, false, CpuKernel::Avx512),
        ] {
            let mut t = tuner();
            t.genoa_startup_frontier = enabled;
            t.candidates = vec![
                CpuKernel::Avx512,
                CpuKernel::Avx2,
                CpuKernel::Avx512256Stream,
                CpuKernel::Avx512256StreamR8D2,
            ];
            let mut state = snapshot();
            state.threads = workers;
            state.kernel = CpuKernel::Avx512256StreamR8D2;
            state.backing_bytes = [0; 5];
            state.backing_bytes[if huge { 2 } else { 3 }] = workers as u64 * CPU_LANE_MEMORY_BYTES;
            let context = Context::from_snapshots(&[state]);
            t.tick(1, &[state], true, true);
            assert_eq!(t.trial.as_ref().unwrap().candidate, expected);
            if expected == CpuKernel::Avx512256Stream {
                assert!(t.pending.is_empty());
                let trial = t.trial.take().unwrap();
                t.cache.winners.push(Winner {
                    context: context.clone(),
                    kernel: "avx512-256-stream-r8d2".into(),
                    timestamp: unix_secs(),
                    base: "avx512-256-stream-r8d2".into(),
                    candidate: "avx512-256-stream".into(),
                    samples: vec![sample(100); 3],
                });
                t.checked = Some(trial.context);
                t.next_check = 0;
                t.tick(RECHECK_SECS + 2, &[state], true, true);
                assert_eq!(t.trial.as_ref().unwrap().candidate, CpuKernel::Avx512);
                assert!(t.pending.contains(&CpuKernel::Avx2));
            }
            let _ = fs::remove_file(&t.path);
        }
    }

    #[test]
    fn windows_frontier_keeps_backing_count_and_periodic_search_boundaries() {
        for (workers, regular, enabled, expected) in [
            (32, true, true, CpuKernel::Avx512256Stream),
            (31, true, true, CpuKernel::Avx512256Stream),
            (30, true, true, CpuKernel::Avx512),
            (32, false, true, CpuKernel::Avx512),
            (32, true, false, CpuKernel::Avx512),
        ] {
            let mut t = tuner();
            t.windows_genoa_startup = true;
            t.genoa_startup_frontier = enabled;
            t.candidates = vec![
                CpuKernel::Avx512,
                CpuKernel::Avx2,
                CpuKernel::Avx512256Stream,
                CpuKernel::Avx512256StreamR8D2,
            ];
            let mut state = snapshot();
            state.threads = workers;
            state.kernel = CpuKernel::Avx512256StreamR8D2;
            state.backing_bytes = [0; 5];
            state.backing_bytes[if regular { 3 } else { 2 }] =
                workers as u64 * CPU_LANE_MEMORY_BYTES;
            let context = Context::from_snapshots(&[state]);
            t.tick(1, &[state], true, true);
            assert_eq!(t.trial.as_ref().unwrap().candidate, expected);
            if expected == CpuKernel::Avx512256Stream {
                assert!(t.pending.is_empty());
                let trial = t.trial.take().unwrap();
                t.cache.winners.push(Winner {
                    context: context.clone(),
                    kernel: "avx512-256-stream-r8d2".into(),
                    timestamp: unix_secs(),
                    base: "avx512-256-stream-r8d2".into(),
                    candidate: "avx512-256-stream".into(),
                    samples: vec![sample(100); 3],
                });
                t.checked = Some(trial.context);
                t.next_check = 0;
                t.tick(RECHECK_SECS + 2, &[state], true, true);
                assert_eq!(t.trial.as_ref().unwrap().candidate, CpuKernel::Avx512);
                assert!(t.pending.contains(&CpuKernel::Avx2));
            }
            let _ = fs::remove_file(&t.path);
        }
        let mut context = Context {
            members: vec![0],
            workers: vec![32],
            backing: vec![[0, 0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0]],
        };
        context.backing[0][0] = CPU_LANE_MEMORY_BYTES;
        context.backing[0][3] -= CPU_LANE_MEMORY_BYTES;
        assert!(!windows_genoa_frontier_context(&context));
        context.backing[0] = [0, 0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0];
        context.members[0] = 1;
        assert!(!windows_genoa_frontier_context(&context));
    }

    #[test]
    fn windows_frontier_policy_and_previous_confirmed_cache_are_scoped() {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_autotune_secs = 6;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        cfg.cpu_autotune_config_path = std::env::temp_dir().join(format!(
            "seine-windows-frontier-cache-{}.json",
            std::process::id()
        ));
        let mut host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("AMD EPYC 9R14".into()),
            logical_cores: 32,
            physical_cores: Some(32),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "windows".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let mut identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x11),
            ..Default::default()
        };
        let paths = [CpuKernel::Avx512256Stream, CpuKernel::Avx512256StreamR8D2];
        assert!(windows_genoa_frontier_for(&cfg, &host, &identity, &paths));
        assert!(!windows_genoa_frontier_for(
            &cfg,
            &host,
            &identity,
            &paths[..1]
        ));
        assert!(!windows_genoa_frontier_for(
            &cfg,
            &host,
            &identity,
            &paths[1..]
        ));
        identity.model = Some(0x02);
        assert!(!windows_genoa_frontier_for(&cfg, &host, &identity, &paths));
        identity.model = Some(0x11);
        host.runtime_environment = "wsl2".into();
        assert!(!windows_genoa_frontier_for(&cfg, &host, &identity, &paths));
        host.runtime_environment = "native".into();
        cfg.cpu_autotune_secs = 12;
        assert!(!windows_genoa_frontier_for(&cfg, &host, &identity, &paths));
        cfg.cpu_autotune_secs = 6;
        if !CpuKernel::Avx512256StreamR8D2.is_supported() {
            return;
        }
        let initial = Tuner::for_identity(&cfg, host.clone(), &identity);
        let _ = fs::remove_file(&initial.path);
        assert!(initial.genoa_startup_frontier);
        let context = Context {
            members: vec![0],
            workers: vec![31],
            backing: vec![[0, 0, 0, 31 * CPU_LANE_MEMORY_BYTES, 0]],
        };
        let mut old = Cache {
            schema: CACHE_SCHEMA,
            host: host.clone(),
            configuration: initial.cache.configuration.replace(
                ";frontier_policy=genoa-windows-regular31-32-stream-first-v1",
                "",
            ),
            winners: vec![Winner {
                context: context.clone(),
                kernel: "avx512-256-stream-r8d2".into(),
                timestamp: unix_secs(),
                base: "avx512-256-stream-r8d2".into(),
                candidate: "avx2".into(),
                samples: vec![sample(100); 3],
            }],
            decisive: Vec::new(),
        };
        for valid in [true, false] {
            if !valid {
                old.winners[0].timestamp = 0;
            }
            fs::write(&initial.path, serde_json::to_vec(&old).unwrap()).unwrap();
            let restored = Tuner::for_identity(&cfg, host.clone(), &identity);
            assert_eq!(
                restored
                    .cache
                    .winner(&context, unix_secs(), &restored.candidates),
                valid.then_some(CpuKernel::Avx512256StreamR8D2)
            );
        }
        let _ = fs::remove_file(&initial.path);
    }

    #[test]
    fn genoa_short_policy_requires_measured_host_default_window_and_supported_path() {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_autotune_secs = 6;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        let mut host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("AMD EPYC 9R14".into()),
            logical_cores: 32,
            physical_cores: Some(32),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "linux".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x11),
            stepping: Some(1),
            l3_cores: None,
        };
        let paths = [CpuKernel::Avx512, CpuKernel::Avx512256Stream];
        // A cached starting count need not be 32; context eligibility is checked
        // again for each actual trial, independently of startup count.
        cfg.threads = 31;
        assert!(genoa_short_trials_for(&cfg, &host, &identity, &paths));
        assert!(!genoa_short_trials_for(&cfg, &host, &identity, &paths[..1]));
        cfg.cpu_autotune_secs = 12;
        assert!(!genoa_short_trials_for(&cfg, &host, &identity, &paths));
        cfg.cpu_autotune_secs = 6;
        host.runtime_environment = "wsl2".into();
        assert!(!genoa_short_trials_for(&cfg, &host, &identity, &paths));
        host.runtime_environment = "native".into();
        host.os = "windows".into();
        assert!(!genoa_short_trials_for(&cfg, &host, &identity, &paths));
        host.os = "linux".into();
        cfg.cpu_profile = CpuPerformanceProfile::Efficiency;
        assert!(!genoa_short_trials_for(&cfg, &host, &identity, &paths));
    }

    #[test]
    fn genoa_short_context_rejects_unmeasured_counts_backing_and_members() {
        for workers in [1, 16, 29, 30, 31, 32, 33] {
            let context = Context {
                members: vec![0],
                workers: vec![workers],
                backing: vec![[0, 0, workers as u64 * CPU_LANE_MEMORY_BYTES, 0, 0]],
            };
            assert_eq!(genoa_short_context(&context), (30..=32).contains(&workers));
            let mut mixed = context.clone();
            mixed.backing[0][2] -= CPU_LANE_MEMORY_BYTES;
            mixed.backing[0][3] = CPU_LANE_MEMORY_BYTES;
            assert!(!genoa_short_context(&mixed));
            let mut different_member = context.clone();
            different_member.members[0] = 1;
            assert!(!genoa_short_context(&different_member));
            let mut multiple = context.clone();
            multiple.members.push(1);
            multiple.workers.push(workers);
            multiple.backing.push(context.backing[0]);
            assert!(!genoa_short_context(&multiple));
        }
    }

    #[test]
    fn genoa_short_trial_requires_two_wins_and_keeps_full_windows_on_fallback_pages() {
        for huge in [true, false] {
            let mut t = tuner();
            t.genoa_short_trials = true;
            t.candidates = vec![CpuKernel::Avx512, CpuKernel::Avx2];
            let mut s = snapshot();
            s.threads = 31;
            s.backing_bytes = [0; 5];
            s.backing_bytes[if huge { 2 } else { 3 }] = 31 * CPU_LANE_MEMORY_BYTES;
            s.kernel = CpuKernel::Avx512;
            let mut now = 0;
            assert!(t.tick(now, &[s], true, true).hold_counts);
            for (index, rate) in [40, 50, 40, 40, 50, 40].into_iter().enumerate() {
                now += 1;
                s.kernel_ready = false;
                t.tick(now, &[s], true, true);
                assert!(t.trial.as_ref().unwrap().window.is_none());
                s.kernel_ready = true;
                t.tick(now, &[s], true, true);
                let seconds = if huge { 10 } else { 30 };
                now += seconds;
                s.hashes += rate * seconds;
                let action = t.tick(now, &[s], true, true);
                if let Some(kernel) = action.kernel {
                    s.kernel = kernel;
                    s.kernel_generation += 1;
                }
                if index < 5 {
                    assert!(t.cache.winners.is_empty());
                }
            }
            assert_eq!(s.kernel, CpuKernel::Avx2);
            assert_eq!(t.cache.winners.len(), 1);
            assert!(t.cache.winners[0]
                .samples
                .iter()
                .all(|sample| sample.secs == if huge { 10 } else { 30 }));
            let _ = fs::remove_file(&t.path);
        }
    }

    #[test]
    fn experimental_genoa_startup_requires_full_count_host_and_supported_path() {
        let mut cfg = super::super::tests::test_config();
        cfg.threads = 32;
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        let mut host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("AMD EPYC 9R14".into()),
            logical_cores: 32,
            physical_cores: Some(32),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "linux".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x11),
            stepping: Some(1),
            l3_cores: None,
        };
        let paths = [CpuKernel::Avx512, CpuKernel::Avx512256Stream];
        assert!(genoa_stream_startup(&cfg, &host, &identity, &paths));
        assert!(!genoa_stream_startup(&cfg, &host, &identity, &paths[..1]));
        cfg.threads = 24;
        assert!(genoa_stream_startup(&cfg, &host, &identity, &paths));
        cfg.threads = 32;
        cfg.cpu_auto_threads_cap = 24;
        assert!(!genoa_stream_startup(&cfg, &host, &identity, &paths));
        cfg.cpu_auto_threads_cap = 32;
        host.runtime_environment = "wsl2".into();
        assert!(!genoa_stream_startup(&cfg, &host, &identity, &paths));
    }

    #[test]
    fn experimental_sapphire_startup_requires_full_count_host_and_supported_path() {
        let mut cfg = super::super::tests::test_config();
        cfg.threads = 32;
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        let mut host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("Intel(R) Xeon(R) Platinum 8488C".into()),
            logical_cores: 32,
            physical_cores: Some(16),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "linux".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("GenuineIntel".into()),
            family: Some(6),
            model: Some(0x8f),
            stepping: Some(1),
            l3_cores: None,
        };
        let paths = [CpuKernel::Avx512, CpuKernel::Avx512256Stream];
        assert!(sapphire_stream_startup(&cfg, &host, &identity, &paths));
        assert!(!sapphire_stream_startup(
            &cfg,
            &host,
            &identity,
            &paths[..1]
        ));
        cfg.threads = 24;
        assert!(sapphire_stream_startup(&cfg, &host, &identity, &paths));
        cfg.threads = 32;
        cfg.cpu_auto_threads_cap = 24;
        assert!(!sapphire_stream_startup(&cfg, &host, &identity, &paths));
        cfg.cpu_auto_threads_cap = 32;
        host.runtime_environment = "wsl2".into();
        assert!(!sapphire_stream_startup(&cfg, &host, &identity, &paths));
    }

    #[test]
    fn sapphire_startup_requires_actual_full_thp_context() {
        for backing in [
            [0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0, 0],
            [0, 0, 31 * CPU_LANE_MEMORY_BYTES, CPU_LANE_MEMORY_BYTES, 0],
            [0, 0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0],
        ] {
            let mut t = three_path_tuner();
            t.sapphire_startup = true;
            t.startup = Some(CpuKernel::Avx512256Stream);
            let mut s = snapshot();
            s.threads = 32;
            s.backing_bytes = backing;
            s.kernel = CpuKernel::Avx512;
            let action = t.tick(1, &[s], true, true);
            assert_eq!(
                action.kernel,
                (backing[2] == 32 * CPU_LANE_MEMORY_BYTES).then_some(CpuKernel::Avx512256Stream)
            );
            assert!(
                t.cache.winners.is_empty(),
                "a startup hint is not a confirmed winner"
            );
        }
    }

    #[test]
    fn turin_startup_policy_preserves_confirmed_cache() {
        if !CpuKernel::Avx512256Stream.is_supported() {
            return;
        }
        let mut cfg = super::super::tests::test_config();
        cfg.threads = 32;
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        cfg.cpu_autotune_config_path = std::env::temp_dir().join(format!(
            "seine-turin-startup-cache-{}.json",
            std::process::id()
        ));
        let host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("AMD EPYC 9R45".into()),
            logical_cores: 32,
            physical_cores: Some(32),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "linux".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x1a),
            model: Some(0x02),
            ..Default::default()
        };
        let fresh = Tuner::for_identity(&cfg, host.clone(), &identity);
        let _ = fs::remove_file(&fresh.path);
        let context = Context {
            members: vec![0],
            workers: vec![32],
            backing: vec![[0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0, 0]],
        };
        let old = Cache {
            schema: CACHE_SCHEMA,
            host: host.clone(),
            configuration: fresh
                .cache
                .configuration
                .replace(";startup=turin9r45-32-thp-v1", ""),
            winners: vec![Winner {
                context: context.clone(),
                kernel: "avx512".into(),
                timestamp: unix_secs(),
                base: "avx512".into(),
                candidate: "avx2".into(),
                samples: vec![sample(100); 3],
            }],
            decisive: Vec::new(),
        };
        assert_ne!(old.configuration, fresh.cache.configuration);
        fs::write(&fresh.path, serde_json::to_vec(&old).unwrap()).unwrap();
        let mut upgraded = Tuner::for_identity(&cfg, host, &identity);
        assert_eq!(upgraded.cache.configuration, fresh.cache.configuration);
        assert_eq!(upgraded.cache.winners.len(), 1);
        let mut state = snapshot();
        state.threads = 32;
        state.backing_bytes = context.backing[0];
        state.kernel = CpuKernel::Avx2;
        assert_eq!(
            upgraded.tick(1, &[state], true, true).kernel,
            Some(CpuKernel::Avx512)
        );
        let _ = fs::remove_file(&fresh.path);
    }

    #[test]
    fn turin_demand_preserves_prior_winner_and_compares_new_path() {
        if !CpuKernel::Avx512256Stream.is_supported() {
            return;
        }
        let mut cfg = super::super::tests::test_config();
        cfg.threads = 32;
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        cfg.cpu_autotune_config_path = std::env::temp_dir().join(format!(
            "seine-turin-demand-cache-{}.json",
            std::process::id()
        ));
        let host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("AMD EPYC 9R45".into()),
            logical_cores: 32,
            physical_cores: Some(32),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "linux".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x1a),
            model: Some(0x02),
            ..Default::default()
        };
        let fresh = Tuner::for_identity(&cfg, host.clone(), &identity);
        let _ = fs::remove_file(&fresh.path);
        let context = Context {
            members: vec![0],
            workers: vec![32],
            backing: vec![[0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0, 0]],
        };
        for prior_kernel in [
            CpuKernel::Avx512,
            CpuKernel::Avx512256StreamR64D3,
            CpuKernel::Avx512256StreamR64D4,
        ] {
            let prior_demand = prior_kernel != CpuKernel::Avx512;
            let mut configuration = fresh
                .cache
                .configuration
                .replace("Avx512256StreamR64D6, ", "");
            if prior_kernel != CpuKernel::Avx512256StreamR64D4 {
                configuration = configuration.replace("Avx512256StreamR64D4, ", "");
            }
            if !prior_demand {
                configuration = configuration.replace("Avx512256StreamR64D3, ", "");
            }
            let old = Cache {
                schema: CACHE_SCHEMA,
                host: host.clone(),
                configuration,
                winners: vec![Winner {
                    context: context.clone(),
                    kernel: prior_kernel.as_str().into(),
                    timestamp: unix_secs(),
                    base: prior_kernel.as_str().into(),
                    candidate: if prior_demand {
                        "avx512-256-stream"
                    } else {
                        "avx2"
                    }
                    .into(),
                    samples: vec![
                        Sample {
                            hashes: 1500,
                            secs: 30
                        };
                        3
                    ],
                }],
                decisive: Vec::new(),
            };
            assert_ne!(old.configuration, fresh.cache.configuration);
            fs::write(&fresh.path, serde_json::to_vec(&old).unwrap()).unwrap();
            let mut upgraded = Tuner::for_identity(&cfg, host.clone(), &identity);
            assert_eq!(upgraded.cache.configuration, fresh.cache.configuration);
            assert!(upgraded.cache.winners.is_empty());
            assert_eq!(upgraded.startup_cache.as_ref().unwrap().winners.len(), 1);
            let mut state = snapshot();
            state.threads = 32;
            state.backing_bytes = context.backing[0];
            state.kernel = CpuKernel::Avx2;
            assert_eq!(
                upgraded.tick(1, &[state], true, true).kernel,
                Some(prior_kernel)
            );
            assert_eq!(upgraded.pending, [CpuKernel::Avx512256StreamR64D6]);
        }
        let _ = fs::remove_file(&fresh.path);
    }

    #[test]
    fn experimental_turin_startup_requires_full_count_host_and_supported_path() {
        let mut cfg = super::super::tests::test_config();
        cfg.threads = 32;
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        let mut host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("AMD EPYC 9R45".into()),
            logical_cores: 32,
            physical_cores: Some(32),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "linux".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x1a),
            model: Some(0x02),
            stepping: Some(1),
            l3_cores: None,
        };
        let paths = [CpuKernel::Avx512, CpuKernel::Avx512256Stream];
        assert!(turin_stream_startup(&cfg, &host, &identity, &paths));
        assert!(!turin_stream_startup(&cfg, &host, &identity, &paths[..1]));
        let demand_paths = [CpuKernel::Avx512256Stream, CpuKernel::Avx512256StreamR64D3];
        let prefetch_paths = [
            CpuKernel::Avx512256Stream,
            CpuKernel::Avx512256StreamR64D3,
            CpuKernel::Avx512256StreamR64D4,
        ];
        let mut depth6_paths = prefetch_paths.to_vec();
        depth6_paths.push(CpuKernel::Avx512256StreamR64D6);
        assert!(turin_depth6_startup(&cfg, &host, &identity, &depth6_paths));
        assert!(!turin_depth6_startup(
            &cfg,
            &host,
            &identity,
            &prefetch_paths
        ));
        let mut neighbor = identity.clone();
        neighbor.model = Some(0x11);
        assert!(!turin_depth6_startup(&cfg, &host, &neighbor, &depth6_paths));
        let mut other_os = host.clone();
        other_os.os = "windows".into();
        assert!(!turin_depth6_startup(
            &cfg,
            &other_os,
            &identity,
            &depth6_paths
        ));
        assert!(turin_prefetch_startup(
            &cfg,
            &host,
            &identity,
            &prefetch_paths
        ));
        assert!(!turin_prefetch_startup(
            &cfg,
            &host,
            &identity,
            &demand_paths
        ));
        let mut unsupported_host = host.clone();
        unsupported_host.os = "windows".into();
        assert!(!turin_prefetch_startup(
            &cfg,
            &unsupported_host,
            &identity,
            &prefetch_paths
        ));
        unsupported_host.os = "linux".into();
        unsupported_host.total_memory_bytes = 64 * 1024_u64.pow(3);
        assert!(!turin_prefetch_startup(
            &cfg,
            &unsupported_host,
            &identity,
            &prefetch_paths
        ));
        assert!(turin_demand_startup(&cfg, &host, &identity, &demand_paths));
        assert!(!turin_demand_startup(
            &cfg,
            &host,
            &identity,
            &demand_paths[..1]
        ));
        let mut neighbor = identity.clone();
        neighbor.model = Some(0x11);
        assert!(!turin_demand_startup(&cfg, &host, &neighbor, &demand_paths));
        let mut windows = host.clone();
        windows.os = "windows".into();
        assert!(!turin_demand_startup(
            &cfg,
            &windows,
            &identity,
            &demand_paths
        ));
        if CpuKernel::Avx512256StreamR64D3.is_supported() {
            let tuner = Tuner::for_identity(&cfg, host.clone(), &identity);
            assert!(tuner.candidates.contains(&CpuKernel::Avx512256StreamR64D3));
            assert!(tuner.candidates.contains(&CpuKernel::Avx512256StreamR64D4));
            assert_eq!(tuner.startup, Some(CpuKernel::Avx512256StreamR64D6));
            let other = Tuner::for_identity(&cfg, host.clone(), &neighbor);
            assert!(!other.candidates.contains(&CpuKernel::Avx512256StreamR64D3));
        }

        cfg.threads = 24;
        assert!(turin_stream_startup(&cfg, &host, &identity, &paths));
        cfg.threads = 32;
        cfg.cpu_auto_threads_cap = 24;
        assert!(!turin_stream_startup(&cfg, &host, &identity, &paths));
        cfg.cpu_auto_threads_cap = 32;
        host.runtime_environment = "wsl2".into();
        assert!(!turin_stream_startup(&cfg, &host, &identity, &paths));
    }

    #[test]
    fn turin_startup_requires_actual_full_thp_context() {
        for backing in [
            [0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0, 0],
            [0, 0, 31 * CPU_LANE_MEMORY_BYTES, CPU_LANE_MEMORY_BYTES, 0],
            [0, 0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0],
        ] {
            let mut t = three_path_tuner();
            t.turin_startup = true;
            t.candidates.push(CpuKernel::Avx512256StreamR64D3);
            t.startup = Some(CpuKernel::Avx512256StreamR64D3);
            let mut s = snapshot();
            s.threads = 32;
            s.backing_bytes = backing;
            s.kernel = CpuKernel::Avx512;
            let action = t.tick(1, &[s], true, true);
            assert_eq!(
                action.kernel,
                (backing[2] == 32 * CPU_LANE_MEMORY_BYTES)
                    .then_some(CpuKernel::Avx512256StreamR64D3)
            );
            assert!(
                t.cache.winners.is_empty(),
                "a startup hint is not a confirmed winner"
            );
        }
    }

    #[test]
    fn turin_prefetch_startup_requires_actual_full_thp_context() {
        for backing in [
            [0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0, 0],
            [0, 0, 31 * CPU_LANE_MEMORY_BYTES, CPU_LANE_MEMORY_BYTES, 0],
            [0, 0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0],
        ] {
            let mut t = three_path_tuner();
            t.turin_startup = true;
            t.candidates.push(CpuKernel::Avx512256StreamR64D4);
            t.startup = Some(CpuKernel::Avx512256StreamR64D4);
            let mut s = snapshot();
            s.threads = 32;
            s.backing_bytes = backing;
            s.kernel = CpuKernel::Avx512;
            let action = t.tick(1, &[s], true, true);
            assert_eq!(
                action.kernel,
                (backing[2] == 32 * CPU_LANE_MEMORY_BYTES)
                    .then_some(CpuKernel::Avx512256StreamR64D4)
            );
            assert!(
                t.cache.winners.is_empty(),
                "a startup hint is not a confirmed winner"
            );
        }
    }

    #[test]
    fn turin_depth6_startup_requires_actual_full_thp_context() {
        for backing in [
            [0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0, 0],
            [0, 0, 31 * CPU_LANE_MEMORY_BYTES, CPU_LANE_MEMORY_BYTES, 0],
            [0, 0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0],
        ] {
            let mut t = three_path_tuner();
            t.turin_startup = true;
            t.candidates.push(CpuKernel::Avx512256StreamR64D6);
            t.startup = Some(CpuKernel::Avx512256StreamR64D6);
            let mut s = snapshot();
            s.threads = 32;
            s.backing_bytes = backing;
            s.kernel = CpuKernel::Avx512;
            let action = t.tick(1, &[s], true, true);
            assert_eq!(
                action.kernel,
                (backing[2] == 32 * CPU_LANE_MEMORY_BYTES)
                    .then_some(CpuKernel::Avx512256StreamR64D6)
            );
            assert!(
                t.cache.winners.is_empty(),
                "a startup hint is not a confirmed winner"
            );
        }
    }

    #[test]
    fn experimental_genoa_context_requires_one_full_thp_32_worker_backend() {
        let context = Context {
            members: vec![0],
            workers: vec![32],
            backing: vec![[0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0, 0]],
        };
        assert!(genoa_stream_context(&context));
        let mut changed = context.clone();
        changed.workers[0] = 24;
        assert!(!genoa_stream_context(&changed));
        changed = context.clone();
        changed.backing[0] = [0, 0, 31 * CPU_LANE_MEMORY_BYTES, CPU_LANE_MEMORY_BYTES, 0];
        assert!(!genoa_stream_context(&changed));
        changed.backing[0] = [32 * CPU_LANE_MEMORY_BYTES, 0, 0, 0, 0];
        assert!(!genoa_stream_context(&changed));
        changed = context.clone();
        changed.members.push(1);
        changed.workers.push(32);
        changed.backing.push(context.backing[0]);
        assert!(!genoa_stream_context(&changed));
    }

    #[test]
    fn windows_genoa_startup_keeps_host_backing_and_cache_boundaries() {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_auto_threads_cap = 32;
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        let mut host = CpuAutotuneHostFingerprint {
            cpu_brand: Some("AMD EPYC 9R14".into()),
            logical_cores: 32,
            physical_cores: Some(32),
            total_memory_bytes: 128 * 1024_u64.pow(3),
            os: "windows".into(),
            runtime_environment: "native".into(),
            ..Default::default()
        };
        let identity = crate::runtime_identity::CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x11),
            ..Default::default()
        };
        let paths = [CpuKernel::Avx512, CpuKernel::Avx512256Stream];
        assert!(windows_genoa_stream_startup(&cfg, &host, &identity, &paths));
        assert!(!genoa_stream_startup(&cfg, &host, &identity, &paths));
        assert!(!windows_genoa_stream_startup(
            &cfg,
            &host,
            &identity,
            &paths[..1]
        ));
        host.os = "linux".into();
        assert!(!windows_genoa_stream_startup(
            &cfg, &host, &identity, &paths
        ));
        host.os = "windows".into();
        cfg.cpu_profile = CpuPerformanceProfile::Efficiency;
        assert!(!windows_genoa_stream_startup(
            &cfg, &host, &identity, &paths
        ));
        cfg.cpu_profile = CpuPerformanceProfile::Throughput;

        let context = Context {
            members: vec![0],
            workers: vec![32],
            backing: vec![[0, 0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0]],
        };
        assert!(windows_genoa_stream_context(&context));
        for backing in [
            [32 * CPU_LANE_MEMORY_BYTES, 0, 0, 0, 0],
            [0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0, 0],
            [CPU_LANE_MEMORY_BYTES, 0, 0, 31 * CPU_LANE_MEMORY_BYTES, 0],
        ] {
            let mut other = context.clone();
            other.backing[0] = backing;
            assert!(!windows_genoa_stream_context(&other));
        }
        let mut other = context.clone();
        other.workers[0] = 31;
        assert!(!windows_genoa_stream_context(&other));
        other = context.clone();
        other.members.push(1);
        assert!(!windows_genoa_stream_context(&other));

        if !CpuKernel::Avx512256Stream.is_supported() {
            return;
        }
        cfg.threads = 32;
        let mut t = Tuner::for_identity(&cfg, host.clone(), &identity);
        cfg.threads = 31;
        let restarted = Tuner::for_identity(&cfg, host, &identity);
        assert_eq!(t.cache.configuration, restarted.cache.configuration);
        assert!(t.windows_genoa_startup && !t.genoa_startup);
        t.cache.winners = vec![Winner {
            context: context.clone(),
            kernel: "avx512".into(),
            timestamp: unix_secs(),
            base: "avx512-256-stream".into(),
            candidate: "avx512".into(),
            samples: [100, 130, 100, 100, 130, 100].map(sample).to_vec(),
        }];
        let mut s = snapshot();
        s.threads = 32;
        s.backing_bytes = context.backing[0];
        s.kernel = CpuKernel::Avx512;
        let action = t.tick(1, &[s], true, true);
        assert_eq!(
            action.kernel, None,
            "confirmed winner precedes startup hint"
        );
        assert!(!action.hold_counts);
    }

    #[test]
    fn experimental_genoa_startup_can_lose_and_cache_a_different_kernel() {
        for windows in [false, true] {
            let mut t = three_path_tuner();
            t.genoa_startup = !windows;
            t.windows_genoa_startup = windows;
            t.startup = Some(CpuKernel::Avx512256Stream);
            let mut s = snapshot();
            s.threads = 32;
            s.backing_bytes = if windows {
                [0, 0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0]
            } else {
                [0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0, 0]
            };
            s.kernel = CpuKernel::Avx512;
            let action = t.tick(1, &[s], true, true);
            assert_eq!(action.kernel, Some(CpuKernel::Avx512256Stream));
            assert!(t.cache.winners.is_empty());
            s.kernel = CpuKernel::Avx512256Stream;
            s.kernel_generation += 1;
            // Plain-kernel challengers wait out the startup half hour.
            let mut now = GENERIC_TRIAL_DEFER_SECS;
            run_comparison(&mut t, &mut s, &mut now, &[100, 130, 100, 100, 130, 100]);
            assert_eq!(s.kernel, CpuKernel::Avx512);
            run_comparison(&mut t, &mut s, &mut now, &[130, 120, 130]);
            assert_eq!(t.cache.winners.len(), 1);
            assert_eq!(t.cache.winners[0].kernel, "avx512");
            let _ = fs::remove_file(&t.path);
        }
    }

    #[test]
    fn streaming_startup_requires_zen4_or_zen5_identity_and_candidate() {
        use crate::runtime_identity::CpuIdentity;
        let ryzen = CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x1a),
            model: Some(0x44),
            ..CpuIdentity::default()
        };
        let all = [
            CpuKernel::Avx512,
            CpuKernel::Avx2,
            CpuKernel::Avx512256Stream,
        ];
        for zen in [
            ryzen.clone(),
            CpuIdentity {
                model: Some(0x02),
                ..ryzen.clone()
            },
            CpuIdentity {
                family: Some(0x19),
                ..ryzen.clone()
            },
        ] {
            assert_eq!(
                startup_kernel_for(&zen, &all),
                Some(CpuKernel::Avx512256Stream)
            );
            assert_eq!(startup_kernel_for(&zen, &all[..2]), None);
        }
        for other in [
            CpuIdentity {
                family: Some(0x17),
                ..ryzen.clone()
            },
            CpuIdentity {
                vendor: Some("GenuineIntel".into()),
                ..ryzen.clone()
            },
            CpuIdentity::default(),
        ] {
            assert_eq!(startup_kernel_for(&other, &all), None);
        }
    }

    #[test]
    fn validated_startup_path_is_compared_against_others_from_eight_workers() {
        let mut t = three_path_tuner();
        t.startup = Some(CpuKernel::Avx512256Stream);
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512;
        s.threads = 4;
        // Below the validated range, start comparisons on the default path.
        let a = t.tick(1, &[s], true, true);
        assert_eq!(a.kernel, None);
        assert_eq!(t.trial.as_ref().map(|t| t.base), Some(CpuKernel::Avx512));

        let mut t = three_path_tuner();
        t.startup = Some(CpuKernel::Avx512256Stream);
        s.threads = 8;
        let a = t.tick(1, &[s], true, true);
        assert_eq!(a.kernel, Some(CpuKernel::Avx512256Stream));
        assert!(a.hold_counts && a.reset_scores);
        assert!(t.cache.winners.is_empty());
        s.kernel = CpuKernel::Avx512256Stream;
        s.kernel_generation += 1;
        // Settles, then defers the plain-kernel challengers for the first half hour.
        assert!(!t.tick(2, &[s], true, true).hold_counts);
        assert!(
            !t.tick(GENERIC_TRIAL_DEFER_SECS - 1, &[s], true, true)
                .hold_counts
        );
        assert!(t.trial.is_none());
        let mut now = GENERIC_TRIAL_DEFER_SECS;
        let a = run_comparison(&mut t, &mut s, &mut now, &[100, 95, 100]);
        assert_eq!(s.kernel, CpuKernel::Avx512256Stream);
        assert!(a
            .message
            .unwrap()
            .contains("avx512-256-stream 100.00 / avx512 95.00"));
        run_comparison(&mut t, &mut s, &mut now, &[100, 90, 100]);
        assert_eq!(s.kernel, CpuKernel::Avx512256Stream);
        assert_eq!(t.cache.winners.len(), 1);
        assert_eq!(t.cache.winners[0].kernel, "avx512-256-stream");
        let _ = fs::remove_file(&t.path);
    }

    #[test]
    fn drifted_baseline_is_reported_separately_from_no_gain() {
        let mut t = three_path_tuner();
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512;
        let mut now = 0;
        let a = run_comparison(&mut t, &mut s, &mut now, &[100, 130, 90]);
        let message = a.message.unwrap();
        assert!(
            message.starts_with("keeping current kernel; baseline drifted"),
            "{message}"
        );
        assert!(
            message.contains("avx512 100.00 / avx2 130.00 / avx512 90.00"),
            "{message}"
        );
        assert!(t.cache.winners.is_empty());
        assert_eq!(t.next_check, now + RETRY_SECS);
        let _ = fs::remove_file(&t.path);
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
        let identity = crate::runtime_identity::cpu_identity();
        assert!(
            identity.vendor.as_deref() == Some("AuthenticAMD")
                && identity.family == Some(0x1a)
                && identity.model == Some(0x02),
            "requires validated EPYC identity; do not allocate 24 arenas on a desktop"
        );
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_config_path =
            std::env::temp_dir().join(format!("seine-epyc-productive-{}", std::process::id()));
        cfg.cpu_autotune_secs = 6;
        let mut t = Tuner::new(&cfg, cpu_autotune_host_fingerprint());
        assert!(
            t.candidates.contains(&CpuKernel::Avx512256Stream),
            "requires runtime support for the streaming kernel"
        );
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
                        pow: crate::backend::PowAlgorithm::Argon2id,
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
            decisive: Vec::new(),
        };
        assert!(cache.compatible(&host, "test"));
        cache.schema = 1;
        assert!(!cache.compatible(&host, "test"));
        cache.schema = CACHE_SCHEMA;
        assert!(cache.compatible(&host, "test"));
        let mut other = host.clone();
        other.build_fingerprint = "changed".into();
        assert!(!cache.compatible(&other, "test"));
        other = host.clone();
        other.affinity_topology = "changed".into();
        assert!(!cache.compatible(&other, "test"));
        assert!(!cache.compatible(&host, "other"));
        let mut changed = context.clone();
        changed.members[0] = 1;
        assert!(cache.winner(&changed, 100, &candidates()).is_none());
        changed = context.clone();
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
    #[test]
    fn abort_between_challengers_backs_off_only_that_count() {
        let mut t = three_path_tuner();
        let mut s = snapshot();
        s.kernel = CpuKernel::Avx512;
        let mut now = 0;
        run_comparison(&mut t, &mut s, &mut now, &[100, 95, 100]);
        assert!(t.is_comparing() && t.trial.is_none());
        assert!(t.abort(now + 1).interrupted);
        assert!(t.pending.is_empty() && t.cache.winners.is_empty());
        assert!(t.retry_pending(now + 2, &[s]));
        s.threads += 1;
        s.warmed += 1;
        assert!(t.tick(now + 2, &[s], true, true).hold_counts);
        let _ = fs::remove_file(t.path);
    }
}
