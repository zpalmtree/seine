//! Which CPU kernels each CPU may use, which it starts on, and when.
//! Architecture eligibility permits a comparison; only measured gains select a path.
use super::*;
use crate::backend::cpu::CpuAdaptiveSnapshot;
use blocknet_pow_kernel::CpuKernel;

// ARM starts on the portable NEON path; SHA3 is eligible only after runtime
// feature detection and must win a productive-work comparison.
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
        if stream.skips_stores() {
            // The plain path stays a challenger so the tuner can turn skipping off.
            paths.push(stream);
            paths.push(if stream == CpuKernel::Avx2StreamSkip {
                CpuKernel::Avx2Stream
            } else {
                CpuKernel::Avx512Stream
            });
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
        // Alder Lake, Raptor Lake and Arrow Lake desktops (AVX2 only) use the AVX2
        // streaming path.
        if matches!(
            identity.model,
            Some(0x97 | 0x9a | 0xb7 | 0xba | 0xbf | 0xc6)
        ) {
            return (AVX2_STREAM_OS && supported.contains(&CpuKernel::Avx2Stream))
                .then_some(CpuKernel::Avx2Stream);
        }
        // Skylake-SP and Cascade Lake (06h/55h), Ice Lake (06h/6Ah, 6Ch) and Granite
        // Rapids (06h/ADh, AEh) Xeons use the same streaming paths.
        if !matches!(
            identity.model,
            Some(0x55 | 0x8f | 0xcf | 0x6a | 0x6c | 0xad | 0xae)
        ) {
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
        // Ryzen 5000 desktops (19h 21h) on Linux start with skipped stores; the plain
        // path is compared against it while mining.
        Some(0x19)
            if cfg!(any(target_os = "linux", test))
                && identity.model == Some(0x21)
                && supported.contains(&CpuKernel::Avx2StreamSkip) =>
        {
            Some(CpuKernel::Avx2StreamSkip)
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

/// Ryzen 3000 and 4000 (17h 71h, 60h), Ryzen 5000 (19h 21h, 50h), Ryzen 7000 (19h
/// 61h), EPYC 9004 (19h 11h, A0h), EPYC 9005 (1Ah 02h), Ryzen 9000 (1Ah 44h), Intel
/// 12th Gen to Core Ultra 200 desktops, and Skylake-SP to Granite Rapids Xeons start on
/// their cohort kernel at any worker count; EPYC 7002 and 7003 (17h 31h, 19h 01h) from
/// four workers.
fn startup_stream_min_workers(identity: &crate::runtime_identity::CpuIdentity) -> usize {
    let any_count = match identity.vendor.as_deref() {
        Some("AuthenticAMD") => matches!(
            (identity.family, identity.model),
            (Some(0x17), Some(0x60 | 0x71))
                | (Some(0x19), Some(0x11 | 0x21 | 0x50 | 0x61 | 0xa0))
                | (Some(0x1a), Some(0x02 | 0x44))
        ),
        Some("GenuineIntel") => is_intel_model(
            identity,
            &[
                0x97, 0x9a, 0xb7, 0xba, 0xbf, 0xc6, 0x55, 0x6a, 0x6c, 0x8f, 0xcf, 0xad, 0xae,
            ],
        ),
        _ => false,
    };
    let from_four = identity.vendor.as_deref() == Some("AuthenticAMD")
        && matches!(
            (identity.family, identity.model),
            (Some(0x17), Some(0x31)) | (Some(0x19), Some(0x01))
        );
    if any_count {
        1
    } else if from_four {
        4
    } else {
        STARTUP_STREAM_MIN_WORKERS
    }
}

fn startup_kernel_for(
    identity: &crate::runtime_identity::CpuIdentity,
    candidates: &[CpuKernel],
) -> Option<CpuKernel> {
    cohort_stream_kernel(identity, candidates)
}

fn is_intel_model(identity: &crate::runtime_identity::CpuIdentity, models: &[u32]) -> bool {
    identity.vendor.as_deref() == Some("GenuineIntel")
        && identity.family == Some(6)
        && identity.model.is_some_and(|m| models.contains(&m))
}

/// Broadwell-EP Xeons (06h/4Fh) start at every allowed logical CPU that fits in RAM.
pub(super) fn intel_server_full_count_start() -> bool {
    is_intel_model(&crate::runtime_identity::cpu_identity(), &[0x4f])
}

/// Share of the threads that fit to start on, for these CPUs. The search still checks.
const START_TABLE: &[(&str, u32, u32, u32, u32)] = &[
    // vendor, family, model, share of the room (numerator, denominator)
    ("AuthenticAMD", 0x19, 0x61, 7, 8),
    ("AuthenticAMD", 0x1a, 0x44, 3, 4),
];

fn start_share(identity: &crate::runtime_identity::CpuIdentity) -> Option<(u32, u32)> {
    START_TABLE
        .iter()
        .find(|(vendor, family, model, _, _)| {
            identity.vendor.as_deref() == Some(*vendor)
                && identity.family == Some(*family)
                && identity.model == Some(*model)
        })
        .map(|&(_, _, _, numerator, denominator)| (numerator, denominator))
}

/// Starting count from the table, for `room` threads that fit.
pub(super) fn field_start(room: usize) -> Option<usize> {
    field_start_for(&crate::runtime_identity::cpu_identity(), room)
}

fn field_start_for(identity: &crate::runtime_identity::CpuIdentity, room: usize) -> Option<usize> {
    let (numerator, denominator) = start_share(identity)?;
    Some(
        (room * numerator as usize)
            .div_ceil(denominator as usize)
            .max(1),
    )
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
pub(crate) fn fixed_count_trials_wanted(workers: usize) -> bool {
    fixed_count_trials_wanted_for(
        &crate::runtime_identity::cpu_identity(),
        &supported_kernels(),
        workers,
    )
}

/// Also true where the cohort default is the skipped-store path, which is always
/// compared with the plain path, including at an explicit `--threads` count, and
/// at a count too low to start on the cohort default.
fn fixed_count_trials_wanted_for(
    identity: &crate::runtime_identity::CpuIdentity,
    supported: &[CpuKernel],
    workers: usize,
) -> bool {
    match cohort_stream_kernel(identity, supported) {
        Some(kernel) if kernel.skips_stores() => true,
        Some(_) => workers < startup_stream_min_workers(identity),
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
/// and Zen 4 desktops, Zen 2 EPYC and Threadripper 3000 at 12 or more; off
/// elsewhere. Ryzen 5000 desktops use the skipped-store kernel instead.
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
    (workers >= startup_stream_min_workers(identity))
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
    context.members == 1
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
    context.members == 1
        && context.workers == [32]
        && context.backing == [[0, 0, 0, 32 * CPU_LANE_MEMORY_BYTES, 0]]
}

struct Context {
    members: usize,
    workers: Vec<usize>,
    backing: Vec<[u64; 5]>,
}

impl Context {
    fn of(snapshots: &[CpuAdaptiveSnapshot]) -> Self {
        let running: Vec<_> = snapshots.iter().filter(|s| s.running).collect();
        Self {
            members: running.len(),
            workers: running.iter().map(|s| s.threads).collect(),
            backing: running.iter().map(|s| s.backing_bytes).collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartGate {
    /// At or above this many workers.
    Workers(usize),
    /// Exactly 32 workers on transparent huge pages.
    FullThp32,
    /// Exactly 32 workers on ordinary pages.
    Regular32,
}

/// The kernels this CPU may try and the one it moves to without a comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KernelPlan {
    pub candidates: Vec<CpuKernel>,
    startup: Option<CpuKernel>,
    gate: StartGate,
}

impl KernelPlan {
    pub(super) fn new(cfg: &Config, host: &CpuAutotuneHostFingerprint) -> Self {
        Self::for_identity(cfg, host, &crate::runtime_identity::cpu_identity())
    }

    fn for_identity(
        cfg: &Config,
        host: &CpuAutotuneHostFingerprint,
        identity: &crate::runtime_identity::CpuIdentity,
    ) -> Self {
        let supported = supported_kernels();
        Self::for_supported(cfg, host, identity, &supported)
    }

    fn for_supported(
        cfg: &Config,
        host: &CpuAutotuneHostFingerprint,
        identity: &crate::runtime_identity::CpuIdentity,
        supported: &[CpuKernel],
    ) -> Self {
        let mut candidates = candidates_for(identity, supported);
        // This exact Zen 4 context starts on the 256-bit kernel.
        if super::adaptive_cpu::full_count_cold_start(cfg, host, identity, 32, 32) {
            for kernel in &mut candidates {
                if matches!(
                    *kernel,
                    CpuKernel::Avx512256StreamL2 | CpuKernel::Avx512StreamL2
                ) {
                    *kernel = CpuKernel::Avx512256Stream;
                }
            }
        }
        let sapphire = sapphire_stream_startup(cfg, host, identity, supported);
        if sapphire && !candidates.contains(&CpuKernel::Avx512256Stream) {
            candidates.push(CpuKernel::Avx512256Stream);
        }
        let turin = turin_stream_startup(cfg, host, identity, supported);
        let turin_demand = turin_demand_startup(cfg, host, identity, supported);
        if turin_demand {
            candidates.insert(0, CpuKernel::Avx512256StreamR64D3);
        }
        let turin_prefetch = turin_prefetch_startup(cfg, host, identity, supported);
        if turin_prefetch {
            candidates.insert(0, CpuKernel::Avx512256StreamR64D4);
        }
        let turin_depth6 = turin_depth6_startup(cfg, host, identity, supported);
        if turin_depth6 {
            candidates.insert(0, CpuKernel::Avx512256StreamR64D6);
        }
        let genoa = genoa_stream_startup(cfg, host, identity, &candidates);
        let windows_genoa = windows_genoa_stream_startup(cfg, host, identity, &candidates);
        let ring = (genoa || windows_genoa) && supported.contains(&CpuKernel::Avx512256StreamR8D2);
        if ring {
            candidates.insert(0, CpuKernel::Avx512256StreamR8D2);
        }
        let startup = if ring {
            Some(CpuKernel::Avx512256StreamR8D2)
        } else if turin_depth6 {
            Some(CpuKernel::Avx512256StreamR64D6)
        } else if turin_prefetch {
            Some(CpuKernel::Avx512256StreamR64D4)
        } else if turin_demand {
            Some(CpuKernel::Avx512256StreamR64D3)
        } else if genoa || windows_genoa || sapphire || turin {
            Some(CpuKernel::Avx512256Stream)
        } else {
            startup_kernel_for(identity, &candidates)
        };
        let gate = if windows_genoa {
            StartGate::Regular32
        } else if genoa || sapphire || turin {
            StartGate::FullThp32
        } else {
            StartGate::Workers(startup_stream_min_workers(identity))
        };
        Self {
            candidates,
            startup,
            gate,
        }
    }

    /// The kernel to move to before any comparison, once arenas show their backing.
    pub(super) fn start_for(&self, snapshots: &[CpuAdaptiveSnapshot]) -> Option<CpuKernel> {
        let kernel = self.startup.filter(|k| self.candidates.contains(k))?;
        let context = Context::of(snapshots);
        let fits = match self.gate {
            StartGate::Workers(minimum) => {
                !context.workers.is_empty() && context.workers.iter().all(|&w| w >= minimum)
            }
            StartGate::FullThp32 => genoa_stream_context(&context),
            StartGate::Regular32 => windows_genoa_stream_context(&context),
        };
        fits.then_some(kernel)
    }

    /// The kernels worth comparing at this count: from the count where a cohort starts
    /// on its own kernel, that excludes the plain paths.
    pub(super) fn candidates_at(&self, workers: usize) -> Vec<CpuKernel> {
        let skip_plain = self.startup.is_some()
            && matches!(self.gate, StartGate::Workers(minimum) if workers >= minimum);
        self.candidates
            .iter()
            .copied()
            .filter(|k| !skip_plain || !matches!(k, CpuKernel::Avx512 | CpuKernel::Avx2))
            .collect()
    }

    pub(super) fn revisions(&self) -> std::collections::BTreeMap<String, u32> {
        self.candidates
            .iter()
            .map(|k| (k.to_string(), k.revision()))
            .collect()
    }
}

/// The kernel a machine last chose under the previous controller, as a starting
/// point for its first run after upgrading.
pub(super) fn legacy_winner(cfg: &Config, workers: usize) -> Option<CpuKernel> {
    let mut name = cfg.cpu_autotune_config_path.as_os_str().to_owned();
    name.push(".kernels.json");
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(std::path::PathBuf::from(name)).ok()?).ok()?;
    value
        .get("winners")?
        .as_array()?
        .iter()
        .rev()
        .find(|winner| {
            winner
                .pointer("/context/workers")
                .and_then(|w| w.as_array())
                .is_some_and(|w| w.iter().all(|n| n.as_u64() == Some(workers as u64)))
        })
        .or_else(|| value.get("winners")?.as_array()?.last())?
        .get("kernel")?
        .as_str()?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // Alder Lake, Raptor Lake and Arrow Lake desktops (AVX2 only) start on AVX2
        // streaming; other Intel client models keep their start.
        let avx2 = [CpuKernel::Avx2, CpuKernel::Avx2Stream];
        for (model, expected) in [
            (0x97, Some(CpuKernel::Avx2Stream)),
            (0xbf, Some(CpuKernel::Avx2Stream)),
            (0xc6, Some(CpuKernel::Avx2Stream)),
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
        // Skylake-SP / Cascade Lake, Ice Lake, Sapphire, Emerald and Granite Rapids
        // start on streaming (full width when available); other Intel models do not.
        for (model, stream) in [
            (0x55, true),
            (0x8f, true),
            (0xcf, true),
            (0x6a, true),
            (0x6c, true),
            (0xad, true),
            (0xae, true),
            (0x9e, false),
        ] {
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
        let skx = CpuIdentity {
            vendor: Some("GenuineIntel".into()),
            family: Some(6),
            model: Some(0x55),
            ..CpuIdentity::default()
        };
        let bdw = CpuIdentity {
            model: Some(0x4f),
            ..skx.clone()
        };
        assert_eq!(startup_kernel_for(&bdw, &supported[1..]), None);
        assert!(is_intel_model(&bdw, &[0x4f, 0x55]));
        assert!(!is_intel_model(
            &CpuIdentity {
                model: Some(0x6a),
                ..skx.clone()
            },
            &[0x4f, 0x55]
        ));
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
        }
        assert_eq!(static_kernel_for_identity(&masked_zen5, &all, 7), None);
        assert_eq!(
            static_kernel_for_identity(&raphael, &all, 1),
            Some(CpuKernel::Avx512256Stream)
        );
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

    #[test]
    fn listed_cpus_start_on_their_cohort_kernel_at_any_count() {
        use crate::runtime_identity::CpuIdentity;
        let amd = |family, model| CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(family),
            model: Some(model),
            ..CpuIdentity::default()
        };
        let intel = |model| CpuIdentity {
            vendor: Some("GenuineIntel".into()),
            family: Some(6),
            model: Some(model),
            ..CpuIdentity::default()
        };
        let avx2 = [CpuKernel::Avx2, CpuKernel::Avx2Stream];
        let avx512 = [
            CpuKernel::Avx512,
            CpuKernel::Avx2,
            CpuKernel::Avx512Stream,
            CpuKernel::Avx512StreamL2,
            CpuKernel::Avx512256Stream,
        ];
        for (identity, supported, expected) in [
            (amd(0x17, 0x71), &avx2[..], CpuKernel::Avx2Stream),
            (amd(0x17, 0x60), &avx2[..], CpuKernel::Avx2Stream),
            (amd(0x19, 0x21), &avx2[..], CpuKernel::Avx2Stream),
            (amd(0x19, 0x50), &avx2[..], CpuKernel::Avx2Stream),
            (amd(0x19, 0x61), &avx512[..], CpuKernel::Avx512StreamL2),
            (amd(0x19, 0xa0), &avx512[..], CpuKernel::Avx512StreamL2),
            (amd(0x1a, 0x44), &avx512[..], CpuKernel::Avx512256Stream),
            (intel(0x97), &avx2[..], CpuKernel::Avx2Stream),
            (intel(0xc6), &avx2[..], CpuKernel::Avx2Stream),
            (intel(0xcf), &avx512[..], CpuKernel::Avx512Stream),
            (intel(0x55), &avx512[..], CpuKernel::Avx512Stream),
            (intel(0x6a), &avx512[..], CpuKernel::Avx512Stream),
            (intel(0xad), &avx512[..], CpuKernel::Avx512Stream),
            (amd(0x19, 0x11), &avx512[..], CpuKernel::Avx512StreamL2),
            (amd(0x1a, 0x02), &avx512[..], CpuKernel::Avx512Stream),
        ] {
            assert_eq!(
                static_kernel_for_identity(&identity, supported, 2),
                Some(expected),
                "{identity:?}"
            );
            assert!(!fixed_count_trials_wanted_for(&identity, supported, 2));
        }
        // These start on it from four workers; below that a pinned count compares.
        for identity in [amd(0x19, 0x01), amd(0x17, 0x31)] {
            assert_eq!(static_kernel_for_identity(&identity, &avx2, 3), None);
            assert_eq!(
                static_kernel_for_identity(&identity, &avx2, 4),
                Some(CpuKernel::Avx2Stream)
            );
            assert!(fixed_count_trials_wanted_for(&identity, &avx2, 3));
            assert!(!fixed_count_trials_wanted_for(&identity, &avx2, 4));
        }
        // Neighbouring cohorts keep the gate, and a low pinned count compares
        // kernels instead of staying on the plain path.
        let turin_dense = amd(0x1a, 0x11);
        assert_eq!(static_kernel_for_identity(&turin_dense, &avx512, 7), None);
        assert!(static_kernel_for_identity(&turin_dense, &avx512, 8).is_some());
        assert!(fixed_count_trials_wanted_for(&turin_dense, &avx512, 7));
        assert!(!fixed_count_trials_wanted_for(&turin_dense, &avx512, 8));
        // An unsupported kernel is never selected, whatever the identity.
        assert_eq!(
            static_kernel_for_identity(&amd(0x19, 0x21), &[CpuKernel::Avx2], 2),
            None
        );
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

    fn plan_for(
        identity: &crate::runtime_identity::CpuIdentity,
        supported: &[CpuKernel],
    ) -> KernelPlan {
        KernelPlan::for_supported(
            &super::super::tests::test_config(),
            &CpuAutotuneHostFingerprint::default(),
            identity,
            supported,
        )
    }

    fn snapshot_with(threads: usize) -> CpuAdaptiveSnapshot {
        let bytes = threads as u64 * CPU_LANE_MEMORY_BYTES;
        CpuAdaptiveSnapshot {
            kernel: CpuKernel::Avx2,
            kernel_generation: 1,
            kernel_ready: true,
            backing_bytes: [0, 0, 0, bytes, 0],
            running: true,
            threads,
            committed_bytes: bytes,
            resident_bytes: bytes,
            hashes: 0,
            hashing: true,
            allocation_failures: 0,
            warmed: threads,
        }
    }

    #[test]
    fn plan_starts_validated_cpus_on_their_kernel_and_skips_known_losers() {
        use crate::runtime_identity::CpuIdentity;
        let zen3_desktop = CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x21),
            ..CpuIdentity::default()
        };
        let supported = [CpuKernel::Avx2, CpuKernel::Avx2Stream];
        let plan = plan_for(&zen3_desktop, &supported);
        assert_eq!(plan.candidates_at(2), [CpuKernel::Avx2Stream]);
        assert_eq!(
            plan.start_for(&[snapshot_with(2)]),
            Some(CpuKernel::Avx2Stream)
        );
    }

    #[test]
    fn plan_keeps_plain_kernels_where_low_counts_are_unmeasured() {
        use crate::runtime_identity::CpuIdentity;
        let milan = CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x01),
            ..CpuIdentity::default()
        };
        let supported = [CpuKernel::Avx2, CpuKernel::Avx2Stream];
        let plan = plan_for(&milan, &supported);
        assert!(plan.candidates_at(3).contains(&CpuKernel::Avx2));
        assert!(!plan.candidates_at(4).contains(&CpuKernel::Avx2));
        assert_eq!(plan.start_for(&[snapshot_with(3)]), None);
        assert_eq!(
            plan.start_for(&[snapshot_with(4)]),
            Some(CpuKernel::Avx2Stream)
        );
    }

    #[test]
    fn start_table_sets_a_share_of_the_room_for_listed_cpus_only() {
        use crate::runtime_identity::CpuIdentity;
        let amd = |family, model| CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(family),
            model: Some(model),
            ..CpuIdentity::default()
        };
        assert_eq!(field_start_for(&amd(0x1a, 0x44), 16), Some(12));
        assert_eq!(field_start_for(&amd(0x19, 0x61), 16), Some(14));
        assert_eq!(field_start_for(&amd(0x19, 0x61), 1), Some(1));
        assert_eq!(field_start_for(&amd(0x19, 0x11), 16), None);
    }

    #[test]
    fn ryzen_5000_desktops_compare_skipped_stores_on_each_machine() {
        use crate::runtime_identity::CpuIdentity;
        let zen3_desktop = CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x21),
            ..CpuIdentity::default()
        };
        let supported = [
            CpuKernel::Avx2,
            CpuKernel::Avx2Stream,
            CpuKernel::Avx2StreamSkip,
        ];
        assert_eq!(
            cohort_stream_kernel(&zen3_desktop, &supported),
            Some(CpuKernel::Avx2StreamSkip)
        );
        let plan = plan_for(&zen3_desktop, &supported);
        for workers in [6, 16] {
            let candidates = plan.candidates_at(workers);
            assert!(
                candidates.contains(&CpuKernel::Avx2StreamSkip),
                "{candidates:?}"
            );
            assert!(
                candidates.contains(&CpuKernel::Avx2Stream),
                "{candidates:?}"
            );
            // A set --threads count still compares the two.
            assert!(fixed_count_trials_wanted_for(
                &zen3_desktop,
                &supported,
                workers
            ));
        }
        assert_eq!(
            plan.start_for(&[snapshot_with(6)]),
            Some(CpuKernel::Avx2StreamSkip)
        );
        // Zen 3 EPYC keeps the plain path.
        let milan = CpuIdentity {
            model: Some(0x01),
            ..zen3_desktop
        };
        assert_eq!(
            cohort_stream_kernel(&milan, &supported),
            Some(CpuKernel::Avx2Stream)
        );
    }

    #[test]
    fn plan_never_offers_an_unsupported_kernel() {
        use crate::runtime_identity::CpuIdentity;
        let zen3_desktop = CpuIdentity {
            vendor: Some("AuthenticAMD".into()),
            family: Some(0x19),
            model: Some(0x21),
            ..CpuIdentity::default()
        };
        let plan = plan_for(&zen3_desktop, &[CpuKernel::Avx2]);
        assert_eq!(plan.candidates_at(8), [CpuKernel::Avx2]);
        assert_eq!(plan.start_for(&[snapshot_with(8)]), None);
    }

    #[test]
    fn legacy_winner_reads_the_previous_cache_format() {
        let mut cfg = super::super::tests::test_config();
        let dir = std::env::temp_dir().join(format!("seine-legacy-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        cfg.cpu_autotune_config_path = dir.join("tune.json");
        fs::write(
            dir.join("tune.json.kernels.json"),
            r#"{"winners":[{"context":{"workers":[8]},"kernel":"avx2"},
                           {"context":{"workers":[16]},"kernel":"avx2-stream"}]}"#,
        )
        .unwrap();
        assert_eq!(legacy_winner(&cfg, 8), Some(CpuKernel::Avx2));
        assert_eq!(legacy_winner(&cfg, 16), Some(CpuKernel::Avx2Stream));
        assert_eq!(legacy_winner(&cfg, 4), Some(CpuKernel::Avx2Stream));
        let _ = fs::remove_dir_all(dir);
    }
}
