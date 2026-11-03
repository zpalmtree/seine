//! Productive CPU tuning: startup uses a seed and the search runs on real work.
//! RAM limits constrain the operating count without replacing a remembered peak.
use super::adaptive_search::{
    BuildFacts, Comparing, Params, Saved, Search, SeedQuality, Setting, SettledSummary, Status,
    TrialEvent, Verdict,
};
use super::*;
use crate::config::{cpu_autotune_capacity, cpu_autotune_worker_ceiling};
use blocknet_pow_kernel::CpuKernel;
use std::thread::{self, JoinHandle};

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

/// Why a saved result does not apply to this machine. A new build, kernel version or
/// RAM size does not invalidate it: the search re-checks what actually changed.
fn seed_miss_reason(
    record: &CpuAutotuneRecord,
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
) -> Option<&'static str> {
    let saved = &record.host_fingerprint;
    if !(10..=CPU_AUTOTUNE_RECORD_SCHEMA_VERSION).contains(&record.schema_version) {
        Some("cache format changed")
    } else if saved.cpu_brand != host.cpu_brand
        || saved.cpu_arch != host.cpu_arch
        || saved.logical_cores != host.logical_cores
        || saved.physical_cores != host.physical_cores
    {
        Some("CPU changed")
    } else if saved.os != host.os || saved.runtime_environment != host.runtime_environment {
        Some("operating system or runtime changed")
    } else if saved.affinity_topology != host.affinity_topology {
        Some("CPU affinity or topology changed")
    } else if record.affinity != cpu_affinity_label(cfg.cpu_affinity)
        || record.page_mode != cfg.cpu_page_mode.as_str()
        || record.profile != cpu_profile_label(cfg.cpu_profile)
        || record.cpu_instances != instances(cfg)
    {
        Some("tuning settings changed")
    } else if record.selected_threads == 0
        || !record.measured_hps.is_finite()
        || record.measured_hps < 0.0
    {
        Some("saved tuning result is invalid")
    } else {
        None
    }
}

/// The kernel the backend starts on: this machine's earlier result, else the one
/// validated for this CPU at this count.
pub(super) fn initial_kernel(cfg: &Config, threads: usize) -> Option<CpuKernel> {
    if let Ok(record) = read_seed(cfg) {
        if let Some(kernel) = record
            .search
            .as_ref()
            .and_then(|saved| saved.kernel.parse::<CpuKernel>().ok())
            .or_else(|| super::adaptive_kernel::legacy_winner(cfg, record.selected_threads))
            .filter(|kernel| kernel.is_supported())
        {
            return Some(kernel);
        }
    }
    super::adaptive_kernel::static_kernel_for(threads)
}

/// A start rule covers this CPU, so the first search can look nearby.
fn family_start(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    max: usize,
    capacity: usize,
) -> bool {
    let identity = crate::runtime_identity::cpu_identity();
    full_count_cold_start(cfg, host, &identity, max, capacity)
        || sapphire_full_count_cold_start(cfg, host, &identity, max, capacity)
        || turin_full_count_cold_start(cfg, host, &identity, max, capacity)
        || cohort_cold_start(cfg, host, max, capacity).is_some()
}

#[cfg(test)]
fn compatible(record: &CpuAutotuneRecord, cfg: &Config, host: &CpuAutotuneHostFingerprint) -> bool {
    seed_miss_reason(record, cfg, host).is_none()
}

fn read_seed(cfg: &Config) -> Result<CpuAutotuneRecord, &'static str> {
    let record = read_cpu_autotune_cache(&cfg.cpu_autotune_config_path)?;
    if let Some(reason) = seed_miss_reason(&record, cfg, &cpu_autotune_host_fingerprint()) {
        return Err(reason);
    }
    Ok(record)
}

fn seed_threads(
    record: Option<&CpuAutotuneRecord>,
    min: usize,
    max: usize,
    available: usize,
    memory_capacity: usize,
    physical: Option<usize>,
) -> usize {
    if available == 0 {
        return 0;
    }
    let floor = min.min(available);
    record
        .map(|r| r.selected_threads)
        .unwrap_or_else(|| {
            // Small machines run every thread; larger ones start on their cores.
            let room = max.min(memory_capacity);
            if room <= 4 {
                room.max(1)
            } else {
                physical.unwrap_or(room / 2).clamp(1, room)
            }
        })
        .clamp(floor, max.min(available))
}

// Cold-start policy for one exact Linux/Windows 32-core/128 GiB Zen 4 host
// context. Keep off other identities, topologies and profiles.
// This changes only the initial count; normal resource shedding and repeated
// count/kernel comparisons still decide the sustained setting.
pub(super) fn full_count_cold_start(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    max: usize,
    memory_capacity: usize,
) -> bool {
    identity.vendor.as_deref() == Some("AuthenticAMD")
        && identity.family == Some(0x19)
        && identity.model == Some(0x11)
        && host.cpu_brand.as_deref() == Some("AMD EPYC 9R14")
        && matches!(host.os.as_str(), "linux" | "windows")
        && host.runtime_environment == "native"
        && host.logical_cores == 32
        && host.physical_cores == Some(32)
        && (120 * 1024_u64.pow(3)..=130 * 1024_u64.pow(3)).contains(&host.total_memory_bytes)
        && cfg.backend_specs.len() == 1
        && instances(cfg) == 1
        && cfg.backend_specs[0]
            .cpu_affinity
            .is_none_or(|affinity| affinity == crate::config::CpuAffinityMode::Auto)
        && cfg.cpu_profile != CpuPerformanceProfile::Efficiency
        && cfg.cpu_page_mode == crate::config::CpuPageMode::Auto
        && cfg.cpu_affinity == crate::config::CpuAffinityMode::Auto
        && max == 32
        && memory_capacity >= 32
}

// Cold-start policy for this exact Sapphire Rapids context.
// Keep neighboring hosts, manual settings and efficiency mode unchanged.
pub(super) fn sapphire_full_count_cold_start(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    max: usize,
    memory_capacity: usize,
) -> bool {
    identity.vendor.as_deref() == Some("GenuineIntel")
        && identity.family == Some(6)
        && identity.model == Some(0x8f)
        && host.cpu_brand.as_deref() == Some("Intel(R) Xeon(R) Platinum 8488C")
        && host.os == "linux"
        && host.runtime_environment == "native"
        && host.logical_cores == 32
        && host.physical_cores == Some(16)
        && (120 * 1024_u64.pow(3)..=130 * 1024_u64.pow(3)).contains(&host.total_memory_bytes)
        && cfg.backend_specs.len() == 1
        && instances(cfg) == 1
        && cfg.backend_specs[0]
            .cpu_affinity
            .is_none_or(|affinity| affinity == crate::config::CpuAffinityMode::Auto)
        && cfg.cpu_profile != CpuPerformanceProfile::Efficiency
        && cfg.cpu_page_mode == crate::config::CpuPageMode::Auto
        && cfg.cpu_affinity == crate::config::CpuAffinityMode::Auto
        && max == 32
        && memory_capacity >= 32
}

// Cold-start policy for this exact Turin context: streaming startup and THP
// prefault order.
// Keep neighboring hosts, manual settings and efficiency mode unchanged.
pub(super) fn turin_full_count_cold_start(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    identity: &crate::runtime_identity::CpuIdentity,
    max: usize,
    memory_capacity: usize,
) -> bool {
    identity.vendor.as_deref() == Some("AuthenticAMD")
        && identity.family == Some(0x1a)
        && identity.model == Some(0x02)
        && host.cpu_brand.as_deref() == Some("AMD EPYC 9R45")
        && host.os == "linux"
        && host.runtime_environment == "native"
        && host.logical_cores == 32
        && host.physical_cores == Some(32)
        && (120 * 1024_u64.pow(3)..=130 * 1024_u64.pow(3)).contains(&host.total_memory_bytes)
        && cfg.backend_specs.len() == 1
        && instances(cfg) == 1
        && cfg.backend_specs[0]
            .cpu_affinity
            .is_none_or(|affinity| affinity == crate::config::CpuAffinityMode::Auto)
        && cfg.cpu_profile != CpuPerformanceProfile::Efficiency
        && cfg.cpu_page_mode == crate::config::CpuPageMode::Auto
        && cfg.cpu_affinity == crate::config::CpuAffinityMode::Auto
        && max == 32
        && memory_capacity >= 32
}

fn initial_threads(
    record: Option<&CpuAutotuneRecord>,
    min: usize,
    max: usize,
    available: usize,
    memory_capacity: usize,
    full_count: bool,
    cohort_start: Option<usize>,
    physical: Option<usize>,
) -> usize {
    match (record, full_count, cohort_start) {
        (None, true, _) => max.min(memory_capacity).min(available),
        (None, false, Some(start)) if available > 0 => {
            start.clamp(min.min(available), max.min(memory_capacity).min(available))
        }
        _ => seed_threads(record, min, max, available, memory_capacity, physical),
    }
}

// Cold start for CPUs whose cohort mines on a streaming kernel. Zen 4/5, Intel
// Xeons with an AVX-512 streaming default and Intel AVX2 desktops start at every
// allowed logical CPU that fits in RAM, as do Broadwell-EP Xeons; Zen 2/3 (AVX2
// streaming) start at the physical core count.
// Count trials still decide afterwards.
fn cohort_cold_start(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    max: usize,
    memory_capacity: usize,
) -> Option<usize> {
    if cfg!(test) || cfg.cpu_profile == CpuPerformanceProfile::Efficiency {
        // Unit tests stay host-independent; `initial_threads` covers the policy.
        return None;
    }
    if let Some(start) = super::adaptive_kernel::field_start(max.min(memory_capacity)) {
        return Some(start);
    }
    if super::adaptive_kernel::intel_server_full_count_start() {
        return Some(max).filter(|&workers| workers > 0);
    }
    match super::adaptive_kernel::cohort_kernel()? {
        CpuKernel::Avx2Stream
            if crate::runtime_identity::cpu_identity().vendor.as_deref()
                == Some("GenuineIntel") =>
        {
            Some(max)
        }
        CpuKernel::Avx2Stream => host.physical_cores,
        _ => Some(max),
    }
    .filter(|&workers| workers > 0)
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
    let seed = match read_seed(cfg) {
        Ok(record) => Some(record),
        Err(reason) => {
            super::ui::detail(
                "MINER",
                format!("cpu-autotune | {reason}; tuning while mining"),
            );
            None
        }
    };
    let full_count = seed.is_none()
        && (full_count_cold_start(
            cfg,
            &cpu_autotune_host_fingerprint(),
            &crate::runtime_identity::cpu_identity(),
            max,
            memory_capacity,
        ) || sapphire_full_count_cold_start(
            cfg,
            &cpu_autotune_host_fingerprint(),
            &crate::runtime_identity::cpu_identity(),
            max,
            memory_capacity,
        ) || turin_full_count_cold_start(
            cfg,
            &cpu_autotune_host_fingerprint(),
            &crate::runtime_identity::cpu_identity(),
            max,
            memory_capacity,
        ));
    let cohort_start = seed
        .is_none()
        .then(|| cohort_cold_start(cfg, &cpu_autotune_host_fingerprint(), max, memory_capacity))
        .flatten();
    let threads = initial_threads(
        seed.as_ref(),
        min,
        max,
        available,
        memory_capacity,
        full_count,
        cohort_start,
        // Unit tests stay host-independent, like `cohort_cold_start`.
        cpu_autotune_host_fingerprint()
            .physical_cores
            .filter(|_| !cfg!(test)),
    );
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
    } else if cohort_start.is_some() && !full_count {
        "validated starting count for this CPU family"
    } else {
        "initial estimate"
    };
    super::ui::detail("MINER", format!("cpu-autotune | starting with {threads} threads ({reason}); tuning while mining, capacity={max}, worker slots={slots}, currently available={available}"));
    if available == 0 {
        super::ui::note("CPU", "paused until enough RAM is free for one thread");
    }
    Ok(())
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

/// Seconds of sustained memory stalls (one tick per second) before shedding a worker.
/// Faulting in new arenas and reclaiming page cache stalls for a few seconds even
/// when the new count fits.
const PRESSURE_TICKS: u8 = 10;

impl PressureBackoff {
    /// `available` is the RAM-based arena capacity and `slots` the worker limit.
    /// Headroom counts RAM only: a worker-slot limit is not memory pressure.
    fn cap(
        &mut self,
        pressure: bool,
        available: usize,
        slots: usize,
        current: usize,
        max: usize,
    ) -> usize {
        self.cap_on(
            cfg!(target_os = "macos"),
            pressure,
            available,
            slots,
            current,
            max,
        )
    }

    /// macOS moves memory in and out of its compressor and counts a changing share of
    /// it as in use, so free-memory arithmetic can fall below the running count with no
    /// pressure reported. With `level_decides`, the system's pressure level alone
    /// decides whether running workers go; the arithmetic still limits growth.
    fn cap_on(
        &mut self,
        level_decides: bool,
        pressure: bool,
        available: usize,
        slots: usize,
        current: usize,
        max: usize,
    ) -> usize {
        // First-touch/reclaim can briefly stall while ample clean cache remains
        // available. Require both sustained stalls and tight arena headroom.
        self.consecutive = if pressure && (level_decides || available <= current) {
            self.consecutive.saturating_add(1)
        } else {
            0
        };
        let room = if level_decides && !pressure {
            available.max(current)
        } else {
            available
        };
        let cap = room.min(slots).min(max);
        if self.consecutive >= PRESSURE_TICKS {
            cap.min(current.saturating_sub(1))
        } else {
            cap
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
        #[cfg(target_os = "macos")]
        {
            // 1 is normal, 2 warn, 4 critical.
            let mut level: libc::c_int = 0;
            let mut size = std::mem::size_of::<libc::c_int>();
            let ok = unsafe {
                libc::sysctlbyname(
                    c"kern.memorystatus_vm_pressure_level".as_ptr(),
                    (&mut level as *mut libc::c_int).cast(),
                    &mut size,
                    std::ptr::null_mut(),
                    0,
                )
            };
            ok == 0 && level >= 2
        }
        #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
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
        telemetry: Option<crate::hardware_telemetry::Recorder>,
        live_changes: Receiver<super::thread_control::LiveThreadChange>,
        history: Arc<super::session::SessionHistory>,
    ) -> Result<Self> {
        let (stop, rx) = bounded(1);
        let handle = if backends.is_empty() {
            None
        } else {
            Some(
                thread::Builder::new()
                    .name("cpu-autotune".into())
                    .spawn(move || {
                        let mut live_changes = live_changes;
                        // Arenas already exist when the controller starts. Use
                        // the same owned-memory accounting as subsequent ticks,
                        // so a large cached count does not look like newly
                        // recovered capacity and postpone kernel selection.
                        let owned = backends.iter().map(|b| b.adaptive_snapshot())
                            .filter(|s| s.running).map(|s| s.committed_bytes).sum();
                        let (min, max, available) = limits(&cfg, owned);
                        let current = backends.iter().filter(|b| b.adaptive_snapshot().running)
                            .map(|b| b.enabled_lanes()).min().unwrap_or(0);
                        let host = cpu_autotune_host_fingerprint();
                        let plan = super::adaptive_kernel::KernelPlan::new(&cfg, &host);
                        let record = read_seed(&cfg).ok();
                        let running_kernel = backends.iter().map(|b| b.adaptive_snapshot())
                            .find(|s| s.running).map_or(CpuKernel::preferred(), |s| s.kernel);
                        let saved = record.as_ref().and_then(|r| r.search.clone());
                        // This machine's own earlier choice outranks the family's start.
                        let mut has_start = record.is_some();
                        let quality = if record.is_some() {
                            SeedQuality::Measured
                        } else if family_start(&cfg, &host, max, available) {
                            SeedQuality::Family
                        } else {
                            SeedQuality::Estimate
                        };
                        let mut search = Search::new(Params {
                            seed: Setting { threads: current, kernel: running_kernel },
                            quality,
                            min,
                            max,
                            down_band: 1.0 - cpu_autotune_peak_floor_frac(cfg.cpu_profile),
                            candidates: {
                                let mut candidates = plan.candidates_at(current);
                                if !candidates.contains(&running_kernel) {
                                    candidates.insert(0, running_kernel);
                                }
                                candidates
                            },
                            saved,
                            build: BuildFacts {
                                hot_fingerprint: crate::runtime_identity::hash_path_fingerprint(),
                                kernel_revisions: plan.revisions(),
                            },
                        });
                        let settled_recorder = telemetry.clone();
                        let mut monitor = telemetry
                            .filter(|_| backends.len() <= seine_telemetry::tuning::MAX_MEMBERS)
                            .map(|recorder| super::adaptive_telemetry::Monitor::new(recorder, &cfg));
                        let has_other_backends = cfg.backend_specs.iter().any(|s| s.kind != BackendKind::Cpu);
                        let started = Instant::now();
                        let mut failures = 0;
                        let mut pressure = MemoryPressure::default();
                        let mut pressure_backoff = PressureBackoff::default();
                        loop {
                            let mut manual = None;
                            let stopping = crossbeam_channel::select! {
                                recv(rx) -> _ => true,
                                recv(live_changes) -> change => {
                                    match change {
                                        Ok(change) => manual = Some(change),
                                        Err(_) => live_changes = crossbeam_channel::never(),
                                    }
                                    false
                                },
                                default(Duration::from_secs(1)) => false,
                            };
                            if stopping {
                                if let Some(monitor) = &mut monitor {
                                    let now = started.elapsed().as_secs();
                                    let snapshots: Vec<_> = backends.iter().map(|b| b.adaptive_snapshot()).collect();
                                    monitor.observe(now, &snapshots, crate::config::CpuAutotuneResources { parallelism: 0, total_bytes: None, available_bytes: None });
                                    monitor.abort(now, seine_telemetry::tuning::TrialReason::UserInterrupted);
                                }
                                break;
                            }
                            let all_snapshots: Vec<_> =
                                backends.iter().map(|b| b.adaptive_snapshot()).collect();
                            // A quarantined backend is never restarted by the tuner.
                            let snapshots: Vec<_> =
                                all_snapshots.iter().filter(|s| s.running).collect();
                            if snapshots.is_empty() {
                                if let Some(monitor) = &mut monitor {
                                    let now = started.elapsed().as_secs();
                                    monitor.observe(now, &all_snapshots, crate::config::CpuAutotuneResources { parallelism: 0, total_bytes: None, available_bytes: None });
                                    monitor.abort(now, seine_telemetry::tuning::TrialReason::ExecutionFailure);
                                }
                                break;
                            }
                            let slots = backends.iter().zip(&all_snapshots)
                                .filter(|(_, snapshot)| snapshot.running)
                                .map(|(backend, _)| backend.worker_capacity()).min().unwrap_or(0);
                            if backends.iter().any(|backend| backend.blake2b_seen()) {
                                // BLAKE2b work runs every provisioned worker without
                                // arenas. Argon2 tuning stops for the rest of the run
                                // so thread, kernel and telemetry results stay Argon2-only.
                                if let Some(monitor) = &mut monitor {
                                    let now = started.elapsed().as_secs();
                                    monitor.observe(now, &all_snapshots, crate::config::CpuAutotuneResources { parallelism: 0, total_bytes: None, available_bytes: None });
                                    monitor.abort(now, seine_telemetry::tuning::TrialReason::ContextChanged);
                                }
                                if let Some(tui) = &tui {
                                    if let Ok(mut state) = tui.lock() {
                                        state.threads = slots * snapshots.len();
                                        if let Some(control) = &mut state.thread_control {
                                            control.current_threads = slots;
                                            control.max_threads = slots;
                                        }
                                    }
                                }
                                super::ui::detail("MINER", "cpu-autotune | stopped: BLAKE2b work runs every CPU worker");
                                break;
                            }
                            let actual = snapshots.iter().map(|s| s.threads).min().unwrap_or(0);
                            let owned = snapshots.iter().map(|s| s.committed_bytes).sum();
                            let count_failures =
                                snapshots.iter().map(|s| s.allocation_failures).sum();
                            let ((_, available), resources) = crate::config::cpu_autotune_capacity_snapshot(&cfg, owned);
                            let cap =
                                pressure_backoff.cap(pressure.elevated(), available, slots, actual, max);
                            let now = started.elapsed().as_secs();
                            if let Some(monitor) = &mut monitor { monitor.observe(now, &all_snapshots, resources); }
                            if !has_start
                                && snapshots.iter().all(|s| s.threads > 0 && s.warmed == s.threads)
                            {
                                has_start = true;
                                if let Some(kernel) = plan.start_for(&all_snapshots) {
                                    search.adopt_start(kernel);
                                }
                            }
                            if let Some(change) = &manual {
                                search.set_manual(change.threads.min(cap).max(1));
                            }
                            search.observe_parallelism(now, resources.parallelism);
                            let rig_hashes = has_other_backends.then(|| history.stats.snapshot().hashes);
                            let output = search.tick_rig(now, &all_snapshots, cap, count_failures > failures, rig_hashes);
                            if let Some(kernel) = output.kernel {
                                for (backend, snapshot) in backends.iter().zip(&all_snapshots) {
                                    if snapshot.running {
                                        if let Err(err) = backend.set_active_kernel(kernel) {
                                            if let Some(monitor) = &mut monitor {
                                                let observed: Vec<_> = backends.iter().map(|b| b.adaptive_snapshot()).collect();
                                                monitor.abort_observed(started.elapsed().as_secs(), &observed, resources, seine_telemetry::tuning::TrialReason::ExecutionFailure);
                                            }
                                            warn("MINER", format!("cpu-autotune | kernel switch failed: {err:#}"));
                                        }
                                    }
                                }
                            }
                            if let Some(threads) = output.threads {
                                for (backend, snapshot) in backends.iter().zip(&all_snapshots) {
                                    if snapshot.running {
                                        backend.set_active_threads(threads);
                                    }
                                }
                            }
                            if let Some(monitor) = &mut monitor {
                                report_trial_events(monitor, now, &output.events);
                            }
                            for detail in &output.details {
                                super::ui::detail("MINER", format!("cpu-autotune | {detail}"));
                            }
                            for note in &output.notes {
                                if note.success {
                                    super::ui::note_success("CPU", &note.text);
                                } else {
                                    super::ui::note("CPU", &note.text);
                                }
                            }
                            if output.settled_now {
                                if let Some(recorder) = &settled_recorder {
                                    recorder.record_settled(settled_report(&search.settled_summary(), now));
                                }
                            }
                            if output.save {
                                let saved = search.saved();
                                if let Err(err) = save_result(&cfg, &host, max, saved) {
                                    warn("MINER", format!("cpu-autotune | could not save progress: {err:#}"));
                                }
                            }
                            failures = count_failures;
                            if let Some(tui) = &tui {
                                if let Ok(mut state) = tui.lock() {
                                    let threads = search.target_threads();
                                    state.threads = threads * snapshots.len();
                                    let status = search.status(cap);
                                    state.tuning_status = status_text(&status);
                                    state.threads_locked = search
                                        .picks_count()
                                        .then(|| threads_locked_notice(&status))
                                        .flatten();
                                    if let Some(control) = &mut state.thread_control {
                                        control.current_threads = threads;
                                        control.max_threads = cap.max(threads);
                                    }
                                }
                            }
                            if let Some(reply) = manual.and_then(|change| change.reply) {
                                let _ = reply.try_send(format!("CPU threads: {}", search.target_threads()));
                            }
                        }
                        // Nothing tunes after this point, so the keys must not stay locked.
                        if let Some(tui) = &tui {
                            if let Ok(mut state) = tui.lock() {
                                state.tuning_status = None;
                                state.threads_locked = None;
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

/// What the dashboard says about tuning, next to the thread count.
pub(super) fn status_text(status: &Status) -> Option<String> {
    let minutes = |secs: u64| secs.div_ceil(60).max(1);
    Some(match status {
        // Mining on the saved result; a different rate starts tuning, with a log line.
        Status::Checking => "done".to_string(),
        Status::Tuning {
            secs_left,
            testing_update,
            comparing,
            ..
        } => {
            let time = format!("about {} min left", minutes(*secs_left));
            match (comparing, testing_update) {
                (Comparing::Threads(a, b), false) => format!("{a} vs {b} threads, {time}"),
                (Comparing::Threads(a, b), true) => {
                    format!("update: {a} vs {b} threads, ~{} min", minutes(*secs_left))
                }
                (Comparing::Kernels, false) => format!("trying kernels, {time}"),
                (Comparing::Kernels, true) => {
                    format!("update: trying kernels, ~{} min", minutes(*secs_left))
                }
                (Comparing::Nothing, false) => time,
                (Comparing::Nothing, true) => format!("testing update, {time}"),
            }
        }
        Status::Rechecking { current, candidate } if current == candidate => {
            "done, one more look at kernels".to_string()
        }
        Status::Rechecking { current, candidate } => {
            format!("done, one more look at {current} vs {candidate}")
        }
        Status::Settled { .. } => "done".to_string(),
        Status::LimitedByRam { best, .. } => format!("low on RAM, best is {best} threads"),
        Status::WaitingForRam => "waiting for RAM".to_string(),
    })
}

/// What the thread keys say while tuning picks the count. They work whenever the
/// count is not moving: done, another look at a close result, on a saved result,
/// and while RAM is short.
pub(super) fn threads_locked_notice(status: &Status) -> Option<String> {
    match status {
        Status::Tuning { secs_left, .. } => Some(format!(
            "Tuning is picking the thread count, about {} min left. \
             To set it yourself, start with --threads N.",
            secs_left.div_ceil(60).max(1)
        )),
        _ => None,
    }
}

fn settled_report(summary: &SettledSummary, now: u64) -> seine_telemetry::tuning::SettledTuning {
    use seine_telemetry::tuning::{CurvePoint, SeedSource, SettledTuning};
    SettledTuning {
        workers: summary.threads as u32,
        kernel: summary.kernel.to_string(),
        hps: summary.hps,
        settled_ms: now.saturating_mul(1000),
        comparisons: summary.comparisons,
        comparing_ms: summary.comparing_secs.min(now).saturating_mul(1000),
        ram_limited: summary.ram_limited,
        seed: match summary.quality {
            SeedQuality::Estimate => SeedSource::Estimate,
            SeedQuality::Family => SeedSource::Family,
            SeedQuality::Measured => SeedSource::Measured,
        },
        after_update: summary.after_update,
        curve: summary
            .curve
            .iter()
            .map(|(threads, kernel, hps)| CurvePoint {
                workers: *threads as u32,
                kernel: (*kernel).to_string(),
                hps: *hps,
            })
            .collect(),
    }
}

fn report_trial_events(
    monitor: &mut super::adaptive_telemetry::Monitor,
    now: u64,
    events: &[TrialEvent],
) {
    use seine_telemetry::tuning::{
        TrialArm, TrialDecision, TrialPolicy, TrialQuantization, TrialReason,
    };
    for event in events {
        match event {
            TrialEvent::Begin { base, candidate } => monitor.begin_settings(
                now,
                *base,
                *candidate,
                TrialPolicy {
                    revision: 2,
                    minimum_sample_ms: 7_000,
                    minimum_hashes: 16,
                    short_sample_ms: None,
                    short_minimum_hashes: None,
                    maximum_baseline_drift_bps: 10_000,
                    required_gain_bps: 25,
                    quantization: TrialQuantization::ThreeWindowHash,
                },
            ),
            TrialEvent::Open => monitor.window(now),
            TrialEvent::Discard => monitor.discard_window(),
            TrialEvent::Close { hashes, secs } => monitor.sample(*hashes, *secs),
            TrialEvent::End {
                verdict,
                applied_candidate,
            } => {
                let (decision, reason) = match verdict {
                    Verdict::Better => (TrialDecision::Gain, TrialReason::Completed),
                    Verdict::Worse | Verdict::Tie => {
                        (TrialDecision::NoConfirmedGain, TrialReason::Completed)
                    }
                    Verdict::Undecided => (TrialDecision::Inconclusive, TrialReason::BaselineDrift),
                };
                let arm = if *applied_candidate {
                    TrialArm::Candidate
                } else {
                    TrialArm::Baseline
                };
                monitor.finish_applied(now, decision, reason, arm);
            }
            TrialEvent::Abort { idle } => monitor.abort(
                now,
                if *idle {
                    TrialReason::TimeBudget
                } else {
                    TrialReason::ContextChanged
                },
            ),
        }
    }
}

fn save_result(
    cfg: &Config,
    host: &CpuAutotuneHostFingerprint,
    max: usize,
    saved: Saved,
) -> Result<()> {
    let record = CpuAutotuneRecord {
        schema_version: CPU_AUTOTUNE_RECORD_SCHEMA_VERSION,
        profile: cpu_profile_label(cfg.cpu_profile).into(),
        affinity: cpu_affinity_label(cfg.cpu_affinity).into(),
        page_mode: cfg.cpu_page_mode.as_str().into(),
        cpu_instances: instances(cfg),
        min_threads: cfg.cpu_autotune_min_threads,
        max_threads: max,
        selected_threads: saved.threads,
        measured_hps: saved.hps,
        autotune_secs: cfg.cpu_autotune_secs,
        host_fingerprint: host.clone(),
        timestamp_unix_secs: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        measurements: BTreeMap::new(),
        memory_limited: saved.ram_limit.is_some(),
        mining_measurement: true,
        search: Some(saved),
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
    Ok(())
}

#[cfg(test)]
#[path = "tuner_sim.rs"]
mod tuner_sim;

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
        assert_eq!(seed_threads(Some(&cached), 1, 16, 3, 16, None), 3);
        assert_eq!(cfg.cpu_auto_threads_cap, 1);
        assert_eq!(
            seed_threads(Some(&cached), 1, 16, 16, 16, None),
            12,
            "next unrestricted run recovers seed"
        );
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
        let mut controller = Controller::start(
            cfg.clone(),
            vec![Arc::clone(&backend)],
            None,
            None,
            crossbeam_channel::never(),
            Arc::new(super::super::session::SessionHistory::new()),
        )
        .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            backend
                .assign_work(WorkAssignment {
                    template: Arc::new(WorkTemplate {
                        work_id: 1,
                        epoch: 1,
                        header_base: Arc::from(vec![0; blocknet_pow_spec::POW_HEADER_BASE_LEN]),
                        pow: crate::backend::PowAlgorithm::Argon2id,
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

    #[test]
    fn constrained_start_provisions_stable_slots_and_keeps_user_ceiling() {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_autotune_max_threads = None;
        cfg.cpu_autotune_min_threads = 1;
        let (min, max) = worker_bounds(&cfg, 16);
        assert_eq!((min, max), (1, 16));
        assert_eq!(seed_threads(None, min, max, 1, 1, None), 1);
        assert_eq!(seed_threads(None, min, max, 8, 8, None), 4);
        cfg.cpu_autotune_max_threads = Some(4);
        let (min, max) = worker_bounds(&cfg, 16);
        let initial = seed_threads(None, min, max, 1, 1, None);
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
        assert_eq!(seed_threads(None, 3, 4, 1, 1, None), 1);
        assert_eq!(seed_threads(None, 3, 4, 4, 4, None), 4);
    }

    #[test]
    fn where_the_pressure_level_decides_arithmetic_alone_sheds_nothing() {
        let mut backoff = PressureBackoff::default();
        // No pressure: running workers stay, and growth still needs room.
        assert_eq!(backoff.cap_on(true, false, 10, 16, 11, 16), 11);
        assert_eq!(backoff.cap_on(true, false, 13, 16, 11, 16), 13);
        // Sustained pressure sheds even with room by the arithmetic.
        for _ in 0..PRESSURE_TICKS - 1 {
            assert_eq!(backoff.cap_on(true, true, 14, 16, 11, 16), 14);
        }
        assert_eq!(backoff.cap_on(true, true, 14, 16, 11, 16), 10);
        // Elsewhere the arithmetic is the limit.
        let mut backoff = PressureBackoff::default();
        assert_eq!(backoff.cap_on(false, false, 10, 16, 11, 16), 10);
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
        assert_eq!(seed_threads(Some(&r), 1, 16, 16, 16, None), 12);
        assert_eq!(seed_threads(Some(&r), 1, 16, 3, 16, None), 3);
        assert_eq!(seed_threads(Some(&r), 1, 16, 0, 16, None), 0);
        assert_eq!(seed_threads(Some(&r), 1, 6, 16, 6, None), 6);
        assert_eq!(seed_threads(None, 8, 16, 3, 16, None), 3);
        assert_eq!(seed_threads(None, 1, 16, 16, 16, None), 8);
    }

    #[test]
    fn genoa_full_count_seed_requires_exact_native_host_and_configuration() {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
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
            stepping: Some(1),
            l3_cores: None,
        };
        assert!(full_count_cold_start(&cfg, &host, &identity, 32, 32));
        let mut windows = host.clone();
        windows.os = "windows".into();
        assert!(full_count_cold_start(&cfg, &windows, &identity, 32, 32));
        for (vendor, family, model) in [
            ("GenuineIntel", 0x19, 0x11),
            ("AuthenticAMD", 0x1a, 0x11),
            ("AuthenticAMD", 0x19, 0x10),
        ] {
            let mut other = identity.clone();
            other.vendor = Some(vendor.into());
            other.family = Some(family);
            other.model = Some(model);
            assert!(!full_count_cold_start(&cfg, &host, &other, 32, 32));
        }
        for field in ["brand", "os", "runtime", "logical", "physical", "memory"] {
            let mut other = host.clone();
            match field {
                "brand" => other.cpu_brand = Some("AMD EPYC 9354P".into()),
                "os" => other.os = "macos".into(),
                "runtime" => other.runtime_environment = "wsl2".into(),
                "logical" => other.logical_cores = 64,
                "physical" => other.physical_cores = Some(16),
                "memory" => other.total_memory_bytes = 64 * 1024_u64.pow(3),
                _ => unreachable!(),
            }
            assert!(
                !full_count_cold_start(&cfg, &other, &identity, 32, 32),
                "{field}"
            );
        }
        assert!(!full_count_cold_start(&cfg, &host, &identity, 24, 32));
        assert!(!full_count_cold_start(&cfg, &host, &identity, 32, 24));
        cfg.cpu_profile = CpuPerformanceProfile::Efficiency;
        assert!(!full_count_cold_start(&cfg, &host, &identity, 32, 32));
        cfg.cpu_profile = CpuPerformanceProfile::Throughput;
        assert!(full_count_cold_start(&cfg, &host, &identity, 32, 32));
        cfg.cpu_page_mode = CpuPageMode::Regular;
        assert!(!full_count_cold_start(&cfg, &host, &identity, 32, 32));
        cfg.cpu_page_mode = CpuPageMode::Auto;
        cfg.backend_specs[0].cpu_affinity = Some(crate::config::CpuAffinityMode::Off);
        assert!(!full_count_cold_start(&cfg, &host, &identity, 32, 32));
        cfg.backend_specs[0].cpu_affinity = None;
        cfg.backend_specs.push(cfg.backend_specs[0].clone());
        assert!(!full_count_cold_start(&cfg, &host, &identity, 32, 32));
    }

    #[test]
    fn sapphire_full_count_seed_requires_exact_native_host_and_configuration() {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
        let host = CpuAutotuneHostFingerprint {
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
        assert!(sapphire_full_count_cold_start(
            &cfg, &host, &identity, 32, 32
        ));
        let mut windows = host.clone();
        windows.os = "windows".into();
        assert!(!sapphire_full_count_cold_start(
            &cfg, &windows, &identity, 32, 32
        ));
        for (vendor, family, model) in [
            ("AuthenticAMD", 6, 0x8f),
            ("GenuineIntel", 7, 0x8f),
            ("GenuineIntel", 6, 0x8e),
        ] {
            let mut other = identity.clone();
            other.vendor = Some(vendor.into());
            other.family = Some(family);
            other.model = Some(model);
            assert!(!sapphire_full_count_cold_start(&cfg, &host, &other, 32, 32));
        }
        for field in ["brand", "os", "runtime", "logical", "physical", "memory"] {
            let mut other = host.clone();
            match field {
                "brand" => other.cpu_brand = Some("AMD EPYC 9354P".into()),
                "os" => other.os = "macos".into(),
                "runtime" => other.runtime_environment = "wsl2".into(),
                "logical" => other.logical_cores = 64,
                "physical" => other.physical_cores = Some(32),
                "memory" => other.total_memory_bytes = 64 * 1024_u64.pow(3),
                _ => unreachable!(),
            }
            assert!(
                !sapphire_full_count_cold_start(&cfg, &other, &identity, 32, 32),
                "{field}"
            );
        }
        assert!(!sapphire_full_count_cold_start(
            &cfg, &host, &identity, 24, 32
        ));
        assert!(!sapphire_full_count_cold_start(
            &cfg, &host, &identity, 32, 24
        ));
        cfg.cpu_profile = CpuPerformanceProfile::Efficiency;
        assert!(!sapphire_full_count_cold_start(
            &cfg, &host, &identity, 32, 32
        ));
        cfg.cpu_profile = CpuPerformanceProfile::Throughput;
        assert!(sapphire_full_count_cold_start(
            &cfg, &host, &identity, 32, 32
        ));
        cfg.cpu_page_mode = CpuPageMode::Regular;
        assert!(!sapphire_full_count_cold_start(
            &cfg, &host, &identity, 32, 32
        ));
        cfg.cpu_page_mode = CpuPageMode::Auto;
        cfg.backend_specs[0].cpu_affinity = Some(crate::config::CpuAffinityMode::Off);
        assert!(!sapphire_full_count_cold_start(
            &cfg, &host, &identity, 32, 32
        ));
        cfg.backend_specs[0].cpu_affinity = None;
        cfg.backend_specs.push(cfg.backend_specs[0].clone());
        assert!(!sapphire_full_count_cold_start(
            &cfg, &host, &identity, 32, 32
        ));
    }

    #[test]
    fn turin_full_count_seed_requires_exact_native_host_and_configuration() {
        let mut cfg = super::super::tests::test_config();
        cfg.cpu_affinity = crate::config::CpuAffinityMode::Auto;
        cfg.backend_specs[0].cpu_affinity = None;
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
            stepping: Some(1),
            l3_cores: None,
        };
        assert!(turin_full_count_cold_start(&cfg, &host, &identity, 32, 32));
        let mut windows = host.clone();
        windows.os = "windows".into();
        assert!(!turin_full_count_cold_start(
            &cfg, &windows, &identity, 32, 32
        ));
        for (vendor, family, model) in [
            ("GenuineIntel", 0x1a, 0x02),
            ("AuthenticAMD", 0x19, 0x02),
            ("AuthenticAMD", 0x1a, 0x44),
        ] {
            let mut other = identity.clone();
            other.vendor = Some(vendor.into());
            other.family = Some(family);
            other.model = Some(model);
            assert!(!turin_full_count_cold_start(&cfg, &host, &other, 32, 32));
        }
        for field in ["brand", "os", "runtime", "logical", "physical", "memory"] {
            let mut other = host.clone();
            match field {
                "brand" => other.cpu_brand = Some("AMD EPYC 9354P".into()),
                "os" => other.os = "macos".into(),
                "runtime" => other.runtime_environment = "wsl2".into(),
                "logical" => other.logical_cores = 64,
                "physical" => other.physical_cores = Some(16),
                "memory" => other.total_memory_bytes = 64 * 1024_u64.pow(3),
                _ => unreachable!(),
            }
            assert!(
                !turin_full_count_cold_start(&cfg, &other, &identity, 32, 32),
                "{field}"
            );
        }
        assert!(!turin_full_count_cold_start(&cfg, &host, &identity, 24, 32));
        assert!(!turin_full_count_cold_start(&cfg, &host, &identity, 32, 24));
        cfg.cpu_profile = CpuPerformanceProfile::Efficiency;
        assert!(!turin_full_count_cold_start(&cfg, &host, &identity, 32, 32));
        cfg.cpu_profile = CpuPerformanceProfile::Throughput;
        assert!(turin_full_count_cold_start(&cfg, &host, &identity, 32, 32));
        cfg.cpu_page_mode = CpuPageMode::Regular;
        assert!(!turin_full_count_cold_start(&cfg, &host, &identity, 32, 32));
        cfg.cpu_page_mode = CpuPageMode::Auto;
        cfg.backend_specs[0].cpu_affinity = Some(crate::config::CpuAffinityMode::Off);
        assert!(!turin_full_count_cold_start(&cfg, &host, &identity, 32, 32));
        cfg.backend_specs[0].cpu_affinity = None;
        cfg.backend_specs.push(cfg.backend_specs[0].clone());
        assert!(!turin_full_count_cold_start(&cfg, &host, &identity, 32, 32));
    }

    #[test]
    fn full_count_seed_preserves_cache_and_transient_resource_limits() {
        let cfg = super::super::tests::test_config();
        let cached = record(&cfg, 24);
        assert_eq!(initial_threads(None, 1, 32, 32, 32, true, None, None), 32);
        assert_eq!(initial_threads(None, 1, 32, 32, 32, false, None, None), 16);
        assert_eq!(
            initial_threads(Some(&cached), 1, 32, 32, 32, true, None, None),
            24
        );
        assert_eq!(
            initial_threads(Some(&cached), 1, 32, 8, 32, true, None, None),
            8
        );
        assert_eq!(initial_threads(None, 1, 32, 8, 32, true, None, None), 8);
        assert_eq!(initial_threads(None, 1, 32, 32, 8, true, None, None), 8);
        assert_eq!(initial_threads(None, 1, 32, 0, 32, true, None, None), 0);
        // Streaming cohorts start at the physical core count, bounded by RAM;
        // cached results and the validated full-count contexts still take precedence.
        assert_eq!(
            initial_threads(None, 1, 32, 28, 28, false, Some(32), None),
            28
        );
        assert_eq!(
            initial_threads(None, 1, 32, 32, 32, false, Some(8), None),
            8
        );
        assert_eq!(initial_threads(None, 1, 32, 6, 32, false, Some(8), None), 6);
        assert_eq!(initial_threads(None, 1, 32, 0, 32, false, Some(8), None), 0);
        assert_eq!(
            initial_threads(Some(&cached), 1, 32, 32, 32, false, Some(8), None),
            24
        );
        assert_eq!(
            initial_threads(None, 1, 32, 32, 32, true, Some(8), None),
            32
        );
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
            search: None,
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
    fn autotune_seed_reports_cache_misses_without_changing_acceptance() {
        let cfg = super::super::tests::test_config();
        let original = record(&cfg, 12);
        let host = original.host_fingerprint.clone();
        assert_eq!(seed_miss_reason(&original, &cfg, &host), None);
        // These change what a result is worth, not whether it applies: the search
        // re-checks them.
        for (field, value) in [
            ("build_fingerprint", serde_json::json!("new-build")),
            ("total_memory_bytes", serde_json::json!(1024)),
            ("kernel_version", serde_json::json!("new-kernel")),
        ] {
            let mut saved = serde_json::to_value(&original).unwrap();
            saved["host_fingerprint"][field] = value;
            let saved = serde_json::from_value(saved).unwrap();
            assert_eq!(seed_miss_reason(&saved, &cfg, &host), None);
        }
        for (field, value, reason) in [
            ("cpu_brand", serde_json::json!("new-cpu"), "CPU changed"),
            (
                "runtime_environment",
                serde_json::json!("new-runtime"),
                "operating system or runtime changed",
            ),
            (
                "affinity_topology",
                serde_json::json!("new-topology"),
                "CPU affinity or topology changed",
            ),
        ] {
            let mut saved = serde_json::to_value(&original).unwrap();
            saved["host_fingerprint"][field] = value;
            let saved = serde_json::from_value(saved).unwrap();
            assert_eq!(seed_miss_reason(&saved, &cfg, &host), Some(reason));
        }
        let mut changed = original.clone();
        changed.schema_version = 0;
        assert_eq!(
            seed_miss_reason(&changed, &cfg, &host),
            Some("cache format changed")
        );
        changed = original.clone();
        changed.selected_threads = 0;
        assert_eq!(
            seed_miss_reason(&changed, &cfg, &host),
            Some("saved tuning result is invalid")
        );
        changed = original;
        changed.profile = "other".into();
        assert_eq!(
            seed_miss_reason(&changed, &cfg, &host),
            Some("tuning settings changed")
        );
    }

    #[test]
    fn thread_keys_wait_only_while_tuning_picks_the_count() {
        use super::super::adaptive_search::Comparing;
        let tuning = Status::Tuning {
            threads: 8,
            kernel: CpuKernel::Avx2Stream,
            done: 1,
            left: 3,
            secs_left: 250,
            testing_update: false,
            comparing: Comparing::Threads(8, 10),
        };
        let notice = threads_locked_notice(&tuning).unwrap();
        assert!(notice.contains("about 5 min left"), "{notice}");
        assert!(notice.contains("--threads N"), "{notice}");
        for open in [
            Status::Checking,
            Status::Settled {
                threads: 8,
                kernel: CpuKernel::Avx2Stream,
                hps: 7.0,
            },
            Status::Rechecking {
                current: 8,
                candidate: 9,
            },
            Status::LimitedByRam {
                threads: 4,
                best: 8,
            },
            Status::WaitingForRam,
        ] {
            assert_eq!(threads_locked_notice(&open), None, "{open:?}");
        }
    }
}
