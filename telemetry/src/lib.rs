//! The upload allowlist, deliberately independent of miner config and logs.
use serde::{Deserialize, Serialize};
pub mod tuning;

pub const SCHEMA_VERSION: u32 = 14;
pub const MAX_BODY_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub schema_version: u32,
    /// Random per-installation ID; never derived from hardware or a wallet.
    pub installation_id: String,
    pub report_id: String,
    pub miner_version: String,
    pub build_target: String,
    pub build_features: String,
    pub debug_build: bool,
    pub hardware: Hardware,
    pub window_secs: u64,
    pub backends: Vec<BackendSample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<Diagnostics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tuning: Option<tuning::TuningReport>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticCode {
    Warning,
    Error,
    BackendWarning,
    BackendError,
    BackendQuarantined,
    RunFailed,
    RustPanic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticKind {
    General,
    Memory,
    Timeout,
    Driver,
    Compilation,
    Connection,
    RejectedShare,
    Authentication,
    Tuning,
    UnsupportedHardware,
}

/// Fixed values only: never serialize error messages, URLs or OS error descriptions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureReason {
    HugePagesUnavailable,
    AllocationFailed,
    DnsFailed,
    ConnectionRefused,
    ConnectionTimedOut,
    ConnectionReset,
    ConnectionFailed,
    ConnectionClosed,
    IoFailed,
    LoginRejected,
    BackendStartFailed,
    NoBackendAvailable,
    CpuStartupTimedOut,
    NvidiaDriverUnavailable,
    NvrtcUnavailable,
    /// The pool refused the session at its connection limit (schema 12).
    PoolConnectionLimit,
    BackendControlFailed,
    BackendEnqueueTimedOut,
    BackendExecutionTimedOut,
    /// Work used a PoW or header format this build cannot mine; mining paused
    /// until an update (schema 14).
    UnsupportedWork,
}

/// An observed session or OS shutdown request, not proof that cleanup finished or the cause
/// of a later termination. Raw signal payloads and user input are never collected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownRequest {
    Interrupt,
    ConsoleClose,
    Logoff,
    SystemShutdown,
    ApiStop,
    ApiRestart,
    TuiQuit,
    TuiThreadChange,
    /// `--auto-update` installed a newer release and restarted the miner (schema 9).
    AutoUpdate,
}

impl ShutdownRequest {
    fn schema7_compatible(self) -> bool {
        matches!(
            self,
            Self::Interrupt | Self::ConsoleClose | Self::Logoff | Self::SystemShutdown
        )
    }

    fn schema8_compatible(self) -> bool {
        !matches!(self, Self::AutoUpdate)
    }
}

/// Last entered cleanup step, not proof that the step completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupStage {
    QuiescingBackends,
    DrainingEvents,
    StoppingNetworkWorkers,
    StoppingTui,
    StoppingAutotune,
    StoppingBackends,
    StoppingTipListener,
    FinalizingTelemetry,
}

/// Versions of the CUDA software actually loaded, not an installed toolkit guess.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NvidiaSoftware {
    /// CUDA compatibility version returned by cuDriverGetVersion (e.g. 12080).
    /// This is NOT the NVIDIA display-driver release number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver_cuda_version: Option<u32>,
    /// NVRTC major/minor, encoded as major * 1000 + minor * 10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nvrtc_version: Option<u32>,
}

impl NvidiaSoftware {
    pub fn validate(&self) -> bool {
        (self.driver_cuda_version.is_some() || self.nvrtc_version.is_some())
            && [self.driver_cuda_version, self.nvrtc_version]
                .into_iter()
                .flatten()
                .all(|v| (1000..=999_990).contains(&v) && v % 10 == 0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticOperation {
    CpuAllocation,
    PoolResolve,
    PoolConnect,
    PoolRead,
    PoolWrite,
    PoolLogin,
    BackendStartup,
    BackendCancel,
    BackendFence,
    /// Accepting a job or block template for mining (schema 14).
    ReceiveWork,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    Starting,
    ResolvingAddress,
    InitializingBackends,
    Tuning,
    Mining,
    ShuttingDown,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureDetail {
    pub reason: FailureReason,
    pub operation: DiagnosticOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub os_error: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_workers: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_workers: Option<u32>,
}

impl FailureDetail {
    pub fn new(reason: FailureReason, operation: DiagnosticOperation) -> Self {
        Self {
            reason,
            operation,
            os_error: None,
            requested_bytes: None,
            ready_workers: None,
            expected_workers: None,
        }
    }
}

/// Last observed checkpoint, not the termination cause or exact termination time.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviousRun {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup_stage: Option<CleanupStage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nvidia_software: Option<NvidiaSoftware>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shutdown_request: Option<ShutdownRequest>,
    pub phase: RunPhase,
    pub uptime_secs: u64,
    pub build_commit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub miner_version: Option<String>,
    /// Up to eight recent diagnostic groups, prioritizing panics/errors and typed causes.
    /// These may already have been uploaded; they are context, not additional incidents.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_events: Vec<DiagnosticEvent>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub events_truncated: bool,
}

impl PreviousRun {
    pub fn validate(&self) -> bool {
        self.nvidia_software
            .as_ref()
            .is_none_or(NvidiaSoftware::validate)
            && self.uptime_secs <= 315_360_000
            && valid_fingerprint(&self.build_commit)
            && self
                .miner_version
                .as_deref()
                .is_none_or(valid_release_version)
            && self.recent_events.len() <= 8
            && self
                .recent_events
                .iter()
                .all(|e| e.validate(self.uptime_secs))
    }
}

pub fn valid_release_version(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-+".contains(&b))
}

fn valid_fingerprint(value: &str) -> bool {
    value == "unknown"
        || (!value.is_empty() && value.len() <= 64 && value.bytes().all(|b| b.is_ascii_hexdigit()))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticEvent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<FailureDetail>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<RunPhase>,
    pub code: DiagnosticCode,
    pub kind: DiagnosticKind,
    /// Repository-relative source or an allowlisted dependency location, never a local path.
    pub source: String,
    pub line: u32,
    pub backend: Option<String>,
    pub count: u64,
    pub first_seen_secs: u64,
    pub last_seen_secs: u64,
}

impl DiagnosticEvent {
    fn schema12_compatible(&self) -> bool {
        self.source != "deps/tokio-1.49.0/src/sync/notify.rs"
            && self.detail.as_ref().is_none_or(|d| {
                !matches!(
                    d.reason,
                    FailureReason::BackendControlFailed
                        | FailureReason::BackendEnqueueTimedOut
                        | FailureReason::BackendExecutionTimedOut
                ) && !matches!(
                    d.operation,
                    DiagnosticOperation::BackendCancel | DiagnosticOperation::BackendFence
                )
            })
    }

    fn schema11_compatible(&self) -> bool {
        self.detail
            .as_ref()
            .is_none_or(|d| d.reason != FailureReason::PoolConnectionLimit)
    }

    fn schema13_compatible(&self) -> bool {
        self.detail.as_ref().is_none_or(|d| {
            d.reason != FailureReason::UnsupportedWork
                && d.operation != DiagnosticOperation::ReceiveWork
        })
    }
    fn schema6_compatible(&self) -> bool {
        self.detail.as_ref().is_none_or(|d| {
            !matches!(
                d.reason,
                FailureReason::NvidiaDriverUnavailable | FailureReason::NvrtcUnavailable
            )
        })
    }
    fn validate(&self, uptime_secs: u64) -> bool {
        self.detail.as_ref().is_none_or(|d| {
            d.os_error
                .is_none_or(|n| (i32::MIN as i64..=u32::MAX as i64).contains(&n))
                && d.requested_bytes.is_none_or(|n| n > 0 && n <= (1u64 << 50))
                && d.ready_workers.is_none_or(|n| n <= 65536)
                && d.expected_workers.is_none_or(|n| n <= 65536)
                && d.ready_workers
                    .zip(d.expected_workers)
                    .is_none_or(|(r, e)| r <= e)
        }) && valid_source(&self.source)
            && self.line <= 1_000_000
            && self.backend.as_deref().is_none_or(backend_name)
            && self.count > 0
            && self.count <= 1_000_000_000_000
            && self.first_seen_secs <= self.last_seen_secs
            && self.last_seen_secs <= uptime_secs
    }

    fn schema4_compatible(&self) -> bool {
        !DEPENDENCY_SOURCES.contains(&self.source.as_str())
            && self.detail.as_ref().is_none_or(|d| {
                d.reason != FailureReason::CpuStartupTimedOut
                    && d.ready_workers.is_none()
                    && d.expected_workers.is_none()
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Diagnostics {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup_stage: Option<CleanupStage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nvidia_software: Option<NvidiaSoftware>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shutdown_request: Option<ShutdownRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<RunPhase>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_run: Option<PreviousRun>,
    pub build_commit: String,
    pub source_fingerprint: String,
    pub mode: String,
    pub requested_backends: Vec<String>,
    pub requested_cpu_threads: u32,
    pub cpu_page_mode: String,
    pub cpu_affinity: String,
    pub cpu_autotune: bool,
    pub uptime_secs: u64,
    /// periodic, shutdown, or failure; short final reports are not performance windows.
    pub reason: String,
    /// An unfinished previous run, which may be a power loss or forced stop, not proof of a crash.
    pub previous_run_unclean: bool,
    pub dropped_events: u64,
    pub dropped_backend_samples: u32,
    pub events: Vec<DiagnosticEvent>,
    /// Release checks and installs in this window (schema 14).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update: Option<UpdateStatus>,
    /// Pool connection setup and churn in this window; pool mode only (schema 14).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<PoolStatus>,
    /// AMD GPUs found at startup and why any were left out (schema 14).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amd_detection: Option<AmdDetection>,
    /// Seconds in this window that mining was paused on work this build cannot
    /// mine (schema 14).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsupported_work_paused_secs: Option<u32>,
}

/// Release-check and self-update outcomes. Counts cover the reporting window.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateStatus {
    /// Whether this process installs new releases by itself (an official
    /// release build with automatic updates on).
    pub auto_install: bool,
    /// Lookups answered by the GitHub releases API.
    pub checks_api: u32,
    /// Lookups answered by the release page after the API refused.
    pub checks_web: u32,
    /// Lookups that failed both ways.
    pub checks_failed: u32,
    pub installs: u32,
    pub install_failures: u32,
    /// Stage of the most recent failed install in this window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_install_failure: Option<UpdateFailure>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateFailure {
    /// No release build for this platform.
    NoBuild,
    /// The release lacks this platform's archive, checksums or signature.
    MissingAsset,
    Download,
    Signature,
    Checksum,
    Extract,
    /// The new executable failed its startup check.
    StartupProbe,
    /// Moving the new executable into place failed.
    Replace,
}

/// Pool settings as booleans and counts only: never hosts, addresses or memos.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolStatus {
    /// The primary pool uses TLS.
    pub tls: bool,
    /// Pools are reached through a SOCKS5 proxy.
    pub socks: bool,
    /// A payout memo is configured.
    pub payout_memo: bool,
    /// Backup pools configured after the primary.
    pub backup_pools: u32,
    /// Switches to another pool in this window, across all pool sessions.
    pub failovers: u32,
    /// Seconds of this window spent on a backup pool.
    pub secs_on_backup: u32,
    /// Connection attempts after the first, across all pool sessions.
    pub reconnects: u32,
    /// Nonce-range refills received.
    pub refills: u32,
}

/// AMD GPUs seen at startup. Counts, never device identifiers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AmdDetection {
    /// The HIP runtime could not be loaded or listed no devices it could query.
    pub hip_unavailable: bool,
    pub found: u32,
    pub selected: u32,
    pub integrated_skipped: u32,
    /// Skipped for an unsupported wavefront size.
    pub unsupported_skipped: u32,
    /// AMD display devices visible to the OS (Linux sysfs, vendor 0x1002),
    /// whether or not HIP can use them; absent where unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pci_devices: Option<u32>,
}

/// Where a GPU backend's hash kernel came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GpuKernelSource {
    /// NVIDIA: a cubin built into Seine for this exact architecture.
    PrebuiltCubin,
    /// NVIDIA: PTX built into Seine, compiled by the driver.
    PrebuiltPtx,
    /// NVIDIA: compiled with NVRTC on this machine.
    Nvrtc,
    /// NVIDIA: an NVRTC build cached from an earlier run.
    NvrtcCached,
    /// NVIDIA: NVRTC was installed but failed, so built-in PTX was used.
    NvrtcFailedPrebuiltPtx,
    /// AMD: a code object built into Seine.
    BuiltinCodeObject,
    /// AMD: compiled with hipRTC on this machine.
    Hiprtc,
}

/// AMD GPU performance level (Linux sysfs) as found at startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AmdPerfLevel {
    AlreadyHigh,
    Raised,
    /// Not raised: the miner lacked permission.
    NeedsRoot,
    /// Not raised: opted out through the environment.
    KeptByOperator,
    /// No sysfs control (other platforms, or not an amdgpu device).
    Unavailable,
    /// This architecture is left at its current level.
    NotRaisedForArch,
}

impl UpdateStatus {
    fn validate(&self) -> bool {
        [
            self.checks_api,
            self.checks_web,
            self.checks_failed,
            self.installs,
            self.install_failures,
        ]
        .iter()
        .all(|&n| n <= 100_000)
            && (self.install_failures > 0 || self.last_install_failure.is_none())
    }
}

impl PoolStatus {
    fn validate(&self, uptime_secs: u64) -> bool {
        self.backup_pools <= 16
            && [self.failovers, self.reconnects]
                .iter()
                .all(|&n| n <= 1_000_000)
            && self.refills <= 100_000_000
            && u64::from(self.secs_on_backup) <= uptime_secs.max(1)
    }
}

impl AmdDetection {
    pub fn is_valid(&self) -> bool {
        self.validate()
    }

    fn validate(&self) -> bool {
        self.found <= 64
            && self.pci_devices.is_none_or(|n| n <= 64)
            && self
                .selected
                .saturating_add(self.integrated_skipped)
                .saturating_add(self.unsupported_skipped)
                <= self.found
    }
}

/// `gfx1100` (AMD) or `sm_86` (NVIDIA compute capability 8.6).
pub fn valid_gpu_arch(value: &str) -> bool {
    let digits = |s: &str, hex: bool, len: std::ops::RangeInclusive<usize>| {
        len.contains(&s.len())
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (hex && (b'a'..=b'f').contains(&b)))
    };
    value
        .strip_prefix("gfx")
        .is_some_and(|rest| digits(rest, true, 3..=5))
        || value
            .strip_prefix("sm_")
            .is_some_and(|rest| digits(rest, false, 2..=4))
}

/// Only these dependency locations can cross the privacy boundary.
pub const DEPENDENCY_SOURCES: &[&str] = &[
    "deps/cudarc-0.19.2/src/lib.rs",
    "deps/cudarc-0.19.2/src/driver/sys/mod.rs",
    "deps/cudarc-0.19.2/src/nvrtc/sys/mod.rs",
    "deps/cudarc-0.19.5/src/lib.rs",
    "deps/cudarc-0.19.5/src/driver/sys/mod.rs",
    "deps/cudarc-0.19.5/src/nvrtc/sys/mod.rs",
    "deps/tokio-1.49.0/src/sync/notify.rs",
];

pub fn valid_source(source: &str) -> bool {
    source == "unknown"
        || DEPENDENCY_SOURCES.contains(&source)
        || (source.starts_with("src/")
            && source.ends_with(".rs")
            && source.len() <= 120
            && !source.contains("..")
            && source
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_/.".contains(&b)))
}

fn backend_name(value: &str) -> bool {
    matches!(value, "cpu" | "nvidia" | "amd" | "metal")
}

impl Diagnostics {
    fn validate(&self) -> bool {
        self.nvidia_software
            .as_ref()
            .is_none_or(NvidiaSoftware::validate)
            && valid_fingerprint(&self.build_commit)
            && valid_fingerprint(&self.source_fingerprint)
            && matches!(self.mode.as_str(), "pool" | "daemon")
            && self.requested_backends.len() <= 4
            && self.requested_backends.iter().all(|b| backend_name(b))
            && self.requested_cpu_threads <= 65536
            && matches!(
                self.cpu_page_mode.as_str(),
                "auto" | "regular" | "large" | "large-1g"
            )
            && matches!(
                self.cpu_affinity.as_str(),
                "auto" | "os" | "off" | "pcore-only"
            )
            && matches!(self.reason.as_str(), "periodic" | "shutdown" | "failure")
            && self
                .previous_run
                .as_ref()
                .is_none_or(|r| self.previous_run_unclean && r.validate())
            && self.events.len() <= 32
            && self.uptime_secs <= 315_360_000
            && self.dropped_events <= 1_000_000_000_000
            && self.dropped_backend_samples <= 128
            && self.events.iter().all(|e| e.validate(self.uptime_secs))
            && self.update.as_ref().is_none_or(UpdateStatus::validate)
            && self
                .pool
                .as_ref()
                .is_none_or(|p| self.mode == "pool" && p.validate(self.uptime_secs))
            && self
                .amd_detection
                .as_ref()
                .is_none_or(AmdDetection::validate)
            && self
                .unsupported_work_paused_secs
                .is_none_or(|s| u64::from(s) <= self.uptime_secs.max(1))
    }

    fn schema13_compatible(&self) -> bool {
        self.update.is_none()
            && self.pool.is_none()
            && self.amd_detection.is_none()
            && self.unsupported_work_paused_secs.is_none()
            && self.events.iter().all(DiagnosticEvent::schema13_compatible)
            && self.previous_run.as_ref().is_none_or(|r| {
                r.recent_events
                    .iter()
                    .all(DiagnosticEvent::schema13_compatible)
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hardware {
    pub os: String,
    pub os_version: Option<String>,
    pub runtime_environment: String,
    pub cpu_model: String,
    pub cpu_vendor: Option<String>,
    pub cpu_family: Option<u32>,
    pub cpu_model_id: Option<u32>,
    pub logical_cpus: u32,
    pub physical_cores: Option<u32>,
    /// NUMA nodes on the host (schema 9); absent when unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numa_nodes: Option<u32>,
    pub memory_gib: u32,
    /// Installed memory modules from SMBIOS (schema 10); absent when unreadable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_setup: Option<MemorySetup>,
    pub supported_cpu_kernels: Vec<String>,
    /// Report-time resource snapshot; absent in schema 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
}

/// Populated memory modules (SMBIOS type 17), so fleet analysis can size
/// single-channel and below-rated-speed setups.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemorySetup {
    pub modules: u32,
    /// "DDR4", "DDR5", "LPDDR5", ...; absent when the table names another type.
    pub memory_type: Option<String>,
    pub registered: bool,
    /// Lowest module rating and configured speed, in MT/s.
    pub rated_mts: Option<u32>,
    pub configured_mts: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub allowed_logical_cpus: Option<u32>,
    /// OS estimate including CPU quotas where supported, not a measured duty cycle.
    pub available_parallelism: Option<u32>,
    pub effective_memory_gib: Option<u32>,
    pub available_memory_gib: Option<u32>,
    /// Explicit `--threads` value, when the operator fixed the CPU worker count
    /// (schema 9). Absent when workers were sized automatically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_threads: Option<u32>,
    /// Whether automatic CPU worker-count tuning was active (schema 9).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_autotune_threads: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendSettings {
    pub backend: String,
    pub device_model: Option<String>,
    pub memory_mib: Option<u64>,
    pub lanes: u32,
    pub cpu_kernel: Option<String>,
    pub cpu_page_mode: Option<String>,
    pub gpu_register_cap: Option<u32>,
    pub gpu_launch_depth: Option<u32>,
    /// Allocator-observed MiB: explicit large, 1 GiB, THP, regular, heap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_backing_mib: Option<[u64; 5]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_affinity: Option<String>,
    /// `blake2b` while the backend mines Faraday BLAKE2b work; absent for
    /// Argon2id (schema 11). Rates are not comparable across the two.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pow: Option<String>,
    /// GPU architecture: `gfx1100` (AMD) or `sm_86` (NVIDIA compute
    /// capability) (schema 14).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_arch: Option<String>,
    /// Where the GPU hash kernel came from (schema 14).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_kernel_source: Option<GpuKernelSource>,
    /// AMD performance level at startup (schema 14).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amd_perf_level: Option<AmdPerfLevel>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendSample {
    /// Session-local index, not a device serial or persistent identifier.
    pub instance: u64,
    pub settings: BackendSettings,
    pub hashes: u64,
    pub observed_secs: f64,
    /// Omit comparisons when tuning changed inside the reporting window.
    pub settings_changed: bool,
}

fn short(value: &str) -> bool {
    !value.is_empty() && value.len() <= 160 && !value.chars().any(char::is_control)
}

fn optional_short(value: &Option<String>) -> bool {
    value.as_deref().is_none_or(short)
}

pub fn valid_id(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

impl Report {
    pub fn validate(&self) -> bool {
        let h = &self.hardware;
        (1..=SCHEMA_VERSION).contains(&self.schema_version)
            && self.tuning.as_ref().is_none_or(|t| {
                self.schema_version >= 6
                    && t.validate()
                    && self.diagnostics.as_ref().is_some_and(|d| {
                        t.trials.iter().all(|trial| {
                            trial.outcome.finished_ms
                                <= d.uptime_secs.saturating_mul(1000).saturating_add(999)
                        })
                    })
                    && serde_json::to_vec(t).is_ok_and(|b| b.len() <= tuning::MAX_TUNING_BYTES)
            })
            && valid_id(&self.installation_id)
            && valid_id(&self.report_id)
            && [
                &self.miner_version,
                &self.build_target,
                &self.build_features,
                &h.os,
                &h.runtime_environment,
                &h.cpu_model,
            ]
            .into_iter()
            .all(|s| short(s))
            && optional_short(&h.os_version)
            && optional_short(&h.cpu_vendor)
            && h.logical_cpus <= 65536
            && h.physical_cores.is_none_or(|v| v <= 65536)
            && h.numa_nodes.is_none_or(|v| (1..=1024).contains(&v))
            && (self.schema_version >= 9 || h.numa_nodes.is_none())
            && h.memory_setup.as_ref().is_none_or(|m| {
                (1..=1024).contains(&m.modules)
                    && optional_short(&m.memory_type)
                    && m.rated_mts.is_none_or(|v| (1..=100_000).contains(&v))
                    && m.configured_mts.is_none_or(|v| (1..=100_000).contains(&v))
            })
            && (self.schema_version >= 10 || h.memory_setup.is_none())
            && h.memory_gib <= 1048576
            && h.resources.as_ref().is_none_or(|r| {
                r.allowed_logical_cpus.is_none_or(|v| v <= 65536)
                    && r.available_parallelism.is_none_or(|v| v <= 65536)
                    && r.effective_memory_gib.is_none_or(|v| v <= 1048576)
                    && r.available_memory_gib.is_none_or(|v| v <= 1048576)
                    && r.requested_threads.is_none_or(|v| (1..=65536).contains(&v))
                    && (self.schema_version >= 9
                        || (r.requested_threads.is_none() && r.cpu_autotune_threads.is_none()))
            })
            && (self.schema_version >= 2
                || (h.resources.is_none()
                    && self.backends.iter().all(|b| {
                        b.settings.cpu_backing_mib.is_none() && b.settings.cpu_affinity.is_none()
                    })))
            && h.supported_cpu_kernels.len() <= 32
            && h.supported_cpu_kernels.iter().all(|s| short(s))
            && (1..=86400).contains(&self.window_secs)
            && (self.window_secs >= 60
                || self
                    .diagnostics
                    .as_ref()
                    .is_some_and(|d| d.reason != "periodic"))
            && self.diagnostics.as_ref().is_none_or(|d| {
                self.schema_version >= 3
                    && d.validate()
                    && (self.schema_version >= 8
                        || (d.shutdown_request.is_none()
                            && d.cleanup_stage.is_none()
                            && d.nvidia_software.is_none()
                            && d.events
                                .iter()
                                .all(|e| !e.source.starts_with("deps/cudarc-0.19.5/"))
                            && d.previous_run.as_ref().is_none_or(|r| {
                                r.recent_events
                                    .iter()
                                    .all(|e| !e.source.starts_with("deps/cudarc-0.19.5/"))
                                    && r.cleanup_stage.is_none()
                                    && r.nvidia_software.is_none()
                                    && r.shutdown_request
                                        .is_none_or(ShutdownRequest::schema7_compatible)
                            })))
                    && (self.schema_version >= 9
                        || (d
                            .shutdown_request
                            .is_none_or(ShutdownRequest::schema8_compatible)
                            && d.previous_run.as_ref().is_none_or(|r| {
                                r.shutdown_request
                                    .is_none_or(ShutdownRequest::schema8_compatible)
                            })))
                    && (self.schema_version >= 14 || d.schema13_compatible())
                    && (self.schema_version >= 13
                        || (d.events.iter().all(DiagnosticEvent::schema12_compatible)
                            && d.previous_run.as_ref().is_none_or(|r| {
                                r.recent_events
                                    .iter()
                                    .all(DiagnosticEvent::schema12_compatible)
                            })))
                    && (self.schema_version >= 12
                        || (d.events.iter().all(DiagnosticEvent::schema11_compatible)
                            && d.previous_run.as_ref().is_none_or(|r| {
                                r.recent_events
                                    .iter()
                                    .all(DiagnosticEvent::schema11_compatible)
                            })))
                    && (self.schema_version >= 7
                        || (d.events.iter().all(DiagnosticEvent::schema6_compatible)
                            && d.previous_run.as_ref().is_none_or(|r| {
                                r.shutdown_request.is_none()
                                    && r.recent_events
                                        .iter()
                                        .all(DiagnosticEvent::schema6_compatible)
                            })))
                    && (self.schema_version >= 5
                        || (d.previous_run.as_ref().is_none_or(|r| {
                            r.miner_version.is_none()
                                && r.recent_events.is_empty()
                                && !r.events_truncated
                        }) && d.events.iter().all(DiagnosticEvent::schema4_compatible)))
                    && (self.schema_version >= 4
                        || (d.phase.is_none()
                            && d.previous_run.is_none()
                            && d.events
                                .iter()
                                .all(|e| e.detail.is_none() && e.phase.is_none())))
            })
            && self.backends.len() <= 128
            && self.backends.iter().all(|b| {
                matches!(
                    b.settings.backend.as_str(),
                    "cpu" | "nvidia" | "amd" | "metal"
                ) && optional_short(&b.settings.device_model)
                    && optional_short(&b.settings.cpu_kernel)
                    && optional_short(&b.settings.cpu_page_mode)
                    && b.settings
                        .cpu_affinity
                        .as_deref()
                        .is_none_or(|v| matches!(v, "auto" | "os" | "off" | "pcore-only"))
                    && b.settings
                        .cpu_backing_mib
                        .is_none_or(|v| v.iter().all(|&mib| mib <= 1073741824))
                    && (b.settings.backend == "cpu"
                        || (b.settings.cpu_backing_mib.is_none()
                            && b.settings.cpu_affinity.is_none()))
                    && b.settings.pow.as_deref().is_none_or(|v| v == "blake2b")
                    && (self.schema_version >= 11 || b.settings.pow.is_none())
                    && b.settings.gpu_arch.as_deref().is_none_or(valid_gpu_arch)
                    && (matches!(b.settings.backend.as_str(), "nvidia" | "amd")
                        || (b.settings.gpu_arch.is_none()
                            && b.settings.gpu_kernel_source.is_none()))
                    && (b.settings.backend == "amd" || b.settings.amd_perf_level.is_none())
                    && (self.schema_version >= 14
                        || (b.settings.gpu_arch.is_none()
                            && b.settings.gpu_kernel_source.is_none()
                            && b.settings.amd_perf_level.is_none()))
                    && b.settings.lanes <= 65536
                    && b.settings.memory_mib.is_none_or(|v| v <= 1073741824)
                    && b.observed_secs.is_finite()
                    && b.observed_secs > 0.0
                    && b.observed_secs <= self.window_secs as f64 + 1.0
                    // BLAKE2b runs at GH/s per GPU, so a day can exceed the Argon2 bound.
                    && b.hashes
                        <= if b.settings.pow.is_some() {
                            1_000_000_000_000_000_000
                        } else {
                            1_000_000_000_000_000
                        }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_details_and_notify_sources_require_schema13_in_current_and_previous_events() {
        let base: Report =
            serde_json::from_str(include_str!("../reliability-fixture.json")).unwrap();
        let mut variants = Vec::new();
        for operation in [
            DiagnosticOperation::BackendCancel,
            DiagnosticOperation::BackendFence,
        ] {
            for reason in [
                FailureReason::BackendControlFailed,
                FailureReason::BackendEnqueueTimedOut,
                FailureReason::BackendExecutionTimedOut,
            ] {
                let mut event = base.diagnostics.as_ref().unwrap().events[0].clone();
                event.detail = Some(FailureDetail::new(reason, operation));
                variants.push(event);
            }
        }
        let mut event = base.diagnostics.as_ref().unwrap().events[0].clone();
        event.source = "deps/tokio-1.49.0/src/sync/notify.rs".into();
        variants.push(event);
        for event in variants {
            for previous in [false, true] {
                let mut report = base.clone();
                let diagnostics = report.diagnostics.as_mut().unwrap();
                if previous {
                    let run = diagnostics.previous_run.as_mut().unwrap();
                    let mut event = event.clone();
                    event.first_seen_secs = 0;
                    event.last_seen_secs = 0;
                    run.recent_events = vec![event];
                } else {
                    diagnostics.events = vec![event.clone()];
                }
                report.schema_version = 13;
                assert!(report.validate());
                report.schema_version = 12;
                assert!(!report.validate());
            }
        }
    }

    #[test]
    fn schema8_context_is_optional_bounded_and_cannot_be_downgraded() {
        let base: serde_json::Value =
            serde_json::from_str(include_str!("../reliability-fixture.json")).unwrap();
        for previous in [false, true] {
            for (field, value) in [
                ("shutdown_request", serde_json::json!("api_stop")),
                ("cleanup_stage", serde_json::json!("stopping_backends")),
                (
                    "nvidia_software",
                    serde_json::json!({"driver_cuda_version": 12080, "nvrtc_version": 12090}),
                ),
            ] {
                let mut value_report = base.clone();
                value_report["schema_version"] = 8.into();
                let context = if previous {
                    &mut value_report["diagnostics"]["previous_run"]
                } else {
                    &mut value_report["diagnostics"]
                };
                context[field] = value;
                let mut report: Report = serde_json::from_value(value_report).unwrap();
                assert!(report.validate(), "{field}, previous={previous}");
                report.schema_version = 7;
                assert!(!report.validate(), "{field} must require schema 8");
            }
        }
        for bad in [
            serde_json::json!({}),
            serde_json::json!({"driver_cuda_version": 0}),
            serde_json::json!({"nvrtc_version": 12081}),
            serde_json::json!({"nvrtc_version": 1_000_000}),
        ] {
            let software: NvidiaSoftware = serde_json::from_value(bad).unwrap();
            assert!(!software.validate());
        }
        for field in ["cleanup_stage", "shutdown_request", "nvidia_software"] {
            let mut value = base.clone();
            value["schema_version"] = 8.into();
            value["diagnostics"][field] = "private free text".into();
            assert!(serde_json::from_value::<Report>(value).is_err());
        }
        let mut report: Report = serde_json::from_value(base).unwrap();
        report.schema_version = 8;
        report.diagnostics.as_mut().unwrap().events[0].source =
            "deps/cudarc-0.19.5/src/driver/sys/mod.rs".into();
        assert!(report.validate());
        report.schema_version = 7;
        assert!(!report.validate());
    }

    #[test]
    fn shutdown_requests_and_loader_causes_require_schema7() {
        let mut report: Report =
            serde_json::from_str(include_str!("../reliability-fixture.json")).unwrap();
        report.schema_version = 7;
        report
            .diagnostics
            .as_mut()
            .unwrap()
            .previous_run
            .as_mut()
            .unwrap()
            .shutdown_request = Some(ShutdownRequest::ConsoleClose);
        assert!(report.validate());
        report.schema_version = 6;
        assert!(!report.validate());
        report
            .diagnostics
            .as_mut()
            .unwrap()
            .previous_run
            .as_mut()
            .unwrap()
            .shutdown_request = None;
        assert!(report.validate());
        for reason in [
            FailureReason::NvidiaDriverUnavailable,
            FailureReason::NvrtcUnavailable,
        ] {
            report.diagnostics.as_mut().unwrap().events[0].detail = Some(FailureDetail::new(
                reason,
                DiagnosticOperation::BackendStartup,
            ));
            assert!(!report.validate());
            report.schema_version = 7;
            assert!(report.validate());
            report.schema_version = 6;
            report.diagnostics.as_mut().unwrap().events[0].detail = None;
            report
                .diagnostics
                .as_mut()
                .unwrap()
                .previous_run
                .as_mut()
                .unwrap()
                .recent_events[0]
                .detail = Some(FailureDetail::new(
                reason,
                DiagnosticOperation::BackendStartup,
            ));
            assert!(!report.validate());
            report.schema_version = 7;
            assert!(report.validate());
            report.schema_version = 6;
        }
        let mut value = serde_json::to_value(&report).unwrap();
        value["diagnostics"]["previous_run"]["shutdown_request"] = "private free text".into();
        assert!(serde_json::from_value::<Report>(value).is_err());
    }

    #[test]
    fn recovered_diagnostics_are_bounded_private_and_schema5_only() {
        let report: Report =
            serde_json::from_str(include_str!("../reliability-fixture.json")).unwrap();
        assert!(report.validate());
        let mut old = report.clone();
        old.schema_version = 4;
        assert!(!old.validate());
        for source in [
            "/home/private/cudarc/src/lib.rs",
            "deps/private.rs",
            "deps/cudarc-0.19.2/src/secret.rs",
        ] {
            let mut bad = report.clone();
            bad.diagnostics
                .as_mut()
                .unwrap()
                .previous_run
                .as_mut()
                .unwrap()
                .recent_events[0]
                .source = source.into();
            assert!(!bad.validate());
        }
        let mut bad = report.clone();
        let previous = bad
            .diagnostics
            .as_mut()
            .unwrap()
            .previous_run
            .as_mut()
            .unwrap();
        previous.recent_events = vec![previous.recent_events[0].clone(); 9];
        assert!(!bad.validate());
        let mut bad = report.clone();
        bad.diagnostics
            .as_mut()
            .unwrap()
            .previous_run
            .as_mut()
            .unwrap()
            .recent_events[0]
            .last_seen_secs = 20;
        assert!(!bad.validate()); // Previous uptime, not the restarted process's uptime.
        let mut bad = report.clone();
        bad.diagnostics
            .as_mut()
            .unwrap()
            .previous_run
            .as_mut()
            .unwrap()
            .miner_version = Some("/home/private".into());
        assert!(!bad.validate());
        let mut value = serde_json::to_value(&report).unwrap();
        value["diagnostics"]["previous_run"]["recent_events"][0]["message"] = "private".into();
        assert!(serde_json::from_value::<Report>(value).is_err());

        let mut max = report.clone();
        let d = max.diagnostics.as_mut().unwrap();
        let mut event = d.events[0].clone();
        event.source = format!("src/{}.rs", "a".repeat(113));
        d.events = vec![event.clone(); 32];
        let previous = d.previous_run.as_mut().unwrap();
        previous.uptime_secs = d.uptime_secs;
        previous.recent_events = vec![event; 8];
        assert!(max.validate());
        assert!(serde_json::to_vec(&max).unwrap().len() < MAX_BODY_BYTES);
    }

    #[test]
    fn readiness_counts_and_dependency_sources_cannot_be_downgraded() {
        let mut report: Report =
            serde_json::from_str(include_str!("../diagnostics-fixture.json")).unwrap();
        report.schema_version = 5;
        let mut detail = FailureDetail::new(
            FailureReason::CpuStartupTimedOut,
            DiagnosticOperation::BackendStartup,
        );
        detail.ready_workers = Some(2);
        detail.expected_workers = Some(8);
        report.diagnostics.as_mut().unwrap().events[0].detail = Some(detail);
        assert!(report.validate());
        report.schema_version = 4;
        assert!(!report.validate());
        report.schema_version = 5;
        report.diagnostics.as_mut().unwrap().events[0]
            .detail
            .as_mut()
            .unwrap()
            .ready_workers = Some(9);
        assert!(!report.validate());
        report.diagnostics.as_mut().unwrap().events[0].detail = None;
        report.diagnostics.as_mut().unwrap().events[0].source = DEPENDENCY_SOURCES[0].into();
        assert!(report.validate());
        report.schema_version = 4;
        assert!(!report.validate());
    }

    #[test]
    fn diagnostics_are_allowlisted_bounded_and_versioned() {
        let value: serde_json::Value =
            serde_json::from_str(include_str!("../diagnostics-fixture.json")).unwrap();
        let report: Report = serde_json::from_value(value.clone()).unwrap();
        assert!(report.validate());
        for field in ["message", "stacktrace", "wallet", "command_line"] {
            let mut bad = value.clone();
            bad["diagnostics"]["events"][0][field] = "secret".into();
            assert!(serde_json::from_value::<Report>(bad).is_err());
        }
        for (field, secret) in [
            ("code", "private-error-text"),
            ("kind", "private-error-text"),
        ] {
            let mut bad = value.clone();
            bad["diagnostics"]["events"][0][field] = secret.into();
            assert!(serde_json::from_value::<Report>(bad).is_err());
        }
        for path in [
            "/home/alice/src/main.rs",
            "src/../../private.rs",
            "C:\\Users\\Alice\\main.rs",
        ] {
            let mut bad = report.clone();
            bad.diagnostics.as_mut().unwrap().events[0].source = path.into();
            assert!(!bad.validate());
        }
        let mut bad = report.clone();
        bad.schema_version = 2;
        assert!(!bad.validate());
        let mut bad = report.clone();
        let d = bad.diagnostics.as_mut().unwrap();
        d.events = vec![d.events[0].clone(); 33];
        assert!(!bad.validate());
        let mut final_report = report;
        final_report.backends.clear();
        final_report.window_secs = 1;
        assert!(!final_report.validate());
        final_report.diagnostics.as_mut().unwrap().reason = "failure".into();
        assert!(final_report.validate());
    }

    #[test]
    fn structured_diagnostics_reject_secrets_limits_and_schema_downgrades() {
        let value: serde_json::Value =
            serde_json::from_str(include_str!("../diagnostics-fixture.json")).unwrap();
        for field in ["reason", "operation", "message"] {
            let mut bad = value.clone();
            bad["diagnostics"]["events"][0]["detail"][field] = "PRIVATE".into();
            assert!(serde_json::from_value::<Report>(bad).is_err());
        }
        let mut report: Report = serde_json::from_value(value).unwrap();
        assert!(report.validate());
        report.schema_version = 3;
        assert!(!report.validate());
        report.diagnostics.as_mut().unwrap().phase = None;
        report.diagnostics.as_mut().unwrap().previous_run = None;
        for event in &mut report.diagnostics.as_mut().unwrap().events {
            event.detail = None;
            event.phase = None;
        }
        assert!(report.validate()); // Real schema 3 remains readable.
        let mut report: Report =
            serde_json::from_str(include_str!("../diagnostics-fixture.json")).unwrap();
        report.diagnostics.as_mut().unwrap().events[0]
            .detail
            .as_mut()
            .unwrap()
            .requested_bytes = Some(u64::MAX);
        assert!(!report.validate());
        report.diagnostics.as_mut().unwrap().events[0]
            .detail
            .as_mut()
            .unwrap()
            .requested_bytes = Some(2147483648);
        report.diagnostics.as_mut().unwrap().events[0]
            .detail
            .as_mut()
            .unwrap()
            .os_error = Some(i64::MAX);
        assert!(!report.validate());
        report.diagnostics.as_mut().unwrap().events[0]
            .detail
            .as_mut()
            .unwrap()
            .os_error = Some(12);
        report.diagnostics.as_mut().unwrap().previous_run_unclean = false;
        assert!(!report.validate());
        report.diagnostics.as_mut().unwrap().previous_run_unclean = true;
        let d = report.diagnostics.as_mut().unwrap();
        d.events = vec![d.events[0].clone(); 32];
        for e in &mut d.events {
            e.source = format!("src/{}.rs", "a".repeat(113));
        }
        assert!(report.validate());
        assert!(serde_json::to_vec(&report).unwrap().len() < MAX_BODY_BYTES);
    }

    #[test]
    fn strict_schema_and_numeric_limits_reject_unintended_data() {
        let value: serde_json::Value =
            serde_json::from_str(include_str!("../fixture.json")).unwrap();
        let report: Report = serde_json::from_value(value.clone()).unwrap();
        assert!(report.validate());
        for path in [
            "wallet_address",
            "hostname",
            "logs",
            "worker_name",
            "api_token",
        ] {
            let mut bad = value.clone();
            bad[path] = "private".into();
            assert!(serde_json::from_value::<Report>(bad).is_err());
        }
        let mut bad = value.clone();
        bad["hardware"]["hostname"] = "private".into();
        assert!(serde_json::from_value::<Report>(bad).is_err());
        let mut report: Report = serde_json::from_value(value).unwrap();
        report.backends[0].observed_secs = f64::NAN;
        assert!(!report.validate());
        report.backends[0].observed_secs = 99999.0;
        assert!(!report.validate());
        report.backends[0].observed_secs = 3600.0;
        report.hardware.cpu_model = "x".repeat(161);
        assert!(!report.validate());
    }

    #[test]
    fn accepts_legacy_and_extended_reports_without_confusing_unknown_with_zero() {
        let value: serde_json::Value =
            serde_json::from_str(include_str!("../fixture.json")).unwrap();
        let mut legacy = value.clone();
        legacy["schema_version"] = 1.into();
        legacy["hardware"]
            .as_object_mut()
            .unwrap()
            .remove("resources");
        for backend in legacy["backends"].as_array_mut().unwrap() {
            backend["settings"]
                .as_object_mut()
                .unwrap()
                .remove("cpu_backing_mib");
            backend["settings"]
                .as_object_mut()
                .unwrap()
                .remove("cpu_affinity");
        }
        let old: Report = serde_json::from_value(legacy).unwrap();
        assert!(old.validate());
        assert!(old.hardware.resources.is_none());
        assert!(old.backends[0].settings.cpu_backing_mib.is_none());
        let mut extended: Report = serde_json::from_value(value).unwrap();
        assert!(extended.validate());
        extended.schema_version = 1;
        assert!(!extended.validate(), "new fields must advertise schema 2");
        extended.schema_version = SCHEMA_VERSION + 1;
        assert!(!extended.validate());
        extended.schema_version = 2;
        extended.backends[0].settings.cpu_backing_mib = Some([u64::MAX; 5]);
        assert!(!extended.validate());
    }

    #[test]
    fn auto_update_shutdown_request_requires_schema_9() {
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../diagnostics-fixture.json")).unwrap();
        value["schema_version"] = 9.into();
        value["diagnostics"]["shutdown_request"] = serde_json::json!("auto_update");
        let mut report: Report = serde_json::from_value(value).unwrap();
        assert!(report.validate(), "schema 9 accepts auto_update");
        report.schema_version = 8;
        assert!(!report.validate(), "auto_update must advertise schema 9");
    }

    #[test]
    fn worker_sizing_fields_require_schema_9() {
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../fixture.json")).unwrap();
        value["schema_version"] = 9.into();
        value["hardware"]["resources"]["requested_threads"] = 6.into();
        value["hardware"]["resources"]["cpu_autotune_threads"] = false.into();
        let mut report: Report = serde_json::from_value(value).unwrap();
        assert!(report.validate());
        report.schema_version = 8;
        assert!(!report.validate(), "sizing fields must advertise schema 9");
        report.schema_version = 9;
        report
            .hardware
            .resources
            .as_mut()
            .unwrap()
            .requested_threads = Some(0);
        assert!(
            !report.validate(),
            "an explicit count is at least one worker"
        );
    }

    #[test]
    fn numa_node_count_requires_schema_9() {
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../fixture.json")).unwrap();
        value["schema_version"] = 9.into();
        value["hardware"]["numa_nodes"] = 2.into();
        let mut report: Report = serde_json::from_value(value).unwrap();
        assert!(report.validate());
        report.schema_version = 8;
        assert!(!report.validate(), "numa_nodes must advertise schema 9");
        report.schema_version = 9;
        report.hardware.numa_nodes = Some(0);
        assert!(!report.validate(), "a host has at least one node");
    }

    #[test]
    fn memory_setup_requires_schema_10() {
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../fixture.json")).unwrap();
        value["schema_version"] = 10.into();
        value["hardware"]["memory_setup"] = serde_json::json!({
            "modules": 1, "memory_type": "DDR5", "registered": false,
            "rated_mts": 4800, "configured_mts": 4800
        });
        let mut report: Report = serde_json::from_value(value).unwrap();
        assert!(report.validate());
        report.schema_version = 9;
        assert!(!report.validate(), "memory_setup must advertise schema 10");
        report.schema_version = 10;
        report.hardware.memory_setup.as_mut().unwrap().modules = 0;
        assert!(
            !report.validate(),
            "a reported setup has at least one module"
        );
        report.hardware.memory_setup.as_mut().unwrap().modules = 2;
        report
            .hardware
            .memory_setup
            .as_mut()
            .unwrap()
            .configured_mts = Some(0);
        assert!(!report.validate(), "speeds are positive when present");
    }

    #[test]
    fn pool_connection_limit_requires_schema_12() {
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../diagnostics-fixture.json")).unwrap();
        value["schema_version"] = 12.into();
        value["diagnostics"]["events"][0]["detail"] = serde_json::json!({
            "reason": "pool_connection_limit",
            "operation": "pool_connect",
        });
        value["diagnostics"]["events"][0]["kind"] = "connection".into();
        value["diagnostics"]["events"][0]["backend"] = serde_json::Value::Null;
        let mut report: Report = serde_json::from_value(value).unwrap();
        assert!(report.validate());
        report.schema_version = 11;
        assert!(!report.validate(), "the reason must advertise schema 12");
    }

    #[test]
    fn blake2b_backend_pow_requires_schema_11() {
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../fixture.json")).unwrap();
        value["schema_version"] = 11.into();
        value["backends"][0]["settings"]["pow"] = "blake2b".into();
        value["backends"][0]["hashes"] = 5_000_000_000_000_000u64.into();
        let mut report: Report = serde_json::from_value(value).unwrap();
        assert!(report.validate(), "GH/s-scale BLAKE2b samples are valid");
        report.schema_version = 10;
        assert!(!report.validate(), "pow must advertise schema 11");
        report.schema_version = 11;
        report.backends[0].settings.pow = None;
        assert!(!report.validate(), "Argon2 samples keep the Argon2 bound");
        report.backends[0].hashes = 1_000;
        assert!(report.validate());
        report.backends[0].settings.pow = Some("argon2id".into());
        assert!(!report.validate(), "Argon2id is reported by omission");
    }

    #[test]
    fn schema14_fixture_populates_every_new_field_and_requires_schema_14() {
        let base: Report = serde_json::from_str(include_str!("../schema14-fixture.json")).unwrap();
        assert!(base.validate());
        let diagnostics = base.diagnostics.as_ref().unwrap();
        assert!(diagnostics.update.is_some());
        assert!(diagnostics.pool.is_some());
        assert!(diagnostics.amd_detection.is_some());
        assert!(diagnostics.unsupported_work_paused_secs.is_some());

        let clear_new_fields = |report: &mut Report| {
            let d = report.diagnostics.as_mut().unwrap();
            d.update = None;
            d.pool = None;
            d.amd_detection = None;
            d.unsupported_work_paused_secs = None;
            d.events.clear();
            for backend in &mut report.backends {
                backend.settings.gpu_arch = None;
                backend.settings.gpu_kernel_source = None;
                backend.settings.amd_perf_level = None;
            }
        };
        let mut cleared = base.clone();
        clear_new_fields(&mut cleared);
        cleared.schema_version = 13;
        assert!(cleared.validate(), "older reports stay accepted");

        let downgrades: Vec<(&str, fn(&mut Report))> = vec![
            ("update", |r| {
                r.diagnostics.as_mut().unwrap().update = Some(UpdateStatus::default());
            }),
            ("pool", |r| {
                r.diagnostics.as_mut().unwrap().pool = Some(PoolStatus::default());
            }),
            ("amd_detection", |r| {
                r.diagnostics.as_mut().unwrap().amd_detection = Some(AmdDetection::default());
            }),
            ("paused", |r| {
                r.diagnostics.as_mut().unwrap().unsupported_work_paused_secs = Some(0);
            }),
            ("event", |r| {
                let mut event = DiagnosticEvent {
                    detail: Some(FailureDetail::new(
                        FailureReason::UnsupportedWork,
                        DiagnosticOperation::ReceiveWork,
                    )),
                    phase: None,
                    code: DiagnosticCode::Warning,
                    kind: DiagnosticKind::General,
                    source: "src/miner/mod.rs".into(),
                    line: 1,
                    backend: None,
                    count: 1,
                    first_seen_secs: 0,
                    last_seen_secs: 0,
                };
                event.phase = Some(RunPhase::Mining);
                r.diagnostics.as_mut().unwrap().events.push(event);
            }),
            ("gpu_arch", |r| {
                r.backends[1].settings.gpu_arch = Some("sm_86".into());
            }),
            ("gpu_kernel_source", |r| {
                r.backends[1].settings.gpu_kernel_source = Some(GpuKernelSource::Nvrtc);
            }),
            ("amd_perf_level", |r| {
                r.backends[2].settings.amd_perf_level = Some(AmdPerfLevel::Raised);
            }),
        ];
        for (name, apply) in downgrades {
            let mut report = cleared.clone();
            apply(&mut report);
            report.schema_version = 14;
            assert!(report.validate(), "{name} is valid at schema 14");
            report.schema_version = 13;
            assert!(!report.validate(), "{name} must require schema 14");
        }
    }

    #[test]
    fn schema14_fields_are_bounded_and_private() {
        let base: Report = serde_json::from_str(include_str!("../schema14-fixture.json")).unwrap();
        let invalid: Vec<(&str, fn(&mut Report))> = vec![
            ("update count", |r| {
                r.diagnostics
                    .as_mut()
                    .unwrap()
                    .update
                    .as_mut()
                    .unwrap()
                    .checks_api = 100_001;
            }),
            ("failure without a count", |r| {
                let update = r.diagnostics.as_mut().unwrap().update.as_mut().unwrap();
                update.install_failures = 0;
            }),
            ("backup pools", |r| {
                r.diagnostics
                    .as_mut()
                    .unwrap()
                    .pool
                    .as_mut()
                    .unwrap()
                    .backup_pools = 17;
            }),
            ("backup time beyond uptime", |r| {
                r.diagnostics
                    .as_mut()
                    .unwrap()
                    .pool
                    .as_mut()
                    .unwrap()
                    .secs_on_backup = 3601;
            }),
            ("pool status in daemon mode", |r| {
                r.diagnostics.as_mut().unwrap().mode = "daemon".into();
            }),
            ("AMD counts beyond found", |r| {
                let amd = r
                    .diagnostics
                    .as_mut()
                    .unwrap()
                    .amd_detection
                    .as_mut()
                    .unwrap();
                amd.selected = amd.found + 1;
            }),
            ("paused beyond uptime", |r| {
                r.diagnostics.as_mut().unwrap().unsupported_work_paused_secs = Some(3601);
            }),
            ("free-form architecture", |r| {
                r.backends[1].settings.gpu_arch = Some("GeForce RTX 4090".into());
            }),
            ("GPU fields on the CPU", |r| {
                r.backends[0].settings.gpu_arch = Some("sm_86".into());
            }),
            ("perf level on NVIDIA", |r| {
                r.backends[1].settings.amd_perf_level = Some(AmdPerfLevel::Raised);
            }),
        ];
        for (name, apply) in invalid {
            let mut report = base.clone();
            apply(&mut report);
            assert!(!report.validate(), "{name} must be rejected");
        }

        let value: serde_json::Value = serde_json::to_value(&base).unwrap();
        for (section, field) in [
            ("pool", "url"),
            ("pool", "host"),
            ("pool", "memo"),
            ("update", "error"),
            ("amd_detection", "device_names"),
        ] {
            let mut bad = value.clone();
            bad["diagnostics"][section][field] = "private".into();
            assert!(
                serde_json::from_value::<Report>(bad).is_err(),
                "{section}.{field} must not be accepted"
            );
        }
    }

    #[test]
    fn gpu_architecture_names_are_allowlisted() {
        for good in ["gfx1100", "gfx90a", "gfx1036", "sm_61", "sm_86", "sm_120"] {
            assert!(valid_gpu_arch(good), "{good}");
        }
        for bad in [
            "",
            "gfx",
            "gfx1100:xnack-",
            "GFX1100",
            "sm_",
            "sm_8.6",
            "sm_12345",
            "gfx123456",
            "../etc",
        ] {
            assert!(!valid_gpu_arch(bad), "{bad}");
        }
    }

    #[test]
    fn rejects_non_random_identifier_formats() {
        assert!(!valid_id("my-hostname"));
        assert!(!valid_id("0123456789abcdef0123456789abcdeg"));
        assert!(valid_id("0123456789abcdef0123456789abcdef"));
    }
}
