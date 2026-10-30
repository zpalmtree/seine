use std::collections::VecDeque;
use std::convert::TryFrom;
use std::ffi::{c_char, CStr, CString};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use blake2::{Blake2b512, Digest};
use blocknet_pow_spec::{pow_params, CPU_LANE_MEMORY_BYTES, POW_HEADER_BASE_LEN, POW_OUTPUT_LEN};
use crossbeam_channel::{
    bounded, Receiver, RecvTimeoutError, SendTimeoutError, Sender, TryRecvError, TrySendError,
};
use cudarc::{
    driver::{
        CudaContext, CudaFunction, CudaSlice, CudaStream, DriverError, LaunchConfig, PushKernelArg,
    },
    nvrtc::{result as nvrtc_result, sys as nvrtc_sys, Ptx},
};
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::backend::NonceChunk;
use crate::backend::{
    pow_hash_from_last_block_words, AssignmentSemantics, BackendCallStatus, BackendCapabilities,
    BackendEvent, BackendExecutionModel, BackendInstanceId, BackendTelemetry, BenchBackend,
    DeadlineSupport, KernelBenchSample, MiningSolution, PowBackend, PreemptionGranularity,
    WorkAssignment, WorkTemplate,
};
use crate::types::hash_meets_target;
/// How long a starting backend waits for the first template before an
/// Argon2 autotune.
const NETWORK_POW_WAIT: Duration = Duration::from_secs(30);
const BACKEND_NAME: &str = "nvidia";

// Check the dependency's own library search before its infallible accessor can
// panic. Successful checks are cached; a failed check can be retried next start.
/// Whether the CUDA NVRTC library loads. Without it only the prebuilt
/// BLAKE2b kernel can run.
fn nvrtc_available() -> bool {
    static AVAILABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *AVAILABLE.get_or_init(|| unsafe {
        cudarc::driver::sys::is_culib_present() && nvrtc_sys::is_culib_present()
    })
}

/// The driver checks of `require_cuda_libraries`, for the BLAKE2b engine,
/// which falls back to prebuilt PTX when NVRTC is missing.
fn require_cuda_driver() -> Result<()> {
    if nvrtc_available() {
        return require_cuda_libraries();
    }
    let driver = unsafe { cudarc::driver::sys::is_culib_present() };
    crate::diagnostics::nvidia_software(cuda_software_versions(driver, false));
    check_cuda_libraries(|| driver, || true)
}

/// Records and reports, once per process, that NVRTC is missing and this
/// GPU mines BLAKE2b only.
fn note_nvrtc_unavailable(device_index: u32) {
    use seine_telemetry::{
        DiagnosticCode, DiagnosticKind, DiagnosticOperation, FailureDetail, FailureReason,
    };
    static NOTED: std::sync::Once = std::sync::Once::new();
    NOTED.call_once(|| {
        crate::diagnostics::record_failure(
            DiagnosticCode::Warning,
            DiagnosticKind::Compilation,
            Some(BACKEND_NAME),
            FailureDetail::new(
                FailureReason::NvrtcUnavailable,
                DiagnosticOperation::BackendStartup,
            ),
        );
    });
    crate::miner::ui::warn(
        "NVIDIA",
        format!(
            "GPU {device_index}: CUDA NVRTC library not found; this GPU mines BLAKE2b blocks only. Install the CUDA NVRTC libraries to mine Argon2 on it"
        ),
    );
}

fn require_cuda_libraries() -> Result<()> {
    static READY: std::sync::OnceLock<seine_telemetry::NvidiaSoftware> = std::sync::OnceLock::new();
    if let Some(software) = READY.get() {
        crate::diagnostics::nvidia_software(software.clone());
        return Ok(());
    }
    let driver = unsafe { cudarc::driver::sys::is_culib_present() };
    let nvrtc = driver && unsafe { nvrtc_sys::is_culib_present() };
    let software = cuda_software_versions(driver, nvrtc);
    crate::diagnostics::nvidia_software(software.clone());
    check_cuda_libraries(|| driver, || nvrtc)?;
    let _ = READY.set(software);
    Ok(())
}

// Query optional symbols from the SAME loaded libraries used by mining. A missing
// version symbol must never trigger the dependency's infallible/panicking wrapper.
// No context, device enumeration, NVML or external process is needed.
fn cuda_software_versions(driver: bool, nvrtc: bool) -> seine_telemetry::NvidiaSoftware {
    let mut software = seine_telemetry::NvidiaSoftware::default();
    unsafe {
        if driver {
            type Version = unsafe extern "C" fn(*mut i32) -> cudarc::driver::sys::CUresult;
            if let Ok(query) = cudarc::driver::sys::culib().get::<Version>(b"cuDriverGetVersion\0")
            {
                let mut version = 0;
                if query(&mut version) == cudarc::driver::sys::CUresult::CUDA_SUCCESS {
                    software.driver_cuda_version = valid_cuda_version(version);
                }
            }
        }
        if nvrtc {
            type Version = unsafe extern "C" fn(*mut i32, *mut i32) -> nvrtc_sys::nvrtcResult;
            if let Ok(query) = nvrtc_sys::culib().get::<Version>(b"nvrtcVersion\0") {
                let (mut major, mut minor) = (0, 0);
                if query(&mut major, &mut minor) == nvrtc_sys::nvrtcResult::NVRTC_SUCCESS
                    && (1..=999).contains(&major)
                    && (0..=99).contains(&minor)
                {
                    software.nvrtc_version = valid_cuda_version(major * 1000 + minor * 10);
                }
            }
        }
    }
    software
}

fn valid_cuda_version(version: i32) -> Option<u32> {
    ((1000..=999_990).contains(&version) && version % 10 == 0).then_some(version as u32)
}

fn check_cuda_libraries(driver: impl FnOnce() -> bool, nvrtc: impl FnOnce() -> bool) -> Result<()> {
    use seine_telemetry::{
        DiagnosticCode, DiagnosticKind, DiagnosticOperation, FailureDetail, FailureReason,
    };
    let failure = if !driver() {
        Some((
            FailureReason::NvidiaDriverUnavailable,
            DiagnosticKind::Driver,
            "NVIDIA driver library is unavailable; install or repair the NVIDIA driver",
        ))
    } else if !nvrtc() {
        Some((FailureReason::NvrtcUnavailable, DiagnosticKind::Compilation,
            "CUDA NVRTC library is unavailable; install the CUDA Toolkit or restore the bundled NVRTC libraries"))
    } else {
        None
    };
    if let Some((reason, kind, message)) = failure {
        crate::diagnostics::record_failure(
            DiagnosticCode::BackendError,
            kind,
            Some(BACKEND_NAME),
            FailureDetail::new(reason, DiagnosticOperation::BackendStartup),
        );
        bail!("{message}");
    }
    Ok(())
}
#[cfg(test)]
mod library_tests {
    use super::*;

    #[test]
    #[ignore = "requires scripts/test_cuda_versions.py loader isolation"]
    fn software_version_probe() {
        let mode = std::env::var("SEINE_TEST_CUDA_VERSIONS").unwrap();
        let guard = crate::diagnostics::install(crate::diagnostics::test_context());
        require_cuda_libraries().unwrap();
        let software = guard.0.snapshot(false).nvidia_software;
        if mode == "present" {
            assert_eq!(
                software,
                Some(seine_telemetry::NvidiaSoftware {
                    driver_cuda_version: Some(12080),
                    nvrtc_version: Some(12090),
                })
            );
            // Cached library readiness must still populate each new session.
            let next = crate::diagnostics::install(crate::diagnostics::test_context());
            require_cuda_libraries().unwrap();
            assert_eq!(next.0.checkpoint().nvidia_software, software);
        } else {
            assert_eq!(software, None);
        }
        assert!(guard.0.snapshot(true).events.is_empty());
    }

    #[test]
    #[ignore = "requires scripts/test_cuda_unused_symbol.py loader isolation"]
    fn unused_driver_symbol_probe() {
        assert_eq!(
            std::env::var("SEINE_TEST_UNUSED_CUDA_SYMBOL").as_deref(),
            Ok("1")
        );
        let guard = crate::diagnostics::install(crate::diagnostics::test_context());
        assert!(unsafe { cudarc::driver::sys::is_culib_present() });
        // The fake driver's only callable entry point returns NO_DEVICE. Never
        // touch a real GPU; exercise the dependency's library/symbol resolution.
        let error = cudarc::driver::result::init().expect_err("fake driver must return NO_DEVICE");
        assert_eq!(error.0, cudarc::driver::sys::CUresult::CUDA_ERROR_NO_DEVICE);
        assert!(
            guard.0.snapshot(true).events.is_empty(),
            "unused symbols must not panic"
        );
    }

    // Run in a fresh subprocess with the test shim blocking only the named library.
    #[test]
    #[ignore = "requires scripts/test_cuda_missing_library.py loader isolation"]
    fn missing_library_probe() {
        let reason = match std::env::var("SEINE_TEST_MISSING_CUDA_LIBRARY")
            .unwrap()
            .as_str()
        {
            "driver" => seine_telemetry::FailureReason::NvidiaDriverUnavailable,
            "nvrtc" => seine_telemetry::FailureReason::NvrtcUnavailable,
            _ => panic!("invalid test library"),
        };
        let guard = crate::diagnostics::install(crate::diagnostics::test_context());
        assert!(require_cuda_libraries().is_err());
        let events = guard.0.snapshot(true).events;
        assert_eq!(
            events.len(),
            1,
            "expected one typed error and no loader panic"
        );
        assert_eq!(events[0].detail.as_ref().unwrap().reason, reason);
    }

    #[test]
    fn missing_libraries_return_typed_errors_without_panicking() {
        let _lock = crate::diagnostics::TEST_LOCK.lock().unwrap();
        let guard = crate::diagnostics::install(crate::diagnostics::test_context());
        assert!(
            check_cuda_libraries(|| false, || panic!("must not probe NVRTC without driver"))
                .is_err()
        );
        assert!(check_cuda_libraries(|| true, || false).is_err());
        assert!(check_cuda_libraries(|| true, || true).is_ok());
        let snapshot = guard.0.snapshot(true);
        assert_eq!(snapshot.events.len(), 2);
        let reasons: Vec<_> = snapshot
            .events
            .iter()
            .map(|e| {
                assert_eq!(e.code, seine_telemetry::DiagnosticCode::BackendError);
                assert_eq!(e.backend.as_deref(), Some("nvidia"));
                e.detail.as_ref().unwrap().reason
            })
            .collect();
        assert!(reasons.contains(&seine_telemetry::FailureReason::NvidiaDriverUnavailable));
        assert!(reasons.contains(&seine_telemetry::FailureReason::NvrtcUnavailable));
    }
}

mod blake2b;

const CUDA_KERNEL_SRC: &str = include_str!("nvidia_kernel.cu");
/// `CUDA_KERNEL_SRC` built as PTX for the network's Argon2 parameters, one per
/// group of `arch_fill_defines`, loaded through the driver when NVRTC is not
/// installed. The Blackwell file targets compute_100 and needs a CUDA 12.8
/// driver, as those GPUs do. Regenerate them with `scripts/gen_argon2_ptx.py`.
const ARGON2_PTX_CC6: &str = include_str!("nvidia/argon2_cc6.ptx");
const ARGON2_PTX_CC7: &str = include_str!("nvidia/argon2_cc7.ptx");
const ARGON2_PTX_CC8: &str = include_str!("nvidia/argon2_cc8.ptx");
const ARGON2_PTX_CC10: &str = include_str!("nvidia/argon2_cc10.ptx");
const PREBUILT_ARGON2_M_BLOCKS: u32 = 1 << 21;

/// The prebuilt Argon2 PTX for a device of compute capability `cc_major`, when
/// the PoW parameters are the ones it was built for.
fn prebuilt_argon2_ptx(cc_major: u32, m_blocks: u32, t_cost: u32) -> Option<&'static str> {
    if m_blocks != PREBUILT_ARGON2_M_BLOCKS || t_cost != 1 {
        return None;
    }
    match cc_major {
        6 => Some(ARGON2_PTX_CC6),
        7 => Some(ARGON2_PTX_CC7),
        8 | 9 => Some(ARGON2_PTX_CC8),
        major if major >= 10 => Some(ARGON2_PTX_CC10),
        _ => None,
    }
}

/// Fill-loop variant per architecture, as NVRTC defines: GTX 10 series (6.x)
/// loads data-independent references two blocks ahead into registers; GTX 16
/// and RTX 20 series (7.x) do the same with blocks held by rows, two blocks per
/// loop iteration; RTX 30 and 40 series (8.x) load them one block ahead, hold
/// blocks by rows and run two blocks per iteration; RTX 50 series and other
/// Blackwell parts (10.x and newer) hold blocks by rows, use the kernel's async
/// reference ring and run two blocks per iteration. All but Blackwell inline
/// the rebuild of unstored blocks; GTX 10 to RTX 20 address each block from one
/// base pointer, RTX 30 and 40 in the data-dependent half. Keep `scripts/gen_argon2_ptx.py` in step.
fn arch_fill_defines(cc_major: u32) -> &'static [&'static str] {
    match cc_major {
        6 => &[
            "SEINE_REF_PREFETCH_DEPTH=2U",
            "SEINE_INLINE_REBUILD=1",
            "SEINE_BLOCK_BASE_POINTER=1",
        ],
        7 => &[
            "SEINE_REF_PREFETCH_DEPTH=2U",
            "SEINE_ROW_LAYOUT=1",
            "SEINE_LOOP_UNROLL=2",
            "SEINE_INLINE_REBUILD=1",
            "SEINE_BLOCK_BASE_POINTER=1",
        ],
        8 | 9 => &[
            "SEINE_REF_RING=1U",
            "SEINE_ROW_LAYOUT=1",
            "SEINE_LOOP_UNROLL=2",
            "SEINE_INLINE_REBUILD=1",
            "SEINE_BLOCK_BASE_POINTER=2",
        ],
        major if major >= 10 => &["SEINE_ROW_LAYOUT=1", "SEINE_LOOP_UNROLL=2"],
        _ => &[],
    }
}

/// `arch_fill_defines` plus SEINE_NVIDIA_DEFINES ("NAME=1,OTHER=0"), whose
/// entries replace an architecture default of the same name.
fn fill_define_options(cc_major: u32) -> Vec<String> {
    let extra = std::env::var("SEINE_NVIDIA_DEFINES").unwrap_or_default();
    let extra = extra
        .split(',')
        .map(str::trim)
        .filter(|define| !define.is_empty())
        .collect::<Vec<_>>();
    let name = |define: &str| define.split('=').next().unwrap_or(define).to_string();
    arch_fill_defines(cc_major)
        .iter()
        .copied()
        .filter(|define| !extra.iter().any(|e| name(e) == name(define)))
        .chain(extra.iter().copied())
        .map(|define| format!("-D{define}"))
        .collect()
}

/// Whether this process builds the Argon2 kernel with NVRTC; otherwise it
/// loads the prebuilt PTX.
fn argon2_uses_nvrtc() -> bool {
    #[cfg(test)]
    if tests::force_prebuilt_argon2() {
        return false;
    }
    nvrtc_available()
}
const MAX_LANES_HARD_LIMIT: usize = 1024;
const ASSIGN_CHANNEL_CAPACITY: usize = 256;
const CONTROL_CHANNEL_CAPACITY: usize = 32;
const EVENT_SEND_WAIT: Duration = Duration::from_millis(5);
const KERNEL_THREADS: u32 = 32;
const SEED_KERNEL_THREADS: u32 = 64;
const DEFAULT_WARPS_PER_BLOCK: u32 = 1;
const DEFAULT_NVIDIA_MAX_RREGCOUNT: u32 = 240;
const NVIDIA_AUTOTUNE_SCHEMA_VERSION: u32 = 10;
const NVIDIA_CUBIN_CACHE_SCHEMA_VERSION: u32 = 2;
const NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_AMPERE_PLUS: &[u32] =
    &[240, 224, 208, 192, 176, 160, 144, 128];
const NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_LEGACY: &[u32] =
    &[224, 208, 192, 176, 160, 144, 128, 112, 96];
// RTX 5090 shortlist; other Blackwell devices keep the full Ampere+ set.
const NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_RTX_5090: &[u32] = &[240, 224, 208];
// Retained for reference; the staged autotune no longer iterates this axis.
const _NVIDIA_AUTOTUNE_LOOP_UNROLL_CANDIDATES: &[bool] = &[false];
const DEFAULT_NVIDIA_AUTOTUNE_SAMPLES: u32 = 2;
// Largest share of a lane's blocks left unstored to fit one more lane: the
// kernel keeps unstored blocks in the second half of the lane.
const NVIDIA_MAX_SKIPPED_LANE_FRACTION: f64 = 0.25;
// Lane strides stay a multiple of 2 MiB.
const NVIDIA_LANE_STRIDE_ALIGN_BLOCKS: u32 = 2048;
const NVIDIA_MEMORY_RESERVE_MIB_FLOOR: u64 = 64;
const NVIDIA_MEMORY_RESERVE_RATIO_DENOM: u64 = 64;
const NVIDIA_AUTOTUNE_MEMORY_BUCKET_MIB: u64 = 512;
const DEFAULT_DISPATCH_ITERS_PER_LANE: u64 = 1 << 21;
const DEFAULT_ALLOCATION_ITERS_PER_LANE: u64 = 1 << 21;
const DEFAULT_HASHES_PER_LAUNCH_PER_LANE: u32 = 2;
const BLACKWELL_DEFAULT_HASHES_PER_LAUNCH_PER_LANE: u32 = 1;
const BLACKWELL_AUTOTUNE_SCORE_TIE_FRAC: f64 = 0.02;
const BLACKWELL_AUTOTUNE_PREFERRED_RREGCOUNT: u32 = 208;
const DEFAULT_CANCEL_CHECK_BLOCK_INTERVAL: u32 = 64;
const AMPERE_PLUS_CANCEL_CHECK_BLOCK_INTERVAL: u32 = 128;
const ADAPTIVE_DEPTH_PRESSURE_CONTROL_BONUS: u32 = 2;
const ADAPTIVE_DEPTH_PRESSURE_REPLACE_BONUS: u32 = 1;
const ADAPTIVE_DEPTH_PRESSURE_DECAY_PER_BATCH: u32 = 1;
const ADAPTIVE_DEPTH_DEADLINE_SLACK_FRAC: f64 = 0.75;
const ADAPTIVE_DEPTH_DEADLINE_LAUNCH_HEADROOM_FRAC: f64 = 0.95;
const ADAPTIVE_DEPTH_HASH_EMA_ALPHA: f64 = 0.25;
const CUDA_TRANSIENT_RETRY_COUNT: u32 = 3;
const CUDA_TRANSIENT_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_millis(2000),
];

fn default_hashes_per_launch_per_lane() -> u32 {
    DEFAULT_HASHES_PER_LAUNCH_PER_LANE
}

fn effective_hashes_per_launch_per_lane_cap(
    requested_cap: u32,
    user_override: bool,
    compute_cap_major: u32,
) -> u32 {
    let requested_cap = requested_cap.max(1);
    if !user_override
        && requested_cap == DEFAULT_HASHES_PER_LAUNCH_PER_LANE
        && compute_cap_major >= 12
    {
        BLACKWELL_DEFAULT_HASHES_PER_LAUNCH_PER_LANE
    } else {
        requested_cap
    }
}

fn select_cancel_check_block_interval(cc_major: u32) -> u32 {
    if cc_major >= 8 {
        AMPERE_PLUS_CANCEL_CHECK_BLOCK_INTERVAL
    } else {
        DEFAULT_CANCEL_CHECK_BLOCK_INTERVAL
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NvidiaDeviceInfo {
    pub(crate) index: u32,
    pub(crate) name: String,
    pub(crate) memory_total_mib: u64,
    pub(crate) memory_free_mib: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct NvidiaKernelTuning {
    max_rregcount: u32,
    block_loop_unroll: bool,
    hashes_per_launch_per_lane: u32,
    max_lanes_hint: Option<usize>,
}

impl Default for NvidiaKernelTuning {
    fn default() -> Self {
        Self {
            max_rregcount: DEFAULT_NVIDIA_MAX_RREGCOUNT,
            block_loop_unroll: false,
            hashes_per_launch_per_lane: DEFAULT_HASHES_PER_LAUNCH_PER_LANE,
            max_lanes_hint: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct NvidiaAutotuneKey {
    device_name: String,
    memory_total_mib: u64,
    #[serde(default)]
    memory_budget_mib: u64,
    #[serde(default)]
    lane_capacity_tier: u32,
    compute_cap_major: u32,
    compute_cap_minor: u32,
    m_cost_kib: u32,
    t_cost: u32,
    kernel_threads: u32,
    hashes_per_launch_per_lane_cap: u32,
    #[serde(default)]
    os: String,
    #[serde(default)]
    kernel_version: String,
    #[serde(default)]
    runtime_environment: String,
    #[serde(default)]
    build_fingerprint: String,
    #[serde(default)]
    cuda_kernel_fingerprint: String,
    #[serde(default)]
    nvrtc_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NvidiaAutotuneRecord {
    key: NvidiaAutotuneKey,
    max_rregcount: u32,
    #[serde(default)]
    block_loop_unroll: bool,
    #[serde(default = "default_hashes_per_launch_per_lane")]
    hashes_per_launch_per_lane: u32,
    #[serde(default)]
    max_lanes_hint: Option<usize>,
    measured_hps: f64,
    autotune_secs: u64,
    #[serde(default)]
    autotune_samples: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    candidate_trace: Vec<NvidiaAutotuneCandidateTrace>,
    #[serde(default)]
    autotune_elapsed_millis: u64,
    timestamp_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct NvidiaAutotuneCandidateTrace {
    tuning: NvidiaKernelTuning,
    samples: Vec<NvidiaAutotuneSampleScore>,
    failed_samples: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    counted_median_hps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    counted_mean_hps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    throughput_median_hps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    throughput_mean_hps: Option<f64>,
    elapsed_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NvidiaAutotuneCache {
    schema_version: u32,
    records: Vec<NvidiaAutotuneRecord>,
}

struct NvidiaShared {
    event_sink: Arc<RwLock<Option<Sender<BackendEvent>>>>,
    dropped_events: AtomicU64,
    hashes: AtomicU64,
    active_lanes: AtomicU64,
    pending_work: AtomicU64,
    inflight_assignment_hashes: AtomicU64,
    inflight_assignment_started_at: Mutex<Option<Instant>>,
    completed_assignments: AtomicU64,
    completed_assignment_hashes: AtomicU64,
    completed_assignment_micros: AtomicU64,
    active_hashes_per_launch_per_lane: AtomicU32,
    cancel_requested: AtomicBool,
    error_emitted: AtomicBool,
    /// The worker's last batch was BLAKE2b (reported in telemetry).
    blake2b_active: AtomicBool,
}

#[derive(Debug, Clone, Copy)]
pub struct NvidiaBackendTuningOptions {
    pub max_rregcount_override: Option<u32>,
    pub max_lanes_override: Option<usize>,
    pub autotune_samples: u32,
    pub dispatch_iters_per_lane: Option<u64>,
    pub allocation_iters_per_lane: Option<u64>,
    pub hashes_per_launch_per_lane: u32,
    pub hashes_per_launch_per_lane_was_set: bool,
    pub fused_target_check: bool,
    pub adaptive_launch_depth: bool,
    pub enforce_template_stop: bool,
}

impl Default for NvidiaBackendTuningOptions {
    fn default() -> Self {
        Self {
            max_rregcount_override: None,
            max_lanes_override: None,
            autotune_samples: DEFAULT_NVIDIA_AUTOTUNE_SAMPLES,
            dispatch_iters_per_lane: None,
            allocation_iters_per_lane: None,
            hashes_per_launch_per_lane: DEFAULT_HASHES_PER_LAUNCH_PER_LANE,
            hashes_per_launch_per_lane_was_set: false,
            fused_target_check: false,
            adaptive_launch_depth: true,
            enforce_template_stop: false,
        }
    }
}

impl NvidiaShared {
    fn new() -> Self {
        Self {
            event_sink: Arc::new(RwLock::new(None)),
            dropped_events: AtomicU64::new(0),
            hashes: AtomicU64::new(0),
            active_lanes: AtomicU64::new(0),
            pending_work: AtomicU64::new(0),
            inflight_assignment_hashes: AtomicU64::new(0),
            inflight_assignment_started_at: Mutex::new(None),
            completed_assignments: AtomicU64::new(0),
            completed_assignment_hashes: AtomicU64::new(0),
            completed_assignment_micros: AtomicU64::new(0),
            active_hashes_per_launch_per_lane: AtomicU32::new(DEFAULT_HASHES_PER_LAUNCH_PER_LANE),
            cancel_requested: AtomicBool::new(false),
            error_emitted: AtomicBool::new(false),
            blake2b_active: AtomicBool::new(false),
        }
    }
}

#[derive(Clone)]
struct CudaInterruptController {
    stream: Arc<CudaStream>,
    cancel_flag: Arc<Mutex<CudaSlice<u32>>>,
}

impl CudaInterruptController {
    fn signal_cancel(&self) -> Result<()> {
        let mut flag = self
            .cancel_flag
            .lock()
            .map_err(|_| anyhow!("nvidia cancel flag lock poisoned"))?;
        let mut view = flag
            .try_slice_mut(0..1)
            .ok_or_else(|| anyhow!("failed to slice CUDA cancel flag"))?;
        self.stream
            .memcpy_htod(&[1u32], &mut view)
            .map_err(cuda_driver_err)?;
        Ok(())
    }
}

struct NvidiaWorker {
    assignment_tx: Sender<WorkerCommand>,
    control_tx: Sender<WorkerCommand>,
    handle: JoinHandle<()>,
}

enum WorkerCommand {
    Assign(WorkAssignment),
    AssignBatch(Vec<WorkAssignment>),
    Cancel(Sender<Result<()>>),
    Fence(Sender<Result<()>>),
    Stop,
}

struct ActiveAssignment {
    work: WorkAssignment,
    next_nonce: u64,
    remaining: u64,
    hashes_done: u64,
    hash_micros_ema: Option<f64>,
    started_at: Instant,
}

impl ActiveAssignment {
    fn new(work: WorkAssignment) -> Self {
        Self {
            next_nonce: work.nonce_chunk.start_nonce,
            remaining: work.nonce_chunk.nonce_count,
            hashes_done: 0,
            hash_micros_ema: None,
            started_at: Instant::now(),
            work,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PendingControlKind {
    Cancel,
    Fence,
}

impl PendingControlKind {
    fn label(self) -> &'static str {
        match self {
            Self::Cancel => "cancel",
            Self::Fence => "fence",
        }
    }
}

struct PendingControlAck {
    kind: PendingControlKind,
    ack_rx: Receiver<Result<()>>,
}

#[derive(Default)]
struct NonblockingControlState {
    pending: Option<PendingControlAck>,
    /// An acknowledgement received while waiting, kept until the caller
    /// polls for that control again; cleared when new work is assigned.
    completed: Option<(PendingControlKind, Result<()>)>,
}

struct FillBatchResult {
    hashes_done: usize,
    /// Every hash in the batch that meets the target, in nonce order.
    solutions: Vec<(u64, [u8; 32])>,
}

struct CudaArgon2Engine {
    stream: Arc<CudaStream>,
    interrupt_stream: Arc<CudaStream>,
    kernel_nonfused: CudaFunction,
    kernel_fused_target: Option<CudaFunction>,
    seed_kernel: CudaFunction,
    m_blocks: u32,
    m_cost_kib: u32,
    t_cost: u32,
    stored_blocks: u32,
    skip_from: u32,
    max_lanes: usize,
    hashes_per_launch_per_lane: usize,
    warps_per_block: u32,
    lane_memory: CudaSlice<u64>,
    seed_blocks: CudaSlice<u64>,
    last_blocks: CudaSlice<u64>,
    ref_table: CudaSlice<u32>,
    nonce_input: CudaSlice<u64>,
    header_base_input: CudaSlice<u8>,
    target_input: CudaSlice<u8>,
    cancel_flag: Arc<Mutex<CudaSlice<u32>>>,
    completed_iters: CudaSlice<u32>,
    found_index: CudaSlice<u32>,
    host_completed_iters: [u32; 1],
    host_found_index: [u32; 1],
    host_last_blocks: Vec<u64>,
    fused_target_check: bool,
    cached_header_base: Vec<u8>,
    cached_target: Option<[u8; POW_OUTPUT_LEN]>,
}

fn header_base_upload_needed(cached: &[u8], next: &[u8]) -> bool {
    cached != next
}

fn target_upload_needed(
    cached: Option<&[u8; POW_OUTPUT_LEN]>,
    next: &[u8; POW_OUTPUT_LEN],
) -> bool {
    cached != Some(next)
}

impl CudaArgon2Engine {
    fn new(
        device_index: u32,
        memory_total_mib: u64,
        memory_free_mib: Option<u64>,
        tuning: NvidiaKernelTuning,
        max_lanes_override: Option<usize>,
        hashes_per_launch_per_lane: u32,
        fused_target_check: bool,
        cubin_cache_dir: &Path,
    ) -> Result<Self> {
        let use_nvrtc = argon2_uses_nvrtc();
        if use_nvrtc {
            require_cuda_libraries()?;
        } else {
            require_cuda_driver()?;
        }
        use_blocking_sync(device_index);
        let ctx = CudaContext::new(device_index as usize).map_err(|err| {
            anyhow!("failed to open CUDA context on device {device_index}: {err:?}")
        })?;
        let stream = ctx.default_stream();
        let interrupt_stream = ctx.new_stream().unwrap_or_else(|_| stream.clone());

        let (cc_major, cc_minor) = ctx
            .compute_capability()
            .map_err(|err| anyhow!("failed to query compute capability: {err:?}"))?;
        let cc_major_u32 = u32::try_from(cc_major.max(0)).unwrap_or(0);

        let params = pow_params()
            .map_err(|err| anyhow!("invalid Argon2 parameters for CUDA backend: {err}"))?;
        let m_blocks = params.block_count() as u32;
        let m_cost_kib = params.m_cost();
        let t_cost = params.t_cost();

        let nvrtc_common_options = vec![
            "--std=c++14".to_string(),
            "--restrict".to_string(),
            "--use_fast_math".to_string(),
            "--ftz=true".to_string(),
            "--fmad=true".to_string(),
            format!("-DSEINE_FIXED_M_BLOCKS={}U", m_blocks),
            format!("-DSEINE_FIXED_T_COST={}U", t_cost),
            format!(
                "-DSEINE_CANCEL_CHECK_BLOCK_INTERVAL={}U",
                select_cancel_check_block_interval(cc_major_u32)
            ),
            format!("-DSEINE_WARPS_PER_BLOCK={}U", DEFAULT_WARPS_PER_BLOCK),
            format!("--maxrregcount={}", tuning.max_rregcount.max(1)),
        ];
        let mut nvrtc_common_options = nvrtc_common_options;
        nvrtc_common_options.extend(fill_define_options(cc_major_u32));
        let nvrtc_cubin_options = {
            let mut options = nvrtc_common_options.clone();
            options.push(format!("--gpu-architecture=sm_{}{}", cc_major, cc_minor));
            options
        };
        let nvrtc_ptx_options = {
            let mut options = nvrtc_common_options;
            options.push(format!(
                "--gpu-architecture=compute_{}{}",
                cc_major, cc_minor
            ));
            options
        };
        let program_name = "seine_argon2id_fill.cu";
        let nvrtc_version = nvrtc_compiler_identity();
        let cache_key = build_cubin_cache_key(
            CUDA_KERNEL_SRC,
            program_name,
            &nvrtc_cubin_options,
            &nvrtc_version,
        );
        let cache_path = cubin_cache_file_path(cubin_cache_dir, &cache_key);
        let module = if !use_nvrtc {
            let ptx = prebuilt_argon2_ptx(cc_major_u32, m_blocks, t_cost).ok_or_else(|| {
                anyhow!("no prebuilt Argon2 kernel for compute capability {cc_major}.{cc_minor}")
            })?;
            ctx.load_module(Ptx::from_src(ptx))
                .map_err(|err| anyhow!("failed to load prebuilt CUDA module: {err:?}"))?
        } else if let Some(cached) = load_cached_cubin(&cache_path) {
            match ctx.load_module(Ptx::from_binary(cached)) {
                Ok(module) => module,
                Err(err) => {
                    let _ = fs::remove_file(&cache_path);
                    let compiled = compile_cuda_module_with_nvrtc_fallback(
                        CUDA_KERNEL_SRC,
                        program_name,
                        &nvrtc_cubin_options,
                        &nvrtc_ptx_options,
                    )
                    .map_err(|compile_err| {
                        anyhow!(
                            "failed to compile CUDA module with NVRTC after cache load failure ({err:?}): {compile_err:#}"
                        )
                    })?;
                    if let NvrtcModuleArtifact::Cubin(cubin) = &compiled {
                        let _ = persist_cached_cubin(&cache_path, cubin);
                    }
                    match compiled {
                        NvrtcModuleArtifact::Cubin(cubin) => {
                            ctx.load_module(Ptx::from_binary(cubin))
                        }
                        NvrtcModuleArtifact::Ptx(ptx) => ctx.load_module(Ptx::from_src(ptx)),
                    }
                    .map_err(|load_err| anyhow!("failed to load CUDA module: {load_err:?}"))?
                }
            }
        } else {
            let compiled = compile_cuda_module_with_nvrtc_fallback(
                CUDA_KERNEL_SRC,
                program_name,
                &nvrtc_cubin_options,
                &nvrtc_ptx_options,
            )
            .map_err(|err| anyhow!("failed to compile CUDA module with NVRTC: {err:#}"))?;
            if let NvrtcModuleArtifact::Cubin(cubin) = &compiled {
                let _ = persist_cached_cubin(&cache_path, cubin);
            }
            match compiled {
                NvrtcModuleArtifact::Cubin(cubin) => ctx.load_module(Ptx::from_binary(cubin)),
                NvrtcModuleArtifact::Ptx(ptx) => ctx.load_module(Ptx::from_src(ptx)),
            }
            .map_err(|load_err| anyhow!("failed to load CUDA module: {load_err:?}"))?
        };
        let kernel_nonfused = module
            .load_function("argon2id_fill_kernel")
            .map_err(|err| anyhow!("failed to load CUDA kernel function: {err:?}"))?;
        // The prebuilt PTX leaves out the optional fused-target entry point.
        let kernel_fused_target = if use_nvrtc {
            Some(
                module
                    .load_function("argon2id_fill_kernel_fused_target")
                    .map_err(|err| anyhow!("failed to load CUDA fused kernel function: {err:?}"))?,
            )
        } else {
            None
        };
        let seed_kernel = module
            .load_function("build_seed_blocks_kernel")
            .map_err(|err| anyhow!("failed to load CUDA seed kernel function: {err:?}"))?;
        let touch_kernel = module
            .load_function("touch_lane_memory_kernel")
            .map_err(|err| anyhow!("failed to load CUDA probe kernel function: {err:?}"))?;
        let ref_table_kernel = module
            .load_function("build_ref_table_kernel")
            .map_err(|err| anyhow!("failed to load CUDA reference-table kernel: {err:?}"))?;
        let ref_table = build_cuda_ref_table(&stream, &ref_table_kernel, m_blocks, t_cost)
            .map_err(|err| anyhow!("failed to build CUDA reference table: {err:?}"))?;

        let lane_bytes = CPU_LANE_MEMORY_BYTES.max(u64::from(m_blocks) * 1024);
        // Free memory as this context sees it, after its own allocations.
        let context_free_mib = ctx
            .mem_get_info()
            .ok()
            .map(|(free, _)| (free as u64) / (1024 * 1024));
        let free_mib = match (memory_free_mib, context_free_mib) {
            (Some(reported), Some(context)) => Some(reported.min(context)),
            (reported, context) => context.or(reported),
        };
        let memory_budget_mib = derive_memory_budget_mib(memory_total_mib, free_mib);
        let memory_budget_bytes = memory_budget_mib.saturating_mul(1024 * 1024);
        let usable_bytes = memory_budget_bytes;
        let by_memory = usize::try_from((usable_bytes / lane_bytes).max(1)).unwrap_or(1);
        let max_lanes_budget = by_memory.max(1).min(MAX_LANES_HARD_LIMIT);
        let skip_supported = t_cost == 1 && !fill_v1_requested();
        #[allow(unused_mut)]
        let mut lane_plans = plan_lane_storage(
            m_blocks,
            usable_bytes,
            max_lanes_budget,
            max_lanes_override,
            skip_supported,
        );
        #[cfg(test)]
        if let Some(blocks) = tests::forced_stored_blocks() {
            lane_plans = vec![(max_lanes_override.unwrap_or(1), blocks)];
        }
        let hashes_per_launch_per_lane = hashes_per_launch_per_lane.max(1) as usize;

        let mut selected = None;
        for (lanes, stored_blocks) in lane_plans {
            let lane_stride_words = u64::from(stored_blocks).saturating_mul(128);
            match try_allocate_cuda_buffers(
                &stream,
                stored_blocks,
                lanes,
                hashes_per_launch_per_lane,
            ) {
                Ok((
                    mut lane_memory,
                    seed_blocks,
                    last_blocks,
                    nonce_input,
                    header_base_input,
                    target_input,
                    cancel_flag,
                    completed_iters,
                    found_index,
                )) => {
                    match probe_cuda_lane_memory(
                        &stream,
                        &touch_kernel,
                        &mut lane_memory,
                        lane_stride_words,
                        lanes,
                    ) {
                        Ok(()) => {
                            selected = Some((
                                lanes,
                                stored_blocks,
                                (
                                    lane_memory,
                                    seed_blocks,
                                    last_blocks,
                                    nonce_input,
                                    header_base_input,
                                    target_input,
                                    cancel_flag,
                                    completed_iters,
                                    found_index,
                                ),
                            ));
                            break;
                        }
                        Err(err) if lanes > 1 && is_cuda_oom_error(&err) => {
                            continue;
                        }
                        Err(err) => {
                            return Err(anyhow!(
                                "failed to validate CUDA buffers for {lanes} lanes on device {device_index}: {err:?}"
                            ));
                        }
                    }
                }
                Err(err) if lanes > 1 && is_cuda_oom_error(&err) => {
                    continue;
                }
                Err(err) => {
                    return Err(anyhow!(
                        "failed to allocate CUDA buffers for {lanes} lanes on device {device_index}: {err:?}"
                    ));
                }
            }
        }

        let (
            max_lanes,
            stored_blocks,
            (
                lane_memory,
                seed_blocks,
                last_blocks,
                nonce_input,
                header_base_input,
                target_input,
                cancel_flag,
                completed_iters,
                found_index,
            ),
        ) = selected
            .ok_or_else(|| anyhow!("failed to allocate CUDA buffers for any lane count"))?;

        Ok(Self {
            stream,
            interrupt_stream,
            kernel_nonfused,
            kernel_fused_target,
            seed_kernel,
            m_blocks,
            m_cost_kib,
            t_cost,
            stored_blocks,
            skip_from: m_blocks - 2 * (m_blocks - stored_blocks),
            max_lanes,
            hashes_per_launch_per_lane,
            warps_per_block: DEFAULT_WARPS_PER_BLOCK,
            lane_memory,
            seed_blocks,
            last_blocks,
            ref_table,
            nonce_input,
            header_base_input,
            target_input,
            cancel_flag: Arc::new(Mutex::new(cancel_flag)),
            completed_iters,
            found_index,
            host_completed_iters: [0u32; 1],
            host_found_index: [u32::MAX; 1],
            host_last_blocks: Vec::new(),
            fused_target_check,
            cached_header_base: Vec::new(),
            cached_target: None,
        })
    }

    fn max_lanes(&self) -> usize {
        self.max_lanes
    }

    fn max_hashes_per_launch_per_lane(&self) -> usize {
        self.hashes_per_launch_per_lane.max(1)
    }

    fn max_hashes_per_launch(&self) -> usize {
        self.max_lanes
            .saturating_mul(self.hashes_per_launch_per_lane)
            .max(1)
    }

    fn interrupt_controller(&self) -> CudaInterruptController {
        CudaInterruptController {
            stream: self.interrupt_stream.clone(),
            cancel_flag: Arc::clone(&self.cancel_flag),
        }
    }

    fn run_fill_batch(
        &mut self,
        header_base: &[u8],
        nonces: &[u64],
        target: Option<&[u8; POW_OUTPUT_LEN]>,
    ) -> Result<FillBatchResult> {
        self.run_fill_batch_preserving_candidate(header_base, nonces, target, None)
    }

    fn run_fill_batch_preserving_candidate(
        &mut self,
        header_base: &[u8],
        nonces: &[u64],
        target: Option<&[u8; POW_OUTPUT_LEN]>,
        network_target: Option<&[u8; POW_OUTPUT_LEN]>,
    ) -> Result<FillBatchResult> {
        let header_len = blocknet_pow_spec::validate_header_length(header_base.len())
            .map_err(|reason| anyhow!(reason))?;

        if nonces.is_empty() {
            return Ok(FillBatchResult {
                hashes_done: 0,
                solutions: Vec::new(),
            });
        }

        let lanes_active = self.max_lanes.min(nonces.len()).max(1);
        let max_hashes = lanes_active
            .saturating_mul(self.hashes_per_launch_per_lane)
            .max(1);
        if nonces.len() > max_hashes {
            bail!(
                "batch size {} exceeds configured CUDA launch capacity {} (lanes={} hashes_per_launch_per_lane={})",
                nonces.len(),
                max_hashes,
                lanes_active,
                self.hashes_per_launch_per_lane
            );
        }

        let requested_hashes = nonces.len();
        if header_base_upload_needed(&self.cached_header_base, header_base) {
            {
                let mut header_view = self
                    .header_base_input
                    .try_slice_mut(0..header_base.len())
                    .ok_or_else(|| anyhow!("failed to slice CUDA header buffer"))?;
                self.stream
                    .memcpy_htod(header_base, &mut header_view)
                    .map_err(cuda_driver_err)?;
            }
            self.cached_header_base.clear();
            self.cached_header_base.extend_from_slice(header_base);
        }
        {
            let mut nonce_view = self
                .nonce_input
                .try_slice_mut(0..requested_hashes)
                .ok_or_else(|| anyhow!("failed to slice CUDA nonce buffer"))?;
            self.stream
                .memcpy_htod(nonces, &mut nonce_view)
                .map_err(cuda_driver_err)?;
        }
        {
            self.host_completed_iters[0] = 0;
            let mut completed_view = self
                .completed_iters
                .try_slice_mut(0..1)
                .ok_or_else(|| anyhow!("failed to slice CUDA completed-iter buffer"))?;
            self.stream
                .memcpy_htod(&self.host_completed_iters, &mut completed_view)
                .map_err(cuda_driver_err)?;
        }
        {
            let mut cancel_flag = self
                .cancel_flag
                .lock()
                .map_err(|_| anyhow!("nvidia cancel flag lock poisoned"))?;
            let mut cancel_view = cancel_flag
                .try_slice_mut(0..1)
                .ok_or_else(|| anyhow!("failed to slice CUDA cancel-flag buffer"))?;
            self.stream
                .memcpy_htod(&[0u32], &mut cancel_view)
                .map_err(cuda_driver_err)?;
        }
        let use_fused_target_check =
            target.is_some() && self.fused_target_check && self.kernel_fused_target.is_some();
        if let Some(target_hash) = target {
            if target_upload_needed(self.cached_target.as_ref(), target_hash) {
                {
                    let mut target_view = self
                        .target_input
                        .try_slice_mut(0..POW_OUTPUT_LEN)
                        .ok_or_else(|| anyhow!("failed to slice CUDA target buffer"))?;
                    self.stream
                        .memcpy_htod(target_hash, &mut target_view)
                        .map_err(cuda_driver_err)?;
                }
                self.cached_target = Some(*target_hash);
            }
            if use_fused_target_check {
                self.host_found_index[0] = u32::MAX;
                {
                    let mut found_view = self
                        .found_index
                        .try_slice_mut(0..1)
                        .ok_or_else(|| anyhow!("failed to slice CUDA found-index buffer"))?;
                    self.stream
                        .memcpy_htod(&self.host_found_index, &mut found_view)
                        .map_err(cuda_driver_err)?;
                }
            }
        }

        let lanes_u32 = u32::try_from(lanes_active).map_err(|_| anyhow!("lane count overflow"))?;
        let requested_hashes_u32 =
            u32::try_from(requested_hashes).map_err(|_| anyhow!("active hash count overflow"))?;
        let lane_launch_iters_u32 = u32::try_from(
            requested_hashes
                .saturating_add(lanes_active.saturating_sub(1))
                .saturating_div(lanes_active),
        )
        .map_err(|_| anyhow!("lane launch iteration overflow"))?;
        {
            let grid = requested_hashes_u32
                .saturating_add(SEED_KERNEL_THREADS.saturating_sub(1))
                .saturating_div(SEED_KERNEL_THREADS)
                .max(1);
            let cfg = LaunchConfig {
                grid_dim: (grid, 1, 1),
                block_dim: (SEED_KERNEL_THREADS, 1, 1),
                shared_mem_bytes: 0,
            };
            unsafe {
                let mut launch = self.stream.launch_builder(&self.seed_kernel);
                launch
                    .arg(&self.header_base_input)
                    .arg(&header_len)
                    .arg(&self.nonce_input)
                    .arg(&requested_hashes_u32)
                    .arg(&self.m_cost_kib)
                    .arg(&self.t_cost)
                    .arg(&self.seed_blocks);
                launch.launch(cfg).map_err(cuda_driver_err)?;
            }
        }

        {
            let wpb = self.warps_per_block;
            let grid_x = (lanes_u32 + wpb - 1) / wpb;
            let cfg = LaunchConfig {
                grid_dim: (grid_x, 1, 1),
                block_dim: (KERNEL_THREADS * wpb, 1, 1),
                shared_mem_bytes: 0,
            };

            let cancel_flag = self
                .cancel_flag
                .lock()
                .map_err(|_| anyhow!("nvidia cancel flag lock poisoned"))?;
            unsafe {
                if use_fused_target_check {
                    let fused = self
                        .kernel_fused_target
                        .as_ref()
                        .ok_or_else(|| anyhow!("fused-target CUDA kernel is not loaded"))?;
                    let mut launch = self.stream.launch_builder(fused);
                    launch
                        .arg(&self.seed_blocks)
                        .arg(&lanes_u32)
                        .arg(&requested_hashes_u32)
                        .arg(&lane_launch_iters_u32)
                        .arg(&self.m_blocks)
                        .arg(&self.t_cost)
                        .arg(&mut self.lane_memory)
                        .arg(&mut self.last_blocks)
                        .arg(&*cancel_flag)
                        .arg(&mut self.completed_iters)
                        .arg(&self.target_input)
                        .arg(&mut self.found_index)
                        .arg(&self.ref_table)
                        .arg(&self.stored_blocks)
                        .arg(&self.skip_from);
                    launch.launch(cfg).map_err(cuda_driver_err)?;
                } else {
                    let mut launch = self.stream.launch_builder(&self.kernel_nonfused);
                    launch
                        .arg(&self.seed_blocks)
                        .arg(&lanes_u32)
                        .arg(&requested_hashes_u32)
                        .arg(&lane_launch_iters_u32)
                        .arg(&self.m_blocks)
                        .arg(&self.t_cost)
                        .arg(&mut self.lane_memory)
                        .arg(&mut self.last_blocks)
                        .arg(&*cancel_flag)
                        .arg(&mut self.completed_iters)
                        .arg(&self.ref_table)
                        .arg(&self.stored_blocks)
                        .arg(&self.skip_from);
                    launch.launch(cfg).map_err(cuda_driver_err)?;
                }
            }
        }
        {
            let completed_view = self
                .completed_iters
                .try_slice(0..1)
                .ok_or_else(|| anyhow!("failed to slice CUDA completed-iter buffer"))?;
            self.stream
                .memcpy_dtoh(&completed_view, &mut self.host_completed_iters)
                .map_err(cuda_driver_err)?;
        }

        let completed_iters = self.host_completed_iters[0].min(lane_launch_iters_u32) as usize;
        let hashes_done = requested_hashes.min(completed_iters.saturating_mul(lanes_active));
        if hashes_done == 0 {
            return Ok(FillBatchResult {
                hashes_done: 0,
                solutions: Vec::new(),
            });
        }

        let mut solutions = Vec::new();
        if let Some(target_hash) = target {
            if use_fused_target_check {
                {
                    let found_view = self
                        .found_index
                        .try_slice(0..1)
                        .ok_or_else(|| anyhow!("failed to slice CUDA found-index buffer"))?;
                    self.stream
                        .memcpy_dtoh(&found_view, &mut self.host_found_index)
                        .map_err(cuda_driver_err)?;
                }
                if self.host_found_index[0] != u32::MAX && self.host_found_index[0] > 0 {
                    let idx = (self.host_found_index[0] - 1) as usize;
                    if idx < hashes_done {
                        solutions.push((nonces[idx], self.read_last_block_hash(idx)?));
                    }
                }
            } else {
                // Finish every hash on the CPU so a batch reports all of its
                // shares, not only the first.
                let words = hashes_done.saturating_mul(128);
                if self.host_last_blocks.len() < words {
                    self.host_last_blocks.resize(words, 0);
                }
                {
                    let blocks_view = self
                        .last_blocks
                        .try_slice(0..words)
                        .ok_or_else(|| anyhow!("failed to slice CUDA last-block buffer"))?;
                    self.stream
                        .memcpy_dtoh(&blocks_view, &mut self.host_last_blocks[..words])
                        .map_err(cuda_driver_err)?;
                }
                for (idx, nonce) in nonces.iter().take(hashes_done).enumerate() {
                    let block: &[u64; 128] = self.host_last_blocks[idx * 128..(idx + 1) * 128]
                        .try_into()
                        .expect("128-word block");
                    let hash = pow_hash_from_last_block_words(block);
                    if hash_meets_target(&hash, target_hash) {
                        solutions.push((*nonce, hash));
                    }
                }
            }
        }
        let _ = network_target;

        Ok(FillBatchResult {
            hashes_done,
            solutions,
        })
    }

    fn read_last_block_hash(&self, hash_idx: usize) -> Result<[u8; 32]> {
        let start = hash_idx
            .checked_mul(128)
            .ok_or_else(|| anyhow!("hash index overflow while reading CUDA last block"))?;
        let end = start
            .checked_add(128)
            .ok_or_else(|| anyhow!("hash index overflow while reading CUDA last block"))?;
        let block_view = self
            .last_blocks
            .try_slice(start..end)
            .ok_or_else(|| anyhow!("failed to slice CUDA last-block buffer"))?;
        let mut host_words = [0u64; 128];
        self.stream
            .memcpy_dtoh(&block_view, &mut host_words)
            .map_err(cuda_driver_err)?;
        Ok(pow_hash_from_last_block_words(&host_words))
    }
}

pub struct NvidiaBackend {
    instance_id: Arc<AtomicU64>,
    requested_device_index: Option<u32>,
    autotune_config_path: PathBuf,
    cubin_cache_dir: PathBuf,
    autotune_secs: u64,
    tuning_options: NvidiaBackendTuningOptions,
    resolved_device: RwLock<Option<NvidiaDeviceInfo>>,
    pci_address: Mutex<Option<String>>,
    kernel_tuning: RwLock<Option<NvidiaKernelTuning>>,
    shared: Arc<NvidiaShared>,
    nonblocking_control: Mutex<NonblockingControlState>,
    interrupt_controller: Arc<RwLock<Option<CudaInterruptController>>>,
    worker: Mutex<Option<NvidiaWorker>>,
    max_lanes: AtomicUsize,
    pinned_lanes: AtomicUsize,
    max_hashes_per_launch_per_lane: AtomicU32,
    /// Whether this GPU can mine Argon2 (NVRTC or a prebuilt kernel for it).
    argon2_supported: AtomicBool,
}

impl NvidiaBackend {
    pub fn new(
        device_index: Option<u32>,
        autotune_config_path: PathBuf,
        autotune_secs: u64,
        tuning_options: NvidiaBackendTuningOptions,
    ) -> Self {
        Self {
            instance_id: Arc::new(AtomicU64::new(0)),
            requested_device_index: device_index,
            cubin_cache_dir: derive_nvidia_cubin_cache_dir(&autotune_config_path),
            autotune_config_path,
            autotune_secs: autotune_secs.max(1),
            tuning_options: NvidiaBackendTuningOptions {
                max_rregcount_override: tuning_options.max_rregcount_override.filter(|v| *v > 0),
                max_lanes_override: tuning_options.max_lanes_override.filter(|v| *v > 0),
                autotune_samples: tuning_options.autotune_samples.max(1),
                dispatch_iters_per_lane: tuning_options.dispatch_iters_per_lane.filter(|v| *v > 0),
                allocation_iters_per_lane: tuning_options
                    .allocation_iters_per_lane
                    .filter(|v| *v > 0),
                hashes_per_launch_per_lane: tuning_options.hashes_per_launch_per_lane.max(1),
                hashes_per_launch_per_lane_was_set: tuning_options
                    .hashes_per_launch_per_lane_was_set,
                fused_target_check: tuning_options.fused_target_check,
                adaptive_launch_depth: tuning_options.adaptive_launch_depth,
                enforce_template_stop: tuning_options.enforce_template_stop,
            },
            resolved_device: RwLock::new(None),
            pci_address: Mutex::new(None),
            kernel_tuning: RwLock::new(None),
            shared: Arc::new(NvidiaShared::new()),
            nonblocking_control: Mutex::new(NonblockingControlState::default()),
            interrupt_controller: Arc::new(RwLock::new(None)),
            worker: Mutex::new(None),
            max_lanes: AtomicUsize::new(1),
            pinned_lanes: AtomicUsize::new(0),
            argon2_supported: AtomicBool::new(nvrtc_available()),
            max_hashes_per_launch_per_lane: AtomicU32::new(
                tuning_options.hashes_per_launch_per_lane.max(1),
            ),
        }
    }

    fn validate_or_select_device(&self) -> Result<NvidiaDeviceInfo> {
        let devices = query_nvidia_devices()?;
        let selected = if let Some(requested_index) = self.requested_device_index {
            devices
                .iter()
                .find(|device| device.index == requested_index)
                .cloned()
                .ok_or_else(|| {
                    let available = devices
                        .iter()
                        .map(|device| device.index.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    anyhow!(
                        "requested NVIDIA device index {} was not found; available indices: [{}]",
                        requested_index,
                        available
                    )
                })?
        } else {
            devices[0].clone()
        };

        let memory_total_bytes = selected.memory_total_mib.saturating_mul(1024 * 1024);
        if memory_total_bytes < CPU_LANE_MEMORY_BYTES {
            bail!(
                "NVIDIA device {} ({}) reports {} MiB VRAM; at least {} MiB is required",
                selected.index,
                selected.name,
                selected.memory_total_mib,
                (CPU_LANE_MEMORY_BYTES / (1024 * 1024)).max(1)
            );
        }

        if let Ok(mut slot) = self.resolved_device.write() {
            *slot = Some(selected.clone());
        }

        Ok(selected)
    }

    fn worker_assignment_tx(&self) -> Result<Sender<WorkerCommand>> {
        let guard = self
            .worker
            .lock()
            .map_err(|_| anyhow!("nvidia worker lock poisoned"))?;
        let Some(worker) = guard.as_ref() else {
            bail!("NVIDIA backend is not started");
        };
        Ok(worker.assignment_tx.clone())
    }

    fn worker_control_tx(&self) -> Result<Sender<WorkerCommand>> {
        let guard = self
            .worker
            .lock()
            .map_err(|_| anyhow!("nvidia worker lock poisoned"))?;
        let Some(worker) = guard.as_ref() else {
            bail!("NVIDIA backend is not started");
        };
        Ok(worker.control_tx.clone())
    }

    fn clear_nonblocking_control_state(&self) {
        if let Ok(mut state) = self.nonblocking_control.lock() {
            state.pending = None;
            state.completed = None;
        }
    }

    fn send_control_blocking(&self, kind: PendingControlKind) -> Result<()> {
        let tx = self.worker_control_tx()?;
        let (ack_tx, ack_rx) = bounded::<Result<()>>(1);
        let cmd = match kind {
            PendingControlKind::Cancel => WorkerCommand::Cancel(ack_tx),
            PendingControlKind::Fence => WorkerCommand::Fence(ack_tx),
        };
        tx.send(cmd).map_err(|_| {
            anyhow!(
                "nvidia worker channel closed while issuing {}",
                kind.label()
            )
        })?;
        ack_rx
            .recv()
            .map_err(|_| anyhow!("nvidia worker did not acknowledge {}", kind.label()))??;
        Ok(())
    }

    /// A completed cancel or fence predates work assigned after it.
    fn forget_completed_control(&self) {
        if let Ok(mut state) = self.nonblocking_control.lock() {
            state.completed = None;
        }
    }

    fn control_nonblocking(&self, kind: PendingControlKind) -> Result<BackendCallStatus> {
        let tx = self.worker_control_tx()?;
        let mut state = self
            .nonblocking_control
            .lock()
            .map_err(|_| anyhow!("nvidia nonblocking control lock poisoned"))?;

        if let Some((completed_kind, result)) = state.completed.take() {
            result?;
            if completed_kind == kind {
                return Ok(BackendCallStatus::Complete);
            }
        }
        if let Some(pending) = state.pending.as_mut() {
            match pending.ack_rx.try_recv() {
                Ok(result) => {
                    let pending_kind = pending.kind;
                    state.pending = None;
                    result?;
                    if pending_kind == kind {
                        return Ok(BackendCallStatus::Complete);
                    }
                }
                Err(TryRecvError::Empty) => {
                    if pending.kind == kind {
                        return Ok(BackendCallStatus::Pending);
                    }
                    return Ok(BackendCallStatus::Pending);
                }
                Err(TryRecvError::Disconnected) => {
                    let pending_kind = pending.kind;
                    state.pending = None;
                    bail!(
                        "nvidia worker disconnected while waiting for {} acknowledgement",
                        pending_kind.label()
                    );
                }
            }
        }

        let (ack_tx, ack_rx) = bounded::<Result<()>>(1);
        let cmd = match kind {
            PendingControlKind::Cancel => WorkerCommand::Cancel(ack_tx),
            PendingControlKind::Fence => WorkerCommand::Fence(ack_tx),
        };
        match tx.try_send(cmd) {
            Ok(()) => {
                state.pending = Some(PendingControlAck { kind, ack_rx });
                if let Some(pending) = state.pending.as_mut() {
                    match pending.ack_rx.try_recv() {
                        Ok(result) => {
                            state.pending = None;
                            result?;
                            return Ok(BackendCallStatus::Complete);
                        }
                        Err(TryRecvError::Empty) => {
                            return Ok(BackendCallStatus::Pending);
                        }
                        Err(TryRecvError::Disconnected) => {
                            let pending_kind = pending.kind;
                            state.pending = None;
                            bail!(
                                "nvidia worker disconnected while waiting for {} acknowledgement",
                                pending_kind.label()
                            );
                        }
                    }
                }
                Ok(BackendCallStatus::Pending)
            }
            Err(TrySendError::Full(_)) => Ok(BackendCallStatus::Pending),
            Err(TrySendError::Disconnected(_)) => {
                bail!(
                    "nvidia worker channel closed while issuing {}",
                    kind.label()
                )
            }
        }
    }

    /// Whether resolving tuning would run the Argon2 autotune: no explicit
    /// register cap, no tuning resolved earlier and no cached result.
    fn needs_autotune(&self, selected: &NvidiaDeviceInfo) -> bool {
        if self.tuning_options.max_rregcount_override.is_some() || !fill_v1_requested() {
            return false;
        }
        if matches!(self.kernel_tuning.read().as_deref(), Ok(Some(_))) {
            return false;
        }
        let key = build_nvidia_autotune_key(
            selected,
            self.tuning_options.hashes_per_launch_per_lane.max(1),
        );
        load_nvidia_cached_tuning(&self.autotune_config_path, &key).is_err()
    }

    /// The network's PoW when this backend starts. Before an autotune, and
    /// while the network loop is already running, waits up to
    /// `NETWORK_POW_WAIT` for the first template.
    fn network_pow_at_start(
        &self,
        selected: &NvidiaDeviceInfo,
    ) -> Option<crate::backend::PowAlgorithm> {
        if let Some(pow) = crate::backend::network_pow() {
            return Some(pow);
        }
        if !crate::backend::network_pow_expected() || !self.needs_autotune(selected) {
            return None;
        }
        let deadline = Instant::now() + NETWORK_POW_WAIT;
        while Instant::now() < deadline {
            thread::sleep(Duration::from_millis(100));
            if let Some(pow) = crate::backend::network_pow() {
                return Some(pow);
            }
        }
        None
    }

    /// `skip_autotune` uses the default tuning when nothing is cached, for a
    /// backend that only expects BLAKE2b work.
    fn resolve_kernel_tuning(
        &self,
        selected: &NvidiaDeviceInfo,
        skip_autotune: bool,
    ) -> NvidiaKernelTuning {
        if let Ok(slot) = self.kernel_tuning.read() {
            if let Some(tuning) = *slot {
                return tuning;
            }
        }

        let requested_hashes_per_launch_per_lane =
            self.tuning_options.hashes_per_launch_per_lane.max(1);
        let cached_hashes_per_launch_per_lane_cap =
            self.effective_hashes_per_launch_per_lane_cap(selected);

        if let Some(forced_rregcount) = self.tuning_options.max_rregcount_override {
            crate::miner::ui::info(
                "NVIDIA",
                format!(
                    "nvidia-autotune | GPU {}: disabled by explicit register cap",
                    selected.index
                ),
            );
            let forced = self.apply_runtime_tuning_overrides(
                NvidiaKernelTuning {
                    max_rregcount: forced_rregcount.max(1),
                    block_loop_unroll: false,
                    hashes_per_launch_per_lane: requested_hashes_per_launch_per_lane,
                    max_lanes_hint: self.resolve_max_lanes_override(),
                },
                requested_hashes_per_launch_per_lane,
            );
            if let Ok(mut slot) = self.kernel_tuning.write() {
                *slot = Some(forced);
            }
            return forced;
        }

        // The default fill loop starts with the built-in tuning; the autotune
        // runs only for the original loop.
        if !fill_v1_requested() {
            let tuning = self.apply_runtime_tuning_overrides(
                NvidiaKernelTuning {
                    hashes_per_launch_per_lane: cached_hashes_per_launch_per_lane_cap,
                    max_lanes_hint: self.resolve_max_lanes_override(),
                    ..NvidiaKernelTuning::default()
                },
                cached_hashes_per_launch_per_lane_cap,
            );
            if let Ok(mut slot) = self.kernel_tuning.write() {
                *slot = Some(tuning);
            }
            return tuning;
        }

        let key = build_nvidia_autotune_key(selected, requested_hashes_per_launch_per_lane);
        match load_nvidia_cached_tuning(&self.autotune_config_path, &key) {
            Ok(cached) => {
                // Keep Blackwell's implicit launch-depth handling unchanged.
                let cached = self
                    .apply_runtime_tuning_overrides(cached, cached_hashes_per_launch_per_lane_cap);
                crate::miner::ui::info(
                    "NVIDIA",
                    format!(
                        "nvidia-autotune | GPU {}: using cached tuning",
                        selected.index
                    ),
                );
                if let Ok(mut slot) = self.kernel_tuning.write() {
                    *slot = Some(cached);
                }
                return cached;
            }
            Err(_) if skip_autotune => {
                crate::miner::ui::info(
                    "NVIDIA",
                    format!(
                        "nvidia-autotune | GPU {}: network is on BLAKE2b; skipping Argon2 autotune",
                        selected.index
                    ),
                );
                return self.apply_runtime_tuning_overrides(
                    NvidiaKernelTuning {
                        hashes_per_launch_per_lane: requested_hashes_per_launch_per_lane,
                        ..NvidiaKernelTuning::default()
                    },
                    requested_hashes_per_launch_per_lane,
                );
            }
            Err(reason) => crate::miner::ui::info(
                "NVIDIA",
                format!(
                    "nvidia-autotune | GPU {}: {reason}; running autotune",
                    selected.index
                ),
            ),
        }

        let tuned = self.apply_runtime_tuning_overrides(
            autotune_nvidia_kernel_tuning(
                selected,
                &self.autotune_config_path,
                self.autotune_secs,
                self.tuning_options.autotune_samples,
                self.resolve_max_lanes_override(),
                requested_hashes_per_launch_per_lane,
            )
            .unwrap_or_else(|_| {
                crate::miner::ui::warn(
                    "NVIDIA",
                    format!(
                        "nvidia-autotune | GPU {}: tuning failed; using default tuning",
                        selected.index
                    ),
                );
                NvidiaKernelTuning {
                    hashes_per_launch_per_lane: requested_hashes_per_launch_per_lane,
                    ..NvidiaKernelTuning::default()
                }
            }),
            requested_hashes_per_launch_per_lane,
        );
        if let Ok(mut slot) = self.kernel_tuning.write() {
            *slot = Some(tuned);
        }
        tuned
    }

    fn effective_hashes_per_launch_per_lane_cap(&self, selected: &NvidiaDeviceInfo) -> u32 {
        let (compute_cap_major, _) =
            query_cuda_compute_capability(selected.index).unwrap_or((0, 0));
        effective_hashes_per_launch_per_lane_cap(
            self.tuning_options.hashes_per_launch_per_lane,
            self.tuning_options.hashes_per_launch_per_lane_was_set,
            compute_cap_major,
        )
    }

    fn apply_runtime_tuning_overrides(
        &self,
        mut tuning: NvidiaKernelTuning,
        hashes_per_launch_per_lane_cap: u32,
    ) -> NvidiaKernelTuning {
        tuning.max_rregcount = tuning.max_rregcount.max(1);
        tuning.hashes_per_launch_per_lane = tuning
            .hashes_per_launch_per_lane
            .max(1)
            .min(hashes_per_launch_per_lane_cap.max(1));
        if let Some(forced) = self.tuning_options.max_lanes_override {
            tuning.max_lanes_hint = Some(forced.max(1));
        } else if tuning.max_lanes_hint.is_none() {
            tuning.max_lanes_hint = self.resolve_max_lanes_override();
        }
        tuning
    }

    fn resolve_max_lanes_override(&self) -> Option<usize> {
        if let Some(forced) = self.tuning_options.max_lanes_override {
            return Some(forced.max(1));
        }
        let pinned = self.pinned_lanes.load(Ordering::Acquire);
        if pinned > 0 {
            Some(pinned.max(1))
        } else {
            None
        }
    }
}

impl Drop for NvidiaBackend {
    fn drop(&mut self) {
        self.stop();
    }
}

impl PowBackend for NvidiaBackend {
    fn hardware_telemetry(&self) -> seine_telemetry::BackendSettings {
        let device = self.resolved_device.try_read().ok().and_then(|d| d.clone());
        let tuning = self.kernel_tuning.try_read().ok().and_then(|t| *t);
        seine_telemetry::BackendSettings {
            backend: "nvidia".into(),
            device_model: device.as_ref().map(|d| d.name.clone()),
            memory_mib: device.map(|d| d.memory_total_mib),
            lanes: self.enabled_lanes() as u32,
            cpu_kernel: None,
            cpu_page_mode: None,
            gpu_register_cap: tuning.map(|t| t.max_rregcount),
            gpu_launch_depth: tuning.map(|t| t.hashes_per_launch_per_lane),
            cpu_backing_mib: None,
            cpu_affinity: None,
            pow: self
                .shared
                .blake2b_active
                .load(Ordering::Acquire)
                .then(|| "blake2b".to_string()),
        }
    }

    fn name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn lanes(&self) -> usize {
        self.max_lanes.load(Ordering::Acquire).max(1)
    }

    fn device_memory_bytes(&self) -> Option<u64> {
        self.resolved_device.read().ok().and_then(|dev| {
            dev.as_ref()
                .map(|d| d.memory_total_mib.saturating_mul(1024 * 1024))
        })
    }

    fn device_index(&self) -> Option<u32> {
        self.resolved_device
            .read()
            .ok()
            .and_then(|dev| dev.as_ref().map(|d| d.index))
    }

    fn pci_address(&self) -> Option<String> {
        let mut cached = self.pci_address.lock().ok()?;
        if cached.is_none() {
            *cached = query_cuda_pci_address(self.device_index()?);
        }
        cached.clone()
    }

    fn set_instance_id(&self, id: BackendInstanceId) {
        self.instance_id.store(id, Ordering::Release);
    }

    fn set_event_sink(&self, sink: Sender<BackendEvent>) {
        if let Ok(mut slot) = self.shared.event_sink.write() {
            *slot = Some(sink);
        }
    }

    fn start(&self) -> Result<()> {
        {
            let guard = self
                .worker
                .lock()
                .map_err(|_| anyhow!("nvidia worker lock poisoned"))?;
            if guard.is_some() {
                return Ok(());
            }
        }

        require_cuda_driver()?;
        let selected = self.validate_or_select_device()?;
        // Without NVRTC, Argon2 runs from the prebuilt PTX where one fits this
        // GPU and the network's parameters; otherwise the backend stays idle on
        // Argon2 work (`supports_pow`) and mines BLAKE2b.
        let argon2_supported = argon2_uses_nvrtc()
            || pow_params().ok().is_some_and(|params| {
                let (cc_major, _) = query_cuda_compute_capability(selected.index).unwrap_or((0, 0));
                prebuilt_argon2_ptx(cc_major, params.block_count() as u32, params.t_cost())
                    .is_some()
            });
        self.argon2_supported
            .store(argon2_supported, Ordering::Release);
        if !argon2_supported {
            note_nvrtc_unavailable(selected.index);
        }
        // A GPU joining a BLAKE2b network skips the Argon2 autotune and engine;
        // the worker builds the engine if Argon2 work arrives.
        let blake2b_only = !argon2_supported
            || self.network_pow_at_start(&selected)
                == Some(crate::backend::PowAlgorithm::Blake2b512);
        let tuning = self.resolve_kernel_tuning(&selected, blake2b_only);
        let engine = if blake2b_only {
            None
        } else {
            Some(
                CudaArgon2Engine::new(
                    selected.index,
                    selected.memory_total_mib,
                    selected.memory_free_mib,
                    tuning,
                    tuning.max_lanes_hint,
                    tuning.hashes_per_launch_per_lane,
                    self.tuning_options.fused_target_check,
                    &self.cubin_cache_dir,
                )
                .with_context(|| {
                    format!(
                        "failed to initialize CUDA engine on NVIDIA device {} ({})",
                        selected.index, selected.name
                    )
                })?,
            )
        };

        let (discovered_lanes, discovered_hashes_per_launch) =
            engine.as_ref().map_or((1, 1), |engine| {
                (
                    engine.max_lanes().max(1),
                    engine.max_hashes_per_launch_per_lane() as u32,
                )
            });
        let interrupt_controller = engine.as_ref().map(|engine| engine.interrupt_controller());
        self.max_lanes.store(discovered_lanes, Ordering::Release);
        self.pinned_lanes
            .fetch_max(discovered_lanes, Ordering::AcqRel);
        self.max_hashes_per_launch_per_lane
            .store(discovered_hashes_per_launch.max(1), Ordering::Release);

        self.shared.error_emitted.store(false, Ordering::Release);
        self.shared.hashes.store(0, Ordering::Release);
        self.shared.active_lanes.store(0, Ordering::Release);
        self.shared.pending_work.store(0, Ordering::Release);
        self.shared
            .inflight_assignment_hashes
            .store(0, Ordering::Release);
        self.shared
            .active_hashes_per_launch_per_lane
            .store(discovered_hashes_per_launch.max(1), Ordering::Release);
        self.shared.cancel_requested.store(false, Ordering::Release);
        if let Ok(mut slot) = self.shared.inflight_assignment_started_at.lock() {
            *slot = None;
        }
        if let Ok(mut slot) = self.interrupt_controller.write() {
            *slot = interrupt_controller;
        }
        self.clear_nonblocking_control_state();

        let (assignment_tx, assignment_rx) = bounded::<WorkerCommand>(ASSIGN_CHANNEL_CAPACITY);
        let (control_tx, control_rx) = bounded::<WorkerCommand>(CONTROL_CHANNEL_CAPACITY);
        let shared = Arc::clone(&self.shared);
        let instance_id = Arc::clone(&self.instance_id);
        let adaptive_launch_depth = self.tuning_options.adaptive_launch_depth;
        let enforce_template_stop = self.tuning_options.enforce_template_stop;
        let mut blake2b_state =
            blake2b::Blake2bWorkerState::new(selected.index, self.cubin_cache_dir.clone());
        let rebuild = {
            let selected = selected.clone();
            let fused_target_check = self.tuning_options.fused_target_check;
            let cubin_cache_dir = self.cubin_cache_dir.clone();
            let interrupt_slot = Arc::clone(&self.interrupt_controller);
            Box::new(move || {
                let engine = CudaArgon2Engine::new(
                    selected.index,
                    selected.memory_total_mib,
                    selected.memory_free_mib,
                    tuning,
                    tuning.max_lanes_hint,
                    tuning.hashes_per_launch_per_lane,
                    fused_target_check,
                    &cubin_cache_dir,
                )?;
                if let Ok(mut slot) = interrupt_slot.write() {
                    *slot = Some(engine.interrupt_controller());
                }
                Ok(engine)
            })
        };
        let mut argon2 = Argon2EngineSlot { engine, rebuild };
        let handle = thread::Builder::new()
            .name(format!(
                "seine-nvidia-worker-{}",
                self.instance_id.load(Ordering::Acquire)
            ))
            .spawn(move || {
                worker_loop(
                    &mut argon2,
                    &mut blake2b_state,
                    assignment_rx,
                    control_rx,
                    shared,
                    instance_id,
                    adaptive_launch_depth,
                    enforce_template_stop,
                )
            })
            .map_err(|err| anyhow!("failed to spawn nvidia worker thread: {err}"))?;

        let mut guard = self
            .worker
            .lock()
            .map_err(|_| anyhow!("nvidia worker lock poisoned"))?;
        *guard = Some(NvidiaWorker {
            assignment_tx,
            control_tx,
            handle,
        });

        Ok(())
    }

    fn stop(&self) {
        let worker = match self.worker.lock() {
            Ok(mut slot) => slot.take(),
            Err(_) => None,
        };
        if let Some(worker) = worker {
            if worker.control_tx.send(WorkerCommand::Stop).is_err() {
                let _ = worker.assignment_tx.send(WorkerCommand::Stop);
            }
            let _ = worker.handle.join();
        }
        if let Ok(mut slot) = self.interrupt_controller.write() {
            *slot = None;
        }
        self.clear_nonblocking_control_state();

        self.shared.active_lanes.store(0, Ordering::Release);
        self.shared.pending_work.store(0, Ordering::Release);
        self.shared
            .inflight_assignment_hashes
            .store(0, Ordering::Release);
        self.shared.active_hashes_per_launch_per_lane.store(
            self.max_hashes_per_launch_per_lane
                .load(Ordering::Acquire)
                .max(1),
            Ordering::Release,
        );
        self.shared.cancel_requested.store(false, Ordering::Release);
        if let Ok(mut slot) = self.shared.inflight_assignment_started_at.lock() {
            *slot = None;
        }
    }

    fn assign_work(&self, work: WorkAssignment) -> Result<()> {
        self.forget_completed_control();
        self.worker_assignment_tx()?
            .send(WorkerCommand::Assign(work))
            .map_err(|_| anyhow!("nvidia worker channel closed while assigning work"))
    }

    fn assign_work_batch(&self, work: &[WorkAssignment]) -> Result<()> {
        self.forget_completed_control();
        let mut batch = normalize_assignment_batch(work)?;
        match batch.len() {
            0 => Ok(()),
            1 => self.assign_work(batch.pop().expect("single-item batch should be present")),
            _ => self
                .worker_assignment_tx()?
                .send(WorkerCommand::AssignBatch(batch))
                .map_err(|_| anyhow!("nvidia worker channel closed while assigning work")),
        }
    }

    fn assign_work_batch_with_deadline(
        &self,
        work: &[WorkAssignment],
        deadline: Instant,
    ) -> Result<()> {
        self.forget_completed_control();
        self.assign_work_batch(work)?;
        if Instant::now() > deadline {
            return Err(anyhow!(
                "assignment call exceeded deadline by {}ms",
                Instant::now()
                    .saturating_duration_since(deadline)
                    .as_millis()
            ));
        }
        Ok(())
    }

    fn supports_assignment_batching(&self) -> bool {
        true
    }

    fn supports_true_nonblocking(&self) -> bool {
        true
    }

    fn assign_work_batch_nonblocking(&self, work: &[WorkAssignment]) -> Result<BackendCallStatus> {
        self.forget_completed_control();
        let mut batch = normalize_assignment_batch(work)?;
        if batch.is_empty() {
            return Ok(BackendCallStatus::Complete);
        }

        let tx = self.worker_assignment_tx()?;
        let cmd = if batch.len() == 1 {
            WorkerCommand::Assign(batch.pop().expect("single-item batch should be present"))
        } else {
            WorkerCommand::AssignBatch(batch)
        };
        match tx.try_send(cmd) {
            Ok(()) => Ok(BackendCallStatus::Complete),
            Err(TrySendError::Full(_)) => Ok(BackendCallStatus::Pending),
            Err(TrySendError::Disconnected(_)) => {
                bail!("nvidia worker channel closed while assigning work")
            }
        }
    }

    fn wait_for_nonblocking_progress(&self, wait_for: Duration) -> Result<()> {
        let wait_for = wait_for.max(Duration::from_micros(10));
        let mut state = self
            .nonblocking_control
            .lock()
            .map_err(|_| anyhow!("nvidia nonblocking control lock poisoned"))?;
        let Some(pending) = state.pending.as_mut() else {
            drop(state);
            thread::sleep(wait_for);
            return Ok(());
        };

        match pending.ack_rx.recv_timeout(wait_for) {
            Ok(result) => {
                let pending_kind = pending.kind;
                state.pending = None;
                state.completed = Some((pending_kind, result));
                Ok(())
            }
            Err(RecvTimeoutError::Timeout) => Ok(()),
            Err(RecvTimeoutError::Disconnected) => {
                let pending_kind = pending.kind;
                state.pending = None;
                bail!(
                    "nvidia worker disconnected while waiting for {} acknowledgement",
                    pending_kind.label()
                );
            }
        }
    }

    fn cancel_work_nonblocking(&self) -> Result<BackendCallStatus> {
        self.control_nonblocking(PendingControlKind::Cancel)
    }

    fn fence_nonblocking(&self) -> Result<BackendCallStatus> {
        self.control_nonblocking(PendingControlKind::Fence)
    }

    fn cancel_work(&self) -> Result<()> {
        self.send_control_blocking(PendingControlKind::Cancel)
    }

    fn request_timeout_interrupt(&self) -> Result<()> {
        self.shared.cancel_requested.store(true, Ordering::Release);
        if let Some(controller) = self
            .interrupt_controller
            .read()
            .ok()
            .and_then(|slot| slot.as_ref().cloned())
        {
            let _ = controller.signal_cancel();
        }
        let tx = match self.worker_control_tx() {
            Ok(tx) => tx,
            Err(_) => return Ok(()),
        };
        let (ack_tx, _ack_rx) = bounded::<Result<()>>(1);
        match tx.try_send(WorkerCommand::Cancel(ack_tx)) {
            Ok(()) | Err(TrySendError::Full(_)) => Ok(()),
            Err(TrySendError::Disconnected(_)) => Ok(()),
        }
    }

    fn fence(&self) -> Result<()> {
        self.send_control_blocking(PendingControlKind::Fence)
    }

    fn take_hashes(&self) -> u64 {
        self.shared.hashes.swap(0, Ordering::AcqRel)
    }

    fn take_telemetry(&self) -> BackendTelemetry {
        let inflight_assignment_micros = if self.shared.pending_work.load(Ordering::Acquire) > 0 {
            self.shared
                .inflight_assignment_started_at
                .lock()
                .ok()
                .and_then(|slot| {
                    slot.as_ref()
                        .map(|started| started.elapsed().as_micros().min(u64::MAX as u128) as u64)
                })
                .unwrap_or(0)
        } else {
            0
        };

        BackendTelemetry {
            active_lanes: self.shared.active_lanes.load(Ordering::Acquire),
            pending_work: self.shared.pending_work.load(Ordering::Acquire),
            dropped_events: self.shared.dropped_events.swap(0, Ordering::AcqRel),
            completed_assignments: self.shared.completed_assignments.swap(0, Ordering::AcqRel),
            completed_assignment_hashes: self
                .shared
                .completed_assignment_hashes
                .swap(0, Ordering::AcqRel),
            completed_assignment_micros: self
                .shared
                .completed_assignment_micros
                .swap(0, Ordering::AcqRel),
            inflight_assignment_hashes: self
                .shared
                .inflight_assignment_hashes
                .load(Ordering::Acquire),
            inflight_assignment_micros,
            ..BackendTelemetry::default()
        }
    }

    fn preemption_granularity(&self) -> PreemptionGranularity {
        let per_launch = self
            .shared
            .active_hashes_per_launch_per_lane
            .load(Ordering::Acquire)
            .max(1) as u64;
        PreemptionGranularity::Hashes((self.lanes() as u64).saturating_mul(per_launch).max(1))
    }

    fn supports_pow(&self, pow: crate::backend::PowAlgorithm) -> bool {
        // BLAKE2b always has a prebuilt kernel; Argon2 needs NVRTC or a
        // prebuilt kernel for this GPU (decided at start).
        pow == crate::backend::PowAlgorithm::Blake2b512
            || self.argon2_supported.load(Ordering::Acquire)
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            preferred_iters_per_lane: Some(
                self.tuning_options
                    .dispatch_iters_per_lane
                    .unwrap_or(DEFAULT_DISPATCH_ITERS_PER_LANE)
                    .max(1),
            ),
            preferred_allocation_iters_per_lane: Some(
                self.tuning_options
                    .allocation_iters_per_lane
                    .unwrap_or(DEFAULT_ALLOCATION_ITERS_PER_LANE)
                    .max(1),
            ),
            preferred_hash_poll_interval: Some(Duration::from_millis(25)),
            preferred_assignment_timeout: None,
            preferred_control_timeout: None,
            preferred_assignment_timeout_strikes: None,
            preferred_worker_queue_depth: Some(32),
            max_inflight_assignments: 32,
            deadline_support: DeadlineSupport::Cooperative,
            assignment_semantics: AssignmentSemantics::Replace,
            execution_model: BackendExecutionModel::Nonblocking,
            nonblocking_poll_min: Some(Duration::from_micros(50)),
            nonblocking_poll_max: Some(Duration::from_millis(1)),
        }
    }

    fn bench_backend(&self) -> Option<&dyn BenchBackend> {
        Some(self)
    }
}

impl BenchBackend for NvidiaBackend {
    fn kernel_bench(&self, seconds: u64, shutdown: &AtomicBool) -> Result<u64> {
        let selected = self.validate_or_select_device()?;
        let tuning = self.resolve_kernel_tuning(&selected, false);
        let mut engine = CudaArgon2Engine::new(
            selected.index,
            selected.memory_total_mib,
            selected.memory_free_mib,
            tuning,
            tuning.max_lanes_hint,
            tuning.hashes_per_launch_per_lane,
            self.tuning_options.fused_target_check,
            &self.cubin_cache_dir,
        )
        .with_context(|| {
            format!(
                "failed to initialize CUDA engine for benchmark on device {} ({})",
                selected.index, selected.name
            )
        })?;

        let header = [0u8; POW_HEADER_BASE_LEN];
        let deadline = Instant::now() + Duration::from_secs(seconds.max(1));
        let mut nonce_cursor = 0u64;
        let mut total = 0u64;
        let mut nonces = vec![0u64; engine.max_hashes_per_launch()];

        while Instant::now() < deadline && !shutdown.load(Ordering::Acquire) {
            let batch_hashes = engine.max_hashes_per_launch().max(1);
            if nonces.len() < batch_hashes {
                nonces.resize(batch_hashes, 0);
            }
            for nonce in nonces.iter_mut().take(batch_hashes) {
                *nonce = nonce_cursor;
                nonce_cursor = nonce_cursor.wrapping_add(1);
            }

            let done = engine.run_fill_batch(&header, &nonces[..batch_hashes], None)?;
            total = total.saturating_add(done.hashes_done as u64);
        }

        Ok(total)
    }

    fn kernel_bench_effective(&self, seconds: u64, shutdown: &AtomicBool) -> Result<u64> {
        Ok(self
            .kernel_bench_effective_samples(1, seconds, shutdown)?
            .into_iter()
            .next()
            .map(|sample| sample.hashes)
            .unwrap_or(0))
    }

    fn kernel_bench_effective_samples(
        &self,
        rounds: u32,
        seconds: u64,
        shutdown: &AtomicBool,
    ) -> Result<Vec<KernelBenchSample>> {
        let selected = self.validate_or_select_device()?;
        let tuning = self.resolve_kernel_tuning(&selected, false);
        let mut engine = CudaArgon2Engine::new(
            selected.index,
            selected.memory_total_mib,
            selected.memory_free_mib,
            tuning,
            tuning.max_lanes_hint,
            tuning.hashes_per_launch_per_lane,
            self.tuning_options.fused_target_check,
            &self.cubin_cache_dir,
        )
        .with_context(|| {
            format!(
                "failed to initialize CUDA engine for benchmark on device {} ({})",
                selected.index, selected.name
            )
        })?;

        let header = [0u8; POW_HEADER_BASE_LEN];
        let impossible_target = [0u8; POW_OUTPUT_LEN];
        let sample_duration = Duration::from_secs(seconds.max(1));
        let mut nonce_cursor = 0u64;
        let mut nonces = vec![0u64; engine.max_hashes_per_launch()];
        let mut samples = Vec::with_capacity(rounds as usize);

        for _ in 0..rounds {
            if shutdown.load(Ordering::Acquire) {
                break;
            }

            let round_started = Instant::now();
            let deadline = round_started + sample_duration;
            let mut total = 0u64;

            while Instant::now() < deadline && !shutdown.load(Ordering::Acquire) {
                let batch_hashes = engine.max_hashes_per_launch().max(1);
                if nonces.len() < batch_hashes {
                    nonces.resize(batch_hashes, 0);
                }
                for nonce in nonces.iter_mut().take(batch_hashes) {
                    *nonce = nonce_cursor;
                    nonce_cursor = nonce_cursor.wrapping_add(1);
                }

                let done = engine.run_fill_batch(
                    &header,
                    &nonces[..batch_hashes],
                    Some(&impossible_target),
                )?;
                total = total.saturating_add(done.hashes_done as u64);
            }

            let elapsed_secs = round_started.elapsed().as_secs_f64().max(0.001);
            samples.push(KernelBenchSample {
                hashes: total,
                elapsed_secs,
                wall_elapsed_secs: elapsed_secs,
            });
        }

        Ok(samples)
    }
}

/// The worker's Argon2 engine. BLAKE2b work drops it to free its lane memory;
/// Argon2 work after that rebuilds it with the start-time parameters.
struct Argon2EngineSlot {
    engine: Option<CudaArgon2Engine>,
    rebuild: Box<dyn FnMut() -> Result<CudaArgon2Engine> + Send>,
}

impl Argon2EngineSlot {
    /// Drops the engine and returns its lane memory to the device. Freed
    /// stream-ordered allocations otherwise stay reserved in the device's
    /// default memory pool.
    fn release(&mut self) {
        let Some(engine) = self.engine.take() else {
            return;
        };
        let stream = Arc::clone(&engine.stream);
        drop(engine);
        let _ = stream.synchronize();
        let ctx = stream.context();
        // Without async allocation the frees above were synchronous and
        // there is no pool to trim.
        if ctx.has_async_alloc() {
            unsafe {
                let mut pool: cudarc::driver::sys::CUmemoryPool = std::ptr::null_mut();
                if cudarc::driver::sys::cuDeviceGetDefaultMemPool(&mut pool, ctx.cu_device())
                    == cudarc::driver::sys::CUresult::CUDA_SUCCESS
                {
                    let _ = cudarc::driver::sys::cuMemPoolTrimTo(pool, 0);
                }
            }
        }
    }

    fn ensure(&mut self) -> Result<&mut CudaArgon2Engine> {
        if self.engine.is_none() {
            self.engine = Some((self.rebuild)()?);
        }
        Ok(self.engine.as_mut().expect("engine present"))
    }
}

fn worker_loop(
    argon2: &mut Argon2EngineSlot,
    blake2b: &mut blake2b::Blake2bWorkerState,
    assignment_rx: Receiver<WorkerCommand>,
    control_rx: Receiver<WorkerCommand>,
    shared: Arc<NvidiaShared>,
    instance_id: Arc<AtomicU64>,
    adaptive_launch_depth: bool,
    enforce_template_stop: bool,
) {
    let mut active: Option<ActiveAssignment> = None;
    let mut queued: VecDeque<WorkAssignment> = VecDeque::new();
    let mut fence_waiters: Vec<Sender<Result<()>>> = Vec::new();
    let mut assignment_open = true;
    let mut control_open = true;
    let mut running = true;
    let mut adaptive_pressure = 0u32;
    let mut transient_retries: u32 = 0;
    // A worker started for BLAKE2b has no Argon2 engine yet; its sizes are
    // read when the engine is first built.
    let mut argon2_sized = argon2.engine.is_some();
    let (mut nonce_buf, mut max_launch_depth, mut argon2_lanes) = match argon2.engine.as_ref() {
        Some(engine) => (
            vec![0u64; engine.max_hashes_per_launch()],
            engine.max_hashes_per_launch_per_lane().max(1),
            engine.max_lanes().max(1),
        ),
        None => (Vec::new(), 1, 1),
    };

    shared
        .active_hashes_per_launch_per_lane
        .store(max_launch_depth as u32, Ordering::Release);

    while running {
        while control_open {
            match control_rx.try_recv() {
                Ok(cmd) => {
                    running = handle_worker_command(
                        cmd,
                        &mut active,
                        &mut queued,
                        &mut fence_waiters,
                        &shared,
                        &mut adaptive_pressure,
                    );
                    if !running {
                        break;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    control_open = false;
                    break;
                }
            }
        }
        if !running {
            break;
        }

        if active.is_none() {
            if assignment_open {
                match assignment_rx.try_recv() {
                    Ok(cmd) => {
                        running = handle_worker_command(
                            cmd,
                            &mut active,
                            &mut queued,
                            &mut fence_waiters,
                            &shared,
                            &mut adaptive_pressure,
                        );
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => assignment_open = false,
                }
            }
            if !running {
                break;
            }

            if active.is_none() {
                if !assignment_open && !control_open {
                    break;
                }
                if assignment_open && control_open {
                    crossbeam_channel::select! {
                        recv(control_rx) -> cmd => match cmd {
                            Ok(cmd) => {
                                running = handle_worker_command(
                                    cmd,
                                    &mut active,
                                    &mut queued,
                                    &mut fence_waiters,
                                    &shared,
                                    &mut adaptive_pressure,
                                );
                            }
                            Err(_) => control_open = false,
                        },
                        recv(assignment_rx) -> cmd => match cmd {
                            Ok(cmd) => {
                                running = handle_worker_command(
                                    cmd,
                                    &mut active,
                                    &mut queued,
                                    &mut fence_waiters,
                                    &shared,
                                    &mut adaptive_pressure,
                                );
                            }
                            Err(_) => assignment_open = false,
                        },
                    }
                } else if assignment_open {
                    match assignment_rx.recv() {
                        Ok(cmd) => {
                            running = handle_worker_command(
                                cmd,
                                &mut active,
                                &mut queued,
                                &mut fence_waiters,
                                &shared,
                                &mut adaptive_pressure,
                            );
                        }
                        Err(_) => assignment_open = false,
                    }
                } else if control_open {
                    match control_rx.recv() {
                        Ok(cmd) => {
                            running = handle_worker_command(
                                cmd,
                                &mut active,
                                &mut queued,
                                &mut fence_waiters,
                                &shared,
                                &mut adaptive_pressure,
                            );
                        }
                        Err(_) => control_open = false,
                    }
                }
                continue;
            }
        }

        loop {
            let mut handled = false;
            if control_open {
                match control_rx.try_recv() {
                    Ok(cmd) => {
                        handled = true;
                        running = handle_worker_command(
                            cmd,
                            &mut active,
                            &mut queued,
                            &mut fence_waiters,
                            &shared,
                            &mut adaptive_pressure,
                        );
                        if !running {
                            break;
                        }
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => control_open = false,
                }
            }
            if assignment_open {
                match assignment_rx.try_recv() {
                    Ok(cmd) => {
                        handled = true;
                        running = handle_worker_command(
                            cmd,
                            &mut active,
                            &mut queued,
                            &mut fence_waiters,
                            &shared,
                            &mut adaptive_pressure,
                        );
                        if !running {
                            break;
                        }
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => assignment_open = false,
                }
            }
            if !handled {
                break;
            }
        }

        if !running {
            break;
        }

        if shared.cancel_requested.swap(false, Ordering::AcqRel) {
            if let Some(current) = active.as_ref() {
                finalize_active_assignment(&shared, current, 0);
            }
            active = None;
            queued.clear();
            clear_worker_pending_state(&shared);
            drain_fence_waiters(&mut fence_waiters, Ok(()));
            adaptive_pressure =
                adaptive_pressure.saturating_add(ADAPTIVE_DEPTH_PRESSURE_CONTROL_BONUS);
            continue;
        }

        let Some(current) = active.as_mut() else {
            continue;
        };

        if current.remaining == 0 {
            let next_pending = queued.len() as u64;
            finalize_active_assignment(&shared, current, next_pending);
            if let Some(next) = queued.pop_front() {
                activate_assignment(&shared, &mut active, next, queued.len() as u64 + 1);
                continue;
            }
            active = None;
            drain_fence_waiters(&mut fence_waiters, Ok(()));
            continue;
        }

        if enforce_template_stop && Instant::now() >= current.work.template.stop_at {
            queued.clear();
            finalize_active_assignment(&shared, current, 0);
            active = None;
            drain_fence_waiters(&mut fence_waiters, Ok(()));
            continue;
        }

        if current.work.template.pow == crate::backend::PowAlgorithm::Blake2b512 {
            let target_snapshot = current.work.template.target_snapshot();
            let stop_at = enforce_template_stop.then_some(current.work.template.stop_at);
            // Free the Argon2 lane memory; BLAKE2b needs none.
            argon2.release();
            shared.blake2b_active.store(true, Ordering::Release);
            shared
                .active_lanes
                .store(argon2_lanes as u64, Ordering::Release);
            let batch = match blake2b.run_batch(
                &current.work.template.header_base,
                current.next_nonce,
                current.remaining,
                &target_snapshot.target,
                stop_at,
            ) {
                Ok(batch) => batch,
                Err(err) => {
                    emit_worker_error(
                        &shared,
                        &instance_id,
                        format!("CUDA BLAKE2b batch failed: {err:#}"),
                    );
                    break;
                }
            };
            current.hashes_done = current.hashes_done.saturating_add(batch.hashes);
            current.next_nonce = current.next_nonce.wrapping_add(batch.hashes);
            current.remaining = current.remaining.saturating_sub(batch.hashes);
            shared.hashes.fetch_add(batch.hashes, Ordering::Relaxed);
            shared
                .inflight_assignment_hashes
                .store(current.hashes_done, Ordering::Release);
            let mut paused = false;
            for (nonce, hash) in batch.solutions {
                send_backend_event(
                    &shared,
                    BackendEvent::Solution(MiningSolution {
                        epoch: current.work.template.epoch,
                        nonce,
                        hash: Some(hash),
                        share_binding_id: target_snapshot.share_binding_id,
                        backend_id: instance_id.load(Ordering::Acquire),
                        backend: BACKEND_NAME,
                        found_at: std::time::Instant::now(),
                    }),
                );
                if current.work.template.pause_on_solution {
                    paused = true;
                    break;
                }
            }
            if paused {
                queued.clear();
                finalize_active_assignment(&shared, current, 0);
                active = None;
                drain_fence_waiters(&mut fence_waiters, Ok(()));
            }
            continue;
        }

        shared.blake2b_active.store(false, Ordering::Release);
        let engine = match argon2.ensure() {
            Ok(engine) => engine,
            Err(err) => {
                emit_worker_error(
                    &shared,
                    &instance_id,
                    format!("failed to rebuild the CUDA Argon2 engine: {err:#}"),
                );
                break;
            }
        };
        if !argon2_sized {
            argon2_sized = true;
            nonce_buf = vec![0u64; engine.max_hashes_per_launch()];
            max_launch_depth = engine.max_hashes_per_launch_per_lane().max(1);
            argon2_lanes = engine.max_lanes().max(1);
            shared
                .active_hashes_per_launch_per_lane
                .store(max_launch_depth as u32, Ordering::Release);
        }

        let mut hashes_per_launch_per_lane = max_launch_depth;
        if adaptive_launch_depth {
            let pressure_levels = adaptive_pressure / ADAPTIVE_DEPTH_PRESSURE_CONTROL_BONUS.max(1);
            for _ in 0..pressure_levels {
                hashes_per_launch_per_lane =
                    (hashes_per_launch_per_lane.saturating_add(1) / 2).max(1);
            }

            if enforce_template_stop {
                let elapsed = current.started_at.elapsed();
                let window = current
                    .work
                    .template
                    .stop_at
                    .saturating_duration_since(current.started_at);
                if window > Duration::ZERO
                    && elapsed.as_secs_f64()
                        >= window.as_secs_f64() * ADAPTIVE_DEPTH_DEADLINE_SLACK_FRAC
                {
                    hashes_per_launch_per_lane = 1;
                }
            }

            // Near round/template boundaries, trim launch depth to avoid long tail launches that
            // finish after stop_at and inflate fence/late-work overhead.
            if let Some(hash_micros) = current
                .hash_micros_ema
                .filter(|value| value.is_finite() && *value > 0.0)
            {
                let remaining_micros = current
                    .work
                    .template
                    .stop_at
                    .saturating_duration_since(Instant::now())
                    .as_secs_f64()
                    * 1_000_000.0;
                if remaining_micros > 0.0 {
                    let lanes_for_estimate = engine.max_lanes().max(1);
                    let budget_micros =
                        remaining_micros * ADAPTIVE_DEPTH_DEADLINE_LAUNCH_HEADROOM_FRAC;
                    while hashes_per_launch_per_lane > 1 {
                        let predicted_hashes = lanes_for_estimate
                            .saturating_mul(hashes_per_launch_per_lane)
                            .max(1) as f64;
                        let predicted_micros = hash_micros * predicted_hashes;
                        if predicted_micros <= budget_micros {
                            break;
                        }
                        hashes_per_launch_per_lane =
                            (hashes_per_launch_per_lane.saturating_add(1) / 2).max(1);
                    }
                }
            }
        }
        hashes_per_launch_per_lane = hashes_per_launch_per_lane.max(1);

        let max_hashes_for_depth = engine
            .max_lanes()
            .saturating_mul(hashes_per_launch_per_lane)
            .max(1);
        let hashes_per_batch = max_hashes_for_depth.min(current.remaining as usize).max(1);
        if nonce_buf.len() < hashes_per_batch {
            nonce_buf.resize(hashes_per_batch, 0);
        }
        for (idx, nonce) in nonce_buf.iter_mut().take(hashes_per_batch).enumerate() {
            *nonce = current.next_nonce.wrapping_add(idx as u64);
        }

        let active_lanes = engine.max_lanes().min(hashes_per_batch).max(1);
        let effective_launch_depth = hashes_per_batch
            .saturating_add(active_lanes.saturating_sub(1))
            .saturating_div(active_lanes)
            .max(1);
        shared
            .active_lanes
            .store(active_lanes as u64, Ordering::Release);
        shared
            .active_hashes_per_launch_per_lane
            .store(effective_launch_depth as u32, Ordering::Release);

        let target_snapshot = current.work.template.target_snapshot();
        let launch_started = Instant::now();
        let network_target = current.work.template.network_target();
        let done = match engine.run_fill_batch_preserving_candidate(
            current.work.template.header_base.as_ref(),
            &nonce_buf[..hashes_per_batch],
            Some(&target_snapshot.target),
            network_target.as_ref(),
        ) {
            Ok(done) => {
                transient_retries = 0;
                done
            }
            Err(err) => {
                let err_msg = format!("{err:#}");
                if is_transient_cuda_error(&err_msg)
                    && transient_retries < CUDA_TRANSIENT_RETRY_COUNT
                {
                    transient_retries += 1;
                    let delay = CUDA_TRANSIENT_RETRY_DELAYS
                        .get(transient_retries as usize - 1)
                        .copied()
                        .unwrap_or(
                            CUDA_TRANSIENT_RETRY_DELAYS[CUDA_TRANSIENT_RETRY_DELAYS.len() - 1],
                        );
                    thread::sleep(delay);
                    continue;
                }
                emit_worker_error(
                    &shared,
                    &instance_id,
                    format!("CUDA batch execution failed: {err_msg}"),
                );
                break;
            }
        };

        adaptive_pressure =
            adaptive_pressure.saturating_sub(ADAPTIVE_DEPTH_PRESSURE_DECAY_PER_BATCH);

        if done.hashes_done == 0 {
            let timeout_cancelled = shared.cancel_requested.swap(false, Ordering::AcqRel);
            let next_pending = if timeout_cancelled {
                0
            } else {
                queued.len() as u64
            };
            if timeout_cancelled {
                queued.clear();
                adaptive_pressure =
                    adaptive_pressure.saturating_add(ADAPTIVE_DEPTH_PRESSURE_CONTROL_BONUS);
            }
            finalize_active_assignment(&shared, current, next_pending);
            if !timeout_cancelled {
                if let Some(next) = queued.pop_front() {
                    activate_assignment(&shared, &mut active, next, queued.len() as u64 + 1);
                    continue;
                }
            }
            active = None;
            drain_fence_waiters(&mut fence_waiters, Ok(()));
            continue;
        }

        let launch_elapsed = launch_started.elapsed();
        current.hashes_done = current.hashes_done.saturating_add(done.hashes_done as u64);
        current.next_nonce = current.next_nonce.wrapping_add(done.hashes_done as u64);
        current.remaining = current.remaining.saturating_sub(done.hashes_done as u64);
        if done.hashes_done > 0 {
            let batch_hashes = done.hashes_done as f64;
            let per_hash_micros = (launch_elapsed.as_secs_f64() * 1_000_000.0) / batch_hashes;
            if per_hash_micros.is_finite() && per_hash_micros > 0.0 {
                current.hash_micros_ema = Some(match current.hash_micros_ema {
                    Some(previous) => {
                        previous * (1.0 - ADAPTIVE_DEPTH_HASH_EMA_ALPHA)
                            + per_hash_micros * ADAPTIVE_DEPTH_HASH_EMA_ALPHA
                    }
                    None => per_hash_micros,
                });
            }
        }

        shared
            .hashes
            .fetch_add(done.hashes_done as u64, Ordering::Relaxed);
        shared
            .inflight_assignment_hashes
            .store(current.hashes_done, Ordering::Release);

        if !done.solutions.is_empty() {
            for &(nonce, hash) in &done.solutions {
                send_backend_event(
                    &shared,
                    BackendEvent::Solution(MiningSolution {
                        epoch: current.work.template.epoch,
                        nonce,
                        hash: Some(hash),
                        share_binding_id: target_snapshot.share_binding_id,
                        backend_id: instance_id.load(Ordering::Acquire),
                        backend: BACKEND_NAME,
                        found_at: std::time::Instant::now(),
                    }),
                );
            }
            if !current.work.template.pause_on_solution {
                continue;
            }
            queued.clear();
            finalize_active_assignment(&shared, current, 0);
            active = None;
            drain_fence_waiters(&mut fence_waiters, Ok(()));
            continue;
        }
    }

    if let Some(active_assignment) = active.as_ref() {
        finalize_active_assignment(&shared, active_assignment, 0);
    }
    queued.clear();
    clear_worker_pending_state(&shared);

    drain_fence_waiters(
        &mut fence_waiters,
        Err(anyhow!("nvidia worker stopped before fence completion")),
    );
}

fn handle_worker_command(
    cmd: WorkerCommand,
    active: &mut Option<ActiveAssignment>,
    queued: &mut VecDeque<WorkAssignment>,
    fence_waiters: &mut Vec<Sender<Result<()>>>,
    shared: &NvidiaShared,
    adaptive_pressure: &mut u32,
) -> bool {
    match cmd {
        WorkerCommand::Assign(work) => {
            shared.cancel_requested.store(false, Ordering::Release);
            if active.is_some() || !queued.is_empty() {
                *adaptive_pressure =
                    adaptive_pressure.saturating_add(ADAPTIVE_DEPTH_PRESSURE_REPLACE_BONUS);
            }
            replace_assignment_queue(shared, active, queued, vec![work]);
            true
        }
        WorkerCommand::AssignBatch(work) => {
            shared.cancel_requested.store(false, Ordering::Release);
            if active.is_some() || !queued.is_empty() {
                *adaptive_pressure =
                    adaptive_pressure.saturating_add(ADAPTIVE_DEPTH_PRESSURE_REPLACE_BONUS);
            }
            replace_assignment_queue(shared, active, queued, work);
            true
        }
        WorkerCommand::Cancel(ack) => {
            shared.cancel_requested.store(false, Ordering::Release);
            if active.is_some() || !queued.is_empty() {
                *adaptive_pressure =
                    adaptive_pressure.saturating_add(ADAPTIVE_DEPTH_PRESSURE_CONTROL_BONUS);
            }
            if let Some(current) = active.as_ref() {
                finalize_active_assignment(shared, current, 0);
                *active = None;
            }
            queued.clear();
            clear_worker_pending_state(shared);
            let _ = ack.send(Ok(()));
            if active.is_none() && queued.is_empty() {
                drain_fence_waiters(fence_waiters, Ok(()));
            }
            true
        }
        WorkerCommand::Fence(ack) => {
            if active.is_none() && queued.is_empty() {
                let _ = ack.send(Ok(()));
            } else {
                *adaptive_pressure =
                    adaptive_pressure.saturating_add(ADAPTIVE_DEPTH_PRESSURE_CONTROL_BONUS);
                fence_waiters.push(ack);
            }
            true
        }
        WorkerCommand::Stop => false,
    }
}

fn replace_assignment_queue(
    shared: &NvidiaShared,
    active: &mut Option<ActiveAssignment>,
    queued: &mut VecDeque<WorkAssignment>,
    assignments: Vec<WorkAssignment>,
) {
    let pending_after_replace = assignments.len() as u64;
    if let Some(current) = active.as_ref() {
        finalize_active_assignment(shared, current, pending_after_replace);
    }
    *active = None;
    queued.clear();

    shared.error_emitted.store(false, Ordering::Release);

    if assignments.is_empty() {
        clear_worker_pending_state(shared);
        return;
    }

    let mut assignments = assignments.into_iter();
    let first = assignments
        .next()
        .expect("non-empty assignments should include an active item");
    queued.extend(assignments);
    activate_assignment(shared, active, first, queued.len() as u64 + 1);
}

fn activate_assignment(
    shared: &NvidiaShared,
    active: &mut Option<ActiveAssignment>,
    work: WorkAssignment,
    pending_work: u64,
) {
    shared
        .pending_work
        .store(pending_work.max(1), Ordering::Release);
    shared.active_lanes.store(0, Ordering::Release);
    shared
        .inflight_assignment_hashes
        .store(0, Ordering::Release);
    if let Ok(mut slot) = shared.inflight_assignment_started_at.lock() {
        *slot = Some(Instant::now());
    }
    *active = Some(ActiveAssignment::new(work));
}

fn clear_worker_pending_state(shared: &NvidiaShared) {
    shared.active_lanes.store(0, Ordering::Release);
    shared.pending_work.store(0, Ordering::Release);
    shared
        .inflight_assignment_hashes
        .store(0, Ordering::Release);
    if let Ok(mut slot) = shared.inflight_assignment_started_at.lock() {
        *slot = None;
    }
}

fn finalize_active_assignment(
    shared: &NvidiaShared,
    active: &ActiveAssignment,
    pending_after: u64,
) {
    shared.active_lanes.store(0, Ordering::Release);
    shared.pending_work.store(pending_after, Ordering::Release);
    shared
        .inflight_assignment_hashes
        .store(0, Ordering::Release);
    if let Ok(mut slot) = shared.inflight_assignment_started_at.lock() {
        *slot = None;
    }

    shared.completed_assignments.fetch_add(1, Ordering::Relaxed);
    shared
        .completed_assignment_hashes
        .fetch_add(active.hashes_done, Ordering::Relaxed);
    shared.completed_assignment_micros.fetch_add(
        active
            .started_at
            .elapsed()
            .as_micros()
            .min(u64::MAX as u128) as u64,
        Ordering::Relaxed,
    );
}

fn drain_fence_waiters(waiters: &mut Vec<Sender<Result<()>>>, result: Result<()>) {
    for waiter in waiters.drain(..) {
        let payload = result
            .as_ref()
            .map(|_| ())
            .map_err(|err| anyhow!("{err:#}"));
        let _ = waiter.send(payload);
    }
}

fn send_backend_event(shared: &NvidiaShared, event: BackendEvent) {
    let outbound = match shared.event_sink.read() {
        Ok(slot) => slot.clone(),
        Err(_) => None,
    };

    let Some(outbound) = outbound else {
        shared.dropped_events.fetch_add(1, Ordering::Relaxed);
        return;
    };

    match outbound.send_timeout(event, EVENT_SEND_WAIT) {
        Ok(()) => {}
        Err(SendTimeoutError::Timeout(_)) => {
            shared.dropped_events.fetch_add(1, Ordering::Relaxed);
        }
        Err(SendTimeoutError::Disconnected(_)) => {
            shared.dropped_events.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn emit_worker_error(shared: &NvidiaShared, instance_id: &AtomicU64, message: String) {
    if shared.error_emitted.swap(true, Ordering::AcqRel) {
        return;
    }

    send_backend_event(
        shared,
        BackendEvent::Error {
            backend_id: instance_id.load(Ordering::Acquire),
            backend: BACKEND_NAME,
            message,
        },
    );
}

fn cuda_driver_err(err: DriverError) -> anyhow::Error {
    anyhow!("CUDA driver error: {err:?}")
}

fn is_transient_cuda_error(msg: &str) -> bool {
    msg.contains("LAUNCH_TIMEOUT")
        || msg.contains("ECC_UNCORRECTABLE")
        || msg.contains("NOT_READY")
        || msg.contains("OPERATING_SYSTEM")
}

fn is_cuda_oom_error(err: &dyn std::fmt::Debug) -> bool {
    let msg = format!("{err:?}");
    msg.contains("OUT_OF_MEMORY") || msg.contains("OutOfMemory")
}

enum NvrtcModuleArtifact {
    Cubin(Vec<u8>),
    Ptx(String),
}

fn nvrtc_arch_target_unsupported(msg: &str) -> bool {
    let msg = msg.to_ascii_lowercase();
    msg.contains("unsupported gpu architecture")
        || msg.contains("invalid value for --gpu-architecture")
        || msg.contains("is not defined for option 'gpu-architecture'")
}

fn compile_cuda_module_with_nvrtc_fallback(
    source: &str,
    program_name: &str,
    cubin_options: &[String],
    ptx_options: &[String],
) -> Result<NvrtcModuleArtifact> {
    match compile_cubin_with_nvrtc(source, program_name, cubin_options) {
        Ok(cubin) => Ok(NvrtcModuleArtifact::Cubin(cubin)),
        Err(cubin_err) => match compile_ptx_with_nvrtc(source, program_name, ptx_options) {
            Ok(ptx) => Ok(NvrtcModuleArtifact::Ptx(ptx)),
            Err(ptx_err) => {
                if nvrtc_arch_target_unsupported(&format!("{cubin_err:#}")) {
                    bail!(
                        "CUBIN compile failed for this GPU architecture and PTX fallback also failed. \
                         For Maxwell/Pascal/Volta GPUs this usually means CUDA 13.x NVRTC is installed; \
                         use CUDA Toolkit/NVRTC 12.x for legacy-architecture builds. \
                         cubin_error: {cubin_err:#}; ptx_error: {ptx_err:#}"
                    );
                }
                bail!(
                    "CUBIN compile failed and PTX fallback failed; cubin_error: {cubin_err:#}; ptx_error: {ptx_err:#}"
                );
            }
        },
    }
}

fn compile_cubin_with_nvrtc(
    source: &str,
    program_name: &str,
    options: &[String],
) -> Result<Vec<u8>> {
    let source_c = CString::new(source)
        .map_err(|_| anyhow!("CUDA kernel source contains interior NUL byte"))?;
    let program_name_c = CString::new(program_name)
        .map_err(|_| anyhow!("CUDA program name contains interior NUL byte"))?;

    let program = nvrtc_result::create_program(&source_c, Some(&program_name_c))
        .map_err(|err| anyhow!("nvrtcCreateProgram failed: {err:?}"))?;

    let compile_result = unsafe { nvrtc_result::compile_program(program, options) };
    if let Err(err) = compile_result {
        let compile_log = unsafe { nvrtc_result::get_program_log(program).ok() }
            .map(|raw| nvrtc_log_to_string(&raw))
            .unwrap_or_default();
        let _ = unsafe { nvrtc_result::destroy_program(program) };
        if compile_log.is_empty() {
            bail!("nvrtcCompileProgram failed: {err:?}");
        }
        bail!("nvrtcCompileProgram failed: {err:?}; log: {compile_log}");
    }

    let cubin = unsafe {
        let mut cubin_size: usize = 0;
        nvrtc_sys::nvrtcGetCUBINSize(program, &mut cubin_size as *mut usize)
            .result()
            .map_err(|err| anyhow!("nvrtcGetCUBINSize failed: {err:?}"))?;
        if cubin_size == 0 {
            bail!("nvrtcGetCUBINSize returned zero bytes");
        }

        let mut cubin = vec![0u8; cubin_size];
        nvrtc_sys::nvrtcGetCUBIN(program, cubin.as_mut_ptr().cast::<c_char>())
            .result()
            .map_err(|err| anyhow!("nvrtcGetCUBIN failed: {err:?}"))?;
        cubin
    };

    unsafe { nvrtc_result::destroy_program(program) }
        .map_err(|err| anyhow!("nvrtcDestroyProgram failed: {err:?}"))?;

    Ok(cubin)
}

fn compile_ptx_with_nvrtc(source: &str, program_name: &str, options: &[String]) -> Result<String> {
    let source_c = CString::new(source)
        .map_err(|_| anyhow!("CUDA kernel source contains interior NUL byte"))?;
    let program_name_c = CString::new(program_name)
        .map_err(|_| anyhow!("CUDA program name contains interior NUL byte"))?;

    let program = nvrtc_result::create_program(&source_c, Some(&program_name_c))
        .map_err(|err| anyhow!("nvrtcCreateProgram failed: {err:?}"))?;

    let compile_result = unsafe { nvrtc_result::compile_program(program, options) };
    if let Err(err) = compile_result {
        let compile_log = unsafe { nvrtc_result::get_program_log(program).ok() }
            .map(|raw| nvrtc_log_to_string(&raw))
            .unwrap_or_default();
        let _ = unsafe { nvrtc_result::destroy_program(program) };
        if compile_log.is_empty() {
            bail!("nvrtcCompileProgram failed: {err:?}");
        }
        bail!("nvrtcCompileProgram failed: {err:?}; log: {compile_log}");
    }

    let ptx_raw = unsafe {
        nvrtc_result::get_ptx(program).map_err(|err| anyhow!("nvrtcGetPTX failed: {err:?}"))?
    };
    let ptx = if ptx_raw.is_empty() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(ptx_raw.as_ptr()) }
            .to_string_lossy()
            .to_string()
    };

    unsafe { nvrtc_result::destroy_program(program) }
        .map_err(|err| anyhow!("nvrtcDestroyProgram failed: {err:?}"))?;

    if ptx.trim().is_empty() {
        bail!("nvrtcGetPTX returned empty output");
    }
    Ok(ptx)
}

fn build_cubin_cache_key(
    source: &str,
    program_name: &str,
    options: &[String],
    compiler_identity: &str,
) -> String {
    let mut hasher = Blake2b512::new();
    hasher.update(NVIDIA_CUBIN_CACHE_SCHEMA_VERSION.to_le_bytes());
    hasher.update(program_name.as_bytes());
    hasher.update([0u8]);
    hasher.update(compiler_identity.as_bytes());
    hasher.update([0u8]);
    for option in options {
        hasher.update(option.as_bytes());
        hasher.update([0u8]);
    }
    hasher.update(source.as_bytes());
    hex::encode(hasher.finalize())
}

fn cuda_kernel_fingerprint() -> String {
    let mut hasher = Blake2b512::new();
    hasher.update(CUDA_KERNEL_SRC.as_bytes());
    hex::encode(hasher.finalize())
}

fn nvrtc_compiler_identity() -> String {
    let available = unsafe { nvrtc_sys::is_culib_present() };
    if !available {
        return "unavailable".to_string();
    }

    let mut major = 0;
    let mut minor = 0;
    let result = unsafe { nvrtc_sys::nvrtcVersion(&mut major, &mut minor) };
    if result.result().is_ok() {
        format!("nvrtc-{major}.{minor}")
    } else {
        "unknown".to_string()
    }
}

fn cubin_cache_file_path(cache_dir: &Path, cache_key: &str) -> PathBuf {
    cache_dir.join(format!("{cache_key}.cubin"))
}

fn load_cached_cubin(path: &Path) -> Option<Vec<u8>> {
    let cubin = fs::read(path).ok()?;
    if cubin.is_empty() {
        None
    } else {
        Some(cubin)
    }
}

fn persist_cached_cubin(path: &Path, cubin: &[u8]) -> Result<()> {
    if cubin.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create NVIDIA CUBIN cache directory '{}'",
                parent.display()
            )
        })?;
    }
    write_file_atomic(path, cubin)
        .with_context(|| format!("failed to write NVIDIA CUBIN cache '{}'", path.display()))?;
    Ok(())
}

/// Writes `payload` through a same-directory temp file and atomic rename, so
/// concurrent readers never observe a truncated file. Concurrent per-device
/// backend instances write these caches in parallel on multi-GPU startups.
fn write_file_atomic(path: &Path, payload: &[u8]) -> std::io::Result<()> {
    static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("cache");
    let tmp_path = path.with_file_name(format!(
        "{file_name}.tmp.{}.{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    if let Err(err) = fs::write(&tmp_path, payload) {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }
    fs::rename(&tmp_path, path).inspect_err(|_| {
        let _ = fs::remove_file(&tmp_path);
    })
}

fn derive_nvidia_cubin_cache_dir(autotune_config_path: &Path) -> PathBuf {
    let cache_parent = autotune_config_path.parent().unwrap_or(Path::new("."));
    let stem = autotune_config_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("seine.nvidia-autotune");
    cache_parent.join(format!("{stem}.cubin-cache"))
}

fn nvrtc_log_to_string(raw: &[c_char]) -> String {
    if raw.is_empty() {
        return String::new();
    }
    unsafe { CStr::from_ptr(raw.as_ptr()) }
        .to_string_lossy()
        .trim()
        .to_string()
}

fn normalize_assignment_batch(work: &[WorkAssignment]) -> Result<Vec<WorkAssignment>> {
    let Some(first) = work.first() else {
        return Ok(Vec::new());
    };

    let mut normalized = Vec::with_capacity(work.len());
    let mut expected_start = first.nonce_chunk.start_nonce;
    let mut total_nonce_count = 0u64;

    for (idx, assignment) in work.iter().enumerate() {
        ensure_compatible_template(&first.template, &assignment.template).with_context(|| {
            format!(
                "assignment batch item {} references a different template",
                idx + 1
            )
        })?;

        if assignment.nonce_chunk.start_nonce != expected_start {
            bail!(
                "assignment batch is not contiguous at item {} (expected nonce {}, got {})",
                idx + 1,
                expected_start,
                assignment.nonce_chunk.start_nonce
            );
        }
        if assignment.nonce_chunk.nonce_count == 0 {
            bail!("assignment batch item {} has zero nonce_count", idx + 1);
        }

        total_nonce_count = total_nonce_count
            .checked_add(assignment.nonce_chunk.nonce_count)
            .ok_or_else(|| anyhow!("assignment batch nonce_count overflow"))?;
        expected_start = expected_start.wrapping_add(assignment.nonce_chunk.nonce_count);
        normalized.push(assignment.clone());
    }

    if total_nonce_count == 0 {
        bail!("assignment batch nonce_count must be non-zero");
    }

    Ok(normalized)
}

fn ensure_compatible_template(first: &Arc<WorkTemplate>, second: &Arc<WorkTemplate>) -> Result<()> {
    if first.work_id != second.work_id {
        bail!(
            "mismatched work ids ({} vs {})",
            first.work_id,
            second.work_id
        );
    }
    if first.epoch != second.epoch {
        bail!("mismatched epochs ({} vs {})", first.epoch, second.epoch);
    }
    if first.target != second.target {
        bail!("mismatched target");
    }
    if first.stop_at != second.stop_at {
        bail!("mismatched stop_at");
    }
    if first.pause_on_solution != second.pause_on_solution {
        bail!("mismatched pause_on_solution");
    }
    if first.header_base.as_ref() != second.header_base.as_ref() {
        bail!("mismatched header_base");
    }
    Ok(())
}

pub(crate) fn query_nvidia_devices() -> Result<Vec<NvidiaDeviceInfo>> {
    let output = std::process::Command::new("nvidia-smi")
        .args([
            "--query-gpu=index,name,memory.total,memory.free",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .context("failed to execute nvidia-smi; ensure NVIDIA drivers are installed")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            bail!(
                "nvidia-smi returned non-zero exit status ({})",
                output.status
            );
        }
        bail!("nvidia-smi query failed: {stderr}");
    }

    let stdout = String::from_utf8(output.stdout).context("nvidia-smi output was not UTF-8")?;
    let devices = parse_nvidia_smi_query_output(&stdout)?;
    if devices.is_empty() {
        bail!("nvidia-smi reported no NVIDIA devices");
    }

    Ok(devices)
}

fn parse_nvidia_smi_query_output(raw: &str) -> Result<Vec<NvidiaDeviceInfo>> {
    let mut devices = Vec::new();
    for (line_idx, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let columns = line.split(',').map(str::trim).collect::<Vec<_>>();
        if columns.len() < 3 {
            bail!(
                "unexpected nvidia-smi output at line {}: '{line}'",
                line_idx + 1
            );
        }

        let index = columns[0].parse::<u32>().with_context(|| {
            format!(
                "invalid GPU index '{}' at line {}",
                columns[0],
                line_idx + 1
            )
        })?;
        let (name_columns_end, memory_total_mib, memory_free_mib) = if columns.len() >= 4 {
            let total_idx = columns.len() - 2;
            let free_idx = columns.len() - 1;
            let total = columns[total_idx].parse::<u64>().with_context(|| {
                format!(
                    "invalid GPU memory.total value '{}' at line {}",
                    columns[total_idx],
                    line_idx + 1
                )
            })?;
            let free = columns[free_idx].parse::<u64>().with_context(|| {
                format!(
                    "invalid GPU memory.free value '{}' at line {}",
                    columns[free_idx],
                    line_idx + 1
                )
            })?;
            (total_idx, total, Some(free))
        } else {
            let total = columns[columns.len() - 1].parse::<u64>().with_context(|| {
                format!(
                    "invalid GPU memory.total value '{}' at line {}",
                    columns[columns.len() - 1],
                    line_idx + 1
                )
            })?;
            (columns.len() - 1, total, None)
        };
        let name = columns[1..name_columns_end].join(",");
        let name = name.trim().to_string();
        if name.is_empty() {
            bail!("missing GPU name at line {}", line_idx + 1);
        }

        devices.push(NvidiaDeviceInfo {
            index,
            name,
            memory_total_mib,
            memory_free_mib,
        });
    }

    Ok(devices)
}

fn derive_memory_budget_mib(memory_total_mib: u64, memory_free_mib: Option<u64>) -> u64 {
    let total = memory_total_mib.max(1);
    let reserve = (total / NVIDIA_MEMORY_RESERVE_RATIO_DENOM)
        .max(NVIDIA_MEMORY_RESERVE_MIB_FLOOR)
        .min(total.saturating_sub(1).max(1));

    let available = memory_free_mib.unwrap_or(total).max(1).min(total);
    available.saturating_sub(reserve).max(1)
}

fn bucket_memory_budget_mib(memory_budget_mib: u64) -> u64 {
    let bucket = NVIDIA_AUTOTUNE_MEMORY_BUCKET_MIB.max(1);
    let rounded = ((memory_budget_mib.saturating_add(bucket / 2)) / bucket).saturating_mul(bucket);
    rounded.max(bucket)
}

fn lane_capacity_tier_from_lanes(lanes: usize) -> u32 {
    match lanes.max(1) {
        1 => 1,
        2 | 3 => 2,
        4..=7 => 4,
        8..=15 => 8,
        16..=23 => 16,
        24..=31 => 24,
        32..=47 => 32,
        48..=63 => 48,
        _ => 64,
    }
}

fn derive_lane_capacity_tier(memory_budget_mib: u64, m_cost_kib: u32) -> u32 {
    let lane_bytes = CPU_LANE_MEMORY_BYTES.max(u64::from(m_cost_kib).saturating_mul(1024));
    let memory_budget_bytes = memory_budget_mib.saturating_mul(1024 * 1024);
    let lanes = usize::try_from((memory_budget_bytes / lane_bytes).max(1))
        .unwrap_or(usize::MAX)
        .min(MAX_LANES_HARD_LIMIT)
        .max(1);
    lane_capacity_tier_from_lanes(lanes)
}

/// Whether SEINE_NVIDIA_DEFINES selects the original fill loop, which stores
/// every block.
fn fill_v1_requested() -> bool {
    std::env::var("SEINE_NVIDIA_DEFINES")
        .map(|defines| {
            defines
                .split(',')
                .map(str::trim)
                .any(|define| define == "SEINE_FILL_V1" || define == "SEINE_FILL_V1=1")
        })
        .unwrap_or(false)
}

/// Lane counts to try, best first, each with the blocks stored per lane. One
/// lane more than fits at full size is tried first when leaving late odd blocks
/// unstored (at most NVIDIA_MAX_SKIPPED_LANE_FRACTION of them) makes room;
/// smaller counts store every block.
fn plan_lane_storage(
    m_blocks: u32,
    usable_bytes: u64,
    full_lanes: usize,
    max_lanes_override: Option<usize>,
    skip_supported: bool,
) -> Vec<(usize, u32)> {
    let packed_blocks = |lanes: usize| -> Option<u32> {
        let per_lane_blocks = usable_bytes / (lanes.max(1) as u64) / 1024;
        let aligned = per_lane_blocks / u64::from(NVIDIA_LANE_STRIDE_ALIGN_BLOCKS)
            * u64::from(NVIDIA_LANE_STRIDE_ALIGN_BLOCKS);
        let aligned = u32::try_from(aligned.min(u64::from(m_blocks))).ok()?;
        let min_blocks =
            (f64::from(m_blocks) * (1.0 - NVIDIA_MAX_SKIPPED_LANE_FRACTION)).ceil() as u32;
        (aligned >= min_blocks).then_some(aligned)
    };
    let full_lanes = full_lanes.clamp(1, MAX_LANES_HARD_LIMIT);
    let (packed_lanes, top_lanes) = match max_lanes_override {
        Some(requested) => {
            let requested = requested.clamp(1, MAX_LANES_HARD_LIMIT);
            (requested, requested)
        }
        None => ((full_lanes + 1).min(MAX_LANES_HARD_LIMIT), full_lanes),
    };
    let mut plans = Vec::new();
    if skip_supported && packed_lanes > full_lanes {
        if let Some(blocks) = packed_blocks(packed_lanes) {
            // Smaller packings for the same lane count, in case the allocation
            // does not fit.
            let min_blocks =
                (f64::from(m_blocks) * (1.0 - NVIDIA_MAX_SKIPPED_LANE_FRACTION)).ceil() as u32;
            let step = 16 * NVIDIA_LANE_STRIDE_ALIGN_BLOCKS;
            plans.extend(
                (0..5)
                    .map(|retry| blocks.saturating_sub(retry * step))
                    .take_while(|&stored| stored >= min_blocks)
                    .map(|stored| (packed_lanes, stored)),
            );
        }
    }
    plans.extend((1..=top_lanes).rev().map(|lanes| (lanes, m_blocks)));
    plans
}

/// Makes threads that wait on this device's primary context sleep instead of
/// spinning, so a waiting GPU worker leaves its CPU core to other work. Best
/// effort: on failure the driver keeps its default scheduling.
fn use_blocking_sync(device_index: u32) {
    use cudarc::driver::{result as driver_result, sys};
    let _ = driver_result::init();
    if let Ok(device) = driver_result::device::get(device_index as std::ffi::c_int) {
        let flags = sys::CUctx_flags::CU_CTX_SCHED_BLOCKING_SYNC as std::ffi::c_uint;
        let _ = unsafe { sys::cuDevicePrimaryCtxSetFlags_v2(device, flags) };
    }
}

/// Reference indices of the data-independent half of pass 0 (indexed by block,
/// entries 0 and 1 unused). They do not depend on the password, so one table
/// serves every lane and hash.
fn build_cuda_ref_table(
    stream: &Arc<CudaStream>,
    kernel: &CudaFunction,
    m_blocks: u32,
    t_cost: u32,
) -> std::result::Result<CudaSlice<u32>, DriverError> {
    let entries = (m_blocks / 2).max(1) as usize;
    let mut table = stream.alloc_zeros::<u32>(entries)?;
    let address_blocks = (m_blocks / 4 / 128).saturating_mul(2).max(1);
    let cfg = LaunchConfig {
        grid_dim: (address_blocks, 1, 1),
        block_dim: (KERNEL_THREADS, 1, 1),
        shared_mem_bytes: 0,
    };
    unsafe {
        let mut launch = stream.launch_builder(kernel);
        launch.arg(&mut table).arg(&m_blocks).arg(&t_cost);
        launch.launch(cfg)?;
    }
    stream.synchronize()?;
    Ok(table)
}

fn try_allocate_cuda_buffers(
    stream: &Arc<CudaStream>,
    m_blocks: u32,
    lanes: usize,
    hashes_per_launch_per_lane: usize,
) -> std::result::Result<
    (
        CudaSlice<u64>,
        CudaSlice<u64>,
        CudaSlice<u64>,
        CudaSlice<u64>,
        CudaSlice<u8>,
        CudaSlice<u8>,
        CudaSlice<u32>,
        CudaSlice<u32>,
        CudaSlice<u32>,
    ),
    DriverError,
> {
    let lane_words_u64 = u64::from(m_blocks).saturating_mul(128);
    let total_lane_words_u64 = lane_words_u64.saturating_mul(lanes as u64);
    let total_lane_words = usize::try_from(total_lane_words_u64).unwrap_or(usize::MAX);
    let launch_capacity = lanes
        .saturating_mul(hashes_per_launch_per_lane.max(1))
        .max(1);

    let lane_memory = unsafe { stream.alloc::<u64>(total_lane_words) }?;
    let seed_blocks = unsafe { stream.alloc::<u64>(launch_capacity.saturating_mul(256)) }?;
    let last_blocks = unsafe { stream.alloc::<u64>(launch_capacity.saturating_mul(128)) }?;
    let nonce_input = unsafe { stream.alloc::<u64>(launch_capacity) }?;
    let header_base_input =
        unsafe { stream.alloc::<u8>(blocknet_pow_spec::MAX_POW_HEADER_BASE_LEN) }?;
    let target_input = unsafe { stream.alloc::<u8>(POW_OUTPUT_LEN) }?;
    let cancel_flag = unsafe { stream.alloc::<u32>(1) }?;
    let completed_iters = unsafe { stream.alloc::<u32>(1) }?;
    let found_index = unsafe { stream.alloc::<u32>(1) }?;
    Ok((
        lane_memory,
        seed_blocks,
        last_blocks,
        nonce_input,
        header_base_input,
        target_input,
        cancel_flag,
        completed_iters,
        found_index,
    ))
}

fn probe_cuda_lane_memory(
    stream: &Arc<CudaStream>,
    kernel: &CudaFunction,
    lane_memory: &mut CudaSlice<u64>,
    lane_stride_words: u64,
    lanes: usize,
) -> std::result::Result<(), DriverError> {
    let lanes_u32 = u32::try_from(lanes).unwrap_or(u32::MAX);
    let cfg = LaunchConfig {
        grid_dim: (lanes_u32, 1, 1),
        block_dim: (1, 1, 1),
        shared_mem_bytes: 0,
    };

    unsafe {
        let mut launch = stream.launch_builder(kernel);
        launch
            .arg(lane_memory)
            .arg(&lanes_u32)
            .arg(&lane_stride_words);
        launch.launch(cfg)?;
    }

    stream.synchronize()?;
    Ok(())
}

fn build_nvidia_autotune_key(
    selected: &NvidiaDeviceInfo,
    hashes_per_launch_per_lane_cap: u32,
) -> NvidiaAutotuneKey {
    let (m_cost_kib, t_cost) = pow_params()
        .map(|params| (params.m_cost(), params.t_cost()))
        .unwrap_or((0, 0));
    let memory_budget_mib = bucket_memory_budget_mib(derive_memory_budget_mib(
        selected.memory_total_mib,
        selected.memory_free_mib,
    ));
    let lane_capacity_tier = derive_lane_capacity_tier(memory_budget_mib, m_cost_kib);
    let (compute_cap_major, compute_cap_minor) =
        query_cuda_compute_capability(selected.index).unwrap_or((0, 0));
    NvidiaAutotuneKey {
        device_name: selected.name.clone(),
        memory_total_mib: selected.memory_total_mib,
        memory_budget_mib,
        lane_capacity_tier,
        compute_cap_major,
        compute_cap_minor,
        m_cost_kib,
        t_cost,
        kernel_threads: KERNEL_THREADS,
        hashes_per_launch_per_lane_cap: hashes_per_launch_per_lane_cap.max(1),
        os: std::env::consts::OS.to_string(),
        kernel_version: crate::runtime_identity::runtime_kernel_version().unwrap_or_default(),
        runtime_environment: crate::runtime_identity::runtime_environment(),
        build_fingerprint: crate::runtime_identity::build_fingerprint(),
        cuda_kernel_fingerprint: cuda_kernel_fingerprint(),
        nvrtc_version: nvrtc_compiler_identity(),
    }
}

/// PCI address of the CUDA device at `device_index`, read from the driver so it
/// names the device CUDA actually opens for that index.
fn query_cuda_pci_address(device_index: u32) -> Option<String> {
    use cudarc::driver::{result as driver_result, sys::CUdevice_attribute};

    // Calling into a driver library that failed to load panics.
    if !unsafe { cudarc::driver::sys::is_culib_present() } {
        return None;
    }
    driver_result::init().ok()?;
    let device = driver_result::device::get(i32::try_from(device_index).ok()?).ok()?;
    let attribute = |attrib| unsafe { driver_result::device::get_attribute(device, attrib) }.ok();
    let domain = attribute(CUdevice_attribute::CU_DEVICE_ATTRIBUTE_PCI_DOMAIN_ID)?;
    let bus = attribute(CUdevice_attribute::CU_DEVICE_ATTRIBUTE_PCI_BUS_ID)?;
    let slot = attribute(CUdevice_attribute::CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID)?;
    Some(format!(
        "{:04x}:{:02x}:{:02x}.0",
        domain.max(0),
        bus.max(0),
        slot.max(0)
    ))
}

fn query_cuda_compute_capability(device_index: u32) -> Option<(u32, u32)> {
    let ctx = CudaContext::new(device_index as usize).ok()?;
    let (major, minor) = ctx.compute_capability().ok()?;
    Some((u32::try_from(major).ok()?, u32::try_from(minor).ok()?))
}

fn empty_nvidia_autotune_cache() -> NvidiaAutotuneCache {
    NvidiaAutotuneCache {
        schema_version: NVIDIA_AUTOTUNE_SCHEMA_VERSION,
        records: Vec::new(),
    }
}

fn load_nvidia_autotune_cache(path: &Path) -> Option<NvidiaAutotuneCache> {
    let raw = fs::read_to_string(path).ok()?;
    let parsed = serde_json::from_str::<NvidiaAutotuneCache>(&raw).ok()?;
    if parsed.schema_version != NVIDIA_AUTOTUNE_SCHEMA_VERSION {
        return None;
    }
    Some(parsed)
}

fn load_nvidia_cached_tuning(
    path: &Path,
    key: &NvidiaAutotuneKey,
) -> Result<NvidiaKernelTuning, &'static str> {
    let raw = fs::read(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            "no saved tuning result"
        } else {
            "saved tuning results could not be read"
        }
    })?;
    let cache: NvidiaAutotuneCache =
        serde_json::from_slice(&raw).map_err(|_| "saved tuning results are invalid")?;
    if cache.schema_version != NVIDIA_AUTOTUNE_SCHEMA_VERSION {
        return Err("cache format changed");
    }
    let exact = cache
        .records
        .iter()
        .filter(|record| &record.key == key && record.max_rregcount > 0)
        .max_by_key(|record| record.timestamp_unix_secs);
    let record = match exact {
        Some(record) => record,
        None => {
            // Fallback: tolerate memory-budget/lane-tier drift between runs
            // (for example, desktop VRAM pressure changes) and reuse the closest
            // compatible tuning for the same device/arch/PoW parameters.
            let mut best: Option<&NvidiaAutotuneRecord> = None;
            for candidate in cache.records.iter().filter(|record| {
                record.max_rregcount > 0 && nvidia_autotune_key_compatible(&record.key, key)
            }) {
                match best {
                    None => best = Some(candidate),
                    Some(current) => {
                        let candidate_distance = memory_budget_distance(
                            candidate.key.memory_budget_mib,
                            key.memory_budget_mib,
                        );
                        let current_distance = memory_budget_distance(
                            current.key.memory_budget_mib,
                            key.memory_budget_mib,
                        );
                        if candidate_distance < current_distance
                            || (candidate_distance == current_distance
                                && candidate.timestamp_unix_secs > current.timestamp_unix_secs)
                        {
                            best = Some(candidate);
                        }
                    }
                }
            }
            best.ok_or_else(|| {
                // Compare only this GPU's newest record. Another GPU in a
                // shared cache must not be reported as a changed build/runtime.
                cache
                    .records
                    .iter()
                    .filter(|record| same_nvidia_device(&record.key, key))
                    .max_by_key(|record| record.timestamp_unix_secs)
                    .map_or("no saved tuning result for this GPU", |record| {
                        nvidia_autotune_key_change(&record.key, key)
                            .unwrap_or("saved tuning result is invalid")
                    })
            })?
        }
    };
    Ok(NvidiaKernelTuning {
        max_rregcount: record.max_rregcount,
        block_loop_unroll: record.block_loop_unroll,
        hashes_per_launch_per_lane: record.hashes_per_launch_per_lane.max(1),
        max_lanes_hint: record.max_lanes_hint,
    })
}

fn same_nvidia_device(lhs: &NvidiaAutotuneKey, rhs: &NvidiaAutotuneKey) -> bool {
    lhs.device_name == rhs.device_name
        && lhs.memory_total_mib == rhs.memory_total_mib
        && lhs.compute_cap_major == rhs.compute_cap_major
        && lhs.compute_cap_minor == rhs.compute_cap_minor
}

fn nvidia_autotune_key_change(
    lhs: &NvidiaAutotuneKey,
    rhs: &NvidiaAutotuneKey,
) -> Option<&'static str> {
    if !same_nvidia_device(lhs, rhs) {
        Some("GPU configuration changed")
    } else if lhs.build_fingerprint != rhs.build_fingerprint {
        Some("miner build changed")
    } else if lhs.cuda_kernel_fingerprint != rhs.cuda_kernel_fingerprint {
        Some("CUDA kernel changed")
    } else if lhs.nvrtc_version != rhs.nvrtc_version {
        Some("CUDA compiler changed")
    } else if lhs.os != rhs.os
        || lhs.kernel_version != rhs.kernel_version
        || lhs.runtime_environment != rhs.runtime_environment
    {
        Some("operating system or runtime changed")
    } else if lhs.m_cost_kib != rhs.m_cost_kib
        || lhs.t_cost != rhs.t_cost
        || lhs.kernel_threads != rhs.kernel_threads
        || lhs.hashes_per_launch_per_lane_cap != rhs.hashes_per_launch_per_lane_cap
    {
        Some("mining or tuning settings changed")
    } else {
        None
    }
}

fn nvidia_autotune_key_compatible(lhs: &NvidiaAutotuneKey, rhs: &NvidiaAutotuneKey) -> bool {
    nvidia_autotune_key_change(lhs, rhs).is_none()
}

fn memory_budget_distance(lhs: u64, rhs: u64) -> u64 {
    lhs.max(rhs) - lhs.min(rhs)
}

fn persist_nvidia_autotune_record(
    path: &Path,
    key: NvidiaAutotuneKey,
    tuning: NvidiaKernelTuning,
    measured_hps: f64,
    autotune_secs: u64,
    autotune_samples: u32,
    candidate_trace: Vec<NvidiaAutotuneCandidateTrace>,
    autotune_elapsed_millis: u64,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create NVIDIA autotune cache directory '{}'",
                parent.display()
            )
        })?;
    }
    // Serializes concurrent load-modify-write cycles: multi-GPU startups
    // autotune per-device instances in parallel, and an unlocked last writer
    // would discard the other instances' records. `None` (locking unsupported
    // on this filesystem) degrades to the old unsynchronized behavior rather
    // than dropping the record. Held until this function returns.
    let _cache_lock = lock_nvidia_autotune_cache(path);
    let mut cache = load_nvidia_autotune_cache(path).unwrap_or_else(empty_nvidia_autotune_cache);
    if cache.schema_version != NVIDIA_AUTOTUNE_SCHEMA_VERSION {
        cache = empty_nvidia_autotune_cache();
    }

    let timestamp_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let updated = NvidiaAutotuneRecord {
        key: key.clone(),
        max_rregcount: tuning.max_rregcount.max(1),
        block_loop_unroll: tuning.block_loop_unroll,
        hashes_per_launch_per_lane: tuning.hashes_per_launch_per_lane.max(1),
        max_lanes_hint: tuning.max_lanes_hint.filter(|lanes| *lanes > 0),
        measured_hps: if measured_hps.is_finite() {
            measured_hps.max(0.0)
        } else {
            0.0
        },
        autotune_secs: autotune_secs.max(1),
        autotune_samples: autotune_samples.max(1),
        candidate_trace,
        autotune_elapsed_millis,
        timestamp_unix_secs,
    };
    if let Some(existing) = cache.records.iter_mut().find(|record| record.key == key) {
        *existing = updated;
    } else {
        cache.records.push(updated);
    }

    let payload = serde_json::to_string_pretty(&cache)?;
    write_file_atomic(path, payload.as_bytes())
        .with_context(|| format!("failed to write NVIDIA autotune cache '{}'", path.display()))?;
    Ok(())
}

/// Best-effort exclusive advisory lock on a `<cache>.lock` sidecar file.
fn lock_nvidia_autotune_cache(path: &Path) -> Option<fs::File> {
    let mut lock_name = path.as_os_str().to_os_string();
    lock_name.push(".lock");
    let lock_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(PathBuf::from(lock_name))
        .ok()?;
    lock_file.lock().ok()?;
    Some(lock_file)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
struct NvidiaAutotuneSampleScore {
    throughput_hps: f64,
    counted_hps: f64,
    elapsed_secs: f64,
}

#[derive(Debug, Clone, Copy)]
struct NvidiaAutotuneCandidateScore {
    tuning: NvidiaKernelTuning,
    counted_median: f64,
    counted_mean: f64,
    throughput_median: f64,
    throughput_mean: f64,
}

fn median_from_sorted(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    if values.len().is_multiple_of(2) {
        let mid = values.len() / 2;
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[values.len() / 2]
    }
}

fn measure_nvidia_kernel_tuning_hps(
    selected: &NvidiaDeviceInfo,
    tuning: NvidiaKernelTuning,
    secs: u64,
    cubin_cache_dir: &Path,
) -> Result<NvidiaAutotuneSampleScore> {
    let mut engine = CudaArgon2Engine::new(
        selected.index,
        selected.memory_total_mib,
        selected.memory_free_mib,
        tuning,
        tuning.max_lanes_hint,
        tuning.hashes_per_launch_per_lane,
        false,
        cubin_cache_dir,
    )?;
    let header = [0u8; POW_HEADER_BASE_LEN];
    let start = Instant::now();
    let stop_at = start + Duration::from_secs(secs.max(1));
    let mut nonce_cursor = 0u64;
    let mut total_hashes = 0u64;
    let mut counted_hashes = 0.0f64;
    let mut nonces = vec![0u64; engine.max_hashes_per_launch()];

    while Instant::now() < stop_at {
        let launch_started = Instant::now();
        let batch_hashes = engine.max_hashes_per_launch().max(1);
        if nonces.len() < batch_hashes {
            nonces.resize(batch_hashes, 0);
        }
        for nonce in nonces.iter_mut().take(batch_hashes) {
            *nonce = nonce_cursor;
            nonce_cursor = nonce_cursor.wrapping_add(1);
        }
        let done = engine.run_fill_batch(&header, &nonces[..batch_hashes], None)?;
        let launch_finished = Instant::now();
        let hashes_done = done.hashes_done as u64;
        let hashes_done_f64 = hashes_done as f64;
        total_hashes = total_hashes.saturating_add(hashes_done);
        if launch_finished <= stop_at {
            counted_hashes += hashes_done_f64;
        } else if launch_started < stop_at && hashes_done_f64 > 0.0 {
            let launch_elapsed = launch_finished
                .saturating_duration_since(launch_started)
                .as_secs_f64()
                .max(1e-9);
            let in_window = stop_at
                .saturating_duration_since(launch_started)
                .as_secs_f64()
                .clamp(0.0, launch_elapsed);
            let window_fraction = (in_window / launch_elapsed).clamp(0.0, 1.0);
            counted_hashes += hashes_done_f64 * window_fraction;
        }
    }

    let elapsed = start.elapsed().as_secs_f64().max(1e-6);
    Ok(NvidiaAutotuneSampleScore {
        throughput_hps: total_hashes as f64 / elapsed,
        counted_hps: counted_hashes / elapsed,
        elapsed_secs: elapsed,
    })
}

fn blackwell_lane_preference(max_lanes_hint: Option<usize>) -> usize {
    max_lanes_hint.unwrap_or(usize::MAX)
}

fn blackwell_regcap_distance(max_rregcount: u32) -> u32 {
    max_rregcount.abs_diff(BLACKWELL_AUTOTUNE_PREFERRED_RREGCOUNT)
}

fn strict_nvidia_autotune_candidate_beats(
    candidate: &NvidiaAutotuneCandidateScore,
    best: &NvidiaAutotuneCandidateScore,
) -> bool {
    const EPS: f64 = 1e-9;
    if candidate.counted_median > best.counted_median + EPS {
        true
    } else if (candidate.counted_median - best.counted_median).abs() <= EPS {
        if candidate.counted_mean > best.counted_mean + EPS {
            true
        } else if (candidate.counted_mean - best.counted_mean).abs() <= EPS {
            if candidate.throughput_median > best.throughput_median + EPS {
                true
            } else if (candidate.throughput_median - best.throughput_median).abs() <= EPS {
                candidate.throughput_mean > best.throughput_mean + EPS
            } else {
                false
            }
        } else {
            false
        }
    } else {
        false
    }
}

fn nvidia_autotune_candidate_beats(
    candidate: &NvidiaAutotuneCandidateScore,
    best: &NvidiaAutotuneCandidateScore,
    compute_cap_major: u32,
) -> bool {
    if compute_cap_major >= 12 {
        let tie_margin = candidate
            .counted_median
            .abs()
            .max(best.counted_median.abs())
            .mul_add(BLACKWELL_AUTOTUNE_SCORE_TIE_FRAC, 1e-9);
        if candidate.counted_median > best.counted_median + tie_margin {
            return true;
        }
        if best.counted_median > candidate.counted_median + tie_margin {
            return false;
        }
        if candidate.tuning.hashes_per_launch_per_lane != best.tuning.hashes_per_launch_per_lane {
            return candidate.tuning.hashes_per_launch_per_lane
                < best.tuning.hashes_per_launch_per_lane;
        }
        let candidate_lanes = blackwell_lane_preference(candidate.tuning.max_lanes_hint);
        let best_lanes = blackwell_lane_preference(best.tuning.max_lanes_hint);
        if candidate_lanes != best_lanes {
            return candidate_lanes > best_lanes;
        }
        let candidate_regcap_distance = blackwell_regcap_distance(candidate.tuning.max_rregcount);
        let best_regcap_distance = blackwell_regcap_distance(best.tuning.max_rregcount);
        if candidate_regcap_distance != best_regcap_distance {
            return candidate_regcap_distance < best_regcap_distance;
        }
    }
    strict_nvidia_autotune_candidate_beats(candidate, best)
}

fn estimated_lane_capacity(selected: &NvidiaDeviceInfo, m_cost_kib: u32) -> usize {
    let lane_bytes = CPU_LANE_MEMORY_BYTES.max(u64::from(m_cost_kib).saturating_mul(1024));
    let memory_budget_mib =
        derive_memory_budget_mib(selected.memory_total_mib, selected.memory_free_mib);
    let memory_budget_bytes = memory_budget_mib.saturating_mul(1024 * 1024);
    usize::try_from((memory_budget_bytes / lane_bytes).max(1))
        .unwrap_or(usize::MAX)
        .min(MAX_LANES_HARD_LIMIT)
        .max(1)
}

fn build_autotune_hash_depth_candidates(max_hashes_per_launch_per_lane: u32) -> Vec<u32> {
    let max_hashes = max_hashes_per_launch_per_lane.max(1);
    let mut candidates = vec![max_hashes, 1];
    if max_hashes > 1 {
        candidates.push(max_hashes.saturating_sub(1));
    }

    let mut reduced = max_hashes;
    while reduced > 1 {
        reduced = (reduced.saturating_add(1) / 2).max(1);
        candidates.push(reduced);
    }

    if max_hashes > 2 {
        let max = max_hashes as u64;
        candidates.push(((max.saturating_mul(3).saturating_add(3)) / 4) as u32);
        candidates.push(((max.saturating_mul(2).saturating_add(2)) / 3) as u32);
    }
    if max_hashes > 3 {
        let max = max_hashes as u64;
        candidates.push(((max.saturating_add(2)) / 3) as u32);
    }
    candidates.sort_unstable();
    candidates.dedup();
    candidates.reverse();
    candidates
}

fn nvidia_autotune_regcap_candidates(compute_cap_major: u32, device_name: &str) -> &'static [u32] {
    if compute_cap_major == 12
        && device_name
            .trim()
            .eq_ignore_ascii_case("NVIDIA GeForce RTX 5090")
    {
        NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_RTX_5090
    } else if compute_cap_major == 0 {
        NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_LEGACY
    } else if compute_cap_major >= 8 {
        NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_AMPERE_PLUS
    } else {
        NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_LEGACY
    }
}

fn build_autotune_lane_candidates(
    selected: &NvidiaDeviceInfo,
    forced_max_lanes: Option<usize>,
    m_cost_kib: u32,
) -> Vec<Option<usize>> {
    if let Some(forced) = forced_max_lanes {
        return vec![Some(forced.max(1).min(MAX_LANES_HARD_LIMIT))];
    }

    let estimated = estimated_lane_capacity(selected, m_cost_kib);
    let mut candidates = vec![None, Some(estimated)];
    if estimated >= 4 {
        candidates.push(Some((estimated.saturating_mul(3) / 4).max(1)));
    }
    if estimated >= 8 {
        candidates.push(Some((estimated / 2).max(1)));
    }
    candidates.sort_unstable_by(|a, b| {
        let lhs = a.unwrap_or(usize::MAX);
        let rhs = b.unwrap_or(usize::MAX);
        rhs.cmp(&lhs)
    });
    candidates.dedup();
    candidates
}

fn autotune_nvidia_kernel_tuning(
    selected: &NvidiaDeviceInfo,
    cache_path: &Path,
    autotune_secs: u64,
    autotune_samples: u32,
    max_lanes_override: Option<usize>,
    hashes_per_launch_per_lane: u32,
) -> Result<NvidiaKernelTuning> {
    let autotune_started = Instant::now();
    let cubin_cache_dir = derive_nvidia_cubin_cache_dir(cache_path);
    let sample_count = autotune_samples.max(1);
    let (compute_cap_major, _) = query_cuda_compute_capability(selected.index).unwrap_or((0, 0));
    let regcap_candidates = nvidia_autotune_regcap_candidates(compute_cap_major, &selected.name);
    let (m_cost_kib, _) = pow_params()
        .map(|params| (params.m_cost(), params.t_cost()))
        .unwrap_or((0, 0));
    let lane_candidates = build_autotune_lane_candidates(selected, max_lanes_override, m_cost_kib);
    let hash_depth_candidates =
        build_autotune_hash_depth_candidates(hashes_per_launch_per_lane.max(1));

    // Stage 1: sweep regcap with default hash depth and lane hint.
    let default_depth = hashes_per_launch_per_lane.max(1);
    let mut best: Option<NvidiaAutotuneCandidateScore> = None;
    let mut candidate_trace = Vec::new();

    let mut evaluate_candidate = |candidate: NvidiaKernelTuning,
                                  best: &mut Option<NvidiaAutotuneCandidateScore>|
     -> bool {
        let candidate_started = Instant::now();
        let mut samples = Vec::with_capacity(sample_count as usize);
        let mut failed_samples = 0u32;
        for _ in 0..sample_count {
            let measured = match measure_nvidia_kernel_tuning_hps(
                selected,
                candidate,
                autotune_secs,
                &cubin_cache_dir,
            ) {
                Ok(score) if score.counted_hps.is_finite() && score.throughput_hps.is_finite() => {
                    score
                }
                _ => {
                    failed_samples = failed_samples.saturating_add(1);
                    continue;
                }
            };
            samples.push(NvidiaAutotuneSampleScore {
                counted_hps: measured.counted_hps.max(0.0),
                throughput_hps: measured.throughput_hps.max(0.0),
                elapsed_secs: measured.elapsed_secs.max(0.0),
            });
        }
        let elapsed_millis =
            u64::try_from(candidate_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        if samples.is_empty() {
            candidate_trace.push(NvidiaAutotuneCandidateTrace {
                tuning: candidate,
                samples,
                failed_samples,
                counted_median_hps: None,
                counted_mean_hps: None,
                throughput_median_hps: None,
                throughput_mean_hps: None,
                elapsed_millis,
            });
            return false;
        }
        let mut counted_samples = samples
            .iter()
            .map(|sample| sample.counted_hps)
            .collect::<Vec<_>>();
        let mut throughput_samples = samples
            .iter()
            .map(|sample| sample.throughput_hps)
            .collect::<Vec<_>>();
        counted_samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        throughput_samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let counted_median = median_from_sorted(&counted_samples);
        let counted_mean = counted_samples.iter().sum::<f64>() / counted_samples.len() as f64;
        let throughput_median = median_from_sorted(&throughput_samples);
        let throughput_mean =
            throughput_samples.iter().sum::<f64>() / throughput_samples.len() as f64;
        let candidate_score = NvidiaAutotuneCandidateScore {
            tuning: candidate,
            counted_median,
            counted_mean,
            throughput_median,
            throughput_mean,
        };
        candidate_trace.push(NvidiaAutotuneCandidateTrace {
            tuning: candidate,
            samples,
            failed_samples,
            counted_median_hps: Some(counted_median),
            counted_mean_hps: Some(counted_mean),
            throughput_median_hps: Some(throughput_median),
            throughput_mean_hps: Some(throughput_mean),
            elapsed_millis,
        });
        let should_replace = match best {
            None => true,
            Some(best_score) => {
                nvidia_autotune_candidate_beats(&candidate_score, best_score, compute_cap_major)
            }
        };
        if should_replace {
            *best = Some(candidate_score);
            true
        } else {
            false
        }
    };

    if compute_cap_major >= 12 {
        for depth in hash_depth_candidates.iter().copied() {
            for &max_rregcount in regcap_candidates {
                let candidate = NvidiaKernelTuning {
                    max_rregcount,
                    block_loop_unroll: false,
                    hashes_per_launch_per_lane: depth,
                    max_lanes_hint: None,
                };
                evaluate_candidate(candidate, &mut best);
            }
        }
    } else {
        // Stage 1: find best regcap
        for &max_rregcount in regcap_candidates {
            let candidate = NvidiaKernelTuning {
                max_rregcount,
                block_loop_unroll: false,
                hashes_per_launch_per_lane: default_depth,
                max_lanes_hint: None,
            };
            evaluate_candidate(candidate, &mut best);
        }

        let best_regcap = best
            .as_ref()
            .map(|score| score.tuning.max_rregcount)
            .unwrap_or(DEFAULT_NVIDIA_MAX_RREGCOUNT);

        // Stage 2: find best hash depth using best regcap
        for depth in hash_depth_candidates.iter().copied() {
            if depth == default_depth {
                continue; // already tested in stage 1
            }
            let candidate = NvidiaKernelTuning {
                max_rregcount: best_regcap,
                block_loop_unroll: false,
                hashes_per_launch_per_lane: depth,
                max_lanes_hint: None,
            };
            evaluate_candidate(candidate, &mut best);
        }
    }

    let best_regcap = best
        .as_ref()
        .map(|score| score.tuning.max_rregcount)
        .unwrap_or(DEFAULT_NVIDIA_MAX_RREGCOUNT);
    let best_depth = best
        .as_ref()
        .map(|score| score.tuning.hashes_per_launch_per_lane)
        .unwrap_or(default_depth);

    // Stage 3: find best lane hint using best regcap + depth
    for max_lanes_hint in lane_candidates.iter().copied() {
        if max_lanes_hint.is_none() {
            continue; // already tested
        }
        let candidate = NvidiaKernelTuning {
            max_rregcount: best_regcap,
            block_loop_unroll: false,
            hashes_per_launch_per_lane: best_depth,
            max_lanes_hint,
        };
        evaluate_candidate(candidate, &mut best);
    }

    let selected_score = best.ok_or_else(|| {
        anyhow!(
            "NVIDIA autotune failed for device {} (index {})",
            selected.name,
            selected.index
        )
    })?;
    let selected_tuning = selected_score.tuning;

    let key = build_nvidia_autotune_key(selected, hashes_per_launch_per_lane.max(1));
    let _ = persist_nvidia_autotune_record(
        cache_path,
        key,
        selected_tuning,
        selected_score.counted_median,
        autotune_secs,
        sample_count,
        candidate_trace,
        u64::try_from(autotune_started.elapsed().as_millis()).unwrap_or(u64::MAX),
    );
    Ok(selected_tuning)
}

#[cfg(test)]
mod tests {
    use super::*;
    use blocknet_pow_kernel::ReusablePowContext;

    /// Installs a worker that acknowledges every cancel and fence at once and
    /// counts the controls it receives.
    fn install_acking_worker(backend: &NvidiaBackend) -> Arc<std::sync::atomic::AtomicUsize> {
        let received = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (assignment_tx, _assignment_rx) = bounded::<WorkerCommand>(4);
        let (control_tx, control_rx) = bounded::<WorkerCommand>(4);
        let seen = Arc::clone(&received);
        let handle = thread::spawn(move || {
            while let Ok(cmd) = control_rx.recv() {
                match cmd {
                    WorkerCommand::Cancel(ack) | WorkerCommand::Fence(ack) => {
                        seen.fetch_add(1, Ordering::SeqCst);
                        let _ = ack.send(Ok(()));
                    }
                    WorkerCommand::Stop => break,
                    _ => {}
                }
            }
        });
        *backend.worker.lock().unwrap() = Some(NvidiaWorker {
            assignment_tx,
            control_tx,
            handle,
        });
        received
    }

    #[test]
    fn nonblocking_cancel_completes_on_the_acknowledgement_it_waited_for() {
        let backend = NvidiaBackend::new(
            None,
            unique_temp_file("control-ack"),
            1,
            NvidiaBackendTuningOptions::default(),
        );
        let received = install_acking_worker(&backend);
        let mut status = backend.cancel_work_nonblocking().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while status == BackendCallStatus::Pending && Instant::now() < deadline {
            backend
                .wait_for_nonblocking_progress(Duration::from_millis(20))
                .unwrap();
            status = backend.cancel_work_nonblocking().unwrap();
        }
        assert_eq!(status, BackendCallStatus::Complete);
        assert_eq!(received.load(Ordering::SeqCst), 1, "one cancel per request");

        // A new request after new work is a fresh cancel, not the old ack.
        backend.forget_completed_control();
        let mut status = backend.cancel_work_nonblocking().unwrap();
        while status == BackendCallStatus::Pending && Instant::now() < deadline {
            backend
                .wait_for_nonblocking_progress(Duration::from_millis(20))
                .unwrap();
            status = backend.cancel_work_nonblocking().unwrap();
        }
        assert_eq!(status, BackendCallStatus::Complete);
        assert_eq!(received.load(Ordering::SeqCst), 2);
        backend.stop();
    }
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_file(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be >= unix epoch")
            .as_nanos();
        path.push(format!(
            "seine-nvidia-autotune-{}-{}-{name}.json",
            std::process::id(),
            now
        ));
        path
    }

    fn unique_temp_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be >= unix epoch")
            .as_nanos();
        path.push(format!(
            "seine-nvidia-test-{}-{}-{name}",
            std::process::id(),
            now
        ));
        path
    }

    fn decrement_target_be(mut target: [u8; POW_OUTPUT_LEN]) -> Option<[u8; POW_OUTPUT_LEN]> {
        for byte in target.iter_mut().rev() {
            if *byte == 0 {
                *byte = 0xff;
                continue;
            }
            *byte -= 1;
            return Some(target);
        }
        None
    }

    fn test_template(work_id: u64) -> Arc<WorkTemplate> {
        Arc::new(WorkTemplate {
            work_id,
            epoch: 7,
            header_base: Arc::<[u8]>::from(vec![1u8; 92]),
            pow: crate::backend::PowAlgorithm::Argon2id,
            target: [0xff; 32],
            dynamic_share_target: None,
            pause_on_solution: true,
            stop_at: Instant::now() + Duration::from_secs(30),
        })
    }

    #[test]
    fn parse_nvidia_smi_query_output_parses_multiple_rows() {
        let parsed = parse_nvidia_smi_query_output(
            "0, NVIDIA GeForce RTX 3080, 10240\n1, NVIDIA RTX A4000, 16384\n",
        )
        .expect("query output should parse");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].index, 0);
        assert_eq!(parsed[0].name, "NVIDIA GeForce RTX 3080");
        assert_eq!(parsed[0].memory_total_mib, 10_240);
        assert_eq!(parsed[1].index, 1);
        assert_eq!(parsed[1].name, "NVIDIA RTX A4000");
        assert_eq!(parsed[1].memory_total_mib, 16_384);
    }

    #[test]
    fn parse_nvidia_smi_query_output_rejects_invalid_rows() {
        let err =
            parse_nvidia_smi_query_output("abc, RTX, 8192").expect_err("invalid index should fail");
        assert!(format!("{err:#}").contains("invalid GPU index"));
    }

    #[test]
    fn concurrent_autotune_persists_preserve_every_record() {
        let path = unique_temp_file("concurrent");
        let handles: Vec<_> = (0..8u32)
            .map(|worker| {
                let path = path.clone();
                thread::spawn(move || {
                    let key = NvidiaAutotuneKey {
                        device_name: format!("Fake GPU {worker}"),
                        memory_total_mib: 10_240,
                        memory_budget_mib: 9_600,
                        lane_capacity_tier: 4,
                        compute_cap_major: 8,
                        compute_cap_minor: 6,
                        m_cost_kib: 2_097_152,
                        t_cost: 1,
                        kernel_threads: KERNEL_THREADS,
                        hashes_per_launch_per_lane_cap: 2,
                        os: "linux".to_string(),
                        kernel_version: "test-kernel".to_string(),
                        runtime_environment: "native".to_string(),
                        build_fingerprint: "test-build".to_string(),
                        cuda_kernel_fingerprint: "test-cuda-kernel".to_string(),
                        nvrtc_version: "nvrtc-test".to_string(),
                    };
                    persist_nvidia_autotune_record(
                        &path,
                        key,
                        NvidiaKernelTuning {
                            max_rregcount: 160,
                            block_loop_unroll: false,
                            hashes_per_launch_per_lane: 2,
                            max_lanes_hint: None,
                        },
                        0.8,
                        2,
                        2,
                        Vec::new(),
                        0,
                    )
                    .expect("concurrent persist should succeed");
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("persist thread should not panic");
        }

        let cache =
            load_nvidia_autotune_cache(&path).expect("cache should parse after concurrent writes");
        assert_eq!(cache.records.len(), 8);
        let _ = fs::remove_file(&path);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn nvidia_autotune_cache_round_trip_loads_latest_record() {
        let path = unique_temp_file("roundtrip");
        let key = NvidiaAutotuneKey {
            device_name: "NVIDIA GeForce RTX 3080".to_string(),
            memory_total_mib: 10_240,
            memory_budget_mib: 9_600,
            lane_capacity_tier: 4,
            compute_cap_major: 8,
            compute_cap_minor: 6,
            m_cost_kib: 2_097_152,
            t_cost: 1,
            kernel_threads: KERNEL_THREADS,
            hashes_per_launch_per_lane_cap: 2,
            os: "linux".to_string(),
            kernel_version: "test-kernel".to_string(),
            runtime_environment: "native".to_string(),
            build_fingerprint: "test-build".to_string(),
            cuda_kernel_fingerprint: "test-cuda-kernel".to_string(),
            nvrtc_version: "nvrtc-test".to_string(),
        };
        persist_nvidia_autotune_record(
            &path,
            key.clone(),
            NvidiaKernelTuning {
                max_rregcount: 160,
                block_loop_unroll: false,
                hashes_per_launch_per_lane: 2,
                max_lanes_hint: None,
            },
            0.8,
            2,
            2,
            Vec::new(),
            0,
        )
        .expect("first record should persist");
        let candidate_trace = vec![NvidiaAutotuneCandidateTrace {
            tuning: NvidiaKernelTuning {
                max_rregcount: 224,
                block_loop_unroll: true,
                hashes_per_launch_per_lane: 2,
                max_lanes_hint: None,
            },
            samples: vec![
                NvidiaAutotuneSampleScore {
                    throughput_hps: 1.1,
                    counted_hps: 1.0,
                    elapsed_secs: 2.1,
                },
                NvidiaAutotuneSampleScore {
                    throughput_hps: 1.2,
                    counted_hps: 1.1,
                    elapsed_secs: 2.2,
                },
            ],
            failed_samples: 0,
            counted_median_hps: Some(1.05),
            counted_mean_hps: Some(1.05),
            throughput_median_hps: Some(1.15),
            throughput_mean_hps: Some(1.15),
            elapsed_millis: 4_300,
        }];
        persist_nvidia_autotune_record(
            &path,
            key.clone(),
            NvidiaKernelTuning {
                max_rregcount: 224,
                block_loop_unroll: true,
                hashes_per_launch_per_lane: 2,
                max_lanes_hint: None,
            },
            1.0,
            2,
            2,
            candidate_trace.clone(),
            4_500,
        )
        .expect("second record should persist");

        let loaded =
            load_nvidia_cached_tuning(&path, &key).expect("cached tuning should be available");
        assert_eq!(loaded.max_rregcount, 224);
        assert!(loaded.block_loop_unroll);
        let cache = load_nvidia_autotune_cache(&path).expect("cache should parse");
        assert_eq!(cache.records.len(), 1);
        assert_eq!(cache.records[0].candidate_trace, candidate_trace);
        assert_eq!(cache.records[0].autotune_elapsed_millis, 4_500);

        let mut legacy_json = serde_json::to_value(&cache).expect("cache should serialize");
        let legacy_record = legacy_json["records"][0]
            .as_object_mut()
            .expect("record should be an object");
        legacy_record.remove("candidate_trace");
        legacy_record.remove("autotune_elapsed_millis");
        let legacy_cache: NvidiaAutotuneCache =
            serde_json::from_value(legacy_json).expect("older cache records should still parse");
        assert!(legacy_cache.records[0].candidate_trace.is_empty());
        assert_eq!(legacy_cache.records[0].autotune_elapsed_millis, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn nvidia_autotune_cache_fallback_uses_closest_compatible_budget() {
        let path = unique_temp_file("budget-fallback");
        let base_key = NvidiaAutotuneKey {
            device_name: "NVIDIA GeForce RTX 3080".to_string(),
            memory_total_mib: 10_240,
            memory_budget_mib: 9_728,
            lane_capacity_tier: 4,
            compute_cap_major: 8,
            compute_cap_minor: 6,
            m_cost_kib: 2_097_152,
            t_cost: 1,
            kernel_threads: KERNEL_THREADS,
            hashes_per_launch_per_lane_cap: 2,
            os: "linux".to_string(),
            kernel_version: "test-kernel".to_string(),
            runtime_environment: "native".to_string(),
            build_fingerprint: "test-build".to_string(),
            cuda_kernel_fingerprint: "test-cuda-kernel".to_string(),
            nvrtc_version: "nvrtc-test".to_string(),
        };
        let closer_key = NvidiaAutotuneKey {
            memory_budget_mib: 8_192,
            lane_capacity_tier: 3,
            ..base_key.clone()
        };
        let farther_key = NvidiaAutotuneKey {
            memory_budget_mib: 7_680,
            lane_capacity_tier: 2,
            ..base_key.clone()
        };

        persist_nvidia_autotune_record(
            &path,
            closer_key,
            NvidiaKernelTuning {
                max_rregcount: 208,
                block_loop_unroll: false,
                hashes_per_launch_per_lane: 2,
                max_lanes_hint: None,
            },
            1.0,
            2,
            2,
            Vec::new(),
            0,
        )
        .expect("closer record should persist");
        persist_nvidia_autotune_record(
            &path,
            farther_key,
            NvidiaKernelTuning {
                max_rregcount: 224,
                block_loop_unroll: true,
                hashes_per_launch_per_lane: 2,
                max_lanes_hint: None,
            },
            1.0,
            2,
            2,
            Vec::new(),
            0,
        )
        .expect("farther record should persist");

        let loaded =
            load_nvidia_cached_tuning(&path, &base_key).expect("fallback tuning should be found");
        assert_eq!(loaded.max_rregcount, 208);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn nvidia_autotune_cache_does_not_cross_hash_depth_caps() {
        let path = unique_temp_file("depth-cap");
        let base_key = NvidiaAutotuneKey {
            device_name: "NVIDIA GeForce RTX 3080".to_string(),
            memory_total_mib: 10_240,
            memory_budget_mib: 9_728,
            lane_capacity_tier: 4,
            compute_cap_major: 8,
            compute_cap_minor: 6,
            m_cost_kib: 2_097_152,
            t_cost: 1,
            kernel_threads: KERNEL_THREADS,
            hashes_per_launch_per_lane_cap: 2,
            os: "linux".to_string(),
            kernel_version: "test-kernel".to_string(),
            runtime_environment: "native".to_string(),
            build_fingerprint: "test-build".to_string(),
            cuda_kernel_fingerprint: "test-cuda-kernel".to_string(),
            nvrtc_version: "nvrtc-test".to_string(),
        };
        let cap1_key = NvidiaAutotuneKey {
            hashes_per_launch_per_lane_cap: 1,
            ..base_key.clone()
        };

        persist_nvidia_autotune_record(
            &path,
            cap1_key,
            NvidiaKernelTuning {
                max_rregcount: 208,
                block_loop_unroll: false,
                hashes_per_launch_per_lane: 1,
                max_lanes_hint: None,
            },
            1.0,
            2,
            2,
            Vec::new(),
            0,
        )
        .expect("cap-1 record should persist");

        let loaded = load_nvidia_cached_tuning(&path, &base_key);
        assert!(
            loaded.is_err(),
            "cache lookup should not reuse tuning from a different launch-depth cap"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn nvidia_autotune_compatibility_does_not_cross_build_or_runtime() {
        let key = NvidiaAutotuneKey {
            device_name: "NVIDIA GeForce RTX 5090".to_string(),
            memory_total_mib: 32_768,
            memory_budget_mib: 30_720,
            lane_capacity_tier: 14,
            compute_cap_major: 12,
            compute_cap_minor: 0,
            m_cost_kib: 2_097_152,
            t_cost: 1,
            kernel_threads: KERNEL_THREADS,
            hashes_per_launch_per_lane_cap: 2,
            os: "linux".to_string(),
            kernel_version: "6.6.0".to_string(),
            runtime_environment: "wsl2".to_string(),
            build_fingerprint: "build-a".to_string(),
            cuda_kernel_fingerprint: "kernel-a".to_string(),
            nvrtc_version: "nvrtc-12.8".to_string(),
        };
        let different_build = NvidiaAutotuneKey {
            build_fingerprint: "build-b".to_string(),
            ..key.clone()
        };
        let different_runtime = NvidiaAutotuneKey {
            runtime_environment: "native".to_string(),
            ..key.clone()
        };

        assert!(!nvidia_autotune_key_compatible(&key, &different_build));
        assert!(!nvidia_autotune_key_compatible(&key, &different_runtime));

        let path = unique_temp_file("cache-diagnostics");
        assert_eq!(
            load_nvidia_cached_tuning(&path, &key).unwrap_err(),
            "no saved tuning result"
        );
        fs::write(&path, "invalid json").unwrap();
        assert_eq!(
            load_nvidia_cached_tuning(&path, &key).unwrap_err(),
            "saved tuning results are invalid"
        );
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert_eq!(
            load_nvidia_cached_tuning(&path, &key).unwrap_err(),
            "saved tuning results could not be read"
        );
        fs::remove_dir(&path).unwrap();
        persist_nvidia_autotune_record(
            &path,
            key.clone(),
            NvidiaKernelTuning::default(),
            1.0,
            2,
            2,
            Vec::new(),
            0,
        )
        .unwrap();
        assert!(load_nvidia_cached_tuning(&path, &key).is_ok());
        assert_eq!(
            load_nvidia_cached_tuning(&path, &different_build).unwrap_err(),
            "miner build changed"
        );
        assert_eq!(
            load_nvidia_cached_tuning(&path, &different_runtime).unwrap_err(),
            "operating system or runtime changed"
        );
        let different_gpu = NvidiaAutotuneKey {
            device_name: "another GPU".into(),
            ..different_build
        };
        assert_eq!(
            load_nvidia_cached_tuning(&path, &different_gpu).unwrap_err(),
            "no saved tuning result for this GPU"
        );
        let different_compiler = NvidiaAutotuneKey {
            nvrtc_version: "another compiler".into(),
            ..key.clone()
        };
        assert_eq!(
            load_nvidia_cached_tuning(&path, &different_compiler).unwrap_err(),
            "CUDA compiler changed"
        );
        let different_kernel = NvidiaAutotuneKey {
            cuda_kernel_fingerprint: "another kernel".into(),
            ..key.clone()
        };
        assert_eq!(
            load_nvidia_cached_tuning(&path, &different_kernel).unwrap_err(),
            "CUDA kernel changed"
        );
        let mut cache = load_nvidia_autotune_cache(&path).unwrap();
        cache.schema_version = 0;
        fs::write(&path, serde_json::to_vec(&cache).unwrap()).unwrap();
        assert_eq!(
            load_nvidia_cached_tuning(&path, &key).unwrap_err(),
            "cache format changed"
        );
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn cubin_cache_key_changes_with_compile_options() {
        let source = "__global__ void k() {}";
        let key_a = build_cubin_cache_key(
            source,
            "k.cu",
            &["--gpu-architecture=sm_86".to_string()],
            "nvrtc-12.8",
        );
        let key_b = build_cubin_cache_key(
            source,
            "k.cu",
            &["--gpu-architecture=sm_89".to_string()],
            "nvrtc-12.8",
        );
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn cubin_cache_key_changes_with_nvrtc_version() {
        let source = "__global__ void k() {}";
        let options = ["--gpu-architecture=sm_86".to_string()];
        let key_a = build_cubin_cache_key(source, "k.cu", &options, "nvrtc-12.8");
        let key_b = build_cubin_cache_key(source, "k.cu", &options, "nvrtc-13.0");
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn cubin_cache_round_trip_reads_written_bytes() {
        let path = unique_temp_file("cubin-cache");
        let payload = vec![1u8, 2, 3, 4, 5];
        persist_cached_cubin(&path, &payload).expect("cubin cache should persist");
        let loaded = load_cached_cubin(&path).expect("cubin cache should load");
        assert_eq!(loaded, payload);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn derive_cubin_cache_dir_follows_autotune_parent_and_stem() {
        let autotune_path = PathBuf::from("/tmp/seine.nvidia-autotune.json");
        let cache_dir = derive_nvidia_cubin_cache_dir(&autotune_path);
        assert_eq!(
            cache_dir,
            PathBuf::from("/tmp/seine.nvidia-autotune.cubin-cache")
        );
    }

    #[test]
    fn normalize_assignment_batch_preserves_contiguous_chunks() {
        let template = test_template(11);
        let assignments = vec![
            WorkAssignment {
                template: Arc::clone(&template),
                nonce_chunk: NonceChunk {
                    start_nonce: 100,
                    nonce_count: 4,
                },
            },
            WorkAssignment {
                template,
                nonce_chunk: NonceChunk {
                    start_nonce: 104,
                    nonce_count: 6,
                },
            },
        ];

        let normalized =
            normalize_assignment_batch(&assignments).expect("batch normalization should succeed");
        assert_eq!(normalized.len(), 2);
        assert_eq!(normalized[0].nonce_chunk.start_nonce, 100);
        assert_eq!(normalized[0].nonce_chunk.nonce_count, 4);
        assert_eq!(normalized[1].nonce_chunk.start_nonce, 104);
        assert_eq!(normalized[1].nonce_chunk.nonce_count, 6);
    }

    #[test]
    fn normalize_assignment_batch_rejects_non_contiguous_chunks() {
        let template = test_template(11);
        let assignments = vec![
            WorkAssignment {
                template: Arc::clone(&template),
                nonce_chunk: NonceChunk {
                    start_nonce: 100,
                    nonce_count: 4,
                },
            },
            WorkAssignment {
                template,
                nonce_chunk: NonceChunk {
                    start_nonce: 105,
                    nonce_count: 6,
                },
            },
        ];

        let err = normalize_assignment_batch(&assignments)
            .expect_err("non-contiguous chunks should fail");
        assert!(format!("{err:#}").contains("not contiguous"));
    }

    #[test]
    fn plan_lane_storage_adds_a_packed_lane_when_close() {
        let m_blocks = 1u32 << 21;
        // 12 GiB card: 11,607 MiB usable fits 5 full lanes; 6 lanes need 5.6% unstored.
        let usable = 11_607u64 * 1024 * 1024;
        let plans = plan_lane_storage(m_blocks, usable, 5, None, true);
        assert_eq!(plans[0].0, 6);
        assert!(plans[0].1 < m_blocks);
        assert_eq!(plans[0].1 % NVIDIA_LANE_STRIDE_ALIGN_BLOCKS, 0);
        assert!(u64::from(plans[0].1) * 1024 * 6 <= usable);
        let packed = plans.iter().take_while(|plan| plan.0 == 6).count();
        assert_eq!(packed, 5);
        assert!(plans[..packed].windows(2).all(|pair| pair[1].1 < pair[0].1));
        assert_eq!(plans[packed], (5, m_blocks));
        assert_eq!(plans.last(), Some(&(1, m_blocks)));
        // Without support every plan stores whole lanes.
        assert_eq!(
            plan_lane_storage(m_blocks, usable, 5, None, false)[0],
            (5, m_blocks)
        );
    }

    #[test]
    fn plan_lane_storage_skips_packing_when_too_far() {
        let m_blocks = 1u32 << 21;
        // 4,500 MiB usable: 3 lanes would leave 27% unstored.
        let usable = 4_500u64 * 1024 * 1024;
        let plans = plan_lane_storage(m_blocks, usable, 2, None, true);
        assert_eq!(plans[0], (2, m_blocks));
        // 11 GB card, 10,600 MiB usable: 6 lanes leave about 14%.
        let usable = 10_600u64 * 1024 * 1024;
        let plans = plan_lane_storage(m_blocks, usable, 5, None, true);
        assert_eq!(plans[0].0, 6);
        assert!(plans[0].1 < m_blocks * 7 / 8);
    }

    #[test]
    fn plan_lane_storage_honours_explicit_lane_count() {
        let m_blocks = 1u32 << 21;
        let usable = 11_607u64 * 1024 * 1024;
        let plans = plan_lane_storage(m_blocks, usable, 5, Some(3), true);
        assert_eq!(plans[0], (3, m_blocks));
        let plans = plan_lane_storage(m_blocks, usable, 5, Some(6), true);
        assert_eq!(plans[0].0, 6);
        assert!(plans[0].1 < m_blocks);
        assert!(plans.contains(&(6, m_blocks)));
    }

    #[test]
    fn derive_memory_budget_uses_free_vram_with_headroom() {
        let budget = derive_memory_budget_mib(10_240, Some(9_800));
        assert_eq!(budget, 9_640);
    }

    #[test]
    fn bucket_memory_budget_rounds_to_nearest_bucket() {
        assert_eq!(bucket_memory_budget_mib(9_640), 9_728);
        assert_eq!(bucket_memory_budget_mib(9_500), 9_728);
        assert_eq!(bucket_memory_budget_mib(9_400), 9_216);
    }

    #[test]
    fn derive_lane_capacity_tier_maps_capacity_ranges() {
        assert_eq!(derive_lane_capacity_tier(2_100, 2_097_152), 1);
        assert_eq!(derive_lane_capacity_tier(4_200, 2_097_152), 2);
        assert_eq!(derive_lane_capacity_tier(9_728, 2_097_152), 4);
    }

    #[test]
    fn is_transient_cuda_error_classifies_known_transient_errors() {
        assert!(is_transient_cuda_error("CUDA_ERROR_LAUNCH_TIMEOUT"));
        assert!(is_transient_cuda_error("something LAUNCH_TIMEOUT happened"));
        assert!(is_transient_cuda_error("ECC_UNCORRECTABLE error"));
        assert!(is_transient_cuda_error("NOT_READY"));
        assert!(is_transient_cuda_error("OPERATING_SYSTEM error"));
    }

    #[test]
    fn is_transient_cuda_error_rejects_non_transient_errors() {
        assert!(!is_transient_cuda_error("CUDA_ERROR_OUT_OF_MEMORY"));
        assert!(!is_transient_cuda_error("invalid argument"));
        assert!(!is_transient_cuda_error("kernel launch failed"));
    }

    #[test]
    fn is_cuda_oom_error_detects_oom_variants() {
        assert!(is_cuda_oom_error(&"CUDA_ERROR_OUT_OF_MEMORY"));
        assert!(is_cuda_oom_error(&"OutOfMemory"));
        assert!(is_cuda_oom_error(&"some context OUT_OF_MEMORY here"));
    }

    #[test]
    fn is_cuda_oom_error_rejects_non_oom() {
        assert!(!is_cuda_oom_error(&"CUDA_ERROR_LAUNCH_TIMEOUT"));
        assert!(!is_cuda_oom_error(&"invalid argument"));
        assert!(!is_cuda_oom_error(&"memory allocation succeeded"));
    }

    #[test]
    fn nvrtc_arch_target_unsupported_matches_known_messages() {
        assert!(nvrtc_arch_target_unsupported(
            "nvrtcCompileProgram failed: NVRTC_ERROR_INVALID_OPTION; log: unsupported gpu architecture 'sm_61'"
        ));
        assert!(nvrtc_arch_target_unsupported(
            "error: value 'sm_61' is not defined for option 'gpu-architecture'"
        ));
        assert!(!nvrtc_arch_target_unsupported(
            "nvrtcCompileProgram failed: syntax error"
        ));
    }

    #[test]
    fn send_backend_event_drops_when_sink_full() {
        let shared = NvidiaShared {
            event_sink: Arc::new(RwLock::new(None)),
            dropped_events: AtomicU64::new(0),
            hashes: AtomicU64::new(0),
            active_lanes: AtomicU64::new(0),
            pending_work: AtomicU64::new(0),
            inflight_assignment_hashes: AtomicU64::new(0),
            inflight_assignment_started_at: Mutex::new(None),
            completed_assignments: AtomicU64::new(0),
            completed_assignment_hashes: AtomicU64::new(0),
            completed_assignment_micros: AtomicU64::new(0),
            active_hashes_per_launch_per_lane: AtomicU32::new(0),
            cancel_requested: AtomicBool::new(false),
            error_emitted: AtomicBool::new(false),
            blake2b_active: AtomicBool::new(false),
        };
        let (event_tx, _event_rx) = crossbeam_channel::bounded(1);
        if let Ok(mut slot) = shared.event_sink.write() {
            *slot = Some(event_tx.clone());
        }
        // Fill the channel
        let _ = event_tx.try_send(BackendEvent::Error {
            backend_id: 1,
            backend: "nvidia",
            message: "filler".to_string(),
        });
        // This should NOT block — it should drop the event
        send_backend_event(
            &shared,
            BackendEvent::Error {
                backend_id: 2,
                backend: "nvidia",
                message: "should be dropped".to_string(),
            },
        );
        assert!(
            shared.dropped_events.load(Ordering::Acquire) >= 1,
            "event should be dropped when channel is full instead of blocking"
        );
    }

    #[test]
    fn blackwell_default_launch_depth_caps_to_one_when_not_overridden() {
        assert_eq!(effective_hashes_per_launch_per_lane_cap(2, false, 12), 1);
        assert_eq!(effective_hashes_per_launch_per_lane_cap(2, false, 13), 1);
    }

    #[test]
    fn explicit_launch_depth_override_is_respected_on_blackwell() {
        assert_eq!(effective_hashes_per_launch_per_lane_cap(2, true, 12), 2);
        assert_eq!(effective_hashes_per_launch_per_lane_cap(4, true, 12), 4);
    }

    #[test]
    fn pre_blackwell_default_launch_depth_stays_unchanged() {
        assert_eq!(effective_hashes_per_launch_per_lane_cap(2, false, 8), 2);
        assert_eq!(effective_hashes_per_launch_per_lane_cap(2, false, 9), 2);
    }

    #[test]
    fn rtx_5090_autotune_uses_measured_regcap_frontier() {
        assert_eq!(
            nvidia_autotune_regcap_candidates(12, "NVIDIA GeForce RTX 5090"),
            NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_RTX_5090
        );
        assert_eq!(
            nvidia_autotune_regcap_candidates(12, " nvidia geforce rtx 5090 "),
            NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_RTX_5090
        );
    }

    #[test]
    fn regcap_shortlist_does_not_leak_to_other_devices() {
        assert_eq!(
            nvidia_autotune_regcap_candidates(12, "NVIDIA GeForce RTX 5080"),
            NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_AMPERE_PLUS
        );
        assert_eq!(
            nvidia_autotune_regcap_candidates(12, "NVIDIA GeForce RTX 5090 Laptop GPU"),
            NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_AMPERE_PLUS
        );
        assert_eq!(
            nvidia_autotune_regcap_candidates(12, "NVIDIA GeForce RTX 5090 D"),
            NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_AMPERE_PLUS
        );
        assert_eq!(
            nvidia_autotune_regcap_candidates(8, "NVIDIA GeForce RTX 5090"),
            NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_AMPERE_PLUS
        );
        assert_eq!(
            nvidia_autotune_regcap_candidates(7, "NVIDIA GeForce RTX 5090"),
            NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_LEGACY
        );
    }

    fn autotune_score(
        max_rregcount: u32,
        depth: u32,
        max_lanes_hint: Option<usize>,
        counted_median: f64,
        counted_mean: f64,
    ) -> NvidiaAutotuneCandidateScore {
        NvidiaAutotuneCandidateScore {
            tuning: NvidiaKernelTuning {
                max_rregcount,
                block_loop_unroll: false,
                hashes_per_launch_per_lane: depth,
                max_lanes_hint,
            },
            counted_median,
            counted_mean,
            throughput_median: counted_median,
            throughput_mean: counted_mean,
        }
    }

    #[test]
    fn blackwell_autotune_tie_prefers_shallower_depth() {
        let deeper = autotune_score(240, 2, None, 1.0, 1.0);
        let shallower = autotune_score(240, 1, None, 0.99, 0.99);

        assert!(nvidia_autotune_candidate_beats(&shallower, &deeper, 12));
        assert!(!nvidia_autotune_candidate_beats(&deeper, &shallower, 12));
    }

    #[test]
    fn blackwell_autotune_tie_prefers_regcap_closer_to_observed_frontier() {
        let high_reg = autotune_score(240, 1, None, 1.0, 1.0);
        let frontier_reg = autotune_score(208, 1, None, 0.99, 0.99);

        assert!(nvidia_autotune_candidate_beats(
            &frontier_reg,
            &high_reg,
            12
        ));
        assert!(!nvidia_autotune_candidate_beats(
            &high_reg,
            &frontier_reg,
            12
        ));
    }

    #[test]
    fn blackwell_autotune_tie_prefers_uncapped_lanes() {
        let capped = autotune_score(208, 1, Some(12), 1.0, 1.0);
        let uncapped = autotune_score(208, 1, None, 0.99, 0.99);

        assert!(nvidia_autotune_candidate_beats(&uncapped, &capped, 12));
        assert!(!nvidia_autotune_candidate_beats(&capped, &uncapped, 12));
    }

    #[test]
    fn pre_blackwell_autotune_keeps_strict_metric_ordering() {
        let faster = autotune_score(240, 2, None, 1.0, 1.0);
        let slower = autotune_score(208, 1, None, 0.99, 0.99);

        assert!(nvidia_autotune_candidate_beats(&faster, &slower, 8));
        assert!(!nvidia_autotune_candidate_beats(&slower, &faster, 8));
    }

    #[test]
    fn header_base_upload_decision_uses_content_not_pointer_identity() {
        let cached = vec![0x11; POW_HEADER_BASE_LEN];
        let mut next = vec![0x11; POW_HEADER_BASE_LEN];

        assert!(
            !header_base_upload_needed(&cached, &next),
            "identical header bytes should not trigger a refresh"
        );

        next[0] ^= 0xFF;
        assert!(
            header_base_upload_needed(&cached, &next),
            "changed header bytes must refresh even if an allocator later reuses the same address"
        );
    }

    #[test]
    fn target_upload_decision_uses_content_not_pointer_identity() {
        let cached = [0x22; POW_OUTPUT_LEN];
        let mut next = [0x22; POW_OUTPUT_LEN];

        assert!(
            !target_upload_needed(Some(&cached), &next),
            "identical target bytes should not trigger a refresh"
        );

        next[POW_OUTPUT_LEN - 1] ^= 0x01;
        assert!(
            target_upload_needed(Some(&cached), &next),
            "changed target bytes must refresh even if an allocator later reuses the same address"
        );
    }

    thread_local! {
        static FORCED_STORED_BLOCKS: std::cell::Cell<Option<u32>> =
            const { std::cell::Cell::new(None) };
        static FORCE_PREBUILT_ARGON2: std::cell::Cell<bool> =
            const { std::cell::Cell::new(false) };
    }

    pub(super) fn forced_stored_blocks() -> Option<u32> {
        FORCED_STORED_BLOCKS.with(|cell| cell.get())
    }

    pub(super) fn force_prebuilt_argon2() -> bool {
        FORCE_PREBUILT_ARGON2.with(|cell| cell.get())
    }

    #[test]
    fn prebuilt_argon2_ptx_matches_the_kernel_source_and_network() {
        use sha2::{Digest, Sha256};
        let digest = format!("{:x}", Sha256::digest(CUDA_KERNEL_SRC.as_bytes()));
        for (ptx, cc_major) in [
            (ARGON2_PTX_CC6, 6),
            (ARGON2_PTX_CC7, 7),
            (ARGON2_PTX_CC8, 8),
            (ARGON2_PTX_CC10, 10),
        ] {
            assert!(
                ptx.contains(&format!("// kernel-sha256: {digest}\n")),
                "argon2 PTX is stale: run scripts/gen_argon2_ptx.py"
            );
            let options = ptx.lines().nth(2).expect("options line");
            assert!(options.contains(&format!(
                "-DSEINE_FIXED_M_BLOCKS={PREBUILT_ARGON2_M_BLOCKS}U"
            )));
            assert!(options.contains("-DSEINE_FIXED_T_COST=1U"));
            assert!(options.contains(&format!(
                "-DSEINE_CANCEL_CHECK_BLOCK_INTERVAL={}U",
                select_cancel_check_block_interval(cc_major)
            )));
            assert!(options.contains(&format!(
                "-DSEINE_WARPS_PER_BLOCK={DEFAULT_WARPS_PER_BLOCK}U"
            )));
            for define in arch_fill_defines(cc_major) {
                assert!(
                    options.contains(&format!("-D{define}")),
                    "{define} for {cc_major}.x"
                );
            }
        }
        let params = pow_params().expect("pow params");
        assert_eq!(params.block_count() as u32, PREBUILT_ARGON2_M_BLOCKS);
        assert_eq!(params.t_cost(), 1);
    }

    #[test]
    fn fill_defines_follow_architecture() {
        assert!(arch_fill_defines(6).contains(&"SEINE_REF_PREFETCH_DEPTH=2U"));
        assert!(!arch_fill_defines(6).contains(&"SEINE_LOOP_UNROLL=2"));
        assert!(arch_fill_defines(7).contains(&"SEINE_BLOCK_BASE_POINTER=1"));
        assert!(arch_fill_defines(8).contains(&"SEINE_BLOCK_BASE_POINTER=2"));
        assert!(arch_fill_defines(7).contains(&"SEINE_REF_PREFETCH_DEPTH=2U"));
        assert!(arch_fill_defines(7).contains(&"SEINE_ROW_LAYOUT=1"));
        assert!(arch_fill_defines(8).contains(&"SEINE_REF_RING=1U"));
        assert!(arch_fill_defines(8).contains(&"SEINE_INLINE_REBUILD=1"));
        assert_eq!(arch_fill_defines(8), arch_fill_defines(9));
        assert!(arch_fill_defines(12).contains(&"SEINE_ROW_LAYOUT=1"));
        assert!(!arch_fill_defines(12).contains(&"SEINE_INLINE_REBUILD=1"));
        assert!(!arch_fill_defines(12).contains(&"SEINE_REF_RING=1U"));
        assert!(arch_fill_defines(5).is_empty());
    }

    #[test]
    fn prebuilt_argon2_ptx_covers_pascal_and_newer() {
        let m = PREBUILT_ARGON2_M_BLOCKS;
        assert!(prebuilt_argon2_ptx(5, m, 1).is_none());
        for (cc_major, ptx) in [
            (6, ARGON2_PTX_CC6),
            (7, ARGON2_PTX_CC7),
            (8, ARGON2_PTX_CC8),
            (9, ARGON2_PTX_CC8),
            (10, ARGON2_PTX_CC10),
            (12, ARGON2_PTX_CC10),
        ] {
            assert!(std::ptr::eq(
                prebuilt_argon2_ptx(cc_major, m, 1).unwrap(),
                ptx
            ));
        }
        assert!(prebuilt_argon2_ptx(8, m / 2, 1).is_none());
        assert!(prebuilt_argon2_ptx(8, m, 2).is_none());
    }

    /// Repeats one full batch and requires identical results each time, which
    /// catches ordering races that a few reference hashes would rarely hit.
    /// SEINE_TEST_LANES (default 8), SEINE_TEST_ROUNDS (default 20) and
    /// SEINE_TEST_STORED_BLOCKS (unset: every block stored) size the run.
    #[test]
    #[ignore = "requires NVIDIA GPU + CUDA runtime; run manually for GPU validity checks"]
    fn gpu_repeated_batches_are_identical() {
        let env = |name: &str| std::env::var(name).ok().and_then(|v| v.parse::<u32>().ok());
        let lanes = env("SEINE_TEST_LANES").unwrap_or(8) as usize;
        let rounds = env("SEINE_TEST_ROUNDS").unwrap_or(20);
        let devices = query_nvidia_devices().expect("failed to query NVIDIA devices");
        let selected = devices.first().expect("no NVIDIA GPU detected").clone();
        FORCED_STORED_BLOCKS.with(|cell| cell.set(env("SEINE_TEST_STORED_BLOCKS")));
        let tuning = NvidiaKernelTuning {
            max_rregcount: DEFAULT_NVIDIA_MAX_RREGCOUNT,
            block_loop_unroll: false,
            hashes_per_launch_per_lane: 2,
            max_lanes_hint: Some(lanes),
        };
        let cache_dir = unique_temp_dir("gpu-repeat");
        let engine = CudaArgon2Engine::new(
            selected.index,
            selected.memory_total_mib,
            selected.memory_free_mib,
            tuning,
            tuning.max_lanes_hint,
            tuning.hashes_per_launch_per_lane,
            false,
            &cache_dir,
        );
        FORCED_STORED_BLOCKS.with(|cell| cell.set(None));
        let mut engine = engine.expect("failed to initialize CUDA engine");
        let mut header_base = [0u8; POW_HEADER_BASE_LEN];
        for (idx, byte) in header_base.iter_mut().enumerate() {
            *byte = ((idx * 41 + 17) & 0xff) as u8;
        }
        let batch = (0..(engine.max_lanes * 2) as u64).collect::<Vec<_>>();
        let everything = [0xff; POW_OUTPUT_LEN];
        let reference = engine
            .run_fill_batch(&header_base, &batch, Some(&everything))
            .expect("CUDA batch should succeed")
            .solutions;
        assert_eq!(reference.len(), batch.len());
        let mut mismatches = 0usize;
        for round in 0..rounds {
            let again = engine
                .run_fill_batch(&header_base, &batch, Some(&everything))
                .expect("CUDA batch should succeed")
                .solutions;
            let differing = reference.iter().zip(&again).filter(|(a, b)| a != b).count()
                + reference.len().abs_diff(again.len());
            if differing > 0 {
                eprintln!(
                    "round {round}: {differing} of {} hashes differ",
                    batch.len()
                );
            }
            mismatches += differing;
        }
        eprintln!(
            "lanes={} stored={} hashes={} mismatches={mismatches}",
            engine.max_lanes,
            engine.stored_blocks,
            batch.len() * (rounds as usize + 1)
        );
        drop(engine);
        let _ = fs::remove_dir_all(cache_dir);
        assert_eq!(mismatches, 0);
    }

    #[test]
    #[ignore = "requires NVIDIA GPU + CUDA runtime; run manually for GPU validity checks"]
    fn gpu_prebuilt_argon2_ptx_matches_cpu_reference() {
        let devices = query_nvidia_devices().expect("failed to query NVIDIA devices");
        let selected = devices.first().expect("no NVIDIA GPU detected").clone();
        let params = pow_params().expect("pow params should be available");
        let mut cpu_ctx = ReusablePowContext::new(params.m_cost());
        let mut header_base = [0u8; POW_HEADER_BASE_LEN];
        for (idx, byte) in header_base.iter_mut().enumerate() {
            *byte = ((idx * 29 + 3) & 0xff) as u8;
        }
        FORCE_PREBUILT_ARGON2.with(|cell| cell.set(true));
        let tuning = NvidiaKernelTuning {
            max_rregcount: DEFAULT_NVIDIA_MAX_RREGCOUNT,
            block_loop_unroll: false,
            hashes_per_launch_per_lane: 2,
            max_lanes_hint: Some(2),
        };
        let cache_dir = unique_temp_dir("gpu-prebuilt");
        let engine = CudaArgon2Engine::new(
            selected.index,
            selected.memory_total_mib,
            selected.memory_free_mib,
            tuning,
            tuning.max_lanes_hint,
            tuning.hashes_per_launch_per_lane,
            false,
            &cache_dir,
        );
        FORCE_PREBUILT_ARGON2.with(|cell| cell.set(false));
        let mut engine = engine.expect("failed to initialize prebuilt CUDA engine");
        assert!(engine.kernel_fused_target.is_none());
        let batch = [11u64, 12, 13, 14];
        let done = engine
            .run_fill_batch(&header_base, &batch, Some(&[0xff; POW_OUTPUT_LEN]))
            .expect("CUDA batch should succeed");
        assert_eq!(done.solutions.len(), batch.len());
        for (nonce, hash) in done.solutions {
            let mut expected = [0u8; POW_OUTPUT_LEN];
            cpu_ctx
                .hash_password_into(&nonce.to_le_bytes(), &header_base, &mut expected)
                .expect("CPU reference hashing should succeed");
            assert_eq!(hash, expected, "nonce {nonce}");
        }
        drop(engine);
        let _ = fs::remove_dir_all(cache_dir);
    }

    #[test]
    #[ignore = "requires NVIDIA GPU + CUDA runtime; run manually for GPU validity checks"]
    fn gpu_hashes_match_cpu_reference_with_unstored_late_blocks() {
        let devices = query_nvidia_devices().expect("failed to query NVIDIA devices");
        let selected = devices.first().expect("no NVIDIA GPU detected").clone();
        let params = pow_params().expect("pow params should be available");
        let m_blocks = params.block_count() as u32;
        let mut cpu_ctx = ReusablePowContext::new(params.m_cost());
        let mut header_base = [0u8; POW_HEADER_BASE_LEN];
        for (idx, byte) in header_base.iter_mut().enumerate() {
            *byte = ((idx * 53 + 5) & 0xff) as u8;
        }
        // 12% unstored (the planner's limit) and 25% (every odd block of the
        // data-dependent half), which exercises chained rebuilds.
        for stored in [
            m_blocks - (m_blocks / 100 * 12) / 2048 * 2048,
            m_blocks - m_blocks / 4,
        ] {
            FORCED_STORED_BLOCKS.with(|cell| cell.set(Some(stored)));
            let tuning = NvidiaKernelTuning {
                max_rregcount: DEFAULT_NVIDIA_MAX_RREGCOUNT,
                block_loop_unroll: false,
                hashes_per_launch_per_lane: 1,
                max_lanes_hint: Some(1),
            };
            let cache_dir = unique_temp_dir("gpu-skip");
            let mut engine = CudaArgon2Engine::new(
                selected.index,
                selected.memory_total_mib,
                selected.memory_free_mib,
                tuning,
                tuning.max_lanes_hint,
                1,
                false,
                &cache_dir,
            )
            .expect("failed to initialize CUDA engine");
            FORCED_STORED_BLOCKS.with(|cell| cell.set(None));
            assert_eq!(engine.stored_blocks, stored);
            for nonce in [3u64, 99, 4_242, 77_777, 1_234_567] {
                let mut expected = [0u8; POW_OUTPUT_LEN];
                cpu_ctx
                    .hash_password_into(&nonce.to_le_bytes(), &header_base, &mut expected)
                    .expect("CPU reference hashing should succeed");
                let hit = engine
                    .run_fill_batch(&header_base, &[nonce], Some(&expected))
                    .expect("CUDA batch should succeed");
                assert_eq!(
                    hit.solutions,
                    vec![(nonce, expected)],
                    "stored={stored} nonce={nonce}"
                );
                let tighter = decrement_target_be(expected).expect("nonzero hash");
                let miss = engine
                    .run_fill_batch(&header_base, &[nonce], Some(&tighter))
                    .expect("CUDA batch should succeed");
                assert!(miss.solutions.is_empty(), "stored={stored} nonce={nonce}");
            }
            drop(engine);
            let _ = fs::remove_dir_all(cache_dir);
        }
    }

    #[test]
    #[ignore = "requires NVIDIA GPU + CUDA runtime; run manually for GPU validity checks"]
    fn gpu_solution_target_bracket_matches_cpu_reference() {
        let devices = query_nvidia_devices().expect(
            "failed to query NVIDIA devices; verify nvidia-smi and CUDA driver are installed",
        );
        assert!(
            !devices.is_empty(),
            "no NVIDIA GPU detected; cannot run GPU validity differential test"
        );
        let selected = devices[0].clone();

        let tuning = NvidiaKernelTuning {
            max_rregcount: DEFAULT_NVIDIA_MAX_RREGCOUNT,
            block_loop_unroll: false,
            hashes_per_launch_per_lane: 1,
            max_lanes_hint: Some(1),
        };
        let cache_dir = unique_temp_dir("gpu-diff");
        let mut engine = CudaArgon2Engine::new(
            selected.index,
            selected.memory_total_mib,
            selected.memory_free_mib,
            tuning,
            tuning.max_lanes_hint,
            tuning.hashes_per_launch_per_lane,
            false,
            &cache_dir,
        )
        .expect("failed to initialize CUDA engine for GPU validity differential test");

        let params = pow_params().expect("pow params should be available");
        let mut cpu_ctx = ReusablePowContext::new(params.m_cost());
        let mut header_base = [0u8; POW_HEADER_BASE_LEN];
        for (idx, byte) in header_base.iter_mut().enumerate() {
            *byte = ((idx * 37 + 11) & 0xff) as u8;
        }

        let nonces = [0u64, 1u64, 7u64, 42u64, 1_000_003u64];
        for nonce in nonces {
            let mut expected_hash = [0u8; POW_OUTPUT_LEN];
            cpu_ctx
                .hash_password_into(&nonce.to_le_bytes(), &header_base, &mut expected_hash)
                .expect("CPU reference hashing should succeed");
            let tighter_target = decrement_target_be(expected_hash).expect(
                "CPU reference hash was zero; choose a different nonce set for target bracketing",
            );

            let hits_equal = engine
                .run_fill_batch(&header_base, &[nonce], Some(&expected_hash))
                .expect("CUDA batch execution should succeed for equal target");
            assert_eq!(
                hits_equal.hashes_done, 1,
                "expected one completed hash for nonce {}",
                nonce
            );
            assert_eq!(
                hits_equal.solutions,
                vec![(nonce, expected_hash)],
                "GPU did not report expected solution for nonce {} at equal target",
                nonce
            );

            let misses_tighter = engine
                .run_fill_batch(&header_base, &[nonce], Some(&tighter_target))
                .expect("CUDA batch execution should succeed for tighter target");
            assert_eq!(
                misses_tighter.hashes_done, 1,
                "expected one completed hash for nonce {}",
                nonce
            );
            assert!(
                misses_tighter.solutions.is_empty(),
                "GPU reported solution for nonce {} at tighter target; GPU hash differs from CPU reference",
                nonce
            );
        }

        // A batch reports every hash that meets the target.
        drop(engine);
        let tuning = NvidiaKernelTuning {
            hashes_per_launch_per_lane: 2,
            max_lanes_hint: Some(2),
            ..tuning
        };
        let mut engine = CudaArgon2Engine::new(
            selected.index,
            selected.memory_total_mib,
            selected.memory_free_mib,
            tuning,
            tuning.max_lanes_hint,
            tuning.hashes_per_launch_per_lane,
            false,
            &cache_dir,
        )
        .expect("failed to initialize two-lane CUDA engine");
        let batch = [5u64, 6, 7, 8];
        let everything = engine
            .run_fill_batch(&header_base, &batch, Some(&[0xff; POW_OUTPUT_LEN]))
            .expect("CUDA batch execution should succeed");
        let reported = everything.solutions.iter().map(|s| s.0).collect::<Vec<_>>();
        assert_eq!(reported, batch.to_vec());
        for (nonce, hash) in everything.solutions {
            let mut expected = [0u8; POW_OUTPUT_LEN];
            cpu_ctx
                .hash_password_into(&nonce.to_le_bytes(), &header_base, &mut expected)
                .expect("CPU reference hashing should succeed");
            assert_eq!(hash, expected, "nonce {nonce}");
        }

        let _ = fs::remove_dir_all(cache_dir);
    }
}
