//! Thin dlopen-based FFI over the HIP runtime (`libamdhip64.so`) and the
//! hipRTC runtime compiler (`libhiprtc.so`).
//!
//! All declarations are hand-written so seine builds on stable Rust on any
//! Linux host without ROCm headers or toolkit installed. The libraries are
//! resolved lazily at backend startup; when they are absent the loader
//! returns a clear "ROCm runtime not found" error and the runtime
//! quarantines the backend like any other backend fault.
//!
//! Library handles are intentionally leaked (never `dlclose`d): GPU runtimes
//! commonly register process-lifetime state and unloading them mid-process is
//! a well-known source of crashes.

use std::ffi::{c_char, c_int, c_uint, c_void, CStr, CString};
use std::sync::OnceLock;

use anyhow::{anyhow, Result};

pub type HipError = c_int;
pub type HiprtcResult = c_int;
pub type HipDeviceptr = *mut c_void;
pub type HipStream = *mut c_void;
pub type HipModule = *mut c_void;
pub type HipFunction = *mut c_void;
pub type HipEvent = *mut c_void;
pub type HiprtcProgram = *mut c_void;

pub const HIP_SUCCESS: HipError = 0;
pub const HIP_ERROR_OUT_OF_MEMORY: HipError = 2;
pub const HIPRTC_SUCCESS: HiprtcResult = 0;
/// `hipStreamNonBlocking`: the stream does not implicitly synchronize with
/// the null stream. Required so cancel-flag writes on the interrupt stream
/// can overtake a long-running fill kernel.
pub const HIP_STREAM_NON_BLOCKING: c_uint = 0x1;

const HIP_LIBRARY_CANDIDATES: &[&str] = &["libamdhip64.so", "libamdhip64.so.6", "libamdhip64.so.5"];
const HIPRTC_LIBRARY_CANDIDATES: &[&str] = &["libhiprtc.so", "libhiprtc.so.6", "libhiprtc.so.5"];

pub const ROCM_RUNTIME_NOT_FOUND: &str = "ROCm runtime not found (libamdhip64.so)";
pub const HIPRTC_NOT_FOUND: &str = "ROCm hipRTC compiler not found (libhiprtc.so)";

/// Legacy ("R0000", pre-ROCm-6 ABI) `hipDeviceProp_t` layout.
///
/// ROCm 6 renamed the current-ABI symbol to `hipGetDevicePropertiesR0600` and
/// keeps exporting plain `hipGetDeviceProperties` with this legacy struct
/// layout for binary compatibility, so resolving the unsuffixed symbol gives
/// a stable ABI across ROCm 5.x and 6.x. Only fields up to and including
/// `gcn_arch_name` are consumed; the generous `_tail` padding absorbs the
/// remaining legacy fields (and any accidental over-write) without this
/// declaration having to track them exactly.
///
/// Bring-up validation: `hip_device_props_parse_sanely` (ignored, hardware)
/// asserts `warp_size ∈ {32, 64}` and `gcn_arch_name` starts with "gfx".
#[repr(C)]
pub struct HipDevicePropR0000 {
    pub name: [c_char; 256],
    pub total_global_mem: usize,
    pub shared_mem_per_block: usize,
    pub regs_per_block: c_int,
    pub warp_size: c_int,
    pub max_threads_per_block: c_int,
    pub max_threads_dim: [c_int; 3],
    pub max_grid_size: [c_int; 3],
    pub clock_rate: c_int,
    pub memory_clock_rate: c_int,
    pub memory_bus_width: c_int,
    pub total_const_mem: usize,
    pub major: c_int,
    pub minor: c_int,
    pub multi_processor_count: c_int,
    pub l2_cache_size: c_int,
    pub max_threads_per_multiprocessor: c_int,
    pub compute_mode: c_int,
    pub clock_instruction_rate: c_int,
    /// `hipDeviceArch_t`: one `unsigned int` worth of feature bitfields.
    pub arch: c_uint,
    pub concurrent_kernels: c_int,
    pub pci_domain_id: c_int,
    pub pci_bus_id: c_int,
    pub pci_device_id: c_int,
    pub max_shared_memory_per_multi_processor: usize,
    pub is_multi_gpu_board: c_int,
    pub can_map_host_memory: c_int,
    pub gcn_arch: c_int,
    pub gcn_arch_name: [c_char; 256],
    /// Padding for the remaining legacy fields the runtime writes
    /// (integrated/cooperative/texture limits/etc. — roughly 300 bytes).
    pub _tail: [u8; 2048],
}

impl Default for HipDevicePropR0000 {
    fn default() -> Self {
        // Safety: the struct is plain-old-data; an all-zero pattern is valid.
        unsafe { std::mem::zeroed() }
    }
}

impl HipDevicePropR0000 {
    pub fn device_name(&self) -> String {
        c_char_field_to_string(&self.name)
    }

    pub fn gcn_arch_name(&self) -> String {
        c_char_field_to_string(&self.gcn_arch_name)
    }
}

fn c_char_field_to_string(field: &[c_char]) -> String {
    let bytes: Vec<u8> = field
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    String::from_utf8_lossy(&bytes).trim().to_string()
}

type HipInitFn = unsafe extern "C" fn(c_uint) -> HipError;
type HipGetDeviceCountFn = unsafe extern "C" fn(*mut c_int) -> HipError;
type HipSetDeviceFn = unsafe extern "C" fn(c_int) -> HipError;
type HipGetDevicePropertiesFn = unsafe extern "C" fn(*mut HipDevicePropR0000, c_int) -> HipError;
type HipMemGetInfoFn = unsafe extern "C" fn(*mut usize, *mut usize) -> HipError;
type HipMallocFn = unsafe extern "C" fn(*mut HipDeviceptr, usize) -> HipError;
type HipFreeFn = unsafe extern "C" fn(HipDeviceptr) -> HipError;
type HipMemcpyHtoDFn = unsafe extern "C" fn(HipDeviceptr, *const c_void, usize) -> HipError;
type HipMemcpyDtoHFn = unsafe extern "C" fn(*mut c_void, HipDeviceptr, usize) -> HipError;
type HipMemcpyHtoDAsyncFn =
    unsafe extern "C" fn(HipDeviceptr, *const c_void, usize, HipStream) -> HipError;
type HipMemcpyDtoHAsyncFn =
    unsafe extern "C" fn(*mut c_void, HipDeviceptr, usize, HipStream) -> HipError;
type HipStreamCreateWithFlagsFn = unsafe extern "C" fn(*mut HipStream, c_uint) -> HipError;
type HipStreamDestroyFn = unsafe extern "C" fn(HipStream) -> HipError;
type HipStreamSynchronizeFn = unsafe extern "C" fn(HipStream) -> HipError;
type HipDeviceSynchronizeFn = unsafe extern "C" fn() -> HipError;
type HipModuleLoadDataFn = unsafe extern "C" fn(*mut HipModule, *const c_void) -> HipError;
type HipModuleUnloadFn = unsafe extern "C" fn(HipModule) -> HipError;
type HipModuleGetFunctionFn =
    unsafe extern "C" fn(*mut HipFunction, HipModule, *const c_char) -> HipError;
#[allow(clippy::too_many_arguments)]
type HipModuleLaunchKernelFn = unsafe extern "C" fn(
    HipFunction,
    c_uint,
    c_uint,
    c_uint,
    c_uint,
    c_uint,
    c_uint,
    c_uint,
    HipStream,
    *mut *mut c_void,
    *mut *mut c_void,
) -> HipError;
type HipGetErrorStringFn = unsafe extern "C" fn(HipError) -> *const c_char;
type HipGetErrorNameFn = unsafe extern "C" fn(HipError) -> *const c_char;
type HipEventCreateFn = unsafe extern "C" fn(*mut HipEvent) -> HipError;
type HipEventDestroyFn = unsafe extern "C" fn(HipEvent) -> HipError;
type HipEventRecordFn = unsafe extern "C" fn(HipEvent, HipStream) -> HipError;
type HipEventSynchronizeFn = unsafe extern "C" fn(HipEvent) -> HipError;
type HipEventElapsedTimeFn = unsafe extern "C" fn(*mut f32, HipEvent, HipEvent) -> HipError;

type HiprtcCreateProgramFn = unsafe extern "C" fn(
    *mut HiprtcProgram,
    *const c_char,
    *const c_char,
    c_int,
    *const *const c_char,
    *const *const c_char,
) -> HiprtcResult;
type HiprtcCompileProgramFn =
    unsafe extern "C" fn(HiprtcProgram, c_int, *const *const c_char) -> HiprtcResult;
type HiprtcGetProgramLogSizeFn = unsafe extern "C" fn(HiprtcProgram, *mut usize) -> HiprtcResult;
type HiprtcGetProgramLogFn = unsafe extern "C" fn(HiprtcProgram, *mut c_char) -> HiprtcResult;
type HiprtcGetCodeSizeFn = unsafe extern "C" fn(HiprtcProgram, *mut usize) -> HiprtcResult;
type HiprtcGetCodeFn = unsafe extern "C" fn(HiprtcProgram, *mut c_char) -> HiprtcResult;
type HiprtcDestroyProgramFn = unsafe extern "C" fn(*mut HiprtcProgram) -> HiprtcResult;
type HiprtcGetErrorStringFn = unsafe extern "C" fn(HiprtcResult) -> *const c_char;
type HiprtcVersionFn = unsafe extern "C" fn(*mut c_int, *mut c_int) -> HiprtcResult;

#[derive(Debug)]
struct Library {
    handle: *mut c_void,
}

// Safety: dlopen handles are process-global and the loader never closes them.
unsafe impl Send for Library {}
unsafe impl Sync for Library {}

impl Library {
    fn open(candidates: &[&str]) -> std::result::Result<Self, String> {
        let mut failures = Vec::new();
        for name in candidates {
            let c_name = CString::new(*name).expect("static soname contains no NUL");
            let handle =
                unsafe { libc::dlopen(c_name.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
            if !handle.is_null() {
                return Ok(Self { handle });
            }
            failures.push(format!("{name}: {}", last_dlerror()));
        }
        Err(failures.join("; "))
    }

    fn sym(&self, name: &str) -> std::result::Result<*mut c_void, String> {
        let c_name = CString::new(name).expect("static symbol name contains no NUL");
        let ptr = unsafe { libc::dlsym(self.handle, c_name.as_ptr()) };
        if ptr.is_null() {
            Err(format!("missing symbol {name}: {}", last_dlerror()))
        } else {
            Ok(ptr)
        }
    }

    fn has_sym(&self, name: &str) -> bool {
        self.sym(name).is_ok()
    }
}

fn last_dlerror() -> String {
    let raw = unsafe { libc::dlerror() };
    if raw.is_null() {
        "unknown dlopen/dlsym error".to_string()
    } else {
        unsafe { CStr::from_ptr(raw) }.to_string_lossy().to_string()
    }
}

macro_rules! load_sym {
    ($lib:expr, $name:literal) => {
        unsafe { std::mem::transmute($lib.sym($name).map_err(|err| err.to_string())?) }
    };
}

/// Resolved HIP + hipRTC entry points.
///
/// The surface intentionally includes a few symbols the engine does not call
/// yet (synchronous copies, device sync, events for kernel timing): they are
/// part of the planned bring-up/tuning toolkit and resolving them up front
/// keeps "runtime present but incomplete" failures at load time.
#[allow(dead_code)]
pub struct HipApi {
    _hip_lib: Library,
    _hiprtc_lib: Library,

    pub hip_init: HipInitFn,
    pub hip_get_device_count: HipGetDeviceCountFn,
    pub hip_set_device: HipSetDeviceFn,
    pub hip_get_device_properties: HipGetDevicePropertiesFn,
    pub hip_mem_get_info: HipMemGetInfoFn,
    pub hip_malloc: HipMallocFn,
    pub hip_free: HipFreeFn,
    pub hip_memcpy_htod: HipMemcpyHtoDFn,
    pub hip_memcpy_dtoh: HipMemcpyDtoHFn,
    pub hip_memcpy_htod_async: HipMemcpyHtoDAsyncFn,
    pub hip_memcpy_dtoh_async: HipMemcpyDtoHAsyncFn,
    pub hip_stream_create_with_flags: HipStreamCreateWithFlagsFn,
    pub hip_stream_destroy: HipStreamDestroyFn,
    pub hip_stream_synchronize: HipStreamSynchronizeFn,
    pub hip_device_synchronize: HipDeviceSynchronizeFn,
    pub hip_module_load_data: HipModuleLoadDataFn,
    pub hip_module_unload: HipModuleUnloadFn,
    pub hip_module_get_function: HipModuleGetFunctionFn,
    pub hip_module_launch_kernel: HipModuleLaunchKernelFn,
    pub hip_get_error_string: HipGetErrorStringFn,
    pub hip_get_error_name: HipGetErrorNameFn,
    pub hip_event_create: HipEventCreateFn,
    pub hip_event_destroy: HipEventDestroyFn,
    pub hip_event_record: HipEventRecordFn,
    pub hip_event_synchronize: HipEventSynchronizeFn,
    pub hip_event_elapsed_time: HipEventElapsedTimeFn,

    pub hiprtc_create_program: HiprtcCreateProgramFn,
    pub hiprtc_compile_program: HiprtcCompileProgramFn,
    pub hiprtc_get_program_log_size: HiprtcGetProgramLogSizeFn,
    pub hiprtc_get_program_log: HiprtcGetProgramLogFn,
    pub hiprtc_get_code_size: HiprtcGetCodeSizeFn,
    pub hiprtc_get_code: HiprtcGetCodeFn,
    pub hiprtc_destroy_program: HiprtcDestroyProgramFn,
    pub hiprtc_get_error_string: HiprtcGetErrorStringFn,
    pub hiprtc_version: HiprtcVersionFn,
}

static HIP_API: OnceLock<std::result::Result<HipApi, String>> = OnceLock::new();

impl HipApi {
    /// Load and cache the HIP/hipRTC entry points for the process lifetime.
    ///
    /// When the ROCm runtime is not installed this fails gracefully with a
    /// stable, operator-facing error message; the AMD backend surfaces it
    /// from `start()` and the runtime quarantines the backend.
    pub fn load() -> Result<&'static HipApi> {
        match HIP_API.get_or_init(Self::load_uncached) {
            Ok(api) => Ok(api),
            Err(message) => Err(anyhow!("{message}")),
        }
    }

    fn load_uncached() -> std::result::Result<HipApi, String> {
        let hip_lib = Library::open(HIP_LIBRARY_CANDIDATES)
            .map_err(|err| format!("{ROCM_RUNTIME_NOT_FOUND}: {err}"))?;

        // Older ROCm builds shipped the hipRTC entry points inside
        // libamdhip64 itself; fall back to it when libhiprtc is absent.
        let hiprtc_lib = match Library::open(HIPRTC_LIBRARY_CANDIDATES) {
            Ok(lib) => lib,
            Err(err) => {
                if hip_lib.has_sym("hiprtcCreateProgram") {
                    Library {
                        handle: hip_lib.handle,
                    }
                } else {
                    return Err(format!("{HIPRTC_NOT_FOUND}: {err}"));
                }
            }
        };

        Ok(HipApi {
            hip_init: load_sym!(hip_lib, "hipInit"),
            hip_get_device_count: load_sym!(hip_lib, "hipGetDeviceCount"),
            hip_set_device: load_sym!(hip_lib, "hipSetDevice"),
            // Deliberately the legacy unsuffixed symbol; see HipDevicePropR0000.
            hip_get_device_properties: load_sym!(hip_lib, "hipGetDeviceProperties"),
            hip_mem_get_info: load_sym!(hip_lib, "hipMemGetInfo"),
            hip_malloc: load_sym!(hip_lib, "hipMalloc"),
            hip_free: load_sym!(hip_lib, "hipFree"),
            hip_memcpy_htod: load_sym!(hip_lib, "hipMemcpyHtoD"),
            hip_memcpy_dtoh: load_sym!(hip_lib, "hipMemcpyDtoH"),
            hip_memcpy_htod_async: load_sym!(hip_lib, "hipMemcpyHtoDAsync"),
            hip_memcpy_dtoh_async: load_sym!(hip_lib, "hipMemcpyDtoHAsync"),
            hip_stream_create_with_flags: load_sym!(hip_lib, "hipStreamCreateWithFlags"),
            hip_stream_destroy: load_sym!(hip_lib, "hipStreamDestroy"),
            hip_stream_synchronize: load_sym!(hip_lib, "hipStreamSynchronize"),
            hip_device_synchronize: load_sym!(hip_lib, "hipDeviceSynchronize"),
            hip_module_load_data: load_sym!(hip_lib, "hipModuleLoadData"),
            hip_module_unload: load_sym!(hip_lib, "hipModuleUnload"),
            hip_module_get_function: load_sym!(hip_lib, "hipModuleGetFunction"),
            hip_module_launch_kernel: load_sym!(hip_lib, "hipModuleLaunchKernel"),
            hip_get_error_string: load_sym!(hip_lib, "hipGetErrorString"),
            hip_get_error_name: load_sym!(hip_lib, "hipGetErrorName"),
            hip_event_create: load_sym!(hip_lib, "hipEventCreate"),
            hip_event_destroy: load_sym!(hip_lib, "hipEventDestroy"),
            hip_event_record: load_sym!(hip_lib, "hipEventRecord"),
            hip_event_synchronize: load_sym!(hip_lib, "hipEventSynchronize"),
            hip_event_elapsed_time: load_sym!(hip_lib, "hipEventElapsedTime"),

            hiprtc_create_program: load_sym!(hiprtc_lib, "hiprtcCreateProgram"),
            hiprtc_compile_program: load_sym!(hiprtc_lib, "hiprtcCompileProgram"),
            hiprtc_get_program_log_size: load_sym!(hiprtc_lib, "hiprtcGetProgramLogSize"),
            hiprtc_get_program_log: load_sym!(hiprtc_lib, "hiprtcGetProgramLog"),
            hiprtc_get_code_size: load_sym!(hiprtc_lib, "hiprtcGetCodeSize"),
            hiprtc_get_code: load_sym!(hiprtc_lib, "hiprtcGetCode"),
            hiprtc_destroy_program: load_sym!(hiprtc_lib, "hiprtcDestroyProgram"),
            hiprtc_get_error_string: load_sym!(hiprtc_lib, "hiprtcGetErrorString"),
            hiprtc_version: load_sym!(hiprtc_lib, "hiprtcVersion"),

            _hip_lib: hip_lib,
            _hiprtc_lib: hiprtc_lib,
        })
    }

    pub fn error_string(&self, code: HipError) -> String {
        let name = c_str_or(
            unsafe { (self.hip_get_error_name)(code) },
            "hipUnknownError",
        );
        let desc = c_str_or(
            unsafe { (self.hip_get_error_string)(code) },
            "unknown error",
        );
        format!("hip error {code} ({name}): {desc}")
    }

    pub fn hiprtc_error_string(&self, code: HiprtcResult) -> String {
        let desc = c_str_or(
            unsafe { (self.hiprtc_get_error_string)(code) },
            "unknown hiprtc error",
        );
        format!("hiprtc error {code}: {desc}")
    }

    /// Bring-up diagnostic; also earmarked for a future autotune cache key
    /// (mirrors the NVIDIA backend's nvrtc compiler identity).
    #[allow(dead_code)]
    pub fn hiprtc_version_string(&self) -> String {
        let mut major: c_int = 0;
        let mut minor: c_int = 0;
        let code = unsafe { (self.hiprtc_version)(&mut major, &mut minor) };
        if code == HIPRTC_SUCCESS {
            format!("hiprtc-{major}.{minor}")
        } else {
            "hiprtc-unknown".to_string()
        }
    }

    pub fn check(&self, code: HipError, context: &str) -> Result<()> {
        if code == HIP_SUCCESS {
            Ok(())
        } else {
            Err(anyhow!("{context}: {}", self.error_string(code)))
        }
    }
}

fn c_str_or(raw: *const c_char, fallback: &str) -> String {
    if raw.is_null() {
        fallback.to_string()
    } else {
        unsafe { CStr::from_ptr(raw) }.to_string_lossy().to_string()
    }
}

pub fn is_hip_oom_code(code: HipError) -> bool {
    code == HIP_ERROR_OUT_OF_MEMORY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loader_reports_missing_rocm_runtime_gracefully() {
        // On hosts without ROCm this must produce the stable operator-facing
        // message; on ROCm hosts the load simply succeeds and the message
        // contract is covered by construction.
        match HipApi::load() {
            Ok(_) => {}
            Err(err) => {
                let message = format!("{err:#}");
                assert!(
                    message.contains(ROCM_RUNTIME_NOT_FOUND) || message.contains(HIPRTC_NOT_FOUND),
                    "unexpected loader error message: {message}"
                );
            }
        }
    }

    #[test]
    fn library_open_reports_all_candidate_failures() {
        let err = Library::open(&["libseine-definitely-absent-a.so", "libseine-absent-b.so"])
            .expect_err("bogus libraries must not load");
        assert!(err.contains("libseine-definitely-absent-a.so"));
        assert!(err.contains("libseine-absent-b.so"));
    }

    #[test]
    fn device_prop_layout_matches_legacy_abi_expectations() {
        // Offsets pinned against the pre-ROCm-6 (R0000) hipDeviceProp_t
        // layout that the legacy `hipGetDeviceProperties` symbol writes.
        assert_eq!(std::mem::offset_of!(HipDevicePropR0000, name), 0);
        assert_eq!(
            std::mem::offset_of!(HipDevicePropR0000, total_global_mem),
            256
        );
        assert_eq!(std::mem::offset_of!(HipDevicePropR0000, warp_size), 276);
        assert_eq!(std::mem::offset_of!(HipDevicePropR0000, major), 328);
        assert_eq!(std::mem::offset_of!(HipDevicePropR0000, gcn_arch_name), 396);
        // Struct must be at least as large as anything the legacy runtime
        // writes; the R0000 struct is well under 1 KiB.
        assert!(std::mem::size_of::<HipDevicePropR0000>() >= 1024);
    }

    #[test]
    fn device_prop_string_fields_trim_at_nul() {
        let mut props = HipDevicePropR0000::default();
        for (idx, byte) in b"gfx1100:sramecc-:xnack-".iter().enumerate() {
            props.gcn_arch_name[idx] = *byte as c_char;
        }
        assert_eq!(props.gcn_arch_name(), "gfx1100:sramecc-:xnack-");
        assert_eq!(props.device_name(), "");
    }

    #[test]
    fn oom_classifier_matches_hip_error_code() {
        assert!(is_hip_oom_code(HIP_ERROR_OUT_OF_MEMORY));
        assert!(!is_hip_oom_code(HIP_SUCCESS));
        assert!(!is_hip_oom_code(1));
    }
}
