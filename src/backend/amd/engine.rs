//! HIP Argon2id engine: device init, hipRTC module compilation, persistent
//! VRAM lane arenas, kernel launches, and cancel-flag plumbing.
//!
//! Mirrors the CUDA `CudaArgon2Engine` structure with a correctness-first
//! posture: one 2 GiB Argon2id arena per lane, lane count probed from free
//! VRAM with allocation backoff, fixed launch depth, no AMD-specific
//! scheduling tricks yet.

use std::ffi::{c_char, c_int, c_uint, c_void, CString};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Result};
use blocknet_pow_spec::{pow_params, CPU_LANE_MEMORY_BYTES, POW_OUTPUT_LEN};

use super::hip_ffi::{
    is_hip_oom_code, HipApi, HipDevicePropR0000, HipDeviceptr, HipError, HipFunction, HipModule,
    HipStream, HIPRTC_SUCCESS, HIP_STREAM_NON_BLOCKING, HIP_SUCCESS,
};
use crate::backend::pow_hash_from_last_block_words;
use crate::types::hash_meets_target;

pub(crate) mod blake2b;

pub(crate) const HIP_KERNEL_SRC: &str = include_str!("amd_kernel.hip");
const HIP_KERNEL_PROGRAM_NAME: &str = "seine_argon2id_fill.hip";

pub(crate) const MAX_LANES_HARD_LIMIT: usize = 1024;
/// Lanes per lane-memory allocation. One hipMalloc per 32 lanes (64 GiB) stays
/// well under the per-allocation ceiling of large cards, which otherwise caps the
/// lane probe long before VRAM runs out.
pub(crate) const LANE_CHUNK_LANES: usize = 32;
pub(crate) const KERNEL_THREADS: u32 = 32;
pub(crate) const SEED_KERNEL_THREADS: u32 = 64;
pub(crate) const EVAL_KERNEL_THREADS: u32 = 64;
/// Hash lanes per thread block. The cooperative group is always 32 threads, so a
/// block is exactly one wavefront: one lane on wave32 (RDNA), two lanes on wave64
/// (CDNA), where `__shfl` with a width of 32 keeps each lane's shuffles inside its
/// own half of the wavefront.
pub(crate) fn warps_per_block(wavefront_size: u32) -> u32 {
    (wavefront_size / KERNEL_THREADS).max(1)
}
/// AMD keeps the conservative default cancel-check cadence; the Ampere+
/// widening in the CUDA backend is NVIDIA-specific.
pub(crate) const CANCEL_CHECK_BLOCK_INTERVAL: u32 = 64;
pub(crate) const AMD_MEMORY_RESERVE_MIB_FLOOR: u64 = 64;
pub(crate) const AMD_MEMORY_RESERVE_RATIO_DENOM: u64 = 64;
/// Wavefront sizes the kernel contract covers: wave32 (RDNA gfx10-gfx12, one
/// lane per wavefront) and wave64 (CDNA gfx9, two lanes per wavefront).
pub(crate) const SUPPORTED_WAVEFRONT_SIZES: [u32; 2] = [32, 64];

pub(crate) fn supports_wavefront_size(wavefront_size: u32) -> bool {
    SUPPORTED_WAVEFRONT_SIZES.contains(&wavefront_size)
}

#[derive(Debug, Clone)]
pub(crate) struct AmdDeviceInfo {
    pub index: u32,
    pub name: String,
    pub gcn_arch_name: String,
    pub wavefront_size: u32,
    pub memory_total_mib: u64,
    pub memory_free_mib: Option<u64>,
    /// PCI address (`0000:c1:00.0`), the key for the device's amdgpu sysfs entries.
    pub pci_address: String,
    /// The GPU shares system memory (APU).
    pub integrated: bool,
}

/// Query all visible HIP devices. Fails with the graceful "ROCm runtime not
/// found" error when the HIP libraries are absent.
pub(crate) fn query_amd_devices() -> Result<Vec<AmdDeviceInfo>> {
    let api = HipApi::load()?;
    api.check(unsafe { (api.hip_init)(0) }, "hipInit failed")?;
    let mut count: c_int = 0;
    api.check(
        unsafe { (api.hip_get_device_count)(&mut count) },
        "hipGetDeviceCount failed",
    )?;
    if count <= 0 {
        bail!("HIP runtime loaded but reported no AMD devices");
    }

    let mut devices = Vec::with_capacity(count as usize);
    for index in 0..count {
        let props = query_device_props(api, index)?;
        let memory_free_mib = query_free_memory_mib(api, index);
        devices.push(AmdDeviceInfo {
            index: index as u32,
            name: props.device_name(),
            gcn_arch_name: props.gcn_arch_name(),
            wavefront_size: props.warp_size.max(0) as u32,
            memory_total_mib: (props.total_global_mem as u64) / (1024 * 1024),
            memory_free_mib,
            pci_address: format!(
                "{:04x}:{:02x}:{:02x}.0",
                props.pci_domain_id.max(0),
                props.pci_bus_id.max(0),
                props.pci_device_id.max(0)
            ),
            integrated: props.integrated != 0,
        });
    }
    Ok(devices)
}

fn query_device_props(api: &'static HipApi, index: c_int) -> Result<HipDevicePropR0000> {
    let mut props = HipDevicePropR0000::default();
    api.check(
        unsafe { (api.hip_get_device_properties)(&mut props, index) },
        &format!("hipGetDeviceProperties failed for device {index}"),
    )?;
    validate_device_props(&props, index as u32)?;
    Ok(props)
}

/// Guard against legacy-ABI drift: if the struct layout assumption were ever
/// wrong these fields would read as garbage, so refuse to continue instead
/// of mining with a corrupt device description.
pub(crate) fn validate_device_props(props: &HipDevicePropR0000, index: u32) -> Result<()> {
    let warp_size = props.warp_size;
    if warp_size != 32 && warp_size != 64 {
        bail!(
            "AMD device {index} reports implausible wavefront size {warp_size}; \
             hipDeviceProp legacy-ABI layout mismatch suspected (ROCm version incompatibility)"
        );
    }
    if props.total_global_mem == 0 {
        bail!(
            "AMD device {index} reports zero total memory; \
             hipDeviceProp legacy-ABI layout mismatch suspected (ROCm version incompatibility)"
        );
    }
    let arch = props.gcn_arch_name();
    if !arch.starts_with("gfx") {
        bail!(
            "AMD device {index} reports unexpected gcnArchName '{arch}'; \
             hipDeviceProp legacy-ABI layout mismatch suspected (ROCm version incompatibility)"
        );
    }
    Ok(())
}

/// Enforce the kernel's wavefront contract (32-thread cooperative groups packed
/// one per wave32 wavefront or two per wave64 wavefront); anything else is
/// refused with a clear message so the runtime quarantines it at startup.
pub(crate) fn validate_wavefront_size(
    wavefront_size: u32,
    device_index: u32,
    device_name: &str,
    gcn_arch_name: &str,
) -> Result<()> {
    if !supports_wavefront_size(wavefront_size) {
        bail!(
            "AMD device {device_index} ({device_name}, {gcn_arch_name}) reports wavefront size \
             {wavefront_size}; the AMD backend supports wave32 (RDNA gfx10-gfx12) and \
             wave64 (CDNA gfx9) devices only."
        );
    }
    Ok(())
}

fn query_free_memory_mib(api: &'static HipApi, index: c_int) -> Option<u64> {
    if unsafe { (api.hip_set_device)(index) } != HIP_SUCCESS {
        return None;
    }
    let mut free: usize = 0;
    let mut total: usize = 0;
    if unsafe { (api.hip_mem_get_info)(&mut free, &mut total) } == HIP_SUCCESS {
        Some((free as u64) / (1024 * 1024))
    } else {
        None
    }
}

pub(crate) fn derive_memory_budget_mib(memory_total_mib: u64, memory_free_mib: Option<u64>) -> u64 {
    let total = memory_total_mib.max(1);
    let reserve = (total / AMD_MEMORY_RESERVE_RATIO_DENOM)
        .max(AMD_MEMORY_RESERVE_MIB_FLOOR)
        .min(total.saturating_sub(1).max(1));

    let available = memory_free_mib.unwrap_or(total).max(1).min(total);
    available.saturating_sub(reserve).max(1)
}

pub(crate) fn lane_capacity_from_budget(memory_budget_mib: u64, lane_bytes: u64) -> usize {
    let memory_budget_bytes = memory_budget_mib.saturating_mul(1024 * 1024);
    usize::try_from((memory_budget_bytes / lane_bytes.max(1)).max(1))
        .unwrap_or(usize::MAX)
        .min(MAX_LANES_HARD_LIMIT)
        .max(1)
}

/// hipRTC options for the kernel build.
///
/// `--offload-arch` pins the exact device target (full target-id including
/// sramecc/xnack features as reported by gcnArchName). The wavefront flag pins
/// the compile-time wavefront size to what the device reported (gfx10+ default
/// to wave32 and gfx9 to wave64 anyway); if a hipRTC build rejects the flag the
/// engine retries without it and relies on the reported `warpSize`.
/// `SEINE_WARPS_PER_BLOCK` tells the kernel how many 32-thread lanes share one
/// block, which is always exactly one wavefront.
/// Default skip boundary (permille of the arena) per GPU architecture; 0 stores every
/// block. RDNA3 (gfx110x).
fn default_skip_permille(gcn_arch_name: &str) -> u64 {
    if gcn_arch_name.trim().starts_with("gfx110") {
        500
    } else {
        0
    }
}

/// First block whose odd successors are not stored (`SEINE_AMD_SKIP_FROM`), or 0 to
/// store every block. `SEINE_AMD_SKIP_PERMILLE` (500-999, or 0 for off) overrides the
/// per-architecture default; single-pass only, never below the data-independent half.
pub(crate) fn skip_from_block(gcn_arch_name: &str, m_blocks: u32, t_cost: u32) -> u32 {
    let permille = std::env::var("SEINE_AMD_SKIP_PERMILLE")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or_else(|| default_skip_permille(gcn_arch_name));
    if t_cost != 1 || !(500..1000).contains(&permille) {
        return 0;
    }
    let from = (u64::from(m_blocks) * permille / 1000) as u32 & !1;
    from.max(m_blocks / 2)
}

/// Blocks each lane occupies in device memory (see the layout in amd_kernel.hip).
pub(crate) fn lane_stride_blocks(m_blocks: u32, skip_from: u32) -> u64 {
    if skip_from == 0 {
        u64::from(m_blocks)
    } else {
        u64::from(skip_from) + u64::from((m_blocks - skip_from) / 2) + 4
    }
}

pub(crate) fn build_hiprtc_options(
    gcn_arch_name: &str,
    wavefront_size: u32,
    m_blocks: u32,
    t_cost: u32,
) -> Vec<String> {
    let wavefront_flag = if wavefront_size == 64 {
        "-mwavefrontsize64"
    } else {
        "-mno-wavefrontsize64"
    };
    let mut options = vec![
        format!("--offload-arch={}", gcn_arch_name.trim()),
        wavefront_flag.to_string(),
        format!("-DSEINE_FIXED_M_BLOCKS={}U", m_blocks),
        format!("-DSEINE_FIXED_T_COST={}U", t_cost),
        format!(
            "-DSEINE_AMD_SKIP_FROM={}U",
            skip_from_block(gcn_arch_name, m_blocks, t_cost)
        ),
        format!(
            "-DSEINE_CANCEL_CHECK_BLOCK_INTERVAL={}U",
            CANCEL_CHECK_BLOCK_INTERVAL
        ),
        format!(
            "-DSEINE_WARPS_PER_BLOCK={}U",
            warps_per_block(wavefront_size)
        ),
    ];
    // Wave-scoped cooperative barriers instead of __syncthreads(): the
    // cooperative group never spans more than one wavefront, so the LLVM AMDGPU
    // memory model needs no waits at wavefront scope. SEINE_AMD_BLOCK_SYNC=1
    // restores the block barrier.
    if std::env::var_os("SEINE_AMD_BLOCK_SYNC").is_none() {
        options.push("-DSEINE_WAVE_SYNC=1".to_string());
    }
    // SEINE_AMD_DEFINES="NAME=1,OTHER=0" adds one -D per comma-separated entry.
    if let Some(extra) = std::env::var_os("SEINE_AMD_DEFINES") {
        for define in extra
            .to_string_lossy()
            .split(',')
            .map(str::trim)
            .filter(|define| !define.is_empty())
        {
            options.push(format!("-D{define}"));
        }
    }
    options
}

pub(crate) fn is_hip_oom_error_message(message: &str) -> bool {
    message.contains("hipErrorOutOfMemory") || message.contains("out of memory")
}

struct Stream {
    api: &'static HipApi,
    raw: HipStream,
}

// Safety: HIP streams are process-wide handles usable from any host thread.
unsafe impl Send for Stream {}
unsafe impl Sync for Stream {}

impl Stream {
    fn new(api: &'static HipApi) -> Result<Self> {
        let mut raw: HipStream = std::ptr::null_mut();
        api.check(
            unsafe { (api.hip_stream_create_with_flags)(&mut raw, HIP_STREAM_NON_BLOCKING) },
            "hipStreamCreateWithFlags failed",
        )?;
        Ok(Self { api, raw })
    }

    fn synchronize(&self) -> Result<()> {
        self.api.check(
            unsafe { (self.api.hip_stream_synchronize)(self.raw) },
            "hipStreamSynchronize failed",
        )
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            let _ = unsafe { (self.api.hip_stream_destroy)(self.raw) };
        }
    }
}

struct Module {
    api: &'static HipApi,
    raw: HipModule,
}

// Safety: HIP modules are process-wide handles usable from any host thread.
unsafe impl Send for Module {}
unsafe impl Sync for Module {}

impl Module {
    fn load(api: &'static HipApi, code: &[u8]) -> Result<Self> {
        let mut raw: HipModule = std::ptr::null_mut();
        api.check(
            unsafe { (api.hip_module_load_data)(&mut raw, code.as_ptr() as *const c_void) },
            "hipModuleLoadData failed",
        )?;
        Ok(Self { api, raw })
    }

    fn function(&self, name: &str) -> Result<Function> {
        let c_name = CString::new(name).expect("static kernel name contains no NUL");
        let mut raw: HipFunction = std::ptr::null_mut();
        self.api.check(
            unsafe { (self.api.hip_module_get_function)(&mut raw, self.raw, c_name.as_ptr()) },
            &format!("hipModuleGetFunction failed for '{name}'"),
        )?;
        Ok(Function { raw })
    }
}

impl Drop for Module {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            let _ = unsafe { (self.api.hip_module_unload)(self.raw) };
        }
    }
}

#[derive(Clone, Copy)]
struct Function {
    raw: HipFunction,
}

// Safety: HIP function handles are process-wide and immutable.
unsafe impl Send for Function {}
unsafe impl Sync for Function {}

pub(crate) struct DeviceBuffer {
    api: &'static HipApi,
    ptr: HipDeviceptr,
    bytes: usize,
}

// Safety: HIP device pointers are process-wide and freed exactly once (Drop).
unsafe impl Send for DeviceBuffer {}

impl DeviceBuffer {
    fn alloc(api: &'static HipApi, bytes: usize) -> std::result::Result<Self, HipError> {
        let mut ptr: HipDeviceptr = std::ptr::null_mut();
        let code = unsafe { (api.hip_malloc)(&mut ptr, bytes.max(1)) };
        if code != HIP_SUCCESS {
            return Err(code);
        }
        Ok(Self {
            api,
            ptr,
            bytes: bytes.max(1),
        })
    }

    fn device_ptr(&self) -> HipDeviceptr {
        self.ptr
    }
}

impl Drop for DeviceBuffer {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            let _ = unsafe { (self.api.hip_free)(self.ptr) };
        }
    }
}

/// Handle used from the backend control path to signal an in-flight kernel
/// to abort at its next cancel checkpoint. Uses its own non-blocking stream
/// so the 4-byte flag write is not queued behind the running fill kernel.
#[derive(Clone)]
pub(crate) struct HipInterruptController {
    api: &'static HipApi,
    device_index: u32,
    stream: Arc<Stream>,
    cancel_flag: Arc<Mutex<DeviceBuffer>>,
}

impl HipInterruptController {
    pub(crate) fn signal_cancel(&self) -> Result<()> {
        // Bind the caller thread to the engine's device before touching it.
        self.api.check(
            unsafe { (self.api.hip_set_device)(self.device_index as c_int) },
            "hipSetDevice failed while signalling cancel",
        )?;
        let flag = self
            .cancel_flag
            .lock()
            .map_err(|_| anyhow!("amd cancel flag lock poisoned"))?;
        let value: u32 = 1;
        self.api.check(
            unsafe {
                (self.api.hip_memcpy_htod_async)(
                    flag.device_ptr(),
                    (&value as *const u32).cast::<c_void>(),
                    std::mem::size_of::<u32>(),
                    self.stream.raw,
                )
            },
            "hipMemcpyHtoDAsync failed while signalling cancel",
        )?;
        self.stream.synchronize()
    }
}

pub(crate) struct FillBatchResult {
    pub hashes_done: usize,
    pub solved_nonce: Option<u64>,
    pub solved_hash: Option<[u8; 32]>,
    /// Further nonces in the batch that meet the share target.
    pub extra_solutions: Vec<(u64, [u8; 32])>,
}

pub(crate) struct HipArgon2Engine {
    api: &'static HipApi,
    device_index: u32,
    stream: Stream,
    interrupt_stream: Arc<Stream>,
    _module: Module,
    kernel_nonfused: Function,
    #[allow(dead_code)]
    kernel_fused_target: Function,
    seed_kernel: Function,
    evaluate_kernel: Function,
    m_blocks: u32,
    m_cost_kib: u32,
    t_cost: u32,
    max_lanes: usize,
    hashes_per_launch_per_lane: usize,
    /// Owns the per-chunk lane buffers the device-side table points into.
    _lane_chunks: Vec<DeviceBuffer>,
    lane_chunk_table: DeviceBuffer,
    lanes_per_chunk: u32,
    seed_blocks: DeviceBuffer,
    last_blocks: DeviceBuffer,
    nonce_input: DeviceBuffer,
    header_base_input: DeviceBuffer,
    target_input: DeviceBuffer,
    cancel_flag: Arc<Mutex<DeviceBuffer>>,
    completed_iters: DeviceBuffer,
    found_index: DeviceBuffer,
    cached_header_base: Vec<u8>,
    cached_target: Option<[u8; POW_OUTPUT_LEN]>,
    /// Lanes per fill-kernel block (one wavefront): 1 on wave32, 2 on wave64.
    warps_per_block: u32,
}

struct EngineBuffers {
    lane_chunks: Vec<DeviceBuffer>,
    lane_chunk_table: DeviceBuffer,
    lanes_per_chunk: u32,
    seed_blocks: DeviceBuffer,
    last_blocks: DeviceBuffer,
    nonce_input: DeviceBuffer,
    header_base_input: DeviceBuffer,
    target_input: DeviceBuffer,
    cancel_flag: DeviceBuffer,
    completed_iters: DeviceBuffer,
    found_index: DeviceBuffer,
}

impl HipArgon2Engine {
    pub(crate) fn new(
        device: &AmdDeviceInfo,
        max_lanes_override: Option<usize>,
        hashes_per_launch_per_lane: u32,
    ) -> Result<Self> {
        let api = HipApi::load()?;
        api.check(unsafe { (api.hip_init)(0) }, "hipInit failed")?;

        validate_wavefront_size(
            device.wavefront_size,
            device.index,
            &device.name,
            &device.gcn_arch_name,
        )?;

        api.check(
            unsafe { (api.hip_set_device)(device.index as c_int) },
            &format!("hipSetDevice failed for AMD device {}", device.index),
        )?;

        let params = pow_params()
            .map_err(|err| anyhow!("invalid Argon2 parameters for AMD backend: {err}"))?;
        let m_blocks = params.block_count() as u32;
        let m_cost_kib = params.m_cost();
        let t_cost = params.t_cost();

        let options = build_hiprtc_options(
            &device.gcn_arch_name,
            device.wavefront_size,
            m_blocks,
            t_cost,
        );
        let code = argon2_kernel_code(api, &device.gcn_arch_name, &options)?;
        let module = Module::load(api, &code)?;
        let kernel_nonfused = module.function("argon2id_fill_kernel")?;
        let kernel_fused_target = module.function("argon2id_fill_kernel_fused_target")?;
        let seed_kernel = module.function("build_seed_blocks_kernel")?;
        let evaluate_kernel = module.function("evaluate_hashes_kernel")?;
        let touch_kernel = module.function("touch_lane_memory_kernel")?;

        let stream = Stream::new(api)?;
        let interrupt_stream = Arc::new(Stream::new(api)?);

        let lane_blocks = lane_stride_blocks(
            m_blocks,
            skip_from_block(&device.gcn_arch_name, m_blocks, t_cost),
        );
        let lane_bytes = if lane_blocks == u64::from(m_blocks) {
            CPU_LANE_MEMORY_BYTES.max(lane_blocks * 1024)
        } else {
            lane_blocks * 1024
        };
        let memory_budget_mib =
            derive_memory_budget_mib(device.memory_total_mib, device.memory_free_mib);
        let max_lanes_budget = lane_capacity_from_budget(memory_budget_mib, lane_bytes);
        let max_lanes = max_lanes_override
            .map(|lanes| lanes.max(1))
            .unwrap_or(max_lanes_budget)
            .min(MAX_LANES_HARD_LIMIT)
            .max(1);
        let lane_stride_words = lane_blocks.saturating_mul(128);
        let hashes_per_launch_per_lane = hashes_per_launch_per_lane.max(1) as usize;

        // Lane probe with allocation backoff: descend from the budgeted lane
        // count until both allocation and a physical-commit touch pass.
        let mut selected: Option<(usize, EngineBuffers)> = None;
        let mut last_error: Option<String> = None;
        for lanes in (1..=max_lanes).rev() {
            match try_allocate_hip_buffers(
                api,
                &stream,
                lane_stride_words,
                lanes,
                hashes_per_launch_per_lane,
            ) {
                Ok(buffers) => {
                    match probe_hip_lane_memory(
                        api,
                        &stream,
                        &touch_kernel,
                        &buffers.lane_chunk_table,
                        buffers.lanes_per_chunk,
                        lane_stride_words,
                        lanes,
                    ) {
                        Ok(()) => {
                            selected = Some((lanes, buffers));
                            break;
                        }
                        Err(err) if lanes > 1 && is_hip_oom_error_message(&format!("{err:#}")) => {
                            last_error = Some(format!("{err:#}"));
                            continue;
                        }
                        Err(err) => {
                            return Err(anyhow!(
                                "failed to validate HIP buffers for {lanes} lanes on device {}: {err:#}",
                                device.index
                            ));
                        }
                    }
                }
                Err(code) if lanes > 1 && is_hip_oom_code(code) => {
                    last_error = Some(api.error_string(code));
                    continue;
                }
                Err(code) => {
                    return Err(anyhow!(
                        "failed to allocate HIP buffers for {lanes} lanes on device {}: {}",
                        device.index,
                        api.error_string(code)
                    ));
                }
            }
        }

        let (max_lanes, buffers) = selected.ok_or_else(|| {
            anyhow!(
                "failed to allocate HIP buffers for any lane count on device {}{}",
                device.index,
                last_error
                    .map(|err| format!(" (last error: {err})"))
                    .unwrap_or_default()
            )
        })?;

        Ok(Self {
            api,
            device_index: device.index,
            stream,
            interrupt_stream,
            _module: module,
            kernel_nonfused,
            kernel_fused_target,
            seed_kernel,
            evaluate_kernel,
            m_blocks,
            m_cost_kib,
            t_cost,
            max_lanes,
            hashes_per_launch_per_lane,
            _lane_chunks: buffers.lane_chunks,
            lane_chunk_table: buffers.lane_chunk_table,
            lanes_per_chunk: buffers.lanes_per_chunk,
            seed_blocks: buffers.seed_blocks,
            last_blocks: buffers.last_blocks,
            nonce_input: buffers.nonce_input,
            header_base_input: buffers.header_base_input,
            target_input: buffers.target_input,
            cancel_flag: Arc::new(Mutex::new(buffers.cancel_flag)),
            completed_iters: buffers.completed_iters,
            found_index: buffers.found_index,
            cached_header_base: Vec::new(),
            cached_target: None,
            warps_per_block: warps_per_block(device.wavefront_size),
        })
    }

    pub(crate) fn max_lanes(&self) -> usize {
        self.max_lanes
    }

    pub(crate) fn max_hashes_per_launch_per_lane(&self) -> usize {
        self.hashes_per_launch_per_lane.max(1)
    }

    pub(crate) fn max_hashes_per_launch(&self) -> usize {
        self.max_lanes
            .saturating_mul(self.hashes_per_launch_per_lane)
            .max(1)
    }

    pub(crate) fn interrupt_controller(&self) -> HipInterruptController {
        HipInterruptController {
            api: self.api,
            device_index: self.device_index,
            stream: Arc::clone(&self.interrupt_stream),
            cancel_flag: Arc::clone(&self.cancel_flag),
        }
    }

    /// Bind the calling thread to this engine's device. HIP device selection
    /// is per host thread, so the worker thread must call this before the
    /// first launch (the engine may have been created on another thread).
    pub(crate) fn bind_thread(&self) -> Result<()> {
        self.api.check(
            unsafe { (self.api.hip_set_device)(self.device_index as c_int) },
            "hipSetDevice failed while binding worker thread",
        )
    }

    fn htod(&self, dst: &DeviceBuffer, offset_bytes: usize, src: &[u8]) -> Result<()> {
        if src.is_empty() {
            return Ok(());
        }
        let end = offset_bytes
            .checked_add(src.len())
            .ok_or_else(|| anyhow!("HIP host-to-device copy range overflow"))?;
        if end > dst.bytes {
            bail!(
                "HIP host-to-device copy out of bounds ({} + {} > {})",
                offset_bytes,
                src.len(),
                dst.bytes
            );
        }
        let dst_ptr = unsafe { (dst.device_ptr() as *mut u8).add(offset_bytes) };
        self.api.check(
            unsafe {
                (self.api.hip_memcpy_htod_async)(
                    dst_ptr.cast::<c_void>(),
                    src.as_ptr().cast::<c_void>(),
                    src.len(),
                    self.stream.raw,
                )
            },
            "hipMemcpyHtoDAsync failed",
        )
    }

    fn dtoh(&self, src: &DeviceBuffer, offset_bytes: usize, dst: &mut [u8]) -> Result<()> {
        if dst.is_empty() {
            return Ok(());
        }
        let end = offset_bytes
            .checked_add(dst.len())
            .ok_or_else(|| anyhow!("HIP device-to-host copy range overflow"))?;
        if end > src.bytes {
            bail!(
                "HIP device-to-host copy out of bounds ({} + {} > {})",
                offset_bytes,
                dst.len(),
                src.bytes
            );
        }
        let src_ptr = unsafe { (src.device_ptr() as *mut u8).add(offset_bytes) };
        self.api.check(
            unsafe {
                (self.api.hip_memcpy_dtoh_async)(
                    dst.as_mut_ptr().cast::<c_void>(),
                    src_ptr.cast::<c_void>(),
                    dst.len(),
                    self.stream.raw,
                )
            },
            "hipMemcpyDtoHAsync failed",
        )?;
        self.stream.synchronize()
    }

    fn launch(
        &self,
        function: &Function,
        grid: (u32, u32, u32),
        block: (u32, u32, u32),
        params: &mut [*mut c_void],
    ) -> Result<()> {
        self.api.check(
            unsafe {
                (self.api.hip_module_launch_kernel)(
                    function.raw,
                    grid.0 as c_uint,
                    grid.1 as c_uint,
                    grid.2 as c_uint,
                    block.0 as c_uint,
                    block.1 as c_uint,
                    block.2 as c_uint,
                    0,
                    self.stream.raw,
                    params.as_mut_ptr(),
                    std::ptr::null_mut(),
                )
            },
            "hipModuleLaunchKernel failed",
        )
    }

    pub(crate) fn run_fill_batch(
        &mut self,
        header_base: &[u8],
        nonces: &[u64],
        target: Option<&[u8; POW_OUTPUT_LEN]>,
    ) -> Result<FillBatchResult> {
        self.run_fill_batch_preserving_candidate(header_base, nonces, target, None)
    }

    pub(crate) fn run_fill_batch_preserving_candidate(
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
                solved_nonce: None,
                solved_hash: None,
                extra_solutions: Vec::new(),
            });
        }

        let lanes_active = self.max_lanes.min(nonces.len()).max(1);
        let max_hashes = lanes_active
            .saturating_mul(self.hashes_per_launch_per_lane)
            .max(1);
        if nonces.len() > max_hashes {
            bail!(
                "batch size {} exceeds configured HIP launch capacity {} (lanes={} hashes_per_launch_per_lane={})",
                nonces.len(),
                max_hashes,
                lanes_active,
                self.hashes_per_launch_per_lane
            );
        }

        let requested_hashes = nonces.len();
        if self.cached_header_base != header_base {
            self.htod(&self.header_base_input, 0, header_base)?;
            self.cached_header_base.clear();
            self.cached_header_base.extend_from_slice(header_base);
        }
        {
            let mut nonce_bytes = Vec::with_capacity(requested_hashes * 8);
            for nonce in nonces {
                nonce_bytes.extend_from_slice(&nonce.to_le_bytes());
            }
            self.htod(&self.nonce_input, 0, &nonce_bytes)?;
        }
        self.htod(&self.completed_iters, 0, &0u32.to_le_bytes())?;
        {
            let cancel_flag = self
                .cancel_flag
                .lock()
                .map_err(|_| anyhow!("amd cancel flag lock poisoned"))?;
            let zero = 0u32.to_le_bytes();
            let dst_ptr = cancel_flag.device_ptr();
            self.api.check(
                unsafe {
                    (self.api.hip_memcpy_htod_async)(
                        dst_ptr,
                        zero.as_ptr().cast::<c_void>(),
                        zero.len(),
                        self.stream.raw,
                    )
                },
                "hipMemcpyHtoDAsync failed while clearing cancel flag",
            )?;
        }
        if let Some(target_hash) = target {
            if self.cached_target.as_ref() != Some(target_hash) {
                self.htod(&self.target_input, 0, target_hash)?;
                self.cached_target = Some(*target_hash);
            }
        }

        let lanes_u32 = u32::try_from(lanes_active).map_err(|_| anyhow!("lane count overflow"))?;
        let requested_hashes_u32 =
            u32::try_from(requested_hashes).map_err(|_| anyhow!("active hash count overflow"))?;
        let lane_launch_iters_u32 = u32::try_from(plan_lane_launch_iters(
            requested_hashes as u64,
            lanes_active as u64,
        ))
        .map_err(|_| anyhow!("lane launch iteration overflow"))?;

        // Seed kernel: one thread per hash.
        {
            let grid = requested_hashes_u32
                .saturating_add(SEED_KERNEL_THREADS.saturating_sub(1))
                .saturating_div(SEED_KERNEL_THREADS)
                .max(1);
            let mut header_ptr = self.header_base_input.device_ptr();
            let mut header_len = header_len;
            let mut nonce_ptr = self.nonce_input.device_ptr();
            let mut active = requested_hashes_u32;
            let mut m_cost_kib = self.m_cost_kib;
            let mut t_cost = self.t_cost;
            let mut seed_ptr = self.seed_blocks.device_ptr();
            let mut params: [*mut c_void; 7] = [
                (&mut header_ptr as *mut HipDeviceptr).cast(),
                (&mut header_len as *mut u32).cast(),
                (&mut nonce_ptr as *mut HipDeviceptr).cast(),
                (&mut active as *mut u32).cast(),
                (&mut m_cost_kib as *mut u32).cast(),
                (&mut t_cost as *mut u32).cast(),
                (&mut seed_ptr as *mut HipDeviceptr).cast(),
            ];
            self.launch(
                &self.seed_kernel,
                (grid, 1, 1),
                (SEED_KERNEL_THREADS, 1, 1),
                &mut params,
            )?;
        }

        // Fill kernel: one wave32 cooperative group per lane.
        {
            let grid_x = (lanes_u32 + self.warps_per_block - 1) / self.warps_per_block;
            let cancel_flag = self
                .cancel_flag
                .lock()
                .map_err(|_| anyhow!("amd cancel flag lock poisoned"))?;
            let mut seed_ptr = self.seed_blocks.device_ptr();
            let mut lanes = lanes_u32;
            let mut active = requested_hashes_u32;
            let mut iters = lane_launch_iters_u32;
            let mut m_blocks = self.m_blocks;
            let mut t_cost = self.t_cost;
            let mut lane_ptr = self.lane_chunk_table.device_ptr();
            let mut lanes_per_chunk = self.lanes_per_chunk;
            let mut last_ptr = self.last_blocks.device_ptr();
            let mut cancel_ptr = cancel_flag.device_ptr();
            let mut completed_ptr = self.completed_iters.device_ptr();
            let mut params: [*mut c_void; 11] = [
                (&mut seed_ptr as *mut HipDeviceptr).cast(),
                (&mut lanes as *mut u32).cast(),
                (&mut active as *mut u32).cast(),
                (&mut iters as *mut u32).cast(),
                (&mut m_blocks as *mut u32).cast(),
                (&mut t_cost as *mut u32).cast(),
                (&mut lane_ptr as *mut HipDeviceptr).cast(),
                (&mut lanes_per_chunk as *mut u32).cast(),
                (&mut last_ptr as *mut HipDeviceptr).cast(),
                (&mut cancel_ptr as *mut HipDeviceptr).cast(),
                (&mut completed_ptr as *mut HipDeviceptr).cast(),
            ];
            self.launch(
                &self.kernel_nonfused,
                (grid_x, 1, 1),
                (KERNEL_THREADS * self.warps_per_block, 1, 1),
                &mut params,
            )?;
        }
        self.stream.synchronize()?;

        let mut completed_bytes = [0u8; 4];
        self.dtoh(&self.completed_iters, 0, &mut completed_bytes)?;
        let completed_iters = u32::from_le_bytes(completed_bytes).min(lane_launch_iters_u32);
        let hashes_done =
            requested_hashes.min((completed_iters as usize).saturating_mul(lanes_active));
        if hashes_done == 0 {
            return Ok(FillBatchResult {
                hashes_done: 0,
                solved_nonce: None,
                solved_hash: None,
                extra_solutions: Vec::new(),
            });
        }

        let mut solved_nonce = None;
        let mut solved_hash = None;
        let mut extra_solutions = Vec::new();
        if let Some(share_target) = target {
            let evaluated_hashes_u32 =
                u32::try_from(hashes_done).map_err(|_| anyhow!("active hash count overflow"))?;
            self.htod(&self.found_index, 0, &u32::MAX.to_le_bytes())?;
            {
                let grid = evaluated_hashes_u32
                    .saturating_add(EVAL_KERNEL_THREADS.saturating_sub(1))
                    .saturating_div(EVAL_KERNEL_THREADS)
                    .max(1);
                let mut last_ptr = self.last_blocks.device_ptr();
                let mut active = evaluated_hashes_u32;
                let mut target_ptr = self.target_input.device_ptr();
                let mut found_ptr = self.found_index.device_ptr();
                let mut params: [*mut c_void; 4] = [
                    (&mut last_ptr as *mut HipDeviceptr).cast(),
                    (&mut active as *mut u32).cast(),
                    (&mut target_ptr as *mut HipDeviceptr).cast(),
                    (&mut found_ptr as *mut HipDeviceptr).cast(),
                ];
                self.launch(
                    &self.evaluate_kernel,
                    (grid, 1, 1),
                    (EVAL_KERNEL_THREADS, 1, 1),
                    &mut params,
                )?;
            }
            let mut found_bytes = [0u8; 4];
            self.dtoh(&self.found_index, 0, &mut found_bytes)?;
            let found_index = u32::from_le_bytes(found_bytes);
            if found_index != u32::MAX && found_index > 0 && (found_index as usize) <= hashes_done {
                // The kernel reports the lowest hit; every later hash in the batch
                // is checked here so no share is dropped. A nonce meeting the
                // (harder) network target goes first so block candidates are
                // never displaced by a share-only nonce.
                let first = (found_index - 1) as usize;
                let mut hits = Vec::new();
                for (offset, hash) in self
                    .read_last_block_hashes(first, hashes_done)?
                    .into_iter()
                    .enumerate()
                {
                    if offset == 0 || hash_meets_target(&hash, share_target) {
                        hits.push((nonces[first + offset], hash));
                    }
                }
                if let Some(network_target) = network_target {
                    if let Some(pos) = hits
                        .iter()
                        .position(|(_, hash)| hash_meets_target(hash, network_target))
                    {
                        let candidate = hits.remove(pos);
                        hits.insert(0, candidate);
                    }
                }
                let mut hits = hits.into_iter();
                if let Some((nonce, hash)) = hits.next() {
                    solved_nonce = Some(nonce);
                    solved_hash = Some(hash);
                }
                extra_solutions.extend(hits);
            }
        }

        Ok(FillBatchResult {
            hashes_done,
            solved_nonce,
            solved_hash,
            extra_solutions,
        })
    }

    /// Pow hashes of last blocks `start..end` of the current batch, read in one copy.
    fn read_last_block_hashes(&self, start: usize, end: usize) -> Result<Vec<[u8; 32]>> {
        let count = end.saturating_sub(start);
        let mut bytes = vec![0u8; count * 128 * 8];
        if count > 0 {
            let offset = start
                .checked_mul(128 * 8)
                .ok_or_else(|| anyhow!("hash index overflow while reading HIP last blocks"))?;
            self.dtoh(&self.last_blocks, offset, &mut bytes)?;
        }
        Ok(bytes
            .chunks_exact(128 * 8)
            .map(|block| {
                let mut words = [0u64; 128];
                for (word, le) in words.iter_mut().zip(block.chunks_exact(8)) {
                    *word = u64::from_le_bytes(le.try_into().expect("8-byte chunk"));
                }
                pow_hash_from_last_block_words(&words)
            })
            .collect())
    }
}

/// Ceil-divide requested hashes over active lanes to get per-lane launch
/// iterations (the kernel's `lane_launch_iters` argument).
pub(crate) fn plan_lane_launch_iters(requested_hashes: u64, lanes_active: u64) -> u64 {
    let lanes = lanes_active.max(1);
    requested_hashes
        .saturating_add(lanes.saturating_sub(1))
        .saturating_div(lanes)
        .max(1)
}

fn try_allocate_hip_buffers(
    api: &'static HipApi,
    stream: &Stream,
    lane_words: u64,
    lanes: usize,
    hashes_per_launch_per_lane: usize,
) -> std::result::Result<EngineBuffers, HipError> {
    let lanes_per_chunk = LANE_CHUNK_LANES.max(1);
    let mut lane_chunks = Vec::new();
    let mut remaining = lanes.max(1);
    while remaining > 0 {
        let chunk_lanes = remaining.min(lanes_per_chunk);
        let bytes = usize::try_from(
            lane_words
                .saturating_mul(chunk_lanes as u64)
                .saturating_mul(8),
        )
        .unwrap_or(usize::MAX);
        lane_chunks.push(DeviceBuffer::alloc(api, bytes)?);
        remaining -= chunk_lanes;
    }
    // Device-side table of chunk base pointers, read by every lane kernel.
    let table: Vec<u8> = lane_chunks
        .iter()
        .flat_map(|chunk| (chunk.device_ptr() as usize as u64).to_le_bytes())
        .collect();
    let lane_chunk_table = DeviceBuffer::alloc(api, table.len())?;
    let code = unsafe {
        (api.hip_memcpy_htod_async)(
            lane_chunk_table.device_ptr(),
            table.as_ptr().cast::<c_void>(),
            table.len(),
            stream.raw,
        )
    };
    if code != HIP_SUCCESS {
        return Err(code);
    }
    let code = unsafe { (api.hip_stream_synchronize)(stream.raw) };
    if code != HIP_SUCCESS {
        return Err(code);
    }
    let launch_capacity = lanes
        .saturating_mul(hashes_per_launch_per_lane.max(1))
        .max(1);

    Ok(EngineBuffers {
        lane_chunks,
        lane_chunk_table,
        lanes_per_chunk: lanes_per_chunk as u32,
        seed_blocks: DeviceBuffer::alloc(api, launch_capacity.saturating_mul(256 * 8))?,
        last_blocks: DeviceBuffer::alloc(api, launch_capacity.saturating_mul(128 * 8))?,
        nonce_input: DeviceBuffer::alloc(api, launch_capacity.saturating_mul(8))?,
        header_base_input: DeviceBuffer::alloc(api, blocknet_pow_spec::MAX_POW_HEADER_BASE_LEN)?,
        target_input: DeviceBuffer::alloc(api, POW_OUTPUT_LEN)?,
        cancel_flag: DeviceBuffer::alloc(api, 4)?,
        completed_iters: DeviceBuffer::alloc(api, 4)?,
        found_index: DeviceBuffer::alloc(api, 4)?,
    })
}

fn probe_hip_lane_memory(
    api: &'static HipApi,
    stream: &Stream,
    kernel: &Function,
    lane_chunk_table: &DeviceBuffer,
    lanes_per_chunk: u32,
    lane_stride_words: u64,
    lanes: usize,
) -> Result<()> {
    let lanes_u32 = u32::try_from(lanes).unwrap_or(u32::MAX);
    let mut lane_ptr = lane_chunk_table.device_ptr();
    let mut lanes_per_chunk_arg = lanes_per_chunk;
    let mut lanes_arg = lanes_u32;
    let mut stride_arg = lane_stride_words;
    let mut params: [*mut c_void; 4] = [
        (&mut lane_ptr as *mut HipDeviceptr).cast(),
        (&mut lanes_per_chunk_arg as *mut u32).cast(),
        (&mut lanes_arg as *mut u32).cast(),
        (&mut stride_arg as *mut u64).cast(),
    ];
    api.check(
        unsafe {
            (api.hip_module_launch_kernel)(
                kernel.raw,
                lanes_u32.max(1),
                1,
                1,
                1,
                1,
                1,
                0,
                stream.raw,
                params.as_mut_ptr(),
                std::ptr::null_mut(),
            )
        },
        "hipModuleLaunchKernel failed while probing lane memory",
    )?;
    stream.synchronize()
}

fn compile_hip_module_with_wavefront_fallback(
    api: &'static HipApi,
    options: &[String],
) -> Result<Vec<u8>> {
    match compile_hip_module(api, options) {
        Ok(code) => Ok(code),
        Err(primary_err) => {
            // Some hipRTC builds reject clang wavefront flags; retry without
            // them and rely on the device's reported wavefront size, which the
            // kernel's SEINE_WARPS_PER_BLOCK already matches.
            let reduced: Vec<String> = options
                .iter()
                .filter(|opt| {
                    opt.as_str() != "-mno-wavefrontsize64" && opt.as_str() != "-mwavefrontsize64"
                })
                .cloned()
                .collect();
            if reduced.len() == options.len() {
                return Err(primary_err);
            }
            compile_hip_module(api, &reduced).map_err(|fallback_err| {
                anyhow!(
                    "hipRTC compile failed with the wavefront flag ({primary_err:#}) \
                     and without it ({fallback_err:#})"
                )
            })
        }
    }
}

/// The Argon2 code object: compiled with hipRTC, or the built-in one for this
/// architecture when hipRTC is missing or fails (or `SEINE_AMD_BUILTIN_KERNELS=1`).
fn argon2_kernel_code(
    api: &'static HipApi,
    gcn_arch_name: &str,
    options: &[String],
) -> Result<Vec<u8>> {
    let key = super::code_objects::options_key(HIP_KERNEL_SRC, options);
    let builtin = super::code_objects::find("argon2", gcn_arch_name, key);
    let prefer_builtin = std::env::var_os("SEINE_AMD_BUILTIN_KERNELS").is_some_and(|v| v == "1");
    if let (Some(code), true) = (builtin, prefer_builtin || api.hiprtc.is_err()) {
        return Ok(code.to_vec());
    }
    match compile_hip_module_with_wavefront_fallback(api, options) {
        Ok(code) => Ok(code),
        Err(err) => match builtin {
            Some(code) => Ok(code.to_vec()),
            None => Err(err.context(format!(
                "failed to compile HIP module with hipRTC (no built-in kernel for {})",
                gcn_arch_name.trim()
            ))),
        },
    }
}

/// hipRTC compile without the wavefront-flag retry, for `--amd-export-code-objects`.
pub(crate) fn compile_hip_source_for_export(
    api: &'static HipApi,
    options: &[String],
) -> Result<Vec<u8>> {
    compile_hip_module(api, options)
}

fn compile_hip_module(api: &'static HipApi, options: &[String]) -> Result<Vec<u8>> {
    compile_hip_source(api, HIP_KERNEL_SRC, HIP_KERNEL_PROGRAM_NAME, options)
}

fn compile_hip_source(
    api: &'static HipApi,
    source: &str,
    program_name: &str,
    options: &[String],
) -> Result<Vec<u8>> {
    let rtc = api.hiprtc.as_ref().map_err(|err| anyhow!("{err}"))?;
    let source = CString::new(source)
        .map_err(|_| anyhow!("HIP kernel source contains interior NUL byte"))?;
    let name = CString::new(program_name)
        .map_err(|_| anyhow!("HIP program name contains interior NUL byte"))?;

    let mut program: super::hip_ffi::HiprtcProgram = std::ptr::null_mut();
    let create_code = unsafe {
        (rtc.create_program)(
            &mut program,
            source.as_ptr(),
            name.as_ptr(),
            0,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if create_code != HIPRTC_SUCCESS {
        bail!(
            "hiprtcCreateProgram failed: {}",
            rtc.error_string(create_code)
        );
    }

    let option_cstrings: Vec<CString> = options
        .iter()
        .map(|opt| CString::new(opt.as_str()))
        .collect::<std::result::Result<_, _>>()
        .map_err(|_| anyhow!("hipRTC option contains interior NUL byte"))?;
    let option_ptrs: Vec<*const c_char> =
        option_cstrings.iter().map(|cstr| cstr.as_ptr()).collect();

    let compile_code =
        unsafe { (rtc.compile_program)(program, option_ptrs.len() as c_int, option_ptrs.as_ptr()) };
    if compile_code != HIPRTC_SUCCESS {
        let log = read_hiprtc_log(rtc, program).unwrap_or_default();
        let _ = unsafe { (rtc.destroy_program)(&mut program) };
        if log.is_empty() {
            bail!(
                "hiprtcCompileProgram failed (options: {options:?}): {}",
                rtc.error_string(compile_code)
            );
        }
        bail!(
            "hiprtcCompileProgram failed (options: {options:?}): {}; log: {log}",
            rtc.error_string(compile_code)
        );
    }

    let mut code_size: usize = 0;
    let size_code = unsafe { (rtc.get_code_size)(program, &mut code_size) };
    if size_code != HIPRTC_SUCCESS || code_size == 0 {
        let _ = unsafe { (rtc.destroy_program)(&mut program) };
        bail!("hiprtcGetCodeSize failed: {}", rtc.error_string(size_code));
    }
    let mut code = vec![0u8; code_size];
    let get_code = unsafe { (rtc.get_code)(program, code.as_mut_ptr() as *mut c_char) };
    if get_code != HIPRTC_SUCCESS {
        let _ = unsafe { (rtc.destroy_program)(&mut program) };
        bail!("hiprtcGetCode failed: {}", rtc.error_string(get_code));
    }
    let destroy_code = unsafe { (rtc.destroy_program)(&mut program) };
    if destroy_code != HIPRTC_SUCCESS {
        bail!(
            "hiprtcDestroyProgram failed: {}",
            rtc.error_string(destroy_code)
        );
    }
    Ok(code)
}

fn read_hiprtc_log(
    rtc: &super::hip_ffi::Hiprtc,
    program: super::hip_ffi::HiprtcProgram,
) -> Option<String> {
    let mut log_size: usize = 0;
    if unsafe { (rtc.get_program_log_size)(program, &mut log_size) } != HIPRTC_SUCCESS
        || log_size <= 1
    {
        return None;
    }
    let mut log = vec![0u8; log_size];
    if unsafe { (rtc.get_program_log)(program, log.as_mut_ptr() as *mut c_char) } != HIPRTC_SUCCESS
    {
        return None;
    }
    let text = String::from_utf8_lossy(&log)
        .trim_end_matches('\0')
        .trim()
        .to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

/// Test/diagnostic accessor for the embedded kernel source (host-side source
/// hygiene checks; hipRTC compile-tests on the bring-up box).
#[allow(dead_code)]
pub(crate) fn hip_kernel_source() -> &'static str {
    HIP_KERNEL_SRC
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_budget_uses_free_vram_with_headroom() {
        // 24 GiB RX 7900 XTX-class card: 24576 MiB total, reserve = 24576/64 = 384 MiB.
        assert_eq!(derive_memory_budget_mib(24_576, Some(24_000)), 23_616);
        // Free unknown: fall back to total minus reserve.
        assert_eq!(derive_memory_budget_mib(24_576, None), 24_192);
        // Reserve floor engages on small cards.
        assert_eq!(derive_memory_budget_mib(2_048, Some(2_048)), 1_984);
    }

    #[test]
    fn lane_capacity_matches_two_gib_lanes_on_24_gib_card() {
        let lane_bytes = 2u64 * 1024 * 1024 * 1024;
        // 23,616 MiB budget / 2 GiB lanes => 11 lanes on a 24 GiB card.
        assert_eq!(
            lane_capacity_from_budget(derive_memory_budget_mib(24_576, Some(24_000)), lane_bytes),
            11
        );
        assert_eq!(lane_capacity_from_budget(1_024, lane_bytes), 1);
    }

    #[test]
    fn lane_launch_iters_ceil_divides_batches() {
        assert_eq!(plan_lane_launch_iters(0, 4), 1);
        assert_eq!(plan_lane_launch_iters(4, 4), 1);
        assert_eq!(plan_lane_launch_iters(5, 4), 2);
        assert_eq!(plan_lane_launch_iters(8, 4), 2);
        assert_eq!(plan_lane_launch_iters(3, 0), 3);
    }

    #[test]
    fn skip_boundary_defaults_to_rdna3_only() {
        if std::env::var_os("SEINE_AMD_SKIP_PERMILLE").is_some() {
            return;
        }
        let blocks = 2_097_152u32;
        let from = skip_from_block("gfx1101", blocks, 1);
        assert_eq!(from, 1_048_576);
        assert_eq!(from % 2, 0);
        assert_eq!(skip_from_block("gfx1201", blocks, 1), 0);
        assert_eq!(skip_from_block("gfx1030", blocks, 1), 0);
        assert_eq!(skip_from_block("gfx1100", blocks, 3), 0, "multi-pass stores every block");
        assert_eq!(lane_stride_blocks(blocks, 0), u64::from(blocks));
        assert_eq!(
            lane_stride_blocks(blocks, from),
            u64::from(from) + u64::from((blocks - from) / 2) + 4
        );
        assert!(build_hiprtc_options("gfx1100", 32, blocks, 1)
            .contains(&format!("-DSEINE_AMD_SKIP_FROM={from}U")));
        assert!(build_hiprtc_options("gfx1201", 32, blocks, 1)
            .contains(&"-DSEINE_AMD_SKIP_FROM=0U".to_string()));
    }

    #[test]
    fn hiprtc_options_pin_arch_wavefront_and_pow_parameters() {
        let options = build_hiprtc_options("gfx1100:sramecc-:xnack-", 32, 2_097_152, 1);
        assert!(options.contains(&"--offload-arch=gfx1100:sramecc-:xnack-".to_string()));
        assert!(options.contains(&"-mno-wavefrontsize64".to_string()));
        assert!(options.contains(&"-DSEINE_FIXED_M_BLOCKS=2097152U".to_string()));
        assert!(options.contains(&"-DSEINE_FIXED_T_COST=1U".to_string()));
        assert!(options.contains(&"-DSEINE_WARPS_PER_BLOCK=1U".to_string()));
        assert!(options.contains(&"-DSEINE_WAVE_SYNC=1".to_string()));

        let options = build_hiprtc_options("gfx942:sramecc+:xnack-", 64, 2_097_152, 1);
        assert!(options.contains(&"--offload-arch=gfx942:sramecc+:xnack-".to_string()));
        assert!(options.contains(&"-mwavefrontsize64".to_string()));
        assert!(!options.contains(&"-mno-wavefrontsize64".to_string()));
        assert!(options.contains(&"-DSEINE_WARPS_PER_BLOCK=2U".to_string()));
    }

    #[test]
    fn lanes_per_block_match_the_wavefront() {
        assert_eq!(warps_per_block(32), 1);
        assert_eq!(warps_per_block(64), 2);
        assert_eq!(warps_per_block(0), 1);
    }

    #[test]
    fn wavefront_guard_accepts_wave32_and_wave64_only() {
        assert!(validate_wavefront_size(32, 0, "RX 7900 XTX", "gfx1100").is_ok());
        assert!(validate_wavefront_size(64, 1, "MI300X", "gfx942").is_ok());
        let err = validate_wavefront_size(16, 2, "bogus", "gfx999")
            .expect_err("other wavefront sizes must be refused");
        let message = format!("{err:#}");
        assert!(message.contains("wave32"));
        assert!(message.contains("wave64"));
    }

    #[test]
    fn device_prop_validation_rejects_abi_garbage() {
        let mut props = HipDevicePropR0000::default();
        props.warp_size = 32;
        props.total_global_mem = 24 * 1024 * 1024 * 1024;
        for (idx, byte) in b"gfx1100".iter().enumerate() {
            props.gcn_arch_name[idx] = *byte as c_char;
        }
        assert!(validate_device_props(&props, 0).is_ok());

        props.warp_size = 12345;
        let err = validate_device_props(&props, 0).expect_err("garbage warp size must fail");
        assert!(format!("{err:#}").contains("ABI layout mismatch"));

        props.warp_size = 32;
        props.gcn_arch_name[0] = 0;
        let err = validate_device_props(&props, 0).expect_err("missing arch must fail");
        assert!(format!("{err:#}").contains("gcnArchName"));
    }

    #[test]
    fn kernel_source_omits_nvidia_only_scheduling_hacks() {
        let src = hip_kernel_source();
        assert!(
            !src.contains("__shfl_sync"),
            "HIP __shfl takes no mask; __shfl_sync must not survive the port"
        );
        assert!(
            !src.contains("prefetch.global"),
            "the CUDA kernel's dead-code prefetch block is not part of the HIP kernel"
        );
        assert!(
            !src.contains("tid < 8U"),
            "the tid<8 prefetch guard is NVIDIA-only and must be omitted"
        );
        assert!(!src.contains("asm volatile"));
    }

    #[test]
    fn kernel_source_exposes_all_required_entry_points() {
        let src = hip_kernel_source();
        for entry in [
            "argon2id_fill_kernel(",
            "argon2id_fill_kernel_fused_target(",
            "build_seed_blocks_kernel(",
            "evaluate_hashes_kernel(",
            "touch_lane_memory_kernel(",
        ] {
            assert!(src.contains(entry), "missing kernel entry point: {entry}");
        }
        assert!(src.contains("extern \"C\""));
        assert!(!src.contains('\0'), "kernel source must be hipRTC-safe");
    }

    #[test]
    #[ignore = "requires AMD GPU + ROCm runtime; run on the ROCm bring-up box"]
    fn hip_device_props_parse_sanely() {
        let devices = query_amd_devices().expect("HIP device query should succeed on a ROCm box");
        assert!(!devices.is_empty());
        for device in &devices {
            assert!(
                device.wavefront_size == 32 || device.wavefront_size == 64,
                "device {} wavefront size {} implausible; legacy hipDeviceProp ABI mismatch",
                device.index,
                device.wavefront_size
            );
            assert!(
                device.gcn_arch_name.starts_with("gfx"),
                "device {} gcnArchName '{}' implausible",
                device.index,
                device.gcn_arch_name
            );
            assert!(device.memory_total_mib > 0);
            eprintln!(
                "device {} {} {} integrated={}",
                device.index, device.name, device.gcn_arch_name, device.integrated
            );
        }
    }

    #[test]
    #[ignore = "requires AMD GPU + ROCm runtime; run on the ROCm bring-up box"]
    fn hiprtc_kernel_compiles_for_device_arch() {
        let devices = query_amd_devices().expect("HIP device query should succeed on a ROCm box");
        let device = devices.first().expect("at least one AMD device");
        let params = pow_params().expect("pow params should be available");
        let api = HipApi::load().expect("HIP runtime should load on a ROCm box");
        let options = build_hiprtc_options(
            &device.gcn_arch_name,
            device.wavefront_size,
            params.block_count() as u32,
            params.t_cost(),
        );
        let code = compile_hip_module_with_wavefront_fallback(api, &options)
            .expect("HIP kernel should compile with hipRTC");
        assert!(!code.is_empty());
    }
}
