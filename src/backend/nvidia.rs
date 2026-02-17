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
    AssignmentSemantics, BackendCallStatus, BackendCapabilities, BackendEvent,
    BackendExecutionModel, BackendInstanceId, BackendTelemetry, BenchBackend, DeadlineSupport,
    KernelBenchSample, MiningSolution, PowBackend, PreemptionGranularity, WorkAssignment,
    WorkTemplate,
};
const BACKEND_NAME: &str = "nvidia";
const CUDA_KERNEL_SRC: &str = include_str!("nvidia_kernel.cu");
const MAX_LANES_HARD_LIMIT: usize = 1024;
const ASSIGN_CHANNEL_CAPACITY: usize = 256;
const CONTROL_CHANNEL_CAPACITY: usize = 32;
const EVENT_SEND_WAIT: Duration = Duration::from_millis(5);
const KERNEL_THREADS: u32 = 32;
const SEED_KERNEL_THREADS: u32 = 64;
const EVAL_KERNEL_THREADS: u32 = 64;
const DEFAULT_NVIDIA_MAX_RREGCOUNT: u32 = 240;
const NVIDIA_AUTOTUNE_SCHEMA_VERSION: u32 = 8;
const NVIDIA_CUBIN_CACHE_SCHEMA_VERSION: u32 = 1;
const NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_AMPERE_PLUS: &[u32] =
    &[240, 224, 208, 192, 176, 160, 144, 128];
const NVIDIA_AUTOTUNE_REGCAP_CANDIDATES_LEGACY: &[u32] =
    &[224, 208, 192, 176, 160, 144, 128, 112, 96];
const NVIDIA_AUTOTUNE_LOOP_UNROLL_CANDIDATES: &[bool] = &[false];
const DEFAULT_NVIDIA_AUTOTUNE_SAMPLES: u32 = 2;
const NVIDIA_MEMORY_RESERVE_MIB_FLOOR: u64 = 64;
const NVIDIA_MEMORY_RESERVE_RATIO_DENOM: u64 = 64;
const NVIDIA_AUTOTUNE_MEMORY_BUCKET_MIB: u64 = 512;
const DEFAULT_DISPATCH_ITERS_PER_LANE: u64 = 1 << 21;
const DEFAULT_ALLOCATION_ITERS_PER_LANE: u64 = 1 << 21;
const DEFAULT_HASHES_PER_LAUNCH_PER_LANE: u32 = 2;
const ADAPTIVE_DEPTH_PRESSURE_CONTROL_BONUS: u32 = 2;
const ADAPTIVE_DEPTH_PRESSURE_REPLACE_BONUS: u32 = 1;
const ADAPTIVE_DEPTH_PRESSURE_DECAY_PER_BATCH: u32 = 1;
const ADAPTIVE_DEPTH_DEADLINE_SLACK_FRAC: f64 = 0.75;
const ADAPTIVE_DEPTH_DEADLINE_LAUNCH_HEADROOM_FRAC: f64 = 0.95;
const ADAPTIVE_DEPTH_HASH_EMA_ALPHA: f64 = 0.25;

fn default_hashes_per_launch_per_lane() -> u32 {
    DEFAULT_HASHES_PER_LAUNCH_PER_LANE
}

#[derive(Debug, Clone)]
struct NvidiaDeviceInfo {
    index: u32,
    name: String,
    memory_total_mib: u64,
    memory_free_mib: Option<u64>,
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
    timestamp_unix_secs: u64,
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
}

#[derive(Debug, Clone, Copy)]
pub struct NvidiaBackendTuningOptions {
    pub max_rregcount_override: Option<u32>,
    pub max_lanes_override: Option<usize>,
    pub autotune_samples: u32,
    pub dispatch_iters_per_lane: Option<u64>,
    pub allocation_iters_per_lane: Option<u64>,
    pub hashes_per_launch_per_lane: u32,
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
}

struct FillBatchResult {
    hashes_done: usize,
    solved_nonce: Option<u64>,
}

struct CudaArgon2Engine {
    stream: Arc<CudaStream>,
    interrupt_stream: Arc<CudaStream>,
    kernel_nonfused: CudaFunction,
    kernel_fused_target: CudaFunction,
    seed_kernel: CudaFunction,
    evaluate_kernel: CudaFunction,
    m_blocks: u32,
    m_cost_kib: u32,
    t_cost: u32,
    max_lanes: usize,
    hashes_per_launch_per_lane: usize,
    lane_memory: CudaSlice<u64>,
    seed_blocks: CudaSlice<u64>,
    last_blocks: CudaSlice<u64>,
    nonce_input: CudaSlice<u64>,
    header_base_input: CudaSlice<u8>,
    target_input: CudaSlice<u8>,
    cancel_flag: Arc<Mutex<CudaSlice<u32>>>,
    completed_iters: CudaSlice<u32>,
    found_index: CudaSlice<u32>,
    host_completed_iters: [u32; 1],
    host_found_index: [u32; 1],
    fused_target_check: bool,
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
        let ctx = CudaContext::new(device_index as usize).map_err(|err| {
            anyhow!("failed to open CUDA context on device {device_index}: {err:?}")
        })?;
        let stream = ctx.default_stream();
        let interrupt_stream = ctx.new_stream().unwrap_or_else(|_| stream.clone());

        let (cc_major, cc_minor) = ctx
            .compute_capability()
            .map_err(|err| anyhow!("failed to query compute capability: {err:?}"))?;

        let params = pow_params()
            .map_err(|err| anyhow!("invalid Argon2 parameters for CUDA backend: {err}"))?;
        let m_blocks = params.block_count() as u32;
        let m_cost_kib = params.m_cost();
        let t_cost = params.t_cost();

        let nvrtc_options = vec![
            "--std=c++14".to_string(),
            "--restrict".to_string(),
            "--use_fast_math".to_string(),
            "--ftz=true".to_string(),
            "--fmad=true".to_string(),
            format!("-DSEINE_FIXED_M_BLOCKS={}U", m_blocks),
            format!("-DSEINE_FIXED_T_COST={}U", t_cost),
            format!("--maxrregcount={}", tuning.max_rregcount.max(1)),
            format!("--gpu-architecture=sm_{}{}", cc_major, cc_minor),
        ];
        let program_name = "seine_argon2id_fill.cu";
        let cache_key = build_cubin_cache_key(CUDA_KERNEL_SRC, program_name, &nvrtc_options);
        let cache_path = cubin_cache_file_path(cubin_cache_dir, &cache_key);
        let mut cubin_loaded_from_cache = false;
        let cubin = if let Some(cached) = load_cached_cubin(&cache_path) {
            cubin_loaded_from_cache = true;
            cached
        } else {
            let compiled = compile_cubin_with_nvrtc(CUDA_KERNEL_SRC, program_name, &nvrtc_options)
                .map_err(|err| anyhow!("failed to compile CUDA CUBIN with NVRTC: {err:#}"))?;
            let _ = persist_cached_cubin(&cache_path, &compiled);
            compiled
        };
        let module = match ctx.load_module(Ptx::from_binary(cubin)) {
            Ok(module) => module,
            Err(err) if cubin_loaded_from_cache => {
                let _ = fs::remove_file(&cache_path);
                let compiled = compile_cubin_with_nvrtc(CUDA_KERNEL_SRC, program_name, &nvrtc_options)
                    .map_err(|compile_err| {
                        anyhow!(
                            "failed to compile CUDA CUBIN with NVRTC after cache load failure ({err:?}): {compile_err:#}"
                        )
                    })?;
                let _ = persist_cached_cubin(&cache_path, &compiled);
                ctx.load_module(Ptx::from_binary(compiled))
                    .map_err(|load_err| anyhow!("failed to load CUDA module: {load_err:?}"))?
            }
            Err(err) => return Err(anyhow!("failed to load CUDA module: {err:?}")),
        };
        let kernel_nonfused = module
            .load_function("argon2id_fill_kernel")
            .map_err(|err| anyhow!("failed to load CUDA kernel function: {err:?}"))?;
        let kernel_fused_target = module
            .load_function("argon2id_fill_kernel_fused_target")
            .map_err(|err| anyhow!("failed to load CUDA fused kernel function: {err:?}"))?;
        let seed_kernel = module
            .load_function("build_seed_blocks_kernel")
            .map_err(|err| anyhow!("failed to load CUDA seed kernel function: {err:?}"))?;
        let evaluate_kernel = module
            .load_function("evaluate_hashes_kernel")
            .map_err(|err| anyhow!("failed to load CUDA hash-eval kernel function: {err:?}"))?;
        let touch_kernel = module
            .load_function("touch_lane_memory_kernel")
            .map_err(|err| anyhow!("failed to load CUDA probe kernel function: {err:?}"))?;

        let lane_bytes = CPU_LANE_MEMORY_BYTES.max(u64::from(m_blocks) * 1024);
        let memory_budget_mib = derive_memory_budget_mib(memory_total_mib, memory_free_mib);
        let memory_budget_bytes = memory_budget_mib.saturating_mul(1024 * 1024);
        let usable_bytes = memory_budget_bytes;
        let by_memory = usize::try_from((usable_bytes / lane_bytes).max(1)).unwrap_or(1);
        let max_lanes_budget = by_memory.max(1).min(MAX_LANES_HARD_LIMIT);
        let max_lanes = max_lanes_override
            .map(|lanes| lanes.max(1))
            .unwrap_or(max_lanes_budget)
            .min(MAX_LANES_HARD_LIMIT)
            .max(1);
        let lane_stride_words = u64::from(m_blocks).saturating_mul(128);
        let hashes_per_launch_per_lane = hashes_per_launch_per_lane.max(1) as usize;

        let mut selected = None;
        for lanes in (1..=max_lanes).rev() {
            match try_allocate_cuda_buffers(&stream, m_blocks, lanes, hashes_per_launch_per_lane) {
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
                        Err(err)
                            if lanes > 1
                                && format!("{err:?}").contains("CUDA_ERROR_OUT_OF_MEMORY") =>
                        {
                            continue;
                        }
                        Err(err) => {
                            return Err(anyhow!(
                                "failed to validate CUDA buffers for {lanes} lanes on device {device_index}: {err:?}"
                            ));
                        }
                    }
                }
                Err(err)
                    if lanes > 1 && format!("{err:?}").contains("CUDA_ERROR_OUT_OF_MEMORY") =>
                {
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
            evaluate_kernel,
            m_blocks,
            m_cost_kib,
            t_cost,
            max_lanes,
            hashes_per_launch_per_lane,
            lane_memory,
            seed_blocks,
            last_blocks,
            nonce_input,
            header_base_input,
            target_input,
            cancel_flag: Arc::new(Mutex::new(cancel_flag)),
            completed_iters,
            found_index,
            host_completed_iters: [0u32; 1],
            host_found_index: [u32::MAX; 1],
            fused_target_check,
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
        if header_base.len() != POW_HEADER_BASE_LEN {
            bail!(
                "invalid header base length: expected {} bytes, got {}",
                POW_HEADER_BASE_LEN,
                header_base.len()
            );
        }

        if nonces.is_empty() {
            return Ok(FillBatchResult {
                hashes_done: 0,
                solved_nonce: None,
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
        {
            let mut header_view = self
                .header_base_input
                .try_slice_mut(0..POW_HEADER_BASE_LEN)
                .ok_or_else(|| anyhow!("failed to slice CUDA header buffer"))?;
            self.stream
                .memcpy_htod(header_base, &mut header_view)
                .map_err(cuda_driver_err)?;
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
        let use_fused_target_check = target.is_some() && self.fused_target_check;
        if let Some(target_hash) = target {
            {
                let mut target_view = self
                    .target_input
                    .try_slice_mut(0..POW_OUTPUT_LEN)
                    .ok_or_else(|| anyhow!("failed to slice CUDA target buffer"))?;
                self.stream
                    .memcpy_htod(target_hash, &mut target_view)
                    .map_err(cuda_driver_err)?;
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
                    .arg(&(POW_HEADER_BASE_LEN as u32))
                    .arg(&self.nonce_input)
                    .arg(&requested_hashes_u32)
                    .arg(&self.m_cost_kib)
                    .arg(&self.t_cost)
                    .arg(&self.seed_blocks);
                launch.launch(cfg).map_err(cuda_driver_err)?;
            }
        }

        {
            let cfg = LaunchConfig {
                // One cooperative block per lane. Threads in the block collaborate on Argon2
                // compression and memory transfer for that lane.
                grid_dim: (lanes_u32, 1, 1),
                block_dim: (KERNEL_THREADS, 1, 1),
                shared_mem_bytes: 0,
            };

            let cancel_flag = self
                .cancel_flag
                .lock()
                .map_err(|_| anyhow!("nvidia cancel flag lock poisoned"))?;
            unsafe {
                if use_fused_target_check {
                    let mut launch = self.stream.launch_builder(&self.kernel_fused_target);
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
                        .arg(&mut self.found_index);
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
                        .arg(&mut self.completed_iters);
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
                solved_nonce: None,
            });
        }

        let mut solved_nonce = None;
        if target.is_some() {
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
            } else {
                let evaluated_hashes_u32 = u32::try_from(hashes_done)
                    .map_err(|_| anyhow!("active hash count overflow"))?;
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
                {
                    let grid = evaluated_hashes_u32
                        .saturating_add(EVAL_KERNEL_THREADS.saturating_sub(1))
                        .saturating_div(EVAL_KERNEL_THREADS)
                        .max(1);
                    let cfg = LaunchConfig {
                        grid_dim: (grid, 1, 1),
                        block_dim: (EVAL_KERNEL_THREADS, 1, 1),
                        shared_mem_bytes: 0,
                    };
                    unsafe {
                        let mut launch = self.stream.launch_builder(&self.evaluate_kernel);
                        launch
                            .arg(&self.last_blocks)
                            .arg(&evaluated_hashes_u32)
                            .arg(&self.target_input)
                            .arg(&mut self.found_index);
                        launch.launch(cfg).map_err(cuda_driver_err)?;
                    }
                }
                {
                    let found_view = self
                        .found_index
                        .try_slice(0..1)
                        .ok_or_else(|| anyhow!("failed to slice CUDA found-index buffer"))?;
                    self.stream
                        .memcpy_dtoh(&found_view, &mut self.host_found_index)
                        .map_err(cuda_driver_err)?;
                }
            }
            if self.host_found_index[0] != u32::MAX && self.host_found_index[0] > 0 {
                let idx = (self.host_found_index[0] - 1) as usize;
                if idx < hashes_done {
                    solved_nonce = Some(nonces[idx]);
                }
            }
        }

        Ok(FillBatchResult {
            hashes_done,
            solved_nonce,
        })
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
    kernel_tuning: RwLock<Option<NvidiaKernelTuning>>,
    shared: Arc<NvidiaShared>,
    nonblocking_control: Mutex<NonblockingControlState>,
    interrupt_controller: RwLock<Option<CudaInterruptController>>,
    worker: Mutex<Option<NvidiaWorker>>,
    max_lanes: AtomicUsize,
    pinned_lanes: AtomicUsize,
    max_hashes_per_launch_per_lane: AtomicU32,
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
                fused_target_check: tuning_options.fused_target_check,
                adaptive_launch_depth: tuning_options.adaptive_launch_depth,
                enforce_template_stop: tuning_options.enforce_template_stop,
            },
            resolved_device: RwLock::new(None),
            kernel_tuning: RwLock::new(None),
            shared: Arc::new(NvidiaShared::new()),
            nonblocking_control: Mutex::new(NonblockingControlState::default()),
            interrupt_controller: RwLock::new(None),
            worker: Mutex::new(None),
            max_lanes: AtomicUsize::new(1),
            pinned_lanes: AtomicUsize::new(0),
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

    fn control_nonblocking(&self, kind: PendingControlKind) -> Result<BackendCallStatus> {
        let tx = self.worker_control_tx()?;
        let mut state = self
            .nonblocking_control
            .lock()
            .map_err(|_| anyhow!("nvidia nonblocking control lock poisoned"))?;

        if let Some(pending) = state.pending.as_mut() {
            match pending.ack_rx.try_recv() {
                Ok(result) => {
                    state.pending = None;
                    result?;
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

    fn resolve_kernel_tuning(&self, selected: &NvidiaDeviceInfo) -> NvidiaKernelTuning {
        if let Ok(slot) = self.kernel_tuning.read() {
            if let Some(tuning) = *slot {
                return tuning;
            }
        }

        if let Some(forced_rregcount) = self.tuning_options.max_rregcount_override {
            let forced = self.apply_runtime_tuning_overrides(NvidiaKernelTuning {
                max_rregcount: forced_rregcount.max(1),
                block_loop_unroll: false,
                hashes_per_launch_per_lane: self.tuning_options.hashes_per_launch_per_lane.max(1),
                max_lanes_hint: self.resolve_max_lanes_override(),
            });
            if let Ok(mut slot) = self.kernel_tuning.write() {
                *slot = Some(forced);
            }
            return forced;
        }

        let key = build_nvidia_autotune_key(selected);
        if let Some(cached) = load_nvidia_cached_tuning(&self.autotune_config_path, &key) {
            let cached = self.apply_runtime_tuning_overrides(cached);
            if let Ok(mut slot) = self.kernel_tuning.write() {
                *slot = Some(cached);
            }
            return cached;
        }

        let tuned = self.apply_runtime_tuning_overrides(
            autotune_nvidia_kernel_tuning(
                selected,
                &self.autotune_config_path,
                self.autotune_secs,
                self.tuning_options.autotune_samples,
                self.resolve_max_lanes_override(),
                self.tuning_options.hashes_per_launch_per_lane,
            )
            .unwrap_or_else(|_| NvidiaKernelTuning::default()),
        );
        if let Ok(mut slot) = self.kernel_tuning.write() {
            *slot = Some(tuned);
        }
        tuned
    }

    fn apply_runtime_tuning_overrides(&self, mut tuning: NvidiaKernelTuning) -> NvidiaKernelTuning {
        tuning.max_rregcount = tuning.max_rregcount.max(1);
        tuning.hashes_per_launch_per_lane = tuning
            .hashes_per_launch_per_lane
            .max(1)
            .min(self.tuning_options.hashes_per_launch_per_lane.max(1));
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

        let selected = self.validate_or_select_device()?;
        let tuning = self.resolve_kernel_tuning(&selected);
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
                "failed to initialize CUDA engine on NVIDIA device {} ({})",
                selected.index, selected.name
            )
        })?;

        let discovered_lanes = engine.max_lanes().max(1);
        let discovered_hashes_per_launch = engine.max_hashes_per_launch_per_lane() as u32;
        let interrupt_controller = engine.interrupt_controller();
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
            *slot = Some(interrupt_controller);
        }
        self.clear_nonblocking_control_state();

        let (assignment_tx, assignment_rx) = bounded::<WorkerCommand>(ASSIGN_CHANNEL_CAPACITY);
        let (control_tx, control_rx) = bounded::<WorkerCommand>(CONTROL_CHANNEL_CAPACITY);
        let shared = Arc::clone(&self.shared);
        let instance_id = Arc::clone(&self.instance_id);
        let adaptive_launch_depth = self.tuning_options.adaptive_launch_depth;
        let enforce_template_stop = self.tuning_options.enforce_template_stop;
        let handle = thread::Builder::new()
            .name(format!(
                "seine-nvidia-worker-{}",
                self.instance_id.load(Ordering::Acquire)
            ))
            .spawn(move || {
                worker_loop(
                    &mut engine,
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
        self.worker_assignment_tx()?
            .send(WorkerCommand::Assign(work))
            .map_err(|_| anyhow!("nvidia worker channel closed while assigning work"))
    }

    fn assign_work_batch(&self, work: &[WorkAssignment]) -> Result<()> {
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
                state.pending = None;
                result?;
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
        let tuning = self.resolve_kernel_tuning(&selected);
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
        let tuning = self.resolve_kernel_tuning(&selected);
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

            samples.push(KernelBenchSample {
                hashes: total,
                elapsed_secs: round_started.elapsed().as_secs_f64().max(0.001),
            });
        }

        Ok(samples)
    }
}

fn worker_loop(
    engine: &mut CudaArgon2Engine,
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
    let mut nonce_buf = vec![0u64; engine.max_hashes_per_launch()];
    let max_launch_depth = engine.max_hashes_per_launch_per_lane().max(1);

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

        let launch_started = Instant::now();
        let done = match engine.run_fill_batch(
            current.work.template.header_base.as_ref(),
            &nonce_buf[..hashes_per_batch],
            Some(&current.work.template.target),
        ) {
            Ok(done) => done,
            Err(err) => {
                emit_worker_error(
                    &shared,
                    &instance_id,
                    format!("CUDA batch execution failed: {err:#}"),
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

        if let Some(nonce) = done.solved_nonce {
            send_backend_event(
                &shared,
                BackendEvent::Solution(MiningSolution {
                    epoch: current.work.template.epoch,
                    nonce,
                    backend_id: instance_id.load(Ordering::Acquire),
                    backend: BACKEND_NAME,
                }),
            );
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
        Err(SendTimeoutError::Timeout(returned)) => {
            if outbound.send(returned).is_err() {
                shared.dropped_events.fetch_add(1, Ordering::Relaxed);
            }
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

fn build_cubin_cache_key(source: &str, program_name: &str, options: &[String]) -> String {
    let mut hasher = Blake2b512::new();
    hasher.update(NVIDIA_CUBIN_CACHE_SCHEMA_VERSION.to_le_bytes());
    hasher.update(program_name.as_bytes());
    hasher.update([0u8]);
    for option in options {
        hasher.update(option.as_bytes());
        hasher.update([0u8]);
    }
    hasher.update(source.as_bytes());
    hex::encode(hasher.finalize())
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
    fs::write(path, cubin)
        .with_context(|| format!("failed to write NVIDIA CUBIN cache '{}'", path.display()))?;
    Ok(())
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
    if first.header_base.as_ref() != second.header_base.as_ref() {
        bail!("mismatched header_base");
    }
    Ok(())
}

fn query_nvidia_devices() -> Result<Vec<NvidiaDeviceInfo>> {
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
    let header_base_input = unsafe { stream.alloc::<u8>(POW_HEADER_BASE_LEN) }?;
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

fn build_nvidia_autotune_key(selected: &NvidiaDeviceInfo) -> NvidiaAutotuneKey {
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
    }
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

fn load_nvidia_cached_tuning(path: &Path, key: &NvidiaAutotuneKey) -> Option<NvidiaKernelTuning> {
    let cache = load_nvidia_autotune_cache(path)?;
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
            best?
        }
    };
    Some(NvidiaKernelTuning {
        max_rregcount: record.max_rregcount,
        block_loop_unroll: record.block_loop_unroll,
        hashes_per_launch_per_lane: record.hashes_per_launch_per_lane.max(1),
        max_lanes_hint: record.max_lanes_hint,
    })
}

fn nvidia_autotune_key_compatible(lhs: &NvidiaAutotuneKey, rhs: &NvidiaAutotuneKey) -> bool {
    lhs.device_name == rhs.device_name
        && lhs.memory_total_mib == rhs.memory_total_mib
        && lhs.compute_cap_major == rhs.compute_cap_major
        && lhs.compute_cap_minor == rhs.compute_cap_minor
        && lhs.m_cost_kib == rhs.m_cost_kib
        && lhs.t_cost == rhs.t_cost
        && lhs.kernel_threads == rhs.kernel_threads
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
) -> Result<()> {
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
        timestamp_unix_secs,
    };
    if let Some(existing) = cache.records.iter_mut().find(|record| record.key == key) {
        *existing = updated;
    } else {
        cache.records.push(updated);
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create NVIDIA autotune cache directory '{}'",
                parent.display()
            )
        })?;
    }
    let payload = serde_json::to_string_pretty(&cache)?;
    fs::write(path, payload)
        .with_context(|| format!("failed to write NVIDIA autotune cache '{}'", path.display()))?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct NvidiaAutotuneSampleScore {
    throughput_hps: f64,
    counted_hps: f64,
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
    })
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

fn nvidia_autotune_regcap_candidates(compute_cap_major: u32) -> &'static [u32] {
    if compute_cap_major == 0 {
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
    let mut best: Option<(NvidiaKernelTuning, f64, f64, f64, f64)> = None;
    let cubin_cache_dir = derive_nvidia_cubin_cache_dir(cache_path);
    let sample_count = autotune_samples.max(1);
    let (compute_cap_major, _) = query_cuda_compute_capability(selected.index).unwrap_or((0, 0));
    let regcap_candidates = nvidia_autotune_regcap_candidates(compute_cap_major);
    let (m_cost_kib, _) = pow_params()
        .map(|params| (params.m_cost(), params.t_cost()))
        .unwrap_or((0, 0));
    let lane_candidates = build_autotune_lane_candidates(selected, max_lanes_override, m_cost_kib);
    let hash_depth_candidates =
        build_autotune_hash_depth_candidates(hashes_per_launch_per_lane.max(1));

    for &max_rregcount in regcap_candidates {
        for &block_loop_unroll in NVIDIA_AUTOTUNE_LOOP_UNROLL_CANDIDATES {
            for hashes_per_launch_per_lane in hash_depth_candidates.iter().copied() {
                for max_lanes_hint in lane_candidates.iter().copied() {
                    let candidate = NvidiaKernelTuning {
                        max_rregcount,
                        block_loop_unroll,
                        hashes_per_launch_per_lane,
                        max_lanes_hint,
                    };
                    let mut counted_samples = Vec::with_capacity(sample_count as usize);
                    let mut throughput_samples = Vec::with_capacity(sample_count as usize);
                    for _ in 0..sample_count {
                        let measured = match measure_nvidia_kernel_tuning_hps(
                            selected,
                            candidate,
                            autotune_secs,
                            &cubin_cache_dir,
                        ) {
                            Ok(score)
                                if score.counted_hps.is_finite()
                                    && score.throughput_hps.is_finite() =>
                            {
                                score
                            }
                            _ => continue,
                        };
                        counted_samples.push(measured.counted_hps.max(0.0));
                        throughput_samples.push(measured.throughput_hps.max(0.0));
                    }
                    if counted_samples.is_empty() {
                        continue;
                    }
                    counted_samples
                        .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    throughput_samples
                        .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let counted_median = median_from_sorted(&counted_samples);
                    let counted_mean =
                        counted_samples.iter().sum::<f64>() / counted_samples.len() as f64;
                    let throughput_median = median_from_sorted(&throughput_samples);
                    let throughput_mean =
                        throughput_samples.iter().sum::<f64>() / throughput_samples.len() as f64;
                    const EPS: f64 = 1e-9;
                    let should_replace = match best {
                        None => true,
                        Some((
                            _,
                            best_counted_median,
                            best_counted_mean,
                            best_throughput_median,
                            best_throughput_mean,
                        )) => {
                            if counted_median > best_counted_median + EPS {
                                true
                            } else if (counted_median - best_counted_median).abs() <= EPS {
                                if counted_mean > best_counted_mean + EPS {
                                    true
                                } else if (counted_mean - best_counted_mean).abs() <= EPS {
                                    if throughput_median > best_throughput_median + EPS {
                                        true
                                    } else if (throughput_median - best_throughput_median).abs()
                                        <= EPS
                                    {
                                        throughput_mean > best_throughput_mean + EPS
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
                    };
                    if should_replace {
                        best = Some((
                            candidate,
                            counted_median,
                            counted_mean,
                            throughput_median,
                            throughput_mean,
                        ));
                    }
                }
            }
        }
    }

    let (selected_tuning, measured_hps, _, _, _) = best.ok_or_else(|| {
        anyhow!(
            "NVIDIA autotune failed for device {} (index {})",
            selected.name,
            selected.index
        )
    })?;

    let key = build_nvidia_autotune_key(selected);
    let _ = persist_nvidia_autotune_record(
        cache_path,
        key,
        selected_tuning,
        measured_hps,
        autotune_secs,
        sample_count,
    );
    Ok(selected_tuning)
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn test_template(work_id: u64) -> Arc<WorkTemplate> {
        Arc::new(WorkTemplate {
            work_id,
            epoch: 7,
            header_base: Arc::<[u8]>::from(vec![1u8; 92]),
            target: [0xff; 32],
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
        )
        .expect("first record should persist");
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
        )
        .expect("second record should persist");

        let loaded =
            load_nvidia_cached_tuning(&path, &key).expect("cached tuning should be available");
        assert_eq!(loaded.max_rregcount, 224);
        assert!(loaded.block_loop_unroll);
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
        )
        .expect("farther record should persist");

        let loaded =
            load_nvidia_cached_tuning(&path, &base_key).expect("fallback tuning should be found");
        assert_eq!(loaded.max_rregcount, 208);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn cubin_cache_key_changes_with_compile_options() {
        let source = "__global__ void k() {}";
        let key_a =
            build_cubin_cache_key(source, "k.cu", &["--gpu-architecture=sm_86".to_string()]);
        let key_b =
            build_cubin_cache_key(source, "k.cu", &["--gpu-architecture=sm_89".to_string()]);
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
}
