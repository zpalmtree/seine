//! Faraday BLAKE2b-512 PoW on NVIDIA GPUs. The host builds the template
//! midstate with `blocknet_pow_kernel`; the device hashes the final block per
//! nonce and reports nonces whose first output limb passes the target. The
//! host re-checks every candidate with the CPU kernel, so only exact hits leave
//! `scan`. The NVIDIA worker drives it through `Blake2bWorkerState` for
//! templates whose height selects BLAKE2b.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use blocknet_pow_kernel::{Blake2bPowMidstate, Blake2bPowSingleBlock};
use cudarc::driver::{
    sys::CUdevice_attribute, CudaContext, CudaFunction, CudaSlice, CudaStream, DeviceRepr,
    LaunchConfig, PushKernelArg,
};
use cudarc::nvrtc::Ptx;

use super::{
    build_cubin_cache_key, compile_cuda_module_with_nvrtc_fallback, cubin_cache_file_path,
    cuda_driver_err, load_cached_cubin, nvrtc_available, nvrtc_compiler_identity,
    persist_cached_cubin, require_cuda_driver, NvrtcModuleArtifact,
};

const KERNEL_SRC: &str = include_str!("blake2b_kernel.cu");
/// `KERNEL_SRC` built for `FARADAY_SHAPE` as PTX, loaded through the driver
/// when NVRTC is not installed. Regenerate with `scripts/gen_blake2b_ptx.py`
/// whenever the kernel changes.
const FARADAY_PTX: &str = include_str!("blake2b_faraday.ptx");
const KERNEL_NAME: &str = "blake2b_pow_scan_kernel";
const PROGRAM_NAME: &str = "seine_blake2b_pow.cu";
const THREADS_PER_BLOCK: u32 = 256;
const BLOCKS_PER_SM: u32 = 16;
const CANDIDATE_CAPACITY: usize = 4096;
/// Wall time one launch aims for: short enough for prompt cancel/job switches
/// (the kernel has no cancel flag), long enough to amortize launch overhead.
const LAUNCH_TARGET: Duration = Duration::from_millis(25);
const INITIAL_LAUNCH_WAVES: u64 = 16;
const MAX_LAUNCH_WAVES: u64 = 4096;

/// Mirrors `Blake2bPowParams` in the kernel; passed by value.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct KernelParams {
    v: [u64; 16],
    m: [u64; 16],
    h0: u64,
    target_limb0: u64,
}

unsafe impl DeviceRepr for KernelParams {}

/// Compile-time layout; one module per distinct header layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KernelShape {
    nonce_word: usize,
    nonce_shift: u32,
    message_words: usize,
    pre_columns: u8,
}

/// Layout of the Faraday header base (156 bytes) that `FARADAY_PTX` is built for.
const FARADAY_SHAPE: KernelShape = KernelShape {
    nonce_word: 3,
    nonce_shift: 32,
    message_words: 5,
    pre_columns: 0b1001,
};

impl KernelShape {
    fn of(layout: &Blake2bPowSingleBlock) -> Self {
        Self {
            nonce_word: layout.nonce_word,
            nonce_shift: layout.nonce_shift,
            message_words: layout.message_words,
            pre_columns: if cfg!(test) && std::env::var_os("SEINE_B2_NO_PRECOMPUTE").is_some() {
                // Benchmark A/B only: compute every round-0 column per nonce.
                0
            } else {
                layout.nonce_free_round0_columns()
            },
        }
    }
}

pub(super) struct CudaBlake2bEngine {
    ctx: Arc<CudaContext>,
    stream: Arc<CudaStream>,
    arch: (i32, i32),
    cubin_cache_dir: PathBuf,
    kernels: Vec<(KernelShape, CudaFunction)>,
    candidates: CudaSlice<u64>,
    candidate_count: CudaSlice<u32>,
    grid_blocks: u32,
    threads_per_block: u32,
    /// Read by the benchmark launch-shape override.
    #[cfg_attr(not(test), allow(dead_code))]
    sm_count: u32,
    /// Tests: load `FARADAY_PTX` even when NVRTC is available.
    force_prebuilt: bool,
}

impl CudaBlake2bEngine {
    pub(super) fn new(device_index: u32, cubin_cache_dir: &Path) -> Result<Self> {
        require_cuda_driver()?;
        let ctx = CudaContext::new(device_index as usize).map_err(|err| {
            anyhow!("failed to open CUDA context on device {device_index}: {err:?}")
        })?;
        let stream = ctx.default_stream();
        let arch = ctx
            .compute_capability()
            .map_err(|err| anyhow!("failed to query compute capability: {err:?}"))?;
        let sm_count = ctx
            .attribute(CUdevice_attribute::CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT)
            .map_err(cuda_driver_err)?;
        let candidates = stream
            .alloc_zeros::<u64>(CANDIDATE_CAPACITY)
            .map_err(cuda_driver_err)?;
        let candidate_count = stream.alloc_zeros::<u32>(1).map_err(cuda_driver_err)?;
        Ok(Self {
            ctx,
            stream,
            arch,
            cubin_cache_dir: cubin_cache_dir.to_path_buf(),
            kernels: Vec::new(),
            candidates,
            candidate_count,
            grid_blocks: u32::try_from(sm_count.max(1)).unwrap_or(1) * BLOCKS_PER_SM,
            threads_per_block: THREADS_PER_BLOCK,
            sm_count: u32::try_from(sm_count.max(1)).unwrap_or(1),
            force_prebuilt: false,
        })
    }

    /// Nonces per full launch wave; `scan` counts that are multiples of this
    /// keep every thread equally busy.
    pub(super) fn wave_nonces(&self) -> u64 {
        u64::from(self.grid_blocks) * u64::from(self.threads_per_block)
    }

    /// Benchmark-only launch shape override.
    #[cfg(test)]
    fn set_launch_shape(&mut self, threads_per_block: u32, blocks_per_sm: u32) {
        self.threads_per_block = threads_per_block.max(32);
        self.grid_blocks = self.sm_count * blocks_per_sm.max(1);
    }

    /// Hashes `count` nonces from `start_nonce` (wrapping) and returns every
    /// nonce whose hash meets `target`, in scan order.
    pub(super) fn scan(
        &mut self,
        midstate: &Blake2bPowMidstate,
        start_nonce: u64,
        count: u64,
        target: &[u8; 32],
    ) -> Result<Vec<u64>> {
        let layout = midstate
            .single_block_layout()
            .ok_or_else(|| anyhow!("BLAKE2b header layout needs two blocks per nonce"))?;
        if count == 0 {
            return Ok(Vec::new());
        }
        let shape = KernelShape::of(&layout);
        let kernel = self.kernel(shape)?;
        let params = KernelParams {
            v: layout.round0_state(shape.pre_columns),
            m: layout.message,
            h0: layout.h[0],
            target_limb0: u64::from_be_bytes(target[..8].try_into().expect("8 bytes")),
        };

        // A launch that finds more candidates than the buffer holds is split
        // and rerun on the GPU. Halves of at most CANDIDATE_CAPACITY nonces
        // cannot overflow, so very easy targets stay exact without a CPU rescan.
        let mut hits = Vec::new();
        let mut ranges = vec![(start_nonce, count)];
        while let Some((range_start, range_count)) = ranges.pop() {
            match self.launch_range(&kernel, &params, range_start, range_count)? {
                Some(candidates) => hits.extend(
                    candidates
                        .into_iter()
                        .filter(|&nonce| midstate.target_bytes(nonce) <= *target),
                ),
                None => {
                    let first = range_count / 2;
                    ranges.push((range_start.wrapping_add(first), range_count - first));
                    ranges.push((range_start, first));
                }
            }
        }
        hits.sort_unstable_by_key(|&nonce| nonce.wrapping_sub(start_nonce));
        Ok(hits)
    }

    /// One kernel launch; `None` when its candidates overflowed the buffer.
    fn launch_range(
        &mut self,
        kernel: &CudaFunction,
        params: &KernelParams,
        start_nonce: u64,
        count: u64,
    ) -> Result<Option<Vec<u64>>> {
        self.stream
            .memset_zeros(&mut self.candidate_count)
            .map_err(cuda_driver_err)?;
        let blocks = u64::from(self.grid_blocks)
            .min(count.div_ceil(u64::from(self.threads_per_block)))
            .max(1) as u32;
        let cfg = LaunchConfig {
            grid_dim: (blocks, 1, 1),
            block_dim: (self.threads_per_block, 1, 1),
            shared_mem_bytes: 0,
        };
        let capacity = CANDIDATE_CAPACITY as u32;
        unsafe {
            let mut launch = self.stream.launch_builder(kernel);
            launch
                .arg(params)
                .arg(&start_nonce)
                .arg(&count)
                .arg(&mut self.candidates)
                .arg(&mut self.candidate_count)
                .arg(&capacity);
            launch.launch(cfg).map_err(cuda_driver_err)?;
        }
        let found = self
            .stream
            .clone_dtoh(&self.candidate_count)
            .map_err(cuda_driver_err)?[0] as usize;
        if found > CANDIDATE_CAPACITY {
            return Ok(None);
        }
        if found == 0 {
            return Ok(Some(Vec::new()));
        }
        let mut candidates = self
            .stream
            .clone_dtoh(&self.candidates)
            .map_err(cuda_driver_err)?;
        candidates.truncate(found);
        Ok(Some(candidates))
    }

    fn kernel(&mut self, shape: KernelShape) -> Result<CudaFunction> {
        if let Some((_, kernel)) = self.kernels.iter().find(|(known, _)| *known == shape) {
            return Ok(kernel.clone());
        }
        if !nvrtc_available() || self.force_prebuilt {
            if shape != FARADAY_SHAPE {
                return Err(anyhow!(
                    "CUDA NVRTC library is unavailable; only Faraday block headers have a prebuilt BLAKE2b kernel"
                ));
            }
            return self.prebuilt_kernel(shape);
        }
        match self.nvrtc_kernel(shape) {
            Ok(kernel) => Ok(kernel),
            // NVRTC is present but cannot build for this GPU (CUDA 13 NVRTC
            // dropped compute capability 5.x-7.0): use the prebuilt PTX, which
            // the driver compiles for any supported GPU.
            Err(err) if shape == FARADAY_SHAPE => {
                self.prebuilt_kernel(shape).map_err(|prebuilt_err| {
                    anyhow!("{err:#}; the prebuilt BLAKE2b kernel also failed: {prebuilt_err:#}")
                })
            }
            Err(err) => Err(err),
        }
    }

    /// Loads the prebuilt Faraday PTX through the driver.
    fn prebuilt_kernel(&mut self, shape: KernelShape) -> Result<CudaFunction> {
        let module = self
            .ctx
            .load_module(Ptx::from_src(FARADAY_PTX))
            .map_err(|err| anyhow!("failed to load the prebuilt BLAKE2b kernel: {err:?}"))?;
        let kernel = module
            .load_function(KERNEL_NAME)
            .map_err(|err| anyhow!("failed to load {KERNEL_NAME}: {err:?}"))?;
        self.kernels.push((shape, kernel.clone()));
        Ok(kernel)
    }

    /// Builds the kernel for `shape` with NVRTC (cached cubin, else cubin or PTX).
    fn nvrtc_kernel(&mut self, shape: KernelShape) -> Result<CudaFunction> {
        let common = vec![
            "--std=c++14".to_string(),
            "--restrict".to_string(),
            format!("-DSEINE_B2_NONCE_WORD={}", shape.nonce_word),
            format!("-DSEINE_B2_NONCE_SHIFT={}", shape.nonce_shift),
            format!("-DSEINE_B2_MSG_WORDS={}", shape.message_words),
            format!("-DSEINE_B2_PRE_COLUMNS={}", shape.pre_columns),
        ];
        let (major, minor) = self.arch;
        let mut cubin_options = common.clone();
        cubin_options.push(format!("--gpu-architecture=sm_{major}{minor}"));
        let mut ptx_options = common;
        ptx_options.push(format!("--gpu-architecture=compute_{major}{minor}"));

        let cache_key = build_cubin_cache_key(
            KERNEL_SRC,
            PROGRAM_NAME,
            &cubin_options,
            &nvrtc_compiler_identity(),
        );
        let cache_path = cubin_cache_file_path(&self.cubin_cache_dir, &cache_key);
        let cached = load_cached_cubin(&cache_path)
            .and_then(|cubin| self.ctx.load_module(Ptx::from_binary(cubin)).ok());
        let module = match cached {
            Some(module) => module,
            None => {
                let artifact = compile_cuda_module_with_nvrtc_fallback(
                    KERNEL_SRC,
                    PROGRAM_NAME,
                    &cubin_options,
                    &ptx_options,
                )?;
                match artifact {
                    NvrtcModuleArtifact::Cubin(cubin) => {
                        let _ = persist_cached_cubin(&cache_path, &cubin);
                        self.ctx.load_module(Ptx::from_binary(cubin))
                    }
                    NvrtcModuleArtifact::Ptx(ptx) => self.ctx.load_module(Ptx::from_src(ptx)),
                }
                .map_err(|err| anyhow!("failed to load BLAKE2b CUDA module: {err:?}"))?
            }
        };
        let kernel = module
            .load_function(KERNEL_NAME)
            .map_err(|err| anyhow!("failed to load {KERNEL_NAME}: {err:?}"))?;
        self.kernels.push((shape, kernel.clone()));
        Ok(kernel)
    }
}

/// One worker's BLAKE2b engine (created on first use), template midstate and
/// adaptive launch size.
pub(super) struct Blake2bWorkerState {
    device_index: u32,
    cubin_cache_dir: PathBuf,
    engine: Option<CudaBlake2bEngine>,
    midstate: Option<(Arc<[u8]>, Blake2bPowMidstate)>,
    launch_nonces: u64,
}

pub(super) struct Blake2bBatch {
    pub(super) hashes: u64,
    /// Exact hits in scan order with their target bytes.
    pub(super) solutions: Vec<(u64, [u8; 32])>,
}

impl Blake2bWorkerState {
    pub(super) fn new(device_index: u32, cubin_cache_dir: PathBuf) -> Self {
        Self {
            device_index,
            cubin_cache_dir,
            engine: None,
            midstate: None,
            launch_nonces: 0,
        }
    }

    /// Runs one launch over at most `remaining` nonces from `start_nonce`,
    /// trimmed so it should finish before `stop_at`.
    pub(super) fn run_batch(
        &mut self,
        header_base: &Arc<[u8]>,
        start_nonce: u64,
        remaining: u64,
        target: &[u8; 32],
        stop_at: Option<Instant>,
    ) -> Result<Blake2bBatch> {
        if self.engine.is_none() {
            self.engine = Some(CudaBlake2bEngine::new(
                self.device_index,
                &self.cubin_cache_dir,
            )?);
        }
        let engine = self.engine.as_mut().expect("engine created above");
        let wave = engine.wave_nonces().max(1);
        if self.launch_nonces == 0 {
            self.launch_nonces = wave * INITIAL_LAUNCH_WAVES;
        }
        let stale =
            !matches!(&self.midstate, Some((header, _)) if Arc::ptr_eq(header, header_base));
        if stale {
            let midstate = Blake2bPowMidstate::new(
                crate::backend::MINING_POW_NETWORK.blake2b_personal(),
                header_base,
            )
            .map_err(|err| anyhow!("BLAKE2b midstate failed: {err}"))?;
            self.midstate = Some((Arc::clone(header_base), midstate));
        }
        let midstate = &self.midstate.as_ref().expect("midstate cached above").1;

        let mut count = self.launch_nonces.min(remaining).max(1);
        if let Some(stop_at) = stop_at {
            let left = stop_at.saturating_duration_since(Instant::now());
            let budget = (count as f64 * left.as_secs_f64() / LAUNCH_TARGET.as_secs_f64()) as u64;
            count = count.min(budget.max(wave.min(count)));
        }

        let started = Instant::now();
        let hits = engine.scan(midstate, start_nonce, count, target)?;
        let elapsed = started.elapsed();
        if count == self.launch_nonces && elapsed > Duration::ZERO {
            let scaled = count as f64 * LAUNCH_TARGET.as_secs_f64() / elapsed.as_secs_f64();
            let waves = (scaled / wave as f64).round() as u64;
            self.launch_nonces = waves.clamp(1, MAX_LAUNCH_WAVES) * wave;
        }
        Ok(Blake2bBatch {
            hashes: count,
            solutions: hits
                .into_iter()
                .map(|nonce| (nonce, midstate.target_bytes(nonce)))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blocknet_pow_spec::{BLAKE2B_POW_PERSONAL_MAINNET, FARADAY_HEADER_BASE_LEN};
    use std::time::Instant;

    fn engine(name: &str) -> CudaBlake2bEngine {
        let dir = std::env::temp_dir().join(format!("seine-blake2b-{name}-{}", std::process::id()));
        CudaBlake2bEngine::new(0, &dir).expect("CUDA BLAKE2b engine on device 0")
    }

    #[test]
    fn prebuilt_ptx_matches_the_kernel_source_and_faraday_layout() {
        use sha2::{Digest, Sha256};
        let digest = hex::encode(Sha256::digest(KERNEL_SRC.as_bytes()));
        assert!(
            FARADAY_PTX.contains(&format!("// kernel-sha256: {digest}\n")),
            "blake2b_faraday.ptx is stale: run scripts/gen_blake2b_ptx.py"
        );
        let layout = midstate(FARADAY_HEADER_BASE_LEN)
            .single_block_layout()
            .expect("the Faraday header fits one final block");
        assert_eq!(KernelShape::of(&layout), FARADAY_SHAPE);
        assert!(FARADAY_PTX.contains(&format!(
            "-DSEINE_B2_NONCE_WORD={} -DSEINE_B2_NONCE_SHIFT={} -DSEINE_B2_MSG_WORDS={} -DSEINE_B2_PRE_COLUMNS={}",
            FARADAY_SHAPE.nonce_word,
            FARADAY_SHAPE.nonce_shift,
            FARADAY_SHAPE.message_words,
            FARADAY_SHAPE.pre_columns
        )));
    }

    fn midstate(len: usize) -> Blake2bPowMidstate {
        let header: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
        Blake2bPowMidstate::new(&BLAKE2B_POW_PERSONAL_MAINNET, &header).unwrap()
    }

    fn target_with_limb0(limb0: u64) -> [u8; 32] {
        let mut target = [0xffu8; 32];
        target[..8].copy_from_slice(&limb0.to_be_bytes());
        target
    }

    #[test]
    #[ignore = "requires NVIDIA GPU + CUDA runtime; run manually under gpuq"]
    fn gpu_blake2b_hits_match_cpu_kernel() {
        let mut engine = engine("parity");
        // The Faraday header, the legacy header and an aligned nonce word.
        for len in [FARADAY_HEADER_BASE_LEN, 92, 152] {
            let midstate = midstate(len);
            let count = 1u64 << 22;
            // About 64 hits per range, wrapping past u64::MAX in the second.
            for start in [0u64, u64::MAX - (count / 2)] {
                for target in [
                    target_with_limb0(u64::MAX >> 16),
                    // Same limb 0; the host must reject on the later limbs.
                    {
                        let mut t = target_with_limb0(u64::MAX >> 16);
                        t[8..].fill(0);
                        t
                    },
                    [0u8; 32],
                    // Every nonce hits: overflowing launches split on the GPU.
                    [0xffu8; 32],
                ] {
                    let mut expected = Vec::new();
                    midstate.scan(start, count, &target, |nonce| expected.push(nonce));
                    let gpu = engine.scan(&midstate, start, count, &target).unwrap();
                    assert_eq!(gpu, expected, "len={len} start={start:#x}");
                }
            }
        }
    }

    #[test]
    #[ignore = "requires NVIDIA GPU + CUDA runtime; run manually under gpuq"]
    fn gpu_blake2b_prebuilt_ptx_matches_cpu_kernel() {
        let mut engine = engine("prebuilt");
        engine.force_prebuilt = true;
        let midstate = midstate(FARADAY_HEADER_BASE_LEN);
        let count = 1u64 << 22;
        for start in [0u64, u64::MAX - (count / 2)] {
            for target in [target_with_limb0(u64::MAX >> 16), [0u8; 32]] {
                let gpu = engine.scan(&midstate, start, count, &target).unwrap();
                let mut cpu = Vec::new();
                midstate.scan(start, count, &target, |nonce| cpu.push(nonce));
                assert_eq!(gpu, cpu, "start={start}");
            }
        }
    }

    #[test]
    #[ignore = "requires NVIDIA GPU + CUDA runtime; run manually under gpuq"]
    fn gpu_blake2b_rate() {
        // SEINE_B2_SHAPES="256x16,128x32" sweeps threads-per-block x blocks-per-SM;
        // SEINE_B2_RUNS repeats each shape (default 1).
        let mut engine = engine("rate");
        // SEINE_B2_PREBUILT=1 times the prebuilt PTX instead of NVRTC's build.
        engine.force_prebuilt = std::env::var_os("SEINE_B2_PREBUILT").is_some();
        let midstate = midstate(FARADAY_HEADER_BASE_LEN);
        let target = target_with_limb0(0);
        let shapes: Vec<(u32, u32)> = std::env::var("SEINE_B2_SHAPES")
            .unwrap_or_else(|_| format!("{THREADS_PER_BLOCK}x{BLOCKS_PER_SM}"))
            .split(',')
            .map(|shape| {
                let (tpb, bpsm) = shape.split_once('x').expect("TPBxBPSM");
                (tpb.parse().expect("tpb"), bpsm.parse().expect("bpsm"))
            })
            .collect();
        let runs: usize = std::env::var("SEINE_B2_RUNS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1);
        for _ in 0..runs {
            for &(tpb, bpsm) in &shapes {
                engine.set_launch_shape(tpb, bpsm);
                let launch = (engine.wave_nonces() * 256).min(1 << 28);
                engine.scan(&midstate, 0, launch, &target).unwrap();
                let started = Instant::now();
                let (mut nonce, mut hashes) = (0u64, 0u64);
                while started.elapsed().as_secs_f64() < 5.0 {
                    engine.scan(&midstate, nonce, launch, &target).unwrap();
                    nonce = nonce.wrapping_add(launch);
                    hashes += launch;
                }
                let secs = started.elapsed().as_secs_f64();
                println!(
                    "blake2b-rate | shape={tpb}x{bpsm} launch={launch} hashes={hashes} secs={secs:.3} ghs={:.3}",
                    hashes as f64 / secs / 1e9
                );
            }
        }
    }

    #[test]
    #[ignore = "requires NVIDIA GPU + CUDA runtime; run manually under gpuq"]
    fn nvidia_backend_mines_blake2b_assignments() {
        use crate::backend::nvidia::{NvidiaBackend, NvidiaBackendTuningOptions};
        use crate::backend::{
            BackendEvent, NonceChunk, PowAlgorithm, PowBackend, WorkAssignment, WorkTemplate,
        };
        use std::collections::BTreeMap;
        use std::time::Duration;

        let dir =
            std::env::temp_dir().join(format!("seine-blake2b-backend-{}", std::process::id()));
        let backend = NvidiaBackend::new(
            Some(0),
            dir.join("seine.nvidia-autotune.json"),
            1,
            NvidiaBackendTuningOptions {
                // A fixed register cap skips Argon2 autotune; one lane keeps VRAM small.
                max_rregcount_override: Some(super::super::DEFAULT_NVIDIA_MAX_RREGCOUNT),
                max_lanes_override: Some(1),
                ..NvidiaBackendTuningOptions::default()
            },
        );
        let (tx, rx) = crossbeam_channel::unbounded();
        backend.set_instance_id(1);
        backend.set_event_sink(tx);
        backend.start().expect("NVIDIA backend starts");

        let midstate = midstate(FARADAY_HEADER_BASE_LEN);
        let header: Vec<u8> = (0..FARADAY_HEADER_BASE_LEN)
            .map(|i| (i * 37 + 11) as u8)
            .collect();
        let target = target_with_limb0(u64::MAX >> 20);
        let count = 1u64 << 26;
        let start = u64::MAX - (count / 3);
        let mut expected = BTreeMap::new();
        midstate.scan(start, count, &target, |nonce| {
            expected.insert(nonce, midstate.target_bytes(nonce));
        });
        assert!(!expected.is_empty());

        backend
            .assign_work(WorkAssignment {
                template: Arc::new(WorkTemplate {
                    work_id: 1,
                    epoch: 1,
                    header_base: Arc::from(header),
                    pow: PowAlgorithm::Blake2b512,
                    target,
                    dynamic_share_target: None,
                    pause_on_solution: false,
                    stop_at: Instant::now() + Duration::from_secs(60),
                }),
                nonce_chunk: NonceChunk {
                    start_nonce: start,
                    nonce_count: count,
                },
            })
            .expect("BLAKE2b assignment accepted");

        let until = Instant::now() + Duration::from_secs(30);
        let mut hashes = 0u64;
        let mut found = BTreeMap::new();
        while (hashes < count || found.len() < expected.len()) && Instant::now() < until {
            hashes += backend.take_hashes();
            if let Ok(BackendEvent::Solution(solution)) = rx.recv_timeout(Duration::from_millis(20))
            {
                assert_eq!(solution.epoch, 1);
                let hash = solution.hash.expect("BLAKE2b solutions carry their hash");
                assert!(found.insert(solution.nonce, hash).is_none(), "duplicate");
            }
        }
        backend.stop();
        assert_eq!(hashes, count);
        assert_eq!(found, expected);
    }

    #[test]
    #[ignore = "requires NVIDIA GPU + CUDA runtime; run manually under gpuq"]
    fn nvidia_backend_frees_argon2_memory_for_blake2b_and_rebuilds_it() {
        use crate::backend::nvidia::{NvidiaBackend, NvidiaBackendTuningOptions};
        use crate::backend::{
            BackendEvent, NonceChunk, PowAlgorithm, PowBackend, WorkAssignment, WorkTemplate,
        };
        use std::collections::BTreeMap;
        use std::time::Duration;

        let dir = std::env::temp_dir().join(format!("seine-blake2b-memory-{}", std::process::id()));
        let backend = NvidiaBackend::new(
            Some(0),
            dir.join("seine.nvidia-autotune.json"),
            1,
            NvidiaBackendTuningOptions {
                max_rregcount_override: Some(super::super::DEFAULT_NVIDIA_MAX_RREGCOUNT),
                max_lanes_override: Some(1),
                ..NvidiaBackendTuningOptions::default()
            },
        );
        let (tx, rx) = crossbeam_channel::unbounded();
        backend.set_instance_id(1);
        backend.set_event_sink(tx);
        backend.start().expect("NVIDIA backend starts");
        let ctx = CudaContext::new(0).expect("CUDA context");
        let free_bytes = || ctx.mem_get_info().expect("memory info").0 as u64;

        let work = |epoch: u64, pow, header_len, start, count| WorkAssignment {
            template: Arc::new(WorkTemplate {
                work_id: epoch,
                epoch,
                header_base: Arc::from(vec![7u8; header_len]),
                pow,
                target: [0xff; 32],
                dynamic_share_target: None,
                pause_on_solution: false,
                stop_at: Instant::now() + Duration::from_secs(60),
            }),
            nonce_chunk: NonceChunk {
                start_nonce: start,
                nonce_count: count,
            },
        };
        let collect = |epoch: u64, count: usize| {
            let until = Instant::now() + Duration::from_secs(60);
            let mut found = BTreeMap::new();
            while found.len() < count && Instant::now() < until {
                if let Ok(BackendEvent::Solution(solution)) =
                    rx.recv_timeout(Duration::from_millis(50))
                {
                    if solution.epoch == epoch {
                        found.insert(solution.nonce, solution.hash.expect("hash"));
                    }
                }
            }
            found
        };

        backend
            // One nonce: the Argon2 path reports one solution per launch.
            .assign_work(work(1, PowAlgorithm::Argon2id, 92, 5, 1))
            .expect("Argon2 work");
        let before = collect(1, 1);
        assert_eq!(before.len(), 1);
        let free_with_argon2 = free_bytes();

        backend
            .assign_work(work(2, PowAlgorithm::Blake2b512, 156, 0, 1 << 20))
            .expect("BLAKE2b work");
        assert_eq!(collect(2, 1 << 20).len(), 1 << 20);
        let free_with_blake2b = free_bytes();
        // The BLAKE2b engine is created after the first reading and holds a
        // few MiB of its own, so most of the 2 GiB lane must come back.
        let lane = blocknet_pow_spec::CPU_LANE_MEMORY_BYTES;
        assert!(
            free_with_blake2b >= free_with_argon2 + lane - lane / 8,
            "BLAKE2b work frees the Argon2 lane: {free_with_argon2} -> {free_with_blake2b}"
        );

        backend
            .assign_work(work(3, PowAlgorithm::Argon2id, 92, 5, 1))
            .expect("Argon2 work after BLAKE2b");
        let after = collect(3, 1);
        backend.stop();
        assert_eq!(after, before, "the rebuilt engine hashes the same");
    }
}
