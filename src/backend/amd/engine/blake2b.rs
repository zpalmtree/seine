//! Faraday BLAKE2b-512 PoW on HIP. The host builds the template midstate with
//! `blocknet_pow_kernel`; the device hashes the final block per nonce and
//! reports nonces whose first output limb passes the target. The host
//! re-checks every candidate with the CPU kernel, so only exact hits leave
//! `scan`.

use std::ffi::{c_int, c_void};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use blocknet_pow_kernel::{Blake2bPowMidstate, Blake2bPowSingleBlock};

use super::{
    compile_hip_source, query_device_props, validate_wavefront_size, AmdDeviceInfo, DeviceBuffer,
    Function, HipApi, Module, Stream,
};

const KERNEL_SRC: &str = include_str!("blake2b_kernel.hip");
const KERNEL_NAME: &str = "blake2b_pow_scan_kernel";
const PROGRAM_NAME: &str = "seine_blake2b_pow.hip";
const THREADS_PER_BLOCK: u32 = 256;
const BLOCKS_PER_CU: u32 = 16;
const CANDIDATE_CAPACITY: usize = 4096;
/// Wall time one launch aims for, keeping cancel and job switches prompt.
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

/// Compile-time layout; one module per distinct header layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KernelShape {
    nonce_word: usize,
    nonce_shift: u32,
    message_words: usize,
    pre_columns: u8,
}

/// Layout of the Faraday header base (156 bytes), the shape release builds
/// carry a built-in code object for.
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
            pre_columns: layout.nonce_free_round0_columns(),
        }
    }
}

pub(crate) struct HipBlake2bEngine {
    api: &'static HipApi,
    device_index: u32,
    gcn_arch_name: String,
    wavefront_size: u32,
    stream: Stream,
    kernels: Vec<(KernelShape, Module, Function)>,
    candidates: DeviceBuffer,
    candidate_count: DeviceBuffer,
    grid_blocks: u32,
    /// GPU candidates whose CPU-recomputed hash fails the kernel's own 32-bit
    /// filter: the GPU computed a wrong hash (an unstable clock or voltage).
    gpu_mismatches: u64,
}

impl HipBlake2bEngine {
    pub(crate) fn new(device: &AmdDeviceInfo) -> Result<Self> {
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
        let compute_units = query_device_props(api, device.index as c_int)?
            .multi_processor_count
            .max(1) as u32;
        let stream = Stream::new(api)?;
        let candidates = DeviceBuffer::alloc(api, CANDIDATE_CAPACITY * 8)
            .map_err(|code| anyhow!("hipMalloc for BLAKE2b candidates failed ({code})"))?;
        let candidate_count = DeviceBuffer::alloc(api, 4)
            .map_err(|code| anyhow!("hipMalloc for BLAKE2b candidate count failed ({code})"))?;
        Ok(Self {
            api,
            device_index: device.index,
            gcn_arch_name: device.gcn_arch_name.clone(),
            wavefront_size: device.wavefront_size,
            stream,
            kernels: Vec::new(),
            candidates,
            candidate_count,
            grid_blocks: compute_units * BLOCKS_PER_CU,
            gpu_mismatches: 0,
        })
    }

    /// Bind the calling thread to this engine's device.
    pub(crate) fn bind_thread(&self) -> Result<()> {
        self.api.check(
            unsafe { (self.api.hip_set_device)(self.device_index as c_int) },
            "hipSetDevice failed while binding worker thread",
        )
    }

    /// Nonces per full launch wave.
    pub(crate) fn wave_nonces(&self) -> u64 {
        u64::from(self.grid_blocks) * u64::from(THREADS_PER_BLOCK)
    }

    /// Hashes `count` nonces from `start_nonce` (wrapping) and returns every
    /// nonce whose hash meets `target`, in scan order.
    pub(crate) fn scan(
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
        // and rerun; halves of at most CANDIDATE_CAPACITY nonces cannot overflow.
        let mut hits = Vec::new();
        let mut ranges = vec![(start_nonce, count)];
        while let Some((range_start, range_count)) = ranges.pop() {
            match self.launch_range(kernel, &params, range_start, range_count)? {
                Some(candidates) => {
                    for nonce in candidates {
                        let bytes = midstate.target_bytes(nonce);
                        if bytes[..4] > target[..4] {
                            self.gpu_mismatches += 1;
                        } else if bytes <= *target {
                            hits.push(nonce);
                        }
                    }
                }
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
        kernel: Function,
        params: &KernelParams,
        start_nonce: u64,
        count: u64,
    ) -> Result<Option<Vec<u64>>> {
        let zero = 0u32;
        self.api.check(
            unsafe {
                (self.api.hip_memcpy_htod_async)(
                    self.candidate_count.device_ptr(),
                    (&zero as *const u32).cast::<c_void>(),
                    4,
                    self.stream.raw,
                )
            },
            "hipMemcpyHtoDAsync failed for the BLAKE2b candidate count",
        )?;
        let blocks = u64::from(self.grid_blocks)
            .min(count.div_ceil(u64::from(THREADS_PER_BLOCK)))
            .max(1) as u32;
        let capacity = CANDIDATE_CAPACITY as u32;
        let mut candidates_ptr = self.candidates.device_ptr();
        let mut count_ptr = self.candidate_count.device_ptr();
        let mut start = start_nonce;
        let mut nonce_count = count;
        let mut params = *params;
        let mut capacity_arg = capacity;
        let mut args: [*mut c_void; 6] = [
            (&mut params as *mut KernelParams).cast(),
            (&mut start as *mut u64).cast(),
            (&mut nonce_count as *mut u64).cast(),
            (&mut candidates_ptr as *mut _ as *mut c_void),
            (&mut count_ptr as *mut _ as *mut c_void),
            (&mut capacity_arg as *mut u32).cast(),
        ];
        self.api.check(
            unsafe {
                (self.api.hip_module_launch_kernel)(
                    kernel.raw,
                    blocks,
                    1,
                    1,
                    THREADS_PER_BLOCK,
                    1,
                    1,
                    0,
                    self.stream.raw,
                    args.as_mut_ptr(),
                    std::ptr::null_mut(),
                )
            },
            "hipModuleLaunchKernel failed for the BLAKE2b kernel",
        )?;

        let mut found = 0u32;
        self.dtoh(&self.candidate_count, (&mut found as *mut u32).cast(), 4)?;
        let found = found as usize;
        if found > CANDIDATE_CAPACITY {
            return Ok(None);
        }
        let mut candidates = vec![0u64; found];
        if found > 0 {
            self.dtoh(&self.candidates, candidates.as_mut_ptr().cast(), found * 8)?;
        }
        Ok(Some(candidates))
    }

    fn dtoh(&self, src: &DeviceBuffer, dst: *mut c_void, bytes: usize) -> Result<()> {
        self.api.check(
            unsafe {
                (self.api.hip_memcpy_dtoh_async)(dst, src.device_ptr(), bytes, self.stream.raw)
            },
            "hipMemcpyDtoHAsync failed for BLAKE2b results",
        )?;
        self.stream.synchronize()
    }

    fn kernel(&mut self, shape: KernelShape) -> Result<Function> {
        if let Some((_, _, kernel)) = self.kernels.iter().find(|(known, ..)| *known == shape) {
            return Ok(*kernel);
        }
        let options = kernel_options(&self.gcn_arch_name, self.wavefront_size, shape);
        let key = crate::backend::amd::code_objects::options_key(KERNEL_SRC, &options);
        let builtin =
            crate::backend::amd::code_objects::find(CODE_OBJECT_KERNEL, &self.gcn_arch_name, key);
        let prefer_builtin =
            std::env::var_os("SEINE_AMD_BUILTIN_KERNELS").is_some_and(|value| value == "1");
        let code = match builtin {
            Some(code) if prefer_builtin || self.api.hiprtc.is_err() => code.to_vec(),
            _ => match compile_hip_source(self.api, KERNEL_SRC, PROGRAM_NAME, &options) {
                Ok(code) => code,
                Err(err) => match builtin {
                    Some(code) => code.to_vec(),
                    None => {
                        return Err(err.context(format!(
                            "failed to compile the HIP BLAKE2b kernel with hipRTC (no built-in kernel for {})",
                            self.gcn_arch_name.trim()
                        )))
                    }
                },
            },
        };
        let module = Module::load(self.api, &code)?;
        let kernel = module.function(KERNEL_NAME)?;
        self.kernels.push((shape, module, kernel));
        Ok(kernel)
    }
}

/// Name of the BLAKE2b kernel in the built-in code object table.
pub(crate) const CODE_OBJECT_KERNEL: &str = "blake2b";

fn kernel_options(gcn_arch_name: &str, wavefront_size: u32, shape: KernelShape) -> Vec<String> {
    let wavefront_flag = if wavefront_size == 64 {
        "-mwavefrontsize64"
    } else {
        "-mno-wavefrontsize64"
    };
    vec![
        format!("--offload-arch={}", gcn_arch_name.trim()),
        wavefront_flag.to_string(),
        format!("-DSEINE_B2_NONCE_WORD={}", shape.nonce_word),
        format!("-DSEINE_B2_NONCE_SHIFT={}", shape.nonce_shift),
        format!("-DSEINE_B2_MSG_WORDS={}", shape.message_words),
        format!("-DSEINE_B2_PRE_COLUMNS={}", shape.pre_columns),
    ]
}

/// hipRTC options and lookup key of the Faraday-layout kernel for a wave32 `target`.
pub(crate) fn export_options(target: &str) -> (Vec<String>, u64) {
    let options = kernel_options(target, 32, FARADAY_SHAPE);
    let key = crate::backend::amd::code_objects::options_key(KERNEL_SRC, &options);
    (options, key)
}

/// Compiles the BLAKE2b kernel with `options`, for `--amd-export-code-objects`.
pub(crate) fn compile_for_export(api: &'static HipApi, options: &[String]) -> Result<Vec<u8>> {
    compile_hip_source(api, KERNEL_SRC, PROGRAM_NAME, options)
}

/// One worker's BLAKE2b engine (created on first use), template midstate and
/// adaptive launch size.
pub(crate) struct Blake2bWorkerState {
    device: AmdDeviceInfo,
    engine: Option<HipBlake2bEngine>,
    midstate: Option<(Arc<[u8]>, Blake2bPowMidstate)>,
    launch_nonces: u64,
    mismatch_warned: bool,
}

pub(crate) struct Blake2bBatch {
    pub(crate) hashes: u64,
    /// Exact hits in scan order with their target bytes.
    pub(crate) solutions: Vec<(u64, [u8; 32])>,
}

impl Blake2bWorkerState {
    /// True once, the first time this GPU returns a candidate that fails the
    /// CPU check of the kernel's own filter.
    pub(crate) fn take_mismatch_warning(&mut self) -> bool {
        let mismatched = self
            .engine
            .as_ref()
            .is_some_and(|engine| engine.gpu_mismatches > 0);
        let warn = mismatched && !self.mismatch_warned;
        self.mismatch_warned |= mismatched;
        warn
    }

    pub(crate) fn new(device: AmdDeviceInfo) -> Self {
        Self {
            device,
            engine: None,
            midstate: None,
            launch_nonces: 0,
            mismatch_warned: false,
        }
    }

    /// Runs one launch over at most `remaining` nonces from `start_nonce`,
    /// trimmed so it should finish before `stop_at`.
    pub(crate) fn run_batch(
        &mut self,
        header_base: &Arc<[u8]>,
        start_nonce: u64,
        remaining: u64,
        target: &[u8; 32],
        stop_at: Option<Instant>,
    ) -> Result<Blake2bBatch> {
        if self.engine.is_none() {
            self.engine = Some(HipBlake2bEngine::new(&self.device)?);
        }
        let engine = self.engine.as_mut().expect("engine created above");
        engine.bind_thread()?;
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

    #[test]
    fn built_in_shape_is_the_faraday_header_layout() {
        let header: Vec<u8> = (0..FARADAY_HEADER_BASE_LEN).map(|i| i as u8).collect();
        let layout = Blake2bPowMidstate::new(&BLAKE2B_POW_PERSONAL_MAINNET, &header)
            .unwrap()
            .single_block_layout()
            .expect("the Faraday header fits one final block");
        assert_eq!(KernelShape::of(&layout), FARADAY_SHAPE);
    }

    fn first_device() -> AmdDeviceInfo {
        super::super::query_amd_devices()
            .expect("HIP runtime")
            .into_iter()
            .next()
            .expect("an AMD GPU")
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
    #[ignore = "requires an AMD GPU + ROCm runtime; run manually on the AMD host"]
    fn hip_blake2b_hits_match_cpu_kernel() {
        let mut engine = HipBlake2bEngine::new(&first_device()).expect("HIP BLAKE2b engine");
        // The Faraday header, the legacy header and an aligned nonce word.
        for len in [FARADAY_HEADER_BASE_LEN, 92, 152] {
            let midstate = midstate(len);
            let count = 1u64 << 22;
            // About 64 hits per range, wrapping past u64::MAX in the second.
            for start in [0u64, u64::MAX - (count / 2)] {
                for target in [
                    target_with_limb0(u64::MAX >> 16),
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
        // A healthy GPU never returns a candidate that fails its own filter.
        assert_eq!(engine.gpu_mismatches, 0);
    }

    #[test]
    #[ignore = "requires an AMD GPU + ROCm runtime; run manually on the AMD host"]
    fn hip_blake2b_rate() {
        let mut engine = HipBlake2bEngine::new(&first_device()).expect("HIP BLAKE2b engine");
        let midstate = midstate(FARADAY_HEADER_BASE_LEN);
        let target = target_with_limb0(0);
        let launch = engine.wave_nonces() * 64;
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
            "hip-blake2b-rate | launch={launch} hashes={hashes} secs={secs:.3} ghs={:.3}",
            hashes as f64 / secs / 1e9
        );
    }

    #[test]
    #[ignore = "requires an AMD GPU + ROCm runtime; run manually on the AMD host"]
    fn amd_backend_mines_blake2b_assignments() {
        use crate::backend::amd::{AmdBackend, AmdBackendTuningOptions};
        use crate::backend::{
            BackendEvent, NonceChunk, PowAlgorithm, PowBackend, WorkAssignment, WorkTemplate,
        };
        use std::collections::BTreeMap;

        let backend = AmdBackend::new(
            Some(first_device().index),
            AmdBackendTuningOptions {
                max_lanes_override: Some(1),
                ..AmdBackendTuningOptions::default()
            },
        );
        let (tx, rx) = crossbeam_channel::unbounded();
        backend.set_instance_id(1);
        backend.set_event_sink(tx);
        backend.start().expect("AMD backend starts");

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

        let until = Instant::now() + Duration::from_secs(60);
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
        assert_eq!(backend.hardware_telemetry().pow.as_deref(), Some("blake2b"));
        backend.stop();
        assert_eq!(hashes, count);
        assert_eq!(found, expected);
    }

    #[test]
    #[ignore = "requires AMD GPU + HIP runtime"]
    fn amd_backend_started_on_blake2b_builds_argon2_on_demand() {
        use crate::backend::amd::{AmdBackend, AmdBackendTuningOptions};
        use crate::backend::{NonceChunk, PowAlgorithm, PowBackend, WorkAssignment, WorkTemplate};

        // A backend that starts on a BLAKE2b network skips the Argon2 engine.
        crate::backend::note_network_pow(PowAlgorithm::Blake2b512);
        let backend = AmdBackend::new(
            Some(first_device().index),
            AmdBackendTuningOptions {
                max_lanes_override: Some(1),
                ..AmdBackendTuningOptions::default()
            },
        );
        let (tx, _rx) = crossbeam_channel::unbounded();
        backend.set_instance_id(1);
        backend.set_event_sink(tx);
        let started = Instant::now();
        backend.start().expect("AMD backend starts");
        println!("blake2b-only start {:.2}s", started.elapsed().as_secs_f64());
        let assign = |pow: PowAlgorithm, header_len: usize, count: u64, work_id: u64| {
            backend
                .assign_work(WorkAssignment {
                    template: Arc::new(WorkTemplate {
                        work_id,
                        epoch: work_id,
                        header_base: Arc::from(vec![7u8; header_len]),
                        pow,
                        target: [0u8; 32],
                        dynamic_share_target: None,
                        pause_on_solution: false,
                        stop_at: Instant::now() + Duration::from_secs(120),
                    }),
                    nonce_chunk: NonceChunk {
                        start_nonce: 0,
                        nonce_count: count,
                    },
                })
                .expect("assignment accepted");
        };
        let drain = |want: u64| {
            let until = Instant::now() + Duration::from_secs(120);
            let mut hashes = 0u64;
            while hashes < want && Instant::now() < until {
                hashes += backend.take_hashes();
                std::thread::sleep(Duration::from_millis(20));
            }
            hashes
        };
        let blake2b_count = 1u64 << 24;
        assign(
            PowAlgorithm::Blake2b512,
            FARADAY_HEADER_BASE_LEN,
            blake2b_count,
            1,
        );
        assert_eq!(drain(blake2b_count), blake2b_count);
        // Argon2 work after a BLAKE2b-only start builds the engine on demand.
        assign(
            PowAlgorithm::Argon2id,
            blocknet_pow_spec::POW_HEADER_BASE_LEN,
            8,
            2,
        );
        assert_eq!(drain(8), 8);
        backend.stop();
        crate::backend::note_network_pow(PowAlgorithm::Argon2id);
    }
}
