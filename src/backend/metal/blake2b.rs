//! Faraday BLAKE2b-512 PoW on Metal. The host builds the template midstate
//! with `blocknet_pow_kernel`; the device hashes the final block per nonce and
//! reports nonces whose first output limb passes the target. The host re-checks
//! every candidate with the CPU kernel, so only exact hits leave `scan`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use blocknet_pow_kernel::{Blake2bPowMidstate, Blake2bPowSingleBlock};
use metal::{
    Buffer, CommandQueue, CompileOptions, ComputePipelineState, Device, MTLCommandBufferStatus,
    MTLResourceOptions, MTLSize,
};

const KERNEL_SRC: &str = include_str!("blake2b_kernel.metal");
const KERNEL_NAME: &str = "blake2b_pow_scan_kernel";
const CANDIDATE_CAPACITY: usize = 4096;
/// Threads in one dispatch; each walks the range with a grid stride.
const GRID_THREADS: u64 = 1 << 18;
const THREADGROUP_THREADS: u64 = 256;
/// Wall time one launch aims for, keeping cancel and job switches prompt.
const LAUNCH_TARGET: Duration = Duration::from_millis(25);
const INITIAL_LAUNCH_WAVES: u64 = 16;
const MAX_LAUNCH_WAVES: u64 = 4096;

/// Mirrors `Blake2bPowParams` in the kernel.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct KernelParams {
    v: [u64; 16],
    m: [u64; 16],
    h0: u64,
    target_limb0: u64,
}

/// Compile-time layout; one pipeline per distinct header layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KernelShape {
    nonce_word: usize,
    nonce_shift: u32,
    message_words: usize,
    pre_columns: u8,
}

impl KernelShape {
    fn of(layout: &Blake2bPowSingleBlock) -> Self {
        Self {
            nonce_word: layout.nonce_word,
            nonce_shift: layout.nonce_shift,
            message_words: layout.message_words,
            pre_columns: layout.nonce_free_round0_columns(),
        }
    }

    fn source(self) -> String {
        format!(
            "#define SEINE_B2_NONCE_WORD {}\n#define SEINE_B2_NONCE_SHIFT {}\n#define SEINE_B2_MSG_WORDS {}\n#define SEINE_B2_PRE_COLUMNS {}\n{KERNEL_SRC}",
            self.nonce_word, self.nonce_shift, self.message_words, self.pre_columns
        )
    }
}

/// Metal objects are not `Send`; create and use this on the worker thread.
pub(super) struct MetalBlake2bEngine {
    device: Device,
    queue: CommandQueue,
    pipelines: Vec<(KernelShape, ComputePipelineState)>,
    candidates: Buffer,
    candidate_count: Buffer,
}

impl MetalBlake2bEngine {
    pub(super) fn new() -> Result<Self> {
        let device =
            Device::system_default().ok_or_else(|| anyhow!("no Metal device available"))?;
        let queue = device.new_command_queue();
        let shared = MTLResourceOptions::StorageModeShared;
        let candidates = device.new_buffer((CANDIDATE_CAPACITY * 8) as u64, shared);
        let candidate_count = device.new_buffer(4, shared);
        Ok(Self {
            device,
            queue,
            pipelines: Vec::new(),
            candidates,
            candidate_count,
        })
    }

    /// Nonces per full dispatch.
    pub(super) fn wave_nonces(&self) -> u64 {
        GRID_THREADS
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
        let pipeline = self.pipeline(shape)?;
        let params = KernelParams {
            v: layout.round0_state(shape.pre_columns),
            m: layout.message,
            h0: layout.h[0],
            target_limb0: u64::from_be_bytes(target[..8].try_into().expect("8 bytes")),
        };

        // A dispatch that finds more candidates than the buffer holds is split
        // and rerun; halves of at most CANDIDATE_CAPACITY nonces cannot overflow.
        let mut hits = Vec::new();
        let mut ranges = vec![(start_nonce, count)];
        while let Some((range_start, range_count)) = ranges.pop() {
            match self.launch_range(&pipeline, &params, range_start, range_count)? {
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

    /// One dispatch; `None` when its candidates overflowed the buffer.
    fn launch_range(
        &mut self,
        pipeline: &ComputePipelineState,
        params: &KernelParams,
        start_nonce: u64,
        count: u64,
    ) -> Result<Option<Vec<u64>>> {
        unsafe {
            *(self.candidate_count.contents() as *mut u32) = 0;
        }
        let capacity = CANDIDATE_CAPACITY as u32;
        let threads = count.min(GRID_THREADS).max(1);
        let group = THREADGROUP_THREADS
            .min(pipeline.max_total_threads_per_threadgroup())
            .max(1);

        // The worker thread has no autorelease pool of its own; drain the
        // command objects of each dispatch here.
        let status = metal::objc::rc::autoreleasepool(|| {
            let command_buffer = self.queue.new_command_buffer();
            let encoder = command_buffer.new_compute_command_encoder();
            encoder.set_compute_pipeline_state(pipeline);
            encoder.set_bytes(
                0,
                std::mem::size_of::<KernelParams>() as u64,
                (params as *const KernelParams).cast(),
            );
            encoder.set_bytes(1, 8, (&start_nonce as *const u64).cast());
            encoder.set_bytes(2, 8, (&count as *const u64).cast());
            encoder.set_buffer(3, Some(&self.candidates), 0);
            encoder.set_buffer(4, Some(&self.candidate_count), 0);
            encoder.set_bytes(5, 4, (&capacity as *const u32).cast());
            encoder.dispatch_threads(
                MTLSize::new(threads, 1, 1),
                MTLSize::new(group.min(threads), 1, 1),
            );
            encoder.end_encoding();
            command_buffer.commit();
            command_buffer.wait_until_completed();
            command_buffer.status()
        });
        if status != MTLCommandBufferStatus::Completed {
            bail!("Metal BLAKE2b dispatch failed with status {status:?}");
        }

        let found = unsafe { *(self.candidate_count.contents() as *const u32) } as usize;
        if found > CANDIDATE_CAPACITY {
            return Ok(None);
        }
        let candidates =
            unsafe { std::slice::from_raw_parts(self.candidates.contents() as *const u64, found) };
        Ok(Some(candidates.to_vec()))
    }

    fn pipeline(&mut self, shape: KernelShape) -> Result<ComputePipelineState> {
        if let Some((_, pipeline)) = self.pipelines.iter().find(|(known, _)| *known == shape) {
            return Ok(pipeline.clone());
        }
        let library = self
            .device
            .new_library_with_source(&shape.source(), &CompileOptions::new())
            .map_err(|err| anyhow!("failed to compile Metal BLAKE2b kernel: {err}"))?;
        let function = library
            .get_function(KERNEL_NAME, None)
            .map_err(|err| anyhow!("failed to load {KERNEL_NAME}: {err}"))?;
        let pipeline = self
            .device
            .new_compute_pipeline_state_with_function(&function)
            .map_err(|err| anyhow!("failed to build Metal BLAKE2b pipeline: {err}"))?;
        self.pipelines.push((shape, pipeline.clone()));
        Ok(pipeline)
    }
}

/// One worker's BLAKE2b engine (created on first use), template midstate and
/// adaptive launch size.
#[derive(Default)]
pub(super) struct Blake2bWorkerState {
    engine: Option<MetalBlake2bEngine>,
    midstate: Option<(Arc<[u8]>, Blake2bPowMidstate)>,
    launch_nonces: u64,
}

pub(super) struct Blake2bBatch {
    pub(super) hashes: u64,
    /// Exact hits in scan order with their target bytes.
    pub(super) solutions: Vec<(u64, [u8; 32])>,
}

impl Blake2bWorkerState {
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
            self.engine = Some(MetalBlake2bEngine::new()?);
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
    #[ignore = "requires a Metal GPU; run manually on macOS"]
    fn metal_blake2b_hits_match_cpu_kernel() {
        let mut engine = MetalBlake2bEngine::new().expect("Metal BLAKE2b engine");
        // The Faraday header, the legacy header and an aligned nonce word.
        for len in [FARADAY_HEADER_BASE_LEN, 92, 152] {
            let midstate = midstate(len);
            let count = 1u64 << 20;
            // About 16 hits per range, wrapping past u64::MAX in the second.
            for start in [0u64, u64::MAX - (count / 2)] {
                for target in [
                    target_with_limb0(u64::MAX >> 16),
                    [0u8; 32],
                    // Every nonce hits: overflowing dispatches split on the GPU.
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
    #[ignore = "requires a Metal GPU; run manually on macOS"]
    fn metal_blake2b_rate() {
        let mut engine = MetalBlake2bEngine::new().expect("Metal BLAKE2b engine");
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
            "metal-blake2b-rate | launch={launch} hashes={hashes} secs={secs:.3} mhs={:.1}",
            hashes as f64 / secs / 1e6
        );
    }

    #[test]
    #[ignore = "requires a Metal GPU; run manually on macOS"]
    fn metal_backend_mines_blake2b_assignments() {
        use crate::backend::metal::MetalBackend;
        use crate::backend::{
            BackendEvent, NonceChunk, PowAlgorithm, PowBackend, WorkAssignment, WorkTemplate,
        };
        use std::collections::BTreeMap;

        let backend = MetalBackend::new(Some(1), 1);
        let (tx, rx) = crossbeam_channel::unbounded();
        backend.set_instance_id(1);
        backend.set_event_sink(tx);
        backend.start().expect("Metal backend starts");

        let midstate = midstate(FARADAY_HEADER_BASE_LEN);
        let header: Vec<u8> = (0..FARADAY_HEADER_BASE_LEN)
            .map(|i| (i * 37 + 11) as u8)
            .collect();
        let target = target_with_limb0(u64::MAX >> 18);
        let count = 1u64 << 24;
        let start = 1u64 << 40;
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
}
