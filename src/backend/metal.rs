use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use blocknet_pow_spec::{pow_params, CPU_LANE_MEMORY_BYTES, POW_HEADER_BASE_LEN, POW_OUTPUT_LEN};
use crossbeam_channel::{bounded, Receiver, Sender};
use metal::{
    Buffer, CommandQueue, CompileOptions, ComputePipelineState, Device, FunctionConstantValues,
    MTLResourceOptions, MTLSize,
};

use crate::backend::{
    pow_hash_from_last_block_words, AssignmentSemantics, BackendCapabilities, BackendEvent,
    BackendExecutionModel, BackendInstanceId, BackendTelemetry, BenchBackend, DeadlineSupport,
    MiningSolution, PowBackend, PreemptionGranularity, WorkAssignment,
};

const BACKEND_NAME: &str = "metal";
const METAL_KERNEL_SRC: &str = include_str!("metal_kernel.metal");
const MAX_LANES_HARD_LIMIT: usize = 64;
const ASSIGN_CHANNEL_CAPACITY: usize = 256;
const CONTROL_CHANNEL_CAPACITY: usize = 32;
const EVENT_SEND_WAIT: Duration = Duration::from_millis(5);
const KERNEL_THREADS: u32 = 32;
const SEED_KERNEL_THREADS: u32 = 64;
const EVAL_KERNEL_THREADS: u32 = 64;
const DEFAULT_HASHES_PER_LAUNCH_PER_LANE: u32 = 2;
const MEMORY_RESERVE_RATIO: f64 = 0.10;

enum WorkerCommand {
    Assign(WorkAssignment, Sender<()>),
    Cancel(Sender<()>),
    Fence(Sender<()>),
    Stop,
}

struct MetalWorker {
    assignment_tx: Sender<WorkerCommand>,
    control_tx: Sender<WorkerCommand>,
    thread: Option<JoinHandle<()>>,
}

struct MetalShared {
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
    cancel_flag_ptr: AtomicU64, // raw pointer stored as u64 for Send
}

impl MetalShared {
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
            cancel_flag_ptr: AtomicU64::new(0),
        }
    }

    fn emit_event(&self, _instance_id: BackendInstanceId, event: BackendEvent) {
        let guard = self.event_sink.read().unwrap();
        if let Some(sink) = guard.as_ref() {
            if sink.send_timeout(event, EVENT_SEND_WAIT).is_err() {
                self.dropped_events.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn emit_solution(
        &self,
        instance_id: BackendInstanceId,
        epoch: u64,
        nonce: u64,
        hash: Option<[u8; 32]>,
        share_binding_id: crate::backend::ShareBindingId,
    ) {
        self.emit_event(
            instance_id,
            BackendEvent::Solution(MiningSolution {
                epoch,
                nonce,
                hash,
                share_binding_id,
                backend_id: instance_id,
                backend: BACKEND_NAME,
            }),
        );
    }

    fn emit_error(&self, instance_id: BackendInstanceId, message: String) {
        if self.error_emitted.swap(true, Ordering::AcqRel) {
            return;
        }
        self.emit_event(
            instance_id,
            BackendEvent::Error {
                backend_id: instance_id,
                backend: BACKEND_NAME,
                message,
            },
        );
    }
}

#[allow(dead_code)]
struct MetalArgon2Engine {
    device: Device,
    command_queue: CommandQueue,
    seed_pipeline: ComputePipelineState,
    fill_pipeline: ComputePipelineState,
    eval_pipeline: ComputePipelineState,
    lanes: usize,
    hashes_per_launch_per_lane: u32,
    max_hashes_per_launch: u32,
    m_blocks: u32,
    t_cost: u32,
    // Buffers (all StorageModeShared for unified memory zero-copy)
    lane_memory: Buffer,
    seed_blocks_buf: Buffer,
    last_blocks_buf: Buffer,
    header_base_buf: Buffer,
    nonce_buf: Buffer,
    target_buf: Buffer,
    cancel_flag_buf: Buffer,
    completed_iters_buf: Buffer,
    found_index_buf: Buffer,
    // Scalar uniform buffers
    header_base_len_buf: Buffer,
    active_hashes_buf: Buffer,
    m_cost_kib_buf: Buffer,
    t_cost_buf: Buffer,
    lanes_active_buf: Buffer,
    active_hashes_fill_buf: Buffer,
    lane_launch_iters_buf: Buffer,
    evaluate_target_buf: Buffer,
    eval_active_hashes_buf: Buffer,
}

fn div_ceil(a: u32, b: u32) -> u32 {
    (a + b - 1) / b
}

impl MetalArgon2Engine {
    fn new(max_lanes_override: Option<usize>, hashes_per_launch_per_lane: u32) -> Result<Self> {
        let device = Device::system_default().ok_or_else(|| anyhow!("no Metal device found"))?;

        let (m_cost_kib, t_cost) = pow_params()
            .map(|params| (params.m_cost(), params.t_cost()))
            .unwrap_or((0, 0));
        if m_cost_kib == 0 || t_cost == 0 {
            bail!("failed to derive PoW params");
        }
        let m_blocks = m_cost_kib; // m_cost is already in KiB = number of 1024-byte blocks

        let command_queue = device.new_command_queue();

        // Compile shader with function constants
        let fc = FunctionConstantValues::new();
        fc.set_constant_value_at_index(
            &m_blocks as *const u32 as *const _,
            metal::MTLDataType::UInt,
            0,
        );
        fc.set_constant_value_at_index(
            &t_cost as *const u32 as *const _,
            metal::MTLDataType::UInt,
            1,
        );

        let opts = CompileOptions::new();
        opts.set_language_version(metal::MTLLanguageVersion::V2_4);
        let library = device
            .new_library_with_source(METAL_KERNEL_SRC, &opts)
            .map_err(|e| anyhow!("Metal shader compilation failed: {e}"))?;

        let seed_fn = library
            .get_function("build_seed_blocks_kernel", Some(fc.clone()))
            .map_err(|e| anyhow!("failed to load build_seed_blocks_kernel: {e}"))?;
        let fill_fn = library
            .get_function("argon2id_fill_kernel", Some(fc.clone()))
            .map_err(|e| anyhow!("failed to load argon2id_fill_kernel: {e}"))?;
        let eval_fn = library
            .get_function("evaluate_hashes_kernel", Some(fc))
            .map_err(|e| anyhow!("failed to load evaluate_hashes_kernel: {e}"))?;

        let seed_pipeline = device
            .new_compute_pipeline_state_with_function(&seed_fn)
            .map_err(|e| anyhow!("failed to create seed pipeline: {e}"))?;
        let fill_pipeline = device
            .new_compute_pipeline_state_with_function(&fill_fn)
            .map_err(|e| anyhow!("failed to create fill pipeline: {e}"))?;
        let eval_pipeline = device
            .new_compute_pipeline_state_with_function(&eval_fn)
            .map_err(|e| anyhow!("failed to create eval pipeline: {e}"))?;

        // Determine lane count from unified memory budget
        let max_working_set = device.recommended_max_working_set_size();
        let per_lane_bytes = CPU_LANE_MEMORY_BYTES; // 2 GiB
        let usable = ((max_working_set as f64) * (1.0 - MEMORY_RESERVE_RATIO)) as u64;
        let estimated_lanes = (usable / per_lane_bytes) as usize;
        let max_lanes = match max_lanes_override {
            Some(forced) => forced.min(MAX_LANES_HARD_LIMIT).max(1),
            None => estimated_lanes.min(MAX_LANES_HARD_LIMIT).max(1),
        };

        // Try-allocate loop: count down from max_lanes
        let hashes_per_launch_per_lane = hashes_per_launch_per_lane.max(1);
        let resource_opts = MTLResourceOptions::StorageModeShared;
        let mut lanes = max_lanes;
        let lane_memory;
        loop {
            let lane_words = m_blocks as u64 * 128;
            let total_bytes = lanes as u64 * lane_words * 8;
            let buf = device.new_buffer(total_bytes, resource_opts);
            if buf.length() >= total_bytes {
                lane_memory = buf;
                break;
            }
            if lanes <= 1 {
                bail!(
                    "failed to allocate lane memory even for 1 lane ({} bytes required)",
                    lane_words * 8
                );
            }
            lanes -= 1;
        }

        let max_hashes_per_launch = (lanes as u32) * hashes_per_launch_per_lane;

        // Allocate auxiliary buffers
        let seed_blocks_buf =
            device.new_buffer(max_hashes_per_launch as u64 * 256 * 8, resource_opts);
        let last_blocks_buf =
            device.new_buffer(max_hashes_per_launch as u64 * 128 * 8, resource_opts);
        let header_base_buf = device.new_buffer(POW_HEADER_BASE_LEN as u64, resource_opts);
        let nonce_buf = device.new_buffer(max_hashes_per_launch as u64 * 8, resource_opts);
        let target_buf = device.new_buffer(POW_OUTPUT_LEN as u64, resource_opts);
        let cancel_flag_buf = device.new_buffer(4, resource_opts);
        let completed_iters_buf = device.new_buffer(4, resource_opts);
        let found_index_buf = device.new_buffer(4, resource_opts);

        // Scalar uniform buffers
        let header_base_len_buf = device.new_buffer(4, resource_opts);
        let active_hashes_buf = device.new_buffer(4, resource_opts);
        let m_cost_kib_buf = device.new_buffer(4, resource_opts);
        let t_cost_buf = device.new_buffer(4, resource_opts);
        let lanes_active_buf = device.new_buffer(4, resource_opts);
        let active_hashes_fill_buf = device.new_buffer(4, resource_opts);
        let lane_launch_iters_buf = device.new_buffer(4, resource_opts);
        let evaluate_target_buf = device.new_buffer(4, resource_opts);
        let eval_active_hashes_buf = device.new_buffer(4, resource_opts);

        // Pre-fill constants that don't change between launches
        write_u32(&m_cost_kib_buf, m_cost_kib);
        write_u32(&t_cost_buf, t_cost);

        Ok(Self {
            device,
            command_queue,
            seed_pipeline,
            fill_pipeline,
            eval_pipeline,
            lanes,
            hashes_per_launch_per_lane,
            max_hashes_per_launch,
            m_blocks,
            t_cost,
            lane_memory,
            seed_blocks_buf,
            last_blocks_buf,
            header_base_buf,
            nonce_buf,
            target_buf,
            cancel_flag_buf,
            completed_iters_buf,
            found_index_buf,
            header_base_len_buf,
            active_hashes_buf,
            m_cost_kib_buf,
            t_cost_buf,
            lanes_active_buf,
            active_hashes_fill_buf,
            lane_launch_iters_buf,
            evaluate_target_buf,
            eval_active_hashes_buf,
        })
    }

    fn cancel_flag_ptr(&self) -> *mut u32 {
        self.cancel_flag_buf.contents() as *mut u32
    }

    fn run_fill_batch(
        &self,
        header_base: &[u8],
        nonces: &[u64],
        target: Option<&[u8; 32]>,
    ) -> Result<FillBatchResult> {
        let requested_hashes = nonces.len() as u32;
        if requested_hashes == 0 {
            return Ok(FillBatchResult {
                hashes_done: 0,
                solved_nonce: None,
                solved_hash: None,
            });
        }
        if requested_hashes > self.max_hashes_per_launch {
            bail!(
                "requested {} hashes but max is {}",
                requested_hashes,
                self.max_hashes_per_launch
            );
        }

        // Write inputs to shared buffers (zero-copy)
        unsafe {
            let hdr_ptr = self.header_base_buf.contents() as *mut u8;
            std::ptr::copy_nonoverlapping(
                header_base.as_ptr(),
                hdr_ptr,
                header_base.len().min(POW_HEADER_BASE_LEN),
            );

            let nonce_ptr = self.nonce_buf.contents() as *mut u64;
            std::ptr::copy_nonoverlapping(nonces.as_ptr(), nonce_ptr, nonces.len());

            if let Some(target) = target {
                let target_ptr = self.target_buf.contents() as *mut u8;
                std::ptr::copy_nonoverlapping(target.as_ptr(), target_ptr, 32);
            }

            // Reset control flags
            let cancel_ptr = self.cancel_flag_buf.contents() as *mut u32;
            *cancel_ptr = 0;
            let completed_ptr = self.completed_iters_buf.contents() as *mut u32;
            *completed_ptr = 0;
            let found_ptr = self.found_index_buf.contents() as *mut u32;
            *found_ptr = u32::MAX;
        }

        // Write scalar uniforms
        write_u32(&self.header_base_len_buf, header_base.len() as u32);
        write_u32(&self.active_hashes_buf, requested_hashes);

        let active_lanes = (self.lanes as u32).min(requested_hashes);
        let lane_launch_iters = div_ceil(requested_hashes, active_lanes);
        let evaluate_target: u32 = if target.is_some() { 1 } else { 0 };

        write_u32(&self.lanes_active_buf, active_lanes);
        write_u32(&self.active_hashes_fill_buf, requested_hashes);
        write_u32(&self.lane_launch_iters_buf, lane_launch_iters);
        write_u32(&self.evaluate_target_buf, evaluate_target);
        write_u32(&self.eval_active_hashes_buf, requested_hashes);

        let cmd_buf = self.command_queue.new_command_buffer();

        // ---- Kernel 1: build_seed_blocks ----
        {
            let encoder = cmd_buf.new_compute_command_encoder();
            encoder.set_compute_pipeline_state(&self.seed_pipeline);
            encoder.set_buffer(0, Some(&self.header_base_buf), 0);
            encoder.set_buffer(1, Some(&self.nonce_buf), 0);
            encoder.set_buffer(2, Some(&self.seed_blocks_buf), 0);
            encoder.set_buffer(3, Some(&self.header_base_len_buf), 0);
            encoder.set_buffer(4, Some(&self.active_hashes_buf), 0);
            encoder.set_buffer(5, Some(&self.m_cost_kib_buf), 0);
            encoder.set_buffer(6, Some(&self.t_cost_buf), 0);

            let threads_per_group = MTLSize::new(SEED_KERNEL_THREADS as u64, 1, 1);
            let total_threads = MTLSize::new(
                div_ceil(requested_hashes, SEED_KERNEL_THREADS) as u64 * SEED_KERNEL_THREADS as u64,
                1,
                1,
            );
            encoder.dispatch_threads(total_threads, threads_per_group);
            encoder.end_encoding();
        }

        // ---- Kernel 2: argon2id_fill ----
        {
            let encoder = cmd_buf.new_compute_command_encoder();
            encoder.set_compute_pipeline_state(&self.fill_pipeline);
            encoder.set_buffer(0, Some(&self.seed_blocks_buf), 0);
            encoder.set_buffer(1, Some(&self.lane_memory), 0);
            encoder.set_buffer(2, Some(&self.last_blocks_buf), 0);
            encoder.set_buffer(3, Some(&self.cancel_flag_buf), 0);
            encoder.set_buffer(4, Some(&self.completed_iters_buf), 0);
            encoder.set_buffer(5, Some(&self.target_buf), 0);
            encoder.set_buffer(6, Some(&self.found_index_buf), 0);
            encoder.set_buffer(7, Some(&self.lanes_active_buf), 0);
            encoder.set_buffer(8, Some(&self.active_hashes_fill_buf), 0);
            encoder.set_buffer(9, Some(&self.lane_launch_iters_buf), 0);
            encoder.set_buffer(10, Some(&self.evaluate_target_buf), 0);

            let threadgroups = MTLSize::new(active_lanes as u64, 1, 1);
            let threads_per_group = MTLSize::new(KERNEL_THREADS as u64, 1, 1);
            encoder.dispatch_thread_groups(threadgroups, threads_per_group);
            encoder.end_encoding();
        }

        // ---- Kernel 3: evaluate_hashes (if no fused target check) ----
        if target.is_some() && evaluate_target == 0 {
            let encoder = cmd_buf.new_compute_command_encoder();
            encoder.set_compute_pipeline_state(&self.eval_pipeline);
            encoder.set_buffer(0, Some(&self.last_blocks_buf), 0);
            encoder.set_buffer(1, Some(&self.target_buf), 0);
            encoder.set_buffer(2, Some(&self.found_index_buf), 0);
            encoder.set_buffer(3, Some(&self.eval_active_hashes_buf), 0);

            let threads_per_group = MTLSize::new(EVAL_KERNEL_THREADS as u64, 1, 1);
            let total_threads = MTLSize::new(
                div_ceil(requested_hashes, EVAL_KERNEL_THREADS) as u64 * EVAL_KERNEL_THREADS as u64,
                1,
                1,
            );
            encoder.dispatch_threads(total_threads, threads_per_group);
            encoder.end_encoding();
        }

        cmd_buf.commit();
        cmd_buf.wait_until_completed();

        // Read back results from shared memory
        let completed_iters = unsafe { *(self.completed_iters_buf.contents() as *const u32) };
        let found_index_one_based = unsafe { *(self.found_index_buf.contents() as *const u32) };

        let hashes_done = (completed_iters as u64)
            .saturating_mul(active_lanes as u64)
            .min(requested_hashes as u64);

        let (solved_nonce, solved_hash) =
            if found_index_one_based != u32::MAX && found_index_one_based > 0 {
                let idx = (found_index_one_based - 1) as usize;
                if idx < nonces.len() {
                    (Some(nonces[idx]), Some(self.read_last_block_hash(idx)?))
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

        Ok(FillBatchResult {
            hashes_done,
            solved_nonce,
            solved_hash,
        })
    }

    fn read_last_block_hash(&self, hash_idx: usize) -> Result<[u8; 32]> {
        let start = hash_idx
            .checked_mul(128)
            .ok_or_else(|| anyhow!("hash index overflow while reading Metal last block"))?;
        let end = start
            .checked_add(128)
            .ok_or_else(|| anyhow!("hash index overflow while reading Metal last block"))?;
        let max_words = (self.max_hashes_per_launch as usize).saturating_mul(128);
        if end > max_words {
            bail!(
                "hash index {} out of range for Metal last-block buffer (capacity words={})",
                hash_idx,
                max_words
            );
        }

        let mut block_words = [0u64; 128];
        unsafe {
            let words_ptr = self.last_blocks_buf.contents() as *const u64;
            let words = std::slice::from_raw_parts(words_ptr.add(start), 128);
            block_words.copy_from_slice(words);
        }
        Ok(pow_hash_from_last_block_words(&block_words))
    }
}

struct FillBatchResult {
    hashes_done: u64,
    solved_nonce: Option<u64>,
    solved_hash: Option<[u8; 32]>,
}

fn write_u32(buf: &Buffer, value: u32) {
    unsafe {
        let ptr = buf.contents() as *mut u32;
        *ptr = value;
    }
}

pub struct MetalBackend {
    instance_id: AtomicU64,
    shared: Arc<MetalShared>,
    worker: Mutex<Option<MetalWorker>>,
    max_lanes: AtomicU64,
    max_lanes_override: Option<usize>,
    hashes_per_launch_per_lane: u32,
}

// Safety: MetalBackend is Send+Sync because:
// - All Metal objects live exclusively on the worker thread
// - cancel_flag_ptr in MetalShared is a raw pointer to StorageModeShared
//   memory that is process-coherent (unified memory on Apple Silicon).
//   Only atomic/volatile writes are performed through it.
// - All other shared state uses std::sync primitives.
unsafe impl Send for MetalBackend {}
unsafe impl Sync for MetalBackend {}

impl MetalBackend {
    pub fn new(max_lanes: Option<usize>, hashes_per_launch_per_lane: u32) -> Self {
        Self {
            instance_id: AtomicU64::new(0),
            shared: Arc::new(MetalShared::new()),
            worker: Mutex::new(None),
            max_lanes: AtomicU64::new(0),
            max_lanes_override: max_lanes,
            hashes_per_launch_per_lane: hashes_per_launch_per_lane.max(1),
        }
    }

    fn signal_cancel(&self) {
        let ptr = self.shared.cancel_flag_ptr.load(Ordering::Acquire);
        if ptr != 0 {
            unsafe {
                let flag = ptr as *mut u32;
                std::ptr::write_volatile(flag, 1);
            }
        }
    }
}

impl PowBackend for MetalBackend {
    fn name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn lanes(&self) -> usize {
        self.max_lanes.load(Ordering::Acquire) as usize
    }

    fn device_memory_bytes(&self) -> Option<u64> {
        Device::system_default().map(|d| d.recommended_max_working_set_size())
    }

    fn set_instance_id(&self, id: BackendInstanceId) {
        self.instance_id.store(id, Ordering::Release);
    }

    fn set_event_sink(&self, sink: Sender<BackendEvent>) {
        let mut guard = self.shared.event_sink.write().unwrap();
        *guard = Some(sink);
    }

    fn start(&self) -> Result<()> {
        let mut worker_guard = self.worker.lock().unwrap();
        if worker_guard.is_some() {
            bail!("Metal backend already started");
        }

        let (assignment_tx, assignment_rx) = bounded(ASSIGN_CHANNEL_CAPACITY);
        let (control_tx, control_rx) = bounded(CONTROL_CHANNEL_CAPACITY);

        let shared = Arc::clone(&self.shared);
        let instance_id = self.instance_id.load(Ordering::Acquire);
        let max_lanes_override = self.max_lanes_override;
        let hashes_per_launch_per_lane = self.hashes_per_launch_per_lane;
        let max_lanes_atomic = {
            // Clone a handle so the worker can write back discovered lanes
            let ptr = &self.max_lanes as *const AtomicU64;
            ptr as u64
        };

        let thread = thread::Builder::new()
            .name(format!("metal-worker-{instance_id}"))
            .spawn(move || {
                metal_worker_loop(
                    instance_id,
                    max_lanes_override,
                    hashes_per_launch_per_lane,
                    max_lanes_atomic,
                    shared,
                    assignment_rx,
                    control_rx,
                );
            })
            .map_err(|e| anyhow!("failed to spawn Metal worker thread: {e}"))?;

        *worker_guard = Some(MetalWorker {
            assignment_tx,
            control_tx,
            thread: Some(thread),
        });

        // Wait for worker to report ready (lanes > 0) or error
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if self.max_lanes.load(Ordering::Acquire) > 0 {
                break;
            }
            if self.shared.error_emitted.load(Ordering::Acquire) {
                bail!("Metal backend worker failed during initialization");
            }
            if Instant::now() > deadline {
                bail!("Metal backend initialization timed out");
            }
            thread::sleep(Duration::from_millis(50));
        }

        Ok(())
    }

    fn stop(&self) {
        let mut worker_guard = self.worker.lock().unwrap();
        if let Some(mut worker) = worker_guard.take() {
            let _ = worker.control_tx.send(WorkerCommand::Stop);
            if let Some(thread) = worker.thread.take() {
                let _ = thread.join();
            }
        }
    }

    fn assign_work(&self, work: WorkAssignment) -> Result<()> {
        let worker_guard = self.worker.lock().unwrap();
        let worker = worker_guard
            .as_ref()
            .ok_or_else(|| anyhow!("Metal backend not started"))?;
        let (ack_tx, ack_rx) = bounded(1);
        worker
            .assignment_tx
            .send(WorkerCommand::Assign(work, ack_tx))
            .map_err(|_| anyhow!("Metal worker channel closed"))?;
        ack_rx
            .recv_timeout(Duration::from_secs(30))
            .map_err(|_| anyhow!("Metal assign_work timed out"))?;
        Ok(())
    }

    fn cancel_work(&self) -> Result<()> {
        let worker_guard = self.worker.lock().unwrap();
        let worker = worker_guard
            .as_ref()
            .ok_or_else(|| anyhow!("Metal backend not started"))?;
        let (ack_tx, ack_rx) = bounded(1);
        worker
            .control_tx
            .send(WorkerCommand::Cancel(ack_tx))
            .map_err(|_| anyhow!("Metal worker channel closed"))?;
        ack_rx
            .recv_timeout(Duration::from_secs(30))
            .map_err(|_| anyhow!("Metal cancel_work timed out"))?;
        Ok(())
    }

    fn request_timeout_interrupt(&self) -> Result<()> {
        self.shared.cancel_requested.store(true, Ordering::Release);
        self.signal_cancel();
        Ok(())
    }

    fn fence(&self) -> Result<()> {
        let worker_guard = self.worker.lock().unwrap();
        let worker = worker_guard
            .as_ref()
            .ok_or_else(|| anyhow!("Metal backend not started"))?;
        let (ack_tx, ack_rx) = bounded(1);
        worker
            .control_tx
            .send(WorkerCommand::Fence(ack_tx))
            .map_err(|_| anyhow!("Metal worker channel closed"))?;
        ack_rx
            .recv_timeout(Duration::from_secs(60))
            .map_err(|_| anyhow!("Metal fence timed out"))?;
        Ok(())
    }

    fn take_hashes(&self) -> u64 {
        self.shared.hashes.swap(0, Ordering::AcqRel)
    }

    fn take_telemetry(&self) -> BackendTelemetry {
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
            inflight_assignment_micros: self
                .shared
                .inflight_assignment_started_at
                .lock()
                .unwrap()
                .map(|t| t.elapsed().as_micros() as u64)
                .unwrap_or(0),
            ..Default::default()
        }
    }

    fn preemption_granularity(&self) -> PreemptionGranularity {
        let lanes = self.lanes() as u64;
        let depth = self
            .shared
            .active_hashes_per_launch_per_lane
            .load(Ordering::Acquire) as u64;
        PreemptionGranularity::Hashes(lanes.saturating_mul(depth).max(1))
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            max_inflight_assignments: 1,
            deadline_support: DeadlineSupport::BestEffort,
            assignment_semantics: AssignmentSemantics::Replace,
            execution_model: BackendExecutionModel::Blocking,
            preferred_iters_per_lane: Some(1 << 20),
            preferred_allocation_iters_per_lane: Some(1 << 20),
            preferred_hash_poll_interval: Some(Duration::from_millis(50)),
            ..Default::default()
        }
    }

    fn bench_backend(&self) -> Option<&dyn BenchBackend> {
        Some(self)
    }
}

impl BenchBackend for MetalBackend {
    fn kernel_bench(&self, seconds: u64, shutdown: &AtomicBool) -> Result<u64> {
        let engine =
            MetalArgon2Engine::new(self.max_lanes_override, self.hashes_per_launch_per_lane)?;

        let max_hashes = engine.max_hashes_per_launch as usize;
        let mut nonces = vec![0u64; max_hashes];
        let header_base = [0u8; POW_HEADER_BASE_LEN];
        let target = [0xffu8; 32];
        let mut total_hashes = 0u64;
        let mut nonce_counter = 0u64;
        let deadline = Instant::now() + Duration::from_secs(seconds);

        while Instant::now() < deadline && !shutdown.load(Ordering::Relaxed) {
            for i in 0..max_hashes {
                nonces[i] = nonce_counter;
                nonce_counter += 1;
            }
            let result = engine.run_fill_batch(&header_base, &nonces, Some(&target))?;
            total_hashes += result.hashes_done;
        }

        Ok(total_hashes)
    }
}

fn metal_worker_loop(
    instance_id: BackendInstanceId,
    max_lanes_override: Option<usize>,
    hashes_per_launch_per_lane: u32,
    max_lanes_atomic_ptr: u64,
    shared: Arc<MetalShared>,
    assignment_rx: Receiver<WorkerCommand>,
    control_rx: Receiver<WorkerCommand>,
) {
    // Initialize engine on worker thread (Metal objects are !Send)
    let engine = match MetalArgon2Engine::new(max_lanes_override, hashes_per_launch_per_lane) {
        Ok(engine) => engine,
        Err(e) => {
            shared.emit_error(instance_id, format!("Metal init failed: {e:#}"));
            return;
        }
    };

    // Store cancel flag pointer for cross-thread cancel signaling
    let cancel_ptr = engine.cancel_flag_ptr() as u64;
    shared.cancel_flag_ptr.store(cancel_ptr, Ordering::Release);

    // Publish discovered lanes
    let lanes = engine.lanes as u64;
    unsafe {
        let atomic = &*(max_lanes_atomic_ptr as *const AtomicU64);
        atomic.store(lanes, Ordering::Release);
    }
    shared.active_lanes.store(lanes, Ordering::Release);
    shared
        .active_hashes_per_launch_per_lane
        .store(engine.hashes_per_launch_per_lane, Ordering::Release);

    let mut active: Option<ActiveAssignment> = None;
    let mut fence_waiters: Vec<Sender<()>> = Vec::new();

    loop {
        // Drain control commands first
        loop {
            match control_rx.try_recv() {
                Ok(WorkerCommand::Cancel(ack)) => {
                    active = None;
                    shared.pending_work.store(0, Ordering::Release);
                    shared
                        .inflight_assignment_hashes
                        .store(0, Ordering::Release);
                    *shared.inflight_assignment_started_at.lock().unwrap() = None;
                    shared.cancel_requested.store(false, Ordering::Release);
                    let _ = ack.send(());
                }
                Ok(WorkerCommand::Fence(ack)) => {
                    if active.is_none() {
                        let _ = ack.send(());
                    } else {
                        fence_waiters.push(ack);
                    }
                }
                Ok(WorkerCommand::Stop) => {
                    for waiter in fence_waiters.drain(..) {
                        let _ = waiter.send(());
                    }
                    return;
                }
                Ok(WorkerCommand::Assign(..)) => {}
                Err(_) => break,
            }
        }

        // Check for new assignment
        match assignment_rx.try_recv() {
            Ok(WorkerCommand::Assign(work, ack)) => {
                // Replace semantics: drop previous assignment
                active = Some(ActiveAssignment::new(work));
                shared.pending_work.store(1, Ordering::Release);
                let _ = ack.send(());
            }
            Ok(WorkerCommand::Stop) => {
                for waiter in fence_waiters.drain(..) {
                    let _ = waiter.send(());
                }
                return;
            }
            _ => {}
        }

        // Process active assignment
        if let Some(ref mut assignment) = active {
            if shared.cancel_requested.swap(false, Ordering::AcqRel) {
                active = None;
                shared.pending_work.store(0, Ordering::Release);
                shared
                    .inflight_assignment_hashes
                    .store(0, Ordering::Release);
                *shared.inflight_assignment_started_at.lock().unwrap() = None;
                // Drain fence waiters
                for waiter in fence_waiters.drain(..) {
                    let _ = waiter.send(());
                }
                continue;
            }

            let remaining = assignment.remaining_nonces();
            if remaining == 0 || Instant::now() > assignment.work.template.stop_at {
                // Assignment complete
                let elapsed = assignment.started_at.elapsed();
                shared.completed_assignments.fetch_add(1, Ordering::Relaxed);
                shared
                    .completed_assignment_hashes
                    .fetch_add(assignment.hashes_done, Ordering::Relaxed);
                shared
                    .completed_assignment_micros
                    .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
                shared
                    .inflight_assignment_hashes
                    .store(0, Ordering::Release);
                *shared.inflight_assignment_started_at.lock().unwrap() = None;
                shared.pending_work.store(0, Ordering::Release);
                active = None;
                for waiter in fence_waiters.drain(..) {
                    let _ = waiter.send(());
                }
                continue;
            }

            // Dispatch a batch
            let batch_size = remaining.min(engine.max_hashes_per_launch as u64) as usize;
            let start_nonce = assignment.next_nonce;
            let nonces: Vec<u64> = (0..batch_size as u64).map(|i| start_nonce + i).collect();

            shared
                .inflight_assignment_hashes
                .store(batch_size as u64, Ordering::Release);
            *shared.inflight_assignment_started_at.lock().unwrap() = Some(Instant::now());
            shared.active_hashes_per_launch_per_lane.store(
                div_ceil(batch_size as u32, engine.lanes as u32).max(1),
                Ordering::Release,
            );

            let target_snapshot = assignment.work.template.target_snapshot();
            match engine.run_fill_batch(
                &assignment.work.template.header_base,
                &nonces,
                Some(&target_snapshot.target),
            ) {
                Ok(result) => {
                    let done = result.hashes_done;
                    assignment.next_nonce += done;
                    assignment.hashes_done += done;
                    shared.hashes.fetch_add(done, Ordering::Relaxed);

                    if let Some(solved_nonce) = result.solved_nonce {
                        shared.emit_solution(
                            instance_id,
                            assignment.work.template.epoch,
                            solved_nonce,
                            result.solved_hash,
                            target_snapshot.share_binding_id,
                        );
                    }
                }
                Err(e) => {
                    shared.emit_error(instance_id, format!("Metal fill batch failed: {e:#}"));
                    drop(active);
                    shared.pending_work.store(0, Ordering::Release);
                    shared
                        .inflight_assignment_hashes
                        .store(0, Ordering::Release);
                    *shared.inflight_assignment_started_at.lock().unwrap() = None;
                    for waiter in fence_waiters.drain(..) {
                        let _ = waiter.send(());
                    }
                    return;
                }
            }
        } else {
            // Idle: block on incoming commands
            crossbeam_channel::select! {
                recv(control_rx) -> msg => {
                    match msg {
                        Ok(WorkerCommand::Cancel(ack)) => {
                            shared.cancel_requested.store(false, Ordering::Release);
                            let _ = ack.send(());
                        }
                        Ok(WorkerCommand::Fence(ack)) => {
                            shared.cancel_requested.store(false, Ordering::Release);
                            let _ = ack.send(());
                        }
                        Ok(WorkerCommand::Stop) => {
                            for waiter in fence_waiters.drain(..) { let _ = waiter.send(()); }
                            return;
                        }
                        _ => {}
                    }
                }
                recv(assignment_rx) -> msg => {
                    match msg {
                        Ok(WorkerCommand::Assign(work, ack)) => {
                            active = Some(ActiveAssignment::new(work));
                            shared.pending_work.store(1, Ordering::Release);
                            let _ = ack.send(());
                        }
                        Ok(WorkerCommand::Stop) => {
                            for waiter in fence_waiters.drain(..) { let _ = waiter.send(()); }
                            return;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

struct ActiveAssignment {
    work: WorkAssignment,
    next_nonce: u64,
    hashes_done: u64,
    started_at: Instant,
}

impl ActiveAssignment {
    fn new(work: WorkAssignment) -> Self {
        let next_nonce = work.nonce_chunk.start_nonce;
        Self {
            work,
            next_nonce,
            hashes_done: 0,
            started_at: Instant::now(),
        }
    }

    fn remaining_nonces(&self) -> u64 {
        let end = self
            .work
            .nonce_chunk
            .start_nonce
            .saturating_add(self.work.nonce_chunk.nonce_count);
        end.saturating_sub(self.next_nonce)
    }
}
