use std::convert::TryFrom;
use std::ffi::{c_char, CStr, CString};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use blake2::{
    digest::{self, Digest, VariableOutput},
    Blake2b512, Blake2bVar,
};
use blocknet_pow_spec::{pow_params, CPU_LANE_MEMORY_BYTES, POW_HEADER_BASE_LEN, POW_OUTPUT_LEN};
use crossbeam_channel::{bounded, Receiver, SendTimeoutError, Sender};
use cudarc::{
    driver::{
        CudaContext, CudaFunction, CudaSlice, CudaStream, DriverError, LaunchConfig, PushKernelArg,
    },
    nvrtc::{result as nvrtc_result, sys as nvrtc_sys, Ptx},
};

use crate::backend::{
    AssignmentSemantics, BackendCapabilities, BackendEvent, BackendExecutionModel,
    BackendInstanceId, BackendTelemetry, BenchBackend, DeadlineSupport, MiningSolution, NonceChunk,
    PowBackend, PreemptionGranularity, WorkAssignment, WorkTemplate,
};
use crate::types::hash_meets_target;

const BACKEND_NAME: &str = "nvidia";
const CUDA_KERNEL_SRC: &str = include_str!("nvidia_kernel.cu");
const MAX_RECOMMENDED_LANES: usize = 16;
const CMD_CHANNEL_CAPACITY: usize = 256;
const EVENT_SEND_WAIT: Duration = Duration::from_millis(5);
const KERNEL_THREADS: u32 = 128;
const ARGON2_VERSION_V13: u32 = 0x13;
const ARGON2_ALGORITHM_ID: u32 = 2; // Argon2id

#[derive(Debug, Clone)]
struct NvidiaDeviceInfo {
    index: u32,
    name: String,
    memory_total_mib: u64,
    memory_free_mib: Option<u64>,
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
    error_emitted: AtomicBool,
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
            error_emitted: AtomicBool::new(false),
        }
    }
}

struct NvidiaWorker {
    tx: Sender<WorkerCommand>,
    handle: JoinHandle<()>,
}

enum WorkerCommand {
    Assign(WorkAssignment),
    Cancel(Sender<Result<()>>),
    Fence(Sender<Result<()>>),
    Stop,
}

struct ActiveAssignment {
    work: WorkAssignment,
    next_nonce: u64,
    remaining: u64,
    hashes_done: u64,
    started_at: Instant,
}

impl ActiveAssignment {
    fn new(work: WorkAssignment) -> Self {
        Self {
            next_nonce: work.nonce_chunk.start_nonce,
            remaining: work.nonce_chunk.nonce_count,
            hashes_done: 0,
            started_at: Instant::now(),
            work,
        }
    }
}

struct CudaArgon2Engine {
    stream: Arc<CudaStream>,
    kernel: CudaFunction,
    m_blocks: u32,
    m_cost_kib: u32,
    t_cost: u32,
    max_lanes: usize,
    lane_memory: CudaSlice<u64>,
    seed_blocks: CudaSlice<u64>,
    last_blocks: CudaSlice<u64>,
    hashes_done: CudaSlice<u64>,
    host_seed_words: Vec<u64>,
    host_last_words: Vec<u64>,
    host_hashes: Vec<[u8; POW_OUTPUT_LEN]>,
}

impl CudaArgon2Engine {
    fn new(device_index: u32, memory_total_mib: u64, memory_free_mib: Option<u64>) -> Result<Self> {
        let ctx = CudaContext::new(device_index as usize).map_err(|err| {
            anyhow!("failed to open CUDA context on device {device_index}: {err:?}")
        })?;
        let stream = ctx.default_stream();

        let (cc_major, cc_minor) = ctx
            .compute_capability()
            .map_err(|err| anyhow!("failed to query compute capability: {err:?}"))?;

        let nvrtc_options = vec![
            "--std=c++14".to_string(),
            "--extra-device-vectorization".to_string(),
            "--restrict".to_string(),
            "--use_fast_math".to_string(),
            "--ftz=true".to_string(),
            "--fmad=true".to_string(),
            "--maxrregcount=128".to_string(),
            format!("--gpu-architecture=sm_{}{}", cc_major, cc_minor),
        ];
        let cubin =
            compile_cubin_with_nvrtc(CUDA_KERNEL_SRC, "seine_argon2id_fill.cu", &nvrtc_options)
                .map_err(|err| anyhow!("failed to compile CUDA CUBIN with NVRTC: {err:#}"))?;
        let module = ctx
            .load_module(Ptx::from_binary(cubin))
            .map_err(|err| anyhow!("failed to load CUDA module: {err:?}"))?;
        let kernel = module
            .load_function("argon2id_fill_kernel")
            .map_err(|err| anyhow!("failed to load CUDA kernel function: {err:?}"))?;

        let params = pow_params()
            .map_err(|err| anyhow!("invalid Argon2 parameters for CUDA backend: {err}"))?;
        let m_blocks = params.block_count() as u32;
        let m_cost_kib = params.m_cost();
        let t_cost = params.t_cost();

        let lane_bytes = CPU_LANE_MEMORY_BYTES.max(u64::from(m_blocks) * 1024);
        let memory_budget_mib = memory_free_mib.unwrap_or(memory_total_mib).max(1);
        let memory_budget_bytes = memory_budget_mib.saturating_mul(1024 * 1024);
        let usable_bytes = memory_budget_bytes.saturating_mul(85) / 100;
        let by_memory = usize::try_from((usable_bytes / lane_bytes).max(1)).unwrap_or(1);
        let max_lanes = by_memory.max(1).min(MAX_RECOMMENDED_LANES);

        let mut selected = None;
        for lanes in (1..=max_lanes).rev() {
            match try_allocate_cuda_buffers(&stream, m_blocks, lanes) {
                Ok(buffers) => {
                    selected = Some((lanes, buffers));
                    break;
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

        let (max_lanes, (lane_memory, seed_blocks, last_blocks, hashes_done)) = selected
            .ok_or_else(|| anyhow!("failed to allocate CUDA buffers for any lane count"))?;

        Ok(Self {
            stream,
            kernel,
            m_blocks,
            m_cost_kib,
            t_cost,
            max_lanes,
            lane_memory,
            seed_blocks,
            last_blocks,
            hashes_done,
            host_seed_words: vec![0u64; max_lanes * 256],
            host_last_words: vec![0u64; max_lanes * 128],
            host_hashes: vec![[0u8; POW_OUTPUT_LEN]; max_lanes],
        })
    }

    fn max_lanes(&self) -> usize {
        self.max_lanes
    }

    fn hash_at(&self, idx: usize) -> &[u8; POW_OUTPUT_LEN] {
        &self.host_hashes[idx]
    }

    fn run_fill_batch(&mut self, header_base: &[u8], nonces: &[u64]) -> Result<usize> {
        if header_base.len() != POW_HEADER_BASE_LEN {
            bail!(
                "invalid header base length: expected {} bytes, got {}",
                POW_HEADER_BASE_LEN,
                header_base.len()
            );
        }

        if nonces.is_empty() {
            return Ok(0);
        }

        if nonces.len() > self.max_lanes {
            bail!(
                "batch size {} exceeds configured CUDA lanes {}",
                nonces.len(),
                self.max_lanes
            );
        }

        for (idx, nonce) in nonces.iter().copied().enumerate() {
            let start = idx * 256;
            let end = start + 256;
            build_seed_blocks_for_nonce(
                header_base,
                nonce,
                self.m_cost_kib,
                self.t_cost,
                &mut self.host_seed_words[start..end],
            )?;
        }

        let lanes = nonces.len();
        let seed_words = lanes * 256;
        {
            let mut seed_view = self
                .seed_blocks
                .try_slice_mut(0..seed_words)
                .ok_or_else(|| anyhow!("failed to slice CUDA seed buffer"))?;
            self.stream
                .memcpy_htod(&self.host_seed_words[..seed_words], &mut seed_view)
                .map_err(cuda_driver_err)?;
        }

        {
            let mut hashes_view = self
                .hashes_done
                .try_slice_mut(0..1)
                .ok_or_else(|| anyhow!("failed to slice CUDA hash counter"))?;
            self.stream
                .memcpy_htod(&[0u64], &mut hashes_view)
                .map_err(cuda_driver_err)?;
        }

        let lanes_u32 = u32::try_from(lanes).map_err(|_| anyhow!("lane count overflow"))?;
        let cfg = LaunchConfig {
            grid_dim: (lanes_u32.div_ceil(KERNEL_THREADS), 1, 1),
            block_dim: (KERNEL_THREADS, 1, 1),
            shared_mem_bytes: 0,
        };

        unsafe {
            let mut launch = self.stream.launch_builder(&self.kernel);
            launch
                .arg(&self.seed_blocks)
                .arg(&lanes_u32)
                .arg(&self.m_blocks)
                .arg(&self.t_cost)
                .arg(&mut self.lane_memory)
                .arg(&mut self.last_blocks)
                .arg(&mut self.hashes_done);
            launch.launch(cfg).map_err(cuda_driver_err)?;
        }

        self.stream.synchronize().map_err(cuda_driver_err)?;

        let last_words = lanes * 128;
        {
            let last_view = self
                .last_blocks
                .try_slice(0..last_words)
                .ok_or_else(|| anyhow!("failed to slice CUDA output buffer"))?;
            self.stream
                .memcpy_dtoh(&last_view, &mut self.host_last_words[..last_words])
                .map_err(cuda_driver_err)?;
        }

        let hashes_done = {
            let mut done = [0u64; 1];
            let done_view = self
                .hashes_done
                .try_slice(0..1)
                .ok_or_else(|| anyhow!("failed to slice CUDA hash counter readback"))?;
            self.stream
                .memcpy_dtoh(&done_view, &mut done)
                .map_err(cuda_driver_err)?;
            usize::try_from(done[0]).unwrap_or(lanes)
        }
        .min(lanes);

        for idx in 0..hashes_done {
            finalize_lane_hash(
                &self.host_last_words[idx * 128..(idx + 1) * 128],
                &mut self.host_hashes[idx],
            )?;
        }

        Ok(hashes_done)
    }
}

pub struct NvidiaBackend {
    instance_id: Arc<AtomicU64>,
    requested_device_index: Option<u32>,
    resolved_device: RwLock<Option<NvidiaDeviceInfo>>,
    shared: Arc<NvidiaShared>,
    worker: Mutex<Option<NvidiaWorker>>,
    max_lanes: AtomicUsize,
}

impl NvidiaBackend {
    pub fn new(device_index: Option<u32>) -> Self {
        Self {
            instance_id: Arc::new(AtomicU64::new(0)),
            requested_device_index: device_index,
            resolved_device: RwLock::new(None),
            shared: Arc::new(NvidiaShared::new()),
            worker: Mutex::new(None),
            max_lanes: AtomicUsize::new(1),
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

    fn worker_tx(&self) -> Result<Sender<WorkerCommand>> {
        let guard = self
            .worker
            .lock()
            .map_err(|_| anyhow!("nvidia worker lock poisoned"))?;
        let Some(worker) = guard.as_ref() else {
            bail!("NVIDIA backend is not started");
        };
        Ok(worker.tx.clone())
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
        let mut engine = CudaArgon2Engine::new(
            selected.index,
            selected.memory_total_mib,
            selected.memory_free_mib,
        )
        .with_context(|| {
            format!(
                "failed to initialize CUDA engine on NVIDIA device {} ({})",
                selected.index, selected.name
            )
        })?;

        self.max_lanes
            .store(engine.max_lanes().max(1), Ordering::Release);

        self.shared.error_emitted.store(false, Ordering::Release);
        self.shared.hashes.store(0, Ordering::Release);
        self.shared.active_lanes.store(0, Ordering::Release);
        self.shared.pending_work.store(0, Ordering::Release);
        self.shared
            .inflight_assignment_hashes
            .store(0, Ordering::Release);
        if let Ok(mut slot) = self.shared.inflight_assignment_started_at.lock() {
            *slot = None;
        }

        let (tx, rx) = bounded::<WorkerCommand>(CMD_CHANNEL_CAPACITY);
        let shared = Arc::clone(&self.shared);
        let instance_id = Arc::clone(&self.instance_id);
        let handle = thread::Builder::new()
            .name(format!(
                "seine-nvidia-worker-{}",
                self.instance_id.load(Ordering::Acquire)
            ))
            .spawn(move || worker_loop(&mut engine, rx, shared, instance_id))
            .map_err(|err| anyhow!("failed to spawn nvidia worker thread: {err}"))?;

        let mut guard = self
            .worker
            .lock()
            .map_err(|_| anyhow!("nvidia worker lock poisoned"))?;
        *guard = Some(NvidiaWorker { tx, handle });

        Ok(())
    }

    fn stop(&self) {
        let worker = match self.worker.lock() {
            Ok(mut slot) => slot.take(),
            Err(_) => None,
        };
        if let Some(worker) = worker {
            let _ = worker.tx.send(WorkerCommand::Stop);
            let _ = worker.handle.join();
        }

        self.shared.active_lanes.store(0, Ordering::Release);
        self.shared.pending_work.store(0, Ordering::Release);
        self.shared
            .inflight_assignment_hashes
            .store(0, Ordering::Release);
        if let Ok(mut slot) = self.shared.inflight_assignment_started_at.lock() {
            *slot = None;
        }
    }

    fn assign_work(&self, work: WorkAssignment) -> Result<()> {
        self.worker_tx()?
            .send(WorkerCommand::Assign(work))
            .map_err(|_| anyhow!("nvidia worker channel closed while assigning work"))
    }

    fn assign_work_batch(&self, work: &[WorkAssignment]) -> Result<()> {
        let Some(collapsed) = collapse_assignment_batch(work)? else {
            return Ok(());
        };
        self.assign_work(collapsed)
    }

    fn assign_work_batch_with_deadline(
        &self,
        work: &[WorkAssignment],
        deadline: Instant,
    ) -> Result<()> {
        let Some(collapsed) = collapse_assignment_batch(work)? else {
            return Ok(());
        };
        self.assign_work_with_deadline(&collapsed, deadline)
    }

    fn supports_assignment_batching(&self) -> bool {
        true
    }

    fn cancel_work(&self) -> Result<()> {
        let tx = self.worker_tx()?;
        let (ack_tx, ack_rx) = bounded::<Result<()>>(1);
        tx.send(WorkerCommand::Cancel(ack_tx))
            .map_err(|_| anyhow!("nvidia worker channel closed while cancelling"))?;
        ack_rx
            .recv()
            .map_err(|_| anyhow!("nvidia worker did not acknowledge cancellation"))??;
        Ok(())
    }

    fn request_timeout_interrupt(&self) -> Result<()> {
        self.cancel_work()
    }

    fn fence(&self) -> Result<()> {
        let tx = self.worker_tx()?;
        let (ack_tx, ack_rx) = bounded::<Result<()>>(1);
        tx.send(WorkerCommand::Fence(ack_tx))
            .map_err(|_| anyhow!("nvidia worker channel closed while fencing"))?;
        ack_rx
            .recv()
            .map_err(|_| anyhow!("nvidia worker did not acknowledge fence"))??;
        Ok(())
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
        PreemptionGranularity::Hashes(self.lanes() as u64)
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            preferred_iters_per_lane: Some(1 << 20),
            preferred_allocation_iters_per_lane: Some(1 << 21),
            preferred_hash_poll_interval: Some(Duration::from_millis(25)),
            preferred_assignment_timeout: None,
            preferred_control_timeout: None,
            preferred_assignment_timeout_strikes: None,
            preferred_worker_queue_depth: Some(32),
            max_inflight_assignments: 32,
            deadline_support: DeadlineSupport::Cooperative,
            assignment_semantics: AssignmentSemantics::Replace,
            execution_model: BackendExecutionModel::Blocking,
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
        let mut engine = CudaArgon2Engine::new(
            selected.index,
            selected.memory_total_mib,
            selected.memory_free_mib,
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
        let mut nonces = vec![0u64; engine.max_lanes().max(1)];

        while Instant::now() < deadline && !shutdown.load(Ordering::Acquire) {
            let lanes = engine.max_lanes().max(1);
            if nonces.len() < lanes {
                nonces.resize(lanes, 0);
            }
            for nonce in nonces.iter_mut().take(lanes) {
                *nonce = nonce_cursor;
                nonce_cursor = nonce_cursor.wrapping_add(1);
            }

            let done = engine.run_fill_batch(&header, &nonces[..lanes])?;
            total = total.saturating_add(done as u64);
        }

        Ok(total)
    }
}

fn worker_loop(
    engine: &mut CudaArgon2Engine,
    rx: Receiver<WorkerCommand>,
    shared: Arc<NvidiaShared>,
    instance_id: Arc<AtomicU64>,
) {
    let mut active: Option<ActiveAssignment> = None;
    let mut fence_waiters: Vec<Sender<Result<()>>> = Vec::new();
    let mut running = true;
    let mut nonce_buf = vec![0u64; engine.max_lanes().max(1)];

    while running {
        if active.is_none() {
            match rx.recv() {
                Ok(cmd) => {
                    running = handle_worker_command(cmd, &mut active, &mut fence_waiters, &shared);
                }
                Err(_) => break,
            }
        }

        while let Ok(cmd) = rx.try_recv() {
            running = handle_worker_command(cmd, &mut active, &mut fence_waiters, &shared);
            if !running {
                break;
            }
        }

        if !running {
            break;
        }

        let Some(current) = active.as_mut() else {
            continue;
        };

        if current.remaining == 0 || Instant::now() >= current.work.template.stop_at {
            finalize_active_assignment(&shared, current);
            active = None;
            drain_fence_waiters(&mut fence_waiters, Ok(()));
            continue;
        }

        let lanes = engine.max_lanes().min(current.remaining as usize).max(1);
        if nonce_buf.len() < lanes {
            nonce_buf.resize(lanes, 0);
        }
        for (idx, nonce) in nonce_buf.iter_mut().take(lanes).enumerate() {
            *nonce = current.next_nonce.wrapping_add(idx as u64);
        }

        shared.active_lanes.store(lanes as u64, Ordering::Release);

        let done = match engine.run_fill_batch(
            current.work.template.header_base.as_ref(),
            &nonce_buf[..lanes],
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

        if done == 0 {
            finalize_active_assignment(&shared, current);
            active = None;
            drain_fence_waiters(&mut fence_waiters, Ok(()));
            continue;
        }

        current.hashes_done = current.hashes_done.saturating_add(done as u64);
        current.next_nonce = current.next_nonce.wrapping_add(done as u64);
        current.remaining = current.remaining.saturating_sub(done as u64);

        shared.hashes.fetch_add(done as u64, Ordering::Relaxed);
        shared
            .inflight_assignment_hashes
            .store(current.hashes_done, Ordering::Release);

        let mut solved_nonce = None;
        for idx in 0..done {
            if hash_meets_target(engine.hash_at(idx), &current.work.template.target) {
                solved_nonce = Some(nonce_buf[idx]);
                break;
            }
        }

        if let Some(nonce) = solved_nonce {
            send_backend_event(
                &shared,
                BackendEvent::Solution(MiningSolution {
                    epoch: current.work.template.epoch,
                    nonce,
                    backend_id: instance_id.load(Ordering::Acquire),
                    backend: BACKEND_NAME,
                }),
            );
            finalize_active_assignment(&shared, current);
            active = None;
            drain_fence_waiters(&mut fence_waiters, Ok(()));
            continue;
        }
    }

    if let Some(active_assignment) = active.as_ref() {
        finalize_active_assignment(&shared, active_assignment);
    } else {
        shared.active_lanes.store(0, Ordering::Release);
        shared.pending_work.store(0, Ordering::Release);
        shared
            .inflight_assignment_hashes
            .store(0, Ordering::Release);
        if let Ok(mut slot) = shared.inflight_assignment_started_at.lock() {
            *slot = None;
        }
    }

    drain_fence_waiters(
        &mut fence_waiters,
        Err(anyhow!("nvidia worker stopped before fence completion")),
    );
}

fn handle_worker_command(
    cmd: WorkerCommand,
    active: &mut Option<ActiveAssignment>,
    fence_waiters: &mut Vec<Sender<Result<()>>>,
    shared: &NvidiaShared,
) -> bool {
    match cmd {
        WorkerCommand::Assign(work) => {
            if let Some(current) = active.as_ref() {
                finalize_active_assignment(shared, current);
            }
            shared.error_emitted.store(false, Ordering::Release);
            shared.pending_work.store(1, Ordering::Release);
            shared.active_lanes.store(0, Ordering::Release);
            shared
                .inflight_assignment_hashes
                .store(0, Ordering::Release);
            if let Ok(mut slot) = shared.inflight_assignment_started_at.lock() {
                *slot = Some(Instant::now());
            }
            *active = Some(ActiveAssignment::new(work));
            true
        }
        WorkerCommand::Cancel(ack) => {
            if let Some(current) = active.as_ref() {
                finalize_active_assignment(shared, current);
                *active = None;
            }
            let _ = ack.send(Ok(()));
            if active.is_none() {
                drain_fence_waiters(fence_waiters, Ok(()));
            }
            true
        }
        WorkerCommand::Fence(ack) => {
            if active.is_none() {
                let _ = ack.send(Ok(()));
            } else {
                fence_waiters.push(ack);
            }
            true
        }
        WorkerCommand::Stop => false,
    }
}

fn finalize_active_assignment(shared: &NvidiaShared, active: &ActiveAssignment) {
    shared.active_lanes.store(0, Ordering::Release);
    shared.pending_work.store(0, Ordering::Release);
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

fn nvrtc_log_to_string(raw: &[c_char]) -> String {
    if raw.is_empty() {
        return String::new();
    }
    unsafe { CStr::from_ptr(raw.as_ptr()) }
        .to_string_lossy()
        .trim()
        .to_string()
}

fn build_seed_blocks_for_nonce(
    header_base: &[u8],
    nonce: u64,
    m_cost_kib: u32,
    t_cost: u32,
    out_words: &mut [u64],
) -> Result<()> {
    if out_words.len() < 256 {
        bail!("seed output buffer too small: expected at least 256 words");
    }

    let mut initial = Blake2b512::new();
    initial.update(1u32.to_le_bytes());
    initial.update((POW_OUTPUT_LEN as u32).to_le_bytes());
    initial.update(m_cost_kib.to_le_bytes());
    initial.update(t_cost.to_le_bytes());
    initial.update(ARGON2_VERSION_V13.to_le_bytes());
    initial.update(ARGON2_ALGORITHM_ID.to_le_bytes());
    initial.update((std::mem::size_of::<u64>() as u32).to_le_bytes());
    initial.update(nonce.to_le_bytes());
    initial.update((header_base.len() as u32).to_le_bytes());
    initial.update(header_base);
    initial.update(0u32.to_le_bytes());
    initial.update(0u32.to_le_bytes());

    let h0 = initial.finalize();

    let lane_index = 0u32.to_le_bytes();
    for (seed_idx, words_chunk) in out_words.chunks_exact_mut(128).take(2).enumerate() {
        let block_index = (seed_idx as u32).to_le_bytes();
        let mut block_bytes = [0u8; 1024];
        blake2b_long(&[h0.as_ref(), &block_index, &lane_index], &mut block_bytes)?;

        for (chunk, dst) in block_bytes.chunks_exact(8).zip(words_chunk.iter_mut()) {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(chunk);
            *dst = u64::from_le_bytes(bytes);
        }
    }

    Ok(())
}

fn finalize_lane_hash(last_block_words: &[u64], out: &mut [u8; POW_OUTPUT_LEN]) -> Result<()> {
    if last_block_words.len() < 128 {
        bail!(
            "last block word buffer too small: expected at least 128 words, got {}",
            last_block_words.len()
        );
    }

    let mut block_bytes = [0u8; 1024];
    for (dst, word) in block_bytes
        .chunks_exact_mut(8)
        .zip(last_block_words.iter().take(128))
    {
        dst.copy_from_slice(&word.to_le_bytes());
    }

    blake2b_long(&[&block_bytes], out)
}

fn blake2b_long(inputs: &[&[u8]], out: &mut [u8]) -> Result<()> {
    if out.is_empty() {
        bail!("blake2b_long output buffer is empty");
    }

    let len_bytes = u32::try_from(out.len())
        .map(|v| v.to_le_bytes())
        .map_err(|_| anyhow!("blake2b_long output length overflow"))?;

    if out.len() <= Blake2b512::output_size() {
        let mut digest = Blake2bVar::new(out.len())
            .map_err(|_| anyhow!("invalid variable Blake2b output length"))?;

        digest::Update::update(&mut digest, &len_bytes);
        for input in inputs {
            digest::Update::update(&mut digest, input);
        }

        digest
            .finalize_variable(out)
            .map_err(|_| anyhow!("failed to finalize Blake2b variable output"))?;
        return Ok(());
    }

    let half_hash_len = Blake2b512::output_size() / 2;
    let mut digest = Blake2b512::new();
    digest.update(len_bytes);
    for input in inputs {
        digest.update(input);
    }

    let mut last_output = digest.finalize();
    out[..half_hash_len].copy_from_slice(&last_output[..half_hash_len]);

    let mut counter = half_hash_len;
    while out.len().saturating_sub(counter) > Blake2b512::output_size() {
        last_output = Blake2b512::digest(last_output);
        let end = counter + half_hash_len;
        out[counter..end].copy_from_slice(&last_output[..half_hash_len]);
        counter = end;
    }

    let last_block_size = out.len().saturating_sub(counter);
    let mut final_digest = Blake2bVar::new(last_block_size)
        .map_err(|_| anyhow!("invalid final Blake2b output length"))?;
    digest::Update::update(&mut final_digest, &last_output);
    final_digest
        .finalize_variable(&mut out[counter..])
        .map_err(|_| anyhow!("failed to finalize tail Blake2b output"))?;

    Ok(())
}

fn collapse_assignment_batch(work: &[WorkAssignment]) -> Result<Option<WorkAssignment>> {
    let Some(first) = work.first() else {
        return Ok(None);
    };

    if work.len() == 1 {
        return Ok(Some(first.clone()));
    }

    let mut expected_start = first
        .nonce_chunk
        .start_nonce
        .wrapping_add(first.nonce_chunk.nonce_count);
    let mut nonce_count = first.nonce_chunk.nonce_count;

    for (idx, assignment) in work.iter().enumerate().skip(1) {
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

        nonce_count = nonce_count
            .checked_add(assignment.nonce_chunk.nonce_count)
            .ok_or_else(|| anyhow!("assignment batch nonce_count overflow"))?;
        expected_start = expected_start.wrapping_add(assignment.nonce_chunk.nonce_count);
    }

    Ok(Some(WorkAssignment {
        template: Arc::clone(&first.template),
        nonce_chunk: NonceChunk {
            start_nonce: first.nonce_chunk.start_nonce,
            nonce_count,
        },
    }))
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

fn try_allocate_cuda_buffers(
    stream: &Arc<CudaStream>,
    m_blocks: u32,
    lanes: usize,
) -> std::result::Result<
    (
        CudaSlice<u64>,
        CudaSlice<u64>,
        CudaSlice<u64>,
        CudaSlice<u64>,
    ),
    DriverError,
> {
    let lane_words_u64 = u64::from(m_blocks).saturating_mul(128);
    let total_lane_words_u64 = lane_words_u64.saturating_mul(lanes as u64);
    let total_lane_words = usize::try_from(total_lane_words_u64).unwrap_or(usize::MAX);

    let lane_memory = unsafe { stream.alloc::<u64>(total_lane_words) }?;
    let seed_blocks = unsafe { stream.alloc::<u64>(lanes.saturating_mul(256)) }?;
    let last_blocks = unsafe { stream.alloc::<u64>(lanes.saturating_mul(128)) }?;
    let hashes_done = stream.alloc_zeros::<u64>(1)?;
    Ok((lane_memory, seed_blocks, last_blocks, hashes_done))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn collapse_assignment_batch_merges_contiguous_chunks() {
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

        let collapsed = collapse_assignment_batch(&assignments)
            .expect("batch collapse should succeed")
            .expect("batch should not be empty");
        assert_eq!(collapsed.nonce_chunk.start_nonce, 100);
        assert_eq!(collapsed.nonce_chunk.nonce_count, 10);
    }

    #[test]
    fn collapse_assignment_batch_rejects_non_contiguous_chunks() {
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

        let err =
            collapse_assignment_batch(&assignments).expect_err("non-contiguous chunks should fail");
        assert!(format!("{err:#}").contains("not contiguous"));
    }

    #[test]
    fn blake2b_long_matches_expected_len_behavior() {
        let mut short = [0u8; 32];
        blake2b_long(&[b"seine"], &mut short).expect("short output should hash");

        let mut long = [0u8; 96];
        blake2b_long(&[b"seine"], &mut long).expect("long output should hash");

        assert_ne!(short, [0u8; 32]);
        assert_ne!(long, [0u8; 96]);
        assert_ne!(&long[..32], &short);
    }
}
