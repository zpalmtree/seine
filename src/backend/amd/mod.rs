//! AMD GPU backend (HIP/ROCm).
//!
//! Mirrors the NVIDIA backend architecture: a persistent worker thread owns a
//! `HipArgon2Engine` with one 2 GiB Argon2id VRAM arena per lane, assignments
//! are queued with replace semantics, and cancel/fence obey the same
//! cooperative deadline contract the NVIDIA backend advertises (a device-side
//! cancel flag checked at fixed block intervals inside the fill kernel plus a
//! control-priority worker channel).
//!
//! Autotune status: deliberately a stub. The engine derives lane count from
//! free VRAM with allocation/probe backoff (like the NVIDIA lane probe) and
//! runs a fixed launch depth of 1 hash per lane per launch. There is no
//! regcap sweep, no launch-depth sweep, and no persisted tuning cache yet:
//! the AMD tuning surface (waves-per-SIMD occupancy, launch depth, cadence of
//! cancel checks, scheduling experiments) is intentionally deferred until the
//! kernel passes the CPU-differential bring-up tests on real RDNA3 hardware.
//! See docs/AMD_BRINGUP.md.

mod engine;
mod hip_ffi;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use blocknet_pow_spec::{CPU_LANE_MEMORY_BYTES, POW_HEADER_BASE_LEN, POW_OUTPUT_LEN};
use crossbeam_channel::{
    bounded, Receiver, RecvTimeoutError, SendTimeoutError, Sender, TryRecvError, TrySendError,
};

use crate::backend::{
    AssignmentSemantics, BackendCallStatus, BackendCapabilities, BackendEvent,
    BackendExecutionModel, BackendInstanceId, BackendTelemetry, BenchBackend, DeadlineSupport,
    KernelBenchSample, MiningSolution, PowBackend, PreemptionGranularity, WorkAssignment,
    WorkTemplate,
};

use engine::{HipArgon2Engine, HipInterruptController};

pub(crate) use engine::{query_amd_devices, AmdDeviceInfo, REQUIRED_WAVEFRONT_SIZE};

const BACKEND_NAME: &str = "amd";
const ASSIGN_CHANNEL_CAPACITY: usize = 256;
const CONTROL_CHANNEL_CAPACITY: usize = 32;
const EVENT_SEND_WAIT: Duration = Duration::from_millis(5);
const DEFAULT_DISPATCH_ITERS_PER_LANE: u64 = 1 << 21;
const DEFAULT_ALLOCATION_ITERS_PER_LANE: u64 = 1 << 21;
/// Fixed launch depth for bring-up: one hash per lane per launch keeps
/// cancel/fence preemption as fine as the kernel's in-fill checkpoints allow.
/// Depth sweeps are part of the deferred AMD tuning surface.
const DEFAULT_AMD_HASHES_PER_LAUNCH_PER_LANE: u32 = 1;

#[derive(Debug, Clone, Copy)]
pub struct AmdBackendTuningOptions {
    pub max_lanes_override: Option<usize>,
    pub hashes_per_launch_per_lane: u32,
}

impl Default for AmdBackendTuningOptions {
    fn default() -> Self {
        Self {
            max_lanes_override: None,
            hashes_per_launch_per_lane: DEFAULT_AMD_HASHES_PER_LAUNCH_PER_LANE,
        }
    }
}

struct AmdShared {
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
    cancel_requested: AtomicBool,
    error_emitted: AtomicBool,
}

impl AmdShared {
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
            cancel_requested: AtomicBool::new(false),
            error_emitted: AtomicBool::new(false),
        }
    }
}

struct AmdWorker {
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

pub struct AmdBackend {
    instance_id: Arc<AtomicU64>,
    requested_device_index: Option<u32>,
    tuning_options: AmdBackendTuningOptions,
    resolved_device: RwLock<Option<AmdDeviceInfo>>,
    shared: Arc<AmdShared>,
    nonblocking_control: Mutex<NonblockingControlState>,
    interrupt_controller: RwLock<Option<HipInterruptController>>,
    worker: Mutex<Option<AmdWorker>>,
    max_lanes: AtomicUsize,
    max_hashes_per_launch_per_lane: AtomicU64,
}

impl AmdBackend {
    pub fn new(device_index: Option<u32>, tuning_options: AmdBackendTuningOptions) -> Self {
        Self {
            instance_id: Arc::new(AtomicU64::new(0)),
            requested_device_index: device_index,
            tuning_options: AmdBackendTuningOptions {
                max_lanes_override: tuning_options.max_lanes_override.filter(|v| *v > 0),
                hashes_per_launch_per_lane: tuning_options.hashes_per_launch_per_lane.max(1),
            },
            resolved_device: RwLock::new(None),
            shared: Arc::new(AmdShared::new()),
            nonblocking_control: Mutex::new(NonblockingControlState::default()),
            interrupt_controller: RwLock::new(None),
            worker: Mutex::new(None),
            max_lanes: AtomicUsize::new(1),
            max_hashes_per_launch_per_lane: AtomicU64::new(u64::from(
                tuning_options.hashes_per_launch_per_lane.max(1),
            )),
        }
    }

    fn validate_or_select_device(&self) -> Result<AmdDeviceInfo> {
        let devices = query_amd_devices()?;
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
                        "requested AMD device index {} was not found; available indices: [{}]",
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
                "AMD device {} ({}) reports {} MiB VRAM; at least {} MiB is required",
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

    fn build_engine(&self, selected: &AmdDeviceInfo) -> Result<HipArgon2Engine> {
        HipArgon2Engine::new(
            selected,
            self.tuning_options.max_lanes_override,
            self.tuning_options.hashes_per_launch_per_lane,
        )
        .with_context(|| {
            format!(
                "failed to initialize HIP engine on AMD device {} ({}, {})",
                selected.index, selected.name, selected.gcn_arch_name
            )
        })
    }

    fn worker_assignment_tx(&self) -> Result<Sender<WorkerCommand>> {
        let guard = self
            .worker
            .lock()
            .map_err(|_| anyhow!("amd worker lock poisoned"))?;
        let Some(worker) = guard.as_ref() else {
            bail!("AMD backend is not started");
        };
        Ok(worker.assignment_tx.clone())
    }

    fn worker_control_tx(&self) -> Result<Sender<WorkerCommand>> {
        let guard = self
            .worker
            .lock()
            .map_err(|_| anyhow!("amd worker lock poisoned"))?;
        let Some(worker) = guard.as_ref() else {
            bail!("AMD backend is not started");
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
        tx.send(cmd)
            .map_err(|_| anyhow!("amd worker channel closed while issuing {}", kind.label()))?;
        ack_rx
            .recv()
            .map_err(|_| anyhow!("amd worker did not acknowledge {}", kind.label()))??;
        Ok(())
    }

    fn control_nonblocking(&self, kind: PendingControlKind) -> Result<BackendCallStatus> {
        let tx = self.worker_control_tx()?;
        let mut state = self
            .nonblocking_control
            .lock()
            .map_err(|_| anyhow!("amd nonblocking control lock poisoned"))?;

        if let Some(pending) = state.pending.as_mut() {
            match pending.ack_rx.try_recv() {
                Ok(result) => {
                    state.pending = None;
                    result?;
                }
                Err(TryRecvError::Empty) => {
                    return Ok(BackendCallStatus::Pending);
                }
                Err(TryRecvError::Disconnected) => {
                    let pending_kind = pending.kind;
                    state.pending = None;
                    bail!(
                        "amd worker disconnected while waiting for {} acknowledgement",
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
                                "amd worker disconnected while waiting for {} acknowledgement",
                                pending_kind.label()
                            );
                        }
                    }
                }
                Ok(BackendCallStatus::Pending)
            }
            Err(TrySendError::Full(_)) => Ok(BackendCallStatus::Pending),
            Err(TrySendError::Disconnected(_)) => {
                bail!("amd worker channel closed while issuing {}", kind.label())
            }
        }
    }
}

impl Drop for AmdBackend {
    fn drop(&mut self) {
        self.stop();
    }
}

impl PowBackend for AmdBackend {
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
                .map_err(|_| anyhow!("amd worker lock poisoned"))?;
            if guard.is_some() {
                return Ok(());
            }
        }

        let selected = self.validate_or_select_device()?;
        let mut engine = self.build_engine(&selected)?;

        let discovered_lanes = engine.max_lanes().max(1);
        let discovered_depth = engine.max_hashes_per_launch_per_lane().max(1) as u64;
        let interrupt_controller = engine.interrupt_controller();
        self.max_lanes.store(discovered_lanes, Ordering::Release);
        self.max_hashes_per_launch_per_lane
            .store(discovered_depth, Ordering::Release);

        self.shared.error_emitted.store(false, Ordering::Release);
        self.shared.hashes.store(0, Ordering::Release);
        self.shared.active_lanes.store(0, Ordering::Release);
        self.shared.pending_work.store(0, Ordering::Release);
        self.shared
            .inflight_assignment_hashes
            .store(0, Ordering::Release);
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
        let handle = thread::Builder::new()
            .name(format!(
                "seine-amd-worker-{}",
                self.instance_id.load(Ordering::Acquire)
            ))
            .spawn(move || worker_loop(&mut engine, assignment_rx, control_rx, shared, instance_id))
            .map_err(|err| anyhow!("failed to spawn amd worker thread: {err}"))?;

        let mut guard = self
            .worker
            .lock()
            .map_err(|_| anyhow!("amd worker lock poisoned"))?;
        *guard = Some(AmdWorker {
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
        self.shared.cancel_requested.store(false, Ordering::Release);
        if let Ok(mut slot) = self.shared.inflight_assignment_started_at.lock() {
            *slot = None;
        }
    }

    fn assign_work(&self, work: WorkAssignment) -> Result<()> {
        self.worker_assignment_tx()?
            .send(WorkerCommand::Assign(work))
            .map_err(|_| anyhow!("amd worker channel closed while assigning work"))
    }

    fn assign_work_batch(&self, work: &[WorkAssignment]) -> Result<()> {
        let mut batch = normalize_assignment_batch(work)?;
        match batch.len() {
            0 => Ok(()),
            1 => self.assign_work(batch.pop().expect("single-item batch should be present")),
            _ => self
                .worker_assignment_tx()?
                .send(WorkerCommand::AssignBatch(batch))
                .map_err(|_| anyhow!("amd worker channel closed while assigning work")),
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
                bail!("amd worker channel closed while assigning work")
            }
        }
    }

    fn wait_for_nonblocking_progress(&self, wait_for: Duration) -> Result<()> {
        let wait_for = wait_for.max(Duration::from_micros(10));
        let mut state = self
            .nonblocking_control
            .lock()
            .map_err(|_| anyhow!("amd nonblocking control lock poisoned"))?;
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
                    "amd worker disconnected while waiting for {} acknowledgement",
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
            .max_hashes_per_launch_per_lane
            .load(Ordering::Acquire)
            .max(1);
        PreemptionGranularity::Hashes((self.lanes() as u64).saturating_mul(per_launch).max(1))
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            preferred_iters_per_lane: Some(DEFAULT_DISPATCH_ITERS_PER_LANE),
            preferred_allocation_iters_per_lane: Some(DEFAULT_ALLOCATION_ITERS_PER_LANE),
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

impl BenchBackend for AmdBackend {
    fn kernel_bench(&self, seconds: u64, shutdown: &AtomicBool) -> Result<u64> {
        let selected = self.validate_or_select_device()?;
        let mut engine = self.build_engine(&selected)?;
        engine.bind_thread()?;

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
        let mut engine = self.build_engine(&selected)?;
        engine.bind_thread()?;

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

fn worker_loop(
    engine: &mut HipArgon2Engine,
    assignment_rx: Receiver<WorkerCommand>,
    control_rx: Receiver<WorkerCommand>,
    shared: Arc<AmdShared>,
    instance_id: Arc<AtomicU64>,
) {
    if let Err(err) = engine.bind_thread() {
        emit_worker_error(
            &shared,
            &instance_id,
            format!("failed to bind AMD worker thread to device: {err:#}"),
        );
        return;
    }

    let mut active: Option<ActiveAssignment> = None;
    let mut queued: VecDeque<WorkAssignment> = VecDeque::new();
    let mut fence_waiters: Vec<Sender<Result<()>>> = Vec::new();
    let mut assignment_open = true;
    let mut control_open = true;
    let mut running = true;
    let mut nonce_buf = vec![0u64; engine.max_hashes_per_launch()];

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
                            );
                        }
                        Err(_) => control_open = false,
                    }
                }
                continue;
            }
        }

        // Drain any queued commands before launching the next batch so
        // cancel/fence preemption is honored between launches as well as at
        // the kernel's in-fill cancel checkpoints.
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

        let max_hashes_for_launch = engine.max_hashes_per_launch().max(1);
        let hashes_per_batch = max_hashes_for_launch.min(current.remaining as usize).max(1);
        if nonce_buf.len() < hashes_per_batch {
            nonce_buf.resize(hashes_per_batch, 0);
        }
        for (idx, nonce) in nonce_buf.iter_mut().take(hashes_per_batch).enumerate() {
            *nonce = current.next_nonce.wrapping_add(idx as u64);
        }

        let active_lanes = engine.max_lanes().min(hashes_per_batch).max(1);
        shared
            .active_lanes
            .store(active_lanes as u64, Ordering::Release);

        let target_snapshot = current.work.template.target_snapshot();
        let network_target = current.work.template.network_target();
        let done = match engine.run_fill_batch_preserving_candidate(
            current.work.template.header_base.as_ref(),
            &nonce_buf[..hashes_per_batch],
            Some(&target_snapshot.target),
            network_target.as_ref(),
        ) {
            Ok(done) => done,
            Err(err) => {
                emit_worker_error(
                    &shared,
                    &instance_id,
                    format!("HIP batch execution failed: {err:#}"),
                );
                break;
            }
        };

        if done.hashes_done == 0 {
            let timeout_cancelled = shared.cancel_requested.swap(false, Ordering::AcqRel);
            let next_pending = if timeout_cancelled {
                0
            } else {
                queued.len() as u64
            };
            if timeout_cancelled {
                queued.clear();
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

        current.hashes_done = current.hashes_done.saturating_add(done.hashes_done as u64);
        current.next_nonce = current.next_nonce.wrapping_add(done.hashes_done as u64);
        current.remaining = current.remaining.saturating_sub(done.hashes_done as u64);

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
                    hash: done.solved_hash,
                    share_binding_id: target_snapshot.share_binding_id,
                    backend_id: instance_id.load(Ordering::Acquire),
                    backend: BACKEND_NAME,
                }),
            );
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
        Err(anyhow!("amd worker stopped before fence completion")),
    );
}

fn handle_worker_command(
    cmd: WorkerCommand,
    active: &mut Option<ActiveAssignment>,
    queued: &mut VecDeque<WorkAssignment>,
    fence_waiters: &mut Vec<Sender<Result<()>>>,
    shared: &AmdShared,
) -> bool {
    match cmd {
        WorkerCommand::Assign(work) => {
            shared.cancel_requested.store(false, Ordering::Release);
            replace_assignment_queue(shared, active, queued, vec![work]);
            true
        }
        WorkerCommand::AssignBatch(work) => {
            shared.cancel_requested.store(false, Ordering::Release);
            replace_assignment_queue(shared, active, queued, work);
            true
        }
        WorkerCommand::Cancel(ack) => {
            shared.cancel_requested.store(false, Ordering::Release);
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
                fence_waiters.push(ack);
            }
            true
        }
        WorkerCommand::Stop => false,
    }
}

fn replace_assignment_queue(
    shared: &AmdShared,
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
    shared: &AmdShared,
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

fn clear_worker_pending_state(shared: &AmdShared) {
    shared.active_lanes.store(0, Ordering::Release);
    shared.pending_work.store(0, Ordering::Release);
    shared
        .inflight_assignment_hashes
        .store(0, Ordering::Release);
    if let Ok(mut slot) = shared.inflight_assignment_started_at.lock() {
        *slot = None;
    }
}

fn finalize_active_assignment(shared: &AmdShared, active: &ActiveAssignment, pending_after: u64) {
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

fn send_backend_event(shared: &AmdShared, event: BackendEvent) {
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

fn emit_worker_error(shared: &AmdShared, instance_id: &AtomicU64, message: String) {
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

// Duplicated from the NVIDIA backend rather than shared: the modules are
// independently feature-gated (each has a stub replacement when disabled)
// and keeping them separate avoids cross-branch churn while both backends
// are under active iteration.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::NonceChunk;
    use blocknet_pow_spec::pow_params;

    fn test_template(work_id: u64) -> Arc<WorkTemplate> {
        Arc::new(WorkTemplate {
            work_id,
            epoch: 7,
            header_base: Arc::<[u8]>::from(vec![1u8; POW_HEADER_BASE_LEN]),
            target: [0xff; 32],
            dynamic_share_target: None,
            pause_on_solution: true,
            stop_at: Instant::now() + Duration::from_secs(30),
        })
    }

    #[test]
    fn backend_start_without_rocm_fails_with_clear_error() {
        // Host-side contract: with no ROCm runtime installed the backend must
        // fail start() gracefully (the runtime then quarantines it). On a
        // ROCm box this exercises real device selection instead.
        let backend = AmdBackend::new(None, AmdBackendTuningOptions::default());
        match backend.start() {
            Ok(()) => backend.stop(),
            Err(err) => {
                let message = format!("{err:#}");
                assert!(
                    message.contains("ROCm runtime not found (libamdhip64.so)")
                        || message.contains("ROCm hipRTC compiler not found")
                        || message.contains("no AMD devices")
                        || message.contains("wave32"),
                    "unexpected start() error without ROCm: {message}"
                );
            }
        }
    }

    #[test]
    fn control_calls_fail_cleanly_when_not_started() {
        let backend = AmdBackend::new(None, AmdBackendTuningOptions::default());
        let err = backend
            .cancel_work()
            .expect_err("cancel before start must fail");
        assert!(format!("{err:#}").contains("not started"));
        let err = backend.fence().expect_err("fence before start must fail");
        assert!(format!("{err:#}").contains("not started"));
        // Timeout interrupts are best-effort and must not error pre-start.
        backend
            .request_timeout_interrupt()
            .expect("timeout interrupt should be a no-op before start");
    }

    #[test]
    fn capabilities_advertise_cooperative_nonblocking_replace_contract() {
        let backend = AmdBackend::new(None, AmdBackendTuningOptions::default());
        let capabilities = backend.capabilities();
        assert_eq!(capabilities.deadline_support, DeadlineSupport::Cooperative);
        assert_eq!(
            capabilities.assignment_semantics,
            AssignmentSemantics::Replace
        );
        assert_eq!(
            capabilities.execution_model,
            BackendExecutionModel::Nonblocking
        );
        assert!(backend.supports_assignment_batching());
        assert!(backend.supports_true_nonblocking());
        assert!(capabilities.max_inflight_assignments > 1);
    }

    #[test]
    fn preemption_granularity_scales_with_lanes_and_depth() {
        let backend = AmdBackend::new(None, AmdBackendTuningOptions::default());
        backend.max_lanes.store(11, Ordering::Release);
        backend
            .max_hashes_per_launch_per_lane
            .store(1, Ordering::Release);
        assert_eq!(
            backend.preemption_granularity(),
            PreemptionGranularity::Hashes(11)
        );
        backend
            .max_hashes_per_launch_per_lane
            .store(2, Ordering::Release);
        assert_eq!(
            backend.preemption_granularity(),
            PreemptionGranularity::Hashes(22)
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
    fn normalize_assignment_batch_rejects_mixed_templates() {
        let assignments = vec![
            WorkAssignment {
                template: test_template(11),
                nonce_chunk: NonceChunk {
                    start_nonce: 100,
                    nonce_count: 4,
                },
            },
            WorkAssignment {
                template: test_template(12),
                nonce_chunk: NonceChunk {
                    start_nonce: 104,
                    nonce_count: 6,
                },
            },
        ];

        let err = normalize_assignment_batch(&assignments)
            .expect_err("mixed-template batches should fail");
        assert!(format!("{err:#}").contains("different template"));
    }

    #[test]
    #[ignore = "requires AMD GPU + ROCm runtime; run on the ROCm bring-up box"]
    fn gpu_solution_target_bracket_matches_cpu_reference() {
        use blocknet_pow_kernel::ReusablePowContext;

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

        let devices = query_amd_devices()
            .expect("failed to query AMD devices; verify ROCm and amdgpu driver are installed");
        assert!(!devices.is_empty(), "no AMD GPU detected");
        let selected = devices[0].clone();

        let mut engine = HipArgon2Engine::new(&selected, Some(1), 1)
            .expect("failed to initialize HIP engine for GPU validity differential test");
        engine.bind_thread().expect("bind should succeed");

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
                .expect("HIP batch execution should succeed for equal target");
            assert_eq!(hits_equal.hashes_done, 1);
            assert_eq!(
                hits_equal.solved_nonce,
                Some(nonce),
                "GPU did not report expected solution for nonce {} at equal target",
                nonce
            );

            let misses_tighter = engine
                .run_fill_batch(&header_base, &[nonce], Some(&tighter_target))
                .expect("HIP batch execution should succeed for tighter target");
            assert_eq!(misses_tighter.hashes_done, 1);
            assert_eq!(
                misses_tighter.solved_nonce, None,
                "GPU reported solution for nonce {} at tighter target; GPU hash differs from CPU reference",
                nonce
            );
        }
    }
}
