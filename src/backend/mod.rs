use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use crossbeam_channel::Sender;

pub mod cpu;
#[cfg(feature = "nvidia")]
pub mod nvidia;
#[cfg(not(feature = "nvidia"))]
pub mod nvidia {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    use anyhow::{bail, Result};
    use crossbeam_channel::Sender;

    use super::{
        AssignmentSemantics, BackendCapabilities, BackendEvent, BackendExecutionModel,
        BackendInstanceId, BenchBackend, DeadlineSupport, PowBackend, PreemptionGranularity,
        WorkAssignment,
    };

    pub struct NvidiaBackend {
        _instance_id: AtomicU64,
        _device_index: Option<u32>,
    }

    impl NvidiaBackend {
        pub fn new(device_index: Option<u32>) -> Self {
            Self {
                _instance_id: AtomicU64::new(0),
                _device_index: device_index,
            }
        }
    }

    impl PowBackend for NvidiaBackend {
        fn name(&self) -> &'static str {
            "nvidia"
        }

        fn lanes(&self) -> usize {
            1
        }

        fn set_instance_id(&self, id: BackendInstanceId) {
            self._instance_id.store(id, Ordering::Release);
        }

        fn set_event_sink(&self, _sink: Sender<BackendEvent>) {}

        fn start(&self) -> Result<()> {
            if let Some(device_index) = self._device_index {
                bail!(
                    "NVIDIA backend device {} is disabled in this build (rebuild with --features nvidia)",
                    device_index
                )
            } else {
                bail!("NVIDIA backend is disabled in this build (rebuild with --features nvidia)")
            }
        }

        fn stop(&self) {}

        fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
            Ok(())
        }

        fn cancel_work(&self) -> Result<()> {
            Ok(())
        }

        fn fence(&self) -> Result<()> {
            Ok(())
        }

        fn preemption_granularity(&self) -> PreemptionGranularity {
            PreemptionGranularity::Unknown
        }

        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities {
                preferred_iters_per_lane: Some(1),
                preferred_allocation_iters_per_lane: None,
                preferred_hash_poll_interval: Some(Duration::from_millis(50)),
                preferred_assignment_timeout: None,
                preferred_control_timeout: None,
                preferred_assignment_timeout_strikes: None,
                max_inflight_assignments: 1,
                deadline_support: DeadlineSupport::BestEffort,
                assignment_semantics: AssignmentSemantics::Replace,
                execution_model: BackendExecutionModel::Blocking,
                nonblocking_poll_min: Some(Duration::from_micros(100)),
                nonblocking_poll_max: Some(Duration::from_millis(5)),
            }
        }

        fn bench_backend(&self) -> Option<&dyn BenchBackend> {
            Some(self)
        }
    }

    impl BenchBackend for NvidiaBackend {
        fn kernel_bench(&self, _seconds: u64, _shutdown: &AtomicBool) -> Result<u64> {
            bail!("kernel benchmark is not implemented for nvidia backend")
        }
    }
}

pub const WORK_ID_MAX: u64 = (1u64 << 63) - 1;
pub type BackendInstanceId = u64;

#[derive(Debug, Clone)]
pub struct WorkTemplate {
    pub work_id: u64,
    pub epoch: u64,
    pub header_base: Arc<[u8]>,
    pub target: [u8; 32],
    pub stop_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub struct NonceChunk {
    pub start_nonce: u64,
    pub nonce_count: u64,
}

#[derive(Debug, Clone)]
pub struct WorkAssignment {
    pub template: Arc<WorkTemplate>,
    pub nonce_chunk: NonceChunk,
}

#[derive(Debug, Clone)]
pub struct MiningSolution {
    pub epoch: u64,
    pub nonce: u64,
    pub backend_id: BackendInstanceId,
    pub backend: &'static str,
}

#[derive(Debug, Clone)]
pub enum BackendEvent {
    Solution(MiningSolution),
    Error {
        backend_id: BackendInstanceId,
        backend: &'static str,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BackendTelemetry {
    pub active_lanes: u64,
    pub pending_work: u64,
    pub dropped_events: u64,
    pub completed_assignments: u64,
    pub completed_assignment_hashes: u64,
    pub completed_assignment_micros: u64,
    pub inflight_assignment_hashes: u64,
    pub inflight_assignment_micros: u64,
    /// Assignment dispatches that timed out before enqueueing in backend workers.
    pub assignment_enqueue_timeouts: u64,
    /// Assignment dispatches that timed out while executing in backend workers.
    pub assignment_execution_timeouts: u64,
    /// Control dispatches (cancel/fence) that timed out before enqueueing.
    pub control_enqueue_timeouts: u64,
    /// Control dispatches (cancel/fence) that timed out while executing.
    pub control_execution_timeouts: u64,
    /// Current consecutive assignment-timeout strikes tracked by runtime.
    pub assignment_timeout_strikes: u32,
    /// Number of assignment enqueue latency samples observed by runtime workers.
    pub assignment_enqueue_latency_samples: u64,
    /// Approximate assignment enqueue p95 latency, in microseconds.
    pub assignment_enqueue_latency_p95_micros: u64,
    /// Maximum assignment enqueue latency, in microseconds.
    pub assignment_enqueue_latency_max_micros: u64,
    /// Number of assignment execution latency samples observed by runtime workers.
    pub assignment_execution_latency_samples: u64,
    /// Approximate assignment execution p95 latency, in microseconds.
    pub assignment_execution_latency_p95_micros: u64,
    /// Maximum assignment execution latency, in microseconds.
    pub assignment_execution_latency_max_micros: u64,
    /// Number of control enqueue latency samples observed by runtime workers.
    pub control_enqueue_latency_samples: u64,
    /// Approximate control enqueue p95 latency, in microseconds.
    pub control_enqueue_latency_p95_micros: u64,
    /// Maximum control enqueue latency, in microseconds.
    pub control_enqueue_latency_max_micros: u64,
    /// Number of control execution latency samples observed by runtime workers.
    pub control_execution_latency_samples: u64,
    /// Approximate control execution p95 latency, in microseconds.
    pub control_execution_latency_p95_micros: u64,
    /// Maximum control execution latency, in microseconds.
    pub control_execution_latency_max_micros: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DeadlineSupport {
    /// Backend operations are expected to obey deadline-aware APIs directly.
    Cooperative,
    /// Runtime can enforce watchdog timeouts, but backend calls may complete late.
    BestEffort,
}

impl DeadlineSupport {
    pub fn describe(self) -> &'static str {
        match self {
            Self::Cooperative => "cooperative",
            Self::BestEffort => "best-effort",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AssignmentSemantics {
    /// Each assignment call replaces any previously queued/in-flight work.
    Replace,
    /// Assignment calls append to backend-local queues until cancel/fence.
    Append,
}

impl AssignmentSemantics {
    pub fn describe(self) -> &'static str {
        match self {
            Self::Replace => "replace",
            Self::Append => "append",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BackendExecutionModel {
    /// Backend dispatch/control calls may block until completion.
    Blocking,
    /// Backend dispatch/control calls use true non-blocking progress hooks.
    Nonblocking,
}

impl BackendExecutionModel {
    pub fn describe(self) -> &'static str {
        match self {
            Self::Blocking => "blocking",
            Self::Nonblocking => "nonblocking",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BackendCapabilities {
    /// Preferred iterations per lane for one dispatched assignment chunk.
    /// Runtime treats this as a dispatch-granularity hint, not a hard contract.
    pub preferred_iters_per_lane: Option<u64>,
    /// Preferred iterations per lane for sizing per-backend nonce reservations.
    /// Runtime uses this to shape allocation windows independently from dispatch chunking.
    pub preferred_allocation_iters_per_lane: Option<u64>,
    /// Preferred backend hash-poll cadence for telemetry/accounting.
    /// Runtime clamps this against operator-configured polling limits.
    pub preferred_hash_poll_interval: Option<Duration>,
    /// Preferred backend-specific assignment-dispatch timeout.
    pub preferred_assignment_timeout: Option<Duration>,
    /// Preferred backend-specific cancel/fence timeout.
    pub preferred_control_timeout: Option<Duration>,
    /// Preferred backend-specific assignment timeout strike threshold.
    pub preferred_assignment_timeout_strikes: Option<u32>,
    /// Maximum number of in-flight assignments this backend can queue efficiently.
    /// Runtime may split one reservation into this many chunks for dispatch.
    pub max_inflight_assignments: u32,
    /// Contract for deadline-aware backend calls.
    pub deadline_support: DeadlineSupport,
    /// Assignment queueing contract for repeated assign calls.
    pub assignment_semantics: AssignmentSemantics,
    /// Dispatch/control execution model consumed by the runtime executor.
    pub execution_model: BackendExecutionModel,
    /// Preferred non-blocking poll backoff floor used by the backend executor.
    pub nonblocking_poll_min: Option<Duration>,
    /// Preferred non-blocking poll backoff ceiling used by the backend executor.
    pub nonblocking_poll_max: Option<Duration>,
}

impl Default for BackendCapabilities {
    fn default() -> Self {
        Self {
            preferred_iters_per_lane: None,
            preferred_allocation_iters_per_lane: None,
            preferred_hash_poll_interval: None,
            preferred_assignment_timeout: None,
            preferred_control_timeout: None,
            preferred_assignment_timeout_strikes: None,
            max_inflight_assignments: 1,
            deadline_support: DeadlineSupport::BestEffort,
            assignment_semantics: AssignmentSemantics::Replace,
            execution_model: BackendExecutionModel::Blocking,
            nonblocking_poll_min: None,
            nonblocking_poll_max: None,
        }
    }
}

pub fn normalize_backend_capabilities(
    mut capabilities: BackendCapabilities,
    supports_assignment_batching: bool,
) -> BackendCapabilities {
    capabilities.max_inflight_assignments = capabilities.max_inflight_assignments.max(1);
    if capabilities.max_inflight_assignments > 1 && !supports_assignment_batching {
        capabilities.max_inflight_assignments = 1;
    }

    capabilities.preferred_assignment_timeout = capabilities
        .preferred_assignment_timeout
        .map(|timeout| timeout.max(Duration::from_millis(1)));
    capabilities.preferred_control_timeout = capabilities
        .preferred_control_timeout
        .map(|timeout| timeout.max(Duration::from_millis(1)));
    capabilities.preferred_assignment_timeout_strikes = capabilities
        .preferred_assignment_timeout_strikes
        .map(|strikes| strikes.max(1));

    capabilities.nonblocking_poll_min = capabilities
        .nonblocking_poll_min
        .map(|min_poll| min_poll.max(Duration::from_micros(10)));
    capabilities.nonblocking_poll_max = capabilities
        .nonblocking_poll_max
        .map(|max_poll| max_poll.max(Duration::from_micros(10)));
    if let (Some(min_poll), Some(max_poll)) = (
        capabilities.nonblocking_poll_min,
        capabilities.nonblocking_poll_max,
    ) {
        if max_poll < min_poll {
            capabilities.nonblocking_poll_max = Some(min_poll);
        }
    }

    capabilities
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_capabilities_clamps_non_batching_inflight_depth() {
        let normalized = normalize_backend_capabilities(
            BackendCapabilities {
                max_inflight_assignments: 8,
                ..BackendCapabilities::default()
            },
            false,
        );

        assert_eq!(normalized.max_inflight_assignments, 1);
    }

    #[test]
    fn normalize_capabilities_preserves_batching_inflight_depth() {
        let normalized = normalize_backend_capabilities(
            BackendCapabilities {
                max_inflight_assignments: 8,
                ..BackendCapabilities::default()
            },
            true,
        );

        assert_eq!(normalized.max_inflight_assignments, 8);
    }

    #[test]
    fn normalize_capabilities_clamps_nonblocking_poll_bounds() {
        let normalized = normalize_backend_capabilities(
            BackendCapabilities {
                max_inflight_assignments: 1,
                nonblocking_poll_min: Some(Duration::from_micros(1)),
                nonblocking_poll_max: Some(Duration::from_micros(5)),
                ..BackendCapabilities::default()
            },
            true,
        );

        assert_eq!(
            normalized.nonblocking_poll_min,
            Some(Duration::from_micros(10))
        );
        assert_eq!(
            normalized.nonblocking_poll_max,
            Some(Duration::from_micros(10))
        );
    }

    #[test]
    fn normalize_capabilities_reorders_inverted_nonblocking_poll_bounds() {
        let normalized = normalize_backend_capabilities(
            BackendCapabilities {
                max_inflight_assignments: 1,
                nonblocking_poll_min: Some(Duration::from_millis(3)),
                nonblocking_poll_max: Some(Duration::from_millis(1)),
                ..BackendCapabilities::default()
            },
            true,
        );

        assert_eq!(
            normalized.nonblocking_poll_min,
            Some(Duration::from_millis(3))
        );
        assert_eq!(
            normalized.nonblocking_poll_max,
            Some(Duration::from_millis(3))
        );
    }
}

pub trait BenchBackend: Send {
    fn kernel_bench(&self, seconds: u64, shutdown: &AtomicBool) -> Result<u64>;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PreemptionGranularity {
    /// Backend checks cancel/work-generation boundaries at least every N hashes.
    Hashes(u64),
    /// Backend-specific or currently unknown granularity.
    Unknown,
}

impl PreemptionGranularity {
    pub fn describe(self) -> String {
        match self {
            Self::Hashes(1) => "per-hash".to_string(),
            Self::Hashes(n) => format!("every {n} hashes"),
            Self::Unknown => "unknown".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[allow(dead_code)]
pub enum BackendCallStatus {
    Complete,
    Pending,
}

pub trait PowBackend: Send + Sync {
    fn name(&self) -> &'static str;

    fn lanes(&self) -> usize;

    fn set_instance_id(&self, id: BackendInstanceId);

    fn set_event_sink(&self, sink: Sender<BackendEvent>);

    fn start(&self) -> Result<()>;

    fn stop(&self);

    /// Assign exactly one work chunk.
    ///
    /// Contract: a call must supersede previously assigned work for this backend
    /// (or produce equivalent stale-safe behavior keyed by `work.template.work_id`).
    fn assign_work(&self, work: WorkAssignment) -> Result<()>;

    /// Assign exactly one work chunk before a soft deadline.
    ///
    /// Default behavior executes the assignment call then reports timeout if the
    /// backend did not return before the deadline.
    #[allow(dead_code)]
    fn assign_work_with_deadline(&self, work: &WorkAssignment, deadline: Instant) -> Result<()> {
        self.assign_work(work.clone())?;
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

    /// Whether this backend can accept true multi-chunk assignment batches.
    ///
    /// If false, runtime clamps `max_inflight_assignments` to `1` and the default
    /// `assign_work_batch` implementation rejects batches longer than one chunk.
    fn supports_assignment_batching(&self) -> bool {
        false
    }

    /// Whether `*_nonblocking` hooks implement true non-blocking progress.
    fn supports_true_nonblocking(&self) -> bool {
        false
    }

    /// Assign one or more work chunks to the backend as one assignment generation.
    ///
    /// Runtime expects one call to represent one generation replacement boundary.
    /// Backends that can queue multiple chunks internally should override this and
    /// return `true` from `supports_assignment_batching`.
    fn assign_work_batch(&self, work: &[WorkAssignment]) -> Result<()> {
        match work {
            [] => Ok(()),
            [single] => self.assign_work(single.clone()),
            _ => bail!(
                "backend {} does not support batched assignment dispatch",
                self.name()
            ),
        }
    }

    /// Assign one or more work chunks before a soft deadline.
    ///
    /// Default behavior executes the assignment call then reports timeout if the
    /// backend did not return before the deadline.
    #[allow(dead_code)]
    fn assign_work_batch_with_deadline(
        &self,
        work: &[WorkAssignment],
        deadline: Instant,
    ) -> Result<()> {
        match work {
            [] => Ok(()),
            [single] => self.assign_work_with_deadline(single, deadline),
            _ => {
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
        }
    }

    /// Optional non-blocking dispatch hook for future high-throughput backends.
    ///
    /// Backends can return `Pending` to indicate the request has not been accepted yet
    /// (for example queue-pressure on persistent GPU schedulers). Runtime will retry
    /// until the task deadline elapses.
    fn assign_work_batch_nonblocking(&self, work: &[WorkAssignment]) -> Result<BackendCallStatus> {
        self.assign_work_batch(work)?;
        Ok(BackendCallStatus::Complete)
    }

    /// Optional wait hook used after non-blocking calls report `Pending`.
    ///
    /// Backends can override this to block on backend-native readiness signals
    /// (for example condition variables, CUDA events, or queue doorbells) instead
    /// of host-side sleep polling.
    fn wait_for_nonblocking_progress(&self, wait_for: Duration) -> Result<()> {
        thread::sleep(wait_for);
        Ok(())
    }

    /// Optional non-blocking cancel hook for future high-throughput backends.
    fn cancel_work_nonblocking(&self) -> Result<BackendCallStatus> {
        self.cancel_work()?;
        Ok(BackendCallStatus::Complete)
    }

    /// Optional non-blocking fence hook for future high-throughput backends.
    fn fence_nonblocking(&self) -> Result<BackendCallStatus> {
        self.fence()?;
        Ok(BackendCallStatus::Complete)
    }

    /// Request the backend to stop processing the current assignment.
    fn cancel_work(&self) -> Result<()> {
        Err(anyhow!(
            "backend {} does not implement cancel_work()",
            self.name()
        ))
    }

    /// Request an immediate, non-blocking timeout interrupt.
    ///
    /// Runtime may call this from watchdog paths when assignment/control calls
    /// overrun their deadline. Implementations should avoid long blocking work.
    fn request_timeout_interrupt(&self) -> Result<()> {
        self.cancel_work()
    }

    /// Request backend cancellation before a soft deadline.
    ///
    /// Default behavior executes cancel then reports timeout if call returns late.
    #[allow(dead_code)]
    fn cancel_work_with_deadline(&self, deadline: Instant) -> Result<()> {
        self.cancel_work()?;
        if Instant::now() > deadline {
            return Err(anyhow!(
                "cancel call exceeded deadline by {}ms",
                Instant::now()
                    .saturating_duration_since(deadline)
                    .as_millis()
            ));
        }
        Ok(())
    }

    /// Wait until all backend workers have observed the most recent control action.
    fn fence(&self) -> Result<()> {
        Err(anyhow!(
            "backend {} does not implement fence()",
            self.name()
        ))
    }

    /// Wait for backend quiesce/fence before a soft deadline.
    ///
    /// Default behavior executes fence then reports timeout if call returns late.
    #[allow(dead_code)]
    fn fence_with_deadline(&self, deadline: Instant) -> Result<()> {
        self.fence()?;
        if Instant::now() > deadline {
            return Err(anyhow!(
                "fence call exceeded deadline by {}ms",
                Instant::now()
                    .saturating_duration_since(deadline)
                    .as_millis()
            ));
        }
        Ok(())
    }

    /// Return and reset hashes completed since the previous call.
    fn take_hashes(&self) -> u64 {
        0
    }

    /// Return and reset backend-local telemetry counters since the previous call.
    fn take_telemetry(&self) -> BackendTelemetry {
        BackendTelemetry::default()
    }

    fn preemption_granularity(&self) -> PreemptionGranularity {
        PreemptionGranularity::Unknown
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::default()
    }

    fn bench_backend(&self) -> Option<&dyn BenchBackend> {
        None
    }
}
