use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use crossbeam_channel::Sender;

pub mod cpu;
#[cfg(feature = "nvidia")]
pub mod nvidia;
#[cfg(not(feature = "nvidia"))]
pub mod nvidia {
    use std::sync::atomic::AtomicBool;

    use anyhow::{bail, Result};
    use crossbeam_channel::Sender;

    use super::{
        BackendEvent, BackendInstanceId, BenchBackend, PowBackend, PreemptionGranularity,
        WorkAssignment,
    };

    pub struct NvidiaBackend {
        _instance_id: BackendInstanceId,
        _device_index: Option<u32>,
    }

    impl NvidiaBackend {
        pub fn new(device_index: Option<u32>) -> Self {
            Self {
                _instance_id: 0,
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

        fn set_instance_id(&mut self, id: BackendInstanceId) {
            self._instance_id = id;
        }

        fn set_event_sink(&mut self, _sink: Sender<BackendEvent>) {}

        fn start(&mut self) -> Result<()> {
            if let Some(device_index) = self._device_index {
                bail!(
                    "NVIDIA backend device {} is disabled in this build (rebuild with --features nvidia)",
                    device_index
                )
            } else {
                bail!("NVIDIA backend is disabled in this build (rebuild with --features nvidia)")
            }
        }

        fn stop(&mut self) {}

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
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BackendCapabilities {
    /// Preferred iterations per lane for one assignment chunk.
    /// Runtime treats this as a scheduling hint, not a hard contract.
    pub preferred_iters_per_lane: Option<u64>,
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

pub trait PowBackend: Send {
    fn name(&self) -> &'static str;

    fn lanes(&self) -> usize;

    fn set_instance_id(&mut self, id: BackendInstanceId);

    fn set_event_sink(&mut self, sink: Sender<BackendEvent>);

    fn start(&mut self) -> Result<()>;

    fn stop(&mut self);

    fn assign_work(&self, work: WorkAssignment) -> Result<()>;

    /// Request the backend to stop processing the current assignment.
    fn cancel_work(&self) -> Result<()> {
        Ok(())
    }

    /// Wait until all backend workers have observed the most recent control action.
    fn fence(&self) -> Result<()> {
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
