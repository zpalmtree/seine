use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use crossbeam_channel::Sender;

pub mod cpu;
pub mod nvidia;

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

    fn quiesce(&self) -> Result<()> {
        self.cancel_work()?;
        self.fence()
    }

    /// Return and reset hashes completed since the previous call.
    fn take_hashes(&self) -> u64 {
        0
    }

    fn preemption_granularity(&self) -> PreemptionGranularity {
        PreemptionGranularity::Unknown
    }

    fn bench_backend(&self) -> Option<&dyn BenchBackend> {
        None
    }
}
