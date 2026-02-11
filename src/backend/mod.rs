use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Result};

pub mod cpu;
pub mod nvidia;

#[derive(Debug, Clone)]
pub struct MiningWork {
    pub epoch: u64,
    pub header_base: Arc<[u8]>,
    pub target: [u8; 32],
    pub start_nonce: u64,
    pub lane_offset: u64,
    pub global_stride: u64,
    pub stop_at: Instant,
}

#[derive(Debug, Clone)]
pub struct MiningSolution {
    pub epoch: u64,
    pub nonce: u64,
    pub backend: String,
}

pub trait PowBackend: Send {
    fn name(&self) -> &'static str;

    fn lanes(&self) -> usize;

    fn start(&mut self) -> Result<()>;

    fn stop(&mut self);

    fn set_work(&self, work: MiningWork) -> Result<()>;

    fn try_recv_solution(&self) -> Option<MiningSolution>;

    fn drain_hashes(&self) -> u64;

    fn drain_errors(&self) -> u64 {
        0
    }

    fn kernel_bench(&self, _seconds: u64, _shutdown: &AtomicBool) -> Result<u64> {
        bail!(
            "kernel benchmark is not implemented for backend '{}'",
            self.name()
        )
    }
}
