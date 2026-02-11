use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

pub mod cpu;
pub mod nvidia;

#[derive(Debug, Clone)]
pub struct MiningJob {
    pub header_base: Arc<[u8]>,
    pub target: [u8; 32],
    pub start_nonce: u64,
    pub threads: usize,
    pub stop_at: Instant,
}

#[derive(Debug, Clone)]
pub struct MiningSolution {
    pub nonce: u64,
    pub hashes: u64,
    pub elapsed: Duration,
}

pub trait PowBackend: Send + Sync {
    fn name(&self) -> &'static str;

    fn mine(
        &self,
        job: &MiningJob,
        shutdown: &AtomicBool,
        total_hashes: &AtomicU64,
    ) -> Result<Option<MiningSolution>>;
}
