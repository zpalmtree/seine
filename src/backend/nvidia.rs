use std::sync::atomic::AtomicBool;

use anyhow::{bail, Result};

use crate::backend::{MiningSolution, MiningWork, PowBackend};

pub struct NvidiaBackend;

impl NvidiaBackend {
    pub fn new() -> Self {
        Self
    }
}

impl PowBackend for NvidiaBackend {
    fn name(&self) -> &'static str {
        "nvidia"
    }

    fn lanes(&self) -> usize {
        1
    }

    fn start(&mut self) -> Result<()> {
        bail!("NVIDIA backend is scaffolded but not implemented yet")
    }

    fn stop(&mut self) {}

    fn set_work(&self, _work: MiningWork) -> Result<()> {
        Ok(())
    }

    fn try_recv_solution(&self) -> Option<MiningSolution> {
        None
    }

    fn drain_hashes(&self) -> u64 {
        0
    }

    fn kernel_bench(&self, _seconds: u64, _shutdown: &AtomicBool) -> Result<u64> {
        bail!("kernel benchmark is not implemented for nvidia backend")
    }
}
