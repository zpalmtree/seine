use std::sync::atomic::{AtomicBool, AtomicU64};

use anyhow::{bail, Result};

use crate::backend::{MiningJob, MiningSolution, PowBackend};

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

    fn mine(
        &self,
        _job: &MiningJob,
        _shutdown: &AtomicBool,
        _total_hashes: &AtomicU64,
    ) -> Result<Option<MiningSolution>> {
        bail!("NVIDIA backend is scaffolded but not implemented yet. Use --backend cpu for now.")
    }
}
