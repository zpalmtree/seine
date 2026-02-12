use std::sync::atomic::AtomicBool;

use anyhow::{bail, Result};
use crossbeam_channel::Sender;

use crate::backend::{BackendEvent, PowBackend, WorkAssignment};

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

    fn set_event_sink(&mut self, _sink: Sender<BackendEvent>) {}

    fn start(&mut self) -> Result<()> {
        bail!("NVIDIA backend is scaffolded but not implemented yet")
    }

    fn stop(&mut self) {}

    fn assign_work(&self, _work: WorkAssignment) -> Result<()> {
        Ok(())
    }

    fn kernel_bench(&self, _seconds: u64, _shutdown: &AtomicBool) -> Result<u64> {
        bail!("kernel benchmark is not implemented for nvidia backend")
    }
}
