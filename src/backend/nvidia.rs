use std::sync::atomic::AtomicBool;

use anyhow::{bail, Result};
use crossbeam_channel::Sender;

use crate::backend::{
    BackendEvent, BackendInstanceId, BenchBackend, PowBackend, PreemptionGranularity,
    WorkAssignment,
};

pub struct NvidiaBackend {
    _instance_id: BackendInstanceId,
}

impl NvidiaBackend {
    pub fn new() -> Self {
        Self { _instance_id: 0 }
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
        bail!("NVIDIA backend is scaffolded but not implemented yet")
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
        // Placeholder until the CUDA worker model is implemented.
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
