use crate::backend::cpu::CpuBackend;
use crate::backend::PowBackend;
use crate::config::{cpu_autotune_capacity, Config};
use anyhow::{bail, Result};
use crossbeam_channel::Sender;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct ThreadChangeRequest {
    pub run_id: u64,
    pub threads: usize,
    /// Current runtime RAM/worker bound, which may differ from the supervisor's
    /// original configuration after automatic recovery or startup fallback.
    pub max_threads: usize,
    /// Receives the dashboard notice: a rejection, or the applied count.
    pub reply: Option<Sender<String>>,
    pub quit_requested: Arc<AtomicBool>,
    pub memory: Option<Arc<ThreadMemoryGuard>>,
    /// Applies the count to the running workers when the run supports it.
    pub live: Option<Sender<LiveThreadChange>>,
}

/// A worker count chosen in the dashboard, applied by the CPU autotune controller
/// to the running workers.
#[derive(Debug)]
pub(crate) struct LiveThreadChange {
    pub threads: usize,
    pub reply: Option<Sender<String>>,
}

#[derive(Clone)]
pub(crate) struct ThreadControl {
    pub run_id: u64,
    pub sender: Sender<ThreadChangeRequest>,
    pub max_threads: usize,
    pub current_threads: usize,
    pub quit_requested: Arc<AtomicBool>,
    pub memory: Option<Arc<ThreadMemoryGuard>>,
    pub live: Option<Sender<LiveThreadChange>>,
}

/// Read actual CPU arena ownership at admission time, never infer it from the
/// requested thread count (workers may be parked, warming, or quarantined).
pub(crate) struct ThreadMemoryGuard {
    cfg: Config,
    backends: Vec<Arc<CpuBackend>>,
}

impl std::fmt::Debug for ThreadMemoryGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadMemoryGuard").finish_non_exhaustive()
    }
}

impl ThreadMemoryGuard {
    pub(crate) fn new(cfg: &Config, backends: Vec<Arc<CpuBackend>>) -> Self {
        Self {
            cfg: cfg.clone(),
            backends,
        }
    }

    pub(crate) fn validate(&self, requested: usize) -> Result<()> {
        let current = self
            .backends
            .iter()
            .map(|b| b.enabled_lanes())
            .min()
            .unwrap_or(0);
        let owned = self
            .backends
            .iter()
            .map(|b| b.adaptive_snapshot().resident_bytes)
            .sum();
        let (_, available) = cpu_autotune_capacity(&self.cfg, owned);
        validate_change(requested, current, available)
    }
}

pub(crate) fn validate_headroom(requested: usize, available: usize) -> Result<()> {
    if requested > available {
        bail!("Not enough RAM for {requested} threads; safe limit: {available}. Each needs 2 GiB plus reserve.");
    }
    Ok(())
}

fn validate_change(requested: usize, current: usize, available: usize) -> Result<()> {
    // Always permit shedding workers, including explicit above-auto counts.
    if requested < current {
        return Ok(());
    }
    validate_headroom(requested, available)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_but_unallocated_workers_do_not_create_headroom() {
        let mut cfg = crate::miner::tests::test_config();
        cfg.cpu_memory_reserve_percent = Some(100);
        let backend = Arc::new(CpuBackend::new(4, crate::config::CpuAffinityMode::Off));
        let guard = ThreadMemoryGuard::new(&cfg, vec![backend]);
        assert!(guard.validate(5).is_err());
        assert!(guard.validate(3).is_ok());
    }

    #[test]
    fn memory_drop_blocks_increase_and_recovery_allows_retry() {
        assert!(validate_change(5, 4, 4)
            .unwrap_err()
            .to_string()
            .contains("Not enough RAM"));
        assert!(validate_change(5, 4, 5).is_ok());
        assert!(validate_change(5, 4, 0).is_err());
    }

    #[test]
    fn memory_pressure_does_not_prevent_shedding_workers() {
        assert!(validate_change(3, 4, 0).is_ok());
        // After teardown the replacement must fit without counting old arenas.
        assert!(validate_headroom(3, 2).is_err());
        assert!(validate_headroom(3, 3).is_ok());
    }
}
