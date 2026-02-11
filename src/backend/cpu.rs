use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use argon2::{Algorithm, Argon2, Version};
use blocknet_pow_spec::{pow_params, POW_OUTPUT_LEN};

use crate::backend::{MiningSolution, MiningWork, PowBackend};
use crate::types::hash_meets_target;

const IDLE_SLEEP: Duration = Duration::from_millis(2);
const STALE_SLEEP: Duration = Duration::from_millis(1);

struct Shared {
    started: AtomicBool,
    shutdown: AtomicBool,
    current_epoch: AtomicU64,
    solved_epoch: AtomicU64,
    work: RwLock<Option<MiningWork>>,
    errors: AtomicU64,
    solution_tx: mpsc::Sender<MiningSolution>,
}

pub struct CpuBackend {
    threads: usize,
    shared: Arc<Shared>,
    thread_hashes: Arc<Vec<AtomicU64>>,
    last_hash_snapshot: Mutex<Vec<u64>>,
    worker_handles: Vec<JoinHandle<()>>,
    solution_rx: Mutex<mpsc::Receiver<MiningSolution>>,
}

impl CpuBackend {
    pub fn new(threads: usize) -> Self {
        let (solution_tx, solution_rx) = mpsc::channel();
        Self {
            threads,
            shared: Arc::new(Shared {
                started: AtomicBool::new(false),
                shutdown: AtomicBool::new(false),
                current_epoch: AtomicU64::new(0),
                solved_epoch: AtomicU64::new(0),
                work: RwLock::new(None),
                errors: AtomicU64::new(0),
                solution_tx,
            }),
            thread_hashes: Arc::new((0..threads.max(1)).map(|_| AtomicU64::new(0)).collect()),
            last_hash_snapshot: Mutex::new(vec![0u64; threads.max(1)]),
            worker_handles: Vec::new(),
            solution_rx: Mutex::new(solution_rx),
        }
    }
}

impl PowBackend for CpuBackend {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn lanes(&self) -> usize {
        self.threads.max(1)
    }

    fn start(&mut self) -> Result<()> {
        if self.shared.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        self.shared.shutdown.store(false, Ordering::SeqCst);
        self.shared.current_epoch.store(0, Ordering::SeqCst);
        self.shared.solved_epoch.store(0, Ordering::SeqCst);
        self.shared.errors.store(0, Ordering::Relaxed);

        for counter in self.thread_hashes.iter() {
            counter.store(0, Ordering::Relaxed);
        }
        if let Ok(mut last) = self.last_hash_snapshot.lock() {
            for value in last.iter_mut() {
                *value = 0;
            }
        }

        if let Ok(mut work) = self.shared.work.write() {
            *work = None;
        }

        if let Ok(rx) = self.solution_rx.lock() {
            while rx.try_recv().is_ok() {}
        }

        for thread_idx in 0..self.threads.max(1) {
            let shared = Arc::clone(&self.shared);
            let thread_hashes = Arc::clone(&self.thread_hashes);
            self.worker_handles.push(thread::spawn(move || {
                cpu_worker_loop(shared, thread_hashes, thread_idx)
            }));
        }

        Ok(())
    }

    fn stop(&mut self) {
        if !self.shared.started.swap(false, Ordering::SeqCst) {
            return;
        }

        self.shared.shutdown.store(true, Ordering::SeqCst);

        for handle in self.worker_handles.drain(..) {
            let _ = handle.join();
        }

        if let Ok(mut work) = self.shared.work.write() {
            *work = None;
        }

        self.shared.current_epoch.store(0, Ordering::SeqCst);
        self.shared.solved_epoch.store(0, Ordering::SeqCst);
    }

    fn set_work(&self, work: MiningWork) -> Result<()> {
        if !self.shared.started.load(Ordering::SeqCst) {
            return Err(anyhow!("CPU backend is not started"));
        }

        self.shared.solved_epoch.store(0, Ordering::SeqCst);
        {
            let mut slot = self
                .shared
                .work
                .write()
                .map_err(|_| anyhow!("CPU work lock poisoned"))?;
            *slot = Some(work.clone());
        }
        self.shared
            .current_epoch
            .store(work.epoch, Ordering::SeqCst);
        Ok(())
    }

    fn try_recv_solution(&self) -> Option<MiningSolution> {
        let rx = self.solution_rx.lock().ok()?;
        rx.try_recv().ok()
    }

    fn drain_hashes(&self) -> u64 {
        let mut total_delta = 0u64;
        let mut snapshot = match self.last_hash_snapshot.lock() {
            Ok(snapshot) => snapshot,
            Err(_) => return 0,
        };

        for (idx, counter) in self.thread_hashes.iter().enumerate() {
            let current = counter.load(Ordering::Relaxed);
            let previous = snapshot[idx];
            let delta = current.saturating_sub(previous);
            snapshot[idx] = current;
            total_delta = total_delta.saturating_add(delta);
        }

        total_delta
    }

    fn drain_errors(&self) -> u64 {
        self.shared.errors.swap(0, Ordering::Relaxed)
    }

    fn kernel_bench(&self, seconds: u64, shutdown: &AtomicBool) -> Result<u64> {
        let params = pow_params().map_err(|e| anyhow!("invalid Argon2 params: {e:?}"))?;
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

        let mut header_base = [0u8; blocknet_pow_spec::POW_HEADER_BASE_LEN];
        for (i, byte) in header_base.iter_mut().enumerate() {
            *byte = (i as u8).wrapping_mul(31).wrapping_add(7);
        }

        let stop_at = Instant::now() + Duration::from_secs(seconds.max(1));
        let mut output = [0u8; POW_OUTPUT_LEN];
        let mut nonce = 0u64;
        let mut hashes = 0u64;

        while Instant::now() < stop_at && !shutdown.load(Ordering::Relaxed) {
            let nonce_bytes = nonce.to_le_bytes();
            argon2
                .hash_password_into(&nonce_bytes, &header_base, &mut output)
                .map_err(|_| anyhow!("kernel benchmark hash failed"))?;
            nonce = nonce.wrapping_add(1);
            hashes = hashes.saturating_add(1);
        }

        Ok(hashes)
    }
}

fn cpu_worker_loop(shared: Arc<Shared>, thread_hashes: Arc<Vec<AtomicU64>>, thread_idx: usize) {
    let params = match pow_params() {
        Ok(p) => p,
        Err(_) => {
            shared.errors.fetch_add(1, Ordering::Relaxed);
            shared.shutdown.store(true, Ordering::SeqCst);
            return;
        }
    };

    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut output = [0u8; POW_OUTPUT_LEN];
    let mut local_epoch = 0u64;
    let mut nonce = 0u64;

    loop {
        if shared.shutdown.load(Ordering::Relaxed) {
            break;
        }

        let work = match shared.work.read() {
            Ok(slot) => slot.clone(),
            Err(_) => {
                shared.errors.fetch_add(1, Ordering::Relaxed);
                shared.shutdown.store(true, Ordering::SeqCst);
                break;
            }
        };

        let Some(work) = work else {
            thread::sleep(IDLE_SLEEP);
            continue;
        };

        if work.epoch != local_epoch {
            local_epoch = work.epoch;
            nonce = work
                .start_nonce
                .wrapping_add(work.lane_offset)
                .wrapping_add(thread_idx as u64);
        }

        if local_epoch == 0 || shared.current_epoch.load(Ordering::Relaxed) != local_epoch {
            thread::yield_now();
            continue;
        }

        if shared.solved_epoch.load(Ordering::Relaxed) == local_epoch
            || Instant::now() >= work.stop_at
        {
            thread::sleep(STALE_SLEEP);
            continue;
        }

        let nonce_bytes = nonce.to_le_bytes();
        if argon2
            .hash_password_into(&nonce_bytes, &work.header_base, &mut output)
            .is_err()
        {
            shared.errors.fetch_add(1, Ordering::Relaxed);
            shared.shutdown.store(true, Ordering::SeqCst);
            break;
        }

        if let Some(counter) = thread_hashes.get(thread_idx) {
            counter.fetch_add(1, Ordering::Relaxed);
        }

        if hash_meets_target(&output, &work.target)
            && shared
                .solved_epoch
                .compare_exchange(0, local_epoch, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            let _ = shared.solution_tx.send(MiningSolution {
                epoch: local_epoch,
                nonce,
                backend: "cpu".to_string(),
            });
        }

        nonce = nonce.wrapping_add(work.global_stride.max(1));
    }
}
