use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use anyhow::{anyhow, Result};
use argon2::{Algorithm, Argon2, Params, Version};

use crate::backend::{MiningJob, MiningSolution, PowBackend};
use crate::types::hash_meets_target;

const POW_MEMORY_KB: u32 = 2 * 1024 * 1024;
const POW_ITERATIONS: u32 = 1;
const POW_PARALLELISM: u32 = 1;
const POW_OUTPUT_LEN: usize = 32;

pub struct CpuBackend;

impl CpuBackend {
    pub fn new() -> Self {
        Self
    }
}

impl PowBackend for CpuBackend {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn mine(
        &self,
        job: &MiningJob,
        shutdown: &AtomicBool,
        total_hashes: &AtomicU64,
    ) -> Result<Option<MiningSolution>> {
        let started = Instant::now();
        let start_hashes = total_hashes.load(Ordering::Relaxed);

        let params = Params::new(
            POW_MEMORY_KB,
            POW_ITERATIONS,
            POW_PARALLELISM,
            Some(POW_OUTPUT_LEN),
        )
        .map_err(|e| anyhow!("failed to initialize Argon2 parameters: {e:?}"))?;

        let found_nonce = Arc::new(AtomicU64::new(u64::MAX));
        let should_stop = Arc::new(AtomicBool::new(false));
        let worker_error = Arc::new(AtomicUsize::new(0));

        thread::scope(|scope| {
            for tid in 0..job.threads {
                let found_nonce = Arc::clone(&found_nonce);
                let should_stop = Arc::clone(&should_stop);
                let worker_error = Arc::clone(&worker_error);

                let header_base = Arc::clone(&job.header_base);
                let params = params.clone();
                let target = job.target;
                let stop_at = job.stop_at;
                let step = job.threads as u64;
                let mut nonce = job.start_nonce.wrapping_add(tid as u64);

                scope.spawn(move || {
                    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
                    let mut output = [0u8; POW_OUTPUT_LEN];

                    loop {
                        if should_stop.load(Ordering::Relaxed)
                            || shutdown.load(Ordering::Relaxed)
                            || Instant::now() >= stop_at
                        {
                            return;
                        }

                        let nonce_bytes = nonce.to_le_bytes();
                        match argon2.hash_password_into(&nonce_bytes, &header_base, &mut output) {
                            Ok(()) => {
                                total_hashes.fetch_add(1, Ordering::Relaxed);

                                if hash_meets_target(&output, &target) {
                                    if found_nonce
                                        .compare_exchange(
                                            u64::MAX,
                                            nonce,
                                            Ordering::SeqCst,
                                            Ordering::SeqCst,
                                        )
                                        .is_ok()
                                    {
                                        should_stop.store(true, Ordering::SeqCst);
                                    }
                                    return;
                                }
                            }
                            Err(_) => {
                                worker_error.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        }

                        nonce = nonce.wrapping_add(step);
                    }
                });
            }
        });

        if worker_error.load(Ordering::Relaxed) > 0 {
            return Err(anyhow!("one or more CPU workers failed to hash"));
        }

        let total_job_hashes = total_hashes
            .load(Ordering::Relaxed)
            .saturating_sub(start_hashes);
        let elapsed = started.elapsed();
        let nonce = found_nonce.load(Ordering::SeqCst);

        if nonce == u64::MAX {
            Ok(None)
        } else {
            Ok(Some(MiningSolution {
                nonce,
                hashes: total_job_hashes,
                elapsed,
            }))
        }
    }
}
