//! Offline nonce-mining prototype, full or online checkpoint storage. No network.
use blocknet_pow_kernel::{CpuKernel, FixedArgon2id, PowBlock, ReducedMemoryContext};
use std::{
    error::Error,
    io,
    sync::{Arc, Barrier},
    time::Instant,
};

fn invalid(s: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, s.into())
}
fn main() -> Result<(), Box<dyn Error>> {
    let a: Vec<_> = std::env::args().skip(1).collect();
    if a.len() != 7 {
        return Err(invalid("usage: mine_reduced memory_kib hashes_per_worker threshold(0=full) kernel retained_cap_percent workers verify(0|1)").into());
    }
    let memory: u32 = a[0].parse()?;
    let hashes: u64 = a[1].parse()?;
    let threshold: u8 = a[2].parse()?;
    let kernel: CpuKernel = a[3].parse().map_err(invalid)?;
    let cap: usize = a[4].parse()?;
    let workers: usize = a[5].parse()?;
    let verify: u8 = a[6].parse()?;
    if !(8..=2097152).contains(&memory)
        || !(1..=1000).contains(&hashes)
        || threshold > 16
        || !(1..=100).contains(&cap)
        || !(1..=32).contains(&workers)
        || verify > 1
    {
        return Err(invalid("argument out of range").into());
    }
    // Build/release metadata never contributes to the hash salt or nonce.
    let barrier = Arc::new(Barrier::new(workers));
    let mut threads = Vec::new();
    for worker in 0..workers {
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || -> Result<_, String> {
            let start = Instant::now();
            let fixed = if threshold == 0 {
                Some(FixedArgon2id::with_kernel(memory, kernel).map_err(|e| format!("{e:?}"))?)
            } else {
                None
            };
            let mut full = if let Some(fixed) = &fixed {
                vec![PowBlock::default(); fixed.block_count()]
            } else {
                vec![]
            };
            let mut reduced = if threshold > 0 {
                Some(
                    ReducedMemoryContext::new(
                        memory,
                        kernel,
                        threshold,
                        memory as usize * cap / 100,
                    )
                    .map_err(|e| format!("{e:?}"))?,
                )
            } else {
                None
            };
            let mut output = [0; 32];
            let salt = [0x37; 92];
            let mut run = |nonce: u64| -> Result<(), String> {
                if let Some(ctx) = reduced.as_mut() {
                    ctx.hash_password_into(&nonce.to_le_bytes(), &salt, &mut output)
                        .map_err(|e| format!("{e:?}"))?;
                } else {
                    fixed
                        .as_ref()
                        .unwrap()
                        .hash_password_into_with_memory(
                            &nonce.to_le_bytes(),
                            &salt,
                            &mut output,
                            &mut full,
                        )
                        .map_err(|e| format!("{e:?}"))?;
                }
                Ok(())
            };
            // Warm and allocate before synchronized measured nonce streams.
            let warmed = run(worker as u64);
            let cold_secs = start.elapsed().as_secs_f64();
            barrier.wait();
            warmed?;
            let start = Instant::now();
            let mut outputs = Vec::new();
            for index in 1..=hashes {
                let nonce = index * workers as u64 + worker as u64;
                run(nonce)?;
            }
            let elapsed = start.elapsed().as_secs_f64();
            // Last nonce is independently verified outside timing. Unit/full-size
            // validation separately covers multiple nonces and dirty reuse.
            let nonce = hashes * workers as u64 + worker as u64;
            outputs.push((nonce, output));
            let stats = reduced.as_ref().map(|r| r.stats());
            drop(full);
            drop(reduced);
            drop(fixed);
            if verify == 1 {
                use argon2::{Algorithm, Argon2, Params, Version};
                let reference = Argon2::new(
                    Algorithm::Argon2id,
                    Version::V0x13,
                    Params::new(memory, 1, 1, Some(32)).unwrap(),
                );
                let mut expected = [0; 32];
                reference
                    .hash_password_into(&nonce.to_le_bytes(), &salt, &mut expected)
                    .map_err(|e| e.to_string())?;
                if expected != output {
                    return Err("reference mismatch".into());
                }
            }
            Ok((worker, cold_secs, elapsed, outputs, stats))
        }));
    }
    let mut longest = 0f64;
    for handle in threads {
        let (worker, cold, elapsed, outputs, stats) = handle
            .join()
            .map_err(|_| invalid("worker panic"))?
            .map_err(invalid)?;
        longest = longest.max(elapsed);
        let digest: String = outputs[0].1.iter().map(|b| format!("{b:02x}")).collect();
        println!("{{\"type\":\"worker\",\"worker\":{worker},\"memory_kib\":{memory},\"threshold\":{threshold},\"kernel\":\"{kernel}\",\"hashes\":{hashes},\"elapsed_secs\":{elapsed},\"cold_secs\":{cold},\"last_nonce\":{},\"last_hash\":\"{digest}\",\"verified\":{},\"allocated_bytes\":{},\"retained_blocks\":{},\"last_compressions\":{},\"max_depth\":{}}}",outputs[0].0,verify==1,stats.map_or(memory as usize*1024+memory as usize/2*8,|s|s.allocated_bytes),stats.map_or(memory as usize,|s|s.retained_blocks),stats.map_or(memory as u64/4*4-2,|s|s.compressions),stats.map_or(0,|s|s.peak_reconstruction_depth));
    }
    println!("{{\"type\":\"summary\",\"workers\":{workers},\"hashes\":{},\"elapsed_secs\":{longest},\"hps\":{}}}",hashes*workers as u64,hashes as f64*workers as f64/longest);
    Ok(())
}
