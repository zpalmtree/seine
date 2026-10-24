//! Exact-hash trace + bounded checkpoint replay; no mining or speed prediction.
use argon2::{Algorithm, Argon2, Params, Version};
use blocknet_pow_kernel::{trace_hash, CpuKernel};
use std::error::Error;
use std::io;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let memory_kib: u32 = args.next().unwrap_or_else(|| "65536".into()).parse()?;
    let nonce: u64 = args.next().unwrap_or_else(|| "0".into()).parse()?;
    let max_work_factor: u64 = args.next().unwrap_or_else(|| "8".into()).parse()?;
    if args.next().is_some()
        || !(8..=2_097_152).contains(&memory_kib)
        || !(1..=64).contains(&max_work_factor)
    {
        return Err(invalid(
            "usage: cpu_tmto [memory_kib:8..2097152] [nonce:u64] [max_work_factor:1..64]",
        )
        .into());
    }
    let salt = [0x37; 92];
    let pwd = nonce.to_le_bytes();
    let mut expected = [0; 32];
    // Independent arena is released before allocating the trace's arena.
    Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(memory_kib, 1, 1, Some(32)).map_err(|e| invalid(e.to_string()))?,
    )
    .hash_password_into(&pwd, &salt, &mut expected)
    .map_err(|e| invalid(e.to_string()))?;
    let trace = trace_hash(memory_kib, &pwd, &salt, CpuKernel::preferred())
        .map_err(|e| invalid(format!("trace failed: {e:?}")))?;
    if trace.hash != expected {
        return Err(invalid("independent reference hash mismatch").into());
    }
    let hash: String = trace.hash.iter().map(|b| format!("{b:02x}")).collect();
    let count = trace.references().len();
    eprintln!("Exact hash verified. Operation-count simulation only; no measured H/s or reduced-memory hash implementation.");
    println!("{{\"schema\":1,\"memory_kib\":{memory_kib},\"nonce\":{nonce},\"salt_byte\":55,\"salt_len\":92,\"hash\":\"{hash}\",\"blocks\":{count},\"max_work_factor\":{max_work_factor},\"replays\":[");
    let variants = [1, 2, 3, 4, 8, 16, 32]
        .into_iter()
        .map(|n| ("periodic", n))
        .chain([2, 3, 4, 8, 16, 32].into_iter().map(|n| ("cost", n)));
    for (index, (policy, parameter)) in variants.enumerate() {
        let budget = (count as u64 - 2) * max_work_factor;
        let r = if policy == "periodic" {
            trace.replay(parameter, budget)
        } else {
            trace.replay_cost_threshold(parameter as u64, budget)
        }
        .map_err(|e| invalid(format!("replay failed: {e:?}")))?;
        let work_ratio = if r.complete {
            (r.compressions as f64 / (count - 2) as f64).to_string()
        } else {
            "null".into()
        };
        println!("{}{{\"policy\":\"{policy}\",\"parameter\":{parameter},\"complete\":{},\"completed_blocks\":{},\"compressions\":{},\"recompressions\":{},\"work_ratio\":{work_ratio},\"peak_resident_blocks\":{},\"retained_blocks\":{},\"peak_stack_frames\":{},\"reference_metadata_bytes\":{}}}",
            if index == 0 { "" } else { "," }, r.complete, r.completed_blocks, r.compressions,
            r.recompressions, r.peak_resident_blocks, r.retained_blocks, r.peak_stack_frames, r.reference_metadata_bytes);
    }
    println!("]}}");
    Ok(())
}
