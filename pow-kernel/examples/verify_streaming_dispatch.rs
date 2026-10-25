//! Native full-size streaming parity, dirty reuse, and guard blocks.
use argon2::{Algorithm, Argon2, Params, Version};
use blocknet_pow_kernel::{CpuKernel, FixedArgon2id, PowBlock};

fn main() {
    assert!(CpuKernel::Avx512256Stream.is_supported());
    const MEMORY_KIB: u32 = 2 * 1024 * 1024;
    let salt = b"streaming-full-size";
    let reference = Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(MEMORY_KIB, 1, 1, Some(32)).unwrap(),
    );
    let mut cases = Vec::new();
    for nonce in [0u64, 12345, u64::MAX] {
        let mut output = [0u8; 32];
        reference
            .hash_password_into(&nonce.to_le_bytes(), salt, &mut output)
            .unwrap();
        cases.push((nonce, output));
    }
    let mut guard = PowBlock::default();
    guard.as_mut().fill(0xa5a5_a5a5_a5a5_a5a5);
    let mut guarded = vec![PowBlock::default(); MEMORY_KIB as usize + 2];
    guarded[0] = guard;
    guarded[MEMORY_KIB as usize + 1] = guard;
    let mut hasher = FixedArgon2id::with_kernel(MEMORY_KIB, CpuKernel::Avx2).unwrap();
    for (nonce, expected) in cases {
        for kernel in [
            CpuKernel::Avx2,
            CpuKernel::Avx512256Stream,
            CpuKernel::Avx512,
            CpuKernel::Avx512256Stream,
            CpuKernel::Avx512256,
            CpuKernel::Avx512256Stream,
            CpuKernel::Avx2,
        ] {
            hasher.set_kernel(kernel).unwrap();
            let mut output = [0u8; 32];
            hasher
                .hash_password_into_with_memory(
                    &nonce.to_le_bytes(),
                    salt,
                    &mut output,
                    &mut guarded[1..MEMORY_KIB as usize + 1],
                )
                .unwrap();
            assert_eq!(output, expected, "nonce={nonce},kernel={kernel}");
            assert_eq!(guarded[0].as_ref(), guard.as_ref());
            assert_eq!(guarded[MEMORY_KIB as usize + 1].as_ref(), guard.as_ref());
            println!("PASS nonce={nonce} kernel={kernel} full_size_bytes={} dirty_reuse=true guards=true",
                MEMORY_KIB as u64 * 1024);
        }
    }
}
