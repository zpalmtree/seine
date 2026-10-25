//! Ring wraparound, slice-boundary, dirty reuse and full-size reference checks.
use argon2::{Algorithm, Argon2, Params, Version};
use blocknet_pow_kernel::{CpuKernel, FixedArgon2id, PowBlock};

fn main() {
    for kib in [
        8,
        60,
        64,
        65,
        68,
        124,
        128,
        132,
        252,
        256,
        260,
        4096,
        2 * 1024 * 1024,
    ] {
        let hasher = FixedArgon2id::with_kernel(kib, CpuKernel::Avx512256).unwrap();
        let mut actual = [0u8; 32];
        let nonces = [0u64, 17, u64::MAX];
        let references: Vec<[u8; 32]> = nonces
            .iter()
            .map(|nonce| {
                let mut result = [0u8; 32];
                Argon2::new(
                    Algorithm::Argon2id,
                    Version::V0x13,
                    Params::new(kib, 1, 1, Some(32)).unwrap(),
                )
                .hash_password_into(&nonce.to_le_bytes(), b"epyc-ring-parity", &mut result)
                .unwrap();
                result
            })
            .collect();
        let count = hasher.block_count();
        let mut arena = vec![PowBlock::default(); count + 2];
        arena[0].as_mut().fill(0xa5a5_a5a5_a5a5_a5a5);
        arena[count + 1].as_mut().fill(0x5a5a_5a5a_5a5a_5a5a);
        for (nonce, reference) in nonces.into_iter().zip(references) {
            for reuse in 0..2 {
                hasher
                    .hash_password_into_with_memory(
                        &nonce.to_le_bytes(),
                        b"epyc-ring-parity",
                        &mut actual,
                        &mut arena[1..count + 1],
                    )
                    .unwrap();
                assert_eq!(
                    actual, reference,
                    "memory={kib} nonce={nonce} reuse={reuse}"
                );
                assert!(arena[0]
                    .as_ref()
                    .iter()
                    .all(|&v| v == 0xa5a5_a5a5_a5a5_a5a5));
                assert!(arena[count + 1]
                    .as_ref()
                    .iter()
                    .all(|&v| v == 0x5a5a_5a5a_5a5a_5a5a));
            }
        }
        println!("memory={kib}: 3 nonces, dirty reuse, boundary guards PASS");
    }
}
