use argon2::{Algorithm, Argon2, Params, Version};
use blocknet_pow_kernel::{CpuKernel, Error, FixedArgon2id, PowBlock};

#[test]
fn explicit_paths_match_reference_and_do_not_change_default_dispatch() {
    let automatic = FixedArgon2id::new(8).isa_label();
    for kernel in CpuKernel::ALL {
        if !kernel.is_supported() {
            assert!(matches!(
                FixedArgon2id::with_kernel(u32::MAX, kernel),
                Err(Error::UnsupportedKernel)
            ));
            continue;
        }
        for memory in [8, 65, 4096] {
            let hasher = FixedArgon2id::with_kernel(memory, kernel).unwrap();
            assert_eq!(hasher.isa_label(), kernel.as_str());
            let mut arena = vec![PowBlock::default(); hasher.block_count()];
            let reference = Argon2::new(
                Algorithm::Argon2id,
                Version::V0x13,
                Params::new(memory, 1, 1, Some(32)).unwrap(),
            );
            for nonce in [0u64, 1, u64::MAX] {
                let mut actual = [0; 32];
                let mut expected = [0; 32];
                hasher
                    .hash_password_into_with_memory(
                        &nonce.to_le_bytes(),
                        b"kernel-selection",
                        &mut actual,
                        &mut arena,
                    )
                    .unwrap();
                reference
                    .hash_password_into(&nonce.to_le_bytes(), b"kernel-selection", &mut expected)
                    .unwrap();
                assert_eq!(
                    actual, expected,
                    "kernel={kernel} memory={memory} nonce={nonce}"
                );
            }
        }
        assert_eq!(FixedArgon2id::new(8).isa_label(), automatic);
    }
}

#[test]
fn kernel_names_are_unambiguous_and_preferred_is_supported() {
    for kernel in CpuKernel::ALL {
        assert_eq!(kernel.as_str().parse::<CpuKernel>().unwrap(), kernel);
    }
    assert!("auto".parse::<CpuKernel>().is_err());
    assert!("avx512f".parse::<CpuKernel>().is_err());
    assert!(CpuKernel::preferred().is_supported());
    assert_ne!(CpuKernel::preferred(), CpuKernel::Avx512256);
    assert_ne!(CpuKernel::preferred(), CpuKernel::Avx512256Stream);
}
