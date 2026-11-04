use argon2::{Error, Params};

pub const POW_MEMORY_KB: u32 = 2 * 1024 * 1024;
pub const POW_ITERATIONS: u32 = 1;
pub const POW_PARALLELISM: u32 = 1;
pub const POW_OUTPUT_LEN: usize = 32;
pub const POW_HEADER_BASE_LEN: usize = 92;

/// First height mined with BLAKE2b-512 and 10-minute blocks. Templates below
/// it stay Argon2id.
pub const FARADAY_ACTIVATION_HEIGHT: u64 = 103_680;

pub fn target_block_seconds(template_height: u64) -> u64 {
    if template_height >= FARADAY_ACTIVATION_HEIGHT {
        600
    } else {
        300
    }
}

/// A transport/resource bound, not a consensus header layout.
pub const MAX_POW_HEADER_BASE_LEN: usize = 4096;

pub fn validate_header_length(length: usize) -> Result<u32, &'static str> {
    if !(8..=MAX_POW_HEADER_BASE_LEN).contains(&length) {
        return Err("header_base must contain 8..4096 bytes");
    }
    Ok(length as u32)
}

/// False until upstream publishes final BLAKE2b parameters and vectors and
/// they pass here. Until then callers choose whether to mine fork-height work
/// with the provisional parameters. Pools and miners name the parameter set
/// with the `pow_blake2b_v1` Stratum capability: if the final parameters
/// differ, the capability gets a new name together with them.
pub const BLAKE2B_POW_PARAMS_FINAL: bool = false;

/// Picks the hash for a template height, refusing provisional BLAKE2b work
/// unless `allow_provisional` is set.
pub fn ensure_supported_pow(
    height: u64,
    allow_provisional: bool,
) -> Result<PowAlgorithm, &'static str> {
    match PowAlgorithm::for_height(height) {
        PowAlgorithm::Blake2b512 if !BLAKE2B_POW_PARAMS_FINAL && !allow_provisional => {
            Err("Faraday PoW is not supported by this build; upgrade before height 103680")
        }
        pow => Ok(pow),
    }
}

pub const CPU_LANE_MEMORY_BYTES: u64 = (POW_MEMORY_KB as u64) * 1024;

pub fn pow_params() -> Result<Params, Error> {
    Params::new(
        POW_MEMORY_KB,
        POW_ITERATIONS,
        POW_PARALLELISM,
        Some(POW_OUTPUT_LEN),
    )
}

// Faraday fork PoW (blocknet-pqc core spec §6, decided 2026-10-26). Upstream
// has not published test vectors yet; the vectors in the tests below come from
// Python's hashlib.blake2b and must be replaced by the official ones.

/// Header v2 without the nonce: v1 fields plus a 64-byte tree root (proposal).
pub const FARADAY_HEADER_BASE_LEN: usize = 156;
pub const BLAKE2B_POW_OUTPUT_LEN: usize = 64;
pub const BLAKE2B_POW_PERSONAL_MAINNET: [u8; 16] = *b"blocknet/pow/v1\0";
pub const BLAKE2B_POW_PERSONAL_TESTNET: [u8; 16] = *b"blocknet/tpow/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PowAlgorithm {
    Argon2id,
    Blake2b512,
}

impl PowAlgorithm {
    /// Picks the hash by template height, never by wall-clock time.
    pub fn for_height(height: u64) -> Self {
        if height >= FARADAY_ACTIVATION_HEIGHT {
            Self::Blake2b512
        } else {
            Self::Argon2id
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PowNetwork {
    Mainnet,
    Testnet,
}

impl PowNetwork {
    pub fn blake2b_personal(self) -> &'static [u8; 16] {
        match self {
            Self::Mainnet => &BLAKE2B_POW_PERSONAL_MAINNET,
            Self::Testnet => &BLAKE2B_POW_PERSONAL_TESTNET,
        }
    }
}

/// Reference BLAKE2b-512 PoW: `header_base || nonce_le`, no key, no salt.
/// Built on the RustCrypto core so it stays independent of seine's kernels.
pub fn blake2b_pow_hash(
    personal: &[u8; 16],
    header_base: &[u8],
    nonce: u64,
) -> [u8; BLAKE2B_POW_OUTPUT_LEN] {
    use blake2::digest::core_api::{Buffer, UpdateCore, VariableOutputCore};
    use blake2::Blake2bVarCore;

    let mut core = Blake2bVarCore::new_with_params(&[], personal, 0, BLAKE2B_POW_OUTPUT_LEN);
    let mut buffer = Buffer::<Blake2bVarCore>::default();
    buffer.digest_blocks(header_base, |blocks| core.update_blocks(blocks));
    buffer.digest_blocks(&nonce.to_le_bytes(), |blocks| core.update_blocks(blocks));
    let mut out = Default::default();
    core.finalize_variable_core(&mut buffer, &mut out);
    out.into()
}

/// The value compared against the target: the first 32 output bytes, read big-endian.
pub fn blake2b_pow_target_bytes(hash: &[u8; BLAKE2B_POW_OUTPUT_LEN]) -> [u8; 32] {
    hash[..32].try_into().expect("32-byte prefix")
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn pattern_header(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 37 + 11) as u8).collect()
    }

    // (network, header_base length, nonce, BLAKE2b-512 hex) from hashlib.blake2b(person=...).
    // Lengths cover the Faraday header, the legacy header, a final block that is
    // exactly full, a nonce straddling blocks 0/1 and a two-block cached prefix.
    const VECTORS: &[(&str, usize, u64, &str)] = &[
        ("mainnet", 156, 0x0000000000000000, "76cf2d15d076a3d5dadd576fc4c2e3c7d6b4e011ef8c8cb6507902585e622e1c158026ec57c35b81972d192dedfec49c769dbff133a838231509183a76395fe5"),
        ("mainnet", 156, 0x0000000000000001, "a188c5989bbdb7cf6200088f8d9d4c5339e874e0371a563b2f77cc7fd02d710858f0aa8f0c6763db7987b3c4ffc1efbe871479aa1a7ff76f93d0a61a9ac8ec2d"),
        ("mainnet", 156, 0xffffffffffffffff, "518203a6cef50bd5902d1638f4844a7ca3bfce4affcc35b5abdf35fc0b6aa60a8ed033a4afd930f160344a3d7f752fee48f203b1e531058721a4ba64d4feb5f2"),
        ("mainnet", 156, 0x0123456789abcdef, "3394957d328818ef398464ab6456730d93751b852d8bd09deb6492fae9ffafa822929515cb8b500a76a3a3e7c54ec25ab3f30acc2fdaebe0e9179c09a332201e"),
        ("mainnet", 92, 0x000000000000002a, "0e15f71f5ea27ffd81e1292cda85d241e4f7976d9b32fadf74ba590375616d9657e0e836385ef27b92ea64ce8759658bdc6b43ad54e052840f13c8b561bcbcf0"),
        ("mainnet", 120, 0x0000000000000007, "d88985b7686e4e09e0f08c0c8aa53b60500613481748bea22945f285991a7ac9acd05558c03c4a445e77240d1d60fc684094a220a9a817d0b75da1f6a9883a91"),
        ("mainnet", 124, 0x0000000000000007, "caa9bb01b9aa0b9c6cf40c77272edd053cc64a4d2205bcf7fa6ee62ec9f096938c77d04ebd9575cc59c39c4308ccd1170a8fb7309a75ae82ce9c1d3e8ce7e124"),
        ("mainnet", 248, 0x0000000000000009, "5a38bb2add0a55a16f794002f304823ed195f67b48309f696ff5a50456733dcd1582eb339a0be342c26b1314905a0f49b255bc6793f4a954b186258260953885"),
        ("testnet", 156, 0x0000000000000000, "9cf25306af1a20afbf5197265ab30d7fd9eee9c44df72059302f770611ae3f11dc8cb7b0a4fa76922c957e9c647b7a7b89eb256bc08b2975edea015004e459e5"),
        ("testnet", 156, 0x0000000000000001, "cf9ea40e5b9c1d92ca857bed7277bd220775c45fa4262f96f3e9cbe56775cf1d1ecb6bb3c1c9fdf0be28eab4df8fc52de88a802c60191c890a2c3343feafaae7"),
        ("testnet", 156, 0xffffffffffffffff, "be5d9bea8d67ccbcc89abc0591564001503295213dd9a3c39771261b098e8f36cb064658d3c620f89e90813161789535a268585ff627bf61260deffc3a8d8b10"),
        ("testnet", 156, 0x0123456789abcdef, "c11a8af26ff1007232c7c8e4c9ea5cff3345b37f6567daa4abe93947ae4ed3e8f73b49c92232ace5eedb8620e6190cd3bf42e3a3e96f8d38a5e23a9c3c0d482a"),
        ("testnet", 92, 0x000000000000002a, "c682bebb871fc6528e396b6fab353d1c3a3180197ae472286a6563c5198a6a2573a239614c64b74412bc8a8d48fc7b157f856bb48f937aa3636d3de17971233b"),
        ("testnet", 120, 0x0000000000000007, "15066d4ab98a21a79c345277951496a3d4ca71c447fb8d94082e45014eec4cb03f541e7d02936398aac4faa424409ddd1310db4e86f38c33adc7290d61f7d93e"),
        ("testnet", 124, 0x0000000000000007, "a46d908848824f76e105fec312a6181e24b284918a30424cdb4825b62de4360023a27d70dc251ce2d03db98726cf4f04cfb303754d8355393d6de9eb7f122b61"),
        ("testnet", 248, 0x0000000000000009, "0f4c276e63c1409ca0ed3bf4a6bb71de162b829a8ec549dd58e634e63b169780c428745822290a90e74cecbebf8c77c79e80ed90d82c464da2ed5ba08c2eed76"),
    ];

    fn network(name: &str) -> PowNetwork {
        match name {
            "mainnet" => PowNetwork::Mainnet,
            "testnet" => PowNetwork::Testnet,
            other => panic!("unknown network {other}"),
        }
    }

    #[test]
    fn blake2b_pow_matches_independent_vectors() {
        for &(net, len, nonce, expected) in VECTORS {
            let hash =
                blake2b_pow_hash(network(net).blake2b_personal(), &pattern_header(len), nonce);
            assert_eq!(hex(&hash), expected, "net={net} len={len} nonce={nonce:#x}");
        }
    }

    #[test]
    fn personalization_separates_networks() {
        let header = pattern_header(FARADAY_HEADER_BASE_LEN);
        assert_ne!(
            blake2b_pow_hash(&BLAKE2B_POW_PERSONAL_MAINNET, &header, 1),
            blake2b_pow_hash(&BLAKE2B_POW_PERSONAL_TESTNET, &header, 1)
        );
        assert_eq!(&BLAKE2B_POW_PERSONAL_MAINNET[..15], b"blocknet/pow/v1");
        assert_eq!(BLAKE2B_POW_PERSONAL_MAINNET[15], 0);
    }

    #[test]
    fn algorithm_switches_at_activation_height() {
        assert_eq!(PowAlgorithm::for_height(0), PowAlgorithm::Argon2id);
        assert_eq!(
            PowAlgorithm::for_height(FARADAY_ACTIVATION_HEIGHT - 1),
            PowAlgorithm::Argon2id
        );
        assert_eq!(
            PowAlgorithm::for_height(FARADAY_ACTIVATION_HEIGHT),
            PowAlgorithm::Blake2b512
        );
        assert_eq!(PowAlgorithm::for_height(u64::MAX), PowAlgorithm::Blake2b512);
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn faraday_selection_uses_template_height() {
        assert_eq!(target_block_seconds(FARADAY_ACTIVATION_HEIGHT - 1), 300);
        assert_eq!(target_block_seconds(FARADAY_ACTIVATION_HEIGHT), 600);
        assert_eq!(
            ensure_supported_pow(FARADAY_ACTIVATION_HEIGHT - 1, false),
            Ok(PowAlgorithm::Argon2id)
        );
        assert!(ensure_supported_pow(FARADAY_ACTIVATION_HEIGHT, false).is_err());
        assert!(ensure_supported_pow(u64::MAX, false).is_err());
        assert_eq!(
            ensure_supported_pow(FARADAY_ACTIVATION_HEIGHT, true),
            Ok(PowAlgorithm::Blake2b512)
        );
        // A reorg can return to legacy work; no wall-clock or latched era.
        assert_eq!(
            ensure_supported_pow(FARADAY_ACTIVATION_HEIGHT - 1, true),
            Ok(PowAlgorithm::Argon2id)
        );
    }

    #[test]
    fn header_lengths_are_dynamic_and_bounded() {
        for len in [8, 92, 156, 512, MAX_POW_HEADER_BASE_LEN] {
            assert_eq!(validate_header_length(len).unwrap() as usize, len);
        }
        assert!(validate_header_length(7).is_err());
        assert!(validate_header_length(MAX_POW_HEADER_BASE_LEN + 1).is_err());
    }
}
