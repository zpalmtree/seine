/// Compiled CPU compression paths. Support checks include OS vector-state support
/// on x86; a CPU family/model name alone never authorizes an instruction set.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CpuKernel {
    Scalar,
    Avx2,
    Avx512,
    /// Experimental 256-bit AVX-512 path; never selected automatically.
    Avx512256,
    Neon,
    /// Cached recent blocks with non-temporal arena stores; selected by measured tuning.
    Avx512256Stream,
    /// Runtime-guarded NEON path using SHA3 XOR/rotate and 64-byte prefetch coverage.
    /// Automatically compared with baseline NEON on hosts with SHA3 support.
    NeonSha3,
    /// Streaming stores with eight cached blocks and two-block prefetch lookahead.
    Avx512256StreamR8D2,
    /// Streaming stores with 64 cached blocks, depth three and demand fences.
    Avx512256StreamR64D3,
    /// Streaming stores with 64 cached blocks, depth four and demand fences.
    Avx512256StreamR64D4,
    /// Streaming stores with 64 cached blocks, depth six and demand fences.
    Avx512256StreamR64D6,
    /// AVX2 compression with two cached blocks, non-temporal arena stores and
    /// three-block data-independent prefetch.
    Avx2Stream,
    /// Full-width AVX-512 compression in the streaming ring loop (Zen 5 servers).
    Avx512Stream,
    /// `Avx512256Stream` plus a far L2 prefetch of data-independent references (Zen 4).
    Avx512256StreamL2,
    /// `Avx512Stream` plus the far L2 prefetch (Zen 4).
    Avx512StreamL2,
    /// `Avx512Stream` with stores skipped from 60% of the arena, independent of
    /// `FixedArgon2id::set_skip_store_permille`.
    Avx512StreamSkip,
}

impl CpuKernel {
    pub const ALL: [Self; 16] = [
        Self::Scalar,
        Self::Avx2,
        Self::Avx512,
        Self::Avx512256,
        Self::Neon,
        Self::Avx512256Stream,
        Self::NeonSha3,
        Self::Avx512256StreamR8D2,
        Self::Avx512256StreamR64D3,
        Self::Avx512256StreamR64D4,
        Self::Avx512256StreamR64D6,
        Self::Avx2Stream,
        Self::Avx512Stream,
        Self::Avx512256StreamL2,
        Self::Avx512StreamL2,
        Self::Avx512StreamSkip,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Avx2 => "avx2",
            Self::Avx512 => "avx512",
            Self::Avx512256 => "avx512-256",
            Self::Neon => "neon",
            Self::Avx512256Stream => "avx512-256-stream",
            Self::NeonSha3 => "neon-sha3",
            Self::Avx512256StreamR8D2 => "avx512-256-stream-r8d2",
            Self::Avx512256StreamR64D3 => "avx512-256-stream-r64d3",
            Self::Avx512256StreamR64D4 => "avx512-256-stream-r64d4",
            Self::Avx512256StreamR64D6 => "avx512-256-stream-r64d6",
            Self::Avx2Stream => "avx2-stream",
            Self::Avx512Stream => "avx512-stream",
            Self::Avx512256StreamL2 => "avx512-256-stream-l2",
            Self::Avx512StreamL2 => "avx512-stream-l2",
            Self::Avx512StreamSkip => "avx512-stream-skip",
        }
    }

    pub fn is_supported(self) -> bool {
        match self {
            Self::Scalar => true,
            #[cfg(target_arch = "x86_64")]
            Self::Avx2 | Self::Avx2Stream => std::arch::is_x86_feature_detected!("avx2"),
            #[cfg(target_arch = "x86_64")]
            Self::Avx512 => {
                std::arch::is_x86_feature_detected!("avx512f")
                    && std::arch::is_x86_feature_detected!("avx512vl")
            }
            #[cfg(target_arch = "x86_64")]
            Self::Avx512256
            | Self::Avx512256Stream
            | Self::Avx512256StreamR8D2
            | Self::Avx512256StreamR64D3
            | Self::Avx512256StreamR64D4
            | Self::Avx512256StreamR64D6
            | Self::Avx512Stream
            | Self::Avx512256StreamL2
            | Self::Avx512StreamL2
            | Self::Avx512StreamSkip => {
                std::arch::is_x86_feature_detected!("avx2")
                    && std::arch::is_x86_feature_detected!("avx512f")
                    && std::arch::is_x86_feature_detected!("avx512vl")
            }
            #[cfg(target_arch = "aarch64")]
            Self::Neon => true,
            #[cfg(target_arch = "aarch64")]
            Self::NeonSha3 => std::arch::is_aarch64_feature_detected!("sha3"),
            _ => false,
        }
    }

    /// The existing automatic dispatch policy, also used by normal mining.
    pub fn preferred() -> Self {
        [Self::Avx512, Self::Avx2, Self::Neon, Self::Scalar]
            .into_iter()
            .find(|kernel| kernel.is_supported())
            .expect("scalar is always supported")
    }
}

impl CpuKernel {
    /// Raise a kernel's number when a change can make it faster or slower: every
    /// miner then measures it again, even if it lost before.
    pub const fn revision(self) -> u32 {
        match self {
            Self::Scalar
            | Self::Avx2
            | Self::Avx512
            | Self::Avx512256
            | Self::Neon
            | Self::Avx512256Stream
            | Self::NeonSha3
            | Self::Avx512256StreamR8D2
            | Self::Avx512256StreamR64D3
            | Self::Avx512256StreamR64D4
            | Self::Avx512256StreamR64D6
            | Self::Avx2Stream
            | Self::Avx512Stream
            | Self::Avx512256StreamL2
            | Self::Avx512StreamL2
            | Self::Avx512StreamSkip => 1,
        }
    }
}

impl std::fmt::Display for CpuKernel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for CpuKernel {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|kernel| kernel.as_str() == value)
            .ok_or_else(|| {
                "expected scalar, avx2, avx512, avx512-256, avx512-256-stream, avx512-256-stream-r8d2, avx512-256-stream-r64d3, avx512-256-stream-r64d4, avx512-256-stream-r64d6, avx2-stream, avx512-stream, avx512-256-stream-l2, avx512-stream-l2, avx512-stream-skip, neon, or neon-sha3"
                    .to_string()
            })
    }
}
