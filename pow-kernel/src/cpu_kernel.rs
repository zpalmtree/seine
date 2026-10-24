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
    /// Runtime-guarded NEON path using SHA3 XOR/rotate and 64-byte prefetch coverage.
    /// Automatically compared with baseline NEON on hosts with SHA3 support.
    #[cfg(target_arch = "aarch64")]
    NeonSha3,
}

impl CpuKernel {
    pub const ALL: [Self; 5 + cfg!(target_arch = "aarch64") as usize] = [
        Self::Scalar,
        Self::Avx2,
        Self::Avx512,
        Self::Avx512256,
        Self::Neon,
        #[cfg(target_arch = "aarch64")]
        Self::NeonSha3,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Avx2 => "avx2",
            Self::Avx512 => "avx512",
            Self::Avx512256 => "avx512-256",
            Self::Neon => "neon",
            #[cfg(target_arch = "aarch64")]
            Self::NeonSha3 => "neon-sha3",
        }
    }

    pub fn is_supported(self) -> bool {
        match self {
            Self::Scalar => true,
            #[cfg(target_arch = "x86_64")]
            Self::Avx2 => std::arch::is_x86_feature_detected!("avx2"),
            #[cfg(target_arch = "x86_64")]
            Self::Avx512 => {
                std::arch::is_x86_feature_detected!("avx512f")
                    && std::arch::is_x86_feature_detected!("avx512vl")
            }
            #[cfg(target_arch = "x86_64")]
            Self::Avx512256 => {
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
                #[cfg(target_arch = "aarch64")]
                let expected = "expected scalar, avx2, avx512, avx512-256, neon, or neon-sha3";
                #[cfg(not(target_arch = "aarch64"))]
                let expected = "expected scalar, avx2, avx512, avx512-256, or neon";
                expected.to_string()
            })
    }
}
