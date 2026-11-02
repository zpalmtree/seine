use sysinfo::System;

pub(crate) fn runtime_environment() -> String {
    let kernel_release = System::kernel_version().unwrap_or_default();
    let proc_version = std::fs::read_to_string("/proc/version").unwrap_or_default();
    classify_runtime_environment(
        std::env::consts::OS,
        &kernel_release,
        &proc_version,
        std::env::var_os("WSL_INTEROP").is_some() || std::env::var_os("WSL_DISTRO_NAME").is_some(),
    )
    .to_string()
}

pub(crate) fn runtime_kernel_version() -> Option<String> {
    System::kernel_version()
}

pub(crate) fn wsl_distro() -> Option<String> {
    std::env::var("WSL_DISTRO_NAME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn build_target() -> &'static str {
    option_env!("SEINE_BUILD_TARGET").unwrap_or("unknown")
}

pub(crate) fn build_host() -> &'static str {
    option_env!("SEINE_BUILD_HOST").unwrap_or("unknown")
}

pub(crate) fn build_profile() -> &'static str {
    option_env!("SEINE_BUILD_PROFILE").unwrap_or("unknown")
}

pub(crate) fn build_opt_level() -> &'static str {
    option_env!("SEINE_BUILD_OPT_LEVEL").unwrap_or("unknown")
}

pub(crate) fn build_rustflags() -> &'static str {
    option_env!("SEINE_BUILD_RUSTFLAGS").unwrap_or("")
}

pub(crate) fn build_features() -> &'static str {
    option_env!("SEINE_BUILD_FEATURES").unwrap_or("")
}

pub(crate) fn rustc_version() -> &'static str {
    option_env!("SEINE_RUSTC_VERSION").unwrap_or("unknown")
}

pub(crate) fn source_fingerprint() -> &'static str {
    option_env!("SEINE_SOURCE_FINGERPRINT").unwrap_or("unknown")
}

pub(crate) fn build_fingerprint() -> String {
    format!(
        "source={};target={};profile={};opt={};rustc={};rustflags={};features={}",
        source_fingerprint(),
        build_target(),
        build_profile(),
        build_opt_level(),
        rustc_version(),
        build_rustflags(),
        build_features(),
    )
}

/// Changes only when a build can hash at a different speed on the CPU.
pub(crate) fn hash_path_fingerprint() -> String {
    format!(
        "source={};target={};opt={};rustc={};rustflags={}",
        option_env!("SEINE_HASH_PATH_FINGERPRINT").unwrap_or("unknown"),
        build_target(),
        build_opt_level(),
        rustc_version(),
        build_rustflags(),
    )
}

fn classify_runtime_environment(
    os: &str,
    kernel_release: &str,
    proc_version: &str,
    has_wsl_env: bool,
) -> &'static str {
    if os != "linux" {
        return "native";
    }

    let marker = format!("{kernel_release} {proc_version}").to_ascii_lowercase();
    if marker.contains("wsl2") || marker.contains("microsoft-standard") {
        "wsl2"
    } else if has_wsl_env || marker.contains("microsoft") {
        "wsl1"
    } else {
        "native"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_classifier_distinguishes_native_linux_and_wsl() {
        assert_eq!(
            classify_runtime_environment("linux", "6.8.0-generic", "Linux", false),
            "native"
        );
        assert_eq!(
            classify_runtime_environment(
                "linux",
                "6.6.87.2-microsoft-standard-WSL2",
                "Linux",
                true
            ),
            "wsl2"
        );
        assert_eq!(
            classify_runtime_environment("linux", "4.4.0-Microsoft", "Linux", true),
            "wsl1"
        );
    }

    #[test]
    fn runtime_classifier_marks_non_linux_as_native() {
        assert_eq!(
            classify_runtime_environment("windows", "", "", true),
            "native"
        );
        assert_eq!(
            classify_runtime_environment("macos", "", "", false),
            "native"
        );
    }

    #[test]
    fn build_fingerprint_contains_toolchain_and_source_identity() {
        let fingerprint = build_fingerprint();
        assert!(fingerprint.contains("source="));
        assert!(fingerprint.contains("target="));
        assert!(fingerprint.contains("rustc="));
    }
}

/// Guest-visible x86 identity. Raw numeric IDs avoid guessing a generation from
/// a brand string; on other architectures these fields are absent.
#[derive(Debug, Clone, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub(crate) struct CpuIdentity {
    pub vendor: Option<String>,
    pub family: Option<u32>,
    pub model: Option<u32>,
    pub stepping: Option<u32>,
    /// Physical cores sharing CPU 0's L3 cache (Linux), when known.
    pub l3_cores: Option<u32>,
}

/// Physical cores sharing CPU 0's last-level (L3) cache, from Linux sysfs.
#[cfg(target_os = "linux")]
fn l3_cores() -> Option<u32> {
    static CORES: std::sync::OnceLock<Option<u32>> = std::sync::OnceLock::new();
    *CORES.get_or_init(|| {
        let cpu0 = std::path::Path::new("/sys/devices/system/cpu/cpu0");
        let read = |path: std::path::PathBuf| std::fs::read_to_string(path).ok();
        let smt = count_cpu_list(&read(cpu0.join("topology/thread_siblings_list"))?)?.max(1);
        std::fs::read_dir(cpu0.join("cache"))
            .ok()?
            .flatten()
            .find_map(|entry| {
                let dir = entry.path();
                (read(dir.join("level"))?.trim() == "3")
                    .then(|| count_cpu_list(&read(dir.join("shared_cpu_list"))?))
                    .flatten()
                    .map(|shared| (shared / smt) as u32)
            })
    })
}

#[cfg(not(target_os = "linux"))]
fn l3_cores() -> Option<u32> {
    None
}

/// Number of CPUs in a Linux cpu list such as `0-15,128-143`.
fn count_cpu_list(raw: &str) -> Option<usize> {
    raw.trim()
        .split(',')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (first, last) = part.split_once('-').unwrap_or((part, part));
            let (first, last) = (first.parse::<usize>().ok()?, last.parse::<usize>().ok()?);
            (first <= last).then_some(last - first + 1)
        })
        .sum()
}

pub(crate) fn cpu_identity() -> CpuIdentity {
    #[cfg(target_arch = "x86_64")]
    {
        // CPUID is available on x86-64. Do not use identity to authorize SIMD;
        // CpuKernel support checks also require OS vector-state support.
        let vendor = unsafe { std::arch::x86_64::__cpuid(0) };
        let mut bytes = Vec::with_capacity(12);
        bytes.extend_from_slice(&vendor.ebx.to_le_bytes());
        bytes.extend_from_slice(&vendor.edx.to_le_bytes());
        bytes.extend_from_slice(&vendor.ecx.to_le_bytes());
        let mut identity = CpuIdentity {
            vendor: String::from_utf8(bytes)
                .ok()
                .filter(|s| !s.trim_matches('\0').is_empty()),
            ..CpuIdentity::default()
        };
        if vendor.eax >= 1 {
            let signature = unsafe { std::arch::x86_64::__cpuid(1) }.eax;
            let (family, model, stepping) = decode_x86_signature(signature);
            identity.family = Some(family);
            identity.model = Some(model);
            identity.stepping = Some(stepping);
        }
        identity.l3_cores = l3_cores();
        identity
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        CpuIdentity::default()
    }
}

#[cfg(any(target_arch = "x86_64", test))]
fn decode_x86_signature(signature: u32) -> (u32, u32, u32) {
    // Intel SDM CPUID leaf 1: display family/model use extensions only for
    // base family 0xF (family), and 0x6 or 0xF (model).
    let base_family = (signature >> 8) & 0xf;
    let family = base_family
        + if base_family == 0xf {
            (signature >> 20) & 0xff
        } else {
            0
        };
    let model = ((signature >> 4) & 0xf)
        | if matches!(base_family, 0x6 | 0xf) {
            (signature >> 12) & 0xf0
        } else {
            0
        };
    (family, model, signature & 0xf)
}

#[cfg(test)]
mod cpu_identity_tests {
    use super::*;

    #[test]
    fn cpuid_display_ids_handle_extended_and_legacy_families() {
        assert_eq!(decode_x86_signature(0x00b40f40), (26, 68, 0));
        assert_eq!(decode_x86_signature(0x000b0671), (6, 183, 1));
        assert_eq!(decode_x86_signature(0x00f70523), (5, 2, 3));
    }
}
