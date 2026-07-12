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
