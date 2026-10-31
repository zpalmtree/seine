//! Kernels compiled ahead of time with hipRTC (`--amd-export-code-objects`) and
//! built into the binary, for hosts without hipRTC (Windows with only the AMD
//! display driver installed). Each entry is keyed by kernel, GPU architecture
//! and a hash of the source plus compile options, so a stale or differently
//! configured object is never used.

use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

use super::engine::{
    build_hiprtc_options, compile_hip_source_for_export, skip_from_block, HIP_KERNEL_SRC,
};
use super::hip_ffi::HipApi;

// (kernel, architecture, key, code object)
include!(concat!(env!("OUT_DIR"), "/amd_code_objects.rs"));

/// RDNA targets exported for release builds.
pub(crate) const EXPORT_TARGETS: &[&str] = &[
    "gfx1030", "gfx1031", "gfx1032", "gfx1034", "gfx1035", "gfx1036", "gfx1100", "gfx1101",
    "gfx1102", "gfx1103", "gfx1150", "gfx1151", "gfx1200", "gfx1201",
];

/// The Windows HIP runtime loads code object version 5.
const EXPORT_CODE_OBJECT_VERSION: &str = "-mcode-object-version=5";

/// FNV-1a over the source and every option except the target architecture.
pub(crate) fn options_key(source: &str, options: &[String]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    let mut feed = |bytes: &[u8]| {
        for &byte in bytes.iter().chain(std::iter::once(&0u8)) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    };
    feed(source.as_bytes());
    for option in options {
        if !option.starts_with("--offload-arch=") {
            feed(option.as_bytes());
        }
    }
    hash
}

fn base_arch(gcn_arch_name: &str) -> &str {
    gcn_arch_name.split(':').next().unwrap_or("").trim()
}

/// The built-in code object for this kernel, architecture and options, if any.
pub(crate) fn find(kernel: &str, gcn_arch_name: &str, key: u64) -> Option<&'static [u8]> {
    let arch = base_arch(gcn_arch_name);
    CODE_OBJECTS
        .iter()
        .find(|(name, target, entry_key, _)| {
            *name == kernel && *target == arch && *entry_key == key
        })
        .map(|entry| entry.3)
}

/// Compiles the Argon2 kernel for every export target with hipRTC and writes the
/// code objects plus `manifest.tsv` into `dir` (read by build.rs through
/// SEINE_AMD_CODE_OBJECTS).
pub fn export_code_objects(dir: &Path) -> Result<()> {
    let api = HipApi::load()?;
    if let Err(err) = &api.hiprtc {
        bail!("{err}");
    }
    let params = blocknet_pow_spec::pow_params()
        .map_err(|err| anyhow!("invalid Argon2 parameters: {err}"))?;
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let mut manifest = String::new();
    let (m_blocks, t_cost) = (params.block_count() as u32, params.t_cost());
    for target in EXPORT_TARGETS {
        // The default skip boundary, plus storing every block where the default skips.
        let default_skip_from = skip_from_block(target, m_blocks, t_cost);
        let mut boundaries = vec![default_skip_from];
        if default_skip_from != 0 {
            boundaries.push(0);
        }
        for skip_from in boundaries {
            let options = build_hiprtc_options(target, 32, m_blocks, t_cost, skip_from);
            let key = options_key(HIP_KERNEL_SRC, &options);
            let mut compile_options = options.clone();
            compile_options.push(EXPORT_CODE_OBJECT_VERSION.to_string());
            let code = compile_hip_source_for_export(api, &compile_options)
                .with_context(|| format!("compile the Argon2 kernel for {target}"))?;
            let file = if skip_from == default_skip_from {
                format!("argon2-{target}.co")
            } else {
                format!("argon2-{target}-store-all.co")
            };
            std::fs::write(dir.join(&file), &code)
                .with_context(|| format!("write {}", dir.join(&file).display()))?;
            manifest.push_str(&format!("argon2\t{target}\t{key:016x}\t{file}\n"));
            println!("{file}: {} bytes", code.len());
        }
    }
    std::fs::write(dir.join("manifest.tsv"), manifest)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_ignores_the_target_but_not_other_options() {
        let a = vec!["--offload-arch=gfx1100".to_string(), "-DX=1".to_string()];
        let b = vec!["--offload-arch=gfx1201".to_string(), "-DX=1".to_string()];
        let c = vec!["--offload-arch=gfx1100".to_string(), "-DX=2".to_string()];
        assert_eq!(options_key("src", &a), options_key("src", &b));
        assert_ne!(options_key("src", &a), options_key("src", &c));
        assert_ne!(options_key("src", &a), options_key("src2", &a));
    }

    #[test]
    fn lookup_matches_the_base_architecture() {
        assert_eq!(base_arch("gfx90a:sramecc+:xnack-"), "gfx90a");
        assert_eq!(base_arch("gfx1100"), "gfx1100");
    }
}
