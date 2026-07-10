use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let source_roots = [
        PathBuf::from("build.rs"),
        PathBuf::from("Cargo.toml"),
        PathBuf::from("Cargo.lock"),
        PathBuf::from("rust-toolchain.toml"),
        PathBuf::from("src"),
        PathBuf::from("pow-kernel"),
        PathBuf::from("pow-spec"),
    ];
    emit_rerun_inputs(&source_roots);
    emit_git_rerun_inputs();
    for key in [
        "SEINE_GIT_TAG",
        "BNMINER_GIT_TAG",
        "SEINE_GIT_COMMIT",
        "BNMINER_GIT_COMMIT",
        "GITHUB_REF_TYPE",
        "GITHUB_REF_NAME",
        "GITHUB_SHA",
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
    ] {
        println!("cargo:rerun-if-env-changed={key}");
    }

    if let Some(tag) = first_nonempty_env(&["SEINE_GIT_TAG", "BNMINER_GIT_TAG"])
        .or_else(github_ref_tag)
        .or_else(|| run_git(&["describe", "--tags", "--exact-match", "HEAD"]))
        .or_else(|| run_git(&["describe", "--tags", "--always"]))
    {
        println!("cargo:rustc-env=SEINE_GIT_TAG={tag}");
    }

    if let Some(commit) = first_nonempty_env(&["SEINE_GIT_COMMIT", "BNMINER_GIT_COMMIT"])
        .or_else(github_sha_short)
        .or_else(|| run_git(&["rev-parse", "--short=12", "HEAD"]))
    {
        println!("cargo:rustc-env=SEINE_GIT_COMMIT={commit}");
    }

    emit_env("SEINE_BUILD_TARGET", env::var("TARGET").ok());
    emit_env("SEINE_BUILD_HOST", env::var("HOST").ok());
    emit_env("SEINE_BUILD_PROFILE", env::var("PROFILE").ok());
    emit_env("SEINE_BUILD_OPT_LEVEL", env::var("OPT_LEVEL").ok());
    emit_env(
        "SEINE_BUILD_RUSTFLAGS",
        first_nonempty_env(&["CARGO_ENCODED_RUSTFLAGS", "RUSTFLAGS"])
            .map(|flags| flags.replace('\u{1f}', " ")),
    );
    emit_env("SEINE_RUSTC_VERSION", rustc_version());
    emit_env("SEINE_BUILD_FEATURES", cargo_features());
    emit_env(
        "SEINE_SOURCE_FINGERPRINT",
        source_fingerprint(&source_roots),
    );
}

fn emit_env(key: &str, value: Option<String>) {
    if let Some(value) = value {
        let value = value.replace(['\r', '\n'], " ");
        println!("cargo:rustc-env={key}={value}");
    }
}

fn first_nonempty_env(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        env::var(key).ok().and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
    })
}

fn run_git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn rustc_version() -> Option<String> {
    let rustc = env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let output = Command::new(rustc).arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|version| version.trim().to_string())
        .filter(|version| !version.is_empty())
}

fn cargo_features() -> Option<String> {
    let mut features = env::vars()
        .filter_map(|(key, value)| {
            if value == "1" {
                key.strip_prefix("CARGO_FEATURE_")
                    .map(|feature| feature.to_ascii_lowercase().replace('_', "-"))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    features.sort();
    Some(features.join(","))
}

fn source_fingerprint(roots: &[PathBuf]) -> Option<String> {
    let mut files = Vec::new();
    for root in roots {
        collect_source_files(root, &mut files);
    }
    files.sort();

    let mut hash = 0xcbf29ce484222325u64;
    for path in files {
        hash = hash_source_path(hash, &path);
        hash = fnv1a_step(hash, 0);
        let contents = fs::read(&path).ok()?;
        hash = hash_source_contents(hash, &contents);
        hash = fnv1a_step(hash, 0xff);
    }
    Some(format!("{hash:016x}"))
}

fn hash_source_path(mut hash: u64, path: &Path) -> u64 {
    // Git may check the same tree out with platform-native separators. Build
    // identity should describe the source tree, not the host path syntax.
    for byte in path.to_string_lossy().replace('\\', "/").bytes() {
        hash = fnv1a_step(hash, byte);
    }
    hash
}

fn hash_source_contents(mut hash: u64, contents: &[u8]) -> u64 {
    // core.autocrlf can materialize committed LF text as CRLF on Windows.
    // Normalize only CRLF pairs so identical Git content fingerprints equally
    // without hiding meaningful standalone carriage returns.
    let mut index = 0usize;
    while index < contents.len() {
        if contents[index] == b'\r' && contents.get(index + 1) == Some(&b'\n') {
            hash = fnv1a_step(hash, b'\n');
            index += 2;
        } else {
            hash = fnv1a_step(hash, contents[index]);
            index += 1;
        }
    }
    hash
}

fn emit_rerun_inputs(roots: &[PathBuf]) {
    let mut files = Vec::new();
    for root in roots {
        collect_source_files(root, &mut files);
    }
    files.sort();
    files.dedup();
    for file in files {
        println!("cargo:rerun-if-changed={}", file.display());
    }
}

fn emit_git_rerun_inputs() {
    if let Some(head_path) = run_git(&["rev-parse", "--git-path", "HEAD"]) {
        println!("cargo:rerun-if-changed={head_path}");
    }
    if let Some(symbolic_ref) = run_git(&["symbolic-ref", "-q", "HEAD"])
        .and_then(|reference| run_git(&["rev-parse", "--git-path", &reference]))
    {
        println!("cargo:rerun-if-changed={symbolic_ref}");
    }
}

fn collect_source_files(path: &Path, files: &mut Vec<PathBuf>) {
    if path.is_file() {
        files.push(path.to_path_buf());
        return;
    }

    let Ok(entries) = fs::read_dir(path) else {
        return;
    };
    for entry in entries.flatten() {
        let child = entry.path();
        if child.is_dir() {
            collect_source_files(&child, files);
        } else if is_build_input(&child) {
            files.push(child);
        }
    }
}

fn is_build_input(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("rs" | "cu" | "metal" | "toml" | "lock")
    )
}

fn fnv1a_step(hash: u64, byte: u8) -> u64 {
    (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
}

fn github_ref_tag() -> Option<String> {
    let is_tag = env::var("GITHUB_REF_TYPE").ok()?.trim().eq("tag");
    if !is_tag {
        return None;
    }
    first_nonempty_env(&["GITHUB_REF_NAME"])
}

fn github_sha_short() -> Option<String> {
    let sha = first_nonempty_env(&["GITHUB_SHA"])?;
    let short = sha.chars().take(12).collect::<String>();
    if short.is_empty() {
        None
    } else {
        Some(short)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;

    #[test]
    fn source_path_hash_normalizes_platform_separators() {
        assert_eq!(
            hash_source_path(OFFSET_BASIS, Path::new("src/main.rs")),
            hash_source_path(OFFSET_BASIS, Path::new(r"src\main.rs"))
        );
    }

    #[test]
    fn source_content_hash_normalizes_crlf() {
        assert_eq!(
            hash_source_contents(OFFSET_BASIS, b"first\nsecond\n"),
            hash_source_contents(OFFSET_BASIS, b"first\r\nsecond\r\n")
        );
        assert_ne!(
            hash_source_contents(OFFSET_BASIS, b"first\rsecond\n"),
            hash_source_contents(OFFSET_BASIS, b"first\nsecond\n")
        );
    }
}
