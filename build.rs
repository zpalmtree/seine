use std::env;
use std::process::Command;

fn main() {
    if let Some(tag) = first_nonempty_env(&["SEINE_GIT_TAG", "BNMINER_GIT_TAG"])
        .or_else(github_ref_tag)
        .or_else(|| run_git(&["describe", "--tags", "--exact-match", "HEAD"]))
    {
        println!("cargo:rustc-env=SEINE_GIT_TAG={tag}");
    }

    if let Some(commit) = first_nonempty_env(&["SEINE_GIT_COMMIT", "BNMINER_GIT_COMMIT"])
        .or_else(github_sha_short)
        .or_else(|| run_git(&["rev-parse", "--short=12", "HEAD"]))
    {
        println!("cargo:rustc-env=SEINE_GIT_COMMIT={commit}");
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
