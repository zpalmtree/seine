//! Self-update from GitHub releases.
//!
//! `seine --self-update` replaces this executable with the newest stable release
//! for the running platform and exits. `--auto-update` does the same unattended
//! when the periodic release check finds a newer version, then restarts the
//! miner with its original arguments.
//!
//! Trust model: the release's `SHA256SUMS.txt` must carry an Ed25519 signature
//! (`SHA256SUMS.txt.sig`, raw 64 bytes over the file's exact bytes) that verifies
//! against the release signing key embedded below, and the downloaded archive's
//! SHA-256 must match that signed list. A release without a valid signature is
//! refused, so control of the GitHub account or a tampered download cannot push
//! a binary; only the holder of the signing key can.
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const RELEASE_API_URL: &str = "https://api.github.com/repos/zpalmtree/seine/releases/latest";
const CHECKSUMS_ASSET: &str = "SHA256SUMS.txt";
const SIGNATURE_ASSET: &str = "SHA256SUMS.txt.sig";
/// Raw Ed25519 public key of the seine release signing key (generated
/// 2026-10-28; the private key lives outside the repository). Rotate by
/// shipping a release signed by both keys, then switching this constant.
const RELEASE_SIGNING_PUBLIC_KEY_HEX: &str =
    "f60ad032847909721c79cc299fe126653119f43d1ee50864a566d08bef0aecee";
/// Checksum lists and signatures are tiny; refuse anything larger.
const MAX_TEXT_ASSET_BYTES: usize = 64 * 1024;
/// Release archives are a few MiB; refuse anything that could fill a disk.
const MAX_ARCHIVE_BYTES: u64 = 256 * 1024 * 1024;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(10 * 60);

static RESTART_PENDING: AtomicBool = AtomicBool::new(false);
static UPDATED_EXECUTABLE: OnceLock<PathBuf> = OnceLock::new();

#[derive(Deserialize)]
struct Release {
    tag_name: String,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    prerelease: bool,
    #[serde(default)]
    assets: Vec<Asset>,
}

#[derive(Clone, Deserialize)]
struct Asset {
    name: String,
    browser_download_url: String,
    #[serde(default)]
    size: u64,
}

pub(crate) enum Outcome {
    UpToDate {
        current: String,
        latest: String,
    },
    Updated {
        from: String,
        to: String,
        path: PathBuf,
    },
}

/// The release archive built for this platform, named as in the release workflow.
pub(crate) fn asset_name() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Some("seine-linux-x86_64.tar.gz"),
        ("linux", "aarch64") => Some("seine-linux-aarch64.tar.gz"),
        ("windows", "x86_64") => Some("seine-windows-x86_64.zip"),
        ("macos", "x86_64") => Some("seine-macos-x86_64.tar.gz"),
        ("macos", "aarch64") => Some("seine-macos-aarch64.tar.gz"),
        _ => None,
    }
}

fn executable_name() -> &'static str {
    if cfg!(windows) {
        "seine.exe"
    } else {
        "seine"
    }
}

/// `seine --self-update`: update in place and report, without starting the miner.
pub(crate) fn run_cli() -> Result<()> {
    cleanup_leftovers();
    match run()? {
        Outcome::UpToDate { current, latest } => {
            crate::console_output::println!(
                "seine {current} is the newest release ({latest}); nothing to do"
            );
        }
        Outcome::Updated { from, to, path } => {
            crate::console_output::println!(
                "updated seine {from} -> {to} at {}; start seine again to use it",
                path.display()
            );
        }
    }
    Ok(())
}

/// Fetches the newest stable release and, when it is newer than this build,
/// replaces the running executable with it.
pub(crate) fn run() -> Result<Outcome> {
    let current = crate::version::release_version().to_string();
    let client = http_client()?;
    let exe = std::env::current_exe().context("cannot locate the running executable")?;
    install_release(
        &client,
        RELEASE_API_URL,
        RELEASE_SIGNING_PUBLIC_KEY_HEX,
        current,
        &exe,
    )
}

fn install_release(
    client: &reqwest::blocking::Client,
    release_url: &str,
    public_key: &str,
    current: String,
    exe: &Path,
) -> Result<Outcome> {
    let release: Release = client
        .get(release_url)
        .header("Accept", "application/vnd.github+json")
        .send()
        .context("release lookup failed")?
        .error_for_status()
        .context("release lookup rejected")?
        .json()
        .context("invalid release response")?;
    if release.draft || release.prerelease {
        bail!(
            "the newest release {} is not a stable version",
            release.tag_name
        );
    }
    if !crate::update_check::is_newer(&release.tag_name, &current) {
        return Ok(Outcome::UpToDate {
            current,
            latest: release.tag_name,
        });
    }
    let name = asset_name().ok_or_else(|| {
        anyhow!(
            "no release build for {}-{}; build from source instead",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;
    let asset = find_asset(&release.assets, name)?;
    let checksums = find_asset(&release.assets, CHECKSUMS_ASSET)?;
    let signature = find_asset(&release.assets, SIGNATURE_ASSET).map_err(|_| {
        anyhow!(
            "release {} has no {SIGNATURE_ASSET}; refusing to install an unsigned release",
            release.tag_name
        )
    })?;
    let checksum_bytes = download_bytes(&client, &checksums.browser_download_url)?;
    let signature_bytes = download_bytes(&client, &signature.browser_download_url)?;
    verify_release_signature(&checksum_bytes, &signature_bytes, public_key).with_context(|| {
        format!(
            "release {} is not signed by the seine release key",
            release.tag_name
        )
    })?;
    let checksum_text =
        String::from_utf8(checksum_bytes).context("checksum list is not UTF-8 text")?;
    let expected = expected_sha256(&checksum_text, name)?;

    let dir = exe
        .parent()
        .ok_or_else(|| anyhow!("executable {} has no parent directory", exe.display()))?;
    let archive = dir.join(format!(".seine-update-{name}"));
    let staged = dir.join(format!(".seine-update-{}", executable_name()));
    let result = stage_update(client, asset, &expected, &archive, &staged)
        .and_then(|()| replace_executable(exe, &staged));
    let _ = fs::remove_file(&archive);
    if result.is_err() {
        let _ = fs::remove_file(&staged);
    }
    result?;
    // The OS may report a deleted or renamed image after replacement.
    let _ = UPDATED_EXECUTABLE.set(exe.to_path_buf());
    Ok(Outcome::Updated {
        from: current,
        to: release.tag_name,
        path: exe.to_path_buf(),
    })
}

/// Marks that the process should re-exec itself once mining has stopped.
pub(crate) fn request_restart() {
    RESTART_PENDING.store(true, Ordering::Release);
}

pub(crate) fn restart_pending() -> bool {
    RESTART_PENDING.load(Ordering::Acquire)
}

/// Starts the (updated) executable with this process's arguments. On Unix this
/// replaces the process image; on Windows it spawns the new process and exits.
pub(crate) fn restart() -> Result<()> {
    let exe = match UPDATED_EXECUTABLE.get() {
        Some(path) => path.clone(),
        None => std::env::current_exe().context("cannot locate the running executable")?,
    };
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let mut command = std::process::Command::new(&exe);
    command.args(&args);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let err = command.exec();
        Err(anyhow!("restart failed: {err}"))
    }
    #[cfg(not(unix))]
    {
        command
            .spawn()
            .with_context(|| format!("restart of {} failed", exe.display()))?;
        std::process::exit(0);
    }
}

/// Removes the previous executable Windows could not delete while it ran.
pub(crate) fn cleanup_leftovers() {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let _ = fs::remove_file(dir.join(old_executable_name()));
        }
    }
}

fn old_executable_name() -> &'static str {
    if cfg!(windows) {
        "seine.old.exe"
    } else {
        "seine.old"
    }
}

fn http_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .timeout(DOWNLOAD_TIMEOUT)
        .user_agent(format!("seine/{}", crate::version::release_version()))
        .build()
        .context("cannot build the download client")
}

fn find_asset<'a>(assets: &'a [Asset], name: &str) -> Result<&'a Asset> {
    assets
        .iter()
        .find(|asset| asset.name == name)
        .ok_or_else(|| anyhow!("the newest release has no {name} asset"))
}

fn download_bytes(client: &reqwest::blocking::Client, url: &str) -> Result<Vec<u8>> {
    let bytes = client
        .get(url)
        .send()
        .with_context(|| format!("download of {url} failed"))?
        .error_for_status()
        .with_context(|| format!("download of {url} rejected"))?
        .bytes()
        .with_context(|| format!("download of {url} was cut short"))?;
    if bytes.len() > MAX_TEXT_ASSET_BYTES {
        bail!("{url} is implausibly large ({} bytes)", bytes.len());
    }
    Ok(bytes.to_vec())
}

/// Checks the raw Ed25519 signature over `message` against the hex-encoded
/// release public key.
fn verify_release_signature(message: &[u8], signature: &[u8], public_key_hex: &str) -> Result<()> {
    let public_key = hex::decode(public_key_hex).context("release signing key is not hex")?;
    if public_key.len() != 32 {
        bail!(
            "release signing key has {} bytes, expected 32",
            public_key.len()
        );
    }
    if signature.len() != 64 {
        bail!(
            "{SIGNATURE_ASSET} has {} bytes, expected a raw 64-byte Ed25519 signature",
            signature.len()
        );
    }
    ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, public_key)
        .verify(message, signature)
        .map_err(|_| anyhow!("signature does not verify"))
}

fn stage_update(
    client: &reqwest::blocking::Client,
    asset: &Asset,
    expected_sha256: &str,
    archive: &Path,
    staged: &Path,
) -> Result<()> {
    if asset.size > MAX_ARCHIVE_BYTES {
        bail!(
            "release asset {} is implausibly large ({} bytes)",
            asset.name,
            asset.size
        );
    }
    download_file(client, &asset.browser_download_url, archive)?;
    let actual = sha256_file(archive)?;
    if actual != expected_sha256 {
        bail!(
            "checksum mismatch for {}: release lists {expected_sha256}, download has {actual}",
            asset.name
        );
    }
    extract_executable(archive, staged)
        .with_context(|| format!("cannot extract {} from {}", executable_name(), asset.name))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(staged, fs::Permissions::from_mode(0o755))
            .context("cannot mark the new executable runnable")?;
    }
    Ok(())
}

fn download_file(client: &reqwest::blocking::Client, url: &str, target: &Path) -> Result<()> {
    let mut response = client
        .get(url)
        .send()
        .with_context(|| format!("download of {url} failed"))?
        .error_for_status()
        .with_context(|| format!("download of {url} rejected"))?;
    let mut file =
        fs::File::create(target).with_context(|| format!("cannot write {}", target.display()))?;
    let mut copied = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = response.read(&mut buffer).context("download interrupted")?;
        if read == 0 {
            break;
        }
        copied += read as u64;
        if copied > MAX_ARCHIVE_BYTES {
            bail!("download exceeded {MAX_ARCHIVE_BYTES} bytes");
        }
        file.write_all(&buffer[..read])
            .with_context(|| format!("cannot write {}", target.display()))?;
    }
    file.flush()?;
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// The lowercase SHA-256 listed for `name` in `sha256sum` output.
fn expected_sha256(checksums: &str, name: &str) -> Result<String> {
    checksums
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let hash = parts.next()?;
            let file = parts.next()?.trim_start_matches('*');
            (file == name && hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()))
                .then(|| hash.to_ascii_lowercase())
        })
        .next()
        .ok_or_else(|| anyhow!("{CHECKSUMS_ASSET} lists no checksum for {name}"))
}

fn extract_executable(archive: &Path, target: &Path) -> Result<()> {
    let wanted = executable_name();
    let file = fs::File::open(archive)?;
    if archive
        .extension()
        .is_some_and(|extension| extension == "zip")
    {
        let mut zip = zip::ZipArchive::new(file)?;
        for index in 0..zip.len() {
            let mut entry = zip.by_index(index)?;
            let is_wanted = entry
                .enclosed_name()
                .and_then(|path| path.file_name().map(|name| name == wanted))
                .unwrap_or(false);
            if is_wanted && entry.is_file() {
                let mut out = fs::File::create(target)?;
                std::io::copy(&mut entry, &mut out)?;
                return Ok(());
            }
        }
    } else {
        let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(file));
        for entry in tar.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.into_owned();
            if path.file_name().is_some_and(|name| name == wanted)
                && entry.header().entry_type().is_file()
            {
                let mut out = fs::File::create(target)?;
                std::io::copy(&mut entry, &mut out)?;
                return Ok(());
            }
        }
    }
    bail!("{wanted} is not in the archive")
}

/// Swaps the staged executable in. The running binary stays usable until the
/// rename lands; Windows keeps it as `seine.old.exe` because a running image
/// cannot be deleted, and `cleanup_leftovers` removes it on the next start.
fn replace_executable(exe: &Path, staged: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        let dir = exe
            .parent()
            .ok_or_else(|| anyhow!("executable has no directory"))?;
        let old = dir.join(old_executable_name());
        let _ = fs::remove_file(&old);
        fs::rename(exe, &old).with_context(|| format!("cannot move {} aside", exe.display()))?;
        if let Err(err) = fs::rename(staged, exe) {
            let _ = fs::rename(&old, exe);
            return Err(err).with_context(|| format!("cannot install {}", exe.display()));
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        fs::rename(staged, exe).with_context(|| format!("cannot install {}", exe.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_signing_key_is_a_32_byte_ed25519_key() {
        assert_eq!(
            hex::decode(RELEASE_SIGNING_PUBLIC_KEY_HEX).unwrap().len(),
            32
        );
    }

    #[test]
    fn signature_verification_accepts_the_signing_key_and_rejects_others() {
        use ring::signature::{Ed25519KeyPair, KeyPair};
        let rng = ring::rand::SystemRandom::new();
        let key =
            Ed25519KeyPair::from_pkcs8(Ed25519KeyPair::generate_pkcs8(&rng).unwrap().as_ref())
                .unwrap();
        let other =
            Ed25519KeyPair::from_pkcs8(Ed25519KeyPair::generate_pkcs8(&rng).unwrap().as_ref())
                .unwrap();
        let message = b"0123  seine-linux-x86_64.tar.gz\n";
        let signature = key.sign(message);
        let public_hex = hex::encode(key.public_key().as_ref());
        let other_hex = hex::encode(other.public_key().as_ref());
        assert!(verify_release_signature(message, signature.as_ref(), &public_hex).is_ok());
        assert!(verify_release_signature(message, signature.as_ref(), &other_hex).is_err());
        let mut tampered = message.to_vec();
        tampered[0] ^= 1;
        assert!(verify_release_signature(&tampered, signature.as_ref(), &public_hex).is_err());
        assert!(verify_release_signature(message, &signature.as_ref()[..63], &public_hex).is_err());
        assert!(verify_release_signature(message, signature.as_ref(), "abcd").is_err());
    }

    #[test]
    fn reads_the_matching_checksum_line_only() {
        let sums = "abc  other.tar.gz\n\
                    0123456789abcdef0123456789abcdef0123456789abcdef0123456789ABCDEF  seine-linux-x86_64.tar.gz\n\
                    deadbeef *seine-windows-x86_64.zip\n";
        assert_eq!(
            expected_sha256(sums, "seine-linux-x86_64.tar.gz").unwrap(),
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        assert!(
            expected_sha256(sums, "seine-windows-x86_64.zip").is_err(),
            "short hash"
        );
        assert!(expected_sha256(sums, "seine-macos-aarch64.tar.gz").is_err());
    }

    #[test]
    fn extracts_the_executable_from_a_release_style_archive() {
        let dir = std::env::temp_dir().join(format!("seine-self-update-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let archive = dir.join("seine-test.tar.gz");
        {
            let file = fs::File::create(&archive).unwrap();
            let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
            let mut builder = tar::Builder::new(encoder);
            let mut header = tar::Header::new_gnu();
            header.set_size(5);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, "./README.md", &b"hello"[..])
                .unwrap();
            let payload = b"#!/bin/sh\n";
            let mut header = tar::Header::new_gnu();
            header.set_size(payload.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            builder
                .append_data(
                    &mut header,
                    format!("./{}", executable_name()),
                    &payload[..],
                )
                .unwrap();
            builder.into_inner().unwrap().finish().unwrap();
        }
        let staged = dir.join("staged");
        extract_executable(&archive, &staged).unwrap();
        assert_eq!(fs::read(&staged).unwrap(), b"#!/bin/sh\n");
        assert_eq!(sha256_file(&staged).unwrap().len(), 64);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn asset_name_matches_the_release_workflow_on_this_platform() {
        let name = asset_name().expect("release builds exist for CI platforms");
        assert!(name.starts_with("seine-"));
        assert!(name.ends_with(".tar.gz") || name.ends_with(".zip"));
    }

    #[test]
    fn restart_fixture_child() {
        let Some(directory) = std::env::var_os("SEINE_TEST_UPDATE_DIRECTORY") else {
            return;
        };
        let directory = PathBuf::from(directory);
        let config = directory.join("seine.config.json");
        assert_eq!(
            fs::read_to_string(&config).unwrap(),
            "{\"payout_memo\":\"0001\",\"pool_worker\":\"saved worker\"}"
        );
        let args = std::env::args_os().collect::<Vec<_>>();
        assert!(args
            .iter()
            .any(|arg| arg == "self_update::tests::restart_fixture_child"));
        if directory.join("installed").exists() {
            cleanup_leftovers();
            fs::write(directory.join("restarted"), "ok").unwrap();
            return;
        }
        let client = http_client().unwrap();
        let result = install_release(
            &client,
            &std::env::var("SEINE_TEST_UPDATE_URL").unwrap(),
            &std::env::var("SEINE_TEST_UPDATE_KEY").unwrap(),
            "0.0.0".into(),
            &std::env::current_exe().unwrap(),
        )
        .unwrap();
        assert!(matches!(result, Outcome::Updated { .. }));
        fs::write(directory.join("installed"), "ok").unwrap();
        request_restart();
        assert!(restart_pending());
        restart().unwrap();
    }

    #[test]
    fn signed_install_rejects_tampering_and_restarts_with_saved_settings() {
        use httpmock::MockServer;
        use ring::signature::{Ed25519KeyPair, KeyPair};
        use std::process::{Command, Stdio};
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let exe = root.join(executable_name());
        fs::copy(std::env::current_exe().unwrap(), &exe).unwrap();
        let archive = root.join(asset_name().unwrap());
        if cfg!(windows) {
            let mut zip = zip::ZipWriter::new(fs::File::create(&archive).unwrap());
            zip.start_file(executable_name(), zip::write::SimpleFileOptions::default())
                .unwrap();
            std::io::copy(&mut fs::File::open(&exe).unwrap(), &mut zip).unwrap();
            zip.finish().unwrap();
        } else {
            let encoder = flate2::write::GzEncoder::new(
                fs::File::create(&archive).unwrap(),
                flate2::Compression::fast(),
            );
            let mut builder = tar::Builder::new(encoder);
            builder
                .append_path_with_name(&exe, executable_name())
                .unwrap();
            builder.into_inner().unwrap().finish().unwrap();
        }
        let server = MockServer::start();
        let rng = ring::rand::SystemRandom::new();
        let key =
            Ed25519KeyPair::from_pkcs8(Ed25519KeyPair::generate_pkcs8(&rng).unwrap().as_ref())
                .unwrap();
        let public_key = hex::encode(key.public_key().as_ref());
        let name = asset_name().unwrap();
        let checksum = format!("{}  {name}\n", sha256_file(&archive).unwrap());
        let signature = key.sign(checksum.as_bytes());
        server.mock(|when, then| {
            when.path("/release");
            then.json_body(serde_json::json!({"tag_name":"v99.0.0", "assets": [
                {"name":name, "browser_download_url":server.url("/archive")},
                {"name":CHECKSUMS_ASSET,"browser_download_url":server.url("/checksums")},
                {"name":SIGNATURE_ASSET,"browser_download_url":server.url("/signature")}
            ]}));
        });
        server.mock(|when, then| {
            when.path("/checksums");
            then.body(checksum.as_bytes());
        });
        let mut bad_signature = server.mock(|when, then| {
            when.path("/signature");
            then.body(vec![0; 64]);
        });
        let client = http_client().unwrap();
        let before = sha256_file(&exe).unwrap();
        assert!(install_release(
            &client,
            &server.url("/release"),
            &public_key,
            "0.0.0".into(),
            &exe
        )
        .is_err());
        assert_eq!(sha256_file(&exe).unwrap(), before);
        bad_signature.delete();
        server.mock(|when, then| {
            when.path("/signature");
            then.body(signature.as_ref());
        });
        let mut bad_archive = server.mock(|when, then| {
            when.path("/archive");
            then.body("tampered");
        });
        let error = install_release(
            &client,
            &server.url("/release"),
            &public_key,
            "0.0.0".into(),
            &exe,
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("checksum mismatch"));
        assert_eq!(sha256_file(&exe).unwrap(), before);
        bad_archive.delete();
        server.mock(|when, then| {
            when.path("/archive");
            then.body(fs::read(&archive).unwrap());
        });
        fs::write(
            root.join("seine.config.json"),
            "{\"payout_memo\":\"0001\",\"pool_worker\":\"saved worker\"}",
        )
        .unwrap();
        let mut child = Command::new(&exe)
            .args([
                "--exact",
                "self_update::tests::restart_fixture_child",
                "--nocapture",
            ])
            .env("SEINE_TEST_UPDATE_DIRECTORY", root)
            .env("SEINE_TEST_UPDATE_URL", server.url("/release"))
            .env("SEINE_TEST_UPDATE_KEY", public_key)
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while std::time::Instant::now() < deadline && !root.join("restarted").exists() {
            std::thread::sleep(Duration::from_millis(50));
        }
        if !root.join("restarted").exists() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("updated process did not restart");
        }
        assert!(child.wait().unwrap().success());
        // On Windows the restarted process is a child of the original process.
        // Let its test harness release the image before removing the fixture.
        for _ in 0..100 {
            if fs::remove_file(&exe).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}
