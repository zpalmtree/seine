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
//! refused, so a tampered download or a replaced release file cannot push a
//! binary. The release workflow signs from a GitHub environment that needs an
//! approval, so anyone who can approve or reconfigure that environment, or who
//! holds the key, can sign.
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
/// Web page that redirects to the newest stable release's tag. Used when the
/// API refuses the lookup (it allows 60 unauthenticated requests per hour per
/// IP, which rigs sharing one address can exhaust).
const RELEASE_WEB_LATEST_URL: &str = "https://github.com/zpalmtree/seine/releases/latest";
const RELEASE_DOWNLOAD_BASE_URL: &str = "https://github.com/zpalmtree/seine/releases/download";
/// Timeout for the release lookup, not the archive download.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
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
/// How long the new executable may take to report its version before the
/// update is abandoned.
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

/// Checks a staged executable before it replaces the running one.
type VerifyExecutable = fn(&Path, &str) -> Result<()>;

static RESTART_PENDING: AtomicBool = AtomicBool::new(false);
/// Set for a Windows process started by `restart`, whose parent relaunches it.
#[cfg(not(unix))]
const SUPERVISED_ENV: &str = "SEINE_RESTART_SUPERVISED";
/// Exit code a supervised Windows process uses to ask for a relaunch.
#[cfg(not(unix))]
const RESTART_EXIT_CODE: i32 = 0x5345;
/// Spare names for a previous executable that is still running.
#[cfg(windows)]
const OLD_EXECUTABLE_SLOTS: u32 = 16;
static UPDATED_EXECUTABLE: OnceLock<PathBuf> = OnceLock::new();
/// The previous executable, kept beside the new one after an update.
static PREVIOUS_EXECUTABLE: OnceLock<PathBuf> = OnceLock::new();
/// Beside the executable from an automatic update until the new version has
/// started once: the new tag and the previous executable's file name, one per
/// line.
const ROLLBACK_MARKER: &str = ".seine-rollback";
/// Set on the process an update restarts into. Only that start can go back to
/// the previous version; a later start by hand with a mistyped flag cannot.
const STARTED_BY_UPDATE_ENV: &str = "SEINE_STARTED_BY_UPDATE";
/// Beside the executable: `<tag> <count>`, the release whose first start
/// failed and how often. Kept beside the executable, which an update has
/// just written, so it holds even where the data directory is read-only.
const FAILED_START_FILE: &str = ".seine-failed-start";
/// Failed first starts after which a release is no longer installed
/// automatically, even when mining is paused for it.
const FAILED_STARTS_BEFORE_GIVING_UP: u32 = 2;

/// The newest stable release as found by a lookup.
#[derive(Deserialize)]
pub(crate) struct Release {
    tag_name: String,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    prerelease: bool,
    #[serde(default)]
    assets: Vec<Asset>,
}

impl Release {
    pub(crate) fn tag(&self) -> &str {
        &self.tag_name
    }
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
    install(latest_release()?)
}

/// The path of the running executable. Linux reports a replaced image as
/// `<path> (deleted)`; this gives the path the executable is installed at.
pub(crate) fn running_exe() -> std::io::Result<PathBuf> {
    let exe = std::env::current_exe()?;
    Ok(installed_path(exe))
}

fn installed_path(exe: PathBuf) -> PathBuf {
    if cfg!(target_os = "linux") && !exe.exists() {
        if let Some(original) = exe
            .to_str()
            .and_then(|path| path.strip_suffix(" (deleted)"))
            .map(PathBuf::from)
        {
            return original;
        }
    }
    exe
}

/// Installs `release` (from `latest_release`) when it is newer than this build.
pub(crate) fn install(release: Release) -> Result<Outcome> {
    let current = crate::version::release_version().to_string();
    let client = http_client()?;
    let exe = running_exe().context("cannot locate the running executable")?;
    if let Some(outcome) = installed_by_another(&release, &current, &exe, probe_executable) {
        return Ok(outcome);
    }
    install_fetched(
        &client,
        release,
        RELEASE_SIGNING_PUBLIC_KEY_HEX,
        current,
        &exe,
        probe_executable,
    )
}

/// Another seine sharing this executable already installed `release`: this
/// process restarts into it and leaves the previous version that one kept.
fn installed_by_another(
    release: &Release,
    current: &str,
    exe: &Path,
    verify: VerifyExecutable,
) -> Option<Outcome> {
    if !crate::update_check::is_newer(&release.tag_name, current)
        || verify(exe, &release.tag_name).is_err()
    {
        return None;
    }
    let _ = UPDATED_EXECUTABLE.set(exe.to_path_buf());
    Some(Outcome::Updated {
        from: current.to_string(),
        to: release.tag_name.clone(),
        path: exe.to_path_buf(),
    })
}

/// Looks up the newest stable release through the GitHub API, falling back to
/// the release web page's redirect when the API fails or rate-limits.
pub(crate) fn latest_release() -> Result<Release> {
    use crate::diagnostics::{update_checked, UpdateCheck};
    let client = lookup_client(reqwest::redirect::Policy::default())?;
    match lookup_release_api(&client, RELEASE_API_URL) {
        Ok(release) => {
            update_checked(UpdateCheck::Api);
            Ok(release)
        }
        Err(api_err) => {
            let client = lookup_client(reqwest::redirect::Policy::none())?;
            let release =
                lookup_release_web(&client, RELEASE_WEB_LATEST_URL, RELEASE_DOWNLOAD_BASE_URL)
                    .map_err(|web_err| anyhow!("{api_err:#}; release page: {web_err:#}"));
            update_checked(if release.is_ok() {
                UpdateCheck::Web
            } else {
                UpdateCheck::Failed
            });
            release
        }
    }
}

/// Records a failed install step for telemetry, passing the result through.
fn at_stage<T>(stage: seine_telemetry::UpdateFailure, result: Result<T>) -> Result<T> {
    if result.is_err() {
        crate::diagnostics::update_install_failed(stage);
    }
    result
}

fn lookup_client(redirect: reqwest::redirect::Policy) -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .timeout(LOOKUP_TIMEOUT)
        .redirect(redirect)
        .user_agent(format!("seine/{}", crate::version::release_version()))
        .build()
        .context("cannot build the release lookup client")
}

fn lookup_release_api(client: &reqwest::blocking::Client, url: &str) -> Result<Release> {
    client
        .get(url)
        .header("Accept", "application/vnd.github+json")
        .send()
        .context("release lookup failed")?
        .error_for_status()
        .context("release lookup rejected")?
        .json()
        .context("invalid release response")
}

/// Reads the tag from the `releases/latest` redirect (GitHub excludes drafts
/// and prereleases there) and names the assets by their download URLs. The
/// signature and checksum checks are the same as for an API lookup.
fn lookup_release_web(
    client: &reqwest::blocking::Client,
    latest_url: &str,
    download_base: &str,
) -> Result<Release> {
    let response = client
        .get(latest_url)
        .send()
        .context("release page unavailable")?;
    if !response.status().is_redirection() {
        bail!("release page returned {}", response.status());
    }
    let location = response
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| anyhow!("release page did not redirect to a release"))?;
    let tag = tag_from_release_location(location)
        .ok_or_else(|| anyhow!("release page redirected to {location}"))?;
    let mut names = vec![CHECKSUMS_ASSET, SIGNATURE_ASSET];
    names.extend(asset_name());
    let assets = names
        .into_iter()
        .map(|name| Asset {
            name: name.to_string(),
            browser_download_url: format!("{download_base}/{tag}/{name}"),
            size: 0,
        })
        .collect();
    Ok(Release {
        tag_name: tag,
        draft: false,
        prerelease: false,
        assets,
    })
}

/// The tag in a `.../releases/tag/<tag>` URL, if it looks like a version.
fn tag_from_release_location(location: &str) -> Option<String> {
    let tag = location
        .split(['?', '#'])
        .next()?
        .rsplit_once("/releases/tag/")?
        .1
        .trim_end_matches('/');
    (crate::update_check::version_triple(tag).is_some()
        && tag
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'+')))
    .then(|| tag.to_string())
}

#[cfg(test)]
fn install_release(
    client: &reqwest::blocking::Client,
    release_url: &str,
    public_key: &str,
    current: String,
    exe: &Path,
    verify: VerifyExecutable,
) -> Result<Outcome> {
    let release = lookup_release_api(client, release_url)?;
    install_fetched(client, release, public_key, current, exe, verify)
}

fn install_fetched(
    client: &reqwest::blocking::Client,
    release: Release,
    public_key: &str,
    current: String,
    exe: &Path,
    verify: VerifyExecutable,
) -> Result<Outcome> {
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
    use seine_telemetry::UpdateFailure;
    let name = at_stage(
        UpdateFailure::NoBuild,
        asset_name().ok_or_else(|| {
            anyhow!(
                "no release build for {}-{}; build from source instead",
                std::env::consts::OS,
                std::env::consts::ARCH
            )
        }),
    )?;
    let (asset, checksums, signature) = at_stage(
        UpdateFailure::MissingAsset,
        (|| {
            let asset = find_asset(&release.assets, name)?;
            let checksums = find_asset(&release.assets, CHECKSUMS_ASSET)?;
            let signature = find_asset(&release.assets, SIGNATURE_ASSET).map_err(|_| {
                anyhow!(
                    "release {} has no {SIGNATURE_ASSET}; refusing to install an unsigned release",
                    release.tag_name
                )
            })?;
            Ok((asset, checksums, signature))
        })(),
    )?;
    let (checksum_bytes, signature_bytes) = at_stage(
        UpdateFailure::Download,
        (|| {
            Ok((
                download_bytes(client, &checksums.browser_download_url)?,
                download_bytes(client, &signature.browser_download_url)?,
            ))
        })(),
    )?;
    at_stage(
        UpdateFailure::Signature,
        verify_release_signature(&checksum_bytes, &signature_bytes, public_key).with_context(
            || {
                format!(
                    "release {} is not signed by the seine release key",
                    release.tag_name
                )
            },
        ),
    )?;
    let expected = at_stage(
        UpdateFailure::Checksum,
        (|| {
            let checksum_text =
                String::from_utf8(checksum_bytes).context("checksum list is not UTF-8 text")?;
            expected_sha256(&checksum_text, name)
        })(),
    )?;

    let dir = exe
        .parent()
        .ok_or_else(|| anyhow!("executable {} has no parent directory", exe.display()))?;
    // Per process, so updaters sharing the directory never write one file.
    let pid = std::process::id();
    let archive = dir.join(format!(".seine-update-{pid}-{name}"));
    let staged = dir.join(format!(".seine-update-{pid}-{}", executable_name()));
    let result = stage_update(client, asset, &expected, &archive, &staged)
        .and_then(|()| {
            at_stage(
                UpdateFailure::StartupProbe,
                verify(&staged, &release.tag_name),
            )
        })
        .and_then(|()| at_stage(UpdateFailure::Replace, replace_executable(exe, &staged)));
    let _ = fs::remove_file(&archive);
    if result.is_err() {
        let _ = fs::remove_file(&staged);
    }
    result?;
    crate::diagnostics::update_installed();
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
/// replaces the process image. Windows cannot, so the first process stays as a
/// small supervisor: it runs the updated executable, relaunches it after each
/// later update, and exits with its exit code, so whatever started Seine still
/// sees one process that ends when mining ends. The child runs in a job that
/// ends it if the supervisor is stopped.
pub(crate) fn restart() -> Result<()> {
    let exe = match UPDATED_EXECUTABLE.get() {
        Some(path) => path.clone(),
        None => running_exe().context("cannot locate the running executable")?,
    };
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let mut command = std::process::Command::new(&exe);
    command.args(&args).env(STARTED_BY_UPDATE_ENV, "1");
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let err = command.exec();
        Err(anyhow!("restart failed: {err}"))
    }
    #[cfg(not(unix))]
    {
        if std::env::var_os(SUPERVISED_ENV).is_some() {
            std::process::exit(RESTART_EXIT_CODE);
        }
        command.env(SUPERVISED_ENV, "1");
        // Console interrupts reach the child too; it stops and returns its code.
        crate::shutdown::supervise_child();
        let job = windows_job::kill_on_close();
        loop {
            let mut child = command
                .spawn()
                .with_context(|| format!("restart of {} failed", exe.display()))?;
            if let Some(job) = &job {
                job.assign(&child);
            }
            {
                use std::os::windows::io::AsRawHandle;
                crate::shutdown::set_supervised_child(child.as_raw_handle());
            }
            let status = child.wait();
            crate::shutdown::set_supervised_child(std::ptr::null_mut());
            let status = status.with_context(|| format!("restart of {} failed", exe.display()))?;
            if status.code() != Some(RESTART_EXIT_CODE) {
                std::process::exit(status.code().unwrap_or(1));
            }
        }
    }
}

/// A Windows job that ends its processes when the supervisor's handle closes,
/// so stopping the supervisor by its process ID also stops the miner.
#[cfg(not(unix))]
mod windows_job {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
        SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };

    pub(super) struct Job(HANDLE);

    /// `None` when the job cannot be created; the child then runs without it.
    pub(super) fn kill_on_close() -> Option<Job> {
        // SAFETY: plain Win32 calls on a handle this function owns; the limit
        // structure is fully initialized before it is passed by pointer.
        unsafe {
            let handle = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if handle.is_null() {
                return None;
            }
            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            let set = SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            );
            (set != 0).then_some(Job(handle))
        }
    }

    impl Job {
        pub(super) fn assign(&self, child: &std::process::Child) {
            // SAFETY: both handles are valid for the duration of the call. A
            // failure (for example a parent job that forbids nesting) leaves
            // the child running outside the job, as before.
            unsafe {
                AssignProcessToJobObject(self.0, child.as_raw_handle() as HANDLE);
            }
        }

        /// What the supervisor's exit does to the job.
        #[cfg(test)]
        pub(super) fn close(self) {
            // SAFETY: this job's own handle, closed once.
            unsafe {
                windows_sys::Win32::Foundation::CloseHandle(self.0);
            }
        }
    }
}

/// Removes the previous executable Windows could not delete while it ran.
pub(crate) fn cleanup_leftovers() {
    if let Ok(exe) = running_exe() {
        if let Some(dir) = exe.parent() {
            remove_stale_staging(dir);
            // The previous version stays until this one has started once.
            if dir.join(ROLLBACK_MARKER).exists() {
                return;
            }
            let _ = fs::remove_file(dir.join(old_executable_name()));
            #[cfg(windows)]
            for slot in 1..=OLD_EXECUTABLE_SLOTS {
                let _ = fs::remove_file(dir.join(numbered_old_executable_name(slot)));
            }
        }
    }
}

/// Staged downloads an updater left behind when it stopped part way. Ones
/// less than a day old may belong to an updater still running.
fn remove_stale_staging(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let stale = entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(".seine-update-"))
            && entry
                .metadata()
                .and_then(|meta| meta.modified())
                .is_ok_and(|modified| {
                    modified
                        .elapsed()
                        .is_ok_and(|age| age > Duration::from_secs(24 * 60 * 60))
                });
        if stale {
            let _ = fs::remove_file(entry.path());
        }
    }
}

/// After an automatic update, lets the new version go back to this one if it
/// cannot start with the same arguments.
pub(crate) fn arm_rollback(exe: &Path, tag: &str) {
    let (Some(dir), Some(previous)) = (exe.parent(), PREVIOUS_EXECUTABLE.get()) else {
        return;
    };
    let Some(name) = previous.file_name().and_then(|name| name.to_str()) else {
        return;
    };
    let _ = fs::write(dir.join(ROLLBACK_MARKER), format!("{tag}\n{name}\n"));
}

struct Rollback {
    tag: String,
    previous: PathBuf,
}

fn read_rollback(dir: &Path) -> Option<Rollback> {
    let text = fs::read_to_string(dir.join(ROLLBACK_MARKER)).ok()?;
    let mut lines = text.lines();
    let tag = lines.next()?.trim().to_string();
    let previous = lines.next()?.trim();
    // A bare file name in the executable's directory, nothing else.
    if previous.is_empty() || Path::new(previous).file_name()? != previous {
        return None;
    }
    Some(Rollback {
        tag,
        previous: dir.join(previous),
    })
}

/// True for the process an update restarted into.
fn started_by_update() -> bool {
    std::env::var_os(STARTED_BY_UPDATE_ENV).is_some()
}

/// The marker was left for this build's version.
fn marker_for_this_version(rollback: &Rollback) -> bool {
    let running = crate::update_check::version_triple(crate::version::release_version());
    running.is_some() && crate::update_check::version_triple(&rollback.tag) == running
}

/// The rollback this process may take: only the start an update made, and
/// only for a marker left for this version.
fn pending_rollback(dir: &Path, started_by_update: bool) -> Option<Rollback> {
    if !started_by_update {
        return None;
    }
    read_rollback(dir).filter(marker_for_this_version)
}

/// The first start after an automatic update, which can still go back.
pub(crate) fn rollback_pending() -> bool {
    running_exe()
        .ok()
        .and_then(|exe| {
            exe.parent()
                .and_then(|dir| pending_rollback(dir, started_by_update()))
        })
        .is_some()
}

/// This version started: the previous one is no longer needed. A seine still
/// on the previous version in this directory leaves the marker to the new one.
pub(crate) fn confirm_started() {
    let Ok(exe) = running_exe() else {
        return;
    };
    let Some(dir) = exe.parent() else {
        return;
    };
    clear_failed_start(dir, crate::version::release_version());
    if !dir.join(ROLLBACK_MARKER).exists() {
        return;
    }
    if read_rollback(dir).is_none_or(|rollback| marker_for_this_version(&rollback)) {
        let _ = fs::remove_file(dir.join(ROLLBACK_MARKER));
        cleanup_leftovers();
    }
}

/// The process an update restarted into failed before mining: put the
/// previous version back and run it with the same arguments. `refused` is set
/// when the arguments or saved settings were refused, which the release would
/// refuse again; other failures may pass, so the release is tried again up to
/// `FAILED_STARTS_BEFORE_GIVING_UP` times. Returns only when there is nothing
/// to go back to. A failure once mining has started is not rolled back.
pub(crate) fn roll_back_failed_start(err: &anyhow::Error, refused: bool) {
    let Ok(exe) = running_exe() else {
        return;
    };
    let Some(dir) = exe.parent() else {
        return;
    };
    let Some(rollback) = pending_rollback(dir, started_by_update()) else {
        return;
    };
    let _ = fs::remove_file(dir.join(ROLLBACK_MARKER));
    if !rollback.previous.is_file() {
        return;
    }
    let failures = record_failed_start(dir, &rollback.tag, refused);
    let reason = format!("{err:#}");
    let reason = reason.lines().next().unwrap_or_default().trim();
    let next = if failures >= FAILED_STARTS_BEFORE_GIVING_UP {
        format!("no longer installing {} automatically", rollback.tag)
    } else {
        format!("trying {} again at a later update check", rollback.tag)
    };
    crate::console_output::eprintln!(
        "seine {} could not start ({reason}); going back to the previous version and {next}",
        rollback.tag,
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        if fs::rename(&rollback.previous, &exe).is_err() {
            return;
        }
        let err = std::process::Command::new(&exe)
            .args(std::env::args_os().skip(1))
            .env_remove(STARTED_BY_UPDATE_ENV)
            .exec();
        crate::console_output::eprintln!("could not start the previous version: {err}");
    }
    #[cfg(not(unix))]
    {
        // The restart supervisor starts whatever is at this path again.
        if std::env::var_os(SUPERVISED_ENV).is_none() {
            return;
        }
        let Ok(failed) = free_old_executable(dir, Some(&rollback.previous)) else {
            return;
        };
        if fs::rename(&exe, &failed).is_err() {
            return;
        }
        if fs::rename(&rollback.previous, &exe).is_err() {
            let _ = fs::rename(&failed, &exe);
            return;
        }
        std::process::exit(RESTART_EXIT_CODE);
    }
}

/// The release a failed first start was recorded for and how many times.
fn read_failed_start(dir: &Path) -> Option<(String, u32)> {
    let text = fs::read_to_string(dir.join(FAILED_START_FILE)).ok()?;
    let (tag, count) = text.trim().split_once(' ')?;
    Some((tag.to_string(), count.trim().parse().ok()?))
}

/// Counts a failed first start of `tag` and returns the new count.
fn record_failed_start(dir: &Path, tag: &str, refused: bool) -> u32 {
    let earlier = read_failed_start(dir)
        .filter(|(recorded, _)| recorded == tag)
        .map_or(0, |(_, count)| count);
    let mut count = earlier.saturating_add(1);
    if refused {
        count = count.max(FAILED_STARTS_BEFORE_GIVING_UP);
    }
    let _ = fs::write(dir.join(FAILED_START_FILE), format!("{tag} {count}\n"));
    count
}

fn clear_failed_start(dir: &Path, tag: &str) {
    let running = crate::update_check::version_triple(tag);
    if read_failed_start(dir).is_some_and(|(recorded, _)| {
        running.is_some() && crate::update_check::version_triple(&recorded) == running
    }) {
        let _ = fs::remove_file(dir.join(FAILED_START_FILE));
    }
}

/// Whether automatic installs gave up on `tag` because its first start
/// failed on this machine.
pub(crate) fn gave_up_on(tag: &str) -> bool {
    running_exe()
        .ok()
        .and_then(|exe| exe.parent().and_then(read_failed_start))
        .is_some_and(|(recorded, count)| recorded == tag && count >= FAILED_STARTS_BEFORE_GIVING_UP)
}

#[cfg(windows)]
fn numbered_old_executable_name(slot: u32) -> String {
    format!("seine.old.{slot}.exe")
}

/// The first spare name for the running executable that is free or can be
/// freed, never `keep`. A restart supervisor keeps an older one in use.
#[cfg(windows)]
fn free_old_executable(dir: &Path, keep: Option<&Path>) -> Result<PathBuf> {
    let names = std::iter::once(old_executable_name().to_string())
        .chain((1..=OLD_EXECUTABLE_SLOTS).map(numbered_old_executable_name));
    for name in names {
        let path = dir.join(name);
        if keep.is_some_and(|keep| keep == path) {
            continue;
        }
        let _ = fs::remove_file(&path);
        if !path.exists() {
            return Ok(path);
        }
    }
    bail!(
        "previous Seine executables in {} are still running",
        dir.display()
    )
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
    use seine_telemetry::UpdateFailure;
    at_stage(
        UpdateFailure::Download,
        (|| {
            if asset.size > MAX_ARCHIVE_BYTES {
                bail!(
                    "release asset {} is implausibly large ({} bytes)",
                    asset.name,
                    asset.size
                );
            }
            download_file(client, &asset.browser_download_url, archive)
        })(),
    )?;
    at_stage(
        UpdateFailure::Checksum,
        (|| {
            let actual = sha256_file(archive)?;
            if actual != expected_sha256 {
                bail!(
                "checksum mismatch for {}: release lists {expected_sha256}, download has {actual}",
                asset.name
            );
            }
            Ok(())
        })(),
    )?;
    at_stage(
        UpdateFailure::Extract,
        (|| {
            extract_executable(archive, staged).with_context(|| {
                format!("cannot extract {} from {}", executable_name(), asset.name)
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(staged, fs::Permissions::from_mode(0o755))
                    .context("cannot mark the new executable runnable")?;
            }
            Ok(())
        })(),
    )
}

/// Runs the staged executable with `--version` and requires it to exit
/// cleanly and report `tag`, so a binary that cannot start on this host (wrong
/// architecture, missing libraries, corrupt file) never replaces a working one.
fn probe_executable(staged: &Path, tag: &str) -> Result<()> {
    use std::process::{Command, Stdio};
    let mut child = Command::new(staged)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("the new executable does not start on this system")?;
    let mut stdout = child.stdout.take().context("no output pipe")?;
    let reader = std::thread::spawn(move || {
        let mut output = Vec::new();
        let _ = (&mut stdout).take(4096).read_to_end(&mut output);
        output
    });
    let deadline = std::time::Instant::now() + PROBE_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("the new executable did not report its version within {PROBE_TIMEOUT:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let output = reader.join().unwrap_or_default();
    if !status.success() {
        bail!("the new executable failed its startup check ({status})");
    }
    let output = String::from_utf8_lossy(&output);
    let expected = crate::update_check::version_triple(tag);
    let reported = output
        .split_whitespace()
        .any(|word| expected.is_some() && crate::update_check::version_triple(word) == expected);
    if !reported {
        bail!(
            "the new executable reports {:?}, expected {tag}",
            output.trim()
        );
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
                // On disk before it replaces the running executable.
                out.sync_all()?;
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
                // On disk before it replaces the running executable.
                out.sync_all()?;
                return Ok(());
            }
        }
    }
    bail!("{wanted} is not in the archive")
}

/// Swaps the staged executable in. The running binary stays usable until the
/// rename lands; Windows keeps it as `seine.old.exe` (or a numbered spare while
/// that one still runs) because a running image cannot be deleted, and
/// `cleanup_leftovers` removes it on a later start.
fn replace_executable(exe: &Path, staged: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        let dir = exe
            .parent()
            .ok_or_else(|| anyhow!("executable has no directory"))?;
        // A previous version an update in progress may still go back to.
        let pending = read_rollback(dir).map(|rollback| rollback.previous);
        let old = free_old_executable(dir, pending.as_deref())?;
        fs::rename(exe, &old).with_context(|| format!("cannot move {} aside", exe.display()))?;
        if let Err(err) = fs::rename(staged, exe) {
            let _ = fs::rename(&old, exe);
            return Err(err).with_context(|| format!("cannot install {}", exe.display()));
        }
        let _ = PREVIOUS_EXECUTABLE.set(old);
        Ok(())
    }
    #[cfg(not(windows))]
    {
        // Keep the previous executable until the new one has started once.
        if let Some(dir) = exe.parent() {
            let previous = dir.join(old_executable_name());
            let _ = fs::remove_file(&previous);
            if fs::hard_link(exe, &previous).is_ok() {
                let _ = PREVIOUS_EXECUTABLE.set(previous);
            }
        }
        fs::rename(staged, exe).with_context(|| format!("cannot install {}", exe.display()))?;
        // Persist the rename itself; best effort.
        if let Some(dir) = exe.parent() {
            if let Ok(dir) = fs::File::open(dir) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(unix))]
    #[test]
    fn stopping_the_restart_supervisor_stops_the_miner() {
        let job = windows_job::kill_on_close().expect("create job");
        let mut child = std::process::Command::new("cmd")
            .args(["/c", "ping", "-n", "60", "127.0.0.1"])
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("start child");
        job.assign(&child);
        assert!(child.try_wait().unwrap().is_none());
        job.close();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while child.try_wait().unwrap().is_none() {
            if std::time::Instant::now() > deadline {
                let _ = child.kill();
                panic!("child outlived the supervisor's job");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    #[test]
    fn a_rollback_marker_names_only_a_file_beside_the_executable() {
        let dir = std::env::temp_dir().join(format!("seine-rollback-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(ROLLBACK_MARKER), "v0.3.2\nseine.old\n/data dir\n").unwrap();
        let rollback = read_rollback(&dir).unwrap();
        assert_eq!(rollback.tag, "v0.3.2");
        assert_eq!(rollback.previous, dir.join("seine.old"));
        for previous in ["../seine", "/bin/sh", ""] {
            fs::write(
                dir.join(ROLLBACK_MARKER),
                format!("v0.3.2\n{previous}\n/data\n"),
            )
            .unwrap();
            assert!(read_rollback(&dir).is_none(), "{previous}");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn only_the_start_an_update_made_takes_a_rollback_for_this_version() {
        let dir = tempfile::tempdir().unwrap();
        let this = crate::version::release_version();
        fs::write(
            dir.path().join(ROLLBACK_MARKER),
            format!("{this}\nseine.old\n"),
        )
        .unwrap();
        assert!(pending_rollback(dir.path(), true).is_some());
        // A later start by hand, say with a mistyped flag, does not.
        assert!(pending_rollback(dir.path(), false).is_none());
        // Nor does a seine of another version sharing the directory.
        fs::write(dir.path().join(ROLLBACK_MARKER), "v0.0.1\nseine.old\n").unwrap();
        assert!(pending_rollback(dir.path(), true).is_none());
    }

    #[test]
    fn a_release_is_given_up_after_repeated_or_refused_first_starts() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();
        assert_eq!(record_failed_start(dir, "v9.0.0", false), 1);
        assert!(FAILED_STARTS_BEFORE_GIVING_UP > 1);
        assert_eq!(
            record_failed_start(dir, "v9.0.0", false),
            FAILED_STARTS_BEFORE_GIVING_UP
        );
        // A newer release starts over.
        assert_eq!(record_failed_start(dir, "v9.0.1", false), 1);
        // Refused arguments are refused again: given up at once.
        assert_eq!(
            record_failed_start(dir, "v9.0.2", true),
            FAILED_STARTS_BEFORE_GIVING_UP
        );
        // Another version starting leaves the record; that version clears it.
        clear_failed_start(dir, "v9.0.1");
        assert!(read_failed_start(dir).is_some());
        clear_failed_start(dir, "v9.0.2");
        assert!(read_failed_start(dir).is_none());
    }

    fn reports_any(_: &Path, _: &str) -> Result<()> {
        Ok(())
    }

    fn reports_none(_: &Path, _: &str) -> Result<()> {
        bail!("other version")
    }

    #[test]
    fn a_release_another_seine_installed_is_not_installed_again() {
        let release = Release {
            tag_name: "v99.0.0".into(),
            draft: false,
            prerelease: false,
            assets: Vec::new(),
        };
        let exe = Path::new("/opt/seine/seine");
        assert!(matches!(
            installed_by_another(&release, "v0.1.0", exe, reports_any),
            Some(Outcome::Updated { .. })
        ));
        assert!(installed_by_another(&release, "v0.1.0", exe, reports_none).is_none());
        assert!(installed_by_another(&release, "v99.0.0", exe, reports_any).is_none());
    }

    #[test]
    fn a_replaced_executable_is_found_at_its_installed_path() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("seine");
        fs::write(&exe, "new").unwrap();
        let reported = PathBuf::from(format!("{} (deleted)", exe.display()));
        let expected = if cfg!(target_os = "linux") {
            exe.clone()
        } else {
            reported.clone()
        };
        assert_eq!(installed_path(reported), expected);
        assert_eq!(installed_path(exe.clone()), exe);
    }

    #[cfg(windows)]
    #[test]
    fn going_back_never_frees_the_previous_version() {
        let dir = tempfile::tempdir().unwrap();
        let previous = dir.path().join(numbered_old_executable_name(1));
        fs::write(&previous, "previous").unwrap();
        // The first spare is held open, as a running supervisor holds it.
        let held = dir.path().join(old_executable_name());
        fs::write(&held, "supervisor").unwrap();
        let _open =
            std::os::windows::fs::OpenOptionsExt::share_mode(&mut fs::OpenOptions::new(), 0)
                .read(true)
                .open(&held)
                .unwrap();
        let free = free_old_executable(dir.path(), Some(&previous)).unwrap();
        assert_eq!(free, dir.path().join(numbered_old_executable_name(2)));
        assert_eq!(fs::read_to_string(&previous).unwrap(), "previous");
    }

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
    fn release_page_redirect_names_the_tag_and_download_urls() {
        use httpmock::MockServer;
        assert_eq!(
            tag_from_release_location("https://github.com/zpalmtree/seine/releases/tag/v0.3.0"),
            Some("v0.3.0".to_string())
        );
        assert_eq!(
            tag_from_release_location("https://github.com/zpalmtree/seine/releases"),
            None
        );
        assert_eq!(tag_from_release_location("/releases/tag/../../x"), None);
        assert_eq!(tag_from_release_location("/releases/tag/v1.2.3/../x"), None);
        let server = MockServer::start();
        server.mock(|when, then| {
            when.path("/latest");
            then.status(302).header(
                "Location",
                "https://github.com/zpalmtree/seine/releases/tag/v9.8.7",
            );
        });
        let client = lookup_client(reqwest::redirect::Policy::none()).unwrap();
        let release =
            lookup_release_web(&client, &server.url("/latest"), "https://dl.example/d").unwrap();
        assert_eq!(release.tag(), "v9.8.7");
        let sums = find_asset(&release.assets, CHECKSUMS_ASSET).unwrap();
        assert_eq!(
            sums.browser_download_url,
            format!("https://dl.example/d/v9.8.7/{CHECKSUMS_ASSET}")
        );
        assert!(find_asset(&release.assets, SIGNATURE_ASSET).is_ok());
        assert!(find_asset(&release.assets, asset_name().unwrap()).is_ok());
        server.mock(|when, then| {
            when.path("/not-redirecting");
            then.status(200).body("page");
        });
        assert!(lookup_release_web(&client, &server.url("/not-redirecting"), "x").is_err());
    }

    #[test]
    fn asset_name_matches_the_release_workflow_on_this_platform() {
        let name = asset_name().expect("release builds exist for CI platforms");
        assert!(name.starts_with("seine-"));
        assert!(name.ends_with(".tar.gz") || name.ends_with(".zip"));
    }

    // The fixtures package the test harness, which has no `--version`.
    fn accept_any(_: &Path, _: &str) -> Result<()> {
        Ok(())
    }

    #[cfg(unix)]
    fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        fs::write(&path, body).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[cfg(unix)]
    #[test]
    fn startup_probe_accepts_only_a_working_binary_of_the_release_version() {
        let dir = tempfile::tempdir().unwrap();
        let good = script(dir.path(), "good", "#!/bin/sh\necho seine v99.0.0\n");
        probe_executable(&good, "v99.0.0").unwrap();
        assert!(probe_executable(&good, "v99.0.1").is_err());
        let failing = script(
            dir.path(),
            "failing",
            "#!/bin/sh\necho seine v99.0.0\nexit 3\n",
        );
        assert!(probe_executable(&failing, "v99.0.0").is_err());
        let garbage = script(dir.path(), "garbage", "\x7fELF not really\n");
        assert!(probe_executable(&garbage, "v99.0.0").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn failed_startup_probe_keeps_the_running_executable() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("seine");
        fs::write(&exe, "original").unwrap();
        let staged = script(dir.path(), "staged", "#!/bin/sh\nexit 1\n");
        let result =
            probe_executable(&staged, "v99.0.0").and_then(|()| replace_executable(&exe, &staged));
        assert!(result.is_err());
        assert_eq!(fs::read_to_string(&exe).unwrap(), "original");
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
            accept_any,
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
            &exe,
            accept_any,
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
            accept_any,
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
