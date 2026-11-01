//! Background check for newer seine releases.
//!
//! One request to the GitHub releases API at startup and every
//! `CHECK_INTERVAL` after. A newer release is logged once per version and
//! flagged in the dashboard header. A failed check or install retries after
//! `RETRY_INTERVAL` plus jitter, and mining code can ask for an earlier check
//! when it receives work this build cannot mine.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use crate::miner::ui;

const LATEST_RELEASE_API_URL: &str = "https://api.github.com/repos/zpalmtree/seine/releases/latest";
const RELEASES_URL: &str = "https://github.com/zpalmtree/seine/releases/latest";
const CHECK_INTERVAL: Duration = Duration::from_secs(12 * 60 * 60);
const RETRY_INTERVAL: Duration = Duration::from_secs(60 * 60);
const RETRY_JITTER_SECS: u64 = 15 * 60;
/// Earliest follow-up after any check when mining asks for one early.
const MIN_REQUESTED_INTERVAL: Duration = Duration::from_secs(10 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Set by the release workflow; source and local builds never replace
/// themselves with the generic release binary.
const RELEASE_BUILD: bool = match option_env!("SEINE_RELEASE_BUILD") {
    Some(value) => value.len() == 1 && value.as_bytes()[0] == b'1',
    None => false,
};

static AUTO_INSTALL: AtomicBool = AtomicBool::new(false);
static WAKE: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());

static STATE: Mutex<UpdateState> = Mutex::new(UpdateState {
    latest: None,
    tui_log_pending: false,
});

struct UpdateState {
    latest: Option<String>,
    // Set when the notice was announced before any dashboard existed, so the
    // first dashboard built afterwards still shows it in its log.
    tui_log_pending: bool,
}

#[derive(Deserialize)]
struct LatestRelease {
    tag_name: String,
}

pub(crate) struct DashboardUpdate {
    pub latest: String,
    pub log_message: Option<String>,
}

pub(crate) fn spawn(auto_update: bool, shutdown: Arc<AtomicBool>) {
    let current = crate::version::release_version();
    // Untagged development builds have nothing meaningful to compare.
    if parse_version(current).is_none() {
        return;
    }
    let auto_install = auto_update && RELEASE_BUILD;
    AUTO_INSTALL.store(auto_install, Ordering::Release);
    if auto_update && !RELEASE_BUILD {
        ui::info(
            "UPDATE",
            "automatic installs are off for builds not made by the seine release workflow; new releases are still announced",
        );
    }
    let _ = thread::Builder::new()
        .name("update-check".to_string())
        .spawn(move || {
            let mut last_check;
            loop {
                last_check = Instant::now();
                let succeeded = match fetch_latest_tag() {
                    Ok(tag) => {
                        record_latest(current, &tag);
                        if auto_install && is_newer(&tag, current) {
                            match apply_update(&shutdown) {
                                Some(true) => return,
                                Some(false) => true,
                                None => false,
                            }
                        } else {
                            true
                        }
                    }
                    Err(_) => false,
                };
                let wait = if succeeded {
                    CHECK_INTERVAL
                } else {
                    RETRY_INTERVAL + Duration::from_secs(jitter_secs(RETRY_JITTER_SECS))
                };
                wait_for_next_check(last_check, wait);
            }
        });
}

/// True when this process installs newer releases by itself.
pub(crate) fn auto_install_active() -> bool {
    AUTO_INSTALL.load(Ordering::Acquire)
}

/// Asks the background checker to look for a release soon, at most once per
/// `MIN_REQUESTED_INTERVAL` after the previous check. A no-op when the checker
/// is not running.
pub(crate) fn request_check_soon() {
    let (lock, cvar) = &WAKE;
    *lock.lock().unwrap_or_else(|e| e.into_inner()) = true;
    cvar.notify_all();
}

fn wait_for_next_check(last_check: Instant, wait: Duration) {
    let (lock, cvar) = &WAKE;
    let mut requested = lock.lock().unwrap_or_else(|e| e.into_inner());
    loop {
        let elapsed = last_check.elapsed();
        if elapsed >= wait {
            break;
        }
        if *requested && elapsed >= MIN_REQUESTED_INTERVAL {
            break;
        }
        let until = if *requested {
            MIN_REQUESTED_INTERVAL.min(wait)
        } else {
            wait
        };
        let (guard, _) = cvar
            .wait_timeout(requested, until.saturating_sub(elapsed))
            .unwrap_or_else(|e| e.into_inner());
        requested = guard;
    }
    *requested = false;
}

/// Spreads retries from rigs that failed together (shared outage, rate limit).
fn jitter_secs(max: u64) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    (nanos ^ u64::from(std::process::id()).wrapping_mul(0x9E37_79B9)) % max.max(1)
}

/// `--auto-update`: install the newer release, then stop mining so `main` can
/// restart the updated executable with the same arguments. Returns `Some(true)`
/// once installed, `Some(false)` when already current, and `None` on failure
/// (retried after `RETRY_INTERVAL`).
fn apply_update(shutdown: &Arc<AtomicBool>) -> Option<bool> {
    match crate::self_update::run() {
        Ok(crate::self_update::Outcome::Updated { from, to, .. }) => {
            ui::info(
                "UPDATE",
                format!("installed seine {to} (was {from}); restarting with the same arguments"),
            );
            crate::self_update::request_restart();
            crate::shutdown::request_auto_update(shutdown);
            Some(true)
        }
        Ok(crate::self_update::Outcome::UpToDate { .. }) => Some(false),
        Err(err) => {
            ui::warn(
                "UPDATE",
                format!("automatic update failed: {err:#}; retrying within about an hour"),
            );
            None
        }
    }
}

/// The newer release for a dashboard being built, plus its log line if it was
/// announced before any dashboard could show it.
pub(crate) fn for_new_dashboard() -> Option<DashboardUpdate> {
    let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
    let latest = state.latest.clone()?;
    let log_message = std::mem::take(&mut state.tui_log_pending).then(|| notice(&latest));
    Some(DashboardUpdate {
        latest,
        log_message,
    })
}

fn record_latest(current: &str, tag: &str) {
    if !is_newer(tag, current) {
        return;
    }
    let already_announced = STATE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .latest
        .as_deref()
        == Some(tag);
    if already_announced {
        return;
    }
    // Announce without holding STATE: the dashboard builder takes STATE while
    // the announcement takes the dashboard lock.
    let delivered_to_tui = ui::announce_update(tag, &notice(tag));
    let mut state = STATE.lock().unwrap_or_else(|e| e.into_inner());
    state.latest = Some(tag.to_string());
    state.tui_log_pending = !delivered_to_tui;
}

fn notice(latest: &str) -> String {
    format!(
        "seine {latest} is available (running {}): {RELEASES_URL}",
        crate::version::ui_display_version()
    )
}

fn fetch_latest_tag() -> Result<String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .user_agent(format!("seine/{}", crate::version::release_version()))
        .build()?;
    let response = client
        .get(LATEST_RELEASE_API_URL)
        .header("Accept", "application/vnd.github+json")
        .send()
        .context("release check unavailable")?;
    if !response.status().is_success() {
        bail!("release check returned {}", response.status());
    }
    let release: LatestRelease = response.json().context("invalid release response")?;
    Ok(release.tag_name)
}

pub(crate) fn is_newer(candidate: &str, current: &str) -> bool {
    match (parse_version(candidate), parse_version(current)) {
        (Some(candidate), Some(current)) => candidate > current,
        _ => false,
    }
}

/// The `(major, minor, patch)` of a release tag or version string.
pub(crate) fn version_triple(raw: &str) -> Option<(u64, u64, u64)> {
    parse_version(raw)
}

/// Parses `v1.2.3` / `1.2.3`, ignoring any `-pre` or `+build` suffix
/// (including `git describe` output such as `v0.2.22-3-gabc1234`).
fn parse_version(raw: &str) -> Option<(u64, u64, u64)> {
    let trimmed = raw.trim();
    let trimmed = trimmed
        .strip_prefix('v')
        .or_else(|| trimmed.strip_prefix('V'))
        .unwrap_or(trimmed);
    let core = trimmed.split(['-', '+']).next()?;
    let mut parts = core.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::{is_newer, jitter_secs, parse_version, wait_for_next_check, WAKE};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    // Both wait tests share the process-wide wake flag.
    static WAKE_TEST: Mutex<()> = Mutex::new(());

    #[test]
    fn requested_check_waits_for_the_minimum_spacing() {
        let _serial = WAKE_TEST.lock().unwrap_or_else(|e| e.into_inner());
        // A request right after a check must not wake the checker early.
        let started = Instant::now();
        *WAKE.0.lock().unwrap() = true;
        wait_for_next_check(started, Duration::from_millis(50));
        assert!(started.elapsed() >= Duration::from_millis(50));
        assert!(!*WAKE.0.lock().unwrap());
    }

    #[test]
    fn requested_check_wakes_once_the_spacing_has_passed() {
        let _serial = WAKE_TEST.lock().unwrap_or_else(|e| e.into_inner());
        let last = Instant::now() - super::MIN_REQUESTED_INTERVAL;
        *WAKE.0.lock().unwrap() = true;
        let started = Instant::now();
        wait_for_next_check(last, Duration::from_secs(3600));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn jitter_stays_in_range() {
        for _ in 0..100 {
            assert!(jitter_secs(900) < 900);
        }
        assert_eq!(jitter_secs(0), 0);
    }

    #[test]
    fn parses_release_tags_and_describe_output() {
        assert_eq!(parse_version("v0.2.22"), Some((0, 2, 22)));
        assert_eq!(parse_version("0.2.22"), Some((0, 2, 22)));
        assert_eq!(parse_version("v0.2.22-3-gabc1234"), Some((0, 2, 22)));
        assert_eq!(parse_version("v1.0.0+build.7"), Some((1, 0, 0)));
    }

    #[test]
    fn rejects_non_version_strings() {
        assert_eq!(parse_version("abc1234"), None);
        assert_eq!(parse_version("v0.2"), None);
        assert_eq!(parse_version("v0.2.22.1"), None);
        assert_eq!(parse_version(""), None);
    }

    #[test]
    fn newer_compares_numerically() {
        assert!(is_newer("v0.2.23", "v0.2.22"));
        assert!(is_newer("v0.10.0", "v0.9.9"));
        assert!(is_newer("v1.0.0", "0.2.22"));
        assert!(!is_newer("v0.2.22", "v0.2.22"));
        assert!(!is_newer("v0.2.21", "v0.2.22"));
        assert!(!is_newer("v0.2.22", "v0.2.22-3-gabc1234"));
        assert!(!is_newer("garbage", "v0.2.22"));
    }
}
