//! Background check for newer seine releases.
//!
//! One release lookup at startup and every `CHECK_INTERVAL` after (the
//! GitHub releases API, or the release page when the API refuses). A newer
//! release is logged once per version and flagged in the dashboard header. A
//! failed check or install retries after `RETRY_INTERVAL` plus jitter, and
//! mining code can ask for an earlier check when it receives work this build
//! cannot mine.
//!
//! With `--auto-update`, a newer release installs after `COUNTDOWN` while
//! mining continues. The dashboard shows the countdown and can install at once
//! or skip that version (remembered in the data dir). Work this build cannot
//! mine installs without waiting.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::miner::ui;

const RELEASES_URL: &str = "https://github.com/zpalmtree/seine/releases/latest";
const CHECK_INTERVAL: Duration = Duration::from_secs(12 * 60 * 60);
const RETRY_INTERVAL: Duration = Duration::from_secs(60 * 60);
const RETRY_JITTER_SECS: u64 = 15 * 60;
/// Earliest follow-up after any check when mining asks for one early.
const MIN_REQUESTED_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// How long a found release waits before it installs.
pub(crate) const COUNTDOWN: Duration = Duration::from_secs(60);
/// Data-dir file holding the release tag the miner chose to skip.
const SKIPPED_FILE: &str = "update-skipped";
/// Data-dir file the installing process leaves for the restarted one:
/// `<from> <to>`.
const INSTALLED_FILE: &str = "update-installed";

/// Set by the release workflow; source and local builds never replace
/// themselves with the generic release binary.
const RELEASE_BUILD: bool = match option_env!("SEINE_RELEASE_BUILD") {
    Some(value) => value.len() == 1 && value.as_bytes()[0] == b'1',
    None => false,
};

static AUTO_INSTALL: AtomicBool = AtomicBool::new(false);
static WAKE: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());
/// Set while mining is paused on work this build cannot mine.
static MINING_BLOCKED: AtomicBool = AtomicBool::new(false);
/// The miner's answer to a running countdown.
static CHOICE: (Mutex<Option<Choice>>, Condvar) = (Mutex::new(None), Condvar::new());

/// What the miner chose while an update counted down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Choice {
    InstallNow,
    Skip,
}

/// An automatic install the dashboard shows in its header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingInstall {
    pub latest: String,
    /// `None` once the download has started.
    pub deadline: Option<Instant>,
}

enum CountdownEnd {
    Install,
    Skip,
    Stopped,
}

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

pub(crate) struct DashboardUpdate {
    pub latest: String,
    pub log_message: Option<String>,
}

/// `api_controlled`: the control API runs the miner (`--service`, `--api-server`).
/// A restart would drop the miner state and token the API set, so new releases
/// are only announced, unless mining is paused until Seine is updated.
pub(crate) fn spawn(
    auto_update: bool,
    api_controlled: bool,
    data_dir: PathBuf,
    shutdown: Arc<AtomicBool>,
) {
    let current = crate::version::release_version();
    // Untagged development builds have nothing meaningful to compare.
    if parse_version(current).is_none() {
        return;
    }
    let auto_install = auto_update && RELEASE_BUILD;
    AUTO_INSTALL.store(auto_install, Ordering::Release);
    crate::diagnostics::update_checker_started(auto_install);
    if auto_update && !RELEASE_BUILD {
        ui::info(
            "UPDATE",
            "automatic installs are off for builds not made by the seine release workflow; new releases are still announced",
        );
    } else if auto_install && api_controlled {
        ui::info(
            "UPDATE",
            "while the control API runs the miner, new releases are announced and install only if mining pauses for one (seine --self-update installs them)",
        );
    }
    let _ = thread::Builder::new()
        .name("update-check".to_string())
        .spawn(move || {
            let mut last_check;
            loop {
                last_check = Instant::now();
                let succeeded = match crate::self_update::latest_release() {
                    Ok(release) => {
                        let tag = release.tag().to_string();
                        record_latest(current, &tag);
                        let blocked = MINING_BLOCKED.load(Ordering::Acquire);
                        if auto_install
                            && is_newer(&tag, current)
                            && (blocked || !api_controlled)
                            && offers_install(&tag, skipped_tag(&data_dir).as_deref(), blocked)
                        {
                            match count_down(&tag, &shutdown) {
                                CountdownEnd::Stopped => return,
                                CountdownEnd::Skip => {
                                    skip(&data_dir, &tag, current);
                                    true
                                }
                                CountdownEnd::Install => {
                                    match apply_update(release, &data_dir, &shutdown) {
                                        Some(true) => return,
                                        Some(false) => true,
                                        None => false,
                                    }
                                }
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

/// Mining paused (or resumed) on work this build cannot mine. While paused, a
/// found release installs without its countdown.
pub(crate) fn set_mining_blocked(blocked: bool) {
    MINING_BLOCKED.store(blocked, Ordering::Release);
    if blocked {
        CHOICE.1.notify_all();
    }
}

#[cfg(test)]
pub(crate) fn mining_blocked() -> bool {
    MINING_BLOCKED.load(Ordering::Acquire)
}

/// The dashboard's answer to the countdown on screen.
pub(crate) fn choose(choice: Choice) {
    let (lock, cvar) = &CHOICE;
    *lock.lock().unwrap_or_else(|e| e.into_inner()) = Some(choice);
    cvar.notify_all();
}

/// Serialises tests that touch the process-wide countdown answer.
#[cfg(test)]
pub(crate) static CHOICE_TEST: Mutex<()> = Mutex::new(());

#[cfg(test)]
pub(crate) fn take_choice() -> Option<Choice> {
    CHOICE.0.lock().unwrap_or_else(|e| e.into_inner()).take()
}

/// Shows the countdown, then waits for it to run out, the miner's choice,
/// blocked mining or shutdown.
fn count_down(tag: &str, shutdown: &AtomicBool) -> CountdownEnd {
    let (lock, cvar) = &CHOICE;
    *lock.lock().unwrap_or_else(|e| e.into_inner()) = None;
    if MINING_BLOCKED.load(Ordering::Acquire) {
        ui::info(
            "UPDATE",
            format!("installing seine {tag} now: mining is paused until Seine is updated"),
        );
        return CountdownEnd::Install;
    }
    let deadline = Instant::now() + COUNTDOWN;
    let shows = ui::set_pending_install(Some(PendingInstall {
        latest: tag.to_string(),
        deadline: Some(deadline),
    }));
    let secs = COUNTDOWN.as_secs();
    if shows {
        ui::note(
            "UPDATE",
            format!(
                "seine {tag} installs in {secs}s: press u to update now or s to skip this version"
            ),
        );
    }
    ui::detail(
        "UPDATE",
        format!("installing seine {tag} in {secs}s; mining continues until then"),
    );
    let mut choice = lock.lock().unwrap_or_else(|e| e.into_inner());
    let end = loop {
        if shutdown.load(Ordering::SeqCst) {
            break CountdownEnd::Stopped;
        }
        match choice.take() {
            Some(Choice::Skip) => break CountdownEnd::Skip,
            Some(Choice::InstallNow) => break CountdownEnd::Install,
            None => {}
        }
        let now = Instant::now();
        if now >= deadline || MINING_BLOCKED.load(Ordering::Acquire) {
            break CountdownEnd::Install;
        }
        // Wake each second to notice shutdown, which has no condvar.
        let wait = deadline
            .saturating_duration_since(now)
            .min(Duration::from_secs(1));
        choice = cvar
            .wait_timeout(choice, wait)
            .unwrap_or_else(|e| e.into_inner())
            .0;
    };
    drop(choice);
    ui::set_pending_install(match end {
        CountdownEnd::Install => Some(PendingInstall {
            latest: tag.to_string(),
            deadline: None,
        }),
        CountdownEnd::Skip | CountdownEnd::Stopped => None,
    });
    end
}

fn skip(data_dir: &Path, tag: &str, current: &str) {
    let _ = std::fs::write(data_dir.join(SKIPPED_FILE), tag);
    ui::info(
        "UPDATE",
        format!(
            "skipped seine {tag}; staying on {current} (seine --self-update installs it later)"
        ),
    );
}

/// A skipped release is not offered again, unless mining is paused until
/// Seine is updated: then it installs anyway.
fn offers_install(tag: &str, skipped: Option<&str>, mining_blocked: bool) -> bool {
    mining_blocked || skipped != Some(tag)
}

fn skipped_tag(data_dir: &Path) -> Option<String> {
    let text = std::fs::read_to_string(data_dir.join(SKIPPED_FILE)).ok()?;
    let tag = text.trim();
    (!tag.is_empty()).then(|| tag.to_string())
}

/// Logs the update a previous process installed before restarting into this
/// one. Call once the dashboard (if any) is up.
pub(crate) fn report_installed(data_dir: &Path) {
    let path = data_dir.join(INSTALLED_FILE);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return;
    };
    let _ = std::fs::remove_file(&path);
    let mut parts = text.split_whitespace();
    let (Some(from), Some(to), None) = (parts.next(), parts.next(), parts.next()) else {
        return;
    };
    if parse_version(to) == parse_version(crate::version::release_version()) {
        ui::success("UPDATE", format!("updated to seine {to} (was {from})"));
    }
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
fn apply_update(
    release: crate::self_update::Release,
    data_dir: &Path,
    shutdown: &Arc<AtomicBool>,
) -> Option<bool> {
    let result = crate::self_update::install(release);
    if !matches!(result, Ok(crate::self_update::Outcome::Updated { .. })) {
        ui::set_pending_install(None);
    }
    match result {
        Ok(crate::self_update::Outcome::Updated { from, to, .. }) => {
            let _ = std::fs::write(data_dir.join(INSTALLED_FILE), format!("{from} {to}"));
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

/// True when `candidate` is a later version than `current`. A final release
/// is later than a prerelease of the same version (`v0.3.0` > `v0.3.0-rc1`);
/// `git describe` suffixes (`v0.3.0-4-gabc1234`) are not prereleases.
pub(crate) fn is_newer(candidate: &str, current: &str) -> bool {
    match (parse_version(candidate), parse_version(current)) {
        (Some(c), Some(r)) if c == r => is_prerelease(current) && !is_prerelease(candidate),
        (Some(candidate), Some(current)) => candidate > current,
        _ => false,
    }
}

fn is_prerelease(raw: &str) -> bool {
    let core = raw.trim().split('+').next().unwrap_or_default();
    let Some((_, suffix)) = core.split_once('-') else {
        return false;
    };
    // `N-gHASH` from `git describe` marks commits after a tag.
    let describe = suffix.split_once("-g").is_some_and(|(count, hash)| {
        !count.is_empty()
            && count.bytes().all(|b| b.is_ascii_digit())
            && !hash.is_empty()
            && hash.bytes().all(|b| b.is_ascii_hexdigit())
    });
    !describe
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

    fn countdown_with(
        answer: impl FnOnce(&std::sync::atomic::AtomicBool) + Send,
    ) -> super::CountdownEnd {
        use std::sync::atomic::AtomicBool;
        let shutdown = AtomicBool::new(false);
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| super::count_down("v9.9.9", &shutdown));
            std::thread::sleep(Duration::from_millis(50));
            answer(&shutdown);
            waiter.join().unwrap()
        })
    }

    #[test]
    fn countdown_ends_on_the_miners_choice_or_shutdown() {
        let _serial = super::CHOICE_TEST.lock().unwrap_or_else(|e| e.into_inner());
        let started = Instant::now();
        assert!(matches!(
            countdown_with(|_| super::choose(super::Choice::Skip)),
            super::CountdownEnd::Skip
        ));
        assert!(matches!(
            countdown_with(|_| super::choose(super::Choice::InstallNow)),
            super::CountdownEnd::Install
        ));
        assert!(matches!(
            countdown_with(|stop| stop.store(true, std::sync::atomic::Ordering::SeqCst)),
            super::CountdownEnd::Stopped
        ));
        assert!(matches!(
            countdown_with(|_| super::set_mining_blocked(true)),
            super::CountdownEnd::Install
        ));
        // Already blocked: no countdown at all.
        assert!(matches!(
            countdown_with(|_| {}),
            super::CountdownEnd::Install
        ));
        super::set_mining_blocked(false);
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn a_choice_from_an_earlier_countdown_does_not_carry_over() {
        let _serial = super::CHOICE_TEST.lock().unwrap_or_else(|e| e.into_inner());
        super::choose(super::Choice::Skip);
        assert!(matches!(
            countdown_with(|_| super::choose(super::Choice::InstallNow)),
            super::CountdownEnd::Install
        ));
    }

    #[test]
    fn skipped_release_is_remembered_in_the_data_dir() {
        let dir = std::env::temp_dir().join(format!("seine-update-skip-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(super::skipped_tag(&dir), None);
        super::skip(&dir, "v9.9.9", "v0.1.0");
        assert_eq!(super::skipped_tag(&dir).as_deref(), Some("v9.9.9"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_skipped_release_installs_once_mining_is_paused_for_it() {
        assert!(!super::offers_install("v9.9.9", Some("v9.9.9"), false));
        assert!(super::offers_install("v9.9.9", Some("v9.9.9"), true));
        assert!(super::offers_install("v9.9.10", Some("v9.9.9"), false));
        assert!(super::offers_install("v9.9.9", None, false));
    }

    #[test]
    fn installed_marker_is_consumed_once() {
        let dir =
            std::env::temp_dir().join(format!("seine-update-installed-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let marker = dir.join(super::INSTALLED_FILE);
        std::fs::write(&marker, "v0.0.1 v9.9.9").unwrap();
        super::report_installed(&dir);
        assert!(!marker.exists());
        super::report_installed(&dir);
        std::fs::remove_dir_all(&dir).unwrap();
    }

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

    #[test]
    fn final_release_is_newer_than_its_prereleases() {
        assert!(is_newer("v0.3.0", "v0.3.0-rc1"));
        assert!(is_newer("v0.3.0", "0.3.0-beta.2"));
        assert!(!is_newer("v0.3.0-rc2", "v0.3.0"));
        assert!(!is_newer("v0.3.0-rc1", "v0.3.0-rc1"));
        assert!(!is_newer("v0.3.0", "v0.3.0-4-gabc1234"));
        assert!(!is_newer("v0.3.0", "v0.3.0+build.7"));
        assert!(is_newer("v0.3.1", "v0.3.0-rc1"));
    }
}
