//! Keeps CPU mining alive when the AMD GPU runtime ends the process.
//!
//! A fault inside the native HIP runtime cannot be caught and takes every other
//! backend down with it, so a card or driver that cannot run compute would stop
//! the whole miner on every start. A small file in the data directory records
//! each native call that was still running when the process ended. After
//! `SKIP_AFTER_FAILED_STARTS` such starts in a row, automatic GPU selection skips
//! AMD. Listing `amd` in `--backend` always tries again, and so does a new version.
//!
//! Each process keeps its own call file next to a lock file it holds while it
//! runs, so seine processes sharing a data directory never count each other's
//! calls as crashes; a call file whose lock is free belonged to a process that
//! ended inside the call.
//!
//! A start only counts as failed when the process died inside a guarded call:
//! backend errors, a clean exit and waiting for pool work do not count. Failed
//! starts are forgotten once every AMD card of the run has completed a few
//! launches or stopped without a crash, and at least one of them worked.

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

const MARKER_FILE: &str = "amd-gpu-startup.json";
/// `<prefix><instance>`: a native call of that process is running; holds the version.
const CALL_PREFIX: &str = "amd-gpu-call-";
/// `<prefix><instance>.lock`: locked for as long as that process runs.
const LOCK_PREFIX: &str = "amd-gpu-instance-";
const SKIP_AFTER_FAILED_STARTS: u32 = 2;
/// Completed launches after which a GPU counts as working.
pub(crate) const LAUNCHES_BEFORE_WORKING: u32 = 3;

static GUARD: Mutex<Option<Guard>> = Mutex::new(None);

#[derive(Default, Deserialize, Serialize)]
struct Marker {
    version: String,
    failed_starts: u32,
    /// Written by earlier versions, which kept one call flag for all processes.
    #[serde(default)]
    in_progress: bool,
}

struct Guard {
    path: PathBuf,
    call_path: PathBuf,
    lock_path: PathBuf,
    /// Held while this process runs; the OS releases it if the process dies.
    _lock: Option<File>,
    version: String,
    failed_starts: u32,
    active_calls: u32,
    /// Cards in this process that have neither worked nor stopped.
    unproven_cards: u32,
    any_card_worked: bool,
}

impl Guard {
    fn load(data_dir: &Path, version: &str) -> Self {
        let path = data_dir.join(MARKER_FILE);
        let marker: Marker = fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default();
        let mut failed_starts = if marker.version == version {
            marker
                .failed_starts
                .saturating_add(u32::from(marker.in_progress))
        } else {
            0
        };
        failed_starts = failed_starts.saturating_add(collect_ended_calls(data_dir, version));
        let instance = instance_id();
        let lock_path = data_dir.join(format!("{LOCK_PREFIX}{instance}.lock"));
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .ok()
            .filter(|file| file.try_lock().is_ok());
        let guard = Self {
            path,
            call_path: data_dir.join(format!("{CALL_PREFIX}{instance}")),
            lock_path,
            _lock: lock,
            version: version.to_string(),
            failed_starts,
            active_calls: 0,
            unproven_cards: 0,
            any_card_worked: false,
        };
        guard.save();
        guard
    }

    fn skips_automatic_start(&self) -> bool {
        self.failed_starts >= SKIP_AFTER_FAILED_STARTS
    }

    fn enter(&mut self) {
        self.active_calls += 1;
        if self.active_calls == 1 && self._lock.is_some() {
            let _ = fs::write(&self.call_path, &self.version);
        }
    }

    fn leave(&mut self) {
        self.active_calls = self.active_calls.saturating_sub(1);
        if self.active_calls == 0 {
            let _ = fs::remove_file(&self.call_path);
        }
    }

    fn card_added(&mut self) {
        self.unproven_cards += 1;
    }

    fn card_settled(&mut self, worked: bool) {
        self.unproven_cards = self.unproven_cards.saturating_sub(1);
        self.any_card_worked |= worked;
        if self.unproven_cards == 0 && self.any_card_worked && self.failed_starts != 0 {
            self.failed_starts = 0;
            self.save();
        }
    }

    fn mark_clean_exit(&mut self) {
        self.active_calls = 0;
        let _ = fs::remove_file(&self.call_path);
        // Released on exit anyway; removing it keeps the directory tidy where
        // the OS allows deleting an open file.
        let _ = fs::remove_file(&self.lock_path);
        self.save();
    }

    /// A failed write leaves the guard ineffective, never the miner stopped.
    fn save(&self) {
        if self.failed_starts == 0 {
            let _ = fs::remove_file(&self.path);
            return;
        }
        let marker = Marker {
            version: self.version.clone(),
            failed_starts: self.failed_starts,
            in_progress: false,
        };
        if let Ok(bytes) = serde_json::to_vec(&marker) {
            if let Some(parent) = self.path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let _ = fs::write(&self.path, bytes);
        }
    }
}

/// A name no other live process uses.
fn instance_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    format!(
        "{}-{nanos:x}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// Counts calls of `version` left by processes that ended inside them, and
/// clears the files of every process that has ended.
fn collect_ended_calls(data_dir: &Path, version: &str) -> u32 {
    let Ok(entries) = fs::read_dir(data_dir) else {
        return 0;
    };
    let mut ended = 0u32;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let instance = if let Some(instance) = name.strip_prefix(CALL_PREFIX) {
            instance
        } else if let Some(instance) = name
            .strip_prefix(LOCK_PREFIX)
            .and_then(|rest| rest.strip_suffix(".lock"))
        {
            instance
        } else {
            continue;
        };
        let lock_path = data_dir.join(format!("{LOCK_PREFIX}{instance}.lock"));
        let owner_running = OpenOptions::new()
            .write(true)
            .open(&lock_path)
            .is_ok_and(|file| file.try_lock().is_err());
        if owner_running {
            continue;
        }
        // Each file is handled under its own entry, so a call is counted
        // whichever of the two the directory lists first.
        if name.starts_with(CALL_PREFIX) {
            let call_path = data_dir.join(name);
            if fs::read_to_string(&call_path).is_ok_and(|call| call.trim() == version) {
                ended = ended.saturating_add(1);
            }
            let _ = fs::remove_file(&call_path);
        } else {
            let _ = fs::remove_file(&lock_path);
        }
    }
    ended
}

fn with_guard<T>(apply: impl FnOnce(&mut Guard) -> T) -> Option<T> {
    GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_mut()
        .map(apply)
}

/// Turns the guard on for this mining process. Later calls keep the first state,
/// so an in-process mining restart does not count as a new start.
pub(crate) fn init(data_dir: &Path) {
    let mut guard = GUARD.lock().unwrap_or_else(|e| e.into_inner());
    if guard.is_none() {
        *guard = Some(Guard::load(data_dir, crate::version::release_version()));
    }
}

/// Whether automatic GPU selection should leave AMD out for this run.
pub(crate) fn skips_automatic_start() -> bool {
    with_guard(|guard| guard.skips_automatic_start()).unwrap_or(false)
}

/// Marks a native runtime call as running until the returned scope is dropped.
#[must_use]
pub(crate) fn enter_native_call() -> NativeCall {
    with_guard(Guard::enter);
    NativeCall
}

pub(crate) struct NativeCall;

impl Drop for NativeCall {
    fn drop(&mut self) {
        with_guard(Guard::leave);
    }
}

/// One AMD card of this process. Earlier failed starts are forgotten only when
/// every card has worked or gone away, so a working card cannot hide one that
/// keeps crashing.
pub(crate) struct Card {
    settled: AtomicBool,
}

impl Card {
    pub(crate) fn new() -> Self {
        with_guard(Guard::card_added);
        Self {
            settled: AtomicBool::new(false),
        }
    }

    /// The card has proved it can run kernels.
    pub(crate) fn mark_working(&self) {
        if !self.settled.swap(true, Ordering::AcqRel) {
            with_guard(|guard| guard.card_settled(true));
        }
    }
}

impl Drop for Card {
    fn drop(&mut self) {
        if !self.settled.swap(true, Ordering::AcqRel) {
            with_guard(|guard| guard.card_settled(false));
        }
    }
}

/// The process is exiting on its own, so a call still running is not a failure.
pub(crate) fn mark_clean_exit() {
    with_guard(Guard::mark_clean_exit);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "seine-amd-crash-guard-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// One process that dies inside a native call.
    fn crashed_start(dir: &Path, version: &str) -> bool {
        let mut guard = Guard::load(dir, version);
        let skipped = guard.skips_automatic_start();
        guard.enter();
        skipped
    }

    #[test]
    fn two_crashed_starts_in_a_row_skip_the_automatic_start() {
        let dir = temp_dir("crashes");
        assert!(!crashed_start(&dir, "v1"));
        assert!(!crashed_start(&dir, "v1"));
        assert!(Guard::load(&dir, "v1").skips_automatic_start());
        // It keeps skipping on later starts of the same version.
        assert!(Guard::load(&dir, "v1").skips_automatic_start());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_new_version_tries_again() {
        let dir = temp_dir("version");
        crashed_start(&dir, "v1");
        crashed_start(&dir, "v1");
        assert!(!Guard::load(&dir, "v2").skips_automatic_start());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn calls_that_return_and_clean_exits_do_not_count() {
        let dir = temp_dir("clean");
        for _ in 0..3 {
            let mut guard = Guard::load(&dir, "v1");
            guard.enter();
            guard.leave();
        }
        for _ in 0..3 {
            let mut guard = Guard::load(&dir, "v1");
            guard.enter();
            guard.mark_clean_exit();
        }
        assert!(!dir.join(MARKER_FILE).exists());
        assert!(!Guard::load(&dir, "v1").skips_automatic_start());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn nested_calls_stay_in_progress_until_the_last_one_returns() {
        let dir = temp_dir("nested");
        let mut guard = Guard::load(&dir, "v1");
        guard.enter();
        guard.enter();
        guard.leave();
        // The process dies while the second card is still inside its call.
        drop(guard);
        assert_eq!(Guard::load(&dir, "v1").failed_starts, 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_failed_start_survives_a_detection_that_returns_until_the_gpu_works() {
        let dir = temp_dir("working");
        crashed_start(&dir, "v1");
        // The next run gets through detection, then dies at its first kernel.
        let mut guard = Guard::load(&dir, "v1");
        guard.enter();
        guard.leave();
        guard.enter();
        drop(guard);
        assert!(Guard::load(&dir, "v1").skips_automatic_start());

        let mut guard = Guard::load(&dir, "v1");
        guard.card_added();
        guard.card_settled(true);
        assert!(!dir.join(MARKER_FILE).exists());
        assert!(!Guard::load(&dir, "v1").skips_automatic_start());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_working_card_does_not_clear_another_that_has_not_worked() {
        let dir = temp_dir("cards");
        crashed_start(&dir, "v1");
        let mut guard = Guard::load(&dir, "v1");
        guard.card_added();
        guard.card_added();
        guard.card_settled(true);
        // The second card dies in its first launch.
        guard.enter();
        drop(guard);
        assert!(Guard::load(&dir, "v1").skips_automatic_start());

        // A card that stops cleanly without working does not hold the others back.
        let mut guard = Guard::load(&dir, "v1");
        guard.card_added();
        guard.card_added();
        guard.card_settled(false);
        assert_eq!(guard.failed_starts, 2);
        guard.card_settled(true);
        assert!(!dir.join(MARKER_FILE).exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_running_process_in_a_native_call_does_not_count_for_another() {
        let dir = temp_dir("shared");
        let mut first = Guard::load(&dir, "v1");
        first.enter();
        // A second seine in the same data directory starts twice meanwhile.
        assert_eq!(Guard::load(&dir, "v1").failed_starts, 0);
        assert_eq!(Guard::load(&dir, "v1").failed_starts, 0);
        first.leave();
        first.mark_clean_exit();
        drop(first);
        assert_eq!(Guard::load(&dir, "v1").failed_starts, 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_marker_from_an_earlier_version_still_counts_its_call() {
        let dir = temp_dir("legacy");
        fs::write(
            dir.join(MARKER_FILE),
            br#"{"version":"v1","failed_starts":1,"in_progress":true}"#,
        )
        .unwrap();
        assert!(Guard::load(&dir, "v1").skips_automatic_start());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_damaged_file_counts_as_no_failures() {
        let dir = temp_dir("damaged");
        fs::write(dir.join(MARKER_FILE), b"{not json").unwrap();
        assert!(!Guard::load(&dir, "v1").skips_automatic_start());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_unwritable_data_directory_never_skips_or_fails() {
        let dir = temp_dir("unwritable");
        let blocker = dir.join("file");
        fs::write(&blocker, b"").unwrap();
        let mut guard = Guard::load(&blocker, "v1");
        guard.enter();
        guard.leave();
        assert!(!guard.skips_automatic_start());
        let _ = fs::remove_dir_all(&dir);
    }
}
