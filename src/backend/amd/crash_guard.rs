//! Keeps CPU mining alive when the AMD GPU runtime ends the process.
//!
//! A fault inside the native HIP runtime cannot be caught and takes every other
//! backend down with it, so a card or driver that cannot run compute would stop
//! the whole miner on every start. A small file in the data directory records
//! each native call that was still running when the process ended. After
//! `SKIP_AFTER_FAILED_STARTS` such starts in a row, automatic GPU selection skips
//! AMD. Listing `amd` in `--backend` always tries again, and so does a new version.
//!
//! A start only counts as failed when the process died inside a guarded call:
//! backend errors, a clean exit and waiting for pool work do not count. Failed
//! starts are forgotten once the GPU has completed a few launches.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

const MARKER_FILE: &str = "amd-gpu-startup.json";
const SKIP_AFTER_FAILED_STARTS: u32 = 2;
/// Completed launches after which a GPU counts as working.
pub(crate) const LAUNCHES_BEFORE_WORKING: u32 = 3;

static GUARD: Mutex<Option<Guard>> = Mutex::new(None);

#[derive(Default, Deserialize, Serialize)]
struct Marker {
    version: String,
    failed_starts: u32,
    in_progress: bool,
}

struct Guard {
    path: PathBuf,
    version: String,
    failed_starts: u32,
    active_calls: u32,
}

impl Guard {
    fn load(data_dir: &Path, version: &str) -> Self {
        let path = data_dir.join(MARKER_FILE);
        let marker: Marker = fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default();
        let failed_starts = if marker.version == version {
            marker
                .failed_starts
                .saturating_add(u32::from(marker.in_progress))
        } else {
            0
        };
        let guard = Self {
            path,
            version: version.to_string(),
            failed_starts,
            active_calls: 0,
        };
        guard.save();
        guard
    }

    fn skips_automatic_start(&self) -> bool {
        self.failed_starts >= SKIP_AFTER_FAILED_STARTS
    }

    fn enter(&mut self) {
        self.active_calls += 1;
        if self.active_calls == 1 {
            self.save();
        }
    }

    fn leave(&mut self) {
        self.active_calls = self.active_calls.saturating_sub(1);
        if self.active_calls == 0 {
            self.save();
        }
    }

    fn mark_working(&mut self) {
        if self.failed_starts != 0 {
            self.failed_starts = 0;
            self.save();
        }
    }

    fn mark_clean_exit(&mut self) {
        self.active_calls = 0;
        self.save();
    }

    /// A failed write leaves the guard ineffective, never the miner stopped.
    fn save(&self) {
        if self.failed_starts == 0 && self.active_calls == 0 {
            let _ = fs::remove_file(&self.path);
            return;
        }
        let marker = Marker {
            version: self.version.clone(),
            failed_starts: self.failed_starts,
            in_progress: self.active_calls > 0,
        };
        if let Ok(bytes) = serde_json::to_vec(&marker) {
            if let Some(parent) = self.path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let _ = fs::write(&self.path, bytes);
        }
    }
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

/// Forgets earlier failed starts once a GPU has proved it can run kernels.
pub(crate) fn mark_working() {
    with_guard(Guard::mark_working);
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
        assert!(Guard::load(&dir, "v1").skips_automatic_start());

        let mut guard = Guard::load(&dir, "v1");
        guard.mark_working();
        assert!(!dir.join(MARKER_FILE).exists());
        assert!(!Guard::load(&dir, "v1").skips_automatic_start());
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
