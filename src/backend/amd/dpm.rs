//! amdgpu DPM performance level for the HIP backend (Linux sysfs).
//!
//! At startup the backend sets each selected RDNA3 (gfx11) device's
//! `power_dpm_force_performance_level` to "high" when the file is writable,
//! prints the command when it is not, and restores the previous value when the
//! miner exits. Other GPUs keep the driver's level.
//!
//! Each raise is also recorded under `/run/seine` (cleared at reboot) with the
//! owning process, so a run after one that was killed before it could restore
//! finds the level it left at "high" and restores it at its own exit.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const LEVEL_FILE: &str = "power_dpm_force_performance_level";
const WANTED_LEVEL: &str = "high";
/// Set to any value to leave the driver's performance level alone.
pub const KEEP_ENV: &str = "SEINE_AMD_KEEP_PERF_LEVEL";

/// Levels this process raised, with the value to restore. The lock also
/// orders raises against the exit-time restore.
static CHANGED: Mutex<Changed> = Mutex::new(Changed {
    levels: Vec::new(),
    exiting: false,
});

struct Changed {
    levels: Vec<(PathBuf, String)>,
    /// Set by the exit-time restore; later raises (a GPU still starting in
    /// the background) leave the level alone.
    exiting: bool,
}

const RECORD_DIR: &str = "/run/seine";

#[cfg(test)]
thread_local! {
    static TEST_RECORD_DIR: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

fn record_dir() -> PathBuf {
    #[cfg(test)]
    if let Some(dir) = TEST_RECORD_DIR.with(|dir| dir.borrow().clone()) {
        return dir;
    }
    PathBuf::from(RECORD_DIR)
}

fn record_path(level_path: &Path) -> PathBuf {
    let name: String = level_path
        .to_string_lossy()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    record_dir().join(format!("amd-perf-level{name}"))
}

/// The process that raised `level_path` and the value it replaced.
fn read_record(level_path: &Path) -> Option<(u32, String)> {
    let text = fs::read_to_string(record_path(level_path)).ok()?;
    let (pid, previous) = text.trim().split_once(' ')?;
    Some((pid.parse().ok()?, previous.trim().to_string()))
}

fn write_record(level_path: &Path, previous: &str) {
    let dir = record_dir();
    if fs::create_dir_all(&dir).is_ok() {
        let _ = fs::write(
            record_path(level_path),
            format!("{} {previous}\n", std::process::id()),
        );
    }
}

fn remove_record(level_path: &Path) {
    if read_record(level_path).is_some_and(|(pid, _)| pid == std::process::id()) {
        let _ = fs::remove_file(record_path(level_path));
    }
}

fn process_alive(pid: u32) -> bool {
    Path::new("/proc").join(pid.to_string()).exists()
}

fn restore(path: &Path, previous: &str) {
    let _ = fs::write(path, format!("{previous}\n"));
    remove_record(path);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PerfLevelOutcome {
    /// Already "high" (another tool or an earlier instance set it).
    AlreadyHigh,
    /// Raised from `from` to "high"; restored at exit.
    Raised { from: String },
    /// The sysfs file is not writable by this user.
    NeedsRoot { from: String },
    /// Opted out through the environment.
    Skipped,
    /// No sysfs entry (not amdgpu, or a platform without the attribute).
    Unavailable,
}

/// `/sys/bus/pci/devices/<address>/power_dpm_force_performance_level`.
pub(crate) fn level_path_for_pci(pci_address: &str) -> PathBuf {
    Path::new("/sys/bus/pci/devices")
        .join(pci_address)
        .join(LEVEL_FILE)
}

/// GPUs whose performance level Seine raises: RDNA3 (gfx11).
pub(crate) fn raises_level_for(gcn_arch_name: &str) -> bool {
    gcn_arch_name.trim().starts_with("gfx11")
}

/// Startup outcome per PCI address, for telemetry.
static TELEMETRY_LEVELS: Mutex<Vec<(String, seine_telemetry::AmdPerfLevel)>> =
    Mutex::new(Vec::new());

/// The performance-level outcome recorded for a device at startup.
pub(crate) fn telemetry_level(pci_address: &str) -> Option<seine_telemetry::AmdPerfLevel> {
    TELEMETRY_LEVELS
        .lock()
        .ok()?
        .iter()
        .find(|(address, _)| address == pci_address)
        .map(|(_, level)| *level)
}

fn note_telemetry_level(pci_address: &str, level: seine_telemetry::AmdPerfLevel) {
    if let Ok(mut levels) = TELEMETRY_LEVELS.lock() {
        match levels
            .iter_mut()
            .find(|(address, _)| address == pci_address)
        {
            // start() re-raises what detection raised; keep that it was us.
            Some(entry)
                if entry.1 == seine_telemetry::AmdPerfLevel::Raised
                    && level == seine_telemetry::AmdPerfLevel::AlreadyHigh => {}
            Some(entry) => entry.1 = level,
            None => levels.push((pci_address.to_string(), level)),
        }
    }
}

/// Raise the device's performance level to "high" if it is not already there,
/// remembering the previous value for [`restore_performance_levels`].
pub(crate) fn raise_performance_level(pci_address: &str, gcn_arch_name: &str) -> PerfLevelOutcome {
    use seine_telemetry::AmdPerfLevel;
    // The level is a Linux amdgpu sysfs control.
    let skipped = if cfg!(not(target_os = "linux")) {
        Some(AmdPerfLevel::Unavailable)
    } else if std::env::var_os(KEEP_ENV).is_some() {
        Some(AmdPerfLevel::KeptByOperator)
    } else if !raises_level_for(gcn_arch_name) {
        Some(AmdPerfLevel::NotRaisedForArch)
    } else {
        None
    };
    if let Some(level) = skipped {
        note_telemetry_level(pci_address, level);
        return PerfLevelOutcome::Skipped;
    }
    let outcome = raise_at_path(&level_path_for_pci(pci_address));
    note_telemetry_level(
        pci_address,
        match &outcome {
            PerfLevelOutcome::AlreadyHigh => AmdPerfLevel::AlreadyHigh,
            PerfLevelOutcome::Raised { .. } => AmdPerfLevel::Raised,
            PerfLevelOutcome::NeedsRoot { .. } => AmdPerfLevel::NeedsRoot,
            PerfLevelOutcome::Skipped => AmdPerfLevel::KeptByOperator,
            PerfLevelOutcome::Unavailable => AmdPerfLevel::Unavailable,
        },
    );
    outcome
}

pub(crate) fn raise_at_path(path: &Path) -> PerfLevelOutcome {
    let mut changed = CHANGED.lock().unwrap_or_else(|e| e.into_inner());
    if changed.exiting {
        return PerfLevelOutcome::Skipped;
    }
    let current = match fs::read_to_string(path) {
        Ok(value) => value.trim().to_string(),
        Err(_) => return PerfLevelOutcome::Unavailable,
    };
    if current == WANTED_LEVEL {
        // A run that raised it and died without restoring left a record:
        // take it over so this run restores the original level.
        let owned = changed.levels.iter().any(|(recorded, _)| recorded == path);
        if let Some((pid, previous)) = read_record(path) {
            if !owned
                && pid != std::process::id()
                && !process_alive(pid)
                && previous != WANTED_LEVEL
            {
                write_record(path, &previous);
                changed.levels.push((path.to_path_buf(), previous.clone()));
                return PerfLevelOutcome::Raised { from: previous };
            }
        }
        return PerfLevelOutcome::AlreadyHigh;
    }
    match fs::write(path, format!("{WANTED_LEVEL}\n")) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            return PerfLevelOutcome::NeedsRoot { from: current };
        }
        Err(_) => return PerfLevelOutcome::Unavailable,
    }
    // The driver may reject the value silently; confirm by reading it back.
    let now = fs::read_to_string(path)
        .map(|value| value.trim().to_string())
        .unwrap_or_default();
    if now != WANTED_LEVEL {
        return PerfLevelOutcome::Unavailable;
    }
    if !changed.levels.iter().any(|(recorded, _)| recorded == path) {
        write_record(path, &current);
        changed.levels.push((path.to_path_buf(), current.clone()));
    }
    PerfLevelOutcome::Raised { from: current }
}

/// Put one device's level back to what it was, if this process raised it.
pub(crate) fn restore_performance_level(pci_address: &str) {
    restore_at_path(&level_path_for_pci(pci_address));
}

pub(crate) fn restore_at_path(path: &Path) {
    let mut changed = CHANGED.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(index) = changed
        .levels
        .iter()
        .position(|(recorded, _)| recorded == path)
    {
        let (_, previous) = changed.levels.remove(index);
        restore(path, &previous);
    }
}

/// Put every level this process raised back to what it was. Raises after
/// this (a GPU still starting) leave the level alone.
pub fn restore_performance_levels() {
    let mut changed = CHANGED.lock().unwrap_or_else(|e| e.into_inner());
    changed.exiting = true;
    for (path, previous) in std::mem::take(&mut changed.levels) {
        restore(&path, &previous);
    }
}

/// One startup line per device, or none when there is nothing for the user to do.
pub(crate) fn startup_notice(
    device_index: u32,
    device_name: &str,
    pci_address: &str,
    outcome: &PerfLevelOutcome,
) -> Option<String> {
    match outcome {
        PerfLevelOutcome::Raised { from } => Some(format!(
            "AMD device {device_index} ({device_name}): performance level {from} -> high; restored at exit \
             (set {KEEP_ENV}=1 to leave it alone)"
        )),
        PerfLevelOutcome::NeedsRoot { from } => Some(format!(
            "AMD device {device_index} ({device_name}): performance level is '{from}'; run \
             `echo high | sudo tee {}` (or run seine as root) for more hashrate",
            level_path_for_pci(pci_address).display()
        )),
        PerfLevelOutcome::AlreadyHigh
        | PerfLevelOutcome::Skipped
        | PerfLevelOutcome::Unavailable => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_path_uses_the_pci_address() {
        assert_eq!(
            level_path_for_pci("0000:c1:00.0"),
            PathBuf::from("/sys/bus/pci/devices/0000:c1:00.0/power_dpm_force_performance_level")
        );
    }

    /// Serialises tests that raise or restore, since an exit-time restore
    /// stops later raises in the process.
    static DPM_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn use_test_record_dir(dir: &Path) -> std::sync::MutexGuard<'static, ()> {
        let guard = DPM_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        CHANGED.lock().unwrap_or_else(|e| e.into_inner()).exiting = false;
        TEST_RECORD_DIR.with(|record| *record.borrow_mut() = Some(dir.join("records")));
        guard
    }

    #[test]
    fn a_level_left_high_by_a_killed_run_is_restored_by_the_next() {
        let dir = std::env::temp_dir().join(format!("seine-dpm-dead-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let _lock = use_test_record_dir(&dir);
        let path = dir.join(LEVEL_FILE);
        fs::write(&path, "high\n").unwrap();
        // No record: someone else set it, so it is left alone.
        assert_eq!(raise_at_path(&path), PerfLevelOutcome::AlreadyHigh);
        // A record from a process that no longer exists is taken over.
        fs::create_dir_all(dir.join("records")).unwrap();
        fs::write(record_path(&path), format!("{} auto\n", u32::MAX)).unwrap();
        assert_eq!(
            raise_at_path(&path),
            PerfLevelOutcome::Raised {
                from: "auto".to_string()
            }
        );
        restore_at_path(&path);
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), "auto");
        assert!(!record_path(&path).exists());
        // A record from a live process (this one, standing in) is not.
        fs::write(&path, "high\n").unwrap();
        fs::write(record_path(&path), "1 auto\n").unwrap();
        assert_eq!(raise_at_path(&path), PerfLevelOutcome::AlreadyHigh);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn raise_and_restore_round_trip_on_a_writable_file() {
        let dir = std::env::temp_dir().join(format!("seine-dpm-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let _lock = use_test_record_dir(&dir);
        let path = dir.join(LEVEL_FILE);
        fs::write(&path, "auto\n").unwrap();
        assert_eq!(
            raise_at_path(&path),
            PerfLevelOutcome::Raised {
                from: "auto".to_string()
            }
        );
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), "high");
        assert_eq!(raise_at_path(&path), PerfLevelOutcome::AlreadyHigh);
        restore_at_path(&path);
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), "auto");
        // A second restore is a no-op, and the global restore has nothing left.
        fs::write(&path, "manual\n").unwrap();
        restore_at_path(&path);
        restore_performance_levels();
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), "manual");
        // After the exit-time restore, a late raise leaves the level alone.
        fs::write(&path, "auto\n").unwrap();
        assert_eq!(raise_at_path(&path), PerfLevelOutcome::Skipped);
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), "auto");
        CHANGED.lock().unwrap_or_else(|e| e.into_inner()).exiting = false;
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_repeated_raise_keeps_the_raised_telemetry() {
        use seine_telemetry::AmdPerfLevel;
        let address = "ffff:ff:ff.1";
        note_telemetry_level(address, AmdPerfLevel::Raised);
        note_telemetry_level(address, AmdPerfLevel::AlreadyHigh);
        assert_eq!(telemetry_level(address), Some(AmdPerfLevel::Raised));
        note_telemetry_level(address, AmdPerfLevel::NeedsRoot);
        assert_eq!(telemetry_level(address), Some(AmdPerfLevel::NeedsRoot));
    }

    #[test]
    fn raises_level_only_on_rdna3() {
        assert!(raises_level_for("gfx1101"));
        assert!(raises_level_for("gfx1100"));
        assert!(!raises_level_for("gfx1201"));
        assert!(!raises_level_for("gfx942:sramecc+:xnack-"));
        assert!(!raises_level_for("gfx1030"));
    }

    #[test]
    fn missing_file_is_unavailable() {
        assert_eq!(
            raise_at_path(Path::new(
                "/nonexistent/seine/power_dpm_force_performance_level"
            )),
            PerfLevelOutcome::Unavailable
        );
    }

    #[test]
    fn notices_only_when_the_user_can_act() {
        assert!(startup_notice(0, "x", "0000:01:00.0", &PerfLevelOutcome::AlreadyHigh).is_none());
        let raised = PerfLevelOutcome::Raised {
            from: "auto".into(),
        };
        assert!(startup_notice(0, "x", "0000:01:00.0", &raised)
            .unwrap()
            .contains("auto -> high"));
        let needs_root = PerfLevelOutcome::NeedsRoot {
            from: "auto".into(),
        };
        assert!(startup_notice(0, "x", "0000:01:00.0", &needs_root)
            .unwrap()
            .contains("sudo tee /sys/bus/pci/devices/0000:01:00.0/"));
    }
}
