//! amdgpu DPM performance level for the HIP backend (Linux sysfs).
//!
//! At startup the backend sets each selected RDNA3 (gfx11) device's
//! `power_dpm_force_performance_level` to "high" when the file is writable,
//! prints the command when it is not, and restores the previous value when the
//! miner exits. Other GPUs keep the driver's level.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const LEVEL_FILE: &str = "power_dpm_force_performance_level";
const WANTED_LEVEL: &str = "high";
/// Set to any value to leave the driver's performance level alone.
pub const KEEP_ENV: &str = "SEINE_AMD_KEEP_PERF_LEVEL";

static CHANGED: Mutex<Vec<(PathBuf, String)>> = Mutex::new(Vec::new());

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

/// Raise the device's performance level to "high" if it is not already there,
/// remembering the previous value for [`restore_performance_levels`].
pub(crate) fn raise_performance_level(pci_address: &str, gcn_arch_name: &str) -> PerfLevelOutcome {
    // The level is a Linux amdgpu sysfs control.
    if cfg!(not(target_os = "linux"))
        || std::env::var_os(KEEP_ENV).is_some()
        || !raises_level_for(gcn_arch_name)
    {
        return PerfLevelOutcome::Skipped;
    }
    raise_at_path(&level_path_for_pci(pci_address))
}

pub(crate) fn raise_at_path(path: &Path) -> PerfLevelOutcome {
    let current = match fs::read_to_string(path) {
        Ok(value) => value.trim().to_string(),
        Err(_) => return PerfLevelOutcome::Unavailable,
    };
    if current == WANTED_LEVEL {
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
    if let Ok(mut changed) = CHANGED.lock() {
        if !changed.iter().any(|(recorded, _)| recorded == path) {
            changed.push((path.to_path_buf(), current.clone()));
        }
    }
    PerfLevelOutcome::Raised { from: current }
}

/// Put one device's level back to what it was, if this process raised it.
pub(crate) fn restore_performance_level(pci_address: &str) {
    restore_at_path(&level_path_for_pci(pci_address));
}

pub(crate) fn restore_at_path(path: &Path) {
    let previous = match CHANGED.lock() {
        Ok(mut changed) => match changed.iter().position(|(recorded, _)| recorded == path) {
            Some(index) => changed.remove(index).1,
            None => return,
        },
        Err(_) => return,
    };
    let _ = fs::write(path, format!("{previous}\n"));
}

/// Put every level this process raised back to what it was.
pub fn restore_performance_levels() {
    let changed = match CHANGED.lock() {
        Ok(mut changed) => std::mem::take(&mut *changed),
        Err(_) => return,
    };
    for (path, previous) in changed {
        let _ = fs::write(&path, format!("{previous}\n"));
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

    #[test]
    fn raise_and_restore_round_trip_on_a_writable_file() {
        let dir = std::env::temp_dir().join(format!("seine-dpm-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
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
        let _ = fs::remove_dir_all(&dir);
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
