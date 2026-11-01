//! Console shutdown admission. Windows terminates the process when a close
//! handler returns, so keep that handler alive briefly while normal cleanup runs.
use seine_telemetry::ShutdownRequest;
use std::sync::{
    atomic::{AtomicBool, AtomicU8, Ordering},
    Arc,
};

static REQUEST: AtomicU8 = AtomicU8::new(0);

pub(crate) fn requested() -> Option<ShutdownRequest> {
    match REQUEST.load(Ordering::Acquire) {
        1 => Some(ShutdownRequest::Interrupt),
        2 => Some(ShutdownRequest::ConsoleClose),
        3 => Some(ShutdownRequest::Logoff),
        4 => Some(ShutdownRequest::SystemShutdown),
        5 => Some(ShutdownRequest::AutoUpdate),
        _ => None,
    }
}

/// `--auto-update` installed a newer release: stop mining the way an interrupt
/// does, so `main` can restart the updated executable with the same arguments.
pub(crate) fn request_auto_update(shutdown: &Arc<AtomicBool>) {
    REQUEST
        .compare_exchange(0, 5, Ordering::AcqRel, Ordering::Acquire)
        .ok();
    shutdown.store(true, Ordering::SeqCst);
}

pub(crate) struct Guard;

#[cfg(not(target_os = "windows"))]
pub(crate) fn install(shutdown: Arc<AtomicBool>) -> anyhow::Result<Guard> {
    ctrlc::set_handler(move || {
        REQUEST
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .ok();
        if shutdown.swap(true, Ordering::SeqCst) {
            // A second interrupt skips normal cleanup; still undo the AMD
            // performance level change.
            crate::backend::amd::restore_performance_levels();
            std::process::exit(130);
        }
    })?;
    Ok(Guard)
}

#[cfg(target_os = "windows")]
mod windows {
    use super::*;
    use std::sync::OnceLock;
    use windows_sys::Win32::System::Console::*;
    use windows_sys::Win32::System::SystemInformation::GetTickCount64;
    use windows_sys::Win32::System::Threading::Sleep;

    static SHUTDOWN: OnceLock<Arc<AtomicBool>> = OnceLock::new();
    pub(super) static COMPLETE: AtomicBool = AtomicBool::new(false);

    // No allocation, locks, logging, disk or network IO in the OS callback.
    unsafe extern "system" fn handler(event: u32) -> i32 {
        let (request, closing) = match event {
            CTRL_C_EVENT | CTRL_BREAK_EVENT => (1, false),
            CTRL_CLOSE_EVENT => (2, true),
            CTRL_LOGOFF_EVENT => (3, true),
            CTRL_SHUTDOWN_EVENT => (4, true),
            _ => return 0,
        };
        let Some(shutdown) = SHUTDOWN.get() else {
            return 0;
        };
        REQUEST
            .compare_exchange(0, request, Ordering::AcqRel, Ordering::Acquire)
            .ok();
        if shutdown.swap(true, Ordering::SeqCst) && !closing {
            std::process::exit(130);
        }
        if closing {
            // Stay below Windows' usual five-second console-close deadline.
            // Cleanup may finish sooner; a stuck backend cannot hang close forever.
            let started = GetTickCount64();
            while GetTickCount64().wrapping_sub(started) < 4_000 {
                if COMPLETE.load(Ordering::Acquire) {
                    break;
                }
                Sleep(10);
            }
        }
        1
    }

    pub(super) fn install(shutdown: Arc<AtomicBool>) -> anyhow::Result<Guard> {
        SHUTDOWN
            .set(shutdown)
            .map_err(|_| anyhow::anyhow!("console handler already installed"))?;
        if unsafe { SetConsoleCtrlHandler(Some(handler), 1) } == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Guard)
    }
}

#[cfg(target_os = "windows")]
pub(crate) fn install(shutdown: Arc<AtomicBool>) -> anyhow::Result<Guard> {
    windows::install(shutdown)
}

impl Drop for Guard {
    fn drop(&mut self) {
        #[cfg(target_os = "windows")]
        windows::COMPLETE.store(true, Ordering::Release);
    }
}
