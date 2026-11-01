//! Keep a double-clicked Windows console open long enough to read a fatal error.
//! Shell launches, redirected I/O and unattended invocations must never pause.

pub(crate) fn pause_after_error() {
    #[cfg(target_os = "windows")]
    {
        use std::io::{self, IsTerminal, Write};
        use windows_sys::Win32::System::Console::GetConsoleProcessList;

        let mut process = 0u32;
        // A one-element buffer is sufficient: a result > 1 means another
        // process (normally the launching shell) owns the console too.
        let attached = unsafe { GetConsoleProcessList(&mut process, 1) };
        if should_pause(
            std::env::args_os().len(),
            io::stdin().is_terminal(),
            io::stderr().is_terminal(),
            attached,
        ) {
            let _ = write!(io::stderr(), "\nPress Enter to close this window...");
            let _ = io::stderr().flush();
            let _ = io::stdin().read_line(&mut String::new());
        }
    }
}

#[cfg(any(target_os = "windows", test))]
fn should_pause(arg_count: usize, input_tty: bool, output_tty: bool, attached: u32) -> bool {
    arg_count == 1 && input_tty && output_tty && attached == 1
}

#[cfg(test)]
mod tests {
    use super::should_pause;

    #[test]
    fn only_pauses_for_an_interactive_no_argument_launch_with_its_own_console() {
        assert!(should_pause(1, true, true, 1));
        assert!(!should_pause(1, true, true, 2)); // existing shell
        assert!(!should_pause(1, true, true, 0)); // API failure / no console
        assert!(!should_pause(2, true, true, 1)); // scripted or service arguments
        assert!(!should_pause(1, false, true, 1));
        assert!(!should_pause(1, true, false, 1));
    }
}
