//! A double-clicked miner gets a console window of its own. Give it a title and
//! enough rows for the dashboard log. Windows opened by the user are theirs:
//! shells, other terminals and redirected output are left exactly as they are.

/// Target size for a miner-owned classic console window.
#[cfg(any(target_os = "windows", test))]
const OWN_CONSOLE_SIZE: (u16, u16) = (120, 40);

/// Grows only: a window the user already made larger stays that way.
#[cfg(any(target_os = "windows", test))]
fn grown_size(current: (u16, u16), target: (u16, u16)) -> Option<(u16, u16)> {
    let grown = (current.0.max(target.0), current.1.max(target.1));
    (grown != current).then_some(grown)
}

pub(crate) fn prepare_own_console() {
    #[cfg(target_os = "windows")]
    {
        use crossterm::{terminal, Command};
        use std::io::IsTerminal;
        use windows_sys::Win32::System::Console::{
            GetConsoleProcessList, GetConsoleWindow, SetConsoleTitleW,
        };
        use windows_sys::Win32::UI::WindowsAndMessaging::GetClassNameW;

        if !std::io::stdout().is_terminal() {
            return;
        }
        let mut process = 0u32;
        // More than one attached process means a shell owns this window too.
        if unsafe { GetConsoleProcessList(&mut process, 1) } != 1 {
            return;
        }
        let title: Vec<u16> = "seine\0".encode_utf16().collect();
        unsafe { SetConsoleTitleW(title.as_ptr()) };

        // Only the classic console host can be resized from here. Under Windows
        // Terminal the console is a hidden pseudo-console whose grid would stop
        // matching the visible window, cutting off the bottom of the dashboard.
        let mut class = [0u16; 32];
        let len =
            unsafe { GetClassNameW(GetConsoleWindow(), class.as_mut_ptr(), class.len() as i32) };
        if String::from_utf16_lossy(&class[..len.max(0) as usize]) != "ConsoleWindowClass" {
            return;
        }
        let Ok(current) = terminal::size() else {
            return;
        };
        if let Some((cols, rows)) = grown_size(current, OWN_CONSOLE_SIZE) {
            // Too large for the monitor fails without changing anything.
            let _ = terminal::SetSize(cols, rows).execute_winapi();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{grown_size, OWN_CONSOLE_SIZE};

    #[test]
    fn only_grows_the_window() {
        assert_eq!(grown_size((120, 30), OWN_CONSOLE_SIZE), Some((120, 40)));
        assert_eq!(grown_size((80, 25), OWN_CONSOLE_SIZE), Some((120, 40)));
        assert_eq!(grown_size((200, 30), OWN_CONSOLE_SIZE), Some((200, 40)));
        assert_eq!(grown_size((120, 40), OWN_CONSOLE_SIZE), None);
        assert_eq!(grown_size((200, 60), OWN_CONSOLE_SIZE), None);
    }
}
