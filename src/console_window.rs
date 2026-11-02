//! A double-clicked miner gets a console window of its own. Give it a title and
//! enough rows for the dashboard log. Windows opened by the user are theirs:
//! shells, other terminals and redirected output are left exactly as they are.

/// Sizes for a miner-owned classic console window, largest first. A smaller
/// screen falls through to the next one that fits.
#[cfg(any(target_os = "windows", test))]
const OWN_CONSOLE_SIZES: [(u16, u16); 2] = [(140, 45), (120, 40)];

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
        for target in OWN_CONSOLE_SIZES {
            let Some((cols, rows)) = grown_size(current, target) else {
                break;
            };
            if terminal::SetSize(cols, rows).execute_winapi().is_ok() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{grown_size, OWN_CONSOLE_SIZES};

    #[test]
    fn only_grows_the_window() {
        let target = OWN_CONSOLE_SIZES[0];
        assert_eq!(grown_size((120, 30), target), Some((140, 45)));
        assert_eq!(grown_size((80, 25), target), Some((140, 45)));
        assert_eq!(grown_size((200, 30), target), Some((200, 45)));
        assert_eq!(grown_size((140, 45), target), None);
        assert_eq!(grown_size((200, 60), target), None);
    }

    #[test]
    fn sizes_run_largest_first() {
        assert!(OWN_CONSOLE_SIZES
            .windows(2)
            .all(|pair| pair[0].0 >= pair[1].0 && pair[0].1 > pair[1].1));
    }
}
