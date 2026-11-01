//! Best-effort status output. Losing a log destination must not stop mining.

macro_rules! println {
    () => { $crate::console_output::println!("") };
    ($($arg:tt)*) => {{
        use std::io::Write as _;
        let _ = writeln!(std::io::stdout().lock(), $($arg)*);
    }};
}

macro_rules! eprintln {
    ($($arg:tt)*) => {{
        use std::io::Write as _;
        let _ = writeln!(std::io::stderr().lock(), $($arg)*);
    }};
}

pub(crate) use {eprintln, println};
