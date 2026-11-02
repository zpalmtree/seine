use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Instant;

use super::tui::{LogEntry, LogLevel, TuiState};
use crate::console_output::{eprintln, println};

const FRAME_INNER_WIDTH: usize = 92;
const KEY_WIDTH: usize = 18;
const LOGO: &[&str] = &[
    "                     ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~",
    "               ~ ~ ~    ╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲    ~ ~ ~",
    "          ~ ~ ~       ╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲       ~ ~ ~",
    "        ~ ~         ╭──────────────  S E I N E  ──────────────╮         ~ ~",
    "          ~ ~ ~       ╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱       ~ ~ ~",
    "               ~ ~ ~    ╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱    ~ ~ ~",
    "                     ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~",
];

static COLOR_ENABLED: OnceLock<bool> = OnceLock::new();
static LOG_START: OnceLock<Instant> = OnceLock::new();
static OUTPUT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static TUI_STATE: Mutex<Option<TuiState>> = Mutex::new(None);
static LOG_SINK: OnceLock<Mutex<Option<LogSink>>> = OnceLock::new();
static VERBOSE: AtomicBool = AtomicBool::new(false);

type LogSink = Arc<dyn Fn(UiLogEvent) + Send + Sync + 'static>;

#[derive(Clone)]
pub(crate) struct UiLogEvent {
    pub elapsed_secs: f64,
    pub level: &'static str,
    pub tag: String,
    pub message: String,
}

/// Who a log line is for. The control API sink receives everything except
/// `DashboardOnly`, so nothing is lost to API clients.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Audience {
    Everyone,
    /// Plain logs; hidden on the dashboard unless `--verbose`.
    PlainLog,
    /// Only with `--verbose`.
    VerboseOnly,
    /// A short dashboard line standing in for the detail in plain logs.
    DashboardOnly,
}

#[derive(Clone, Copy)]
enum Level {
    Info,
    Success,
    Warn,
    Error,
    Mined,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Success => "success",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Mined => "mined",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Success => "OK",
            Self::Warn => "WARN",
            Self::Error => "ERR",
            Self::Mined => "**",
        }
    }

    fn level_style(self) -> &'static str {
        match self {
            Self::Info => "48;5;31;1;97",
            Self::Success => "48;5;28;1;97",
            Self::Warn => "48;5;214;1;30",
            Self::Error => "48;5;160;1;97",
            Self::Mined => "48;5;178;1;30",
        }
    }

    fn body_style(self) -> &'static str {
        match self {
            Self::Info => "38;5;153",
            Self::Success => "38;5;120",
            Self::Warn => "38;5;223",
            Self::Error => "38;5;217",
            Self::Mined => "1;38;5;220",
        }
    }

    fn use_stderr(self) -> bool {
        matches!(self, Self::Warn | Self::Error)
    }
}

pub(super) fn startup_banner(lines: &[(&str, String)]) {
    let colors = use_color();
    println!();
    frame_top(colors);
    frame_blank(colors);
    for (idx, line) in LOGO.iter().enumerate() {
        frame_center(line, logo_tone(idx), colors);
    }
    frame_blank(colors);
    frame_rule("runtime", colors);
    for (key, value) in lines {
        frame_kv(key, value, colors);
    }
    frame_bottom(colors);
    println!();
}

pub(crate) fn set_verbose(enabled: bool) {
    VERBOSE.store(enabled, Ordering::Relaxed);
}

pub(crate) fn info(tag: &str, message: impl AsRef<str>) {
    log(Level::Info, Audience::Everyone, tag, message.as_ref());
}

pub(crate) fn success(tag: &str, message: impl AsRef<str>) {
    log(Level::Success, Audience::Everyone, tag, message.as_ref());
}

/// Routine activity for plain logs; the dashboard already shows its effect.
pub(crate) fn detail(tag: &str, message: impl AsRef<str>) {
    log(Level::Info, Audience::PlainLog, tag, message.as_ref());
}

/// Internal settings, shown only with `--verbose`.
pub(crate) fn verbose(tag: &str, message: impl AsRef<str>) {
    log(Level::Info, Audience::VerboseOnly, tag, message.as_ref());
}

pub(crate) fn note(tag: &str, message: impl AsRef<str>) {
    log(Level::Info, Audience::DashboardOnly, tag, message.as_ref());
}

pub(crate) fn note_success(tag: &str, message: impl AsRef<str>) {
    log(
        Level::Success,
        Audience::DashboardOnly,
        tag,
        message.as_ref(),
    );
}

#[track_caller]
pub(crate) fn warn(tag: &str, message: impl AsRef<str>) {
    crate::diagnostics::record(
        seine_telemetry::DiagnosticCode::Warning,
        crate::diagnostics::classify(message.as_ref()),
        None,
    );
    log(Level::Warn, Audience::Everyone, tag, message.as_ref());
}

#[track_caller]
pub(super) fn error(tag: &str, message: impl AsRef<str>) {
    crate::diagnostics::record(
        seine_telemetry::DiagnosticCode::Error,
        crate::diagnostics::classify(message.as_ref()),
        None,
    );
    log(Level::Error, Audience::Everyone, tag, message.as_ref());
}

pub(super) fn mined(tag: &str, message: impl AsRef<str>) {
    log(Level::Mined, Audience::Everyone, tag, message.as_ref());
}

pub(super) fn notify_dev_fee_mode(is_dev: bool) {
    let mode = if is_dev {
        "mining for dev"
    } else {
        "mining for user"
    };
    emit_log_sink(UiLogEvent {
        elapsed_secs: log_elapsed().as_secs_f64(),
        level: "info",
        tag: "DEV FEE".to_string(),
        message: mode.to_string(),
    });
}

/// Logs a newer-release notice and flags it in the live dashboard header.
/// Returns false when no dashboard was live to receive it.
pub(crate) fn announce_update(latest: &str, message: &str) -> bool {
    let tui_state = lock(&TUI_STATE).clone();
    if let Some(state) = &tui_state {
        if let Ok(mut state) = state.lock() {
            state.update_available = Some(latest.to_string());
        }
    }
    info("UPDATE", message);
    tui_state.is_some()
}

pub(super) fn set_tui_state(state: TuiState) {
    *lock(&TUI_STATE) = Some(state);
}

pub(super) fn current_tui_state() -> Option<TuiState> {
    lock(&TUI_STATE).clone()
}

pub(super) fn set_share_difficulty(difficulty: Option<u64>) {
    if let Some(state) = current_tui_state() {
        if let Ok(mut state) = state.lock() {
            state.share_difficulty = difficulty;
        }
    }
}

/// Names what the miner is doing while there is no mining loop to report it.
pub(super) fn set_startup_phase(phase: &str) {
    if let Some(state) = current_tui_state() {
        if let Ok(mut state) = state.lock() {
            state.state = phase.to_string();
        }
    }
}

pub(super) fn clear_tui_state(state: &TuiState) {
    let mut slot = lock(&TUI_STATE);
    if slot
        .as_ref()
        .is_some_and(|active| Arc::ptr_eq(active, state))
    {
        *slot = None;
    }
}

pub(crate) fn set_log_sink(sink: Option<Arc<dyn Fn(UiLogEvent) + Send + Sync + 'static>>) {
    let mut slot = lock(log_sink_lock());
    *slot = sink;
}

fn level_to_tui(level: Level) -> LogLevel {
    match level {
        Level::Info => LogLevel::Info,
        Level::Success => LogLevel::Success,
        Level::Warn => LogLevel::Warn,
        Level::Error => LogLevel::Error,
        Level::Mined => LogLevel::Mined,
    }
}

fn log(level: Level, audience: Audience, tag: &str, message: &str) {
    let elapsed_secs = log_elapsed().as_secs_f64();
    if audience != Audience::DashboardOnly {
        emit_log_sink(UiLogEvent {
            elapsed_secs,
            level: level.as_str(),
            tag: tag.to_string(),
            message: message.to_string(),
        });
    }
    let verbose = VERBOSE.load(Ordering::Relaxed);

    let tui_state = lock(&TUI_STATE).clone();
    if let Some(tui_state) = tui_state {
        if let Ok(mut state) = tui_state.lock() {
            if !shows_on_dashboard(audience, verbose)
                || suppress_in_tui(tag, message)
                || suppress_dev_fee_related_in_tui(tag, message, state.dev_fee_active)
            {
                return;
            }
            let entry = LogEntry::new(
                state.started_at.elapsed().as_secs_f64(),
                level_to_tui(level),
                tag,
                message,
            );
            state.push_log(entry);
            return;
        }
    }

    if !shows_in_plain_log(audience, verbose) {
        return;
    }

    let colors = use_color();
    let time_plain = format!("{:>7.1}s", elapsed_secs);
    let level_plain = format!(" {:^4} ", level.label());
    let tag_plain = format!(" {:<8} ", tag);
    let prefix_plain = format!("{time_plain} {level_plain} {tag_plain}");
    let prefix = format!(
        "{} {} {}",
        paint(&time_plain, "2;37", colors),
        paint(&level_plain, level.level_style(), colors),
        paint(&tag_plain, "48;5;236;1;250", colors),
    );
    let max_body = if output_is_terminal() {
        terminal_columns()
            .saturating_sub(prefix_plain.chars().count() + 1)
            .max(16)
    } else {
        usize::MAX
    };
    let constrained = constrain_line(message, max_body);
    let body = style_message(&constrained, level.body_style(), colors);

    let _out_guard = lock(output_lock());
    if level.use_stderr() {
        eprintln!("{prefix} {body}");
    } else {
        println!("{prefix} {body}");
    }
}

fn emit_log_sink(event: UiLogEvent) {
    let sink = {
        let slot = lock(log_sink_lock());
        slot.clone()
    };
    if let Some(sink) = sink {
        sink(event);
    }
}

fn log_sink_lock() -> &'static Mutex<Option<LogSink>> {
    LOG_SINK.get_or_init(|| Mutex::new(None))
}

fn shows_on_dashboard(audience: Audience, verbose: bool) -> bool {
    match audience {
        Audience::Everyone | Audience::DashboardOnly => true,
        // Verbose runs keep the summary line next to the detail it replaces.
        Audience::PlainLog | Audience::VerboseOnly => verbose,
    }
}

fn shows_in_plain_log(audience: Audience, verbose: bool) -> bool {
    match audience {
        Audience::Everyone | Audience::PlainLog => true,
        Audience::VerboseOnly => verbose,
        Audience::DashboardOnly => false,
    }
}

fn suppress_in_tui(tag: &str, message: &str) -> bool {
    if matches!(tag, "BACKEND" | "BENCH") && message.starts_with("telemetry |") {
        return true;
    }
    if tag == "MINER" && message.starts_with("template-history |") {
        return true;
    }
    if tag == "DAEMON"
        && (message.starts_with("daemon did not return blocktemplate;")
            || message.starts_with("daemon still not returning blocktemplate"))
    {
        return true;
    }
    tag == "NETWORK" && message == "blocktemplate fetch recovered"
}

fn suppress_dev_fee_related_in_tui(tag: &str, message: &str, dev_fee_active: bool) -> bool {
    if tag == "DEV FEE" {
        return true;
    }
    if tag == "CONN" && message.starts_with("switching pool session:") {
        return true;
    }
    if matches!(tag, "CONN" | "AUTH") && message.contains("dev session") {
        return true;
    }
    if tag == "SHARE" && message.contains("(dev)") {
        return true;
    }
    dev_fee_active && matches!(tag, "CONN" | "AUTH" | "JOB" | "SHARE" | "VARDIFF")
}

fn frame_top(colors: bool) {
    println!(
        "{}{}{}",
        paint("╭", "1;34", colors),
        paint(&"─".repeat(FRAME_INNER_WIDTH), "1;34", colors),
        paint("╮", "1;34", colors),
    );
}

fn frame_bottom(colors: bool) {
    println!(
        "{}{}{}",
        paint("╰", "1;34", colors),
        paint(&"─".repeat(FRAME_INNER_WIDTH), "1;34", colors),
        paint("╯", "1;34", colors),
    );
}

fn frame_blank(colors: bool) {
    frame_row("", "0", colors);
}

fn frame_rule(label: &str, colors: bool) {
    let label = format!(" {} ", label.to_ascii_uppercase());
    let side = FRAME_INNER_WIDTH.saturating_sub(label.chars().count()) / 2;
    let right = FRAME_INNER_WIDTH
        .saturating_sub(label.chars().count())
        .saturating_sub(side);
    frame_row(
        &format!("{}{}{}", "─".repeat(side), label, "─".repeat(right)),
        "38;5;75",
        colors,
    );
}

fn frame_center(text: &str, style: &str, colors: bool) {
    let clipped = clip(text, FRAME_INNER_WIDTH);
    frame_row(
        &format!("{:^width$}", clipped, width = FRAME_INNER_WIDTH),
        style,
        colors,
    );
}

fn frame_kv(key: &str, value: &str, colors: bool) {
    let key_text = format!("{:<width$}", format!("{key}:"), width = KEY_WIDTH);
    let max_value = FRAME_INNER_WIDTH.saturating_sub(2 + KEY_WIDTH + 1);
    let value_text = clip(value, max_value);
    let used = 2 + key_text.chars().count() + 1 + value_text.chars().count();
    let padding = " ".repeat(FRAME_INNER_WIDTH.saturating_sub(used));

    println!(
        "{}  {} {}{}{}",
        paint("│", "1;34", colors),
        paint(&key_text, "1;96", colors),
        paint(&value_text, "1;97", colors),
        padding,
        paint("│", "1;34", colors),
    );
}

fn frame_row(text: &str, style: &str, colors: bool) {
    let row = format!(
        "{:<width$}",
        clip(text, FRAME_INNER_WIDTH),
        width = FRAME_INNER_WIDTH
    );
    println!(
        "{}{}{}",
        paint("│", "1;34", colors),
        paint(&row, style, colors),
        paint("│", "1;34", colors),
    );
}

fn style_message(message: &str, base_style: &str, colors: bool) -> String {
    if !colors || message.is_empty() {
        return message.to_string();
    }

    let mut styled = String::with_capacity(message.len() + 32);
    for part in message.split_inclusive(char::is_whitespace) {
        let token = part.trim_end_matches(char::is_whitespace);
        let whitespace = &part[token.len()..];
        if !token.is_empty() {
            styled.push_str(&style_token(token, base_style, colors));
        }
        styled.push_str(whitespace);
    }
    styled
}

fn style_token(token: &str, base_style: &str, colors: bool) -> String {
    if token == "|" {
        return paint("│", "2;37", colors);
    }

    if let Some((key, value)) = token.split_once('=') {
        if !key.is_empty() && !value.is_empty() {
            let (value_core, suffix) = split_trailing_punctuation(value);
            return format!(
                "{}{}{}{}",
                paint(key, "1;94", colors),
                paint("=", "2;37", colors),
                paint(value_core, "1;97", colors),
                maybe_paint(suffix, "2;37", colors),
            );
        }
    }

    if token.ends_with(':') {
        return paint(token, "1;94", colors);
    }

    let (core, suffix) = split_trailing_punctuation(token);
    if core.starts_with("http://") || core.starts_with("https://") {
        return format!(
            "{}{}",
            paint(core, "4;38;5;81", colors),
            maybe_paint(suffix, "2;37", colors)
        );
    }
    if looks_numeric(core) {
        return format!(
            "{}{}",
            paint(core, "1;96", colors),
            maybe_paint(suffix, "2;37", colors)
        );
    }

    format!(
        "{}{}",
        paint(core, base_style, colors),
        maybe_paint(suffix, "2;37", colors)
    )
}

fn split_trailing_punctuation(token: &str) -> (&str, &str) {
    let mut split = token.len();
    for (idx, ch) in token.char_indices().rev() {
        if matches!(ch, ',' | ';' | ')' | ']' | '}') {
            split = idx;
        } else {
            break;
        }
    }
    token.split_at(split)
}

fn looks_numeric(token: &str) -> bool {
    if token.is_empty() {
        return false;
    }
    let mut has_digit = false;
    for ch in token.chars() {
        if ch.is_ascii_digit() {
            has_digit = true;
            continue;
        }
        if matches!(ch, '.' | '%' | '/' | '_' | '+' | '-') {
            continue;
        }
        if ch.is_ascii_alphabetic() {
            continue;
        }
        return false;
    }
    has_digit
}

fn logo_tone(idx: usize) -> &'static str {
    match idx {
        0 | 6 => "38;5;45",
        1 | 5 => "38;5;81",
        2 | 4 => "38;5;117",
        _ => "1;97",
    }
}

fn clip(text: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    if text.chars().count() <= max_width {
        return text.to_string();
    }
    text.chars().take(max_width).collect()
}

fn log_elapsed() -> std::time::Duration {
    LOG_START.get_or_init(Instant::now).elapsed()
}

fn use_color() -> bool {
    *COLOR_ENABLED.get_or_init(|| {
        if let Some(force) = std::env::var_os("CLICOLOR_FORCE") {
            if force.to_string_lossy() != "0" {
                return true;
            }
        }
        if std::env::var_os("NO_COLOR").is_some() {
            return false;
        }
        if let Some(choice) = std::env::var_os("CLICOLOR") {
            if choice.to_string_lossy() == "0" {
                return false;
            }
        }
        if std::env::var("TERM")
            .map(|term| term.eq_ignore_ascii_case("dumb"))
            .unwrap_or(false)
        {
            return false;
        }
        std::io::stdout().is_terminal() || std::io::stderr().is_terminal()
    })
}

fn output_is_terminal() -> bool {
    std::io::stdout().is_terminal() || std::io::stderr().is_terminal()
}

fn terminal_columns() -> usize {
    crossterm::terminal::size()
        .map(|(cols, _)| cols as usize)
        .ok()
        .or_else(|| {
            std::env::var("COLUMNS")
                .ok()
                .and_then(|raw| raw.parse::<usize>().ok())
        })
        .filter(|cols| *cols >= 60)
        .unwrap_or(120)
}

fn constrain_line(message: &str, max_chars: usize) -> String {
    if max_chars == usize::MAX || message.chars().count() <= max_chars {
        return message.to_string();
    }
    if max_chars <= 3 {
        return ".".repeat(max_chars);
    }
    let keep = max_chars - 3;
    let mut out = String::with_capacity(max_chars);
    out.push_str(&message.chars().take(keep).collect::<String>());
    out.push_str("...");
    out
}

fn paint(text: &str, style: &str, enabled: bool) -> String {
    if enabled {
        format!("\x1b[{style}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

fn maybe_paint(text: &str, style: &str, enabled: bool) -> String {
    if text.is_empty() {
        String::new()
    } else {
        paint(text, style, enabled)
    }
}

fn output_lock() -> &'static Mutex<()> {
    OUTPUT_LOCK.get_or_init(|| Mutex::new(()))
}

fn lock<T>(mutex: &'static Mutex<T>) -> MutexGuard<'static, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::{
        shows_in_plain_log, shows_on_dashboard, suppress_dev_fee_related_in_tui, suppress_in_tui,
        Audience,
    };

    #[test]
    fn each_audience_reaches_only_its_outputs() {
        for verbose in [false, true] {
            assert!(shows_on_dashboard(Audience::Everyone, verbose));
            assert!(shows_in_plain_log(Audience::Everyone, verbose));
            assert!(shows_on_dashboard(Audience::DashboardOnly, verbose));
            assert!(!shows_in_plain_log(Audience::DashboardOnly, verbose));
            assert!(shows_in_plain_log(Audience::PlainLog, verbose));
            assert_eq!(shows_on_dashboard(Audience::PlainLog, verbose), verbose);
            assert_eq!(shows_on_dashboard(Audience::VerboseOnly, verbose), verbose);
            assert_eq!(shows_in_plain_log(Audience::VerboseOnly, verbose), verbose);
        }
    }

    #[test]
    fn logging_survives_closed_output_pipes() {
        use std::io::{BufRead, BufReader, Read, Write};
        use std::process::{Command, Stdio};

        const CHILD: &str = "SEINE_TEST_CLOSED_OUTPUT";
        if std::env::var_os(CHILD).is_some() {
            // The parent closes both readers before allowing logging to start.
            std::io::stdout().write_all(b"output-ready\n").unwrap();
            std::io::stdout().flush().unwrap();
            let mut ready = [0];
            std::io::stdin().read_exact(&mut ready).unwrap();
            super::startup_banner(&[("test", "closed output".into())]);
            super::info("TEST", "mining continues");
            super::success("TEST", "share accepted");
            super::warn("TEST", "warning survives");
            super::error("TEST", "error survives");
            super::mined("TEST", "block found");
            // Bypass the test harness, which itself prints to stdout.
            std::process::exit(0);
        }
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "miner::ui::tests::logging_survives_closed_output_pipes",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            assert_ne!(
                output.read_line(&mut line).unwrap(),
                0,
                "child exited before logging"
            );
            if line.trim() == "output-ready" {
                break;
            }
            line.clear();
        }
        drop(output);
        drop(child.stderr.take());
        child.stdin.take().unwrap().write_all(&[1]).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success(), "logging terminated the process: {status}");
                break;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("logging child did not exit");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    fn suppresses_telemetry_lines_in_tui() {
        assert!(suppress_in_tui(
            "BACKEND",
            "telemetry | cpu#1:active_peak=1"
        ));
        assert!(suppress_in_tui("BENCH", "telemetry | cpu#1:active_peak=1"));
        assert!(!suppress_in_tui("BACKEND", "quarantined cpu#1"));
        assert!(!suppress_in_tui("MINER", "telemetry | cpu#1:active_peak=1"));
        assert!(suppress_in_tui(
            "MINER",
            "template-history | max=16 retention=81.25s"
        ));
        assert!(suppress_in_tui(
            "DAEMON",
            "daemon did not return blocktemplate; it may still be syncing or unresponsive, retrying"
        ));
        assert!(suppress_in_tui(
            "DAEMON",
            "daemon still not returning blocktemplate (possibly syncing or unresponsive), retrying"
        ));
        assert!(suppress_in_tui("NETWORK", "blocktemplate fetch recovered"));
        assert!(!suppress_in_tui("MINER", "connected and mining"));
    }

    #[test]
    fn suppresses_dev_fee_related_lines_in_tui() {
        assert!(suppress_dev_fee_related_in_tui(
            "DEV FEE",
            "mining for dev",
            false
        ));
        assert!(suppress_dev_fee_related_in_tui(
            "CONN",
            "switching pool session: user -> dev",
            false
        ));
        assert!(suppress_dev_fee_related_in_tui(
            "AUTH",
            "dev session login accepted",
            false
        ));
        assert!(suppress_dev_fee_related_in_tui(
            "SHARE",
            "accepted (dev)",
            false
        ));
        assert!(suppress_dev_fee_related_in_tui(
            "JOB",
            "new job height=3133 difficulty=60",
            true
        ));
        assert!(!suppress_dev_fee_related_in_tui(
            "JOB",
            "new job height=3134 difficulty=59",
            false
        ));
        assert!(!suppress_dev_fee_related_in_tui(
            "MINER",
            "connected and mining",
            true
        ));
    }
}
