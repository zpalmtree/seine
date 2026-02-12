use std::io::{IsTerminal, Write};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Instant;

const FRAME_INNER_WIDTH: usize = 92;
const KEY_WIDTH: usize = 18;
const MAX_LOG_LINE_WIDTH: usize = 104;
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
static STATUS_LINE: OnceLock<Mutex<Option<String>>> = OnceLock::new();

#[derive(Clone, Copy)]
enum Level {
    Info,
    Success,
    Warn,
    Error,
}

impl Level {
    fn label(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Success => "OK",
            Self::Warn => "WARN",
            Self::Error => "ERR",
        }
    }

    fn level_style(self) -> &'static str {
        match self {
            Self::Info => "48;5;31;1;97",
            Self::Success => "48;5;28;1;97",
            Self::Warn => "48;5;214;1;30",
            Self::Error => "48;5;160;1;97",
        }
    }

    fn body_style(self) -> &'static str {
        match self {
            Self::Info => "38;5;153",
            Self::Success => "38;5;120",
            Self::Warn => "38;5;223",
            Self::Error => "38;5;217",
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

pub(super) fn info(tag: &str, message: impl AsRef<str>) {
    log(Level::Info, tag, message.as_ref());
}

pub(super) fn success(tag: &str, message: impl AsRef<str>) {
    log(Level::Success, tag, message.as_ref());
}

pub(super) fn warn(tag: &str, message: impl AsRef<str>) {
    log(Level::Warn, tag, message.as_ref());
}

pub(super) fn error(tag: &str, message: impl AsRef<str>) {
    log(Level::Error, tag, message.as_ref());
}

pub(super) fn status_line_enabled() -> bool {
    output_is_terminal() && std::env::var_os("SEINE_NO_STATUS").is_none()
}

pub(super) fn set_status_line(message: impl AsRef<str>) {
    if !status_line_enabled() {
        return;
    }

    let colors = use_color();
    let max_body = terminal_columns()
        .min(MAX_LOG_LINE_WIDTH)
        .saturating_sub(10);
    let clean = message.as_ref().replace(['\n', '\r'], " ");
    let body = constrain_line(&clean, max_body.max(16));
    let rendered = if colors {
        format!(
            "{} {}",
            paint(" STATUS ", "48;5;27;1;97", true),
            paint(&body, "1;96", true),
        )
    } else {
        format!("[STATUS] {body}")
    };

    let _out_guard = lock(output_lock());
    {
        let mut slot = lock(status_store());
        *slot = Some(rendered.clone());
    }
    draw_status_line(&rendered);
}

pub(super) fn clear_status_line() {
    if !status_line_enabled() {
        return;
    }
    let _out_guard = lock(output_lock());
    let mut slot = lock(status_store());
    if slot.take().is_some() {
        clear_terminal_line();
    }
}

fn log(level: Level, tag: &str, message: &str) {
    let colors = use_color();
    let time_plain = format!("{:>7.1}s", log_elapsed().as_secs_f64());
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
            .min(MAX_LOG_LINE_WIDTH)
            .saturating_sub(prefix_plain.chars().count() + 1)
            .max(16)
    } else {
        usize::MAX
    };
    let constrained = constrain_line(message, max_body);
    let body = style_message(&constrained, level.body_style(), colors);

    let _out_guard = lock(output_lock());
    let status_before = {
        let slot = lock(status_store());
        slot.clone()
    };
    if status_before.is_some() {
        clear_terminal_line();
    }

    if level.use_stderr() {
        eprintln!("{prefix} {body}");
    } else {
        println!("{prefix} {body}");
    }

    if let Some(status) = status_before {
        draw_status_line(&status);
    }
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
    std::env::var("COLUMNS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
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

fn status_store() -> &'static Mutex<Option<String>> {
    STATUS_LINE.get_or_init(|| Mutex::new(None))
}

fn lock<T>(mutex: &'static Mutex<T>) -> MutexGuard<'static, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn clear_terminal_line() {
    print!("\r\x1b[2K");
    let _ = std::io::stdout().flush();
}

fn draw_status_line(rendered: &str) {
    print!("\r{rendered}\x1b[K");
    let _ = std::io::stdout().flush();
}
