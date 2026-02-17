use std::collections::VecDeque;
use std::io::{self, Stdout};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Terminal;

const LOG_CAPACITY: usize = 200;
const BLOCK_MARKER_CAPACITY: usize = 4096;

#[derive(Clone)]
pub struct LogEntry {
    pub elapsed_secs: f64,
    pub level: LogLevel,
    pub tag: String,
    pub message: String,
}

#[derive(Clone, Copy)]
pub enum LogLevel {
    Info,
    Success,
    Warn,
    Error,
    Mined,
}

impl LogLevel {
    pub fn label(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Success => " OK ",
            Self::Warn => "WARN",
            Self::Error => " ERR",
            Self::Mined => " ** ",
        }
    }

    fn color(self) -> Color {
        match self {
            Self::Info => Color::Rgb(140, 160, 180),
            Self::Success => Color::Rgb(120, 190, 120),
            Self::Warn => Color::Rgb(210, 180, 100),
            Self::Error => Color::Rgb(210, 110, 110),
            Self::Mined => Color::Rgb(255, 220, 100),
        }
    }

    fn label_bg(self) -> Color {
        match self {
            Self::Info => Color::Rgb(35, 55, 80),
            Self::Success => Color::Rgb(25, 70, 25),
            Self::Warn => Color::Rgb(100, 80, 20),
            Self::Error => Color::Rgb(100, 25, 25),
            Self::Mined => Color::Rgb(130, 100, 10),
        }
    }

    fn label_fg(self) -> Color {
        match self {
            Self::Info => Color::Rgb(160, 180, 210),
            Self::Success => Color::Rgb(140, 210, 140),
            Self::Warn => Color::Rgb(230, 200, 120),
            Self::Error => Color::Rgb(230, 140, 140),
            Self::Mined => Color::Rgb(255, 230, 120),
        }
    }
}

#[derive(Clone)]
pub struct DeviceHashrate {
    pub name: String,
    pub current: String,
    pub average: String,
}

pub struct TuiStateInner {
    // Network
    pub height: String,
    pub difficulty: String,
    pub epoch: u64,
    pub state: String,

    // Mining
    pub round_hashrate: String,
    pub avg_hashrate: String,
    pub total_hashes: u64,
    pub templates: u64,
    pub submitted: u64,
    pub accepted: u64,
    pub device_hashrates: Vec<DeviceHashrate>,
    pub pending_nvidia: u64,
    pub pending_nvidia_since: Option<Instant>,

    // Config (set once at startup)
    pub api_url: String,
    pub threads: usize,
    pub refresh_secs: u64,
    pub sse_enabled: bool,
    pub backends_desc: String,
    pub accounting: String,
    pub version: String,

    // Timing
    pub started_at: Instant,

    // Log
    pub log_entries: VecDeque<LogEntry>,

    // Wave markers for mined blocks (elapsed seconds when each block was accepted)
    pub block_found_ticks: VecDeque<u64>,
}

impl TuiStateInner {
    pub fn new() -> Self {
        Self {
            height: "---".to_string(),
            difficulty: "---".to_string(),
            epoch: 0,
            state: "initializing".to_string(),

            round_hashrate: "0.000 H/s".to_string(),
            avg_hashrate: "0.000 H/s".to_string(),
            total_hashes: 0,
            templates: 0,
            submitted: 0,
            accepted: 0,
            device_hashrates: Vec::new(),
            pending_nvidia: 0,
            pending_nvidia_since: None,

            api_url: String::new(),
            threads: 0,
            refresh_secs: 0,
            sse_enabled: false,
            backends_desc: String::new(),
            accounting: String::new(),
            version: String::new(),

            started_at: Instant::now(),

            log_entries: VecDeque::with_capacity(LOG_CAPACITY),

            block_found_ticks: VecDeque::with_capacity(BLOCK_MARKER_CAPACITY),
        }
    }

    pub fn push_log(&mut self, entry: LogEntry) {
        if self.log_entries.len() >= LOG_CAPACITY {
            self.log_entries.pop_front();
        }
        self.log_entries.push_back(entry);
    }

    pub fn push_block_found_tick(&mut self, tick: u64) {
        if self.block_found_ticks.len() >= BLOCK_MARKER_CAPACITY {
            self.block_found_ticks.pop_front();
        }
        self.block_found_ticks.push_back(tick);
    }

    fn uptime(&self) -> String {
        let secs = self.started_at.elapsed().as_secs();
        if secs < 60 {
            format!("{}s", secs)
        } else if secs < 3600 {
            format!("{}m {:02}s", secs / 60, secs % 60)
        } else {
            format!(
                "{}h {:02}m {:02}s",
                secs / 3600,
                (secs % 3600) / 60,
                secs % 60
            )
        }
    }
}

pub type TuiState = Arc<Mutex<TuiStateInner>>;

pub fn new_tui_state() -> TuiState {
    Arc::new(Mutex::new(TuiStateInner::new()))
}

pub struct TuiRenderer {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TuiRenderer {
    pub fn new() -> io::Result<Self> {
        install_panic_hook();
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        Ok(Self { terminal })
    }

    pub fn render(&mut self, state: &TuiStateInner) -> io::Result<()> {
        self.terminal.draw(|frame| {
            let area = frame.area();
            draw_dashboard(frame, area, state);
        })?;
        Ok(())
    }
}

impl Drop for TuiRenderer {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

fn install_panic_hook() {
    let original = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original(info);
    }));
}

// --- Layout & Drawing ---

const BORDER_STYLE: Style = Style::new().fg(Color::Rgb(60, 70, 90));
const TITLE_STYLE: Style = Style::new()
    .fg(Color::Rgb(140, 170, 200))
    .add_modifier(Modifier::BOLD);
const LABEL_STYLE: Style = Style::new().fg(Color::Rgb(100, 120, 150));
const VALUE_STYLE: Style = Style::new()
    .fg(Color::Rgb(220, 220, 220))
    .add_modifier(Modifier::BOLD);
const DIM_STYLE: Style = Style::new().fg(Color::Rgb(90, 90, 90));

fn draw_dashboard(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let wide = area.width >= 80;

    let stats_height: u16 = if wide { 6 } else { 14 };
    let active_device_count = state.device_hashrates.len() as u16;
    let pending_count = state.pending_nvidia as u16;
    let active_rows = if wide {
        (active_device_count + 1) / 2 // 2 per row in wide mode
    } else {
        active_device_count
    };
    // Pending devices are always 1-per-line (not paired in wide mode)
    let device_rows = active_rows + pending_count;
    let devices_height: u16 = if device_rows > 0 { 2 + device_rows } else { 0 };
    let config_height: u16 = 4;
    let header_height: u16 = 4;

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(header_height),
            Constraint::Length(stats_height),
            Constraint::Length(devices_height),
            Constraint::Length(config_height),
            Constraint::Min(4),
        ])
        .split(area);

    draw_header(frame, chunks[0], state);
    draw_stats(frame, chunks[1], state, wide);
    if device_rows > 0 {
        draw_devices(frame, chunks[2], state, wide);
    }
    draw_config(frame, chunks[3], state);
    draw_log(frame, chunks[4], state);
}

fn draw_header(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let uptime = state.uptime();
    let tick = state.started_at.elapsed().as_secs();

    let title = format!(" seine {} ", state.version);
    let uptime_text = format!(" uptime {uptime} ");

    let inner_width = area.width.saturating_sub(2) as usize;

    let mut lines = wave_lines(inner_width, tick, &state.block_found_ticks);

    // Overlay uptime on the last wave row
    if let Some(last) = lines.last_mut() {
        let uptime_len = uptime_text.chars().count();
        let keep = inner_width.saturating_sub(uptime_len);
        last.spans.truncate(keep);
        last.spans.push(Span::styled(uptime_text, DIM_STYLE));
    }

    let block = Block::default()
        .title(Span::styled(title, TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);
}

fn wave_lines(width: usize, tick: u64, block_ticks: &VecDeque<u64>) -> Vec<Line<'static>> {
    const CREST_COLOR: Color = Color::Rgb(65, 125, 175);
    const WAVE_COLOR: Color = Color::Rgb(45, 85, 130);
    const BLOCK_COLOR: Color = Color::Rgb(230, 190, 60);

    // Two dense layers (coprime lengths so they drift in/out of phase)
    let upper: Vec<char> = "≈ ~ ≈ ~ ≈ ~ ~ ≈ ~ ≈ ^ ≈ ~ ≈ ~ ~ ≈ ".chars().collect();
    let lower: Vec<char> = "~ ≈ ~ ≈ ~ ~ ≈ ~ ~ ≈ ~ ^ ≈ ~ ≈ ~ ≈ ~ ".chars().collect();

    // Block indicators scroll left from right edge on the crest row.
    // Snap marker columns to visible crest glyphs so markers replace waves
    // instead of appearing in the inter-glyph spacing.
    let mut block_mask = vec![false; width];
    let crest_offset = tick as usize % upper.len();
    for &block_tick in block_ticks {
        let age = tick.saturating_sub(block_tick) as usize;
        if age < width {
            let raw_col = width - 1 - age;
            let snapped_col = snap_wave_column_to_glyph(raw_col, width, &upper, crest_offset);
            block_mask[snapped_col] = true;
        }
    }

    vec![
        wave_row(
            width,
            tick as usize,
            &upper,
            CREST_COLOR,
            Some(&block_mask),
            BLOCK_COLOR,
        ),
        wave_row(width, tick as usize, &lower, WAVE_COLOR, None, BLOCK_COLOR),
    ]
}

fn snap_wave_column_to_glyph(
    target: usize,
    width: usize,
    pattern: &[char],
    offset: usize,
) -> usize {
    if width == 0 || pattern.is_empty() {
        return 0;
    }
    let bounded_target = target.min(width - 1);
    let is_glyph_col = |idx: usize| pattern[(offset + idx) % pattern.len()] != ' ';

    if is_glyph_col(bounded_target) {
        return bounded_target;
    }

    for delta in 1..width {
        if let Some(left) = bounded_target.checked_sub(delta) {
            if is_glyph_col(left) {
                return left;
            }
        }
        if let Some(right) = bounded_target.checked_add(delta) {
            if right < width && is_glyph_col(right) {
                return right;
            }
        }
    }

    bounded_target
}

fn wave_row(
    width: usize,
    offset_base: usize,
    pattern: &[char],
    color: Color,
    block_mask: Option<&[bool]>,
    block_color: Color,
) -> Line<'static> {
    let plen = pattern.len();
    let offset = offset_base % plen;
    let mut spans = Vec::with_capacity(width);

    for i in 0..width {
        if block_mask
            .and_then(|mask| mask.get(i))
            .copied()
            .unwrap_or(false)
        {
            spans.push(Span::styled(
                "◆",
                Style::default()
                    .fg(block_color)
                    .add_modifier(Modifier::BOLD),
            ));
        } else {
            let ch = pattern[(offset + i) % plen];
            if ch == ' ' {
                spans.push(Span::raw(" "));
            } else {
                spans.push(Span::styled(ch.to_string(), Style::default().fg(color)));
            }
        }
    }

    Line::from(spans)
}

fn draw_stats(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner, wide: bool) {
    if wide {
        draw_stats_wide(frame, area, state);
    } else {
        draw_stats_narrow(frame, area, state);
    }
}

fn draw_stats_wide(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(34),
            Constraint::Percentage(33),
        ])
        .split(area);

    // Hashrate panel
    let total_str = format_u64(state.total_hashes);
    let hashrate_lines = vec![
        kv_line("Current", &state.round_hashrate),
        kv_line("Average", &state.avg_hashrate),
        kv_line("Total", &total_str),
    ];
    let hashrate_block = Block::default()
        .title(Span::styled(" HASHRATE ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    let hashrate = Paragraph::new(hashrate_lines).block(hashrate_block);
    frame.render_widget(hashrate, cols[0]);

    // Network panel
    let state_color = match state.state.as_str() {
        "working" => Color::Rgb(100, 170, 100),
        "stale-tip" | "stale-refresh" => Color::Rgb(200, 170, 80),
        "solved" => Color::Rgb(120, 190, 120),
        _ => Color::Rgb(180, 180, 180),
    };
    let epoch_str = state.epoch.to_string();
    let network_lines = vec![
        kv_line("Height", &state.height),
        kv_line("Difficulty", &state.difficulty),
        kv_line("Epoch", &epoch_str),
        Line::from(vec![
            Span::styled("  State      ".to_string(), LABEL_STYLE),
            Span::styled(
                state.state.clone(),
                Style::default()
                    .fg(state_color)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
    ];
    let network_block = Block::default()
        .title(Span::styled(" NETWORK ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    let network = Paragraph::new(network_lines).block(network_block);
    frame.render_widget(network, cols[1]);

    // Mining panel
    let submitted_str = format_u64(state.submitted);
    let accepted_str = format_u64(state.accepted);
    let templates_str = format_u64(state.templates);
    let mining_lines = vec![
        kv_line("Submitted", &submitted_str),
        kv_line("Accepted", &accepted_str),
        kv_line("Templates", &templates_str),
    ];
    let mining_block = Block::default()
        .title(Span::styled(" MINING ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    let mining = Paragraph::new(mining_lines).block(mining_block);
    frame.render_widget(mining, cols[2]);
}

fn draw_stats_narrow(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(4),
        ])
        .split(area);

    // Hashrate
    let total_str = format_u64(state.total_hashes);
    let hashrate_lines = vec![
        kv_line("Current", &state.round_hashrate),
        kv_line("Average", &state.avg_hashrate),
        kv_line("Total", &total_str),
    ];
    let hashrate_block = Block::default()
        .title(Span::styled(" HASHRATE ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    frame.render_widget(
        Paragraph::new(hashrate_lines).block(hashrate_block),
        rows[0],
    );

    // Network
    let epoch_str = state.epoch.to_string();
    let network_lines = vec![
        kv_line("Height", &state.height),
        kv_line("Difficulty", &state.difficulty),
        kv_line("Epoch", &epoch_str),
    ];
    let network_block = Block::default()
        .title(Span::styled(" NETWORK ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    frame.render_widget(Paragraph::new(network_lines).block(network_block), rows[1]);

    // Mining
    let submitted_str = format_u64(state.submitted);
    let accepted_str = format_u64(state.accepted);
    let mining_lines = vec![
        kv_line("Submitted", &submitted_str),
        kv_line("Accepted", &accepted_str),
    ];
    let mining_block = Block::default()
        .title(Span::styled(" MINING ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    frame.render_widget(Paragraph::new(mining_lines).block(mining_block), rows[2]);
}

fn draw_devices(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner, wide: bool) {
    let mut lines: Vec<Line<'static>> = if wide {
        // Wide: lay out devices side by side, 2 per row
        state
            .device_hashrates
            .chunks(2)
            .map(|pair| {
                let mut spans = Vec::new();
                for (i, dev) in pair.iter().enumerate() {
                    if i > 0 {
                        spans.push(Span::styled("    ", Style::default()));
                    }
                    spans.push(Span::styled("  ", Style::default()));
                    spans.push(Span::styled(
                        "▸ ",
                        Style::default().fg(Color::Rgb(70, 100, 130)),
                    ));
                    spans.push(Span::styled(format!("{:<10}", dev.name), DEVICE_NAME_STYLE));
                    spans.push(Span::styled(format!("{:<14}", dev.current), VALUE_STYLE));
                    spans.push(Span::styled("avg ", DEVICE_AVG_STYLE));
                    spans.push(Span::styled(dev.average.clone(), DEVICE_AVG_STYLE));
                }
                Line::from(spans)
            })
            .collect()
    } else {
        // Narrow: one device per line
        state
            .device_hashrates
            .iter()
            .map(|dev| device_line(&dev.name, &dev.current, &dev.average))
            .collect()
    };

    // Show pending NVIDIA backends that are still initializing.
    if state.pending_nvidia > 0 {
        let online_nvidia = state
            .device_hashrates
            .iter()
            .filter(|d| d.name.starts_with("nvidia#"))
            .count() as u64;
        let pending_elapsed = state
            .pending_nvidia_since
            .map(|started| started.elapsed())
            .unwrap_or_default();
        let pending_status = pending_nvidia_status_label(pending_elapsed);
        let pending_elapsed_label = format_duration_compact(pending_elapsed);
        let pending_message = format!("{pending_status} ({pending_elapsed_label})");
        for i in 0..state.pending_nvidia {
            let name = format!("nvidia#{}", online_nvidia + i + 1);
            lines.push(Line::from(vec![
                Span::styled("  ", Style::default()),
                Span::styled("▸ ", Style::default().fg(Color::Rgb(70, 100, 130))),
                Span::styled(format!("{:<10}", name), DEVICE_NAME_STYLE),
                Span::styled(pending_message.clone(), DIM_STYLE),
            ]));
        }
    }

    let block = Block::default()
        .title(Span::styled(" DEVICES ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

fn draw_config(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let sse_str = if state.sse_enabled { "on" } else { "off" };
    let line1 = Line::from(vec![
        Span::styled("  API: ", LABEL_STYLE),
        Span::styled(&state.api_url, VALUE_STYLE),
        Span::styled("  Threads: ", LABEL_STYLE),
        Span::styled(state.threads.to_string(), VALUE_STYLE),
        Span::styled("  Refresh: ", LABEL_STYLE),
        Span::styled(format!("{}s", state.refresh_secs), VALUE_STYLE),
        Span::styled("  SSE: ", LABEL_STYLE),
        Span::styled(sse_str, VALUE_STYLE),
    ]);
    let line2 = Line::from(vec![
        Span::styled("  Backends: ", LABEL_STYLE),
        Span::styled(&state.backends_desc, VALUE_STYLE),
        Span::styled("  Accounting: ", LABEL_STYLE),
        Span::styled(&state.accounting, VALUE_STYLE),
    ]);
    let config_block = Block::default()
        .title(Span::styled(" CONFIG ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    let config = Paragraph::new(vec![line1, line2]).block(config_block);
    frame.render_widget(config, area);
}

fn draw_log(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let log_block = Block::default()
        .title(Span::styled(" LOG ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);

    let inner_height = area.height.saturating_sub(2) as usize;
    let inner_width = area.width.saturating_sub(2) as usize;
    // Fixed prefix: "HHHHHHs LLLL TTTTTTTT " = 8 + 6 + 10 + 1 = 25 chars
    let prefix_width = 25;
    let msg_width = inner_width.saturating_sub(prefix_width);
    let start = state.log_entries.len().saturating_sub(inner_height);
    let visible: Vec<Line> = state
        .log_entries
        .iter()
        .skip(start)
        .map(|entry| {
            let time = Span::styled(format!("{:>6.0}s ", entry.elapsed_secs), DIM_STYLE);
            let level = Span::styled(
                format!(" {} ", entry.level.label()),
                Style::default()
                    .fg(entry.level.label_fg())
                    .bg(entry.level.label_bg())
                    .add_modifier(Modifier::BOLD),
            );
            let tag = Span::styled(
                format!(" {:<8} ", entry.tag),
                Style::default()
                    .fg(Color::Rgb(200, 200, 200))
                    .bg(Color::Rgb(40, 40, 40))
                    .add_modifier(Modifier::BOLD),
            );
            let body = truncate_str(&entry.message, msg_width);
            let msg_style = if matches!(entry.level, LogLevel::Mined) {
                Style::default()
                    .fg(entry.level.color())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(entry.level.color())
            };
            let msg = Span::styled(format!(" {body}"), msg_style);
            Line::from(vec![time, level, tag, msg])
        })
        .collect();

    let log = Paragraph::new(visible).block(log_block);
    frame.render_widget(log, area);
}

fn kv_line(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("  {:<10} ", label), LABEL_STYLE),
        Span::styled(value.to_string(), VALUE_STYLE),
    ])
}

const DEVICE_NAME_STYLE: Style = Style::new().fg(Color::Rgb(100, 140, 160));
const DEVICE_AVG_STYLE: Style = Style::new().fg(Color::Rgb(130, 140, 155));

fn device_line(name: &str, current: &str, average: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled("   ", Style::default()),
        Span::styled("▸ ", Style::default().fg(Color::Rgb(70, 100, 130))),
        Span::styled(format!("{:<10}", name), DEVICE_NAME_STYLE),
        Span::styled(current.to_string(), VALUE_STYLE),
        Span::styled("  avg ", DEVICE_AVG_STYLE),
        Span::styled(average.to_string(), DEVICE_AVG_STYLE),
    ])
}

fn truncate_str(s: &str, max: usize) -> String {
    if max <= 3 || s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max - 1).collect();
    out.push('…');
    out
}

fn pending_nvidia_status_label(elapsed: Duration) -> &'static str {
    if elapsed < Duration::from_secs(20) {
        "starting CUDA runtime"
    } else if elapsed < Duration::from_secs(90) {
        "preparing NVIDIA backend"
    } else {
        "still initializing (first run may take a few minutes)"
    }
}

fn format_duration_compact(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else {
        let mins = secs / 60;
        let rem = secs % 60;
        format!("{mins}m {rem:02}s")
    }
}

fn format_u64(n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_u64_adds_commas() {
        assert_eq!(format_u64(0), "0");
        assert_eq!(format_u64(999), "999");
        assert_eq!(format_u64(1_000), "1,000");
        assert_eq!(format_u64(1_234_567), "1,234,567");
    }

    #[test]
    fn log_ring_buffer_capacity() {
        let mut state = TuiStateInner::new();
        for i in 0..250 {
            state.push_log(LogEntry {
                elapsed_secs: i as f64,
                level: LogLevel::Info,
                tag: "TEST".to_string(),
                message: format!("msg {}", i),
            });
        }
        assert_eq!(state.log_entries.len(), LOG_CAPACITY);
        assert_eq!(state.log_entries.front().unwrap().message, "msg 50");
    }

    #[test]
    fn block_marker_history_is_capped() {
        let mut state = TuiStateInner::new();
        for i in 0..(BLOCK_MARKER_CAPACITY + 32) {
            state.push_block_found_tick(i as u64);
        }
        assert_eq!(state.block_found_ticks.len(), BLOCK_MARKER_CAPACITY);
        assert_eq!(state.block_found_ticks.front().copied(), Some(32));
    }

    #[test]
    fn uptime_formatting() {
        let state = TuiStateInner::new();
        // Just verify it doesn't panic - actual value depends on timing
        let _uptime = state.uptime();
    }

    #[test]
    fn block_markers_snap_to_crest_glyphs() {
        let mut block_ticks = VecDeque::new();
        let tick = 120u64;
        let width = 80usize;
        for age in 0..24u64 {
            block_ticks.push_back(tick - age);
        }

        let lines = wave_lines(width, tick, &block_ticks);
        let crest = lines.first().expect("crest row should exist");
        let pattern: Vec<char> = "≈ ~ ≈ ~ ≈ ~ ~ ≈ ~ ≈ ^ ≈ ~ ≈ ~ ~ ≈ ".chars().collect();
        let offset = tick as usize % pattern.len();

        for (idx, span) in crest.spans.iter().enumerate() {
            if span.content.as_ref() == "◆" {
                let ch = pattern[(offset + idx) % pattern.len()];
                assert_ne!(ch, ' ', "marker landed in a spacing column at {idx}");
            }
        }
    }
}
