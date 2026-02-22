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
    pub network_hashrate: String,
    pub epoch: u64,
    pub state: String,
    pub blocktemplate_retry_since: Option<Instant>,

    // Mining
    pub round_hashrate: String,
    pub avg_hashrate: String,
    pub total_hashes: u64,
    pub templates: u64,
    pub submitted: u64,
    pub stale_shares: u64,
    pub accepted: u64,
    pub wallet_address: String,
    pub wallet_pending: String,
    pub wallet_unlocked: String,
    pub device_hashrates: Vec<DeviceHashrate>,
    pub pending_nvidia: u64,
    pub pending_nvidia_since: Option<Instant>,
    pub dev_fee_active: bool,

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
            network_hashrate: "---".to_string(),
            epoch: 0,
            state: "initializing".to_string(),
            blocktemplate_retry_since: None,

            round_hashrate: "0.000 H/s".to_string(),
            avg_hashrate: "0.000 H/s".to_string(),
            total_hashes: 0,
            templates: 0,
            submitted: 0,
            stale_shares: 0,
            accepted: 0,
            wallet_address: "---".to_string(),
            wallet_pending: "---".to_string(),
            wallet_unlocked: "---".to_string(),
            device_hashrates: Vec::new(),
            pending_nvidia: 0,
            pending_nvidia_since: None,
            dev_fee_active: false,

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
// Keep block marker glyph single-cell across terminals.
const BLOCK_MARKER_GLYPH: &str = "♦";

fn draw_dashboard(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let wide = area.width >= 80;

    let stats_height: u16 = if wide { 5 } else { 14 };
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
    let config_height: u16 = if wide { 4 } else { 8 };
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
                BLOCK_MARKER_GLYPH,
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

fn format_state_display(state: &TuiStateInner) -> (String, Color) {
    let mut state_display = if state.dev_fee_active && state.state == "working" {
        "working (dev fee)".to_string()
    } else {
        state.state.clone()
    };

    let mut state_color = match state.state.as_str() {
        "working" => Color::Rgb(100, 170, 100),
        "stale-tip" | "stale-refresh" => Color::Rgb(200, 170, 80),
        "solved" => Color::Rgb(120, 190, 120),
        "wallet-required" | "wallet-password-required" => Color::Rgb(220, 170, 90),
        "daemon-syncing" => Color::Rgb(215, 165, 95),
        "daemon-unavailable" => Color::Rgb(210, 120, 100),
        "invalid-address" => Color::Rgb(220, 105, 95),
        _ => Color::Rgb(180, 180, 180),
    };

    if state.dev_fee_active && state.state == "working" {
        state_color = Color::Rgb(170, 140, 200);
    }

    if let Some(retry_since) = state.blocktemplate_retry_since {
        let retry_for = format_duration_compact(retry_since.elapsed());
        state_display = if state.state == "daemon-unavailable" {
            format!("daemon-unavailable ({retry_for})")
        } else {
            format!("{state_display} | tpl retry {retry_for}")
        };
        state_color = Color::Rgb(215, 165, 95);
    }

    (state_display, state_color)
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
    let (state_display, state_color) = format_state_display(state);
    let network_label_width = "Global".chars().count();
    let network_lines = vec![
        kv_line_with_label_width("Height", &state.height, network_label_width),
        kv_line_with_label_width("Global", &state.network_hashrate, network_label_width),
        Line::from(vec![
            Span::styled(format!("  {:<network_label_width$} ", "State"), LABEL_STYLE),
            Span::styled(
                state_display,
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
    let stale_shares_str = format_u64(state.stale_shares);
    let accepted_str = format_u64(state.accepted);
    let mining_lines = vec![
        kv_line("Submitted", &submitted_str),
        kv_line("Stale", &stale_shares_str),
        kv_line("Accepted", &accepted_str),
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
            Constraint::Length(4),
            Constraint::Length(5),
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
    let network_label_width = "Global".chars().count();
    let network_lines = vec![
        kv_line_with_label_width("Height", &state.height, network_label_width),
        kv_line_with_label_width("Global", &state.network_hashrate, network_label_width),
    ];
    let network_block = Block::default()
        .title(Span::styled(" NETWORK ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    frame.render_widget(Paragraph::new(network_lines).block(network_block), rows[1]);

    // Mining
    let submitted_str = format_u64(state.submitted);
    let stale_shares_str = format_u64(state.stale_shares);
    let accepted_str = format_u64(state.accepted);
    let mining_lines = vec![
        kv_line("Submitted", &submitted_str),
        kv_line("Stale", &stale_shares_str),
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
    if area.width >= 90 {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area);
        draw_config_runtime_panel(frame, cols[0], state);
        draw_config_wallet_panel(frame, cols[1], state);
        return;
    }

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Min(4)])
        .split(area);
    draw_config_runtime_panel(frame, rows[0], state);
    draw_config_wallet_panel(frame, rows[1], state);
}

fn draw_config_runtime_panel(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let line1 = Line::from(vec![
        Span::styled("  API: ", LABEL_STYLE),
        Span::styled(&state.api_url, VALUE_STYLE),
        Span::styled("  Threads: ", LABEL_STYLE),
        Span::styled(state.threads.to_string(), VALUE_STYLE),
    ]);
    let line2 = Line::from(vec![
        Span::styled("  Backends: ", LABEL_STYLE),
        Span::styled(&state.backends_desc, VALUE_STYLE),
    ]);
    let config_block = Block::default()
        .title(Span::styled(" CONFIG ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    let config = Paragraph::new(vec![line1, line2]).block(config_block);
    frame.render_widget(config, area);
}

fn draw_config_wallet_panel(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let address_str = compact_wallet_address(&state.wallet_address);
    let inner_width = area.width.saturating_sub(2) as usize;
    let wallet_lines = vec![
        wallet_line_with_right(
            "Pending",
            &state.wallet_pending,
            "Address",
            &address_str,
            inner_width,
        ),
        kv_line("Unlocked", &state.wallet_unlocked),
    ];
    let wallet_block = Block::default()
        .title(Span::styled(" WALLET ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    let wallet = Paragraph::new(wallet_lines).block(wallet_block);
    frame.render_widget(wallet, area);
}

fn draw_log(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let log_block = Block::default()
        .title(Span::styled(" LOG ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);

    let inner_height = area.height.saturating_sub(2) as usize;
    let inner_width = area.width.saturating_sub(2) as usize;
    let now_elapsed_secs = state.started_at.elapsed().as_secs_f64();
    // Build wrapped lines from the end so recent entries are always visible.
    let mut all_lines: Vec<Line> = Vec::new();
    let elapsed_labels: Vec<String> = state
        .log_entries
        .iter()
        .map(|entry| {
            format_elapsed_log_ago(log_entry_age_secs(now_elapsed_secs, entry.elapsed_secs))
        })
        .collect();
    let row_indent = "  ";
    let time_col_width = elapsed_labels
        .iter()
        .map(|label| label.chars().count())
        .max()
        .unwrap_or(8)
        .max(8);
    for (entry, elapsed) in state.log_entries.iter().zip(elapsed_labels.iter()) {
        let time_text = format!(" {elapsed:>time_col_width$} ");
        let time = Span::styled(time_text.clone(), DIM_STYLE);
        let level_text = format!(" {} ", entry.level.label());
        let level = Span::styled(
            level_text.clone(),
            Style::default()
                .fg(entry.level.label_fg())
                .bg(entry.level.label_bg())
                .add_modifier(Modifier::BOLD),
        );
        let tag_text = format!(" {:<8} ", entry.tag);
        let tag_width = tag_text.chars().count();
        let tag = Span::styled(
            tag_text,
            Style::default()
                .fg(Color::Rgb(200, 200, 200))
                .bg(Color::Rgb(40, 40, 40))
                .add_modifier(Modifier::BOLD),
        );
        let message_separator = " ";
        let prefix_width = row_indent.chars().count()
            + level_text.chars().count()
            + tag_width
            + message_separator.chars().count();
        let suffix_width = time_text.chars().count();
        let msg_width = inner_width
            .saturating_sub(prefix_width + suffix_width)
            .max(1);
        let msg_style = if matches!(entry.level, LogLevel::Mined) {
            Style::default()
                .fg(entry.level.color())
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(entry.level.color())
        };
        let chunks = wrap_str(&entry.message, msg_width);
        for (i, chunk) in chunks.iter().enumerate() {
            if i == 0 {
                let msg = Span::styled(chunk.clone(), msg_style);
                let gap =
                    inner_width.saturating_sub(prefix_width + chunk.chars().count() + suffix_width);
                all_lines.push(Line::from(vec![
                    Span::raw(row_indent),
                    level.clone(),
                    tag.clone(),
                    Span::raw(message_separator),
                    msg,
                    Span::raw(" ".repeat(gap)),
                    time.clone(),
                ]));
            } else {
                let pad = " ".repeat(prefix_width);
                let msg = Span::styled(format!("{pad}{chunk}"), msg_style);
                all_lines.push(Line::from(vec![msg]));
            }
        }
    }
    let start = all_lines.len().saturating_sub(inner_height);
    let visible: Vec<Line> = all_lines.into_iter().skip(start).collect();

    let log = Paragraph::new(visible).block(log_block);
    frame.render_widget(log, area);
}

fn log_entry_age_secs(now_elapsed_secs: f64, entry_elapsed_secs: f64) -> f64 {
    let now = now_elapsed_secs.max(0.0);
    let entry = entry_elapsed_secs.max(0.0);
    if now >= entry {
        now - entry
    } else {
        0.0
    }
}

fn format_elapsed_log_ago(elapsed_secs: f64) -> String {
    let total = elapsed_secs.max(0.0).floor() as u64;
    if total < 60 {
        return if total == 1 {
            "1 sec ago".to_string()
        } else {
            format!("{total} sec ago")
        };
    }

    let minutes = total / 60;
    if minutes < 60 {
        return if minutes == 1 {
            "1 min ago".to_string()
        } else {
            format!("{minutes} mins ago")
        };
    }

    let hours = minutes / 60;
    if hours < 24 {
        return if hours == 1 {
            "1 hour ago".to_string()
        } else {
            format!("{hours} hours ago")
        };
    }

    let days = hours / 24;
    if days < 7 {
        return if days == 1 {
            "1 day ago".to_string()
        } else {
            format!("{days} days ago")
        };
    }

    let weeks = days / 7;
    if weeks == 1 {
        "1 week ago".to_string()
    } else {
        format!("{weeks} weeks ago")
    }
}

fn kv_line(label: &str, value: &str) -> Line<'static> {
    kv_line_with_label_width(label, value, 10)
}

fn kv_line_with_label_width(label: &str, value: &str, label_width: usize) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("  {:<label_width$} ", label), LABEL_STYLE),
        Span::styled(value.to_string(), VALUE_STYLE),
    ])
}

fn wallet_line_with_right(
    left_label: &str,
    left_value: &str,
    right_label: &str,
    right_value: &str,
    inner_width: usize,
) -> Line<'static> {
    const RIGHT_MARGIN: usize = 1;
    let left_label_text = format!("  {:<10} ", left_label);
    let left_value_text = left_value.to_string();
    let right_label_text = format!("  {right_label} ");
    let right_value_text = right_value.to_string();
    let usable_width = inner_width.saturating_sub(RIGHT_MARGIN);
    let left_width = left_label_text.chars().count() + left_value_text.chars().count();
    let right_width = right_label_text.chars().count() + right_value_text.chars().count();
    let gap = usable_width
        .saturating_sub(left_width + right_width)
        .max(2usize);

    Line::from(vec![
        Span::styled(left_label_text, LABEL_STYLE),
        Span::styled(left_value_text, VALUE_STYLE),
        Span::raw(" ".repeat(gap)),
        Span::styled(right_label_text, LABEL_STYLE),
        Span::styled(right_value_text, VALUE_STYLE),
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

fn wrap_str(s: &str, max: usize) -> Vec<String> {
    if max == 0 {
        return vec![s.to_string()];
    }
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        return vec![s.to_string()];
    }
    let mut lines = Vec::new();
    let mut pos = 0;
    while pos < chars.len() {
        let end = (pos + max).min(chars.len());
        lines.push(chars[pos..end].iter().collect());
        pos = end;
    }
    lines
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

fn compact_wallet_address(address: &str) -> String {
    const KEEP: usize = 6;
    let chars: Vec<char> = address.chars().collect();
    if chars.len() <= KEEP * 2 {
        return address.to_string();
    }

    let head: String = chars.iter().take(KEEP).collect();
    let tail: String = chars[chars.len() - KEEP..].iter().collect();
    format!("{head}...{tail}")
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
    fn log_elapsed_formatter_uses_relative_ago_style() {
        assert_eq!(format_elapsed_log_ago(0.0), "0 sec ago");
        assert_eq!(format_elapsed_log_ago(65.0), "1 min ago");
        assert_eq!(format_elapsed_log_ago(3_600.0), "1 hour ago");
        assert_eq!(format_elapsed_log_ago(32_857.9), "9 hours ago");
        assert_eq!(format_elapsed_log_ago(360_000.0), "4 days ago");
    }

    #[test]
    fn log_entry_age_is_computed_from_current_elapsed_time() {
        assert_eq!(log_entry_age_secs(100.0, 95.0), 5.0);
        assert_eq!(log_entry_age_secs(95.0, 100.0), 0.0);
        assert_eq!(log_entry_age_secs(-1.0, 5.0), 0.0);
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
            if span.content.as_ref() == BLOCK_MARKER_GLYPH {
                let ch = pattern[(offset + idx) % pattern.len()];
                assert_ne!(ch, ' ', "marker landed in a spacing column at {idx}");
            }
        }
    }

    #[test]
    fn state_display_shows_blocktemplate_retry_status() {
        let mut state = TuiStateInner::new();
        state.state = "working".to_string();
        state.blocktemplate_retry_since = Instant::now().checked_sub(Duration::from_secs(12));

        let (display, color) = format_state_display(&state);

        assert!(display.contains("tpl retry"));
        assert_eq!(color, Color::Rgb(215, 165, 95));
    }
}
