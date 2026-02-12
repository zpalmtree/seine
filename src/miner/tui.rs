use std::collections::VecDeque;
use std::io::{self, Stdout};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Terminal;

const LOG_CAPACITY: usize = 200;

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
}

impl LogLevel {
    pub fn label(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Success => " OK ",
            Self::Warn => "WARN",
            Self::Error => " ERR",
        }
    }

    fn color(self) -> Color {
        match self {
            Self::Info => Color::Rgb(140, 160, 180),
            Self::Success => Color::Rgb(120, 190, 120),
            Self::Warn => Color::Rgb(210, 180, 100),
            Self::Error => Color::Rgb(210, 110, 110),
        }
    }

    fn label_bg(self) -> Color {
        match self {
            Self::Info => Color::Rgb(35, 55, 80),
            Self::Success => Color::Rgb(25, 70, 25),
            Self::Warn => Color::Rgb(100, 80, 20),
            Self::Error => Color::Rgb(100, 25, 25),
        }
    }

    fn label_fg(self) -> Color {
        match self {
            Self::Info => Color::Rgb(160, 180, 210),
            Self::Success => Color::Rgb(140, 210, 140),
            Self::Warn => Color::Rgb(230, 200, 120),
            Self::Error => Color::Rgb(230, 140, 140),
        }
    }
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
    pub backend_rates: String,

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
            backend_rates: String::new(),

            api_url: String::new(),
            threads: 0,
            refresh_secs: 0,
            sse_enabled: false,
            backends_desc: String::new(),
            accounting: String::new(),
            version: String::new(),

            started_at: Instant::now(),

            log_entries: VecDeque::with_capacity(LOG_CAPACITY),
        }
    }

    pub fn push_log(&mut self, entry: LogEntry) {
        if self.log_entries.len() >= LOG_CAPACITY {
            self.log_entries.pop_front();
        }
        self.log_entries.push_back(entry);
    }

    fn uptime(&self) -> String {
        let secs = self.started_at.elapsed().as_secs();
        if secs < 60 {
            format!("{}s", secs)
        } else if secs < 3600 {
            format!("{}m {:02}s", secs / 60, secs % 60)
        } else {
            format!("{}h {:02}m {:02}s", secs / 3600, (secs % 3600) / 60, secs % 60)
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

    pub fn poll_quit(&self) -> bool {
        if event::poll(std::time::Duration::ZERO).unwrap_or(false) {
            if let Ok(Event::Key(key)) = event::read() {
                if key.code == KeyCode::Char('c')
                    && key.modifiers.contains(KeyModifiers::CONTROL)
                {
                    return true;
                }
                if key.code == KeyCode::Char('q') {
                    return true;
                }
            }
        }
        false
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

    // Compute row heights
    let stats_height = if wide { 6 } else { 14 };
    let config_height = 4;
    let header_height = 3;

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(header_height),
            Constraint::Length(stats_height),
            Constraint::Length(config_height),
            Constraint::Min(4),
        ])
        .split(area);

    draw_header(frame, chunks[0], state);
    draw_stats(frame, chunks[1], state, wide);
    draw_config(frame, chunks[2], state);
    draw_log(frame, chunks[3], state);
}

fn draw_header(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let uptime = state.uptime();

    let title = format!(" seine {} ", state.version);
    let uptime_text = format!("uptime {uptime} ");

    let inner_width = area.width.saturating_sub(2) as usize;
    let padding = inner_width.saturating_sub(uptime_text.len());

    let content = Line::from(vec![
        Span::raw(" ".repeat(padding.max(1))),
        Span::styled(uptime_text, DIM_STYLE),
    ]);

    let block = Block::default()
        .title(Span::styled(title, TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);

    let paragraph = Paragraph::new(content).block(block);
    frame.render_widget(paragraph, area);
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
                Style::default().fg(state_color).add_modifier(Modifier::BOLD),
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
    frame.render_widget(Paragraph::new(hashrate_lines).block(hashrate_block), rows[0]);

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
    let start = state.log_entries.len().saturating_sub(inner_height);
    let visible: Vec<Line> = state
        .log_entries
        .iter()
        .skip(start)
        .map(|entry| {
            let time = Span::styled(
                format!("{:>7.1}s ", entry.elapsed_secs),
                DIM_STYLE,
            );
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
            let msg = Span::styled(
                format!(" {}", entry.message),
                Style::default().fg(entry.level.color()),
            );
            Line::from(vec![time, level, tag, msg])
        })
        .collect();

    let log = Paragraph::new(visible)
        .block(log_block)
        .wrap(Wrap { trim: true });
    frame.render_widget(log, area);
}

fn kv_line(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("  {:<10} ", label), LABEL_STYLE),
        Span::styled(value.to_string(), VALUE_STYLE),
    ])
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
    fn uptime_formatting() {
        let state = TuiStateInner::new();
        // Just verify it doesn't panic - actual value depends on timing
        let _uptime = state.uptime();
    }
}
