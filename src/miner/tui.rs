use std::collections::VecDeque;
use std::io::{self, Stdout};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant};

use crossterm::cursor::Show;
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
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

use super::thread_control::{ThreadChangeRequest, ThreadControl};

const LOG_CAPACITY: usize = 2_000;
const BLOCK_MARKER_CAPACITY: usize = 4096;
const CONFIG_TWO_COLUMN_MIN_WIDTH: u16 = 80;
/// Below this many rows the header gives up its second wave row to the log.
const TALL_DASHBOARD_MIN_HEIGHT: u16 = 40;
const LOG_PAGE_ENTRIES: usize = 10;
const THREAD_NOTICE_LIFETIME: Duration = Duration::from_secs(8);

#[derive(Clone)]
pub struct LogEntry {
    pub elapsed_secs: f64,
    pub level: LogLevel,
    pub tag: String,
    pub message: String,
}

impl LogEntry {
    pub fn new(elapsed_secs: f64, level: LogLevel, tag: &str, message: &str) -> Self {
        Self {
            elapsed_secs,
            level,
            tag: tag.to_string(),
            message: message.to_string(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Success,
    Warn,
    Error,
    Mined,
}

impl LogLevel {
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
    /// The pool's current share difficulty for this miner.
    pub share_difficulty: Option<u64>,
    pub stale_shares: u64,
    pub accepted: u64,
    /// Resubmits after retryable ("server busy, retry") pool rejects.
    pub share_retries: u64,
    pub pool_address: String,
    pub pool_pending: String,
    pub pool_paid: String,
    pub pool_payout_minimum: Option<String>,
    pub wallet_visible: bool,
    pub wallet_address: String,
    pub wallet_pending: String,
    pub wallet_unlocked: String,
    pub device_hashrates: Vec<DeviceHashrate>,
    /// Backend names ("nvidia", "amd") of GPUs still initializing.
    pub pending_gpus: Vec<&'static str>,
    pub pending_gpus_since: Option<Instant>,
    pub dev_fee_active: bool,

    // Config (set once at startup)
    pub mode: String,
    pub api_url: String,
    pub pool_worker: String,
    pub threads: usize,
    pub thread_control: Option<ThreadControl>,
    pub thread_change_pending: bool,
    pub thread_notice: Option<String>,
    /// The notice on screen and when it appeared, so it can make way for the log.
    thread_notice_shown: Option<(String, Instant)>,
    thread_reply: Option<crossbeam_channel::Receiver<String>>,
    pub refresh_secs: u64,
    pub sse_enabled: bool,
    pub backends_desc: String,
    pub accounting: String,
    pub version: String,
    pub update_available: Option<String>,

    // Timing
    pub started_at: Instant,

    // Log
    pub log_entries: VecDeque<LogEntry>,
    /// Entries scrolled back from the newest; 0 follows new lines.
    pub log_scroll: usize,
    /// Bumped on every log change so the renderer can redraw promptly.
    pub log_seq: u64,

    /// Set once the process is shutting down.
    pub stopping: bool,
    /// True while the dashboard is open but no mining run has been set up yet.
    pub before_first_run: bool,

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
            share_difficulty: None,
            stale_shares: 0,
            accepted: 0,
            share_retries: 0,
            pool_address: "---".to_string(),
            pool_pending: "---".to_string(),
            pool_paid: "---".to_string(),
            pool_payout_minimum: None,
            wallet_visible: false,
            wallet_address: "---".to_string(),
            wallet_pending: "---".to_string(),
            wallet_unlocked: "---".to_string(),
            device_hashrates: Vec::new(),
            pending_gpus: Vec::new(),
            pending_gpus_since: None,
            dev_fee_active: false,

            mode: String::new(),
            api_url: String::new(),
            pool_worker: String::new(),
            threads: 0,
            thread_control: None,
            thread_change_pending: false,
            thread_notice: None,
            thread_notice_shown: None,
            thread_reply: None,
            refresh_secs: 0,
            sse_enabled: false,
            backends_desc: String::new(),
            accounting: String::new(),
            version: String::new(),
            update_available: None,

            started_at: Instant::now(),

            log_entries: VecDeque::new(),
            log_scroll: 0,
            log_seq: 0,
            stopping: false,
            before_first_run: false,

            block_found_ticks: VecDeque::with_capacity(BLOCK_MARKER_CAPACITY),
        }
    }

    pub fn push_log(&mut self, entry: LogEntry) {
        self.log_seq = self.log_seq.wrapping_add(1);
        // A repeated line stays on one row and just moves its age forward.
        if let Some(last) = self.log_entries.back_mut() {
            if last.level == entry.level && last.tag == entry.tag && last.message == entry.message {
                last.elapsed_secs = entry.elapsed_secs;
                return;
            }
        }
        if self.log_entries.len() >= LOG_CAPACITY {
            self.log_entries.pop_front();
        }
        self.log_entries.push_back(entry);
        // Keep what the reader is looking at in place while new lines arrive.
        if self.log_scroll > 0 {
            self.log_scroll = (self.log_scroll + 1).min(self.log_entries.len() - 1);
        }
    }

    /// Clears what belongs to one mining run; a thread change or restart keeps
    /// the log, balances and session totals on screen.
    pub(super) fn reset_for_new_run(&mut self) {
        self.before_first_run = false;
        self.state = "starting".to_string();
        self.blocktemplate_retry_since = None;
        self.round_hashrate = "0.000 H/s".to_string();
        self.share_difficulty = None;
        self.device_hashrates.clear();
        self.pending_gpus.clear();
        self.pending_gpus_since = None;
        self.dev_fee_active = false;
        self.thread_control = None;
        self.thread_change_pending = false;
        self.thread_notice = None;
        self.thread_reply = None;
    }

    /// Clears a finished thread notice after a while. Returns true when it did.
    pub(super) fn expire_thread_notice(&mut self, now: Instant) -> bool {
        let Some(notice) = self
            .thread_notice
            .as_ref()
            .filter(|_| !self.thread_change_pending)
        else {
            self.thread_notice_shown = None;
            return false;
        };
        match &self.thread_notice_shown {
            Some((shown, since)) if shown == notice => {
                if now.saturating_duration_since(*since) < THREAD_NOTICE_LIFETIME {
                    return false;
                }
                self.thread_notice = None;
                self.thread_notice_shown = None;
                true
            }
            _ => {
                self.thread_notice_shown = Some((notice.clone(), now));
                false
            }
        }
    }

    fn scroll_log(&mut self, key: KeyCode) -> bool {
        let oldest = self.log_entries.len().saturating_sub(1);
        self.log_scroll = match key {
            KeyCode::Up => self.log_scroll + 1,
            KeyCode::Down => self.log_scroll.saturating_sub(1),
            KeyCode::PageUp => self.log_scroll + LOG_PAGE_ENTRIES,
            KeyCode::PageDown => self.log_scroll.saturating_sub(LOG_PAGE_ENTRIES),
            KeyCode::Home => oldest,
            KeyCode::End => 0,
            _ => return false,
        }
        .min(oldest);
        true
    }

    pub(super) fn poll_thread_reply(&mut self) -> bool {
        let message = match self.thread_reply.as_ref().map(|rx| rx.try_recv()) {
            Some(Ok(message)) => message,
            Some(Err(crossbeam_channel::TryRecvError::Disconnected)) => {
                "Thread change unavailable; try again.".to_string()
            }
            Some(Err(crossbeam_channel::TryRecvError::Empty)) | None => return false,
        };
        self.thread_change_pending = false;
        self.thread_notice = Some(message);
        self.thread_reply = None;
        true
    }

    /// Returns true only for an explicit quit. No backend work runs on the UI thread.
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if key.kind != KeyEventKind::Press {
            return false;
        }
        if key.code == KeyCode::Char('q')
            || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
        {
            crate::diagnostics::stop_requested(seine_telemetry::ShutdownRequest::TuiQuit);
            if let Some(control) = &self.thread_control {
                control.quit_requested.store(true, Ordering::SeqCst);
            }
            return true;
        }
        if self.scroll_log(key.code) {
            return false;
        }
        self.poll_thread_reply();
        if !matches!(
            key.code,
            KeyCode::Char('+') | KeyCode::Char('=') | KeyCode::Char('-')
        ) {
            return false;
        }
        let Some(control) = self.thread_control.as_ref() else {
            self.thread_notice =
                Some("Thread controls unavailable: no active CPU backend.".to_string());
            return false;
        };
        if self.thread_change_pending {
            self.thread_notice = Some("Thread change in progress; please wait.".to_string());
            return false;
        }
        let threads = match key.code {
            KeyCode::Char('+') | KeyCode::Char('=') => {
                if control.current_threads >= control.max_threads {
                    self.thread_notice = Some(format!(
                        "Thread limit: {} (RAM/CPU cap). Cannot add a worker.",
                        control.max_threads
                    ));
                    return false;
                }
                control.current_threads + 1
            }
            KeyCode::Char('-') => {
                if control.current_threads <= 1 {
                    self.thread_notice =
                        Some("Minimum: 1 CPU thread. Cannot reduce further.".to_string());
                    return false;
                }
                control.current_threads - 1
            }
            _ => return false,
        };
        if threads == control.current_threads {
            return false;
        }
        let (reply, receiver) = crossbeam_channel::bounded(1);
        let request = ThreadChangeRequest {
            memory: control.memory.clone(),
            reply: Some(reply),
            live: control.live.clone(),
            max_threads: control.max_threads.max(control.current_threads),
            run_id: control.run_id,
            threads,
            quit_requested: Arc::clone(&control.quit_requested),
        };
        self.thread_notice = Some(match control.sender.try_send(request) {
            Ok(()) => {
                self.thread_change_pending = true;
                self.thread_reply = Some(receiver);
                format!(
                    "Checking RAM: {} -> {threads} threads...",
                    control.current_threads
                )
            }
            Err(_) => "Thread change unavailable; try again.".to_string(),
        });
        false
    }

    pub fn push_block_found_tick(&mut self, tick: u64) {
        if self.block_found_ticks.len() >= BLOCK_MARKER_CAPACITY {
            self.block_found_ticks.pop_front();
        }
        self.block_found_ticks.push_back(tick);
    }

    pub(super) fn uptime(&self) -> String {
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

/// Puts the terminal back when the process must exit without running destructors.
pub(super) fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen, Show);
}

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
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let original = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = disable_raw_mode();
            let _ = execute!(io::stdout(), LeaveAlternateScreen);
            original(info);
        }));
    });
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
const UPDATE_STYLE: Style = Style::new()
    .fg(Color::Rgb(230, 200, 120))
    .bg(Color::Rgb(100, 80, 20))
    .add_modifier(Modifier::BOLD);
// Keep block marker glyph single-cell across terminals.
const BLOCK_MARKER_GLYPH: &str = "♦";

fn draw_dashboard(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    // Give feedback its own wrapped rows: inline notices were clipped by the
    // two-column config panel and by narrow terminals.
    let area = if let Some(notice) = &state.thread_notice {
        let height = (notice.chars().count().div_ceil(area.width.max(1) as usize) as u16 + 1)
            .min(area.height.saturating_sub(1));
        frame.render_widget(
            Paragraph::new(notice.as_str())
                .style(VALUE_STYLE)
                .wrap(Wrap { trim: true }),
            Rect { height, ..area },
        );
        Rect {
            y: area.y + height,
            height: area.height - height,
            ..area
        }
    } else {
        area
    };
    let wide = area.width >= 80;
    let config_two_columns = config_uses_two_columns(area.width);

    let stats_height: u16 = if wide { 5 } else { 14 };
    let active_device_count = state.device_hashrates.len() as u16;
    let pending_count = state.pending_gpus.len() as u16;
    let active_rows = if wide {
        (active_device_count + 1) / 2 // 2 per row in wide mode
    } else {
        active_device_count
    };
    // Pending devices are always 1-per-line (not paired in wide mode)
    let device_rows = active_rows + pending_count;
    let devices_height: u16 = if device_rows > 0 { 2 + device_rows } else { 0 };
    let runtime_panel_height: u16 = 4;
    let wallet_panel_height = wallet_panel_height(state, area.width);
    let config_height: u16 = if config_two_columns {
        runtime_panel_height.max(wallet_panel_height)
    } else {
        runtime_panel_height + wallet_panel_height
    };
    let header_height: u16 = if area.height >= TALL_DASHBOARD_MIN_HEIGHT {
        4
    } else {
        3
    };

    if area.height < header_height + stats_height + devices_height + config_height + 4 {
        draw_compact_dashboard(frame, area, state);
        return;
    }

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

fn thread_line(state: &TuiStateInner) -> Line<'static> {
    let hint = if state.thread_control.is_some() {
        "  [+] [-]"
    } else {
        ""
    };
    let threads = if state.before_first_run {
        "---".to_string()
    } else {
        state.threads.to_string()
    };
    Line::from(vec![
        Span::styled("Threads: ", LABEL_STYLE),
        Span::styled(threads, VALUE_STYLE),
        Span::styled(hint, TITLE_STYLE),
    ])
}

fn draw_compact_dashboard(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let (state_display, state_color) = format_state_display(state);
    // Essential information comes first, even in a terminal only a few rows high.
    let mut lines = vec![
        thread_line(state),
        Line::from(vec![
            Span::styled("State: ", LABEL_STYLE),
            Span::styled(state_display, Style::default().fg(state_color)),
        ]),
        Line::from(format!("Current: {}", state.round_hashrate)),
        Line::from(format!("Average: {}", state.avg_hashrate)),
        Line::from(if state.mode == "pool" {
            format!(
                "Accepted: {}  Difficulty: {}",
                state.accepted,
                share_difficulty_display(state)
            )
        } else {
            format!(
                "Accepted: {}  Submitted: {}",
                state.accepted, state.submitted
            )
        }),
        Line::from(format!(
            "{}: {}  Height: {}",
            if state.mode == "pool" {
                "Rejected"
            } else {
                "Stale"
            },
            rejected_shares_display(state, format_plain_u64),
            state.height
        )),
    ];
    if state.mode == "pool" {
        if let Some(minimum) = &state.pool_payout_minimum {
            lines.insert(4, Line::from(format!("Min payout: {minimum}")));
        }
    }
    if let Some(latest) = &state.update_available {
        lines.push(Line::from(Span::styled(
            format!("Update {latest} available"),
            UPDATE_STYLE,
        )));
    }
    let summary_height = (lines.len() as u16).min(area.height);
    frame.render_widget(
        Paragraph::new(lines).style(VALUE_STYLE),
        Rect {
            height: summary_height,
            ..area
        },
    );
    let log_height = area.height.saturating_sub(summary_height);
    if log_height >= 3 {
        draw_log(
            frame,
            Rect {
                y: area.y + summary_height,
                height: log_height,
                ..area
            },
            state,
        );
    }
}

fn draw_header(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let uptime = state.uptime();
    let tick = state.started_at.elapsed().as_secs();

    let mut title = vec![Span::styled(
        format!(" seine {} ", state.version),
        TITLE_STYLE,
    )];
    if let Some(latest) = &state.update_available {
        title.push(Span::styled(
            format!(" update {latest} available "),
            UPDATE_STYLE,
        ));
    }
    let uptime_text = format!(" uptime {uptime} ");

    let inner_width = area.width.saturating_sub(2) as usize;

    let mut lines = wave_lines(inner_width, tick, &state.block_found_ticks);
    lines.truncate(area.height.saturating_sub(2).max(1) as usize);

    // Overlay uptime on the last wave row
    if let Some(last) = lines.last_mut() {
        let uptime_len = uptime_text.chars().count();
        let keep = inner_width.saturating_sub(uptime_len);
        last.spans.truncate(keep);
        last.spans.push(Span::styled(uptime_text, DIM_STYLE));
    }

    let block = Block::default()
        .title(Line::from(title))
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
    if state.stopping {
        return ("stopping".to_string(), Color::Rgb(200, 170, 80));
    }
    let mut state_display = if state.dev_fee_active && state.state == "working" {
        "working (dev fee)".to_string()
    } else {
        friendly_state_label(&state.state).to_string()
    };

    let mut state_color = match state.state.as_str() {
        "working" => Color::Rgb(100, 170, 100),
        "pool-disconnected" | "pool-resync" => Color::Rgb(215, 165, 95),
        "pool-login-rejected" => Color::Rgb(220, 105, 95),
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

fn friendly_state_label(state: &str) -> &str {
    match state {
        "initializing" => "starting",
        "pool-connecting" => "connecting",
        "pool-connected" => "connected",
        "pool-authenticated" => "logged in",
        "waiting-pool-job" => "waiting for work",
        "pool-disconnected" => "reconnecting",
        "pool-resync" => "resyncing with pool",
        "pool-login-rejected" => "login rejected",
        other => other,
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
    let mining_lines = mining_panel_lines(state);
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
    let mining_lines = mining_panel_lines(state);
    let mining_block = Block::default()
        .title(Span::styled(" MINING ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    frame.render_widget(Paragraph::new(mining_lines).block(mining_block), rows[2]);
}

fn mining_panel_lines(state: &TuiStateInner) -> Vec<Line<'static>> {
    let first = if state.mode == "pool" {
        kv_line("Difficulty", &share_difficulty_display(state))
    } else {
        kv_line("Submitted", &format_u64(state.submitted))
    };
    let rejected_or_stale_label = if state.mode == "pool" {
        "Rejected"
    } else {
        "Stale"
    };
    vec![
        first,
        kv_line(
            rejected_or_stale_label,
            &rejected_shares_display(state, format_u64),
        ),
        kv_line("Accepted", &format_u64(state.accepted)),
    ]
}

fn share_difficulty_display(state: &TuiStateInner) -> String {
    state
        .share_difficulty
        .map_or_else(|| "---".to_string(), format_u64)
}

/// Rejected/stale count, with busy-reject resubmits alongside in pool mode so
/// an overloaded pool is visible without inflating the rejected count.
fn rejected_shares_display(state: &TuiStateInner, format: fn(u64) -> String) -> String {
    let rejected = format(state.stale_shares);
    if state.mode == "pool" && state.share_retries > 0 {
        format!("{rejected} ({} retried)", format(state.share_retries))
    } else {
        rejected
    }
}

fn format_plain_u64(value: u64) -> String {
    value.to_string()
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

    // Show GPU backends that are still initializing.
    if !state.pending_gpus.is_empty() {
        let pending_elapsed = state
            .pending_gpus_since
            .map(|started| started.elapsed())
            .unwrap_or_default();
        let pending_elapsed_label = format_duration_compact(pending_elapsed);
        let mut shown_per_kind: Vec<(&str, u64)> = Vec::new();
        for kind in &state.pending_gpus {
            let shown = match shown_per_kind.iter_mut().find(|(seen, _)| seen == kind) {
                Some((_, shown)) => {
                    *shown += 1;
                    *shown
                }
                None => {
                    shown_per_kind.push((kind, 1));
                    1
                }
            };
            let online = state
                .device_hashrates
                .iter()
                .filter(|d| d.name.starts_with(&format!("{kind}#")))
                .count() as u64;
            let pending_status = pending_gpu_status_label(kind, pending_elapsed);
            let pending_message = format!("{pending_status} ({pending_elapsed_label})");
            let name = format!("{kind}#{}", online + shown);
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
    let wallet_height = wallet_panel_height(state, area.width);
    if config_uses_two_columns(area.width) {
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
        .constraints([Constraint::Length(4), Constraint::Length(wallet_height)])
        .split(area);
    draw_config_runtime_panel(frame, rows[0], state);
    draw_config_wallet_panel(frame, rows[1], state);
}

fn config_uses_two_columns(width: u16) -> bool {
    width >= CONFIG_TWO_COLUMN_MIN_WIDTH
}

fn wallet_panel_height(state: &TuiStateInner, config_width: u16) -> u16 {
    let base = if state.mode == "pool" && state.wallet_visible {
        6
    } else {
        4
    };
    let panel_width = if config_uses_two_columns(config_width) {
        config_width / 2
    } else {
        config_width
    };
    base + if state.mode == "pool" {
        pool_paid_lines(state, panel_width.saturating_sub(2) as usize).len() as u16 - 1
    } else {
        0
    }
}

fn pool_paid_lines(state: &TuiStateInner, inner_width: usize) -> Vec<Line<'static>> {
    let label = if state.wallet_visible {
        "P.Paid"
    } else {
        "Paid"
    };
    if let Some(minimum) = &state.pool_payout_minimum {
        let line =
            wallet_line_with_right(label, &state.pool_paid, "Min payout", minimum, inner_width);
        if line.width() <= inner_width {
            return vec![line];
        }
        return vec![
            kv_line(label, &state.pool_paid),
            kv_line("Min payout", minimum),
        ];
    }
    vec![kv_line(label, &state.pool_paid)]
}

fn draw_config_runtime_panel(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let (endpoint_label, endpoint) = if state.mode == "pool" {
        ("  Pool: ", endpoint_for_display(&state.api_url))
    } else {
        ("  Daemon: ", state.api_url.as_str())
    };
    let line1 = Line::from(vec![
        Span::styled(endpoint_label, LABEL_STYLE),
        Span::styled(endpoint, VALUE_STYLE),
    ]);
    // Thread keys first: a long worker name may clip, the controls must not.
    let mut line2 = thread_line(state);
    line2.spans.insert(0, Span::raw("  "));
    if state.mode == "pool" {
        line2.spans.push(Span::styled("  Worker: ", LABEL_STYLE));
        line2
            .spans
            .push(Span::styled(state.pool_worker.clone(), VALUE_STYLE));
    }
    let config_block = Block::default()
        .title(Span::styled(" CONFIG ", TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    let config = Paragraph::new(vec![line1, line2]).block(config_block);
    frame.render_widget(config, area);
}

fn endpoint_for_display(endpoint: &str) -> &str {
    endpoint.strip_prefix("stratum+tcp://").unwrap_or(endpoint)
}

/// Drops the right-hand value when both will not fit, rather than clipping it.
fn wallet_line_fitted(
    left_label: &str,
    left_value: &str,
    right_label: &str,
    right_value: &str,
    inner_width: usize,
) -> Line<'static> {
    let line = wallet_line_with_right(
        left_label,
        left_value,
        right_label,
        right_value,
        inner_width,
    );
    if line.width() <= inner_width {
        line
    } else {
        kv_line(left_label, left_value)
    }
}

fn draw_config_wallet_panel(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let inner_width = area.width.saturating_sub(2) as usize;
    let pool_address = compact_wallet_address(&state.pool_address);
    let wallet_address = compact_wallet_address(&state.wallet_address);
    let (title, mut wallet_lines) = if state.mode == "pool" && state.wallet_visible {
        (
            " BALANCES ",
            vec![
                wallet_line_fitted(
                    "P.Pending",
                    &state.pool_pending,
                    "Miner",
                    &pool_address,
                    inner_width,
                ),
                kv_line("P.Paid", &state.pool_paid),
                wallet_line_fitted(
                    "W.Pending",
                    &state.wallet_pending,
                    "Address",
                    &wallet_address,
                    inner_width,
                ),
                kv_line("Unlocked", &state.wallet_unlocked),
            ],
        )
    } else if state.mode == "pool" {
        (
            " POOL BALANCE ",
            vec![
                wallet_line_fitted(
                    "Pending",
                    &state.pool_pending,
                    "Miner",
                    &pool_address,
                    inner_width,
                ),
                kv_line("Paid", &state.pool_paid),
            ],
        )
    } else {
        (
            " WALLET ",
            vec![
                wallet_line_fitted(
                    "Pending",
                    &state.wallet_pending,
                    "Address",
                    &wallet_address,
                    inner_width,
                ),
                kv_line("Unlocked", &state.wallet_unlocked),
            ],
        )
    };
    if state.mode == "pool" {
        wallet_lines.splice(1..2, pool_paid_lines(state, inner_width));
    }
    let wallet_block = Block::default()
        .title(Span::styled(title, TITLE_STYLE))
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);
    let wallet = Paragraph::new(wallet_lines).block(wallet_block);
    frame.render_widget(wallet, area);
}

fn draw_log(frame: &mut ratatui::Frame, area: Rect, state: &TuiStateInner) {
    let title = if state.log_scroll > 0 {
        " LOG  paused, End to follow "
    } else {
        " LOG "
    };
    let hint = if state.stopping {
        " q again to force quit "
    } else {
        " ↑↓ scroll  q quit "
    };
    let log_block = Block::default()
        .title(Span::styled(title, TITLE_STYLE))
        .title(Line::from(Span::styled(hint, DIM_STYLE)).right_aligned())
        .borders(Borders::ALL)
        .border_style(BORDER_STYLE);

    let inner_height = area.height.saturating_sub(2) as usize;
    let inner_width = area.width.saturating_sub(2) as usize;
    let now_elapsed_secs = state.started_at.elapsed().as_secs_f64();
    // Walk back from the newest entry and stop as soon as the panel is full.
    let mut rows: Vec<Line> = Vec::with_capacity(inner_height);
    for entry in state.log_entries.iter().rev().skip(state.log_scroll) {
        rows.extend(
            log_entry_rows(entry, now_elapsed_secs, inner_width)
                .into_iter()
                .rev(),
        );
        if rows.len() >= inner_height {
            break;
        }
    }
    rows.truncate(inner_height);
    rows.reverse();

    frame.render_widget(Paragraph::new(rows).block(log_block), area);
}

fn log_entry_rows(
    entry: &LogEntry,
    now_elapsed_secs: f64,
    inner_width: usize,
) -> Vec<Line<'static>> {
    const TAG_WIDTH: usize = 9;
    const AGE_WIDTH: usize = 9;
    // The age column gives way to the message on very narrow terminals.
    const AGE_MIN_INNER_WIDTH: usize = 60;
    let tag = Span::styled(
        format!(" {:<width$}", entry.tag, width = TAG_WIDTH - 1),
        Style::default()
            .fg(entry.level.label_fg())
            .bg(entry.level.label_bg())
            .add_modifier(Modifier::BOLD),
    );
    let prefix_width = TAG_WIDTH + 1;
    let age_width = if inner_width >= AGE_MIN_INNER_WIDTH {
        AGE_WIDTH
    } else {
        0
    };
    let message_width = inner_width.saturating_sub(prefix_width + age_width).max(1);
    let message_style = if matches!(entry.level, LogLevel::Mined) {
        Style::default()
            .fg(entry.level.color())
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(entry.level.color())
    };
    let mut rows = Vec::new();
    for (index, chunk) in wrap_str(&entry.message, message_width)
        .into_iter()
        .enumerate()
    {
        if index > 0 {
            rows.push(Line::from(vec![
                Span::raw(" ".repeat(prefix_width)),
                Span::styled(chunk, message_style),
            ]));
            continue;
        }
        let mut spans = vec![tag.clone(), Span::raw(" ")];
        let used = chunk.chars().count();
        spans.push(Span::styled(chunk, message_style));
        if age_width > 0 {
            let age =
                format_elapsed_log_ago(log_entry_age_secs(now_elapsed_secs, entry.elapsed_secs));
            spans.push(Span::raw(" ".repeat(message_width.saturating_sub(used))));
            spans.push(Span::styled(
                format!("{age:>width$} ", width = AGE_WIDTH - 1),
                DIM_STYLE,
            ));
        }
        rows.push(Line::from(spans));
    }
    rows
}

fn log_entry_age_secs(now_elapsed_secs: f64, entry_elapsed_secs: f64) -> f64 {
    (now_elapsed_secs - entry_elapsed_secs).max(0.0)
}

fn format_elapsed_log_ago(elapsed_secs: f64) -> String {
    let total = elapsed_secs.max(0.0).floor() as u64;
    match total {
        0..=4 => "now".to_string(),
        5..=59 => format!("{total}s ago"),
        60..=3_599 => format!("{}m ago", total / 60),
        3_600..=86_399 => format!("{}h ago", total / 3_600),
        _ => format!("{}d ago", total / 86_400),
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

/// Wraps at spaces, splitting only words longer than a whole row (URLs, paths).
fn wrap_str(s: &str, max: usize) -> Vec<String> {
    let max = max.max(1);
    let mut lines = Vec::new();
    let mut line = String::new();
    let mut width = 0;
    for word in s.split(' ') {
        let word_width = word.chars().count();
        if width > 0 && width + 1 + word_width > max {
            lines.push(std::mem::take(&mut line));
            width = 0;
        }
        if width > 0 {
            line.push(' ');
            width += 1;
        }
        let mut chars = word.chars();
        let mut remaining = word_width;
        while width + remaining > max {
            let take = max - width;
            line.extend(chars.by_ref().take(take));
            lines.push(std::mem::take(&mut line));
            remaining -= take;
            width = 0;
        }
        line.extend(chars);
        width += remaining;
    }
    lines.push(line);
    lines
}

fn pending_gpu_status_label(kind: &str, elapsed: Duration) -> &'static str {
    if elapsed < Duration::from_secs(20) {
        if kind == "amd" {
            "starting HIP runtime"
        } else {
            "starting CUDA runtime"
        }
    } else if elapsed < Duration::from_secs(90) {
        if kind == "amd" {
            "preparing AMD backend"
        } else {
            "preparing NVIDIA backend"
        }
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
    use ratatui::backend::TestBackend;

    fn render_text(state: &TuiStateInner, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal
            .draw(|frame| draw_dashboard(frame, frame.area(), state))
            .unwrap();
        let buffer = terminal.backend().buffer();
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn payout_minimum_survives_full_and_compact_layouts() {
        let mut state = TuiStateInner::new();
        state.mode = "pool".into();
        state.pool_payout_minimum = Some("10 BNT".into());
        for wallet_visible in [false, true] {
            state.wallet_visible = wallet_visible;
            for (width, height) in [(160, 32), (120, 30), (80, 24), (40, 18), (40, 8)] {
                let text = render_text(&state, width, height);
                assert!(text.contains("Min payout"), "{width}x{height}: {text}");
                assert!(text.contains("10 BNT"), "{width}x{height}: {text}");
                if width >= 120 {
                    assert!(
                        text.lines().any(|line| line.contains("Paid")
                            && line.contains("Min payout")
                            && line.contains("10 BNT")),
                        "{width}x{height}: {text}"
                    );
                }
            }
        }
        state.pool_payout_minimum = None;
        assert!(!render_text(&state, 120, 30).contains("Min payout"));
        state.mode = "daemon".into();
        state.pool_payout_minimum = Some("10 BNT".into());
        assert!(!render_text(&state, 120, 30).contains("Min payout"));
    }

    #[test]
    fn essential_stats_survive_short_narrow_and_device_heavy_layouts() {
        let mut state = TuiStateInner::new();
        state.threads = 7;
        state.state = "working".into();
        state.round_hashrate = "1.234 H/s".into();
        state.wallet_visible = true;
        state.mode = "pool".into();
        state.pending_gpus = vec!["nvidia"; 12];
        state.pending_gpus.extend(["amd"; 4]);
        for (width, height) in [
            (40, 8),
            (60, 12),
            (79, 24),
            (80, 24),
            (100, 10),
            (160, 12),
            (160, 50),
        ] {
            let text = render_text(&state, width, height);
            assert!(text.contains("Threads"), "{width}x{height}: {text}");
            assert!(text.contains("working"), "{width}x{height}: {text}");
            assert!(text.contains("1.234 H/s"), "{width}x{height}: {text}");
        }
        // Even an extreme resize must not panic, and the count takes priority.
        assert!(render_text(&state, 24, 1).contains("Threads: 7"));
        render_text(&state, 0, 0);
    }

    #[test]
    fn update_notice_shows_in_full_and_compact_layouts_without_displacing_stats() {
        let mut state = TuiStateInner::new();
        state.version = "v0.2.22".into();
        state.threads = 7;
        state.state = "working".into();
        assert!(!render_text(&state, 160, 50).contains("available"));

        state.update_available = Some("v0.2.23".into());
        let full = render_text(&state, 160, 50);
        assert!(
            full.contains(" seine v0.2.22  update v0.2.23 available "),
            "{full}"
        );
        let compact = render_text(&state, 60, 12);
        assert!(compact.contains("Update v0.2.23 available"), "{compact}");
        assert!(render_text(&state, 24, 1).contains("Threads: 7"));
    }

    fn controlled_state() -> (
        TuiStateInner,
        crossbeam_channel::Receiver<ThreadChangeRequest>,
    ) {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let mut state = TuiStateInner::new();
        state.threads = 2;
        state.thread_control = Some(ThreadControl {
            live: None,
            run_id: 42,
            sender,
            memory: None,
            current_threads: 2,
            max_threads: 4,
            quit_requested: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        (state, receiver)
    }

    fn press(state: &mut TuiStateInner, code: KeyCode) -> bool {
        state.handle_key(KeyEvent::new(code, KeyModifiers::NONE))
    }

    #[test]
    fn dashboard_keys_request_change_immediately_and_preserve_active_count() {
        for (key, expected) in [('+', 3), ('=', 3), ('-', 1)] {
            let (mut state, receiver) = controlled_state();
            for (width, height) in [(80, 12), (120, 34)] {
                assert!(render_text(&state, width, height).contains("Threads: 2  [+] [-]"));
            }
            press(&mut state, KeyCode::Char(key));
            let request = receiver.try_recv().unwrap();
            assert_eq!((request.run_id, request.threads), (42, expected));
            assert_eq!(request.max_threads, 4);
            assert_eq!(state.threads, 2);
            assert!(state.thread_change_pending);
            for (width, height) in [(80, 12), (120, 34)] {
                let text = render_text(&state, width, height);
                assert!(text.contains("Checking RAM: 2 ->"));
                assert!(text.contains("Threads: 2"));
            }
            // Do not enqueue another restart while the first is being applied.
            press(&mut state, KeyCode::Char(key));
            assert!(receiver.is_empty());
            assert!(press(&mut state, KeyCode::Char('q')));
            assert!(request.quit_requested.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn dashboard_keys_respect_limits_and_ignore_releases() {
        for (current, key) in [(0, '-'), (1, '-'), (4, '+'), (6, '+')] {
            let (mut state, receiver) = controlled_state();
            state.thread_control.as_mut().unwrap().current_threads = current;
            press(&mut state, KeyCode::Char(key));
            assert!(receiver.is_empty());
            assert!(!state.thread_change_pending);
            assert!(state
                .thread_notice
                .as_ref()
                .unwrap()
                .contains(if key == '+' {
                    "Thread limit:"
                } else {
                    "Minimum:"
                }));
        }
        let (mut state, receiver) = controlled_state();
        state.thread_control.as_mut().unwrap().current_threads = 6;
        press(&mut state, KeyCode::Char('-'));
        let request = receiver.try_recv().unwrap();
        assert_eq!(request.threads, 5);
        assert_eq!(
            request.max_threads, 6,
            "allow reducing an explicit above-auto count"
        );
        let (mut state, receiver) = controlled_state();
        for kind in [KeyEventKind::Release, KeyEventKind::Repeat] {
            state.handle_key(KeyEvent::new_with_kind(
                KeyCode::Char('+'),
                KeyModifiers::NONE,
                kind,
            ));
        }
        assert!(receiver.is_empty());
        for code in [KeyCode::Char('t'), KeyCode::Enter, KeyCode::Esc] {
            press(&mut state, code);
        }
        assert!(receiver.is_empty());
    }

    #[test]
    fn a_live_change_clears_pending_and_a_lost_controller_does_not_stick() {
        let (mut state, receiver) = controlled_state();
        press(&mut state, KeyCode::Char('+'));
        assert!(state.thread_change_pending);
        let request = receiver.try_recv().unwrap();
        request
            .reply
            .unwrap()
            .send("CPU threads: 3".to_string())
            .unwrap();
        assert!(state.poll_thread_reply());
        assert!(!state.thread_change_pending);
        assert_eq!(state.thread_notice.as_deref(), Some("CPU threads: 3"));

        press(&mut state, KeyCode::Char('+'));
        assert!(state.thread_change_pending);
        drop(receiver.try_recv().unwrap());
        assert!(state.poll_thread_reply());
        assert!(
            !state.thread_change_pending,
            "a dropped reply must not block changes"
        );
    }

    #[test]
    fn ram_rejection_is_visible_and_unblocks_retry() {
        let (mut state, receiver) = controlled_state();
        press(&mut state, KeyCode::Char('+'));
        let request = receiver.try_recv().unwrap();
        let reason = super::super::thread_control::validate_headroom(3, 2)
            .unwrap_err()
            .to_string();
        request.reply.unwrap().send(reason.clone()).unwrap();
        state.poll_thread_reply();
        assert!(!state.thread_change_pending);
        assert_eq!(state.threads, 2);
        for (width, height) in [(40, 18), (80, 12), (160, 34)] {
            let text = render_text(&state, width, height);
            // Whitespace can change at word wrap; no words may be clipped.
            assert!(text
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .contains(&reason));
        }
        press(&mut state, KeyCode::Char('-'));
        assert_eq!(receiver.try_recv().unwrap().threads, 1);
        assert!(state.thread_change_pending);
    }

    #[test]
    fn unavailable_thread_control_can_retry() {
        let (mut state, receiver) = controlled_state();
        drop(receiver);
        press(&mut state, KeyCode::Char('+'));
        assert!(!state.thread_change_pending);
        assert!(render_text(&state, 80, 12).contains("Thread change unavailable"));
    }

    #[test]
    fn gpu_only_session_has_no_thread_control() {
        let mut state = TuiStateInner::new();
        press(&mut state, KeyCode::Char('+'));
        press(&mut state, KeyCode::Char('-'));
        assert!(!state.thread_change_pending);
        assert!(!render_text(&state, 80, 24).contains("[+] [-]"));
    }

    #[test]
    fn rejected_shares_display_shows_pool_retries_separately() {
        let mut state = TuiStateInner::new();
        state.mode = "pool".to_string();
        state.stale_shares = 3;
        assert_eq!(rejected_shares_display(&state, format_u64), "3");
        state.share_retries = 1_250;
        assert_eq!(
            rejected_shares_display(&state, format_u64),
            "3 (1,250 retried)"
        );
        state.mode = "solo".to_string();
        assert_eq!(rejected_shares_display(&state, format_u64), "3");
    }

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
        for i in 0..LOG_CAPACITY + 50 {
            state.push_log(LogEntry::new(
                i as f64,
                LogLevel::Info,
                "TEST",
                &format!("msg {i}"),
            ));
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
    fn config_layout_uses_two_columns_only_when_wide_enough() {
        assert!(!config_uses_two_columns(CONFIG_TWO_COLUMN_MIN_WIDTH - 1));
        assert!(config_uses_two_columns(CONFIG_TWO_COLUMN_MIN_WIDTH));
    }

    fn test_entry(message: &str) -> LogEntry {
        LogEntry::new(1.0, LogLevel::Info, "TEST", message)
    }

    #[test]
    fn finished_thread_notice_makes_way_for_the_log() {
        let mut state = TuiStateInner::new();
        let start = Instant::now();
        state.thread_notice = Some("CPU threads: 4".into());
        assert!(!state.expire_thread_notice(start));
        assert!(!state.expire_thread_notice(start + THREAD_NOTICE_LIFETIME / 2));
        assert!(state.expire_thread_notice(start + THREAD_NOTICE_LIFETIME));
        assert_eq!(state.thread_notice, None);

        // A change still in progress keeps its notice, and restarts the clock after.
        state.thread_notice = Some("Checking RAM".into());
        state.thread_change_pending = true;
        assert!(!state.expire_thread_notice(start));
        assert!(!state.expire_thread_notice(start + THREAD_NOTICE_LIFETIME * 2));
        assert!(state.thread_notice.is_some());
        state.thread_change_pending = false;
        assert!(!state.expire_thread_notice(start + THREAD_NOTICE_LIFETIME * 2));
        assert!(state.thread_notice.is_some());
    }

    #[test]
    fn pool_mode_shows_share_difficulty_in_every_layout() {
        let mut state = TuiStateInner::new();
        state.mode = "pool".into();
        state.submitted = 7;
        state.accepted = 3;
        assert!(render_text(&state, 120, 30).contains("Difficulty ---"));

        state.share_difficulty = Some(12_345);
        for (width, height) in [(120, 30), (80, 24), (60, 30), (60, 12)] {
            let text = render_text(&state, width, height);
            assert!(text.contains("12,345"), "{width}x{height}: {text}");
            assert!(!text.contains("Submitted"), "{width}x{height}: {text}");
        }
    }

    #[test]
    fn daemon_mode_keeps_submitted_because_it_has_no_share_difficulty() {
        let mut state = TuiStateInner::new();
        state.mode = "daemon".into();
        for (width, height) in [(120, 30), (60, 12)] {
            let text = render_text(&state, width, height);
            assert!(text.contains("Submitted"), "{width}x{height}: {text}");
            assert!(!text.contains("Difficulty"), "{width}x{height}: {text}");
        }
    }

    #[test]
    fn repeated_lines_stay_on_one_row_without_a_counter() {
        let mut state = TuiStateInner::new();
        let mut repeat = test_entry("pool connection lost");
        state.push_log(repeat.clone());
        repeat.elapsed_secs = 9.0;
        state.push_log(repeat);
        assert_eq!(state.log_entries.len(), 1);
        assert_eq!(state.log_entries[0].elapsed_secs, 9.0);
        assert_eq!(state.log_entries[0].message, "pool connection lost");

        state.push_log(test_entry("connected to pool"));
        state.push_log(test_entry("pool connection lost"));
        assert_eq!(state.log_entries.len(), 3);
    }

    #[test]
    fn scrolling_back_pins_the_view_until_end_is_pressed() {
        let mut state = TuiStateInner::new();
        for i in 0..100 {
            state.push_log(test_entry(&format!("line-{i:03}")));
        }
        assert!(render_text(&state, 120, 30).contains("line-099"));

        assert!(!press(&mut state, KeyCode::PageUp));
        assert_eq!(state.log_scroll, LOG_PAGE_ENTRIES);
        let paused = render_text(&state, 120, 30);
        assert!(paused.contains("line-089"));
        assert!(!paused.contains("line-099"));
        assert!(paused.contains("End to follow"));

        // A new line must not move what the reader is looking at.
        state.push_log(test_entry("line-new"));
        let still = render_text(&state, 120, 30);
        assert!(still.contains("line-089"));
        assert!(!still.contains("line-new"));

        press(&mut state, KeyCode::Home);
        assert!(render_text(&state, 120, 30).contains("line-000"));
        press(&mut state, KeyCode::Up);
        assert_eq!(state.log_scroll, state.log_entries.len() - 1);

        press(&mut state, KeyCode::End);
        assert_eq!(state.log_scroll, 0);
        assert!(render_text(&state, 120, 30).contains("line-new"));
    }

    fn log_inner_rows(state: &TuiStateInner, width: u16, height: u16) -> usize {
        // One bordered panel holds the log: count rows between its borders.
        let text = render_text(state, width, height);
        let lines: Vec<&str> = text.lines().collect();
        let top = lines
            .iter()
            .position(|line| line.contains(" LOG "))
            .unwrap();
        lines.len() - top - 2
    }

    #[test]
    fn common_window_sizes_leave_the_log_room_to_read() {
        let mut state = TuiStateInner::new();
        state.mode = "pool".into();
        state.device_hashrates.push(DeviceHashrate {
            name: "cpu#1".into(),
            current: "1.000 kH/s".into(),
            average: "1.000 kH/s".into(),
        });
        assert_eq!(log_inner_rows(&state, 120, 30), 13);
        assert_eq!(log_inner_rows(&state, 80, 24), 7);
        assert_eq!(log_inner_rows(&state, 120, 40), 22);
        assert_eq!(log_inner_rows(&state, 140, 45), 27);
    }

    #[test]
    fn thread_keys_survive_a_long_worker_name_at_80_columns() {
        let (mut state, _rx) = controlled_state();
        state.mode = "pool".into();
        state.backends_desc = "cpu#1".into();
        state.api_url = "stratum+tcp://bntpool.com:3333".into();
        state.pool_worker = "a-very-long-worker-name-from-the-config".into();
        let text = render_text(&state, 80, 24);
        assert!(text.contains("[+] [-]"), "{text}");
        assert!(text.contains("Pool: bntpool.com:3333"), "{text}");
    }

    #[test]
    fn narrow_wallet_panel_keeps_balances_instead_of_clipping_the_address() {
        let mut state = TuiStateInner::new();
        state.mode = "pool".into();
        state.pool_address = "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789AbCdEfGhIj".into();
        state.pool_pending = "123.45678901 BNT".into();
        let text = render_text(&state, 80, 24);
        assert!(text.contains("123.45678901 BNT"));
    }

    #[test]
    fn wrap_prefers_spaces_and_splits_only_oversized_words() {
        assert_eq!(wrap_str("one two three", 7), ["one two", "three"]);
        assert_eq!(wrap_str("abcdefghij kl", 4), ["abcd", "efgh", "ij", "kl"]);
        assert_eq!(wrap_str("short", 80), ["short"]);
        assert_eq!(wrap_str("", 10), [""]);
    }

    #[test]
    fn startup_states_read_as_plain_words() {
        let mut state = TuiStateInner::new();
        state.state = "pool-connecting".into();
        assert_eq!(format_state_display(&state).0, "connecting");
        state.state = "preparing miners".into();
        assert_eq!(format_state_display(&state).0, "preparing miners");
        state.stopping = true;
        assert_eq!(format_state_display(&state).0, "stopping");
        assert!(render_text(&state, 120, 30).contains("q again to force quit"));
    }

    #[test]
    fn uptime_formatting() {
        let state = TuiStateInner::new();
        // Just verify it doesn't panic - actual value depends on timing
        let _uptime = state.uptime();
    }

    #[test]
    fn log_elapsed_formatter_uses_relative_ago_style() {
        assert_eq!(format_elapsed_log_ago(0.0), "now");
        assert_eq!(format_elapsed_log_ago(12.0), "12s ago");
        assert_eq!(format_elapsed_log_ago(65.0), "1m ago");
        assert_eq!(format_elapsed_log_ago(3_600.0), "1h ago");
        assert_eq!(format_elapsed_log_ago(32_857.9), "9h ago");
        assert_eq!(format_elapsed_log_ago(360_000.0), "4d ago");
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

#[cfg(test)]
#[path = "thread_showcase.rs"]
mod thread_showcase;
