//! Full-screen entry form for first-run setup, shown before mining starts.
use std::io;

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Terminal;

const FORM_MAX_WIDTH: u16 = 110;
const FORM_HINT: &str = "Enter to continue   Ctrl+C to quit";

const BORDER: Style = Style::new().fg(Color::Rgb(60, 70, 90));
const TITLE: Style = Style::new()
    .fg(Color::Rgb(140, 170, 200))
    .add_modifier(Modifier::BOLD);
const LABEL: Style = Style::new().fg(Color::Rgb(100, 120, 150));
const VALUE: Style = Style::new()
    .fg(Color::Rgb(220, 220, 220))
    .add_modifier(Modifier::BOLD);
const CURSOR: Style = Style::new()
    .fg(Color::Rgb(16, 19, 24))
    .bg(Color::Rgb(220, 220, 220));
const HINT: Style = Style::new().fg(Color::Rgb(90, 90, 90));
const ERROR: Style = Style::new().fg(Color::Rgb(210, 110, 110));

pub(crate) struct PromptSpec<'a> {
    pub help: &'a str,
    pub label: &'a str,
    pub initial: &'a str,
    pub validate: &'a dyn Fn(&str) -> Result<(), String>,
}

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Continue,
    Submit,
    Cancel,
}

struct Form<'a> {
    spec: PromptSpec<'a>,
    input: String,
    error: Option<String>,
}

impl<'a> Form<'a> {
    fn new(spec: PromptSpec<'a>) -> Self {
        let input = spec.initial.to_string();
        Self {
            spec,
            input,
            error: None,
        }
    }

    fn value(&self) -> &str {
        self.input.trim()
    }

    fn handle_key(&mut self, key: KeyEvent) -> Action {
        if key.kind == KeyEventKind::Release {
            return Action::Continue;
        }
        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Action::Cancel,
            KeyCode::Esc => Action::Cancel,
            KeyCode::Enter => {
                let check = if self.value().is_empty() {
                    Err("This is required.".to_string())
                } else {
                    (self.spec.validate)(self.value())
                };
                match check {
                    Ok(()) => Action::Submit,
                    Err(message) => {
                        self.error = Some(message);
                        Action::Continue
                    }
                }
            }
            KeyCode::Backspace => {
                self.input.pop();
                self.error = None;
                Action::Continue
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.input.clear();
                self.error = None;
                Action::Continue
            }
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.input.push(ch);
                self.error = None;
                Action::Continue
            }
            _ => Action::Continue,
        }
    }

    /// Borders, then every row the wrapped text needs, so nothing falls off.
    fn height_for(&self, width: u16) -> u16 {
        let text_width = width.saturating_sub(4).max(1) as usize;
        let rows = |text: &str| text.chars().count().div_ceil(text_width).max(1) as u16;
        let error_rows = self.error.as_deref().map_or(0, rows);
        let hint_rows = rows(FORM_HINT);
        2 + rows(self.spec.help) + 1 + rows(self.spec.label) + 1 + 1 + error_rows + hint_rows
    }

    fn draw(&self, frame: &mut ratatui::Frame) {
        let area = frame.area();
        let width = area.width.min(FORM_MAX_WIDTH);
        let height = area.height.min(self.height_for(width));
        let form = Rect {
            x: area.x + (area.width - width) / 2,
            y: area.y + (area.height - height) / 2,
            width,
            height,
        };
        frame.render_widget(Clear, area);

        let inner_width = width.saturating_sub(4) as usize;
        let mut lines = vec![
            Line::from(Span::styled(self.spec.help.to_string(), VALUE)),
            Line::raw(""),
            Line::from(Span::styled(self.spec.label.to_string(), LABEL)),
            input_line(&self.input, inner_width),
            Line::raw(""),
        ];
        if let Some(error) = &self.error {
            lines.push(Line::from(Span::styled(error.clone(), ERROR)));
        }
        lines.push(Line::from(Span::styled(FORM_HINT, HINT)));

        let block = Block::default()
            .title(Span::styled(" seine setup ", TITLE))
            .borders(Borders::ALL)
            .border_style(BORDER);
        let inner = block.inner(form);
        frame.render_widget(block, form);
        frame.render_widget(
            Paragraph::new(lines).wrap(Wrap { trim: false }),
            Rect {
                x: inner.x + 1,
                width: inner.width.saturating_sub(2),
                ..inner
            },
        );
    }
}

/// Shows the end of what was typed, so a long pasted address never hides the cursor.
fn input_line(input: &str, width: usize) -> Line<'static> {
    let visible = width.saturating_sub(3);
    let count = input.chars().count();
    let tail: String = input.chars().skip(count.saturating_sub(visible)).collect();
    Line::from(vec![
        Span::styled("> ", LABEL),
        Span::styled(tail, VALUE),
        Span::styled(" ", CURSOR),
    ])
}

/// Draws the form for the showcase, in each state a user can reach.
#[cfg(test)]
pub(super) fn draw_for_showcase(
    frame: &mut ratatui::Frame,
    spec: PromptSpec<'_>,
    typed: &str,
    error: Option<&str>,
) {
    let mut form = Form::new(spec);
    form.input.push_str(typed);
    form.error = error.map(str::to_string);
    form.draw(frame);
}

/// Returns the entered value. Cancelling leaves the process: nothing has started
/// yet, and a double-clicked console should simply close.
pub(crate) fn prompt_screen(spec: PromptSpec<'_>) -> Result<String> {
    let mut form = Form::new(spec);
    enable_raw_mode().context("failed to open the setup screen")?;
    let mut stdout = io::stdout();
    if let Err(err) = execute!(stdout, EnterAlternateScreen) {
        let _ = disable_raw_mode();
        return Err(err).context("failed to open the setup screen");
    }
    let outcome = run_form(&mut form);
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen);
    match outcome.context("setup screen failed")? {
        Action::Submit => Ok(form.value().to_string()),
        _ => std::process::exit(130),
    }
}

fn run_form(form: &mut Form<'_>) -> io::Result<Action> {
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let result = loop {
        terminal.draw(|frame| form.draw(frame))?;
        // Drain what is queued before redrawing so a pasted address arrives at once.
        let mut action = Action::Continue;
        loop {
            match event::read()? {
                Event::Key(key) => action = form.handle_key(key),
                Event::Resize(_, _) => {}
                _ => continue,
            }
            if action != Action::Continue || !event::poll(std::time::Duration::ZERO)? {
                break;
            }
        }
        if action != Action::Continue {
            break action;
        }
    };
    let _ = terminal.show_cursor();
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    fn spec<'a>(validate: &'a dyn Fn(&str) -> Result<(), String>) -> PromptSpec<'a> {
        PromptSpec {
            help: "Welcome.",
            label: "Address",
            initial: "",
            validate,
        }
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn render(form: &Form<'_>, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal.draw(|frame| form.draw(frame)).unwrap();
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
    fn invalid_input_stays_on_the_form_with_a_reason() {
        let validate = |value: &str| {
            if value == "good" {
                Ok(())
            } else {
                Err("Not a Blocknet address.".to_string())
            }
        };
        let mut form = Form::new(spec(&validate));
        assert_eq!(form.handle_key(key(KeyCode::Enter)), Action::Continue);
        assert_eq!(form.error.as_deref(), Some("This is required."));

        form.handle_key(key(KeyCode::Char('b')));
        assert_eq!(form.error, None);
        assert_eq!(form.handle_key(key(KeyCode::Enter)), Action::Continue);
        assert!(render(&form, 120, 30).contains("Not a Blocknet address."));

        form.handle_key(key(KeyCode::Backspace));
        for ch in "good".chars() {
            form.handle_key(key(KeyCode::Char(ch)));
        }
        assert_eq!(form.handle_key(key(KeyCode::Enter)), Action::Submit);
        assert_eq!(form.value(), "good");
    }

    #[test]
    fn default_value_is_accepted_with_enter_and_cancel_is_available() {
        let validate = |_: &str| Ok(());
        let mut form = Form::new(PromptSpec {
            initial: "stratum+tcp://bntpool.com:3333",
            ..spec(&validate)
        });
        assert!(render(&form, 80, 24).contains("stratum+tcp://bntpool.com:3333"));
        assert_eq!(form.handle_key(key(KeyCode::Enter)), Action::Submit);
        assert_eq!(form.value(), "stratum+tcp://bntpool.com:3333");
        assert_eq!(form.handle_key(key(KeyCode::Esc)), Action::Cancel);
        assert_eq!(
            form.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Action::Cancel
        );
    }

    #[test]
    fn long_input_keeps_its_end_visible_on_small_terminals() {
        let validate = |_: &str| Ok(());
        let mut form = Form::new(spec(&validate));
        for ch in "a".repeat(120).chars().chain("END".chars()) {
            form.handle_key(key(KeyCode::Char(ch)));
        }
        assert!(render(&form, 60, 15).contains("END"));
        assert!(render(&form, 120, 30).contains("END"));
    }

    #[test]
    fn narrow_terminals_keep_the_whole_error_and_the_key_hint() {
        let validate = |_: &str| Err("That is not a Blocknet address. Use @name.".to_string());
        let mut form = Form::new(spec(&validate));
        form.handle_key(key(KeyCode::Char('x')));
        form.handle_key(key(KeyCode::Enter));
        let text = render(&form, 30, 20);
        assert!(text.contains("Use @name."), "{text}");
        assert!(text.contains("Ctrl+C"), "{text}");
    }

    #[test]
    fn tiny_terminals_do_not_panic() {
        let validate = |_: &str| Ok(());
        let form = Form::new(spec(&validate));
        render(&form, 10, 3);
    }
}
