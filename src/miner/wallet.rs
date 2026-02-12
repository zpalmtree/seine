use std::env;
use std::fs;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::is_raw_mode_enabled;

use crate::api::{is_wallet_already_loaded_error, ApiClient};
use crate::config::Config;

use super::mining_tui::{begin_prompt_session, render_tui_now, set_tui_state_label, TuiDisplay};
use super::ui::{error, info, success, warn};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum WalletPasswordSource {
    CliFlag,
    PasswordFile,
    Environment,
    Prompt,
}

impl WalletPasswordSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::CliFlag => "--wallet-password",
            Self::PasswordFile => "--wallet-password-file",
            Self::Environment => "SEINE_WALLET_PASSWORD",
            Self::Prompt => "terminal prompt",
        }
    }
}

pub(super) fn auto_load_wallet(
    client: &ApiClient,
    cfg: &Config,
    shutdown: &AtomicBool,
    tui: &mut Option<TuiDisplay>,
) -> Result<bool> {
    const MAX_PROMPT_ATTEMPTS: u32 = 3;

    let (mut password, source) = match resolve_wallet_password(cfg)? {
        Some((password, source)) => (password, source),
        None => {
            let Some(password) = prompt_wallet_password(tui)? else {
                return Ok(false);
            };
            (password, WalletPasswordSource::Prompt)
        }
    };
    let mut prompt_attempt = 1u32;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(false);
        }

        match client.load_wallet(&password) {
            Ok(()) => {
                success("WALLET", format!("loaded via {}", source.as_str()));
                password.clear();
                return Ok(true);
            }
            Err(err) if is_wallet_already_loaded_error(&err) => {
                info("WALLET", "already loaded");
                password.clear();
                return Ok(true);
            }
            Err(err)
                if source == WalletPasswordSource::Prompt
                    && prompt_attempt < MAX_PROMPT_ATTEMPTS =>
            {
                warn("WALLET", format!("load failed: {err:#}"));
                render_tui_now(tui);
                prompt_attempt += 1;
                let Some(next_password) = prompt_wallet_password(tui)? else {
                    return Ok(false);
                };
                password.clear();
                password = next_password;
                continue;
            }
            Err(err) => {
                password.clear();
                return Err(err).with_context(|| {
                    format!("wallet load request failed using {}", source.as_str())
                });
            }
        }
    }
}

fn resolve_wallet_password(cfg: &Config) -> Result<Option<(String, WalletPasswordSource)>> {
    if let Some(password) = &cfg.wallet_password {
        if password.is_empty() {
            bail!("--wallet-password is empty");
        }
        return Ok(Some((password.clone(), WalletPasswordSource::CliFlag)));
    }

    if let Some(path) = &cfg.wallet_password_file {
        let password = read_password_file(path)?;
        if password.is_empty() {
            bail!("wallet password file is empty: {}", path.display());
        }
        return Ok(Some((password, WalletPasswordSource::PasswordFile)));
    }

    if let Ok(password) = env::var("SEINE_WALLET_PASSWORD") {
        if !password.is_empty() {
            return Ok(Some((password, WalletPasswordSource::Environment)));
        }
    }

    if let Ok(password) = env::var("BNMINER_WALLET_PASSWORD") {
        if !password.is_empty() {
            return Ok(Some((password, WalletPasswordSource::Environment)));
        }
    }

    Ok(None)
}

fn read_password_file(path: &std::path::Path) -> Result<String> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read wallet password file at {}", path.display()))?;
    Ok(raw.trim_end_matches(['\r', '\n']).to_string())
}

fn prompt_wallet_password(tui: &mut Option<TuiDisplay>) -> Result<Option<String>> {
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return Ok(None);
    }

    let _prompt_guard = begin_prompt_session();
    let raw_mode = is_raw_mode_enabled().unwrap_or(false);

    set_tui_state_label(tui, "awaiting-wallet");
    error(
        "WALLET",
        "ACTION REQUIRED: wallet password needed to continue mining",
    );
    warn(
        "WALLET",
        "password input is hidden; type password and press Enter",
    );
    render_tui_now(tui);
    if raw_mode {
        return prompt_wallet_password_raw_mode();
    }

    let password = rpassword::prompt_password("wallet password (input hidden): ")
        .context("failed to read wallet password from terminal")?;
    if password.is_empty() {
        return Ok(None);
    }
    Ok(Some(password))
}

fn prompt_wallet_password_raw_mode() -> Result<Option<String>> {
    let mut password = String::new();

    loop {
        let event = event::read().context("failed to read wallet password key event")?;
        let Event::Key(key) = event else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }

        match key.code {
            KeyCode::Enter => break,
            KeyCode::Esc => return Ok(None),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Ok(None);
            }
            KeyCode::Backspace => {
                password.pop();
            }
            KeyCode::Char(ch) => {
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                {
                    password.push(ch);
                }
            }
            _ => {}
        }
    }

    if password.is_empty() {
        return Ok(None);
    }
    Ok(Some(password))
}
