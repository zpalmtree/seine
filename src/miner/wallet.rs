use std::env;
use std::fs;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::is_raw_mode_enabled;

use crate::config::Config;
use crate::daemon_api::{
    is_no_wallet_loaded_error, is_retryable_api_error, is_timeout_api_error,
    is_wallet_already_loaded_error, is_wallet_wrong_password_error, ApiClient,
};

use super::mining_tui::{begin_prompt_session, render_tui_now, set_tui_state_label, TuiDisplay};
use super::ui::{error, info, success, warn};

const MAX_PROMPT_ATTEMPTS: u32 = 3;
const WALLET_LOAD_RETRY_DELAY: Duration = Duration::from_secs(3);
const DAEMON_RETRY_LOG_INTERVAL: Duration = Duration::from_secs(10);

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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum WalletLoadAcceptance {
    Loaded,
    AlreadyLoaded,
}

impl WalletLoadAcceptance {
    fn ready_message(self, source: WalletPasswordSource) -> String {
        match self {
            Self::Loaded => format!("loaded via {}", source.as_str()),
            Self::AlreadyLoaded => "already loaded".to_string(),
        }
    }

    fn wait_message(self, pending: WalletReadinessPendingKind) -> &'static str {
        match (self, pending) {
            (Self::Loaded, WalletReadinessPendingKind::NotLoaded) => {
                "wallet/load succeeded, but wallet is not ready yet; waiting for daemon wallet load to finish"
            }
            (Self::Loaded, WalletReadinessPendingKind::Timeout) => {
                "wallet/load succeeded, but wallet readiness check timed out; waiting for daemon wallet load to finish"
            }
            (Self::Loaded, WalletReadinessPendingKind::Transient) => {
                "wallet/load succeeded, but wallet readiness check is temporarily unavailable; waiting for daemon wallet load to finish"
            }
            (Self::AlreadyLoaded, WalletReadinessPendingKind::NotLoaded) => {
                "wallet/load reports already loaded, but wallet is not ready yet; waiting for daemon wallet load to finish"
            }
            (Self::AlreadyLoaded, WalletReadinessPendingKind::Timeout) => {
                "wallet/load reports already loaded, but wallet readiness check timed out; waiting for daemon wallet load to finish"
            }
            (Self::AlreadyLoaded, WalletReadinessPendingKind::Transient) => {
                "wallet/load reports already loaded, but wallet readiness check is temporarily unavailable; waiting for daemon wallet load to finish"
            }
        }
    }

    fn readiness_error_context(self, source: WalletPasswordSource) -> String {
        match self {
            Self::Loaded => format!(
                "wallet readiness check failed after wallet/load succeeded via {}",
                source.as_str()
            ),
            Self::AlreadyLoaded => {
                "wallet readiness check failed after wallet/load reported already loaded"
                    .to_string()
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum WalletReadiness {
    Ready,
    Pending(WalletReadinessPendingKind),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum WalletReadinessPendingKind {
    NotLoaded,
    Timeout,
    Transient,
}

pub(super) fn auto_load_wallet(
    client: &ApiClient,
    cfg: &Config,
    shutdown: &AtomicBool,
    tui: &mut Option<TuiDisplay>,
) -> Result<bool> {
    let (mut password, source) = match resolve_wallet_password(cfg)? {
        Some((password, source)) => (password, source),
        None => {
            if !cfg.allow_wallet_prompt {
                return Ok(false);
            }
            let Some(password) = prompt_wallet_password(tui)? else {
                return Ok(false);
            };
            (password, WalletPasswordSource::Prompt)
        }
    };
    let mut prompt_attempt = 1u32;
    let mut last_transient_log_at: Option<Instant> = None;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(false);
        }

        match client.load_wallet(&password) {
            Ok(()) => {
                let result = wait_for_wallet_ready(
                    client,
                    source,
                    WalletLoadAcceptance::Loaded,
                    shutdown,
                    tui,
                    &mut last_transient_log_at,
                );
                password.clear();
                return result;
            }
            Err(err) if is_wallet_already_loaded_error(&err) => {
                let result = wait_for_wallet_ready(
                    client,
                    source,
                    WalletLoadAcceptance::AlreadyLoaded,
                    shutdown,
                    tui,
                    &mut last_transient_log_at,
                );
                password.clear();
                return result;
            }
            Err(err) if is_wallet_wrong_password_error(&err) => {
                set_tui_state_label(tui, "wallet-password-required");
                if source == WalletPasswordSource::Prompt && prompt_attempt < MAX_PROMPT_ATTEMPTS {
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
                password.clear();
                return Err(err).with_context(|| {
                    format!("wallet password was rejected using {}", source.as_str())
                });
            }
            Err(err) if is_retryable_api_error(&err) => {
                set_tui_state_label(tui, "daemon-syncing");
                let now = Instant::now();
                if last_transient_log_at.is_none_or(|last| {
                    now.saturating_duration_since(last) >= DAEMON_RETRY_LOG_INTERVAL
                }) {
                    if is_timeout_api_error(&err) {
                        warn(
                            "DAEMON",
                            "wallet/load timed out; daemon may still be syncing or stalled, retrying with cached wallet password",
                        );
                    } else {
                        warn(
                            "DAEMON",
                            "wallet/load temporarily unavailable; daemon may still be syncing or stalled, retrying with cached wallet password",
                        );
                    }
                    last_transient_log_at = Some(now);
                }
                render_tui_now(tui);
                if !sleep_with_shutdown(shutdown, WALLET_LOAD_RETRY_DELAY) {
                    password.clear();
                    return Ok(false);
                }
                continue;
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

fn wait_for_wallet_ready(
    client: &ApiClient,
    source: WalletPasswordSource,
    acceptance: WalletLoadAcceptance,
    shutdown: &AtomicBool,
    tui: &mut Option<TuiDisplay>,
    last_transient_log_at: &mut Option<Instant>,
) -> Result<bool> {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(false);
        }

        match probe_wallet_readiness(client) {
            Ok(WalletReadiness::Ready) => {
                let message = acceptance.ready_message(source);
                match acceptance {
                    WalletLoadAcceptance::Loaded => success("WALLET", message),
                    WalletLoadAcceptance::AlreadyLoaded => info("WALLET", message),
                }
                return Ok(true);
            }
            Ok(WalletReadiness::Pending(pending)) => {
                set_tui_state_label(tui, "daemon-syncing");
                maybe_log_wallet_readiness_wait(acceptance, pending, last_transient_log_at);
                render_tui_now(tui);
                if !sleep_with_shutdown(shutdown, WALLET_LOAD_RETRY_DELAY) {
                    return Ok(false);
                }
            }
            Err(err) => return Err(err).context(acceptance.readiness_error_context(source)),
        }
    }
}

fn probe_wallet_readiness(client: &ApiClient) -> Result<WalletReadiness> {
    match client.get_wallet_address() {
        Ok(_) => Ok(WalletReadiness::Ready),
        Err(err) if is_no_wallet_loaded_error(&err) => Ok(WalletReadiness::Pending(
            WalletReadinessPendingKind::NotLoaded,
        )),
        Err(err) if is_timeout_api_error(&err) => Ok(WalletReadiness::Pending(
            WalletReadinessPendingKind::Timeout,
        )),
        Err(err) if is_retryable_api_error(&err) => Ok(WalletReadiness::Pending(
            WalletReadinessPendingKind::Transient,
        )),
        Err(err) => Err(err),
    }
}

fn maybe_log_wallet_readiness_wait(
    acceptance: WalletLoadAcceptance,
    pending: WalletReadinessPendingKind,
    last_transient_log_at: &mut Option<Instant>,
) {
    let now = Instant::now();
    if last_transient_log_at
        .is_none_or(|last| now.saturating_duration_since(last) >= DAEMON_RETRY_LOG_INTERVAL)
    {
        warn("DAEMON", acceptance.wait_message(pending));
        *last_transient_log_at = Some(now);
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

fn sleep_with_shutdown(shutdown: &AtomicBool, duration: Duration) -> bool {
    let deadline = Instant::now() + duration;
    while !shutdown.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        let sleep_for = deadline
            .saturating_duration_since(now)
            .min(Duration::from_millis(100));
        std::thread::sleep(sleep_for);
    }
    false
}

fn prompt_wallet_password(tui: &mut Option<TuiDisplay>) -> Result<Option<String>> {
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return Ok(None);
    }

    let _prompt_guard = begin_prompt_session();
    let raw_mode = is_raw_mode_enabled().unwrap_or(false);

    set_tui_state_label(tui, "wallet-password-required");
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

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use serde_json::json;

    fn test_client(server: &MockServer) -> ApiClient {
        let base = server.url("").trim_end_matches('/').to_string();
        ApiClient::new(
            base,
            "testtoken".to_string(),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .expect("test client should be created")
    }

    #[test]
    fn probe_wallet_readiness_reports_ready_when_wallet_address_is_available() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/api/wallet/address")
                .header("authorization", "Bearer testtoken");
            then.status(200).json_body(json!({
                "address": "Pwallet123"
            }));
        });

        let readiness =
            probe_wallet_readiness(&test_client(&server)).expect("wallet should be ready");
        assert_eq!(readiness, WalletReadiness::Ready);
        mock.assert();
    }

    #[test]
    fn probe_wallet_readiness_reports_pending_when_wallet_is_not_ready() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/api/wallet/address")
                .header("authorization", "Bearer testtoken");
            then.status(503)
                .json_body(json!({"error": "no wallet loaded"}));
        });

        let readiness = probe_wallet_readiness(&test_client(&server))
            .expect("wallet readiness probe should classify pending state");
        assert_eq!(
            readiness,
            WalletReadiness::Pending(WalletReadinessPendingKind::NotLoaded)
        );
        mock.assert();
    }

    #[test]
    fn probe_wallet_readiness_reports_timeout_pending() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/api/wallet/address")
                .header("authorization", "Bearer testtoken");
            then.status(408).json_body(json!({"error": "timeout"}));
        });

        let readiness = probe_wallet_readiness(&test_client(&server))
            .expect("wallet readiness probe should classify timeouts as pending");
        assert_eq!(
            readiness,
            WalletReadiness::Pending(WalletReadinessPendingKind::Timeout)
        );
        mock.assert();
    }

    #[test]
    fn probe_wallet_readiness_reports_transient_pending() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/api/wallet/address")
                .header("authorization", "Bearer testtoken");
            then.status(503)
                .json_body(json!({"error": "daemon syncing"}));
        });

        let readiness = probe_wallet_readiness(&test_client(&server))
            .expect("wallet readiness probe should classify transient errors as pending");
        assert_eq!(
            readiness,
            WalletReadiness::Pending(WalletReadinessPendingKind::Transient)
        );
        mock.assert();
    }
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
