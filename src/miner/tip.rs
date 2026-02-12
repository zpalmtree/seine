use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde_json::Value;

use crate::api::{is_unauthorized_error, ApiClient};
use crate::config::read_token_from_cookie_file;

use super::ui::{success, warn};

const RETRY_LOG_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(super) struct TipSignal {
    stale: Arc<AtomicBool>,
    current_template_height: Arc<AtomicU64>,
    last_new_block: Arc<Mutex<Option<LastNewBlock>>>,
    sequence: Arc<AtomicU64>,
    refresh_on_same_height: bool,
}

struct LastNewBlock {
    hash: String,
    height: Option<u64>,
}

impl TipSignal {
    pub(super) fn new(refresh_on_same_height: bool) -> Self {
        Self {
            stale: Arc::new(AtomicBool::new(false)),
            current_template_height: Arc::new(AtomicU64::new(0)),
            last_new_block: Arc::new(Mutex::new(None)),
            sequence: Arc::new(AtomicU64::new(0)),
            refresh_on_same_height,
        }
    }

    pub(super) fn take_stale(&self) -> bool {
        self.stale.swap(false, Ordering::AcqRel)
    }

    pub(super) fn snapshot_sequence(&self) -> u64 {
        self.sequence.load(Ordering::Acquire)
    }

    pub(super) fn set_current_template_height(&self, height: u64) {
        self.current_template_height
            .store(height, Ordering::Release);
        let should_clear_stale = if let Ok(last_event) = self.last_new_block.lock() {
            matches!(
                last_event.as_ref(),
                Some(last) if last.height.is_some_and(|h| h.saturating_add(1) < height)
            )
        } else {
            false
        };
        if should_clear_stale {
            self.stale.store(false, Ordering::Release);
        }
    }

    fn mark_stale_for_new_block(&self, hash: &str, event_height: Option<u64>) {
        if let Some(height) = event_height {
            let template_height = self.current_template_height.load(Ordering::Acquire);
            if template_height != 0 && height.saturating_add(1) < template_height {
                return;
            }
        }

        let mut changed = false;
        if let Ok(mut last_event) = self.last_new_block.lock() {
            let same_height = matches!(
                (last_event.as_ref().and_then(|last| last.height), event_height),
                (Some(last_height), Some(height)) if last_height == height
            );
            if same_height {
                // Daemon-side event streams can replay competing hashes at the same height.
                // Coalesce by default, but allow forcing refresh for same-height hash changes.
                let hash_changed = last_event
                    .as_ref()
                    .map(|last| last.hash != hash)
                    .unwrap_or(false);
                *last_event = Some(LastNewBlock {
                    hash: hash.to_string(),
                    height: event_height,
                });
                changed = self.refresh_on_same_height && hash_changed;
            } else if !matches!(
                last_event.as_ref(),
                Some(last) if last.hash == hash && last.height == event_height
            ) {
                *last_event = Some(LastNewBlock {
                    hash: hash.to_string(),
                    height: event_height,
                });
                changed = true;
            }
        } else {
            // If lock state is poisoned, err on the side of refreshing work.
            changed = true;
        }
        if changed {
            self.stale.store(true, Ordering::Release);
            self.sequence.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn mark_stale_on_unparsed_event(&self) {
        self.stale.store(true, Ordering::Release);
        self.sequence.fetch_add(1, Ordering::AcqRel);
    }
}

pub(super) struct TipListener {
    signal: TipSignal,
    handle: Option<JoinHandle<()>>,
}

impl TipListener {
    pub(super) fn signal(&self) -> &TipSignal {
        &self.signal
    }

    pub(super) fn detach(mut self) {
        if let Some(handle) = self.handle.take() {
            drop(handle);
        }
    }

    pub(super) fn join(mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Default)]
struct RetryTracker {
    failures: u64,
    last_log_at: Option<Instant>,
    outage_logged: bool,
}

impl RetryTracker {
    fn note_failure(&mut self, tag: &str, first: &str, repeat: &str, immediate_first: bool) {
        let now = Instant::now();
        self.failures = self.failures.saturating_add(1);

        let should_log = if self.failures == 1 {
            immediate_first
        } else {
            self.last_log_at
                .is_none_or(|last| now.saturating_duration_since(last) >= RETRY_LOG_INTERVAL)
        };

        if should_log {
            let message = if self.failures == 1 { first } else { repeat };
            warn(tag, message);
            self.last_log_at = Some(now);
            self.outage_logged = true;
        }
    }

    fn note_recovered(&mut self, tag: &str, message: &str) {
        if self.failures == 0 {
            return;
        }
        if !self.outage_logged {
            *self = Self::default();
            return;
        }
        success(tag, message);
        *self = Self::default();
    }
}

enum TokenRefreshOutcome {
    Refreshed,
    Unchanged,
    Unavailable,
    Failed(String),
}

pub(super) fn spawn_tip_listener(
    client: ApiClient,
    shutdown: Arc<AtomicBool>,
    refresh_on_same_height: bool,
    token_cookie_path: Option<PathBuf>,
) -> TipListener {
    let tip_signal = TipSignal::new(refresh_on_same_height);
    let signal = tip_signal.clone();

    let handle = thread::spawn(move || {
        let mut retry = RetryTracker::default();
        while !shutdown.load(Ordering::Relaxed) {
            match client.open_events_stream() {
                Ok(resp) => {
                    retry.note_recovered("EVENTS", "events stream reconnected");
                    if let Err(_err) = stream_tip_events(resp, &signal, &shutdown) {
                        if !shutdown.load(Ordering::Relaxed) {
                            retry.note_failure(
                                "EVENTS",
                                "failed to fetch events; reconnecting",
                                "still failing to fetch events; reconnecting",
                                false,
                            );
                        }
                    }
                }
                Err(err) => {
                    if !shutdown.load(Ordering::Relaxed) {
                        if is_unauthorized_error(&err) {
                            match refresh_api_token_from_cookie(
                                &client,
                                token_cookie_path.as_deref(),
                            ) {
                                TokenRefreshOutcome::Refreshed => {
                                    success("AUTH", "auth refreshed from cookie");
                                    continue;
                                }
                                TokenRefreshOutcome::Unchanged => {
                                    retry.note_failure(
                                        "AUTH",
                                        "auth expired; waiting for new cookie token",
                                        "auth still expired; waiting for new cookie token",
                                        true,
                                    );
                                }
                                TokenRefreshOutcome::Unavailable => {
                                    retry.note_failure(
                                        "AUTH",
                                        "auth failed; static --token cannot auto-refresh",
                                        "still waiting for manual token refresh",
                                        true,
                                    );
                                }
                                TokenRefreshOutcome::Failed(msg) => {
                                    retry.note_failure(
                                        "AUTH",
                                        &msg,
                                        "failed to refresh auth token from cookie",
                                        true,
                                    );
                                }
                            }
                        }

                        retry.note_failure(
                            "EVENTS",
                            "failed to fetch events; reconnecting",
                            "still failing to fetch events; reconnecting",
                            false,
                        );
                    }
                }
            }

            if !shutdown.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(1));
            }
        }
    });

    TipListener {
        signal: tip_signal,
        handle: Some(handle),
    }
}

fn stream_tip_events(
    resp: reqwest::blocking::Response,
    signal: &TipSignal,
    shutdown: &AtomicBool,
) -> Result<()> {
    let mut frame = SseFrameState::default();
    let reader = BufReader::new(resp);

    for line_result in reader.lines() {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let line = line_result.context("failed reading SSE event stream")?;
        process_sse_line(&line, &mut frame, signal);
    }
    process_sse_frame(&frame, signal);

    if !shutdown.load(Ordering::Relaxed) {
        bail!("SSE stream closed by peer");
    }

    Ok(())
}

#[derive(Default)]
struct SseFrameState {
    event_name: String,
    data_lines: Vec<String>,
}

impl SseFrameState {
    fn reset(&mut self) {
        self.event_name.clear();
        self.data_lines.clear();
    }
}

fn process_sse_line(line: &str, frame: &mut SseFrameState, signal: &TipSignal) {
    if line.is_empty() {
        process_sse_frame(frame, signal);
        frame.reset();
        return;
    }

    if line.starts_with(':') {
        return;
    }

    let (field, raw_value) = line
        .split_once(':')
        .map_or((line, ""), |(f, rest)| (f, rest));
    let value = raw_value.strip_prefix(' ').unwrap_or(raw_value);

    match field {
        "event" => frame.event_name = value.to_string(),
        "data" => frame.data_lines.push(value.to_string()),
        // `id` and `retry` are intentionally ignored for now.
        _ => {}
    }
}

fn process_sse_frame(frame: &SseFrameState, signal: &TipSignal) {
    if frame.event_name != "new_block" || frame.data_lines.is_empty() {
        return;
    }

    let payload = frame.data_lines.join("\n");
    if let Some(event) = extract_new_block_event(&payload) {
        signal.mark_stale_for_new_block(&event.hash, event.height);
        return;
    }

    // If parsing fails, preserve old behavior and refresh once.
    signal.mark_stale_on_unparsed_event();
}

struct NewBlockEvent {
    hash: String,
    height: Option<u64>,
}

fn extract_new_block_event(payload: &str) -> Option<NewBlockEvent> {
    let value: Value = serde_json::from_str(payload).ok()?;
    let hash = value
        .get("hash")
        .and_then(Value::as_str)
        .map(str::to_string)?;
    let height = value.get("height").and_then(Value::as_u64);
    Some(NewBlockEvent { hash, height })
}

fn refresh_api_token_from_cookie(
    client: &ApiClient,
    cookie_path: Option<&Path>,
) -> TokenRefreshOutcome {
    let Some(cookie_path) = cookie_path else {
        return TokenRefreshOutcome::Unavailable;
    };

    let token = match read_token_from_cookie_file(cookie_path) {
        Ok(token) => token,
        Err(_) => return TokenRefreshOutcome::Failed("failed reading API cookie".to_string()),
    };

    match client.replace_token(token) {
        Ok(true) => TokenRefreshOutcome::Refreshed,
        Ok(false) => TokenRefreshOutcome::Unchanged,
        Err(_) => TokenRefreshOutcome::Failed("failed updating API token".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn emit_new_block(signal: &TipSignal, hash: &str, height: u64) {
        let mut frame = SseFrameState::default();
        process_sse_line("event: new_block", &mut frame, signal);
        process_sse_line(
            &format!("data: {{\"hash\":\"{hash}\",\"height\":{height}}}"),
            &mut frame,
            signal,
        );
        process_sse_line("", &mut frame, signal);
    }

    #[test]
    fn duplicate_new_block_hashes_are_coalesced() {
        let signal = TipSignal::new(false);
        emit_new_block(&signal, "abc", 1);
        assert!(signal.take_stale());

        emit_new_block(&signal, "abc", 1);
        assert!(!signal.take_stale());
    }

    #[test]
    fn historical_new_block_events_are_ignored() {
        let signal = TipSignal::new(false);
        signal.set_current_template_height(1761);

        emit_new_block(&signal, "old", 1759);
        assert!(!signal.take_stale());

        emit_new_block(&signal, "tip", 1760);
        assert!(signal.take_stale());
    }

    #[test]
    fn setting_template_height_clears_only_historical_stale_state() {
        let signal = TipSignal::new(false);

        emit_new_block(&signal, "old", 1750);
        signal.set_current_template_height(1762);
        assert!(!signal.take_stale());
    }

    #[test]
    fn new_block_hash_change_triggers_refresh() {
        let signal = TipSignal::new(false);

        emit_new_block(&signal, "abc", 1);
        let _ = signal.take_stale();

        emit_new_block(&signal, "def", 2);
        assert!(signal.take_stale());
    }

    #[test]
    fn multiline_new_block_payload_is_parsed() {
        let signal = TipSignal::new(false);
        let mut frame = SseFrameState::default();

        process_sse_line("event: new_block", &mut frame, &signal);
        process_sse_line("data: {\"hash\":\"abc\",", &mut frame, &signal);
        process_sse_line("data: \"height\":123}", &mut frame, &signal);
        process_sse_line("", &mut frame, &signal);

        assert!(signal.take_stale());
    }

    #[test]
    fn sse_comment_lines_are_ignored() {
        let signal = TipSignal::new(false);
        let mut frame = SseFrameState::default();

        process_sse_line(": ping", &mut frame, &signal);
        process_sse_line(": keepalive", &mut frame, &signal);
        process_sse_line("", &mut frame, &signal);

        assert!(!signal.take_stale());
    }

    #[test]
    fn same_height_hash_change_is_coalesced() {
        let signal = TipSignal::new(false);

        emit_new_block(&signal, "abc", 1782);
        assert!(signal.take_stale());

        emit_new_block(&signal, "def", 1782);
        assert!(!signal.take_stale());
    }

    #[test]
    fn same_height_hash_change_can_trigger_refresh_when_enabled() {
        let signal = TipSignal::new(true);

        emit_new_block(&signal, "abc", 1782);
        assert!(signal.take_stale());

        emit_new_block(&signal, "def", 1782);
        assert!(signal.take_stale());
    }
}
