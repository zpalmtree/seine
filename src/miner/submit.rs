use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::Result;
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TrySendError};

use crate::api::{is_retryable_api_error, is_unauthorized_error, ApiClient};
use crate::backend::MiningSolution;
use crate::types::{set_block_nonce, BlockTemplateResponse, SubmitBlockResponse, TemplateBlock};

use super::auth::{refresh_api_token_from_cookie, TokenRefreshOutcome};
use super::mining_tui::TuiDisplay;
use super::stats::Stats;
use super::ui::{error, info, mined, warn};

const SUBMIT_REQUEST_CAPACITY: usize = 128;
const SUBMIT_RESULT_CAPACITY: usize = 128;
const SUBMIT_RETRY_MAX_ATTEMPTS: u32 = 4;
const SUBMIT_RETRY_BASE_DELAY: Duration = Duration::from_millis(200);
const SUBMIT_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub(super) enum SubmitTemplate {
    Compact { template_id: String },
    FullBlock { block: Arc<TemplateBlock> },
}

impl SubmitTemplate {
    pub(super) fn from_template(template: &BlockTemplateResponse) -> Self {
        if let Some(template_id) = template
            .template_id
            .as_ref()
            .map(|template_id| template_id.trim())
            .filter(|template_id| !template_id.is_empty())
        {
            Self::Compact {
                template_id: template_id.to_string(),
            }
        } else {
            Self::FullBlock {
                block: Arc::new(template.block.clone()),
            }
        }
    }
}

pub(super) struct SubmitRequest {
    pub(super) template: SubmitTemplate,
    pub(super) solution: MiningSolution,
    pub(super) is_dev_fee: bool,
}

enum SubmitAttemptPayload {
    Compact { template_id: String },
    FullBlock { block: TemplateBlock },
}

impl SubmitAttemptPayload {
    fn from_request(request: &SubmitRequest) -> Self {
        match &request.template {
            SubmitTemplate::Compact { template_id } => Self::Compact {
                template_id: template_id.clone(),
            },
            SubmitTemplate::FullBlock { block } => Self::FullBlock {
                block: (**block).clone(),
            },
        }
    }
}

pub(super) enum SubmitOutcome {
    Response(SubmitBlockResponse),
    RetryableError(String),
    StaleHeightError {
        message: String,
        expected_height: u64,
        got_height: u64,
    },
    TerminalError(String),
}

pub(super) struct SubmitResult {
    pub(super) solution: MiningSolution,
    pub(super) outcome: SubmitOutcome,
    pub(super) attempts: u32,
    pub(super) is_dev_fee: bool,
}

pub(super) struct SubmitWorker {
    handle: Option<thread::JoinHandle<()>>,
    request_tx: Option<Sender<SubmitRequest>>,
    result_rx: Receiver<SubmitResult>,
    done_rx: Receiver<()>,
}

pub(super) enum SubmitEnqueueOutcome {
    Queued,
    Full(SubmitRequest),
    Closed(SubmitRequest),
}

impl SubmitWorker {
    pub(super) fn spawn(
        client: ApiClient,
        shutdown: Arc<AtomicBool>,
        token_cookie_path: Option<PathBuf>,
    ) -> Self {
        let (request_tx, request_rx) = bounded::<SubmitRequest>(SUBMIT_REQUEST_CAPACITY);
        let (result_tx, result_rx) = bounded::<SubmitResult>(SUBMIT_RESULT_CAPACITY);
        let (done_tx, done_rx) = bounded::<()>(1);

        let handle = thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                match request_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(request) => {
                        if result_tx
                            .send(process_submit_request(
                                &client,
                                request,
                                shutdown.as_ref(),
                                token_cookie_path.as_ref(),
                            ))
                            .is_err()
                        {
                            let _ = done_tx.send(());
                            return;
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }

            while let Ok(request) = request_rx.try_recv() {
                if result_tx
                    .send(process_submit_request(
                        &client,
                        request,
                        shutdown.as_ref(),
                        token_cookie_path.as_ref(),
                    ))
                    .is_err()
                {
                    let _ = done_tx.send(());
                    return;
                }
            }

            let _ = done_tx.send(());
        });

        Self {
            handle: Some(handle),
            request_tx: Some(request_tx),
            result_rx,
            done_rx,
        }
    }

    pub(super) fn submit(&self, request: SubmitRequest) -> SubmitEnqueueOutcome {
        let Some(tx) = self.request_tx.as_ref() else {
            return SubmitEnqueueOutcome::Closed(request);
        };

        match tx.try_send(request) {
            Ok(()) => SubmitEnqueueOutcome::Queued,
            Err(TrySendError::Full(request)) => SubmitEnqueueOutcome::Full(request),
            Err(TrySendError::Disconnected(request)) => SubmitEnqueueOutcome::Closed(request),
        }
    }

    pub(super) fn drain_results(
        &self,
        stats: &Stats,
        tui: &mut Option<TuiDisplay>,
    ) -> Vec<SubmitResult> {
        let mut drained = Vec::new();
        while let Ok(result) = self.result_rx.try_recv() {
            handle_submit_result(&result, stats, tui);
            drained.push(result);
        }
        drained
    }

    pub(super) fn detach(mut self) {
        self.request_tx = None;
        if let Some(handle) = self.handle.take() {
            drop(handle);
        }
    }

    pub(super) fn shutdown_for(&mut self, wait: Duration) -> bool {
        self.request_tx = None;
        let wait = wait.max(Duration::from_millis(1));
        let done = matches!(
            self.done_rx.recv_timeout(wait),
            Ok(()) | Err(RecvTimeoutError::Disconnected)
        );

        if done {
            if let Some(handle) = self.handle.take() {
                if handle.join().is_err() {
                    error("SUBMIT", "submit worker thread panicked");
                }
            }
        } else if let Some(handle) = self.handle.take() {
            drop(handle);
        }

        done
    }
}

pub(super) fn process_submit_request(
    client: &ApiClient,
    request: SubmitRequest,
    shutdown: &AtomicBool,
    token_cookie_path: Option<&PathBuf>,
) -> SubmitResult {
    let max_attempts = SUBMIT_RETRY_MAX_ATTEMPTS.max(1);
    let nonce = request.solution.nonce;
    let is_dev_fee = request.is_dev_fee;
    let solution = request.solution.clone();
    let mut payload = SubmitAttemptPayload::from_request(&request);
    let mut attempts = 0u32;

    loop {
        attempts = attempts.saturating_add(1);
        match submit_request_once(client, &mut payload, nonce) {
            Ok(resp) => {
                return SubmitResult {
                    solution,
                    outcome: SubmitOutcome::Response(resp),
                    attempts,
                    is_dev_fee,
                };
            }
            Err(err) => {
                if shutdown.load(Ordering::Relaxed) {
                    return SubmitResult {
                        solution,
                        outcome: SubmitOutcome::TerminalError(
                            "submit aborted by shutdown".to_string(),
                        ),
                        attempts,
                        is_dev_fee,
                    };
                }

                let unauthorized = is_unauthorized_error(&err);
                let mut error_context = format!("{err:#}");
                let mut retryable = is_retryable_api_error(&err);
                if unauthorized {
                    match refresh_api_token_from_cookie(
                        client,
                        token_cookie_path.map(PathBuf::as_path),
                    ) {
                        TokenRefreshOutcome::Refreshed => {
                            retryable = true;
                            if attempts < max_attempts {
                                continue;
                            }
                            error_context =
                                "auth refreshed, but submit retry budget was exhausted".to_string();
                        }
                        TokenRefreshOutcome::Unchanged => {
                            retryable = false;
                            error_context =
                                format!("auth expired and cookie token was unchanged: {err:#}");
                        }
                        TokenRefreshOutcome::Unavailable => {
                            retryable = false;
                            error_context = format!(
                                "auth expired and no cookie refresh source is available: {err:#}"
                            );
                        }
                        TokenRefreshOutcome::Failed(msg) => {
                            retryable = true;
                            error_context = format!("{msg}: {err:#}");
                        }
                    }
                }

                if retryable && attempts < max_attempts {
                    if !sleep_with_shutdown(shutdown, submit_retry_delay(attempts)) {
                        return SubmitResult {
                            solution,
                            outcome: SubmitOutcome::TerminalError(
                                "submit aborted by shutdown".to_string(),
                            ),
                            attempts,
                            is_dev_fee,
                        };
                    }
                    continue;
                }

                return SubmitResult {
                    solution,
                    outcome: if retryable {
                        SubmitOutcome::RetryableError(format!(
                            "submit failed after {attempts} attempt(s): {error_context}"
                        ))
                    } else {
                        let message =
                            format!("submit failed after {attempts} attempt(s): {error_context}");
                        if let Some((expected_height, got_height)) =
                            parse_stale_height_error(&message)
                        {
                            SubmitOutcome::StaleHeightError {
                                message,
                                expected_height,
                                got_height,
                            }
                        } else {
                            SubmitOutcome::TerminalError(message)
                        }
                    },
                    attempts,
                    is_dev_fee,
                };
            }
        }
    }
}

fn submit_request_once(
    client: &ApiClient,
    payload: &mut SubmitAttemptPayload,
    nonce: u64,
) -> Result<SubmitBlockResponse> {
    match payload {
        SubmitAttemptPayload::Compact { template_id } => {
            client.submit_block(&(), Some(template_id.as_str()), nonce)
        }
        SubmitAttemptPayload::FullBlock { block } => {
            set_block_nonce(block, nonce);
            client.submit_block(block, None, nonce)
        }
    }
}

fn submit_retry_delay(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(8);
    let multiplier = 1u32 << shift;
    SUBMIT_RETRY_BASE_DELAY
        .saturating_mul(multiplier)
        .min(SUBMIT_RETRY_MAX_DELAY)
}

fn parse_stale_height_error(message: &str) -> Option<(u64, u64)> {
    let lower = message.to_ascii_lowercase();
    let marker = "invalid height: expected ";
    let marker_idx = lower.find(marker)?;
    let after_marker = &lower[marker_idx + marker.len()..];
    let (expected_part, got_part) = after_marker.split_once(", got ")?;
    let expected_height = parse_leading_u64(expected_part.trim())?;
    let got_height = parse_leading_u64(got_part.trim())?;
    Some((expected_height, got_height))
}

fn parse_leading_u64(value: &str) -> Option<u64> {
    let digits: String = value.chars().take_while(|ch| ch.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn handle_submit_result(result: &SubmitResult, stats: &Stats, tui: &mut Option<TuiDisplay>) {
    if result.is_dev_fee {
        // Still track stats, but don't log anything for dev fee submissions.
        if let SubmitOutcome::Response(resp) = &result.outcome {
            if resp.accepted {
                stats.bump_accepted();
            }
        }
        return;
    }
    match &result.outcome {
        SubmitOutcome::Response(resp) => {
            if resp.accepted {
                stats.bump_accepted();
                if let Some(display) = tui.as_mut() {
                    display.mark_block_found();
                }
                let height = resp
                    .height
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let hash = resp.hash.as_deref().unwrap_or("unknown").to_string();
                mined("SUBMIT", format!("block accepted at height {height}"));
                mined("SUBMIT", format!("hash {}", compact_hash(&hash)));
                if result.attempts > 1 {
                    info(
                        "SUBMIT",
                        format!(
                            "accepted epoch={} nonce={} after {} submit attempts",
                            result.solution.epoch, result.solution.nonce, result.attempts
                        ),
                    );
                }
            } else {
                warn(
                    "SUBMIT",
                    format!(
                        "rejected by daemon epoch={} nonce={} backend={}#{} attempts={}",
                        result.solution.epoch,
                        result.solution.nonce,
                        result.solution.backend,
                        result.solution.backend_id,
                        result.attempts
                    ),
                );
            }
        }
        SubmitOutcome::StaleHeightError {
            message: _,
            expected_height,
            got_height,
        } => {
            warn(
                "SUBMIT",
                format!(
                    "stale solution rejected epoch={} nonce={} backend={}#{}: daemon advanced tip (expected {}, got {})",
                    result.solution.epoch,
                    result.solution.nonce,
                    result.solution.backend,
                    result.solution.backend_id,
                    expected_height,
                    got_height
                ),
            );
        }
        SubmitOutcome::RetryableError(message) => {
            warn(
                "SUBMIT",
                format!(
                    "submit failed (retryable) epoch={} nonce={} backend={}#{} attempts={}: {}",
                    result.solution.epoch,
                    result.solution.nonce,
                    result.solution.backend,
                    result.solution.backend_id,
                    result.attempts,
                    message
                ),
            );
        }
        SubmitOutcome::TerminalError(message) => {
            error(
                "SUBMIT",
                format!(
                    "submit failed epoch={} nonce={} backend={}#{} attempts={}: {}",
                    result.solution.epoch,
                    result.solution.nonce,
                    result.solution.backend,
                    result.solution.backend_id,
                    result.attempts,
                    message
                ),
            );
        }
    }
}

fn compact_hash(hash: &str) -> String {
    let value = hash.trim();
    let len = value.chars().count();
    if len <= 8 {
        return value.to_string();
    }

    let prefix: String = value.chars().take(4).collect();
    let suffix: String = value
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{prefix}...{suffix}")
}

fn sleep_with_shutdown(shutdown: &AtomicBool, duration: Duration) -> bool {
    let deadline = std::time::Instant::now() + duration;
    while !shutdown.load(Ordering::Relaxed) {
        let now = std::time::Instant::now();
        if now >= deadline {
            return true;
        }
        let sleep_for = deadline
            .saturating_duration_since(now)
            .min(Duration::from_millis(100));
        thread::sleep(sleep_for);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::parse_stale_height_error;

    #[test]
    fn parse_stale_height_reject_extracts_expected_and_got() {
        let message = "submit failed after 1 attempt(s): submitblock failed (500 Internal Server Error): invalid block: invalid height: expected 60, got 59";
        assert_eq!(parse_stale_height_error(message), Some((60, 59)));
    }

    #[test]
    fn parse_stale_height_reject_ignores_other_errors() {
        let message =
            "submit failed after 1 attempt(s): submitblock failed (400 Bad Request): invalid_pow";
        assert_eq!(parse_stale_height_error(message), None);
    }
}
