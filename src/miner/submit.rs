use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::Result;
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TrySendError};

use crate::backend::MiningSolution;
use crate::daemon_api::{is_retryable_api_error, is_unauthorized_error, ApiClient};
use crate::types::{
    set_block_nonce, template_height as extract_template_height, BlockTemplateResponse,
    SubmitBlockResponse, TemplateBlock,
};

use super::auth::{refresh_api_token_from_cookie, TokenRefreshOutcome};
use super::mining_tui::TuiDisplay;
use super::stats::Stats;
use super::ui::{error, info, success, warn};

const SUBMIT_REQUEST_CAPACITY: usize = 128;
const SUBMIT_RESULT_CAPACITY: usize = 128;
const SUBMIT_RETRY_MAX_ATTEMPTS: u32 = 4;
const SUBMIT_RETRY_BASE_DELAY: Duration = Duration::from_millis(200);
const SUBMIT_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub(super) enum SubmitTemplate {
    Compact {
        template_id: String,
        template_height: Option<u64>,
    },
    FullBlock {
        block: Arc<TemplateBlock>,
        template_height: Option<u64>,
    },
}

impl SubmitTemplate {
    pub(super) fn from_template(template: &BlockTemplateResponse) -> Self {
        let template_height = extract_template_height(&template.block);
        if let Some(template_id) = template
            .template_id
            .as_ref()
            .map(|template_id| template_id.trim())
            .filter(|template_id| !template_id.is_empty())
        {
            Self::Compact {
                template_id: template_id.to_string(),
                template_height,
            }
        } else {
            Self::FullBlock {
                block: Arc::new(template.block.clone()),
                template_height,
            }
        }
    }

    pub(super) fn template_height(&self) -> Option<u64> {
        match self {
            SubmitTemplate::Compact {
                template_height, ..
            }
            | SubmitTemplate::FullBlock {
                template_height, ..
            } => *template_height,
        }
    }
}

pub(super) struct SubmitRequest {
    pub(super) request_id: u64,
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
            SubmitTemplate::Compact { template_id, .. } => Self::Compact {
                template_id: template_id.clone(),
            },
            SubmitTemplate::FullBlock { block, .. } => Self::FullBlock {
                block: (**block).clone(),
            },
        }
    }
}

pub(super) enum SubmitOutcome {
    Response(SubmitBlockResponse),
    RetryableError(String),
    StaleHeightError {
        #[allow(dead_code)]
        message: String,
        expected_height: u64,
        got_height: u64,
    },
    StaleTipError {
        reason: &'static str,
    },
    TerminalError(String),
}

pub(super) struct SubmitResult {
    pub(super) solution: MiningSolution,
    pub(super) template_height: Option<u64>,
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
        current_tip_height: Arc<AtomicU64>,
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
                                &current_tip_height,
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
                        &current_tip_height,
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
    current_tip_height: &AtomicU64,
) -> SubmitResult {
    let max_attempts = SUBMIT_RETRY_MAX_ATTEMPTS.max(1);
    let request_id = request.request_id;
    let nonce = request.solution.nonce;
    let is_dev_fee = request.is_dev_fee;
    let solution = request.solution.clone();
    let template_height = request.template.template_height();
    let mut payload = SubmitAttemptPayload::from_request(&request);
    let mut attempts = 0u32;

    loop {
        attempts = attempts.saturating_add(1);
        match submit_request_once(client, &mut payload, nonce, request_id) {
            Ok(resp) => {
                return SubmitResult {
                    solution,
                    template_height,
                    outcome: SubmitOutcome::Response(resp),
                    attempts,
                    is_dev_fee,
                };
            }
            Err(err) => {
                if shutdown.load(Ordering::Relaxed) {
                    return SubmitResult {
                        solution,
                        template_height,
                        outcome: SubmitOutcome::TerminalError(
                            "submit aborted by shutdown".to_string(),
                        ),
                        attempts,
                        is_dev_fee,
                    };
                }

                let unauthorized = is_unauthorized_error(&err);
                let mut error_context = format!("{err:#}");
                if let Some(outcome) = stale_submit_outcome(attempts, &error_context) {
                    return SubmitResult {
                        solution,
                        template_height,
                        outcome,
                        attempts,
                        is_dev_fee,
                    };
                }
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

                if let Some(outcome) = stale_submit_outcome(attempts, &error_context) {
                    return SubmitResult {
                        solution,
                        template_height,
                        outcome,
                        attempts,
                        is_dev_fee,
                    };
                }

                if retryable && attempts < max_attempts {
                    if !sleep_with_shutdown(shutdown, submit_retry_delay(attempts)) {
                        return SubmitResult {
                            solution,
                            template_height,
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
                    template_height,
                    outcome: if retryable {
                        SubmitOutcome::RetryableError(format!(
                            "submit failed after {attempts} attempt(s): {error_context}"
                        ))
                    } else {
                        // stale_submit_outcome was already checked above (line 334)
                        // and error_context has not changed since, so skip the
                        // redundant re-parse and fall through to tip inference.
                        infer_stale_from_tip(template_height, current_tip_height).unwrap_or_else(
                            || {
                                SubmitOutcome::TerminalError(format!(
                                    "submit failed after {attempts} attempt(s): {error_context}"
                                ))
                            },
                        )
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
    request_id: u64,
) -> Result<SubmitBlockResponse> {
    match payload {
        SubmitAttemptPayload::Compact { template_id } => {
            client.submit_block(&(), Some(template_id.as_str()), nonce, request_id)
        }
        SubmitAttemptPayload::FullBlock { block } => {
            set_block_nonce(block, nonce);
            client.submit_block(block, None, nonce, request_id)
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

fn stale_submit_outcome(attempts: u32, error_context: &str) -> Option<SubmitOutcome> {
    let message = format!("submit failed after {attempts} attempt(s): {error_context}");
    if let Some((expected_height, got_height)) = parse_stale_height_error(&message) {
        return Some(SubmitOutcome::StaleHeightError {
            message,
            expected_height,
            got_height,
        });
    }
    parse_stale_tip_reject_reason(&message).map(|reason| SubmitOutcome::StaleTipError { reason })
}

/// Infer staleness when the daemon returns a generic rejection (e.g. "block rejected")
/// without detailed height info. If the solution's template height is behind the current
/// tip, we know the block is stale even though the daemon didn't say so explicitly.
fn infer_stale_from_tip(
    template_height: Option<u64>,
    current_tip_height: &AtomicU64,
) -> Option<SubmitOutcome> {
    let solution_height = template_height?;
    let tip_height = current_tip_height.load(Ordering::Acquire);
    if tip_height > 0 && solution_height < tip_height {
        Some(SubmitOutcome::StaleHeightError {
            message: format!(
                "inferred stale: solution was for height {} but tip is now at height {}",
                solution_height, tip_height
            ),
            expected_height: tip_height,
            got_height: solution_height,
        })
    } else {
        None
    }
}

fn stale_submit_summary(message: &str) -> Option<String> {
    if let Some((expected_height, got_height)) = parse_stale_height_error(message) {
        return Some(format!(
            "tip advanced before submit (expected height {}, solution was for height {})",
            expected_height, got_height
        ));
    }
    if let Some(reason) = parse_stale_tip_reject_reason(message) {
        let summary = match reason {
            "prev-hash mismatch" => "template no longer matches current tip".to_string(),
            "duplicate-or-stale" => "block already accepted elsewhere".to_string(),
            "rejected-as-stale" => "daemon rejected block as stale".to_string(),
            _ => format!("template rejected ({reason})"),
        };
        return Some(summary);
    }

    let lower = message.to_ascii_lowercase();
    if lower.contains("submitblock")
        && lower.contains("invalid block")
        && lower.contains("height")
        && lower.contains("expected")
        && lower.contains("got")
    {
        return Some("tip advanced before submit".to_string());
    }
    None
}

fn compact_submit_error(message: &str) -> String {
    message
        .split_once("attempt(s): ")
        .map(|(_, tail)| tail.to_string())
        .unwrap_or_else(|| message.to_string())
}

fn parse_stale_height_error(message: &str) -> Option<(u64, u64)> {
    let lower = message.to_ascii_lowercase();
    if !lower.contains("height") {
        return None;
    }
    let expected_height = parse_u64_after_keyword(&lower, "expected")?;
    let got_height = parse_u64_after_keyword(&lower, "got")?;
    Some((expected_height, got_height))
}

fn parse_stale_tip_reject_reason(message: &str) -> Option<&'static str> {
    let lower = message.to_ascii_lowercase();
    if lower.contains("invalid prev hash") {
        return Some("prev-hash mismatch");
    }
    if lower.contains("duplicate or stale") {
        return Some("duplicate-or-stale");
    }
    // Sanitized message from the daemon's ErrStaleBlock handler (no internal details).
    if lower.contains("rejected as stale") {
        return Some("rejected-as-stale");
    }
    None
}

fn parse_leading_u64(value: &str) -> Option<u64> {
    let digits: String = value.chars().take_while(|ch| ch.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn parse_u64_after_keyword(value: &str, keyword: &str) -> Option<u64> {
    let idx = value.find(keyword)?;
    let tail = &value[idx + keyword.len()..];
    let digit_idx = tail.find(|ch: char| ch.is_ascii_digit())?;
    parse_leading_u64(&tail[digit_idx..])
}

fn handle_submit_result(result: &SubmitResult, stats: &Stats, tui: &mut Option<TuiDisplay>) {
    let template_height = result.template_height;

    if result.is_dev_fee {
        // Do not report dev fee submissions in user-facing stats/logs.
        return;
    }
    match &result.outcome {
        SubmitOutcome::Response(resp) => {
            if resp.accepted {
                // Daemon only returns accepted=true after full block validation and chain insert.
                stats.bump_accepted();
                if let Some(display) = tui.as_mut() {
                    display.mark_block_found();
                }
                let accepted_height = resp
                    .height
                    .or(template_height)
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                success(
                    "ACCEPT",
                    format!("block accepted at height={accepted_height}"),
                );
                if result.attempts > 1 {
                    info(
                        "ACCEPT",
                        format!("accepted after {} submit attempts", result.attempts,),
                    );
                }
            } else {
                let daemon_height = resp
                    .height
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                warn(
                    "SUBMIT",
                    format!(
                        "submit rejected by daemon at height={daemon_height} (attempts={})",
                        result.attempts,
                    ),
                );
            }
        }
        SubmitOutcome::StaleHeightError {
            message: _,
            expected_height,
            got_height,
        } => {
            stats.bump_stale_shares();
            warn(
                "SUBMIT",
                format!(
                    "stale solution: chain tip advanced before submit (expected height {}, solution was for height {})",
                    expected_height, got_height
                ),
            );
        }
        SubmitOutcome::StaleTipError { reason } => {
            stats.bump_stale_shares();
            warn(
                "SUBMIT",
                format!("stale solution: template no longer matches current tip ({reason})"),
            );
        }
        SubmitOutcome::RetryableError(message) => {
            if let Some(summary) = stale_submit_summary(message) {
                stats.bump_stale_shares();
                warn("SUBMIT", format!("stale solution: {summary}"));
            } else {
                warn(
                    "SUBMIT",
                    format!(
                        "submit failed (retryable): {}",
                        compact_submit_error(message)
                    ),
                );
            }
        }
        SubmitOutcome::TerminalError(message) => {
            if let Some(summary) = stale_submit_summary(message) {
                stats.bump_stale_shares();
                warn("SUBMIT", format!("stale solution: {summary}"));
            } else {
                error(
                    "SUBMIT",
                    format!("submit failed: {}", compact_submit_error(message)),
                );
            }
        }
    }
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
    use std::sync::atomic::AtomicU64;

    use super::{
        handle_submit_result, infer_stale_from_tip, parse_stale_height_error,
        parse_stale_tip_reject_reason, stale_submit_outcome, stale_submit_summary, Stats,
        SubmitOutcome, SubmitResult,
    };

    #[test]
    fn parse_stale_height_reject_extracts_expected_and_got() {
        let message = "submit failed after 1 attempt(s): submitblock failed (500 Internal Server Error): invalid block: invalid height: expected 60, got 59";
        assert_eq!(parse_stale_height_error(message), Some((60, 59)));
    }

    #[test]
    fn parse_stale_height_reject_tolerates_variant_spacing() {
        let message = "submitblock failed: invalid block: invalid height expected 60 got 59";
        assert_eq!(parse_stale_height_error(message), Some((60, 59)));
    }

    #[test]
    fn parse_stale_height_reject_ignores_other_errors() {
        let message =
            "submit failed after 1 attempt(s): submitblock failed (400 Bad Request): invalid_pow";
        assert_eq!(parse_stale_height_error(message), None);
    }

    #[test]
    fn parse_stale_tip_reject_detects_prev_hash_mismatch() {
        let message = "submit failed after 1 attempt(s): submitblock failed (400 Bad Request): invalid block: invalid prev hash: does not link to best block";
        assert_eq!(
            parse_stale_tip_reject_reason(message),
            Some("prev-hash mismatch")
        );
    }

    #[test]
    fn parse_stale_tip_reject_detects_duplicate_or_stale() {
        let message = "submit failed after 1 attempt(s): submitblock failed (400 Bad Request): block not accepted (duplicate or stale)";
        assert_eq!(
            parse_stale_tip_reject_reason(message),
            Some("duplicate-or-stale")
        );
    }

    #[test]
    fn stale_submit_outcome_detects_stale_height() {
        let outcome = stale_submit_outcome(
            1,
            "submitblock failed (500 Internal Server Error): invalid block: invalid height: expected 60, got 59",
        );
        match outcome {
            Some(SubmitOutcome::StaleHeightError {
                expected_height,
                got_height,
                ..
            }) => {
                assert_eq!(expected_height, 60);
                assert_eq!(got_height, 59);
            }
            _ => panic!("expected stale height outcome"),
        }
    }

    #[test]
    fn stale_submit_outcome_detects_stale_tip() {
        let outcome = stale_submit_outcome(
            1,
            "submitblock failed (400 Bad Request): block not accepted (duplicate or stale)",
        );
        match outcome {
            Some(SubmitOutcome::StaleTipError { reason, .. }) => {
                assert_eq!(reason, "duplicate-or-stale");
            }
            _ => panic!("expected stale tip outcome"),
        }
    }

    #[test]
    fn stale_submit_summary_detects_stale_height_patterns() {
        let summary = stale_submit_summary(
            "submit failed after 1 attempt(s): submitblock failed (500): invalid block: invalid height expected 77 got 76",
        );
        assert!(summary.is_some());
    }

    #[test]
    fn parse_stale_tip_reject_detects_rejected_as_stale() {
        let message = "submit failed after 1 attempt(s): submitblock failed (400 Bad Request): block rejected as stale";
        assert_eq!(
            parse_stale_tip_reject_reason(message),
            Some("rejected-as-stale")
        );
    }

    #[test]
    fn stale_submit_outcome_detects_rejected_as_stale() {
        let outcome = stale_submit_outcome(
            1,
            "submitblock failed (400 Bad Request): block rejected as stale",
        );
        match outcome {
            Some(SubmitOutcome::StaleTipError { reason, .. }) => {
                assert_eq!(reason, "rejected-as-stale");
            }
            _ => panic!("expected stale tip outcome for 'rejected as stale'"),
        }
    }

    #[test]
    fn stale_submit_summary_detects_rejected_as_stale() {
        let summary = stale_submit_summary(
            "submit failed after 1 attempt(s): submitblock failed (400 Bad Request): block rejected as stale",
        );
        assert_eq!(summary.as_deref(), Some("daemon rejected block as stale"));
    }

    #[test]
    fn infer_stale_when_solution_behind_tip() {
        let tip = AtomicU64::new(61);
        let result = infer_stale_from_tip(Some(60), &tip);
        match result {
            Some(SubmitOutcome::StaleHeightError {
                expected_height,
                got_height,
                ..
            }) => {
                assert_eq!(expected_height, 61);
                assert_eq!(got_height, 60);
            }
            _ => panic!("expected inferred stale height outcome"),
        }
    }

    #[test]
    fn no_infer_stale_when_height_matches_tip() {
        let tip = AtomicU64::new(60);
        assert!(infer_stale_from_tip(Some(60), &tip).is_none());
    }

    #[test]
    fn no_infer_stale_when_tip_unknown() {
        let tip = AtomicU64::new(0);
        assert!(infer_stale_from_tip(Some(60), &tip).is_none());
    }

    #[test]
    fn no_infer_stale_when_template_height_unknown() {
        let tip = AtomicU64::new(61);
        assert!(infer_stale_from_tip(None, &tip).is_none());
    }

    #[test]
    fn dev_fee_accept_does_not_increment_user_accepted_count() {
        let stats = Stats::new();
        let mut tui = None;
        let result = SubmitResult {
            solution: crate::backend::MiningSolution {
                epoch: 1,
                nonce: 7,
                backend_id: 1,
                backend: "cpu",
            },
            template_height: Some(10),
            outcome: SubmitOutcome::Response(crate::types::SubmitBlockResponse {
                accepted: true,
                hash: None,
                height: Some(10),
            }),
            attempts: 1,
            is_dev_fee: true,
        };

        handle_submit_result(&result, &stats, &mut tui);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.accepted, 0);
    }

    #[test]
    fn stale_tip_result_increments_stale_share_count() {
        let stats = Stats::new();
        let mut tui = None;
        let result = SubmitResult {
            solution: crate::backend::MiningSolution {
                epoch: 1,
                nonce: 9,
                backend_id: 1,
                backend: "cpu",
            },
            template_height: Some(10),
            outcome: SubmitOutcome::StaleTipError {
                reason: "duplicate-or-stale",
            },
            attempts: 1,
            is_dev_fee: false,
        };

        handle_submit_result(&result, &stats, &mut tui);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.stale_shares, 1);
        assert_eq!(snapshot.accepted, 0);
    }
}
