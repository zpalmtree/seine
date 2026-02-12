use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TrySendError};

use crate::api::{is_unauthorized_error, ApiClient};
use crate::config::Config;
use crate::types::BlockTemplateResponse;

use super::auth::{refresh_api_token_from_cookie, TokenRefreshOutcome};
use super::ui::error;

pub(super) struct TemplatePrefetch {
    handle: Option<JoinHandle<()>>,
    request_tx: Option<Sender<PrefetchRequest>>,
    result_rx: Receiver<PrefetchResult>,
    done_rx: Receiver<()>,
    pending_tip_sequence: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct PrefetchRequest {
    tip_sequence: u64,
}

#[derive(Debug)]
struct PrefetchResult {
    tip_sequence: u64,
    template: Option<BlockTemplateResponse>,
}

impl TemplatePrefetch {
    pub(super) fn spawn(client: ApiClient, cfg: Config, shutdown: Arc<AtomicBool>) -> Self {
        let (request_tx, request_rx) = bounded::<PrefetchRequest>(1);
        let (result_tx, result_rx) = bounded::<PrefetchResult>(1);
        let (done_tx, done_rx) = bounded::<()>(1);

        let handle = thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                let request = match request_rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(request) => request,
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                };
                let template = fetch_template_prefetch_once(&client, &cfg, shutdown.as_ref());
                if result_tx
                    .send(PrefetchResult {
                        tip_sequence: request.tip_sequence,
                        template,
                    })
                    .is_err()
                {
                    break;
                }
            }
            let _ = done_tx.send(());
        });

        Self {
            handle: Some(handle),
            request_tx: Some(request_tx),
            result_rx,
            done_rx,
            pending_tip_sequence: None,
        }
    }

    pub(super) fn pending_tip_sequence(&self) -> Option<u64> {
        self.pending_tip_sequence
    }

    pub(super) fn request_if_idle(&mut self, tip_sequence: u64) {
        if self.pending_tip_sequence.is_some() {
            return;
        }
        let Some(request_tx) = self.request_tx.as_ref() else {
            return;
        };
        match request_tx.try_send(PrefetchRequest { tip_sequence }) {
            Ok(()) => {
                self.pending_tip_sequence = Some(tip_sequence);
            }
            Err(TrySendError::Full(_)) => {
                // Worker already has a queued request; mark pending to avoid spin.
                self.pending_tip_sequence = Some(tip_sequence);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.request_tx = None;
                self.pending_tip_sequence = None;
            }
        }
    }

    pub(super) fn wait_for_result(
        &mut self,
        wait: Duration,
    ) -> Option<(u64, Option<BlockTemplateResponse>)> {
        let wait = wait.max(Duration::from_millis(1));
        match self.result_rx.recv_timeout(wait) {
            Ok(result) => {
                self.pending_tip_sequence = None;
                Some((result.tip_sequence, result.template))
            }
            Err(RecvTimeoutError::Timeout) => {
                // Release pending marker so callers can enqueue a fresher request.
                self.pending_tip_sequence = None;
                None
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.pending_tip_sequence = None;
                None
            }
        }
    }

    pub(super) fn detach(mut self) {
        self.request_tx = None;
        if let Some(handle) = self.handle.take() {
            drop(handle);
        }
    }

    pub(super) fn shutdown_for(&mut self, wait: Duration) -> bool {
        self.pending_tip_sequence = None;
        self.request_tx = None;
        let wait = wait.max(Duration::from_millis(1));
        let done = matches!(
            self.done_rx.recv_timeout(wait),
            Ok(()) | Err(RecvTimeoutError::Disconnected)
        );

        if done {
            if let Some(handle) = self.handle.take() {
                if handle.join().is_err() {
                    error("TEMPLATE", "prefetch thread panicked");
                }
            }
        } else if let Some(handle) = self.handle.take() {
            drop(handle);
        }

        done
    }
}

fn fetch_template_prefetch_once(
    client: &ApiClient,
    cfg: &Config,
    shutdown: &AtomicBool,
) -> Option<BlockTemplateResponse> {
    if shutdown.load(Ordering::Relaxed) {
        return None;
    }

    match client.get_block_template() {
        Ok(template) => Some(template),
        Err(err) if is_unauthorized_error(&err) => {
            if matches!(
                refresh_api_token_from_cookie(client, cfg.token_cookie_path.as_deref()),
                TokenRefreshOutcome::Refreshed
            ) {
                client.get_block_template().ok()
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefetch_timeout_clears_pending_marker() {
        let (request_tx, _request_rx) = bounded::<PrefetchRequest>(1);
        let (_result_tx, result_rx) = bounded::<PrefetchResult>(1);
        let (_done_tx, done_rx) = bounded::<()>(1);
        let mut prefetch = TemplatePrefetch {
            handle: None,
            request_tx: Some(request_tx),
            result_rx,
            done_rx,
            pending_tip_sequence: Some(7),
        };

        assert!(prefetch.wait_for_result(Duration::from_millis(1)).is_none());
        assert!(prefetch.pending_tip_sequence.is_none());
    }

    #[test]
    fn prefetch_full_queue_marks_request_pending() {
        let (request_tx, _request_rx) = bounded::<PrefetchRequest>(1);
        request_tx
            .try_send(PrefetchRequest { tip_sequence: 1 })
            .expect("prefill request channel should succeed");
        let (_result_tx, result_rx) = bounded::<PrefetchResult>(1);
        let (_done_tx, done_rx) = bounded::<()>(1);
        let mut prefetch = TemplatePrefetch {
            handle: None,
            request_tx: Some(request_tx),
            result_rx,
            done_rx,
            pending_tip_sequence: None,
        };

        prefetch.request_if_idle(2);
        assert_eq!(prefetch.pending_tip_sequence, Some(2));
    }
}
