use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender, TrySendError};

use crate::config::Config;
use crate::daemon_api::{
    is_invalid_blocktemplate_address_error, is_no_wallet_loaded_error, is_unauthorized_error,
    ApiClient,
};
use crate::types::BlockTemplateResponse;

use super::auth::{refresh_api_token_from_cookie, TokenRefreshOutcome};
use super::ui::error;

pub(super) struct TemplatePrefetch {
    handle: Option<JoinHandle<()>>,
    request_tx: Option<Sender<PrefetchRequest>>,
    result_rx: Receiver<PrefetchResult>,
    done_rx: Receiver<()>,
    inflight_tip_sequence: Option<u64>,
    desired_tip_sequence: Option<u64>,
    desired_address: Option<String>,
}

#[derive(Debug, Clone)]
struct PrefetchRequest {
    tip_sequence: u64,
    address: Option<String>,
}

#[derive(Debug)]
pub(super) enum PrefetchOutcome {
    Template(Box<BlockTemplateResponse>),
    NoWalletLoaded,
    Unauthorized,
    InvalidAddress,
    Unavailable,
}

#[derive(Debug)]
struct PrefetchResult {
    tip_sequence: u64,
    outcome: PrefetchOutcome,
}

impl TemplatePrefetch {
    pub(super) fn spawn(client: ApiClient, cfg: Config, shutdown: Arc<AtomicBool>) -> Self {
        let (request_tx, request_rx) = bounded::<PrefetchRequest>(4);
        let (result_tx, result_rx) = unbounded::<PrefetchResult>();
        let (done_tx, done_rx) = bounded::<()>(1);

        let handle = thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                let mut request = match request_rx.recv_timeout(Duration::from_secs(5)) {
                    Ok(request) => request,
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                };
                while let Ok(next) = request_rx.try_recv() {
                    if next.tip_sequence >= request.tip_sequence {
                        request = next;
                    }
                }

                let outcome = fetch_template_once(
                    &client,
                    &cfg,
                    shutdown.as_ref(),
                    request.address.as_deref(),
                );
                if result_tx
                    .send(PrefetchResult {
                        tip_sequence: request.tip_sequence,
                        outcome,
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
            inflight_tip_sequence: None,
            desired_tip_sequence: None,
            desired_address: None,
        }
    }

    fn queue_desired_request(&mut self) -> bool {
        let Some(request_tx) = self.request_tx.as_ref() else {
            return false;
        };
        let Some(tip_sequence) = self.desired_tip_sequence else {
            return false;
        };
        match request_tx.try_send(PrefetchRequest {
            tip_sequence,
            address: self.desired_address.clone(),
        }) {
            Ok(()) => {
                self.inflight_tip_sequence = Some(tip_sequence);
                true
            }
            Err(TrySendError::Full(_)) => {
                // Keep the existing inflight marker so we do not claim the newest
                // tip sequence is queued when channel backpressure rejected it.
                false
            }
            Err(TrySendError::Disconnected(_)) => {
                self.request_tx = None;
                self.inflight_tip_sequence = None;
                self.desired_tip_sequence = None;
                false
            }
        }
    }

    pub(super) fn request_if_idle(&mut self, tip_sequence: u64, address: Option<&str>) {
        let desired = self
            .desired_tip_sequence
            .map_or(tip_sequence, |current| current.max(tip_sequence));
        self.desired_tip_sequence = Some(desired);
        if self.desired_address.as_deref() != address {
            self.desired_address = address.map(str::to_string);
        }
        if self.inflight_tip_sequence.is_some() {
            return;
        }
        let _ = self.queue_desired_request();
    }

    pub(super) fn is_closed(&self) -> bool {
        self.request_tx.is_none()
    }

    pub(super) fn wait_for_result(&mut self, wait: Duration) -> Option<(u64, PrefetchOutcome)> {
        let wait = wait.max(Duration::from_millis(1));
        match self.result_rx.recv_timeout(wait) {
            Ok(mut result) => {
                while let Ok(next) = self.result_rx.try_recv() {
                    result = next;
                }
                self.inflight_tip_sequence = None;
                let desired = self.desired_tip_sequence;
                if desired.is_some_and(|pending| result.tip_sequence >= pending) {
                    self.desired_tip_sequence = None;
                } else {
                    let _ = self.queue_desired_request();
                }
                Some((result.tip_sequence, result.outcome))
            }
            Err(RecvTimeoutError::Timeout) => {
                if self.inflight_tip_sequence.is_none() {
                    let _ = self.queue_desired_request();
                }
                None
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.request_tx = None;
                self.inflight_tip_sequence = None;
                self.desired_tip_sequence = None;
                None
            }
        }
    }

    pub(super) fn detach(mut self) {
        self.inflight_tip_sequence = None;
        self.desired_tip_sequence = None;
        self.request_tx = None;
        if let Some(handle) = self.handle.take() {
            drop(handle);
        }
    }

    pub(super) fn shutdown_for(&mut self, wait: Duration) -> bool {
        self.inflight_tip_sequence = None;
        self.desired_tip_sequence = None;
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

fn prefetch_request_timeout(cfg: &Config) -> Duration {
    cfg.request_timeout.max(Duration::from_millis(200))
}

pub(super) fn fetch_template_once(
    client: &ApiClient,
    cfg: &Config,
    shutdown: &AtomicBool,
    address: Option<&str>,
) -> PrefetchOutcome {
    if shutdown.load(Ordering::Relaxed) {
        return PrefetchOutcome::Unavailable;
    }

    let timeout = prefetch_request_timeout(cfg);
    match client.get_block_template_with_timeout(timeout, address) {
        Ok(template) => PrefetchOutcome::Template(Box::new(template)),
        Err(err) if is_no_wallet_loaded_error(&err) => PrefetchOutcome::NoWalletLoaded,
        Err(err) if is_invalid_blocktemplate_address_error(&err) => PrefetchOutcome::InvalidAddress,
        Err(err) if is_unauthorized_error(&err) => {
            if matches!(
                refresh_api_token_from_cookie(client, cfg.token_cookie_path.as_deref()),
                TokenRefreshOutcome::Refreshed
            ) {
                match client.get_block_template_with_timeout(timeout, address) {
                    Ok(template) => PrefetchOutcome::Template(Box::new(template)),
                    Err(err) if is_no_wallet_loaded_error(&err) => PrefetchOutcome::NoWalletLoaded,
                    Err(err) if is_invalid_blocktemplate_address_error(&err) => {
                        PrefetchOutcome::InvalidAddress
                    }
                    Err(err) if is_unauthorized_error(&err) => PrefetchOutcome::Unauthorized,
                    Err(_) => PrefetchOutcome::Unavailable,
                }
            } else {
                PrefetchOutcome::Unauthorized
            }
        }
        Err(_) => PrefetchOutcome::Unavailable,
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefetch_timeout_keeps_inflight_marker() {
        let (request_tx, _request_rx) = bounded::<PrefetchRequest>(1);
        let (_result_tx, result_rx) = bounded::<PrefetchResult>(1);
        let (_done_tx, done_rx) = bounded::<()>(1);
        let mut prefetch = TemplatePrefetch {
            handle: None,
            request_tx: Some(request_tx),
            result_rx,
            done_rx,
            inflight_tip_sequence: Some(7),
            desired_tip_sequence: Some(7),
            desired_address: None,
        };

        assert!(prefetch.wait_for_result(Duration::from_millis(1)).is_none());
        assert_eq!(prefetch.inflight_tip_sequence, Some(7));
    }

    #[test]
    fn prefetch_disconnect_marks_worker_closed() {
        let (request_tx, _request_rx) = bounded::<PrefetchRequest>(1);
        let (result_tx, result_rx) = bounded::<PrefetchResult>(1);
        let (_done_tx, done_rx) = bounded::<()>(1);
        drop(result_tx);
        let mut prefetch = TemplatePrefetch {
            handle: None,
            request_tx: Some(request_tx),
            result_rx,
            done_rx,
            inflight_tip_sequence: Some(7),
            desired_tip_sequence: Some(7),
            desired_address: None,
        };

        assert!(prefetch.wait_for_result(Duration::from_millis(1)).is_none());
        assert!(prefetch.is_closed());
    }

    #[test]
    fn prefetch_full_queue_does_not_overstate_inflight_tip() {
        let (request_tx, _request_rx) = bounded::<PrefetchRequest>(1);
        request_tx
            .try_send(PrefetchRequest {
                tip_sequence: 1,
                address: None,
            })
            .expect("prefill request channel should succeed");
        let (_result_tx, result_rx) = unbounded::<PrefetchResult>();
        let (_done_tx, done_rx) = bounded::<()>(1);
        let mut prefetch = TemplatePrefetch {
            handle: None,
            request_tx: Some(request_tx),
            result_rx,
            done_rx,
            inflight_tip_sequence: None,
            desired_tip_sequence: None,
            desired_address: None,
        };

        prefetch.request_if_idle(2, None);
        assert_eq!(prefetch.inflight_tip_sequence, None);
    }

    #[test]
    fn prefetch_full_queue_preserves_existing_inflight_marker() {
        let (request_tx, _request_rx) = bounded::<PrefetchRequest>(1);
        request_tx
            .try_send(PrefetchRequest {
                tip_sequence: 1,
                address: None,
            })
            .expect("prefill request channel should succeed");
        let (_result_tx, result_rx) = unbounded::<PrefetchResult>();
        let (_done_tx, done_rx) = bounded::<()>(1);
        let mut prefetch = TemplatePrefetch {
            handle: None,
            request_tx: Some(request_tx),
            result_rx,
            done_rx,
            inflight_tip_sequence: Some(1),
            desired_tip_sequence: Some(1),
            desired_address: None,
        };

        prefetch.request_if_idle(2, None);
        assert_eq!(prefetch.inflight_tip_sequence, Some(1));
    }

    #[test]
    fn wait_for_result_returns_latest_available_prefetch() {
        let (request_tx, _request_rx) = bounded::<PrefetchRequest>(1);
        let (result_tx, result_rx) = unbounded::<PrefetchResult>();
        let (_done_tx, done_rx) = bounded::<()>(1);
        let mut prefetch = TemplatePrefetch {
            handle: None,
            request_tx: Some(request_tx),
            result_rx,
            done_rx,
            inflight_tip_sequence: Some(7),
            desired_tip_sequence: Some(7),
            desired_address: None,
        };

        result_tx
            .send(PrefetchResult {
                tip_sequence: 5,
                outcome: PrefetchOutcome::Unavailable,
            })
            .expect("first result should enqueue");
        result_tx
            .send(PrefetchResult {
                tip_sequence: 7,
                outcome: PrefetchOutcome::Unavailable,
            })
            .expect("second result should enqueue");

        let result = prefetch
            .wait_for_result(Duration::from_millis(1))
            .expect("a prefetched result should be returned");
        assert_eq!(result.0, 7);
        assert!(prefetch.inflight_tip_sequence.is_none());
    }

    #[test]
    fn request_if_idle_reuses_address_allocation_on_unchanged_address() {
        let (request_tx, _request_rx) = bounded::<PrefetchRequest>(4);
        let (_result_tx, result_rx) = unbounded::<PrefetchResult>();
        let (_done_tx, done_rx) = bounded::<()>(1);
        let mut prefetch = TemplatePrefetch {
            handle: None,
            request_tx: Some(request_tx),
            result_rx,
            done_rx,
            inflight_tip_sequence: None,
            desired_tip_sequence: None,
            desired_address: Some("test_addr".to_string()),
        };

        // First call sets the address
        prefetch.request_if_idle(1, Some("test_addr"));
        assert_eq!(prefetch.desired_address.as_deref(), Some("test_addr"));
        assert_eq!(prefetch.inflight_tip_sequence, Some(1));
    }
}
