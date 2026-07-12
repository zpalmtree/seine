use std::sync::atomic::Ordering;

use crossbeam_channel::{SendTimeoutError, Sender};

use crate::backend::BackendEvent;

use super::{Shared, CRITICAL_EVENT_RETRY_MAX_WAIT, CRITICAL_EVENT_RETRY_WAIT};

const MAX_EVENT_SEND_RETRIES: u32 = 10;

#[cfg(any(target_os = "linux", target_os = "windows"))]
pub(super) fn emit_warning(shared: &Shared, message: String) {
    emit_event(
        shared,
        BackendEvent::Warning {
            backend_id: shared.instance_id.load(Ordering::Acquire),
            backend: "cpu",
            message,
        },
    );
}

pub(super) fn emit_error(shared: &Shared, message: String) {
    if shared.error_emitted.swap(true, Ordering::AcqRel) {
        return;
    }
    emit_event(
        shared,
        BackendEvent::Error {
            backend_id: shared.instance_id.load(Ordering::Acquire),
            backend: "cpu",
            message,
        },
    );
}

pub(super) fn emit_event(shared: &Shared, event: BackendEvent) {
    let dispatch_tx = match shared.event_dispatch_tx.read() {
        Ok(slot) => slot.clone(),
        Err(_) => None,
    };
    let Some(dispatch_tx) = dispatch_tx else {
        shared.dropped_events.fetch_add(1, Ordering::Relaxed);
        return;
    };
    send_dispatch_event(shared, &dispatch_tx, event);
}

pub(super) fn send_dispatch_event(shared: &Shared, tx: &Sender<BackendEvent>, event: BackendEvent) {
    send_event_with_backpressure(shared, tx, event);
}

pub(super) fn forward_event(shared: &Shared, event: BackendEvent) {
    let tx = match shared.event_sink.read() {
        Ok(slot) => slot.clone(),
        Err(_) => None,
    };
    let Some(tx) = tx else {
        shared.dropped_events.fetch_add(1, Ordering::Relaxed);
        return;
    };

    match event {
        BackendEvent::Solution(solution) => {
            send_critical_event(shared, &tx, BackendEvent::Solution(solution));
        }
        BackendEvent::Warning { .. } => {
            send_event_with_backpressure(shared, &tx, event);
        }
        BackendEvent::Error {
            backend_id,
            backend,
            message,
        } => {
            send_critical_event(
                shared,
                &tx,
                BackendEvent::Error {
                    backend_id,
                    backend,
                    message,
                },
            );
        }
    }
}

fn send_critical_event(shared: &Shared, tx: &Sender<BackendEvent>, event: BackendEvent) {
    send_event_with_backpressure(shared, tx, event);
}

fn send_event_with_backpressure(shared: &Shared, tx: &Sender<BackendEvent>, event: BackendEvent) {
    let mut queued = event;
    let mut retry_wait = CRITICAL_EVENT_RETRY_WAIT;
    let mut retries = 0u32;
    loop {
        if !shared.started.load(Ordering::Acquire) {
            shared.dropped_events.fetch_add(1, Ordering::Relaxed);
            return;
        }

        match tx.send_timeout(queued, retry_wait) {
            Ok(()) => return,
            Err(SendTimeoutError::Disconnected(_)) => {
                shared.dropped_events.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(SendTimeoutError::Timeout(returned)) => {
                retries += 1;
                if retries >= MAX_EVENT_SEND_RETRIES {
                    shared.dropped_events.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                queued = returned;
                retry_wait = (retry_wait.saturating_mul(2)).min(CRITICAL_EVENT_RETRY_MAX_WAIT);
            }
        }
    }
}
