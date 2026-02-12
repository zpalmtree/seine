use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use crossbeam_channel::Receiver;

use crate::backend::{BackendEvent, BackendInstanceId};

use super::backend_executor::{self, BackendTask, BackendTaskKind};
use super::ui::{error, info, warn};
use super::{
    backend_names, remove_backend_by_id, BackendSlot, RuntimeBackendEventAction, RuntimeMode,
};

type BackendFailureMap = BTreeMap<BackendInstanceId, (&'static str, Vec<String>)>;

pub(super) fn cancel_backend_slots(
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
    control_timeout: Duration,
) -> Result<RuntimeBackendEventAction> {
    control_backend_slots(backends, mode, false, control_timeout)
}

pub(super) fn quiesce_backend_slots(
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
    control_timeout: Duration,
) -> Result<RuntimeBackendEventAction> {
    control_backend_slots(backends, mode, true, control_timeout)
}

pub(super) fn handle_runtime_backend_event(
    event: BackendEvent,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
) -> Result<(
    RuntimeBackendEventAction,
    Option<crate::backend::MiningSolution>,
)> {
    match event {
        BackendEvent::Solution(solution) => {
            let backend_active = backends.iter().any(|slot| slot.id == solution.backend_id);
            if !backend_active {
                return Ok((RuntimeBackendEventAction::None, None));
            }

            match mode {
                RuntimeMode::Mining => {
                    return Ok((RuntimeBackendEventAction::None, Some(solution)));
                }
                RuntimeMode::Bench => {
                    if solution.epoch == epoch {
                        info(
                            "BENCH",
                            format!(
                                "unexpected solution from {}#{} at nonce={}",
                                solution.backend, solution.backend_id, solution.nonce
                            ),
                        );
                    }
                }
            }

            Ok((RuntimeBackendEventAction::None, None))
        }
        BackendEvent::Error {
            backend_id,
            backend,
            message,
        } => {
            match mode {
                RuntimeMode::Mining => {
                    error(
                        "BACKEND",
                        format!("{backend}#{backend_id} runtime error: {message}"),
                    );
                }
                RuntimeMode::Bench => {
                    error(
                        "BENCH",
                        format!("backend '{backend}#{backend_id}' runtime error: {message}"),
                    );
                }
            }

            let removed = remove_backend_by_id(backends, backend_id);
            if removed {
                if backends.is_empty() {
                    match mode {
                        RuntimeMode::Mining => {
                            bail!(
                                "all mining backends are unavailable after failure in '{backend}#{backend_id}'"
                            );
                        }
                        RuntimeMode::Bench => {
                            bail!(
                                "all benchmark backends are unavailable after failure in '{backend}#{backend_id}'"
                            );
                        }
                    }
                }

                match mode {
                    RuntimeMode::Mining => {
                        warn(
                            "BACKEND",
                            format!(
                                "quarantined {backend}#{backend_id}; continuing with {}",
                                backend_names(backends)
                            ),
                        );
                    }
                    RuntimeMode::Bench => {
                        warn(
                            "BENCH",
                            format!(
                                "quarantined {backend}#{backend_id}; remaining backends={}",
                                backend_names(backends)
                            ),
                        );
                    }
                }
                Ok((RuntimeBackendEventAction::TopologyChanged, None))
            } else {
                if mode == RuntimeMode::Mining {
                    warn(
                        "BACKEND",
                        format!("ignoring error from unavailable backend '{backend}#{backend_id}'"),
                    );
                }
                Ok((RuntimeBackendEventAction::None, None))
            }
        }
    }
}

pub(super) fn drain_runtime_backend_events(
    backend_events: &Receiver<BackendEvent>,
    epoch: u64,
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
) -> Result<(
    RuntimeBackendEventAction,
    Option<crate::backend::MiningSolution>,
)> {
    let mut action = RuntimeBackendEventAction::None;
    let mut solution = None;
    while let Ok(event) = backend_events.try_recv() {
        let (event_action, maybe_solution) =
            handle_runtime_backend_event(event, epoch, backends, mode)?;
        if event_action == RuntimeBackendEventAction::TopologyChanged {
            action = RuntimeBackendEventAction::TopologyChanged;
        }
        if solution.is_none() {
            solution = maybe_solution;
        }
    }
    Ok((action, solution))
}

fn control_backend_slots(
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
    include_fence: bool,
    control_timeout: Duration,
) -> Result<RuntimeBackendEventAction> {
    if backends.is_empty() {
        return Ok(RuntimeBackendEventAction::None);
    }

    let timeout = control_timeout.max(Duration::from_millis(1));
    let mut failures = BackendFailureMap::new();

    let (mut survivors, cancel_failures) = run_backend_control_phase(
        std::mem::take(backends),
        BackendControlPhase::Cancel,
        timeout,
    );
    merge_backend_failures(&mut failures, cancel_failures);

    if include_fence && !survivors.is_empty() {
        let (after_fence, fence_failures) =
            run_backend_control_phase(survivors, BackendControlPhase::Fence, timeout);
        survivors = after_fence;
        merge_backend_failures(&mut failures, fence_failures);
    }

    *backends = survivors;
    backend_executor::prune_backend_workers(backends);

    if failures.is_empty() {
        return Ok(RuntimeBackendEventAction::None);
    }

    for (backend_id, (backend, messages)) in &failures {
        let details = messages.join(" | ");
        match mode {
            RuntimeMode::Mining => {
                error(
                    "BACKEND",
                    format!("{backend}#{backend_id} control error: {details}"),
                );
            }
            RuntimeMode::Bench => {
                error(
                    "BENCH",
                    format!("backend '{backend}#{backend_id}' control error: {details}"),
                );
            }
        }
    }

    if backends.is_empty() {
        let failed = failures
            .iter()
            .map(|(backend_id, (backend, _))| format!("{backend}#{backend_id}"))
            .collect::<Vec<_>>()
            .join(",");
        match mode {
            RuntimeMode::Mining => {
                bail!("all mining backends are unavailable after control failure in {failed}");
            }
            RuntimeMode::Bench => {
                bail!("all benchmark backends are unavailable after control failure in {failed}");
            }
        }
    }

    let remaining = backend_names(backends);
    for (backend_id, (backend, _)) in &failures {
        match mode {
            RuntimeMode::Mining => {
                warn(
                    "BACKEND",
                    format!(
                        "quarantined {backend}#{backend_id} after control failure; continuing with {remaining}"
                    ),
                );
            }
            RuntimeMode::Bench => {
                warn(
                    "BENCH",
                    format!(
                        "quarantined {backend}#{backend_id} after control failure; remaining backends={remaining}"
                    ),
                );
            }
        }
    }

    Ok(RuntimeBackendEventAction::TopologyChanged)
}

#[derive(Debug, Clone, Copy)]
enum BackendControlPhase {
    Cancel,
    Fence,
}

impl BackendControlPhase {
    fn action_label(self) -> &'static str {
        match self {
            Self::Cancel => "cancel",
            Self::Fence => "fence",
        }
    }
}

fn run_backend_control_phase(
    slots: Vec<BackendSlot>,
    phase: BackendControlPhase,
    timeout: Duration,
) -> (Vec<BackendSlot>, BackendFailureMap) {
    if slots.is_empty() {
        return (Vec::new(), BackendFailureMap::new());
    }

    let timeout = timeout.max(Duration::from_millis(1));
    let expected = slots.len();
    let mut slots_by_idx: Vec<Option<BackendSlot>> = slots.into_iter().map(Some).collect();
    let backend_tasks = slots_by_idx
        .iter()
        .enumerate()
        .filter_map(|(idx, slot_opt)| {
            slot_opt.as_ref().map(|slot| BackendTask {
                idx,
                backend_id: slot.id,
                backend: slot.backend.name(),
                backend_handle: Arc::clone(&slot.backend),
                kind: match phase {
                    BackendControlPhase::Cancel => BackendTaskKind::Cancel,
                    BackendControlPhase::Fence => BackendTaskKind::Fence,
                },
            })
        })
        .collect();
    let mut outcomes = backend_executor::dispatch_backend_tasks(backend_tasks, timeout);
    if outcomes.len() < expected {
        outcomes.resize_with(expected, || None);
    }

    let mut survivors = Vec::new();
    let mut failures = BackendFailureMap::new();
    let action_label = phase.action_label();

    for (idx, outcome_slot) in outcomes.iter_mut().enumerate().take(expected) {
        let Some(slot) = slots_by_idx.get_mut(idx).and_then(Option::take) else {
            continue;
        };
        let backend_id = slot.id;
        let backend = slot.backend.name();
        match outcome_slot.take() {
            Some(outcome) => match outcome.result {
                Ok(()) => survivors.push((idx, slot)),
                Err(err) => {
                    backend_executor::quarantine_backend(Arc::clone(&slot.backend));
                    backend_executor::remove_backend_worker(backend_id);
                    failures
                        .entry(backend_id)
                        .or_insert_with(|| (backend, Vec::new()))
                        .1
                        .push(format!("{action_label} failed: {err:#}"));
                }
            },
            None => {
                backend_executor::quarantine_backend(Arc::clone(&slot.backend));
                backend_executor::remove_backend_worker(backend_id);
                failures
                    .entry(backend_id)
                    .or_insert_with(|| (backend, Vec::new()))
                    .1
                    .push(format!(
                        "{action_label} timed out after {}ms; backend quarantined",
                        timeout.as_millis()
                    ));
            }
        }
    }

    survivors.sort_by_key(|(idx, _)| *idx);
    (
        survivors
            .into_iter()
            .map(|(_, slot)| slot)
            .collect::<Vec<_>>(),
        failures,
    )
}

fn merge_backend_failures(failures: &mut BackendFailureMap, additional: BackendFailureMap) {
    for (backend_id, (backend, messages)) in additional {
        failures
            .entry(backend_id)
            .or_insert_with(|| (backend, Vec::new()))
            .1
            .extend(messages);
    }
}
