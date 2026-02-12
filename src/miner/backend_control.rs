use std::collections::BTreeMap;

use anyhow::{bail, Result};
use crossbeam_channel::Receiver;

use crate::backend::{BackendEvent, BackendInstanceId};

use super::ui::{error, info, warn};
use super::{
    backend_names, remove_backend_by_id, BackendSlot, RuntimeBackendEventAction, RuntimeMode,
};

pub(super) fn cancel_backend_slots(
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
) -> Result<RuntimeBackendEventAction> {
    control_backend_slots(backends, mode, false)
}

pub(super) fn quiesce_backend_slots(
    backends: &mut Vec<BackendSlot>,
    mode: RuntimeMode,
) -> Result<RuntimeBackendEventAction> {
    control_backend_slots(backends, mode, true)
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
            if solution.epoch == epoch && backend_active {
                match mode {
                    RuntimeMode::Mining => {
                        return Ok((RuntimeBackendEventAction::None, Some(solution)));
                    }
                    RuntimeMode::Bench => {
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
) -> Result<RuntimeBackendEventAction> {
    if backends.is_empty() {
        return Ok(RuntimeBackendEventAction::None);
    }

    let mut failures = collect_backend_control_failures(backends, BackendControlPhase::Cancel);
    if include_fence {
        let fence_failures = collect_backend_control_failures(backends, BackendControlPhase::Fence);
        merge_backend_failures(&mut failures, fence_failures);
    }

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

    for backend_id in failures.keys().copied().collect::<Vec<_>>() {
        let removed = remove_backend_by_id(backends, backend_id);
        if !removed {
            continue;
        }

        let remaining = backend_names(backends);
        let backend = failures
            .get(&backend_id)
            .map(|(name, _)| *name)
            .unwrap_or("unknown");
        match mode {
            RuntimeMode::Mining => warn(
                "BACKEND",
                format!(
                    "quarantined {backend}#{backend_id} after control failure; continuing with {remaining}"
                ),
            ),
            RuntimeMode::Bench => warn(
                "BENCH",
                format!(
                    "quarantined {backend}#{backend_id} after control failure; remaining backends={remaining}"
                ),
            ),
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

    Ok(RuntimeBackendEventAction::TopologyChanged)
}

#[derive(Debug, Clone, Copy)]
enum BackendControlPhase {
    Cancel,
    Fence,
}

fn collect_backend_control_failures(
    backends: &mut [BackendSlot],
    phase: BackendControlPhase,
) -> BTreeMap<BackendInstanceId, (&'static str, Vec<String>)> {
    let mut failures: BTreeMap<BackendInstanceId, (&'static str, Vec<String>)> = BTreeMap::new();

    for slot in backends {
        let result = match phase {
            BackendControlPhase::Cancel => slot.backend.cancel_work(),
            BackendControlPhase::Fence => slot.backend.fence(),
        };

        if let Err(err) = result {
            let action = match phase {
                BackendControlPhase::Cancel => "cancel",
                BackendControlPhase::Fence => "fence",
            };
            failures
                .entry(slot.id)
                .or_insert_with(|| (slot.backend.name(), Vec::new()))
                .1
                .push(format!("{action} failed: {err:#}"));
        }
    }

    failures
}

fn merge_backend_failures(
    failures: &mut BTreeMap<BackendInstanceId, (&'static str, Vec<String>)>,
    additional: BTreeMap<BackendInstanceId, (&'static str, Vec<String>)>,
) {
    for (backend_id, (backend, messages)) in additional {
        failures
            .entry(backend_id)
            .or_insert_with(|| (backend, Vec::new()))
            .1
            .extend(messages);
    }
}
