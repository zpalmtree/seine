use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use argon2::{Algorithm, Argon2, Block, Version};
use blocknet_pow_spec::POW_OUTPUT_LEN;

use crate::backend::{BackendEvent, MiningSolution};
use crate::types::hash_meets_target;

use super::{
    emit_error, emit_event, flush_hashes, lane_quota_for_chunk, mark_startup_failed,
    mark_worker_active, mark_worker_inactive, mark_worker_ready, pow_params, request_shutdown,
    request_work_pause, should_flush_hashes, wait_for_work_update, Shared,
    MAX_DEADLINE_CHECK_INTERVAL, SOLVED_MASK,
};

pub(super) fn cpu_worker_loop(
    shared: Arc<Shared>,
    thread_idx: usize,
    core_id: Option<core_affinity::CoreId>,
) {
    if let Some(core_id) = core_id {
        let _ = core_affinity::set_for_current(core_id);
    }

    let params = match pow_params() {
        Ok(p) => p,
        Err(_) => {
            mark_startup_failed(&shared);
            emit_error(
                &shared,
                format!("cpu thread {thread_idx}: invalid Argon2 params"),
            );
            request_shutdown(&shared);
            return;
        }
    };

    let mut memory_blocks = vec![Block::default(); params.block_count()];
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut output = [0u8; POW_OUTPUT_LEN];
    mark_worker_ready(&shared);
    let mut local_generation = 0u64;
    let mut local_work = None;
    let mut worker_active = false;
    let mut nonce = 0u64;
    let mut lane_iters = 0u64;
    let lane_stride = shared.hash_slots.len().max(1) as u64;
    let mut lane_quota = 0u64;
    let mut pending_hashes = 0u64;
    let mut next_flush_at = Instant::now() + shared.hash_flush_interval;
    let mut control_hashes_remaining = 0u64;
    let mut next_deadline_check_at = Instant::now();

    loop {
        let global_generation = shared.work_generation.load(Ordering::Acquire);
        if global_generation != local_generation || local_work.is_none() {
            if local_work.is_some() {
                flush_hashes(&shared, thread_idx, &mut pending_hashes);
                mark_worker_inactive(&shared, &mut worker_active);
            }
            match wait_for_work_update(&shared, local_generation) {
                Ok(Some((generation, work))) => {
                    nonce = work.nonce_chunk.start_nonce.wrapping_add(thread_idx as u64);
                    lane_iters = 0;
                    lane_quota = lane_quota_for_chunk(
                        work.nonce_chunk.nonce_count,
                        thread_idx as u64,
                        lane_stride,
                    );
                    next_flush_at = Instant::now() + shared.hash_flush_interval;
                    control_hashes_remaining = 0;
                    next_deadline_check_at = Instant::now();
                    local_generation = generation;
                    local_work = Some(work);
                    if lane_quota > 0 {
                        mark_worker_active(&shared, &mut worker_active);
                    } else {
                        local_work = None;
                    }
                    continue;
                }
                Ok(None) => break,
                Err(err) => {
                    emit_error(
                        &shared,
                        format!("cpu thread {thread_idx}: control wait failed ({err})"),
                    );
                    request_shutdown(&shared);
                    break;
                }
            }
        }

        let Some(work) = local_work.as_ref() else {
            continue;
        };
        let template = &work.template;

        if lane_iters >= lane_quota {
            flush_hashes(&shared, thread_idx, &mut pending_hashes);
            mark_worker_inactive(&shared, &mut worker_active);
            local_work = None;
            continue;
        }

        if shared.solution_state.load(Ordering::Acquire) != template.work_id {
            flush_hashes(&shared, thread_idx, &mut pending_hashes);
            mark_worker_inactive(&shared, &mut worker_active);
            local_work = None;
            continue;
        }

        let now = Instant::now();
        let deadline_check_due = now >= next_deadline_check_at;
        if control_hashes_remaining == 0 || deadline_check_due {
            if now >= template.stop_at {
                flush_hashes(&shared, thread_idx, &mut pending_hashes);
                mark_worker_inactive(&shared, &mut worker_active);
                local_work = None;
                continue;
            }

            if control_hashes_remaining == 0 {
                control_hashes_remaining = lane_quota
                    .saturating_sub(lane_iters)
                    .clamp(1, shared.control_check_interval_hashes.max(1));
            }
            if deadline_check_due {
                next_deadline_check_at = now + MAX_DEADLINE_CHECK_INTERVAL;
            }
        }

        let nonce_bytes = nonce.to_le_bytes();
        if argon2
            .hash_password_into_with_memory(
                &nonce_bytes,
                &template.header_base,
                &mut output,
                &mut memory_blocks,
            )
            .is_err()
        {
            emit_error(
                &shared,
                format!("cpu thread {thread_idx}: hash_password_into_with_memory failed"),
            );
            request_shutdown(&shared);
            break;
        }

        lane_iters = lane_iters.saturating_add(1);
        control_hashes_remaining = control_hashes_remaining.saturating_sub(1);
        pending_hashes = pending_hashes.saturating_add(1);

        let now = Instant::now();
        let should_flush =
            should_flush_hashes(pending_hashes, now, next_flush_at, shared.hash_batch_size);
        if should_flush {
            flush_hashes(&shared, thread_idx, &mut pending_hashes);
            next_flush_at = now + shared.hash_flush_interval;
        }

        if hash_meets_target(&output, &template.target) {
            let solved_state = SOLVED_MASK | template.work_id;
            if shared
                .solution_state
                .compare_exchange(
                    template.work_id,
                    solved_state,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                if let Err(err) = request_work_pause(&shared) {
                    emit_error(
                        &shared,
                        format!(
                            "cpu thread {thread_idx}: failed to pause workers after solution ({err})"
                        ),
                    );
                    request_shutdown(&shared);
                    break;
                }
                emit_event(
                    &shared,
                    BackendEvent::Solution(MiningSolution {
                        epoch: template.epoch,
                        nonce,
                        backend_id: shared.instance_id.load(Ordering::Acquire),
                        backend: "cpu",
                    }),
                );
                flush_hashes(&shared, thread_idx, &mut pending_hashes);
                mark_worker_inactive(&shared, &mut worker_active);
                local_work = None;
                continue;
            }
        }

        nonce = nonce.wrapping_add(lane_stride);
    }

    flush_hashes(&shared, thread_idx, &mut pending_hashes);
    mark_worker_inactive(&shared, &mut worker_active);
}
