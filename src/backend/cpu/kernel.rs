use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use blocknet_pow_spec::{POW_MEMORY_KB, POW_OUTPUT_LEN};

use crate::backend::{BackendEvent, MiningSolution};
use crate::config::CpuPageMode;
use crate::types::hash_meets_target;

#[cfg(any(target_os = "linux", target_os = "windows"))]
use super::emit_warning;
use super::{
    emit_error, emit_event, fixed_argon, flush_hashes, lane_quota_for_chunk, mark_worker_active,
    mark_worker_inactive, mark_worker_ready, request_shutdown, request_work_pause,
    set_thread_high_perf, should_flush_hashes, wait_for_work_update, Shared,
    MAX_DEADLINE_CHECK_INTERVAL, SOLVED_MASK,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SolutionDisposition {
    Continue,
    PauseAssignment,
}

fn handle_found_solution(
    shared: &Shared,
    template: &crate::backend::WorkTemplate,
    share_binding_id: crate::backend::ShareBindingId,
    thread_idx: usize,
    nonce: u64,
    output: [u8; POW_OUTPUT_LEN],
) -> Result<SolutionDisposition, String> {
    if template.pause_on_solution {
        let solved_state = SOLVED_MASK | template.work_id;
        if shared
            .solution_state
            .compare_exchange(
                template.work_id,
                solved_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            request_work_pause(shared).map_err(|err| {
                format!("cpu thread {thread_idx}: failed to pause workers after solution ({err})")
            })?;
            emit_event(
                shared,
                BackendEvent::Solution(MiningSolution {
                    epoch: template.epoch,
                    nonce,
                    hash: Some(output),
                    share_binding_id,
                    backend_id: shared.instance_id.load(Ordering::Acquire),
                    backend: "cpu",
                }),
            );
            return Ok(SolutionDisposition::PauseAssignment);
        }
        return Ok(SolutionDisposition::Continue);
    }

    if shared.solution_state.load(Ordering::Acquire) == template.work_id {
        emit_event(
            shared,
            BackendEvent::Solution(MiningSolution {
                epoch: template.epoch,
                nonce,
                hash: Some(output),
                share_binding_id,
                backend_id: shared.instance_id.load(Ordering::Acquire),
                backend: "cpu",
            }),
        );
    }
    Ok(SolutionDisposition::Continue)
}

pub(super) fn cpu_worker_loop(
    shared: Arc<Shared>,
    thread_idx: usize,
    core_id: Option<core_affinity::CoreId>,
) {
    set_thread_high_perf();
    if let Some(core_id) = core_id {
        let _ = core_affinity::set_for_current(core_id);
    }

    let hasher = fixed_argon::FixedArgon2id::new(POW_MEMORY_KB);
    let block_count = hasher.block_count();
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    let block_bytes = block_count * std::mem::size_of::<fixed_argon::PowBlock>();

    let mut arena = match PowArena::new(block_count, shared.page_mode) {
        Ok(arena) => arena,
        Err(err) => {
            let message = format!(
                "cpu thread {thread_idx}: {} page-mode arena allocation failed ({err})",
                shared.page_mode.as_str()
            );
            shared
                .arena_allocation_failures
                .fetch_add(1, Ordering::AcqRel);
            if let Ok(mut slot) = shared.startup_error.lock() {
                if slot.is_none() {
                    *slot = Some(message.clone());
                }
            }
            shared.startup_failed.store(true, Ordering::Release);
            emit_error(&shared, message);
            shared.ready_cv.notify_all();
            request_shutdown(&shared);
            return;
        }
    };
    record_arena_backing(&shared, &arena, block_count);
    #[cfg(target_os = "linux")]
    emit_linux_hugepage_diagnostics(&shared, thread_idx, &arena, block_bytes);
    #[cfg(target_os = "windows")]
    emit_windows_large_page_diagnostics(&shared, thread_idx, &arena, block_bytes);
    let memory_blocks = arena.as_mut_slice();

    let mut output = [0u8; POW_OUTPUT_LEN];
    mark_worker_ready(&shared);
    let mut local_generation = 0u64;
    let mut local_work = None;
    let mut worker_active = false;
    let mut nonce = 0u64;
    let mut lane_iters = 0u64;
    let lane_stride = shared.hash_slots.len().max(1) as u64;
    let hash_batch_size = shared.hash_batch_size.max(1);
    let control_check_interval_hashes = shared.control_check_interval_hashes.max(1);
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

        let loop_started_at = Instant::now();
        let deadline_check_due = loop_started_at >= next_deadline_check_at;
        if control_hashes_remaining == 0 || deadline_check_due {
            if loop_started_at >= template.stop_at {
                flush_hashes(&shared, thread_idx, &mut pending_hashes);
                mark_worker_inactive(&shared, &mut worker_active);
                local_work = None;
                continue;
            }

            if control_hashes_remaining == 0 {
                control_hashes_remaining = lane_quota
                    .saturating_sub(lane_iters)
                    .clamp(1, control_check_interval_hashes);
            }
            if deadline_check_due {
                next_deadline_check_at = loop_started_at + MAX_DEADLINE_CHECK_INTERVAL;
            }
        }

        let target_snapshot = template.target_snapshot();
        let nonce_bytes = nonce.to_le_bytes();
        if hasher
            .hash_password_into_with_memory(
                &nonce_bytes,
                &template.header_base,
                &mut output,
                memory_blocks,
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

        lane_iters += 1;
        control_hashes_remaining -= 1;
        pending_hashes += 1;

        let hash_completed_at = Instant::now();
        let should_flush = should_flush_hashes(
            pending_hashes,
            hash_completed_at,
            next_flush_at,
            hash_batch_size,
        );
        if should_flush {
            flush_hashes(&shared, thread_idx, &mut pending_hashes);
            next_flush_at = hash_completed_at + shared.hash_flush_interval;
        }

        if hash_meets_target(&output, &target_snapshot.target) {
            match handle_found_solution(
                &shared,
                template,
                target_snapshot.share_binding_id,
                thread_idx,
                nonce,
                output,
            ) {
                Ok(SolutionDisposition::PauseAssignment) => {
                    flush_hashes(&shared, thread_idx, &mut pending_hashes);
                    mark_worker_inactive(&shared, &mut worker_active);
                    local_work = None;
                    continue;
                }
                Ok(SolutionDisposition::Continue) => {}
                Err(message) => {
                    emit_error(&shared, message);
                    request_shutdown(&shared);
                    break;
                }
            }
        }

        nonce = nonce.wrapping_add(lane_stride);
    }

    flush_hashes(&shared, thread_idx, &mut pending_hashes);
    mark_worker_inactive(&shared, &mut worker_active);
}

#[cfg(target_os = "linux")]
const HUGEPAGE_BYTES: usize = 2 * 1024 * 1024;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const HUGEPAGE_1G_BYTES: usize = 1024 * 1024 * 1024;
#[cfg(target_os = "linux")]
const MADV_COLLAPSE: libc::c_int = 25;

/// Arena used by CPU hashing workers.
///
/// On Unix we prefer an mmap-backed arena to avoid eagerly touching every page
/// in user space. Linux further tries explicit hugetlb pages first.
pub(super) enum PowArena {
    #[cfg(unix)]
    Mmap(MmapArena),
    #[cfg(target_os = "windows")]
    Virtual(VirtualArena),
    Heap {
        blocks: Vec<fixed_argon::PowBlock>,
        allocation_failures: u64,
    },
}

impl PowArena {
    pub(super) fn new(block_count: usize, page_mode: CpuPageMode) -> Result<Self, String> {
        let byte_len = block_count.saturating_mul(std::mem::size_of::<fixed_argon::PowBlock>());
        #[cfg(unix)]
        match MmapArena::new(block_count, byte_len, page_mode) {
            Ok(arena) => return Ok(Self::Mmap(arena)),
            Err(err) if page_mode.requires_explicit_large_pages() => return Err(err),
            Err(_) => {}
        }
        #[cfg(target_os = "windows")]
        match VirtualArena::new(block_count, byte_len, page_mode) {
            Ok(arena) => return Ok(Self::Virtual(arena)),
            Err(err) if page_mode.requires_explicit_large_pages() => return Err(err),
            Err(_) => {}
        }

        let mut blocks = Vec::new();
        blocks
            .try_reserve_exact(block_count)
            .map_err(|err| format!("heap fallback reserve failed: {err}"))?;
        blocks.resize_with(block_count, fixed_argon::PowBlock::default);
        Ok(Self::Heap {
            blocks,
            allocation_failures: 1,
        })
    }

    pub(super) fn as_mut_slice(&mut self) -> &mut [fixed_argon::PowBlock] {
        match self {
            #[cfg(unix)]
            Self::Mmap(arena) => arena.as_mut_slice(),
            #[cfg(target_os = "windows")]
            Self::Virtual(arena) => arena.as_mut_slice(),
            Self::Heap { blocks, .. } => blocks.as_mut_slice(),
        }
    }

    #[cfg(target_os = "linux")]
    fn mmap_ref(&self) -> Option<&MmapArena> {
        match self {
            Self::Mmap(arena) => Some(arena),
            Self::Heap { .. } => None,
        }
    }

    #[cfg(target_os = "windows")]
    fn virtual_ref(&self) -> Option<&VirtualArena> {
        match self {
            Self::Virtual(arena) => Some(arena),
            Self::Heap { .. } => None,
        }
    }

    fn backing_observation(&self, requested_bytes: u64) -> ArenaBackingObservation {
        match self {
            #[cfg(unix)]
            Self::Mmap(arena) => arena.backing_observation(requested_bytes),
            #[cfg(target_os = "windows")]
            Self::Virtual(arena) => arena.backing_observation(requested_bytes),
            Self::Heap {
                allocation_failures,
                ..
            } => ArenaBackingObservation {
                heap_workers: 1,
                heap_bytes: requested_bytes,
                allocation_failures: *allocation_failures,
                ..ArenaBackingObservation::default()
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ArenaBackingObservation {
    explicit_large_workers: u64,
    explicit_large_1g_workers: u64,
    transparent_huge_workers: u64,
    regular_workers: u64,
    heap_workers: u64,
    explicit_large_bytes: u64,
    explicit_large_1g_bytes: u64,
    transparent_huge_bytes: u64,
    regular_bytes: u64,
    heap_bytes: u64,
    allocation_failures: u64,
}

pub(super) fn record_arena_backing(shared: &Shared, arena: &PowArena, block_count: usize) {
    let requested_bytes = block_count
        .saturating_mul(std::mem::size_of::<fixed_argon::PowBlock>())
        .min(u64::MAX as usize) as u64;
    let observation = arena.backing_observation(requested_bytes);
    shared
        .arena_explicit_large_workers
        .fetch_add(observation.explicit_large_workers, Ordering::AcqRel);
    shared
        .arena_explicit_large_1g_workers
        .fetch_add(observation.explicit_large_1g_workers, Ordering::AcqRel);
    shared
        .arena_transparent_huge_workers
        .fetch_add(observation.transparent_huge_workers, Ordering::AcqRel);
    shared
        .arena_regular_workers
        .fetch_add(observation.regular_workers, Ordering::AcqRel);
    shared
        .arena_heap_workers
        .fetch_add(observation.heap_workers, Ordering::AcqRel);
    shared
        .arena_explicit_large_bytes
        .fetch_add(observation.explicit_large_bytes, Ordering::AcqRel);
    shared
        .arena_explicit_large_1g_bytes
        .fetch_add(observation.explicit_large_1g_bytes, Ordering::AcqRel);
    shared
        .arena_transparent_huge_bytes
        .fetch_add(observation.transparent_huge_bytes, Ordering::AcqRel);
    shared
        .arena_regular_bytes
        .fetch_add(observation.regular_bytes, Ordering::AcqRel);
    shared
        .arena_heap_bytes
        .fetch_add(observation.heap_bytes, Ordering::AcqRel);
    shared
        .arena_allocation_failures
        .fetch_add(observation.allocation_failures, Ordering::AcqRel);
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use super::PowArena;
    use super::{handle_found_solution, SolutionDisposition};
    use crate::backend::cpu::CpuBackend;
    use crate::backend::{BackendEvent, WorkTemplate};
    use crate::config::CpuAffinityMode;
    #[cfg(target_os = "linux")]
    use crate::config::CpuPageMode;
    use blocknet_pow_spec::{POW_HEADER_BASE_LEN, POW_OUTPUT_LEN};
    use crossbeam_channel::unbounded;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[cfg(target_os = "linux")]
    #[test]
    fn regular_page_mode_reports_measured_regular_backing() {
        let block_count = 16;
        let arena = PowArena::new(block_count, CpuPageMode::Regular)
            .expect("small regular-page arena should allocate");
        let requested_bytes =
            (block_count * std::mem::size_of::<blocknet_pow_kernel::PowBlock>()) as u64;
        let observation = arena.backing_observation(requested_bytes);

        assert_eq!(observation.explicit_large_workers, 0);
        assert_eq!(observation.explicit_large_1g_workers, 0);
        assert_eq!(observation.transparent_huge_workers, 0);
        assert_eq!(observation.regular_workers, 1);
        assert_eq!(observation.heap_workers, 0);
        assert_eq!(observation.regular_bytes, requested_bytes);
        assert_eq!(observation.allocation_failures, 0);
    }

    // Allocation needs a pre-reserved 1 GiB HugeTLB pool, e.g.:
    //   echo 2 | sudo tee /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages
    // so this test is ignored by default; run with `cargo test -- --ignored`.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    #[test]
    #[ignore = "requires a pre-reserved 1 GiB HugeTLB pool"]
    fn large_1g_page_mode_reports_measured_1g_backing() {
        let block_count = 16;
        let arena = PowArena::new(block_count, CpuPageMode::Large1G)
            .expect("1 GiB hugepage arena should allocate from a reserved pool");
        let requested_bytes =
            (block_count * std::mem::size_of::<blocknet_pow_kernel::PowBlock>()) as u64;
        let observation = arena.backing_observation(requested_bytes);

        assert_eq!(observation.explicit_large_1g_workers, 1);
        assert_eq!(observation.explicit_large_1g_bytes, requested_bytes);
        assert_eq!(observation.explicit_large_workers, 0);
        assert_eq!(observation.transparent_huge_workers, 0);
        assert_eq!(observation.regular_workers, 0);
        assert_eq!(observation.heap_workers, 0);
        assert_eq!(observation.allocation_failures, 0);
    }

    #[test]
    fn non_terminal_solution_emits_event_without_pausing_assignment() {
        let backend = CpuBackend::new(1, CpuAffinityMode::Off);
        backend.shared.started.store(true, Ordering::Release);
        backend.shared.instance_id.store(11, Ordering::Release);
        backend.shared.solution_state.store(7, Ordering::Release);
        let (event_tx, event_rx) = unbounded();
        if let Ok(mut slot) = backend.shared.event_dispatch_tx.write() {
            *slot = Some(event_tx);
        }

        let template = WorkTemplate {
            work_id: 7,
            epoch: 3,
            header_base: Arc::from(vec![0u8; POW_HEADER_BASE_LEN]),
            target: [0xFF; 32],
            dynamic_share_target: None,
            pause_on_solution: false,
            stop_at: Instant::now() + Duration::from_secs(1),
        };

        let disposition =
            handle_found_solution(&backend.shared, &template, 0, 0, 42, [0xAB; POW_OUTPUT_LEN])
                .expect("non-terminal solution handling should succeed");

        assert_eq!(disposition, SolutionDisposition::Continue);
        assert_eq!(backend.shared.solution_state.load(Ordering::Acquire), 7);
        match event_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("solution event should be queued")
        {
            BackendEvent::Solution(solution) => {
                assert_eq!(solution.epoch, 3);
                assert_eq!(solution.nonce, 42);
                assert_eq!(solution.share_binding_id, 0);
                assert_eq!(solution.backend_id, 11);
            }
            other => panic!("expected solution event, got {other:?}"),
        }
    }
}

#[cfg(target_os = "linux")]
fn emit_linux_hugepage_diagnostics(
    shared: &Shared,
    thread_idx: usize,
    arena: &PowArena,
    block_bytes: usize,
) {
    if thread_idx != 0 || shared.page_mode != CpuPageMode::Auto {
        return;
    }

    let per_worker_pages_needed = (block_bytes + HUGEPAGE_BYTES - 1) / HUGEPAGE_BYTES;
    let worker_count = shared.hash_slots.len().max(1);
    let total_pages_needed = per_worker_pages_needed.saturating_mul(worker_count);
    let total_kib = ((block_bytes as u64) + 1023) / 1024;

    let Some(mmap_arena) = arena.mmap_ref() else {
        emit_warning(
            shared,
            format!(
                "mmap allocation unavailable — falling back to heap pages (significant TLB pressure likely). \
                 HugeTLB target: {} pages for this backend ({} workers x {} pages/worker).",
                total_pages_needed,
                worker_count,
                per_worker_pages_needed
            ),
        );
        return;
    };

    if mmap_arena.is_explicit_huge() {
        return;
    }

    let huge_kib = mmap_arena.anon_huge_kib().unwrap_or(0);
    if huge_kib >= total_kib {
        return;
    }

    let pct = if total_kib == 0 {
        0.0
    } else {
        (huge_kib as f64 * 100.0) / total_kib as f64
    };
    emit_warning(
        shared,
        format!(
            "MAP_HUGETLB unavailable; hugepage coverage is {:.1}% ({} / {} MiB) after MADV_HUGEPAGE+MADV_COLLAPSE. Throughput may be lower.",
            pct,
            huge_kib / 1024,
            total_kib / 1024,
        ),
    );
    emit_warning(
        shared,
        format!(
            "HugeTLB fix: reserve ~{} pages for this backend ({} per worker): sudo sysctl -w vm.nr_hugepages={}. Or reduce worker count.",
            total_pages_needed,
            per_worker_pages_needed,
            total_pages_needed,
        ),
    );
}

#[cfg(target_os = "windows")]
fn emit_windows_large_page_diagnostics(
    shared: &Shared,
    thread_idx: usize,
    arena: &PowArena,
    block_bytes: usize,
) {
    if thread_idx != 0 || shared.page_mode != CpuPageMode::Auto {
        return;
    }

    let Some(arena) = arena.virtual_ref() else {
        emit_warning(
            shared,
            "VirtualAlloc failed; CPU hashing fell back to heap memory and may suffer extra TLB pressure"
                .to_owned(),
        );
        return;
    };
    let Some(error) = arena.large_page_error() else {
        return;
    };

    emit_warning(
        shared,
        format!(
            "Windows large-page allocation unavailable for {} MiB per worker (Win32 error {}); using regular VirtualAlloc pages",
            block_bytes / (1024 * 1024),
            error,
        ),
    );
    emit_warning(
        shared,
        "Large-page fix: grant this account 'Lock pages in memory' (SeLockMemoryPrivilege), sign out and back in, then restart Seine"
            .to_owned(),
    );
}

#[cfg(target_os = "windows")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VirtualBacking {
    LargePages,
    Regular { large_page_error: u32 },
}

/// RAII wrapper around a Windows VirtualAlloc-backed arena.
#[cfg(target_os = "windows")]
pub(super) struct VirtualArena {
    ptr: *mut u8,
    block_count: usize,
    allocation_len: usize,
    allocation_failures: u64,
    backing: VirtualBacking,
}

#[cfg(target_os = "windows")]
impl VirtualArena {
    fn new(block_count: usize, byte_len: usize, page_mode: CpuPageMode) -> Result<Self, String> {
        use windows_sys::Win32::Foundation::GetLastError;
        use windows_sys::Win32::System::Memory::{
            GetLargePageMinimum, VirtualAlloc, MEM_COMMIT, MEM_LARGE_PAGES, MEM_RESERVE,
            PAGE_READWRITE,
        };

        if page_mode == CpuPageMode::Large1G {
            return Err(
                "--cpu-page-mode large-1g is only supported on x86_64 Linux (1 GiB HugeTLB pages)"
                    .into(),
            );
        }
        let try_large_pages = page_mode != CpuPageMode::Regular;
        let mut large_page_error = 0;
        let mut allocation_failures = 0u64;
        if try_large_pages {
            let large_page_minimum = unsafe { GetLargePageMinimum() };
            if large_page_minimum == 0 {
                allocation_failures = allocation_failures.saturating_add(1);
                if page_mode == CpuPageMode::Large {
                    return Err("GetLargePageMinimum reported no supported large-page size".into());
                }
            } else {
                match enable_lock_memory_privilege() {
                    Ok(()) => {
                        let allocation_len = round_up_to_multiple(byte_len, large_page_minimum)
                            .ok_or_else(|| {
                                "large-page allocation size overflowed usize".to_owned()
                            })?;
                        let ptr = unsafe {
                            VirtualAlloc(
                                std::ptr::null(),
                                allocation_len,
                                MEM_RESERVE | MEM_COMMIT | MEM_LARGE_PAGES,
                                PAGE_READWRITE,
                            )
                        };
                        if !ptr.is_null() {
                            return Ok(Self {
                                ptr: ptr.cast(),
                                block_count,
                                allocation_len,
                                allocation_failures,
                                backing: VirtualBacking::LargePages,
                            });
                        }
                        large_page_error = unsafe { GetLastError() };
                    }
                    Err(error) => large_page_error = error,
                }
                allocation_failures = allocation_failures.saturating_add(1);
                if page_mode == CpuPageMode::Large {
                    return Err(format!(
                        "MEM_LARGE_PAGES VirtualAlloc unavailable (Win32 error {large_page_error})"
                    ));
                }
            }
        }

        let ptr = unsafe {
            VirtualAlloc(
                std::ptr::null(),
                byte_len,
                MEM_RESERVE | MEM_COMMIT,
                PAGE_READWRITE,
            )
        };
        if ptr.is_null() {
            let error = unsafe { GetLastError() };
            return Err(format!(
                "regular VirtualAlloc failed for {byte_len} bytes (Win32 error {error})"
            ));
        }
        Ok(Self {
            ptr: ptr.cast(),
            block_count,
            allocation_len: byte_len,
            allocation_failures,
            backing: VirtualBacking::Regular { large_page_error },
        })
    }

    fn as_mut_slice(&mut self) -> &mut [fixed_argon::PowBlock] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.ptr.cast::<fixed_argon::PowBlock>(),
                self.block_count,
            )
        }
    }

    fn large_page_error(&self) -> Option<u32> {
        match self.backing {
            VirtualBacking::LargePages => None,
            VirtualBacking::Regular { large_page_error } => Some(large_page_error),
        }
    }

    fn backing_observation(&self, _requested_bytes: u64) -> ArenaBackingObservation {
        let allocation_bytes = self.allocation_len.min(u64::MAX as usize) as u64;
        match self.backing {
            VirtualBacking::LargePages => ArenaBackingObservation {
                explicit_large_workers: 1,
                explicit_large_bytes: allocation_bytes,
                allocation_failures: self.allocation_failures,
                ..ArenaBackingObservation::default()
            },
            VirtualBacking::Regular { .. } => ArenaBackingObservation {
                regular_workers: 1,
                regular_bytes: allocation_bytes,
                allocation_failures: self.allocation_failures,
                ..ArenaBackingObservation::default()
            },
        }
    }
}

#[cfg(target_os = "windows")]
impl Drop for VirtualArena {
    fn drop(&mut self) {
        use windows_sys::Win32::System::Memory::{VirtualFree, MEM_RELEASE};
        unsafe {
            let _ = VirtualFree(self.ptr.cast(), 0, MEM_RELEASE);
        }
    }
}

#[cfg(target_os = "windows")]
unsafe impl Send for VirtualArena {}

#[cfg(target_os = "windows")]
unsafe impl Sync for VirtualArena {}

#[cfg(any(
    target_os = "windows",
    all(target_os = "linux", target_arch = "x86_64")
))]
fn round_up_to_multiple(value: usize, multiple: usize) -> Option<usize> {
    if multiple == 0 {
        return None;
    }
    value
        .checked_add(multiple - 1)?
        .checked_div(multiple)?
        .checked_mul(multiple)
}

#[cfg(target_os = "windows")]
fn enable_lock_memory_privilege() -> Result<(), u32> {
    use std::sync::OnceLock;
    use windows_sys::Win32::Foundation::{
        CloseHandle, GetLastError, SetLastError, ERROR_NOT_ALL_ASSIGNED, LUID,
    };
    use windows_sys::Win32::Security::{
        AdjustTokenPrivileges, LookupPrivilegeValueW, LUID_AND_ATTRIBUTES, SE_PRIVILEGE_ENABLED,
        TOKEN_ADJUST_PRIVILEGES, TOKEN_PRIVILEGES, TOKEN_QUERY,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    static RESULT: OnceLock<Result<(), u32>> = OnceLock::new();
    *RESULT.get_or_init(|| unsafe {
        let mut token = std::ptr::null_mut();
        if OpenProcessToken(
            GetCurrentProcess(),
            TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY,
            &mut token,
        ) == 0
        {
            return Err(GetLastError());
        }

        let result = (|| {
            let mut luid = std::mem::zeroed::<LUID>();
            let privilege_name = "SeLockMemoryPrivilege\0".encode_utf16().collect::<Vec<_>>();
            if LookupPrivilegeValueW(std::ptr::null(), privilege_name.as_ptr(), &mut luid) == 0 {
                return Err(GetLastError());
            }
            let privileges = TOKEN_PRIVILEGES {
                PrivilegeCount: 1,
                Privileges: [LUID_AND_ATTRIBUTES {
                    Luid: luid,
                    Attributes: SE_PRIVILEGE_ENABLED,
                }],
            };
            SetLastError(0);
            if AdjustTokenPrivileges(
                token,
                0,
                &privileges,
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            ) == 0
            {
                return Err(GetLastError());
            }
            let error = GetLastError();
            if error == ERROR_NOT_ALL_ASSIGNED {
                return Err(error);
            }
            Ok(())
        })();
        let _ = CloseHandle(token);
        result
    })
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MmapBacking {
    #[cfg(target_os = "linux")]
    ExplicitHugeTLB,
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    ExplicitHugeTLB1G,
    #[cfg(target_os = "linux")]
    TransparentHuge,
    Regular,
}

/// RAII wrapper around an mmap-backed arena.
#[cfg(unix)]
pub(super) struct MmapArena {
    ptr: *mut u8,
    byte_len: usize,
    block_count: usize,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    backing: MmapBacking,
    allocation_failures: u64,
}

#[cfg(unix)]
impl MmapArena {
    pub(super) fn new(
        block_count: usize,
        byte_len: usize,
        page_mode: CpuPageMode,
    ) -> Result<Self, String> {
        #[cfg(target_os = "linux")]
        {
            if page_mode == CpuPageMode::Large1G {
                return Self::new_hugetlb_1g(block_count, byte_len);
            }
            let mut allocation_failures = 0u64;
            if page_mode != CpuPageMode::Regular {
                // Attempt 1: MAP_HUGETLB for guaranteed 2 MB pages.
                let ptr = unsafe {
                    libc::mmap(
                        std::ptr::null_mut(),
                        byte_len,
                        libc::PROT_READ | libc::PROT_WRITE,
                        libc::MAP_PRIVATE | libc::MAP_ANON | libc::MAP_HUGETLB | libc::MAP_POPULATE,
                        -1,
                        0,
                    )
                };
                if ptr != libc::MAP_FAILED {
                    return Ok(Self {
                        ptr: ptr as *mut u8,
                        byte_len,
                        block_count,
                        backing: MmapBacking::ExplicitHugeTLB,
                        allocation_failures,
                    });
                }
                let error = std::io::Error::last_os_error();
                allocation_failures = allocation_failures.saturating_add(1);
                if page_mode == CpuPageMode::Large {
                    return Err(format!(
                        "MAP_HUGETLB mmap failed for {byte_len} bytes: {error}"
                    ));
                }
            }

            // Attempt 2: a regular mapping with either THP hints or an explicit THP ban.
            let populate_flag = if page_mode == CpuPageMode::Regular {
                0
            } else {
                libc::MAP_POPULATE
            };
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    byte_len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANON | populate_flag,
                    -1,
                    0,
                )
            };
            if ptr == libc::MAP_FAILED {
                let error = std::io::Error::last_os_error();
                return Err(format!("regular mmap failed for {byte_len} bytes: {error}"));
            }
            let backing = if page_mode == CpuPageMode::Regular {
                unsafe {
                    let _ = libc::madvise(ptr, byte_len, libc::MADV_NOHUGEPAGE);
                    // Apply MADV_NOHUGEPAGE before faulting pages so even hosts using
                    // THP=always cannot turn the control lane into a hidden THP run.
                    std::ptr::write_bytes(ptr.cast::<u8>(), 0, byte_len);
                }
                MmapBacking::Regular
            } else {
                unsafe {
                    let _ = libc::madvise(ptr, byte_len, libc::MADV_HUGEPAGE);
                    let _ = libc::madvise(ptr, byte_len, MADV_COLLAPSE);
                }
                MmapBacking::TransparentHuge
            };
            return Ok(Self {
                ptr: ptr as *mut u8,
                byte_len,
                block_count,
                backing,
                allocation_failures,
            });
        }

        #[cfg(not(target_os = "linux"))]
        {
            if page_mode.requires_explicit_large_pages() {
                return Err(format!(
                    "explicit large pages (--cpu-page-mode {}) are unsupported on this Unix target",
                    page_mode.as_str()
                ));
            }
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    byte_len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANON,
                    -1,
                    0,
                )
            };
            if ptr == libc::MAP_FAILED {
                let error = std::io::Error::last_os_error();
                return Err(format!("mmap failed for {byte_len} bytes: {error}"));
            }
            Ok(Self {
                ptr: ptr as *mut u8,
                byte_len,
                block_count,
                backing: MmapBacking::Regular,
                allocation_failures: 0,
            })
        }
    }

    /// Maps the arena from the pre-reserved 1 GiB HugeTLB pool and fails
    /// closed: `large-1g` never falls back to another page class.
    #[cfg(target_os = "linux")]
    fn new_hugetlb_1g(block_count: usize, byte_len: usize) -> Result<Self, String> {
        #[cfg(not(target_arch = "x86_64"))]
        {
            let _ = (block_count, byte_len);
            Err("--cpu-page-mode large-1g requires x86_64 Linux (1 GiB HugeTLB pages)".to_owned())
        }

        #[cfg(target_arch = "x86_64")]
        {
            // HugeTLB mapping lengths must be page-size multiples. Worker
            // arenas are exactly 2 GiB (two 1 GiB pages); round up defensively.
            let map_len = round_up_to_multiple(byte_len, HUGEPAGE_1G_BYTES)
                .ok_or_else(|| "1 GiB hugepage allocation size overflowed usize".to_owned())?;
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    map_len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE
                        | libc::MAP_ANON
                        | libc::MAP_HUGETLB
                        | libc::MAP_HUGE_1GB
                        | libc::MAP_POPULATE,
                    -1,
                    0,
                )
            };
            if ptr == libc::MAP_FAILED {
                let error = std::io::Error::last_os_error();
                return Err(format!(
                    "MAP_HUGETLB|MAP_HUGE_1GB mmap failed for {map_len} bytes: {error}; reserve \
                     1 GiB pages via /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages"
                ));
            }
            Ok(Self {
                ptr: ptr as *mut u8,
                byte_len: map_len,
                block_count,
                backing: MmapBacking::ExplicitHugeTLB1G,
                allocation_failures: 0,
            })
        }
    }

    pub(super) fn as_mut_slice(&mut self) -> &mut [fixed_argon::PowBlock] {
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr as *mut fixed_argon::PowBlock, self.block_count)
        }
    }

    #[cfg(target_os = "linux")]
    fn is_explicit_huge(&self) -> bool {
        matches!(self.backing, MmapBacking::ExplicitHugeTLB)
    }

    #[cfg(target_os = "linux")]
    fn anon_huge_kib(&self) -> Option<u64> {
        read_smaps_anon_huge_kib(self.ptr as usize)
    }

    fn backing_observation(&self, requested_bytes: u64) -> ArenaBackingObservation {
        #[cfg(target_os = "linux")]
        {
            match self.backing {
                MmapBacking::ExplicitHugeTLB => ArenaBackingObservation {
                    explicit_large_workers: 1,
                    explicit_large_bytes: requested_bytes,
                    allocation_failures: self.allocation_failures,
                    ..ArenaBackingObservation::default()
                },
                #[cfg(target_arch = "x86_64")]
                MmapBacking::ExplicitHugeTLB1G => ArenaBackingObservation {
                    explicit_large_1g_workers: 1,
                    explicit_large_1g_bytes: requested_bytes,
                    allocation_failures: self.allocation_failures,
                    ..ArenaBackingObservation::default()
                },
                MmapBacking::TransparentHuge => {
                    let transparent_huge_bytes = self
                        .anon_huge_kib()
                        .unwrap_or(0)
                        .saturating_mul(1024)
                        .min(requested_bytes);
                    let regular_bytes = requested_bytes.saturating_sub(transparent_huge_bytes);
                    ArenaBackingObservation {
                        transparent_huge_workers: u64::from(transparent_huge_bytes > 0),
                        regular_workers: u64::from(regular_bytes > 0),
                        transparent_huge_bytes,
                        regular_bytes,
                        allocation_failures: self.allocation_failures,
                        ..ArenaBackingObservation::default()
                    }
                }
                MmapBacking::Regular => {
                    let transparent_huge_bytes = self
                        .anon_huge_kib()
                        .unwrap_or(0)
                        .saturating_mul(1024)
                        .min(requested_bytes);
                    let regular_bytes = requested_bytes.saturating_sub(transparent_huge_bytes);
                    ArenaBackingObservation {
                        transparent_huge_workers: u64::from(transparent_huge_bytes > 0),
                        regular_workers: u64::from(regular_bytes > 0),
                        transparent_huge_bytes,
                        regular_bytes,
                        allocation_failures: self.allocation_failures,
                        ..ArenaBackingObservation::default()
                    }
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            ArenaBackingObservation {
                regular_workers: 1,
                regular_bytes: requested_bytes,
                allocation_failures: self.allocation_failures,
                ..ArenaBackingObservation::default()
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn read_smaps_anon_huge_kib(addr: usize) -> Option<u64> {
    let smaps = std::fs::read_to_string("/proc/self/smaps").ok()?;
    let mut in_target_mapping = false;
    for line in smaps.lines() {
        if let Some((start, end)) = parse_smaps_region_header(line) {
            in_target_mapping = addr >= start && addr < end;
            continue;
        }
        if in_target_mapping {
            if let Some(rest) = line.strip_prefix("AnonHugePages:") {
                return rest
                    .split_whitespace()
                    .next()
                    .and_then(|value| value.parse::<u64>().ok());
            }
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn parse_smaps_region_header(line: &str) -> Option<(usize, usize)> {
    let range = line.split_whitespace().next()?;
    let (start, end) = range.split_once('-')?;
    Some((
        usize::from_str_radix(start, 16).ok()?,
        usize::from_str_radix(end, 16).ok()?,
    ))
}

#[cfg(unix)]
impl Drop for MmapArena {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.byte_len);
        }
    }
}

// Safety: mmap-backed arenas are thread-confined in this backend.
#[cfg(unix)]
unsafe impl Send for MmapArena {}
#[cfg(unix)]
unsafe impl Sync for MmapArena {}
