use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use blocknet_pow_spec::{POW_MEMORY_KB, POW_OUTPUT_LEN};

use crate::backend::{BackendEvent, MiningSolution};
use crate::types::hash_meets_target;

#[cfg(target_os = "linux")]
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
    #[cfg(target_os = "linux")]
    let block_bytes = block_count * std::mem::size_of::<fixed_argon::PowBlock>();

    let mut arena = PowArena::new(block_count);
    #[cfg(target_os = "linux")]
    emit_linux_hugepage_diagnostics(&shared, thread_idx, &arena, block_bytes);
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

        if hash_meets_target(&output, &template.target) {
            match handle_found_solution(&shared, template, thread_idx, nonce, output) {
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
#[cfg(target_os = "linux")]
const MADV_COLLAPSE: libc::c_int = 25;

/// Arena used by CPU hashing workers.
///
/// On Unix we prefer an mmap-backed arena to avoid eagerly touching every page
/// in user space. Linux further tries explicit hugetlb pages first.
pub(super) enum PowArena {
    #[cfg(unix)]
    Mmap(MmapArena),
    Heap(Vec<fixed_argon::PowBlock>),
}

impl PowArena {
    pub(super) fn new(block_count: usize) -> Self {
        let byte_len = block_count.saturating_mul(std::mem::size_of::<fixed_argon::PowBlock>());
        #[cfg(unix)]
        if let Some(arena) = MmapArena::new(block_count, byte_len) {
            return Self::Mmap(arena);
        }
        Self::Heap(vec![fixed_argon::PowBlock::default(); block_count])
    }

    pub(super) fn as_mut_slice(&mut self) -> &mut [fixed_argon::PowBlock] {
        match self {
            #[cfg(unix)]
            Self::Mmap(arena) => arena.as_mut_slice(),
            Self::Heap(blocks) => blocks.as_mut_slice(),
        }
    }

    #[cfg(target_os = "linux")]
    fn mmap_ref(&self) -> Option<&MmapArena> {
        match self {
            Self::Mmap(arena) => Some(arena),
            Self::Heap(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{handle_found_solution, SolutionDisposition};
    use crate::backend::cpu::CpuBackend;
    use crate::backend::{BackendEvent, WorkTemplate};
    use crate::config::CpuAffinityMode;
    use blocknet_pow_spec::{POW_HEADER_BASE_LEN, POW_OUTPUT_LEN};
    use crossbeam_channel::unbounded;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

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
            pause_on_solution: false,
            stop_at: Instant::now() + Duration::from_secs(1),
        };

        let disposition =
            handle_found_solution(&backend.shared, &template, 0, 42, [0xAB; POW_OUTPUT_LEN])
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
    if thread_idx != 0 {
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

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MmapBacking {
    #[cfg(target_os = "linux")]
    ExplicitHugeTLB,
    #[cfg(target_os = "linux")]
    TransparentHuge,
    #[cfg(not(target_os = "linux"))]
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
}

#[cfg(unix)]
impl MmapArena {
    pub(super) fn new(block_count: usize, byte_len: usize) -> Option<Self> {
        #[cfg(target_os = "linux")]
        {
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
                return Some(Self {
                    ptr: ptr as *mut u8,
                    byte_len,
                    block_count,
                    backing: MmapBacking::ExplicitHugeTLB,
                });
            }

            // Attempt 2: regular mmap + THP hints.
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    byte_len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANON | libc::MAP_POPULATE,
                    -1,
                    0,
                )
            };
            if ptr == libc::MAP_FAILED {
                return None;
            }
            unsafe {
                let _ = libc::madvise(ptr, byte_len, libc::MADV_HUGEPAGE);
                let _ = libc::madvise(ptr, byte_len, MADV_COLLAPSE);
            }
            return Some(Self {
                ptr: ptr as *mut u8,
                byte_len,
                block_count,
                backing: MmapBacking::TransparentHuge,
            });
        }

        #[cfg(not(target_os = "linux"))]
        {
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
                return None;
            }
            Some(Self {
                ptr: ptr as *mut u8,
                byte_len,
                block_count,
                backing: MmapBacking::Regular,
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
