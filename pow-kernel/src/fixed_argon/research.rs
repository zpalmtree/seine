//! Exact dependency traces and bounded, online checkpoint-recomputation models.
//!
//! This is an operation-count simulator, not a reduced-memory hash implementation
//! or a performance estimate. A trace is first obtained from a real full-memory
//! hash. Replay reveals each reference only when its block is first computed;
//! recomputations may use previously recorded references, never future ones.
use super::*;

#[derive(Debug)]
pub struct ReferenceTrace {
    pub requested_memory_kib: u32,
    pub hash: [u8; 32],
    // Entries 0 and 1 are seeds; every subsequent entry is strictly before i-1.
    references: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayError {
    InvalidStride,
    InvalidTrace,
}

#[derive(Debug)]
pub struct Replay {
    pub complete: bool,
    pub completed_blocks: usize,
    pub compressions: u64,
    pub recompressions: u64,
    pub peak_resident_blocks: usize,
    pub retained_blocks: usize,
    pub peak_stack_frames: usize,
    pub reference_metadata_bytes: usize,
}

/// Generate a dependency trace from the exact fixed t=1,p=1 Argon2id hash.
/// Allocates the full arena and excludes any memory/time tradeoff simulation.
pub fn trace_hash(
    memory_kib: u32,
    pwd: &[u8],
    salt: &[u8],
    kernel: CpuKernel,
) -> Result<ReferenceTrace> {
    let fixed = FixedArgon2id::with_kernel(memory_kib, kernel)?;
    let mut hash = [0; 32];
    verify_inputs(pwd, salt, &hash)?;
    let mut memory = vec![PowBlock::default(); fixed.block_count];
    initialize_lane_blocks(
        &mut memory,
        initial_hash(memory_kib, ARGON2_T_COST, ARGON2_LANES, pwd, salt, &hash),
    )?;
    let mut references = vec![0u32; fixed.block_count];
    match fixed.isa {
        ISA_AVX512 => trace_fill::<ISA_AVX512>(&fixed, &mut memory, &mut references),
        ISA_AVX512_256 => trace_fill::<ISA_AVX512_256>(&fixed, &mut memory, &mut references),
        ISA_AVX2 => trace_fill::<ISA_AVX2>(&fixed, &mut memory, &mut references),
        ISA_NEON => trace_fill::<ISA_NEON>(&fixed, &mut memory, &mut references),
        _ => trace_fill::<ISA_SCALAR>(&fixed, &mut memory, &mut references),
    }
    finalize(&memory, &mut hash)?;
    Ok(ReferenceTrace {
        requested_memory_kib: memory_kib,
        hash,
        references,
    })
}

fn trace_fill<const ISA: u8>(
    fixed: &FixedArgon2id,
    memory: &mut [PowBlock],
    references: &mut [u32],
) {
    // Logging deliberately prioritizes correctness over matching hot-loop timing.
    for current in 2..fixed.block_count {
        let reference = if current < fixed.block_count / 2 {
            fixed.data_independent_ref_indexes[current]
        } else {
            reference_index(current - 1, memory[current - 1].0[0])
        };
        references[current] = reference as u32;
        fill_block_from_refs::<ISA>(memory, current - 1, reference, current);
    }
}

impl ReferenceTrace {
    pub fn references(&self) -> &[u32] {
        &self.references
    }

    /// Keep seeds and every `stride`th block; release other blocks after their
    /// last currently active consumer. Always hold the previous output while
    /// producing the next. Inputs and the output occupy distinct block slots.
    /// The explicit stack bounds Rust stack usage even for adversarial graphs.
    pub fn replay(
        &self,
        stride: usize,
        max_compressions: u64,
    ) -> std::result::Result<Replay, ReplayError> {
        if stride == 0 {
            return Err(ReplayError::InvalidStride);
        }
        self.replay_mask(
            (0..self.references.len())
                .map(|block| block < 2 || block.is_multiple_of(stride))
                .collect(),
            max_compressions,
        )
    }

    /// Online cost-aware retention: save a newly produced block when the upper
    /// bound on its reconstruction cost reaches `threshold`. Each decision uses
    /// only its already known parents and prior retention decisions, never future
    /// data-dependent references. This is a heuristic, not the literature's
    /// optimal ranking tradeoff.
    pub fn replay_cost_threshold(
        &self,
        threshold: u64,
        max_compressions: u64,
    ) -> std::result::Result<Replay, ReplayError> {
        if threshold == 0 {
            return Err(ReplayError::InvalidStride);
        }
        self.validate()?;
        let mut costs = vec![0u64; self.references.len()];
        let mut retained = vec![false; self.references.len()];
        retained[0] = true;
        retained[1] = true;
        for block in 2..self.references.len() {
            let cost = 1u64
                .saturating_add(costs[block - 1])
                .saturating_add(costs[self.references[block] as usize]);
            retained[block] = cost >= threshold;
            costs[block] = if retained[block] { 0 } else { cost };
        }
        self.replay_mask(retained, max_compressions)
    }

    fn validate(&self) -> std::result::Result<(), ReplayError> {
        let count = self.references.len();
        if count < 8
            || self
                .references
                .iter()
                .enumerate()
                .skip(2)
                .any(|(i, &r)| r as usize >= i - 1)
        {
            return Err(ReplayError::InvalidTrace);
        }
        Ok(())
    }

    fn replay_mask(
        &self,
        retained: Vec<bool>,
        max_compressions: u64,
    ) -> std::result::Result<Replay, ReplayError> {
        self.validate()?;
        let count = self.references.len();
        let mut model = Model {
            references: &self.references,
            retained,
            pins: vec![0; count],
            resident: vec![false; count],
            live: 2,
            result: Replay {
                complete: false,
                completed_blocks: 2,
                compressions: 0,
                recompressions: 0,
                peak_resident_blocks: 2,
                retained_blocks: 2,
                peak_stack_frames: 0,
                reference_metadata_bytes: count * std::mem::size_of::<u32>(),
            },
        };
        model.resident[0] = true;
        model.resident[1] = true;
        model.pins[1] = 1; // previous output held across the forward traversal
        let mut stack = Vec::new();
        for current in 2..count {
            stack.push((current, 0u8));
            while let Some((block, stage)) = stack.pop() {
                model.result.peak_stack_frames =
                    model.result.peak_stack_frames.max(stack.len() + 1);
                debug_assert!(block <= current, "replay must not know future references");
                if stage == 0 && model.resident[block] {
                    model.pins[block] += 1;
                    continue;
                }
                match stage {
                    0 => {
                        debug_assert!(block >= 2);
                        stack.push((block, 1));
                        stack.push((block - 1, 0));
                    }
                    1 => {
                        stack.push((block, 2));
                        stack.push((model.references[block] as usize, 0));
                    }
                    _ => {
                        if model.result.compressions >= max_compressions {
                            return Ok(model.result);
                        }
                        model.result.compressions += 1;
                        model.result.recompressions += u64::from(block < current);
                        // The result cannot overwrite an input still needed by
                        // another stack frame. Count its distinct slot first.
                        debug_assert!(!model.resident[block]);
                        model.resident[block] = true;
                        model.pins[block] = 1;
                        model.live += 1;
                        model.result.peak_resident_blocks =
                            model.result.peak_resident_blocks.max(model.live);
                        if model.permanent(block) {
                            model.result.retained_blocks += 1;
                        }
                        model.release(block - 1);
                        model.release(model.references[block] as usize);
                    }
                }
            }
            model.release(current - 1);
            model.result.completed_blocks = current + 1;
        }
        model.result.complete = true;
        Ok(model.result)
    }
}

struct Model<'a> {
    references: &'a [u32],
    retained: Vec<bool>,
    pins: Vec<u32>,
    resident: Vec<bool>,
    live: usize,
    result: Replay,
}

impl Model<'_> {
    fn permanent(&self, block: usize) -> bool {
        self.retained[block]
    }

    fn release(&mut self, block: usize) {
        debug_assert!(self.pins[block] > 0);
        self.pins[block] -= 1;
        if self.pins[block] == 0 && !self.permanent(block) {
            self.resident[block] = false;
            self.live -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::{Algorithm, Argon2, Params, Version};

    #[test]
    fn cost_retention_is_online_and_bounds_reconstruction_work() {
        let trace = trace_hash(1024, &[0; 8], &[0x37; 92], CpuKernel::Scalar).unwrap();
        let baseline = trace.replay_cost_threshold(1, 1022).unwrap();
        assert!(baseline.complete);
        assert_eq!(baseline.recompressions, 0);
        for threshold in [2, 4, 8, 16] {
            let full = trace.replay_cost_threshold(threshold, 100_000).unwrap();
            assert!(full.complete);
            assert!(full.peak_resident_blocks < trace.references.len());
            // A future suffix must not change the already processed prefix.
            let prefix = ReferenceTrace {
                requested_memory_kib: 512,
                hash: [0; 32],
                references: trace.references[..512].to_vec(),
            };
            let prefix_run = prefix.replay_cost_threshold(threshold, 100_000).unwrap();
            let stopped = trace
                .replay_cost_threshold(threshold, prefix_run.compressions)
                .unwrap();
            assert_eq!(stopped.completed_blocks, 512);
            assert_eq!(stopped.compressions, prefix_run.compressions);
            assert_eq!(
                stopped.peak_resident_blocks,
                prefix_run.peak_resident_blocks
            );
        }
    }

    #[test]
    fn traces_match_independent_hash_and_all_supported_kernels() {
        for memory_kib in [8, 33, 1024] {
            for nonce in [0u64, 17] {
                let pwd = nonce.to_le_bytes();
                let salt = [0x37; 92];
                let mut expected = [0; 32];
                Argon2::new(
                    Algorithm::Argon2id,
                    Version::V0x13,
                    Params::new(memory_kib, 1, 1, Some(32)).unwrap(),
                )
                .hash_password_into(&pwd, &salt, &mut expected)
                .unwrap();
                let scalar = trace_hash(memory_kib, &pwd, &salt, CpuKernel::Scalar).unwrap();
                assert_eq!(scalar.hash, expected);
                for kernel in CpuKernel::ALL.into_iter().filter(|k| k.is_supported()) {
                    let candidate = trace_hash(memory_kib, &pwd, &salt, kernel).unwrap();
                    assert_eq!(candidate.hash, expected, "{memory_kib} {kernel}");
                    assert_eq!(candidate.references, scalar.references);
                }
                let replay = scalar.replay(1, u64::MAX).unwrap();
                assert!(replay.complete);
                assert_eq!(replay.compressions as usize, scalar.references.len() - 2);
                assert_eq!(replay.recompressions, 0);
                assert_eq!(replay.peak_resident_blocks, scalar.references.len());
            }
        }
    }

    #[test]
    fn checkpoint_replay_counts_recomputed_blocks_and_live_inputs() {
        // Every block refers to seed 0. With even checkpoints, producing block
        // 4 uses the live output 3; 3 need not be recomputed. No reference forces
        // an older discarded odd block back into memory.
        let mut trace = ReferenceTrace {
            requested_memory_kib: 8,
            hash: [0; 32],
            references: vec![0; 8],
        };
        let simple = trace.replay(2, 100).unwrap();
        assert!(simple.complete);
        assert_eq!(simple.compressions, 6);
        assert_eq!(simple.peak_resident_blocks, 6);
        trace.references[7] = 3; // old discarded block 3 requires one recompute
        let replay = trace.replay(2, 100).unwrap();
        assert!(replay.complete);
        assert_eq!(replay.recompressions, 1);
        assert_eq!(replay.compressions, 7);
        assert_eq!(replay.peak_resident_blocks, 7);
    }

    #[test]
    fn rejects_bad_graphs_and_bounds_explosive_replays() {
        let mut trace = ReferenceTrace {
            requested_memory_kib: 8,
            hash: [0; 32],
            references: vec![0; 8],
        };
        assert_eq!(trace.replay(0, 10).unwrap_err(), ReplayError::InvalidStride);
        let limited = trace.replay(8, 2).unwrap();
        assert!(!limited.complete);
        assert_eq!(limited.completed_blocks, 4);
        assert_eq!(limited.compressions, 2);
        trace.references[4] = 3;
        assert_eq!(trace.replay(2, 10).unwrap_err(), ReplayError::InvalidTrace);
    }
}
