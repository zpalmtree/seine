//! Experimental online checkpoint hashing. No full arena or future trace is used.
//! Separate from production dispatch; caller supplies a hard retained-block cap.
use super::*;

const CHUNK_BLOCKS: usize = 4096;
const ABSENT: u32 = u32::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReducedMemoryError {
    Hash(Error),
    InvalidThreshold,
    CapacityExceeded,
}
impl From<Error> for ReducedMemoryError {
    fn from(value: Error) -> Self {
        Self::Hash(value)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReducedMemoryStats {
    pub compressions: u64,
    pub retained_blocks: usize,
    /// Owned buffers including address tables and metadata; excludes allocator
    /// bookkeeping, process overhead and bounded compression/recursion stack.
    pub allocated_bytes: usize,
    pub peak_reconstruction_depth: usize,
}

pub struct ReducedMemoryContext {
    fixed: FixedArgon2id,
    threshold: u8,
    max_retained: usize,
    references: Vec<u32>,
    costs: Vec<u8>,
    slots: Vec<u32>,
    arena: Vec<Box<[PowBlock]>>,
    retained: usize,
    stats: ReducedMemoryStats,
}

impl ReducedMemoryContext {
    pub fn new(
        memory_kib: u32,
        kernel: CpuKernel,
        threshold: u8,
        max_retained: usize,
    ) -> std::result::Result<Self, ReducedMemoryError> {
        if !(1..=16).contains(&threshold) {
            return Err(ReducedMemoryError::InvalidThreshold);
        }
        // Streaming stores require the production full-arena fill protocol.
        if kernel == CpuKernel::Avx512256Stream {
            return Err(Error::UnsupportedKernel.into());
        }
        if memory_kib < 8 {
            return Err(Error::MemoryTooLittle.into());
        }
        let fixed = FixedArgon2id::with_kernel(memory_kib, kernel)?;
        let count = fixed.block_count;
        if max_retained < 2 {
            return Err(ReducedMemoryError::CapacityExceeded);
        }
        Ok(Self {
            fixed,
            threshold,
            max_retained: max_retained.min(count),
            references: vec![0; count],
            costs: vec![0; count],
            slots: vec![ABSENT; count],
            arena: Vec::new(),
            retained: 0,
            stats: ReducedMemoryStats::default(),
        })
    }

    pub fn stats(&self) -> ReducedMemoryStats {
        self.stats
    }

    pub fn hash_password_into(
        &mut self,
        pwd: &[u8],
        salt: &[u8],
        out: &mut [u8],
    ) -> std::result::Result<ReducedMemoryStats, ReducedMemoryError> {
        verify_inputs(pwd, salt, out)?;
        self.retained = 0;
        self.slots.fill(ABSENT);
        self.costs.fill(0);
        self.stats = ReducedMemoryStats::default();
        let mut seeds = [PowBlock::default(); 2];
        initialize_lane_blocks(
            &mut seeds,
            initial_hash(
                self.fixed.requested_memory_kib,
                ARGON2_T_COST,
                ARGON2_LANES,
                pwd,
                salt,
                out,
            ),
        )?;
        self.keep(0, &seeds[0])?;
        self.keep(1, &seeds[1])?;
        match self.fixed.isa {
            ISA_AVX2 => self.fill::<ISA_AVX2>(seeds[1], out)?,
            ISA_AVX512 => self.fill::<ISA_AVX512>(seeds[1], out)?,
            ISA_AVX512_256 => self.fill::<ISA_AVX512_256>(seeds[1], out)?,
            ISA_NEON => self.fill::<ISA_NEON>(seeds[1], out)?,
            _ => self.fill::<ISA_SCALAR>(seeds[1], out)?,
        }
        self.stats.retained_blocks = self.retained;
        self.stats.allocated_bytes = self
            .arena
            .iter()
            .map(|c| c.len() * PowBlock::SIZE)
            .sum::<usize>()
            + self.arena.capacity() * std::mem::size_of::<Box<[PowBlock]>>()
            + self.references.capacity() * std::mem::size_of::<u32>()
            + self.slots.capacity() * std::mem::size_of::<u32>()
            + self.costs.capacity()
            + self.fixed.data_independent_ref_indexes.len() * std::mem::size_of::<usize>();
        Ok(self.stats)
    }

    fn keep(
        &mut self,
        block: usize,
        value: &PowBlock,
    ) -> std::result::Result<(), ReducedMemoryError> {
        if self.retained >= self.max_retained {
            return Err(ReducedMemoryError::CapacityExceeded);
        }
        let chunk = self.retained / CHUNK_BLOCKS;
        if chunk == self.arena.len() {
            let remaining = self.max_retained - chunk * CHUNK_BLOCKS;
            self.arena
                .push(vec![PowBlock::default(); remaining.min(CHUNK_BLOCKS)].into_boxed_slice());
        }
        self.arena[chunk][self.retained % CHUNK_BLOCKS] = *value;
        self.slots[block] = self.retained as u32;
        self.retained += 1;
        Ok(())
    }

    // Every non-retained block has reconstruction cost < threshold. Both
    // parents strictly precede it, and their costs are lower. Thus depth is
    // bounded by threshold (<=16), independently of the arena/hash size.
    fn restore<const ISA: u8>(
        &self,
        block: usize,
        depth: usize,
        stats: &mut ReducedMemoryStats,
    ) -> PowBlock {
        stats.peak_reconstruction_depth = stats.peak_reconstruction_depth.max(depth);
        let slot = self.slots[block];
        if slot != ABSENT {
            return self.arena[slot as usize / CHUNK_BLOCKS][slot as usize % CHUNK_BLOCKS];
        }
        debug_assert!(block >= 2 && depth <= self.threshold as usize);
        let prev = self.restore::<ISA>(block - 1, depth + 1, stats);
        let reference = self.restore::<ISA>(self.references[block] as usize, depth + 1, stats);
        stats.compressions += 1;
        let mut result = PowBlock::default();
        compress_into::<ISA>(&prev, &reference, &mut result);
        result
    }

    fn fill<const ISA: u8>(
        &mut self,
        mut previous: PowBlock,
        out: &mut [u8],
    ) -> std::result::Result<(), ReducedMemoryError> {
        let mut stats = ReducedMemoryStats::default();
        for current in 2..self.fixed.block_count {
            let reference = if current < self.fixed.block_count / 2 {
                self.fixed.data_independent_ref_indexes[current]
            } else {
                reference_index(current - 1, previous.0[0])
            };
            debug_assert!(reference < current - 1);
            // Record only this newly discovered reference, never a future one.
            self.references[current] = reference as u32;
            let rhs = self.restore::<ISA>(reference, 0, &mut stats);
            let mut result = PowBlock::default();
            compress_into::<ISA>(&previous, &rhs, &mut result);
            stats.compressions += 1;
            let cost = 1 + self.costs[current - 1] as u16 + self.costs[reference] as u16;
            if cost >= self.threshold as u16 {
                self.keep(current, &result)?;
            } else {
                self.costs[current] = cost as u8;
            }
            previous = result;
        }
        finalize(std::slice::from_ref(&previous), out)?;
        self.stats = stats;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::{Algorithm, Argon2, Params, Version};

    #[test]
    fn online_hash_matches_reference_across_boundaries_kernels_and_dirty_reuse() {
        for memory in [8, 33, 255, 4096] {
            for kernel in CpuKernel::ALL
                .into_iter()
                .filter(|k| k.is_supported() && *k != CpuKernel::Avx512256Stream)
            {
                for threshold in [1, 2, 3, 8, 16] {
                    let mut reduced =
                        ReducedMemoryContext::new(memory, kernel, threshold, memory as usize)
                            .unwrap();
                    for nonce in [0u64, 17, u64::MAX] {
                        let salt = [nonce as u8; 92];
                        let mut reference = [0; 32];
                        Argon2::new(
                            Algorithm::Argon2id,
                            Version::V0x13,
                            Params::new(memory, 1, 1, Some(32)).unwrap(),
                        )
                        .hash_password_into(&nonce.to_le_bytes(), &salt, &mut reference)
                        .unwrap();
                        let mut output = [0; 32];
                        let stats = reduced
                            .hash_password_into(&nonce.to_le_bytes(), &salt, &mut output)
                            .unwrap();
                        assert_eq!(
                            output, reference,
                            "memory={memory} kernel={kernel} threshold={threshold} nonce={nonce}"
                        );
                        assert!(stats.peak_reconstruction_depth <= threshold as usize);
                        if threshold == 1 {
                            assert_eq!(stats.compressions, (memory / 4 * 4 - 2) as u64);
                        } else if memory >= 255 {
                            assert!(stats.retained_blocks < (memory / 4 * 4) as usize);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn capacity_failure_is_bounded_and_context_can_be_reused() {
        let mut ctx = ReducedMemoryContext::new(4096, CpuKernel::Scalar, 2, 2).unwrap();
        for nonce in [0u64, 9] {
            assert_eq!(
                ctx.hash_password_into(&nonce.to_le_bytes(), &[0x37; 92], &mut [0; 32]),
                Err(ReducedMemoryError::CapacityExceeded)
            );
            assert_eq!(ctx.arena.iter().map(|c| c.len()).sum::<usize>(), 2);
        }
        assert!(ReducedMemoryContext::new(8, CpuKernel::Scalar, 0, 8).is_err());
        assert!(ReducedMemoryContext::new(8, CpuKernel::Scalar, 17, 8).is_err());
    }
    #[test]
    #[ignore = "full 2 GiB reference and reduced arenas; run optimized"]
    fn full_size_reduced_memory_parity() {
        let mut contexts = [2, 3].map(|threshold| {
            ReducedMemoryContext::new(2097152, CpuKernel::preferred(), threshold, 2097152 * 7 / 10)
                .unwrap()
        });
        for nonce in [0u64, 17, u64::MAX] {
            let salt = [0x37; 92];
            let mut expected = [0; 32];
            Argon2::new(
                Algorithm::Argon2id,
                Version::V0x13,
                Params::new(2097152, 1, 1, Some(32)).unwrap(),
            )
            .hash_password_into(&nonce.to_le_bytes(), &salt, &mut expected)
            .unwrap();
            for ctx in &mut contexts {
                let mut output = [0; 32];
                let stats = ctx
                    .hash_password_into(&nonce.to_le_bytes(), &salt, &mut output)
                    .unwrap();
                assert_eq!(
                    output, expected,
                    "nonce={nonce} threshold={}",
                    ctx.threshold
                );
                assert!(stats.allocated_bytes < 2097152 * 1024 * 7 / 10);
                println!(
                    "FULL_PARITY nonce={nonce} threshold={} bytes={} retained={} compressions={}",
                    ctx.threshold, stats.allocated_bytes, stats.retained_blocks, stats.compressions
                );
            }
        }
    }
}
