// Inserted into impl FixedArgon2id by prepare_kernel.py. Experimental only.
// The ring serves recent references until a whole batch of streaming writes is
// fenced. At cur % 64 == 0 all older arena references are therefore committed.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,avx512f,avx512vl")]
unsafe fn fill_streaming_ring(&self, memory: &mut [PowBlock]) {
    use std::arch::x86_64::{__m256i, _mm256_load_si256, _mm256_stream_si256, _mm_mfence};
    const RING: usize = 64;
    const MASK: usize = RING - 1;
    let mut ring = Box::new([PowBlock::default(); RING]);
    ring[0] = memory[0];
    ring[1] = memory[1];
    let arena = memory.as_mut_ptr();
    let cache = ring.as_mut_ptr();
    debug_assert_eq!(arena as usize % 64, 0);
    debug_assert_eq!(cache as usize % 64, 0);
    for slice in 0..SYNC_POINTS {
        let first = if slice == 0 { 2 } else { 0 };
        let di = slice < 2;
        let base = slice * self.segment_length;
        for block in first..self.segment_length {
            let cur = base + block;
            if cur & MASK == 0 {
                // MFENCE orders prior WC stores before subsequent arena loads.
                _mm_mfence();
            }
            let prev = &*cache.add((cur - 1) & MASK);
            let reference = if di {
                self.data_independent_ref_indexes[cur]
            } else {
                reference_index(cur - 1, prev.0[0])
            };
            debug_assert!(reference < cur - 1);
            // A distance of exactly RING aliases the output slot, so use the
            // already-fenced arena in that case, never the cached ring.
            let reference_ptr = if cur - reference < RING {
                cache.add(reference & MASK)
            } else {
                arena.add(reference)
            };
            let dst = cache.add(cur & MASK);
            debug_assert_ne!(dst, reference_ptr);
            if di {
                if block + 3 < self.segment_length {
                    let ahead = self.data_independent_ref_indexes[cur + 3];
                    // Recent references are already cached, or are not yet
                    // produced. Only issue an arena prefetch for older blocks.
                    if ahead < cur && cur - ahead >= RING {
                        prefetch_pow_block(&*arena.add(ahead));
                    }
                }
                avx512_256::compress_into(prev, &*reference_ptr, &mut *dst);
            } else if block + 1 < self.segment_length {
                avx512_256::compress_into_mid_prefetch_ring(
                    prev,
                    &*reference_ptr,
                    &mut *dst,
                    cur,
                    arena,
                    cache,
                    cur + 1,
                );
            } else {
                avx512_256::compress_into(prev, &*reference_ptr, &mut *dst);
            }
            // Contiguous stores fill each complete 64-byte cache line. The
            // arena is never used as a pre-round backup in this candidate.
            let src = (*dst).0.as_ptr();
            let output = (*arena.add(cur)).0.as_mut_ptr();
            for vector in 0..32 {
                let value = _mm256_load_si256(src.add(vector * 4).cast::<__m256i>());
                _mm256_stream_si256(output.add(vector * 4).cast::<__m256i>(), value);
            }
        }
    }
    _mm_mfence(); // Finalization consumes the final arena block.
}
