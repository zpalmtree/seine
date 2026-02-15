use std::convert::TryInto;
use std::num::Wrapping;

use blake2::digest::{Digest, VariableOutput};
use blake2::{Blake2b512, Blake2bVar};

const ARGON2_VERSION_13: u32 = 0x13;
const ARGON2_TYPE_ID: u32 = 2;
const ARGON2_LANES: u32 = 1;
const ARGON2_T_COST: u32 = 1;
#[allow(dead_code)]
const ISA_SCALAR: u8 = 0;
#[allow(dead_code)]
const ISA_AVX2: u8 = 1;
#[allow(dead_code)]
const ISA_AVX512: u8 = 2;
#[allow(dead_code)]
const ISA_NEON: u8 = 3;
const ADDRESSES_IN_BLOCK: usize = 128;
const SYNC_POINTS: usize = 4;
const MIN_PWD_LEN: usize = 0;
const MAX_PWD_LEN: usize = 0xFFFF_FFFF;
const MIN_SALT_LEN: usize = 8;
const MAX_SALT_LEN: usize = 0xFFFF_FFFF;
const MIN_OUTPUT_LEN: usize = 4;
const TRUNC: u64 = u32::MAX as u64;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum Error {
    PwdTooLong,
    SaltTooShort,
    SaltTooLong,
    OutputTooShort,
    OutputTooLong,
    MemoryTooLittle,
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub(super) struct FixedArgon2id {
    requested_memory_kib: u32,
    block_count: usize,
    segment_length: usize,
    data_independent_ref_indexes: Box<[usize]>,
}

impl FixedArgon2id {
    pub(super) fn new(requested_memory_kib: u32) -> Self {
        let lanes = ARGON2_LANES as usize;
        let min_blocks = 2 * SYNC_POINTS * lanes;
        let memory_blocks = requested_memory_kib.max(min_blocks as u32) as usize;
        let segment_length = memory_blocks / (lanes * SYNC_POINTS);
        let block_count = segment_length * lanes * SYNC_POINTS;
        let data_independent_ref_indexes =
            precompute_data_independent_ref_indexes(block_count, segment_length);
        Self {
            requested_memory_kib,
            block_count,
            segment_length,
            data_independent_ref_indexes,
        }
    }

    pub(super) const fn block_count(&self) -> usize {
        self.block_count
    }

    pub(super) fn hash_password_into_with_memory(
        &self,
        pwd: &[u8],
        salt: &[u8],
        out: &mut [u8],
        memory_blocks: &mut [PowBlock],
    ) -> Result<()> {
        verify_inputs(pwd, salt, out)?;
        let memory_blocks = memory_blocks
            .get_mut(..self.block_count)
            .ok_or(Error::MemoryTooLittle)?;

        let initial_hash = initial_hash(
            self.requested_memory_kib,
            ARGON2_T_COST,
            ARGON2_LANES,
            pwd,
            salt,
            out,
        );

        initialize_lane_blocks(memory_blocks, initial_hash)?;

        #[cfg(target_arch = "x86_64")]
        {
            if std::arch::is_x86_feature_detected!("avx512f")
                && std::arch::is_x86_feature_detected!("avx512vl")
            {
                self.fill_blocks::<ISA_AVX512>(memory_blocks);
            } else if std::arch::is_x86_feature_detected!("avx2") {
                self.fill_blocks::<ISA_AVX2>(memory_blocks);
            } else {
                self.fill_blocks::<ISA_SCALAR>(memory_blocks);
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            self.fill_blocks::<ISA_NEON>(memory_blocks);
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            self.fill_blocks::<ISA_SCALAR>(memory_blocks);
        }

        finalize(memory_blocks, out)
    }

    fn fill_blocks<const ISA: u8>(&self, memory_blocks: &mut [PowBlock]) {
        debug_assert_eq!(ARGON2_LANES, 1);
        debug_assert_eq!(memory_blocks.len(), self.block_count);
        for slice in 0..SYNC_POINTS {
            let data_independent_addressing = slice < (SYNC_POINTS / 2);

            let first_block = if slice == 0 { 2 } else { 0 };

            let mut cur_index = slice * self.segment_length + first_block;
            let mut prev_index = cur_index - 1;

            if data_independent_addressing {
                let ref_base = slice * self.segment_length;
                // Prefetch the first ref blocks before entering the loop.
                // AArch64 uses 3-ahead, so prime the first 3; others use 1-ahead.
                #[cfg(target_arch = "aarch64")]
                {
                    for pre in 0..3usize {
                        let idx = first_block + pre;
                        if idx < self.segment_length {
                            prefetch_pow_block(&memory_blocks[self.data_independent_ref_indexes[ref_base + idx]]);
                        }
                    }
                }
                #[cfg(not(target_arch = "aarch64"))]
                if first_block < self.segment_length {
                    prefetch_pow_block(&memory_blocks[self.data_independent_ref_indexes[ref_base + first_block]]);
                }
                for block in first_block..self.segment_length {
                    let ref_index = self.data_independent_ref_indexes[ref_base + block];
                    // Prefetch a future iteration's ref block while current
                    // compress runs.  On AArch64, look 3 iterations ahead to
                    // give the memory system more time; on x86_64, 1-ahead is
                    // sufficient given lower random-access latency.
                    #[cfg(target_arch = "aarch64")]
                    {
                        let ahead = block + 3;
                        if ahead < self.segment_length {
                            prefetch_pow_block(&memory_blocks[self.data_independent_ref_indexes[ref_base + ahead]]);
                        }
                    }
                    #[cfg(not(target_arch = "aarch64"))]
                    if block + 1 < self.segment_length {
                        prefetch_pow_block(&memory_blocks[self.data_independent_ref_indexes[ref_base + block + 1]]);
                    }
                    fill_block_from_refs::<ISA>(memory_blocks, prev_index, ref_index, cur_index);
                    prev_index = cur_index;
                    cur_index += 1;
                }
            } else {
                let slice_prefix = (slice * self.segment_length) - 1;
                for block in first_block..self.segment_length {
                    let rand = memory_blocks[prev_index].as_ref()[0];
                    let reference_area_size = slice_prefix + block;
                    let ref_index = reference_index(reference_area_size, rand);
                    debug_assert!(ref_index < self.block_count);

                    // On AArch64 with NEON, use mid-compress prefetch: after
                    // column round 0 writes dst[0] (its final value), compute
                    // the NEXT ref_index and prefetch it.  The remaining 7
                    // column rounds (~150 ns) overlap with the DRAM fetch,
                    // giving 25× more lead time than the post-compress prefetch.
                    #[cfg(target_arch = "aarch64")]
                    {
                        if ISA == ISA_NEON && block + 1 < self.segment_length {
                            let next_ref_area = slice_prefix + block + 1;
                            fill_block_from_refs_mid_prefetch(
                                memory_blocks, prev_index, ref_index, cur_index,
                                next_ref_area,
                            );
                        } else {
                            fill_block_from_refs::<ISA>(memory_blocks, prev_index, ref_index, cur_index);
                        }
                    }
                    #[cfg(not(target_arch = "aarch64"))]
                    {
                        fill_block_from_refs::<ISA>(memory_blocks, prev_index, ref_index, cur_index);

                        // Post-compress prefetch for non-AArch64.
                        if block + 1 < self.segment_length {
                            let next_rand = memory_blocks[cur_index].as_ref()[0];
                            let next_ref_area = slice_prefix + block + 1;
                            let next_ref = reference_index(next_ref_area, next_rand);
                            prefetch_pow_block(&memory_blocks[next_ref]);
                        }
                    }

                    prev_index = cur_index;
                    cur_index += 1;
                }
            }
        }
    }
}

fn precompute_data_independent_ref_indexes(
    block_count: usize,
    segment_length: usize,
) -> Box<[usize]> {
    debug_assert_eq!(ARGON2_LANES, 1);
    debug_assert!(ADDRESSES_IN_BLOCK.is_power_of_two());
    const ADDRESS_MASK: usize = ADDRESSES_IN_BLOCK - 1;

    let slice_count = SYNC_POINTS / 2;
    let mut refs = vec![0usize; slice_count * segment_length];
    let zero_block = PowBlock::default();

    for slice in 0..slice_count {
        let mut address_block = PowBlock::default();
        let mut input_block = PowBlock::default();
        input_block.as_mut()[..6].copy_from_slice(&[
            0,
            0,
            slice as u64,
            block_count as u64,
            ARGON2_T_COST as u64,
            ARGON2_TYPE_ID as u64,
        ]);

        let first_block = if slice == 0 {
            update_address_block::<ISA_SCALAR>(&mut address_block, &mut input_block, &zero_block);
            2
        } else {
            0
        };
        let mut address_idx = first_block & ADDRESS_MASK;
        let refs_base = slice * segment_length;
        let slice_prefix = (slice * segment_length).saturating_sub(1);

        for block in first_block..segment_length {
            if address_idx == 0 {
                update_address_block::<ISA_SCALAR>(
                    &mut address_block,
                    &mut input_block,
                    &zero_block,
                );
            }

            let rand = address_block.as_ref()[address_idx];
            address_idx = (address_idx + 1) & ADDRESS_MASK;

            let reference_area_size = if slice == 0 {
                block - 1
            } else {
                slice_prefix + block
            };
            let ref_index = reference_index(reference_area_size, rand);
            debug_assert!(ref_index < block_count);

            refs[refs_base + block] = ref_index;
        }
    }

    refs.into_boxed_slice()
}

#[inline(always)]
fn reference_index(reference_area_size: usize, rand: u64) -> usize {
    debug_assert!(reference_area_size > 0);
    let mapped = ((rand as u32 as u64) * (rand as u32 as u64)) >> 32;
    let offset = ((reference_area_size as u64 * mapped) >> 32) as usize;
    (reference_area_size - 1) - offset
}

#[inline(always)]
fn fill_block_from_refs<const ISA: u8>(
    memory_blocks: &mut [PowBlock],
    prev_index: usize,
    ref_index: usize,
    cur_index: usize,
) {
    debug_assert!(prev_index < memory_blocks.len());
    debug_assert!(ref_index < memory_blocks.len());
    debug_assert!(cur_index < memory_blocks.len());
    debug_assert_ne!(cur_index, prev_index);
    debug_assert_ne!(cur_index, ref_index);

    // Safety: caller guarantees these indices are in-bounds and cur_index does not
    // alias prev_index or ref_index (dst always advances forward past both sources).
    // We use raw pointer arithmetic to obtain the three disjoint references without
    // triggering the borrow checker's simultaneous immutable+mutable restriction.
    unsafe {
        let base = memory_blocks.as_mut_ptr();
        let prev = &*base.add(prev_index);
        let refb = &*base.add(ref_index);
        let dst = &mut *base.add(cur_index);
        compress_into::<ISA>(prev, refb, dst);
    }
}

/// Like [`fill_block_from_refs`] but uses the mid-compress prefetch variant
/// on AArch64 NEON.  After column round 0 writes `dst[0]`, it computes the
/// next iteration's ref_index and prefetches the target block, overlapping
/// ~150 ns of remaining column-round compute with the DRAM fetch.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn fill_block_from_refs_mid_prefetch(
    memory_blocks: &mut [PowBlock],
    prev_index: usize,
    ref_index: usize,
    cur_index: usize,
    next_ref_area_size: usize,
) {
    debug_assert!(prev_index < memory_blocks.len());
    debug_assert!(ref_index < memory_blocks.len());
    debug_assert!(cur_index < memory_blocks.len());
    debug_assert_ne!(cur_index, prev_index);
    debug_assert_ne!(cur_index, ref_index);

    unsafe {
        let base = memory_blocks.as_mut_ptr();
        let prev = &*base.add(prev_index);
        let refb = &*base.add(ref_index);
        let dst = &mut *base.add(cur_index);
        compress_neon_into_mid_prefetch(
            prev,
            refb,
            dst,
            next_ref_area_size,
            base as *const PowBlock,
        );
    }
}

/// Prefetch a PowBlock into cache.  Issues two prefetches to resolve the TLB
/// entry and prime the hardware prefetcher for the full 1 KiB block.
///
/// - x86_64: offsets 0 and 512 (64-byte cache lines).
/// - AArch64: all 8 cache lines at 128-byte stride (full 1024-byte block).
#[inline(always)]
fn prefetch_pow_block(block: &PowBlock) {
    #[cfg(target_arch = "x86_64")]
    {
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            let ptr = block as *const PowBlock as *const i8;
            _mm_prefetch(ptr, _MM_HINT_T0);
            _mm_prefetch(ptr.add(512), _MM_HINT_T0);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe {
            let ptr = block as *const PowBlock as *const u8;
            std::arch::asm!(
                "prfm pldl1keep, [{ptr}]",
                "prfm pldl1keep, [{ptr}, #128]",
                "prfm pldl1keep, [{ptr}, #256]",
                "prfm pldl1keep, [{ptr}, #384]",
                "prfm pldl1keep, [{ptr}, #512]",
                "prfm pldl1keep, [{ptr}, #640]",
                "prfm pldl1keep, [{ptr}, #768]",
                "prfm pldl1keep, [{ptr}, #896]",
                ptr = in(reg) ptr,
                options(nostack, preserves_flags),
            );
        }
    }
}

#[inline(always)]
fn verify_inputs(pwd: &[u8], salt: &[u8], out: &[u8]) -> Result<()> {
    if pwd.len() < MIN_PWD_LEN || pwd.len() > MAX_PWD_LEN {
        return Err(Error::PwdTooLong);
    }
    if salt.len() < MIN_SALT_LEN {
        return Err(Error::SaltTooShort);
    }
    if salt.len() > MAX_SALT_LEN {
        return Err(Error::SaltTooLong);
    }
    if out.len() < MIN_OUTPUT_LEN {
        return Err(Error::OutputTooShort);
    }
    if out.len() > u32::MAX as usize {
        return Err(Error::OutputTooLong);
    }
    Ok(())
}

fn initial_hash(
    memory_kib: u32,
    t_cost: u32,
    lanes: u32,
    pwd: &[u8],
    salt: &[u8],
    out: &[u8],
) -> [u8; 64] {
    let mut digest = Blake2b512::new();
    Digest::update(&mut digest, lanes.to_le_bytes());
    Digest::update(&mut digest, (out.len() as u32).to_le_bytes());
    Digest::update(&mut digest, memory_kib.to_le_bytes());
    Digest::update(&mut digest, t_cost.to_le_bytes());
    Digest::update(&mut digest, ARGON2_VERSION_13.to_le_bytes());
    Digest::update(&mut digest, ARGON2_TYPE_ID.to_le_bytes());
    Digest::update(&mut digest, (pwd.len() as u32).to_le_bytes());
    Digest::update(&mut digest, pwd);
    Digest::update(&mut digest, (salt.len() as u32).to_le_bytes());
    Digest::update(&mut digest, salt);
    Digest::update(&mut digest, 0u32.to_le_bytes());
    Digest::update(&mut digest, 0u32.to_le_bytes());
    let output = digest.finalize();
    output.into()
}

fn initialize_lane_blocks(memory_blocks: &mut [PowBlock], initial_hash: [u8; 64]) -> Result<()> {
    for (idx, block) in memory_blocks.iter_mut().take(2).enumerate() {
        let mut hash = [0u8; PowBlock::SIZE];
        let i = idx as u32;
        let lane = 0u32;
        blake2b_long(
            &[&initial_hash, &i.to_le_bytes(), &lane.to_le_bytes()],
            &mut hash,
        )?;
        block.load(&hash);
    }
    Ok(())
}

fn finalize(memory_blocks: &[PowBlock], out: &mut [u8]) -> Result<()> {
    let Some(last_block) = memory_blocks.last() else {
        return Err(Error::MemoryTooLittle);
    };
    let mut blockhash_bytes = [0u8; PowBlock::SIZE];
    for (chunk, value) in blockhash_bytes.chunks_mut(8).zip(last_block.iter()) {
        chunk.copy_from_slice(&value.to_le_bytes());
    }
    blake2b_long(&[&blockhash_bytes], out)
}

#[inline(always)]
fn update_address_block<const ISA: u8>(
    address_block: &mut PowBlock,
    input_block: &mut PowBlock,
    zero_block: &PowBlock,
) {
    input_block.as_mut()[6] = input_block.as_mut()[6].wrapping_add(1);
    *address_block = compress::<ISA>(zero_block, input_block);
    *address_block = compress::<ISA>(zero_block, address_block);
}

#[inline(always)]
fn compress<const ISA: u8>(rhs: &PowBlock, lhs: &PowBlock) -> PowBlock {
    #[cfg(target_arch = "x86_64")]
    {
        if ISA == ISA_AVX512 {
            unsafe {
                return compress_avx512(rhs, lhs);
            }
        }
        if ISA == ISA_AVX2 {
            unsafe {
                return compress_avx2(rhs, lhs);
            }
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if ISA == ISA_NEON {
            unsafe {
                return compress_neon(rhs, lhs);
            }
        }
    }
    PowBlock::compress(rhs, lhs)
}

/// In-place variant of [`compress`] that writes the result directly into `dst`,
/// bypassing the ABI return-value memcpy that `#[target_feature]` functions incur.
///
/// # Safety (caller obligations)
/// `dst` must not alias `rhs` or `lhs`.
#[inline(always)]
fn compress_into<const ISA: u8>(rhs: &PowBlock, lhs: &PowBlock, dst: &mut PowBlock) {
    #[cfg(target_arch = "x86_64")]
    {
        if ISA == ISA_AVX512 {
            unsafe {
                compress_avx512_into(rhs, lhs, dst);
                return;
            }
        }
        if ISA == ISA_AVX2 {
            unsafe {
                compress_avx2_into(rhs, lhs, dst);
                return;
            }
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if ISA == ISA_NEON {
            unsafe {
                compress_neon_into(rhs, lhs, dst);
            }
            return;
        }
    }
    *dst = PowBlock::compress(rhs, lhs);
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn avx2_blamka(
    a: std::arch::x86_64::__m256i,
    b: std::arch::x86_64::__m256i,
) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::{_mm256_add_epi64, _mm256_mul_epu32};
    let product = _mm256_mul_epu32(a, b);
    let doubled_product = _mm256_add_epi64(product, product);
    _mm256_add_epi64(_mm256_add_epi64(a, b), doubled_product)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn avx2_rotr_64_32(value: std::arch::x86_64::__m256i) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::{_mm256_or_si256, _mm256_slli_epi64, _mm256_srli_epi64};
    _mm256_or_si256(
        _mm256_srli_epi64::<32>(value),
        _mm256_slli_epi64::<32>(value),
    )
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn avx2_rotr_64_24(value: std::arch::x86_64::__m256i) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::{_mm256_or_si256, _mm256_slli_epi64, _mm256_srli_epi64};
    _mm256_or_si256(
        _mm256_srli_epi64::<24>(value),
        _mm256_slli_epi64::<40>(value),
    )
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn avx2_rotr_64_16(value: std::arch::x86_64::__m256i) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::{_mm256_or_si256, _mm256_slli_epi64, _mm256_srli_epi64};
    _mm256_or_si256(
        _mm256_srli_epi64::<16>(value),
        _mm256_slli_epi64::<48>(value),
    )
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn avx2_rotr_64_63(value: std::arch::x86_64::__m256i) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::{_mm256_or_si256, _mm256_slli_epi64, _mm256_srli_epi64};
    _mm256_or_si256(
        _mm256_srli_epi64::<63>(value),
        _mm256_slli_epi64::<1>(value),
    )
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn avx2_round(
    a: &mut std::arch::x86_64::__m256i,
    b: &mut std::arch::x86_64::__m256i,
    c: &mut std::arch::x86_64::__m256i,
    d: &mut std::arch::x86_64::__m256i,
) {
    use std::arch::x86_64::{_mm256_permute4x64_epi64, _mm256_xor_si256};

    *a = avx2_blamka(*a, *b);
    *d = avx2_rotr_64_32(_mm256_xor_si256(*d, *a));
    *c = avx2_blamka(*c, *d);
    *b = avx2_rotr_64_24(_mm256_xor_si256(*b, *c));
    *a = avx2_blamka(*a, *b);
    *d = avx2_rotr_64_16(_mm256_xor_si256(*d, *a));
    *c = avx2_blamka(*c, *d);
    *b = avx2_rotr_64_63(_mm256_xor_si256(*b, *c));

    let mut bb = _mm256_permute4x64_epi64::<0x39>(*b);
    let mut cc = _mm256_permute4x64_epi64::<0x4E>(*c);
    let mut dd = _mm256_permute4x64_epi64::<0x93>(*d);

    *a = avx2_blamka(*a, bb);
    dd = avx2_rotr_64_32(_mm256_xor_si256(dd, *a));
    cc = avx2_blamka(cc, dd);
    bb = avx2_rotr_64_24(_mm256_xor_si256(bb, cc));
    *a = avx2_blamka(*a, bb);
    dd = avx2_rotr_64_16(_mm256_xor_si256(dd, *a));
    cc = avx2_blamka(cc, dd);
    bb = avx2_rotr_64_63(_mm256_xor_si256(bb, cc));

    *b = _mm256_permute4x64_epi64::<0x93>(bb);
    *c = _mm256_permute4x64_epi64::<0x4E>(cc);
    *d = _mm256_permute4x64_epi64::<0x39>(dd);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn compress_avx2(rhs: &PowBlock, lhs: &PowBlock) -> PowBlock {
    use std::arch::x86_64::{
        __m256i, _mm256_loadu_si256, _mm256_set_epi64x, _mm256_storeu_si256, _mm256_xor_si256,
    };

    let mut r = PowBlock::default();
    let mut q = PowBlock::default();

    let rhs_ptr = rhs.0.as_ptr();
    let lhs_ptr = lhs.0.as_ptr();
    let r_ptr = r.0.as_mut_ptr();
    let q_ptr = q.0.as_mut_ptr();

    for vec_idx in 0..(PowBlock::SIZE / 32) {
        let offset = vec_idx * 4;
        let rv = _mm256_xor_si256(
            _mm256_loadu_si256(rhs_ptr.add(offset) as *const __m256i),
            _mm256_loadu_si256(lhs_ptr.add(offset) as *const __m256i),
        );
        _mm256_storeu_si256(r_ptr.add(offset) as *mut __m256i, rv);
        _mm256_storeu_si256(q_ptr.add(offset) as *mut __m256i, rv);
    }

    for row in 0..8 {
        let base = row * 16;
        let mut a = _mm256_loadu_si256(q_ptr.add(base) as *const __m256i);
        let mut b = _mm256_loadu_si256(q_ptr.add(base + 4) as *const __m256i);
        let mut c = _mm256_loadu_si256(q_ptr.add(base + 8) as *const __m256i);
        let mut d = _mm256_loadu_si256(q_ptr.add(base + 12) as *const __m256i);
        avx2_round(&mut a, &mut b, &mut c, &mut d);
        _mm256_storeu_si256(q_ptr.add(base) as *mut __m256i, a);
        _mm256_storeu_si256(q_ptr.add(base + 4) as *mut __m256i, b);
        _mm256_storeu_si256(q_ptr.add(base + 8) as *mut __m256i, c);
        _mm256_storeu_si256(q_ptr.add(base + 12) as *mut __m256i, d);
    }

    for idx in 0..8 {
        let base = idx * 2;

        let mut a = _mm256_set_epi64x(
            q.0[base + 17] as i64,
            q.0[base + 16] as i64,
            q.0[base + 1] as i64,
            q.0[base] as i64,
        );
        let mut b = _mm256_set_epi64x(
            q.0[base + 49] as i64,
            q.0[base + 48] as i64,
            q.0[base + 33] as i64,
            q.0[base + 32] as i64,
        );
        let mut c = _mm256_set_epi64x(
            q.0[base + 81] as i64,
            q.0[base + 80] as i64,
            q.0[base + 65] as i64,
            q.0[base + 64] as i64,
        );
        let mut d = _mm256_set_epi64x(
            q.0[base + 113] as i64,
            q.0[base + 112] as i64,
            q.0[base + 97] as i64,
            q.0[base + 96] as i64,
        );

        avx2_round(&mut a, &mut b, &mut c, &mut d);

        let mut aa = [0u64; 4];
        let mut bb = [0u64; 4];
        let mut cc = [0u64; 4];
        let mut dd = [0u64; 4];
        _mm256_storeu_si256(aa.as_mut_ptr() as *mut __m256i, a);
        _mm256_storeu_si256(bb.as_mut_ptr() as *mut __m256i, b);
        _mm256_storeu_si256(cc.as_mut_ptr() as *mut __m256i, c);
        _mm256_storeu_si256(dd.as_mut_ptr() as *mut __m256i, d);

        q.0[base] = aa[0];
        q.0[base + 1] = aa[1];
        q.0[base + 16] = aa[2];
        q.0[base + 17] = aa[3];
        q.0[base + 32] = bb[0];
        q.0[base + 33] = bb[1];
        q.0[base + 48] = bb[2];
        q.0[base + 49] = bb[3];
        q.0[base + 64] = cc[0];
        q.0[base + 65] = cc[1];
        q.0[base + 80] = cc[2];
        q.0[base + 81] = cc[3];
        q.0[base + 96] = dd[0];
        q.0[base + 97] = dd[1];
        q.0[base + 112] = dd[2];
        q.0[base + 113] = dd[3];
    }

    for vec_idx in 0..(PowBlock::SIZE / 32) {
        let offset = vec_idx * 4;
        let qv = _mm256_loadu_si256(q_ptr.add(offset) as *const __m256i);
        let rv = _mm256_loadu_si256(r_ptr.add(offset) as *const __m256i);
        _mm256_storeu_si256(q_ptr.add(offset) as *mut __m256i, _mm256_xor_si256(qv, rv));
    }

    q
}

/// In-place AVX2 block compression: writes result directly into `dst`, eliminating the
/// 1 KiB ABI return-value memcpy that the by-value `compress_avx2` incurs per call.
///
/// # Safety
/// * Requires AVX2.
/// * `dst` must not alias `rhs` or `lhs`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn compress_avx2_into(rhs: &PowBlock, lhs: &PowBlock, dst: &mut PowBlock) {
    use std::arch::x86_64::{
        __m128i, __m256i, _mm256_castsi128_si256, _mm256_castsi256_si128,
        _mm256_extracti128_si256, _mm256_inserti128_si256, _mm256_loadu_si256,
        _mm256_storeu_si256, _mm256_xor_si256, _mm_loadu_si128, _mm_storeu_si128,
        _mm_xor_si128,
    };

    // q lives on the stack as the working buffer for round permutations.
    // dst doubles as the pre-round XOR backup (r), eliminating the second stack buffer.
    let mut q = PowBlock::default();

    let rhs_ptr = rhs.0.as_ptr();
    let lhs_ptr = lhs.0.as_ptr();
    let dst_ptr = dst.0.as_mut_ptr();
    let q_ptr = q.0.as_mut_ptr();

    // Phase 1: XOR rhs ^ lhs → store to both dst (pre-round backup) and q (working copy).
    for vec_idx in 0..(PowBlock::SIZE / 32) {
        let offset = vec_idx * 4;
        let rv = _mm256_xor_si256(
            _mm256_loadu_si256(rhs_ptr.add(offset) as *const __m256i),
            _mm256_loadu_si256(lhs_ptr.add(offset) as *const __m256i),
        );
        _mm256_storeu_si256(dst_ptr.add(offset) as *mut __m256i, rv);
        _mm256_storeu_si256(q_ptr.add(offset) as *mut __m256i, rv);
    }

    // Phase 2: Row rounds on q (contiguous 16-element rows).
    for row in 0..8 {
        let base = row * 16;
        let mut a = _mm256_loadu_si256(q_ptr.add(base) as *const __m256i);
        let mut b = _mm256_loadu_si256(q_ptr.add(base + 4) as *const __m256i);
        let mut c = _mm256_loadu_si256(q_ptr.add(base + 8) as *const __m256i);
        let mut d = _mm256_loadu_si256(q_ptr.add(base + 12) as *const __m256i);
        avx2_round(&mut a, &mut b, &mut c, &mut d);
        _mm256_storeu_si256(q_ptr.add(base) as *mut __m256i, a);
        _mm256_storeu_si256(q_ptr.add(base + 4) as *mut __m256i, b);
        _mm256_storeu_si256(q_ptr.add(base + 8) as *mut __m256i, c);
        _mm256_storeu_si256(q_ptr.add(base + 12) as *mut __m256i, d);
    }

    // Phase 3: Column rounds — gather from q via 128-bit pair loads, compute the
    // round, then scatter results directly into dst XOR'd with the pre-round backup
    // that dst still holds.  This fuses the column scatter with the final XOR,
    // eliminating the separate Phase-4 pass over both buffers.
    for idx in 0..8 {
        let base = idx * 2;

        // Gather: 128-bit pair loads from q, combined into 256-bit registers.
        // Each pair (q[off], q[off+1]) is contiguous, pairs separated by stride 16.
        let mut a = _mm256_inserti128_si256::<1>(
            _mm256_castsi128_si256(_mm_loadu_si128(q_ptr.add(base) as *const __m128i)),
            _mm_loadu_si128(q_ptr.add(base + 16) as *const __m128i),
        );
        let mut b = _mm256_inserti128_si256::<1>(
            _mm256_castsi128_si256(_mm_loadu_si128(q_ptr.add(base + 32) as *const __m128i)),
            _mm_loadu_si128(q_ptr.add(base + 48) as *const __m128i),
        );
        let mut c = _mm256_inserti128_si256::<1>(
            _mm256_castsi128_si256(_mm_loadu_si128(q_ptr.add(base + 64) as *const __m128i)),
            _mm_loadu_si128(q_ptr.add(base + 80) as *const __m128i),
        );
        let mut d = _mm256_inserti128_si256::<1>(
            _mm256_castsi128_si256(_mm_loadu_si128(q_ptr.add(base + 96) as *const __m128i)),
            _mm_loadu_si128(q_ptr.add(base + 112) as *const __m128i),
        );

        avx2_round(&mut a, &mut b, &mut c, &mut d);

        // Fused scatter + final XOR: for each 128-bit pair of the column result,
        // load the corresponding pre-round pair from dst, XOR, and write back.
        macro_rules! scatter_xor_pair {
            ($vec:expr, $lo_off:expr, $hi_off:expr) => {{
                let lo = _mm256_castsi256_si128($vec);
                let hi = _mm256_extracti128_si256::<1>($vec);
                let d_lo = _mm_loadu_si128(dst_ptr.add($lo_off) as *const __m128i);
                let d_hi = _mm_loadu_si128(dst_ptr.add($hi_off) as *const __m128i);
                _mm_storeu_si128(
                    dst_ptr.add($lo_off) as *mut __m128i,
                    _mm_xor_si128(lo, d_lo),
                );
                _mm_storeu_si128(
                    dst_ptr.add($hi_off) as *mut __m128i,
                    _mm_xor_si128(hi, d_hi),
                );
            }};
        }

        scatter_xor_pair!(a, base, base + 16);
        scatter_xor_pair!(b, base + 32, base + 48);
        scatter_xor_pair!(c, base + 64, base + 80);
        scatter_xor_pair!(d, base + 96, base + 112);
    }
    // No Phase 4 needed — final XOR was fused into the column scatter above.
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512vl")]
unsafe fn compress_avx512(rhs: &PowBlock, lhs: &PowBlock) -> PowBlock {
    // Keep AVX-512 dispatch wired while reusing the known-correct scalar transform.
    PowBlock::compress(rhs, lhs)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512vl")]
unsafe fn compress_avx512_into(rhs: &PowBlock, lhs: &PowBlock, dst: &mut PowBlock) {
    *dst = PowBlock::compress(rhs, lhs);
}

// ---------------------------------------------------------------------------
// AArch64 NEON SIMD path
// ---------------------------------------------------------------------------
//
// NEON is mandatory on AArch64, so no runtime feature detection or
// `#[target_feature]` is needed. Functions inline freely — no ABI memcpy.

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn neon_blamka(
    a: std::arch::aarch64::uint64x2_t,
    b: std::arch::aarch64::uint64x2_t,
) -> std::arch::aarch64::uint64x2_t {
    // a + b + 2*(lo32(a)*lo32(b))  =  (a+b) + lo(a)*lo(b) + lo(a)*lo(b)
    // Use inline asm to force UMLAL.2D (widening multiply-accumulate).
    // LLVM decomposes vmlal_u32 intrinsics into UMULL+ADD, missing the fused
    // instruction.  The double-UMLAL has fast accumulator forwarding on Apple
    // Silicon, making it faster than the theoretically shorter UMULL+SHL+ADD.
    let out: std::arch::aarch64::uint64x2_t;
    std::arch::asm!(
        "xtn  {lo_a}.2s, {a:v}.2d",
        "xtn  {lo_b}.2s, {b:v}.2d",
        "add  {out:v}.2d, {a:v}.2d, {b:v}.2d",
        "umlal {out:v}.2d, {lo_a}.2s, {lo_b}.2s",
        "umlal {out:v}.2d, {lo_a}.2s, {lo_b}.2s",
        a = in(vreg) a,
        b = in(vreg) b,
        lo_a = out(vreg) _,
        lo_b = out(vreg) _,
        out = out(vreg) out,
        options(pure, nomem, nostack, preserves_flags),
    );
    out
}

/// Full BLAMKA round on 16 u64 elements packed as 4 lo/hi NEON register pairs.
///
/// Performs both the column step and the diagonal step, matching the scalar
/// `permute!` macro semantics. The diagonal step rotates b/c/d lane assignments
/// via `vextq_u64` to achieve cross-lane permutation without scatter/gather.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn neon_round(
    a_lo: &mut std::arch::aarch64::uint64x2_t,
    a_hi: &mut std::arch::aarch64::uint64x2_t,
    b_lo: &mut std::arch::aarch64::uint64x2_t,
    b_hi: &mut std::arch::aarch64::uint64x2_t,
    c_lo: &mut std::arch::aarch64::uint64x2_t,
    c_hi: &mut std::arch::aarch64::uint64x2_t,
    d_lo: &mut std::arch::aarch64::uint64x2_t,
    d_hi: &mut std::arch::aarch64::uint64x2_t,
) {
    use std::arch::aarch64::{vextq_u64, vxarq_u64};

    // Column step: explicitly interleave lo and hi half-rounds so the OOO
    // core sees both independent chains early in the decode window, rather
    // than serializing one chain then the other (as the compiler tends to do
    // when two sequential neon_half_round calls are inlined).
    //
    // G step 1: a = a + b + 2*lo32(a)*lo32(b);  d = rotr32(d ^ a)
    *a_lo = neon_blamka(*a_lo, *b_lo);
    *a_hi = neon_blamka(*a_hi, *b_hi);
    *d_lo = vxarq_u64::<32>(*d_lo, *a_lo);
    *d_hi = vxarq_u64::<32>(*d_hi, *a_hi);
    // G step 2: c = c + d + 2*lo32(c)*lo32(d);  b = rotr24(b ^ c)
    *c_lo = neon_blamka(*c_lo, *d_lo);
    *c_hi = neon_blamka(*c_hi, *d_hi);
    *b_lo = vxarq_u64::<24>(*b_lo, *c_lo);
    *b_hi = vxarq_u64::<24>(*b_hi, *c_hi);
    // G step 3: a = a + b + 2*lo32(a)*lo32(b);  d = rotr16(d ^ a)
    *a_lo = neon_blamka(*a_lo, *b_lo);
    *a_hi = neon_blamka(*a_hi, *b_hi);
    *d_lo = vxarq_u64::<16>(*d_lo, *a_lo);
    *d_hi = vxarq_u64::<16>(*d_hi, *a_hi);
    // G step 4: c = c + d + 2*lo32(c)*lo32(d);  b = rotr63(b ^ c)
    *c_lo = neon_blamka(*c_lo, *d_lo);
    *c_hi = neon_blamka(*c_hi, *d_hi);
    *b_lo = vxarq_u64::<63>(*b_lo, *c_lo);
    *b_hi = vxarq_u64::<63>(*b_hi, *c_hi);

    // Diagonal step: rotate b by 1, c by 2, d by 3 across the 4-wide logical
    // vector, then run the half-rounds interleaved, then un-rotate.
    let mut bb_lo = vextq_u64::<1>(*b_lo, *b_hi);
    let mut bb_hi = vextq_u64::<1>(*b_hi, *b_lo);
    let mut cc_lo = *c_hi;
    let mut cc_hi = *c_lo;
    let mut dd_lo = vextq_u64::<1>(*d_hi, *d_lo);
    let mut dd_hi = vextq_u64::<1>(*d_lo, *d_hi);

    // G step 1
    *a_lo = neon_blamka(*a_lo, bb_lo);
    *a_hi = neon_blamka(*a_hi, bb_hi);
    dd_lo = vxarq_u64::<32>(dd_lo, *a_lo);
    dd_hi = vxarq_u64::<32>(dd_hi, *a_hi);
    // G step 2
    cc_lo = neon_blamka(cc_lo, dd_lo);
    cc_hi = neon_blamka(cc_hi, dd_hi);
    bb_lo = vxarq_u64::<24>(bb_lo, cc_lo);
    bb_hi = vxarq_u64::<24>(bb_hi, cc_hi);
    // G step 3
    *a_lo = neon_blamka(*a_lo, bb_lo);
    *a_hi = neon_blamka(*a_hi, bb_hi);
    dd_lo = vxarq_u64::<16>(dd_lo, *a_lo);
    dd_hi = vxarq_u64::<16>(dd_hi, *a_hi);
    // G step 4
    cc_lo = neon_blamka(cc_lo, dd_lo);
    cc_hi = neon_blamka(cc_hi, dd_hi);
    bb_lo = vxarq_u64::<63>(bb_lo, cc_lo);
    bb_hi = vxarq_u64::<63>(bb_hi, cc_hi);

    // Un-rotate.
    *b_lo = vextq_u64::<1>(bb_hi, bb_lo);
    *b_hi = vextq_u64::<1>(bb_lo, bb_hi);
    *c_lo = cc_hi;
    *c_hi = cc_lo;
    *d_lo = vextq_u64::<1>(dd_lo, dd_hi);
    *d_hi = vextq_u64::<1>(dd_hi, dd_lo);
}

/// By-value NEON compress (used in `update_address_block` where dst may alias lhs).
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn compress_neon(rhs: &PowBlock, lhs: &PowBlock) -> PowBlock {
    let mut result = PowBlock::default();
    compress_neon_into(rhs, lhs, &mut result);
    result
}

/// In-place NEON block compression with fused phases.
///
/// Fuses Phase 1 (XOR) with Phase 2 (row rounds) and Phase 3 (column rounds)
/// with Phase 4 (final XOR).  This eliminates 128 stores to q in Phase 1 and
/// 128 loads from q in Phase 2/4, reducing total memory ops by ~36%.
///
/// # Safety
/// `dst` must not alias `rhs` or `lhs`.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn compress_neon_into(rhs: &PowBlock, lhs: &PowBlock, dst: &mut PowBlock) {
    use std::arch::aarch64::{uint64x2_t, veorq_u64, vld1q_u64, vst1q_u64};

    // q is the working buffer between row and column rounds.
    // Use MaybeUninit — the fused Phase 1+2 loop fully writes q.
    let mut q = std::mem::MaybeUninit::<PowBlock>::uninit();

    let rhs_ptr = rhs.0.as_ptr();
    let lhs_ptr = lhs.0.as_ptr();
    let dst_ptr = dst.0.as_mut_ptr();
    let q_ptr = q.as_mut_ptr() as *mut u64;

    // Fused Phase 1+2: XOR rhs^lhs directly into registers, store backup to
    // dst, row-round in registers, store result to q.  Eliminates the separate
    // Phase 1 store-to-q and Phase 2 load-from-q round-trip.
    for row in 0..8 {
        let base = row * 16;
        let mut a_lo: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base)), vld1q_u64(lhs_ptr.add(base)));
        let mut a_hi: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 2)), vld1q_u64(lhs_ptr.add(base + 2)));
        let mut b_lo: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 4)), vld1q_u64(lhs_ptr.add(base + 4)));
        let mut b_hi: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 6)), vld1q_u64(lhs_ptr.add(base + 6)));
        let mut c_lo: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 8)), vld1q_u64(lhs_ptr.add(base + 8)));
        let mut c_hi: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 10)), vld1q_u64(lhs_ptr.add(base + 10)));
        let mut d_lo: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 12)), vld1q_u64(lhs_ptr.add(base + 12)));
        let mut d_hi: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 14)), vld1q_u64(lhs_ptr.add(base + 14)));

        // Store pre-round XOR to dst as backup for Phase 4.
        vst1q_u64(dst_ptr.add(base), a_lo);
        vst1q_u64(dst_ptr.add(base + 2), a_hi);
        vst1q_u64(dst_ptr.add(base + 4), b_lo);
        vst1q_u64(dst_ptr.add(base + 6), b_hi);
        vst1q_u64(dst_ptr.add(base + 8), c_lo);
        vst1q_u64(dst_ptr.add(base + 10), c_hi);
        vst1q_u64(dst_ptr.add(base + 12), d_lo);
        vst1q_u64(dst_ptr.add(base + 14), d_hi);

        // Row round in registers — no load/store needed.
        neon_round(
            &mut a_lo, &mut a_hi,
            &mut b_lo, &mut b_hi,
            &mut c_lo, &mut c_hi,
            &mut d_lo, &mut d_hi,
        );

        // Store row-rounded result to q for column rounds.
        vst1q_u64(q_ptr.add(base), a_lo);
        vst1q_u64(q_ptr.add(base + 2), a_hi);
        vst1q_u64(q_ptr.add(base + 4), b_lo);
        vst1q_u64(q_ptr.add(base + 6), b_hi);
        vst1q_u64(q_ptr.add(base + 8), c_lo);
        vst1q_u64(q_ptr.add(base + 10), c_hi);
        vst1q_u64(q_ptr.add(base + 12), d_lo);
        vst1q_u64(q_ptr.add(base + 14), d_hi);
    }

    // Fused Phase 3+4: Column-round q, then XOR with dst backup and store
    // directly to dst.  Eliminates the separate Phase 3 store-to-q and
    // Phase 4 load-from-q round-trip.
    for idx in 0..8 {
        let base = idx * 2;
        let mut a_lo: uint64x2_t = vld1q_u64(q_ptr.add(base));
        let mut a_hi: uint64x2_t = vld1q_u64(q_ptr.add(base + 16));
        let mut b_lo: uint64x2_t = vld1q_u64(q_ptr.add(base + 32));
        let mut b_hi: uint64x2_t = vld1q_u64(q_ptr.add(base + 48));
        let mut c_lo: uint64x2_t = vld1q_u64(q_ptr.add(base + 64));
        let mut c_hi: uint64x2_t = vld1q_u64(q_ptr.add(base + 80));
        let mut d_lo: uint64x2_t = vld1q_u64(q_ptr.add(base + 96));
        let mut d_hi: uint64x2_t = vld1q_u64(q_ptr.add(base + 112));

        neon_round(
            &mut a_lo, &mut a_hi,
            &mut b_lo, &mut b_hi,
            &mut c_lo, &mut c_hi,
            &mut d_lo, &mut d_hi,
        );

        // XOR column-rounded result with pre-round backup in dst, store to dst.
        vst1q_u64(dst_ptr.add(base), veorq_u64(a_lo, vld1q_u64(dst_ptr.add(base))));
        vst1q_u64(dst_ptr.add(base + 16), veorq_u64(a_hi, vld1q_u64(dst_ptr.add(base + 16))));
        vst1q_u64(dst_ptr.add(base + 32), veorq_u64(b_lo, vld1q_u64(dst_ptr.add(base + 32))));
        vst1q_u64(dst_ptr.add(base + 48), veorq_u64(b_hi, vld1q_u64(dst_ptr.add(base + 48))));
        vst1q_u64(dst_ptr.add(base + 64), veorq_u64(c_lo, vld1q_u64(dst_ptr.add(base + 64))));
        vst1q_u64(dst_ptr.add(base + 80), veorq_u64(c_hi, vld1q_u64(dst_ptr.add(base + 80))));
        vst1q_u64(dst_ptr.add(base + 96), veorq_u64(d_lo, vld1q_u64(dst_ptr.add(base + 96))));
        vst1q_u64(dst_ptr.add(base + 112), veorq_u64(d_hi, vld1q_u64(dst_ptr.add(base + 112))));
    }
}

/// Like [`compress_neon_into`] but issues a prefetch for the NEXT iteration's
/// ref block after column round 0 completes.  At that point `dst[0]` has its
/// final value, giving ~150 ns of DRAM lead time instead of the ~6 ns that
/// the post-compress prefetch provides.
///
/// Used for data-dependent slices (2, 3) where the next ref_index depends on
/// the current block's output.
///
/// # Safety
/// Same as `compress_neon_into`.  Additionally, `memory_blocks_base` must
/// point to a valid PowBlock array of at least `block_count` elements.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn compress_neon_into_mid_prefetch(
    rhs: &PowBlock,
    lhs: &PowBlock,
    dst: &mut PowBlock,
    next_ref_area_size: usize,
    memory_blocks_base: *const PowBlock,
) {
    use std::arch::aarch64::{uint64x2_t, veorq_u64, vld1q_u64, vst1q_u64};

    let mut q = std::mem::MaybeUninit::<PowBlock>::uninit();

    let rhs_ptr = rhs.0.as_ptr();
    let lhs_ptr = lhs.0.as_ptr();
    let dst_ptr = dst.0.as_mut_ptr();
    let q_ptr = q.as_mut_ptr() as *mut u64;

    // Fused Phase 1+2 (identical to compress_neon_into).
    for row in 0..8 {
        let base = row * 16;
        let mut a_lo: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base)), vld1q_u64(lhs_ptr.add(base)));
        let mut a_hi: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 2)), vld1q_u64(lhs_ptr.add(base + 2)));
        let mut b_lo: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 4)), vld1q_u64(lhs_ptr.add(base + 4)));
        let mut b_hi: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 6)), vld1q_u64(lhs_ptr.add(base + 6)));
        let mut c_lo: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 8)), vld1q_u64(lhs_ptr.add(base + 8)));
        let mut c_hi: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 10)), vld1q_u64(lhs_ptr.add(base + 10)));
        let mut d_lo: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 12)), vld1q_u64(lhs_ptr.add(base + 12)));
        let mut d_hi: uint64x2_t = veorq_u64(vld1q_u64(rhs_ptr.add(base + 14)), vld1q_u64(lhs_ptr.add(base + 14)));

        vst1q_u64(dst_ptr.add(base), a_lo);
        vst1q_u64(dst_ptr.add(base + 2), a_hi);
        vst1q_u64(dst_ptr.add(base + 4), b_lo);
        vst1q_u64(dst_ptr.add(base + 6), b_hi);
        vst1q_u64(dst_ptr.add(base + 8), c_lo);
        vst1q_u64(dst_ptr.add(base + 10), c_hi);
        vst1q_u64(dst_ptr.add(base + 12), d_lo);
        vst1q_u64(dst_ptr.add(base + 14), d_hi);

        neon_round(
            &mut a_lo, &mut a_hi,
            &mut b_lo, &mut b_hi,
            &mut c_lo, &mut c_hi,
            &mut d_lo, &mut d_hi,
        );

        vst1q_u64(q_ptr.add(base), a_lo);
        vst1q_u64(q_ptr.add(base + 2), a_hi);
        vst1q_u64(q_ptr.add(base + 4), b_lo);
        vst1q_u64(q_ptr.add(base + 6), b_hi);
        vst1q_u64(q_ptr.add(base + 8), c_lo);
        vst1q_u64(q_ptr.add(base + 10), c_hi);
        vst1q_u64(q_ptr.add(base + 12), d_lo);
        vst1q_u64(q_ptr.add(base + 14), d_hi);
    }

    // Fused Phase 3+4, column 0: this writes dst[0] to its final value.
    {
        let mut a_lo: uint64x2_t = vld1q_u64(q_ptr.add(0));
        let mut a_hi: uint64x2_t = vld1q_u64(q_ptr.add(16));
        let mut b_lo: uint64x2_t = vld1q_u64(q_ptr.add(32));
        let mut b_hi: uint64x2_t = vld1q_u64(q_ptr.add(48));
        let mut c_lo: uint64x2_t = vld1q_u64(q_ptr.add(64));
        let mut c_hi: uint64x2_t = vld1q_u64(q_ptr.add(80));
        let mut d_lo: uint64x2_t = vld1q_u64(q_ptr.add(96));
        let mut d_hi: uint64x2_t = vld1q_u64(q_ptr.add(112));

        neon_round(
            &mut a_lo, &mut a_hi,
            &mut b_lo, &mut b_hi,
            &mut c_lo, &mut c_hi,
            &mut d_lo, &mut d_hi,
        );

        vst1q_u64(dst_ptr.add(0), veorq_u64(a_lo, vld1q_u64(dst_ptr.add(0))));
        vst1q_u64(dst_ptr.add(16), veorq_u64(a_hi, vld1q_u64(dst_ptr.add(16))));
        vst1q_u64(dst_ptr.add(32), veorq_u64(b_lo, vld1q_u64(dst_ptr.add(32))));
        vst1q_u64(dst_ptr.add(48), veorq_u64(b_hi, vld1q_u64(dst_ptr.add(48))));
        vst1q_u64(dst_ptr.add(64), veorq_u64(c_lo, vld1q_u64(dst_ptr.add(64))));
        vst1q_u64(dst_ptr.add(80), veorq_u64(c_hi, vld1q_u64(dst_ptr.add(80))));
        vst1q_u64(dst_ptr.add(96), veorq_u64(d_lo, vld1q_u64(dst_ptr.add(96))));
        vst1q_u64(dst_ptr.add(112), veorq_u64(d_hi, vld1q_u64(dst_ptr.add(112))));
    }

    // --- Mid-compress prefetch ---
    // dst[0] now has its final value.  Compute the NEXT iteration's ref_index
    // and prefetch it.  The remaining 7 column iterations (~150 ns of compute)
    // overlap with the DRAM fetch.
    {
        let rand = *dst_ptr;
        let next_ref = reference_index(next_ref_area_size, rand);
        prefetch_pow_block(&*memory_blocks_base.add(next_ref));
    }

    // Remaining column rounds (idx=1..8).
    for idx in 1..8 {
        let base = idx * 2;
        let mut a_lo: uint64x2_t = vld1q_u64(q_ptr.add(base));
        let mut a_hi: uint64x2_t = vld1q_u64(q_ptr.add(base + 16));
        let mut b_lo: uint64x2_t = vld1q_u64(q_ptr.add(base + 32));
        let mut b_hi: uint64x2_t = vld1q_u64(q_ptr.add(base + 48));
        let mut c_lo: uint64x2_t = vld1q_u64(q_ptr.add(base + 64));
        let mut c_hi: uint64x2_t = vld1q_u64(q_ptr.add(base + 80));
        let mut d_lo: uint64x2_t = vld1q_u64(q_ptr.add(base + 96));
        let mut d_hi: uint64x2_t = vld1q_u64(q_ptr.add(base + 112));

        neon_round(
            &mut a_lo, &mut a_hi,
            &mut b_lo, &mut b_hi,
            &mut c_lo, &mut c_hi,
            &mut d_lo, &mut d_hi,
        );

        vst1q_u64(dst_ptr.add(base), veorq_u64(a_lo, vld1q_u64(dst_ptr.add(base))));
        vst1q_u64(dst_ptr.add(base + 16), veorq_u64(a_hi, vld1q_u64(dst_ptr.add(base + 16))));
        vst1q_u64(dst_ptr.add(base + 32), veorq_u64(b_lo, vld1q_u64(dst_ptr.add(base + 32))));
        vst1q_u64(dst_ptr.add(base + 48), veorq_u64(b_hi, vld1q_u64(dst_ptr.add(base + 48))));
        vst1q_u64(dst_ptr.add(base + 64), veorq_u64(c_lo, vld1q_u64(dst_ptr.add(base + 64))));
        vst1q_u64(dst_ptr.add(base + 80), veorq_u64(c_hi, vld1q_u64(dst_ptr.add(base + 80))));
        vst1q_u64(dst_ptr.add(base + 96), veorq_u64(d_lo, vld1q_u64(dst_ptr.add(base + 96))));
        vst1q_u64(dst_ptr.add(base + 112), veorq_u64(d_hi, vld1q_u64(dst_ptr.add(base + 112))));
    }
}

fn blake2b_long(inputs: &[&[u8]], out: &mut [u8]) -> Result<()> {
    if out.is_empty() {
        return Err(Error::OutputTooShort);
    }
    if out.len() > u32::MAX as usize {
        return Err(Error::OutputTooLong);
    }
    let len_bytes = (out.len() as u32).to_le_bytes();

    if out.len() <= Blake2b512::output_size() {
        let mut digest = Blake2bVar::new(out.len()).map_err(|_| Error::OutputTooLong)?;
        blake2::digest::Update::update(&mut digest, &len_bytes);
        for input in inputs {
            blake2::digest::Update::update(&mut digest, input);
        }
        digest
            .finalize_variable(out)
            .map_err(|_| Error::OutputTooLong)?;
        return Ok(());
    }

    let half_hash_len = Blake2b512::output_size() / 2;
    let mut digest = Blake2b512::new();
    Digest::update(&mut digest, len_bytes);
    for input in inputs {
        Digest::update(&mut digest, input);
    }
    let mut last_output = digest.finalize();
    out[..half_hash_len].copy_from_slice(&last_output[..half_hash_len]);

    let mut counter = 0usize;
    let out_len = out.len();
    for chunk in out[half_hash_len..]
        .chunks_exact_mut(half_hash_len)
        .take_while(|_| {
            counter = counter.saturating_add(half_hash_len);
            out_len.saturating_sub(counter) > 64
        })
    {
        last_output = Blake2b512::digest(last_output);
        chunk.copy_from_slice(&last_output[..half_hash_len]);
    }

    let last_block_size = out.len().saturating_sub(counter);
    let mut digest = Blake2bVar::new(last_block_size).map_err(|_| Error::OutputTooLong)?;
    blake2::digest::Update::update(&mut digest, &last_output);
    digest
        .finalize_variable(&mut out[counter..])
        .map_err(|_| Error::OutputTooLong)?;
    Ok(())
}

#[rustfmt::skip]
macro_rules! permute_step {
    ($a:expr, $b:expr, $c:expr, $d:expr) => {
        $a = (Wrapping($a) + Wrapping($b) + (Wrapping(2) * Wrapping(($a & TRUNC) * ($b & TRUNC)))).0;
        $d = ($d ^ $a).rotate_right(32);
        $c = (Wrapping($c) + Wrapping($d) + (Wrapping(2) * Wrapping(($c & TRUNC) * ($d & TRUNC)))).0;
        $b = ($b ^ $c).rotate_right(24);

        $a = (Wrapping($a) + Wrapping($b) + (Wrapping(2) * Wrapping(($a & TRUNC) * ($b & TRUNC)))).0;
        $d = ($d ^ $a).rotate_right(16);
        $c = (Wrapping($c) + Wrapping($d) + (Wrapping(2) * Wrapping(($c & TRUNC) * ($d & TRUNC)))).0;
        $b = ($b ^ $c).rotate_right(63);
    };
}

macro_rules! permute {
    (
        $v0:expr, $v1:expr, $v2:expr, $v3:expr,
        $v4:expr, $v5:expr, $v6:expr, $v7:expr,
        $v8:expr, $v9:expr, $v10:expr, $v11:expr,
        $v12:expr, $v13:expr, $v14:expr, $v15:expr,
    ) => {
        permute_step!($v0, $v4, $v8, $v12);
        permute_step!($v1, $v5, $v9, $v13);
        permute_step!($v2, $v6, $v10, $v14);
        permute_step!($v3, $v7, $v11, $v15);
        permute_step!($v0, $v5, $v10, $v15);
        permute_step!($v1, $v6, $v11, $v12);
        permute_step!($v2, $v7, $v8, $v13);
        permute_step!($v3, $v4, $v9, $v14);
    };
}

#[derive(Copy, Clone, Debug)]
#[repr(align(64))]
pub(super) struct PowBlock([u64; Self::SIZE / 8]);

impl PowBlock {
    pub(super) const SIZE: usize = 1024;

    #[inline(always)]
    fn load(&mut self, input: &[u8; Self::SIZE]) {
        for (idx, chunk) in input.chunks(8).enumerate() {
            self.0[idx] = u64::from_le_bytes(chunk.try_into().expect("chunk must be 8 bytes"));
        }
    }

    #[inline(always)]
    fn iter(&self) -> std::slice::Iter<'_, u64> {
        self.0.iter()
    }

    #[inline(always)]
    fn compress(rhs: &Self, lhs: &Self) -> Self {
        let r = *rhs ^ lhs;
        let mut q = r;

        for chunk in q.0.chunks_exact_mut(16) {
            #[rustfmt::skip]
            permute!(
                chunk[0], chunk[1], chunk[2], chunk[3],
                chunk[4], chunk[5], chunk[6], chunk[7],
                chunk[8], chunk[9], chunk[10], chunk[11],
                chunk[12], chunk[13], chunk[14], chunk[15],
            );
        }

        for idx in 0..8 {
            let base = idx * 2;
            #[rustfmt::skip]
            permute!(
                q.0[base], q.0[base + 1],
                q.0[base + 16], q.0[base + 17],
                q.0[base + 32], q.0[base + 33],
                q.0[base + 48], q.0[base + 49],
                q.0[base + 64], q.0[base + 65],
                q.0[base + 80], q.0[base + 81],
                q.0[base + 96], q.0[base + 97],
                q.0[base + 112], q.0[base + 113],
            );
        }

        q ^= &r;
        q
    }
}

impl Default for PowBlock {
    fn default() -> Self {
        Self([0u64; Self::SIZE / 8])
    }
}

impl AsRef<[u64]> for PowBlock {
    fn as_ref(&self) -> &[u64] {
        &self.0
    }
}

impl AsMut<[u64]> for PowBlock {
    fn as_mut(&mut self) -> &mut [u64] {
        &mut self.0
    }
}

impl std::ops::BitXor<&PowBlock> for PowBlock {
    type Output = PowBlock;

    fn bitxor(mut self, rhs: &PowBlock) -> Self::Output {
        self ^= rhs;
        self
    }
}

impl std::ops::BitXorAssign<&PowBlock> for PowBlock {
    fn bitxor_assign(&mut self, rhs: &PowBlock) {
        for (dst, src) in self.0.iter_mut().zip(rhs.0.iter()) {
            *dst ^= src;
        }
    }
}

#[cfg(test)]
mod tests {
    use argon2::{Algorithm, Argon2, Params, Version};

    use super::{FixedArgon2id, PowBlock};

    #[test]
    fn fixed_kernel_matches_reference_for_small_memory_configs() {
        let memory_kib_values = [8u32, 32u32, 65u32, 4096u32];
        let salts = [
            b"12345678".as_slice(),
            b"headerbase0123456789abcdefghijklmnop".as_slice(),
        ];
        let nonces = [0u64, 1u64, 7u64, 42u64, 1_000_003u64];

        for memory_kib in memory_kib_values {
            let params =
                Params::new(memory_kib, 1, 1, Some(32)).expect("reference params should be valid");
            let reference_block_count = params.block_count();
            let reference = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
            let fixed = FixedArgon2id::new(memory_kib);
            let mut fixed_memory = vec![PowBlock::default(); fixed.block_count()];
            let mut reference_memory = vec![argon2::Block::default(); reference_block_count];

            for salt in salts {
                for nonce in nonces {
                    let nonce_bytes = nonce.to_le_bytes();
                    let mut expected = [0u8; 32];
                    let mut actual = [0u8; 32];

                    reference
                        .hash_password_into_with_memory(
                            &nonce_bytes,
                            salt,
                            &mut expected,
                            &mut reference_memory,
                        )
                        .expect("reference hashing should succeed");
                    fixed
                        .hash_password_into_with_memory(
                            &nonce_bytes,
                            salt,
                            &mut actual,
                            &mut fixed_memory,
                        )
                        .expect("fixed hashing should succeed");
                    assert_eq!(
                        actual, expected,
                        "mismatch for m_cost={memory_kib} nonce={nonce}"
                    );
                }
            }
        }
    }
}
