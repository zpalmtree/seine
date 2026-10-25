//! 256-bit AVX-512 compression candidate for EPYC.
//!
//! Uses paired 256-bit rows/columns and native rotate instructions. Data-dependent
//! reference prefetch starts after column zero, leaving seven columns to overlap
//! the load. All q elements are initialized before the column phase reads them.
//! Runtime selection belongs to the parent; every entry point requires AVX2,
//! AVX-512F and AVX-512VL with operating-system vector state enabled.

use super::{avx2_blamka, prefetch_pow_block, reference_index, PowBlock};

#[inline(always)]
#[allow(clippy::too_many_arguments)]
unsafe fn avx512vl_round(
    a: &mut std::arch::x86_64::__m256i,
    b: &mut std::arch::x86_64::__m256i,
    c: &mut std::arch::x86_64::__m256i,
    d: &mut std::arch::x86_64::__m256i,
) {
    use std::arch::x86_64::{_mm256_permute4x64_epi64, _mm256_ror_epi64, _mm256_xor_si256};

    *a = avx2_blamka(*a, *b);
    *d = _mm256_ror_epi64::<32>(_mm256_xor_si256(*d, *a));
    *c = avx2_blamka(*c, *d);
    *b = _mm256_ror_epi64::<24>(_mm256_xor_si256(*b, *c));
    *a = avx2_blamka(*a, *b);
    *d = _mm256_ror_epi64::<16>(_mm256_xor_si256(*d, *a));
    *c = avx2_blamka(*c, *d);
    *b = _mm256_ror_epi64::<63>(_mm256_xor_si256(*b, *c));

    let mut bb = _mm256_permute4x64_epi64::<0x39>(*b);
    let mut cc = _mm256_permute4x64_epi64::<0x4E>(*c);
    let mut dd = _mm256_permute4x64_epi64::<0x93>(*d);

    *a = avx2_blamka(*a, bb);
    dd = _mm256_ror_epi64::<32>(_mm256_xor_si256(dd, *a));
    cc = avx2_blamka(cc, dd);
    bb = _mm256_ror_epi64::<24>(_mm256_xor_si256(bb, cc));
    *a = avx2_blamka(*a, bb);
    dd = _mm256_ror_epi64::<16>(_mm256_xor_si256(dd, *a));
    cc = avx2_blamka(cc, dd);
    bb = _mm256_ror_epi64::<63>(_mm256_xor_si256(bb, cc));

    *b = _mm256_permute4x64_epi64::<0x93>(bb);
    *c = _mm256_permute4x64_epi64::<0x4E>(cc);
    *d = _mm256_permute4x64_epi64::<0x39>(dd);
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
unsafe fn avx512vl_round_pair(
    a0: &mut std::arch::x86_64::__m256i,
    b0: &mut std::arch::x86_64::__m256i,
    c0: &mut std::arch::x86_64::__m256i,
    d0: &mut std::arch::x86_64::__m256i,
    a1: &mut std::arch::x86_64::__m256i,
    b1: &mut std::arch::x86_64::__m256i,
    c1: &mut std::arch::x86_64::__m256i,
    d1: &mut std::arch::x86_64::__m256i,
) {
    use std::arch::x86_64::{_mm256_permute4x64_epi64, _mm256_ror_epi64, _mm256_xor_si256};

    *a0 = avx2_blamka(*a0, *b0);
    *a1 = avx2_blamka(*a1, *b1);
    *d0 = _mm256_ror_epi64::<32>(_mm256_xor_si256(*d0, *a0));
    *d1 = _mm256_ror_epi64::<32>(_mm256_xor_si256(*d1, *a1));
    *c0 = avx2_blamka(*c0, *d0);
    *c1 = avx2_blamka(*c1, *d1);
    *b0 = _mm256_ror_epi64::<24>(_mm256_xor_si256(*b0, *c0));
    *b1 = _mm256_ror_epi64::<24>(_mm256_xor_si256(*b1, *c1));
    *a0 = avx2_blamka(*a0, *b0);
    *a1 = avx2_blamka(*a1, *b1);
    *d0 = _mm256_ror_epi64::<16>(_mm256_xor_si256(*d0, *a0));
    *d1 = _mm256_ror_epi64::<16>(_mm256_xor_si256(*d1, *a1));
    *c0 = avx2_blamka(*c0, *d0);
    *c1 = avx2_blamka(*c1, *d1);
    *b0 = _mm256_ror_epi64::<63>(_mm256_xor_si256(*b0, *c0));
    *b1 = _mm256_ror_epi64::<63>(_mm256_xor_si256(*b1, *c1));

    let mut bb0 = _mm256_permute4x64_epi64::<0x39>(*b0);
    let mut bb1 = _mm256_permute4x64_epi64::<0x39>(*b1);
    let mut cc0 = _mm256_permute4x64_epi64::<0x4E>(*c0);
    let mut cc1 = _mm256_permute4x64_epi64::<0x4E>(*c1);
    let mut dd0 = _mm256_permute4x64_epi64::<0x93>(*d0);
    let mut dd1 = _mm256_permute4x64_epi64::<0x93>(*d1);

    *a0 = avx2_blamka(*a0, bb0);
    *a1 = avx2_blamka(*a1, bb1);
    dd0 = _mm256_ror_epi64::<32>(_mm256_xor_si256(dd0, *a0));
    dd1 = _mm256_ror_epi64::<32>(_mm256_xor_si256(dd1, *a1));
    cc0 = avx2_blamka(cc0, dd0);
    cc1 = avx2_blamka(cc1, dd1);
    bb0 = _mm256_ror_epi64::<24>(_mm256_xor_si256(bb0, cc0));
    bb1 = _mm256_ror_epi64::<24>(_mm256_xor_si256(bb1, cc1));
    *a0 = avx2_blamka(*a0, bb0);
    *a1 = avx2_blamka(*a1, bb1);
    dd0 = _mm256_ror_epi64::<16>(_mm256_xor_si256(dd0, *a0));
    dd1 = _mm256_ror_epi64::<16>(_mm256_xor_si256(dd1, *a1));
    cc0 = avx2_blamka(cc0, dd0);
    cc1 = avx2_blamka(cc1, dd1);
    bb0 = _mm256_ror_epi64::<63>(_mm256_xor_si256(bb0, cc0));
    bb1 = _mm256_ror_epi64::<63>(_mm256_xor_si256(bb1, cc1));

    *b0 = _mm256_permute4x64_epi64::<0x93>(bb0);
    *b1 = _mm256_permute4x64_epi64::<0x93>(bb1);
    *c0 = _mm256_permute4x64_epi64::<0x4E>(cc0);
    *c1 = _mm256_permute4x64_epi64::<0x4E>(cc1);
    *d0 = _mm256_permute4x64_epi64::<0x39>(dd0);
    *d1 = _mm256_permute4x64_epi64::<0x39>(dd1);
}

/// # Safety
/// Requires AVX2, AVX-512F and AVX-512VL.
#[target_feature(enable = "avx2,avx512f,avx512vl")]
pub(super) unsafe fn compress(rhs: &PowBlock, lhs: &PowBlock) -> PowBlock {
    let mut dst = PowBlock::default();
    compress_into(rhs, lhs, &mut dst);
    dst
}

/// # Safety
/// Requires AVX2, AVX-512F and AVX-512VL. Output must not alias either input.
#[target_feature(enable = "avx2,avx512f,avx512vl")]
pub(super) unsafe fn compress_into(rhs: &PowBlock, lhs: &PowBlock, dst: &mut PowBlock) {
    use std::arch::x86_64::{
        __m256i, _mm256_loadu_si256, _mm256_permute2x128_si256, _mm256_storeu_si256,
        _mm256_xor_si256,
    };

    // q lives on the stack as the working buffer for round permutations.
    // dst doubles as the pre-round XOR backup (r), eliminating the second stack buffer.
    let mut q = std::mem::MaybeUninit::<PowBlock>::uninit();

    let rhs_ptr = rhs.0.as_ptr();
    let lhs_ptr = lhs.0.as_ptr();
    let dst_ptr = dst.0.as_mut_ptr();
    let q_ptr = q.as_mut_ptr() as *mut u64;

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

    // Phase 2: Interleave pairs of contiguous 16-element row rounds.
    for row in (0..8).step_by(2) {
        let base0 = row * 16;
        let mut a0 = _mm256_loadu_si256(q_ptr.add(base0) as *const __m256i);
        let mut b0 = _mm256_loadu_si256(q_ptr.add(base0 + 4) as *const __m256i);
        let mut c0 = _mm256_loadu_si256(q_ptr.add(base0 + 8) as *const __m256i);
        let mut d0 = _mm256_loadu_si256(q_ptr.add(base0 + 12) as *const __m256i);

        let base1 = (row + 1) * 16;
        let mut a1 = _mm256_loadu_si256(q_ptr.add(base1) as *const __m256i);
        let mut b1 = _mm256_loadu_si256(q_ptr.add(base1 + 4) as *const __m256i);
        let mut c1 = _mm256_loadu_si256(q_ptr.add(base1 + 8) as *const __m256i);
        let mut d1 = _mm256_loadu_si256(q_ptr.add(base1 + 12) as *const __m256i);

        avx512vl_round_pair(
            &mut a0, &mut b0, &mut c0, &mut d0, &mut a1, &mut b1, &mut c1, &mut d1,
        );

        _mm256_storeu_si256(q_ptr.add(base0) as *mut __m256i, a0);
        _mm256_storeu_si256(q_ptr.add(base0 + 4) as *mut __m256i, b0);
        _mm256_storeu_si256(q_ptr.add(base0 + 8) as *mut __m256i, c0);
        _mm256_storeu_si256(q_ptr.add(base0 + 12) as *mut __m256i, d0);

        _mm256_storeu_si256(q_ptr.add(base1) as *mut __m256i, a1);
        _mm256_storeu_si256(q_ptr.add(base1 + 4) as *mut __m256i, b1);
        _mm256_storeu_si256(q_ptr.add(base1 + 8) as *mut __m256i, c1);
        _mm256_storeu_si256(q_ptr.add(base1 + 12) as *mut __m256i, d1);
    }

    // Phase 3: Adjacent columns share 256-bit contiguous loads. Split their
    // 128-bit halves into independent rounds, then recombine for contiguous
    // scatter + XOR against the pre-round backup in dst.
    for idx in (0..8).step_by(2) {
        let base = idx * 2;
        let a_lo = _mm256_loadu_si256(q_ptr.add(base) as *const __m256i);
        let a_hi = _mm256_loadu_si256(q_ptr.add(base + 16) as *const __m256i);
        let mut a0 = _mm256_permute2x128_si256::<0x20>(a_lo, a_hi);
        let mut a1 = _mm256_permute2x128_si256::<0x31>(a_lo, a_hi);
        let b_lo = _mm256_loadu_si256(q_ptr.add(base + 32) as *const __m256i);
        let b_hi = _mm256_loadu_si256(q_ptr.add(base + 48) as *const __m256i);
        let mut b0 = _mm256_permute2x128_si256::<0x20>(b_lo, b_hi);
        let mut b1 = _mm256_permute2x128_si256::<0x31>(b_lo, b_hi);
        let c_lo = _mm256_loadu_si256(q_ptr.add(base + 64) as *const __m256i);
        let c_hi = _mm256_loadu_si256(q_ptr.add(base + 80) as *const __m256i);
        let mut c0 = _mm256_permute2x128_si256::<0x20>(c_lo, c_hi);
        let mut c1 = _mm256_permute2x128_si256::<0x31>(c_lo, c_hi);
        let d_lo = _mm256_loadu_si256(q_ptr.add(base + 96) as *const __m256i);
        let d_hi = _mm256_loadu_si256(q_ptr.add(base + 112) as *const __m256i);
        let mut d0 = _mm256_permute2x128_si256::<0x20>(d_lo, d_hi);
        let mut d1 = _mm256_permute2x128_si256::<0x31>(d_lo, d_hi);
        avx512vl_round_pair(
            &mut a0, &mut b0, &mut c0, &mut d0, &mut a1, &mut b1, &mut c1, &mut d1,
        );
        let a_lo = _mm256_permute2x128_si256::<0x20>(a0, a1);
        _mm256_storeu_si256(
            dst_ptr.add(base) as *mut __m256i,
            _mm256_xor_si256(
                a_lo,
                _mm256_loadu_si256(dst_ptr.add(base) as *const __m256i),
            ),
        );
        let a_hi = _mm256_permute2x128_si256::<0x31>(a0, a1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 16) as *mut __m256i,
            _mm256_xor_si256(
                a_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 16) as *const __m256i),
            ),
        );
        let b_lo = _mm256_permute2x128_si256::<0x20>(b0, b1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 32) as *mut __m256i,
            _mm256_xor_si256(
                b_lo,
                _mm256_loadu_si256(dst_ptr.add(base + 32) as *const __m256i),
            ),
        );
        let b_hi = _mm256_permute2x128_si256::<0x31>(b0, b1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 48) as *mut __m256i,
            _mm256_xor_si256(
                b_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 48) as *const __m256i),
            ),
        );
        let c_lo = _mm256_permute2x128_si256::<0x20>(c0, c1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 64) as *mut __m256i,
            _mm256_xor_si256(
                c_lo,
                _mm256_loadu_si256(dst_ptr.add(base + 64) as *const __m256i),
            ),
        );
        let c_hi = _mm256_permute2x128_si256::<0x31>(c0, c1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 80) as *mut __m256i,
            _mm256_xor_si256(
                c_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 80) as *const __m256i),
            ),
        );
        let d_lo = _mm256_permute2x128_si256::<0x20>(d0, d1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 96) as *mut __m256i,
            _mm256_xor_si256(
                d_lo,
                _mm256_loadu_si256(dst_ptr.add(base + 96) as *const __m256i),
            ),
        );
        let d_hi = _mm256_permute2x128_si256::<0x31>(d0, d1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 112) as *mut __m256i,
            _mm256_xor_si256(
                d_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 112) as *const __m256i),
            ),
        );
    }
    // No Phase 4 needed — final XOR was fused into the column scatter above.
}

/// # Safety
/// Requires AVX2, AVX-512F and AVX-512VL. Output must not alias either input.
/// `memory_blocks_base` must reference at least `next_ref_area_size` valid blocks,
/// and that area size must be nonzero.
#[target_feature(enable = "avx2,avx512f,avx512vl")]
pub(super) unsafe fn compress_into_mid_prefetch(
    rhs: &PowBlock,
    lhs: &PowBlock,
    dst: &mut PowBlock,
    next_ref_area_size: usize,
    memory_blocks_base: *const PowBlock,
) {
    use std::arch::x86_64::{
        __m128i, __m256i, _mm256_castsi128_si256, _mm256_castsi256_si128, _mm256_extracti128_si256,
        _mm256_inserti128_si256, _mm256_loadu_si256, _mm256_permute2x128_si256,
        _mm256_storeu_si256, _mm256_xor_si256, _mm_loadu_si128, _mm_storeu_si128, _mm_xor_si128,
    };

    let mut q = std::mem::MaybeUninit::<PowBlock>::uninit();

    let rhs_ptr = rhs.0.as_ptr();
    let lhs_ptr = lhs.0.as_ptr();
    let dst_ptr = dst.0.as_mut_ptr();
    let q_ptr = q.as_mut_ptr() as *mut u64;

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

    // Phase 2: Interleave pairs of contiguous 16-element row rounds.
    for row in (0..8).step_by(2) {
        let base0 = row * 16;
        let mut a0 = _mm256_loadu_si256(q_ptr.add(base0) as *const __m256i);
        let mut b0 = _mm256_loadu_si256(q_ptr.add(base0 + 4) as *const __m256i);
        let mut c0 = _mm256_loadu_si256(q_ptr.add(base0 + 8) as *const __m256i);
        let mut d0 = _mm256_loadu_si256(q_ptr.add(base0 + 12) as *const __m256i);

        let base1 = (row + 1) * 16;
        let mut a1 = _mm256_loadu_si256(q_ptr.add(base1) as *const __m256i);
        let mut b1 = _mm256_loadu_si256(q_ptr.add(base1 + 4) as *const __m256i);
        let mut c1 = _mm256_loadu_si256(q_ptr.add(base1 + 8) as *const __m256i);
        let mut d1 = _mm256_loadu_si256(q_ptr.add(base1 + 12) as *const __m256i);

        avx512vl_round_pair(
            &mut a0, &mut b0, &mut c0, &mut d0, &mut a1, &mut b1, &mut c1, &mut d1,
        );

        _mm256_storeu_si256(q_ptr.add(base0) as *mut __m256i, a0);
        _mm256_storeu_si256(q_ptr.add(base0 + 4) as *mut __m256i, b0);
        _mm256_storeu_si256(q_ptr.add(base0 + 8) as *mut __m256i, c0);
        _mm256_storeu_si256(q_ptr.add(base0 + 12) as *mut __m256i, d0);

        _mm256_storeu_si256(q_ptr.add(base1) as *mut __m256i, a1);
        _mm256_storeu_si256(q_ptr.add(base1 + 4) as *mut __m256i, b1);
        _mm256_storeu_si256(q_ptr.add(base1 + 8) as *mut __m256i, c1);
        _mm256_storeu_si256(q_ptr.add(base1 + 12) as *mut __m256i, d1);
    }

    // Scatter-XOR for the unpaired first and last column rounds.
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

    // Phase 3a: Column round 0 — writes dst[0] to its final value.
    {
        let base = 0;
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

        avx512vl_round(&mut a, &mut b, &mut c, &mut d);

        scatter_xor_pair!(a, base, base + 16);
        scatter_xor_pair!(b, base + 32, base + 48);
        scatter_xor_pair!(c, base + 64, base + 80);
        scatter_xor_pair!(d, base + 96, base + 112);
    }

    // --- Mid-compress prefetch ---
    // dst[0] now has its final value.  Compute the NEXT iteration's ref_index
    // and prefetch it.  The remaining 7 column rounds (~35-40 ns of compute)
    // overlap with the DRAM fetch.
    {
        let rand = *dst_ptr;
        let next_ref = reference_index(next_ref_area_size, rand);
        prefetch_pow_block(&*memory_blocks_base.add(next_ref));
    }

    // Phase 3b: Pair adjacent columns 1+2, 3+4, and 5+6 after the prefetch.
    // Their contiguous loads/stores use the same layout as compress_avx512_into.
    for idx in (1..7).step_by(2) {
        let base = idx * 2;
        let a_lo = _mm256_loadu_si256(q_ptr.add(base) as *const __m256i);
        let a_hi = _mm256_loadu_si256(q_ptr.add(base + 16) as *const __m256i);
        let mut a0 = _mm256_permute2x128_si256::<0x20>(a_lo, a_hi);
        let mut a1 = _mm256_permute2x128_si256::<0x31>(a_lo, a_hi);
        let b_lo = _mm256_loadu_si256(q_ptr.add(base + 32) as *const __m256i);
        let b_hi = _mm256_loadu_si256(q_ptr.add(base + 48) as *const __m256i);
        let mut b0 = _mm256_permute2x128_si256::<0x20>(b_lo, b_hi);
        let mut b1 = _mm256_permute2x128_si256::<0x31>(b_lo, b_hi);
        let c_lo = _mm256_loadu_si256(q_ptr.add(base + 64) as *const __m256i);
        let c_hi = _mm256_loadu_si256(q_ptr.add(base + 80) as *const __m256i);
        let mut c0 = _mm256_permute2x128_si256::<0x20>(c_lo, c_hi);
        let mut c1 = _mm256_permute2x128_si256::<0x31>(c_lo, c_hi);
        let d_lo = _mm256_loadu_si256(q_ptr.add(base + 96) as *const __m256i);
        let d_hi = _mm256_loadu_si256(q_ptr.add(base + 112) as *const __m256i);
        let mut d0 = _mm256_permute2x128_si256::<0x20>(d_lo, d_hi);
        let mut d1 = _mm256_permute2x128_si256::<0x31>(d_lo, d_hi);
        avx512vl_round_pair(
            &mut a0, &mut b0, &mut c0, &mut d0, &mut a1, &mut b1, &mut c1, &mut d1,
        );
        let a_lo = _mm256_permute2x128_si256::<0x20>(a0, a1);
        _mm256_storeu_si256(
            dst_ptr.add(base) as *mut __m256i,
            _mm256_xor_si256(
                a_lo,
                _mm256_loadu_si256(dst_ptr.add(base) as *const __m256i),
            ),
        );
        let a_hi = _mm256_permute2x128_si256::<0x31>(a0, a1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 16) as *mut __m256i,
            _mm256_xor_si256(
                a_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 16) as *const __m256i),
            ),
        );
        let b_lo = _mm256_permute2x128_si256::<0x20>(b0, b1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 32) as *mut __m256i,
            _mm256_xor_si256(
                b_lo,
                _mm256_loadu_si256(dst_ptr.add(base + 32) as *const __m256i),
            ),
        );
        let b_hi = _mm256_permute2x128_si256::<0x31>(b0, b1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 48) as *mut __m256i,
            _mm256_xor_si256(
                b_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 48) as *const __m256i),
            ),
        );
        let c_lo = _mm256_permute2x128_si256::<0x20>(c0, c1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 64) as *mut __m256i,
            _mm256_xor_si256(
                c_lo,
                _mm256_loadu_si256(dst_ptr.add(base + 64) as *const __m256i),
            ),
        );
        let c_hi = _mm256_permute2x128_si256::<0x31>(c0, c1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 80) as *mut __m256i,
            _mm256_xor_si256(
                c_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 80) as *const __m256i),
            ),
        );
        let d_lo = _mm256_permute2x128_si256::<0x20>(d0, d1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 96) as *mut __m256i,
            _mm256_xor_si256(
                d_lo,
                _mm256_loadu_si256(dst_ptr.add(base + 96) as *const __m256i),
            ),
        );
        let d_hi = _mm256_permute2x128_si256::<0x31>(d0, d1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 112) as *mut __m256i,
            _mm256_xor_si256(
                d_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 112) as *const __m256i),
            ),
        );
    }
    // Column 7 is the unpaired tail; a pair here would cross the block boundary.
    {
        let base = 7 * 2;

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

        avx512vl_round(&mut a, &mut b, &mut c, &mut d);

        scatter_xor_pair!(a, base, base + 16);
        scatter_xor_pair!(b, base + 32, base + 48);
        scatter_xor_pair!(c, base + 64, base + 80);
        scatter_xor_pair!(d, base + 96, base + 112);
    }
}

#[target_feature(enable = "avx2,avx512f,avx512vl")]
pub(super) unsafe fn compress_into_mid_prefetch_ring(
    rhs: &PowBlock,
    lhs: &PowBlock,
    dst: &mut PowBlock,
    next_ref_area_size: usize,
    memory_blocks_base: *const PowBlock,
    ring_base: *const PowBlock,
    next_index: usize,
) {
    use std::arch::x86_64::{
        __m128i, __m256i, _mm256_castsi128_si256, _mm256_castsi256_si128, _mm256_extracti128_si256,
        _mm256_inserti128_si256, _mm256_loadu_si256, _mm256_permute2x128_si256,
        _mm256_storeu_si256, _mm256_xor_si256, _mm_loadu_si128, _mm_storeu_si128, _mm_xor_si128,
    };

    let mut q = std::mem::MaybeUninit::<PowBlock>::uninit();

    let rhs_ptr = rhs.0.as_ptr();
    let lhs_ptr = lhs.0.as_ptr();
    let dst_ptr = dst.0.as_mut_ptr();
    let q_ptr = q.as_mut_ptr() as *mut u64;

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

    // Phase 2: Interleave pairs of contiguous 16-element row rounds.
    for row in (0..8).step_by(2) {
        let base0 = row * 16;
        let mut a0 = _mm256_loadu_si256(q_ptr.add(base0) as *const __m256i);
        let mut b0 = _mm256_loadu_si256(q_ptr.add(base0 + 4) as *const __m256i);
        let mut c0 = _mm256_loadu_si256(q_ptr.add(base0 + 8) as *const __m256i);
        let mut d0 = _mm256_loadu_si256(q_ptr.add(base0 + 12) as *const __m256i);

        let base1 = (row + 1) * 16;
        let mut a1 = _mm256_loadu_si256(q_ptr.add(base1) as *const __m256i);
        let mut b1 = _mm256_loadu_si256(q_ptr.add(base1 + 4) as *const __m256i);
        let mut c1 = _mm256_loadu_si256(q_ptr.add(base1 + 8) as *const __m256i);
        let mut d1 = _mm256_loadu_si256(q_ptr.add(base1 + 12) as *const __m256i);

        avx512vl_round_pair(
            &mut a0, &mut b0, &mut c0, &mut d0, &mut a1, &mut b1, &mut c1, &mut d1,
        );

        _mm256_storeu_si256(q_ptr.add(base0) as *mut __m256i, a0);
        _mm256_storeu_si256(q_ptr.add(base0 + 4) as *mut __m256i, b0);
        _mm256_storeu_si256(q_ptr.add(base0 + 8) as *mut __m256i, c0);
        _mm256_storeu_si256(q_ptr.add(base0 + 12) as *mut __m256i, d0);

        _mm256_storeu_si256(q_ptr.add(base1) as *mut __m256i, a1);
        _mm256_storeu_si256(q_ptr.add(base1 + 4) as *mut __m256i, b1);
        _mm256_storeu_si256(q_ptr.add(base1 + 8) as *mut __m256i, c1);
        _mm256_storeu_si256(q_ptr.add(base1 + 12) as *mut __m256i, d1);
    }

    // Scatter-XOR for the unpaired first and last column rounds.
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

    // Phase 3a: Column round 0 — writes dst[0] to its final value.
    {
        let base = 0;
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

        avx512vl_round(&mut a, &mut b, &mut c, &mut d);

        scatter_xor_pair!(a, base, base + 16);
        scatter_xor_pair!(b, base + 32, base + 48);
        scatter_xor_pair!(c, base + 64, base + 80);
        scatter_xor_pair!(d, base + 96, base + 112);
    }

    // --- Mid-compress prefetch ---
    // dst[0] now has its final value.  Compute the NEXT iteration's ref_index
    // and prefetch it.  The remaining 7 column rounds (~35-40 ns of compute)
    // overlap with the DRAM fetch.
    {
        let rand = *dst_ptr;
        let next_ref = reference_index(next_ref_area_size, rand);
        if next_index - next_ref < 64 {
            prefetch_pow_block(&*ring_base.add(next_ref & 63));
        } else {
            prefetch_pow_block(&*memory_blocks_base.add(next_ref));
        }
    }

    // Phase 3b: Pair adjacent columns 1+2, 3+4, and 5+6 after the prefetch.
    // Their contiguous loads/stores use the same layout as compress_avx512_into.
    for idx in (1..7).step_by(2) {
        let base = idx * 2;
        let a_lo = _mm256_loadu_si256(q_ptr.add(base) as *const __m256i);
        let a_hi = _mm256_loadu_si256(q_ptr.add(base + 16) as *const __m256i);
        let mut a0 = _mm256_permute2x128_si256::<0x20>(a_lo, a_hi);
        let mut a1 = _mm256_permute2x128_si256::<0x31>(a_lo, a_hi);
        let b_lo = _mm256_loadu_si256(q_ptr.add(base + 32) as *const __m256i);
        let b_hi = _mm256_loadu_si256(q_ptr.add(base + 48) as *const __m256i);
        let mut b0 = _mm256_permute2x128_si256::<0x20>(b_lo, b_hi);
        let mut b1 = _mm256_permute2x128_si256::<0x31>(b_lo, b_hi);
        let c_lo = _mm256_loadu_si256(q_ptr.add(base + 64) as *const __m256i);
        let c_hi = _mm256_loadu_si256(q_ptr.add(base + 80) as *const __m256i);
        let mut c0 = _mm256_permute2x128_si256::<0x20>(c_lo, c_hi);
        let mut c1 = _mm256_permute2x128_si256::<0x31>(c_lo, c_hi);
        let d_lo = _mm256_loadu_si256(q_ptr.add(base + 96) as *const __m256i);
        let d_hi = _mm256_loadu_si256(q_ptr.add(base + 112) as *const __m256i);
        let mut d0 = _mm256_permute2x128_si256::<0x20>(d_lo, d_hi);
        let mut d1 = _mm256_permute2x128_si256::<0x31>(d_lo, d_hi);
        avx512vl_round_pair(
            &mut a0, &mut b0, &mut c0, &mut d0, &mut a1, &mut b1, &mut c1, &mut d1,
        );
        let a_lo = _mm256_permute2x128_si256::<0x20>(a0, a1);
        _mm256_storeu_si256(
            dst_ptr.add(base) as *mut __m256i,
            _mm256_xor_si256(
                a_lo,
                _mm256_loadu_si256(dst_ptr.add(base) as *const __m256i),
            ),
        );
        let a_hi = _mm256_permute2x128_si256::<0x31>(a0, a1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 16) as *mut __m256i,
            _mm256_xor_si256(
                a_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 16) as *const __m256i),
            ),
        );
        let b_lo = _mm256_permute2x128_si256::<0x20>(b0, b1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 32) as *mut __m256i,
            _mm256_xor_si256(
                b_lo,
                _mm256_loadu_si256(dst_ptr.add(base + 32) as *const __m256i),
            ),
        );
        let b_hi = _mm256_permute2x128_si256::<0x31>(b0, b1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 48) as *mut __m256i,
            _mm256_xor_si256(
                b_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 48) as *const __m256i),
            ),
        );
        let c_lo = _mm256_permute2x128_si256::<0x20>(c0, c1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 64) as *mut __m256i,
            _mm256_xor_si256(
                c_lo,
                _mm256_loadu_si256(dst_ptr.add(base + 64) as *const __m256i),
            ),
        );
        let c_hi = _mm256_permute2x128_si256::<0x31>(c0, c1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 80) as *mut __m256i,
            _mm256_xor_si256(
                c_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 80) as *const __m256i),
            ),
        );
        let d_lo = _mm256_permute2x128_si256::<0x20>(d0, d1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 96) as *mut __m256i,
            _mm256_xor_si256(
                d_lo,
                _mm256_loadu_si256(dst_ptr.add(base + 96) as *const __m256i),
            ),
        );
        let d_hi = _mm256_permute2x128_si256::<0x31>(d0, d1);
        _mm256_storeu_si256(
            dst_ptr.add(base + 112) as *mut __m256i,
            _mm256_xor_si256(
                d_hi,
                _mm256_loadu_si256(dst_ptr.add(base + 112) as *const __m256i),
            ),
        );
    }
    // Column 7 is the unpaired tail; a pair here would cross the block boundary.
    {
        let base = 7 * 2;

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

        avx512vl_round(&mut a, &mut b, &mut c, &mut d);

        scatter_xor_pair!(a, base, base + 16);
        scatter_xor_pair!(b, base + 32, base + 48);
        scatter_xor_pair!(c, base + 64, base + 80);
        scatter_xor_pair!(d, base + 96, base + 112);
    }
}

#[cfg(test)]
mod tests {
    use super::PowBlock;

    #[test]
    fn compression_matches_scalar_with_prefetch_and_shared_inputs() {
        if !(std::arch::is_x86_feature_detected!("avx2")
            && std::arch::is_x86_feature_detected!("avx512f")
            && std::arch::is_x86_feature_detected!("avx512vl"))
        {
            return;
        }

        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next_word = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let prefetch_memory = [PowBlock::default(); 16];
        for case in 0..64 {
            let mut lhs = PowBlock::default();
            let mut rhs = PowBlock::default();
            for (a, b) in lhs.0.iter_mut().zip(rhs.0.iter_mut()) {
                *a = match case {
                    0 => 0,
                    1 => u64::MAX,
                    2 => 0xaaaa_aaaa_aaaa_aaaa,
                    _ => next_word(),
                };
                *b = match case {
                    0 | 1 => 0,
                    2 => 0x5555_5555_5555_5555,
                    _ => next_word(),
                };
            }

            // A reference block can be the preceding block, so immutable inputs
            // may alias. Guard blocks detect writes outside the 1 KiB output.
            for rhs in [&rhs, &lhs] {
                let expected = PowBlock::compress(&lhs, rhs);
                // The feature checks above cover the returning wrapper too.
                assert_eq!(unsafe { super::compress(&lhs, rhs) }.0, expected.0);
                for prefetch in [false, true] {
                    let guard = PowBlock([0xa5a5_a5a5_a5a5_a5a5; 128]);
                    let mut output = [guard; 3];
                    // All required ISA features are checked above. Inputs and output are disjoint;
                    // reference_index maps this area size inside prefetch_memory.
                    unsafe {
                        if prefetch {
                            super::compress_into_mid_prefetch(
                                &lhs,
                                rhs,
                                &mut output[1],
                                prefetch_memory.len(),
                                prefetch_memory.as_ptr(),
                            );
                        } else {
                            super::compress_into(&lhs, rhs, &mut output[1]);
                        }
                    }
                    assert_eq!(output[1].0, expected.0, "case={case} prefetch={prefetch}");
                    assert_eq!(output[0].0, guard.0);
                    assert_eq!(output[2].0, guard.0);
                }
            }
        }
    }
}
