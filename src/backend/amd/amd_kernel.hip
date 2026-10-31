// SPDX-License-Identifier: BSD-3-Clause
// HIP Argon2id lane-fill kernel specialized for Seine's PoW profile.
//
// Port of src/backend/nvidia_kernel.cu: the same 32-thread cooperative block
// compression, unrolled global loads/stores, closed-form G-round indexing and
// shuffle-fused G-rounds. The CUDA kernel's NVRTC/PTXAS-specific dead-code
// block is not part of this port.
//
// Wavefront contract: the cooperative group is exactly 32 threads per hash
// lane and the thread block is exactly one wavefront. On wave32 (RDNA
// gfx10-gfx12, compiled with -mno-wavefrontsize64) SEINE_WARPS_PER_BLOCK == 1;
// on wave64 (CDNA gfx9, -mwavefrontsize64) the host sets
// SEINE_WARPS_PER_BLOCK == 2 so two lanes share one wavefront, and every
// __shfl below passes ARGON2_COOP_THREADS as its width so shuffles stay inside
// each lane's 32-thread segment. The host engine refuses any other warpSize.
//
// hipRTC compiles HIP device code without explicit includes (__global__,
// __shfl, atomics and friends are predeclared, mirroring NVRTC).

extern "C" {

constexpr unsigned int ARGON2_COOP_THREADS = 32U;
constexpr unsigned int SEED_KERNEL_THREADS = 64U;
constexpr unsigned int EVAL_KERNEL_THREADS = 64U;
constexpr unsigned int BLAKE2B_BLOCK_BYTES = 128U;
constexpr unsigned int BLAKE2B_OUT_BYTES = 64U;
constexpr unsigned int POW_OUTPUT_BYTES = 32U;
constexpr unsigned int ARGON2_VERSION_V13 = 0x13U;
constexpr unsigned int ARGON2_ALGORITHM_ID = 2U;
#ifndef SEINE_FIXED_M_BLOCKS
#define SEINE_FIXED_M_BLOCKS 0U
#endif
#ifndef SEINE_FIXED_T_COST
#define SEINE_FIXED_T_COST 0U
#endif
#ifndef SEINE_CANCEL_CHECK_BLOCK_INTERVAL
#define SEINE_CANCEL_CHECK_BLOCK_INTERVAL 64U
#endif
constexpr unsigned int CANCEL_CHECK_BLOCK_INTERVAL = SEINE_CANCEL_CHECK_BLOCK_INTERVAL;
#ifndef SEINE_WARPS_PER_BLOCK
#define SEINE_WARPS_PER_BLOCK 1U
#endif
constexpr unsigned int WARPS_PER_BLOCK = SEINE_WARPS_PER_BLOCK;

// Cooperative-group barrier. The thread block is exactly one wavefront (one
// lane on wave32, two on wave64), so a wave-scoped fence (LDS/global ordering
// for this wave only) plus a scheduling barrier is what the LLVM AMDGPU memory
// model requires. The wave-scoped form is the default; the host passes
// SEINE_WAVE_SYNC unless SEINE_AMD_BLOCK_SYNC is set.
__device__ __forceinline__ void coop_sync() {
#if defined(SEINE_WAVE_SYNC) && SEINE_WAVE_SYNC
    __builtin_amdgcn_fence(__ATOMIC_SEQ_CST, "wavefront");
    __builtin_amdgcn_wave_barrier();
#else
    __syncthreads();
#endif
}

// Lane-to-lane value exchange inside the 32-thread cooperative group.
// HIP __shfl takes no participation mask (contrast CUDA's masked *_sync
// shuffle intrinsics); wave32 execution makes all 32 lanes implicit
// participants.
__device__ __forceinline__ unsigned long long coop_shfl(
    unsigned long long value,
    unsigned int src_lane
) {
    return __shfl(value, static_cast<int>(src_lane), ARGON2_COOP_THREADS);
}

__device__ __forceinline__ unsigned int coop_shfl_u32(
    unsigned int value,
    unsigned int src_lane
) {
    return __shfl(value, static_cast<int>(src_lane), ARGON2_COOP_THREADS);
}

// Compression-path switches; the host may override any of them through
// SEINE_AMD_DEFINES.
// SEINE_AMD_DIRECT_LAYOUT: load the compression input in the row-round layout and
//   store the column-round result from registers (one LDS transposition per block).
// SEINE_AMD_DPP_SHFL: the G-round quad rotations use DPP quad_perm moves instead
//   of ds_bpermute shuffles.
// SEINE_AMD_CARRY_WORD0: word 0 of the block just written is broadcast from the
//   thread that holds it and used as the next data-dependent reference seed.
#ifndef SEINE_AMD_DIRECT_LAYOUT
#define SEINE_AMD_DIRECT_LAYOUT 1
#endif
#ifndef SEINE_AMD_DPP_SHFL
#define SEINE_AMD_DPP_SHFL 1
#endif
#ifndef SEINE_AMD_CARRY_WORD0
#define SEINE_AMD_CARRY_WORD0 1
#endif
// SEINE_AMD_DI_PREFETCH (off): in the data-independent half, load the next
//   block's reference block into registers one block early.
// Read the feed-forward words with the column loads (direct layout only).
#ifndef SEINE_AMD_FF_EARLY
#define SEINE_AMD_FF_EARLY 1
#endif
#ifndef SEINE_AMD_DI_PREFETCH
#define SEINE_AMD_DI_PREFETCH 0
#endif
// SEINE_AMD_PIPELINE: single-pass fill (t_cost 1, one lane per wavefront, direct
// layout) that keeps the previous block in registers and loads each reference
// block one step ahead. On by default for RDNA2 (gfx103x), RDNA3 (gfx110x) and
// RDNA4 (gfx120x).
#ifndef SEINE_AMD_PIPELINE
#if defined(__gfx1030__) || defined(__gfx1031__) || defined(__gfx1032__) || defined(__gfx1033__) || \
    defined(__gfx1034__) || defined(__gfx1035__) || defined(__gfx1036__) || \
    defined(__gfx1100__) || defined(__gfx1101__) || defined(__gfx1102__) || defined(__gfx1103__) || \
    defined(__gfx1200__) || defined(__gfx1201__)
#define SEINE_AMD_PIPELINE 1
#else
#define SEINE_AMD_PIPELINE 0
#endif
#endif
// SEINE_AMD_SKIP_FROM (even, at least m_blocks / 2; 0 = off): odd blocks from this
// index on, except the last, are not stored and are rebuilt when referenced. Lane
// layout: blocks below the boundary at their own index, even blocks above it at half
// density, then the last block, two rebuild slots and a sink for skipped stores.
// Requires the pipelined single-pass fill.
#ifndef SEINE_AMD_SKIP_FROM
#define SEINE_AMD_SKIP_FROM 0U
#endif
#if SEINE_AMD_PIPELINE && SEINE_WARPS_PER_BLOCK == 1 && SEINE_AMD_DIRECT_LAYOUT
#define SEINE_AMD_PIPELINE_ACTIVE 1
#else
#define SEINE_AMD_PIPELINE_ACTIVE 0
#endif
#if SEINE_AMD_SKIP_FROM && !SEINE_AMD_PIPELINE_ACTIVE
#error "SEINE_AMD_SKIP_FROM requires the pipelined single-pass fill"
#endif

// Lane l of each aligned 4-lane group receives the value held by lane (l + K) & 3.
// Three fixed rotations (the file has C linkage, so no C++ generics): the DPP
// control word must be a compile-time constant.
#if SEINE_AMD_DPP_SHFL
#define SEINE_DEFINE_QUAD_ROT(K)                                                         \
    __device__ __forceinline__ unsigned long long quad_rot_##K(                         \
        unsigned long long value, unsigned int shfl_base, unsigned int lane             \
    ) {                                                                                  \
        /* quad_perm:[K, K+1, K+2, K+3] (mod 4); all rows/banks, no bound control */    \
        const int lo = __builtin_amdgcn_update_dpp(                                      \
            0, static_cast<int>(static_cast<unsigned int>(value)),                       \
            (((K) & 3) | ((((K) + 1) & 3) << 2) | ((((K) + 2) & 3) << 4) |               \
             ((((K) + 3) & 3) << 6)),                                                    \
            0xF, 0xF, false                                                              \
        );                                                                               \
        const int hi = __builtin_amdgcn_update_dpp(                                      \
            0, static_cast<int>(static_cast<unsigned int>(value >> 32)),                 \
            (((K) & 3) | ((((K) + 1) & 3) << 2) | ((((K) + 2) & 3) << 4) |               \
             ((((K) + 3) & 3) << 6)),                                                    \
            0xF, 0xF, false                                                              \
        );                                                                               \
        (void)shfl_base;                                                                 \
        (void)lane;                                                                      \
        return (static_cast<unsigned long long>(static_cast<unsigned int>(hi)) << 32) |  \
               static_cast<unsigned long long>(static_cast<unsigned int>(lo));           \
    }
#else
#define SEINE_DEFINE_QUAD_ROT(K)                                                         \
    __device__ __forceinline__ unsigned long long quad_rot_##K(                         \
        unsigned long long value, unsigned int shfl_base, unsigned int lane             \
    ) {                                                                                  \
        return coop_shfl(value, shfl_base + ((lane + (K)) & 3U));                        \
    }
#endif
SEINE_DEFINE_QUAD_ROT(1)
SEINE_DEFINE_QUAD_ROT(2)
SEINE_DEFINE_QUAD_ROT(3)

// Broadcast the value held by thread 0 of this cooperative group to the group.
__device__ __forceinline__ unsigned long long coop_broadcast0(unsigned long long value) {
#if SEINE_WARPS_PER_BLOCK == 1
    // One lane per wavefront: thread 0 is wave lane 0, which readfirstlane returns.
    const unsigned int lo = static_cast<unsigned int>(
        __builtin_amdgcn_readfirstlane(static_cast<int>(static_cast<unsigned int>(value)))
    );
    const unsigned int hi = static_cast<unsigned int>(
        __builtin_amdgcn_readfirstlane(static_cast<int>(static_cast<unsigned int>(value >> 32)))
    );
    return (static_cast<unsigned long long>(hi) << 32) | static_cast<unsigned long long>(lo);
#else
    return coop_shfl(value, 0U);
#endif
}

struct Blake2bState {
    unsigned long long h[8];
    unsigned long long t0;
    unsigned long long t1;
    unsigned char buf[BLAKE2B_BLOCK_BYTES];
    unsigned int buflen;
};

constexpr unsigned long long BLAKE2B_IV[8] = {
    0x6A09E667F3BCC908ULL,
    0xBB67AE8584CAA73BULL,
    0x3C6EF372FE94F82BULL,
    0xA54FF53A5F1D36F1ULL,
    0x510E527FADE682D1ULL,
    0x9B05688C2B3E6C1FULL,
    0x1F83D9ABFB41BD6BULL,
    0x5BE0CD19137E2179ULL,
};

constexpr unsigned char BLAKE2B_SIGMA[12][16] = {
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
    {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
    {11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4},
    {7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8},
    {9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13},
    {2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9},
    {12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11},
    {13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10},
    {6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5},
    {10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0},
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
    {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
};

__device__ __forceinline__ void device_memcpy(
    unsigned char *dst,
    const unsigned char *src,
    unsigned int len
) {
    for (unsigned int i = 0U; i < len; ++i) {
        dst[i] = src[i];
    }
}

__device__ __forceinline__ void store_u32_le(unsigned char *dst, unsigned int value) {
    dst[0] = static_cast<unsigned char>(value & 0xFFU);
    dst[1] = static_cast<unsigned char>((value >> 8) & 0xFFU);
    dst[2] = static_cast<unsigned char>((value >> 16) & 0xFFU);
    dst[3] = static_cast<unsigned char>((value >> 24) & 0xFFU);
}

__device__ __forceinline__ void store_u64_le(
    unsigned char *dst,
    unsigned long long value
) {
    for (unsigned int i = 0U; i < 8U; ++i) {
        dst[i] = static_cast<unsigned char>((value >> (8U * i)) & 0xFFULL);
    }
}

__device__ __forceinline__ unsigned long long load_u64_le(const unsigned char *src) {
    unsigned long long value = 0ULL;
    for (unsigned int i = 0U; i < 8U; ++i) {
        value |= static_cast<unsigned long long>(src[i]) << (8U * i);
    }
    return value;
}

// Rotate right by a constant 0 < n < 64, one 32-bit funnel shift per half.
__device__ __forceinline__ unsigned long long rotr64(
    unsigned long long x,
    unsigned int n
) {
    const unsigned int lo = static_cast<unsigned int>(x);
    const unsigned int hi = static_cast<unsigned int>(x >> 32);
    unsigned int rlo;
    unsigned int rhi;
    if (n == 32U) {
        rlo = hi;
        rhi = lo;
    } else if (n < 32U) {
        rlo = __builtin_amdgcn_alignbit(hi, lo, n);
        rhi = __builtin_amdgcn_alignbit(lo, hi, n);
    } else {
        rlo = __builtin_amdgcn_alignbit(lo, hi, n - 32U);
        rhi = __builtin_amdgcn_alignbit(hi, lo, n - 32U);
    }
    return (static_cast<unsigned long long>(rhi) << 32) | static_cast<unsigned long long>(rlo);
}

__device__ __forceinline__ void blake2b_g(
    unsigned long long v[16],
    unsigned int a,
    unsigned int b,
    unsigned int c,
    unsigned int d,
    unsigned long long x,
    unsigned long long y
) {
    v[a] = v[a] + v[b] + x;
    v[d] = rotr64(v[d] ^ v[a], 32U);
    v[c] = v[c] + v[d];
    v[b] = rotr64(v[b] ^ v[c], 24U);
    v[a] = v[a] + v[b] + y;
    v[d] = rotr64(v[d] ^ v[a], 16U);
    v[c] = v[c] + v[d];
    v[b] = rotr64(v[b] ^ v[c], 63U);
}

__device__ __forceinline__ void blake2b_compress(
    Blake2bState &state,
    const unsigned char block[BLAKE2B_BLOCK_BYTES],
    unsigned long long last_block_flag
) {
    unsigned long long m[16];
    unsigned long long v[16];
    for (unsigned int i = 0U; i < 16U; ++i) {
        m[i] = load_u64_le(block + (i * 8U));
    }
    for (unsigned int i = 0U; i < 8U; ++i) {
        v[i] = state.h[i];
        v[i + 8U] = BLAKE2B_IV[i];
    }
    v[12] ^= state.t0;
    v[13] ^= state.t1;
    v[14] ^= last_block_flag;

    for (unsigned int round = 0U; round < 12U; ++round) {
        const unsigned char *s = BLAKE2B_SIGMA[round];
        blake2b_g(v, 0U, 4U, 8U, 12U, m[s[0]], m[s[1]]);
        blake2b_g(v, 1U, 5U, 9U, 13U, m[s[2]], m[s[3]]);
        blake2b_g(v, 2U, 6U, 10U, 14U, m[s[4]], m[s[5]]);
        blake2b_g(v, 3U, 7U, 11U, 15U, m[s[6]], m[s[7]]);
        blake2b_g(v, 0U, 5U, 10U, 15U, m[s[8]], m[s[9]]);
        blake2b_g(v, 1U, 6U, 11U, 12U, m[s[10]], m[s[11]]);
        blake2b_g(v, 2U, 7U, 8U, 13U, m[s[12]], m[s[13]]);
        blake2b_g(v, 3U, 4U, 9U, 14U, m[s[14]], m[s[15]]);
    }

    for (unsigned int i = 0U; i < 8U; ++i) {
        state.h[i] ^= v[i] ^ v[i + 8U];
    }
}

__device__ __forceinline__ void blake2b_init(Blake2bState &state, unsigned int out_len) {
    for (unsigned int i = 0U; i < 8U; ++i) {
        state.h[i] = BLAKE2B_IV[i];
    }
    state.h[0] ^= 0x01010000ULL ^ static_cast<unsigned long long>(out_len);
    state.t0 = 0ULL;
    state.t1 = 0ULL;
    state.buflen = 0U;
    for (unsigned int i = 0U; i < BLAKE2B_BLOCK_BYTES; ++i) {
        state.buf[i] = 0U;
    }
}

__device__ __forceinline__ void blake2b_increment_counter(
    Blake2bState &state,
    unsigned int increment
) {
    const unsigned long long prev = state.t0;
    state.t0 += static_cast<unsigned long long>(increment);
    if (state.t0 < prev) {
        state.t1 += 1ULL;
    }
}

__device__ __forceinline__ void blake2b_update(
    Blake2bState &state,
    const unsigned char *input,
    unsigned int input_len
) {
    unsigned int offset = 0U;
    while (offset < input_len) {
        const unsigned int space = BLAKE2B_BLOCK_BYTES - state.buflen;
        const unsigned int take = (input_len - offset < space)
            ? (input_len - offset)
            : space;
        device_memcpy(state.buf + state.buflen, input + offset, take);
        state.buflen += take;
        offset += take;

        if (state.buflen == BLAKE2B_BLOCK_BYTES) {
            blake2b_increment_counter(state, BLAKE2B_BLOCK_BYTES);
            blake2b_compress(state, state.buf, 0ULL);
            state.buflen = 0U;
        }
    }
}

__device__ __forceinline__ void blake2b_final(
    Blake2bState &state,
    unsigned char *out,
    unsigned int out_len
) {
    blake2b_increment_counter(state, state.buflen);
    for (unsigned int i = state.buflen; i < BLAKE2B_BLOCK_BYTES; ++i) {
        state.buf[i] = 0U;
    }
    blake2b_compress(state, state.buf, 0xFFFFFFFFFFFFFFFFULL);

    unsigned char digest[BLAKE2B_OUT_BYTES];
    for (unsigned int i = 0U; i < 8U; ++i) {
        store_u64_le(digest + (i * 8U), state.h[i]);
    }
    for (unsigned int i = 0U; i < out_len; ++i) {
        out[i] = digest[i];
    }
}

__device__ __forceinline__ void blake2b_hash(
    const unsigned char *input,
    unsigned int input_len,
    unsigned char *out,
    unsigned int out_len
) {
    Blake2bState state;
    blake2b_init(state, out_len);
    blake2b_update(state, input, input_len);
    blake2b_final(state, out, out_len);
}

__device__ __forceinline__ void blake2b_long_1024_words(
    const unsigned char *input,
    unsigned int input_len,
    unsigned long long *out_words
) {
    unsigned char out_len_le[4];
    store_u32_le(out_len_le, 1024U);

    unsigned char last_output[64];
    Blake2bState state;
    blake2b_init(state, 64U);
    blake2b_update(state, out_len_le, 4U);
    blake2b_update(state, input, input_len);
    blake2b_final(state, last_output, 64U);

    unsigned int out_word_idx = 0U;
    for (unsigned int i = 0U; i < 4U; ++i) {
        out_words[out_word_idx++] = load_u64_le(last_output + i * 8U);
    }
    unsigned int counter = 32U;

    while ((1024U - counter) > 64U) {
        unsigned char next_output[64];
        blake2b_hash(last_output, 64U, next_output, 64U);
        for (unsigned int i = 0U; i < 64U; ++i) {
            last_output[i] = next_output[i];
        }
        for (unsigned int i = 0U; i < 4U; ++i) {
            out_words[out_word_idx++] = load_u64_le(last_output + i * 8U);
        }
        counter += 32U;
    }

    unsigned char tail_output[64];
    blake2b_hash(last_output, 64U, tail_output, 64U);
    for (unsigned int i = 0U; i < 8U; ++i) {
        out_words[out_word_idx++] = load_u64_le(tail_output + i * 8U);
    }
}

__device__ __forceinline__ void blake2b_long_32_from_words(
    const unsigned long long *block_words,
    unsigned char out_hash[POW_OUTPUT_BYTES]
) {
    unsigned char out_len_le[4];
    store_u32_le(out_len_le, POW_OUTPUT_BYTES);

    Blake2bState state;
    blake2b_init(state, POW_OUTPUT_BYTES);
    blake2b_update(state, out_len_le, 4U);

    unsigned char chunk[128];
    for (unsigned int chunk_idx = 0U; chunk_idx < 8U; ++chunk_idx) {
        const unsigned long long *chunk_words = block_words + chunk_idx * 16U;
        for (unsigned int i = 0U; i < 16U; ++i) {
            store_u64_le(chunk + i * 8U, chunk_words[i]);
        }
        blake2b_update(state, chunk, 128U);
    }
    blake2b_final(state, out_hash, POW_OUTPUT_BYTES);
}

__device__ __forceinline__ bool hash_meets_target_be(
    const unsigned char hash[POW_OUTPUT_BYTES],
    const unsigned char target[POW_OUTPUT_BYTES]
) {
    for (unsigned int i = 0U; i < POW_OUTPUT_BYTES; ++i) {
        if (hash[i] < target[i]) {
            return true;
        }
        if (hash[i] > target[i]) {
            return false;
        }
    }
    return true;
}

__global__ void build_seed_blocks_kernel(
    const unsigned char *__restrict__ header_base,
    unsigned int header_base_len,
    const unsigned long long *__restrict__ nonces,
    unsigned int active_hashes,
    unsigned int m_cost_kib,
    unsigned int t_cost,
    unsigned long long *__restrict__ seed_blocks
) {
    const unsigned int hash_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (hash_idx >= active_hashes) {
        return;
    }

    unsigned char h0[64];
    {
        unsigned char tmp4[4];
        unsigned char tmp8[8];
        Blake2bState state;
        blake2b_init(state, 64U);

        store_u32_le(tmp4, 1U);
        blake2b_update(state, tmp4, 4U);

        store_u32_le(tmp4, POW_OUTPUT_BYTES);
        blake2b_update(state, tmp4, 4U);

        store_u32_le(tmp4, m_cost_kib);
        blake2b_update(state, tmp4, 4U);

        store_u32_le(tmp4, t_cost);
        blake2b_update(state, tmp4, 4U);

        store_u32_le(tmp4, ARGON2_VERSION_V13);
        blake2b_update(state, tmp4, 4U);

        store_u32_le(tmp4, ARGON2_ALGORITHM_ID);
        blake2b_update(state, tmp4, 4U);

        store_u32_le(tmp4, 8U);
        blake2b_update(state, tmp4, 4U);

        store_u64_le(tmp8, nonces[hash_idx]);
        blake2b_update(state, tmp8, 8U);

        store_u32_le(tmp4, header_base_len);
        blake2b_update(state, tmp4, 4U);
        blake2b_update(state, header_base, header_base_len);

        store_u32_le(tmp4, 0U);
        blake2b_update(state, tmp4, 4U);
        blake2b_update(state, tmp4, 4U);

        blake2b_final(state, h0, 64U);
    }

    unsigned char seed_input[72];
    for (unsigned int i = 0U; i < 64U; ++i) {
        seed_input[i] = h0[i];
    }
    // lane index for p=1 is always zero.
    seed_input[68] = 0U;
    seed_input[69] = 0U;
    seed_input[70] = 0U;
    seed_input[71] = 0U;

    for (unsigned int seed_idx = 0U; seed_idx < 2U; ++seed_idx) {
        unsigned long long *seed_out_words =
            seed_blocks + static_cast<unsigned long long>(hash_idx) * 256ULL +
            static_cast<unsigned long long>(seed_idx) * 128ULL;
        store_u32_le(seed_input + 64U, seed_idx);
        blake2b_long_1024_words(seed_input, 72U, seed_out_words);
    }
}

// SEINE_AMD_BLAMKA_MAD: BlaMka as one 64-bit multiply-add plus a 32-bit fix-up.
// On by default for RDNA2 (gfx103x) and RDNA3 (gfx110x).
#ifndef SEINE_AMD_BLAMKA_MAD
#if defined(__gfx1030__) || defined(__gfx1031__) || defined(__gfx1032__) || defined(__gfx1033__) || \
    defined(__gfx1034__) || defined(__gfx1035__) || defined(__gfx1036__) || \
    defined(__gfx1100__) || defined(__gfx1101__) || defined(__gfx1102__) || defined(__gfx1103__)
#define SEINE_AMD_BLAMKA_MAD 1
#else
#define SEINE_AMD_BLAMKA_MAD 0
#endif
#endif
#if SEINE_AMD_BLAMKA_MAD
// a * b + c with a and b 32-bit; kept as a single instruction.
__device__ __forceinline__ unsigned long long mad_u64_u32(
    unsigned int a, unsigned int b, unsigned long long c
) {
    unsigned long long r;
    asm("v_mad_u64_u32 %0, null, %1, %2, %3" : "=v"(r) : "v"(a), "v"(b), "v"(c));
    return r;
}
#endif

// x is the operand produced two steps earlier, y the one produced by the previous step.
__device__ __forceinline__ unsigned long long blamka(
    unsigned long long x,
    unsigned long long y
) {
#if SEINE_AMD_BLAMKA_MAD
    // 2*x_lo = (2*x_lo mod 2^32) + (x_lo >> 31) * 2^32, so x + y + 2*x_lo*y_lo equals
    // (x + (2*x_lo mod 2^32) * y_lo) + (y_lo | ((((x_lo >> 31) ? y_lo : 0) + y_hi) << 32)).
    const unsigned int xl = static_cast<unsigned int>(x);
    const unsigned int yl = static_cast<unsigned int>(y);
    const unsigned int yh = static_cast<unsigned int>(y >> 32);
    const unsigned int xmask = static_cast<unsigned int>(static_cast<int>(xl) >> 31);
    const unsigned long long p = mad_u64_u32(xl << 1, yl, x);
    unsigned long long q = static_cast<unsigned long long>(yl) |
                           (static_cast<unsigned long long>((yl & xmask) + yh) << 32);
    asm("" : "+v"(q));
    return p + q;
#else
    const unsigned long long low = 0xFFFFFFFFULL;
    return x + y + 2ULL * (x & low) * (y & low);
#endif
}

__device__ __forceinline__ void permute_step(
    unsigned long long &a,
    unsigned long long &b,
    unsigned long long &c,
    unsigned long long &d
) {
    a = blamka(a, b);
    d = rotr64(d ^ a, 32U);
    c = blamka(c, d);
    b = rotr64(b ^ c, 24U);

    a = blamka(a, b);
    d = rotr64(d ^ a, 16U);
    c = blamka(c, d);
    b = rotr64(b ^ c, 63U);
}

// Returns word 0 of the block just written (valid when SEINE_AMD_CARRY_WORD0).
// PRELOADED: the four reference-block words this thread needs arrive in l0..l3
// instead of being loaded from lhs. prefetch: when non-null, the next block's
// reference block, whose four words are loaded into pf0..pf3 after this block's
// own loads.
__device__ __forceinline__ unsigned long long compress_block_coop_impl(
    bool PRELOADED,
    const unsigned long long *__restrict__ rhs,
    const unsigned long long *__restrict__ lhs,
    unsigned long long *__restrict__ out,
    bool xor_out,
    unsigned long long *__restrict__ scratch_r,
    unsigned long long *__restrict__ scratch_q,
    unsigned int tid,
    unsigned long long l0,
    unsigned long long l1,
    unsigned long long l2,
    unsigned long long l3,
    const unsigned long long *__restrict__ prefetch,
    unsigned long long &pf0,
    unsigned long long &pf1,
    unsigned long long &pf2,
    unsigned long long &pf3
) {
    const unsigned int state = tid >> 2;
    const unsigned int lane = tid & 3U;
    const unsigned int shfl_base = state * 4U; // absolute lane base for shuffles
    const unsigned int base = state * 16U;
    unsigned long long a;
    unsigned long long b;
    unsigned long long c;
    unsigned long long d;
#if SEINE_AMD_DIRECT_LAYOUT
    // Row layout straight from global memory: thread (state, lane) owns column
    // `lane` of row `state`. The XORed input stays in scratch_r for the final
    // feed-forward, where the thread holding each word after the column round
    // reads it back.
    {
        const unsigned int w0 = base + lane;
        const unsigned long long r0 = rhs[w0]       ^ (PRELOADED ? l0 : lhs[w0]);
        const unsigned long long r1 = rhs[w0 + 4U]  ^ (PRELOADED ? l1 : lhs[w0 + 4U]);
        const unsigned long long r2 = rhs[w0 + 8U]  ^ (PRELOADED ? l2 : lhs[w0 + 8U]);
        const unsigned long long r3 = rhs[w0 + 12U] ^ (PRELOADED ? l3 : lhs[w0 + 12U]);
        if (prefetch != nullptr) {
            pf0 = prefetch[w0];
            pf1 = prefetch[w0 + 4U];
            pf2 = prefetch[w0 + 8U];
            pf3 = prefetch[w0 + 12U];
        }
        scratch_r[w0]       = r0;
        scratch_r[w0 + 4U]  = r1;
        scratch_r[w0 + 8U]  = r2;
        scratch_r[w0 + 12U] = r3;
        a = r0;
        b = r1;
        c = r2;
        d = r3;
    }
#else
    // Issue all global loads into registers first.
    {
        const unsigned long long r0 = rhs[tid]       ^ (PRELOADED ? l0 : lhs[tid]);
        const unsigned long long r1 = rhs[tid + 32U]  ^ (PRELOADED ? l1 : lhs[tid + 32U]);
        const unsigned long long r2 = rhs[tid + 64U]  ^ (PRELOADED ? l2 : lhs[tid + 64U]);
        const unsigned long long r3 = rhs[tid + 96U]  ^ (PRELOADED ? l3 : lhs[tid + 96U]);
        if (prefetch != nullptr) {
            pf0 = prefetch[tid];
            pf1 = prefetch[tid + 32U];
            pf2 = prefetch[tid + 64U];
            pf3 = prefetch[tid + 96U];
        }
        scratch_r[tid]       = r0;  scratch_q[tid]       = r0;
        scratch_r[tid + 32U] = r1;  scratch_q[tid + 32U] = r1;
        scratch_r[tid + 64U] = r2;  scratch_q[tid + 64U] = r2;
        scratch_r[tid + 96U] = r3;  scratch_q[tid + 96U] = r3;
    }
    coop_sync();
    // Row columns: load from shared in row layout.
    a = scratch_q[base + lane];
    b = scratch_q[base + 4U + lane];
    c = scratch_q[base + 8U + lane];
    d = scratch_q[base + 12U + lane];
#endif

    // G-rounds with wave-shuffle fusion and closed-form indexing.
    // --- Fused row rounds (columns + diagonals) ---
    {
        permute_step(a, b, c, d);
        // Row diagonals: rotate outputs within the quad instead of a shared-memory
        // round-trip. Thread lane l needs: a from self, b from (l+1)&3, c from
        // (l+2)&3, d from (l+3)&3.
        unsigned long long a2 = a; // same thread, same position
        unsigned long long b2 = quad_rot_1(b, shfl_base, lane);
        unsigned long long c2 = quad_rot_2(c, shfl_base, lane);
        unsigned long long d2 = quad_rot_3(d, shfl_base, lane);
        permute_step(a2, b2, c2, d2);
        // Store diagonal results to shared memory for column rounds.
        // Diagonal indices: base + lane, base + 4 + (lane+1)&3, base + 8 + (lane+2)&3, base + 12 + (lane+3)&3
        scratch_q[base + lane] = a2;
        scratch_q[base + 4U + ((lane + 1U) & 3U)] = b2;
        scratch_q[base + 8U + ((lane + 2U) & 3U)] = c2;
        scratch_q[base + 12U + ((lane + 3U) & 3U)] = d2;
    }
    coop_sync();

    // --- Fused column rounds (columns + diagonals) ---
    unsigned long long word0 = 0ULL;
    {
        const unsigned int cb = state * 2U;
        const unsigned int col_sub = (lane >> 1) * 16U + (lane & 1U);
        // Column columns: load from shared, permute
        unsigned long long ca = scratch_q[cb + col_sub];
        unsigned long long cc = scratch_q[cb + 32U + col_sub];
        unsigned long long cd = scratch_q[cb + 64U + col_sub];
        unsigned long long ce = scratch_q[cb + 96U + col_sub];
        const unsigned int p0 = lane;
        const unsigned int p1 = (lane + 1U) & 3U;
        const unsigned int p2 = (lane + 2U) & 3U;
        const unsigned int p3 = (lane + 3U) & 3U;
        const unsigned int q0 = cb + (p0 >> 1) * 16U + (p0 & 1U);
        const unsigned int q1 = cb + 32U + (p1 >> 1) * 16U + (p1 & 1U);
        const unsigned int q2 = cb + 64U + (p2 >> 1) * 16U + (p2 & 1U);
        const unsigned int q3 = cb + 96U + (p3 >> 1) * 16U + (p3 & 1U);
#if SEINE_AMD_DIRECT_LAYOUT && SEINE_AMD_FF_EARLY
        const unsigned long long ff0 = scratch_r[q0];
        const unsigned long long ff1 = scratch_r[q1];
        const unsigned long long ff2 = scratch_r[q2];
        const unsigned long long ff3 = scratch_r[q3];
        __builtin_amdgcn_sched_barrier(0);
#endif
        permute_step(ca, cc, cd, ce);
        // Column diagonals: quad rotation instead of shared-memory round-trip.
        unsigned long long ca2 = ca;
        unsigned long long cc2 = quad_rot_1(cc, shfl_base, lane);
        unsigned long long cd2 = quad_rot_2(cd, shfl_base, lane);
        unsigned long long ce2 = quad_rot_3(ce, shfl_base, lane);
        permute_step(ca2, cc2, cd2, ce2);
#if SEINE_AMD_DIRECT_LAYOUT
        // Feed-forward and store from registers; thread 0 holds word 0 (q0 == 0)
        // for the broadcast below.
#if SEINE_AMD_FF_EARLY
        const unsigned long long m0 = ca2 ^ ff0;
        const unsigned long long m1 = cc2 ^ ff1;
        const unsigned long long m2 = cd2 ^ ff2;
        const unsigned long long m3 = ce2 ^ ff3;
#else
        const unsigned long long m0 = ca2 ^ scratch_r[q0];
        const unsigned long long m1 = cc2 ^ scratch_r[q1];
        const unsigned long long m2 = cd2 ^ scratch_r[q2];
        const unsigned long long m3 = ce2 ^ scratch_r[q3];
#endif
        if (xor_out) {
            const unsigned long long o0 = out[q0] ^ m0;
            const unsigned long long o1 = out[q1] ^ m1;
            const unsigned long long o2 = out[q2] ^ m2;
            const unsigned long long o3 = out[q3] ^ m3;
            out[q0] = o0;
            out[q1] = o1;
            out[q2] = o2;
            out[q3] = o3;
            word0 = o0;
        } else {
            out[q0] = m0;
            out[q1] = m1;
            out[q2] = m2;
            out[q3] = m3;
            word0 = m0;
        }
#else
        // Store diagonal results back.
        scratch_q[q0] = ca2;
        scratch_q[q1] = cc2;
        scratch_q[q2] = cd2;
        scratch_q[q3] = ce2;
#endif
    }
#if !SEINE_AMD_DIRECT_LAYOUT
    coop_sync();

    // Unrolled final store: read all shared-memory values into registers
    // first, then issue all global stores.
    {
        const unsigned long long m0 = scratch_q[tid]       ^ scratch_r[tid];
        const unsigned long long m1 = scratch_q[tid + 32U] ^ scratch_r[tid + 32U];
        const unsigned long long m2 = scratch_q[tid + 64U] ^ scratch_r[tid + 64U];
        const unsigned long long m3 = scratch_q[tid + 96U] ^ scratch_r[tid + 96U];
        if (xor_out) {
            const unsigned long long o0 = out[tid] ^ m0;
            out[tid]       = o0;
            out[tid + 32U] ^= m1;
            out[tid + 64U] ^= m2;
            out[tid + 96U] ^= m3;
            word0 = o0;
        } else {
            out[tid]       = m0;
            out[tid + 32U] = m1;
            out[tid + 64U] = m2;
            out[tid + 96U] = m3;
            word0 = m0;
        }
    }
#endif
#if SEINE_AMD_CARRY_WORD0
    return coop_broadcast0(word0);
#else
    (void)word0;
    return 0ULL;
#endif
}

__device__ __forceinline__ unsigned long long compress_block_coop(
    const unsigned long long *__restrict__ rhs,
    const unsigned long long *__restrict__ lhs,
    unsigned long long *__restrict__ out,
    bool xor_out,
    unsigned long long *__restrict__ scratch_r,
    unsigned long long *__restrict__ scratch_q,
    unsigned int tid
) {
    unsigned long long unused0 = 0ULL;
    unsigned long long unused1 = 0ULL;
    unsigned long long unused2 = 0ULL;
    unsigned long long unused3 = 0ULL;
    return compress_block_coop_impl(
        false, rhs, lhs, out, xor_out, scratch_r, scratch_q, tid,
        0ULL, 0ULL, 0ULL, 0ULL, nullptr, unused0, unused1, unused2, unused3
    );
}

__device__ __forceinline__ void update_address_block_coop(
    unsigned long long *__restrict__ address_block,
    unsigned long long *__restrict__ input_block,
    const unsigned long long *__restrict__ zero_block,
    unsigned long long *__restrict__ scratch_r,
    unsigned long long *__restrict__ scratch_q,
    unsigned int tid
) {
    if (tid == 0U) {
        input_block[6] += 1ULL;
    }
    coop_sync();
    compress_block_coop(
        zero_block,
        input_block,
        address_block,
        false,
        scratch_r,
        scratch_q,
        tid
    );
    compress_block_coop(
        zero_block,
        address_block,
        address_block,
        false,
        scratch_r,
        scratch_q,
        tid
    );
}

// Lane memory is allocated in chunks of `lanes_per_chunk` lanes (one hipMalloc
// each) because a single allocation hits per-allocation limits long before VRAM
// runs out on large cards. `lane_chunks` is a
// device-side table of chunk base pointers.
__device__ __forceinline__ unsigned long long *lane_base(
    unsigned long long *const *lane_chunks,
    unsigned int lanes_per_chunk,
    unsigned int lane,
    unsigned long long lane_stride_words
) {
    return lane_chunks[lane / lanes_per_chunk]
        + static_cast<unsigned long long>(lane % lanes_per_chunk) * lane_stride_words;
}

__global__ void touch_lane_memory_kernel(
    unsigned long long *const *lane_chunks,
    unsigned int lanes_per_chunk,
    unsigned int lanes_active,
    unsigned long long lane_stride_words
) {
    const unsigned int lane = blockIdx.x * blockDim.x + threadIdx.x;
    if (lane >= lanes_active || lane_stride_words == 0ULL || lanes_per_chunk == 0U) {
        return;
    }

    volatile unsigned long long *lane_ptr =
        lane_base(lane_chunks, lanes_per_chunk, lane, lane_stride_words);

    // Touch one 4 KiB page at a time (512 x u64) to force physical commit.
    for (unsigned long long word = 0ULL; word < lane_stride_words; word += 512ULL) {
        unsigned long long v = lane_ptr[word];
        lane_ptr[word] = v;
    }

    const unsigned long long tail = lane_stride_words - 1ULL;
    unsigned long long tail_v = lane_ptr[tail];
    lane_ptr[tail] = tail_v;
}

__device__ __forceinline__ unsigned long long skip_last_slot(unsigned int m_blocks) {
    return static_cast<unsigned long long>(SEINE_AMD_SKIP_FROM) +
        static_cast<unsigned long long>((m_blocks - SEINE_AMD_SKIP_FROM) / 2U);
}

// Blocks per lane in device memory.
__device__ __forceinline__ unsigned long long lane_stride_blocks(unsigned int m_blocks) {
    return (SEINE_AMD_SKIP_FROM != 0U) ? skip_last_slot(m_blocks) + 4ULL
                                       : static_cast<unsigned long long>(m_blocks);
}

__device__ __forceinline__ bool block_skipped(unsigned int index, unsigned int m_blocks) {
    return SEINE_AMD_SKIP_FROM != 0U && index >= SEINE_AMD_SKIP_FROM && (index & 1U) != 0U &&
        index != m_blocks - 1U;
}

// Device slot of a stored block.
__device__ __forceinline__ unsigned long long block_slot(unsigned int index, unsigned int m_blocks) {
    if (SEINE_AMD_SKIP_FROM == 0U || index < SEINE_AMD_SKIP_FROM) {
        return index;
    }
    if (index == m_blocks - 1U) {
        return skip_last_slot(m_blocks);
    }
    return static_cast<unsigned long long>(SEINE_AMD_SKIP_FROM) +
        static_cast<unsigned long long>((index - SEINE_AMD_SKIP_FROM) >> 1);
}

// Slot of a reference block for the hot load: (i + min(i, S)) / 2 is i below the
// boundary and S + (i - S) / 2 above it, and a skipped (odd) index lands on its stored
// predecessor, so the address never waits on the skip test. References are never the
// last block. Returned as a word offset.
__device__ __forceinline__ unsigned long long ref_word_offset(unsigned int index) {
    const unsigned int doubled = index + (index < SEINE_AMD_SKIP_FROM ? index : SEINE_AMD_SKIP_FROM);
    return static_cast<unsigned long long>(doubled & ~1U) * 64ULL;
}

// Slot a block's store goes to: its own, or the sink when the store is skipped.
__device__ __forceinline__ unsigned long long store_slot(unsigned int index, unsigned int m_blocks) {
    return block_skipped(index, m_blocks) ? skip_last_slot(m_blocks) + 3ULL
                                          : block_slot(index, m_blocks);
}

#if SEINE_AMD_PIPELINE_ACTIVE
// Pass-0 reference index for block `cur`: the reference area is every block
// before `cur` except its predecessor.
__device__ __forceinline__ unsigned int pass0_ref_index(
    unsigned long long rand64,
    unsigned int cur
) {
    const unsigned long long area = static_cast<unsigned long long>(cur) - 1ULL;
    unsigned long long map = rand64 & 0xFFFFFFFFULL;
    map = (map * map) >> 32;
    return static_cast<unsigned int>(area - 1ULL - ((area * map) >> 32));
}


// Thread (state, lane) owns words w0, w0 + 4, w0 + 8, w0 + 12 of a block in the
// row layout (w0 = state * 16 + lane).
__device__ __forceinline__ void load_row_words(
    const unsigned long long *block,
    unsigned int w0,
    unsigned long long &v0,
    unsigned long long &v1,
    unsigned long long &v2,
    unsigned long long &v3
) {
    v0 = block[w0];
    v1 = block[w0 + 4U];
    v2 = block[w0 + 8U];
    v3 = block[w0 + 12U];
}

// One block of the pipelined fill: x (previous block, row layout) XOR y (reference
// block, row layout) through the row and column rounds. Stores the block to `out` in
// column layout, leaves it in x in row layout and sets word0. NEXT_LOADS runs
// after word 0 is known and before the stores, so the caller can issue the next
// reference load there.
#define SEINE_PIPELINED_BLOCK(OUT_PTR, NEXT_LOADS)                                    \
    {                                                                                \
        unsigned long long a = x0 ^ y0;                                              \
        unsigned long long b = x1 ^ y1;                                              \
        unsigned long long c = x2 ^ y2;                                              \
        unsigned long long d = x3 ^ y3;                                              \
        scratch_r[w0] = a;                                                           \
        scratch_r[w0 + 4U] = b;                                                      \
        scratch_r[w0 + 8U] = c;                                                      \
        scratch_r[w0 + 12U] = d;                                                     \
        permute_step(a, b, c, d);                                                    \
        unsigned long long b2 = quad_rot_1(b, shfl_base, lane);                      \
        unsigned long long c2 = quad_rot_2(c, shfl_base, lane);                      \
        unsigned long long d2 = quad_rot_3(d, shfl_base, lane);                      \
        permute_step(a, b2, c2, d2);                                                 \
        scratch_q[w0] = a;                                                           \
        scratch_q[state * 16U + 4U + p1] = b2;                                       \
        scratch_q[state * 16U + 8U + p2] = c2;                                       \
        scratch_q[state * 16U + 12U + p3] = d2;                                      \
        coop_sync();                                                                 \
        unsigned long long ca = scratch_q[cb + col_sub];                             \
        unsigned long long cc = scratch_q[cb + 32U + col_sub];                       \
        unsigned long long cd = scratch_q[cb + 64U + col_sub];                       \
        unsigned long long ce = scratch_q[cb + 96U + col_sub];                       \
        const unsigned long long ff0 = scratch_r[q0];                                \
        const unsigned long long ff1 = scratch_r[q1];                                \
        const unsigned long long ff2 = scratch_r[q2];                                \
        const unsigned long long ff3 = scratch_r[q3];                                \
        __builtin_amdgcn_sched_barrier(0);                                           \
        permute_step(ca, cc, cd, ce);                                                \
        unsigned long long cc2 = quad_rot_1(cc, shfl_base, lane);                    \
        unsigned long long cd2 = quad_rot_2(cd, shfl_base, lane);                    \
        unsigned long long ce2 = quad_rot_3(ce, shfl_base, lane);                    \
        permute_step(ca, cc2, cd2, ce2);                                             \
        const unsigned long long m0 = ca ^ ff0;                                      \
        const unsigned long long m1 = cc2 ^ ff1;                                     \
        const unsigned long long m2 = cd2 ^ ff2;                                     \
        const unsigned long long m3 = ce2 ^ ff3;                                     \
        word0 = coop_broadcast0(m0);                                                 \
        NEXT_LOADS                                                                   \
        unsigned long long *out_block = (OUT_PTR);                                   \
        out_block[q0] = m0;                                                          \
        out_block[q1] = m1;                                                          \
        out_block[q2] = m2;                                                          \
        out_block[q3] = m3;                                                          \
        coop_sync();                                                                 \
        scratch_q[q0] = m0;                                                          \
        scratch_q[q1] = m1;                                                          \
        scratch_q[q2] = m2;                                                          \
        scratch_q[q3] = m3;                                                          \
        coop_sync();                                                                 \
        x0 = scratch_q[w0];                                                          \
        x1 = scratch_q[w0 + 4U];                                                     \
        x2 = scratch_q[w0 + 8U];                                                     \
        x3 = scratch_q[w0 + 12U];                                                    \
        coop_sync();                                                                 \
    }

// Rebuilds skipped block `index` of a single-pass fill: B[i] = G(B[i - 1], B[ref(i)]),
// where B[i - 1] is stored and B[ref(i)] may itself be skipped. The chain of skipped
// blocks is walked down to a stored one (indices kept in `stack`, 256 entries), then
// compressed back up through the two rebuild slots. Returns the rebuilt block.
__device__ __forceinline__ const unsigned long long *rebuild_skipped_block(
    unsigned long long *memory,
    unsigned int m_blocks,
    unsigned int index,
    unsigned int *__restrict__ stack,
    unsigned long long *__restrict__ scratch_r,
    unsigned long long *__restrict__ scratch_q,
    unsigned int tid
) {
    unsigned int depth = 0U;
    while (block_skipped(index, m_blocks) && depth < 256U) {
        if (tid == 0U) stack[depth] = index;
        ++depth;
        const unsigned long long prev_word0 = memory[block_slot(index - 1U, m_blocks) * 128ULL];
        index = pass0_ref_index(prev_word0, index);
    }
    coop_sync();
    const unsigned long long rebuild_slot = skip_last_slot(m_blocks) + 1ULL;
    const unsigned long long *src = memory + block_slot(index, m_blocks) * 128ULL;
    while (depth > 0U) {
        --depth;
        const unsigned int block = stack[depth];
        unsigned long long *dst = memory + (rebuild_slot + (depth & 1U)) * 128ULL;
        compress_block_coop(
            memory + block_slot(block - 1U, m_blocks) * 128ULL, src, dst, false,
            scratch_r, scratch_q, tid
        );
        __threadfence_block();
        coop_sync();
        src = dst;
    }
    return src;
}

// Fills blocks 2..m_blocks-1 of one lane for t_cost 1. Returns false when the
// cancel flag stopped it. Each loop body issues exactly one set of reference loads
// (the next block's, with the current block's index standing in when there is no
// next), so the previous set is always the one being waited for.
__device__ __forceinline__ bool fill_single_pass_pipelined(
    unsigned long long *memory,
    unsigned int m_blocks,
    unsigned long long *__restrict__ my_address,
    unsigned long long *__restrict__ my_input,
    unsigned long long *__restrict__ my_zero,
    unsigned long long *__restrict__ scratch_r,
    unsigned long long *__restrict__ scratch_q,
    unsigned int tid,
    const volatile unsigned int *__restrict__ cancel_flag
) {
    const unsigned int segment_length = m_blocks / 4U;
    const unsigned int di_end = 2U * segment_length;
    const unsigned int state = tid >> 2;
    const unsigned int lane = tid & 3U;
    const unsigned int shfl_base = state * 4U;
    const unsigned int w0 = state * 16U + lane;
    const unsigned int cb = state * 2U;
    const unsigned int p1 = (lane + 1U) & 3U;
    const unsigned int p2 = (lane + 2U) & 3U;
    const unsigned int p3 = (lane + 3U) & 3U;
    const unsigned int q0 = cb + (lane >> 1) * 16U + (lane & 1U);
    const unsigned int q1 = cb + 32U + (p1 >> 1) * 16U + (p1 & 1U);
    const unsigned int q2 = cb + 64U + (p2 >> 1) * 16U + (p2 & 1U);
    const unsigned int q3 = cb + 96U + (p3 >> 1) * 16U + (p3 & 1U);
    const unsigned int col_sub = (lane >> 1) * 16U + (lane & 1U);

    unsigned long long x0, x1, x2, x3;
    unsigned long long y0, y1, y2, y3;
    unsigned long long word0 = 0ULL;
    load_row_words(memory + 128ULL, w0, x0, x1, x2, x3);

    // Data-independent half: slices 0 and 1.
    for (unsigned int slice = 0U; slice < 2U; ++slice) {
        for (unsigned int i = tid; i < 128U; i += ARGON2_COOP_THREADS) {
            my_address[i] = 0ULL;
            my_input[i] = 0ULL;
        }
        if (tid == 0U) {
            my_input[0] = 0ULL; // pass
            my_input[1] = 0ULL; // lane index (p=1)
            my_input[2] = static_cast<unsigned long long>(slice);
            my_input[3] = static_cast<unsigned long long>(m_blocks);
            my_input[4] = 1ULL; // t_cost
            my_input[5] = 2ULL; // Argon2id
        }
        coop_sync();
        update_address_block_coop(my_address, my_input, my_zero, scratch_r, scratch_q, tid);
        coop_sync();
        const unsigned int first_block = (slice == 0U) ? 2U : 0U;
        unsigned int cur_ref = pass0_ref_index(
            my_address[first_block], slice * segment_length + first_block
        );
        load_row_words(memory + static_cast<unsigned long long>(cur_ref) * 128ULL, w0, y0, y1, y2, y3);
        for (unsigned int block = first_block; block < segment_length; ++block) {
            const unsigned int cur = slice * segment_length + block;
            if (((block - first_block) % CANCEL_CHECK_BLOCK_INTERVAL) == 0U) {
                unsigned int cancel_val = 0U;
                if (tid == 0U) cancel_val = cancel_flag[0];
                cancel_val = coop_shfl_u32(cancel_val, 0U);
                if (cancel_val != 0U) {
                    return false;
                }
            }
            // The next block's address word; a fresh address block is generated one
            // block early (this block's word was already used).
            const bool has_next = (block + 1U) < segment_length;
            if (has_next && ((block + 1U) & 127U) == 0U) {
                update_address_block_coop(my_address, my_input, my_zero, scratch_r, scratch_q, tid);
                coop_sync();
            }
            const unsigned int next_ref = has_next
                ? pass0_ref_index(my_address[(block + 1U) & 127U], cur + 1U)
                : cur_ref;
            unsigned long long n0, n1, n2, n3;
            load_row_words(memory + static_cast<unsigned long long>(next_ref) * 128ULL, w0, n0, n1, n2, n3);
            SEINE_PIPELINED_BLOCK(memory + static_cast<unsigned long long>(cur) * 128ULL, )
            y0 = n0;
            y1 = n1;
            y2 = n2;
            y3 = n3;
            cur_ref = next_ref;
        }
    }

    // Data-dependent half: the next reference index is word 0 of the block just
    // computed, so its load goes out before this block is stored.
    load_row_words(
        memory + static_cast<unsigned long long>(pass0_ref_index(word0, di_end)) * 128ULL,
        w0, y0, y1, y2, y3
    );
#if SEINE_AMD_SKIP_FROM
    // A reference that was not stored gets a placeholder load here and is rebuilt at
    // the top of the next iteration (address-block LDS is free in this half), so the
    // block keeps a single load site.
    bool rebuild_pending = false;
    unsigned int rebuild_index = 0U;
    for (unsigned int cur = di_end; cur < m_blocks; ++cur) {
        if (((cur - di_end) % CANCEL_CHECK_BLOCK_INTERVAL) == 0U) {
            unsigned int cancel_val = 0U;
            if (tid == 0U) cancel_val = cancel_flag[0];
            cancel_val = coop_shfl_u32(cancel_val, 0U);
            if (cancel_val != 0U) {
                return false;
            }
        }
        if (rebuild_pending) {
            // B[r] = G(B[r - 1], B[ref(r)]) in registers with the block macro (its store
            // goes to the sink); a reference that is itself skipped is rebuilt in the slots.
            // The placeholder load already brought B[r - 1] (ref_word_offset of an odd
            // index is its stored predecessor), so it becomes the rebuild's previous block.
            const unsigned long long sx0 = x0, sx1 = x1, sx2 = x2, sx3 = x3;
            x0 = y0;
            x1 = y1;
            x2 = y2;
            x3 = y3;
            const unsigned int inner_ref = pass0_ref_index(coop_broadcast0(x0), rebuild_index);
            if (block_skipped(inner_ref, m_blocks)) {
                const unsigned long long *rebuilt = rebuild_skipped_block(
                    memory, m_blocks, inner_ref, reinterpret_cast<unsigned int *>(my_address),
                    my_input, my_zero, tid
                );
                load_row_words(rebuilt, w0, y0, y1, y2, y3);
            } else {
                load_row_words(memory + block_slot(inner_ref, m_blocks) * 128ULL, w0, y0, y1, y2, y3);
            }
            SEINE_PIPELINED_BLOCK(memory + store_slot(rebuild_index, m_blocks) * 128ULL, )
            y0 = x0;
            y1 = x1;
            y2 = x2;
            y3 = x3;
            x0 = sx0;
            x1 = sx1;
            x2 = sx2;
            x3 = sx3;
        }
        unsigned long long n0, n1, n2, n3;
        SEINE_PIPELINED_BLOCK(
            memory + store_slot(cur, m_blocks) * 128ULL,
            {
                const unsigned int next_ref = (cur + 1U < m_blocks)
                    ? pass0_ref_index(word0, cur + 1U)
                    : 0U;
                load_row_words(memory + ref_word_offset(next_ref), w0, n0, n1, n2, n3);
                rebuild_pending = block_skipped(next_ref, m_blocks);
                rebuild_index = next_ref;
            }
        )
        y0 = n0;
        y1 = n1;
        y2 = n2;
        y3 = n3;
    }
#else
    for (unsigned int cur = di_end; cur < m_blocks; ++cur) {
        if (((cur - di_end) % CANCEL_CHECK_BLOCK_INTERVAL) == 0U) {
            unsigned int cancel_val = 0U;
            if (tid == 0U) cancel_val = cancel_flag[0];
            cancel_val = coop_shfl_u32(cancel_val, 0U);
            if (cancel_val != 0U) {
                return false;
            }
        }
        unsigned long long n0, n1, n2, n3;
        SEINE_PIPELINED_BLOCK(
            memory + static_cast<unsigned long long>(cur) * 128ULL,
            {
                const unsigned int next_ref = (cur + 1U < m_blocks)
                    ? pass0_ref_index(word0, cur + 1U)
                    : 0U;
                load_row_words(memory + static_cast<unsigned long long>(next_ref) * 128ULL, w0, n0, n1, n2, n3);
            }
        )
        y0 = n0;
        y1 = n1;
        y2 = n2;
        y3 = n3;
    }
#endif
    return true;
}
#undef SEINE_PIPELINED_BLOCK
#endif

__device__ __forceinline__ void argon2id_fill_kernel_impl(
    const unsigned long long *__restrict__ seed_blocks,
    unsigned int lanes_active,
    unsigned int active_hashes,
    unsigned int lane_launch_iters,
    unsigned int m_blocks,
    unsigned int t_cost,
    unsigned long long *const *__restrict__ lane_chunks,
    unsigned int lanes_per_chunk,
    unsigned long long *__restrict__ out_last_blocks,
    const volatile unsigned int *__restrict__ cancel_flag,
    unsigned int *__restrict__ completed_iters,
    const unsigned char *__restrict__ target,
    unsigned int *__restrict__ found_index_one_based,
    bool fused_target_check
) {
    const unsigned int warp_id = threadIdx.x / ARGON2_COOP_THREADS;
    const unsigned int lane = blockIdx.x * WARPS_PER_BLOCK + warp_id;
    const unsigned int tid = threadIdx.x % ARGON2_COOP_THREADS;
    const unsigned int effective_m_blocks =
        (SEINE_FIXED_M_BLOCKS > 0U) ? SEINE_FIXED_M_BLOCKS : m_blocks;
    const unsigned int effective_t_cost =
        (SEINE_FIXED_T_COST > 0U) ? SEINE_FIXED_T_COST : t_cost;
    if (lane >= lanes_active || effective_m_blocks < 8U || lanes_per_chunk == 0U) {
        return;
    }

    const unsigned long long lane_stride_words = lane_stride_blocks(effective_m_blocks) * 128ULL;
    unsigned long long *memory =
        lane_base(lane_chunks, lanes_per_chunk, lane, lane_stride_words);
    const unsigned int segment_length = effective_m_blocks / 4U;
    const unsigned int lane_length = effective_m_blocks;

    __shared__ unsigned long long address_block[WARPS_PER_BLOCK * 128];
    __shared__ unsigned long long input_block[WARPS_PER_BLOCK * 128];
    __shared__ unsigned long long zero_block[WARPS_PER_BLOCK * 128];
    __shared__ unsigned long long scratch_r[WARPS_PER_BLOCK * 128];
    __shared__ unsigned long long scratch_q[WARPS_PER_BLOCK * 128];
    unsigned long long *my_address = address_block + warp_id * 128U;
    unsigned long long *my_input = input_block + warp_id * 128U;
    unsigned long long *my_zero = zero_block + warp_id * 128U;
    unsigned long long *my_scratch_r = scratch_r + warp_id * 128U;
    unsigned long long *my_scratch_q = scratch_q + warp_id * 128U;
    bool abort_requested = false;

    for (unsigned int iter = 0U; iter < lane_launch_iters; ++iter) {
        {
            unsigned int cancel_val = 0U;
            if (tid == 0U) cancel_val = cancel_flag[0];
            cancel_val = coop_shfl_u32(cancel_val, 0U);
            if (cancel_val != 0U) break;
        }

        const unsigned long long global_hash_idx =
            static_cast<unsigned long long>(iter) * lanes_active +
            static_cast<unsigned long long>(lane);
        if (global_hash_idx >= static_cast<unsigned long long>(active_hashes)) {
            break;
        }

        const unsigned long long *seed = seed_blocks + global_hash_idx * 256ULL;
        for (unsigned int i = tid; i < 256U; i += ARGON2_COOP_THREADS) {
            memory[i] = seed[i];
        }
        coop_sync();

        for (unsigned int i = tid; i < 128U; i += ARGON2_COOP_THREADS) {
            my_address[i] = 0ULL;
            my_input[i] = 0ULL;
            my_zero[i] = 0ULL;
        }
        coop_sync();

#if SEINE_AMD_PIPELINE_ACTIVE
        if (effective_t_cost == 1U) {
            abort_requested = !fill_single_pass_pipelined(
                memory, effective_m_blocks, my_address, my_input, my_zero,
                my_scratch_r, my_scratch_q, tid, cancel_flag
            );
        } else
#endif
        for (unsigned int pass = 0; pass < effective_t_cost && !abort_requested; ++pass) {
            for (unsigned int slice = 0; slice < 4U && !abort_requested; ++slice) {
                const bool data_independent = (pass == 0U && slice < 2U);
                if (data_independent) {
                    for (unsigned int i = tid; i < 128U; i += ARGON2_COOP_THREADS) {
                        my_address[i] = 0ULL;
                        my_input[i] = 0ULL;
                    }
                    if (tid == 0U) {
                        my_input[0] = static_cast<unsigned long long>(pass);
                        my_input[1] = 0ULL; // lane index (p=1)
                        my_input[2] = static_cast<unsigned long long>(slice);
                        my_input[3] = static_cast<unsigned long long>(effective_m_blocks);
                        my_input[4] = static_cast<unsigned long long>(effective_t_cost);
                        my_input[5] = 2ULL; // Argon2id
                    }
                    coop_sync();

                    // Argon2id slice 0 starts at block index 2. Prime the first address block
                    // so indices 2..127 read initialized address words (matches CPU/reference flow).
                    if (pass == 0U && slice == 0U) {
                        update_address_block_coop(
                            my_address,
                            my_input,
                            my_zero,
                            my_scratch_r,
                            my_scratch_q,
                            tid
                        );
                        coop_sync();
                    }
                }

                const unsigned int first_block =
                    (pass == 0U && slice == 0U) ? 2U : 0U;

                unsigned int cur_index =
                    slice * segment_length + first_block;
                unsigned int prev_index =
                    (slice == 0U && first_block == 0U)
                        ? (cur_index + lane_length - 1U)
                        : (cur_index - 1U);

                // Word 0 of the previous block seeds the data-dependent reference
                // index: loaded once per segment and then carried in a register
                // when SEINE_AMD_CARRY_WORD0, else re-read every block.
                unsigned long long prev_word0 = 0ULL;
#if SEINE_AMD_CARRY_WORD0
                if (!data_independent) {
                    prev_word0 = memory[static_cast<unsigned long long>(prev_index) * 128ULL];
                }
#endif
                // Data-independent prefetch state: pf_valid means pf0..pf3 hold this
                // thread's words of the current block's reference block.
                bool pf_valid = false;
                unsigned long long pf0 = 0ULL;
                unsigned long long pf1 = 0ULL;
                unsigned long long pf2 = 0ULL;
                unsigned long long pf3 = 0ULL;
                for (unsigned int block = first_block; block < segment_length; ++block) {
                    if (((block - first_block) % CANCEL_CHECK_BLOCK_INTERVAL) == 0U) {
                        unsigned int cancel_val = 0U;
                        if (tid == 0U) cancel_val = cancel_flag[0];
                        cancel_val = coop_shfl_u32(cancel_val, 0U);
                        if (cancel_val != 0U) {
                            abort_requested = true;
                            break;
                        }
                    }

                    unsigned long long rand64;
                    if (data_independent) {
                        const unsigned int address_index = block & 127U;
                        if (address_index == 0U) {
                            update_address_block_coop(
                                my_address,
                                my_input,
                                my_zero,
                                my_scratch_r,
                                my_scratch_q,
                                tid
                            );
                        }
                        coop_sync();
                        rand64 = my_address[address_index];
                    } else {
#if SEINE_AMD_CARRY_WORD0
                        rand64 = prev_word0;
#else
                        rand64 = memory[static_cast<unsigned long long>(prev_index) * 128ULL];
#endif
                    }

                    unsigned long long reference_area_size;
                    if (pass == 0U) {
                        if (slice == 0U) {
                            reference_area_size = static_cast<unsigned long long>(block - 1U);
                        } else {
                            reference_area_size =
                                static_cast<unsigned long long>(slice) * segment_length +
                                static_cast<unsigned long long>(block) - 1ULL;
                        }
                    } else {
                        reference_area_size =
                            static_cast<unsigned long long>(lane_length - segment_length) +
                            static_cast<unsigned long long>(block) - 1ULL;
                    }

                    unsigned long long map = rand64 & 0xFFFFFFFFULL;
                    map = (map * map) >> 32;
                    const unsigned long long relative_position =
                        reference_area_size - 1ULL -
                        ((reference_area_size * map) >> 32);

                    const unsigned int start_position =
                        (pass != 0U && slice != 3U) ? ((slice + 1U) * segment_length) : 0U;
                    const unsigned int ref_index =
                        static_cast<unsigned int>(
                            (static_cast<unsigned long long>(start_position) +
                             relative_position) %
                            static_cast<unsigned long long>(lane_length)
                        );

                    const unsigned long long dst_offset =
                        static_cast<unsigned long long>(cur_index) * 128ULL;
                    const unsigned long long rhs_offset =
                        static_cast<unsigned long long>(prev_index) * 128ULL;
                    const unsigned long long lhs_offset =
                        static_cast<unsigned long long>(ref_index) * 128ULL;

                    // Next block's reference block, when it is data independent and its
                    // address word is already in the address block (not the first of
                    // the next 128, which needs a fresh address block).
                    const unsigned long long *next_ref = nullptr;
#if SEINE_AMD_DI_PREFETCH
                    if (data_independent && (block + 1U) < segment_length &&
                        ((block + 1U) & 127U) != 0U) {
                        const unsigned long long rand_next = my_address[(block + 1U) & 127U];
                        const unsigned long long area_next =
                            (slice == 0U)
                                ? static_cast<unsigned long long>(block)
                                : (static_cast<unsigned long long>(slice) * segment_length +
                                   static_cast<unsigned long long>(block));
                        unsigned long long map_next = rand_next & 0xFFFFFFFFULL;
                        map_next = (map_next * map_next) >> 32;
                        const unsigned long long rel_next =
                            area_next - 1ULL - ((area_next * map_next) >> 32);
                        const unsigned int ref_next = static_cast<unsigned int>(
                            rel_next % static_cast<unsigned long long>(lane_length)
                        );
                        next_ref = memory + static_cast<unsigned long long>(ref_next) * 128ULL;
                    }
#endif
                    unsigned long long new_word0;
                    const bool xor_out = (pass != 0U);
                    if (pf_valid) {
                        new_word0 = compress_block_coop_impl(
                            true,
                            memory + rhs_offset,
                            memory + lhs_offset,
                            memory + dst_offset,
                            xor_out,
                            my_scratch_r,
                            my_scratch_q,
                            tid,
                            pf0, pf1, pf2, pf3,
                            next_ref,
                            pf0, pf1, pf2, pf3
                        );
                    } else {
                        new_word0 = compress_block_coop_impl(
                            false,
                            memory + rhs_offset,
                            memory + lhs_offset,
                            memory + dst_offset,
                            xor_out,
                            my_scratch_r,
                            my_scratch_q,
                            tid,
                            0ULL, 0ULL, 0ULL, 0ULL,
                            next_ref,
                            pf0, pf1, pf2, pf3
                        );
                    }
                    pf_valid = (next_ref != nullptr);
                    coop_sync();
#if SEINE_AMD_CARRY_WORD0
                    prev_word0 = new_word0;
#else
                    (void)new_word0;
#endif

                    prev_index = cur_index;
                    cur_index += 1U;
                }
            }
        }
        if (abort_requested) {
            break;
        }

        const unsigned long long *last =
            memory + block_slot(lane_length - 1U, lane_length) * 128ULL;
        unsigned long long *out = out_last_blocks + global_hash_idx * 128ULL;
        {
            const unsigned long long v0 = last[tid];
            const unsigned long long v1 = last[tid + 32U];
            const unsigned long long v2 = last[tid + 64U];
            const unsigned long long v3 = last[tid + 96U];
            out[tid]       = v0;
            out[tid + 32U] = v1;
            out[tid + 64U] = v2;
            out[tid + 96U] = v3;
        }
        coop_sync();
        if (fused_target_check && tid == 0U) {
            unsigned char hash[POW_OUTPUT_BYTES];
            blake2b_long_32_from_words(last, hash);
            if (hash_meets_target_be(hash, target)) {
                atomicMin(
                    found_index_one_based,
                    static_cast<unsigned int>(global_hash_idx) + 1U
                );
            }
        }
        coop_sync();
        if (tid == 0U) {
            atomicMax(completed_iters, iter + 1U);
        }
        coop_sync();
    }
}

__global__ void argon2id_fill_kernel(
    const unsigned long long *__restrict__ seed_blocks,
    unsigned int lanes_active,
    unsigned int active_hashes,
    unsigned int lane_launch_iters,
    unsigned int m_blocks,
    unsigned int t_cost,
    unsigned long long *const *__restrict__ lane_chunks,
    unsigned int lanes_per_chunk,
    unsigned long long *__restrict__ out_last_blocks,
    const volatile unsigned int *__restrict__ cancel_flag,
    unsigned int *__restrict__ completed_iters
) {
    argon2id_fill_kernel_impl(
        seed_blocks,
        lanes_active,
        active_hashes,
        lane_launch_iters,
        m_blocks,
        t_cost,
        lane_chunks,
        lanes_per_chunk,
        out_last_blocks,
        cancel_flag,
        completed_iters,
        nullptr,
        nullptr,
        false
    );
}

__global__ void argon2id_fill_kernel_fused_target(
    const unsigned long long *__restrict__ seed_blocks,
    unsigned int lanes_active,
    unsigned int active_hashes,
    unsigned int lane_launch_iters,
    unsigned int m_blocks,
    unsigned int t_cost,
    unsigned long long *const *__restrict__ lane_chunks,
    unsigned int lanes_per_chunk,
    unsigned long long *__restrict__ out_last_blocks,
    const volatile unsigned int *__restrict__ cancel_flag,
    unsigned int *__restrict__ completed_iters,
    const unsigned char *__restrict__ target,
    unsigned int *__restrict__ found_index_one_based
) {
    argon2id_fill_kernel_impl(
        seed_blocks,
        lanes_active,
        active_hashes,
        lane_launch_iters,
        m_blocks,
        t_cost,
        lane_chunks,
        lanes_per_chunk,
        out_last_blocks,
        cancel_flag,
        completed_iters,
        target,
        found_index_one_based,
        true
    );
}

__global__ void evaluate_hashes_kernel(
    const unsigned long long *__restrict__ last_blocks,
    unsigned int active_hashes,
    const unsigned char *__restrict__ target,
    unsigned int *__restrict__ found_index_one_based
) {
    const unsigned int hash_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (hash_idx >= active_hashes) {
        return;
    }

    const unsigned long long *last =
        last_blocks + static_cast<unsigned long long>(hash_idx) * 128ULL;
    unsigned char hash[POW_OUTPUT_BYTES];
    blake2b_long_32_from_words(last, hash);
    if (hash_meets_target_be(hash, target)) {
        atomicMin(found_index_one_based, hash_idx + 1U);
    }
}

} // extern "C"
