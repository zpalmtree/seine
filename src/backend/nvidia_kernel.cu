// SPDX-License-Identifier: BSD-3-Clause
// CUDA Argon2id lane-fill kernel specialized for Seine's PoW profile.

extern "C" {

constexpr unsigned int ARGON2_COOP_THREADS = 32U;
constexpr unsigned int ARGON2_COOP_WARP_MASK = 0xFFFFFFFFU;
constexpr unsigned int SEED_KERNEL_THREADS = 64U;
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
static_assert(
    (CANCEL_CHECK_BLOCK_INTERVAL & (CANCEL_CHECK_BLOCK_INTERVAL - 1U)) == 0U,
    "the cancel check interval must be a power of two"
);
#ifndef SEINE_WARPS_PER_BLOCK
#define SEINE_WARPS_PER_BLOCK 1U
#endif
constexpr unsigned int WARPS_PER_BLOCK = SEINE_WARPS_PER_BLOCK;
// SEINE_FILL_V1=1 selects the original fill loop.
#ifndef SEINE_FILL_V1
#define SEINE_FILL_V1 0
#endif
// Reference blocks loaded ahead in the data-independent slices.
#ifndef SEINE_REF_PREFETCH_DEPTH
#define SEINE_REF_PREFETCH_DEPTH 1U
#endif
constexpr unsigned int REF_PREFETCH_DEPTH = SEINE_REF_PREFETCH_DEPTH;

__device__ __forceinline__ void coop_sync() {
    __syncwarp(ARGON2_COOP_WARP_MASK);
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

__device__ __forceinline__ unsigned long long rotr64(
    unsigned long long x,
    unsigned int n
) {
    return (x >> n) | (x << (64U - n));
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

__device__ __noinline__ void blake2b_compress(
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

#pragma unroll 1
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

__device__ __forceinline__ void blake2b_long_32_from_block(
    const unsigned char *block_bytes,
    unsigned char out_hash[POW_OUTPUT_BYTES]
) {
    unsigned char out_len_le[4];
    store_u32_le(out_len_le, POW_OUTPUT_BYTES);

    Blake2bState state;
    blake2b_init(state, POW_OUTPUT_BYTES);
    blake2b_update(state, out_len_le, 4U);
    blake2b_update(state, block_bytes, 1024U);
    blake2b_final(state, out_hash, POW_OUTPUT_BYTES);
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

__device__ __forceinline__ unsigned long long blamka(
    unsigned long long x,
    unsigned long long y
) {
    const unsigned long long low = 0xFFFFFFFFULL;
    return x + y + 2ULL * (x & low) * (y & low);
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

__device__ __forceinline__ void permute_round(
    unsigned long long &v0,
    unsigned long long &v1,
    unsigned long long &v2,
    unsigned long long &v3,
    unsigned long long &v4,
    unsigned long long &v5,
    unsigned long long &v6,
    unsigned long long &v7,
    unsigned long long &v8,
    unsigned long long &v9,
    unsigned long long &v10,
    unsigned long long &v11,
    unsigned long long &v12,
    unsigned long long &v13,
    unsigned long long &v14,
    unsigned long long &v15
) {
    permute_step(v0, v4, v8, v12);
    permute_step(v1, v5, v9, v13);
    permute_step(v2, v6, v10, v14);
    permute_step(v3, v7, v11, v15);
    permute_step(v0, v5, v10, v15);
    permute_step(v1, v6, v11, v12);
    permute_step(v2, v7, v8, v13);
    permute_step(v3, v4, v9, v14);
}

__device__ __forceinline__ void compress_block_coop(
    const unsigned long long *__restrict__ rhs,
    const unsigned long long *__restrict__ lhs,
    unsigned long long *__restrict__ out,
    bool xor_out,
    unsigned long long *__restrict__ scratch_r,
    unsigned long long *__restrict__ scratch_q,
    unsigned int tid
) {
    // Issue all global loads into registers first.
    const unsigned long long r0 = rhs[tid]       ^ lhs[tid];
    const unsigned long long r1 = rhs[tid + 32U]  ^ lhs[tid + 32U];
    const unsigned long long r2 = rhs[tid + 64U]  ^ lhs[tid + 64U];
    const unsigned long long r3 = rhs[tid + 96U]  ^ lhs[tid + 96U];
    scratch_r[tid]       = r0;  scratch_q[tid]       = r0;
    scratch_r[tid + 32U] = r1;  scratch_q[tid + 32U] = r1;
    scratch_r[tid + 64U] = r2;  scratch_q[tid + 64U] = r2;
    scratch_r[tid + 96U] = r3;  scratch_q[tid + 96U] = r3;
    coop_sync();

    // G-rounds with warp-shuffle fusion.
    // Fuse row-columns → row-diagonals and col-columns → col-diagonals
    // by passing values between lanes via __shfl_sync.
    const unsigned int state = tid >> 2;
    const unsigned int lane = tid & 3U;
    const unsigned int shfl_base = state * 4U; // absolute lane base for shuffles

    // --- Fused row rounds (columns + diagonals) ---
    {
        const unsigned int base = state * 16U;
        // Row columns: load from shared, permute
        unsigned long long a = scratch_q[base + lane];
        unsigned long long b = scratch_q[base + 4U + lane];
        unsigned long long c = scratch_q[base + 8U + lane];
        unsigned long long d = scratch_q[base + 12U + lane];
        permute_step(a, b, c, d);
        // Row diagonals: shuffle outputs instead of shared-memory round-trip.
        // Thread lane l needs: a from self, b from (l+1)&3, c from (l+2)&3, d from (l+3)&3
        unsigned long long a2 = a; // same thread, same position
        unsigned long long b2 = __shfl_sync(ARGON2_COOP_WARP_MASK, b, shfl_base + ((lane + 1U) & 3U));
        unsigned long long c2 = __shfl_sync(ARGON2_COOP_WARP_MASK, c, shfl_base + ((lane + 2U) & 3U));
        unsigned long long d2 = __shfl_sync(ARGON2_COOP_WARP_MASK, d, shfl_base + ((lane + 3U) & 3U));
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
    {
        const unsigned int b = state * 2U;
        const unsigned int col_sub = (lane >> 1) * 16U + (lane & 1U);
        // Column columns: load from shared, permute
        unsigned long long a = scratch_q[b + col_sub];
        unsigned long long c = scratch_q[b + 32U + col_sub];
        unsigned long long d = scratch_q[b + 64U + col_sub];
        unsigned long long e = scratch_q[b + 96U + col_sub];
        permute_step(a, c, d, e);
        // Column diagonals: shuffle instead of shared-memory round-trip.
        unsigned long long a2 = a;
        unsigned long long c2 = __shfl_sync(ARGON2_COOP_WARP_MASK, c, shfl_base + ((lane + 1U) & 3U));
        unsigned long long d2 = __shfl_sync(ARGON2_COOP_WARP_MASK, d, shfl_base + ((lane + 2U) & 3U));
        unsigned long long e2 = __shfl_sync(ARGON2_COOP_WARP_MASK, e, shfl_base + ((lane + 3U) & 3U));
        permute_step(a2, c2, d2, e2);
        // Store diagonal results back.
        const unsigned int p0 = lane;
        const unsigned int p1 = (lane + 1U) & 3U;
        const unsigned int p2 = (lane + 2U) & 3U;
        const unsigned int p3 = (lane + 3U) & 3U;
        scratch_q[b + (p0 >> 1) * 16U + (p0 & 1U)] = a2;
        scratch_q[b + 32U + (p1 >> 1) * 16U + (p1 & 1U)] = c2;
        scratch_q[b + 64U + (p2 >> 1) * 16U + (p2 & 1U)] = d2;
        scratch_q[b + 96U + (p3 >> 1) * 16U + (p3 & 1U)] = e2;
    }
    coop_sync();

    // Unrolled final store: read all shared-memory values into registers
    // first, then issue all global stores.
    {
        const unsigned long long m0 = scratch_q[tid]       ^ scratch_r[tid];
        const unsigned long long m1 = scratch_q[tid + 32U] ^ scratch_r[tid + 32U];
        const unsigned long long m2 = scratch_q[tid + 64U] ^ scratch_r[tid + 64U];
        const unsigned long long m3 = scratch_q[tid + 96U] ^ scratch_r[tid + 96U];
        if (xor_out) {
            out[tid]       ^= m0;
            out[tid + 32U] ^= m1;
            out[tid + 64U] ^= m2;
            out[tid + 96U] ^= m3;
        } else {
            out[tid]       = m0;
            out[tid + 32U] = m1;
            out[tid + 64U] = m2;
            out[tid + 96U] = m3;
        }
    }
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

// Pass-0 reference index for block `index`: the reference area is every block
// before the previous one (t_cost = 1, one lane).
__device__ __forceinline__ unsigned int pass0_ref_index(
    unsigned int index,
    unsigned long long rand64
) {
    const unsigned long long area = static_cast<unsigned long long>(index - 1U);
    unsigned long long map = rand64 & 0xFFFFFFFFULL;
    map = (map * map) >> 32;
    return static_cast<unsigned int>(area - 1ULL - ((area * map) >> 32));
}

// Reference indices of the data-independent blocks (pass 0, slices 0 and 1).
// They do not depend on the password, so one table serves every lane and hash.
// One warp per address block; entries 0 and 1 are unused.
__global__ void build_ref_table_kernel(
    unsigned int *__restrict__ ref_table,
    unsigned int m_blocks,
    unsigned int t_cost
) {
    const unsigned int tid = threadIdx.x;
    const unsigned int segment_length = m_blocks / 4U;
    const unsigned int address_blocks_per_slice = segment_length / 128U;
    const unsigned int slice = blockIdx.x / address_blocks_per_slice;
    const unsigned int counter = blockIdx.x % address_blocks_per_slice + 1U;
    if (slice >= 2U) {
        return;
    }

    __shared__ unsigned long long input_block[128];
    __shared__ unsigned long long address_block[128];
    __shared__ unsigned long long zero_block[128];
    __shared__ unsigned long long scratch_r[128];
    __shared__ unsigned long long scratch_q[128];
    for (unsigned int i = tid; i < 128U; i += ARGON2_COOP_THREADS) {
        input_block[i] = 0ULL;
        zero_block[i] = 0ULL;
    }
    coop_sync();
    if (tid == 0U) {
        input_block[2] = static_cast<unsigned long long>(slice);
        input_block[3] = static_cast<unsigned long long>(m_blocks);
        input_block[4] = static_cast<unsigned long long>(t_cost);
        input_block[5] = 2ULL; // Argon2id
        input_block[6] = static_cast<unsigned long long>(counter);
    }
    coop_sync();
    compress_block_coop(zero_block, input_block, address_block, false, scratch_r, scratch_q, tid);
    coop_sync();
    compress_block_coop(zero_block, address_block, address_block, false, scratch_r, scratch_q, tid);
    coop_sync();
    for (unsigned int w = tid; w < 128U; w += ARGON2_COOP_THREADS) {
        const unsigned int index = slice * segment_length + (counter - 1U) * 128U + w;
        if (index >= 2U) {
            ref_table[index] = pass0_ref_index(index, address_block[w]);
        }
    }
}

// Shared-memory slot of block word w (an XOR permutation within each 64 words).
#define SQ(w) ((w) ^ ((((w) >> 4) & 1U) << 3) ^ ((((w) >> 5) & 1U) << 2))

#ifndef SEINE_ROW_LAYOUT
#define SEINE_ROW_LAYOUT 0
#endif

#if SEINE_ROW_LAYOUT
// Word of a block that thread `tid` holds as its k-th register value: thread
// (s, l) = (tid / 4, tid % 4) holds column l of row s, words 16s + l + 4k.
__device__ __forceinline__ unsigned int held_word(unsigned int tid, unsigned int k) {
    return (tid >> 2) * 16U + (tid & 3U) + 4U * k;
}
// Distance in words between a thread's consecutive held words.
constexpr unsigned int HELD_STRIDE = 4U;

// Argon2 compression of a block held in registers (see held_word), through the
// column round. Returns output word 0 (valid in thread 0); compress_finish
// produces the whole block.
__device__ __forceinline__ unsigned long long compress_begin(
    const unsigned long long (&x)[4],
    unsigned long long *__restrict__ scratch_q,
    unsigned int tid
) {
    unsigned long long word0;
    const unsigned int state = tid >> 2;
    const unsigned int lane = tid & 3U;
    const unsigned int shfl_base = state * 4U;
    {
        const unsigned int base = state * 16U;
        unsigned long long a = x[0];
        unsigned long long b = x[1];
        unsigned long long c = x[2];
        unsigned long long d = x[3];
        permute_step(a, b, c, d);
        unsigned long long a2 = a;
        unsigned long long b2 = __shfl_sync(ARGON2_COOP_WARP_MASK, b, shfl_base + ((lane + 1U) & 3U));
        unsigned long long c2 = __shfl_sync(ARGON2_COOP_WARP_MASK, c, shfl_base + ((lane + 2U) & 3U));
        unsigned long long d2 = __shfl_sync(ARGON2_COOP_WARP_MASK, d, shfl_base + ((lane + 3U) & 3U));
        permute_step(a2, b2, c2, d2);
        scratch_q[SQ(base + lane)] = a2;
        scratch_q[SQ(base + 4U + ((lane + 1U) & 3U))] = b2;
        scratch_q[SQ(base + 8U + ((lane + 2U) & 3U))] = c2;
        scratch_q[SQ(base + 12U + ((lane + 3U) & 3U))] = d2;
    }
    coop_sync();
    {
        const unsigned int b = state * 2U;
        const unsigned int col_sub = (lane >> 1) * 16U + (lane & 1U);
        unsigned long long a = scratch_q[SQ(b + col_sub)];
        unsigned long long c = scratch_q[SQ(b + 32U + col_sub)];
        unsigned long long d = scratch_q[SQ(b + 64U + col_sub)];
        unsigned long long e = scratch_q[SQ(b + 96U + col_sub)];
        permute_step(a, c, d, e);
        unsigned long long a2 = a;
        unsigned long long c2 = __shfl_sync(ARGON2_COOP_WARP_MASK, c, shfl_base + ((lane + 1U) & 3U));
        unsigned long long d2 = __shfl_sync(ARGON2_COOP_WARP_MASK, d, shfl_base + ((lane + 2U) & 3U));
        unsigned long long e2 = __shfl_sync(ARGON2_COOP_WARP_MASK, e, shfl_base + ((lane + 3U) & 3U));
        permute_step(a2, c2, d2, e2);
        const unsigned int p0 = lane;
        const unsigned int p1 = (lane + 1U) & 3U;
        const unsigned int p2 = (lane + 2U) & 3U;
        const unsigned int p3 = (lane + 3U) & 3U;
        // The shuffles above follow every thread's column reads, so these slots
        // are free to overwrite.
        scratch_q[SQ(b + (p0 >> 1) * 16U + (p0 & 1U))] = a2;
        word0 = a2 ^ x[0];
        scratch_q[SQ(b + 32U + (p1 >> 1) * 16U + (p1 & 1U))] = c2;
        scratch_q[SQ(b + 64U + (p2 >> 1) * 16U + (p2 & 1U))] = d2;
        scratch_q[SQ(b + 96U + (p3 >> 1) * 16U + (p3 & 1U))] = e2;
    }
    return word0;
}
#else
// Word of a block that thread `tid` holds as its k-th register value.
__device__ __forceinline__ unsigned int held_word(unsigned int tid, unsigned int k) {
    return tid + 32U * k;
}
// Distance in words between a thread's consecutive held words.
constexpr unsigned int HELD_STRIDE = 32U;

// Argon2 compression of a block held in registers (see held_word), through the
// column round. Returns output word 0 (valid in thread 0); compress_finish
// produces the whole block.
__device__ __forceinline__ unsigned long long compress_begin(
    const unsigned long long (&x)[4],
    unsigned long long *__restrict__ scratch_q,
    unsigned int tid
) {
    unsigned long long word0;
    scratch_q[SQ(tid)]       = x[0];
    scratch_q[SQ(tid + 32U)] = x[1];
    scratch_q[SQ(tid + 64U)] = x[2];
    scratch_q[SQ(tid + 96U)] = x[3];
    coop_sync();

    const unsigned int state = tid >> 2;
    const unsigned int lane = tid & 3U;
    const unsigned int shfl_base = state * 4U;
    {
        const unsigned int base = state * 16U;
        unsigned long long a = scratch_q[SQ(base + lane)];
        unsigned long long b = scratch_q[SQ(base + 4U + lane)];
        unsigned long long c = scratch_q[SQ(base + 8U + lane)];
        unsigned long long d = scratch_q[SQ(base + 12U + lane)];
        permute_step(a, b, c, d);
        unsigned long long a2 = a;
        unsigned long long b2 = __shfl_sync(ARGON2_COOP_WARP_MASK, b, shfl_base + ((lane + 1U) & 3U));
        unsigned long long c2 = __shfl_sync(ARGON2_COOP_WARP_MASK, c, shfl_base + ((lane + 2U) & 3U));
        unsigned long long d2 = __shfl_sync(ARGON2_COOP_WARP_MASK, d, shfl_base + ((lane + 3U) & 3U));
        permute_step(a2, b2, c2, d2);
        scratch_q[SQ(base + lane)] = a2;
        scratch_q[SQ(base + 4U + ((lane + 1U) & 3U))] = b2;
        scratch_q[SQ(base + 8U + ((lane + 2U) & 3U))] = c2;
        scratch_q[SQ(base + 12U + ((lane + 3U) & 3U))] = d2;
    }
    coop_sync();
    {
        const unsigned int b = state * 2U;
        const unsigned int col_sub = (lane >> 1) * 16U + (lane & 1U);
        unsigned long long a = scratch_q[SQ(b + col_sub)];
        unsigned long long c = scratch_q[SQ(b + 32U + col_sub)];
        unsigned long long d = scratch_q[SQ(b + 64U + col_sub)];
        unsigned long long e = scratch_q[SQ(b + 96U + col_sub)];
        permute_step(a, c, d, e);
        unsigned long long a2 = a;
        unsigned long long c2 = __shfl_sync(ARGON2_COOP_WARP_MASK, c, shfl_base + ((lane + 1U) & 3U));
        unsigned long long d2 = __shfl_sync(ARGON2_COOP_WARP_MASK, d, shfl_base + ((lane + 2U) & 3U));
        unsigned long long e2 = __shfl_sync(ARGON2_COOP_WARP_MASK, e, shfl_base + ((lane + 3U) & 3U));
        permute_step(a2, c2, d2, e2);
        const unsigned int p0 = lane;
        const unsigned int p1 = (lane + 1U) & 3U;
        const unsigned int p2 = (lane + 2U) & 3U;
        const unsigned int p3 = (lane + 3U) & 3U;
        scratch_q[SQ(b + (p0 >> 1) * 16U + (p0 & 1U))] = a2;
        word0 = a2 ^ x[0];
        scratch_q[SQ(b + 32U + (p1 >> 1) * 16U + (p1 & 1U))] = c2;
        scratch_q[SQ(b + 64U + (p2 >> 1) * 16U + (p2 & 1U))] = d2;
        scratch_q[SQ(b + 96U + (p3 >> 1) * 16U + (p3 & 1U))] = e2;
    }
    return word0;
}

#endif

// Completes compress_begin: z = P(x) ^ x.
__device__ __forceinline__ void compress_finish(
    const unsigned long long (&x)[4],
    unsigned long long (&z)[4],
    unsigned long long *__restrict__ scratch_q,
    unsigned int tid
) {
    coop_sync();
#pragma unroll
    for (unsigned int k = 0; k < 4U; ++k) {
        z[k] = scratch_q[SQ(held_word(tid, k))] ^ x[k];
    }
}

__device__ __forceinline__ void compress_regs(
    const unsigned long long (&x)[4],
    unsigned long long (&z)[4],
    unsigned long long *__restrict__ scratch_q,
    unsigned int tid
) {
    compress_begin(x, scratch_q, tid);
    compress_finish(x, z, scratch_q, tid);
}

// Address a block's held words from one base pointer (constant offsets)
// instead of one index per word: SEINE_BLOCK_BASE_POINTER 1 everywhere, 2 in
// the data-dependent half only.
#ifndef SEINE_BLOCK_BASE_POINTER
#define SEINE_BLOCK_BASE_POINTER 0
#endif

__device__ __forceinline__ void load_block_words(
    const unsigned long long *__restrict__ block,
    unsigned long long (&v)[4],
    unsigned int tid,
    bool base_pointer
) {
    if (base_pointer) {
        const unsigned long long *__restrict__ p = block + held_word(tid, 0U);
#pragma unroll
        for (unsigned int k = 0; k < 4U; ++k) {
            v[k] = p[k * HELD_STRIDE];
        }
    } else {
#pragma unroll
        for (unsigned int k = 0; k < 4U; ++k) {
            v[k] = block[held_word(tid, k)];
        }
    }
}

__device__ __forceinline__ void store_block_words(
    unsigned long long *__restrict__ block,
    const unsigned long long (&v)[4],
    unsigned int tid,
    bool base_pointer
) {
    if (base_pointer) {
        unsigned long long *__restrict__ p = block + held_word(tid, 0U);
#pragma unroll
        for (unsigned int k = 0; k < 4U; ++k) {
            p[k * HELD_STRIDE] = v[k];
        }
    } else {
#pragma unroll
        for (unsigned int k = 0; k < 4U; ++k) {
            block[held_word(tid, k)] = v[k];
        }
    }
}

__device__ __forceinline__ void load_block_regs(
    const unsigned long long *__restrict__ block,
    unsigned long long (&v)[4],
    unsigned int tid
) {
    load_block_words(block, v, tid, SEINE_BLOCK_BASE_POINTER == 1);
}

__device__ __forceinline__ void store_block_regs(
    unsigned long long *__restrict__ block,
    const unsigned long long (&v)[4],
    unsigned int tid
) {
    store_block_words(block, v, tid, SEINE_BLOCK_BASE_POINTER == 1);
}

// The same for the data-dependent half.
__device__ __forceinline__ void load_block_regs_dd(
    const unsigned long long *__restrict__ block,
    unsigned long long (&v)[4],
    unsigned int tid
) {
    load_block_words(block, v, tid, SEINE_BLOCK_BASE_POINTER != 0);
}

__device__ __forceinline__ void store_block_regs_dd(
    unsigned long long *__restrict__ block,
    const unsigned long long (&v)[4],
    unsigned int tid
) {
    store_block_words(block, v, tid, SEINE_BLOCK_BASE_POINTER != 0);
}

__device__ __forceinline__ bool cancel_requested(
    const volatile unsigned int *__restrict__ cancel_flag,
    unsigned int tid
) {
    (void)tid;
    return __any_sync(ARGON2_COOP_WARP_MASK, cancel_flag[0] != 0U);
}

// Ampere and newer copy data-independent reference blocks into a shared-memory
// ring several blocks ahead; older GPUs load one block ahead into registers.
#ifndef SEINE_REF_RING
#define SEINE_REF_RING 4U
#endif
constexpr unsigned int REF_RING = SEINE_REF_RING;
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 800 && SEINE_REF_RING > 1
#define SEINE_ASYNC_REFS 1
#else
#define SEINE_ASYNC_REFS 0
#endif

#ifndef SEINE_RING_FENCE
#define SEINE_RING_FENCE 1
#endif
constexpr unsigned int RING_FENCE_LAG = SEINE_RING_FENCE ? 1U : 0U;

#if SEINE_ASYNC_REFS
// Each thread copies the words it reads (and stored) itself, so a copy needs no
// warp barrier before or after.
// When `copy` is false the slot is zero-filled without reading `block`.
__device__ __forceinline__ void copy_block_async(
    unsigned long long *__restrict__ slot,
    const unsigned long long *__restrict__ block,
    bool copy,
    unsigned int tid
) {
    const unsigned int bytes = copy ? 8U : 0U;
    const unsigned int dst =
        static_cast<unsigned int>(__cvta_generic_to_shared(slot + held_word(tid, 0U)));
    const unsigned long long *src = block + held_word(tid, 0U);
#pragma unroll
    for (unsigned int k = 0; k < 4U; ++k) {
        asm volatile(
            "cp.async.ca.shared.global [%0], [%1], 8, %2;\n"
            :: "r"(dst + k * HELD_STRIDE * 8U), "l"(src + k * HELD_STRIDE), "r"(bytes)
            : "memory"
        );
    }
}

__device__ __forceinline__ void commit_copies() {
    asm volatile("cp.async.commit_group;\n" ::: "memory");
}

__device__ __forceinline__ void wait_oldest_copy() {
    asm volatile("cp.async.wait_group %0;\n" :: "n"(REF_RING - 1U) : "memory");
}
#endif

// Odd blocks at or after `skip_from` (even) are not stored; the even blocks
// there are packed into consecutive slots.
__device__ __forceinline__ bool block_skipped(unsigned int index, unsigned int skip_from) {
    return index >= skip_from && (index & 1U) != 0U;
}

__device__ __forceinline__ unsigned long long block_slot(unsigned int index, unsigned int skip_from) {
    const unsigned int slot = index < skip_from ? index : skip_from + ((index - skip_from) >> 1);
    return static_cast<unsigned long long>(slot) * 128ULL;
}

// Each further level needs another unstored reference in a row, so even at a
// quarter of a lane unstored a deeper chain practically never occurs.
constexpr unsigned int MAX_REBUILD_DEPTH = 64U;

#ifndef SEINE_LOOP_UNROLL
#define SEINE_LOOP_UNROLL 1
#endif
// Blocks per iteration of the main fill loops.
constexpr int LOOP_UNROLL = SEINE_LOOP_UNROLL;

// Rebuilds unstored block `index` into `out`. Its previous block is always
// stored (and already in `index_prev` when `have_prev`); a reference to another
// unstored block is rebuilt first. Returns false if the chain is deeper than
// MAX_REBUILD_DEPTH.
#ifndef SEINE_INLINE_REBUILD
#define SEINE_INLINE_REBUILD 0
#endif
#if SEINE_INLINE_REBUILD
__device__ __forceinline__ bool rebuild_block(
#else
__device__ __noinline__ bool rebuild_block(
#endif
    const unsigned long long *__restrict__ memory,
    unsigned int index,
    unsigned int skip_from,
    unsigned long long *__restrict__ scratch_q,
    const unsigned long long (&index_prev)[4],
    bool have_prev,
    unsigned long long (&out)[4],
    unsigned int tid
) {
    unsigned int chain[MAX_REBUILD_DEPTH];
    unsigned int depth = 0U;
    unsigned int current = index;
    unsigned long long prev_block[4];
    unsigned int ref;
#pragma unroll 1
    while (true) {
        chain[depth++] = current;
        if (current == index && have_prev) {
#pragma unroll
            for (unsigned int k = 0; k < 4U; ++k) {
                prev_block[k] = index_prev[k];
            }
        } else {
            load_block_regs_dd(memory + block_slot(current - 1U, skip_from), prev_block, tid);
        }
        ref = pass0_ref_index(
            current,
            __shfl_sync(ARGON2_COOP_WARP_MASK, prev_block[0], 0)
        );
        // Voted, so the whole warp leaves the loop together.
        if (__all_sync(ARGON2_COOP_WARP_MASK, !block_skipped(ref, skip_from))) {
            break;
        }
        if (depth == MAX_REBUILD_DEPTH) {
            return false;
        }
        current = ref;
    }
    load_block_regs_dd(memory + block_slot(ref, skip_from), out, tid);
#pragma unroll 1
    for (unsigned int level = depth; level-- > 0U;) {
        if (level + 1U != depth) {
            if (level == 0U && have_prev) {
#pragma unroll
                for (unsigned int k = 0; k < 4U; ++k) {
                    prev_block[k] = index_prev[k];
                }
            } else {
                load_block_regs_dd(memory + block_slot(chain[level] - 1U, skip_from), prev_block, tid);
            }
        }
        unsigned long long x[4];
#pragma unroll
        for (unsigned int k = 0; k < 4U; ++k) {
            x[k] = prev_block[k] ^ out[k];
        }
        compress_regs(x, out, scratch_q, tid);
    }
    return true;
}

// Diagnostics: with SEINE_PHASE_TIMING the fill kernel prints block 0's cycles
// per block for each half of its last hash.
#ifdef SEINE_PHASE_TIMING
__device__ long long seine_phase_cycles[2];
#endif

// One lane's pass-0 fill (t_cost = 1) with the previous block kept in
// registers. Data-independent references come from `ref_table` and are loaded
// ahead (REF_RING blocks with async copies, else REF_PREFETCH_DEPTH); a
// reference not yet written when its load would be issued is loaded when it is
// used. Returns false if cancelled. On success `prev` holds the last block.
__device__ __forceinline__ bool fill_lane_pass0(
    unsigned long long *__restrict__ memory,
    const unsigned long long *__restrict__ seed,
    const unsigned int *__restrict__ ref_table,
    unsigned int lane_length,
    unsigned int skip_from,
    const volatile unsigned int *__restrict__ cancel_flag,
    unsigned long long *__restrict__ scratch_q,
    unsigned long long *__restrict__ ref_ring,
    unsigned long long (&prev)[4],
    unsigned int tid
) {
    constexpr unsigned int D = REF_PREFETCH_DEPTH;
    unsigned long long block0[4];
    load_block_regs(seed, block0, tid);
    load_block_regs(seed + 128ULL, prev, tid);
    store_block_regs(memory, block0, tid);
    store_block_regs(memory + 128ULL, prev, tid);

    const unsigned int half = lane_length / 2U;
#ifdef SEINE_PHASE_TIMING
    const long long phase_t0 = clock64();
#endif
#if SEINE_ASYNC_REFS
    (void)D;
#if SEINE_RING_FENCE
    // Async copies below read blocks this thread just stored; order the stores
    // first.
    __threadfence_block();
#endif
    // Bit s is set when ring slot s must be loaded at use instead.
    unsigned int late = 0U;
#pragma unroll 1
    for (unsigned int index = 2U; index + 1U < 2U + REF_RING; ++index) {
        const unsigned int ref = ref_table[index];
        const bool ready = ref < 2U;
        copy_block_async(
            ref_ring + (index % REF_RING) * 128U,
            memory + (ready ? static_cast<unsigned long long>(ref) * 128ULL : 0ULL),
            ready,
            tid
        );
        late |= ready ? 0U : (1U << (index % REF_RING));
        commit_copies();
    }
    unsigned int next_ref = ref_table[2U + REF_RING - 1U];
    for (unsigned int chunk = 2U; chunk < half; chunk += CANCEL_CHECK_BLOCK_INTERVAL) {
    if (cancel_requested(cancel_flag, tid)) {
        return false;
    }
    const unsigned int chunk_end = min(chunk + CANCEL_CHECK_BLOCK_INTERVAL, half);
#pragma unroll (LOOP_UNROLL)
    for (unsigned int index = chunk; index < chunk_end; ++index) {
        // Copy the reference of block index + REF_RING - 1 if it is already
        // written; otherwise that block loads it when it runs.
        const unsigned int ahead = index + REF_RING - 1U;
        const unsigned int ahead_bit = 1U << (ahead % REF_RING);
        // Blocks up to index - 2 are ordered before this copy by the fence in
        // the previous iteration; a newer reference is loaded at use.
        const bool ready = ahead < half && next_ref + RING_FENCE_LAG < index;
        copy_block_async(
            ref_ring + (ahead % REF_RING) * 128U,
            memory + (ready ? static_cast<unsigned long long>(next_ref) * 128ULL : 0ULL),
            ready,
            tid
        );
        commit_copies();
        late = ready ? (late & ~ahead_bit) : (late | ahead_bit);
        next_ref = ref_table[min(ahead + 1U, half - 1U)];
        wait_oldest_copy();
        const unsigned long long *ref_block = ref_ring + (index % REF_RING) * 128U;
        unsigned long long x[4];
#pragma unroll
        for (unsigned int k = 0; k < 4U; ++k) {
            x[k] = ref_block[held_word(tid, k)];
        }
        if ((late & (1U << (index % REF_RING))) != 0U) {
            load_block_regs(
                memory + static_cast<unsigned long long>(ref_table[index]) * 128ULL,
                x,
                tid
            );
        }
#pragma unroll
        for (unsigned int k = 0; k < 4U; ++k) {
            x[k] ^= prev[k];
        }
        compress_begin(x, scratch_q, tid);
#if SEINE_RING_FENCE
        // Orders the previous block's store before the next iteration's copy.
        __threadfence_block();
#endif
        compress_finish(x, prev, scratch_q, tid);
        store_block_regs(memory + static_cast<unsigned long long>(index) * 128ULL, prev, tid);
    }
    }
    asm volatile("cp.async.wait_all;\n" ::: "memory");
#else
    unsigned long long ring[D][4];
    unsigned int next_ref[D];
    unsigned int ready = 0U;
#pragma unroll
    for (unsigned int u = 0; u < D; ++u) {
        const unsigned int index = 2U + u;
        const unsigned int ref = ref_table[index];
        if (ref < 2U) {
            load_block_regs(memory + static_cast<unsigned long long>(ref) * 128ULL, ring[u], tid);
            ready |= 1U << u;
        }
        next_ref[u] = ref_table[index + D];
    }

    static_assert(CANCEL_CHECK_BLOCK_INTERVAL % D == 0U, "chunks must hold whole prefetch rounds");
    for (unsigned int chunk = 2U; chunk < half; chunk += CANCEL_CHECK_BLOCK_INTERVAL) {
    if (cancel_requested(cancel_flag, tid)) {
        return false;
    }
    const unsigned int chunk_end = min(chunk + CANCEL_CHECK_BLOCK_INTERVAL, half);
#pragma unroll (LOOP_UNROLL)
    for (unsigned int base = chunk; base < chunk_end; base += D) {
#pragma unroll
        for (unsigned int u = 0; u < D; ++u) {
            const unsigned int index = base + u;
            if (index >= chunk_end) {
                break;
            }
            if ((ready & (1U << u)) == 0U) {
                const unsigned int ref = ref_table[index];
                load_block_regs(memory + static_cast<unsigned long long>(ref) * 128ULL, ring[u], tid);
            }
            unsigned long long x[4];
#pragma unroll
            for (unsigned int k = 0; k < 4U; ++k) {
                x[k] = prev[k] ^ ring[u][k];
            }
            // Load the reference of block index + D while this block compresses.
            const unsigned int ahead = index + D;
            if (ahead < half && next_ref[u] < index) {
                load_block_regs(memory + static_cast<unsigned long long>(next_ref[u]) * 128ULL, ring[u], tid);
                ready |= 1U << u;
            } else {
                ready &= ~(1U << u);
            }
            if (ahead + D < half) {
                next_ref[u] = ref_table[ahead + D];
            }
            compress_regs(x, prev, scratch_q, tid);
            store_block_regs(memory + static_cast<unsigned long long>(index) * 128ULL, prev, tid);
        }
    }
    }
#endif

#ifdef SEINE_PHASE_TIMING
    const long long phase_t1 = clock64();
#endif
    // Data-dependent blocks before `skip_from` (and before the last block):
    // every block they can reference is stored. Each block's reference is
    // loaded as soon as the previous block's first word is known.
    const unsigned int dense_end = min(skip_from, lane_length - 1U);
    unsigned long long next[4];
    load_block_regs_dd(
        memory + static_cast<unsigned long long>(pass0_ref_index(
                     half, __shfl_sync(ARGON2_COOP_WARP_MASK, prev[0], 0))) * 128ULL,
        next,
        tid
    );
    for (unsigned int chunk = half; chunk < dense_end; chunk += CANCEL_CHECK_BLOCK_INTERVAL) {
    if (cancel_requested(cancel_flag, tid)) {
        return false;
    }
    const unsigned int chunk_end = min(chunk + CANCEL_CHECK_BLOCK_INTERVAL, dense_end);
#pragma unroll (LOOP_UNROLL)
    for (unsigned int index = chunk; index < chunk_end; ++index) {
        unsigned long long x[4];
#pragma unroll
        for (unsigned int k = 0; k < 4U; ++k) {
            x[k] = prev[k] ^ next[k];
        }
        // The next block's reference is older than `dense_end`, so it is stored
        // (also for the first block after this loop).
        const unsigned long long word0 = compress_begin(x, scratch_q, tid);
        const unsigned int ref = pass0_ref_index(
            index + 1U, __shfl_sync(ARGON2_COOP_WARP_MASK, word0, 0));
        load_block_regs_dd(memory + static_cast<unsigned long long>(ref) * 128ULL, next, tid);
        compress_finish(x, prev, scratch_q, tid);
        store_block_regs_dd(memory + static_cast<unsigned long long>(index) * 128ULL, prev, tid);
    }
    }

    // The rest may reference unstored blocks; odd blocks here are not stored.
    // `next` holds the next block's reference, or for an unstored reference
    // its (stored) previous block, from which the reference is rebuilt.
    bool next_is_prev = false;
    unsigned int tail_ref = 0U;
    for (unsigned int chunk = dense_end; chunk < lane_length; chunk += CANCEL_CHECK_BLOCK_INTERVAL) {
    if (cancel_requested(cancel_flag, tid)) {
        return false;
    }
    const unsigned int chunk_end = min(chunk + CANCEL_CHECK_BLOCK_INTERVAL, lane_length);
#pragma unroll 1
    for (unsigned int index = chunk; index < chunk_end; ++index) {
        unsigned long long x[4];
        if (next_is_prev) {
            unsigned long long ref_prev[4];
#pragma unroll
            for (unsigned int k = 0; k < 4U; ++k) {
                ref_prev[k] = next[k];
            }
            if (!rebuild_block(memory, tail_ref, skip_from, scratch_q, ref_prev, true, x, tid)) {
                return false;
            }
        } else {
#pragma unroll
            for (unsigned int k = 0; k < 4U; ++k) {
                x[k] = next[k];
            }
        }
#pragma unroll
        for (unsigned int k = 0; k < 4U; ++k) {
            x[k] ^= prev[k];
        }
        const unsigned long long word0 = compress_begin(x, scratch_q, tid);
        next_is_prev = false;
        if (index + 1U < lane_length) {
            tail_ref = pass0_ref_index(
                index + 1U, __shfl_sync(ARGON2_COOP_WARP_MASK, word0, 0));
            next_is_prev = __any_sync(ARGON2_COOP_WARP_MASK, block_skipped(tail_ref, skip_from));
            load_block_regs_dd(
                memory + block_slot(next_is_prev ? tail_ref - 1U : tail_ref, skip_from), next, tid);
        }
        compress_finish(x, prev, scratch_q, tid);
        if (index + 1U < lane_length && !block_skipped(index, skip_from)) {
            store_block_regs_dd(memory + block_slot(index, skip_from), prev, tid);
        }
    }
    }
#ifdef SEINE_PHASE_TIMING
    const long long phase_t2 = clock64();
    if (blockIdx.x == 0U) {
        seine_phase_cycles[0] = phase_t1 - phase_t0;
        seine_phase_cycles[1] = phase_t2 - phase_t1;
    }
#endif
    return true;
}

__global__ void touch_lane_memory_kernel(
    unsigned long long *lane_memory,
    unsigned int lanes_active,
    unsigned long long lane_stride_words
) {
    const unsigned int lane = blockIdx.x * blockDim.x + threadIdx.x;
    if (lane >= lanes_active || lane_stride_words == 0ULL) {
        return;
    }

    volatile unsigned long long *lane_ptr =
        lane_memory + static_cast<unsigned long long>(lane) * lane_stride_words;

    // Touch one 4 KiB page at a time (512 x u64) to force physical commit.
    for (unsigned long long word = 0ULL; word < lane_stride_words; word += 512ULL) {
        unsigned long long v = lane_ptr[word];
        lane_ptr[word] = v;
    }

    const unsigned long long tail = lane_stride_words - 1ULL;
    unsigned long long tail_v = lane_ptr[tail];
    lane_ptr[tail] = tail_v;
}

__device__ __forceinline__ void argon2id_fill_kernel_impl(
    const unsigned long long *__restrict__ seed_blocks,
    unsigned int lanes_active,
    unsigned int active_hashes,
    unsigned int lane_launch_iters,
    unsigned int m_blocks,
    unsigned int t_cost,
    unsigned long long *__restrict__ lane_memory,
    unsigned long long *__restrict__ out_last_blocks,
    const volatile unsigned int *__restrict__ cancel_flag,
    unsigned int *__restrict__ completed_iters,
    const unsigned char *__restrict__ target,
    unsigned int *__restrict__ found_index_one_based,
    bool fused_target_check,
    const unsigned int *__restrict__ ref_table,
    unsigned int stored_blocks,
    unsigned int skip_from
) {
    // With one warp per block the lane is the block index.
    const unsigned int warp_id = WARPS_PER_BLOCK == 1U ? 0U : threadIdx.x / ARGON2_COOP_THREADS;
    const unsigned int lane = blockIdx.x * WARPS_PER_BLOCK + warp_id;
    const unsigned int tid = threadIdx.x % ARGON2_COOP_THREADS;
    const unsigned int effective_m_blocks =
        (SEINE_FIXED_M_BLOCKS > 0U) ? SEINE_FIXED_M_BLOCKS : m_blocks;
    const unsigned int effective_t_cost =
        (SEINE_FIXED_T_COST > 0U) ? SEINE_FIXED_T_COST : t_cost;
    if (lane >= lanes_active || effective_m_blocks < 8U) {
        return;
    }

    const unsigned long long lane_stride_words =
        static_cast<unsigned long long>(stored_blocks) * 128ULL;
    unsigned long long *memory =
        lane_memory + static_cast<unsigned long long>(lane) * lane_stride_words;
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

#if !SEINE_FILL_V1
#if SEINE_ASYNC_REFS
    __shared__ __align__(16) unsigned long long ref_ring[WARPS_PER_BLOCK * REF_RING * 128];
    unsigned long long *my_ref_ring = ref_ring + warp_id * REF_RING * 128U;
#else
    unsigned long long *my_ref_ring = nullptr;
#endif
    if (effective_t_cost == 1U) {
        for (unsigned int iter = 0U; iter < lane_launch_iters; ++iter) {
            if (cancel_requested(cancel_flag, tid)) break;
            const unsigned long long global_hash_idx =
                static_cast<unsigned long long>(iter) * lanes_active +
                static_cast<unsigned long long>(lane);
            if (global_hash_idx >= static_cast<unsigned long long>(active_hashes)) {
                break;
            }
            unsigned long long last[4];
            if (!fill_lane_pass0(
                    memory,
                    seed_blocks + global_hash_idx * 256ULL,
                    ref_table,
                    lane_length,
                    skip_from,
                    cancel_flag,
                    my_scratch_q,
                    my_ref_ring,
                    last,
                    tid)) {
                break;
            }
            store_block_regs(out_last_blocks + global_hash_idx * 128ULL, last, tid);
            if (fused_target_check) {
                // The final block is never referenced, so its slot (or the last
                // stored slot when late blocks are skipped) is free to reuse.
                unsigned long long *last_in_memory =
                    memory + (static_cast<unsigned long long>(stored_blocks) - 1ULL) * 128ULL;
                store_block_regs(last_in_memory, last, tid);
                coop_sync();
                if (tid == 0U) {
                    unsigned char hash[POW_OUTPUT_BYTES];
                    blake2b_long_32_from_words(last_in_memory, hash);
                    if (hash_meets_target_be(hash, target)) {
                        atomicMin(
                            found_index_one_based,
                            static_cast<unsigned int>(global_hash_idx) + 1U
                        );
                    }
                }
            }
            coop_sync();
            if (tid == 0U) {
                atomicMax(completed_iters, iter + 1U);
            }
            coop_sync();
        }
#ifdef SEINE_PHASE_TIMING
        if (blockIdx.x == 0U && tid == 0U) {
            printf("seine-phase first=%.1f second=%.1f cycles/block\n",
                   (double)seine_phase_cycles[0] / (double)(effective_m_blocks / 2U - 2U),
                   (double)seine_phase_cycles[1] / (double)(effective_m_blocks / 2U));
        }
#endif
        return;
    }
#endif

    for (unsigned int iter = 0U; iter < lane_launch_iters; ++iter) {
        {
            unsigned int cancel_val = 0U;
            if (tid == 0U) cancel_val = cancel_flag[0];
            cancel_val = __shfl_sync(ARGON2_COOP_WARP_MASK, cancel_val, 0);
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

                for (unsigned int block = first_block; block < segment_length; ++block) {
                    if (((block - first_block) % CANCEL_CHECK_BLOCK_INTERVAL) == 0U) {
                        unsigned int cancel_val = 0U;
                        if (tid == 0U) cancel_val = cancel_flag[0];
                        cancel_val = __shfl_sync(ARGON2_COOP_WARP_MASK, cancel_val, 0);
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
                        rand64 = memory[static_cast<unsigned long long>(prev_index) * 128ULL];
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

                    // Prefetch hint for the next data-independent block.
                    if (data_independent && block + 1U < segment_length) {
                        const unsigned int next_addr_idx = (block + 1U) & 127U;
                        // Skip when the address_block boundary would require an
                        // update (next_addr_idx == 0); the update happens at the
                        // top of the NEXT iteration instead.
                        if (next_addr_idx != 0U && tid < 8U) {
                            const unsigned long long next_rand = my_address[next_addr_idx];
                            unsigned long long next_ref_area;
                            if (slice == 0U) {
                                next_ref_area = static_cast<unsigned long long>(block);
                            } else {
                                next_ref_area =
                                    static_cast<unsigned long long>(slice) * segment_length +
                                    static_cast<unsigned long long>(block + 1U) - 1ULL;
                            }
                            unsigned long long nm = next_rand & 0xFFFFFFFFULL;
                            nm = (nm * nm) >> 32;
                            const unsigned long long nrel =
                                next_ref_area - 1ULL - ((next_ref_area * nm) >> 32);
                            const unsigned int nref =
                                static_cast<unsigned int>(
                                    (static_cast<unsigned long long>(start_position) + nrel) %
                                    static_cast<unsigned long long>(lane_length)
                                );
                            const unsigned long long pf_base =
                                static_cast<unsigned long long>(nref) * 128ULL;
                            // Dropped on sm_120+; kept as codegen hint.
                            asm volatile(
                                "prefetch.global.L2 [%0];"
                                :: "l"(memory + pf_base + tid * 16ULL)
                            );
                        }
                    }

                    if (pass == 0U) {
                        compress_block_coop(
                            memory + rhs_offset,
                            memory + lhs_offset,
                            memory + dst_offset,
                            false,
                            my_scratch_r,
                            my_scratch_q,
                            tid
                        );
                    } else {
                        compress_block_coop(
                            memory + rhs_offset,
                            memory + lhs_offset,
                            memory + dst_offset,
                            true,
                            my_scratch_r,
                            my_scratch_q,
                            tid
                        );
                    }
                    coop_sync();

                    prev_index = cur_index;
                    cur_index += 1U;
                }
            }
        }
        if (abort_requested) {
            break;
        }

        const unsigned long long *last =
            memory + (static_cast<unsigned long long>(lane_length) - 1ULL) * 128ULL;
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
    unsigned long long *__restrict__ lane_memory,
    unsigned long long *__restrict__ out_last_blocks,
    const volatile unsigned int *__restrict__ cancel_flag,
    unsigned int *__restrict__ completed_iters,
    const unsigned int *__restrict__ ref_table,
    unsigned int stored_blocks,
    unsigned int skip_from
) {
    argon2id_fill_kernel_impl(
        seed_blocks,
        lanes_active,
        active_hashes,
        lane_launch_iters,
        m_blocks,
        t_cost,
        lane_memory,
        out_last_blocks,
        cancel_flag,
        completed_iters,
        nullptr,
        nullptr,
        false,
        ref_table,
        stored_blocks,
        skip_from
    );
}

// SEINE_NO_FUSED_TARGET leaves out the optional fused-target entry point.
#ifndef SEINE_NO_FUSED_TARGET
__global__ void argon2id_fill_kernel_fused_target(
    const unsigned long long *__restrict__ seed_blocks,
    unsigned int lanes_active,
    unsigned int active_hashes,
    unsigned int lane_launch_iters,
    unsigned int m_blocks,
    unsigned int t_cost,
    unsigned long long *__restrict__ lane_memory,
    unsigned long long *__restrict__ out_last_blocks,
    const volatile unsigned int *__restrict__ cancel_flag,
    unsigned int *__restrict__ completed_iters,
    const unsigned char *__restrict__ target,
    unsigned int *__restrict__ found_index_one_based,
    const unsigned int *__restrict__ ref_table,
    unsigned int stored_blocks,
    unsigned int skip_from
) {
    argon2id_fill_kernel_impl(
        seed_blocks,
        lanes_active,
        active_hashes,
        lane_launch_iters,
        m_blocks,
        t_cost,
        lane_memory,
        out_last_blocks,
        cancel_flag,
        completed_iters,
        target,
        found_index_one_based,
        true,
        ref_table,
        stored_blocks,
        skip_from
    );
}
#endif

} // extern "C"
