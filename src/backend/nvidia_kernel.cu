// SPDX-License-Identifier: BSD-3-Clause
// CUDA Argon2id lane-fill kernel specialized for Seine's PoW profile.

extern "C" {

constexpr unsigned int ARGON2_COOP_THREADS = 32U;
constexpr unsigned int ARGON2_COOP_WARP_MASK = 0xFFFFFFFFU;
constexpr unsigned int SEED_KERNEL_THREADS = 64U;
constexpr unsigned int EVAL_KERNEL_THREADS = 64U;
constexpr unsigned int CANCEL_CHECK_BLOCK_INTERVAL = 32U;
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

__device__ __forceinline__ void blake2b_long_1024(
    const unsigned char *input,
    unsigned int input_len,
    unsigned char *out
) {
    unsigned char out_len_le[4];
    store_u32_le(out_len_le, 1024U);

    unsigned char last_output[64];
    Blake2bState state;
    blake2b_init(state, 64U);
    blake2b_update(state, out_len_le, 4U);
    blake2b_update(state, input, input_len);
    blake2b_final(state, last_output, 64U);

    unsigned int counter = 0U;
    for (unsigned int i = 0U; i < 32U; ++i) {
        out[counter + i] = last_output[i];
    }
    counter += 32U;

    while ((1024U - counter) > 64U) {
        unsigned char next_output[64];
        blake2b_hash(last_output, 64U, next_output, 64U);
        for (unsigned int i = 0U; i < 64U; ++i) {
            last_output[i] = next_output[i];
        }
        for (unsigned int i = 0U; i < 32U; ++i) {
            out[counter + i] = last_output[i];
        }
        counter += 32U;
    }

    const unsigned int tail_len = 1024U - counter;
    blake2b_hash(last_output, 64U, out + counter, tail_len);
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

    unsigned char seed_bytes[1024];
    for (unsigned int seed_idx = 0U; seed_idx < 2U; ++seed_idx) {
        store_u32_le(seed_input + 64U, seed_idx);
        blake2b_long_1024(seed_input, 72U, seed_bytes);
        unsigned long long *out_words =
            seed_blocks + static_cast<unsigned long long>(hash_idx) * 256ULL +
            static_cast<unsigned long long>(seed_idx) * 128ULL;
        for (unsigned int word = 0U; word < 128U; ++word) {
            out_words[word] = load_u64_le(seed_bytes + word * 8U);
        }
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
    d = ((d ^ a) >> 32) | ((d ^ a) << (64 - 32));
    c = blamka(c, d);
    b = ((b ^ c) >> 24) | ((b ^ c) << (64 - 24));

    a = blamka(a, b);
    d = ((d ^ a) >> 16) | ((d ^ a) << (64 - 16));
    c = blamka(c, d);
    b = ((b ^ c) >> 63) | ((b ^ c) << 1);
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
    for (unsigned int i = tid; i < 128U; i += ARGON2_COOP_THREADS) {
        const unsigned long long r = rhs[i] ^ lhs[i];
        scratch_r[i] = r;
        scratch_q[i] = r;
    }
    coop_sync();

    // Use all 32 threads: 8 independent states x 4 threads/state.
    {
        const unsigned int state = tid >> 2; // 0..7
        const unsigned int lane = tid & 3U;  // 0..3
        const unsigned int base = state * 16U;

        unsigned int i0 = 0U;
        unsigned int i1 = 0U;
        unsigned int i2 = 0U;
        unsigned int i3 = 0U;
        switch (lane) {
            case 0U:
                i0 = base + 0U;
                i1 = base + 4U;
                i2 = base + 8U;
                i3 = base + 12U;
                break;
            case 1U:
                i0 = base + 1U;
                i1 = base + 5U;
                i2 = base + 9U;
                i3 = base + 13U;
                break;
            case 2U:
                i0 = base + 2U;
                i1 = base + 6U;
                i2 = base + 10U;
                i3 = base + 14U;
                break;
            default:
                i0 = base + 3U;
                i1 = base + 7U;
                i2 = base + 11U;
                i3 = base + 15U;
                break;
        }

        unsigned long long a = scratch_q[i0];
        unsigned long long b = scratch_q[i1];
        unsigned long long c = scratch_q[i2];
        unsigned long long d = scratch_q[i3];
        permute_step(a, b, c, d);
        scratch_q[i0] = a;
        scratch_q[i1] = b;
        scratch_q[i2] = c;
        scratch_q[i3] = d;
    }
    coop_sync();

    {
        const unsigned int state = tid >> 2; // 0..7
        const unsigned int lane = tid & 3U;  // 0..3
        const unsigned int base = state * 16U;

        unsigned int i0 = 0U;
        unsigned int i1 = 0U;
        unsigned int i2 = 0U;
        unsigned int i3 = 0U;
        switch (lane) {
            case 0U:
                i0 = base + 0U;
                i1 = base + 5U;
                i2 = base + 10U;
                i3 = base + 15U;
                break;
            case 1U:
                i0 = base + 1U;
                i1 = base + 6U;
                i2 = base + 11U;
                i3 = base + 12U;
                break;
            case 2U:
                i0 = base + 2U;
                i1 = base + 7U;
                i2 = base + 8U;
                i3 = base + 13U;
                break;
            default:
                i0 = base + 3U;
                i1 = base + 4U;
                i2 = base + 9U;
                i3 = base + 14U;
                break;
        }

        unsigned long long a = scratch_q[i0];
        unsigned long long b = scratch_q[i1];
        unsigned long long c = scratch_q[i2];
        unsigned long long d = scratch_q[i3];
        permute_step(a, b, c, d);
        scratch_q[i0] = a;
        scratch_q[i1] = b;
        scratch_q[i2] = c;
        scratch_q[i3] = d;
    }
    coop_sync();

    // Column round: 8 independent interleaved states x 4 threads/state.
    {
        const unsigned int state = tid >> 2; // 0..7
        const unsigned int lane = tid & 3U;  // 0..3
        const unsigned int b = state * 2U;

        unsigned int i0 = 0U;
        unsigned int i1 = 0U;
        unsigned int i2 = 0U;
        unsigned int i3 = 0U;
        switch (lane) {
            case 0U:
                i0 = b;
                i1 = b + 32U;
                i2 = b + 64U;
                i3 = b + 96U;
                break;
            case 1U:
                i0 = b + 1U;
                i1 = b + 33U;
                i2 = b + 65U;
                i3 = b + 97U;
                break;
            case 2U:
                i0 = b + 16U;
                i1 = b + 48U;
                i2 = b + 80U;
                i3 = b + 112U;
                break;
            default:
                i0 = b + 17U;
                i1 = b + 49U;
                i2 = b + 81U;
                i3 = b + 113U;
                break;
        }

        unsigned long long a = scratch_q[i0];
        unsigned long long c = scratch_q[i1];
        unsigned long long d = scratch_q[i2];
        unsigned long long e = scratch_q[i3];
        permute_step(a, c, d, e);
        scratch_q[i0] = a;
        scratch_q[i1] = c;
        scratch_q[i2] = d;
        scratch_q[i3] = e;
    }
    coop_sync();

    {
        const unsigned int state = tid >> 2; // 0..7
        const unsigned int lane = tid & 3U;  // 0..3
        const unsigned int b = state * 2U;

        unsigned int i0 = 0U;
        unsigned int i1 = 0U;
        unsigned int i2 = 0U;
        unsigned int i3 = 0U;
        switch (lane) {
            case 0U:
                i0 = b;
                i1 = b + 33U;
                i2 = b + 80U;
                i3 = b + 113U;
                break;
            case 1U:
                i0 = b + 1U;
                i1 = b + 48U;
                i2 = b + 81U;
                i3 = b + 96U;
                break;
            case 2U:
                i0 = b + 16U;
                i1 = b + 49U;
                i2 = b + 64U;
                i3 = b + 97U;
                break;
            default:
                i0 = b + 17U;
                i1 = b + 32U;
                i2 = b + 65U;
                i3 = b + 112U;
                break;
        }

        unsigned long long a = scratch_q[i0];
        unsigned long long c = scratch_q[i1];
        unsigned long long d = scratch_q[i2];
        unsigned long long e = scratch_q[i3];
        permute_step(a, c, d, e);
        scratch_q[i0] = a;
        scratch_q[i1] = c;
        scratch_q[i2] = d;
        scratch_q[i3] = e;
    }
    coop_sync();

    for (unsigned int i = tid; i < 128U; i += ARGON2_COOP_THREADS) {
        const unsigned long long mixed = scratch_q[i] ^ scratch_r[i];
        if (xor_out) {
            out[i] ^= mixed;
        } else {
            out[i] = mixed;
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
    const unsigned char *__restrict__ target,
    unsigned int *__restrict__ found_index_one_based,
    unsigned int evaluate_target
) {
    const unsigned int lane = blockIdx.x;
    const unsigned int tid = threadIdx.x;
    const unsigned int effective_m_blocks =
        (SEINE_FIXED_M_BLOCKS > 0U) ? SEINE_FIXED_M_BLOCKS : m_blocks;
    const unsigned int effective_t_cost =
        (SEINE_FIXED_T_COST > 0U) ? SEINE_FIXED_T_COST : t_cost;
    if (lane >= lanes_active || effective_m_blocks < 8U) {
        return;
    }

    const unsigned long long lane_stride_words =
        static_cast<unsigned long long>(effective_m_blocks) * 128ULL;
    unsigned long long *memory =
        lane_memory + static_cast<unsigned long long>(lane) * lane_stride_words;
    const unsigned int segment_length = effective_m_blocks / 4U;
    const unsigned int lane_length = effective_m_blocks;

    __shared__ unsigned long long address_block[128];
    __shared__ unsigned long long input_block[128];
    __shared__ unsigned long long zero_block[128];
    __shared__ unsigned long long scratch_r[128];
    __shared__ unsigned long long scratch_q[128];
    __shared__ unsigned int cancel_requested;
    bool abort_requested = false;

    for (unsigned int iter = 0U; iter < lane_launch_iters; ++iter) {
        if (tid == 0U) {
            cancel_requested = cancel_flag[0];
        }
        coop_sync();
        if (cancel_requested != 0U) {
            break;
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
            address_block[i] = 0ULL;
            input_block[i] = 0ULL;
            zero_block[i] = 0ULL;
        }
        coop_sync();

        for (unsigned int pass = 0; pass < effective_t_cost && !abort_requested; ++pass) {
            for (unsigned int slice = 0; slice < 4U && !abort_requested; ++slice) {
                const bool data_independent = (pass == 0U && slice < 2U);
                if (data_independent) {
                    for (unsigned int i = tid; i < 128U; i += ARGON2_COOP_THREADS) {
                        address_block[i] = 0ULL;
                        input_block[i] = 0ULL;
                    }
                    if (tid == 0U) {
                        input_block[0] = static_cast<unsigned long long>(pass);
                        input_block[1] = 0ULL; // lane index (p=1)
                        input_block[2] = static_cast<unsigned long long>(slice);
                        input_block[3] = static_cast<unsigned long long>(effective_m_blocks);
                        input_block[4] = static_cast<unsigned long long>(effective_t_cost);
                        input_block[5] = 2ULL; // Argon2id
                    }
                    coop_sync();
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
                        if (tid == 0U) {
                            cancel_requested = cancel_flag[0];
                        }
                        coop_sync();
                        if (cancel_requested != 0U) {
                            abort_requested = true;
                            break;
                        }
                    }

                    unsigned long long rand64;
                    if (data_independent) {
                        const unsigned int address_index = block & 127U;
                        if (address_index == 0U) {
                            update_address_block_coop(
                                address_block,
                                input_block,
                                zero_block,
                                scratch_r,
                                scratch_q,
                                tid
                            );
                        }
                        coop_sync();
                        rand64 = address_block[address_index];
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
                    if (pass == 0U) {
                        compress_block_coop(
                            memory + rhs_offset,
                            memory + lhs_offset,
                            memory + dst_offset,
                            false,
                            scratch_r,
                            scratch_q,
                            tid
                        );
                    } else {
                        compress_block_coop(
                            memory + rhs_offset,
                            memory + lhs_offset,
                            memory + dst_offset,
                            true,
                            scratch_r,
                            scratch_q,
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
        for (unsigned int i = tid; i < 128U; i += ARGON2_COOP_THREADS) {
            out[i] = last[i];
        }
        coop_sync();
        if (evaluate_target != 0U && tid == 0U) {
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
