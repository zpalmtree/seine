// SPDX-License-Identifier: BSD-3-Clause
// CUDA Argon2id lane-fill kernel specialized for Seine's PoW profile.

extern "C" {

constexpr unsigned int ARGON2_COOP_THREADS = 32U;
constexpr unsigned int ARGON2_COOP_WARP_MASK = 0xFFFFFFFFU;
#ifndef SEINE_FIXED_M_BLOCKS
#define SEINE_FIXED_M_BLOCKS 0U
#endif
#ifndef SEINE_FIXED_T_COST
#define SEINE_FIXED_T_COST 0U
#endif

__device__ __forceinline__ void coop_sync() {
    __syncwarp(ARGON2_COOP_WARP_MASK);
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

    if (tid < 8U) {
        const unsigned int base = tid * 16U;
        unsigned long long v0 = scratch_q[base + 0U];
        unsigned long long v1 = scratch_q[base + 1U];
        unsigned long long v2 = scratch_q[base + 2U];
        unsigned long long v3 = scratch_q[base + 3U];
        unsigned long long v4 = scratch_q[base + 4U];
        unsigned long long v5 = scratch_q[base + 5U];
        unsigned long long v6 = scratch_q[base + 6U];
        unsigned long long v7 = scratch_q[base + 7U];
        unsigned long long v8 = scratch_q[base + 8U];
        unsigned long long v9 = scratch_q[base + 9U];
        unsigned long long v10 = scratch_q[base + 10U];
        unsigned long long v11 = scratch_q[base + 11U];
        unsigned long long v12 = scratch_q[base + 12U];
        unsigned long long v13 = scratch_q[base + 13U];
        unsigned long long v14 = scratch_q[base + 14U];
        unsigned long long v15 = scratch_q[base + 15U];

        permute_round(
            v0, v1, v2, v3, v4, v5, v6, v7,
            v8, v9, v10, v11, v12, v13, v14, v15
        );

        scratch_q[base + 0U] = v0;
        scratch_q[base + 1U] = v1;
        scratch_q[base + 2U] = v2;
        scratch_q[base + 3U] = v3;
        scratch_q[base + 4U] = v4;
        scratch_q[base + 5U] = v5;
        scratch_q[base + 6U] = v6;
        scratch_q[base + 7U] = v7;
        scratch_q[base + 8U] = v8;
        scratch_q[base + 9U] = v9;
        scratch_q[base + 10U] = v10;
        scratch_q[base + 11U] = v11;
        scratch_q[base + 12U] = v12;
        scratch_q[base + 13U] = v13;
        scratch_q[base + 14U] = v14;
        scratch_q[base + 15U] = v15;
    }
    coop_sync();

    if (tid < 8U) {
        const unsigned int b = tid * 2U;
        unsigned long long v0 = scratch_q[b];
        unsigned long long v1 = scratch_q[b + 1U];
        unsigned long long v2 = scratch_q[b + 16U];
        unsigned long long v3 = scratch_q[b + 17U];
        unsigned long long v4 = scratch_q[b + 32U];
        unsigned long long v5 = scratch_q[b + 33U];
        unsigned long long v6 = scratch_q[b + 48U];
        unsigned long long v7 = scratch_q[b + 49U];
        unsigned long long v8 = scratch_q[b + 64U];
        unsigned long long v9 = scratch_q[b + 65U];
        unsigned long long v10 = scratch_q[b + 80U];
        unsigned long long v11 = scratch_q[b + 81U];
        unsigned long long v12 = scratch_q[b + 96U];
        unsigned long long v13 = scratch_q[b + 97U];
        unsigned long long v14 = scratch_q[b + 112U];
        unsigned long long v15 = scratch_q[b + 113U];

        permute_round(
            v0, v1, v2, v3, v4, v5, v6, v7,
            v8, v9, v10, v11, v12, v13, v14, v15
        );

        scratch_q[b] = v0;
        scratch_q[b + 1U] = v1;
        scratch_q[b + 16U] = v2;
        scratch_q[b + 17U] = v3;
        scratch_q[b + 32U] = v4;
        scratch_q[b + 33U] = v5;
        scratch_q[b + 48U] = v6;
        scratch_q[b + 49U] = v7;
        scratch_q[b + 64U] = v8;
        scratch_q[b + 65U] = v9;
        scratch_q[b + 80U] = v10;
        scratch_q[b + 81U] = v11;
        scratch_q[b + 96U] = v12;
        scratch_q[b + 97U] = v13;
        scratch_q[b + 112U] = v14;
        scratch_q[b + 113U] = v15;
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
    unsigned int m_blocks,
    unsigned int t_cost,
    unsigned long long *__restrict__ lane_memory,
    unsigned long long *__restrict__ out_last_blocks
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
    const unsigned long long *seed =
        seed_blocks + static_cast<unsigned long long>(lane) * 256ULL;

    for (unsigned int i = tid; i < 256U; i += ARGON2_COOP_THREADS) {
        memory[i] = seed[i];
    }
    coop_sync();

    const unsigned int segment_length = effective_m_blocks / 4U;
    const unsigned int lane_length = effective_m_blocks;

    __shared__ unsigned long long address_block[128];
    __shared__ unsigned long long input_block[128];
    __shared__ unsigned long long zero_block[128];
    __shared__ unsigned long long scratch_r[128];
    __shared__ unsigned long long scratch_q[128];

    for (unsigned int i = tid; i < 128U; i += ARGON2_COOP_THREADS) {
        address_block[i] = 0ULL;
        input_block[i] = 0ULL;
        zero_block[i] = 0ULL;
    }
    coop_sync();

    for (unsigned int pass = 0; pass < effective_t_cost; ++pass) {
        for (unsigned int slice = 0; slice < 4U; ++slice) {
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

    const unsigned long long *last =
        memory + (static_cast<unsigned long long>(lane_length) - 1ULL) * 128ULL;
    unsigned long long *out =
        out_last_blocks + static_cast<unsigned long long>(lane) * 128ULL;
    for (unsigned int i = tid; i < 128U; i += ARGON2_COOP_THREADS) {
        out[i] = last[i];
    }
}

} // extern "C"
