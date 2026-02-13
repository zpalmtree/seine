// SPDX-License-Identifier: BSD-3-Clause
// CUDA Argon2id lane-fill kernel specialized for Seine's PoW profile.

extern "C" {

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

__device__ __forceinline__ void compress_block(
    const unsigned long long *rhs,
    const unsigned long long *lhs,
    unsigned long long *out,
    unsigned long long *scratch_r,
    unsigned long long *scratch_q
) {
#pragma unroll
    for (int i = 0; i < 128; ++i) {
        unsigned long long r = rhs[i] ^ lhs[i];
        scratch_r[i] = r;
        scratch_q[i] = r;
    }

#pragma unroll
    for (int row = 0; row < 8; ++row) {
        unsigned long long *v = scratch_q + row * 16;
        permute_round(
            v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
            v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
        );
    }

#pragma unroll
    for (int i = 0; i < 8; ++i) {
        const int b = i * 2;
        permute_round(
            scratch_q[b], scratch_q[b + 1],
            scratch_q[b + 16], scratch_q[b + 17],
            scratch_q[b + 32], scratch_q[b + 33],
            scratch_q[b + 48], scratch_q[b + 49],
            scratch_q[b + 64], scratch_q[b + 65],
            scratch_q[b + 80], scratch_q[b + 81],
            scratch_q[b + 96], scratch_q[b + 97],
            scratch_q[b + 112], scratch_q[b + 113]
        );
    }

#pragma unroll
    for (int i = 0; i < 128; ++i) {
        out[i] = scratch_q[i] ^ scratch_r[i];
    }
}

__device__ __forceinline__ void update_address_block(
    unsigned long long *address_block,
    unsigned long long *input_block,
    const unsigned long long *zero_block,
    unsigned long long *scratch_r,
    unsigned long long *scratch_q
) {
    input_block[6] += 1ULL;
    compress_block(
        zero_block,
        input_block,
        address_block,
        scratch_r,
        scratch_q
    );
    compress_block(
        zero_block,
        address_block,
        address_block,
        scratch_r,
        scratch_q
    );
}

__global__ void argon2id_fill_kernel(
    const unsigned long long *seed_blocks,
    unsigned int lanes_active,
    unsigned int m_blocks,
    unsigned int t_cost,
    unsigned long long *lane_memory,
    unsigned long long *out_last_blocks,
    unsigned long long *out_hashes_done
) {
    const unsigned int lane = blockIdx.x * blockDim.x + threadIdx.x;
    if (lane >= lanes_active || m_blocks < 8U) {
        return;
    }

    const unsigned long long lane_stride_words =
        static_cast<unsigned long long>(m_blocks) * 128ULL;
    unsigned long long *memory =
        lane_memory + static_cast<unsigned long long>(lane) * lane_stride_words;
    const unsigned long long *seed =
        seed_blocks + static_cast<unsigned long long>(lane) * 256ULL;

#pragma unroll
    for (int i = 0; i < 256; ++i) {
        memory[i] = seed[i];
    }

    const unsigned int segment_length = m_blocks / 4U;
    const unsigned int lane_length = m_blocks;

    unsigned long long address_block[128];
    unsigned long long input_block[128];
    unsigned long long zero_block[128];
    unsigned long long block_tmp[128];
    unsigned long long scratch_r[128];
    unsigned long long scratch_q[128];

#pragma unroll
    for (int i = 0; i < 128; ++i) {
        address_block[i] = 0ULL;
        input_block[i] = 0ULL;
        zero_block[i] = 0ULL;
        block_tmp[i] = 0ULL;
        scratch_r[i] = 0ULL;
        scratch_q[i] = 0ULL;
    }

    for (unsigned int pass = 0; pass < t_cost; ++pass) {
        for (unsigned int slice = 0; slice < 4U; ++slice) {
            const bool data_independent = (pass == 0U && slice < 2U);
            if (data_independent) {
#pragma unroll
                for (int i = 0; i < 128; ++i) {
                    address_block[i] = 0ULL;
                    input_block[i] = 0ULL;
                }
                input_block[0] = static_cast<unsigned long long>(pass);
                input_block[1] = 0ULL; // lane index (p=1)
                input_block[2] = static_cast<unsigned long long>(slice);
                input_block[3] = static_cast<unsigned long long>(m_blocks);
                input_block[4] = static_cast<unsigned long long>(t_cost);
                input_block[5] = 2ULL; // Argon2id
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
                        update_address_block(
                            address_block,
                            input_block,
                            zero_block,
                            scratch_r,
                            scratch_q
                        );
                    }
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

                compress_block(
                    memory + static_cast<unsigned long long>(prev_index) * 128ULL,
                    memory + static_cast<unsigned long long>(ref_index) * 128ULL,
                    block_tmp,
                    scratch_r,
                    scratch_q
                );

                const unsigned long long dst_offset =
                    static_cast<unsigned long long>(cur_index) * 128ULL;
                if (pass == 0U) {
#pragma unroll
                    for (int i = 0; i < 128; ++i) {
                        memory[dst_offset + static_cast<unsigned long long>(i)] = block_tmp[i];
                    }
                } else {
#pragma unroll
                    for (int i = 0; i < 128; ++i) {
                        memory[dst_offset + static_cast<unsigned long long>(i)] ^= block_tmp[i];
                    }
                }

                prev_index = cur_index;
                cur_index += 1U;
            }
        }
    }

    const unsigned long long *last =
        memory + (static_cast<unsigned long long>(lane_length) - 1ULL) * 128ULL;
    unsigned long long *out =
        out_last_blocks + static_cast<unsigned long long>(lane) * 128ULL;
#pragma unroll
    for (int i = 0; i < 128; ++i) {
        out[i] = last[i];
    }

    if (out_hashes_done != nullptr) {
        atomicAdd(out_hashes_done, 1ULL);
    }
}

} // extern "C"
