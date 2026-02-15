# CPU optimization journal

This journal tracks CPU backend/hash-kernel tuning attempts and measured outcomes.

## Benchmark protocol

### Kernel benchmark (single lane, deterministic)

```bash
cargo run --release -- --bench --bench-kind kernel --backend cpu --threads 1 --disable-cpu-autotune-threads --bench-secs 12 --bench-rounds 4 --bench-warmup-rounds 1 --ui plain --bench-output <report.json>
```

### Backend benchmark (persistent worker path)

```bash
cargo run --release -- --bench --bench-kind backend --backend cpu --threads 1 --disable-cpu-autotune-threads --bench-secs 12 --bench-rounds 4 --bench-warmup-rounds 1 --ui plain --bench-output <report.json>
```

## Baseline

- Kernel baseline report: `data/bench_cpu_kernel_baseline.json`
- Kernel summary: `avg=1.188 H/s`, `median=1.167 H/s`, `hashes=57 counted=57`
- Backend baseline report: `data/bench_cpu_backend_baseline_head.json` (captured from detached baseline worktree at `32a3ded`)
- Backend baseline summary: `avg=1.193 H/s`, `median=1.195 H/s`, `hashes=60 counted=56 late=4`

## Attempted changes

### Attempt 1: inner-loop arithmetic cleanup and bounds-check reduction

- Simplified `fill_blocks` arithmetic for single-lane invariants.
- Replaced saturating arithmetic in hot loops with direct arithmetic under proven bounds.
- Added `fill_block_from_refs` helper with unchecked element access in the fill loop.
- Kernel report: `data/bench_cpu_kernel_opt1.json`
- Result: `avg=1.188 H/s` (no measurable gain versus baseline in this protocol).
- Status: folded into later implementation where still useful/readable, but not sufficient alone.

### Attempt 2: precompute deterministic Argon2id data-independent references (kept)

- Added one-time precomputation for data-independent slice reference indices in `FixedArgon2id::new`.
- Reused precomputed indices for slice `0` and `1` in `fill_blocks`.
- Removed per-hash address-block generation and reference mapping for the deterministic half of the pass.
- Kept data-dependent slices (`2`, `3`) runtime-computed to preserve Argon2id semantics.
- Kernel reports: `data/bench_cpu_kernel_opt2.json`, `data/bench_cpu_kernel_opt3.json`
- Kernel result: `avg=1.250 H/s`, `median=1.250 H/s`, `hashes=60 counted=60`
- Kernel delta vs baseline: `+5.26%` average H/s (`1.250 / 1.188 - 1`).
- Backend report: `data/bench_cpu_backend_opt3.json`
- Backend result: `avg=1.201 H/s`, `median=1.202 H/s`, `hashes=60 counted=56 late=4`
- Backend delta vs baseline: `+0.70%` average H/s (`1.201 / 1.193 - 1`).
- Status: kept.

## Validation

- Correctness: `cargo test fixed_kernel_matches_reference_for_small_memory_configs -- --nocapture`
- Full suite (at time of Attempt 2): `cargo test` (`177 passed`).

## 2026-02-14 full miner + CPU backend rework sweep

All runs in this section used the same protocol as above, plus long-window confirmation runs:

```bash
cargo run --release -- --bench --bench-kind backend --backend cpu --threads 1 --disable-cpu-autotune-threads --bench-secs 30 --bench-rounds 3 --bench-warmup-rounds 1 --ui plain --bench-output <report.json>
```

### Session baseline (same host/session, fresh capture)

- Kernel baseline report: `data/bench_cpu_kernel_rework_baseline.json`
- Kernel summary: `avg=1.125 H/s`, `median=1.125 H/s`, `counted=54`
- Backend baseline report: `data/bench_cpu_backend_rework_baseline.json`
- Backend summary: `avg=1.070 H/s`, `median=1.063 H/s`, `counted=49`, `late=4`

### Attempt 3: deep fixed-argon hot-path refactor (reverted)

- Changes trialed:
  - BLAMKA arithmetic rewrite (`Wrapping` -> `wrapping_*` + low-word multiply helper).
  - AVX2 path dispatch refactor.
  - Loop/index structure rewrites (including one aggressive unrolled variant).
- Probe result (aggressive unrolled variant): `data/bench_cpu_kernel_rework_probe1.json` => `avg=0.688 H/s` (severe regression).
- A/B result vs head:
  - Kernel: `data/bench_cpu_kernel_ab_opt.json` (`1.083`) vs `data/bench_cpu_kernel_ab_head.json` (`1.167`) => `-7.14%`.
  - Backend: `data/bench_cpu_backend_ab_opt.json` (`1.045`) vs `data/bench_cpu_backend_ab_head.json` (`1.065`) => `-1.88%`.
- Status: reverted.

### Attempt 4: deadline-aware hash start gating (reverted)

- Added per-worker rolling hash-time estimate and skipped starting hashes near `stop_at`.
- Backend report: `data/bench_cpu_backend_rework_opt1.json`
- Result: `avg=1.000 H/s`, `late=0` but lower counted throughput than session baseline (`1.070 H/s`).
- Status: reverted.

### Attempt 5: backend tuning sweep (not adopted)

- Short sweep (`8s x 2 rounds`) over:
  - `default (64/1/50)`, `128/1/100`, `256/4/200`, `256/8/200`, `512/16/250`, `64/8/50`.
- Best short-run candidate: `256/4/200` (`data/bench_cpu_backend_tune_b256_c4_f200.json`, `avg=1.106 H/s`).
- Long-run confirmation:
  - Default: `data/bench_cpu_backend_default_long.json` => `avg=1.075 H/s`.
  - Tuned `256/4/200`: `data/bench_cpu_backend_tuned_long.json` => `avg=1.067 H/s`.
  - Throughput-like `256/8/200`: `data/bench_cpu_backend_throughput_long.json` => `avg=1.078 H/s` (effectively noise-level uplift vs default).
  - Affinity off: `data/bench_cpu_backend_affinity_off_long.json` => `avg=1.073 H/s`.
- Status: defaults unchanged.

### Attempt 6: cache AVX2 feature detection at hasher construction (not adopted)

- Change: store `use_avx2` in `FixedArgon2id::new` and branch on cached bool per hash instead of calling feature detection in `hash_password_into_with_memory`.
- Long-run repeats:
  - Kernel cached-bool: `data/bench_cpu_kernel_avx2cache_long.json` => `avg=1.089 H/s`.
  - Kernel head repeat: `data/bench_cpu_kernel_head_long_repeat2.json` => `avg=1.089 H/s`.
  - Backend cached-bool: `data/bench_cpu_backend_avx2cache_long.json` => `avg=1.089 H/s`.
  - Backend head repeat: `data/bench_cpu_backend_head_long_repeat2.json` => `avg=1.081 H/s`.
- Note: counted hashes were effectively identical in repeated runs; observed delta tracked fence jitter.
- Status: not adopted (no reproducible throughput gain).

### Sweep validation

- Correctness checks during each retained candidate: `cargo test fixed_kernel_matches_reference_for_small_memory_configs -- --nocapture`
- Full suite at end of sweep: `cargo test` (`180 passed`).

## 2026-02-14 additional miner/backend/kernel rewrite pass (A/B with detached baseline)

This pass explicitly paired baseline and candidate runs from a detached `HEAD` worktree (`bedc837`) and the active worktree, with sequential (non-overlapping) benchmark execution.

### Notes on benchmark hygiene

- The first capture (`data/bench_cpu_kernel_rewrite_baseline.json`, `data/bench_cpu_backend_rewrite_baseline.json`) accidentally ran kernel/backend in parallel and is not used for decision-making.
- All acceptance decisions below use sequential A/B runs.
- Long runs showed thermal drift over time, so late-phase baseline repeats were also captured for pairing.

### Attempt 7: Linux huge-page hint + deterministic-slice prefetch (not adopted)

- Candidate reports:
  - Kernel: `data/bench_cpu_kernel_attempt7_opt_seq.json` (`avg=1.417 H/s`)
  - Backend: `data/bench_cpu_backend_attempt7_opt_seq.json` (`avg=1.332 H/s`)
- Baseline (same protocol):
  - Kernel: `data/bench_cpu_kernel_attempt7_baseline_seq.json` (`avg=1.396 H/s`)
  - Backend: `data/bench_cpu_backend_attempt7_baseline_seq.json` (`avg=1.373 H/s`)
- Outcome:
  - Kernel showed a short-run uplift, but backend regressed materially.
  - Status: rejected; prefetch removed.

### Attempt 7b: Linux huge-page hint only (not adopted)

- Candidate reports:
  - Kernel: `data/bench_cpu_kernel_attempt7_opt2_seq.json` (`avg=1.417 H/s`)
  - Backend: `data/bench_cpu_backend_attempt7_opt2_seq.json` (`avg=1.358 H/s`)
- Long confirmation:
  - Baseline long backend: `data/bench_cpu_backend_attempt7_baseline_long.json` (`avg=1.360 H/s`)
  - Candidate long backend: `data/bench_cpu_backend_attempt7_opt2_long.json` (`avg=1.351 H/s`)
- Outcome: no reproducible backend gain; status rejected.

### Attempt 8: isolated BLAMKA helper rewrite (not adopted)

- Candidate reports:
  - Kernel: `data/bench_cpu_kernel_attempt8_opt_seq.json` (`avg=1.396 H/s`)
  - Backend: `data/bench_cpu_backend_attempt8_opt_seq.json` (`avg=1.361 H/s`)
- Outcome: kernel flat, backend below baseline; status rejected.

### Attempt 9: AVX2 dispatch once per hash (remove per-block target-feature call indirection) (not adopted)

- Candidate reports:
  - Kernel short: `data/bench_cpu_kernel_attempt9_opt_seq.json` (`avg=1.417 H/s`)
  - Backend short: `data/bench_cpu_backend_attempt9_opt_seq.json` (`avg=1.354 H/s`)
  - Kernel long: `data/bench_cpu_kernel_attempt9_opt_long.json` (`avg=1.322 H/s`)
  - Kernel long repeat: `data/bench_cpu_kernel_attempt9_opt_long_repeat2.json` (`avg=1.333 H/s`)
  - Backend long: `data/bench_cpu_backend_attempt9_opt_long.json` (`avg=1.364 H/s`)
  - Backend long repeats: `data/bench_cpu_backend_attempt9_opt_long_repeat2.json` (`avg=1.324 H/s`), `data/bench_cpu_backend_attempt9_opt_long_repeat3_late.json` (`avg=1.319 H/s`)
- Paired late baseline:
  - `data/bench_cpu_backend_attempt7_baseline_long_repeat2_late.json` (`avg=1.319 H/s`)
- Outcome: results stayed inside run-to-run noise once paired; no robust, repeatable uplift. Status rejected.

### Attempt 10: raw-pointer block access in `fill_block_from_refs` (not adopted)

- Candidate reports:
  - Backend short (late phase): `data/bench_cpu_backend_attempt10_opt_seq.json` (`avg=1.330 H/s`)
  - Kernel short (late phase): `data/bench_cpu_kernel_attempt10_opt_seq_late3.json` (`avg=1.333 H/s`)
  - Backend long (late phase): `data/bench_cpu_backend_attempt10_opt_long_late3.json` (`avg=1.266 H/s`)
- Paired late baseline:
  - Backend short: `data/bench_cpu_backend_attempt7_baseline_seq_late3.json` (`avg=1.322 H/s`)
  - Kernel short: `data/bench_cpu_kernel_attempt7_baseline_seq_late3.json` (`avg=1.354 H/s`)
  - Backend long: `data/bench_cpu_backend_attempt7_baseline_long_late4.json` (`avg=1.294 H/s`)
- Outcome: long-run backend regression; status rejected.

### Final status of this pass

- No candidate delivered a stable, reproducible hashrate increase across both kernel and backend paths.
- Code was reverted to `HEAD` after benchmarking to avoid carrying unproven changes.
- All attempt artifacts were kept under `data/` for future A/B reference.

## 2026-02-14 AVX2 SIMD rewrite + thermal-stable interleaved A/B harness

This pass implemented the remaining rework items that were previously proposed:

- True SIMD kernel path for CPU Argon2 block compression (AVX2).
- Native-build workflow and profile (`scripts/build_cpu_native.sh`, `profile.release-native`).
- Thermal-stable interleaved A/B benchmark harness (`scripts/bench_cpu_ab.sh`) with cooldown + alternating order.

### Attempt 11: AVX2 SIMD `PowBlock::compress` path (adopted)

- Implemented AVX2 vector path in `src/backend/cpu/fixed_argon.rs`:
  - Runtime ISA dispatch per hash (`scalar` / `avx2` / `avx512`).
  - AVX2 BLAMKA + rotate + row/column permutation rounds.
  - Safe scalar fallback preserved.
  - AVX-512 dispatch remains wired but currently reuses scalar transform for correctness.
- Interleaved A/B (detached baseline worktree `f953098` vs candidate, release profile):
  - Backend summary: `data/bench_cpu_ab_backend_attempt11b_release/summary.txt`
    - Baseline avg: `1.305448201389 H/s`
    - Candidate avg: `1.567800481676 H/s`
    - Delta: `+20.0967%`
  - Kernel summary: `data/bench_cpu_ab_kernel_attempt11b_release/summary.txt`
    - Baseline avg: `1.333333333333 H/s`
    - Candidate avg: `1.583333333333 H/s`
    - Delta: `+18.7500%`
- Status: adopted.

### Attempt 12: native-build profile/workflow A/B (not adopted as default)

- Added host-native build wrapper:
  - `scripts/build_cpu_native.sh` (`RUSTFLAGS += -C target-cpu=native`).
- Added per-side profile/native selection to A/B harness for fair release-vs-native comparison.
- Interleaved A/B on same candidate code (`release` vs `release-native + target-cpu=native`):
  - First run: `data/bench_cpu_ab_backend_native_attempt11b/summary.txt` => `-0.3836%`
  - Retest after simplifying `profile.release-native` to inherit `release` only:
    - `data/bench_cpu_ab_backend_native_attempt11c/summary.txt` => `-0.8391%`
- Status: keep workflow as optional/manual, not default.

### Validation

- Correctness: `cargo test fixed_kernel_matches_reference_for_small_memory_configs -- --nocapture`
- Full suite after changes: `cargo test` (`177 passed`).

## 2026-02-14 in-place compress to eliminate memcpy ABI overhead

### Attempt 13: AVX2 explicit rotation shuffles & column-round 128-bit loads (not adopted)

- Replaced scalar `rotr32` / `rotr24` / `rotr16` with explicit `_mm256_shuffle_epi32` / `_mm256_shuffle_epi8`.
- Replaced column-round scalar gather/scatter with 128-bit `_mm_loadu_si128` / `_mm_storeu_si128` loads.
- Assembly comparison (`objdump -d`) showed LLVM was already emitting identical `vpshufd $0xb1`, `vpshufb`, and aligned loads in the baseline.
- Kernel A/B result: 0% delta (no-op).
- Status: reverted.

### Attempt 14: in-place `compress_into` to eliminate 1 KB memcpy per block (adopted)

- **Root cause identified**: `compress_avx2` is annotated with `#[target_feature(enable = "avx2")]`, which prevents inlining across ABI boundaries. Since `PowBlock` is 1024 bytes, the x86-64 SysV ABI returns it via a hidden pointer + `memcpy`. This `call memcpy@GLIBC` executes ~2M times per hash (~2 GB of unnecessary copies per hash).
- **Fix**: Added `compress_into<ISA>(rhs, lhs, dst)` dispatch + `compress_avx2_into` that writes XOR and permutation results directly to `dst` (the destination block in the memory array) instead of returning a stack-allocated `PowBlock`.
- **Borrow checker workaround**: `fill_block_from_refs` now uses raw pointer arithmetic (`as_mut_ptr() + .add()`) to obtain simultaneous `&prev`, `&refb`, `&mut dst` from the same `memory_blocks` slice, since the caller guarantees non-overlapping indices.
- Original `compress` / `compress_avx2` retained for `update_address_block` (precomputation, not hot path).
- Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 20s cooldown, release profile):
  - Kernel summary: `data/bench_cpu_ab_kernel_inplace/summary.txt`
    - Baseline avg: `1.372 H/s`
    - Candidate avg: `1.428 H/s`
    - Delta: `+4.05%`
  - Backend summary: `data/bench_cpu_ab_backend_inplace/summary.txt`
    - Baseline avg: `1.361 H/s`
    - Candidate avg: `1.449 H/s`
    - Delta: `+6.41%`
  - Per-pair backend breakdown (candidate wins all 4):
    - Pair 1: baseline=1.361, candidate=1.483 (B first)
    - Pair 2: baseline=1.395, candidate=1.461 (C first)
    - Pair 3: baseline=1.382, candidate=1.414 (B first)
    - Pair 4: baseline=1.309, candidate=1.438 (C first)
- Status: adopted.

### Attempt 15: software prefetching for next ref block in fill_blocks (adopted)

- **Rationale**: With in-place compress eliminating ~2 GB of memcpy per hash, the memory subsystem is less saturated. The `fill_blocks` hot loop randomly accesses `memory_blocks[ref_index]` across a 2 GiB array (~524K pages), far exceeding L3 cache (32 MB) and L2 dTLB (2048 entries). Prefetching the next `ref` block during the current compress hides DRAM + TLB miss latency.
- **Previous attempt 7**: Tried prefetching but (a) was combined with huge pages, (b) code still had memcpy overhead, (c) benchmark methodology was non-interleaved. Now viable with in-place compress reducing memory pressure.
- **Implementation**:
  - Added `prefetch_pow_block()` helper: issues `prefetcht0` at offsets 0 and 512 (resolves TLB, primes hardware prefetcher for full 1 KiB).
  - Data-independent slices (0, 1): prefetch `ref_indexes[block+1]` before current compress.
  - Data-dependent slices (2, 3): after writing `cur_index`, read its first `u64` (L1-hot), compute next `ref_index`, and prefetch that block.
- Baseline: commit `4216b32` (in-place compress).
- Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 20s cooldown, release profile):
  - Kernel summary: `data/bench_cpu_ab_kernel_prefetch/summary.txt`
    - Baseline avg: `1.458 H/s`
    - Candidate avg: `1.514 H/s`
    - Delta: `+3.81%`
  - Backend summary: `data/bench_cpu_ab_backend_prefetch/summary.txt`
    - Baseline avg: `1.447 H/s`
    - Candidate avg: `1.502 H/s`
    - Delta: `+3.82%`
  - Candidate wins all 4 pairs in both benchmarks.
- Status: adopted.

### Attempt 16: fat LTO (`lto = "fat"` instead of `lto = "thin"`) (adopted)

- **Rationale**: Thin LTO performs parallel, module-scoped link-time optimization. Fat LTO serializes the entire program into a single LLVM module for deeper cross-crate inlining and optimization. With `codegen-units = 1` already set, the main additional benefit is cross-crate optimization (e.g., inlining `argon2`, `blake2`, `blocknet-pow-spec` calls into our hot path). Build time increases from ~25s to ~67s but this only affects development.
- **Change**: `Cargo.toml` `[profile.release]` `lto = "thin"` -> `lto = "fat"`.
- Baseline: commit `86015db` (prefetch, thin LTO).
- Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 20s cooldown, release profile):
  - Kernel summary: `data/bench_cpu_ab_kernel_fatlto/summary.txt`
    - Baseline avg: `1.531 H/s`
    - Candidate avg: `1.650 H/s`
    - Delta: `+7.80%`
  - Backend summary: `data/bench_cpu_ab_backend_fatlto/summary.txt`
    - Baseline avg: `1.520 H/s`
    - Candidate avg: `1.616 H/s`
    - Delta: `+6.31%`
  - Per-pair backend breakdown (candidate wins all 4):
    - Pair 1: baseline=1.573, candidate=1.661 (B first)
    - Pair 2: baseline=1.508, candidate=1.652 (C first)
    - Pair 3: baseline=1.505, candidate=1.552 (B first)
    - Pair 4: baseline=1.496, candidate=1.600 (C first)
- Status: adopted.
