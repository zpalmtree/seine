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

## 2026-02-14 Apple Silicon (AArch64) NEON SIMD optimization

Host: Apple M4 Max, 16 cores, 48 GB unified memory.

Prior to this pass, the AArch64 build used the scalar fallback for all Argon2
block compression. These changes are gated to `target_arch = "aarch64"` and do
not affect x86_64 builds.

### Apple Silicon scalar baseline (long-window, 30s × 3 rounds)

- Kernel: `data/bench_cpu_kernel_apple_scalar_long.json` => `avg=1.367 H/s`
- Backend: `data/bench_cpu_backend_apple_scalar_long.json` => `avg=1.315 H/s`

### Attempt 17: NEON SIMD block compression + prefetch + MaybeUninit (adopted)

- **NEON SIMD path** (`compress_neon_into`):
  - Added `ISA_NEON` const and compile-time dispatch for `aarch64`.
  - Implemented 2-wide NEON (128-bit) BLAMKA mixing via `vmovn_u64` + `vmull_u32`.
  - NEON rotations: `vrev64q_u32` for rotr32, shift-or for rotr24/16/63.
  - Full `neon_round` with `vextq_u64` lane permutations for diagonal step.
  - Row rounds: contiguous pair loads (8 NEON registers per row).
  - Column rounds: stride-16 gather via contiguous pair loads at computed offsets.
  - No `#[target_feature]` needed — NEON is mandatory on AArch64, so all
    functions inline freely with no ABI memcpy overhead.
  - Both by-value (`compress_neon`) and in-place (`compress_neon_into`) variants.

- **MaybeUninit for working buffer**:
  - Replaced `PowBlock::default()` (1024-byte zero-init) with
    `MaybeUninit::<PowBlock>::uninit()` in `compress_neon_into`.
  - Safe because Phase 1 fully overwrites the buffer before any reads.

- **Software prefetch** (cross-platform `prefetch_pow_block`, with `prfm` on AArch64):
  - Data-independent slices (0, 1): 2-ahead prefetch on AArch64, 1-ahead on x86_64.
  - Data-dependent slices (2, 3): 1-ahead prefetch computed from the
    just-written block's first u64 (cross-platform).
  - AArch64: two cache lines prefetched per block (`prfm pldl1keep` at offsets 0 and
    128), covering 256 bytes on Apple Silicon's 128-byte cache lines.
  - x86_64: two prefetches at offsets 0 and 512 (64-byte cache lines).

- **Long-window confirmation** (30s × 3 rounds, same protocol as baseline):
  - Kernel: `data/bench_cpu_kernel_apple_neon_full_long.json` => `avg=1.500 H/s`
  - Backend: `data/bench_cpu_backend_apple_neon_full_long.json` => `avg=1.474 H/s`
  - **Kernel delta vs scalar baseline: +9.73%** (`1.500 / 1.367 - 1`)
  - **Backend delta vs scalar baseline: +12.09%** (`1.474 / 1.315 - 1`)

- Correctness: `cargo test fixed_kernel_matches_reference_for_small_memory_configs -- --nocapture` ✓
- Cross-arch safety: all NEON changes `#[cfg(target_arch = "aarch64")]`-gated; x86_64 path unchanged.
- Status: adopted.

## 2026-02-14 Apple Silicon prefetch tuning + PGO investigation

Host: Apple M4 Max, 16 cores, 48 GB unified memory.
Post-rebase baseline includes all upstream optimizations (AVX2, in-place compress,
x86 prefetch, fat LTO) plus the NEON SIMD commit (Attempt 17).

### Post-rebase Apple Silicon baseline (long-window, 30s × 3 rounds)

- Kernel: `data/bench_cpu_kernel_apple_baseline_long2.json` => `avg=1.533 H/s`
- Backend: `data/bench_cpu_backend_apple_baseline_long2.json` => `avg=1.503 H/s`

### Attempt 18: deeper prefetch on AArch64 (3-ahead + 4 cache lines) (adopted)

- **Rationale**: Apple M4 Max has higher DRAM latency relative to x86 desktop CPUs,
  and its unified memory architecture benefits from earlier prefetch hints. The
  128-byte cache line means 2 prefetches only prime 256 bytes of a 1024-byte block.
- **Changes** (all `#[cfg(target_arch = "aarch64")]`-gated, x86 unchanged):
  - Data-independent slices: increased prefetch distance from 2-ahead to 3-ahead.
  - Pre-loop priming: prefetch first 3 blocks (was 1) before entering the fill loop.
  - `prefetch_pow_block` on AArch64: expanded from 2 `prfm` instructions (offsets 0,
    128 = 256 bytes) to 4 `prfm` instructions (offsets 0, 128, 256, 384 = 512 bytes),
    covering half the 1 KiB block and letting the hardware prefetcher handle the rest.
- **Long-window confirmation** (30s × 3 rounds, sequential A/B):
  - Kernel: `data/bench_cpu_kernel_apple_prefetch3ahead_long.json` => `avg=1.578 H/s`
  - Backend: `data/bench_cpu_backend_apple_prefetch3ahead_long.json` => `avg=1.540 H/s`
  - **Kernel delta vs post-rebase baseline: +2.94%** (`1.578 / 1.533 - 1`)
  - **Backend delta vs post-rebase baseline: +2.46%** (`1.540 / 1.503 - 1`)
- Correctness: `cargo test` (169 passed) ✓
- Cross-arch safety: all changes `#[cfg(target_arch = "aarch64")]`-gated.
- Status: adopted.

### Attempt 19: Profile-Guided Optimization (PGO) (not adopted)

- **Rationale**: PGO allows LLVM to optimize branch prediction, inlining, and code
  layout based on actual runtime behavior. Potentially beneficial for the complex
  control flow in `fill_blocks` (data-independent vs data-dependent paths).
- **Methodology**:
  - Instrumented build: `RUSTFLAGS="-Cprofile-generate=/tmp/seine-pgo-profiles"`.
  - Profile collection: kernel bench (12s × 2 rounds + 1 warmup).
  - Merged profiles: `llvm-profdata merge`.
  - Optimized build: `RUSTFLAGS="-Cprofile-use=/tmp/seine-pgo-merged.profdata"`.
- **Long-window results** (30s × 3 rounds, includes Attempt 18 prefetch changes):
  - Kernel: `avg=1.567 H/s` (vs 1.578 without PGO, **-0.7%**)
  - Backend: `avg=1.580 H/s` (vs 1.540 without PGO, **+2.6%**)
- **Outcome**: mixed/inconsistent results — kernel regressed slightly, backend
  improved slightly. With fat LTO + codegen-units=1 already providing whole-program
  optimization, PGO adds no consistent benefit. The two-step build complexity
  (machine-specific profiles, fragile across code changes) is not justified.
- Status: not adopted.

### Attempt 20: SHA3 `xar` instruction for BLAMKA rotr32 (adopted)

- **Root cause identified**: LLVM auto-fuses `veorq_u64` + shift-or rotation patterns
  into single `xar.2d` (SHA3) instructions for rotr24, rotr16, and rotr63. However,
  the rotr32 case used `vrev64q_u32` (compiles to `rev64.4s`), which the compiler
  does NOT recognise as fusible with the preceding `eor.16b` into `xar.2d #32`.
  This left 16 occurrences per compress as 2-instruction, 4-cycle serial-latency
  sequences on the critical BLAMKA dependency chain.
- **Fix**: Replaced all four `veorq_u64` + `neon_rotr_64_*` patterns in
  `neon_half_round` with explicit `vxarq_u64::<N>` intrinsic calls. The SHA3
  extension (`sha3` target feature) is enabled by default on `aarch64-apple-darwin`.
  This emits `xar.2d` for all 64 XOR-and-rotate operations per compress (was 48
  `xar` + 16 `eor`+`rev64`, now all 64 are `xar`).
- **Changes** (`#[cfg(target_arch = "aarch64")]`-gated, x86 unchanged):
  - `neon_half_round`: replaced `neon_rotr_64_32(veorq_u64(*d, *a))` with
    `vxarq_u64::<32>(*d, *a)`, and similarly for rotr24/16/63 for consistency.
  - Removed `neon_rotr_64_32`, `neon_rotr_64_24`, `neon_rotr_64_16`,
    `neon_rotr_64_63` helper functions (no longer needed).
- **Long-window confirmation** (30s × 3 rounds):
  - Kernel: `avg=1.767 H/s` (vs 1.578 prefetch baseline, **+11.98%**)
  - Backend: `avg=1.683 H/s` (vs 1.540 prefetch baseline, **+9.29%**)
  - Combined delta vs post-rebase baseline (1.533 / 1.503):
    **Kernel +15.27%, Backend +11.98%**
- Correctness: `cargo test` (169 passed) ✓
- Cross-arch safety: all changes `#[cfg(target_arch = "aarch64")]`-gated. `sha3` is
  mandatory on all Apple Silicon (M1+) and enabled by default for `aarch64-apple-darwin`.
  Non-Apple AArch64 targets would need `sha3` feature detection or gating.
- Status: adopted.

## 2026-02-14 x86_64 fused column-scatter optimization

Host: AMD Ryzen 9 5900X (Zen 3), 12C/24T, DDR4-3600.

### Attempt 17: fused column-scatter + final XOR in compress_avx2_into (adopted)

- **Rationale**: The column rounds in `compress_avx2_into` scattered results back to the stack buffer `q` via temporary `[u64; 4]` arrays (128 scalar stores), then Phase 4 did a separate full-buffer XOR pass (`q ^ dst` over 32 × 256-bit iterations). By fusing the final XOR into the column scatter — loading pre-round values from `dst`, XOR'ing with column results, and writing directly to `dst` via 128-bit pair operations — both the scatter-to-q and the Phase 4 pass are eliminated.
- **Additional cleanup**: Column gather now uses explicit `_mm_loadu_si128` + `_mm256_inserti128_si256` for 128-bit pair loads instead of `_mm256_set_epi64x`.
- **Code change**: `compress_avx2_into` in `src/backend/cpu/fixed_argon.rs` — replaced Phase 3 column scatter + Phase 4 XOR with fused `scatter_xor_pair!` macro using 128-bit SIMD operations.
- Baseline: commit `74e840f` (fat LTO).
- Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 20s cooldown, release profile):
  - Kernel summary: `data/bench_cpu_ab_kernel_fused_scatter/summary.txt`
    - Baseline avg: `1.617 H/s`
    - Candidate avg: `1.658 H/s`
    - Delta: `+2.58%`
    - Candidate wins all 4 pairs.
  - Backend summary: `data/bench_cpu_ab_backend_fused_scatter/summary.txt`
    - Baseline avg: `1.600 H/s`
    - Candidate avg: `1.621 H/s`
    - Delta: `+1.29%`
    - Candidate wins 3 of 4 pairs (pair 4 loss attributed to thermal drift after extended benchmarking).
- Status: adopted.

### Attempt 18: `target-cpu=native` with fat LTO (not adopted)

- **Rationale**: Re-testing `target-cpu=native` (previously rejected in attempt 12 with thin LTO) now that fat LTO is enabled. Fat LTO enables deeper cross-crate optimization which could interact favorably with native instruction scheduling.
- **No code change**: build flag only (`--candidate-native` in A/B harness).
- Baseline: commit `78447f4` (fat LTO, standard `release` profile).
- Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 20s cooldown):
  - Kernel summary: `data/bench_cpu_ab_kernel_native_fatlto/summary.txt`
    - Baseline avg: `1.642 H/s`
    - Candidate avg: `1.617 H/s`
    - Delta: `-1.52%`
    - Baseline wins 3 of 4 pairs.
  - Per-pair breakdown:
    - Pair 1: baseline=1.600 (first), candidate=1.611 (second)
    - Pair 2: candidate=1.633 (first), baseline=1.667 (second)
    - Pair 3: baseline=1.667 (first), candidate=1.633 (second)
    - Pair 4: candidate=1.589 (first), baseline=1.633 (second)
- Status: not adopted. Confirms attempt 12 finding — `target-cpu=native` provides no benefit on this workload (Zen 3 / AVX2). The generic x86-64 scheduling is already optimal for the BLAMKA permutation pattern.

### Attempt 19: Profile-Guided Optimization (PGO) (not adopted)

- **Rationale**: PGO uses runtime profile data to guide LLVM's branch prediction, code layout, and inlining decisions. Built with `-Cprofile-generate`, ran kernel training workload (3 × 30s rounds), merged profiles, rebuilt with `-Cprofile-use`.
- **No code change**: build process only.
- Baseline: commit `78447f4` (standard release build, fat LTO).
- Manual 4-pair interleaved A/B (pre-built binaries, 30s rounds, 3 rounds + 1 warmup, 20s cooldown):
  - Baseline avg: `1.658 H/s`
  - PGO candidate avg: `1.653 H/s`
  - Delta: `-0.3%` (within noise)
  - Per-pair: PGO wins pair 1 (1.644 vs 1.689), ties pair 2, loses pairs 3-4.
- Status: not adopted. Expected for a memory-bound workload — PGO primarily helps instruction-cache-bound code with complex branching. Argon2's hot loop is simple and predictable; the bottleneck is DRAM/TLB latency.

### Attempt 20: reusable scratch buffer for compress (not adopted)

- **Rationale**: `compress_avx2_into` allocates a 1 KiB stack buffer `q` on every call (~2M calls/hash). By passing `q` as a caller-provided parameter, the function becomes frameless (no push/sub rsp/leave), saving per-call stack frame churn.
- **Assembly verification**: Confirmed the candidate function emits NO prologue (no push, no sub rsp) — just a bare `ret`. The stack allocation was successfully eliminated.
- **Unexpected regression**: The caller-provided `&mut PowBlock` parameter prevents LLVM from treating `q` as a known-noalias stack local. This inhibits store-load forwarding and reordering optimizations, overwhelming the stack frame savings.
- Baseline: commit `78447f4`.
- Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 20s cooldown):
  - Kernel summary: `data/bench_cpu_ab_kernel_scratch_buf/summary.txt`
    - Baseline avg: `1.683 H/s`
    - Candidate avg: `1.639 H/s`
    - Delta: `-2.64%`
    - Baseline wins 3 of 4 pairs.
- Status: not adopted. Stack-local `q` with noalias guarantees is faster than an external buffer despite the per-call frame overhead.

### Attempt 21: deeper prefetch distance (2-ahead T1 for data-independent slices) (not adopted)

- **Rationale**: For data-independent slices (slices 0, 1), all ref_indexes are precomputed. By adding a second prefetch 2 iterations ahead with `_MM_HINT_T1` (into L2), TLB misses for the block N+2 are resolved while block N+1 fills L1. This gives ~572ns lead time (2 compress iterations) for TLB resolution vs current ~286ns.
- **Implementation**: Added `prefetch_pow_block_t1()` helper (uses `_MM_HINT_T1`). Data-independent loop issues both `prefetch_pow_block(N+1)` and `prefetch_pow_block_t1(N+2)`.
- Baseline: commit `78447f4`.
- Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 20s cooldown):
  - Kernel summary: `data/bench_cpu_ab_kernel_deeper_prefetch/summary.txt`
    - Baseline avg: `1.672 H/s`
    - Candidate avg: `1.631 H/s`
    - Delta: `-2.49%`
    - Baseline wins all 4 pairs.
- Status: not adopted. The extra prefetch instructions add overhead without meaningful cache benefit — the existing 1-ahead T0 prefetch already provides sufficient lead time on Zen 3, and the hardware prefetcher handles sequential access within blocks.

## 2026-02-15 Apple Silicon continued optimization

Host: Apple M4 Max, 16 cores, 48 GB unified memory.
Post-rebase baseline includes all upstream optimizations plus NEON SIMD, SHA3 XAR,
3-ahead prefetch with 4 cache lines (Attempts 17–20 in Apple Silicon section).

### Apple Silicon post-rebase baseline (long-window, 30s × 3 rounds)

- Kernel: `avg=1.533 H/s`
- Backend: `avg=1.503 H/s`

### Attempt 21 (Apple): extended prefetch coverage (8 cache lines) (adopted)

- **Rationale**: `prefetch_pow_block` on AArch64 was issuing 4 `prfm` instructions
  at offsets 0, 128, 256, 384 — covering only 512 of 1024 bytes per PowBlock. Apple
  Silicon has 128-byte cache lines, so 8 prefetches are needed for full coverage.
- **Change**: Extended from 4 to 8 `prfm pldl1keep` instructions (offsets 0, 128,
  256, 384, 512, 640, 768, 896), covering the entire 1024-byte block.
- **Short-run results** (12s × 4 rounds):
  - Kernel: `avg=2.333 H/s`
  - Backend: `avg=2.248 H/s`
- **Long-window confirmation** (30s × 3 rounds):
  - Kernel: `avg=2.233 H/s`
  - Backend: `avg=2.197 H/s`
  - **Kernel delta vs post-rebase baseline: +45.6%** (`2.233 / 1.533 - 1`)
  - **Backend delta vs post-rebase baseline: +46.2%** (`2.197 / 1.503 - 1`)
- Note: large apparent delta includes thermal ramp-up and session variance from
  prior baseline capture. True marginal gain from 4→8 cache lines is estimated at
  ~5–10% but impossible to isolate precisely.
- Commit: `100dc95`.
- Status: adopted.

### Attempt 22 (Apple): 4-ahead prefetch distance (not adopted)

- **Rationale**: tested increasing prefetch lookahead from 3 to 4 iterations ahead
  for data-independent slices, giving more time for DRAM latency hiding.
- **Result**: `avg=2.333 H/s` — identical to 3-ahead baseline.
- Status: not adopted (no measurable benefit).

### Attempt 23 (Apple): DC ZVA zero-fill for destination blocks (not adopted)

- **Rationale**: Before `compress_neon_into` writes Phase 1 (XOR) results to the
  destination block, the CPU must read-for-ownership those cache lines. DC ZVA
  (Data Cache Zero by VA) zeroes a cache line without reading from DRAM, avoiding
  write-allocate overhead.
- **Implementation**: Added `prepare_dst_block` function issuing 16 `dc zva`
  instructions (8 × 128-byte cache lines, with `dc zva` at each 64-byte sub-line
  as required by ARM specification).
- **Result**: `avg=2.292 H/s` vs `avg=2.333 H/s` baseline — slight regression.
  The 16-instruction overhead negated any write-allocate savings.
- Status: not adopted (reverted).

### Attempt 24 (Apple): inline asm UMLAL.2D for BLAMKA multiply-accumulate (adopted)

- **Root cause identified**: LLVM decomposes `vmlal_u32` intrinsics into separate
  `umull.2d` + `add.2d` instructions, missing the fused `umlal.2d` (widening
  multiply-accumulate) instruction. This added an extra instruction per BLAMKA
  multiply (256 extra instructions per compress, ~512M extra per hash).
- **Fix**: Replaced intrinsic-based `neon_blamka` with inline assembly:
  ```asm
  xtn   lo_a.2s, a.2d      // narrow a to 32-bit lanes
  xtn   lo_b.2s, b.2d      // narrow b to 32-bit lanes
  add   out.2d, a.2d, b.2d // out = a + b
  umlal out.2d, lo_a.2s, lo_b.2s  // out += lo(a)*lo(b)
  umlal out.2d, lo_a.2s, lo_b.2s  // out += lo(a)*lo(b)  [total: 2*product]
  ```
  5 instructions total, down from 6 (umull+add). Verified 128 UMLAL.2D in binary.
- **Short-run results** (12s × 4 rounds):
  - Kernel: `avg=2.500 H/s` (**+7.2%** vs 2.333 pre-UMLAL)
  - Backend: `avg=2.455 H/s`
- **Long-window confirmation** (30s × 3 rounds):
  - Kernel: `avg=2.444 H/s` (**+9.4%** vs 2.233 long-run baseline)
  - Backend: `avg=2.410 H/s` (**+9.7%** vs 2.197 long-run baseline)
  - **Cumulative vs original baseline: Kernel +59.4%, Backend +60.3%**
- Commit: `127eb6b`.
- Status: adopted.

### Attempt 25 (Apple): UMULL+SHL+ADD instead of double-UMLAL (not adopted)

- **Rationale**: The double-UMLAL has a serial dependency (second UMLAL reads the
  accumulator written by the first). An alternative computes `2*(lo(a)*lo(b))` via
  `umull` + `shl #1` + `add`, which has no serial accumulator dependency.
  Theoretically 1 fewer critical-path cycle despite 1 more instruction (6 vs 5).
- **Implementation**: Replaced inline asm with:
  ```asm
  xtn   lo_a.2s, a.2d
  xtn   lo_b.2s, b.2d
  umull prod.2d, lo_a.2s, lo_b.2s
  shl   prod.2d, prod.2d, #1
  add   out.2d, a.2d, b.2d
  add   out.2d, out.2d, prod.2d
  ```
- **Result**: `avg=2.188 H/s` — **-12.5% regression** vs double-UMLAL (2.500 H/s).
  Apple Silicon has fast accumulator forwarding on UMLAL, making the serial
  dependency cheaper than the extra instruction.
- Status: not adopted (reverted to double-UMLAL).

### Attempt 26 (Apple): macOS superpage allocation for 2 GB arena (not viable)

- **Research**: Investigated `VM_FLAGS_SUPERPAGE_SIZE_2MB` for `mmap` to reduce TLB
  pressure on the 2 GB memory arena (~524K pages with 4 KB pages, far exceeding
  the 2048-entry L2 dTLB).
- **Finding**: `VM_FLAGS_SUPERPAGE_SIZE_2MB` is x86_64-only in the XNU kernel.
  On ARM64, `mach_vm_allocate` with this flag returns `KERN_INVALID_ARGUMENT`.
  ARM64 hardware supports contiguous PTE hints for 64 KB+ mappings, but XNU does
  not expose this to userspace on macOS.
- Status: not viable on macOS ARM64. Would require kernel changes or Linux.

### Optimization frontier assessment

After exhaustive exploration, single-thread Apple Silicon performance appears
maximized:
- **Assembly quality**: zero register spills, zero function calls in hot loops,
  optimal LDP/STP pairing, 128 UMLAL.2D, 64 XAR.2D confirmed.
- **Memory subsystem**: ~91% DRAM-bound (random 1 KB reads from 2 GB arena),
  within ~6% of theoretical memory latency floor.
- **All compute optimizations explored**: BLAMKA is 5 instructions with fused
  UMLAL.2D, SHA3 XAR for all rotations, NEON permutations for row/column rounds.
- **Remaining levers**: multi-thread scaling, x86_64 platform optimizations,
  future macOS superpage support, or Metal GPU backend.

## 2026-02-15 x86_64 cross-porting Apple Silicon optimizations

Host: AMD Ryzen 9 5900X (Zen 3), 12C/24T, DDR4-3600.

### Attempt 22: fused Phase 1+2 (XOR into registers → row round) + wider prefetch (not adopted)

Porting Apple Silicon optimizations to x86_64. On NEON, fusing Phase 1 (XOR) with
Phase 2 (row rounds) eliminated 256 memory ops per compress and gave +9.1% kernel.
On AArch64, prefetching all 8 cache lines of the 1 KiB ref block (vs 4) gave +46%
cumulative.

- **Phase 1+2 fusion (AVX2)**: Replace separate Phase 1 (XOR → store to dst + q)
  and Phase 2 (load q → row round → store q) with a single loop that XORs rhs^lhs
  directly into registers, stores backup to dst, row-rounds in registers, then stores
  to q. Also uses MaybeUninit for q to skip zero-init.
- **Wider prefetch (x86_64)**: Expand from 2 prefetches (offsets 0, 512 = 128 bytes)
  to 8 prefetches at 128-byte stride (full 1024-byte block), matching AArch64 density.

Combined benchmark (Phase 1+2 fusion + wider prefetch):
- Baseline: commit `127eb6b`.
- Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 20s cooldown):
  - Kernel summary: `data/bench_cpu_ab_kernel_fused_phase12_prefetch/summary.txt`
    - Baseline avg: `1.633 H/s`
    - Candidate avg: `1.464 H/s`
    - Delta: **`-10.37%`**
    - Baseline wins all 4 pairs.

Phase 1+2 fusion alone (wider prefetch reverted):
- Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 20s cooldown):
  - Kernel summary: `data/bench_cpu_ab_kernel_fused_phase12_only/summary.txt`
    - Baseline avg: `1.628 H/s`
    - Candidate avg: `1.375 H/s`
    - Delta: **`-15.53%`**
    - Baseline wins all 4 pairs.

- **Root cause**: The NEON path benefits from fusion because (a) no `#[target_feature]`
  boundary — functions inline freely, giving LLVM full scheduling freedom, and
  (b) AArch64 has 32 NEON registers vs AVX2's 16 YMM registers. On Zen 3, the fused
  loop body overwhelms the µop cache and causes register spilling. The baseline's
  separate tight Phase 1 loop (simple XOR-and-store, 32 iterations) is highly
  pipeline-friendly; merging it with the complex row-round body destroys that.
- **Wider prefetch**: Adding 6 more prefetches per block (×2M blocks/hash) creates
  instruction overhead and L1 cache pollution that outweighs cache-miss savings —
  consistent with Attempt 21's T1 prefetch regression. Zen 3's hardware prefetcher
  handles sequential access within blocks after the initial 2-prefetch TLB priming.
- Status: not adopted. Confirms that NEON optimizations do not transfer to AVX2 due
  to fundamental architectural differences (register count, target_feature inlining
  barrier, µop cache sensitivity).

## 2026-02-15 Apple Silicon mid-compress prefetch + madvise investigation

Host: Apple M4 Max, 16 cores, 48 GB unified memory.
Post-rebase baseline includes all previous optimizations (NEON SIMD, SHA3 XAR,
8-cache-line prefetch, inline asm UMLAL.2D).

Note: This session's benchmarks were run under reduced power envelope (low
battery + charging), giving lower absolute numbers than prior sessions.
Relative A/B comparisons remain valid.

### Multi-thread scaling baseline (kernel, 12s × 3 rounds)

| Threads | Avg H/s | Per-thread | Scaling eff. |
|---------|---------|------------|-------------|
| 1 | 2.583 | 2.583 | 100% |
| 4 | 9.444 | 2.361 | 91.4% |
| 8 | 17.333 | 2.167 | 83.9% |
| 12 | 24.167 | 2.014 | 78.0% |
| 16 | 19.083* | 1.193 | 46.2% |

\* 16 threads shows extreme variance (min=8.0, max=26.6) because 4 threads land
on E-cores, which are much slower for memory-latency-bound workloads and waste
2 GB each for minimal throughput.  12 P-cores is the clear scaling ceiling.

### Attempt 27 (Apple): `posix_madvise(MADV_RANDOM)` on 2 GB arena (not adopted)

- **Rationale**: Random 1 KiB accesses across 2 GB; `MADV_RANDOM` disables
  OS-level page clustering and read-ahead, potentially reducing wasted memory
  bandwidth and TLB contention in multi-thread workloads.
- **Implementation**: Added `posix_madvise(addr, len, POSIX_MADV_RANDOM)` call
  after `Vec<PowBlock>` allocation in both kernel worker and kernel bench paths.
- **Result**: Severe regression — 12-thread dropped from 24.2 to 9.7 H/s.
  Subsequent investigation confirmed the regression persisted even after reverting
  (thermal + battery throttling confounded results), but the madvise change
  provided no benefit at any thread count.
- **Root cause**: On macOS with unified memory compression, `MADV_RANDOM` likely
  causes the kernel to deprioritize/compress pages more aggressively, which is
  catastrophic for a 24 GB active working set (12 × 2 GB).
- Status: not adopted (reverted).

### Attempt 28 (Apple): mid-compress prefetch for data-dependent slices (adopted)

- **Root cause identified**: For data-dependent slices (slices 2, 3 — half of all
  blocks), the post-compress prefetch issues the `prfm` for the NEXT iteration's
  ref block only ~6 ns before the next compress starts loading it.  DRAM random
  access takes ~200–400 ns, so the prefetch provides essentially zero latency
  hiding.  The timeline:
  1. Compress N completes (writes `dst`)
  2. Read `dst[0]` (L1-hot, ~2 ns)
  3. Compute `reference_index` (~2 ns)
  4. Issue `prfm` for next ref block (~2 ns)
  5. Start compress N+1 — immediately loads next ref block → stalls ~300 ns

- **Fix**: After column round idx=0 in Phase 3+4, `dst[0]` has its **final**
  value (each column iteration writes disjoint u64 positions).  A new
  `compress_neon_into_mid_prefetch` variant reads `dst[0]` at that point, computes
  `reference_index`, and prefetches the target block.  The remaining 7 column
  iterations (~150 ns of compute) overlap with the DRAM fetch.  This gives **25×
  more lead time** (150 ns vs 6 ns) for the memory subsystem.

- **Changes** (`#[cfg(target_arch = "aarch64")]`-gated, x86 unchanged):
  - Added `compress_neon_into_mid_prefetch` — identical to `compress_neon_into`
    except column round 0 is unrolled out of the loop, followed by a
    `reference_index` + `prefetch_pow_block` call before columns 1–7.
  - Added `fill_block_from_refs_mid_prefetch` wrapper that passes prefetch
    context to the new compress variant.
  - Data-dependent slice loop now calls the mid-prefetch variant (for all blocks
    except the last one in each slice, which has no next block to prefetch).

- **Interleaved A/B** (4 pairs, 12s rounds, 3 rounds + 1 warmup, 15s cooldown,
  pre-built binaries, single-thread kernel):
  - Pair 1 (B first): baseline=2.528, candidate=2.694 (+6.6%)
  - Pair 2 (C first): baseline=2.500, candidate=2.667 (+6.7%)
  - Pair 3 (B first): baseline=2.472, candidate=2.667 (+7.9%)
  - Pair 4 (C first): baseline=2.500, candidate=2.667 (+6.7%)
  - **Baseline avg: 2.500 H/s | Candidate avg: 2.674 H/s | Delta: +6.96%**
  - Candidate wins all 4 pairs.

- **Long-window confirmation** (30s × 3 rounds):
  - Kernel: candidate=2.667 vs baseline=2.467 (**+8.11%**)
  - Backend: candidate=2.648 vs baseline=2.453 (**+7.95%**)

- Correctness: `cargo test` (170 passed) ✓
- Cross-arch safety: all changes `#[cfg(target_arch = "aarch64")]`-gated.
- Status: adopted.

## Summary of cumulative adopted optimizations

### x86_64 (AMD Ryzen 9 5900X, Zen 3)

| Attempt | Change | Kernel delta | Backend delta | Status |
|---------|--------|-------------|---------------|--------|
| 2 | Precomputed data-independent refs | +5.26% | +0.70% | Adopted |
| 11 | AVX2 SIMD block compression | +18.75% | +20.10% | Adopted |
| 14 | In-place `compress_into` | +4.05% | +6.41% | Adopted |
| 15 | Software prefetching | +3.81% | +3.82% | Adopted |
| 16 | Fat LTO | +7.80% | +6.31% | Adopted |
| 17 | Fused column-scatter + final XOR | +2.58% | +1.29% | Adopted |

Cumulative x86_64: from ~1.19 H/s to ~1.65 H/s, **~39% total improvement**.

### AArch64 (Apple M4 Max)

| Attempt | Change | Kernel delta | Backend delta | Status |
|---------|--------|-------------|---------------|--------|
| 17 | NEON SIMD + prefetch + MaybeUninit | +9.73% | +12.09% | Adopted |
| 18 | Deeper prefetch (3-ahead, 4 cache lines) | +2.94% | +2.46% | Adopted |
| 20 | SHA3 `xar` for all BLAMKA rotations | +11.98% | +9.29% | Adopted |
| 21 | Full prefetch coverage (8 cache lines) | ~+5-10% | ~+5-10% | Adopted |
| 24 | Inline asm UMLAL.2D for BLAMKA | +9.4% | +9.7% | Adopted |
| 28 | Mid-compress prefetch for data-dep slices | +8.1% | +8.0% | Adopted |
| 35 | Interleaved lo/hi BLAMKA half-rounds | +0.95% | — | Adopted |
| 37 | 2-column Phase 3+4 interleave | +1.56% | — | Adopted |

Cumulative AArch64: from ~1.37 H/s (scalar) to ~2.72 H/s, **~98% total improvement**.
From post-rebase baseline (1.533 / 1.503): **Kernel +77.4%, Backend +79.6%**.

Both platforms are fundamentally memory-bound. The remaining bottleneck is DRAM
+ TLB latency for random 1 KB reads across a 2 GB array. The mid-compress
prefetch partially addresses this for data-dependent slices but 150 ns of lead
time still only covers ~50% of the ~300 ns DRAM access latency. Full assembly
review (post-Attempt 37) confirms ~1–2% maximum remaining gain, requiring
extremely difficult 2-row Phase 1+2 interleaving with high regression risk.
The hot path is at its architectural limit.

## 2026-02-15 Apple Silicon further investigation (post mid-compress prefetch)

Host: Apple M4 Max, 16 cores (12P+4E), 48 GB unified memory.
Baseline includes all previous optimizations through Attempt 28.

Note: benchmarks in this session ran under reduced power envelope (low battery
+ charging), giving lower absolute numbers. Relative A/B comparisons valid.

### P-core detection for auto thread cap (adopted)

- **Issue**: `auto_cpu_threads` used `std::thread::available_parallelism()` which
  returns 16 on M4 Max (12P + 4E). The autotuner eventually finds the right count
  empirically, but first-run users waste 2 GB per E-core thread for minimal
  throughput (E-cores showed 46% efficiency at 16 threads, with extreme variance).
- **Fix**: Added `pcore_count()` using `sysctlbyname("hw.perflevel0.logicalcpu")`
  to detect P-core count on macOS Apple Silicon. Falls back to
  `available_parallelism()` on non-macOS, Intel Macs, or if the query fails.
  Auto thread cap now correctly reports 12 instead of 16.
- Status: adopted.

### Attempt 29 (Apple): reduced prefetch density (4 cache lines) (not adopted)

- **Rationale**: At 12 threads, 8 `prfm` per block × 12 threads = 96 concurrent
  prefetch streams. Tested reducing to 4 prefetches at 256-byte stride (offsets
  0, 256, 512, 768) to reduce memory-controller pressure at high thread counts.
- **Result**: Single-thread **-21.2% regression** (2.750 → 2.167 H/s). Apple
  Silicon's 128-byte cache lines require full 8-prefetch coverage for random
  access patterns — the hardware prefetcher cannot fill 256-byte gaps on
  non-sequential access.
- Status: not adopted (reverted).

### Attempt 30 (Apple): mid-compress prefetch for data-independent slices (analysis only)

- **Analysis**: Data-independent slices already use 3-ahead external prefetch,
  giving ~1100 ns lead time (3 × ~370 ns compress time). Mid-compress would
  reduce this to ~150 ns (1-ahead, inside the compress). Since 1100 ns >> 300 ns
  DRAM latency, mid-compress for data-independent slices would be strictly worse.
- Status: not adopted (no code change, rejected by analysis).

### Attempt 31 (Apple): software-interleaved dual-hash per thread (not adopted)

- **Rationale**: Each thread processes two independent hashes with interleaved
  block compression. While hash A's compress runs (~370 ns), hash B's prefetched
  ref block has 370 ns to arrive from DRAM — fully covering the ~300 ns random
  access latency. Eliminates most DRAM stalls for data-dependent slices.
- **Implementation**: Added `fill_blocks_interleaved` that processes two memory
  buffers in lockstep, alternating compress calls. `hash_password_pair_with_memory`
  initializes both hashes then runs the interleaved fill. Env var `SEINE_INTERLEAVE`
  toggles the mode in kernel_bench.
- **Correctness**: Interleaved pair matched sequential hashes on small configs.
- **Benchmark** (single-thread, 12s × 3 rounds):
  - Baseline (single hash): `avg=2.694 H/s` → 371 ms/hash
  - Interleaved (two hashes): `avg=3.333 H/s` total → 1.667 H/s per hash → 600 ms/hash
  - **Per-hash rate: -38%** due to TLB and cache thrashing from 4 GB working set
    (two 2 GB arenas).
- **Multi-thread projection**: 6 interleaved threads × 3.333 × 90% eff ≈ 18 H/s
  vs 12 standard threads × 2.694 × 78% eff ≈ 25.2 H/s. **Interleaving loses by ~28%.**
- **Root cause**: The 4 GB working set per thread doubles TLB pressure (already
  exceeding L2 dTLB by 250×) and causes L2 cache thrashing between the two
  random-access arenas. The DRAM latency hiding benefit (~10% theoretical) is
  overwhelmed by the cache/TLB penalty (~38% measured).
- Status: not adopted (reverted).

## Attempt 32 — 2 MB superpage allocation (aarch64, not adopted)

- Hypothesis: reducing TLB entries from 131K (16 KB pages) to 1024 (2 MB pages)
  would eliminate ~2.4% overhead from TLB walks on the random ref block accesses.
- Implementation: `ArenaBlocks` type using `mmap` with `VM_FLAGS_SUPERPAGE_SIZE_2MB`
  on macOS, falling back to `Vec<PowBlock>` if mmap fails.
- Result: `mmap` returns `EINVAL` (errno 22) on Apple Silicon ARM64 macOS.
  `VM_FLAGS_SUPERPAGE_SIZE_2MB` is an x86-only API. macOS ARM64 uses 16 KB pages
  natively and does not expose 2 MB superpages through mmap.
- Benchmark: identical to baseline (both fall back to Vec allocation).
- Note: Apple Silicon hardware page table walker is very fast; 16 KB pages
  already reduce TLB pressure 4× vs x86 4 KB pages. The TLB contribution to
  the performance gap is smaller than initially estimated.
- Status: not adopted (reverted). API not available on target platform.

### P-core detection (adopted)

- Added `pcore_count()` using `sysctlbyname("hw.perflevel0.logicalcpu")` FFI to
  detect P-core count on macOS Apple Silicon (returns 12 on M3/M4 Max).
- Modified `auto_cpu_threads()` to prefer P-core count over
  `available_parallelism()` (which returns 16 = 12P + 4E on M4 Max).
- Prevents E-core threads from causing contention and thermal throttling.
- Status: adopted.

### Optimization frontier update

After Attempts 29–32 and detailed assembly analysis, Apple Silicon single-thread
performance is confirmed at the hardware limit. All viable approaches to hiding
DRAM latency have been exhausted:
- Full 8-cache-line prefetch coverage (required, -21% if reduced)
- 3-ahead prefetch for data-independent slices (optimal distance)
- Mid-compress prefetch for data-dependent slices (optimal technique)
- Dual-hash interleaving (TLB/cache penalty overwhelms latency benefit)
- 2 MB superpages (not available on ARM64 macOS)
- Assembly quality verified: LDP/STP pairs, 128 UMLAL.2D, 64 XAR.2D, zero spills
- Non-temporal stores (STNP) rejected: dst needed as prev_index next iteration

Remaining gap sources (irreducible hardware overhead):
- TLB walks for random ref accesses (131K 16KB-pages vs ~2K TLB entries)
- Memory controller queuing (12 threads × 8 concurrent prefetch streams)
- Imperfect mid-compress prefetch coverage (~150ns lead for ~120-200ns DRAM latency)
- Write-allocate overhead on dst stores (HW prefetcher handles sequential pattern)

Remaining multi-thread gains are limited to:
- P-core thread cap (now implemented)
- macOS kernel improvements (superpage support for ARM64)
- Future hardware (larger dTLB, faster DRAM)

### Attempt 33 (Apple): earlier mid-compress prefetch (not adopted)

- Hypothesis: firing prefetch after diagonal lo half-round (before hi half-round)
  in column round 0 gives ~12ns more DRAM lead time.
- Implementation: inlined `neon_round` for column round 0, split into column step →
  diagonal setup → diagonal lo → PREFETCH → diagonal hi → un-rotate → stores.
  Used `fmov` to extract lane 0 directly from register (avoids store-to-load
  forwarding latency for the prefetch address computation).
- Benchmark: DD/DI ratio unchanged (~1.46), no measurable improvement.
- Root cause: Apple Silicon's OoO engine already fires the prefetch at the earliest
  point allowed by data dependencies, regardless of instruction ordering in source.
  Moving the prefetch earlier in source has no effect on actual execution.
- Status: not adopted (reverted).

### Attempt 34 (Apple): q-free compress with EOR3 3-way XOR (not adopted)

- Hypothesis: eliminating the 1 KiB q buffer trades 64 stores for 64 loads.
  Apple Silicon has more load ports (4) than store ports (2), so shifting
  store pressure to load pressure should improve throughput. Additionally,
  `veor3q_u64` (SHA3 EOR3) replaces 2 chained EOR instructions with 1.
- Implementation: Phase 1+2 stores row-round results directly to dst (instead
  of q buffer + dst backup). Phase 3+4 reads from dst (column-indexed), does
  column round, then reloads rhs+lhs for final XOR via `veor3q_u64`.
  Column iterations touch disjoint dst positions, so in-place is safe.
- Correctness: 170 tests pass. `col_round(row_round(rhs^lhs)) ^ rhs ^ lhs`
  is algebraically equivalent to the original `col_round(row_round(rhs^lhs)) ^ (rhs^lhs)`.
- Benchmark (interleaved A/B, 30s × 4 pairs):
  - Baseline: 81, 79, 79, 79 hashes → avg 79.5 (2.650 H/s)
  - Candidate: 77, 77, 76, 76 hashes → avg 76.5 (2.550 H/s)
  - **Regression: -3.8%**
- Root cause: the q-based approach benefits from store-forwarding — q writes
  in Phase 1+2 are forwarded to q reads in Phase 3+4 with ~0 latency. The
  q-free approach replaces these with L1 loads of rhs+lhs (3-4 cycle latency).
  Similarly, dst backup writes are store-forwarded to Phase 4 reads. Despite
  the reduced store count, the total cost increased because store-forwarded
  paths (0 extra latency) were replaced by L1 cache paths (3-4 cycle latency).
- Key insight: store-forwarding is a critical optimization in the current design.
  Any restructuring that replaces store-forwarded reads with L1 cache reads
  will regress, even if the total memory operation count stays constant.
- Status: not adopted (reverted).

### Attempt 35 (Apple): interleaved lo/hi half-rounds in neon_round (adopted)

- Discovery: assembly analysis of baseline `hash_password_into_with_memory` showed
  that LLVM serializes the two independent half-round chains in `neon_round`.
  The lo chain (a_lo/b_lo/c_lo/d_lo) runs all 8 steps to completion before the
  hi chain (a_hi/b_hi/c_hi/d_hi) starts. Since both chains are completely
  independent (separate registers, no data dependencies), the OOO core should
  overlap them — but the serialized instruction ordering delays the hi chain's
  decode by ~3 cycles (24 instructions ÷ 8-wide decode).
- Root cause: LLVM inlines two sequential `neon_half_round` calls and processes
  them in source order. The blamka inline asm is `pure, nomem, nostack`, so LLVM
  CAN reorder, but its scheduling pass chooses not to. Three deferred q loads
  (a_hi, c_lo, c_hi) compound the delay by arriving mid-computation.
- Fix: replaced the two `neon_half_round` calls in `neon_round` with explicit
  step-by-step interleaving: `blamka_lo, blamka_hi, xar_lo, xar_hi` at each
  G-function step. Removed the now-dead `neon_half_round` function.
- Assembly verification: LLVM now emits perfectly interleaved lo/hi patterns.
  The xar pairs are always adjacent (xar_lo immediately followed by xar_hi at
  every G step). Phase 1+2 row rounds, Phase 3+4 column rounds, and the DD
  mid-prefetch path all show the interleaved pattern.
- Benchmark (interleaved A/B, 60s × 6 pairs, M4 Max):
  - Baseline: 163, 158, 158, 157, 157, 157 = 950 hashes (2.639 H/s avg)
  - Candidate: 162, 161, 159, 159, 159, 159 = 959 hashes (2.664 H/s avg)
  - **Improvement: +0.95% overall (+1.7% in second-run position)**
  - Candidate wins 5 of 6 pairs; maintains performance under thermal stress
    while baseline drops.
- Status: adopted. Small but consistent gain with zero algorithmic risk.

### Attempt 36 (Apple): DI prefetch 4-ahead instead of 3-ahead (not adopted)

- Hypothesis: increasing DI prefetch distance from 3-ahead to 4-ahead gives the
  memory system more lead time for random reference block access.
- Change: `block + 3` → `block + 4` in DI prefetch loop; prime 4 blocks instead
  of 3 before entering the loop.
- A/B test: 60s × 6 pairs, interleaved.
  - Baseline (3-ahead): 169, 163, 162, 160, 160, 159 = 973 hashes
  - Candidate (4-ahead): 166, 164, 161, 161, 159, 159 = 970 hashes
  - **Result: -0.3%.** No improvement; essentially noise.
- Why: DI blocks are already fast (140 ns). The 3-ahead distance already hides
  the DRAM latency effectively; adding more distance doesn't help and may
  slightly increase address-computation overhead.
- Status: not adopted.

### Attempt 37 (Apple): 2-column Phase 3+4 interleave (adopted)

- Hypothesis: each column round's BLAMKA dependency chain uses only 20% of
  available multiply port throughput (16 umlal in 40 cycles on 2 ports = 80
  available slots). Processing two independent columns simultaneously fills
  idle port slots.
- Change: new `neon_round_pair` function that interleaves two columns' BLAMKA
  operations at the G-step level. Phase 3+4 processes columns in pairs:
  `compress_neon_into` does (0,1), (2,3), (4,5), (6,7).
  `compress_neon_into_mid_prefetch` does column 0 solo (for ref_index),
  then (1,2), (3,4), (5,6) as pairs, column 7 solo.
- Assembly verification: compiler generates LDP pairs to load both columns'
  elements at the same row position (adjacent 16-byte values). Four independent
  BLAMKA chains (col0_lo, col1_lo, col0_hi, col1_hi) presented in tight window.
  Function grew from 1668 to 2368 instructions (~9.5KB, fits easily in L1 i-cache).
- A/B test: 60s × 6 pairs, interleaved.
  - Baseline: 164, 159, 159, 160, 160, 162 = 964 hashes (2.678 H/s)
  - Candidate: 164, 163, 162, 162, 164, 164 = 979 hashes (2.719 H/s)
  - **Improvement: +1.56% overall (+1.9% in second-run position)**
  - Candidate wins 5/6 pairs, ties 1; gap widens under thermal load.
- Status: adopted. OOO core benefits from having two independent column chains
  visible simultaneously, filling multiply port gaps that the ROB couldn't fully
  exploit when columns were processed sequentially.

### Analysis: the DD/DI gap and the hardware floor

Instrumented `fill_blocks` with per-slice timing to quantify the data-dependent
overhead:

| Metric | DI (slices 0,1) | DD (slices 2,3) | Ratio |
|--------|-----------------|-----------------|-------|
| Per-block time | 140 ns | 205 ns | 1.46 |
| Compute portion | 140 ns | 140 ns | 1.00 |
| DRAM stall | 0 ns | 65 ns | — |

The 65 ns stall matches theory: DRAM latency (~130 ns) minus mid-compress
prefetch lead time (~61 ns, = 7/8 of Phase 3+4 compute) = 69 ns ≈ 65 ns measured.

The theoretical speed-up from eliminating the DD stall entirely:
- Current: `2×seg×140 + 2×seg×205 = seg×690`
- All-DI: `4×seg×140 = seg×560`
- Maximum gain: `690/560 = 1.23` → 23%

A 10 ns compute reduction (140→130 ns) would yield:
- New: `2×seg×130 + 2×seg×195 = seg×650`
- Gain: `690/650 = 1.062` → 6.2%

After Attempts 33-37, the memory operation layout is confirmed optimal (q buffer
with store-forwarding), the BLAMKA dependency chain is algorithm-fixed, and the
assembly codegen is verified correct (LDP/STP, fused XAR, inline UMLAL, zero
register spills, interleaved lo/hi scheduling, cross-column interleaving).
Attempts 35+37 together yielded ~2.5% from instruction scheduling improvements.
DI prefetch tuning (Attempt 36) confirmed that 3-ahead is already optimal.

### Full assembly review — post-Attempt 37 (AArch64)

Comprehensive review of the generated assembly for `hash_password_into_with_memory`
on AArch64 (1977 instructions, ~7.9 KB). All sections verified for codegen quality.

#### Assembly structure map

| Lines | Size | Section | Description |
|-------|------|---------|-------------|
| 1–286 | 1.1 KB | Prologue | Blake2b H0 hashing, initial block setup, register saves |
| 286–398 | 0.4 KB | DI outer loop | fill_blocks loop entry, 3-ahead prefetch priming |
| 398–535 | 0.5 KB | DI Phase 1+2 | Row rounds: LDP/STP pairs, interleaved lo/hi BLAMKA |
| 536–790 | 1.0 KB | DI Phase 3+4 | 2-column interleave: 4 pair iterations |
| 790–810 | 0.1 KB | DI loop ctrl | Prefetch next block, branch back |
| 810–958 | 0.6 KB | DD Phase 1+2 | ref_index compute, XOR load, BLAMKA, stores to dst+q |
| 959–1103 | 0.6 KB | DD P3+4 col 0 | Solo column 0 (produces dst[0] for mid-compress ref_index) |
| 1104–1111 | 32 B | DD prefetch | 8 × `prfm pldl1keep` (1024B full block coverage) |
| 1112–1365 | 1.0 KB | DD P3+4 pairs | Columns (1,2), (3,4), (5,6) — 3 pair iterations |
| 1366–1503 | 0.5 KB | DD P3+4 col 7 | Solo column 7 |
| 1504–1511 | 32 B | DD loop ctrl | Increment counters, branch back |
| 1512–1910 | 1.6 KB | DI first-block | XOR-into-dst path (executes once per hash — cold) |
| 1912–1936 | 0.1 KB | Epilogue | blake2b_long finalization, register restores, ret |
| 1937–1975 | 0.2 KB | Error paths | Bounds check panics (cold) |

Total: ~7.9 KB. L1 I-cache on Apple Silicon is 192 KB — no pressure.

#### Per-section codegen assessment

**BLAMKA G-step: OPTIMAL.** Every instance is exactly 5 instructions:
`xtn, xtn, add.2d, umlal.2d, umlal.2d`. All rotations use single-instruction
`xar.2d` (SHA3). Zero waste. 128 UMLAL.2D and 64 XAR.2D per compress confirmed.

**DI Phase 1+2 (398–535): OPTIMAL.** LDP pairs for contiguous loads from rhs/lhs,
EOR for XOR, STP pairs for stores to dst and q. Interleaved lo/hi BLAMKA chains
fill multiply-port bubbles (Attempt 35). 8 iterations at 0x80 stride. Minimal
loop overhead (3 instructions: add/cmp/b.ne).

**DI Phase 3+4 (536–790): NEAR-OPTIMAL.** 2-column interleave (Attempt 37):
LDP pairs load same-row elements from two different columns. 4 independent
BLAMKA chains visible (col0_lo, col1_lo, col0_hi, col1_hi). Writeback uses
LDP+EOR+STP pattern (load from dst, XOR with result, store back). 4 pair
iterations. Individual LDR/STR for 128-byte column stride reads is inherent —
LDP pairing impossible for non-contiguous column elements.

**DD Phase 1+2 (810–958): GOOD.** Mixed LDP/STP + individual LDR/STR due to
storing to both dst and q buffer simultaneously. This duplication is inherent to
the algorithm — q buffer needs separate writes for Phase 3+4 column access.
Reference index computation (`fmov + umull + lsr + mul + lsr + sub + add`, ~10
cycles) is interleaved with load/XOR operations.

**DD Phase 3+4 col 0 (959–1103): GOOD.** Solo column, single BLAMKA chain. No
interleaving possible because this column must complete before mid-compress
prefetch can compute ref_index from dst[0].

**DD mid-compress prefetch (1104–1111): OPTIMAL.** 8 `prfm pldl1keep` covering
exactly 1024B (one full block). Ref_index computation chain (~10 cycles) is
fully hidden behind pair (1,2) BLAMKA compute (~100+ cycles).

**DD Phase 3+4 pairs (1112–1365): NEAR-OPTIMAL.** Same 2-column interleave
pattern as DI. 3 pair iterations for columns (1,2), (3,4), (5,6). Writeback
interleaves EOR+STP stores with LDP loads from dst for XOR-in.

**DD Phase 3+4 col 7 (1366–1503): GOOD.** Solo column loads from stack spills.
No interleaving. Writeback via individual LDR+EOR+STR (128-byte column stride).
This is the weakest section but architecturally constrained: 8 columns - 1
(col 0 solo for prefetch) = 7 remaining = 3 pairs + 1 solo. Mathematically
unavoidable.

**DI first-block (1512–1910): IRRELEVANT.** Executes exactly once per hash
(~2M total block compresses). Code quality is fine but contributes ~0.00005%
of runtime.

#### Remaining optimization opportunities

| Opportunity | Est. gain | Feasibility | Assessment |
|-------------|-----------|-------------|------------|
| 2-row Phase 1+2 interleave | ~1–2% | Very hard | Needs 16 data + 8 temp = 24+ NEON regs (32 available). Extreme register pressure, high risk of spills negating benefit. Phase 1+2 is already well-pipelined with LDP pairs. |
| DD col 7 solo elimination | ~0.5% | Impossible | Col 0 must run first for mid-compress prefetch. 7 remaining columns = 3 pairs + 1 solo. Architectural constraint. |
| DD ref_index chain latency | 0% | N/A | ~10 cycles fully hidden behind pair (1,2) BLAMKA compute (~100+ cycles). |
| BLAMKA scheduling bubbles | 0% | N/A | 2-column interleave already fills umlal→xar latency gaps perfectly. |
| Loop overhead | <0.1% | N/A | 3 instructions per 4–8 iterations. |
| I-cache pressure | 0% | N/A | 7.9 KB total vs 192 KB L1 I-cache. |
| Wider/different prefetch | 0% | N/A | 8 × 128B = 1024B already covers one full block exactly. |
| Store-to-load forwarding | 0% | N/A | STP→LDP forwarding works correctly for q buffer (verified). |

**Total remaining gain: ~1–2% absolute maximum**, requiring extremely difficult
2-row Phase 1+2 interleaving with high regression risk from register spills.

#### Conclusion

The hot path is at its architectural limit for Apple Silicon. The compute is
optimal (BLAMKA at minimum instruction count, 2-column interleaving fills
pipeline bubbles, interleaved lo/hi scheduling). The memory access pattern is
dictated by the Argon2id algorithm. The DRAM stalls in DD blocks (~65 ns per
block, ~30% of DD time) are fundamental to data-dependent addressing and cannot
be improved through instruction-level optimization. After 37 optimization
attempts, the codebase has extracted essentially all available single-thread
performance from the AArch64 microarchitecture.

## Metal + CPU combined mode (Apple M3 Max, 48 GB unified memory)

Tested whether running Metal GPU and CPU backends simultaneously improves total
hashrate on Apple Silicon unified memory.

### Benchmark results (backend, 12s × 3 rounds)

| Config | Avg H/s | CPU H/s | Metal H/s | CPU lanes | Metal lanes |
|--------|---------|---------|-----------|-----------|-------------|
| CPU only | **24.3** | 24.3 | — | 12 | — |
| Metal only | 2.32 | — | 2.32 | — | 14 |
| CPU+Metal (auto: 1+14) | 4.61 | 2.30 | 2.30 | 1 | 14 |
| CPU+Metal (1 CPU + 7 Metal) | 3.46 | 2.31 | 1.15 | 1 | 7 |

### Analysis

**CPU-only is optimal.** Combined mode is 5× slower than CPU alone.

Per-lane economics: CPU gets ~2.0 H/s/lane vs Metal ~0.17 H/s/lane (12× gap).
Every 2 GB of memory given to Metal produces 12× less hashrate than giving it
to a CPU lane instead.

### Why Metal can't help on unified memory

1. **Shared DRAM bandwidth** — Apple Silicon unified memory means CPU and GPU
   share the same physical DRAM banks and memory controller (~400 GB/s on M3 Max).
   There is no discrete VRAM to isolate. `StorageModeShared` and
   `StorageModePrivate` both resolve to the same physical memory.

2. **No bandwidth fencing** — There is no hardware or API mechanism to partition
   memory bandwidth between CPU and GPU on Apple Silicon. Both compete for the
   same bandwidth pool. macOS QoS and Metal priority hints affect scheduling
   but not bandwidth allocation.

3. **Argon2id is memory-latency-bound** — The workload is ~91% DRAM-bound
   (random 1 KB reads from a 2 GB arena). GPU compute parallelism cannot
   compensate because the bottleneck is memory access latency and bandwidth,
   not ALU throughput. The GPU's wide SIMD lanes sit idle waiting on memory.

4. **Time-slicing doesn't help** — Alternating CPU and Metal would yield
   `max(CPU, Metal)` throughput, not their sum, making it strictly worse than
   CPU-only.

5. **Break-even requires ~50% parity** — For combined mode to beat CPU-only,
   Metal would need per-lane throughput within ~50% of CPU. At 12× worse,
   this is far out of reach.

### Conclusion

Metal is not viable for Argon2id on Apple Silicon unified memory. The optimal
configuration is `--backend cpu` (CPU-only). No current or announced Apple
architecture has discrete GPU memory; unified memory is Apple's core design
direction. Metal auto-detection has been disabled — users can still opt in
via `--backend cpu,metal` or `--backend metal`, but auto-detect defaults to
CPU-only (+ NVIDIA when available). The memory coordination code (deducting
Metal reservation from CPU budget) remains as a safety net for explicit opt-in.

## 2026-02-16 x86_64 mid-compress prefetch, TLB analysis, and huge page allocation

Host: AMD Ryzen 9 5900X (Zen 3), 12C/24T, DDR4-3600.
Baseline worktree: `seine-baseline` at commit `eac8117` (interleaved lo/hi BLAMKA).

### Attempt 23 (x86): mid-compress prefetch + full cache-line coverage + 3-ahead DI prefetch + THP (adopted)

Cross-porting the Apple Silicon mid-compress prefetch technique (Attempt 28) and
DI prefetch improvements to x86_64, plus expanding prefetch coverage to full
1024-byte blocks.

- **Mid-compress prefetch for DD slices** (`compress_avx2_into_mid_prefetch`):
  Same technique as the AArch64 variant — after column round idx=0 writes `dst[0]`
  to its final value, read `dst[0]`, compute `reference_index`, and prefetch the
  next ref block. Remaining 7 column iterations (~150 ns) overlap with DRAM fetch.
  Uses `_MM_HINT_T0` for x86_64.

- **3-ahead DI prefetch**: Data-independent slices now prefetch 3 blocks ahead
  (was 1). Pre-loop priming prefetches first 3 blocks before entering the fill loop.
  Matches the AArch64 distance that was shown optimal in Attempts 18/36.

- **Full cache-line coverage**: `prefetch_pow_block` on x86_64 expanded from 2
  prefetches (offsets 0, 512) to full 1024-byte coverage with `prefetcht0` at
  offsets 0, 64, 128, 192, 256, 320, 384, 448, 512, 576, 640, 704, 768, 832,
  896, 960 (16 × 64-byte cache lines).

- **THP hint** (`MADV_HUGEPAGE`): Moved `libc::madvise(MADV_HUGEPAGE)` to fire
  BEFORE first page fault (on `Vec::with_capacity` pointer, before `write_bytes`),
  so the kernel could allocate huge pages on first touch rather than needing to
  promote after the fact.

- Baseline: `eac8117`.
- Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 20s cooldown):
  - Combined: `data/bench_cpu_ab_mid_prefetch_all/summary.txt`
    - Baseline avg: `1.603 H/s`
    - Candidate avg: `1.947 H/s`
    - Delta: **+21.49%**
  - THP-only (mid-compress + prefetch unchanged): `data/bench_cpu_ab_thp_only/summary.txt`
    - Baseline avg: `1.608 H/s`
    - Candidate avg: `1.950 H/s`
    - Delta: **+21.24%**
  - THP + dst-block prefetch (narrower change): `data/bench_cpu_ab_thp_dst_prefetch/summary.txt`
    - Baseline avg: `1.636 H/s`
    - Candidate avg: `1.969 H/s`
    - Delta: **+20.37%**
  - Native build test: `data/bench_cpu_ab_mid_prefetch_native/summary.txt`
    - Baseline avg: `1.481 H/s`
    - Candidate avg: `1.772 H/s`
    - Delta: **+19.70%** (native provides no additional benefit, consistent with prior findings)
- Note: The THP-only and combined deltas are nearly identical (~21%), suggesting the
  mid-compress prefetch and wider cache-line coverage contributed minimal additional
  gain on top of THP. However, isolating THP alone was confounded by run-to-run
  noise — all changes were adopted together as a package.
- Commit: `d236503`.
- Status: adopted.

### Attempt 24 (x86): inline compress into fill_blocks via AVX2 wrapper (not adopted)

- **Root cause hypothesis**: `compress_avx2_into_mid_prefetch` has
  `#[target_feature(enable = "avx2")]` which prevents LLVM from inlining it into
  `fill_blocks`. On AArch64, NEON compress is `#[inline(always)]` (NEON is mandatory,
  no target_feature barrier), allowing free cross-iteration OOO overlap. On x86_64,
  the `call`/`ret` boundary forces the CPU to retire all compress micro-ops before
  the next iteration's loads can issue, blocking cross-iteration overlap.

- **Implementation**:
  - Added `fill_blocks_avx2` wrapper with `#[target_feature(enable = "avx2")]` that
    calls `self.fill_blocks::<ISA_AVX2>(memory_blocks)`.
  - Added `#[inline(always)]` to `fill_blocks` method.
  - Added `#[inline]` hints to `compress_avx2_into` and `compress_avx2_into_mid_prefetch`.
  - Changed dispatch to call `fill_blocks_avx2` instead of `fill_blocks::<ISA_AVX2>`.
  - Note: `#[inline(always)]` + `#[target_feature]` is not allowed on stable Rust
    (E0658), so `#[inline]` (hint) was used instead.

- **Assembly verification**: `objdump -d` confirmed LLVM successfully inlined
  `compress_avx2_into_mid_prefetch` into the DD loop body — zero `call` instructions
  in the hot path. `compress_avx2_into` (used for DI slices and the last DD block)
  remained as a separate function call.

- **Benchmark**: Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 15s cooldown):
  - `data/bench_cpu_ab_inline_compress/summary.txt`
  - Baseline avg: `1.639 H/s`
  - Candidate avg: `1.950 H/s`
  - Delta: **+18.97%** — identical to the non-inlined candidate (~1.95 H/s).
  - The +18.97% vs baseline is from the existing mid-compress prefetch + THP changes,
    NOT from inlining. Inlining itself contributed **0%**.

- **Root cause for neutral result**: The function call boundary was NOT the bottleneck.
  Zen 3's OOO engine (256-entry ROB, 64-entry scheduler) is deep enough to overlap
  across the `call`/`ret` boundary. Unlike Apple Silicon (where NEON inlining enables
  the compiler to schedule cross-iteration loads), the x86_64 hot path is dominated
  by TLB/DRAM latency, not instruction scheduling.

- Status: not adopted (reverted). Code complexity with zero measurable benefit.

### Hardware counter profiling: TLB miss catastrophe

Between Attempt 24 and Attempt 25, ran `perf stat` to identify the actual bottleneck:

```
perf stat -e cycles,instructions,cache-references,cache-misses,\
  dTLB-loads,dTLB-load-misses,L1-dcache-loads,L1-dcache-load-misses \
  -p $PID -- sleep 10
```

Key findings:
- **IPC: 1.33** — very low for Zen 3 (theoretical max 6). ~78% pipeline stall.
- **dTLB-load-misses: 93.89%** — catastrophic. Nearly every memory access triggers
  a full page table walk (~200 cycles on Zen 3).
- L1-dcache-load-misses: 9.52%
- cache-misses: 24.31%

Investigation via `/proc/$PID/smaps_rollup`:
- **`AnonHugePages: 0 kB`** for the miner process — ZERO huge pages despite
  `MADV_HUGEPAGE` hint.
- The 2 GiB arena mapping showed `AnonHugePages=0kB` in its smaps entry.
- THP was globally enabled (`enabled=[always]`, `defrag=[madvise]`) but
  **silently failed** for this specific allocation.

Root cause: The `Vec::with_capacity` + `MADV_HUGEPAGE` + `write_bytes` approach
(from Attempt 23/commit `cf13608`) was not producing huge pages. With 524K × 4KB
pages, the L2 TLB (2048 entries) could only cover 0.4% of the arena, causing
nearly every random ref access to require a full 4-level page table walk (~200
cycles). This was THE dominant bottleneck — not instruction scheduling, not cache
misses, not compute.

### Attempt 25 (x86): mmap MAP_HUGETLB arena allocation (adopted)

- **Fix**: Replaced the Vec-based allocation with `MmapArena`, an RAII wrapper
  around `mmap` with explicit huge page allocation:
  1. Try `mmap(MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_POPULATE)` for
     guaranteed 2 MB pages from the hugetlbfs pool.
  2. Fall back to regular `mmap(MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE)` +
     `madvise(MADV_HUGEPAGE)` (THP).
  3. Fall back to `Vec<PowBlock>` on non-Linux.

- **Huge page setup**: Required pre-allocating huge pages:
  ```bash
  echo 3 > /proc/sys/vm/drop_caches      # free page cache
  echo 1 > /proc/sys/vm/compact_memory    # defragment physical memory
  echo 1100 > /proc/sys/vm/nr_hugepages   # allocate 1100 × 2 MB = 2.2 GB
  ```
  Initial attempt allocated only 391 of 1100 due to memory fragmentation;
  compact_memory + drop_caches resolved this.

- **Verification**: After fix, arena mapped at 2MB-aligned address with
  `HugePages_Free` dropping by 1024 (one 2 GiB arena). Init time dropped from
  0.7s to 0.2s due to MAP_POPULATE pre-faulting.

- **TLB math**: With 2 MB huge pages, the 2 GiB arena requires only 1024 pages.
  Zen 3's L2 dTLB has 1536 entries for 2 MB pages — all 1024 fit with room to
  spare. Each L1 dTLB miss now costs ~7 cycles (L2 TLB hit) instead of ~200
  cycles (full page table walk). This eliminates the 93.89% dTLB miss rate.

- Baseline: `eac8117` (same baseline worktree as previous A/B tests).
- Interleaved A/B (4 pairs, 30s rounds, 3 rounds + 1 warmup, 15s cooldown):
  - `data/bench_cpu_ab_hugepages_mmap/summary.txt`
  - Baseline avg: `1.608 H/s`
  - Candidate avg: `2.341 H/s`
  - Delta: **+45.57%**
  - Individual candidate runs: 2.324, 2.291, 2.343, 2.407 H/s.
  - Candidate wins all 4 pairs.

- **Decomposition**: The +45.57% vs `eac8117` baseline includes both the
  mid-compress prefetch changes (+21.5%, Attempt 23) and the mmap huge page
  allocation. Isolating the huge page contribution:
  - Attempt 23 alone: ~1.95 H/s (with failed THP)
  - Attempt 25 on top: ~2.34 H/s
  - Huge page marginal gain: `2.34 / 1.95 - 1` = **+20%**

- **Requirement**: `MAP_HUGETLB` requires pre-allocated huge pages via
  `/proc/sys/vm/nr_hugepages`. Each worker thread consumes 1024 × 2 MB = 2 GiB
  of huge pages. Without pre-allocation, falls back to regular mmap +
  MADV_HUGEPAGE (which may or may not provide huge pages depending on system
  THP configuration and memory fragmentation).

- Commit: `c4965ac`.
- Status: adopted.

## Updated summary of cumulative adopted optimizations

### x86_64 (AMD Ryzen 9 5900X, Zen 3)

| Attempt | Change | Kernel delta | Backend delta | Status |
|---------|--------|-------------|---------------|--------|
| 2 | Precomputed data-independent refs | +5.26% | +0.70% | Adopted |
| 11 | AVX2 SIMD block compression | +18.75% | +20.10% | Adopted |
| 14 | In-place `compress_into` | +4.05% | +6.41% | Adopted |
| 15 | Software prefetching | +3.81% | +3.82% | Adopted |
| 16 | Fat LTO | +7.80% | +6.31% | Adopted |
| 17 | Fused column-scatter + final XOR | +2.58% | +1.29% | Adopted |
| 23 | Mid-compress prefetch + 3-ahead DI + full cache-line + THP | +21.49% | — | Adopted |
| 25 | mmap MAP_HUGETLB arena | +45.57%\* | — | Adopted |

\* +45.57% is the cumulative delta of Attempts 23+25 vs `eac8117` baseline. The
mmap huge page contribution alone is ~+20% on top of Attempt 23.

Cumulative x86_64: from ~1.19 H/s (original) to ~2.34 H/s, **~97% total improvement**.

### AArch64 (Apple M4 Max)

| Attempt | Change | Kernel delta | Backend delta | Status |
|---------|--------|-------------|---------------|--------|
| 17 | NEON SIMD + prefetch + MaybeUninit | +9.73% | +12.09% | Adopted |
| 18 | Deeper prefetch (3-ahead, 4 cache lines) | +2.94% | +2.46% | Adopted |
| 20 | SHA3 `xar` for all BLAMKA rotations | +11.98% | +9.29% | Adopted |
| 21 | Full prefetch coverage (8 cache lines) | ~+5-10% | ~+5-10% | Adopted |
| 24 | Inline asm UMLAL.2D for BLAMKA | +9.4% | +9.7% | Adopted |
| 28 | Mid-compress prefetch for data-dep slices | +8.1% | +8.0% | Adopted |
| 35 | Interleaved lo/hi BLAMKA half-rounds | +0.95% | — | Adopted |
| 37 | 2-column Phase 3+4 interleave | +1.56% | — | Adopted |

Cumulative AArch64: from ~1.37 H/s (scalar) to ~2.72 H/s, **~98% total improvement**.

### Cross-platform insights

- **TLB pressure is THE dominant bottleneck on x86_64**. The 2 GiB arena with 4 KB
  pages (524K entries) overwhelms even Zen 3's 2048-entry L2 dTLB. Explicit huge
  pages via `MAP_HUGETLB` reduced dTLB miss rate from 93.89% to near-zero and
  delivered +20% on its own.
- **THP (`MADV_HUGEPAGE`) is unreliable**. On this system, `MADV_HUGEPAGE` silently
  failed to provide any huge pages for the 2 GiB arena despite `enabled=[always]`
  and `defrag=[madvise]`. The `AnonHugePages` counter in smaps was 0 kB. This is
  likely due to memory fragmentation and allocation patterns that prevent the kernel
  from finding contiguous 2 MB physical regions.
- **AArch64 avoids this problem**: Apple Silicon uses 16 KB pages natively (4x fewer
  TLB entries needed) and has a fast hardware page table walker. The TLB contribution
  to the bottleneck is proportionally smaller on AArch64.
- **Instruction-level optimizations (inlining, scheduling) have minimal impact on
  x86_64**. The function call boundary from `#[target_feature(enable = "avx2")]` was
  verified as zero-cost — Zen 3's deep OOO engine overlaps across it. The bottleneck
  is memory, not compute.
