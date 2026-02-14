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
- Full suite: `cargo test` (`177 passed`).
