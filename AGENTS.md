# seine — Agent Guide

seine is the external Blocknet miner other people run on their own hardware.
The goal of almost every change: **the best hashrate on each user's exact
hardware, with zero configuration.** A user who runs `seine --address ...`
and touches nothing should get the right kernel, thread/lane count, page
mode, and affinity for their CPU and GPUs.

## Principles

- **Zero-config first.** A win that needs a flag helps nobody but us. Land it
  as an automatic default (gated to the hardware it was measured on) or as a
  candidate the runtime tuner can find by itself. Flags are for experiments
  and overrides, not the shipping path.
- **Gate by precise identity, not one global policy.** If something helps
  one architecture and is neutral or harmful on others, enable it only where
  it was measured: x86 vendor/family/model, aarch64 features, CUDA compute
  capability or exact device name, HIP `gcnArchName`. Don't reject it because
  it doesn't win everywhere, and don't extend it to "similar" hardware
  without evidence.
- **Two independent gates.** Identity decides whether we *try* a path. The
  runtime ISA/OS support check (`is_x86_feature_detected!`, etc.) decides
  whether we *can*. Never let identity bypass the support check, and always
  keep a safe fallback (`CpuKernel::preferred()`, the generic GPU candidate
  set).
- **Evidence before defaults.** A new default needs a paired A/B on the
  target hardware (interleaved, thermally stable).
- **Correctness is non-negotiable.** Every kernel variant must match the
  reference Argon2id output. Consensus validation stays in the daemon, so a
  wrong hash costs users rejected shares, not chain safety.

## Where per-arch decisions live

| Decision | Location |
|---|---|
| CPU kernel variants + ISA support check + fallback order | `pow-kernel/src/cpu_kernel.rs` (`CpuKernel`, `is_supported`, `preferred`) |
| Kernel implementations | `pow-kernel/src/fixed_argon.rs`, `pow-kernel/src/fixed_argon/` |
| Which CPU kernels the live tuner may try, per identity | `src/miner/adaptive_kernel.rs` (`candidates_for`) |
| CPU identity (vendor/family/model) and build/runtime fingerprint | `src/runtime_identity.rs` (`cpu_identity`) |
| Live CPU worker count tuning, joint count/kernel comparisons (each candidate count finishes its kernel sweep), memory/PSI shedding | `src/miner/adaptive_cpu.rs` (`tick_joint`), `src/miner/adaptive_kernel.rs` |
| CPU topology / affinity (NUMA, L3, SMT, macOS P-cores) | `src/backend/cpu/topology.rs`, `src/config.rs` (`pcore_count`, `macos_hybrid_pcore_parallelism_cap`) |
| Page mode (HugeTLB / Windows large pages / THP) | `src/config.rs`, `src/backend/cpu.rs` |
| NVIDIA per-arch fill loop variants (NVRTC defines), launch depth, cancel interval, prebuilt PTX per arch | `src/backend/nvidia.rs` (`arch_fill_defines`, `prebuilt_argon2_ptx`, `BLACKWELL_*`, `select_cancel_check_block_interval`); regenerate PTX with `scripts/gen_argon2_ptx.py` |
| AMD arch gating (wave32 only) and hipRTC flags | `src/config.rs` (`partition_wave32_amd_devices`), `src/backend/amd/engine.rs` (`build_hiprtc_options`) |
| Apple GPU | `src/backend/metal.rs` |

Every autotune cache is keyed by host/build/topology/page-mode fingerprints.
When you add an identity-gated path, add it to the fingerprint or candidate
set so stale caches from a different path get re-tuned, not reused.

## Adding or changing an arch-specific path

1. Measure on the exact target hardware (rented or remote is fine; the dev
   box is not a mining rig, so tear down hugepages and miners afterwards).
2. Gate it in the matching location above, with a short comment naming the
   identity and why the path suits it. Keep benchmark numbers out of comments.
3. Add a unit test for the gate: the intended identity selects the path, a
   neighbouring identity doesn't, and an unsupported ISA never does (see the
   `candidates_for` tests).
4. Check parity: kernel reference tests in `pow-kernel`, plus
   `pow-kernel/examples/verify_streaming_dispatch.rs` for streaming/ring
   kernels.
5. Add a user-facing line to `CHANGELOG.md` if defaults change.

## Runtime invariants (don't break these)

- Backends get disjoint nonce chunks per round. Solutions are never dropped
  (lossless `Solution` events, deferred re-queue on submit backlog).
- A faulty backend is quarantined alone; mining continues on the rest.
- Mining control loops do no network I/O (prefetch/submit workers own it).

## Formatting

- Use the pinned toolchain: `cargo +1.93.0 fmt` (see `rust-toolchain.toml`).
- Format only files you edited. No repo-wide `cargo fmt --all` and no
  formatting-only churn in commits unless asked.

## Release notes (`CHANGELOG.md`)

Write for miners deciding whether to upgrade: what improves, who benefits and any
action they need to take, in short plain bullets with the biggest benefit first.

## References

- Every flag: `docs/MINER_FLAGS.md` / `seine --help`.
