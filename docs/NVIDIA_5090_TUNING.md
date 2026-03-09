# NVIDIA 5090 Tuning Overview

Measured on March 9, 2026 on this host:

- GPU: `NVIDIA GeForce RTX 5090`
- Driver: `590.48.01`
- VRAM: `32607 MiB`
- Seine benchmark mode: local `--bench` runs only, no daemon or pool traffic

## Current Backend Shape

The NVIDIA path is split between the runtime/backend wrapper in `src/backend/nvidia.rs` and the CUDA kernel in `src/backend/nvidia_kernel.cu`.

Current design, in practical terms:

- NVRTC compiles the CUDA source at runtime and writes per-option CUBIN cache entries on disk.
- Lane sizing is VRAM-aware: the backend reserves headroom, allocates the Argon2 lane arenas, then touch-probes them to catch OOM/commit failures early.
- One warp (`32` threads) cooperatively computes one hash lane at a time.
- On this 5090, the runtime settled on `14` active lanes.
- The worker path uses separate assignment and control channels, plus nonblocking cancel/fence support, so control traffic does not wait behind queued assignments.
- Launch depth is adaptive near deadlines; the backend can trim `hashes_per_launch_per_lane` to reduce late work at round end.
- The hot kernel path already includes the recent Blackwell wins:
  - unrolled cooperative loads/stores to raise memory-level parallelism
  - warp-shuffle fused G-round handoff instead of extra shared-memory round trips
  - Blackwell-specific autotune tie-breaks and cached depth clamping

## Benchmark Method

Representative commands used for this pass:

```bash
cargo run --release -- --bench --bench-kind backend --backend nvidia --nvidia-devices 0 --bench-secs 10 --bench-rounds 3 --bench-warmup-rounds 1 --ui plain
```

```bash
cargo run --release -- --bench --bench-kind kernel --backend nvidia --nvidia-devices 0 --bench-secs 10 --bench-rounds 3 --bench-warmup-rounds 1 --ui plain
```

```bash
cargo run --release -- --bench --bench-kind kernel-effective --backend nvidia --nvidia-devices 0 --bench-secs 10 --bench-rounds 3 --bench-warmup-rounds 1 --ui plain
```

Notes:

- I used a fresh temp `--data-dir` per benchmark variant to avoid cross-run config drift.
- Cached runs reused the same `--nvidia-autotune-config` so they hit the existing autotune record and CUBIN cache.
- Benchmark mode now skips persisted pool-input validation, so repeated `--bench` runs no longer fail just because a reused `seine.config.json` lacks pool address fields.

## Results

### Cold start vs steady state

| Profile | Init time | Avg H/s | Median H/s | Late hash % | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| Fresh default cache | `246.6s` | `7.953` | `7.854` | `16.67%` | First Blackwell autotune on an empty cache selected `rreg=224`, `depth=1`, `14` lanes |
| Cached default | `0.6s` | `8.897` | `8.868` | `14.29%` | Best steady-state result from this pass; same autotuned `224/1` profile |

Steady-state guidance should use the cached number. The fresh-cache run is mainly a cold-start cost measurement.

### Kernel vs backend

| Kind | Avg H/s | Median H/s | Delta vs cached backend |
| --- | ---: | ---: | ---: |
| `kernel` | `9.800` | `9.800` | `+10.15%` |
| `kernel-effective` | `8.662` | `8.736` | `-2.65%` |
| `backend` cached default | `8.897` | `8.868` | baseline |

Takeaway: the raw kernel ceiling is only about ten percent above the steady backend result. Most of the large historical wins have already been captured; remaining gains are likely to come from kernel/control-tail cleanup, not from rewriting the entire backend wrapper.

### Fixed-point sweep on the 5090

All runs below used `10s x 3 rounds` with `1` warmup round and the same cached CUBIN/autotune artifacts.

| Variant | Avg H/s | Delta vs cached default | Late hash % | Verdict |
| --- | ---: | ---: | ---: | --- |
| cached default (`autotuned 224/1`) | `8.897` | baseline | `14.29%` | keep |
| forced `208/1` | `8.842` | `-0.62%` | `14.29%` | no win |
| forced `224/1` | `8.741` | `-1.75%` | `14.29%` | no win |
| forced `240/1` | `8.859` | `-0.43%` | `14.29%` | no clear win |
| forced `224/2` | `8.691` | `-2.31%` | `14.29%` | worse |
| forced `224/2` + no adaptive depth | `8.847` | `-0.56%` | `25.00%` | not worth the extra late work |
| forced `224/1` + fused target check | `8.536` | `-4.05%` | `15.00%` | keep fused off |

### Interleaved check: cached default vs forced `240/1`

A short two-pair alternating A/B run did not confirm the tiny one-shot `240/1` edge:

| Variant | Mean avg H/s | Outcome |
| --- | ---: | --- |
| cached default | `9.126` | won both pairs |
| forced `240/1` | `8.545` | `-6.37%` vs default over this short A/B |

This card still shows meaningful thermal/boost variance, so the exact percentage should not be over-read. The useful result is simpler: the default cached/autotuned path held up better than forcing `240/1`.

## What To Do On A 5090

- Leave the default cached/autotuned NVIDIA profile alone.
  - On this host, that means `14` lanes and an autotuned `224/1` record.
- Do not enable `--nvidia-fused-target-check` by default on Blackwell.
- Do not force depth `2` globally.
  - With adaptive depth disabled, late work jumped from `14.29%` to `25.00%`.
- Treat `208/1`, `224/1`, and `240/1` as a narrow frontier.
  - None of them beat the cached default cleanly enough in this pass to justify a new hard-coded default.

## Where The Next Gains Are Most Likely

### 1. Cut Blackwell cold-start time

The biggest practical pain point is startup, not steady-state throughput. Fresh-cache initialization took `246.6s` here.

High-value directions:

- narrow the Blackwell regcap/depth search using the already-observed frontier
- persist and trust more of the fresh-autotune decision path when the device identity is unchanged
- consider a faster first-pass heuristic followed by background re-tune instead of blocking startup on the full search

### 2. Reduce fence/control tail without regressing throughput

The backend still loses time at round-end fence/cancel boundaries. The historical log already showed that simply changing the in-kernel cancel-check cadence is usually a regression, so the better target is launch-shaping rather than inner-loop polling.

High-value directions:

- finer outer launch partitioning near round end
- smarter preemption-aware batch sizing that keeps full depth away from `stop_at`
- A/B around control-path latency and fence jitter, not just raw kernel H/s

### 3. Keep treating the 5090 workload as latency-bound

During active runs on this host, spot `nvidia-smi` samples showed:

- GPU util near `100%`
- power draw around `156-161 W`
- reported memory-controller util around `6%`

Inference: this workload is not power-limited on the 5090. Bigger wins are more likely to come from reducing dependency stalls and end-of-round waste than from chasing higher board power.

## Bottom Line

The 5090 is already using the current NVIDIA backend effectively. The right default on this host is still the cached autotuned path, not a manually forced regcap/depth override. The most useful next optimization target is Blackwell startup/autotune time; the most useful steady-state target is fence/cancel tail reduction without increasing late work.
