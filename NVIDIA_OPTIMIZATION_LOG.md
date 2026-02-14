# NVIDIA Optimization Log

This log tracks every measurable NVIDIA backend optimization attempt so we can iterate without repeating dead ends.

## Benchmark Protocol
- Command: `cargo run --release -- --bench --bench-kind kernel --backend nvidia --nvidia-devices 0 --bench-secs 10 --bench-rounds 3 --ui plain`
- GPU target: RTX 3080 (10 GiB)
- Rule: keep only changes that improve multi-round average H/s and do not increase failure rate (OOM/crash/correctness risk).
- Variance note: results move with clocks/thermals; prioritize 3-round averages over single-round samples.

## Current Best Known Config
- Commit: `working tree` (post-pass)
- Kernel model: cooperative per-lane execution (`32` threads/lane block) with full-warp compression mapping (`8` states x `4` threads/state)
- NVRTC flags include: `--maxrregcount=<autotuned>`, `-DSEINE_FIXED_M_BLOCKS=2097152U`, `-DSEINE_FIXED_T_COST=1U`
- NVIDIA autotune schema: `7`; regcap sweep: `240/224/208/192/160`
  - Autotune key includes CUDA compute capability, bucketed effective memory budget (512 MiB buckets), and lane-capacity tier.
  - Candidate scoring uses deadline-window counted H/s median across `nvidia_autotune_samples` (default `2`), then mean counted H/s and throughput tie-breaks.
- Startup lane sizing now uses free-VRAM-aware budgeting with a small reserve (`max(total/64, 64 MiB)`)
- Kernel hot path writes compression output directly to destination memory (no `block_tmp` shared staging buffer)
- Runtime hot path:
  - no explicit pre-`memcpy_dtoh` stream synchronize in `run_fill_batch`
  - seed expansion and target checks are now GPU-side kernels (`build_seed_blocks_kernel`, `evaluate_hashes_kernel`) with single-index D2H readback for solutions
  - optional fused in-fill target-check path is available via `--nvidia-fused-target-check` (default off after regression on RTX 3080)
- Default launch depth: `nvidia_hashes_per_launch_per_lane=2` (override to `1` for finer preemption).
- Observed best 3-round averages so far (2026-02-14): kernel `1.600 H/s`, backend `1.532 H/s` (counted-window + fence accounting)

## Attempt History
| Date | Commit/State | Change | Result | Outcome |
| --- | --- | --- | --- | --- |
| 2026-02-13 | `2b5ed78` | Initial native CUDA backend | `0.065 H/s` | Baseline |
| 2026-02-13 | pre-`35190c1` | One lane per CUDA block launch geometry | `0.076 H/s` | Kept |
| 2026-02-13 | pre-`35190c1` | Full free-VRAM lane sizing without probe | OOM at startup | Reverted |
| 2026-02-13 | pre-`35190c1` | Shared-memory scratch rewrite (old kernel) | `0.050 H/s` | Reverted |
| 2026-02-13 | pre-`35190c1` | VRAM touch-probe + lane backoff + regcap tuning | up to `0.128 H/s` | Kept in `35190c1` |
| 2026-02-13 | `1198bd2` | Cooperative per-lane kernel rewrite | `0.983 H/s` avg | Kept |
| 2026-02-13 | working tree | Cooperative threads sweep: `24/32/40/48` | `0.574/0.978/0.590/0.766 H/s` | `32` kept |
| 2026-02-13 | working tree | Lane cap sweep: `1/2/3/4` | `0.246/0.487/0.736/0.957 H/s` | Highest lane count kept |
| 2026-02-13 | working tree | Regcap A/B: `224` vs `160` | `1.007` vs `0.973 H/s` avg | `224` kept |
| 2026-02-13 | working tree | PTXAS cache hint `-dlcm=ca` | `0.985 H/s` avg | Reverted |
| 2026-02-13 | working tree | Thread-0-only ref-index mapping micro-opt | `0.849 H/s` avg | Reverted |
| 2026-02-13 | working tree | Cooperative loop unroll pragma pass | `0.583 H/s` | Reverted |
| 2026-02-13 | working tree | Warp-sync + device hash-counter removal + lane-0 ref-index broadcast | `0.977 H/s` avg | Reverted (slower than baseline) |
| 2026-02-13 | working tree | Keep warp-sync + drop device hash-counter + restore per-thread ref-index + remove pre-D2H stream sync | `1.027 H/s` avg | Kept (new best, stable over rerun) |
| 2026-02-13 | working tree | Remove post-store `coop_sync()` in inner block loop | `1.014 H/s` avg | Reverted (regressed vs 1.026 baseline) |
| 2026-02-13 | working tree | Direct compression write-to-destination (drop `block_tmp` shared staging) | `1.120 H/s` avg | Kept (clear uplift over baseline) |
| 2026-02-13 | working tree | Compile-time Argon2 constants via NVRTC defines (`SEINE_FIXED_M_BLOCKS`, `SEINE_FIXED_T_COST`) | `1.214 H/s` avg | Kept (new best, stable over rerun) |
| 2026-02-13 | working tree | NVIDIA autotune schema bump `1 -> 2` + regcap candidate sweep `240/224/208/192/160` | baseline `1.162 H/s`; candidates `0.896/1.202 H/s`; final rerun `1.208 H/s` | Kept (`+3.44%` best-vs-baseline; first candidate outlier tied to one-time cache rebuild) |
| 2026-02-13 | working tree | Drop NVRTC option `--extra-device-vectorization` from NVIDIA kernel compile flags | baseline `1.094 H/s`; candidates `1.179/1.193 H/s`; final rerun `1.193 H/s` | Kept (`+9.05%` best-vs-baseline, `+8.41%` mean, stable across rerun) |
| 2026-02-13 | working tree | NVIDIA autotune schema bump `2 -> 3` + regcap candidate sweep `240/224/208/192/176/160/144` | baseline `1.143 H/s`; candidates `0.864/1.143 H/s`; final reruns `0.874/1.122 H/s` | Reverted (best delta `0.00%`, below `0.5%` keep gate; introduced one-time cache-rebuild outlier) |
| 2026-02-13 | working tree | Add NVRTC PTXAS cache hint `--ptxas-options=-dlcm=cg` | baseline `1.139 H/s`; candidates `1.045/1.048 H/s`; final rerun `1.138 H/s` | Reverted (`-7.99%` best-vs-baseline, clear and stable regression) |
| 2026-02-13 | working tree | Drop NVRTC option `--use_fast_math` from NVIDIA kernel compile flags | baseline `1.138 H/s`; candidates `1.114/1.130 H/s`; final rerun `1.121 H/s` | Reverted (`-0.70%` best-vs-baseline, below keep gate and negative) |
| 2026-02-14 | working tree | Split NVIDIA worker channels (`assign` vs `control`), enable true nonblocking backend calls, and initially add deadline-tail lane throttling plus loop-unroll autotune candidate | same-machine `HEAD` baseline (fixed regcap `160`): kernel `1.184 H/s`, backend `1.213 H/s`; initial candidate: kernel `1.119 H/s`, backend `1.049 H/s` | Partially reverted (tail throttling + active unroll path regressed throughput) |
| 2026-02-14 | working tree | Keep worker channel split + nonblocking control path; revert deadline-tail lane throttling | backend reruns with fixed regcap `160`: `1.195/1.248 H/s` vs `HEAD` `1.213 H/s` | Kept (`+2.89%` best-vs-baseline; control enqueue no longer blocks behind long kernel batches) |
| 2026-02-14 | working tree | Remove live `SEINE_BLOCK_LOOP_UNROLL` NVRTC/kernel path; keep cache compatibility (`schema=2`, `#[serde(default)] block_loop_unroll`) and unroll candidate disabled | kernel reruns with fixed regcap `160`: `1.163/1.201 H/s` vs `HEAD` `1.184 H/s`; backend rerun `1.279 H/s` vs `HEAD` `1.213 H/s` | Kept (kernel best `+1.44%`; backend best `+5.44%`; avoids forced cache-rebuild churn) |
| 2026-02-14 | working tree | True NVIDIA batch queueing (no collapse), free-VRAM-aware lane budgeting (`derive_memory_budget_mib`), autotune key/schema update (`schema=3`, include compute capability) | fresh baseline: kernel `0.964 H/s`, backend `1.045 H/s`; clean kernel candidates `1.149/1.117/1.155 H/s`; backend candidates `1.183/1.171/1.164 H/s` (additional low-clock samples `1.044/1.044 H/s`) | Kept (kernel mean `+18.29%`; backend mean `+7.29%` across all clean samples; queueing correctness + OOM resilience improved) |
| 2026-02-14 | working tree | Deadline-tail dynamic lane throttling via per-assignment hash-time EMA | kernel candidate `0.881 H/s`; backend candidate `0.880 H/s` vs same-pass baseline `0.964/1.045 H/s` | Reverted (clear throughput regression; reduced late shares but hurt total H/s) |
| 2026-02-14 | working tree | Little-endian memcpy fast path for seed-word expansion + final block byte packing | kernel `1.136 H/s` vs local kept-state `1.117 H/s`; backend `1.167 H/s` vs local kept-state `1.171 H/s` | Reverted (backend delta `-0.34%`, below keep gate and negative) |
| 2026-02-14 | working tree | Warp-utilization rewrite in `compress_block_coop` (`tid<8` phases -> full-warp cooperative state mapping) | baseline: kernel `1.032 H/s`, backend `1.056 H/s`; candidates: kernel `1.282/1.220 H/s`, backend `1.105/1.207 H/s` | Kept (kernel mean `+21.22%`; backend mean `+9.47%`; clear sustained uplift) |
| 2026-02-14 | working tree | NVIDIA tuning surface expansion (`--nvidia-max-rregcount`, `--nvidia-max-lanes`, `--nvidia-dispatch-iters-per-lane`, `--nvidia-allocation-iters-per-lane`, `--nvidia-autotune-samples`), autotune schema bump `3 -> 4`, key update (`memory_budget_mib`), per-candidate multi-sample median scoring | validation reruns (before launch-depth default switch): kernel `1.600 H/s` (`/tmp/nv-final-kernel.log`), backend `1.386 H/s` (`/tmp/nv-final-backend.log`) | Kept (robust autotune cache identity + lower tuning variance; no correctness/runtime regressions) |
| 2026-02-14 | working tree | Multi-hash-per-launch path + increase default `nvidia_hashes_per_launch_per_lane` `1 -> 2` | baseline default(1) backend `1.386/1.330 H/s`; depth-2 runs `1.459/1.455 H/s`; final default-state validation `1.503 H/s` | Kept (higher counted throughput on RTX 3080; baseline-mean `1.358` -> final `1.503` (`+10.68%`); tradeoff: late-hash share increased from `25%` to `42.86-50.00%`, keep CLI override for finer preemption) |
| 2026-02-14 | working tree | GPU-side seed and solution evaluation pipeline (`build_seed_blocks_kernel` + `evaluate_hashes_kernel`), worker loop migrated to GPU-found nonce path, pinned lane hint across starts, autotune key/schema update (`4 -> 5`) with memory-budget bucketing + lane-capacity tier | final validation reruns: kernel `1.600 H/s` (`/tmp/nv-final-kernel-20260214.log`), backend `1.532 H/s` (`/tmp/nv-final-backend-20260214.log`) | Kept (backend uplift vs prior best `1.503 H/s` is `+1.93%`, stable 3-round kernel parity, and removes host-side hashing overhead + cache-key churn) |
| 2026-02-14 | working tree | NVIDIA policy/config surface + optional fused in-kernel target evaluation + finer in-kernel cancel checkpoints + autotune schema bump `6 -> 7` with deadline-aware counted-H/s scoring and expanded depth candidates | backend validation on RTX 3080 (fixed `--nvidia-max-rregcount 224 --nvidia-hashes-per-launch-per-lane 2`): separate eval path `1.559 H/s` (`/tmp/nv-reg224-d2-long.json`), fused path `1.480 H/s` (`/tmp/nv-postchange-backend-long.json`), strict separate path `1.518 H/s` (`/tmp/nv-reg224-d2-long-strict.json`) | Kept with fused default OFF (control responsiveness improved under stale/cancel pressure and autotune now optimizes deadline-window effective work; fused path retained as experimental knob) |

## New Entry Template
Copy this row and fill in all fields after each pass:

`| YYYY-MM-DD | <commit or working tree> | <exact code/config change> | <H/s avg and command> | <Kept/Reverted + why> |`
