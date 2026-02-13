# NVIDIA Optimization Log

This log tracks every measurable NVIDIA backend optimization attempt so we can iterate without repeating dead ends.

## Benchmark Protocol
- Command: `cargo run --release -- --bench --bench-kind kernel --backend nvidia --nvidia-devices 0 --bench-secs 10 --bench-rounds 3 --ui plain`
- GPU target: RTX 3080 (10 GiB)
- Rule: keep only changes that improve multi-round average H/s and do not increase failure rate (OOM/crash/correctness risk).
- Variance note: results move with clocks/thermals; prioritize 3-round averages over single-round samples.

## Current Best Known Config
- Commit: `working tree` (post-pass)
- Kernel model: cooperative per-lane execution (`32` threads/lane block)
- NVRTC flags include: `--maxrregcount=224`, `-DSEINE_FIXED_M_BLOCKS=2097152U`, `-DSEINE_FIXED_T_COST=1U`
- Kernel hot path writes compression output directly to destination memory (no `block_tmp` shared staging buffer)
- Runtime hot path: no explicit pre-`memcpy_dtoh` stream synchronize in `run_fill_batch`
- Observed best 3-round average so far: `1.214 H/s` (2026-02-13)

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

## New Entry Template
Copy this row and fill in all fields after each pass:

`| YYYY-MM-DD | <commit or working tree> | <exact code/config change> | <H/s avg and command> | <Kept/Reverted + why> |`
