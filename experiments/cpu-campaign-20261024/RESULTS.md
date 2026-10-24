# CPU architecture campaign results

**Work in progress.** Baseline v0.2.21 (`359265e`), current 2 GiB Argon2id PoW.
All rates below are completed hashes divided by actual elapsed time, including
round-end fences. They describe the tested configuration, not a guaranteed gain
on another machine. No campaign PR has been merged or released.

## Architecture evidence collected

| Machine / configuration | Control → candidate H/s | Evidence and disposition |
| --- | ---: | --- |
| Native Windows Ryzen 9950X3D, 1 worker, regular pages, AVX2 → AVX-512 | 2.416 → 2.621 | Four alternating pairs, all positive (+2.55% to +11.12%). Supports existing automatic ISA comparison. |
| Same Ryzen, 8 workers spread across both CCDs | 7.224 → 7.528 | Four pairs, +0.53% to +6.65%; retain automatic per-machine selection. |
| Same Ryzen, 4 physical cores → four SMT threads on two physical cores | 6.482 → 6.174 | Four pairs, every SMT arrangement slower (−3.11% to −6.18%); supports physical-first placement. |
| Same Ryzen, CCD/cache placement | Inconclusive | Four-pair CCD/spreading comparisons do not justify a new default. The 1-worker CCD series includes a −36.77% reversal; retained rather than discarded. |
| Graviton3 / Neoverse-V1, 1 worker, 2 MiB HugeTLB, NEON → runtime SHA3 + 64-byte prefetch | 0.820 → 1.228 | Four clean alternating pairs, +49.63% to +49.82%. Candidate eligible for automatic productive-work confirmation. |
| Same Graviton3, 16 physical workers, 2 MiB HugeTLB | 12.316 → 17.930 | Four pairs, +44.61% to +46.18%. Both sides rebuilt in separate Cargo targets. |
| Same Graviton3, regular pages, 1 / 8 workers | 0.756 → 1.038 / 5.862 → 7.791 | Two alternating pairs each, +37.09% to +37.48% / +32.69% to +33.14%. |
| Graviton2 / Neoverse-N1 without SHA3, 1 / 16 workers, 2 MiB HugeTLB, baseline → candidate portable NEON | 0.682 → 0.707 / 10.583 → 11.059 | Four longer alternating pairs: +3.60% to +3.67% / +4.38% to +4.55%. This is the fallback path, not SHA3 execution. |
| Intel Xeon Platinum 8375C / Ice Lake, 1 / 12 workers, 2 MiB HugeTLB, published AVX2 → AVX-512 | 1.449 → 1.853 / 11.139 → 12.118 | Two opposite-order screening passes; full AVX-512 remains the stronger shipping path here. |
| Same Ice Lake, experimental 256-bit AVX-512, 1 / 12 workers | 1.552 / 11.913 | Slower than full AVX-512 in this screen; remains benchmark-only. |
| Same Ice Lake, streaming ring at 12 workers | 12.191 | No material improvement over published full AVX-512 (12.112); slower than separately rebuilt default-feature full AVX-512 (12.450). No promotion. |
| Intel Xeon Platinum 8488C / Sapphire Rapids, 1 / 12 workers, 2 MiB HugeTLB, AVX2 → AVX-512 | 1.617 → 1.726 / 12.512 → 13.099 | Four longer alternating pairs, full AVX-512 +6.13% to +7.97% / +4.05% to +5.27%. Also leads across the preliminary worker-count screen. |
| Same Sapphire Rapids, experimental 256-bit AVX-512, 1 / 12 workers | 1.654 / 12.737 | Four longer pairs: full AVX-512 +4.10% to +4.62% / +2.22% to +3.10% over this path; no promotion. |
| Same Sapphire Rapids, streaming ring at 8 / 12 workers | 11.666 / 12.973 | Two opposite-order screens; full AVX-512 11.605 / 12.969. No material gain; no promotion. |
| Same Ice Lake, independent-hash interleaving, equal 4 GiB arenas | 1.977 versus ordinary SMT 2.232 | Rejected early after both run orders; best interleaving is about 11.4% slower than the same-physical-core SMT control. |

Newer Intel, older Intel/Zen and Graviton4 results remain pending.
EPYC evidence from the coordinated session is described separately below; it is
not relabelled as a v0.2.21 campaign measurement.

## Reproducibility and limitations

The [evidence directory](evidence/) keeps normalized per-round hash counts,
actual elapsed/fence times, page backing, error counters, build fingerprints and
raw-report SHA256 values. Windows result manifests additionally retain each
logical CPU mask, comparison role and pair order. Original reports and complete
SHA-verified cloud archives remain in ignored campaign data. The
[controllers](README.md#scripts) reproduce the configurations.

Native Windows tests used the published MSVC release. WSL's synthetic single-L3
topology was not used to infer CCD placement. Windows explicit large-page
allocation failed with error 1450, so the reported desktop comparisons require
regular backing and remain limited to eight workers / 16 GiB arenas. No native
Windows large-page or multi-processor-group claim is made.

Graviton3 uses 64-byte cache lines and has runtime SHA3 support. The candidate
combines XAR-based XOR/rotate with complete 64-byte prefetch coverage. Separate
screens isolated the SHA3 and prefetch contributions before combining them.
The baseline NEON path remains available; startup preference stays unchanged.
Native Graviton2 / Neoverse-N1, which lacks SHA3, passed three full-size
reference nonces and dirty reuse; unsupported SHA3 selection is rejected and
NEON remains active. Apple Silicon candidate performance remains untested.

The first Ice Lake streaming comparison is **excluded**: a shared Cargo target
reused an experimental library in the purported baseline. Symbol inspection
exposed the contamination. Fresh isolated builds, symbol checks and full-size
reference parity precede the replacement measurements above. The published
release screen and separately built interhash runner were unaffected.

The interhash timebox ended early at 11:44 UTC. Every mode used two full arenas:
sequential 1.867 H/s, pair chunks 1/4/16 at 1.977/1.875/1.843, ordinary workers on
one logical CPU 1.832, on both SMT siblings of one physical core 2.232, and on two
physical cores 3.599. All four direct modes passed full-size reference parity,
three nonce pairs and dirty reuse. This rejects the experiment on this Ice Lake
configuration; it does not prove a universal result across architectures.

## Resource lifecycle findings

Both Graviton3 and Ice Lake reproduced a constrained-start recovery bug: a
process restarted under `memory.high=3G` remained capped at one worker after
the limit rose to 12 GiB. Earlier pressure, zero-worker parking, productive
recovery and CPU-load phases passed, but the restart failure prevented the
subsequent opt-out phase. These runs are recorded as failures, not partial
passes. Graviton3 independently confirmed automatic SHA3 selection at 194.1
seconds and a later two-to-three-worker increase before the restart.

The repair separates parked worker capacity and nonce stride from changing RAM
limits, while active arenas remain bounded by the current allowance. Recovery
gets a debounced opportunity even during kernel comparisons. The standalone
repair passes 484 CPU-only workspace tests (four ignored) and independent
review by the autotune owner. Native reruns additionally require at least
4 GiB of resident anonymous memory after restart recovery, so a requested
thread count alone cannot hide an undersized backend. The complete fixed Sapphire Rapids rerun passed all nine phases, including
fixed-thread opt-out. Restart recovery reached three workers and 6.028 GiB
anonymous residency; no OOM kills or quarantine occurred. Both full-arena
growth/switch tests, full-size ISA parity, 485 workspace tests and strict kernel
Clippy passed. The same complete lifecycle also passed on Graviton2, including
three workers and 6.028 GiB anonymous residency after constrained restart.

Two clean alternating Ice Lake regression pairs compare the ARM-change build
against its separately rebuilt CPU-only baseline: approximately −0.29% at one
worker and −0.27% at twelve. This small negative is retained explicitly. Full
reference parity and native workspace tests passed.

The follow-up review found a separate startup regression in the recovery repair:
parked OS threads can exhaust a tight PID budget. A network-free local backend
probe with explicit 16-CPU affinity, `pids.max=12` and `memory.high=3G` starts
with the legacy one-slot capacity, but the new sixteen-slot capacity fails at
worker 9 with EAGAIN. Both use one enabled arena. Owned test units were cleaned
up. The [reproducer patch](pid-startup-probe.patch), [controller](pid_probe.py)
and [evidence](evidence/pid-startup-regression.json) are retained. The autotune owner implemented focused hardening: nested cgroup PID headroom
bounds parked slots, adaptive resource-related spawn failures clean up and retry
with fewer workers, and manual counts retain RAM-safe limits. The same real OS
probe now passes with four provisioned slots and unchanged stride 16; the legacy
one-slot control also passes. This probe deliberately bypasses preflight and is
not a full pool runtime test. PR #43 remains draft while combined native pool
startup and resource lifecycle checks run on Graviton4 and Xeon 6975P-C.

## Reviewable changes

- [PR #40](https://github.com/zpalmtree/seine/pull/40): Windows tuning-cache
  identity includes permitted CPUs and canonical physical-core groups. Native
  mask tests and CPU-only workspace tests passed. No placement policy change.
- [PR #41](https://github.com/zpalmtree/seine/pull/41): inconclusive kernel
  confirmations retry after five minutes without caching a loss; aborted new
  contexts do not inherit an old 30-minute delay. CPU-only workspace tests and
  independent review by the autotune owner passed. Stable below-threshold
  results retain their existing policy.
- [Draft PR #43](https://github.com/zpalmtree/seine/pull/43): recoverable CPU
  worker capacity after temporary RAM limits, stacked on #41. Native fixed
  lifecycle passed on Sapphire Rapids and Graviton2. PID-limit hardening and
  final combined native confirmation are in progress.
- [PR #45](https://github.com/zpalmtree/seine/pull/45): native CPU-only Linux
  ARM release archive with kernel tests and full-size reference verification.
  Native source build, package contents/digests and YAML checks pass; the hosted
  release workflow has not been run.
- [Draft PR #44](https://github.com/zpalmtree/seine/pull/44): runtime ARM
  SHA3/prefetch candidate, stacked on #43. Native full-size parity, small
  boundary tests, real-arena worker switches and Graviton3 automatic selection
  passed. Graviton4 and complete repaired lifecycle confirmation remain pending.

Strict workspace/binary Clippy is not clean at baseline. On Ice Lake, baseline
and candidate all-target runs produced the same 65 error-message lines (63
underlying diagnostics plus build summaries); strict kernel-crate Clippy passed.
The Windows cache PR's binary-only check found 61 existing diagnostics, all in
source files byte-identical to its parent. These are disclosed, not counted as
passing checks.

## Coordinated EPYC evidence

The EPYC owner completed its work and released all cloud resources before this
campaign acquired the shared 32-vCPU quota. On its older base `785a06f`, a 64 KiB
streaming ring on EPYC 9R45 at 24 workers measured 45.143 H/s versus 35.328 AVX2,
with three alternating pairs (+27.34% to +28.42%), full-size parity, dirty reuse
and sentinel checks. One-worker and eight-worker screens did not establish a
benefit. The candidate still needs a distinct production identity and applicable
worker-switch validation before promotion. No DRAM-bandwidth claim is inferred
from its available performance counters.

## Resources and completion

Current resource accounting, additional hardware gaps, final PRs and verified
cleanup will be added before the 20:27:13 UTC deadline. Every lease has an
independent guest shutdown and EC2 terminate-on-shutdown behavior. The campaign
reserves at most $40 incremental cloud spending and stops new experiments by
17:57:13 UTC to protect the final 2.5-hour confirmation/review/cleanup window.
