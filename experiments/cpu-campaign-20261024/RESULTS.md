# CPU architecture campaign results

**Completed CPU architecture investigation and confirmation.** Baseline v0.2.21 (`359265e`), current 2 GiB Argon2id PoW.
All rates below are completed hashes divided by actual elapsed time, including
round-end fences. They describe the tested configuration, not a guaranteed gain
on another machine. Following the user’s merge instruction, PRs #40 and #45 have been merged.
The memory/PID subset retained an AVX2 regression and PR #43 is held in draft.
Standalone retry PR #41 also retained a negative signal and remains draft.
No release has been published.

## Architecture evidence collected

| Machine / configuration | Control → candidate H/s | Evidence and disposition |
| --- | ---: | --- |
| Native Windows Ryzen 9950X3D, 1 worker, regular pages, AVX2 → AVX-512 | 2.416 → 2.621 | Four alternating pairs, all positive (+2.55% to +11.12%). Supports existing automatic ISA comparison. |
| Same Ryzen, 8 workers spread across both CCDs | 7.224 → 7.528 | Four pairs, +0.53% to +6.65%; retain automatic per-machine selection. |
| Same Ryzen, 4 physical cores → four SMT threads on two physical cores | 6.482 → 6.174 | Four pairs, every SMT arrangement slower (−3.11% to −6.18%); supports physical-first placement. |
| Same Ryzen, CCD/cache placement | Inconclusive | Four-pair CCD/spreading comparisons do not justify a new default. The 1-worker CCD series includes a −36.77% reversal; retained rather than discarded. |
| Graviton3 / Neoverse-V1, 1 worker, 2 MiB HugeTLB, NEON → final `f20e7bb` SHA3 + 64-byte prefetch | 0.824 → 1.230 | Four corrected alternating pairs, +49.17% to +49.42%; full-size runtime parity passed. |
| Same Graviton3, 16 physical workers, 2 MiB HugeTLB | 12.520 → 18.170 | Four corrected alternating pairs, +44.31% to +45.91%; frozen generic CPU-only binaries. |
| Same Graviton3, regular pages, 1 / 8 workers | 0.762 → 1.038 / 5.892 → 7.807 | Four corrected alternating pairs each, +35.52% to +36.72% / +32.23% to +32.86%. |
| Graviton2 / Neoverse-N1 without SHA3, 1 / 16 workers, 2 MiB HugeTLB, baseline → final `f20e7bb` portable NEON | 0.680 → 0.707 / 10.609 → 11.047 | Four corrected alternating pairs: +3.81% to +3.95% / +3.83% to +4.55%. Full-size dirty-reuse parity and unsupported SHA3 rejection passed. This is the fallback path, not SHA3 execution. |
| Graviton4 / Neoverse-V2, 1 / 16 workers, 2 MiB HugeTLB, NEON → runtime SHA3 + prefetch | 0.891 → 1.373 / 13.800 → 21.503 | Four corrected, balanced, actual-order-verified pairs: +53.94% to +54.33% / +55.11% to +57.00%. Combined `636e688` lifecycle passed; the final `f20e7bb` boundary change is near-flat on ARM (two pairs, sixteen-worker mean −0.29%). |
| Intel Xeon Platinum 8375C / Ice Lake, 1 / 12 workers, 2 MiB HugeTLB, published AVX2 → AVX-512 | 1.449 → 1.853 / 11.139 → 12.118 | Two screening passes (order limitation below); full AVX-512 remains the stronger shipping path here. |
| Ice Lake, final `f20e7bb` versus baseline, 1 / 12 workers, AVX2 | 1.490 → 1.458 / 11.369 → 11.219 | Two corrected pairs: −2.640% to −1.691% / −1.721% to −0.920%; all negative. Combined-stack regression remains unresolved. |
| Same final Ice Lake comparison, full AVX-512 | 1.836 → 1.825 / 12.205 → 12.045 | Two corrected pairs: −0.782% to −0.413% / −1.334% to −1.280%; all negative. Native full-size parity and evidence audit passed; no promotion. |
| Same Ice Lake, experimental 256-bit AVX-512, 1 / 12 workers | 1.552 / 11.913 | Slower than full AVX-512 in this screen; remains benchmark-only. |
| Same Ice Lake, streaming ring at 12 workers | 12.191 | No material improvement over published full AVX-512 (12.112); slower than separately rebuilt default-feature full AVX-512 (12.450). No promotion. |
| Intel Xeon Platinum 8488C / Sapphire Rapids, 1 / 12 workers, 2 MiB HugeTLB, AVX2 → AVX-512 | 1.617 → 1.726 / 12.512 → 13.099 | Four longer repeated pairs (AVX2-relative order limited), full AVX-512 +6.13% to +7.97% / +4.05% to +5.27%. Also leads across the preliminary worker-count screen. |
| Sapphire Rapids, final `f20e7bb` versus baseline, 1 / 12 workers, AVX2 | 1.593 → 1.595 / 12.142 → 12.213 | Two corrected pairs: +0.116% to +0.129% / +0.471% to +0.700%; native full-size parity passed. |
| Same final Sapphire Rapids comparison, full AVX-512 | 1.716 → 1.721 / 12.767 → 12.727 | Two corrected pairs: +0.202% to +0.286% / −0.783% to +0.154%; mixed at twelve workers, no material gain claimed. |
| Same Sapphire Rapids, experimental 256-bit AVX-512, 1 / 12 workers | 1.654 / 12.737 | Four longer pairs: full AVX-512 +4.10% to +4.62% / +2.22% to +3.10% over this path; no promotion. |
| Same Sapphire Rapids, streaming ring at 8 / 12 workers | 11.666 / 12.973 | Two screens (order limitation below); full AVX-512 11.605 / 12.969. No material gain; no promotion. |
| Intel Xeon 6975P-C / C8i, 1 / 12 workers, 2 MiB HugeTLB, AVX2 → AVX-512 | 1.809 → 1.938 / 12.428 → 12.916 | Two screens (order limitation below); full AVX-512 leads at all tested counts. |
| Same Xeon 6975P-C, experimental 256-bit AVX-512, 1 / 12 workers | 1.859 / 12.448 | Does not beat full AVX-512; no promotion. |
| Xeon 6975P-C, final `f20e7bb` versus baseline, 12 workers, full AVX-512 | 12.486 → 12.324 | Four longer corrected pairs, all negative (−0.063% to −3.719%), ratio of mean rates −1.304%. Earlier two short pairs also negative. Unresolved regression signal; PR #44 remains draft. |
| Skylake / Xeon 8124M, joint tuning prototype, four 900-second fresh-process/cache ABBA windows | 6.241 → 6.152 logged H/s | Both paired differences negative (−2.006%, −0.836%); aggregate −1.424%. No promotion; subsequent lifecycle intentionally deferred. These include startup/trials/shutdown and are not fixed-kernel rates. |
| Intel Xeon Platinum 8275CL / Cascade Lake, 12 workers, full AVX-512, baseline → combined `636e688` | 9.303 → 9.036 | Four corrected longer pairs lose 2.60–3.34%; regression retained and repaired below. |
| Same Cascade Lake, baseline → x86-boundary fix `f20e7bb` | 9.303 → 9.267 | Four pairs: −0.93% to +0.004%; about −0.39% mean. Consistently ahead of the regressed build; no x86 speedup claim. |
| Same Cascade Lake, published AVX2 / full AVX-512 / experimental 256-bit path, 12 workers | 10.247 / 9.254 / 10.405 | Four corrected longer pairs. AVX2 beats full-width here; narrow path's small advantage does not justify promotion. |
| Haswell / Xeon E5-2666 v3, final AVX2 regression, 1 / 11 workers | 1.175 → 1.175 / 9.722 → 9.679 | Two balanced pairs; eleven-worker differences −1.30% and +0.41%, so no repeatable loss or gain established. |
| Zen 1 / EPYC 7571, final AVX2 regression, 1 / 16 workers | 0.901 → 0.905 / 4.594 → 4.596 | Two balanced pairs and native full-size parity; sixteen-worker differences −0.027% to +0.111%, near-flat. |
| Zen 2 / EPYC 7R32, final AVX2 regression, 1 / 12 workers | 1.883 → 1.885 / 6.012 → 6.007 | Two balanced pairs and native full-size parity; twelve-worker differences −0.088% to −0.078%, near-flat. |
| Ice Lake, proposed #40/#41/#43/#45 subset, 12 workers, AVX2 | 11.234 → 10.997 | Two reversed pairs, −2.277% and −1.930%; #43 held in draft. Full AVX-512 was mixed (+2.051%, −0.257%). |
| Same guest, standalone retry PR #41, 12 workers, AVX2 | 11.198 → 11.095 | Two reversed pairs, −1.011% and −0.821%; #41 also held. Native full-size parity and independent artifact audit passed. Source-level cause remains unresolved. |
| Same Ice Lake, independent-hash interleaving, equal 4 GiB arenas | 1.977 versus ordinary SMT 2.232 | Rejected early after both run orders; best interleaving is about 11.4% slower than the same-physical-core SMT control. |

Final Graviton2/3 corrected confirmations passed. Fresh Xeon 6975P-C confirmation
retains a negative final-build signal, so ARM PR #44 remains draft. Final Sapphire
Rapids checks passed correctness with small/mixed timing differences. Ice Lake
confirmation also retained negative differences in all eight paired comparisons.
Following the user’s subsequent merge instruction, the proposed subset and
standalone retry PR were confirmed separately. Their AVX2 losses kept #41 and
#43 out of production; only #40 and #45 were merged.
EPYC evidence from the coordinated session is described separately below; it is
not relabelled as a v0.2.21 campaign measurement.

## Reproducibility and limitations

The [evidence directory](evidence/) keeps normalized per-round hash counts,
actual elapsed/fence times, page backing, error counters, build fingerprints and
raw-report SHA256 values. Windows result manifests additionally retain each
logical CPU mask, comparison role and pair order. Original reports and complete
SHA-verified cloud archives remain in ignored campaign data; the public
[archive index](evidence/archive-index.json) records their digests and verification.
The
[controllers](README.md#scripts) reproduce the configurations.

Some older shell controllers inherited `SEINE_GIT_COMMIT` into every comparison
process. The report's Git label can therefore name the same revision for distinct
executables. Raw labels remain unchanged. The public
[binary-provenance index](evidence/binary-provenance.json) records the actual
experiment binary SHA256 values, compiled source fingerprints and frozen source
mappings. These identities, rather than an overridable Git annotation, establish
which builds were compared. This includes the final Graviton4 boundary check:
its earlier binary is `636e688`, even though both raw Git labels say `f20e7bb`.

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
The [retained interhash evidence](evidence/interhash-rejected.json) includes all
fourteen timed reports, completed run order, physical-core groups, full-size
parity lines and original archive/file digests.

## Benchmark order review and corrected confirmation

At 15:48 UTC, controller review found that variant rotation used the index in
an already-reversed worker/configuration loop. Reversing both loops can cancel:
the two-variant/two-worker ARM matrices always ran baseline first at one worker
and candidate first at sixteen. Those earlier gains did not meet
the intended per-worker order-reversal requirement. Raw results remain intact;
normalized evidence is labelled `order_limited`, and corrected native Graviton2,
Graviton3 and Graviton4 confirmation was required before ARM promotion; the
corrected series passed, while later x86 regression gates remained unresolved.

Graviton4 and fresh Graviton2/3 completed the corrected four-pass protocol with
gains in every pair; their original interrupted/order-limited runs remain archived.
Graviton2/3 used the final frozen `f20e7bb` binary. Graviton3 also passed four
corrected pairs at each regular-page worker count.

The fix rotates by each configuration's stable original index and asserts the
actual completed variant sequence reverses on every pass. The in-progress
Graviton4 matrix was interrupted, archived and replaced. Final x86 regressions
and subsequent architecture screens use the corrected controller. Earlier Intel
screens retain the limitation; Sapphire Rapids full-width versus 256-bit relative
order did reverse, but its comparison against AVX2 did not. The separate native
Windows four-pair controller and direct interhash reversal are unaffected.
The order repair changed the controller; archived binaries and measured samples remained unchanged.

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

Two repeated, order-limited Ice Lake regression pairs compare the ARM-change
build against its separately rebuilt CPU-only baseline: approximately −0.29%
at one worker and −0.27% at twelve. Each worker count kept the same variant order
in both passes. These small negatives and that limitation are retained explicitly.
Full reference parity and native workspace tests passed.

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
not a full pool runtime test. PR #43 passed combined native pool startup and resource lifecycle checks on
Graviton4 and Xeon 6975P-C, but the later separate merge-subset AVX2 regression
returned it to draft; correctness did not clear the performance gate.
The final combined candidate `636e688` passes all-feature/all-target Linux
compilation and scoped Rust formatting. A separate read-only review by the
research owner found no blocker in nonce stride, teardown/retry, PID headroom,
manual bounds or platform-scoped resource errors. Native confirmation passed;
the reserve remains explicitly best effort against concurrent tasks.

The first full-pool limit of 24 tasks was insufficient even for v0.2.21: its
existing sysinfo/Rayon pool creates sixteen threads during startup discovery,
and the baseline then fails spawning the pool client. The candidate instead
reports insufficient headroom before provisioning. Both failed runs are retained;
the revised full-pool acceptance budget is 40 tasks, still below the combined
CPU-sized parked pool plus runtime requirement. No Rayon environment override
is used and product source remains frozen. Reducing startup's persistent Rayon
thread footprint is a separate future optimization.

The frozen final combined candidate `636e688` passed **495 CPU-only workspace
tests** (five resource-dependent tests ignored), full-size scalar/AVX2/AVX-512/256
parity, both explicit real-arena tests and strict kernel Clippy on Xeon 6975P-C.
Its complete nine-phase lifecycle passed, including restart growth to three
workers / 6.028 GiB anonymous residency, no OOM/quarantine and opt-out. The
40-task full-pool check provisioned nine CPU slots, observed at most 35 tasks
and 84 completed hashes before clean shutdown. Graviton4 also passed all nine
phases, with three workers / 6.02784 GiB after restart and no OOM/quarantine. Its
40-task check observed nine slots, 35 tasks and 54 completed hashes.

Corrected-order final x86 regression pairs at one/twelve workers show AVX2 mean
changes of +0.26% / −0.48% (the latter pairs range −1.13% to +0.16%) and full
AVX-512 +0.53% / +0.58%. These small differences are retained without claiming
an x86 kernel optimization. Both builds use the same CPU-only generic profile.
All sixteen reports passed actual-order verification; a deliberately partial
first-pass export was rejected without creating a confirmation artifact.

## Cross-architecture code-generation regression investigation

Cascade Lake exposed a repeatable approximately 3% full-width AVX-512 slowdown
in the combined `636e688` candidate at twelve workers. The same frozen binaries
were near-flat on Xeon 6975P-C. This is retained as a regression, not dismissed
because the ARM gains are larger.

Static inspection of the actual miner found that the ARM refactor's small
wrapper caused x86 fill loops to inline into the dispatch function. The enclosing
function grew from 1,780 to 32,130 bytes and hot-loop register allocation changed.
That size includes alternate ISA paths which are not all executed by one hash.
The AVX-512 compression instructions and referenced constants remained identical.

An isolated four-line change, `f20e7bb`, keeps the x86 fill wrapper out of line
without applying that attribute to ARM. The resulting dispatch and all four
fill functions match the baseline's normalized instruction sequences and sizes.
See [static evidence](evidence/x86-codegen-review.json). Static equivalence does
not establish performance recovery by itself. The completed four-pair native
comparison measured 9.303 / 9.036 / 9.267 H/s respectively: the fix consistently
beats the regressed build while retaining a roughly −0.39% baseline-relative mean.
The ARM check is near-flat (sixteen-worker pairs −0.712% and +0.142%). The fix
is now incorporated in the ARM PR. Corrected final Graviton2/3 confirmation
passed. Fresh final-binary Xeon 6975P-C results retain a negative signal:
two short full-width pairs at twelve workers were −0.271% and −1.274%; four
longer balanced pairs were −0.063%, −0.071%, −3.719% and −1.480%. The longer
means are 12.486 versus 12.324 H/s (−1.304%). Both sides show throughput drift;
these measurements do not establish a stable effect size or isolate the causal
change within the combined stack. They cannot be discarded: actual order,
executable identities, 24 GiB backing, zero errors and full-size parity passed
independent audit. Normalized instruction equivalence did not establish runtime
performance equivalence. **PR #44 remains draft pending investigation.**

The separate combined integration branch includes PRs #40, #41, #43, #44 and
#45 plus this candidate fix. It passes 497 workspace tests (five ignored),
all-feature/all-target Linux compilation, Windows GNU test cross-compilation,
scoped formatting and strict kernel Clippy. Cross-compilation is not native
Windows execution. [Evidence](evidence/integration-check.json) retains log hashes.

## Final merge and experiment disposition

The user subsequently authorized merging the good PRs. Two production changes
were merged into `master`, followed by fresh verification of that exact source
(`0e6b43e`): **479 CPU-only workspace tests passed, four ignored**.

- **Merged [#40](https://github.com/zpalmtree/seine/pull/40):** Windows tuning-cache
  identity includes permitted CPUs and canonical physical-core groups. Native
  mask tests passed. Existing physical-first placement is retained; no new CCD
  preference is introduced.
- **Merged [#45](https://github.com/zpalmtree/seine/pull/45):** native CPU-only Linux
  ARM release packaging, with kernel tests and full-size verification in the
  workflow. Native source/package/digest/YAML checks passed. The archive requires
  glibc 2.39, with a documented source-build fallback. The hosted release workflow
  has not been executed and no release was published.
- **Draft [#41](https://github.com/zpalmtree/seine/pull/41):** retry inconclusive
  kernel comparisons without caching a false loss. Its original 479 tests and
  independent source review passed, but standalone native Ice Lake AVX2 pairs
  lost 1.011% and 0.821%. This gate supersedes its earlier review-ready status.
- **Draft [#43](https://github.com/zpalmtree/seine/pull/43):** recover worker
  capacity after temporary RAM limits while respecting PID headroom and manual
  bounds. Native CPU-load, pressure, zero-worker parking, restart recovery and
  opt-out checks passed on ARM and x86. The proposed #40/#41/#43/#45 subset passed
  496 tests (five ignored), all-feature compilation, scoped formatting and native
  full-size parity, yet its twelve-worker AVX2 pairs lost 2.277% and 1.930%.
  Correctness does not establish performance neutrality. Standalone #41 also
  loses throughput, so these comparisons do not isolate the causal change to #43.
- **Draft [#44](https://github.com/zpalmtree/seine/pull/44):** runtime ARM
  SHA3/prefetch candidate. Corrected native ARM confirmation shows repeatable
  gains, but combined-stack Intel regression signals and held prerequisites
  prevent integration. Startup remains portable NEON; runtime feature detection
  admits the challenger, and productive-work confirmation is required before
  automatic selection. Existing opt-out remains available.
- **Draft [#46](https://github.com/zpalmtree/seine/pull/46):** joint count/kernel
  prototype. It passed 506 local tests (five ignored), full-size parity and build
  checks. Four 900-second fresh-process/cache ABBA windows on actual Skylake
  Xeon 8124M produced −2.006% and −0.836% paired logged-work differences, aggregate
  −1.424%. No alternate kernel was promoted; final worker counts matched controls.
  About 197 extra seconds per candidate window went to productive kernel trials.
  A cooldown could skip comparison at a candidate count. The subsequent prototype
  lifecycle was intentionally interrupted to prioritize final Intel checks;
  raw `FAILED / InterruptedError(15)` and scope decision remain, with successful
  cleanup. It establishes neither a product failure nor a lifecycle pass.

The standalone retry isolation used the same guest after the proposed-subset
matrix, fresh isolated binaries and actual reverse-order confirmation. The
[accepted-subset evidence](evidence/icelake-accepted-final-confirm.json) and
[retry-only evidence](evidence/icelake-retry-only-confirm.json) retain every pair.
Static inspection found matching normalized hash/compression instructions in the
subset, but code placement differed in a worker wrapper; no added steady-state
work or changed fixed-benchmark nonce stride was found. Static similarity cannot
clear the native performance gate. Source-level attribution remains open.

The final archive controller initially matched a third, separately named retry
parity log and failed its exact-two-logs export assertion. Native matrices had
completed and all archive files verified. Explicit control/candidate filenames
recovered the parity export; the original controller error and duplicate failure
archive remain. This was an exporter error, not a native test failure.

All proposed runtime changes preserve productive startup and automatic defaults
with `--disable-cpu-autotune-threads` as opt-out. Explicit fixed counts retain
the startup ISA unless tuning is explicitly enabled. No new PoW, GPU default,
automatic 256-bit AVX-512, Intel streaming-ring or interhash default was merged.
Interhash was rejected against the stronger equal-RAM SMT control; new CCD policy
was inconclusive. Intel ring/narrow kernels did not justify promotion.

Strict repository-wide Clippy remains limited by existing diagnostics (124 in
the final combined check; none on changed lines). Strict kernel Clippy passed.
The larger combined integration passed 497 tests (five ignored), all-feature
Linux compilation and Windows test cross-compilation. Cross-compilation is not
native Windows execution. GitHub returned no hosted check results; local/native
evidence is identified explicitly.

Subsequent user-requested investigations continue separately: draft
[#47](https://github.com/zpalmtree/seine/pull/47) addresses the joint cooldown gap,
and draft [#49](https://github.com/zpalmtree/seine/pull/49) isolates ARM enum/CLI
changes. Neither has native performance evidence sufficient for promotion.
Their work does not change this campaign's held gates or cleanup scope.

## Coordinated EPYC evidence

The initial EPYC investigation released its cloud resources before this
campaign acquired the shared 32-vCPU quota. On its older base `785a06f`, a 64 KiB
streaming ring on EPYC 9R45 at 24 workers measured 45.143 H/s versus 35.328 AVX2,
with three alternating pairs (+27.34% to +28.42%), full-size parity, dirty reuse
and sentinel checks. One-worker and eight-worker screens did not establish a
benefit. The candidate still needs a distinct production identity and applicable
worker-switch validation before promotion. No DRAM-bandwidth claim is inferred
from its available performance counters.

## Remaining hardware and experience gaps

- Final-candidate native Zen 3 and Zen 4/5 EPYC validation remains separate work.
  Existing coordinated results use older sources and are not relabelled as
  fresh measurements of this campaign's frozen combined candidate.
- The 9950X3D campaign used regular pages and at most eight workers. Native
  Windows large pages, higher worker counts, final combined-binary performance
  and machines spanning Windows processor groups remain unverified.
- Apple Silicon, Intel hybrid P/E-core machines, pre-AVX2 legacy x86 and
  multi-socket NUMA systems need native candidate measurements. Cloud results apply to the observed guest
  topology; they do not establish bare-metal bandwidth or frequency behavior.
- The separate joint-count/kernel prototype still needs repeatable end-to-end
  benefit across more than one architecture before becoming a default.
  Its fixed-kernel results alone cannot establish that policy benefit.
- Startup discovery's persistent Rayon thread pool remains an opportunity to
  reduce overhead and support tighter PID budgets. The campaign's 24-task
  failures affected the baseline too; they were not hidden by an environment
  override.

GPU, a new PoW and energy measurements were intentionally outside this campaign.

## Resources and completion

The first fresh final-regression C8i rental encountered an SSH connection timeout
before creating its campaign directory or running a benchmark. Its instance and
disk were verified deleted, and its charges remain in the ledger. The replacement
uses bounded retries only for proven pre-connection timeouts; command execution
errors are never replayed. This is an operational failure, not a benchmark result.

The campaign began at **10:27:13 UTC**, froze new experiments at **17:57:13**,
and reserved the final 2.5 hours for confirmation, review, documentation and
cleanup within the **20:27:13** finish bound. After the later explicit merge
instruction, one additional accepted-subset/isolation lease received an
independent **20:20** deadline; the $40 cap and ten-hour finish bound were unchanged.
It was terminated early at 20:12:58. All other rentals had deadlines no later than 20:00.

**All 18 campaign rentals and their known root disks were verified deleted.**
Final cloud cleanup at 20:13:06 also verified the campaign security group absent
and removed local SSH keys/known-host material. Known disk IDs were checked
independently of tags. Native Windows cleanup verified zero owned processes,
removed its temporary directory and retained 450 verified text artifacts. All
campaign test units/controllers ended, and 15 disposable build-target directories
were removed. Committed source worktrees, frozen binaries and raw evidence are
intentionally retained for review; unrelated resources were untouched.

Live-price-based estimated compute, storage and IPv4 cost is **$12.02**.
Including the retained **$18 transfer contingency**, the conservative estimate is
**$30.02**. The maximum recorded reservation commitment was **$38.82**, below the
**$40 cap**. These are estimates/reservations, not an AWS invoice. Unused reserved
hours were released only after both termination and disk deletion proof.
See [cost/cleanup evidence](evidence/cloud-costs-cleanup.json),
[local cleanup](evidence/local-cleanup.json), [Windows cleanup](evidence/windows-cleanup.json)
and [merged-source tests](evidence/merged-master-validation.json).

After quota release, the separately requested EPYC implementation/validation
continues under its own ownership and deadlines. Those later resources and the
separate regression investigations are outside this campaign's 18-rental ledger.
