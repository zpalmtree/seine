# CPU Optimization Log

This log tracks CPU backend/hash-kernel tuning attempts and measured outcomes.

## Conventions

- Attempt numbers are historical labels and may repeat across platform-specific sections (`x86_64` vs `Apple`).
- The canonical roll-up is in `Updated summary of cumulative adopted optimizations` near the top of this file.
- Short-window probes are directional unless explicitly confirmed with long or interleaved A/B runs.
- For ongoing maintenance, add new sections in newest-to-oldest order.

## Newest-first index

- `2026-10-24 joint count/kernel crossover prototype (native validation pending)`
- `2026-10-24 parked CPU worker task budgets and spawn failure recovery`
- `2026-10-24 automatic joint CPU kernel and worker selection`
- `2026-10-24 stable efficiency selections during productive rechecks`
- `2026-10-24 productive CPU autotuning and changing resource budgets`

- `2026-10-24 cross-generation CPU kernel matrix`
- `2026-10-24 EPYC 9R14 256-bit AVX-512 kernel experiment`
- `2026-10-24 explicit CPU kernel benchmarks and architecture identity`
- `2026-10-24 CPU sampler panic handling`
- `2026-10-24 CPU autotuner warmed sampling and lifecycle timing`
- `2026-10-24 independent two-node Milan release validation`
- `2026-10-24 Intel AVX2 release validation`
- `2026-10-24 two-node EPYC placement and autotuner validation`
- `2026-10-24 automatic Linux L3 placement: worker-only EPYC validation`
- `2026-10-24 EPYC 8534P hourly rental: cache placement, threads, and page backing`
- `2026-10-23 Linux NUMA defaults and automatic RAM headroom`
- `2026-10-24 AVX2 paired rounds and contiguous column loads/stores`
- `Updated summary of cumulative adopted optimizations`
- `2026-07-11 1 GiB explicit hugepages on WSL Zen 5 (adopted, pending boot pool)`
- `2026-07-11 verifiable page modes, native Windows, and WSL HugeTLB confirmation`
- `2026-07-10 Apple Silicon scheduler, SME2, PGO, and autotuner follow-up`
- `2026-07-10 cross-host affinity, memory-pressure, and native Windows validation`
- `2026-03-13 Zen 5 AVX-512 column gather/scatter + MaybeUninit`
- `2026-03-09 Zen 5 hugepage reservation + AVX-512 vpermq retest`
- `2026-03-09 Zen 5 (Ryzen 9 9950X3D) AVX-512 dispatch audit + native retest`
- `2026-02-18 Apple Silicon superpage arena + pcore-only affinity trial`
- `2026-02-18 Apple Silicon arena residency + shared-arena trials`
- `2026-02-18 Apple Silicon hot-path revisit + build/runtime retests`
- `2026-02-17 Apple Silicon long-window confirmations + perflevel affinity cap trial`
- `2026-02-17 Apple Silicon cache-line alignment + DI L2 prefetch revisit`
- `First-principles performance analysis (x86_64, Zen 3)`
- `2026-02-16 x86_64 mid-compress prefetch, TLB analysis, and huge page allocation`
- `Metal + CPU combined mode (Apple M4 Max, 48 GB unified memory)`
- `2026-02-15 Apple Silicon further investigation (post mid-compress prefetch)`
- `2026-02-15 Apple Silicon mid-compress prefetch + madvise investigation`
- `2026-02-15 x86_64 cross-porting Apple Silicon optimizations`
- `2026-02-15 Apple Silicon continued optimization`
- `2026-02-14 x86_64 fused column-scatter optimization`
- `2026-02-14 Apple Silicon prefetch tuning + PGO investigation`
- `2026-02-14 Apple Silicon (AArch64) NEON SIMD optimization`
- `2026-02-14 in-place compress to eliminate memcpy ABI overhead`
- `2026-02-14 AVX2 SIMD rewrite + thermal-stable interleaved A/B harness`
- `2026-02-14 additional miner/backend/kernel rewrite pass (A/B with detached baseline)`
- `2026-02-14 full miner + CPU backend rework sweep`
- `Attempted changes` (Attempts 1-16 and cross-platform setup)
- `Validation`
- `Baseline`
- `Benchmark protocol`

## 2026-10-24 joint count/kernel crossover prototype (native validation pending)

Status: isolated implementation candidate on
`codex/joint-crossover-campaign-20261024`, based on `636e688`; **not merged or
native-validated**. This does not change the architecture campaign's frozen
binary or its separate performance-regression investigation.

The existing policy chose a kernel only while the count policy was at its
baseline. A proposed count that lost on the incumbent path was rejected before
its alternate kernel could be tested. Wider count probes could therefore remain
trapped at eight/full512 even when twelve/AVX2 was faster. Commit `2aefacb` records
a regression that fails against `636e688` with “candidate count was rejected
without testing its alternate kernel.”

The final fixture models 8/full512 at 9.72 H/s, 12/full512 at 9.28 H/s and
12/AVX2 at 10.23 H/s, using integer completed hashes and fractional carry so the
normal count uncertainty margin remains meaningful. These are synthetic rates
representing the shape observed by the C5 campaign, not new hardware results.

The prototype allows established-kernel selection at a proposed count before
sampling that count. It captures the original count, kernel and backing, then
restores that complete baseline for confirmation. Count sampling waits for kernel
acknowledgements. If the winning combination changes the kernel as well as the
count, a second fresh configuration A/B/A must confirm it before publishing the
count hint. The first gain cannot raise the retained profile floor. Fully
confirmed per-count kernel evidence can still be cached independently; a restart
reuses that evidence but must repeat the count comparisons from scratch.

Pressure, allocation failure, interrupted or inconclusive kernel trials, changed
baseline backing, inconsistent repeated candidate backing, or a joint deadline
discard the comparison and restore the original kernel at a safe count. The
default joint deadline is 15 minutes, extended to at least six count windows
plus ten minutes when a longer count window is configured. Kernel-switch waits
have a ten-minute bound. Existing memory recovery/allocation backoff, profile
floors, opt-out, min/max constraints and fixed nonce stride remain in effect.
No worker backend, hash kernel, consensus, nonce allocation or startup path was
edited. This remains heuristic count exploration, not an exhaustive global
configuration search. New contexts require additional productive trial time;
the initial connection still has no offline sweep.

Local validation with Rust 1.93.0:

- `cargo +1.93.0 test --workspace --no-default-features --quiet -- --test-threads=4`:
  **503 passed** (496 root-binary, seven library tests), five opt-in tests ignored.
- Regression coverage includes the crossover, a gain that disappears on its
  second comparison, restart after the first gain, delayed acknowledgements,
  changed baseline backing, reduced/zero RAM capacity, allocation failure,
  interrupted inner trials, deadlines and explicitly long count windows.
- Existing multi-hour synthetic profile/recovery and small-arena worker/nonce
  tests continue to pass.
- Windows GNU CPU test compilation passed:
  `cargo +1.93.0 check --no-default-features --target x86_64-pc-windows-gnu --tests --quiet`.
  This is a compile check, not native Windows execution.
- Changed Rust files pass pinned formatting; `git diff --check` passes.

Native validation remains with the architecture campaign: exercise automatic
8/full512 to 12/AVX2 selection on the crossover host, verify repeated confirmation
and productive hashes through restores, and assess count/kernel trial overhead
alongside the independent frozen-build regression. No native mining, full-size
arena test, cloud rental, PR or merge was performed by this implementation task.

## 2026-10-24 parked CPU worker task budgets and spawn failure recovery

The memory-recovery change preserved CPU-sized parked lanes but also eagerly
created an OS thread for every lane. The architecture campaign reproduced the
resulting startup regression from `fbe9e86`: with affinity restricted to 16 CPUs,
`memory.high=3 GiB`, and `pids.max=12`, the legacy one-slot backend starts while
the 16-slot backend fails creating worker 9 with EAGAIN. This is backend startup
evidence, not a full pool-session or performance measurement.

This follow-up separates the CPU/user search maximum, startup worker slots, and
the RAM-safe manual/offline limit. Linux cgroup-v2 PID headroom takes the minimum
remaining budget across ancestors. The startup reserve accounts for event and
control threads, both user/dev pool clients, HTTP runtimes, optional TUI/API/daemon
threads, plus explicit DNS/driver margin. Already-running API Tokio workers are
included in `pids.current`; they are not reserved a second time. This remains a
best-effort preflight, not a guarantee against competing tasks or driver threads.
Zero residual capacity reports a clear error instead of forcing one worker.

Automatic CPU worker creation also handles resource exhaustion after preflight:
join and release the failed attempt, then retry at half the successfully spawned
count (at least one). Attempts strictly decrease; an inability to create the
first worker returns an error. Explicit fixed counts and non-resource errors
remain failures. The original nonce stride never changes; the controller caps
active workers to actual provisioned slots. Restarting can provision more slots;
the saved higher count and CPU-only cache maximum survive the restriction.
Manual TUI requests carry their live RAM/worker limit to the supervisor so its
pre-start RAM estimate cannot widen a reduced limit or reject later recovery.

Validation in this implementation worktree: 485 root-binary CPU-only tests passed, five
hardware/resource-dependent tests ignored; Linux all-feature compile check passed.
Windows GNU CPU-only cross-compilation, including the tests, also passed.
Injected spawn failures exercise real small-arena workers, cleanup, manual-count
failure, recovery, park/resume, nonce uniqueness and fixed-stride coverage, plus
cancel/fence completion. Filesystem fixtures cover nested/sibling PID usage,
unlimited levels and invalid/missing usage. Policy/manual-control tests cover
bounded probing, unchanged cache maxima, and stale supervisor limits.

Windows portability follow-up: the spawn retry classifier also recognizes
155/164 (thread limits) and 1451 through 1455 (pool/quota/commit exhaustion), in
addition to 8/14/1450. Values are checked against Microsoft's
[0–499 error codes](https://learn.microsoft.com/en-us/windows/win32/debug/system-error-codes--0-499-)
and [1300–1699 error codes](https://learn.microsoft.com/en-us/windows/win32/debug/system-error-codes--1300-1699-).
Two additional pure tests run on Linux and cover supported Windows codes,
non-resource failures, and keeping Windows raw-code interpretation Windows-only.
The 485-test figure above excludes these new tests and the workspace libraries'
seven tests; combined/native confirmation remains with the architecture campaign.

Native confirmation is owned by the architecture campaign. The ignored
`miner::adaptive_cpu::tests::pid_budget_startup_probe` keeps its existing controller
interface (`SEINE_PID_PROBE_LEGACY`, `--expect all-pass`). Its candidate deliberately
bypasses PID preflight to exercise OS spawn failure and retry under `TasksMax=12`;
it logs provisioned slots and the unchanged stride. A full startup/preflight test
needs a larger budget for networking/control (for example, 24 tasks with 16 CPUs
in plain CPU-only pool mode). A 12-task backend-only pass must not be presented
as proof that the full pool/API runtime fits. No release or hardware run was
performed by the implementation session.

## 2026-10-24 automatic joint CPU kernel and worker selection

Automatic mining tuning now serializes established AVX2/AVX-512 kernel trials
with adaptive worker-count trials. The widest supported ISA remains the startup
fallback. Two productive A/B/A comparisons must each exceed a 2% gain plus hash
quantization, with baseline drift capped at 5%, before promoting another path.
Windows run 30–60 seconds and require 32 completed hashes; a trial has a ten-minute
hard limit. Interrupted comparisons return to the prior path. ARM/NEON and
AVX2-only hosts do not spend time testing scalar alternatives. Experimental
`avx512-256` remains benchmark-only.

Workers switch only between complete hashes, retaining their arenas, precomputed
reference addresses, assignments, and nonce cursors. Per-worker generation
acknowledgements include a completed hash and counter flush before measurement.
Kernel choices and their raw sample counts/times are cached per worker-count and
observed backing vectors, with CPU/build/topology/configuration identity. Stable
contexts are revisited after 30 minutes. The existing CPU autotune opt-out also
disables automatic path selection; fixed-thread and offline benchmark behavior
remain explicit and deterministic.

Integration review caught two regressions before handoff. Resetting the joint
peak/search direction on cached path changes could cycle efficiency mode between
low- and high-count winners; the joint profile frontier and search direction now
survive confirmed path changes while count-specific reference samples reset.
Clearing scores also preserves a higher cached count when current RAM limits a
run to fewer workers. Four-hour stable-load simulations assert every later
confirmed selection, rather than checking one endpoint.

Validation on Ryzen 9 9950X3D:

- Linux CPU workspace: **477 tests passed**, four resource-heavy tests excluded
  from the normal suite. The new full-size test was then run separately.
- Optimized one-worker **2 GiB** live AVX2 → AVX-512 → preferred-path smoke passed
  in 10.96 s: stable arena accounting, no allocation failures, distinct sampled
  nonces, and all three outputs matching RustCrypto. Reference allocation followed
  worker teardown so peak arena use stayed at 2 GiB. This is correctness evidence,
  not a throughput comparison.
- **35 native Windows GNU tests passed**, covering adaptive policies, hash-boundary
  switching, request guards, and reference parity. Windows all-target cross-check
  and default-feature all-target check also passed.
- ARM/macOS pow-kernel cross-check passed. Full macOS application cross-check was
  unavailable because this Linux environment lacks a Darwin C cross-compiler;
  native macOS runtime validation remains outstanding.
- Formatting, diff checks, and Clippy completed; existing repository warnings
  remain. No AWS instances or other external benchmark resources were created.

These checks establish correctness and simulated convergence, not an all-host
hashrate gain or global optimality. The earlier architecture matrix motivates
count-dependent path selection; actual gains are decided on each mining host.

## 2026-10-24 stable efficiency selections during productive rechecks

- Review of the merged productive controller found that efficiency mode could
  repeatedly commit higher counts even when its current count already delivered
  at least 75% of the tracked peak. A four-hour deterministic simulation using
  rates `[0,24,48,72,96,112,124,132,136]` reproduced a confirmation of seven workers
  at 1,859 seconds, although five workers met the profile floor. The previous
  endpoint-only test missed the repeated climb and descent.
- Initial exploration still discovers the peak. Once discovery completes,
  upward acceptance also requires the current count to fall below the profile
  floor. Probes continue, and a genuinely new peak can queue a farther probe
  while retaining the lower operating count between measurements. CPU-rate
  changes, unstable comparisons, and returning RAM reopen discovery.
- Recovery tests cover normal/load/normal operation selecting 5→2→5 workers,
  including a load change that leaves the two-worker rate unchanged, plus a RAM
  ceiling growing from two to eight workers. These check every confirmation
  after settling, not just the final count. The steady-load regression checks
  repeated confirmations over four simulated hours.
- Validation: CPU-only workspace suite **465 passed**, three opt-in tests
  ignored; default-feature all-target check, Windows GNU CPU-only cross-check,
  scoped formatting, and Clippy completed. Clippy retains the existing 61
  binary/63 test warnings, with none in the edited policy module. This follow-up
  changes only selection policy; the prior full-size lifecycle validation below
  was not repeated. Logs are retained locally under
  `data/adaptive-autotune-20261024/efficiency-cycling/`.

## 2026-10-24 productive CPU autotuning and changing resource budgets

- Automatic mining previously ran an offline startup search and kept its selected
  count for the session. Temporary CPU load or low available RAM could therefore
  determine the operating count long after conditions improved.
- Mining now starts from a compatible cached count, or half the stable automatic
  capacity, capped by current RAM. Productive baseline/candidate/baseline trials
  measure completed hashes while the miner remains connected. Every arena warms
  before measurement; each window takes at least 30 seconds and 20 hashes.
  Baseline drift above 8%, sustained work loss, and RAM changes invalidate trials.
  Balanced/efficiency floors remain 99%/75% of the tracked peak; upward changes
  require a gain exceeding 2% plus hash-count quantization. Periodic local and
  occasional wider probes revisit changing CPU load without a new startup sweep.
- CPU lanes keep fixed nonce strides and individual cursors. Disabled workers
  release their arenas and reference tables; survivors keep theirs. Zero enabled
  workers is supported. Recovery waits for 30 seconds of headroom; failed arena
  allocations wait five minutes and do not quarantine healthy workers. Explicit
  CLI/API/TUI counts retain manual control unless automatic tuning is requested.
- Budgets include allocations in progress as well as resident arenas. A real
  two-worker pilot exposed a race where first-touch RAM was initially attributed
  to other applications, causing the new worker to be immediately removed.
  A blocked-allocator regression now verifies the credit before backing appears.
  CPU/user ceilings are kept separate from RAM headroom for pressure decisions.
- Linux reads the actual cgroup-v2 membership and inherited `memory.high` and
  `memory.max`, crediting clean inactive file cache. The pinned sysinfo version
  only checked the mount root. The ordinary tool process's 4 GiB `memory.high`
  correctly permits only one 2 GiB arena after reserve/overhead. Linux PSI prefers
  the miner's own cgroup to avoid reacting to unrelated host stalls; system PSI is
  the fallback. Pressure and Windows low-memory notifications require three
  consecutive samples and tight headroom before shedding workers; byte-budget
  reductions apply immediately.
- Schema 11 stores mining observations and the resource-limit flag. Compatible
  schema-10 records remain startup seeds. A temporarily lower RAM ceiling retains
  a previous higher selected count on disk. Offline kernel and live mining scores
  remain distinct; hardware/build/topology/page-mode compatibility still applies.
- Validation after integration with v0.2.20: CPU-only workspace suite **462 passed**,
  three opt-in tests ignored;
  default-feature all-target check, Windows GNU CPU-only cross-check, scoped
  formatting and Clippy completed (existing warnings retained). Deterministic
  simulations cover a peak changing 8→2→8 workers, noncompounding efficiency
  selection, interrupted trials, constrained-cache preservation, and recovery.
  Real small-arena workers cover nonce uniqueness, allocation failure, parking,
  new work while parked, cancellation, resumed accounting, and bounded shutdown.
- An optimized full-size backend lifecycle smoke after v0.2.20 integration
  (at most 4 GiB of arenas) passed 1→2→0→1 resizing: startup **0.808 s**, first
  two hashes **2.036 s**, release **0.844 s**, 12 completed hashes.
  These are local lifecycle observations, not a
  cross-host throughput improvement claim. Native Windows/macOS runtime testing
  remains outstanding; the macOS cross-check lacked an Apple C toolchain/SDK.
- Before integration, the live controller completed 445 hashes while responding to pressure and
  discarding an unstable comparison, then confirmed two workers after 223.614 s.
  After stopping the first backend, cached preparation reused that count in
  **0.0112 s**. The run used a bounded scope under `agents-tool.slice` with
  `MemoryHigh=6G`, `MemoryMax=8G`, and `CPUQuota=200%`; arenas remained at most
  4 GiB. The integrated rerun using the shipping `auto` affinity completed
  **192 hashes** and confirmed two workers after **94.935 s**, then reused the
  cache in **0.0112 s**. Both full-size opt-in tests passed on the integrated code.
- Local evidence is retained under ignored `data/adaptive-autotune-20261024/`,
  including failed/interrupted pilots. No cloud resources or GPU jobs were used.

## 2026-10-24 cross-generation CPU kernel matrix

- Real x86-64 EC2 guests cover AMD Zen 1/Naples, Zen 2/Rome, Zen 3/Milan,
  Zen 4/Genoa, Zen 5/Turin, and Intel Broadwell, Cascade Lake, Ice Lake,
  Sapphire Rapids and Granite Rapids. Full 2 GiB reference parity runs on
  every host before timing. An ISA override on a newer CPU is not used as a
  substitute for an older architecture.
- AVX2-only hosts compare pre-pairing against paired AVX2; AVX-512 hosts
  compare two explicit paths in one portable binary. Fixed workers, page
  backing, affinity and disabled autotuning isolate each comparison.
  Four alternating pairs use a warmup plus two measured rounds per invocation.
  The main matrix uses 12-second rounds; independent Turin and larger Genoa
  followups use 20 seconds. Completed H/s includes round-end fence time.
- Full protocol, generation/CPUID identity, rates, paired intervals, source and
  executable hashes, followups, limitations and cloud cleanup are recorded in
  [the architecture matrix](experiments/cpu-architecture-20261024/README.md).
  Its machine-readable summary preserves counted and completed rates separately.
- Paired AVX2 improves every tested AVX2-only configuration (+3.2% to +34.4%).
  Existing AVX-512 improves every measured Intel case (+6.1% to +28.7%). Genoa
  favors AVX2 across the measured one/four/eight/twelve-worker configurations.
  Turin repeats a worker-count crossover on a second VM: AVX-512 gains about
  15% at one HugeTLB worker but loses 1.7% and 2.6% at four. The larger Genoa
  guest has two L3 groups and fixed `os` placement; production `auto` placement
  differs. These results do not justify a blanket generation-specific default.
- `avx512-256` integrates the cooperating EPYC experiment under a distinct,
  explicit benchmark label. AVX2 + AVX-512F/VL and OS vector support are required.
  It is never automatically selected. The module is unchanged from its full-size
  Genoa/local reference checks; the rejected row-fusion prototype is not included.
- Integration preserves the upstream warmed sampler and panic barriers from
  `2ed5beb`. After that merge: 438 Rust tests passed, one existing 1 GiB HugeTLB
  test remained ignored, and 10 architecture-analysis tests passed. Full-size
  one-worker CLI smoke checks passed for automatic and explicit SIMD backends,
  all four benchmark modes for the new path, and cross-kernel baseline rejection.
  Default-feature compilation and CPU-only Clippy passed with existing warnings.
  Earlier kernel/test cross-checks passed for Windows x86-64 GNU and macOS
  AArch64; those operating systems were not executed in this matrix.
- Normal ISA preference and CPU autotune policy remain unchanged. Existing
  cache keys include the full build fingerprint, so integrating kernel/source
  changes invalidates old tuning observations without a separate schema bump.
  The new benchmark override is never persisted into a tuning cache.

## 2026-10-24 EPYC 9R14 256-bit AVX-512 kernel experiment

- Added an isolated 256-bit AVX-512 compression module (`7cf7981`) and handed it
  to the architecture session, which integrated explicit benchmark selection as
  `avx512-256` in `ac4feba`. No automatic kernel preference or autotuning policy
  changes are made by this experiment.
- Host: EC2 c7a.2xlarge, AMD EPYC 9R14, AuthenticAMD family 25/model 17/stepping 1,
  eight physical cores, one NUMA node and one L3 group. Portable Rust 1.93.0
  CPU-only release builds, fixed workers, verified page backing, autotuning off.
- The candidate combines paired 256-bit rows/columns, native AVX-512VL rotates,
  and data-dependent prefetch after column zero. It requires AVX2 + AVX-512F/VL.
  The disassembly uses no ZMM operations or high YMM registers; no speedup is
  attributed to an expanded register file.
- Three alternating pairs, each with one warmup and two measured 20-second
  backend rounds, found these completed-hash gains against existing AVX-512:
  **+5.71% at one HugeTLB worker**, **+5.53% at four HugeTLB workers**, and
  **+1.86% at four ordinary-page workers**. Every pair improved.
- The necessary same-binary AVX2 control changes the interpretation: the new
  path beats paired AVX2 by **+2.13% at one HugeTLB worker**, but only **+0.27%
  at four**, with mixed pair signs (+0.34%, -0.12%, +0.58%). A useful four-worker
  advantage over AVX2 is not established by that comparison.
- Native integrated confirmation checked all three SIMD paths at four workers
  with two opposite-order passes. AVX2 beat existing AVX-512 by +4.07% with
  HugeTLB and +2.06% with ordinary pages. The new path was +1.30% / -0.08%
  relative to AVX2. These shorter checks confirm integration and the weaker
  existing AVX-512 default, but do not establish a broad generation preference.
- Both experimental builds and the distinct-label integration passed full 2 GiB
  RustCrypto parity on the EPYC host. The module's scalar-comparison tests cover
  shared inputs, edge/random words, ordinary/mid-prefetch paths and guarded
  outputs. Six kernel tests pass; Windows x86-64 GNU and macOS AArch64 checks
  compile. The architecture session separately reports 432 workspace tests
  passed, one existing 1 GiB HugeTLB test ignored, and strict kernel Clippy.
- No memory-backing failures, backend timeouts, swapping or OOM events occurred.
  All paired results and the integrated check have checksummed local archives.
  A pilot stopped on a checker field-name error and is excluded. See
  `experiments/epyc-avx512vl/README.md`, `results.json`, source hashes and runners.
  Raw evidence is under `data/epyc-avx512vl`, `data/epyc-control` and
  `data/epyc-final` in the EPYC worktree.
- A second candidate fused the 256-bit input XOR/backup with row rounds to
  remove a scratch-buffer store/reload. It passed full-size reference checks but
  regressed four-worker HugeTLB throughput by **13.37%** in all three paired
  comparisons (7.40367 to 6.41361 completed H/s). It is rejected and retained only
  as an experimental patch/results, not in the kernel module.
- Scope: an eight-core guest with one/four-worker measurements, not a full EPYC
  socket or Siena/Zen 4c. The architecture session owns the subsequent 8/12-worker
  Genoa check and coordinates any cache/default change with the autotune session.

## 2026-10-24 explicit CPU kernel benchmarks and architecture identity

- Added benchmark-only `--bench-cpu-kernel scalar|avx2|avx512|neon`. Selection
  is passed per backend, requires fixed threads and CPU-only benchmarking with
  autotuning disabled, and rejects unsupported paths before arena allocation.
  Normal mining and autotuning retain the existing automatic ISA preference.
- Schema-14 reports record selected/requested kernels, supported kernels and
  guest-visible x86 vendor/family/model/stepping. CPU baseline comparisons now
  require matching known kernel identities, including under ignore-environment.
  Auto and explicit selection of the same kernel remain comparable. The paired
  A/B harness supports intentional cross-kernel experiments; see
  `docs/benchmarks.md`. No generation-specific kernel preference is introduced.
- Validation host: Ryzen 9 9950X3D, WSL2, AuthenticAMD family 26/model 68/stepping 0.
  CPU-only generic release build, Rust 1.93.0; executable SHA-256
  `cfc9cda0a7ef16a1c7ba0a36f12f730b60c4fc4d2d92074bb58bfdd404668d27`.
- All 431 workspace tests passed; the existing test requiring a reserved 1 GiB
  HugeTLB pool was ignored. Supported explicit kernels match RustCrypto for
  three memory sizes and three nonces each, including a non-aligned memory size.
  Unsupported kernels fail before precomputation/allocation.
- Full 2 GiB, one-worker, regular-page CLI smoke runs completed for automatic,
  scalar, AVX2 and AVX-512 backend selection; AVX2 also completed kernel,
  kernel-effective and end-to-end modes. CLI restrictions, same/different-kernel
  baseline behavior and unchanged autotune-cache contents/mtime were checked.
  These two-second rounds validate plumbing, not relative throughput.
- CPU-only Clippy and default-feature compilation passed (existing Clippy
  warnings remain). Kernel/tests cross-checks passed for Windows x86-64 GNU and
  macOS AArch64; neither target was executed. Edited-file formatting passed.
- Local evidence and the CLI validation runner are in ignored
  `data/kernel-selection-validation/`. No AWS resources were provisioned.

## 2026-10-24 CPU sampler panic handling

- Review of the warmed sampler (`f35e6c1`) found that a lane panic during
  measured hashing strands its peers at the finish barrier. An injected panic
  reproduced the hang; a five-second process timeout was required to stop it.
- Setup/warmup and measured hash actions now convert unwinding panics into
  lane-specific errors, allowing every worker to reach the synchronization
  barriers. Sampling rejects the failed measurement. Normal score calculation
  and warmup policy are unchanged.
- A bounded regression covers panics during setup, warmup, and measured hashing;
  errors preserve the failing lane, phase, and panic message. The test fails
  after five seconds if a barrier stops releasing workers. Integrated with the
  v0.2.19 source and cancellation coverage, 428 CPU-only workspace tests pass,
  with one pre-reserved 1 GiB HugeTLB test ignored. Default-feature compilation and
  CPU-only Clippy pass with existing warnings. Scoped Rust 1.93.0 formatting
  and `git diff --check` pass.
- Reproduction and validation logs are retained in
  `data/sampler-review/` in the sampler review worktree. This is failure-path
  validation; the warmed sampler's performance comparison is recorded separately.

## 2026-10-24 CPU autotuner warmed sampling and lifecycle timing

- Prior two-node EPYC validation found a fresh tune selecting 10 workers while
  an alternating 10/12-worker comparison favored 12 by 4.42%. Its trace stopped
  the ascending search at 11 workers: arena setup and teardown were included
  in the short-sample denominator, and first-touch hashing was not warmed.
- Candidate `f35e6c1` keeps the search thresholds, profile policy, memory cap,
  page policy, and affinity unchanged. Each worker allocates after pinning and
  completes one warmup hash before a shared measurement start. Scoring uses
  completed hashes divided by elapsed time through the last worker's actual
  completion, before a final barrier releases any arena for teardown. Warmup
  hashes are excluded; last-hash overrun is included. Total lifecycle time is
  logged separately.
- The generic kernel benchmark retains its full-wall accounting and no added
  warmup; warmed timing is used only for CPU autotune. Sampling propagates
  setup/hash errors, releases all lanes on setup/warmup failure, and skips
  incomplete samples on shutdown. Schema 10 invalidates old autotune scores.
- Local validation after integrating current mainline: 425 CPU-binary tests
  passed, one pre-reserved 1 GiB HugeTLB test ignored; default-feature compilation and CPU-only Clippy passed with
  existing warnings. Tests cover excluded warmup/lifecycle time, included
  last-hash overrun, barrier release on setup/warmup failure, cancellation,
  weighting confirmation scores by hashing time instead of lifecycle time, and
  releasing barriers after panics in setup, warmup, or measured hashing.
- Native comparison uses identical portable Rust 1.93.0 CPU-only builds apart
  from this sampler change, baseline `cb682a8` and candidate `f35e6c1`, on an
  EC2 `c6a.8xlarge` EPYC 7R13 guest with two NUMA nodes, 16 physical cores,
  32 vCPUs and 64 GiB RAM. Artifacts, exact runners and binary hashes are kept
  in `data/autotune-steady-20261024/`. A 12-worker warmup preconditions the host;
  three fresh-cache pairs alternate old/new order, with ordinary pages,
  automatic affinity, default balanced profile and six-second sample windows.
  Each selected count is checked with one warmup and two 20-second backend
  rounds. A final run checks cache reuse. These are offline measurements.
  Final integration `fb3a4ba` includes newer mainline terminal/session controls,
  version metadata, and panic-to-error guards without changing the scoring rule
  or hash kernel. It receives a separate bounded 12..16-worker fresh tune and
  cache-hit smoke check; those samples are excluded from paired means. An earlier
  partial pilot predating the finish barrier is also retained separately.

| Pair | Old workers | New workers | Old counted H/s | New counted H/s | Gain |
|---|---:|---:|---:|---:|---:|
| 1 | 11 | 16 | 11.195 | 11.693 | +4.45% |
| 2 | 10 | 14 | 10.972 | 11.608 | +5.80% |
| 3 | 12 | 14 | 11.405 | 11.719 | +2.76% |

- All three pairs favor the corrected sampler. Mean counted throughput rises
  from 11.191 to 11.674 H/s (+4.32%); all-completed throughput rises from
  11.724 to 12.380 H/s (+5.59%). These gains come from selecting more effective
  worker counts, not a faster hash kernel. Exact selection still varies (14/16);
  this removes a systematic scoring bias but does not guarantee a global optimum
  or the same gain on other hardware, page modes, or profiles.
- Selected scores underestimate subsequent completed-hash throughput by
  21.45% on average before the fix versus 0.69% afterward (individual new
  errors: -0.44%, -0.40%, -1.22%). This metric compares each score with that
  same run's selected count, not with a separately assumed optimal count.
- Tradeoffs: mean fresh-tuning time increases from 174.7 to 277.1 seconds
  (+58.6%, approximately 102 extra seconds). Each sample adds a warmup, and
  corrected scores allow the unchanged search to explore higher thread counts.
  Selected arena memory rises from 20–24 GiB to 28–32 GiB on this 64 GiB guest;
  the default 5% RAM reserve still bounds the search. Cached launches skip tuning.
- Final integrated binary `fb3a4ba` passes a fresh 12..16-worker tune, selecting
  14 at 12.064 sample H/s and measuring 11.508 counted / 12.185 completed H/s.
  Its cache-hit run reuses 14 and measures 11.599 / 12.283 H/s. Both the frozen
  candidate and final build log cache reuse without repeating the search.
- Every benchmark report verifies ordinary-page backing and zero allocation
  failures, dropped events, or assignment/control timeouts. Host counters show
  no swap/OOM activity and negligible CPU steal (under 0.001%). Several runs
  emit the existing slow-stop/interrupt warning after writing their reports;
  all exit successfully, and the runner waits for each exit before starting
  the next process. Final inspection confirms no Seine process remains.
- All 47 final artifact files pass the remote SHA-256 manifest after download.
  Exact scripts, binaries, fingerprints, timings, logs, raw reports and cleanup
  evidence are retained in `data/autotune-steady-20261024/`. The rental was
  terminated after approximately 49 minutes; its root disk and dedicated
  security group were deleted, its Spot request cancelled, and its private SSH
  key removed. Observed compute pricing was $0.5276/hour (about $0.43 compute,
  before storage/network charges), with a $0.60/hour request ceiling.

## 2026-10-24 independent two-node Milan release validation

- **Host/build**: a separate EC2 `c6a.8xlarge` on-demand VM, AMD EPYC 7R13,
  16 physical cores/32 vCPUs, 64 GiB RAM, Ubuntu 24.04. The guest exposes two
  NUMA nodes, each with eight physical cores and one L3 sharing group. Both
  variants use the same generic Rust 1.93.0 CPU-only release binary from the
  combined AVX2 + NUMA/L3 merge `8e96bba`, SHA-256
  `2015385d5487d4d085f6190cd7103627f9f354549a760b9bff50d3490cbf6dd1`.
  This independently repeats the placement comparison below; it does not
  repeat the fresh-autotune experiment.
- **Method**: `--cpu-affinity os` versus `auto`, fixed worker counts, autotuning
  disabled, four order-balanced pairs per configuration. Each backend sample
  has one warmup and two 12-second measured rounds, with three-second cooldowns.
  Ordinary pages disable THP; explicit 2 MiB HugeTLB backing is verified for
  every large-page worker. A 32 GiB HugeTLB pool is released after the trials.
- **Observed placement**: separate live probes verify both page modes. OS order
  pins eight workers to CPUs 0-7, placing all 16 GiB of arenas on node 0. Auto
  uses CPUs 0,8,1,9,2,10,3,11: four workers and 8 GiB of arenas per node. Every
  worker uses a distinct physical core. These CPU IDs describe this guest;
  they are not recommended settings for other hosts.

| Workers | Backing | OS-order completed H/s | Auto completed H/s | Completed gain | Paired bootstrap 95% interval |
|---|---|---:|---:|---:|---:|
| 8 | Regular | 6.1885 | 10.2441 | +65.53% | +64.29%..+66.63% |
| 8 | 2 MiB HugeTLB | 5.9944 | 11.8514 | +97.71% | +97.36%..+98.20% |
| 16 | 2 MiB HugeTLB | 11.6764 | 11.7535 | +0.66% | +0.42%..+0.91% |

- **Accounting/limits**: this table counts all completed hashes over actual
  elapsed time, including the round-end fence. Deadline-counted backend
  `avg_hps` changes are 5.5548 to 9.5994 (+72.81%), 5.3582 to 11.2108 (+109.23%),
  and 10.4080 to 10.4818 (+0.71%), respectively. Every pair favors auto under
  both metrics. The all-core control changes by less than 1%; the large gains
  occur where OS order crowds eight workers into one of the two nodes.
  Four-pair intervals describe repeatability on this VM, not variation across
  machines. This measures combined NUMA/L3 placement and does not isolate
  memory-bandwidth and cache contributions or establish a universal gain.
- **Checks**: binary hashes match before/after testing. Allocation failures,
  page fallback, dropped events, assignment/control timeouts, swap activity,
  and OOM counter deltas are zero. Maximum aggregate CPU steal is 0.0011%; the
  longest measurement fence is 0.775 seconds. The earlier Intel checks cover
  reference-hash correctness of the kernel changes, while this run exercises
  the combined release code on the exposed two-node topology.
- **Evidence/cleanup**: `data/numa-milan-20261024/` retains exact source/binary
  identities, runner/analyzer scripts, `comparison.json`, raw benchmark reports,
  live worker/arena snapshots, and host counters. `cleanup.json` records the
  terminated instance and deleted root disk/security group. The temporary SSH
  key is removed and the expiry timer stopped after collection.

## 2026-10-24 Intel AVX2 release validation

- **Host/builds**: EC2 `r4.8xlarge`, Intel Xeon E5-2686 v4 (Broadwell),
  16 cores/32 vCPUs, 244 GiB advertised RAM (239 GiB visible), Ubuntu 24.04.
  The guest exposes AVX2, no AVX-512, and one NUMA node. Fresh, independent
  generic Rust 1.93.0 release builds compare `v0.2.17` (`710ea09`) with the
  AVX2 merge (`ac213e9`); this comparison excludes the later NUMA/L3 change.
  Audited AVX2 function sizes are 1888/2544 bytes versus 2596/3736 bytes.
- **Method**: persistent CPU backend, fixed worker counts, affinity `auto`,
  autotuning disabled, four order-balanced A/B pairs per configuration.
  Each sample has one warmup and two 12-second measured rounds, with a
  three-second cooldown. Regular pages disable THP; large-page runs verify
  explicit 2 MiB HugeTLB backing for every worker. The 32 GiB pool is released
  after testing. Local/guest binary hashes match and remain stable.

| Workers | Page backing | Baseline H/s | Candidate H/s | Gain | Paired bootstrap 95% interval |
|---------|--------------|-------------:|--------------:|-----:|-----------------------------:|
| 1 | Regular | 0.6902 | 0.7713 | +11.74% | +11.33%..+12.25% |
| 1 | 2 MiB HugeTLB | 0.8856 | 0.9984 | +12.74% | +12.41%..+13.21% |
| 8 | 2 MiB HugeTLB | 6.4700 | 7.2453 | +11.98% | +11.85%..+12.12% |
| 16 | 2 MiB HugeTLB | 9.9549 | 10.2222 | +2.69% | +1.78%..+3.60% |

- **Accounting**: the table uses backend `avg_hps`, excluding late hashes and
  including fence time. All-completed throughput gains are +10.63%, +11.88%,
  +11.06% and +2.68%, respectively. Every pair favors the candidate under both
  metrics. The paired bootstrap intervals describe four pairs on this VM, not variance
  across Intel machines or every AVX2 implementation.
- **Correctness/controls**: both builds match full 2 GiB RustCrypto reference
  hashes; all three kernel tests and 18 CPU-backend tests pass on the guest.
  Allocation fallback, allocation failures, dropped events, assignment/control
  timeouts, swap activity and OOM counter deltas are zero. Maximum aggregate
  CPU steal is 0.0349%; the longest round-end fence is 1.116 seconds.
- **Combined release check**: the later AVX2 + NUMA/L3 build (`8e96bba`) passes
  separate 8-worker backend smoke runs on both regular and explicit 2 MiB
  pages, with verified backing and no dropped events or control timeouts.
  The combined code also passes 410 local workspace tests; one pre-reserved
  1 GiB HugeTLB test is ignored. The Intel guest's single-node layout does not
  establish multi-node placement performance.
- **Evidence/cleanup**: `data/intel-avx2-20261024/` retains `sources.json`,
  `comparison.json`, raw reports/test logs in `results/`, the combined-build
  manifest and smoke reports in `results/integration/`, and exact run/analysis
  scripts. The instance is terminated and its disk and temporary security
  group are deleted; `cleanup.json` records verification.

## 2026-10-24 two-node EPYC placement and autotuner validation

- Host: EC2 `c6a.8xlarge` Spot instance, AMD EPYC 7R13, 16 physical cores /
  32 vCPUs, 64 GiB RAM, Ubuntu 24.04. Linux exposes two NUMA nodes with eight
  physical cores each: node 0 CPUs `0-7,16-23`, node 1 `8-15,24-31`. Each node
  is also one L3 sharing group. These results validate the exposed VM topology;
  they do not establish a universal bare-metal speedup or isolate cache effects
  from memory bandwidth.
- Tested source `e00089c` has the same tree as merge `8e96bba` (PR #29).
  Portable Rust 1.93.0 CPU-only release binary SHA-256:
  `c20fc2a248c205ab591ad8b56998fb993e0b78030d7bada72ed5b380562c9610`.
  Both variants use that identical binary, differing only in affinity policy:
  `--cpu-affinity os` versus `auto`. Artifacts and exact runner/analyzer scripts are retained in
  `data/numa-autotune-20261024/`.
- Fixed-count comparisons use two measured 20-second backend rounds plus one
  warmup, three alternating pairs at eight workers, two pairs at 16 workers,
  and three-second cooldowns. Ordinary pages disable THP. Huge-page runs require
  verified explicit 2 MiB backing; 16 GiB is reserved on each node beforehand.
  This is offline backend throughput, not pool mining hashrate.

| Workers | Backing | OS-order mean H/s | Automatic mean H/s | Counted delta |
|---|---|---:|---:|---:|
| 8 | Ordinary pages | 5.790 | 10.031 | +73.25% |
| 8 | 2 MiB HugeTLB | 5.635 | 11.720 | +107.98% |
| 16 | 2 MiB HugeTLB | 11.177 | 11.211 | +0.30% (effectively unchanged) |

- All three eight-worker pairs favor `auto` in each page mode. Counting all
  completed hashes over actual elapsed time also improves: +68.68% with
  ordinary pages and +101.14% with HugeTLB. The smaller all-core delta is not
  evidence of a meaningful gain when both policies already use every core.
- Live inspection verifies eight OS-order workers occupy node 0, with nearly
  all 16 GiB of arenas on that node. Automatic placement uses four workers per
  node and approximately 8 GiB of arenas on each. Helper threads retain the
  full CPU mask. Per-process NUMA maps, `numastat`, worker affinity, and backing
  telemetry are retained alongside the raw reports.
- Fresh autotuner checks use the default balanced profile, ordinary pages,
  automatic affinity, default six-second sampling, no explicit thread count,
  and separate empty caches. Explicit huge pages are released first. The
  default 5% RAM reserve yields a cap of 28 workers on this guest; each selected
  count is then evaluated with one warmup and two 20-second backend rounds.

| Fresh cache | Selected workers | Arena GiB | Short-sample H/s | Warm counted H/s | Warm completed H/s |
|---|---:|---:|---:|---:|---:|
| 1 | 12 | 24 | 9.271 | 11.351 | 11.931 |
| 2 | 10 | 20 | 8.950 | 10.944 | 11.428 |
| 3 | 12 | 24 | 9.391 | 11.405 | 11.983 |

- All caches use schema 9 with a nonempty topology fingerprint and match the
  benchmark worker count. A subsequent cache hit reuses 12 workers without a
  search and measures 11.306 counted H/s. Thread selection is not fully stable:
  one fresh run chooses 10 workers, below the other runs' warm throughput.
- A separate three-pair alternating comparison holds automatic affinity and
  ordinary pages constant and compares explicit 10 versus 12 workers. It uses
  the same 20-second rounds, warmup, and cooldown protocol as above. Twelve
  workers average 11.050 versus 10.582 counted H/s (+4.42%); paired gains are
  +4.67%, +4.06%, and +4.54%. All-completed throughput improves 5.06%. This
  confirms a measurable opportunity missed by that fresh tune, beyond the
  balanced profile's 1% near-peak target; it does not justify hard-coding 12
  workers for other machines.
- Follow-up: investigate short-sample scoring and search repeatability. Source
  inspection shows autotune's outer wall timer includes kernel benchmark
  setup/teardown, while the hashing deadline starts after arena allocation.
  This is a possible source of ranking bias, not an established cause from
  these measurements. No autotuner sampling or selection code was changed by
  this validation; the existing longer finalist remeasurements do not guarantee
  steady-state optimal selection.
- All fixed-count, fresh-tune, and cache-hit reports verify expected page
  backing and zero allocation failures, dropped events, or assignment/control
  timeouts. Main-run host counters show no swap/OOM activity and negligible CPU
  steal (under 0.001%). The cache-hit process emitted a slow-stop/detach warning
  after producing its report, exited successfully, and was confirmed gone
  before the separate thread-count comparison. Measurement fences completed.
- The merged source passed 407 CPU-binary tests (one pre-reserved 1 GiB HugeTLB
  test ignored), default-feature compilation, CPU-only Clippy with existing
  warnings, scoped formatting, and diff checks. The earlier 416-test count
  included unrelated work present on the original development branch.
- All 118 final result files passed the downloaded SHA-256 manifest, including
  raw reports, live placement snapshots, and runner logs. HugeTLB reservations
  were returned to zero and no Seine process remained before shutdown. The
  rental was terminated after approximately 39 minutes; its root disk and
  dedicated security group were deleted, its one-time Spot request cancelled,
  and its dedicated local SSH private key removed. No benchmark VM is retained.
  Observed Spot compute pricing was $0.5276/hour (about $0.35 for this run,
  before storage/network charges); the request ceiling was $0.60/hour.

## 2026-10-24 automatic Linux L3 placement: worker-only EPYC validation

- Linux `--cpu-affinity auto` now interleaves physical cores across L3 sharing
  groups within each NUMA node, retaining node balance before cache balance and
  all eligible physical cores before SMT siblings. `--cpu-affinity os` preserves
  the OS-order pinning opt-out; `off` still leaves placement to the scheduler.
- Discovery uses sysfs unified level-3 sharing masks, intersected with allowed
  CPUs. It validates symmetric membership and consistent SMT siblings instead
  of assuming cache IDs or index numbers are globally meaningful. Missing or
  inconsistent cache data retains NUMA-only placement. Incomplete NUMA/core
  data or a single node/cache retains OS order. Workers still pin before arena
  allocation; helper-thread affinity and memory policies are unchanged.
- Autotune schema 9 invalidates old placement measurements and fingerprints
  cache sharing domains as well as allowed CPUs and NUMA/core topology.
  Benchmark policy metadata identifies the NUMA/L3 strategy. Thread counts
  remain adaptive; this does not impose an EPYC-specific count or page policy.
- Local validation: 416 CPU-binary tests passed, one pre-reserved 1 GiB HugeTLB
  test ignored. CPU-only Clippy and default-feature compilation passed with
  existing warnings. Edited Rust files pass pinned-toolchain formatting;
  `git diff --check` passes. Topology coverage includes restricted CPU masks,
  one-node cache spreading, uneven NUMA/cache groups, SMT, shared caches across
  NUMA nodes, malformed/incomplete sysfs, and the original 64-core EPYC layout.
- Native validation artifacts, frozen source/binary, and exact scripts are in
  `data/epyc-l3-validation-20261024/`. This uses a second rental because the
  original 64-core offer was out of stock: AMD EPYC 8434P, 48 cores / 96 threads,
  384 GB advertised RAM (377 GiB visible), one NUMA node, eight L3 groups.
  The identical portable CPU-only binary is used for `os` and `auto`, SHA-256
  `b174c46e833c3e401dc602c7769f24da1149377dc56ef8c2b3e5113c4dca7332`.
- Live inspection verified 32 automatic workers occupy all eight L3 groups,
  four per group. OS order occupies six groups (6/6/6/6/6/2). Helper threads
  retain CPUs 0-95 in both variants. At 24 workers, `auto` uses three per group
  versus six per group in four groups for `os`. At 48 workers, both policies
  use every physical core, six per group. RAID resync was paused throughout
  all comparisons and restored afterward. Every measured arena reported
  ordinary pages, with zero allocation failures.
- Measurements used alternating A/B order, two measured rounds plus one warmup
  per invocation, and five-second cooldowns. The 24/32-worker comparisons used
  three pairs of 30-second rounds; the 48-worker control used two pairs of
  20-second rounds. These are offline backend measurements, not pool hashrate.

| Workers | OS-order mean H/s | Automatic mean H/s | Counted delta | Interpretation |
|---|---:|---:|---:|---|
| 24 | 18.376 | 21.449 | +16.72% | Consistent gain in all three pairs |
| 32 | 21.745 | 22.052 | +1.41% | Small, variable; inconclusive |
| 48 | 19.715 | 21.325 | +8.17% | Slow first baseline; not a reliable gain |

- **Adopted**: automatic L3 spreading with the existing `os` opt-out. The
  strongest implementation evidence is 24 workers: paired gains of 16.01%,
  17.44%, and 16.73%. Including all completed hashes instead of only hashes
  counted within the deadline also improves, 19.157 to 22.230 H/s (+16.04%).
- At 32 workers, paired deltas were +2.62%, +2.09%, and -0.59%; both variants
  slowed in the third pair. At 48 workers, the first baseline was 17.985 H/s
  versus 21.445 in the second; the second pair was essentially tied (+0.008%).
  A background snapshot found no large competing userspace workload and the
  mirror remained paused, but this does not establish the source of variance.
  Do not advertise the 48-worker mean as an affinity gain or compare its shorter
  deadline-counted windows directly with the 24/32-worker runs.
- This validates the worker-only implementation on a second EPYC model, not a
  universal percentage gain. Multi-node NUMA throughput was unmeasured at this
  stage; the newer two-node validation above supplies that evidence.
  All 37 downloaded result files passed their remote SHA-256 manifest. The
  hourly server was deleted after about 42 minutes, its dedicated SSH key was
  removed, and the task browser tab was closed; no rental was retained.

## 2026-10-24 EPYC 8534P hourly rental: cache placement, threads, and page backing

- Host: Scaleway EM-I620E-NVMe, AMD EPYC 8534P, 64 cores / 128 threads,
  576 GB advertised RAM (566 GiB visible), Ubuntu 26.04.1, kernel 7.0.0-38.
  Linux exposed **one NUMA node** and eight L3 sharing groups. This rental
  therefore does **not** validate the new multi-node NUMA default.
- CPU-only portable release binary, Rust 1.93.0, base `b19904e` plus the NUMA/RAM
  changes; SHA-256
  `f7f4fffa7e1835381f26bea0b0bd5286d8f07b58cf3656b1913dff6715dbd33b`.
  Source patch, new topology module, provenance, and binary are retained in
  `data/epyc-rental-8le6c9cd/`; raw reports are under `results/`.
- Main comparisons used three alternating A/B pairs, 30-second backend rounds,
  two measured rounds plus one warmup, five-second cooldowns, explicit thread
  counts, and disabled autotuning. Page backing was verified for every run.
  Rates below are benchmark deadline-counted H/s including the end fence;
  they are not pool mining measurements.

| Comparison | Baseline mean H/s | Candidate mean H/s | Candidate delta |
|---|---:|---:|---:|
| 64 vs 32 workers, 2 MiB HugeTLB | 16.209 | 17.429 | +7.53% (unstable baseline) |
| Ordinary vs 2 MiB pages, 32 workers | 17.614 | 17.412 | -1.14% |
| Compact vs cache-spread placement, 32 workers, ordinary pages | 17.614 | 20.842 | +18.32% |

- The 64-worker runs ranged from 14.372 to 18.766 H/s, so their average is not
  sufficient evidence for a universal 32-worker limit. The fresh OS was also
  synchronizing its RAID mirror; this was recorded, paused for both later
  comparisons, and resumed afterward. Its contribution to the variance was
  not established. The short 16/32/64/128 sweep is directional only.
- Cache-spread used `taskset` with CPUs `0,2,4,...,62`: four physical cores in
  each of eight L3 groups, instead of CPUs `0-31` occupying four groups.
  Both variants used 32 workers and 64 GiB of arena memory. Live per-thread
  affinity inspection confirmed the spread. Candidate runs were 21.467,
  20.410, and 20.649 H/s; baselines were 17.645, 17.609, and 17.590 H/s.
  Counting all completed hashes including late hashes also improved, from
  18.650 to 21.878 H/s (+17.31%). The taskset mask restricts the whole process,
  including helper threads, so a worker-only implementation needs its own A/B.
- No page-policy or single-node affinity default was changed from this test.
  The next supported experiment is automatic L3-group spreading with an OS-order
  opt-out, followed by a native-host A/B of that implementation. Do not hard-code
  this machine's CPU IDs on other hosts or claim a multi-node speedup.
- All downloaded result files passed their remote SHA-256 manifest. The hourly
  server was deleted after about 51 minutes; no benchmark resource was retained.

## 2026-10-23 Linux NUMA defaults and automatic RAM headroom

- Default Linux `auto` placement now interleaves physical cores across usable NUMA
  nodes before SMT siblings, respecting allowed CPU/memory masks. Single-node,
  memoryless/restricted-local-memory, and incomplete/inconsistent topologies
  retain OS order. Arena allocation already occurs after worker pinning.
- `--cpu-affinity os` restores OS ordering with pinning; `off` disables pinning.
- Automatic sizing/autotuning subtracts 5% of effective total RAM from available
  memory; `--cpu-memory-reserve-percent 0` opts out. Explicit worker counts retain
  existing semantics, and the existing minimum-one-worker rule still applies.
- Autotune schema 8 includes allowed CPUs and detected NUMA layout in cache
  identity. Benchmark reports distinguish the new placement policy from OS order.
- Synthetic tests cover node spreading, SMT ordering, sparse/asymmetric CPU sets,
  memory restrictions, inconsistent/missing topology, and RAM budget arithmetic.
- This initial implementation had no measured EPYC speedup. The newer two-node
  validations above supply controlled regular/HugeTLB evidence. They do not
  supersede the earlier WSL results; single-node WSL placement remains unchanged.

## 2026-10-24 AVX2 paired rounds and contiguous column loads/stores

- **Adopted**: interleave two independent AVX2 BLAMKA row/column rounds and
  combine adjacent columns into 256-bit loads/stores with 128-bit-half
  permutations. The data-dependent path retains column 0 before the existing
  mid-compress prefetch, then pairs columns 1+2, 3+4, 5+6 and handles column 7
  separately. Runtime ISA selection and the AVX-512/NEON/scalar kernels are unchanged.
- **Host/method**: Ryzen 9 9950X3D, WSL, pinned logical CPU 0, Rust 1.93.0,
  generic release build with fat LTO. Six order-balanced samples per variant,
  at least six seconds each, measuring complete 2 GiB Argon2id hashes using
  actual elapsed time. The reused arena had verified 2 GiB THP backing;
  allocation, reference-index precomputation and warmup were outside timing.
- **Full-hash kernel**: stock AVX2 `1.9501 H/s`, candidate `2.0644 H/s`
  (**+5.86%**, winning all six paired rounds). Paired bootstrap 95% interval
  was `+3.62%..+8.20%`; it describes these short local samples only. Stock
  AVX-512 remained faster at `2.2144 H/s` and stays preferred on this host.
- **Compression only**: `131.15 ns` to `115.12 ns` per 1 KiB block
  (**+13.92% throughput**, five samples). This is not a full-miner gain.
- **Correctness**: the investigation passed 640 reference comparisons across
  eight configurations and matched full 2 GiB warmup hashes. In-repo regression
  coverage now forces AVX2 on AVX-512 hosts and compares complete hashes with
  RustCrypto Argon2. Direct compression tests cover both prefetch paths,
  shared immutable inputs, varied block words and output boundary guards.
- **Applied validation**: kernel tests pass in debug and release; kernel Clippy
  passes with `-D warnings`. Workspace/all-target compilation and kernel/test
  compile checks for Windows x86-64 and macOS ARM64 pass. Applied AVX2 hot
  functions retain the measured candidate's sizes (2596/3736 bytes) and contain
  no EVEX instructions or ZMM register operands.
- **Evidence**: `data/isa-audit-8bn0w33f/REPORT.txt`, `hash-confirmation.jsonl`,
  `micro-contiguous.jsonl`, `summary.json` and assembly dumps. The candidate's
  hot functions contain no AVX-512 instructions. Nearby variants (unrolling,
  phase fusion, pairing rows only, and 256-bit AVX-512 rotates) were screened
  separately; only the pure-AVX2 paired/contiguous variant was adopted.
- **AVX2-only cloud follow-up**: Ubuntu 24.04 on an EC2 `c6a.8xlarge`, AMD
  EPYC 7R13 (Milan/Zen 3), 16 physical cores/32 vCPUs, 64 GiB RAM and two NUMA
  nodes. The guest exposes AVX2 and no AVX-512. Generic Rust 1.93.0 release
  builds with fat LTO compare baseline `b19904e` with candidate `ecf200b`;
  the only code difference is `pow-kernel/src/fixed_argon.rs`. These builds
  exclude the separate Linux NUMA-default changes in the main working tree.
- **Cloud method**: persistent CPU backend, fixed 1/8/16 workers, affinity
  `auto`, autotuning disabled, four order-balanced A/B pairs per configuration.
  Each sample has one warmup and two 12-second measured rounds, with a
  three-second cooldown between samples. Regular pages disable THP; large-page
  runs require verified explicit 2 MiB HugeTLB backing for every worker.
  The large-page pool reserves 32 GiB and is released after the tests.
- **Build provenance**: the baseline was rebuilt in a fresh, separate target
  directory, and the AVX2 function sizes were audited: baseline 1888/2544 bytes,
  candidate 2596/3736 bytes. An earlier batch reused a cached candidate kernel
  in the baseline build and was discarded; none of those timings appear below.
  Local and guest binary SHA-256 values match and remain unchanged across runs.
- **Cloud correctness/controls**: both builds match the RustCrypto reference
  for a full 2 GiB hash; all three kernel tests and 18 CPU-backend tests pass
  on the AVX2-only guest. Benchmark reports show no allocation fallback,
  allocation failures, dropped events or assignment/control timeouts. Swap
  and OOM counter deltas remain zero; aggregate CPU steal stays below 0.001%
  in every trial. The longest measured round-end fence is 0.788 seconds.

| Workers | Page backing | Baseline H/s | Candidate H/s | Gain | Paired bootstrap 95% interval |
|---------|--------------|-------------:|--------------:|-----:|-----------------------------:|
| 1 | Regular | 1.4748 | 1.5907 | +7.85% | +7.06%..+8.41% |
| 1 | 2 MiB HugeTLB | 1.8845 | 2.1639 | +14.83% | +14.53%..+15.09% |
| 8 | 2 MiB HugeTLB | 5.2100 | 5.3684 | +3.04% | +2.78%..+3.43% |
| 16 | 2 MiB HugeTLB | 10.2070 | 10.4470 | +2.35% | +1.12%..+3.61% |

- **Accounting/interpretation**: the table uses reported backend `avg_hps`,
  which excludes late hashes but includes fence time. Counting all completed
  hashes over actual measured elapsed time gives gains of +7.47%, +14.23%,
  +2.51% and +2.22%, respectively. Every pair favors the candidate under both
  metrics. The intervals enumerate all 256 resamples of four paired deltas;
  they describe these short samples, not variation across machines. The
  smaller multi-worker gains are consistent with shared-memory bandwidth
  limiting throughput; hardware bandwidth counters were not measured.
- **Cloud evidence**: `data/cloud-avx2-2026-10-24/comparison.json`, `sources.json`,
  `run_cloud.py`, `analyze_cloud.py` and `results/` retain the binary identities,
  raw reports, backing checks, test logs and analysis. The instance was
  terminated, and its root disk and temporary security group were deleted
  after collection; verification is recorded in `cleanup.json`.
- **Limits**: these measurements cover one Zen 5/WSL host and one AVX2-only
  Milan cloud VM. The Intel release-validation section above adds Broadwell
  coverage. These AVX2 comparisons do not measure the separate NUMA-default
  changes. The external screenshot's claimed C# advantage remains unverified.

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

### x86_64 (AMD Ryzen 9 9950X3D, Zen 5)

| Attempt | Change | Kernel delta | Backend delta | Status |
|---------|--------|-------------|---------------|--------|
| 48 | Compile AVX-512 x86 path into normal builds and keep runtime dispatch | +19.65% (1T kernel) | -0.28% at 16T backend (effectively flat) | Adopted |
| 49 | `target-cpu=native` on top of the AVX-512-enabled build | +0.00% (1T kernel) | +0.71% at 16T backend | Recommended for host-local builds |
| 51 | AVX-512 256-bit contiguous column gather/scatter + MaybeUninit q buffer | +1.5% (1T kernel) | memory-bound at 14T | Adopted |
| 2026-10-24 | AVX2 paired rounds + contiguous column loads/stores | +5.86% (forced AVX2, 1T kernel) | Unmeasured | Adopted |

Zen 5 result: AVX-512 materially reduces per-hash compute cost, but once 16 CPU
lanes are active the backend is mostly memory/hugepage limited on this host, so
the kernel win does not translate into a large end-to-end throughput gain unless
the build is also tuned for the local CPU and the hugepage situation is improved.

Operational note: a partial runtime HugeTLB reservation (`6136 / 16384` pages)
raised the same host's 16T native backend from ~`7.998 H/s` to ~`8.479 H/s`
(~**+6.0%**), which makes early hugepage reservation the highest-leverage next
step on this machine.

### x86_64 (AMD EPYC 7R13, Zen 3 / AVX2-only EC2)

| Attempt | Change | Kernel delta | Backend delta | Status |
|---------|--------|-------------|---------------|--------|
| 2026-10-24 | AVX2 paired rounds + contiguous column loads/stores | Unmeasured | +7.85% at 1T regular; +14.83% / +3.04% / +2.35% at 1T / 8T / 16T HugeTLB | Adopted |

### x86_64 (Intel Xeon E5-2686 v4, Broadwell / AVX2-only EC2)

| Attempt | Change | Kernel delta | Backend delta | Status |
|---------|--------|-------------|---------------|--------|
| 2026-10-24 | AVX2 paired rounds + contiguous column loads/stores | Unmeasured | +11.74% at 1T regular; +12.74% / +11.98% / +2.69% at 1T / 8T / 16T HugeTLB | Adopted |

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
| 38 (row) | 2-row Phase 1+2 interleave | +4.31% | +5.94% | Adopted |
| 38 (alignment) | AArch64 `PowBlock` 128-byte alignment | ~0% (1T) | +0.44% short, +0.10% long (12T backend) | Adopted |
| 47 | macOS `pcore-only` affinity-tag/QoS default | ~0% | +0.94% at 14T in 2026-07 paired retest | Retained |
| 52 | Balanced-autotuner finalist confirmation | — | protects +2.57% from a noisy 13T choice on this host | Adopted |

Cumulative AArch64: from ~1.37 H/s (scalar) to ~2.88 H/s, **~110% total improvement**.

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
- **Zen 5 AVX-512 meaningfully lowers compute cost but does not remove the memory
  wall**. On Ryzen 9 9950X3D, enabling the AVX-512 kernel cut instructions/hash by
  ~38.5% and cycles/hash by ~17.2% in a pinned 1T kernel run, yet a 16T backend A/B
  remained effectively flat without stronger HugeTLB coverage.
- **On this Zen 5 host, partial HugeTLB coverage was worth more than the next
  AVX-512 micro-optimization**. Runtime-reserving only `6136` hugepages still gave
  roughly +6% at 16T native backend throughput, while a subsequent AVX-512
  diagonal-permute micro-tweak improved the 1T kernel by ~1% but regressed the
  16T native backend and was rejected.

## 2026-07-11 1 GiB explicit hugepages on WSL Zen 5 (adopted, pending boot pool)

`--cpu-page-mode large-1g` (added in `2ba6617`) maps each 2 GiB arena as two
1 GiB HugeTLB pages via `MAP_HUGETLB | MAP_HUGE_1GB`, fail-closed like `large`.

- Host: Ryzen 9 9950X3D, WSL2 (48 GiB VM). Runtime reservation on the booted
  system materialized only `5/18` one-GiB pages even after explicit compaction
  passes, so the measurement used 2 threads (4 pages) with the 2 MiB pool
  (9,677 pages) untouched as the baseline side.
- Kernel A/B, same `release-native` binary both sides, three alternating
  pairs, 12 s x 3 rounds + warmup, 10 s cooldowns:
  `large` (2 MiB) `4.5045 H/s` vs `large-1g` `4.5997 H/s`.
- Paired geometric gain: **+2.121%** (95% bootstrap CI **+0.832% to
  +3.756%**), faster in 3/3 pairs. Backing telemetry recorded exactly two
  explicit arenas per class with zero fallback on both sides.
- Interpretation: with 2 MiB pages one lane's 2 GiB arena needs 1,024 dTLB
  entries; with 1 GiB pages it needs two. The residual dTLB pressure that
  survived Attempt 25's 2 MiB pool is measurable and now mostly recoverable.
- Adoption: boot-time pool configured (`.wslconfig` `kernelCommandLine =
  hugepagesz=1G hugepages=19`), the 2 MiB sysctl reservation commented out;
  both effective at the next WSL restart. Mine with `--cpu-page-mode
  large-1g` afterwards. Rollback: `sudo sysctl vm.nr_hugepages=9677` plus
  `--cpu-page-mode large`.
- Open confirmation: repeat at the 9-lane production profile once the boot
  pool exists — bandwidth saturation at 9 lanes may compress the per-lane
  TLB gain below the 2-thread figure.
- Artifacts: `perf-results/2026-07-11/wsl-1g-hugepage-kernel/`.

## 2026-07-11 verifiable page modes, native Windows, and WSL HugeTLB confirmation

The miner now exposes `--cpu-page-mode auto|regular|large` and records actual
arena backing per worker and byte in benchmark reports. Required-large mode
fails closed instead of silently comparing fallback pages. The CPU A/B harness
validates schema-12 backing telemetry, exact two-GiB arenas, zero fallback for
`large`, no THP contamination for `regular`, and stable backing between runs.

### WSL2 explicit HugeTLB (adopted operationally)

- Host: Ryzen 9 9950X3D, 32 visible logical CPUs, WSL2 configured for 48 GiB
  RAM and 8 GiB swap.
- Reserved `9677` two-MiB HugeTLB pages: nine two-GiB arenas plus 5% headroom.
- Four alternating 9-lane backend pairs, 20 seconds x 3 measured rounds plus
  one warmup and 15-second cooldowns.
- Regular pages: `7.83867 H/s`; explicit HugeTLB: `8.15861 H/s`.
- Paired geometric gain: **+4.096%** (95% bootstrap CI **+2.904% to +5.766%**),
  all four pairs faster.
- Every candidate report recorded exactly nine / 18 GiB explicit-large arenas,
  zero fallback/failures, and no swap growth.

This is the retained recommendation for WSL/Linux hosts that can reserve the
full pool before fragmentation. The reservation permanently removes roughly
18.9 GiB from ordinary memory on this configuration, so it remains an explicit
operator choice rather than an automatic system mutation.

### Native Windows large pages (supported, not a performance recommendation)

After assigning `SeLockMemoryPrivilege`, native Windows successfully allocated
all nine two-GiB arenas with large pages. A matched four-pair test measured
`7.85859 H/s` regular versus `7.85163 H/s` large: paired delta **-0.074%**
(95% CI **-1.124% to +1.780%**), with only one of four pairs faster. Keep the
portable allocator and fail-closed mode, but do not recommend large pages for
this Windows/9950X3D profile based on hashrate.

### Rejected follow-ups

- Windows L3/CCD-balanced affinity: implemented with complete-topology checks
  and physical-first fallback, then measured at `+0.438%` (95% CI `-1.000%` to
  `+1.425%`, two of three pairs faster). It failed the 0.75%/consistency gate
  and was reverted.
- x86 prefetch coverage reduced from all sixteen cache lines to offsets 0/512:
  two reverse-order one-lane HugeTLB kernel pairs regressed by **13.8%** and
  **15.5%**. The third pair was cancelled and full-block coverage retained.

### Cross-target checks

- macOS M4 Max schema-12 smoke: 16 `auto` workers recorded exactly 32 GiB of
  regular backing with zero failures and averaged `29.242 H/s` in the short
  validation; swap increased by about 659 MiB. This confirms the existing
  recommendation: 14 `pcore-only` for balanced use, 16 `auto` only for maximum
  throughput (the longer prior run reached `30.938 H/s`).
- The existing native-Windows mixed profile remains valid: 9 CPU lanes plus 14
  RTX 5090 lanes measured `17.353 H/s`, with separate system RAM and VRAM. Page
  mode does not change the backend/Stratum contract, and native Windows large
  pages were neutral, so the known regular-page mixed profile remains preferred.

## 2026-07-10 Apple Silicon scheduler, SME2, PGO, and autotuner follow-up

Host: Apple M4 Max, 12 performance + 4 efficiency cores, 48 GiB unified
memory, macOS 26.5, Rust 1.93.0. Unless noted otherwise, comparisons used the
CPU-only release build, 14 persistent backend workers, `pcore-only`, three
alternating pairs, one 15-second warmup plus two measured 15-second rounds per
leg, and ten-second cooldowns.

### Scheduler and build retests (not adopted)

| Candidate | Baseline mean | Candidate mean | Paired geometric delta | 95% CI | Result |
|---|---:|---:|---:|---:|---|
| Utility QoS only for workers 12+ | 29.0528 | 29.0979 | +0.148% | -1.053% to +1.555% | Reject; slower in 2/3 pairs |
| Affinity tags off | 29.0217 | 28.7079 | -1.085% | -2.267% to +0.130% | Reject; retain `pcore-only` |
| LLVM PGO trained on a 60-second 14-lane backend run | 28.8322 | 28.6717 | -0.545% | -1.284% to +0.402% | Reject; slower in 2/3 pairs |
| Pair DD columns 0+1 before next-reference prefetch | 29.0345 | 28.6865 | -1.207% | -2.031% to +0.086% | Reject; earlier prefetch is worth more than added ILP |

The PGO profile was produced with the Rust 1.93-matched LLVM 21.1.8 tools and
contained 734 functions / 24,392 blocks. Exact-source binary identities and
profile checksums are preserved under
`perf-results/2026-07-10/mac-pgo/`.

Powermetrics showed why splitting QoS did not help. The existing 14-worker
process already consumed approximately 11.99 P-core CPU equivalents plus 2.0
E-core equivalents while every worker reported User Interactive QoS. The E
cluster was active at 2592 MHz and the P cores were saturated. macOS was already
placing the two spill workers as intended.

### SME2 feasibility (not adopted)

The M4 exposes SME/SME2 with a 64-byte streaming vector length. Isolated
`SMSTART`, predicate, load/store, add, and widening-multiply probes executed
successfully. A 512-bit implementation then packed two independent Blamka
rounds into eight u64 lanes and matched the scalar reference.

Two ABI/toolchain details were required: Clang's locally-streaming prologue
issued `RDSVL` before `SMSTART` and faulted, so a fixed-width wrapper entered
streaming mode first; that wrapper also had to preserve non-streaming
callee-saved `d8`–`d15` around the mode transition.

Even after replacing full-width multiplies with SVE2 widening `UMLALB`, the
amortized result was decisively slower. Eight round pairs under one streaming
transition took about 338.6 ns versus 102.3 ns for tuned NEON. Apple executes
the wide streaming operations over multiple cycles, while the current NEON
schedule already exposes equivalent lane parallelism to the wide OOO core.
No Seine SME code was added.

### Attempt 52 (Apple): confirm close balanced-autotuner finalists (adopted)

A fresh 1..14 default-style autotune measured 13 lanes at 27.300 H/s and 14 at
27.452 H/s. The balanced profile therefore cached 13 because it appeared within
the intended 99%-of-peak floor. A longer direct A/B showed that decision was
wrong:

- 13 lanes: 28.3329 H/s arithmetic mean.
- 14 lanes: 29.0609 H/s arithmetic mean.
- Paired geometric gain: **+2.572%** (95% CI **+1.914% to +2.920%**), 14 faster
  in all three pairs.
- Swap did not increase during the comparison.

The root cause was finalist sampling variance, not the 99% balanced policy.
Six-second scans could also swap the apparent peak outright; one validation
reported 13 at 29.105 and 14 at 28.828 H/s. An initial fix that only confirmed a
lower selection against a distinct measured peak was therefore insufficient.

The adopted design confirms close balanced candidates before caching:

- A runner-up within 97% of the provisional peak triggers confirmation, as
  does any balanced selection below the provisional peak.
- The confirmation window is twice `--cpu-autotune-secs`, bounded to 6..30
  seconds.
- Initial and confirmation samples combine actual hashes and wall time.
- Finalists run in descending thread order, reversing the ascending ramp to
  reduce thermal/run-position bias.
- Clear peaks skip the extra work; the 99% memory-saving selection rule remains
  unchanged.

Final native validation initially measured 13 at 28.970 and 14 at 29.003 H/s.
Reversed 12-second confirmations produced combined rates of 27.977 and 28.751
H/s respectively, and the tuner correctly cached 14. The native macOS
CPU-only suite passed all 352 tests. Implemented in `14e23e5`.

## 2026-07-10 cross-host affinity, memory-pressure, and native Windows validation

The benchmark report now accounts for the measured wall interval and lifecycle
overhead, and `benchctl` captures host/build identity around paired runs. Absolute
rates below should therefore not be compared directly with older reports that used
nominal windows or different lifecycle accounting.

### Native Windows (Ryzen 9 9950X3D)

- Host: Windows 11, 16 physical / 32 logical CPUs, 64 GiB, native Rust 1.93 MSVC.
- Four 2 GiB lanes, High Performance selected only during each experiment and the
  original Balanced plan restored in `finally`.
- `auto` physical-core-first versus no affinity:
  - `5.6725` -> `6.8009 H/s`
  - paired geometric delta **+20.01%** (95% bootstrap CI **+12.16% to +27.03%**).
- Exact same-source binary isolation, old raw logical order versus complete
  physical-core-first topology:
  - `5.7912` -> `6.6980 H/s`
  - paired geometric delta **+15.81%** (95% CI **+8.34% to +20.00%**), all three
    pairs faster and candidate CV `0.78%`.
- Adopted only for native Windows. Incomplete, overlapping, or unknown topology
  groups fail closed to the original order.
- The new `VirtualAlloc` arena path safely fell back to regular pages because the
  account lacked `SeLockMemoryPrivilege` (Win32 1300). No privilege or reboot was
  applied during this validation.

### WSL2 (same Ryzen host)

- WSL exposed 10 physical / 20 logical CPUs and a 16 GiB memory limit.
- A same-code raw-order versus physical-first A/B measured `5.7894` versus
  `5.7245 H/s`: **-1.14%**, with a wide **-7.20% to +2.21%** confidence interval
  and mixed pair signs.
- Transparent-hugepage coverage varied from `0.2%` to `39.9%` between legs, and
  earlier interleaved work measured a significant `-8.21%` physical-first
  regression. Affinity cannot be isolated reliably under those conditions.
- Conclusion: retain legacy OS/core_affinity ordering on Linux and WSL. Revisit
  native Linux separately with a fixed HugeTLB reservation; do not infer native
  Linux policy from WSL's virtual topology.

### Apple M4 Max (12P + 4E, 48 GiB)

CPU-only steady backend results (`5 x 20s`, one warmup unless noted):

| Lanes / affinity | Average H/s | Memory observation |
|---|---:|---|
| 10 / `pcore-only` | 23.856 | no swap pressure observed |
| 12 / `pcore-only` | 27.348 | no swap pressure observed |
| 14 / `auto` | 29.323 | no swap created |
| 16 / `auto` | 30.938 | created/used about 3 GiB swap |
| 16 / `pcore-only` | 30.000 | began with swap already in use |

The shipping 14-lane affinity decision used a separate three-pair interleaved A/B:

- `pcore-only`: `29.2556 H/s`
- `auto`: `28.9809 H/s`
- `auto` paired geometric delta **-0.94%** (95% CI **-1.22% to -0.45%**), all
  three pairs slower.

Retain `pcore-only` as the balanced macOS default. A dedicated 48 GiB M4 Max can
explicitly use `--threads 16 --cpu-affinity auto` for the highest observed rate,
but it trades another ~5.7% over the paired 14-lane baseline for substantial OS
memory pressure.

### 15-lane follow-up (not recommended)

The missing point between the balanced 14-lane setting and the pressured
16-lane maximum was measured at commit `90a57da` with the CPU-only release build.
Three alternating `pcore-only` pairs used one 15-second warmup plus two measured
15-second rounds per leg and ten-second cooldowns:

- 14 lanes: `29.1262 H/s` arithmetic mean.
- 15 lanes: `29.4098 H/s` arithmetic mean.
- Paired geometric delta: **+0.968%** (95% bootstrap CI **-0.465% to +2.086%**),
  with two of three pairs faster.
- Swap used increased by `951,383,491` bytes (`907.31 MiB`) over the wrapped
  sequence, and macOS expanded total swap by exactly `1 GiB`.

The gain is small, uncertain, and costs measurable swap activity, so 15 lanes is
not a worthwhile operating point. Retain 14 lanes for balanced use and reserve
16 lanes for an explicit maximum-throughput choice. The CPU A/B harness now
accepts separate `--baseline-threads` and `--candidate-threads` values so future
lane-count comparisons remain interleaved and reproducible.

Semantic correction: `core_affinity` on macOS maps its values to Mach
`THREAD_AFFINITY_POLICY` tags, not hard logical CPU IDs. `pcore-only` limits the
tag set using the perflevel0 count and combines that with a high-QoS scheduler
preference; it does not guarantee P-core placement. Historical sections below
describe the original hypothesis and are retained as experiment history.

Metal remains explicit opt-in. Prior M4 Max measurements (`24.3 H/s` CPU-only,
`2.32 H/s` Metal-only, `4.61 H/s` combined under the older harness) and the new
CPU scaling results do not justify active Metal optimization on unified memory.

## 2026-03-13 Zen 5 AVX-512 column gather/scatter + MaybeUninit

Host: AMD Ryzen 9 9950X3D, 16 cores / 32 threads, Linux x86_64, AVX-512F + AVX-512VL available.

### Attempt 51 (Zen 5): 256-bit contiguous column gather/scatter + MaybeUninit q buffer (adopted)

- **Hypothesis**: Two independent micro-optimizations to the AVX-512 compress functions:
  1. **256-bit contiguous column gather/scatter**: Since each column pair has
     `base_j = base_i + 2`, the two columns' 128-bit chunks are adjacent in memory
     and can be loaded as a single 256-bit contiguous load. A `VSHUFI64X2` (imm8=0xD8)
     swaps 128-bit chunks 1↔2 to convert between the contiguous memory layout and the
     round's per-column register layout. This replaces 4×128-bit `VINSERTI32X4` gathers
     with 2×256-bit loads + 1 shuffle, and 4×128-bit `VEXTRACTI32X4` + XOR + stores
     with 1 shuffle + 2×256-bit XOR + stores. Net: ~160 fewer micro-ops per compress in
     the column phase (352 → 192).
  2. **MaybeUninit for q buffer**: The AVX-512 path zero-initialized a 1 KiB `PowBlock`
     on the stack as the working buffer, but Phase 1 writes all 128 u64s via 512-bit
     stores before any read. Switching to `MaybeUninit` (matching what the NEON path
     already does) eliminates ~1 KiB of dead zero-writes per compress call (~2M calls
     per hash).

- **Changes**:
  - `compress_avx512_into`: MaybeUninit for q, 256-bit gather/scatter with `VSHUFI64X2`.
  - `compress_avx512_into_mid_prefetch`: Same changes, preserving the mid-compress
    prefetch insertion point after column pair 0.
  - Correctness: `fixed_kernel_matches_reference_for_small_memory_configs` passes.

- **Kernel benchmark (1T, 15s, 3 rounds, 1 warmup)**:
  - Baseline: avg=3.378 H/s, median=3.400 H/s
    (`data/bench_baseline_kernel_1t.json`)
  - Candidate: avg=3.444 H/s, median=3.467 H/s (short run)
    (`data/bench_avx512_opt_kernel_1t.json`)
  - Delta: **+1.97%** (short run)
  - Long confirmation (20s, 5 rounds): avg=3.430 H/s, median=3.450 H/s → **+1.5%**
    (`data/bench_avx512_opt_kernel_1t_long.json`)

- **Backend benchmark (14T, 15s, 3 rounds, no HugeTLB)**:
  - avg=7.674 H/s, median=7.690 H/s
    (`data/bench_avx512_opt_backend_14t.json`)
  - No same-config baseline available; multi-thread remains memory/TLB-bound as
    expected from prior analysis.

- **Conclusion**:
  - +1.5% 1T kernel improvement is real and consistent across multiple runs.
  - The column gather/scatter optimization contributes the bulk of the savings
    (micro-op reduction in the hottest phase); MaybeUninit adds a small constant
    savings from eliminating dead zero-writes.
  - Multi-thread impact is negligible as expected — the memory wall dominates.
  - Adopted: the change is pure upside with no downside risk.

## 2026-03-09 Zen 5 hugepage reservation + AVX-512 vpermq retest

Host: AMD Ryzen 9 9950X3D, 16 cores / 32 threads, Linux x86_64, AVX-512F + AVX-512VL available.

### Runtime HugeTLB reservation trial (operationally adopted; system-level, not repo code)

- **Goal**: test whether the host was still memory/TLB-bound after the AVX-512
  dispatch fix by provisioning HugeTLB pages for the 16-thread native backend.
- **Runtime action**:
  - `sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches; echo 1 > /proc/sys/vm/compact_memory; sysctl -w vm.nr_hugepages=17204'`
- **Observed result**:
  - The kernel accepted the request but only materialized `6136` hugepages
    (`~12 GiB`) because memory was already fragmented.
  - `HugePages_Total` and `vm.nr_hugepages` both settled at `6136`, not `17204`.
- **Benchmark result (16T, native, direct run)**:
  - `data/bench_ryzen9950x3d_backend_t16_native_hugepages_partial.json`
  - Throughput: `8.479 H/s`.
  - Previous 16T native A/B mean without reserved HugeTLB pages:
    `7.9977 H/s` from `data/bench_cpu_ab_ryzen9950x3d_backend_t16_native/summary.txt`.
  - Effective uplift: **~+6.0%** despite only partial reservation.
- **Conclusion**:
  - HugeTLB availability is the largest remaining lever on this machine.
  - The correct operational fix is to keep `vm.nr_hugepages=17204` persisted and
    reboot so the kernel reserves pages before fragmentation sets in.

### Attempt 50 (Zen 5): AVX-512 diagonal immediate `vpermq` permutes (not adopted)

- **Hypothesis**: replace the AVX-512 diagonal step's `_mm512_permutexvar_epi64`
  calls plus vector index constants with `_mm512_permutex_epi64::<IMM>` so the
  rotate pattern is encoded as immediate `vpermq` shuffles.
- **Why it looked promising**:
  - Same within-256-bit-lane permutation semantics.
  - Removes vector constant construction from the hottest AVX-512 round helper.
- **Kernel A/B result (1T)**:
  - `data/bench_cpu_ab_ryzen9950x3d_kernel_vpermq/summary.txt`
  - Baseline `3.4000 H/s` vs candidate `3.4333 H/s` (**+0.9804%**).
- **Backend A/B result (16T native, partial HugeTLB pool active)**:
  - `data/bench_cpu_ab_ryzen9950x3d_backend_t16_native_vpermq/summary.txt`
  - Baseline `8.0067 H/s` vs candidate `7.9513 H/s` (**-0.6916%**).
- **Conclusion**:
  - The micro-kernel win is real but too small and does not survive the full
    backend workload on this host.
  - Rejected and reverted.

## 2026-03-09 Zen 5 (Ryzen 9 9950X3D) AVX-512 dispatch audit + native retest

Host: AMD Ryzen 9 9950X3D, 16 cores / 32 threads, Linux x86_64, AVX-512F + AVX-512VL available.

### Attempt 48 (Zen 5): make the existing AVX-512 kernel reachable from normal `seine` builds (adopted)

- **Problem found**: the AVX-512 implementation already existed in
  `src/backend/cpu/fixed_argon.rs`, but it was compiled only when
  `cfg(feature = "avx512")` was enabled.
- **Wiring gap**: `pow-kernel` declared an `avx512` feature, but the top-level
  `seine` package did not define or forward it, so ordinary `cargo build`,
  `cargo build --no-default-features`, `make build-cpu`, and `make build-native`
  never compiled the AVX-512 path.
- **Fix adopted**:
  - Remove the crate-level AVX-512 gate on x86_64 and always compile the AVX-512
    helpers for x86_64 targets.
  - Keep runtime ISA dispatch based on `is_x86_feature_detected!("avx512f")` and
    `is_x86_feature_detected!("avx512vl")`, so execution still falls back safely
    to AVX2 or scalar on older CPUs.
  - Export ISA-label helpers from `blocknet-pow-kernel` for future observability.

### Benchmark protocol

- Interleaved A/B harness:
  - `scripts/bench_cpu_ab.sh --baseline-dir /media/Code/blocknet/seine-baseline-avx512 --candidate-dir /media/Code/blocknet/seine --no-default-features`
- CPU-only builds to isolate the miner CPU backend from CUDA/default-feature noise.
- Kernel benchmarks use `1T`, `10s`, `2 rounds`, `1 warmup`, `3 pairs`.
- Backend benchmarks use `16T`, `10s`, `2 rounds`, `1 warmup`, `3 pairs`.
- Dedicated host-native comparison uses the same patched tree for both sides, with
  the candidate built under `RUSTFLAGS=-C target-cpu=native`.

### Kernel result: clear compute win

- Interleaved A/B:
  - `data/bench_cpu_ab_ryzen9950x3d_kernel_avx512/summary.txt`
  - Baseline `2.8833 H/s` vs candidate `3.4500 H/s` (**+19.6532%**).
- `perf stat` sanity check (pinned 1T, 5-second kernel run):
  - Baseline log: `data/ryzen9950x3d_perf/kernel_baseline_perf.csv`
  - Candidate log: `data/ryzen9950x3d_perf/kernel_candidate_perf.csv`
  - Baseline: ~`3.971e9` cycles/hash, ~`6.259e9` instructions/hash.
  - Candidate: ~`3.288e9` cycles/hash, ~`3.852e9` instructions/hash.
  - Delta: **-17.2% cycles/hash**, **-38.5% instructions/hash**.
- **Conclusion**: the AVX-512 path is genuinely faster on Zen 5; the improvement
  is not measurement noise.

### Backend result at the highest safe lane count on this host: essentially flat

- Interleaved A/B at `16T`:
  - `data/bench_cpu_ab_ryzen9950x3d_backend_t16_avx512/summary.txt`
  - Baseline `7.9266 H/s` vs candidate `7.9040 H/s` (**-0.2846%**).
- Interpretation:
  - This is effectively flat/noise-level once full backend scheduling, memory
    traffic, and fencing are included.
  - On this host, the backend safety check allows `16T`, while `24T` and `32T`
    exceed available safe memory for the 2 GiB-per-lane CPU backend.
- **Conclusion**: at 16 lanes, the Ryzen 9 9950X3D backend is dominated by memory
  behavior rather than raw compress-round instruction throughput.

### Attempt 49 (Zen 5): `target-cpu=native` on top of AVX-512-enabled code (recommended for dedicated local builds)

- Interleaved A/B at `16T`:
  - `data/bench_cpu_ab_ryzen9950x3d_backend_t16_native/summary.txt`
  - Generic release `7.9414 H/s` vs native release `7.9977 H/s` (**+0.7098%**).
- Matching 1T kernel A/B:
  - `data/bench_cpu_ab_ryzen9950x3d_kernel_native/summary.txt`
  - Generic release `3.4000 H/s` vs native release `3.4000 H/s` (**0.0000%**).
- **Conclusion**: `target-cpu=native` does not change the kernel on this host, but
  it does provide a small backend-level improvement. Keep it as the recommended
  build mode for dedicated Ryzen miners (`make build-native` /
  `./scripts/build_cpu_native.sh --cpu-only`).

### Hugepage / memory observations on this host

- No HugeTLB pool was reserved during the run (`HugePages_Total=0`), so all CPU
  backend runs relied on the fallback path.
- The backend repeatedly warned that `MAP_HUGETLB` was unavailable and reported
  partial or zero hugepage coverage after `MADV_HUGEPAGE` + `MADV_COLLAPSE`.
- A short patched-build scaling sweep showed:
  - `8T`: ~`7.41 H/s`
  - `16T`: ~`8.07 H/s`
  - `24T`: rejected by memory safety (needs ~48 GiB while available memory was ~45.09 GiB)
  - `32T`: rejected by memory safety (needs ~64 GiB; effective memory limit was ~60.20 GiB)
- **Takeaway**: on this 9950X3D system, hugepage provisioning and memory headroom
  matter more than ISA selection once the miner is running many CPU lanes.

### Net outcome

- Keep the automatic x86_64 AVX-512 runtime dispatch; the kernel win is large and real.
- Recommend `build-native` for dedicated local Ryzen deployments.
- Do **not** overstate the expected full-backend gain: without explicit HugeTLB
  pages, Zen 5 remains largely memory-limited at 16 CPU lanes.

## 2026-02-18 Apple Silicon superpage arena + pcore-only affinity trial

Host: Apple M4 Max, 16 cores (12P + 4E), 48 GB unified memory.

### Attempt 46 (Apple): 2 MiB macOS superpage arena allocation (not adopted)

- **Hypothesis**: Back Argon2 worker arenas with explicit 2 MiB superpages on macOS
  to reduce TLB pressure and improve throughput.
- **Implementation tested**:
  - Added best-effort superpage arena path using `vm_allocate(..., VM_FLAGS_ANYWHERE | VM_FLAGS_SUPERPAGE_SIZE_2MB)` with regular mmap fallback.
  - Exposed via `--cpu-superpage-2mb`.
- **A/B result (12T interleaved)**:
  - `data/bench_cpu_ab_superpage2mb_t12_20260218_043138/summary.txt`
  - Baseline `27.5619 H/s` vs candidate `27.4619 H/s` (**-0.3627%**).
- **Conclusion**: no gain on Apple Silicon in this workload window; code path reverted.

### Attempt 47 (Apple): `--cpu-affinity pcore-only` topology mode (adopted; default on macOS)

- **Hypothesis**: For mixed P/E-core Apple SoCs, pinning CPU hashing workers to the
  P-core set may improve consistency/throughput, especially at higher thread counts
  where auto affinity may include E-core IDs.
- **Implementation**:
  - Added `CpuAffinityMode::PcoreOnly` and CLI value `--cpu-affinity pcore-only`.
  - CPU backend affinity resolver now truncates core ID set to `hw.perflevel0.logicalcpu`
    on macOS for this mode.
  - Policy update: set `pcore-only` as the default `--cpu-affinity` on macOS
    (other platforms remain `auto`).
- **Measured results**:
  - 12T interleaved:
    - `data/bench_cpu_ab_pcore_only_t12_20260218_043817/summary.txt`
    - Baseline `27.3583 H/s` vs candidate `27.3981 H/s` (**+0.1454%**, near-noise).
  - 16T interleaved (directional):
    - `data/bench_cpu_ab_pcore_only_t16_20260218_044428/summary.txt`: **+2.3863%**
    - `data/bench_cpu_ab_pcore_only_t16_confirm_20260218_044839/summary.txt`: **+4.9146%** (high thermal variance across legs; treat as directional).
  - Post-cleanup sanity check (single pass, 12T):
    - `data/bench_cpu_pcore_only_sanity_20260218_045556/`
    - Baseline `26.8672 H/s` vs candidate `27.0638 H/s` (**+0.7318%**).
- **Conclusion**:
  - `pcore-only` appears neutral-to-slightly positive at 12T and potentially more
    beneficial at 16T under mixed thermal/scheduling states.
  - Adopted as default on macOS; remains selectable explicitly via `--cpu-affinity auto`
    when users want prior behavior.

## 2026-02-18 Apple Silicon arena residency + shared-arena trials

Host: Apple M4 Max, 16 cores (12P + 4E), 48 GB unified memory.

### Goal

- Re-test two previously discussed memory-allocation ideas on current Apple Silicon code:
  - Per-worker arena residency (`prefault + mlock` best effort).
  - One shared mega-arena split into per-lane slices.

### Benchmark protocol

- Primary check: interleaved backend benchmark at `12T`.
  - Harness output: `data/bench_cpu_arena_modes_20260218_035432/summary.tsv`
  - Per-leg records: `data/bench_cpu_arena_modes_20260218_035432/results.tsv`
  - Config: `--bench-kind backend`, `--bench-secs 12`, `--bench-rounds 3`, `--bench-warmup-rounds 1`, `pairs=2`, `cooldown=10s`.
- Secondary directional check: single pass at `1T`.
  - Output: `data/bench_cpu_arena_modes_t1_20260218_041153/`.
  - Config: `--bench-kind backend`, `--bench-secs 20`, `--bench-rounds 3`, `--bench-warmup-rounds 1`.

### Results

- **12T interleaved means**:
  - Baseline: `27.6163 H/s`
  - `--cpu-arena-residency on`: `27.5877 H/s` (**-0.1035%**)
  - `--cpu-shared-arena`: `27.4265 H/s` (**-0.6873%**)
  - Both enabled: `27.4524 H/s` (**-0.5936%**)
- **1T directional (single pass)**:
  - Baseline: `2.8884 H/s`
  - Residency only: `2.8782 H/s` (**-0.3505%**)
  - Shared only: `2.8983 H/s` (**+0.3444%**, low-confidence single-run signal)
  - Both enabled: `2.8935 H/s` (**+0.1779%**, low-confidence single-run signal)

### Conclusion

- No repeatable throughput gain at target multi-thread throughput (`12T`); all tested variants regressed.
- The small 1T shared-arena uptick did not justify keeping extra unsafe allocation complexity without multi-thread benefit.
- **Status**: not adopted; experimental code paths were reverted after measurement.

## 2026-02-18 Apple Silicon hot-path revisit + build/runtime retests

Host: Apple M4 Max, 16 cores (12P + 4E), 48 GB unified memory.
Baseline worktree: `../seine-baseline-d65dda4`.

### Hot-path assembly sanity check (AArch64 release)

- `target/release/seine` disassembly confirms `hash_password_into_with_memory` is still one large inlined hot body (`0x1000d3b90`, 8,589 disassembly lines in `otool -tvV` extraction), with:
  - BLAMKA arithmetic still emitted as `umlal.2d` pairs.
  - Rotations still emitted as `xar.2d`.
  - DD/DI prefetch still emitted as full 8-line `prfm pldl1keep` coverage.
  - Stack frame still 128-byte aligned (`and sp, ..., #...ff80`) from Attempt 38.
- Throughput-to-latency check from measured baseline (`27.1883 H/s` at `12T`):
  - Per-thread rate: `2.2657 H/s`.
  - Seconds per hash: `0.441 s/hash`.
  - Argon2 working set per hash: `POW_MEMORY_KB = 2,097,152` blocks (`pow-spec/src/lib.rs`).
  - Effective block cadence: ~`210 ns/block`.
- Interpretation: per-block service time is in the same range as Apple unified-memory DRAM latency under load, so remaining software-only headroom is small and mostly memory-system constrained.

### Attempt 41 (Apple): DD `pldl1strm` streaming prefetch hint (not adopted)

- **Hypothesis**: DD references may benefit from streaming L1 hints (`pldl1strm`) vs keep hints (`pldl1keep`) by reducing residency pressure.
- **Implementation issue discovered**: the variant was wired into the AVX2 DD path, not the AArch64 NEON DD path; on Apple Silicon this made the helper dead code.
- **A/B result (12T interleaved)**:
  - `data/bench_cpu_ab_aarch64_dd_l1strm_t12_warm1/summary.txt`
  - Baseline `27.2280 H/s` vs candidate `26.8403 H/s` (**-1.4242%**).
- **Conclusion**: rejected and reverted; no actionable AArch64 gain.

### Attempt 42 (Apple): DD loop split + unchecked hot-loop index loads (not adopted)

- **Hypothesis**: remove per-iteration DD tail/mid-prefetch branching and cut predictable bounds-check branches in the hottest loop.
- **A/B result (12T interleaved)**:
  - `data/bench_cpu_ab_aarch64_dd_loopsplit_unchecked_t12_warm1/summary.txt`
  - Baseline `27.1482 H/s` vs candidate `27.0752 H/s` (**-0.2689%**).
- **Conclusion**: slight regression / noise-negative; reverted.

### Attempt 43 (Apple): data-independent ref table `usize -> u32` (not adopted)

- **Hypothesis**: halve DI metadata bandwidth and footprint by storing precomputed ref indexes as `u32`.
- **A/B result (12T interleaved)**:
  - `data/bench_cpu_ab_aarch64_di_refs_u32_t12_warm1/summary.txt`
  - Baseline `27.3052 H/s` vs candidate `26.9069 H/s` (**-1.4586%**).
- **Conclusion**: clear regression; reverted.

### Attempt 44 (Apple): `release` vs `release` + `target-cpu=native` (not adopted)

- **Hypothesis**: host-native codegen may improve scheduling/throughput without kernel source changes.
- **A/B result (12T interleaved, identical code path)**:
  - `data/bench_cpu_ab_release_vs_native_t12_warm1/summary.txt`
  - Baseline `27.1883 H/s` vs native candidate `27.0655 H/s` (**-0.4514%**).
- **Conclusion**: no gain on this host/build; keep default non-native release for parity.

### Attempt 45 (Apple): CPU backend telemetry/control knob retests (not adopted)

- **Goal**: re-check earlier runtime knobs after later kernel/layout changes.
- **Results (12T interleaved)**:
  - `data/bench_cpu_ab_cpuknob_b128_c1_f100_t12_warm1_full/summary.txt`: **-1.0281%**
  - `data/bench_cpu_ab_cpuknob_b128_c1_f50_t12_warm1/summary.txt`: **-0.2903%**
  - `data/bench_cpu_ab_cpuknob_b64_c2_f50_t12_warm1/summary.txt`: **-0.3318%**
- **Conclusion**: baseline defaults remain best (`batch=64`, `control_check_interval_hashes=1`, `flush_ms=50`).

### Net outcome of 2026-02-18 sweep

- No new adopted changes from this batch.
- Current Apple Silicon implementation remains memory-latency-limited on the DD path; recent software-only hot-loop/codegen tweaks did not produce repeatable gains.

### Attempt 34 (x86): multi-hash interleaving — loop-level and intra-compress (not adopted)

- **Hypothesis**: Run 2 independent hashes per thread with interleaved block
  compression to fill the OOO window with truly independent work. Zen 3's 256-entry
  ROB can hold ~3.6 of 8 independent row-round iterations (71 insns each) for a
  single hash — a second hash could fill the remaining ROB capacity.

- **Three approaches tested**:

  1. **Loop-level interleaving** (`fill_blocks_pair` + sequential compress calls):
     Added `hash_pair_with_memory` that initializes two hashes, then alternates
     `fill_block_from_refs` calls between arenas A and B.
     - Result: **2.067 H/s (−16.2% vs 2.467 baseline)**
     - The `call`/`ret` boundary between compress calls prevents OOO overlap.

  2. **Sequential dual-arena (control)**: Two separate `hash_password_into_with_memory`
     calls with two arenas, no interleaving.
     - Result: **2.400 H/s (−2.7%, neutral)** — confirms the regression is from
       interleaving, not from having two arenas.

  3. **Intra-compress interleaving** (`compress_pair_avx2`): Full interleaved compress
     function with both hashes' Phase 1/2/3 inline using AVX2 intrinsics directly.
     Both BLAMKA dependency chains in the same function body.
     - Result: **1.600 H/s (−35.1% vs baseline)**
     - Phase 2 loop body: 170 instructions (vs 71 for single hash).
     - ROB holds ~1.5 iterations of the pair loop vs ~3.6 iterations of the single
       hash loop. The second hash REDUCES visible inter-iteration parallelism.

- **TLB analysis** (`perf stat`): Interleaved version showed 1.67% dTLB miss rate.
  Two 2 GiB arenas need 2048 × 2 MB TLB entries vs Zen 3's 1536-entry L2 dTLB.

- **Root cause of failure**: Zen 3's 256-entry ROB already exploits ~3.6× inter-
  iteration parallelism from the 8 independent row-round iterations within a single
  hash. Adding a second hash to the loop body doubles the iteration size, halving the
  number of iterations visible in the ROB (3.6 → ~1.5). The net ILP DECREASES because
  the lost inter-iteration overlap exceeds the gained inter-hash overlap.

- **Attempted `#[inline(always)]` on compress**: `#[inline(always)]` + `#[target_feature]`
  is not allowed in stable Rust (E0658). LLVM also chose not to inline
  `compress_avx2_into` even when caller/callee share the same target_feature
  (function too large at 436+ instructions).

- All interleaving code removed. Hugepages reverted to 1100 (single arena per thread).
- Status: not adopted. Multi-hash interleaving is counterproductive on Zen 3.

### Optimization frontier — x86_64

After 34 attempts (11 x86-specific), single-thread x86_64 performance is confirmed
near the architectural limit for Zen 3:

- **Measured**: ~2.47 H/s (888 cycles per compress, IPC 1.62)
- **Throughput bound**: ~3.22 H/s (680 cycles, perfect 2-pipe ILP)
- **Latency bound**: ~2.13 H/s (1032 cycles, fully serial)
- **Gap analysis**: 23% below throughput bound, locked behind G-function dependency
  chains that Zen 3's 256-entry ROB can only partially overlap (~3.6 of 8
  independent iterations).

Closing the gap requires hardware changes:
1. **Wider OOO window** — Zen 5 (448-entry ROB) could overlap ~5-6 iterations
2. **AVX-512** — halves instruction count, better throughput utilization
3. **Algorithmic changes** — not possible (Argon2id spec is fixed)

## 2026-02-17 Apple Silicon long-window confirmations + perflevel affinity cap trial

Host: Apple M4 Max, 16 cores (12P + 4E), 48 GB unified memory.

### Attempt 38 follow-up (Apple): long-window thermal-stable confirmation (kept)

- **Goal**: Re-check the kept 128-byte `PowBlock` alignment change under longer,
  thermal-stable windows before treating short-run gains as real.
- **Long interleaved A/B results**:
  - Single-thread (`60s`, `pairs=4`):
    - `data/bench_cpu_ab_aarch64_align128_long_t1/summary.txt`
    - Baseline `2.956368 H/s` vs candidate `2.948562 H/s` (**-0.2640%**)
  - 12-thread (`60s`, `pairs=3`):
    - `data/bench_cpu_ab_aarch64_align128_long_t12/summary.txt`
    - Baseline `27.248794 H/s` vs candidate `27.275803 H/s` (**+0.0991%**)
- **Conclusion**: short-window +0.44% at 12T compresses to a smaller but still
  positive long-window gain on 12T; 1T remains near-noise/slightly negative.
- **Status**: kept (already integrated in commit `806b294`).

### Attempt 40 (Apple): macOS perflevel0 affinity cap for `--cpu-affinity auto` (not adopted)

- **Hypothesis**: On Apple Silicon, cap auto-affinity core IDs to
  `hw.perflevel0.logicalcpu` (P-core count) so explicit high thread-count runs
  do not drift into weaker affinity behavior.
- **Implementation tested**:
  - `src/backend/cpu.rs`: added a macOS-only `sysctlbyname("hw.perflevel0.logicalcpu")`
    lookup and truncated auto-affinity core IDs to that count.
  - Applied in both backend worker startup and kernel benchmark path.
- **Interleaved A/B results (vs commit `806b294`)**:
  - Short 30s windows:
    - `data/bench_cpu_ab_macos_pcore_affinity_t12/summary.txt`: **+1.1825%**
    - `data/bench_cpu_ab_macos_pcore_affinity_t16/summary.txt`: **+6.2639%** (contained an outlier baseline leg)
  - Long 60s confirmation:
    - `data/bench_cpu_ab_macos_pcore_affinity_t12_long/summary.txt`: **-0.1475%**
    - `data/bench_cpu_ab_macos_pcore_affinity_t16_long/summary.txt`: **+0.0537%**
- **Conclusion**: effect is non-repeatable and collapses to noise/neutral in long
  runs, with slight regression at 12T.
- **Status**: reverted, not adopted.

## 2026-02-17 Apple Silicon cache-line alignment + DI L2 prefetch revisit

Host: Apple M4 Max, 16 cores (12P + 4E), 48 GB unified memory.
Baseline worktree: `../seine-baseline-e004732` at commit `e004732`.

### Attempt 38 (Apple): AArch64 `PowBlock` 128-byte alignment (adopted)

- **Hypothesis**: Apple Silicon L1D cache lines are 128 bytes. `PowBlock` was
  64-byte aligned, which also meant stack-local `PowBlock` temporaries (notably
  the hot `q` working buffer inside NEON compress) only had 64-byte alignment.
  Aligning `PowBlock` to 128 bytes on AArch64 should improve cache-line
  alignment for these hot temporary buffers and reduce split-line pressure.
- **Change**:
  - `src/backend/cpu/fixed_argon.rs`: `PowBlock` now uses
    `#[cfg_attr(target_arch = "aarch64", repr(align(128)))]` and retains
    `repr(align(64))` on non-AArch64 targets.
- **Assembly verification**:
  - `hash_password_into_with_memory` prologue stack alignment changed from
    `and sp, ..., #...ffc0` (64-byte) to `and sp, ..., #...ff80` (128-byte),
    confirming stronger stack alignment for local buffers.
  - Instruction mix in the hot function remains effectively unchanged
    (`umlal.2d`, `xar.2d`, `prfm` counts unchanged), indicating the gain comes
    from memory layout/alignment rather than altered arithmetic scheduling.
- **Interleaved A/B benchmark results**:
  - Single-thread (directional non-regression):
    - `data/bench_cpu_ab_aarch64_align128_t1/summary.txt`
    - Baseline `2.907667 H/s` vs candidate `2.908460 H/s` (**+0.0273%**, noise)
  - 12-thread (P-core scale):
    - `data/bench_cpu_ab_aarch64_align128_t12/summary.txt`
    - Baseline `27.305216 H/s` vs candidate `27.425387 H/s` (**+0.4401%**)
  - Final sanity run on kept code:
    - `data/bench_cpu_backend_align128_finalcheck_t12.json`
    - `avg=27.478 H/s`
- **Status**: adopted (small but repeatable multi-thread uplift, no single-thread regression).

### Attempt 39 (Apple): data-independent L2 prefetch hint (`pldl2keep`) (not adopted)

- **Hypothesis**: DI (slices 0/1) prefetches are farther-ahead, so prefetched
  blocks could live in L2 (`pldl2keep`) instead of L1 (`pldl1keep`) to reduce
  L1 pressure under high lane counts.
- **Change**:
  - Routed DI prefetch call-sites to an AArch64 `pldl2keep` helper, while DD
    prefetches stayed `pldl1keep`.
- **Interleaved A/B benchmark results**:
  - Single-thread:
    - `data/bench_cpu_ab_aarch64_align128_l2di_t1/summary.txt`
    - Baseline `2.897697 H/s` vs candidate `2.880682 H/s` (**-0.5872%**)
  - 12-thread quick confirmation:
    - `data/bench_cpu_ab_aarch64_align128_l2di_t12_quick/summary.txt`
    - Baseline `26.970669 H/s` vs candidate `25.940109 H/s` (**-3.8210%**)
- **Conclusion**: DI L2-hint prefetch under-serves the near-term working set;
  the baseline L1 hint remains superior on this workload/host.
- **Status**: reverted.

## First-principles performance analysis (x86_64, Zen 3)

### Instruction profile (compress_avx2_into, 1,437 dynamic instructions)

| Instruction | Count | Latency | Notes |
|-------------|-------|---------|-------|
| vpmuludq | 128 | 3c | BLAMKA multiply-accumulate |
| vpaddq | 416 | 1c | Addition / shift-by-1 |
| vpermq | 104 | 3c | Cross-lane shuffle |
| vpxor/vxorps | 224 | 1c | XOR |
| vpshufb | 88 | 1c | In-lane shuffle (rotations) |
| vpshufd | 56 | 1c | In-lane shuffle (rotr32) |
| vpsrlq | 32 | 1c | Shift (rotr63) |
| vpor | 32 | 1c | OR (rotr63) |
| vmovaps/vmovdqa | 250 | — | Loads/stores |
| vinserti128/vextracti128 | 48 | 3c | Column gather/scatter |
| **Total compute** | **1,080** | | Excluding loads/stores |

### Critical path through one BLAMKA G-function: 29 cycles

Each G = 4 chained multiply-accumulate-XOR-rotate steps. The dependency chain is
a→d→c→b through each step, with vpmuludq (3c latency) as the bottleneck at each
multiply-accumulate. Each avx2_round = 2 chained G calls = 58 cycles critical path.

### Throughput vs latency bounds

The 8 row-round iterations are independent (disjoint 16-element slices of q).
The 8 column-round iterations are also independent (disjoint column groups).
Zen 3's 256-entry ROB can hold ~3.6 row-round iterations (71 insns each),
enabling partial overlap of independent iterations.

| Bound | Cycles | ns @4.6 GHz | H/s (×2M blocks) |
|-------|--------|-------------|-------------------|
| Throughput (perfect ILP, 2 FP pipes) | ~680 | 148 | 3.22 |
| **Measured** | **~888** | **193** | **2.47** |
| Latency (fully serial rounds) | ~1032 | 224 | 2.13 |

- **Measured IPC**: 1,437 insns / 888 cycles = 1.62 (reasonable for dependency-heavy SIMD code)
- **Headroom vs throughput bound**: ~23% — locked behind G-function dependency chains
  that Zen 3's OOO engine can only partially overlap (~2-3 of 8 independent iterations
  at a time due to ROB/scheduler depth)
- **Margin above latency bound**: ~16% — OOO successfully overlaps some independent iterations

### What would close the gap

1. **Wider OOO window** — hardware upgrade; Zen 5 has 448-entry ROB which could
   overlap ~5-6 iterations and approach the throughput bound.
2. **AVX-512** — halves instruction count for same compute, better throughput
   utilisation (not available on Zen 3).
3. **Multi-hash interleaving** — run 2 independent hashes per thread to fill the
   OOO window with truly independent work. Requires 4 GiB memory per thread.
4. **Fundamental algorithmic changes** — not possible (Argon2id spec is fixed).

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

### Attempt 26 (x86): perf record profiling of AVX2 hot path

- **Method**: `perf record -g --call-graph dwarf,16384 -F 5000` on kernel bench
  (1 thread, 30s, AMD Ryzen 9 5900X / Zen 3, no AVX-512).
- **Results** (53K samples):
  - `compress_avx2_into_mid_prefetch` (DD path): 51.67%
  - `compress_avx2_into` (DI path): 45.32%
  - `clear_page_erms` (kernel page zeroing): 1.52%
  - `hash_password_into_with_memory` (loop overhead): 0.84%
- **Phase breakdown** within compress functions:
  - XOR+copy (Phase 1): ~10%
  - Column rounds (Phase 2): 41-47%
  - Diagonal/column rounds (Phase 3): 33-36%
  - Writeback: 10-13%
  - Prefetch instructions (mid-compress only): <1%
- **Hottest single instruction**: `vpermq` (3-cycle cross-lane shuffle), 1-3% each.
- **Key insight**: 97% of time is in the compress functions, evenly distributed
  across compute phases. XOR+copy at only 10% confirms prefetching is effective.
  The workload is fundamentally compute-bound within the compress.
- Status: informational (guided subsequent experiments).

### Attempt 27 (x86): reduce prefetch count from 16 to 4-8 cache lines (not adopted)

- **Hypothesis**: HW sequential prefetcher might handle nearby cache lines after
  explicit prefetch of the first few lines.
- **Results** (30s kernel bench):
  - 4-line prefetch: 1.800 H/s (**−25%**)
  - 8-line prefetch: 1.900 H/s (**−21%**)
  - 16-line prefetch (baseline): 2.400 H/s
- **Conclusion**: All 16 cache lines (1 KB PowBlock) MUST be explicitly prefetched.
  AMD Zen 3's L2 HW prefetcher does not help for random 1 KB block accesses —
  the stride between successive ref blocks is random, so spatial/sequential
  prefetchers cannot predict the pattern.
- Status: not adopted (reverted).

### Attempt 28 (x86): prefetcht1 (L2 hint) instead of prefetcht0 (L1) (not adopted)

- **Hypothesis**: `prefetcht1` brings data to L2 only, reducing L1 pollution.
  If the compress function re-fetches from L2 via its own loads, L2 hit latency
  (~4-5 cycles) may be acceptable while freeing L1 for working data.
- **Result**: 2.400 H/s — identical to `prefetcht0` baseline.
- **Conclusion**: On AMD Zen 3, `prefetcht0` and `prefetcht1` are likely
  treated identically by the hardware. AMD documentation confirms that prefetch
  hint levels are implementation-defined.
- Status: not adopted (reverted).

### Attempt 29 (x86): 1 GB huge pages (not viable at runtime)

- **Hypothesis**: 1 GB huge pages would reduce TLB entries from 1024 (2 MB pages)
  to just 2, potentially hitting L1 dTLB on every access.
- **Result**: `nr_hugepages` writes to `/sys/kernel/mm/hugepages/hugepages-1048576kB/`
  return 0 despite 34 GB free RAM. Memory too fragmented for contiguous 1 GB
  physical regions.
- **Requirement**: Must be reserved at boot via kernel command line
  (`hugepagesz=1G hugepages=2`), not at runtime.
- **Expected gain**: Minimal — with 2 MB pages, all 1024 entries already fit in
  Zen 3's 1536-entry L2 dTLB, so most accesses already hit L2 TLB (~7 cycles).
  1 GB pages would only save the L1→L2 TLB lookup, which is a small fraction of
  the ~195 ns compress time.
- Status: not viable at runtime (requires boot-time kernel params).

### Attempt 30 (x86): DI prefetch distance tuning 1-5 (not adopted)

- **Hypothesis**: Different prefetch-ahead distances might better overlap DRAM
  latency with compute.
- **Results** (30s kernel bench, multiple rounds):
  - Distance=1: 2.433 H/s (neutral)
  - Distance=2: 2.490 H/s (neutral, within noise)
  - Distance=3 (baseline): 2.500 H/s
  - Distance=4: 2.467 H/s (neutral)
  - Distance=5: 2.367 H/s (neutral)
- Extended 5-round verification (distance=2 vs distance=3): both median 2.500 H/s.
- **Conclusion**: DRAM latency (~80 ns) is fully covered by 1 compress call
  (~195 ns), so even distance=1 is sufficient. Prefetch distance is irrelevant
  when compute already dominates.
- Status: not adopted (left const at 3 with comment).

### Attempt 31 (x86): DD mid-compress prefetch removal (confirms +17% contribution)

- **Hypothesis**: Measure the actual contribution of mid-compress prefetch by
  disabling it in the DD loop.
- **Results** (30s kernel bench):
  - Without mid-compress prefetch: 2.100 H/s
  - With mid-compress prefetch (baseline): 2.467 H/s
  - Delta: **−14.9%** (removing prefetch is a regression)
- **Conclusion**: Mid-compress prefetch provides ~17% uplift in the DD path by
  overlapping DRAM fetch with the remaining 7 column rounds (~140 ns of compute).
  Without it, the DD path stalls on cache misses after each compress call.
- Status: informational (confirms Attempt 23's mid-compress prefetch is essential).

### Attempt 32 (x86): non-temporal stores in compress writeback (analysis only)

- **Hypothesis**: `vmovntdq` stores bypass cache, reducing pollution for data that
  won't be read again soon.
- **Analysis**: Every written block (`dst`) is read back in the very next iteration:
  - As `lhs` in Phase 1 of the next compress (all 16 cache lines loaded)
  - As `prev_index` for DD random seed (first u64)
  NT stores would force these reads to miss L1/L2 cache and fetch from DRAM,
  costing ~1280 ns (16 lines × 80 ns) per block — far exceeding any cache
  pollution benefit.
- Status: not adopted (analysis shows it would cause ~50% regression).

### Attempt 33 (x86): software pipelining / dual-block overlap (analysis only)

- **Hypothesis**: Overlap two blocks' execution by starting the next block's
  Phase 1 while the current block's Phase 3 completes.
- **Analysis**:
  - **DD path**: Serial dependency prevents overlap. Each block's ref_index depends
    on the previous block's `dst[0]`, which isn't available until Phase 3 of the
    current compress completes.
  - **DI path**: Prefetch already fires 3 blocks ahead, providing ~585 ns of lead
    time (3 × 195 ns). DRAM latency is ~80 ns. Overlap is already >7× sufficient.
  - Loop overhead is only 0.84% of total — no room for scheduling improvements.
- Status: not adopted (DD path is inherently serial; DI path already overlapped).

## Metal + CPU combined mode (Apple M4 Max, 48 GB unified memory)

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
   share the same physical DRAM banks and memory controller (~400 GB/s on M4 Max).
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

### Attempt 32 (Apple): 2 MB superpage allocation (not adopted)

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

## Validation

- Correctness: `cargo test fixed_kernel_matches_reference_for_small_memory_configs -- --nocapture`
- Full suite (at time of Attempt 2): `cargo test` (`177 passed`).

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

## Baseline

- Kernel baseline report: `data/bench_cpu_kernel_baseline.json`
- Kernel summary: `avg=1.188 H/s`, `median=1.167 H/s`, `hashes=57 counted=57`
- Backend baseline report: `data/bench_cpu_backend_baseline_head.json` (captured from detached baseline worktree at `32a3ded`)
- Backend baseline summary: `avg=1.193 H/s`, `median=1.195 H/s`, `hashes=60 counted=56 late=4`

## Benchmark protocol

### Kernel benchmark (single lane, deterministic)

```bash
cargo run --release -- --bench --bench-kind kernel --backend cpu --threads 1 --disable-cpu-autotune-threads --bench-secs 12 --bench-rounds 4 --bench-warmup-rounds 1 --ui plain --bench-output <report.json>
```

### Backend benchmark (persistent worker path)

```bash
cargo run --release -- --bench --bench-kind backend --backend cpu --threads 1 --disable-cpu-autotune-threads --bench-secs 12 --bench-rounds 4 --bench-warmup-rounds 1 --ui plain --bench-output <report.json>
```
