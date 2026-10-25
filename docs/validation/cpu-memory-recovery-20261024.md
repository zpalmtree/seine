> **Final merge gate: keep this PR draft.** The proposed #40/#41/#43/#45 subset
> (excluding ARM and joint-tuning drafts) measured twelve-worker Ice Lake AVX2
> differences of −2.277% and −1.930% in reversed pairs. Full AVX-512 was mixed
> (+2.051%, −0.257%). Correctness and lifecycle results below still stand, but
> they do not establish performance neutrality. Standalone #41 also measured −1.011% and −0.821%; both PRs remain held.
> The subset result alone does not isolate a causal change in #43. See the final campaign report in PR #42.

# CPU memory-capacity recovery validation

A native campaign lifecycle reproduced the same bug on Graviton3 and Ice Lake:
a miner restarted with `memory.high=3G` stayed at one worker after the limit
rose to 12 GiB. Startup RAM capacity had fixed the controller maximum, parked
backend slots/nonce stride, and retained-cache ceiling.

The repair derives the parked-slot ceiling from permitted CPUs and explicit
worker bounds. Startup still seeds from the current RAM budget, and every
active arena remains constrained by the refreshed allowance. Recovery has a
30-second headroom debounce, respects allocation backoff, and gets a count
probe opportunity even when kernel comparison would otherwise hold the count.
A short withdrawn headroom spike does not interrupt a comparison.

## Checks

- Standalone review commit `4a622f3`: CPU-only workspace tests, **484 passed,
  four ignored**. This branch stacks on the kernel-retry fix in PR #41.
- Pinned Rust 1.93.0 formatting of changed Rust files and `git diff --check`: pass.
- Independent review by the existing autotune owner: no remaining blocker.
- Combined campaign candidate `1246967`: CPU-only workspace **485 passed,
  four ignored**. The additional test belongs to the separate ARM candidate.
- Combined strict all-target Clippy: 124 existing diagnostics across binary and
  binary-test targets; none in the three changed implementation files. This
  is not reported as a clean workspace lint run.
- Native Sapphire Rapids / Xeon Platinum 8488C on combined candidate `1246967`:
  **485 workspace tests passed**, four ignored; scalar/AVX2/AVX-512/256 full
  2 GiB reference parity passed; both ignored real-arena growth/switch tests
  passed; strict kernel Clippy passed.
- The complete native Sapphire Rapids lifecycle passed CPU contention, actual
  `memory.high` reclaim pressure, zero-worker parking, productive recovery,
  constrained restart, preserved cache hints, and fixed-two-worker opt-out.
  The restarted process grew from one to two and then three workers; anonymous
  residency reached **6.028 GiB**, proving real arena growth. No OOM kills or
  backend quarantine occurred. All four owned service units were inactive and
  the HugeTLB pool was zero before subsequent timing.
- The native recovery archive SHA256 is
  `76dca6714fd3b7ebf8131673c9f28e3b5ca9712917cc234f633c619111678cd2`;
  all 24 archived files were individually verified. Final ARM repeats follow below.

Original failed lifecycle archives and their interpretations are retained in
the architecture campaign. The failure is not hidden by extending its timeout.

## PID-budget hardening and final native candidate

Review exposed a separate resource limit: parking one OS thread per permitted
CPU can exhaust a tight PID budget even when only one arena is enabled. A real
16-CPU, 12-task backend probe reproduced EAGAIN at worker nine. The repair now
considers nested cgroup PID headroom, retries resource-related adaptive spawn
failures after joining partially started workers, and preserves the original
nonce stride. Explicit worker counts retain RAM-safe limits. The same probe
passes with four provisioned slots and stride sixteen; it deliberately bypasses
preflight to exercise the actual spawn-failure recovery path.

The frozen combined candidate `636e688` includes these fixes and the separate
ARM work. On Xeon 6975P-C it passed **495 workspace tests, five ignored**, full
2 GiB scalar/AVX2/full-width and 256-bit AVX-512 reference parity, both explicit
real-arena tests, and strict kernel Clippy. All nine resource-lifecycle phases
passed; restart recovery reached three workers and 6.028 GiB anonymous residency.
There were no OOM kills or quarantined backends, and fixed-worker opt-out passed.

A full plain pool runtime with sixteen CPUs, one arena's RAM and a 40-task
limit provisioned nine parked slots, peaked at 35 tasks, completed 84 observed
hashes and stopped cleanly. Native Graviton4 repeats passed **493 workspace
tests, four ignored**, scalar/NEON/SHA3 full-size parity, three-nonce dirty arena
dispatch switches, both real-arena tests, strict kernel Clippy and the same
40-task pool check (54 observed hashes). Test counts differ by platform.

The initial 24-task full-pool test failed on both v0.2.21 and the candidate:
existing sysinfo discovery leaves a sixteen-thread Rayon pool before normal
runtime startup. Both failures are retained. The 40-task checks use the default
environment, without a Rayon override. Reducing that startup footprint remains
future work; PID headroom remains best effort when unrelated tasks race startup.

Final combined all-feature/all-target compilation and scoped formatting pass.
Strict workspace Clippy still reports 124 pre-existing diagnostics across binary
and test targets, none on changed lines; only kernel Clippy is reported clean.
Independent read-only review found no blocker in stride preservation, partial
startup teardown, nested limits, manual bounds or platform-scoped spawn errors.

Graviton4's final `636e688` lifecycle also passed all nine phases, including
restart growth to three workers and **6.02784 GiB** anonymous residency, productive
recovery, zero-worker parking and fixed-worker opt-out. No OOM kills or backend
quarantine occurred. All owned service units were inactive and the HugeTLB pool
was zero before lease cleanup. The final native archive contains 221 individually
verified files, SHA256
`34e36b972ce5d2da2a0ea28c511be3ac52503b38add2888ac1fc4c4fe94041b1`.
