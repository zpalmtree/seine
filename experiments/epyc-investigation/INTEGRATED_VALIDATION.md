# Streaming-kernel production integration

The production path is `avx512-256-stream`. Its 64 KiB recent-block ring and
non-temporal stores retain the full 2 GiB Argon2 arena and the existing PoW.
The original `avx512-256` path remains separately identifiable.

Automatic trials require AMD vendor `AuthenticAMD`, family 26/model 2, plus
independent AVX2, AVX-512F/VL and OS vector-state support. This is the validated
Turin identity, not a rule for every EPYC generation. The initial fallback is
unchanged. Every eligible challenger is measured, and a new path needs two
successful A/B/A comparisons. No worker-count cutoff forces streaming stores.
The existing tuning opt-out remains available.

A sweep retains a confirmed incumbent while examining the next challenger.
Count trials remain paused between comparisons. Interrupted or noisy sweeps
cannot save a partial result. Cache schema 2 fingerprints the candidate set and
keeps worker counts and actual memory backing separate. The included
noisy-comparison/cache correction comes from PR #41; the native controls below
validate this complete integration, not that standalone patch in isolation.

## Build and measurement identity

The final compiled source is commit `4f5fec9119fdfe6c9d24608fe9bc2a42eec49b4f`,
based on accepted master `0e6b43e`. Later commits add only experiment runners and
documentation. Master `1fe7f23` adds the architecture report under `experiments/`
and changes no compiled source, Cargo inputs or features.

Rust 1.93.0, locked dependencies, portable `release` profile, no extra Rustflags.
Default-feature and CPU-only (`--no-default-features`) binaries were built and
frozen separately. Each upload and completed comparison verifies its SHA-256.
The CPU-only control is a clean build of v0.2.21 source `359265e`; the default
control is the published v0.2.21 Linux binary. Exact hashes, build fingerprints,
protocols, per-round results, health counters and cleanup receipts are in
[`integration-results.json`](integration-results.json).

Both hosts use offline backend work with completed hashes divided by measured
elapsed time including the round-end fence. Arenas must use explicit 2 MiB
HugeTLB pages: 24 GiB for the 12-worker Intel controls and 48 GiB for the
24-worker Turin confirmation. Affinity is `auto`, count tuning is disabled for
paired runs, and warmup is excluded from the reported score. Paired runs use
both baseline/candidate orders. They reject swapping, OOM kills, excessive CPU
steal, allocation failures and backend control/assignment errors.

## Native results

| Host / comparison | Baseline H/s | Candidate H/s | Paired changes | Geometric change |
|---|---:|---:|---|---:|
| Intel cpu-only-avx2 | 11.4914 | 11.5406 | +0.66%, +0.19% | +0.43% |
| Intel cpu-only-avx512 | 12.2692 | 12.2787 | +0.38%, -0.23% | +0.08% |
| Intel default-avx2 | 10.8412 | 11.3167 | +1.61%, +7.33% | +4.43% |
| Intel default-avx512 | 11.7212 | 12.0248 | +5.48%, -0.03% | +2.68% |
| Turin streaming vs avx2 | 34.4705 | 44.7420 | +30.06%, +29.53% | +29.80% |
| Turin streaming vs avx512 | 33.8080 | 44.7359 | +32.50%, +32.15% | +32.32% |
| Turin streaming vs avx512-256 | 35.1175 | 44.7295 | +27.54%, +27.20% | +27.37% |

The Intel CPU-only controls are effectively neutral and do not reproduce the
standalone retry patch's earlier AVX2 loss. Default-profile legs vary by up to about
10%; no repeatable loss was observed, but these controls establish neither an
Intel speedup nor a tight regression bound for that profile. The Turin gains repeat in both orders against all
three controls. All measured arenas used the required backing with no swap,
OOM, allocation, event-loss or assignment/control failures.

The real tuner first confirmed AVX2, then confirmed streaming against that
incumbent in two A/B/A cycles. It completed 14,096 hashes, cached streaming for
24 workers with 48 GiB explicit backing, and released the arenas on shutdown.
Its final six 30-second samples were 1053/1371/1051 and 1049/1365/1056 hashes
(A/B/A). The test passed in 387.41 seconds.

The Intel controls use two opposite-order pairs, two measured 20-second rounds
per invocation, and one warmup. EPYC confirmation uses two opposite-order pairs,
one measured 20-second round per invocation, and one warmup. Earlier prototype
confirmation retains three longer pairs per control in `kernel-results.json`.
These small paired samples describe repeatability on the tested hosts, not
uncertainty across every processor, system layout or workload.

## Correctness and automatic selection

- 488 CPU/workspace tests passed after rebasing; five hardware tests are ignored
  by the ordinary suite. No failures.
- The streaming ring matches RustCrypto at ring/slice/rounding boundaries, with
  dirty reuse and guard blocks. Full 2 GiB parity and 21 kernel switches across
  three nonces passed locally. The optimized real-worker kernel-switch test
  also passed with nonce continuity and teardown checks.
- Native Turin validation repeats full-size parity, dirty arena switches and
  tuner/coordinator tests with the actual three-candidate eligibility set.
- A dedicated native productive test gives the real tuner 24 workers and 48 GiB
  of explicit large pages. It requires streaming selection after repeated
  measured gains, verifies the cached winner, then checks that worker shutdown
  releases the arenas. Its transcript is retained in the numeric evidence.
- Formatting checks and strict kernel Clippy passed. Workspace Clippy completed
  with existing warnings. Default-feature compilation, Windows CPU workspace
  cross-checks and Linux/Apple ARM kernel cross-checks passed. Full application
  ARM cross-compilation was unavailable without the C cross toolchain/SDK.

## Reproduction and cleanup

`validate_intel.py` runs the matched-profile Intel controls. On a disposable
Turin guest with 24,576 local 2 MiB HugeTLB pages, `validate_integrated.py` runs
the parity checks, paired kernel comparisons and optimized ignored productive
test. Both runners require `expected-binaries.json`; do not substitute a new
binary while a comparison is running.

`cloud.py` gives each rental an independent systemd shutdown/termination timer
and owner-only SSH access. The Intel lease is bounded to 25 minutes and Turin
to 35 minutes. Artifacts are archived and checked against a per-file SHA-256
manifest before termination. `summarize_integration.py` refuses to export the
final evidence unless both validation markers and all cleanup receipts exist.

Both rentals, root disks, dedicated security groups and private keys are
verified deleted. Intel cleanup completed at 2026-10-24T20:35:52.785245+00:00;
Turin cleanup completed at 2026-10-24T20:52:54.592659+00:00. The downloaded
archives verified all 80 Intel and
68 Turin files against their SHA-256 manifests.

Live AWS Pricing API records give $0.68/hour for c6i.4xlarge and $1.72432/hour
for c8a.8xlarge. Charging conservatively from launch through final cleanup gives
$0.7009 compute for these two integration rentals, excluding EBS, public IPv4
and data transfer. This is an estimate, not an AWS billing statement.

## Handoff

[PR #48](https://github.com/zpalmtree/seine/pull/48) contains the production
integration and final evidence. Its GitHub record is authoritative for merge
state and the merge commit. All validation gates and cleanup are complete at
this report's commit. No deployment or release tag is included.

Raw archives and frozen binaries remain under
`data/epyc-integration/{intel,epyc}-integration-20261024/` and
`data/epyc-integration/frozen/` in the implementation worktree. The compact JSON
contains the raw report hashes, full per-round backend telemetry, build and
source fingerprints, exact control pairs, selection transcript and cleanup.
Start further production work from the merged master. Preserve the separate
uniform and streaming identities; extending automatic eligibility to another
CPU model or changing ring lifetime requires new correctness/performance
validation. NUMA allocation experiments remain separate and are not integrated
by this change.
