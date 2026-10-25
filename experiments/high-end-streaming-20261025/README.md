# High-end streaming follow-up — 2026-10-25

Owner: architecture session `01a16bd3-6dc7-70f2-a185-b86550c4dbeb`.
Baseline: published v0.2.22, commit `6e5411768d5d67365ecaff3abc30d280fb30cb0d`.
This branch contains research tooling and evidence, not an eligibility expansion.

## What streaming means

The shipping `avx512-256-stream` CPU kernel writes completed Argon2 blocks to the
full arena with non-temporal stores. Ordinary stores to cold cache lines can
first fetch their old contents even though the kernel will overwrite the entire
line. Streaming stores can avoid that traffic and reduce cache pollution.

Argon2 also reads earlier blocks. This implementation keeps 64 recent 1 KiB
blocks in a cached ring, serves recent references there, and fences streaming
writes before older arena reads. It retains the full 2 GiB per hash and computes
the same PoW. This is a memory-access optimization, not a reduced-memory algorithm.
It can help most when workers saturate memory bandwidth; a low-worker-count
speedup is not guaranteed. See `pow-kernel/src/fixed_argon.rs::fill_streaming_ring`
for the actual store, ring and fence rules.

## Scope and decision rule

Compare the shipping `avx512-256-stream` kernel against `avx2`, `avx512`, and
`avx512-256` on high-end Zen 4 Genoa and, when the desktop is idle, native Windows
Ryzen 9950X3D. CPU only, existing full 2 GiB PoW. Explicit selections are research
controls; users should continue receiving validated automatic behavior.

Genoa protocol is fixed before launch in `run.py`: full-size reference parity and
dirty-arena/kernel-switch parity; two opposite-order screens at 8 and 24 workers;
then four fresh alternating pairs at 24 workers against the strongest uniform
screen kernel. Confirmation measures two 20-second rounds after a 20-second
warmup per invocation. Completed-hash throughput includes the measurement fence.
Each report must prove worker count, affinity, kernel, 2 GiB/worker explicit large
page backing, zero allocation/control failures, zero swap/OOM, and bounded steal.
The result is host/configuration-specific; four pairs do not establish an entire
architecture's distribution.

A positive explicit-kernel comparison alone does not expand automatic eligibility.
That requires a separately reviewed production change, repeatable confirmation,
native productive tuner behavior and cache/restart/resource-pressure validation.
Clear losers stay out of production defaults.

## Reproduction and safety

`run.py` imports the existing `experiments/epyc-investigation/bench.py`. Copy both
alongside the verified published Linux executable, `verify_cpu_kernels`,
`verify_streaming_dispatch`, and `expected-binaries.json`. Verifier binaries are
reused from source `4f5fec9119fdfe6c9d24608fe9bc2a42eec49b4f`; `pow-kernel` and
`pow-spec` trees are byte-identical to the release. Preserve that distinct verifier
provenance; do not relabel those executables as release builds.

Run only on an owned disposable 32-core, 64 GiB c7a.8xlarge. The runner checks
family/model/topology, requires an initially empty HugeTLB pool, reserves 48 GiB,
and restores the pool in `finally`. It has a 27-minute process deadline. The
instance additionally needs an independently verified 35-minute shutdown timer
and EC2 shutdown-to-terminate behavior. Collect/check evidence, then explicitly
terminate and verify instance, root volume, security group and private-key cleanup.
Never rely on the timer alone as proof of cleanup.

Local raw evidence belongs under ignored `data/high-end-streaming-20261025/`.
Publish compact results, hashes, protocol, pricing/budget and cleanup receipts here
after completion. Never commit SSH private keys, credentials or oversized binaries.

## Coordination and budget

The EPYC session finished PR #48 and released its reservations. This session used
and cleaned its c7a slot; the research session owns PR #47/#49 and reduced-memory work.
The shared us-east-1 Standard quota is 32 vCPUs, so a c7a.8xlarge fills it.
The research session subsequently reserved that quota for separate authorized work.

Live c7a price checked via AWS Pricing: $1.64224/hour. The original campaign's
closed conservative commitments are $37.44941333333335. Reserve $1 for the separate
EPYC integration's $0.7009 compute plus incidental costs, and $1.50 for this
35-minute Genoa lease (about $0.958 compute plus incidental costs): $39.9494 total,
below the original $40 ceiling. This is not a new budget authorization.

Native Windows timing was initially deferred for the desktop's TF2 benchmark.
After it exited, native MSVC full-size parity passed and the bounded screen began.
`windows_screen.py` uses the published executable at 8 and 16 physical cores,
ordinary pages, reversed four-kernel screens and four alternating confirmation
pairs against the strongest 16-worker uniform kernel. `windows_topology.ps1`
queries native core/cache masks afresh. `windows_bench.py` checks coordinator
admission before each run, watches Gaming Mode and TF2 process return, bounds each
owned miner to 180 seconds and 32 GiB of arenas, checks 10 GiB initial free headroom,
and kills only its own miner on failure. Native correctness uses separately built
MSVC verifiers from the exact release source (`verify-windows.ps1`).

The first timing attempt overlapped another session's local compilation and was
discarded. The accepted run used an owned `generic-exclusive` gpuq reservation
for CPU timing isolation and explicit coordination to prevent local heavy work.
The harness verifies that reservation before each invocation and during hashing.
It completed, its native temporary tree was removed, and the desktop was released.
Read `RESULTS.md` and cleanup receipts before starting any new reservation; no
active lease or unfinished benchmark from this follow-up needs resuming.

`audit_reports.py` independently checks all 24 reports per host for exact release
identity, native environment, AMD family/model, current PoW, CPU-only execution,
fixed worker counts, kernel/page requests and measured backing. Keep completed
hashes divided by elapsed time including the fence; the report's `avg_hps` counts
only hashes completed inside the configured measurement window and is different.
Do not silently reuse old benchmark identities or treat WSL as native Windows.

## Related evidence / handoff

- Release: https://github.com/zpalmtree/seine/releases/tag/v0.2.22
- Release CI: https://github.com/zpalmtree/seine/actions/runs/40145936184
- Merged Turin integration: `../epyc-investigation/INTEGRATED_VALIDATION.md`.
- Original campaign: `../cpu-campaign-20261024/RESULTS.md`.
- NUMA allocator research remains on `codex/high-end-numa-20261024` with its own
  `experiments/high-end-numa/HANDOFF.md`; it must be rebased/rebuilt/revalidated
  before combining with the v0.2.22 streaming code. It is not part of this release.
