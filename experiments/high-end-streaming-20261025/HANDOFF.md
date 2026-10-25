# Handoff: v0.2.22 and high-end streaming follow-up

Read `RESULTS.md`, then `README.md`. This is CPU-only work on the existing 2 GiB
PoW. The user prefers validated automatic behavior, with opt-outs only for concrete
reasons, and wants higher-end machines prioritized. Do not claim a guaranteed gain.

## Source and worktrees

- Published v0.2.22 / `origin/master` at release time:
  `6e5411768d5d67365ecaff3abc30d280fb30cb0d`.
- Release worktree: `/home/zp/Code/blocknet/seine-release-v0.2.22`, branch
  `codex/release-v0.2.22`. Versioned tests are in its ignored
  `data/release-verification/`; a copy is committed with this evidence.
- This research worktree: `/home/zp/Code/blocknet/seine-high-end-streaming`, branch
  `codex/high-end-streaming-20261025`. No production dispatch or tuner edits.
- Shared `/home/zp/Code/blocknet/seine` checkout is dirty and belongs to other work;
  do not reset, switch or overwrite it.
- Earlier NUMA allocator work is separate in `seine-high-end-numa`, branch
  `codex/high-end-numa-20261024`. Read its own handoff and rebase/rebuild before
  combining with v0.2.22. Its old measurements do not validate this release.

## Provenance and acceptance

Benchmark miners are the downloaded, checksum-verified v0.2.22 release assets.
Published Linux binary SHA-256 is
`d336f33f6f610b868a2f0f6ccedfa7316481c62fb4e209db4a8b5d891a6df260`.
Reports must carry source fingerprint `7ad8df30888fb8f1`, commit `6e5411768d5d`,
native runtime, the correct native target and exact CPU identity.

Linux verification/test executables are frozen EPYC integration artifacts built
at `4f5fec9119fdfe6c9d24608fe9bc2a42eec49b4f`; production source trees match v0.2.22,
but package version/build identity is distinct. See `evidence/verifier-provenance.json`.
Windows MSVC verifiers were built from a `git archive v0.2.22` of the source trees,
using native Rust 1.93.0, portable release, two compile jobs. Do not mix these
identities or claim that a source-identical old verifier is a release asset.

Genoa confirmation is valid: 24 workers / 48 GiB explicit 2 MiB pages,
AVX2 28.0672 -> streaming 35.6975 H/s, four alternating pairs, +27.1859% geometric
gain. Eight-worker screen is effectively flat. All parity, report/backing/health
checks and 480 native miner tests passed. `evidence/genoa/` contains the raw record.

The first Windows timing attempt is **inadmissible** because another session was
compiling locally. Keep `data/high-end-streaming-20261025/windows-contaminated/`
and its rejection metadata separate from any clean rerun. A later admission
attempt refused active TF2 before hashing. Coordinator preflight failures also
produced no samples. Correctness results remain valid. Only the final reserved
run may contribute to the results table; never merge the discarded samples back.

## Reservation and cleanup discipline

Genoa instance `i-04f82f51670b91a3e`, root disk, security group and private key were
deleted and verified at 2026-10-25 00:40:07 UTC. The 35-minute independent timer,
shutdown-to-terminate setting, initial/final HugeTLB pool and archive hashes were
verified. This session has no continuing cloud reservation. The research session
subsequently reserved the shared 32-vCPU US East quota for its own work; do not
assume it is free just because this instance was terminated.

Original campaign budget stays capped at $40. Conservative combined reservation
is $39.9494; this rental's compute estimate is $0.50334 plus incidental costs.
The research session has a separately approved $10 budget for its own follow-ups.
Do not reuse that authorization or silently reset either cap.

Windows uses task-owned files under
`C:\Users\zp\AppData\Local\Temp\seine-high-end-20261025-archdispatch`.
The clean rerun uses its `reserved-run` subdirectory and an owned `gpuq`
`generic-exclusive` reservation solely to exclude competing desktop benchmarks.
The harness checks that the active job ID matches its own reservation, watches
reservation loss/Gaming Mode/TF2 return and kills only its own miner on failure.
It bounds arenas to 32 GiB, checks 10 GiB starting free headroom and limits each
miner to 180 seconds. No GPU hashes are computed. A native command launched by
the coordinator needs the explicit WSL PATH recorded in the queue command.

Do not leave a queued/running reservation or native miner at handoff completion.
Retain raw evidence and frozen binaries in ignored local `data/`, then remove
the owned native temporary tree after collection. Preserve unrelated jobs/files.

## Coordination

- Architecture/release owner: `01a16bd3-6dc7-70f2-a185-b86550c4dbeb`.
- EPYC integration: `01a16bdf-d015-7131-b708-45a8a4676231`; PR #48 merged, rentals
  cleaned, no ongoing reservation from that work.
- Research: `01a16d32-8101-72c0-bf3e-b714c85544d9`; owns PR #47 tuner integration,
  PR #49 Intel regression isolation and reduced-memory prototype. It explicitly
  agreed to avoid local heavy work until this session releases the desktop.
- Autotune session: `01a16c04-edcd-7ed0-b2a0-033e45efa841`.

## Next production work

Use the explicit-kernel results to select candidates for automatic trials, not to
force a kernel or thread count. Genoa identity is AMD family `0x19`, model `0x11`;
9950X3D is family `0x1a`, model `0x44`. Shipping eligibility currently includes only
validated Turin family `0x1a`, model `0x02`. Any extension must retain independent
ISA/OS support checks, candidate-set cache fingerprints and repeated gain checks.

Coordinate a minimal eligibility change with PR #47, then verify native productive
selection, cold/warm cache and restart behavior, and memory/load handling for the
actual new identity and page backing. The existing ignored
`epyc_streaming_productive_selection` test uses 24 workers and mandatory HugeTLB;
it cannot be copied unchanged onto native Windows. Keep default improvements
automatic and add no speculative user-facing knobs.

True multi-socket NUMA and additional high-end Zen/Threadripper identities remain
hardware gaps. Intel/ARM integration is being handled in research; avoid duplicate
leases and shared-target build-artifact collisions.
