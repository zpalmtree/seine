# Proposed draft: prepare high-end NUMA allocation experiments and handoff

Base: `master`. Head: `codex/high-end-numa-20261024`. **Keep as draft.**
The connected GitHub integration returned HTTP403 when creating this PR; the
branch is pushed but no PR number has been assigned.

Uneven HugeTLB pools can place a worker's 2 GiB arena on a remote NUMA node despite
CPU pinning. This prepares a current-source experiment to compare existing
allocation with temporary local/remote binding and verify sampled page placement.
No native NUMA performance gain is established and no production default changes.

The `numa-experiment` feature is disabled in normal builds. Its guard restores
the calling thread's memory policy after every allocation or error and on
unwinding. Local/remote controls require successful pinning and reject existing
nondefault memory policies. The runner uses one frozen executable, reversed order
at each worker count, full-size reference parity, explicit RAM/deadline limits,
and backing/locality/error checks. It does not provision machines or alter pools.

Validation: 480 workspace tests passed (six ignored); three optimized targeted
tests passed, including full 2 GiB scalar/AVX2/AVX-512/256-bit parity against three
inputs with dirty reuse; eight Python tests passed; a real one-worker WSL miner
smoke passed the evidence validator. Nonexperimental CPU compilation and scoped
formatting passed. Clippy completed with existing diagnostics and none in the
new module. WSL checks are correctness/integration evidence only.

Native execution is blocked: Ohio HPC quota is zero and the benchmark role lacks
EC2 inspection access there. No rental or incremental cloud spend was created.
Do not merge as a performance improvement until native confirmation and a
production-policy review are complete.

Start at [HANDOFF.md](HANDOFF.md) for commands, coordination, prior evidence,
verified access blockers, source/binary hashes and remaining gates.
