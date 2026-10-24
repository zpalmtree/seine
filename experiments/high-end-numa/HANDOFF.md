# High-end CPU / NUMA optimization handoff

This is the entry point for another engineer or Claude. Read the repository
`AGENTS.md` and parent workspace instructions first. This work is separate from
the completed ten-hour campaign in `../cpu-campaign-20261024/RESULTS.md`.

## User intent and current status

- Prioritize useful throughput improvements on modern, high-end hardware.
  Zen 4/5 EPYC and multisocket NUMA come first, then high-end Ryzen/Xeon and ARM.
- CPU only, current 2 GiB PoW. Respect Gaming Mode and bounded memory use.
- Improvements should work with default settings. An opt-out is **not required**
  unless a concrete tradeoff warrants one. Do not add options merely for policy.
- User explicitly requested durable documentation for a likely Claude handoff.
- **No NUMA performance gain is established by this branch. Do not promote it.**
  This branch prepares native experiments; shipping builds do not enable the
  `numa-experiment` feature. The environment selector is research instrumentation.
- Initial base: `1fe7f239fafb3b9de5df9728babbf041d6c03910` (master after PR42).
  Worktree: `/home/zp/Code/blocknet/seine-high-end-numa`.
  Branch: `codex/high-end-numa-20261024`.
- No cloud resources have been created by this follow-up. No cloud spend has
  been added. Do not reset the previous campaign's $40 cap implicitly; record
  an authorized budget and conservative commitments before another rental.

## Coordination and boundaries

| Owner / thread | Work | Boundary |
|---|---|---|
| EPYC `01a16bdf-d015-7131-b708-45a8a4676231` | PR48, streaming kernel and multi-challenger selection | Owns kernel changes and its us-east-1 rentals; no allocator changes |
| Research `01a16d32-8101-72c0-bf3e-b714c85544d9` | Draft PR47 joint-tune retry, PR49 ARM enum isolation | Separate branches; native validation outstanding at initial handoff |
| This branch | NUMA allocation observation, native matrix, handoff | No shared hardware reservation and no production merge |

At 20:29 UTC on October 24, EPYC reported c6i 16-vCPU testing until roughly
20:36 (hard deadline 20:42), followed by a c8a 32-vCPU run for about 35 minutes.
These are **historical estimates**, not permission to use its resources. Check
with that owner before using the shared us-east-1 quota. It requested no changes
to compiled master source until its frozen candidate finished validation.
This branch remains isolated. Never touch the dirty main `seine` checkout.

## Evidence we are building on

The EPYC session's committed investigation is at
`/home/zp/Code/blocknet/seine-epyc-investigation/experiments/epyc-investigation/`:
`README.md`, `numa-results.json`, `arena-hook.patch`, `arena_audit.rs`, and
`run_numa.py`. Source commit: `71af1e0683caa5569da0db4776b37032609c8c9a`;
experiment base: `785a06f`, **not** this branch's current base.

On an AWS Milan guest with two exposed NUMA nodes:

- Balanced huge-page pools: local binding was effectively neutral (+0.057%).
- Deliberately putting all HugeTLB capacity on one node: local binding plus
  local THP fallback improved eight-worker throughput by 40.40%.
- That gain combines memory locality **and** distribution across memory nodes.
  It is not a universal remote-memory penalty or a proven multisocket result.
- The earlier prototype held its policy guard through the worker lifetime.
  This experiment scopes the guard to each allocation/reallocation, restores
  before observation and error propagation, and checks restoration on unwind.

Current shipping Linux placement already spreads workers across NUMA/L3 groups
and allocates after pinning. The question is whether HugeTLB pool placement
defeats that locality, and when local fallback improves sustained throughput.
Do not describe existing spreading as newly implemented here.

## Hardware access blockers (verified, not assumptions)

Using the existing `seine-bench` / `SeineBenchmark` role in `us-east-2`:

- `service-quotas:GetServiceQuota` works.
- **Running On-Demand HPC instances** (`L-F7808C92`) = **0 vCPUs**.
  Hpc7a.96xlarge needs 192; Hpc6a.48xlarge needs 96. This is separate from the
  Standard quota, which is 5 in this region and is not the HPC launch gate.
- `ec2:DescribeInstances` and `ec2:DescribeInstanceTypeOfferings` were denied
  because no identity-based policy allows them in Ohio.
- `servicequotas:ListServiceQuotas` was also denied. Do not work around this by
  switching credentials. No alternate principal or attempted launch was used.
- AWS Pricing API verified Linux shared On-Demand compute rates in Ohio:
  Hpc7a.96xlarge **$7.20/h**, Hpc6a.48xlarge **$2.88/h**. Requote before launch.
- No quota-increase request, support ticket, IAM change, or reservation was made.

Required before hardware execution: an AWS administrator enables the benchmark
role's tagged EC2 lifecycle access in Ohio (including inspection, launch,
termination and root-volume cleanup), and approves the HPC quota. Request **192
HPC vCPUs** for one Hpc7a.96xlarge; do not request merely Standard vCPUs. Retain
the existing ownership-tag restrictions. Capture the actual policy and complete
a dry-run permission check before renting. The role's full IAM policy is not
available in this branch; do not invent an administrator policy from two errors.

Target hardware references:
- Hpc7a two-socket Genoa: https://aws.amazon.com/blogs/hpc/optimizing-mpi-application-performance-on-hpc7a-by-effectively-using-both-efa-devices/
- Hpc6a two-socket Milan: https://aws.amazon.com/ec2/instance-types/hpc6a/
- Separate quota categories: https://docs.aws.amazon.com/ec2/latest/instancetypes/ec2-instance-quotas.html
- Alternative dual EPYC 7443: https://www.cherryservers.com/pricing/dedicated-servers/2x-amd-epyc-7443
  Listed EUR0.632/h but all displayed locations had zero stock when checked.

## Reproduce and continue

See `README.md` for exact build, tests, inventory and matrix commands.
`probe.py` is read-only and records CPU/socket/node masks, distance tables,
HugeTLB pools, memory limits context, and kernel settings. WSL inventory can
validate tooling; it **cannot** establish native CCD/NUMA performance.

`run.py` runs a single frozen instrumented executable with different research
policies, avoiding different-binary code-generation confounds. Every worker count
gets alternating audit/local order independently. It validates full-size parity,
actual arena bytes, page mode, sampled placement, errors, swap/OOM, actual order
and executable hashes. Failure leaves all raw logs; no summary is promoted from
partial comparisons. It does not provision a machine or mutate page pools.

Next native experiments, in order:

1. Capture topology and prove the exposed two-socket/node mapping; inspect
   hypervisor reporting. NUMA nodes are not necessarily physical sockets.
2. Balanced 2 MiB pools, `auto` and `large`, audit versus local, workers
   1/8/16/24/32 initially. Increase only within explicit RAM and deadline bounds.
3. Disposable-host imbalanced pools, `auto`: same comparison. Record pool setup,
   exact resulting backing and restoration; never alter another tenant's pool.
4. Remote binding as a diagnostic control. This selector chooses the first
   different allowed node, which may be **on the same socket**. Classify its
   actual socket using topology; do not claim cross-socket latency automatically.
5. One socket versus both, then one process per socket with equal total workers
   and RAM. The supplied runner supports `--socket` for a single-process arm;
   concurrent per-socket aggregation is **not implemented** yet.
6. Only after a repeatable win: derive a production policy that respects existing
   memory policies and cgroup restrictions, tests local exhaustion and worker
   restarts, and passes balanced/single-node regressions. Strict `MPOL_BIND` is
   an experiment, not automatically the correct production behavior.

Before each rental: agree ownership with existing sessions, record exact source
and binary SHA256s, verify prices/quota/budget, and install an independent guest
termination deadline plus EC2 shutdown-to-terminate and root-disk deletion.
The runner's deadline is **not** a substitute for that independent safeguard.
Afterward: checksum artifacts, terminate owned instance, verify root disks and
temporary networking/keys are deleted, update costs and this handoff.

## Validation and artifacts

Implementation commit: `7728086d6eba5bbe1854ee4834e3739bf0487b4b`.
Validation results are recorded in `validation.json`:

- 480 workspace tests passed, six ignored in the ordinary suite.
- Three targeted optimized tests passed, including the two ignored NUMA tests:
  policy restoration on success/error/unwind and full-size reference parity.
- Audit and local allocation each passed scalar, AVX2, AVX-512 and 256-bit
  AVX-512 parity against three distinct reference inputs and dirty reuse.
- Eight Python evidence/cleanup tests passed, including actual timed-process
  termination. The runner refused a WSL performance matrix as intended.
- A real one-worker miner startup/benchmark produced a 2 GiB local arena and
  passed the report/placement validator. This was a WSL integration smoke test;
  its timing is deliberately not promoted as a NUMA result.
- Nonexperimental CPU compilation and scoped Rust formatting passed. Clippy
  completed with 61 diagnostics and none in the new module; no warning-free
  workspace claim is made.

Build logs, local inventory and raw test logs live under ignored `data/` in this
worktree. Public `evidence/` includes parity, restoration and Python test logs.
Frozen miner and optimized test executables are in `data/frozen/`; their hashes
and the compiled source-file hashes are in `validation.json`. Both build caches
are intentionally retained for continuation; no benchmark or build process is
left running by this branch. No rental, disk, security group, SSH key, support
case or account change was created by this follow-up.

Keep this document, exact commits and validation manifest together when handing
off. Another assistant should not need chat history to identify what is proven.
