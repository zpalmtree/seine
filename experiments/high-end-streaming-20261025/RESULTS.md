# v0.2.22 follow-up: high-end AMD streaming validation

The release is published at https://github.com/zpalmtree/seine/releases/tag/v0.2.22
from `6e5411768d5d67365ecaff3abc30d280fb30cb0d`. All five platform builds passed.
All archive and internal file checksums were verified, including the new Linux
ARM64 CPU-only archive. The exact tagged source passed 488 local workspace tests
(five ignored). This research branch changes no production kernel eligibility.

## Genoa — confirmed explicit-kernel gain

Native AWS c7a.8xlarge, AMD EPYC 9R14, family 25/model 17, 32 physical CPUs, one
exposed NUMA node, 64 GiB RAM. Published Linux executable, default release features,
automatic CPU affinity, explicit 2 MiB HugeTLB backing. Every worker retains its
full 2 GiB arena. This is not a multi-socket NUMA result.

| Workers | AVX2 | AVX-512 | 256-bit AVX-512 | Streaming | Evidence |
|---|---:|---:|---:|---:|---|
| 8 | 16.510 H/s | 16.008 H/s | 16.486 H/s | 16.469 H/s | Two opposite-order screening passes |
| 24 | 27.858 H/s | 27.224 H/s | 27.800 H/s | 35.021 H/s | Two opposite-order screening passes |
| 24 | 28.067 H/s | — | — | 35.698 H/s | Four fresh alternating confirmation pairs |

The confirmation gain is **27.19%** geometrically across pairs. Individual paired
gains are 27.26%, 27.43%, 26.81%, and 27.25%. Each invocation uses a 20-second
warmup and two 20-second measured rounds; completed-hash throughput includes the
measurement fence. The strongest mean uniform kernel from the independent screen
(AVX2) was selected before confirmation. The narrow descriptive resampling
interval in the JSON describes this session, not an architecture-wide confidence
bound. Eight-worker streaming is effectively flat and should not be forced.

Correctness and controls:

- Full 2 GiB reference parity for every supported kernel; 21 dirty-arena/kernel
  transitions across three reference inputs, with guard blocks preserved.
- 480 native miner regression tests passed, five ignored. The frozen test binary
  predates the release tag; all `src`, `pow-kernel`, `pow-spec`, and `build.rs`
  content matches the release. Root package version/metadata is distinct; see
  `evidence/verifier-provenance.json` rather than relabeling the test executable.
- All 24 benchmark reports independently audited for release source/build identity,
  native CPU identity, CPU-only execution, current PoW, explicit kernel requests,
  worker counts and measured page backing.
- Zero swap-in/out, OOM kills, allocation failures, dropped events or control
  timeouts. Maximum conservative single-CPU steal bound was 0.1%.
- Initially empty HugeTLB pool restored to zero. Collected archive and all 94 file
  checksums verified before rental cleanup.

Raw reports, checks, logs, protocol and summaries: `evidence/genoa/`.
Independent report audit: `evidence/genoa-audit.json`.

## Native Windows 9950X3D

The published Windows MSVC executable starts correctly. Native MSVC verifiers built
from the exact release source passed full-size reference parity and all 21 guarded
dirty-arena/kernel transitions. Fresh native topology confirms 16 physical cores,
adjacent SMT siblings, a 96 MiB L3 CCD on CPUs 0–15 and a 32 MiB L3 CCD on CPUs
16–31. The accepted performance run uses ordinary pages, physical-core CPU masks,
and 16 GiB / 32 GiB arenas at 8 / 16 workers respectively.

The initial performance screen and partial confirmation were **discarded** after
the research session reported overlapping local four-job Rust/fat-LTO builds.
Those samples cannot support a speed claim, despite apparent gains. A first quiet
retry then correctly refused an active TF2 benchmark before hashing. The clean
rerun uses an owned `gpuq` exclusive reservation, plus explicit coordination that
the research session will perform no local heavy work until the reservation is
released. This reservation is for CPU timing isolation; no GPU hashing is used.

The reserved run completed successfully at **2026-10-25 01:04:59 UTC**. All 24
reports passed independent source/CPU/kernel/PoW/backing audits; all invocation
metadata identify the owned reservation and confirm the requested affinity mask.
The frozen harness and published binary hashes are unchanged. There were zero
reported allocation failures, dropped events or control timeouts.

| Workers | AVX2 | AVX-512 | 256-bit AVX-512 | Streaming | Evidence |
|---|---:|---:|---:|---:|---|
| 8, spread over both CCDs | 7.052 H/s | 7.255 H/s | 7.002 H/s | 10.001 H/s | Two opposite-order screening passes |
| 16, one thread per core | 6.860 H/s | 7.010 H/s | 7.025 H/s | 10.216 H/s | Two opposite-order screening passes |
| 16, one thread per core | — | — | 7.066 H/s | 10.312 H/s | Four fresh alternating confirmation pairs |

Confirmation gain is **45.94%** geometrically across pairs, with individual gains
of 43.37%, 45.38%, 43.48% and 51.70%. Each invocation has a 20-second warmup and
two 20-second measured rounds. The control is the strongest mean uniform kernel
from the independent 16-worker screen, although its lead over full AVX-512 is
small. Some reports retain short-window/fence-jitter warnings; throughput includes
the fence, all four pairs improve substantially, and the final pair varies more.
These are within-session results on one desktop, not architecture-wide confidence
bounds or evidence that 16 workers is the optimal automatic worker count.

Raw reports, metadata, topology, summaries and generated native scripts are in
`evidence/windows/`. Parity logs and audits are in `evidence/`; the complete native
temporary tree was removed after collection. The reservation is terminal, no owned
native processes remain, and the desktop was explicitly released to research.
See `evidence/windows-cleanup.json` and `evidence/windows-reservation-complete.json`.

## Budget and cleanup

Live c7a price: $1.64224/hour. Lifetime through verified cleanup was at most
1,103.372 seconds, giving **$0.50334 compute** as a conservative upper estimate.
EBS, public IPv4 and small transfers are additional. The original $1.50 allowance
is retained in conservative accounting: old campaign commitments $37.4494 + $1
EPYC integration allowance + $1.50 this lease = **$39.9494/$40**. No budget reset.
The research session's separately approved $10 follow-up is not charged to this
lease or claimed as this session's authorization.

Instance `i-04f82f51670b91a3e`, its root volume, security group and local private key
were verified deleted at **2026-10-25 00:40:07 UTC**. The independent 35-minute
termination timer and shutdown-to-terminate behavior were checked before work;
cleanup completed early and was checked explicitly. Shared quota was released to
the research session. See `evidence/genoa-cleanup.json` and
`evidence/genoa-spending.json`.

## Production decision and remaining work

Accepted for follow-up: Genoa streaming at high worker counts and native Windows
9950X3D streaming with ordinary pages. Rejected: forcing
it at low counts, treating one exposed NUMA node as multi-socket validation, and
using contaminated desktop timings. No unproven default changes are included in
v0.2.22 or this branch.

Next production step is a narrow automatic-trial eligibility change for validated
identities in `src/miner/adaptive_kernel.rs`, retaining the independent ISA/OS
check and repeated-gain requirement. Before merging, run native productive
selection, cold/warm cache/restart tests and resource-pressure behavior on each
new identity/backing. Coordinate with the research session's PR #47 integration
instead of independently changing its tuner state machine. Candidate-set cache
fingerprints must continue to invalidate old incompatible decisions. These data
support allowing a measured trial; they do not justify a forced kernel or worker
count. Add no new opt-out unless a concrete failure mode requires one.

Remaining hardware coverage includes genuine multi-socket NUMA, high-end Zen 3,
Threadripper variants, and new identities/backing configurations beyond these
specific machines. Intel and ARM integration work remains owned by the research
session; do not duplicate its reservations or mix its candidate binaries with
this release baseline.
