# Large servers: NUMA, L3 groups and memory headroom

Seine is built to fill big machines. On Linux, the default `--cpu-affinity auto`
spreads workers across usable NUMA nodes and across L3 cache groups within each
node, using one thread per physical core before SMT siblings. This also spreads
workers on single-node servers with multiple L3 groups.

## How placement works

- Respects the process CPU and memory masks.
- Each worker pins itself before allocating its 2 GiB arena, so Linux's local
  allocation policy places memory near that worker. Existing memory policies still
  apply; placement does not enforce memory binding or guarantee local huge-page
  availability.
- Missing or inconsistent cache data keeps NUMA-only placement. A single node with
  one cache group (or unavailable cache data), and incomplete NUMA/core topology,
  keep OS CPU order.

Opt-outs:

- `--cpu-affinity os`: pin in OS CPU order without topology reordering.
- `--cpu-affinity off`: let the scheduler place workers.

Compare `auto` against `os` with the same thread count and verified page backing
before attributing a hashrate change to placement. HugeTLB pools need enough pages
on the nodes hosting workers; a sufficient global count alone does not guarantee
local placement. See [Linux HugePages](linux-hugepages.md).

## Memory headroom

Automatic thread sizing and autotuning reserve 5% of effective total RAM (at most
512 MiB on hosts with 16 GiB or more, else at most 1 GiB), in addition
to any GPU/Metal reservation. Adjust this with `--cpu-memory-reserve-percent <0..99>`;
`0` disables that reserve. Explicit thread counts keep their existing memory checks
and disable tuning unless `--cpu-autotune-threads` is requested.

Automatic CPU workers also release memory when headroom falls and return once
conditions stabilise, including pausing every CPU worker if no complete arena
fits.

## Kernel selection on EPYC

Automatic tuning compares the CPU kernels available on your processor while
mining, and only switches after two confirmed gains at the current worker count and page backing. No configuration is
needed. Results for each EPYC generation are on the [performance page](../PERFORMANCE.md).

Autotune caches are invalidated when the allowed CPU set or detected NUMA/L3 layout
changes, including changes that leave the CPU count unchanged.

How thread counts, kernels and memory recovery are tuned: [automatic tuning](autotuning.md).
