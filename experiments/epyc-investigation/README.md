# Sequential EPYC investigation, 2026-10-24

This directory preserves isolated experiments against **785a06f**, including the
portable `avx512-256` kernel. It changes neither production allocation policy nor
automatic kernel/thread selection. Results apply to the measured hosts and
worker counts, not all EPYC systems or the unidentified server screenshot.

The useful kernel result is a **64-block cached ring with streaming arena stores**.
At 24 workers on the Zen 5 EPYC 9R45 guest, long alternating comparisons measured
**45.14 completed H/s**, versus **35.33 for AVX2 (+27.78%)** and **36.04 for the
existing 256-bit AVX-512 kernel (+25.27%)**. Each control has three pairs, two
20-second measured rounds and one warmup per invocation. All six pair changes
are positive; final-hash completion and the end fence are included in elapsed
time. This is experimental: the one-worker screen regresses by 3.53%, and eight
workers are effectively flat. It is not a universal replacement.

## Findings, in investigation order

| # | Investigation | Measured result and decision |
|---|---|---|
| 1 | NUMA placement and local allocation | Balanced pools: +0.057%, no useful change. Deliberately one-node HugeTLB pool: local fallback/distribution +40.40% at eight workers. Preserve as a specific allocation-policy opportunity. |
| 2 | 1 GiB versus 2 MiB pages | +0.056% at 12 workers on Milan; no useful gain. |
| 3 | Earlier DD reference prefetch | 256-bit path regresses 0.88–4.96%; full-width path is +0.39% / -0.64% at one / 24 workers. Reject. |
| 4 | Different DI/DD kernels | Mixed kernels lose to the stronger uniform control. Reject tested mixtures. |
| 5 | T1 / NTA DD cache hints | T1 loses 3–7%; NTA stays within 0.3%. Retain T0. |
| 6 | Streaming stores and cached recent blocks | Confirmed +27.78% versus AVX2 and +25.27% versus uniform 256-bit AVX-512 at 24 workers. Retain experimental candidate for selective integration. |
| 7 | Four independent row rounds | Against full-width AVX-512: -3.49% / -1.97% at one / eight workers. Against 256-bit AVX-512 at 24 workers: -1.24%. Reject as a replacement. |
| 8 | NPS, channels, clocks and PMUs | Core-counter diagnostics completed. BIOS/NPS, DIMM channels, socket clocks/power and uncore counters are unavailable on these guests; physical-server work remains hardware-limited. |

`campaign.json` records decisions; `numa-results.json`, `kernel-results.json`,
and `phase-results.json` retain numeric evidence. `INTEGRATION.md` describes the
candidate's correctness requirements and remaining production validation.
Automatic tuning and shipping kernel selection remain with their existing owners.

Both rentals, their disks and temporary security groups are deleted, and private
SSH keys are removed. `evidence.json` records archive checksums, verified cleanup,
health checks and an exact completed/count/fence example. The Turin archive has
786 verified files and the Milan archive has 136. Across 124 accepted Turin
backend invocations (136 measured rounds), no swap/OOM events occurred; the
largest conservative single-CPU steal bound was 0.084% of measured time.

## Hosts and controls

- Milan: AWS c6a.8xlarge, EPYC 7R13, 16 physical cores / 32 threads, two NUMA
  nodes, two L3 groups, 64 GiB advertised memory. NUMA and page-size experiments.
- Turin: AWS c8a.8xlarge, EPYC 9R45, 32 physical cores without SMT exposed, one
  NUMA node, four L3 groups, 64 GiB advertised memory. Kernel experiments.
- Both are disposable guests. Guest NUMA/page placement does not establish the
  hypervisor's physical placement. Neither provides BIOS/NPS access, DIMM channel
  controls, or a complete physical socket. Milan exposes no CPU PMU; Turin exposes
  a core PMU but no IBS, L3, or memory-controller PMU.
- Every accepted backend comparison uses the same fixed thread count, automatic
  topology placement, explicit kernel, disabled autotuning, and verified backing.
  Variants run sequentially with alternating A/B and B/A order. Reports retain
  counted and completed H/s; comparisons use completed hashes / elapsed time.
- NUMA/page trials use three pairs of two 20-second measured rounds after one
  warmup. Short kernel screens use two opposite-order pairs, one 12-second
  measured round after one warmup. A short screen is not adoption evidence.
- Long streaming confirmations use three pairs per control, two 20-second
  measured rounds after one warmup. Pair changes versus AVX2 are +28.418%,
  +27.340%, +27.585%; versus uniform 256-bit AVX-512 they are +25.516%,
  +24.991%, +25.304%. These describe repeatability within this guest.
- Checks reject allocation fallbacks in strict modes, timed backend errors,
  swap, OOM kills, and aggregate guest steal above 2%. Arena traces query one
  resident address per 2 MiB using `move_pages`, not every ordinary page.

## Reproduction

Use a **dedicated disposable worktree** at 785a06f. The generator deliberately
replaces the two kernel source files from that frozen commit. Do not run it in a
worktree containing unrelated kernel edits.

1. Apply `arena-hook.patch`; copy `arena_audit.rs` to
   `src/backend/cpu/epyc_arena_audit.rs`.
2. Run `build_kernel.py VARIANT --target /absolute/isolated/target`. It generates
   and formats the candidate with Rust 1.93.0, runs kernel tests, builds the CPU
   binary and verifier, verifies full-size RustCrypto parity, and saves sources,
   patches, disassembly and SHA-256 metadata under the ignored data directory.
3. Copy frozen variant directories to `kernels/` on a dedicated host. Reserve the
   required local HugeTLB pools before timing. `run_kernel.py` uses `bench.py` and
   fails if a requested output directory already exists.
4. Most candidates use the explicit CLI label `avx512-256`. The full-width
   `early-seed-512`, `dd-t1-512`, and `dd-nta-512` candidates use `avx512` instead.
   Candidate identity is the variant **plus binary hash**, not just the report's
   kernel label. Automatic preference is unchanged.

`run_numa.py`, copied to the remote directory also as `numa_common.py`, supports
`run_imbalance.py`. `run_pages.py` reserves both page sizes, verifies pool counts,
and restores original pools in `finally`. These scripts target the explicitly
documented disposable topology, not a general production host.

To reproduce phase diagnostics, first restore the frozen kernel sources, apply
`phase-profile.patch`, and copy `epyc_phase.rs` to
`pow-kernel/examples/epyc_phase.rs`. Build the example with `epyc-profile` enabled.
`run_phases.py` takes actual backend CPU placement from `phase-cpus.json`, prepares
workers serially, and starts their timed loops together. Each worker checks a
full-size reference and dirty reuse before reporting READY. These split-loop
timings guide hypotheses; backend measurements decide throughput.
The initial phase pass used the same nonce sequence in each private worker
arena. The final PMU pass uses `cpu << 32 | count` to vary DD streams across
workers; both versions are archived. DI addresses are inherently precomputed
and shared in pattern, while the arena storage remains private.

`pmu.py` can attach four grouped user-space core counters after READY, excluding
reference hashing, arena allocation and warmup. It rejects unavailable or
multiplexed groups. Counts include small userspace reporting/exit overhead, and
are diagnostic guest measurements. A six-counter group could not schedule on
the Turin guest; no values from that failed probe enter the results.

For optional streaming-store counters, `prepare_loop.py` derives a probe using
the normal hashing API instead of the split profiler. After the phase-helper
setup above, generate the desired kernel, build `epyc_loop` without the
`epyc-profile` feature, and freeze that executable separately. Its phase fields
are zero; only hashes, elapsed time, and grouped counter deltas are meaningful.
The normal API keeps the streaming ring alive across both phases of each hash.
After the complete independent-stream core-counter pass, broader request/fill
counter groups were deferred to prioritize long paired throughput confirmation
and stronger kernel controls within the lease. The request-group process was
stopped during preparation, before its timed interval; its partial directory and
the explicit plan-change record are retained. No completed throughput pair was
removed by that decision.

The completed normal-loop store probe (one baseline then one candidate, 24
workers, 15 seconds) measured **24.06% fewer cycles/hash**, **8.68% more retired
instructions/hash**, and **42.70% fewer L2 data-cache store requests/hash** for
the streaming candidate. All four counters ran without multiplexing. This is
consistent with reduced cache-store pressure, but is one diagnostic pass and
does not measure DRAM bandwidth. The WCB closure event increased 34.43%; its
absolute counts were not validated as a count of all streaming cache lines, so
do not convert it into memory traffic. The guest exposes no cpufreq control or
EDAC channel inventory. The full-width uniform path's lower instruction count
but higher cycles at 24 workers likewise rules out instruction count alone as
a useful performance predictor here.

Regenerate the summaries from the verified Turin archive with
`analyze_archive.py ARCHIVE --out kernel-results.json` and
`analyze_profiles.py ARCHIVE --out phase-results.json`.

## Candidate details

- **NUMA allocation:** balanced pools showed no useful benefit from strict local
  binding. The deliberately imbalanced pool placed all eight arenas on one
  node, half remote. Strict local allocation with automatic local THP fallback
  restored both locality and distribution across memory nodes. Its approximately
  40% gain therefore combines these effects; it is not a general remote-latency
  percentage. The separate balanced, all-remote control lost about 7%.
- **Earlier seed:** compute the next reference immediately after column zero's
  final `a` update, before the final `d/c/b` updates and scatter. Inspect assembly
  to verify actual instruction placement and unchanged control functions.
- **Hybrid phases:** specialize slices 0–1 and 2–3 to different existing kernels.
  There is no per-block ISA decision. Reference addressing and the 2 GiB arena
  remain unchanged.
- **DD cache hints:** substitute T1 or NTA for T0 on the data-dependent prefetch
  only. Data-independent lookahead and 16-cache-line coverage remain unchanged.
- **Streaming ring:** retain 64 recent 1 KiB outputs in a private cached ring;
  stream full cache lines into the arena; issue MFENCE every 64 blocks and before
  finalization. A reference less than 64 blocks old reads the ring. A reference
  exactly 64 blocks old must read the fenced arena because its ring slot aliases
  the destination. This is a performance prototype, not production policy.
  `verify_candidate.rs` checks ring/slice boundaries, three inputs, dirty reuse,
  output guards, and the full 2 GiB configuration. The ring is 64 KiB; it does not
  fit completely in a Zen 5 core's 48 KiB L1 data cache.
- **Four rows:** interleave four independent 256-bit row rounds to use additional
  AVX-512VL registers. Columns retain the original paired schedule. Confirm
  register use and code size in disassembly before interpreting timings.

## Hardware limits and primary references

Scaleway, used for earlier full-server experiments, explicitly disallows user
BIOS changes. Its remote KVM option does not authorize an NPS/firmware sweep.
The current AWS standard-instance quota is 32 vCPUs; a bounded, support-case-free
request for 64 was not approved. No support ticket was opened. BIOS/NPS, physical
channel population, and full-socket clock/power experiments therefore need
hardware with the relevant control and visibility.

- [AMD Zen 5 software optimization guide](https://docs.amd.com/v/u/en-US/58455_1.00)
- [AMD EPYC 9005 BIOS/workload guide](https://docs.amd.com/v/u/en-US/58467_amd-epyc-9005-tg-bios-and-workload)
- [Linux HugeTLB documentation](https://docs.kernel.org/admin-guide/mm/hugetlbpage.html)
- [Linux Zen 5 PMU event definitions](https://github.com/torvalds/linux/tree/master/tools/perf/pmu-events/arch/x86/amdzen5)
- [Scaleway BIOS policy](https://www.scaleway.com/en/docs/elastic-metal/faq/#can-i-change-the-bios-settings-or-update-the-firmware-of-an-elastic-metal-server)

Raw artifacts live under `data/epyc-investigation/`, including frozen executables,
source hashes, excluded pilots, complete reports, topology, cloud ownership and
cleanup records. Downloads are verified against remote SHA-256 manifests before
deleting their host. Private rental SSH keys are removed during cleanup.

One imbalanced-pool pilot was excluded in full after discovering that the checker
expected startup fallback failures in interval-only benchmark counters. The
corrected run verifies actual page backing and zero timed failures. Also, the
first summary script used floor-indexed resampling percentiles; later summaries
use interpolated percentiles. Paired rates and percentage changes are unaffected.
The final analysis uses individual pair changes and recomputed descriptive
intervals rather than treating two-pair intervals as population confidence.
