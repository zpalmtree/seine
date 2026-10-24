# CPU memory tradeoff screening and energy tooling

This investigation follows the CPU performance review. Runtime joint
kernel/worker tuning and the efficiency-controller correction are owned by the
architecture/autotune sessions. The EPYC session owns expanded-register and
phase-specific kernels, NUMA locality, page-size and prefetch experiments. This
branch contains isolated research tooling and does not alter production hashing.

## Exact-reference dependency replay

Source baseline: v0.2.20 (`b63006f`). The `cpu-research` feature generates the
exact t=1,p=1 Argon2id dependency graph while hashing a full arena. Every hash is
independently checked against RustCrypto before replay. Measurements use the
full 2,097,152 KiB setting, a fixed 92-byte salt filled with 0x37, and nonces
0, 1 and 2 encoded as little-endian u64 passwords. Source/binary hashes and raw
result checksums are recorded in [manifest.json](manifest.json).

These are deterministic operation counts, **not throughput measurements**.
The elapsed times in the manifest are execution provenance, not benchmark
comparisons. Both policies are online: they use an address only when its block
is first produced, then retain that address for later reconstruction. Neither
uses future data-dependent addresses to decide what to store.

| Retention policy | Mean peak block storage | Mean compression work |
| --- | ---: | ---: |
| All blocks | 100.000% | 1.0000x |
| Every second block | 50.000% | 2.0049x |
| Reconstruction-cost threshold 2 | 61.810% | 1.3819x |
| Reconstruction-cost threshold 3 | 50.022% | 1.7497x |
| Reconstruction-cost threshold 4 | 44.183% | 2.1116x |
| Reconstruction-cost threshold 8 | 35.353% | 3.5344x |
| Reconstruction-cost threshold 16 | 30.680% | 6.3576x |

The cost heuristic saves a newly produced block when its estimated rebuilding
cost reaches the threshold. That estimate depends only on the two parents'
previously established costs. Seeds and saved blocks have zero rebuilding cost.
Live inputs and the previous forward output remain resident while needed;
output allocation is counted before releasing input slots.

Periodic spacing of 3 or more and cost threshold 32 did not finish within the
8x normal-compression budget. Their observed partial residency is not a
completed-hash memory requirement. The full raw results retain completion
fractions and null work ratios for those cases. [Summary](summary.json).

All policies also need reference metadata: this model records 8 MiB at full
size. The table excludes that metadata, policy bookkeeping, allocator overhead,
and SIMD scratch space. The simulator itself initially allocates the full
arena to obtain the trace; it does not demonstrate a reduced-memory miner.

Decision: uniform checkpoints are a weak next step. The cost-aware policy is a
more credible candidate for a future RAM-limited host, particularly thresholds
2 and 3. Extra computation, irregular reconstruction, cache/TLB behavior and
metadata handling may eliminate its capacity benefit. This result does not
justify enabling recomputation or predicting a CPU hashrate increase. No
reduced-memory path is added to normal mining. The current workstation does
not demonstrate a RAM-capacity bottleneck that would justify that complexity.

This is a simple heuristic screen, not the optimal ranking or low-storage
tradeoff described in [RFC 9106 section 7.2](https://www.rfc-editor.org/rfc/rfc9106.html#section-7.2).

## Energy instrumentation

The read-only [energy tool](../../scripts/bench_cpu_energy.py) implements
package-counter capture and paired worker/power comparisons. It records whole
invocation energy, handles bounded counter wraps, checks actual backing and
hash accounting, and cleans up its own benchmark process on interruption.
See [CPU_ENERGY.md](../../docs/CPU_ENERGY.md) for its exact measurement contract.

The [capability probe](energy-capability.json) found no counters in WSL. The user
explicitly deferred hardware energy measurements. Synthetic counter tests
validate arithmetic and process handling; they are not hardware measurements.
A real v0.2.20 one-worker, regular-page, full-size
[end-to-end report](energy-report-contract-smoke.json) validated the report
parser, including the completed hash produced during its fence. The same check
passed with the published [v0.2.21 binary](energy-report-contract-v0.2.21.json)
after integration with the automatic kernel tuner: four completed hashes,
including one during the fence, with no swap or OOM activity. These short runs
check the report contract; their throughput is not used as optimization evidence.

## Validation

- After integration with v0.2.21/master `0a63aec`, 11 kernel tests pass with
  `cpu-research`, including independent hash equality
  across supported kernels, non-aligned memory sizes, bounded reconstruction,
  live-block accounting, and prefix consistency of online retention.
- All seven default-feature kernel tests pass with research disabled.
- Full 2 GiB independent reference checks passed for all three recorded nonces.
- Strict kernel Clippy and Windows x86-64 GNU/macOS AArch64 all-target checks
  pass with the feature. Cross-compilation is not native runtime validation.
- 15 energy-tool tests cover arithmetic, domain deduplication and coverage,
  report rejection, simulated capture, comparison consistency and timeout/SIGTERM
  cleanup.
- Hardware energy measurements and production recomputation are deferred;
  production kernel dispatch and PoW parameters are unchanged by this branch.
