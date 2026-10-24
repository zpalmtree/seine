# Joint count/kernel prototype: native result and disposition

**Research only; keep PR #46 draft.** Frozen prototype `e6809bb` has the same
hash kernels as control `f20e7bb`. No production default has been changed.

The C5 guest was an Intel Xeon Platinum 8124M (Skylake), not the earlier
Cascade Lake 8275CL crossover machine. Both generic CPU-only binaries passed
native full-size scalar/AVX2/full AVX-512/256-bit reference parity. A fixed-kernel
gate at twelve workers had mixed differences; passing that gate is not proof of
zero regression or of better tuning.

Four 900-second windows completed in control/candidate/candidate/control order.
Each started a fresh process with a separate tuning cache; OS/file caches were
not flushed. The configured cap was twelve 2 GiB arenas.

| Window | Logged completed hashes | Elapsed seconds | Logged H/s |
| --- | ---: | ---: | ---: |
| joint-control-a | 5651 | 900.858 | 6.272909 |
| joint-candidate-a | 5544 | 901.892 | 6.147078 |
| joint-candidate-b | 5548 | 901.117 | 6.156804 |
| joint-control-b | 5593 | 900.829 | 6.208728 |

Paired differences were −2.006% and −0.836%; aggregate logged throughput was
6.240819 → 6.151939 H/s (−1.424%). These windows include startup, tuning trials
and shutdown. Unreported final hashes can be omitted; the rates are lower
bounds on logged work, but their ratios are not mathematical lower bounds on
relative gain. This result does not establish a universal loss; it provides no
basis for promoting this policy.

All four execution/health/cleanup protocols passed, with no swap or OOM.
The subsequent candidate lifecycle was intentionally interrupted after the
completed windows to free the shared quota for final Intel regression checks
within the campaign deadline. Raw `FAILED / InterruptedError(15)` is preserved
with a `CAMPAIGN-SCOPE-STOP.json` sidecar. Global cleanup passed: all registered
units inactive and hugepage pool zero. Do not count the interrupted lifecycle
as either a product failure or a pass.

The frozen prototype passed 506 local workspace tests (five opt-in tests
ignored), full-size local parity, all-feature/all-target Linux compilation and
Windows GNU test compilation. Cross-compilation is not native execution.
Pressure/restart coverage of this prototype and a repeatable productive-work
benefit across architectures are still required before promotion.

[Campaign evidence PR #42](https://github.com/zpalmtree/seine/pull/42) retains
normalized trajectories, raw hashes, fixed-kernel evidence, scope decision and
archive SHA256 `93cfb1e3fbff6fc4f63ebf7441fa994ba99d1d9cbb24cb0fe8628e3f74ce8dd5`.

## Trajectory review

The autotune owner independently reviewed all four trajectories. Connections
started in about 1.8 seconds; candidates spent about 197 additional seconds in
productive kernel comparisons. Final selections matched their paired controls
(nine workers in pair A, eight in B), and no alternate kernel was promoted.
This does not establish native successful repeated joint confirmation.

Candidate A's interruption at about 235 seconds triggered a five-minute kernel
cooldown; twelve workers were later rejected before that cooldown expired,
without an AVX2 comparison at that count. This is a coverage gap in the prototype.
Candidate B did test twelve-worker AVX2: 6.700 H/s versus 6.533, about +2.55%,
below the uncertainty-adjusted 6.764 H/s threshold (+3.53%). It correctly remained
unpromoted; the log does not imply the alternate kernel was slower.

A follow-up should preserve unresolved count candidates through kernel cooldowns
while bounding exploration overhead, then measure warm-cache amortization and
changing load/RAM. These are unimplemented research directions, not validated
improvements or changes to this frozen candidate.
