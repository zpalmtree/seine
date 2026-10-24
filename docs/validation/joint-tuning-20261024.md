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
