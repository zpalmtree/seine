# Native ARM runtime SHA3 validation

Baseline: v0.2.21 (`359265e`). Current PoW: 2 GiB Argon2id, one iteration,
one lane. Rates use completed hashes divided by actual elapsed time including
the round-end fence. These are measured configurations, not promised gains.

## Graviton3 / Neoverse-V1

Four alternating pairs used separate Cargo targets for baseline and candidate,
generic AArch64 compilation without global `+sha3`, and explicit 2 MiB HugeTLB
backing. Each run used two 30-second measured rounds after a warmup round.

| Physical workers | Baseline NEON H/s | SHA3 + 64-byte prefetch H/s | Paired gain range |
| --- | ---: | ---: | ---: |
| 1 | 0.820 | 1.228 | +49.63% to +49.82% |
| 16 | 12.316 | 17.930 | +44.61% to +46.18% |

Regular-page confirmation used two alternating pairs: one worker 0.756 → 1.038
H/s (+37.09% to +37.48%); eight workers 5.862 → 7.791 (+32.69% to +33.14%).
Earlier screens isolated the SHA3 and prefetch contributions before combining
both. No failed allocation, dropped event, control timeout, or unexpected page
fallback occurred in the accepted benchmark reports.

Native full-size reference parity passed for nonces 0, 12345 and u64::MAX,
switching NEON → SHA3 → NEON with dirty arena reuse. Small arena tests cover
segment boundaries and guard regions. Real full-arena worker switching passed.
Native CPU-only workspace tests and strict kernel-crate Clippy passed.

Productive automatic mining confirmed `neon-sha3` after 194.1 seconds, followed
by a confirmed two-to-three-worker increase at 289.1 seconds. CPU load and RAM
pressure phases passed. A constrained restart exposed a separate pre-existing
worker-capacity bug; its repair is a prerequisite branch. The failed lifecycle
is retained and is not described as an end-to-end pass.

## Graviton2 / Neoverse-N1

This CPU does not expose SHA3. The generic candidate passes native full-size
reference parity for all three nonces and dirty reuse. Both construction and
runtime selection reject `neon-sha3`, leaving baseline NEON active. The real
full-arena worker switch test passes.

Four longer alternating HugeTLB pairs also confirm the candidate's ordinary
NEON fallback against v0.2.21: one worker **0.6822 → 0.7070 H/s** (+3.60% to
+3.67%); sixteen workers **10.5834 → 11.0587 H/s** (+4.38% to +4.55%). These
are fallback code-generation results, not SHA3 execution on unsupported hardware.
A separate prefetch64-only screen did not beat this fallback, so no additional
kernel identity is proposed.

The exact generic verifier built on Graviton3 (SHA256
`c590c348f34a8b579fb51670394368795059818c1d702d738cc4d621a029a4ac`)
also ran unchanged on Graviton2: unsupported SHA3 selection was rejected and
all three full-size NEON reference/dirty-reuse cases passed. The 43-file
confirmation archive was individually verified, SHA256
`f8475309f38f454bbad373729efa4b4339ce5ca8ca526d963bc3d943535d7107`.

## Independent review

The existing research-session owner reviewed commit `079d5db` against the
memory-recovery prerequisite and found no blocking issues. The read-only review
covered runtime guards, private ISA state and target-feature boundaries, generic
fallback, prefetch bounds/non-aliasing, cache support checks and automatic
candidate limits. It did not independently rerun the hardware measurements.

## x86 regression and remaining confirmation

Ice Lake ran two clean alternating pairs against the separately rebuilt
CPU-only baseline: approximately −0.29% at one worker and −0.27% at twelve.
Full-size scalar, AVX2, AVX-512 and AVX-512/256 reference parity passed. Strict
workspace Clippy reports the same existing diagnostics on both builds; strict
kernel-crate Clippy passes.

The kernel library also builds with pinned Rust 1.93.0 for
`aarch64-apple-darwin` in an isolated Cargo target. This verifies compilation
only; Apple Silicon candidate execution and performance remain untested.
Native Graviton4 and the repaired complete lifecycle remain pending. No Apple cache-line policy change is
made. The startup preference stays NEON everywhere; SHA3 is runtime-guarded and
must pass the normal repeated productive comparison. Fixed-thread opt-out keeps
the startup path. Experimental AVX-512/256 remains outside automatic candidates.

Raw reports, archive hashes and normalized rounds are retained by the separate
architecture evidence PR. The production source tree here matches campaign
candidate `1246967` apart from documentation/comments.
