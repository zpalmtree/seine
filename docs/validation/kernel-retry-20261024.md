# Kernel retry merge gate

Keep PR #41 draft. Standalone `c654937` passed its original 479 CPU-only tests
(four ignored), but final native Ice Lake AVX2 confirmation at twelve workers
measured 11.197785 → 11.095259 H/s. The two actual reversed pairs were −1.010509%
and −0.820854%. Both builds were generic Rust1.93 CPU-only release builds in
isolated targets, with full-size reference parity and verified 24GiB backing.
Independent audit verified all four reports, rates, raw hashes, order, identities
and all20 archived files. No performance-related source change was made afterward.

This is a regression signal in this configuration, not proof of a causal source
change or a universal loss. It remains unresolved even though the fixed-count
benchmark bypasses the adaptive controller. Instruction similarity is not runtime
performance equivalence. Do not merge this PR on correctness tests alone.

Archive SHA256: `02148ced3967e3fdfc46bdd8fc5ba685c056c42a138e4472ea079998e3be2a61`.
The full evidence, including larger-stack losses and review, is in campaign PR #42.
