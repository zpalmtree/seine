# Streaming-ring candidate handoff

Production integration now gives this implementation its own
`avx512-256-stream` identity, leaves the original 256-bit path intact, and gates
automatic trials on validated AMD family 26/model 2 plus ISA/OS support.
Repeated productive comparisons decide whether to use it. The requirements
below document the original prototype and remain relevant to future changes;
the production tests additionally cover live switches and dirty arena reuse.
See [integrated validation](INTEGRATED_VALIDATION.md) for the final builds,
native comparisons and productive-selection evidence.

`streaming-candidate.patch` is the exact kernel-only experimental patch against
785a06f. It does not include the shared arena-audit hook used to validate page
placement. The measured executable is identified in `frozen-kernels.json` and in
each paired protocol. `streaming_ring.rs` and `prepare_kernel.py` explain and
regenerate the implementation. At that research checkpoint, production source
files were unchanged.

The archived prototype used explicit benchmark selection of `avx512-256` and
replaced that implementation only in its isolated executable. Production uses
the distinct identity above and an architecture eligibility gate, preserving
the original 256-bit path for independent measurement. It requires AVX2,
AVX-512F and AVX-512VL. Do not enable the path on ISA support alone before
correctness and performance validation on the intended architecture.

The useful outcome is at 24 workers on the measured EPYC 9R45 guest. It regresses
at one worker and is flat at eight. Productive kernel/worker tuning decides whether to retain it; there is no
supported universal worker cutoff or blanket replacement. Any cache fingerprint must distinguish implementations.

Memory and correctness requirements:

- Preserve the entire consensus 2 GiB arena, one lane and one pass. The 64 KiB
  ring is additional cached storage, not a time-memory tradeoff.
- For `cur - reference < 64`, read the ring. At exactly 64, use the arena: the
  ring slot aliases the destination. References are strictly behind the output
  only because this is the fixed one-pass, one-lane path.
- Fence previous NT batches before an older arena reference can be read, and
  fence the final batch before finalization consumes the final arena block.
  Keep complete, aligned cache-line destination writes.
- The prototype allocates the ring per hash. Hoisting it into persistent worker
  state is a separate change requiring dirty-reuse, kernel-switch and performance
  validation; do not silently substitute that change for the measured binary.
- Existing validation includes all supported kernel outputs against RustCrypto
  at 2 GiB; three additional full-size streaming inputs with repeated dirty reuse;
  small ring/slice/rounding boundaries; and sentinel blocks around the arena.
  Production validation additionally exercises both directions of live kernel
  switching on one persistent arena, nonce continuity and worker teardown; the
  backend suite retains cancellation and error/accounting coverage.

Long backend confirmation retains counted hashes, late hashes, elapsed time,
the end fence, allocation backing and per-run health counters in the raw reports.
The reported rates include completed work and its elapsed time. Evidence is
within one guest/host; it does not establish a general percentage for all EPYC
servers. CPU PMCs can support the traffic hypothesis, but the guest has no memory
controller or L3 PMU for direct socket bandwidth measurements.
