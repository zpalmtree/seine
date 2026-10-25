# blocknet-pow-kernel

Reusable fixed-parameter Argon2id kernel extracted from Seine's CPU backend
for reuse by external verifiers.

This crate exports:
- `FixedArgon2id`
- `PowBlock`
- `ReusablePowContext`

## CPU memory-recomputation research

The opt-in `cpu-research` feature adds exact dependency tracing and bounded
checkpoint-recomputation simulations. It is not enabled by the miner and does
not change PoW parameters or automatic kernel dispatch.

```bash
# Reduced-memory screening; not a production hash-rate benchmark.
cargo +1.93.0 run --release --locked -p blocknet-pow-kernel \
  --features cpu-research --example cpu_tmto -- 65536 0 8

# Production parameters, nonce 0, stop each replay at 8x normal compressions.
cargo +1.93.0 run --release --locked -p blocknet-pow-kernel \
  --features cpu-research --example cpu_tmto -- 2097152 0 8
```

Arguments are memory KiB, nonce, and maximum work factor. JSON goes to stdout;
diagnostics go to stderr. Every trace is checked against RustCrypto first,
using sequential arenas to keep peak arena allocation near 2 GiB at production
parameters. Normal host and cgroup headroom is still required.

The simulator compares fixed-spacing checkpoints with an online reconstruction
cost heuristic. Both retain the previous output, count live input/output block
slots, and use an explicit dependency stack. Cost decisions use only already
known parents; future data-dependent references never inform retention. An
incomplete replay has a null work ratio, not an estimated completed result.

Reported residency is block storage plus a separately reported reference-table
size. It excludes implementation-specific allocation, index and policy metadata.
This is not a reduced-memory hashing implementation, an optimal tradeoff search,
or a prediction of CPU throughput. See the [full-size experiment](../experiments/cpu-research-20261024/README.md).
