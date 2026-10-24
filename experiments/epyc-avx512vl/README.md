# EPYC 256-bit AVX-512 kernel experiment

This experiment evaluates a 256-bit AVX-512 implementation on a temporary
EC2 c7a.2xlarge guest used only for this experiment: EPYC 9R14,
AuthenticAMD family 25/model 17/stepping 1,
eight physical cores, one NUMA node and one 32 MiB L3 group. It does not establish
performance on a full socket, on Siena/Zen 4c, or on older AVX2-only EPYC CPUs.

The candidate retains the existing paired AVX2 row/column layout while replacing
its rotate sequences with native AVX-512VL rotates. Unlike the existing 512-bit
path, its data-dependent reference prefetch starts after column zero instead of
after the first two columns. These changes were tested together; the measurements
do not isolate their individual contributions.

## Completed cells against the existing AVX-512 path

Completed-hash throughput includes hashes finished at the round fence and uses
actual elapsed time. The gain column is the geometric mean of paired ratios.

| Page backing | Workers | Existing AVX-512 | 256-bit candidate | Paired gain |
| --- | ---: | ---: | ---: | ---: |
| 2 MiB HugeTLB | 1 | 2.18951 H/s | 2.31460 H/s | +5.71% |
| 2 MiB HugeTLB | 4 | 7.02106 H/s | 7.40921 H/s | +5.53% |
| Ordinary pages | 4 | 5.57921 H/s | 5.68311 H/s | +1.86% |

All three pairs improved in each cell. The respective pair ranges are
+5.62% to +5.82%, +5.21% to +6.03%, and +1.70% to +2.02%. Counted-hash gains are
+5.87%, +5.66%, and +2.00%.

## Same-binary control against paired AVX2

| Page backing | Workers | Paired AVX2 | 256-bit candidate | Paired gain |
| --- | ---: | ---: | ---: | ---: |
| 2 MiB HugeTLB | 1 | 2.26686 H/s | 2.31522 H/s | +2.13% |
| 2 MiB HugeTLB | 4 | 7.40398 H/s | 7.42390 H/s | +0.27% |

The one-worker completed-hash gains repeat across all three pairs
(+2.20%, +2.13%, +2.07%). The four-worker pairs are +0.34%, -0.12%, +0.58%:
**no clear four-worker advantage over AVX2 is demonstrated**. Counted-hash gains
are +2.18% and +0.26%. This is why a gain against the old AVX-512 path alone is
insufficient to establish the best EPYC dispatch policy. No automatic preference
is added by this experiment.

## Integrated three-path confirmation

The architecture session integrated the module as explicit `avx512-256` at
`ac4febae92970dddb9012812df50337f26b19247`. That portable executable passed
full 2 GiB RustCrypto parity for scalar, AVX2, AVX-512 and `avx512-256` on the
EPYC host. Two opposite-order passes compared all three SIMD paths at four
workers, with one warmup and one measured 20-second round per invocation.
These shorter checks confirm integration; they do not replace the three-pair
experiments above.

| Completed-hash comparison | HugeTLB | Ordinary pages |
| --- | ---: | ---: |
| AVX2 vs existing AVX-512 | +4.07% | +2.06% |
| 256-bit path vs existing AVX-512 | +5.42% | +1.98% |
| 256-bit path vs AVX2 | +1.30% | -0.08% |

The small four-worker difference between AVX2 and the new path varies with the
build/run protocol. Their ordinary-page result is effectively tied. AVX2's
advantage over the existing AVX-512 default is more consistent. The architecture
session owns higher-worker validation and any default/cache policy change;
this kernel experiment makes neither change.

## Fused-row follow-up: rejected

The follow-up fuses input XOR/backup with the 256-bit paired row rounds, removing
an intermediate scratch-buffer write/read. It preserves the column phase and
prefetch placement. `fused-candidate.patch` applies **after** `candidate.patch`.

Three alternating four-worker HugeTLB pairs used the same 20-second/two-round
protocol. Completed throughput fell from **7.40367 to 6.41361 H/s (-13.37%)**;
the three deltas were -13.36%, -13.38%, and -13.37%. Full-size reference parity
passed on the host, so this is a performance rejection. No fused-row change is
included in the standalone kernel module.

## Source and protocol

- Frozen source: `9c414716471b19970336539cf64d91d0de59635c`, the architecture
  session's explicit-kernel benchmark API. Autotuning is disabled throughout.
- Portable Rust 1.93.0 CPU-only release builds, fat LTO, one codegen unit, no
  `target-cpu=native` or extra RUSTFLAGS. The first comparison differs only in
  the AVX-512 compression implementation (plus build identity).
- Three alternating pairs per cell, one warmup round followed by two measured
  20-second persistent-backend rounds. Each process exits before the next starts.
- Fixed one/four workers; explicit 2 MiB HugeTLB backing, plus a four-worker
  ordinary-page cell. Automatic affinity has the same order as OS affinity on
  this guest's single-node, single-L3, no-SMT topology.
- The direct AVX2 control uses the **same candidate executable** for both paths:
  its unchanged paired AVX2 implementation versus its experimental 256-bit path.
- The experimental replacement binary still labels its path `avx512`. Binary
  hashes and the experiment manifest distinguish the implementation. The separate
  architecture integration introduces a distinct explicit selection label.

`candidate.patch` captures the tested replacement, while kernel-module commit
`7cf7981` extracts it into `pow-kernel/src/fixed_argon/avx512_256.rs` for dispatch
integration. The latter has no automatic selection preference by itself.

## Correctness and controls

Both experimental executables passed full 2 GiB RustCrypto reference parity,
locally and on the EPYC host. Existing small-memory tests cover multiple sizes,
salts and nonces. The extracted module also compares ordinary and mid-prefetch
compression against scalar results for edge/random inputs, identical input
references, and guarded output blocks. Six kernel-crate tests pass. Windows
x86-64 GNU and macOS AArch64 cross-checks compile; those platforms were not run.

The runner verifies worker counts, exact arena backing bytes, allocation errors,
event drops, backend timeouts, swap/OOM counters and CPU steal time. The initial
pilot stopped after one baseline because the checker used the wrong hugepage
field name. It restored the pool, was archived separately, and is excluded from
all paired results. The accepted run uses `memory_explicit_large_workers` and
checks both worker counts and bytes.

Disassembly confirms native 256-bit rotates and no ZMM instructions in the
candidate compression functions. The compiler uses only YMM0–15 here, so no
performance claim is attributed to the larger AVX-512 register namespace.

## Reproduction and evidence

Use a disposable host with adequate free memory and a dedicated experiment
directory. `run.py` expects `bin/{baseline,candidate,verify-baseline,verify-candidate}`
and requires sudo only to reserve and restore the HugeTLB pool. It never connects
to a pool or daemon, and creates a new `results` directory rather than overwriting
an existing run. A caller should also impose a whole-experiment timeout.

```sh
python3 experiments/epyc-avx512vl/prepare.py
rustup run 1.93.0 rustfmt --edition 2021 --config skip_children=true \
  data/epyc-avx512vl/generated-fixed_argon.rs
```

The generator writes an artifact, not the checkout's kernel. Build the frozen
baseline in an isolated checkout and save its binaries before copying that
generated file into the isolated checkout for the candidate build. Use the
`verify_cpu_kernels` example for each variant. `candidate.patch` is an alternative
to the generator and applies to the frozen base above.

```sh
# Inside the prepared remote experiment directory:
timeout 32m python3 -u run.py

# In a separate directory with both miner paths pointing to the same candidate:
timeout 16m python3 -u run.py --baseline-kernel avx2 --cells large:1 large:4

# Analyze a downloaded result directory:
python3 experiments/epyc-avx512vl/analyze.py PATH/TO/results
```

Local binaries, generated sources, disassembly, raw reports, host identity,
build logs and cloud lifecycle records are retained under ignored
`data/epyc-avx512vl/`, `data/epyc-control/`, `data/epyc-final/`, and
`data/epyc-vl-fused/` in the EPYC worktree. An older 512-bit row-fusion patch was
also prepared locally but not run on the server; the measured follow-up above
applies fusion to the 256-bit candidate instead.

The test instance, root volume, security group and both temporary private keys
were removed and verified. Runtime was 54.0 minutes, approximately $0.37 in
compute charges. `verification.json` records cleanup and 55 counter-check
windows; the 196-file raw-artifact manifest is retained locally. Maximum observed
CPU steal was 0.00306%, with no swapping, OOM events, backing failures or backend
timeouts. The final scheduled shutdown blocked new unprivileged SSH logins five
minutes before power-off; ephemeral EC2 Instance Connect root access restored
access, all remaining results were retrieved, and manual termination completed.
