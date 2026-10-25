# Seine benchmark lab controller

`scripts/benchctl.py` captures benchmark conditions, analyzes the paired output
from the CPU and NVIDIA A/B scripts, and records reproducible manifests around
arbitrary benchmark commands. It uses only the Python standard library and
never changes host settings or closes applications.

## Capture the host before testing

Run the preflight from the repository containing the build under test:

```bash
python3 scripts/benchctl.py preflight \
  --repo . \
  --artifact target/release/seine \
  --output data/preflight.json \
  > data/preflight.stdout.json
```

The JSON on stdout includes the runtime (native Linux, WSL, macOS, or Windows),
kernel, visible CPU topology, memory and swap, load, available power-policy
hints, HugeTLB/THP state, an NVIDIA snapshot when `nvidia-smi` is available,
Git/toolchain/build identities, and structured confounder warnings. A concise
assessment is printed to stderr, so redirecting stdout remains safe for
automation. `--artifact` can be repeated to hash more than one binary.

Preflight warnings are observations, not automatic host changes. In particular,
the controller does not stop GPU applications, change a power plan, reserve
pages, or restart WSL.

## Analyze an interleaved A/B run

Both `bench_cpu_ab.sh` and `bench_nvidia_ab.sh` produce a compatible
`results.tsv`:

```bash
python3 scripts/benchctl.py compare \
  data/bench_cpu_ab_backend_20260710_120000/results.tsv \
  --json-output data/cpu-comparison.json
```

For CPU lane-count comparisons, pass `--baseline-threads` and
`--candidate-threads` to `bench_cpu_ab.sh`. `--threads` remains the shared
default when either variant-specific value is omitted.

For page-policy comparisons, use `--page-mode` for both variants or
`--baseline-page-mode` / `--candidate-page-mode` independently. When these
flags are present, the harness verifies report schema 12 backing telemetry,
rejects a required-large run that fell back, rejects THP contamination in a
regular-page control, and rejects backing changes between repeated runs. For
example, compare the same Windows or Linux binary with ordinary and explicit
large pages:

```bash
bash scripts/bench_cpu_ab.sh \
  --baseline-dir . --candidate-dir . \
  --baseline-binary /absolute/path/to/seine \
  --candidate-binary /absolute/path/to/seine \
  --baseline-page-mode regular \
  --candidate-page-mode large \
  --threads 8 --pairs 4 --bench-secs 20 --bench-rounds 3
```

For same-binary configuration comparisons, repeat `--baseline-miner-arg` and
`--candidate-miner-arg` once per argument. Arguments after `--` still apply to
both variants. For example:

```bash
bash scripts/bench_cpu_ab.sh \
  --baseline-dir . --candidate-dir . --threads 14 \
  --baseline-miner-arg '--cpu-affinity' \
  --baseline-miner-arg pcore-only \
  --candidate-miner-arg '--cpu-affinity' \
  --candidate-miner-arg off
```

For prebuilt or differently instrumented builds, pass absolute executable paths
with `--baseline-binary` and `--candidate-binary`. The harness runs those files
directly, so Cargo-only options such as `--native`, `--no-default-features`, and
`--features` are intentionally rejected for that side.

The comparison rejects missing, duplicate, non-positive, malformed, or
unpaired rows. Three complete pairs are required by default; change this only
with `--min-pairs`. It reports:

- arithmetic baseline and candidate means;
- sample coefficient of variation for each variant;
- the mean paired log ratio, expressed as a geometric percentage delta;
- pair-sign consistency; and
- a deterministic 95% paired-bootstrap confidence interval.

The paired log ratio is the primary estimate because each candidate run is
matched to its neighboring baseline run. This reduces the effect of thermal or
background-load drift that would distort a ratio of two unpaired means.

Use confidence-bound gates for a statistically supported decision:

```bash
# Exit 10 only for a confirmed regression worse than 2%: CI upper bound < -2%.
python3 scripts/benchctl.py compare results.tsv \
  --max-regression-pct 2 \
  --regression-exit-code 10

# Exit 11 unless the CI lower bound establishes at least a 1% improvement.
python3 scripts/benchctl.py compare results.tsv \
  --require-improvement-pct 1 \
  --improvement-exit-code 11
```

Confidence bounds are the default gate statistic. Pass
`--gate-statistic estimate` to gate directly on the paired geometric point
estimate. Bootstrap results are reproducible for a fixed `--seed` and
`--bootstrap-samples`.

## Wrap an existing benchmark command

The run wrapper executes an argument vector directly, without a shell, and
records preflight, exact argv, working directory, selected performance-related
environment variables, UTC start/end times, elapsed time, and exit status:

```bash
python3 scripts/benchctl.py run \
  --cwd . \
  --output-dir data/runs/cpu-backend-01 \
  --artifact target/release/seine \
  --label cpu-backend-01 \
  -- target/release/seine \
     --bench --bench-kind backend --backend cpu --threads 16 \
     --bench-secs 30 --bench-rounds 5 --ui plain
```

The child inherits the terminal and its exit code becomes the wrapper's exit
code. `preflight.json`, `postflight.json`, and `manifest.json` are written
atomically in the output directory. The v2 manifest embeds both host captures
and records post-minus-pre available-memory and swap deltas, which makes a run
that created memory pressure visible even when the benchmark itself remained
fast. Shell metacharacters in child arguments remain literal arguments.

Run the controller tests with:

```bash
python3 -m unittest scripts/test_benchctl.py -v
```


## CPU kernel selection and architecture evidence

One portable executable includes the CPU kernels for its target architecture.
Automatic dispatch remains unchanged: x86-64 prefers AVX-512F/VL, then AVX2,
then scalar; AArch64 uses NEON. These are instruction-set paths, not claims of
separate tuning for every Zen or Intel generation.

For an explicit, offline comparison, use `--bench-cpu-kernel`. It requires
`--bench`, a fixed `--threads` argument, CPU-only backends, and CPU autotuning
to be disabled. Explicit `--threads` already disables implicit autotuning;
`--disable-cpu-autotune-threads` makes that intent clear. Combining the override
with forced autotuning is rejected. The override is not persisted and does not
read or write CPU autotune caches. There is no global/environment override.
Unsupported instruction sets fail before hash-worker or arena startup; there
is no silent fallback. All benchmark kinds honor the selection.

`avx512-256` is an experimental 256-bit implementation using AVX-512 rotate
instructions. It requires AVX2, AVX-512F and AVX-512VL, including OS support.
Select it explicitly for benchmarks; automatic mining and autotuning never
choose it. Its report identity is distinct from the existing `avx512` path.

`avx512-256-stream` is a separate implementation with cached recent blocks and
non-temporal destination stores. It has the same ISA requirements and preserves
the full arena and PoW output. Explicit benchmarks may select it on any supported
host. Automatic mining trials initially include it only on AMD family 26/model 2,
where repeated EPYC measurements justify testing it; promotion still requires
two confirmed gains at the current worker count and page backing. Neither
streaming nor the experimental uniform 256-bit path replaces the fallback ISA.

```sh
./target/release/seine --bench --bench-kind backend --backend cpu \
  --threads 1 --disable-cpu-autotune-threads --cpu-page-mode regular \
  --bench-cpu-kernel avx2 --bench-secs 20 --bench-rounds 3 \
  --bench-warmup-rounds 1 --bench-output avx2.json
```

Schema-14 reports record:

- `config_fingerprint.cpu_kernel`: selected path, or null for a GPU-only run.
- `config_fingerprint.cpu_kernel_requested`: explicit override, or null for auto.
- `environment.cpu_kernels_supported`: paths supported by this host and build.
- `environment.cpu_identity`: x86 vendor and decoded family/model/stepping;
  these are null on other architectures. Existing CPU brand, architecture,
  target triple, compiler/build flags and fingerprints remain available.

The numeric identity is what the current OS/VM exposes. It is not an inferred
microarchitecture label or proof of physical-host capabilities. x86 instruction
checks also account for OS vector-state support. Family/model decoding follows
[Intel's CPUID specification](https://cdrdv2-public.intel.com/843860/architecture-instruction-set-extensions-programming-reference-dec-24.pdf).

`--bench-baseline` rejects different selected CPU kernels even with
`--bench-baseline-policy ignore-environment`; it also rejects CPU baselines
without kernel identity. Capture a new baseline for older CPU reports. Auto
and explicit selection of the *same* kernel remain comparable. Strict policy
also checks the raw CPU identity; ignore-environment permits intentional
cross-host/build comparisons while retaining kernel/config/PoW checks.

Use the paired A/B harness for a deliberate kernel comparison. For example,
with a single pinned binary on a host supporting both paths:

```sh
bash scripts/bench_cpu_ab.sh \
  --baseline-dir . --candidate-dir . \
  --baseline-binary "$PWD/target/release/seine" \
  --candidate-binary "$PWD/target/release/seine" \
  --bench-kind backend --threads 1 --page-mode regular \
  --pairs 4 --bench-secs 20 --bench-rounds 3 --bench-warmup-rounds 1 \
  --baseline-miner-arg --bench-cpu-kernel --baseline-miner-arg avx2 \
  --candidate-miner-arg --bench-cpu-kernel --candidate-miner-arg avx512 \
  --output-dir data/kernel-avx2-vs-avx512
python3 scripts/benchctl.py compare data/kernel-avx2-vs-avx512/results.tsv
```

Keep page backing, affinity, worker count and binary fixed. Do not call an
AVX2 override on Zen 5 evidence for Zen 3 or Intel. Add microarchitecture-specific
implementations or dispatch rules only after correctness and repeatable
same-host backend measurements support them. No new architecture-specific
preference or autotuning policy is introduced here.

The [2026-10-24 architecture matrix](../experiments/cpu-architecture-20261024/README.md)
records real AMD Zen 1–5 and five Intel server generations, including the
worker-count and page-backing limits of those measurements.
