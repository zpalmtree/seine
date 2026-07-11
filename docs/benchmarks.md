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
