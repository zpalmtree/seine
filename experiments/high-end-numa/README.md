# Native NUMA allocation experiment

Start with [HANDOFF.md](HANDOFF.md). This branch has **no measured native NUMA
speedup** and does not enable a production allocation policy.

## Build and correctness

Use isolated targets; never share a Cargo target directory between candidate
and control sources. Run `gpuq gaming check --quiet` before local builds/tests.

```bash
CARGO_TARGET_DIR=data/target-numa CARGO_BUILD_JOBS=4 \
  cargo +1.93.0 test --no-default-features --features numa-experiment \
  --bin seine numa_experiment

CARGO_TARGET_DIR=data/target-numa-release CARGO_BUILD_JOBS=4 \
  cargo +1.93.0 build --release --no-default-features --features numa-experiment

CARGO_TARGET_DIR=data/target-numa-release CARGO_BUILD_JOBS=4 \
  cargo +1.93.0 test --release --no-default-features --features numa-experiment \
  --bin seine --no-run --message-format=json > data/numa-test-build.jsonl

python3 -m unittest discover -s experiments/high-end-numa -p 'test_*.py'
python3 experiments/high-end-numa/probe.py > data/numa-inventory.json
```

The Cargo JSON `compiler-artifact` entry with `profile.test=true` identifies the
optimized test executable. Freeze that executable and the miner together with
their SHA256s, source commit, feature set, rustc version and build environment.
Clear inherited `SEINE_GIT_COMMIT` labels when building; compiled-source and
binary hashes establish identity, not a manually overridable version label.

Run native restoration and full-size reference checks in an isolated process:

```bash
/absolute/path/to/optimized-test-binary \
  backend::cpu::numa_experiment::tests:: \
  --include-ignored --nocapture --test-threads=1
```

The parity test uses one 2 GiB arena at a time after generating three RustCrypto
references. It checks audit/local allocation, every supported kernel, and dirty
reuse. WSL results can establish syscall/correctness behavior only, not physical
NUMA placement or a hashrate improvement. Full-size checks are ignored during
ordinary unit testing because of their memory/time requirements.

## Native performance matrix

Use a dedicated, otherwise idle Linux host, cgroup v2, at least two allowed
memory nodes, and two exposed sockets. `--guest-numa` permits a two-node guest
without claiming multisocket evidence. Choose an absolute deadline fitting the
rental; the example date below is illustrative and must be replaced.

```bash
python3 experiments/high-end-numa/run.py \
  --binary /opt/numa/bin/seine \
  --parity-binary /opt/numa/bin/seine-tests \
  --output /opt/numa/results/balanced-auto \
  --workers 1 8 16 24 32 --max-arena-gib 64 \
  --policies audit local --page-mode auto --kernel avx512 \
  --pairs 4 --seconds 30 --rounds 2 \
  --deadline-utc 2026-10-25T04:00:00Z --plan
```

`--plan` inventories, hashes and writes a schedule without running binaries.
Inspect the estimated upper runtime. For execution remove `--plan` and use a new
output directory; existing directories are never overwritten. The runner
performs parity before timing and checks current RAM/cgroup headroom before each
invocation. It records every command and result. It never connects to a pool.

For one-socket arms add `--socket <exposed-id>`; worker counts must fit the allowed
CPUs. Run ordinary and large-page arms separately. Keep kernel identity fixed
within allocation comparisons; test the EPYC session's winning kernel separately
after its source is frozen. Use the same binary for audit/local/remote arms.

`SEINE_EXPERIMENT_ARENA_POLICY` exists only when `numa-experiment` is compiled:

| Value | Behavior |
|---|---|
| unset | Ordinary allocator without observation |
| `audit` | Observe current placement; leave inherited memory policy intact |
| `local` | Require default inherited policy and successful single-CPU pinning; temporarily bind allocation to that CPU's allowed node |
| `remote` | Same prerequisites; bind to first different allowed node for a diagnostic control |

The policy is restored on allocation success, error and unwinding. Every worker
reallocation receives a fresh guard. Existing nondefault policies are rejected
by local/remote controls rather than silently replaced. Sampling is one resident
address per 2 MiB; it is not an exhaustive audit of every 4 KiB page. Missing
pages, denied placement queries or unconfirmed locality invalidate a run.

The runner does not set up or restore HugeTLB pools: record those operations
separately on the disposable host and guarantee cleanup independently. Do not
reuse the older experiment's hard-coded two-node pool script on an unfamiliar
machine. A general safe pool-mutation controller remains future work.

## Acceptance

- Full-size parity and dirty reuse must pass on the target host.
- Every pair must have reversed order at each worker count, verified from actual
  completed records. Compare completed hashes over elapsed time including fences.
- Explain differences in actual page backing/locality; do not call a combined
  backing/distribution improvement a pure remote-latency result.
- Reject regressions on balanced pools, single-node machines, inherited policies,
  constrained memory and worker restart/reallocation before enabling any default.
- No success claim from simulation, synthetic topology, a short screen, or a
  failed/partial matrix. Keep negative and failed runs in the handoff.
