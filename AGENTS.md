# seine — Architecture & Tuning Reference

This file preserves the full engineering reference for AI agents doing optimization work on seine.

## Agent Formatting Policy

- Use the repository-pinned Rust toolchain for formatting (`rust-toolchain.toml` currently pins `1.93.0`).
- Run formatting via the pinned toolchain explicitly: `cargo +1.93.0 fmt` (or `rustup run 1.93.0 rustfmt`).
- Do not run repository-wide formatting (`cargo fmt --all`) unless the user explicitly asks for it.
- By default, format only the files that were edited for the task.
- Do not include unrelated formatting-only changes in commits unless the user explicitly requests them.

## Status

- CPU backend: implemented (Argon2id, consensus-compatible params).
- CPU optimization journal: see `CPU_OPTIMIZATION_LOG.md` for measured tuning history and kept/reverted attempts.
- NVIDIA backend: native CUDA Argon2id lane-fill engine with runtime NVRTC compilation, persistent VRAM-sized lanes, full-warp cooperative compression, and multi-hash-per-launch batching.
- NVIDIA optimization journal: see `NVIDIA_OPTIMIZATION_LOG.md` for measured tuning history and kept/reverted attempts.
- Runtime architecture: supports multiple backends in one process with persistent workers, configurable bounded backend event queues with lossless `Solution` delivery and deduplicated backend `Error` events (prevents multi-thread error storms from stalling worker teardown), coalesced tip notifications (deduped across SSE reconnects), template prefetch overlap to reduce round-boundary idle, and optional strict quiesce barriers for round-accurate hash accounting.
  - Backend assignment/control dispatch now runs through one shared per-backend task executor with panic capture and timeout quarantine to avoid duplicated control paths and extra thread churn.
  - Per-backend executors prioritize control commands (cancel/fence/stop) on a dedicated control lane, so queued assignment bursts do not delay control enqueue.
  - Backend dispatch deadlines are now scoped per task dispatch (not one shared batch deadline) to avoid false timeout quarantine as backend counts grow.
  - Mining control flow no longer performs direct template/submit HTTP calls; dedicated template-prefetch and submit workers own network I/O so scheduler/control loops stay responsive under network jitter.
  - Submit backlog handling is non-blocking for the mining control loop; when the backlog hard cap is saturated, new submit attempts are deferred (not dropped), and retryable submit failures are re-queued through deferred-solution flow.
  - `PowBackend` now exposes optional non-blocking assign/cancel/fence hooks (`*_nonblocking`) for future persistent-kernel GPU backends that need queue-pressure signaling.
  - Runtime assigns disjoint nonce chunks per backend per round (backend-local scheduling inside each chunk) so CPU and future GPU implementations can iterate independently without nonce overlap.
  - Runtime supports batched per-backend work assignment via backend queue-depth hints (`max_inflight_assignments`) so future GPU backends can overlap control and kernel scheduling.
  - Backends explicitly advertise deadline semantics (`cooperative` vs `best-effort`) so timeout behavior is visible before mixing heterogeneous devices.
  - Mining mode supports adaptive weighted nonce allocation (`--work-allocation adaptive`) based on observed backend throughput, with `--work-allocation static` available for fixed lane-based splitting.
  - NVIDIA backend topology supports multiple explicit device instances via `--nvidia-devices` (for example `--backend nvidia --nvidia-devices 0,1`).
  - Runtime is split into `src/miner/{mining,template_prefetch,tip,bench,scheduler,stats,work_allocator,backend_control,round_control}.rs` to keep orchestration, scheduling, and backend control isolated for faster iteration.

## Runtime Tuning Knobs

- `--backend-event-capacity` (default `1024`) controls bounded backend event queue size.
- `--backend-assign-timeout-ms` (default `1000`) bounds per-backend assignment dispatch calls.
- `--backend-assign-timeout-strikes` (default `3`) sets consecutive assignment timeout strikes before backend quarantine.
- `--backend-control-timeout-ms` (default `60000`) bounds `cancel/fence` control calls; timed-out backends are quarantined.
- By default, backends reporting best-effort deadlines are quarantined; pass `--allow-best-effort-deadlines` to keep them active.
- Active backends run a startup deadline probe (`cancel`/`fence`) with a bounded watchdog timeout before mining/benchmark rounds begin; probe failures are quarantined.
- `--hash-poll-ms` (default `200`) controls backend hash counter polling cadence.
  - Runtime may tighten this cadence based on backend capability hints (for example non-CPU accelerators) while preserving the configured upper bound.
  - CPU backend does not force a lower poll hint, so CPU-only runs keep `--hash-poll-ms` as the effective poll cadence.

### CPU Backend Tuning

- `--cpu-profile` (`balanced`, `throughput`, `efficiency`; default `balanced`) applies preset CPU threading/poll/flush defaults.
  - Profile defaults apply when related knobs are omitted; explicit flags still override profile defaults.
- `--cpu-hash-batch-size` (default `64`) controls per-worker hash counter flush batch size.
- `--cpu-control-check-interval-hashes` (default `1`) controls per-worker control polling cadence.
  - CPU stop/deadline checks are additionally time-bounded to keep round-end late-hash skew stable under coarse hash-interval settings.
- `--cpu-hash-flush-ms` (default `50`) controls time-based hash counter flush cadence.
- `--cpu-event-dispatch-capacity` (default `256`) controls internal CPU backend event dispatch buffering.
- CPU thread autotuning is enabled by default when `--threads` is omitted.
  - Results are persisted to `<seine-data-dir>/seine.cpu-autotune.json` and reused on subsequent runs.
  - `--cpu-autotune-threads` forces autotuning even when `--threads` is set.
  - `--disable-cpu-autotune-threads` disables autotuning.
  - `--cpu-autotune-min-threads`, `--cpu-autotune-max-threads`, and `--cpu-autotune-secs` bound autotuner search range and base sample window (`--cpu-autotune-secs` default: `6`).
  - Autotune uses a binary-style peak search for larger thread ranges, then locally sweeps neighboring candidates for final selection.
  - Final thread selection is profile-aware: `throughput` picks peak H/s, while `balanced` and `efficiency` bias toward lower thread counts when they remain close to peak throughput (reducing RAM pressure).
  - Candidate sampling auto-extends up to an internal cap to collect a minimum hash count on very slow lanes (reduces variance vs fixed very short windows).
  - `--cpu-autotune-config` overrides the persisted autotune config path.

### NVIDIA Backend Tuning

- `--nvidia-autotune-secs` (default `5`) controls per-candidate benchmark window for regcap autotune.
- `--nvidia-autotune-samples` (default `2`) runs multiple samples per candidate; autotune prioritizes median deadline-window counted H/s (then mean counted H/s, then throughput tie-breaks).
- `--nvidia-autotune-config` overrides the persisted NVIDIA autotune cache path (`<seine-data-dir>/seine.nvidia-autotune.json` by default).
- `--nvidia-max-rregcount` forces a fixed register cap and skips autotune/cache lookup.
- `--nvidia-max-lanes` caps active NVIDIA lanes per device instance.
- `--nvidia-dispatch-iters-per-lane` and `--nvidia-allocation-iters-per-lane` override scheduler/allocator lane-iteration hints.
- `--nvidia-hashes-per-launch-per-lane` (default `2`) controls CUDA launch depth per lane (`higher => fewer launches`, often higher H/s on this workload, but coarser cancel/fence preemption).
  - On Blackwell, if the flag is left unset and a cached/default tuning record resolves to depth `2`, Seine clamps the runtime depth to `1` for finer preemption. Explicit CLI overrides keep the requested depth.
  - Blackwell fresh autotune also re-probes regcap+depth jointly and breaks near ties toward shallower full-lane profiles, with a soft preference for the measured `rreg=208` frontier.
- `--nvidia-no-adaptive-launch-depth` disables backend pressure/deadline-based launch-depth shaping.
- `--nvidia-fused-target-check` enables in-fill-kernel target checking (disabled by default; can regress throughput on some GPUs).
- `--nvidia-template-stop-policy` (`auto`, `on`, `off`) controls whether NVIDIA workers enforce template `stop_at`; `auto` follows `--strict-round-accounting`.

### Scheduler & Stats Tuning

- `--stats-secs` (default `10`) controls periodic stats log emission cadence.
- `--work-allocation` (`adaptive` or `static`) controls backend nonce-chunk splitting policy in mining mode.
  - Adaptive mode now also incorporates solved/stale rounds with reduced gain so weights stay fresh under frequent tip churn.
- `--sub-round-rebalance-ms` (default `0`, disabled) enables optional in-round redistribution cadence for adaptive mode when you want faster response to live throughput shifts.
- `--request-timeout-secs` (default `10`) controls JSON API request timeout for template/submit/wallet calls.
- `--events-stream-timeout-secs` (default `10`) controls SSE connect timeout per attempt (stream itself is long-lived).
- `--events-idle-timeout-secs` (default `90`) bounds one SSE stream request lifetime before reconnect to avoid liveness stalls.
- `--prefetch-wait-ms` (default `250`) bounds how long mining waits for prefetched templates before falling back to direct fetch.
- `--tip-listener-join-wait-ms` (default `250`) bounds shutdown wait for SSE listener thread before detaching.
- `--submit-join-wait-ms` (default `2000`) bounds shutdown wait for submit worker thread before detaching.

### Round Accounting & Nonce Management

- Relaxed accounting is now the default (higher throughput, less exact round attribution).
- `--strict-round-accounting` enables per-round quiesce barriers for exact accounting.
  - If any backend reports append assignment semantics, relaxed mode still issues round-boundary cancels to prevent stale queue carry-over.
- `--refresh-on-same-height` forces immediate refresh on same-height `new_block` hash changes.
- Nonce space is reserved deterministically per epoch via `--nonce-iters-per-lane` (default `2^36` iterations per lane), avoiding overlap between refresh rounds without relying on sampled hash counters.
  - Assignment retry windows now consume additional scheduler span so backend quarantine/retry paths also stay non-overlapping across future rounds.
  - The runtime logs backend preemption granularity on startup so strict round-accounting fence behavior is visible (`per-hash`, `every N hashes`, or `unknown`).

### Misc Runtime Notes

- Late-solution template retention is timeout-aware (derived from refresh/control/assign/prefetch timing) with time-based eviction and a bounded cache (entry and memory caps) to reduce stale drops during backend lag/spiky tip churn.
- Deferred solution submission deduplicates by `(epoch, nonce)` across backends and suppresses repeat submit attempts across later rounds.
- `--cpu-affinity` (`auto`, `pcore-only`, or `off`) controls CPU worker pinning policy.
  - Default is `pcore-only` on macOS Apple Silicon (`auto` on other platforms).
  - `pcore-only` pins CPU hashing workers to the perflevel0 logical CPU set (P-core logical IDs) and can help at higher lane counts on Apple Silicon.
- `--ui` (`auto`, `tui`, `plain`) controls rendering mode. `auto` enables TUI only when stdout/stderr are terminals.
- A backend runtime fault quarantines only that backend; mining continues on remaining active backends when possible.
- CPU backend runtime errors are latched per assignment so only the first fault event is emitted, avoiding queue saturation during shutdown.
- This miner is intentionally external so consensus-critical validation remains in the daemon.

## Performance Benchmarking

Run deterministic local benchmarking (no API connection needed):

- `--bench-kind kernel`: hash kernel only (single backend).
- `--bench-kind kernel-effective`: kernel path with wall-time accounting and target/eval enabled (single backend), useful for isolating backend orchestration overhead.
- `--bench-kind backend`: persistent backend workers (steady-state throughput).
- `--bench-kind end-to-end`: includes backend start/stop per round.
- Kernel benchmarks report both `elapsed` (steady benchmark window, from `--bench-secs`) and `wall` (full runtime including startup/teardown) per round.
- Worker benchmarks always apply a round-end measurement fence so round H/s is comparable across strict/relaxed accounting modes.
  - Worker benchmarks now honor `--work-allocation` (`adaptive`/`static`) so scheduler tuning can be measured without mining mode.
  - Benchmark reports now expose `counted_hashes`, `late_hashes`, and `late_hash_pct` per round plus aggregate late-hash accounting in the summary; throughput uses measured elapsed + fence time to avoid inflation when backend preemption is coarse.
  - `--bench-warmup-rounds` runs unreported warmup rounds before measured rounds to reduce startup/cache jitter in short benchmark runs.
  - Baseline compatibility now gates on measurement-critical runtime settings; context-only timeout knobs remain in the report but do not block comparisons.
  - Baseline comparison validates benchmark/config/environment compatibility (including report schema, runtime fingerprint, and PoW fingerprint) before computing deltas.
  - In benchmark mode, default `start_nonce` is deterministic (`0`) unless `--start-nonce` is explicitly provided.

```bash
cd seine
cargo run --release -- --bench --bench-kind backend --backend cpu --threads 1 --bench-secs 20 --bench-rounds 3 --bench-output bench.json
```

Compare against a previous baseline:

```bash
cargo run --release -- --bench --bench-kind backend --backend cpu --threads 1 --bench-secs 20 --bench-rounds 3 --bench-baseline bench.json
```

Allow cross-build baseline comparison while keeping runtime/config/PoW compatibility checks:

```bash
cargo run --release -- --bench --bench-kind backend --backend cpu --threads 1 --bench-secs 20 --bench-rounds 3 --bench-baseline bench.json --bench-baseline-policy ignore-environment
```

Fail CI on regression versus baseline (example: fail below `-5%`):

```bash
cargo run --release -- --bench --bench-kind backend --backend cpu --threads 1 --bench-secs 20 --bench-rounds 3 --bench-baseline bench.json --bench-fail-below-pct 5
```

### Host-native CPU build

Build with host ISA tuning (`target-cpu=native`) and the `release-native` profile:

```bash
./scripts/build_cpu_native.sh --cpu-only
```

### Thermal-stable CPU A/B harness

Run interleaved baseline/candidate CPU benchmarks with cooldown gaps to reduce thermal drift bias:

```bash
./scripts/bench_cpu_ab.sh \
  --baseline-dir ../seine-baseline \
  --candidate-dir . \
  --bench-kind backend \
  --pairs 4 \
  --bench-secs 20 \
  --bench-rounds 3 \
  --bench-warmup-rounds 1 \
  --threads 1 \
  --cooldown-secs 20 \
  --profile release
```

Output goes to `data/bench_cpu_ab_<kind>_<timestamp>/` and includes:
- `results.tsv` with per-run metrics.
- `summary.txt` with baseline/candidate means and percent delta.
- Optional: use `--baseline-profile` / `--candidate-profile` and `--baseline-native` / `--candidate-native` to A/B build profiles or ISA flags in one interleaved run.

### Thermal-stable NVIDIA A/B harness

Run interleaved baseline/candidate NVIDIA benchmarks with cooldown gaps to reduce thermal drift bias:

```bash
./scripts/bench_nvidia_ab.sh \
  --baseline-dir ../seine-baseline \
  --candidate-dir . \
  --bench-kind backend \
  --pairs 4 \
  --bench-secs 20 \
  --bench-rounds 3 \
  --bench-warmup-rounds 1 \
  --cooldown-secs 20 \
  --nvidia-devices 0 \
  --nvidia-max-rregcount 208 \
  --nvidia-hashes-per-launch-per-lane 2 \
  --profile release
```

Output goes to `data/bench_nvidia_ab_<kind>_<timestamp>/` and includes:
- `results.tsv` with per-run benchmark metrics plus GPU start/end snapshots (`temp/sm_clock/mem_clock/power/util`).
- `summary.txt` with baseline/candidate means, percent delta, and averaged late-hash percentage.
- Optional: use `--nvidia-fused-target-check`, `--nvidia-no-adaptive-launch-depth`, and trailing `-- <extra miner args>` for controlled A/B sweeps.
