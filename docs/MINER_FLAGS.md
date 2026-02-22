# Miner Flag Reference

This page documents every `seine` CLI flag and what it controls.

For the raw CLI output, run:

```bash
./seine --help
```

## Connectivity, Auth, and Paths

| Flag | Values / Default | What it does |
|---|---|---|
| `--api-url` | URL, default auto-detected then `http://127.0.0.1:8332` | Blocknet daemon API base URL. |
| `--token` | string | Explicit daemon API bearer token. |
| `--wallet-password` | string | Wallet password for automatic wallet load when needed. |
| `--wallet-password-file` | path | File containing the wallet password. |
| `--address` | string | Override payout address for block template requests. If it matches daemon wallet address, TUI wallet stats remain available; otherwise pending/unlocked display as `---`. |
| `--cookie` | path | Explicit `api.cookie` path. Used to read daemon API token. |
| `--daemon-dir` | path, default `./blocknet-data-mainnet` | Daemon data dir used to find `api.cookie` when `--cookie` is not set. |
| `--data-dir` | path, default `./seine-data` | Seine data dir for persisted state (autotune caches, etc). |

## Backend Selection and Topology

| Flag | Values / Default | What it does |
|---|---|---|
| `--backend` | `cpu`, `nvidia`, `metal`; repeatable / comma-separated; default auto | Selects mining backends. Auto mode selects CPU and NVIDIA (when available). |
| `--nvidia-devices` | comma-separated GPU indices | Creates one NVIDIA backend instance per listed device index. Requires NVIDIA backend selected. |
| `--threads` | integer `>=1`; alias: `--cpu-threads` | CPU threads per CPU backend instance. If omitted, auto-sized by CPU/RAM/profile. |
| `--cpu-affinity` | `off`, `auto`, `pcore-only`; default platform-dependent | CPU worker pinning policy. |
| `--cpu-profile` | `balanced`, `throughput`, `efficiency`; default `balanced` | Profile presets for CPU defaults (threads/poll/flush/event batching). |
| `--cpu-threads-per-instance` | comma-separated integers `>=1` | Per-CPU-backend thread counts; length must match CPU instances. |
| `--cpu-affinity-per-instance` | comma-separated `off|auto|pcore-only` | Per-CPU-backend affinity; length must match CPU instances. |
| `--allow-oversubscribe` | bool flag (default `false`) | Allows start even when configured CPU lanes exceed detected RAM budget. |

## Runtime Loop, Scheduling, and Nonce Space

| Flag | Values / Default | What it does |
|---|---|---|
| `--refresh-secs` | integer `>=1`, default `20` | Max time spent on one template before refresh. |
| `--request-timeout-secs` | integer `>=1`, default `10` | Timeout for template/submit/wallet JSON API calls. |
| `--events-stream-timeout-secs` | integer `>=1`, default `10` | Timeout for each SSE connection attempt to daemon `/api/events`. |
| `--events-idle-timeout-secs` | integer `>=1`, default `90` | Max lifetime of one daemon SSE request before reconnect. |
| `--stats-secs` | integer `>=1`, default `10` | Periodic stats print interval. |
| `--backend-event-capacity` | integer `>=1`, default `1024` | Capacity of runtime bounded backend event queue. |
| `--hash-poll-ms` | integer `>=1` | Backend hash polling interval. If omitted, profile default is used. |
| `--start-nonce` | `u64` | Sets nonce seed (default random; benchmark mode defaults to `0`). |
| `--nonce-iters-per-lane` | integer `>=1`, default `68719476736` (`2^36`) | Iteration span per lane before rotating reservation window. |
| `--work-allocation` | `adaptive` or `static`, default `adaptive` | Nonce chunk split policy across active backends. |
| `--sub-round-rebalance-ms` | integer `>=0`, default `0` | Optional in-round adaptive rebalance interval (`0` disables). |
| `--disable-sse` | bool flag (default `false`) | Disables daemon tip SSE; miner falls back to refresh timer only. |
| `--refresh-on-same-height` | bool flag (default `false`) | Refresh on same-height `new_block` hash change events. |
| `--ui` | `auto`, `tui`, `plain`; default `auto` | Output mode selection. |

## CPU Backend Tuning

| Flag | Values / Default | What it does |
|---|---|---|
| `--cpu-hash-batch-size` | integer `>=1` | CPU worker hash counter flush batch size. |
| `--cpu-control-check-interval-hashes` | integer `>=1` | CPU worker control poll cadence (in hashes). |
| `--cpu-hash-flush-ms` | integer `>=1` | CPU worker time-based hash flush interval. |
| `--cpu-event-dispatch-capacity` | integer `>=1` | CPU backend internal event dispatch queue size. |
| `--cpu-autotune-threads` | bool flag | Forces CPU autotune thread search at startup. |
| `--disable-cpu-autotune-threads` | bool flag | Disables CPU thread autotune. |
| `--cpu-autotune-min-threads` | integer `>=1`, default `1` | Lower bound for CPU autotune thread search. |
| `--cpu-autotune-max-threads` | integer `>=1` | Upper bound for CPU autotune thread search. |
| `--cpu-autotune-secs` | integer `>=1`, default `6` | Base sample window per CPU autotune candidate. |
| `--cpu-autotune-config` | path | CPU autotune cache file (default under `--data-dir`). |

## NVIDIA Backend Tuning

| Flag | Values / Default | What it does |
|---|---|---|
| `--nvidia-autotune-secs` | integer `>=1`, default `5` | Base sample window per NVIDIA autotune candidate. |
| `--nvidia-autotune-samples` | integer `>=1`, default `2` | Sample count per NVIDIA autotune candidate (median-priority scoring). |
| `--nvidia-autotune-config` | path | NVIDIA autotune cache file (default under `--data-dir`). |
| `--nvidia-max-rregcount` | integer `>=1` | Forces register cap and skips NVIDIA autotune/cache lookup. |
| `--nvidia-max-lanes` | integer `>=1` | Caps active NVIDIA lanes per device instance. |
| `--nvidia-dispatch-iters-per-lane` | integer `>=1` | Overrides NVIDIA scheduler dispatch hint (iters/lane). |
| `--nvidia-allocation-iters-per-lane` | integer `>=1` | Overrides NVIDIA reservation/allocation hint (iters/lane). |
| `--nvidia-hashes-per-launch-per-lane` | integer `>=1`, default `2` | Work depth per CUDA launch per lane. |
| `--nvidia-no-adaptive-launch-depth` | bool flag | Disables adaptive launch depth shaping. |
| `--nvidia-fused-target-check` | bool flag | Enables fused in-kernel target checks. |
| `--nvidia-template-stop-policy` | `auto`, `on`, `off`; default `auto` | Controls backend enforcement of template `stop_at`. |

## Metal Backend Tuning

| Flag | Values / Default | What it does |
|---|---|---|
| `--metal-max-lanes` | integer `>=1` | Caps active Metal lanes. |
| `--metal-hashes-per-launch-per-lane` | integer `>=1`, default `2` | Work depth per Metal dispatch per lane. |

## Backend Deadlines, Quarantine, and Shutdown

| Flag | Values / Default | What it does |
|---|---|---|
| `--backend-assign-timeout-ms` | integer `>=1`, default `1000` | Timeout for a backend assignment dispatch call. |
| `--backend-assign-timeout-ms-per-instance` | comma-separated integers `>=1` | Per-backend-instance assignment timeout override. |
| `--backend-assign-timeout-strikes` | integer `>=1`, default `3` | Consecutive assign timeout strikes before quarantine. |
| `--backend-assign-timeout-strikes-per-instance` | comma-separated integers `>=1` | Per-backend-instance assign strike threshold override. |
| `--backend-control-timeout-ms` | integer `>=1`, default `60000` | Timeout for backend cancel/fence control calls. |
| `--backend-control-timeout-ms-per-instance` | comma-separated integers `>=1` | Per-backend-instance control timeout override. |
| `--allow-best-effort-deadlines` | bool flag (default `false`) | Keeps backends active even if they report best-effort deadlines. |
| `--prefetch-wait-ms` | integer `>=1`, default `250` | Wait budget for template prefetch before direct fetch fallback. |
| `--tip-listener-join-wait-ms` | integer `>=1`, default `250` | Shutdown wait budget for tip listener thread before detaching. |
| `--submit-join-wait-ms` | integer `>=1`, default `2000` | Shutdown wait budget for submit worker thread before detaching. |
| `--strict-round-accounting` | bool flag (default relaxed) | Enables round-end quiesce barriers for exact round attribution. |

## Control API Server

| Flag | Values / Default | What it does |
|---|---|---|
| `--api-server` | bool flag (default `false`) | Starts local control API alongside immediate mining startup. |
| `--service` | bool flag (default `false`) | Starts control API in idle mode (waits for `/v1/miner/start`). |
| `--api-bind` | `host:port`, default `127.0.0.1:9977` | Control API bind address. |
| `--api-allow-unsafe-bind` | bool flag (default `false`) | Allows non-loopback bind addresses. |
| `--api-cors` | string, default `*` | CORS allow-origin value for control API responses. |

## Benchmark Mode

| Flag | Values / Default | What it does |
|---|---|---|
| `--bench` | bool flag (default `false`) | Runs local benchmark instead of live mining. |
| `--bench-kind` | `kernel`, `kernel-effective`, `backend`, `end-to-end`; default `backend` | Selects benchmark scope. |
| `--bench-secs` | integer `>=1`, default `20` | Per-round measured benchmark duration. |
| `--bench-rounds` | integer `>=1`, default `3` | Number of measured rounds. |
| `--bench-warmup-rounds` | integer `>=0`, default `0` | Warmup rounds before measured rounds. |
| `--bench-output` | path | Writes benchmark JSON report. |
| `--bench-baseline` | path | Baseline benchmark JSON for comparison. |
| `--bench-fail-below-pct` | float `>=0` | Fails run when regression exceeds threshold (requires `--bench-baseline`). |
| `--bench-baseline-policy` | `strict`, `ignore-environment`; default `strict` | Baseline compatibility policy. |

## Built-in CLI Flags

| Flag | What it does |
|---|---|
| `-h`, `--help` | Prints help output. |
| `-V`, `--version` | Prints version. |

## Hidden/Compatibility Flag

| Flag | What it does |
|---|---|
| `--relaxed-accounting` | Deprecated hidden compatibility alias. Relaxed accounting is already the default. |

## Important Validation Rules

- `--service` cannot be combined with `--bench`.
- `--cpu-autotune-threads` and `--disable-cpu-autotune-threads` are mutually exclusive.
- In API start/patch payloads, `token` and `cookie_path` are mutually exclusive.
- Per-instance lists must match expanded backend instance count (or CPU instance count for CPU-specific lists).
- Numeric knobs documented as `>=1` are rejected when passed as `0`.
