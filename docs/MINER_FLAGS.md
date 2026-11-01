# Miner Flag Reference

This page documents every `seine` CLI flag and what it controls.

For the raw CLI output, run:

```bash
./seine --help
```

## Connectivity, Auth, and Paths

| Flag | Values / Default | What it does |
|---|---|---|
| `--mode` | `pool` or `daemon`, default `pool` | Selects mining mode. |
| `--pool-url` (`-o`, `--url`) | `host:port`, `stratum+tcp://host:port` or `stratum+ssl://host:port` | Stratum pool endpoint used in pool mode. `stratum+ssl://` (also `stratum+tls://`) connects over TLS. Repeat the flag or separate URLs with commas to add backup pools in failover order. See [pool connections](guides/configuration.md#pool-connections). |
| `--payout-memo` | UTF-8 text, unset by default | Exchange deposit memo, preserved exactly. Requires a full deposit address and a compatible pool; verified Longhorn TLS endpoints use their address-suffix format automatically. Not supported in daemon mode or farm-proxy hosting. |
| `--no-payout-memo` | flag | Ignores a saved payout memo for this run when switching back to a personal address. |
| `--pool-worker` (`--worker`) | string | Worker name sent during pool login. |
| `--pool-proxy` (`--socks5`) | `socks5://[user:pass@]host:port` | Sends pool connections, including backup pools and dev-fee rounds, through a SOCKS5 proxy. The proxy resolves pool hostnames. Pool stats requests use it too; update checks and optional telemetry connect directly. |
| `--proxy-listen` | `host:port` | Runs Seine as a farm proxy instead of mining. Rigs connect to this address and share one session with `--pool-url`; every share pays `--address`. See [farm proxy](guides/farm-proxy.md). |
| `--pass` (`-p`) | string | Accepted so launch templates that always pass a pool password keep working. Blocknet pools do not use passwords; the value is ignored. |
| `--api-url` | URL, default auto-detected then `http://127.0.0.1:8332` | Blocknet daemon API base URL. Seine prefers Blocknet wrapper config/pidfiles/processes before falling back to loopback. Optional in daemon mode for standard wrapper-managed installs; optional in pool mode for local daemon wallet balance. |
| `--token` | string | Explicit daemon API bearer token. Optional override in daemon mode; optional in pool mode for local daemon wallet balance. |
| `--wallet-password` | string | Wallet password for automatic wallet load when needed. |
| `--wallet-password-file` | path | File containing the wallet password. |
| `--address` (`-u`, `--user`) | string | `ADDRESS.WORKER` also sets the worker name when `--pool-worker` is not given. Full payout address or Blocknet ID shortname (`@name` or `$name`) for pool login or daemon templates. Shortnames are verified and resolved once per mining run; quote them in shell commands, e.g. `--address '$name'`. In daemon mode, if the resolved address matches the daemon wallet, TUI wallet stats remain available; otherwise pending/unlocked display as `---`. |
| `--cookie` | path | Explicit `api.cookie` path used to read daemon API token. Optional override in daemon mode; optional in pool mode for local daemon wallet balance. |
| `--daemon-dir` | path, default wrapper-managed `~/.config/bnt/data/mainnet` | Daemon data dir used to find `api.cookie` when `--cookie` is not set. Also used by pool mode when probing for a local daemon wallet. |
| `--data-dir` | path, default `./seine-data` | Seine data dir for persisted state (`seine.config.json`, autotune caches, etc). |

## Hardware Telemetry

| Flag | Values / Default | What it does |
|---|---|---|
| `--telemetry` | `on`, `off`; on when a collector is configured | Saves the hardware/performance reporting preference in `seine.telemetry.json`. Applies to interactive and headless mining. |
| `--telemetry-url` | HTTPS URL; no default unless supplied by the release builder | Sets the collector destination. A new destination preserves an explicit opt-out. HTTP is allowed only on loopback for local testing. |

See [collection fields and self-hosting](../telemetry/README.md).

## Backend Selection and Topology

| Flag | Values / Default | What it does |
|---|---|---|
| `--backend` | `cpu`, `nvidia`, `metal`, `amd`; repeatable / comma-separated; default auto | Selects mining backends. Auto mode selects CPU plus all detected NVIDIA GPUs and discrete AMD GPUs (one instance per device). `amd` needs a Linux release build or a `--features amd` source build; integrated AMD GPUs are used only when `amd` is listed. See [AMD GPUs](guides/amd.md). |
| `--nvidia-devices` | comma-separated GPU indices; default all detected devices | Creates one NVIDIA backend instance per listed device index; listing indices subsets/pins devices and disables auto-expansion. Requires NVIDIA backend selected. |
| `--amd-devices` | comma-separated GPU indices; default all detected wave32 devices | Creates one AMD backend instance per listed device index; listing indices subsets/pins devices and disables auto-expansion. Requires AMD backend selected. |
| `--threads` | integer `>=1`; alias: `--cpu-threads` | CPU threads per CPU backend instance. If omitted, auto-sized by CPU/RAM/profile. |
| `--cpu-affinity` | `off`, `auto`, `os`, `pcore-only`; default `pcore-only` on macOS, `auto` elsewhere | CPU worker affinity policy. Native Windows `auto` uses complete physical-core topology before SMT; Linux `auto` spreads physical cores across NUMA nodes and L3 groups within each node before SMT. Missing cache data retains NUMA-only placement; a single node/cache or incomplete NUMA/core topology retains OS order. `os` opts out of reordering while retaining pinning. macOS modes use best-effort Mach affinity tags/QoS, not hard CPU IDs. |
| `--cpu-page-mode` | `auto`, `regular`, `large`, `large-1g`; default `auto` | CPU arena page contract. `auto` prefers explicit large pages and falls back; `regular` forces ordinary pages and disables Linux THP; `large` requires every worker to use Windows large pages or Linux HugeTLB and fails closed; `large-1g` requires explicit 1 GiB HugeTLB pages (x86_64 Linux/WSL only, two 1 GiB pages per worker, reserve via `/sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages` or the `hugepagesz=1G` boot parameter) and fails closed. `large` and `large-1g` are unsupported on macOS. |
| `--cpu-profile` | `balanced`, `throughput`, `efficiency`; default `balanced` | Profile presets for CPU polling, flushing and batching. Both `balanced` and `throughput` target the highest measured hashrate; only opt-in `efficiency` may choose fewer workers below that peak. All profiles retain memory safeguards. |
| `--cpu-threads-per-instance` | comma-separated integers `>=1` | Per-CPU-backend thread counts; length must match CPU instances. |
| `--cpu-affinity-per-instance` | comma-separated `off|auto|os|pcore-only` | Per-CPU-backend affinity; length must match CPU instances. |
| `--cpu-memory-reserve-percent` | integer `0..99`; default `5` (at most 512 MiB on hosts with 16 GiB or more, else at most 1 GiB) | Subtract this percentage of effective total RAM from the automatic CPU sizing/autotune budget. `0` opts out. Explicit thread counts bypass the reserve unless autotuning is forced; fixed counts retain RAM checks; adaptive mining may pause at zero workers when RAM is insufficient. |
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
| `--disable-update-check` | bool flag (default `false`) | Disables the startup and 12-hourly GitHub check for newer seine releases. |
| `--self-update` | bool flag (default `false`) | Replaces this executable with the newest stable release for the platform (archive SHA-256 checked against the release's `SHA256SUMS.txt`), then exits without mining. Needs no other flags. |
| `--auto-update` | bool flag (default `true`) | When the update check finds a newer stable release, installs it the same way (release signature and checksum verified, and the new executable must report the release version before it replaces this one), stops mining cleanly and restarts with the same arguments. Applies to official release downloads only; self-compiled builds just log the release. Ignored with `--disable-update-check`. |
| `--no-auto-update` | bool flag (default `false`) | Keeps mining the current version; a newer release is only logged. |
| `--refresh-on-same-height` | bool flag (default `false`) | Refresh on same-height `new_block` hash change events. |
| `--ui` | `auto`, `tui`, `plain`; default `auto` | Output mode selection. |

## CPU Backend Tuning

| Flag | Values / Default | What it does |
|---|---|---|
| `--cpu-hash-batch-size` | integer `>=1` | CPU worker hash counter flush batch size. |
| `--cpu-control-check-interval-hashes` | integer `>=1` | CPU worker control poll cadence (in hashes). |
| `--cpu-hash-flush-ms` | integer `>=1` | CPU worker time-based hash flush interval. |
| `--cpu-event-dispatch-capacity` | integer `>=1` | CPU backend internal event dispatch queue size. |
| `--cpu-autotune-threads` | bool flag | Enables adaptive CPU tuning during mining, even when `--threads` is set; benchmark mode uses the offline kernel search. |
| `--disable-cpu-autotune-threads` | bool flag | Disables CPU thread autotune and runtime worker adjustment. |
| `--cpu-autotune-min-threads` | integer `>=1`, default `1` | Lower performance-search bound; memory pressure can temporarily reduce the count below it, including to zero. |
| `--cpu-autotune-max-threads` | integer `>=1` | Upper bound for CPU autotune thread search. |
| `--cpu-autotune-secs` | integer `>=1`, default `6` | Offline sample window. Mining uses `max(30, 5 × value)` seconds per productive A/B/A window, with at least 20 hashes; warming and interrupted windows are excluded. |
| `--cpu-autotune-config` | path | CPU autotune cache file (default under `--data-dir`); mining reuses compatible seeds across changes in available RAM and records subsequent progress. |

## NVIDIA Backend Tuning

| Flag | Values / Default | What it does |
|---|---|---|
| `--nvidia-autotune-secs` | integer `>=1`, default `5` | Base sample window per NVIDIA autotune candidate. |
| `--nvidia-autotune-samples` | integer `>=1`, default `2` | Sample count per NVIDIA autotune candidate (median-priority scoring). |
| `--nvidia-autotune-config` | path | NVIDIA autotune cache file (default under `--data-dir`). New winner records also retain the complete candidate/sample/timing trace for cross-hardware analysis; older winner-only records remain compatible. |
| `--nvidia-max-rregcount` | integer `>=1` | Forces register cap and skips NVIDIA autotune/cache lookup. |
| `--nvidia-max-lanes` | integer `>=1` | Caps active NVIDIA lanes per device instance. |
| `--nvidia-dispatch-iters-per-lane` | integer `>=1` | Overrides NVIDIA scheduler dispatch hint (iters/lane). |
| `--nvidia-allocation-iters-per-lane` | integer `>=1` | Overrides NVIDIA reservation/allocation hint (iters/lane). |
| `--nvidia-hashes-per-launch-per-lane` | integer `>=1`, default `2` | Work depth per CUDA launch per lane. On Blackwell, when this flag is left unset, Seine now treats depth `1` as the preferred runtime/fresh-autotune frontier: cached/default depth-`2` records are clamped to `1`, and fresh autotune jointly re-probes regcap+depth with Blackwell-only tie-breaks that favor shallower full-lane profiles. Explicit overrides keep the requested depth. |
| `--nvidia-no-adaptive-launch-depth` | bool flag | Disables adaptive launch depth shaping. |
| `--nvidia-fused-target-check` | bool flag | Enables fused in-kernel target checks. |
| `--nvidia-template-stop-policy` | `auto`, `on`, `off`; default `auto` | Controls backend enforcement of template `stop_at`. |

## AMD Backend Tuning

| Flag | Values / Default | What it does |
|---|---|---|
| `--amd-max-lanes` | integer `>=1` | Caps active AMD lanes per device instance. |

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
| `--api-cors` | string, default empty (no CORS headers) | CORS allow-origin value for control API responses. |
| `--api-token` | string, default generated | Fixed bearer token for the control API. When unset, Seine generates one per start. |
| `--api-cookie` | path, default `<data-dir>/control-api.cookie` | Where the generated control API token is written. |

## Benchmark Mode

| Flag | Values / Default | What it does |
|---|---|---|
| `--bench` | bool flag (default `false`) | Runs local benchmark instead of live mining. |
| `--bench-kind` | `kernel`, `kernel-effective`, `backend`, `end-to-end`; default `backend` | Selects benchmark scope. |
| `--bench-cpu-kernel` | `scalar`, `avx2`, `avx512`, `avx512-256`, `avx512-256-stream`, `neon`, `neon-sha3`; unset = automatic | Benchmark-only CPU kernel selection. Requires `--bench`, explicit `--threads`, CPU-only backends, and disabled CPU autotuning; unsupported host/build paths fail. |
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
- In pool mode, `address` and `pool_url` are required.
- In pool mode, daemon auth is optional; when available, Seine will keep pool balance stats in the TUI and also show the local daemon wallet balance.
- In daemon mode, Seine auto-detects daemon API auth for standard Blocknet wrapper-managed installs; `token`, `cookie`, and `daemon-dir` remain manual overrides for custom layouts.
- Per-instance lists must match expanded backend instance count (or CPU instance count for CPU-specific lists).
- Numeric knobs documented as `>=1` are rejected when passed as `0`.
