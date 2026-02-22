# seine

External miner for Blocknet with pluggable CPU and NVIDIA GPU backends.

![seine TUI](screenshot.png)

## Quick Start

### 1. Sync the Blocknet daemon

If this is your first time running the daemon, start it without any flags to create a wallet and sync the blockchain:

```bash
./blocknet
```

You will be prompted to set a wallet password. Once the daemon starts, wait for it to fully sync — you'll see `[sync] progress` messages reach 100%:

![Blocknet initial sync](screenshot-sync.png)

When sync is complete, type `exit` to shut down the daemon.

### 2. Restart the daemon with the API enabled

```bash
./blocknet --daemon --api 127.0.0.1:8332
```

### 3. Run the miner

**Option A — Pre-built binary** (from [Releases](../../releases)):

```bash
# Download the binary for your platform, then:
./seine
```

**Option B — Build from source:**

```bash
cargo build --release
./target/release/seine
```

### Zero-argument mode (recommended)

Running `./seine` with no flags is the default path.
Prerequisite: the Blocknet daemon is running with `--api` and has a readable `api.cookie`.

What it auto-detects:
- daemon API URL from running daemon args (`--api`) when available, else `http://127.0.0.1:8332`
- daemon auth token from `api.cookie` (using detected daemon data dir, then `--daemon-dir`)
- mining backends (CPU + NVIDIA when available)
- CPU thread count from available cores and RAM

If your daemon is not running yet, or you want to override detection, set these explicitly:

```bash
./seine --api-url http://127.0.0.1:8332 --cookie /path/to/data/api.cookie
```

Full CLI reference: [`docs/MINER_FLAGS.md`](docs/MINER_FLAGS.md)

## Requirements

| Platform | CPU | GPU (NVIDIA) |
|----------|-----|--------------|
| Linux x86_64 | works out of the box | CUDA driver + NVRTC libs |
| macOS x86_64 | works out of the box | — |
| macOS ARM | works out of the box (Metal experimental) | — |
| Windows x86_64 | works out of the box | CUDA driver + NVRTC libs |

Each CPU thread needs ~2 GB RAM (Argon2id parameters). Seine auto-sizes thread count from available cores and memory.

## Linux HugePages (CPU Throughput)

For best CPU mining performance on Linux, reserve explicit HugeTLB pages so each worker can map its Argon2 arena with `MAP_HUGETLB` (the backend falls back to THP if unavailable, which is often slower/inconsistent under fragmentation).

- Sizing rule (2 MB hugepages): `nr_hugepages ~= threads * 1024`
- Each CPU worker needs about `2 GiB` of hugepages (`1024 * 2 MB`)
- Add small headroom if possible (for example `+5%`)

Example for `--threads 4`:

```bash
# 4 workers * 1024 pages/worker = 4096 hugepages (~8 GiB)
sudo sysctl -w vm.nr_hugepages=4096
```

If the kernel cannot allocate enough pages (fragmented memory), compact and retry:

```bash
echo 3 | sudo tee /proc/sys/vm/drop_caches
echo 1 | sudo tee /proc/sys/vm/compact_memory
sudo sysctl -w vm.nr_hugepages=4096
```

Verify reservation:

```bash
grep -E 'HugePages_Total|HugePages_Free|Hugepagesize' /proc/meminfo
```

Persist across reboots:

```bash
echo 'vm.nr_hugepages=4096' | sudo tee /etc/sysctl.d/99-seine-hugepages.conf
sudo sysctl --system
```

Runtime checks:
- Startup warns with exact sizing/commands when HugeTLB is under-provisioned (`hugepages | CPU lanes=... need ...`).
- Per-backend fallback warnings still appear if a worker falls back from `MAP_HUGETLB` (`MAP_HUGETLB unavailable; hugepage coverage...`).

## Configuration

All miner flags are documented in [`docs/MINER_FLAGS.md`](docs/MINER_FLAGS.md).

```bash
# Set CPU thread count explicitly
./seine --threads 4

# Force a specific backend (auto-detects by default)
./seine --backend cpu
./seine --backend nvidia
./seine --backend cpu,nvidia

# Wallet password (if wallet is encrypted)
./seine --wallet-password-file /path/to/wallet.pass

# Mine to a specific payout address (instead of daemon wallet default)
./seine --address PpkFxY...

# Plain log output instead of TUI
./seine --ui plain
```

Note: when `--address` matches the daemon wallet address, Seine keeps wallet pending/unlocked stats in the TUI. If it differs, TUI balance fields show `---` for the override address.

## Control API

Seine now supports a local control API for alternative frontends.
Full API docs: [`docs/API.md`](docs/API.md)

### Service mode (idle until API start)

```bash
./seine --service --api-bind 127.0.0.1:9977 --token <daemon-token>
```

Then start mining via API:

```bash
curl -s -X POST http://127.0.0.1:9977/v1/miner/start \
  -H 'content-type: application/json' \
  -d '{}'
```

### Embedded mode (mine immediately + expose API)

```bash
./seine --api-server --api-bind 127.0.0.1:9977
```

`/v1/miner/start` accepts `token` or `cookie_path` when you want to inject daemon auth at runtime.

Key endpoints:
- `GET /v1/runtime/state`
- `POST /v1/miner/start`
- `POST /v1/miner/stop`
- `GET /v1/events/stream` (SSE)
- `GET /metrics`

Control API endpoints are open by default; no API key is required.

Password sources (checked in order): `--wallet-password`, `--wallet-password-file`, `SEINE_WALLET_PASSWORD` env var, interactive prompt.

## GPU Mining

### NVIDIA

Requires CUDA driver and NVRTC libraries on the host. Seine compiles kernels at startup via NVRTC.

```bash
# Auto-detect all GPUs
./seine --backend nvidia

# Select specific devices
./seine --backend nvidia --nvidia-devices 0,1
```

If CUDA initialization fails, NVIDIA backends are quarantined and CPU mining continues.

### Metal (macOS ARM)

Metal support is experimental. Pre-built macOS ARM binaries include it. To build from source:

```bash
cargo build --release --no-default-features --features metal
```

## Building from Source

Requires [Rust](https://rustup.rs/) and GNU Make.

Using Make:

```bash
# Default build (includes NVIDIA support)
make

# CPU-only build (no CUDA dependency)
make build-cpu

# Host-native CPU build (optimized for your specific CPU)
make build-native

# Run tests
make test

# Package a release zip for current platform
make release

# Bump Cargo version + create matching git tag
make tag-release TAG=v0.1.10
```

Or directly with Cargo:

```bash
# Default build (includes NVIDIA support)
cargo build --release

# CPU-only build (no CUDA dependency)
cargo build --release --no-default-features

# Host-native CPU build (optimized for your specific CPU)
./scripts/build_cpu_native.sh --cpu-only
```

## Benchmarking

Run offline benchmarks without a daemon connection:

```bash
./seine --bench --bench-kind backend --backend cpu --threads 1 --bench-secs 20 --bench-rounds 3
```

## Further Reading

- [AGENTS.md](AGENTS.md) — Architecture details, tuning knobs, benchmarking harness
- [docs/MINER_FLAGS.md](docs/MINER_FLAGS.md) — Complete CLI flag reference
- [docs/API.md](docs/API.md) — Control API guide (REST, SSE, metrics)
- [docs/openapi/seine-api-v1.yaml](docs/openapi/seine-api-v1.yaml) — OpenAPI specification
- [CPU_OPTIMIZATION_LOG.md](CPU_OPTIMIZATION_LOG.md) — CPU backend tuning history
- [NVIDIA_OPTIMIZATION_LOG.md](NVIDIA_OPTIMIZATION_LOG.md) — NVIDIA backend tuning history
