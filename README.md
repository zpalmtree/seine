<p align="center">
  <img src="docs/assets/brand/banner.jpg" alt="seine — the fastest Blocknet miner" width="900">
</p>

<p align="center">
  <b>The fastest miner for <a href="https://blocknetcrypto.com">Blocknet</a>.</b><br>
  Tuned kernels for every modern CPU and NVIDIA GPU, zero configuration. Download, run, mine.
</p>

<p align="center">
  <a href="../../releases/latest"><img alt="Latest release" src="https://img.shields.io/github/v/release/zpalmtree/seine?style=flat-square&color=1BA3A0&labelColor=063B44"></a>
  <a href="../../releases"><img alt="Downloads" src="https://img.shields.io/github/downloads/zpalmtree/seine/total?style=flat-square&color=1BA3A0&labelColor=063B44"></a>
  <img alt="Platforms" src="https://img.shields.io/badge/Windows%20·%20Linux%20·%20macOS-x86__64%20%2B%20ARM64-1BA3A0?style=flat-square&labelColor=063B44">
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/license-BSD--3-1BA3A0?style=flat-square&labelColor=063B44"></a>
</p>

<p align="center">
  <img src="docs/assets/screenshots/dashboard.png" alt="seine mining on bntpool from an Apple M4 Max at 30 H/s, 5 of 5 shares accepted" width="900">
</p>

## Why seine

- **Fast on whatever you own.** Hand-tuned Argon2id kernels for AVX2, AVX-512,
  Arm NEON/SHA3 and NVIDIA Blackwell. Seine benchmarks them *while it mines* and
  keeps the fastest for your machine.
- **Zero configuration.** Run `./seine`, paste your address, and you're mining in
  seconds. Threads, kernels, memory and CPU placement tune themselves.
- **CPU and GPU together.** Every core and every NVIDIA GPU mines at once.
- **Pool or solo.** Mines on [bntpool.com](https://bntpool.com) out of the box, works
  with any Stratum pool, or solo against your own Blocknet node.
- **Built for rigs and fleets.** NUMA-aware placement, huge pages, a local control
  API with live events, and Prometheus metrics.

[Windows setup guide](https://seineminer.com/blog/mine-blocknet-on-windows/) · [Connect to bntpool.com](https://bntpool.com/blog/mine-blocknet-on-bntpool/) · [Mining locally but pool hashrate is zero?](https://bntpool.com/blog/track-your-miner-on-bntpool/#zero-hashrate)

## Get started

**1. Download** the archive for your platform from the
[latest release](../../releases/latest):

| Platform | Archive | GPU |
|---|---|---|
| Windows x86_64 | `seine-windows-x86_64.zip` | NVIDIA (drivers only, CUDA runtime bundled) |
| Linux x86_64 | `seine-linux-x86_64.tar.gz` | NVIDIA (driver only), AMD Radeon RDNA (ROCm HIP runtime) |
| Linux ARM64 | `seine-linux-aarch64.tar.gz` | CPU only |
| macOS Apple Silicon | `seine-macos-aarch64.tar.gz` | CPU; Metal GPU for BLAKE2b blocks |
| macOS Intel | `seine-macos-x86_64.tar.gz` | CPU only |
| HiveOS rigs | `seine-hiveos-<version>.tar.gz` | NVIDIA (NVRTC bundled), AMD; see the [HiveOS guide](docs/guides/hiveos.md) |
| mmpOS rigs | `seine-mmpos-<version>.tar.gz` | NVIDIA (NVRTC bundled), AMD; see the [mmpOS guide](docs/guides/mmpos.md) |

The Linux builds run on glibc 2.31 or newer: Ubuntu 20.04, Debian 11 and later.

**2. Run it:**

```bash
./seine
```

**3. Enter your Blocknet address** (or `@shortname`) when prompted, and accept the
default pool. That's it: seine starts hashing straight away and keeps tuning
itself as it goes.

Each CPU thread needs about 2 GB of RAM. Seine sizes itself to your machine
automatically. More options: [setup and configuration](docs/guides/configuration.md).

## Performance

<p align="center">
  <img src="docs/assets/charts/best.svg" alt="Best measured hashrate by platform: AMD EPYC 9355P Zen 5 97.1 H/s, EPYC 9554P Zen 4 75.0, Arm server (AWS Graviton4) 39.3, Apple M4 Max 30.9, Intel Xeon 26.0" width="900">
</p>

**97.1 H/s on the AMD EPYC 9355P** with default settings, and **75.0 H/s on
the EPYC 9554P** with 64 mining threads and 1 GiB huge pages.
[Measurements and settings](docs/PERFORMANCE.md).

Every number is a measured result from repeated runs. Results for every CPU tested:
**[performance results](docs/PERFORMANCE.md)**.

## Get the most from your hardware

Seine is fast out of the box. These guides cover the last few percent:

| Hardware | Guide |
|---|---|
| Linux / WSL | [Huge pages](docs/guides/linux-hugepages.md): +4–8% on CPU |
| Windows | [Large pages and CPU placement](docs/guides/windows-large-pages.md) |
| EPYC, Xeon, many-core servers | [NUMA, L3 groups and memory headroom](docs/guides/large-servers.md) |
| Apple Silicon | [Threads, scheduling and Metal](docs/guides/apple-silicon.md) |
| NVIDIA GPUs | [Requirements, multi-GPU and tuning](docs/guides/nvidia.md) |

## Documentation

- [Setup and configuration](docs/guides/configuration.md): first run, flags, dashboard controls, shortnames, dev fee
- [How automatic tuning works](docs/guides/autotuning.md): thread counts, CPU kernels, memory recovery
- [Solo mining](docs/guides/solo-mining.md): mine against your own Blocknet node
- [HiveOS](docs/guides/hiveos.md): add seine to a flight sheet with one installation URL
- [mmpOS](docs/guides/mmpos.md): add seine to a miner profile with one download URL
- [Farm proxy](docs/guides/farm-proxy.md): many rigs, one pool connection
- [Control API](docs/API.md): REST, live events and metrics for dashboards ([OpenAPI](docs/openapi/seine-api-v1.yaml))
- [Flag reference](docs/MINER_FLAGS.md): every command-line option
- [Building from source](docs/BUILDING.md)
- [Changelog](CHANGELOG.md)
- For contributors: [architecture and agent notes](AGENTS.md)

## License

[Privacy policy](PRIVACY.md).

[BSD 3-Clause](LICENSE).
