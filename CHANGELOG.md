# Changelog

User-focused release notes. Each `## vX.Y.Z` section below becomes the body of
the corresponding GitHub release (the release workflow extracts it by tag and
appends the auto-generated compare link). Write for miners deciding whether to
upgrade: lead with what they gain, keep it brief, skip internal refactors.
`scripts/release_tag.sh` refuses to tag a version that has no section here.

## v0.2.22

**Faster CPU mining on validated AMD EPYC Turin processors.** Automatic tuning
can now compare a streaming-store kernel with the existing CPU kernels while
mining, selecting it only after two confirmed gains. On a tested EPYC 9R45 at
24 workers with 48 GiB of explicit 2 MiB pages, it delivered 44.7 H/s versus
34.5 H/s for AVX2. Results depend on the processor, worker count and memory
backing; automatic eligibility currently covers the validated Turin CPU identity.
No configuration is required.

**More reliable kernel comparisons.** Tuning retains the best confirmed kernel
while checking other candidates and retries inconclusive comparisons without
caching a partial sweep. Cached choices include the eligible kernel set.

**Windows tuning follows CPU affinity changes.** Cached worker counts and kernel
choices are refreshed when the permitted CPU mask or physical-core grouping
changes, including equal-sized masks that select a different CCD or SMT layout.

**Native Linux ARM downloads.** Releases now include a CPU-only Linux ARM64
archive, built and checked on an ARM runner with full-size reference parity.

## v0.2.21

**Start mining sooner and adapt as conditions change.** Automatic CPU tuning
starts from a compatible cached worker count or an estimate capped by available
RAM, then compares settings while mining. Temporary CPU load no longer determines
the worker count for the whole session. Tuning does not reconnect to the pool or
restart GPU backends.

**Recover from changing RAM availability.** Automatic CPU workers release memory
when headroom falls and return after conditions stabilize, including pausing all
CPU workers if no complete arena fits. Temporary RAM limits preserve the remembered
higher worker count for later use. Allocation failures back off before retrying.

**Automatic CPU kernel selection.** Mining can compare AVX2 and AVX-512 while
continuing useful work, alongside adaptive worker-count tuning. It changes paths
only after two confirmed gains, remembers choices for the measured worker count
and memory backing, and keeps memory-pressure recovery active during comparisons.
No configuration is required; the existing CPU autotuning opt-out disables it.
Experimental kernels remain outside automatic selection.

**Steadier efficiency mode.** Once a lower worker count meets the profile's
hashrate target, it stays selected between probes instead of repeatedly climbing
to known higher counts. New CPU capacity, RAM availability, and kernel choices
can still trigger useful exploration. Explicit worker counts remain fixed unless
autotuning is requested.

## v0.2.20

**More reliable CPU thread tuning.** Fresh autotuning now warms each worker's
memory before measuring hashing speed and excludes setup and teardown from its
score. This reduces bias against larger worker counts. Failed or panicking
sample workers release their peers instead of leaving tuning stuck. Fresh
tuning can take longer because of the warmup; cached choices are refreshed.

**Compare CPU kernels on your own hardware.** Offline benchmarks can explicitly
select `scalar`, `avx2`, `avx512`, `avx512-256`, or `neon` with
`--bench-cpu-kernel`, subject to CPU support. This requires CPU-only benchmarks,
fixed threads and disabled autotuning. The experimental `avx512-256` path uses
256-bit AVX-512 operations and is never chosen automatically for mining.

**More trustworthy benchmark comparisons.** Reports now identify the selected
kernel and CPU vendor/family/model/stepping. Baseline comparisons reject
mismatched or missing CPU kernel identities, including with
`--bench-baseline-policy ignore-environment`; capture a new baseline for older
CPU reports. The paired A/B harness supports deliberate cross-kernel tests.

The release includes reproducible results across AMD Zen 1–5 EPYC and five
Intel server generations. Kernel winners vary with worker count, page backing
and placement, so the automatic mining ISA preference remains unchanged.

## v0.2.19

**Adjust CPU threads from the dashboard.** Press `+` or `-` beside the thread
count to add or remove one thread per CPU backend; `=` also increases the count.
Workers restart and pool mining reconnects briefly, while session totals,
uptime, average hashrate, logs and block markers are preserved. The current
hashrate refreshes for the new workers. Manual changes disable CPU autotuning
for the current run and are not saved as a new default.

**Keep the essentials visible in small terminals.** A compact dashboard keeps
the thread count, adjustment keys, mining state and hashrate visible when the
full layout no longer fits.

**Read startup errors before the Windows window closes.** When double-clicking
`seine.exe` opens its own console and startup fails, the error stays visible
until you press Enter. Existing terminal and scripted launches keep their usual
exit behavior.

## v0.2.18

**Faster CPU mining on AVX2 processors.** The Argon2 kernel does more work in
parallel and uses fewer column loads and stores. Gains depend on the CPU,
worker count and memory backing. Existing AVX-512 and ARM kernels keep their
current instruction paths.

**Smarter CPU placement on Linux.** Automatic worker placement spreads work
across NUMA nodes and L3 cache groups, using physical cores before SMT siblings.
This helps machines where OS-order placement crowds workers into only part of
the available memory and cache resources. Cached tuning choices are refreshed
for the new placement policy. Use `--cpu-affinity os` to restore OS-order pinning.

**More RAM headroom with automatic CPU thread counts.** Sizing and autotuning
now reserve 5% of effective RAM from their budget on all platforms. Use
`--cpu-memory-reserve-percent 0` to remove that reserve. Explicit thread counts
retain their existing behavior unless thread autotuning is forced.

## v0.2.17

**Less pool polling and better recovery when the pool is busy.** Seine reuses
network hashrate readings for 30 seconds and staggers telemetry refreshes to
reduce request bursts from rigs sharing an internet connection. It respects
the pool's retry delay for both rate limits and temporary overload, and avoids
requesting full miner details when a balance request fails temporarily.

Recommended for pool miners, especially operators running several rigs behind
one public IP. Mining and share submission are unchanged.

## v0.2.16

**Mine with your Blocknet ID shortname.** Use `--address '@name'` or
`--address '$name'` in pool or solo mode, or enter a shortname during first-run
setup. The control API accepts shortnames too. Seine verifies the signed
mapping, shows the full payout address, and keeps it fixed for the mining run,
including reconnects. Quote shortnames in shell commands so `$` is passed
literally. Full wallet addresses continue to work without a lookup.

**Multi-GPU rigs now mine on every GPU out of the box.** Seine detects all
NVIDIA GPUs at startup and runs one backend instance per device — no more
`--nvidia-devices 0,1,...` needed (that flag still works to pin or subset
devices, e.g. `--nvidia-devices 0` for single-GPU behavior). AMD builds get
the same treatment: a bare `--backend amd` uses every supported (wave32) AMD
GPU. Benchmarks (`--bench`) exercise the same multi-GPU topology.

**GPU tuning results are safe under parallel startup.** With several GPUs
initializing at once, the NVIDIA autotune cache is now written under a file
lock with atomic replacement, so one device's tuning results can no longer
overwrite another's (which previously forced a wasted re-tune on the next
start).

**The control API now lists your GPUs.** `GET /v1/backends` includes an
`available_devices` array (vendor, index, name, VRAM) so dashboards and
scripts can build explicit per-device backend specs without guessing indices.

Compatibility notes for scripted setups:
- `--backend-*-per-instance` lists must match the new instance count on
  multi-GPU rigs (the error message now says how many instances came from
  auto-detection and how to pin the count).
- Control API: a `backend_specs` entry without `device_index` now expands to
  all detected GPUs instead of device 0 only; pass `device_index` explicitly
  to keep one device. `GET /v1/backends` reports concrete per-GPU
  `device_index` values.

## v0.2.15

**Windows GPU mining now works out of the box.** The Windows package bundles
the NVIDIA NVRTC libraries seine needs — no CUDA toolkit installation, no PATH
setup. Unzip and mine.

**Much faster GPU startup.** First launch on an RTX 5090 tunes in about half
the time it used to; results are cached so later launches start in seconds.

**Faster CPU mining on Linux/WSL (opt-in).** New `--cpu-page-mode large` and
`large-1g` use reserved hugepages for roughly 6% and 8% more CPU throughput
respectively — see the README's "HugeTLB provisioning" section for the
one-time host setup.

**You'll know when something breaks.** If a GPU or CPU backend fails and
mining continues degraded, seine now warns you repeatedly instead of silently
mining at reduced speed.

**Solo miners: no more lost blocks after a daemon restart.** If the daemon has
forgotten your block template, seine automatically resubmits the full block.

Also: smarter thread autotuning on Apple Silicon (no more caching a
slightly-worse lane count), physical-core-first CPU affinity on Windows, and
expanded benchmarking tools.

## v0.2.14

**GPU pool miners: stop losing blocks.** When a batched GPU round found both a
pool share and an actual block solution, the block could be discarded in favor
of the share. Blocks are now always preserved and submitted.

**Version reporting.** seine now tells the pool its version and backend, so
pools can let you know when an update is worth it.
