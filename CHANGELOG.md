# Changelog

User-focused release notes. Each `## vX.Y.Z` section below becomes the body of
the corresponding GitHub release (the release workflow extracts it by tag and
appends the auto-generated compare link). Write for miners deciding whether to
upgrade: lead with what they gain, keep it brief, skip internal refactors.
`scripts/release_tag.sh` refuses to tag a version that has no section here.

## Unreleased

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
