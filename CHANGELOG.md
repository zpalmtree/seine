# Changelog

User-focused release notes. Each `## vX.Y.Z` section below becomes the body of
the corresponding GitHub release (the release workflow extracts it by tag and
appends the auto-generated compare link). Write for miners deciding whether to
upgrade: lead with what they gain, keep it brief, skip internal refactors.
`scripts/release_tag.sh` refuses to tag a version that has no section here.

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
