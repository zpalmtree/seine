# AMD GPUs

Seine mines on AMD Radeon RDNA GPUs (RX 6000, RX 7000, RX 9000 and the Radeon PRO
parts built on the same chips) through its HIP backend. With no `--backend` flag it
mines on every discrete AMD GPU it finds, alongside the CPU. Integrated Radeon
graphics stay off unless you list `amd` yourself.

```bash
# Default: CPU plus every discrete AMD GPU
./seine --address YOUR_ADDRESS

# GPU-only: one backend instance per detected device
./seine --backend amd --address YOUR_ADDRESS

# Select specific devices, or cap the VRAM lanes on a shared card
./seine --backend amd --amd-devices 0 --amd-max-lanes 4
```

## Requirements

- **Linux x86_64** release archive (`seine-linux-x86_64.tar.gz`) or a source build
  with `--features amd`. Windows is not supported yet.
- The **amdgpu kernel driver** (in-tree in current distributions, or AMD's
  `amdgpu-dkms`) and the **ROCm HIP runtime**: `hip-runtime-amd` from AMD's
  repository, or the full ROCm install. ROCm 6.x and 10.x both work. Seine compiles
  its kernel at startup for the exact GPU with hipRTC; RDNA2, RDNA3 and RDNA4 cards
  also run without hipRTC, using kernels built into the release binary.
- Your user must be able to open `/dev/kfd` and `/dev/dri/renderD*` (the `render`
  and `video` groups on most distributions).
- One **2 GiB VRAM lane per concurrent hash** (1.5-2 GiB on RDNA2, RDNA3 and RDNA4,
  chosen per card for the most hashrate): a 16 GB card runs 7 lanes (RDNA2-4: about
  9), a 24 GB card 11 (RDNA3: about 14), a 32 GB card 15 (RDNA2 and RDNA4: about 19).
  Seine sizes this automatically from free VRAM and `--amd-max-lanes`.

If the runtime is missing, Seine mines on the CPU only; with `--backend amd` it says
so at startup.

## Performance level

On RDNA3 cards (RX 7000, Radeon PRO W7000/V710) Seine sets the amdgpu performance
level to `high` at startup when it can write the sysfs entry (root), prints the
`echo high | sudo tee /sys/bus/pci/devices/<address>/power_dpm_force_performance_level`
command when it cannot, and restores the previous level when it exits. Running as
root or applying that command is worth a lot on cards that run only a few lanes.
Other AMD GPUs keep the driver's own level, which is faster there.
Set `SEINE_AMD_KEEP_PERF_LEVEL=1` to leave the driver's setting alone.

## Settings

There are no tuning knobs beyond `--amd-max-lanes`. `SEINE_AMD_BLOCK_SYNC=1`
switches the kernel to a block-wide barrier if a driver misbehaves with the
default wave-scoped one.
