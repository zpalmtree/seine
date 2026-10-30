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
  with `--features amd`. Windows is not supported yet: the backend loads
  `libamdhip64.so` and `libhiprtc.so` at startup and compiles its kernel with
  hipRTC, which the Windows driver does not ship.
- The **amdgpu kernel driver** (in-tree in current distributions, or AMD's
  `amdgpu-dkms`) and the **ROCm HIP runtime**: `hip-runtime-amd` from AMD's
  repository, or the full ROCm install. ROCm 6.x and 10.x both work; the kernel is
  compiled at startup for the exact GPU, so no architecture list is needed.
- Your user must be able to open `/dev/kfd` and `/dev/dri/renderD*` (the `render`
  and `video` groups on most distributions).
- One **2 GiB VRAM lane per concurrent hash**: a 16 GB card runs 7 lanes, a 24 GB
  card 11, a 32 GB card 15. Seine sizes this automatically from free VRAM.

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
