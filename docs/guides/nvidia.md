# NVIDIA GPUs

Seine mines on every detected NVIDIA GPU by default, alongside the CPU, and the
CPU and GPU hashrates add together.

```bash
# Default: no flags needed — CPU plus every detected NVIDIA GPU
./seine

# GPU-only: one backend instance per detected device
./seine --backend nvidia

# Select specific devices (also the single-GPU opt-out, e.g. --nvidia-devices 0)
./seine --backend nvidia --nvidia-devices 0,1
```

If CUDA initialization fails, NVIDIA backends are quarantined and CPU mining
continues. If a backend fails mid-run, Seine keeps warning
(`running degraded: N/M backends active`) instead of silently mining slower.

## Requirements

You need an up-to-date NVIDIA display driver. Seine builds its kernels with NVRTC
when it is installed and otherwise uses kernels built into Seine.

- **Windows:** the release archive bundles the CUDA 12.8.61 NVRTC DLLs beside
  `seine.exe`. No CUDA Toolkit installation or `PATH` change is needed.
- **Linux:** the driver is enough. If NVRTC is installed (from your distribution or
  NVIDIA's CUDA packages) Seine uses it; otherwise it loads its built-in kernels for
  GeForce GTX 10 series and newer.
- **Source builds on Windows:** the build uses cudarc's CUDA 12.x loader, so a
  machine with only CUDA 13.x DLL names (for example `nvrtc64_130_0.dll`) is not
  discovered. Install the CUDA 12.8 NVRTC redistributable or add its `bin`
  directory to `PATH`.

## Tuning

There is nothing to tune: each GPU starts mining within seconds with as many
lanes as its free memory holds, and all GPUs start at the same time.

Native Windows is about 9% faster than WSL2 on the same RTX 5090 because it avoids
WSL's job-boundary latency.

## AMD GPUs

Radeon RDNA GPUs mine through the HIP backend on Linux: see
[AMD GPUs](amd.md). Discrete AMD GPUs are used automatically.
