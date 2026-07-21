# AMD (HIP/ROCm) Backend Bring-Up Plan

Target: **RX 7900 XTX (gfx1100, RDNA3, 24 GB, wave32)** on a rented Linux
ROCm box. The backend is scaffolded to compile and unit-test on any x86_64
Linux host with no AMD hardware or ROCm installation; everything below is the
first-hardware iteration checklist.

## Status / scope

- `--features amd` (OFF by default). All HIP/hipRTC FFI is hand-declared and
  dlopen'd at runtime (`libamdhip64.so`, `libhiprtc.so`) — no ROCm headers or
  toolkit needed at build time.
- Kernel: HIP port of the CUDA Argon2id fill/seed/eval kernels
  (`src/backend/amd/amd_kernel.hip`), keeping A29 full-warp cooperative
  mapping, A77/A78 unrolled loads/stores, A81 closed-form G-round indexing,
  and A82 shuffle fusion. The A73 dead-code-prefetch codegen hack and its
  first-8-threads guard are NVIDIA/PTXAS-specific and were omitted.
- Wavefront strategy: **wave32 only**. hipRTC compiles with
  `--offload-arch=<gcnArchName>` plus `-mno-wavefrontsize64` (with a retry
  without the flag if a hipRTC build rejects it), and the engine hard-refuses
  devices reporting `warpSize != 32` — so CDNA (wave64) boxes quarantine at
  startup with a clear message. The cooperative barrier is `__syncthreads()`
  (block == one wave32 wavefront), chosen for correctness over speed.
- Autotune: stub. Lanes derive from free VRAM with allocation/probe backoff
  (11 × 2 GiB lanes expected on 24 GB); launch depth fixed at 1. Regcap/depth
  sweeps and any AMD-specific scheduling work come after this checklist
  passes.

## Recommended rental image

- Vast.ai or RunPod, Ubuntu 22.04 or 24.04 with **ROCm 6.x** preinstalled
  (e.g. `rocm/dev-ubuntu-22.04:6.2` or newer; any image where `rocminfo`
  and `/opt/rocm/lib/libamdhip64.so` exist). Kernel driver `amdgpu` must be
  loaded on the host; verify the container/VM actually exposes
  `/dev/kfd` and `/dev/dri`.
- Install build deps: `rustup` (repo pins 1.93.0 via rust-toolchain.toml),
  `build-essential`, `git`.
- If the loader cannot find the libraries, add ROCm to the search path:
  `export LD_LIBRARY_PATH=/opt/rocm/lib:$LD_LIBRARY_PATH`.

Sanity: `rocminfo | grep gfx` should print `gfx1100` and
`rocm-smi --showmeminfo vram` should show ~24 GB.

## Verification sequence

Run steps in order; each gate must pass before the next is meaningful.

### (a) Unit tests, including the ignored hardware tests

```bash
cargo test --features amd
cargo test --features amd -- --ignored --nocapture
```

The ignored set, in dependency order:

1. `backend::amd::hip_ffi::…loader_reports_missing_rocm…` (not ignored; on
   the box it must take the Ok path — i.e. simply pass).
2. `backend::amd::engine::tests::hip_device_props_parse_sanely` — validates
   the hand-declared legacy (`R0000`) `hipDeviceProp_t` ABI: wavefront size
   must read 32 and `gcnArchName` must start with `gfx`. **If this fails, the
   struct layout in `src/backend/amd/hip_ffi.rs` is wrong for the installed
   ROCm — fix offsets before anything else; every later step depends on it.**
3. `backend::amd::engine::tests::hiprtc_kernel_compiles_for_device_arch` —
   first real hipRTC compile of the ported kernel. Compile-log errors surface
   in the test failure message.
4. `backend::amd::tests::gpu_solution_target_bracket_matches_cpu_reference`

### (b) Small-scale correctness differential vs the CPU backend

This mirrors the NVIDIA backend's ignored GPU-vs-CPU differential
(`gpu_solution_target_bracket_matches_cpu_reference` in
`src/backend/nvidia.rs`, which brackets each nonce with target == CPU hash
(must solve) and target == hash − 1 (must not solve)):

```bash
cargo test --features amd gpu_solution_target_bracket_matches_cpu_reference -- --ignored --nocapture
```

A pass proves the full pipeline — seed blocks, 2 GiB Argon2id lane fill,
last-block export, Blake2b target evaluation — is bit-exact against
`blocknet-pow-kernel` for several nonces. If hashes mismatch, bisect with the
seed kernel first (dump `seed_blocks` DtoH and compare against the CPU H0
path) before suspecting the compression loop.

### (c) Single-lane kernel benchmark

```bash
cargo run --release --features amd -- --bench --bench-kind kernel \
  --backend amd --amd-max-lanes 1 --bench-secs 20 --bench-rounds 3 \
  --bench-output bench-amd-1lane.json
```

Expect roughly 0.4–0.5 H/s per lane if RDNA3 behaves like other
DRAM-latency-bound runs of this kernel (one hash ≈ sequential walk over
2 GiB). Also run `--bench-kind kernel-effective` to include the eval path.

### (d) Lane scaling on 24 GB

```bash
for lanes in 1 2 4 8 11; do
  cargo run --release --features amd -- --bench --bench-kind backend \
    --backend amd --amd-max-lanes $lanes --bench-secs 20 --bench-rounds 3 \
    --bench-warmup-rounds 1 --bench-output bench-amd-${lanes}lane.json
done
```

- 11 lanes is the expected maximum: 24 576 MiB total − 384 MiB reserve →
  ~23.6 GiB budget / 2 GiB per lane (the engine backs off automatically if
  the display or other processes hold VRAM).
- Scaling should be near-linear until DRAM latency/bandwidth saturates.
- **Expected ballpark (speculative until measured):** the workload is
  DRAM-latency-bound; RX 7900 XTX has ~960 GB/s bandwidth vs the RTX 5090's
  ~1.79 TB/s, so if latency characteristics are similar, expect roughly half
  of the 5090's ~9.6 H/s backend rate — i.e. **~4–5 H/s** at full lanes.
  Treat anything within 2–8 H/s as plausible for a first unoptimized run;
  the number exists to catch order-of-magnitude breakage, not to be a target.
- Check `late_hash_pct` in the report: with depth 1 and cancel checkpoints
  every 64 blocks, round-end preemption should be comparable to the NVIDIA
  backend's; a large value suggests the device-side cancel flag isn't being
  observed (verify the `volatile` load in the kernel actually bypasses
  caches on RDNA3 — see "Known risks" below).

### (e) Mining smoke test (pool)

```bash
cargo run --release --features amd -- --backend cpu,amd
```

Confirm: backend line shows `amd` with 11 lanes; cancel/fence startup probe
passes (the backend advertises cooperative deadlines, so it is NOT gated
behind `--allow-best-effort-deadlines`); shares from backend `amd` are
accepted by the pool.

## Known risks to verify on hardware (in priority order)

1. **`hipDeviceProp_t` legacy ABI layout** — covered by step (a)/test 2.
2. **hipRTC option acceptance** — `--offload-arch` with a full target id
   (`gfx1100:sramecc-:xnack-`) and `-mno-wavefrontsize64`; the engine
   already retries without the wave flag and surfaces the compile log.
3. **Cancel-flag visibility during a running kernel** — the interrupt
   controller writes via a non-blocking stream while the fill kernel polls a
   `volatile` global pointer. Verify with a mid-round cancel (`Ctrl-C`
   during mining or the step-(d) round fences) that fences complete in
   ≪ 1 s rather than after a full launch.
4. **`__shfl` on 64-bit values under wave32** — exercised by the (b)
   differential; a wrong-width shuffle shows up as hash mismatches.
5. **Multi-device** — a bare `--backend amd` on a 2-GPU rental now
   auto-expands to one instance per wave32 device (`--amd-devices 0,1`
   pins the same topology explicitly); device binding is per worker
   thread (`bind_thread()`), which this verifies.

## After bring-up (deferred tuning surface)

Only after (a)–(e) pass, in an AMD_OPTIMIZATION_LOG.md mirroring the NVIDIA
process: launch-depth sweep (`hashes_per_launch_per_lane`), cancel-check
cadence, waves-per-CU occupancy experiments, `s_setprio`/scheduling hints,
`__builtin_amdgcn` barrier relaxations replacing `__syncthreads()`,
wave64-native kernel for CDNA, and a persisted autotune cache keyed like the
NVIDIA one (device, arch, hiprtc version, kernel fingerprint).
