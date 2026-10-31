#!/usr/bin/env python3
"""Regenerates src/backend/nvidia/argon2_cc*.ptx, the Argon2 fill kernel built
for the network's Argon2 parameters with each architecture group's defines,
which Seine loads through the driver when NVRTC is not installed, and
argon2_sm*.cubin, the same kernel built for exact architectures, which Seine
loads without compiling when the GPU matches (listed in argon2_cubins.txt).

Usage: scripts/gen_argon2_ptx.py <libnvrtc.so.12 from CUDA 12.0> <libnvrtc.so.12 from CUDA 12.8> <libnvrtc.so.13>

NVRTC 12.0 (the nvidia-cuda-nvrtc-cu12==12.0.76 wheel) builds the files for
GTX 10 through RTX 40: the PTX ISA it emits loads on every CUDA 12 and 13 driver.
NVRTC 12.8 (nvidia-cuda-nvrtc-cu12==12.8.93) builds the Blackwell file for
compute_100, which needs a CUDA 12.8 driver, the minimum for those GPUs.
NVRTC 12.8 also builds the Turing to Ada cubins and NVRTC 13
(nvidia-cuda-nvrtc==13.0.88) the Blackwell cubin; on an older driver a cubin
fails to load and Seine compiles with NVRTC or loads the PTX. The
header lines record the kernel source hash and options that a unit test
compares against nvidia.rs.
"""
import ctypes
import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KERNEL = ROOT / "src/backend/nvidia_kernel.cu"
# Network Argon2 parameters (2 GiB, one pass) and the per-architecture cancel
# check interval; must match prebuilt_argon2_ptx() in nvidia.rs.
COMMON = [
    "--std=c++14",
    "--restrict",
    "--use_fast_math",
    "--ftz=true",
    "--fmad=true",
    "-DSEINE_FIXED_M_BLOCKS=2097152U",
    "-DSEINE_FIXED_T_COST=1U",
    "-DSEINE_WARPS_PER_BLOCK=1U",
    "-DSEINE_NO_FUSED_TARGET",
]
# Per-architecture fill defines and cancel check interval; must match
# arch_fill_defines() and select_cancel_check_block_interval() in nvidia.rs.
TARGETS = {
    "argon2_cc6.ptx": ["--gpu-architecture=compute_60", "-DSEINE_CANCEL_CHECK_BLOCK_INTERVAL=64U",
                       "-DSEINE_REF_PREFETCH_DEPTH=2U", "-DSEINE_INLINE_REBUILD=1", "-DSEINE_BLOCK_BASE_POINTER=1"],
    "argon2_cc7.ptx": ["--gpu-architecture=compute_75", "-DSEINE_CANCEL_CHECK_BLOCK_INTERVAL=64U",
                       "-DSEINE_REF_PREFETCH_DEPTH=2U", "-DSEINE_ROW_LAYOUT=1", "-DSEINE_LOOP_UNROLL=2",
                       "-DSEINE_INLINE_REBUILD=1", "-DSEINE_BLOCK_BASE_POINTER=1"],
    "argon2_cc8.ptx": ["--gpu-architecture=compute_80", "-DSEINE_CANCEL_CHECK_BLOCK_INTERVAL=128U",
                       "-DSEINE_REF_RING=1U", "-DSEINE_ROW_LAYOUT=1", "-DSEINE_LOOP_UNROLL=4",
                       "-DSEINE_INLINE_REBUILD=1", "-DSEINE_BLOCK_BASE_POINTER=2"],
    "argon2_cc10.ptx": ["--gpu-architecture=compute_100", "-DSEINE_CANCEL_CHECK_BLOCK_INTERVAL=128U",
                        "-DSEINE_REF_RING=1U", "-DSEINE_REF_PREFETCH_DEPTH=16U", "-DSEINE_ROW_LAYOUT=1"],
}


def check(lib, result, program=None):
    if result == 0:
        return
    message = lib.nvrtcGetErrorString(result).decode()
    if program is not None:
        size = ctypes.c_size_t()
        lib.nvrtcGetProgramLogSize(program, ctypes.byref(size))
        log = ctypes.create_string_buffer(size.value)
        lib.nvrtcGetProgramLog(program, log)
        message += "\n" + log.value.decode(errors="replace")
    sys.exit(f"nvrtc failed: {message}")


def compile_ptx(lib, source, options):
    program = ctypes.c_void_p()
    check(lib, lib.nvrtcCreateProgram(ctypes.byref(program), source, b"seine_argon2id_fill.cu", 0, None, None))
    encoded = (ctypes.c_char_p * len(options))(*[o.encode() for o in options])
    check(lib, lib.nvrtcCompileProgram(program, len(options), encoded), program)
    size = ctypes.c_size_t()
    check(lib, lib.nvrtcGetPTXSize(program, ctypes.byref(size)))
    ptx = ctypes.create_string_buffer(size.value)
    check(lib, lib.nvrtcGetPTX(program, ptx))
    lib.nvrtcDestroyProgram(ctypes.byref(program))
    return ptx.value.decode()


# Exact-architecture cubins: the group's PTX options for one sm_XX target, plus
# the register cap the NVRTC path passes (DEFAULT_NVIDIA_MAX_RREGCOUNT).
CUBINS = {
    "argon2_sm61.cubin": ("argon2_cc6.ptx", "sm_61"),
    "argon2_sm75.cubin": ("argon2_cc7.ptx", "sm_75"),
    "argon2_sm86.cubin": ("argon2_cc8.ptx", "sm_86"),
    "argon2_sm89.cubin": ("argon2_cc8.ptx", "sm_89"),
    "argon2_sm120.cubin": ("argon2_cc10.ptx", "sm_120"),
}
CUBIN_EXTRA = ["--maxrregcount=240"]

# Files built with NVRTC 12.8 (second argument) and NVRTC 13 (third). The
# Turing to Ada cubins need a CUDA 12.8 driver; on an older one they fail to load
# and Seine compiles with NVRTC or loads the PTX.
NEWER_NVRTC = {"argon2_cc10.ptx", "argon2_sm75.cubin", "argon2_sm86.cubin", "argon2_sm89.cubin"}
NEWEST_NVRTC = {"argon2_sm120.cubin"}


def compile_cubin(lib, source, options):
    program = ctypes.c_void_p()
    check(lib, lib.nvrtcCreateProgram(ctypes.byref(program), source, b"seine_argon2id_fill.cu", 0, None, None))
    encoded = (ctypes.c_char_p * len(options))(*[o.encode() for o in options])
    check(lib, lib.nvrtcCompileProgram(program, len(options), encoded), program)
    size = ctypes.c_size_t()
    check(lib, lib.nvrtcGetCUBINSize(program, ctypes.byref(size)))
    cubin = ctypes.create_string_buffer(size.value)
    check(lib, lib.nvrtcGetCUBIN(program, cubin))
    lib.nvrtcDestroyProgram(ctypes.byref(program))
    return cubin.raw


def load_nvrtc(path):
    lib = ctypes.CDLL(path)
    lib.nvrtcGetErrorString.restype = ctypes.c_char_p
    major, minor = ctypes.c_int(), ctypes.c_int()
    check(lib, lib.nvrtcVersion(ctypes.byref(major), ctypes.byref(minor)))
    return lib, major, minor


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    libs = [load_nvrtc(sys.argv[1]), load_nvrtc(sys.argv[2]), load_nvrtc(sys.argv[3])]
    source = KERNEL.read_bytes()
    digest = hashlib.sha256(source).hexdigest()
    for name, extra in TARGETS.items():
        lib, major, minor = libs[1] if name in NEWER_NVRTC else libs[0]
        options = COMMON + extra
        ptx = compile_ptx(lib, source, options)
        header = (
            f"// Generated by scripts/gen_argon2_ptx.py with NVRTC {major.value}.{minor.value}; do not edit.\n"
            f"// kernel-sha256: {digest}\n"
            f"// options: {' '.join(options)}\n"
        )
        output = ROOT / "src/backend/nvidia" / name
        output.write_text(header + ptx)
        print(f"wrote {output.relative_to(ROOT)} (NVRTC {major.value}.{minor.value}, kernel {digest[:12]})")
    manifest = [
        "# Generated by scripts/gen_argon2_ptx.py; do not edit.",
        f"kernel-sha256: {digest}",
    ]
    for name, (ptx_name, arch) in CUBINS.items():
        lib, major, minor = libs[2] if name in NEWEST_NVRTC else libs[1] if name in NEWER_NVRTC else libs[0]
        options = [o for o in COMMON + TARGETS[ptx_name] if not o.startswith("--gpu-architecture=")]
        options += [f"--gpu-architecture={arch}"] + CUBIN_EXTRA
        output = ROOT / "src/backend/nvidia" / name
        output.write_bytes(compile_cubin(lib, source, options))
        manifest.append(f"{name}: NVRTC {major.value}.{minor.value}: {' '.join(options)}")
        print(f"wrote {output.relative_to(ROOT)} (NVRTC {major.value}.{minor.value}, {arch})")
    (ROOT / "src/backend/nvidia/argon2_cubins.txt").write_text("\n".join(manifest) + "\n")


if __name__ == "__main__":
    main()
