#!/usr/bin/env python3
"""Reproducible host capture and paired A/B analysis for Seine benchmarks.

The implementation intentionally uses only the Python standard library so the
same controller can run on Linux, WSL, macOS, and Windows benchmark hosts.
"""

from __future__ import annotations

import argparse
import csv
import ctypes
import datetime as dt
import hashlib
import json
import math
import os
import platform
import random
import re
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


PREFLIGHT_SCHEMA = "seine-benchmark-preflight/v1"
COMPARISON_SCHEMA = "seine-benchmark-comparison/v1"
RUN_SCHEMA = "seine-benchmark-run/v1"
DEFAULT_BOOTSTRAP_SAMPLES = 20_000
DEFAULT_BOOTSTRAP_SEED = 0x5E1E


class BenchctlError(ValueError):
    """Expected input or environment error suitable for a concise CLI message."""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, path)


def read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except (OSError, PermissionError):
        return None


def run_capture(
    argv: Sequence[str], *, cwd: Optional[Path] = None, timeout: float = 5.0
) -> Tuple[Optional[str], Optional[str]]:
    """Run an argv directly and return (stdout, error); never invoke a shell."""

    try:
        completed = subprocess.run(
            list(argv),
            cwd=str(cwd) if cwd is not None else None,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
            shell=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, str(exc)
    stdout = completed.stdout.strip()
    if completed.returncode != 0:
        detail = completed.stderr.strip() or stdout or f"exit code {completed.returncode}"
        return None, detail
    return stdout, None


def detect_runtime() -> Dict[str, Any]:
    system = platform.system()
    release = platform.release()
    version = platform.version()
    if system == "Linux":
        osrelease = read_text(Path("/proc/sys/kernel/osrelease")) or release
        is_wsl = "microsoft" in osrelease.lower() or bool(os.environ.get("WSL_DISTRO_NAME"))
        kind = "wsl" if is_wsl else "native-linux"
    elif system == "Darwin":
        kind = "macos"
    elif system == "Windows":
        kind = "windows"
    else:
        kind = "unknown"
    result: Dict[str, Any] = {
        "kind": kind,
        "system": system,
        "release": release,
        "version": version,
        "machine": platform.machine(),
        "hostname": platform.node(),
        "python": platform.python_version(),
    }
    if kind == "wsl":
        result["wsl_distribution"] = os.environ.get("WSL_DISTRO_NAME")
        result["wsl_interop"] = bool(os.environ.get("WSL_INTEROP"))
    if system == "Darwin":
        result["macos_version"] = platform.mac_ver()[0] or None
    return result


def parse_linux_cpu_list(value: str) -> List[int]:
    """Parse Linux cpulist syntax such as ``0-7:2,12,14-15``."""

    cpus: set[int] = set()
    for raw_part in value.strip().split(","):
        part = raw_part.strip()
        if not part:
            continue
        match = re.fullmatch(r"(\d+)(?:-(\d+)(?::(\d+))?)?", part)
        if not match:
            raise BenchctlError(f"invalid Linux CPU list component: {part!r}")
        start = int(match.group(1))
        end = int(match.group(2) or start)
        step = int(match.group(3) or 1)
        if step < 1 or end < start:
            raise BenchctlError(f"invalid Linux CPU range: {part!r}")
        cpus.update(range(start, end + 1, step))
    return sorted(cpus)


def process_affinity() -> Optional[List[int]]:
    get_affinity = getattr(os, "sched_getaffinity", None)
    if get_affinity is None:
        return None
    try:
        return sorted(get_affinity(0))
    except OSError:
        return None


def first_cpuinfo_value(keys: Iterable[str]) -> Optional[str]:
    text = read_text(Path("/proc/cpuinfo"))
    if not text:
        return None
    values: Dict[str, str] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        values.setdefault(key.strip().lower(), value.strip())
    for key in keys:
        if key.lower() in values:
            return values[key.lower()]
    return None


def linux_cpu_topology(available: Sequence[int]) -> Dict[str, Any]:
    groups: Dict[Tuple[str, str], List[int]] = {}
    fallback_groups: Dict[Tuple[int, ...], List[int]] = {}
    for cpu in available:
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        package = read_text(topology / "physical_package_id")
        core = read_text(topology / "core_id")
        if package is not None and core is not None:
            groups.setdefault((package, core), []).append(cpu)
            continue
        siblings = read_text(topology / "thread_siblings_list")
        if siblings:
            try:
                key = tuple(parse_linux_cpu_list(siblings))
            except BenchctlError:
                continue
            fallback_groups.setdefault(key, []).append(cpu)

    core_groups: List[Dict[str, Any]] = []
    if groups:
        for (package, core), cpus in sorted(
            groups.items(), key=lambda item: min(item[1])
        ):
            core_groups.append(
                {
                    "package": int(package) if package.isdigit() else package,
                    "core": int(core) if core.isdigit() else core,
                    "logical_cpus": sorted(cpus),
                }
            )
    else:
        for index, cpus in enumerate(
            sorted(fallback_groups.values(), key=lambda values: min(values))
        ):
            core_groups.append(
                {"package": None, "core": index, "logical_cpus": sorted(cpus)}
            )

    sockets = {group["package"] for group in core_groups if group["package"] is not None}
    return {
        "physical_cores": len(core_groups) or None,
        "sockets": len(sockets) or None,
        "core_groups": core_groups or None,
        "smt_visible": any(len(group["logical_cpus"]) > 1 for group in core_groups)
        if core_groups
        else None,
    }


def sysctl_value(name: str) -> Optional[str]:
    executable = shutil.which("sysctl")
    if executable is None:
        return None
    stdout, _ = run_capture([executable, "-n", name])
    return stdout


def int_or_none(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None


def windows_cpu_details() -> Dict[str, Any]:
    details: Dict[str, Any] = {}
    powershell = shutil.which("powershell.exe") or shutil.which("powershell")
    if powershell is None:
        return details
    script = (
        "Get-CimInstance Win32_Processor | "
        "Select-Object Name,NumberOfCores,NumberOfLogicalProcessors | "
        "ConvertTo-Json -Compress"
    )
    stdout, error = run_capture(
        [powershell, "-NoProfile", "-NonInteractive", "-Command", script], timeout=8
    )
    if error or not stdout:
        return details
    try:
        rows = json.loads(stdout)
    except json.JSONDecodeError:
        return details
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        return details
    valid_rows = [row for row in rows if isinstance(row, dict)]
    if not valid_rows:
        return details
    details["model"] = "; ".join(
        str(row.get("Name", "")).strip()
        for row in valid_rows
        if str(row.get("Name", "")).strip()
    ) or None
    try:
        details["physical_cores"] = sum(int(row["NumberOfCores"]) for row in valid_rows)
        details["logical_host"] = sum(
            int(row["NumberOfLogicalProcessors"]) for row in valid_rows
        )
        details["sockets"] = len(valid_rows)
    except (KeyError, TypeError, ValueError):
        pass
    return details


def collect_cpu(runtime: Mapping[str, Any]) -> Dict[str, Any]:
    host_logical = os.cpu_count()
    affinity = process_affinity()
    visible = len(affinity) if affinity is not None else host_logical
    result: Dict[str, Any] = {
        "architecture": platform.machine(),
        "model": platform.processor() or None,
        "logical_host": host_logical,
        "logical_available": visible,
        "available_cpu_ids": affinity,
        "physical_cores": None,
        "sockets": None,
        "core_groups": None,
        "smt_visible": None,
    }
    kind = runtime["kind"]
    if kind in ("native-linux", "wsl"):
        result["model"] = first_cpuinfo_value(("model name", "hardware", "processor"))
        available = affinity if affinity is not None else list(range(host_logical or 0))
        result.update(linux_cpu_topology(available))
    elif kind == "macos":
        result["model"] = sysctl_value("machdep.cpu.brand_string") or sysctl_value(
            "hw.model"
        )
        result["physical_cores"] = int_or_none(sysctl_value("hw.physicalcpu"))
        result["logical_host"] = int_or_none(sysctl_value("hw.logicalcpu")) or host_logical
        result["logical_available"] = result["logical_host"]
        perf_levels = int_or_none(sysctl_value("hw.nperflevels"))
        if perf_levels:
            levels = []
            for index in range(perf_levels):
                levels.append(
                    {
                        "level": index,
                        "physical_cores": int_or_none(
                            sysctl_value(f"hw.perflevel{index}.physicalcpu")
                        ),
                        "logical_cpus": int_or_none(
                            sysctl_value(f"hw.perflevel{index}.logicalcpu")
                        ),
                        "name": sysctl_value(f"hw.perflevel{index}.name"),
                    }
                )
            result["performance_levels"] = levels
        physical = result["physical_cores"]
        logical = result["logical_available"]
        if physical and logical:
            result["smt_visible"] = logical > physical
    elif kind == "windows":
        result.update(windows_cpu_details())
        physical = result.get("physical_cores")
        logical = result.get("logical_available")
        if physical and logical:
            result["smt_visible"] = logical > physical
    return result


def parse_proc_meminfo(text: str) -> Dict[str, int]:
    result: Dict[str, int] = {}
    for line in text.splitlines():
        match = re.fullmatch(r"([^:]+):\s*(\d+)\s*(kB)?", line.strip())
        if not match:
            continue
        value = int(match.group(2))
        if match.group(3):
            value *= 1024
        result[match.group(1)] = value
    return result


def parse_size(value: str) -> Optional[int]:
    match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)(?:i?B)?\s*", value, re.I)
    if not match:
        return None
    scale = "KMGTPE".find(match.group(2).upper()) + 1 if match.group(2) else 0
    return int(float(match.group(1)) * (1024**scale))


def windows_memory() -> Dict[str, Optional[int]]:
    class MemoryStatusEx(ctypes.Structure):
        _fields_ = [
            ("length", ctypes.c_ulong),
            ("memory_load", ctypes.c_ulong),
            ("total_physical", ctypes.c_ulonglong),
            ("available_physical", ctypes.c_ulonglong),
            ("total_page_file", ctypes.c_ulonglong),
            ("available_page_file", ctypes.c_ulonglong),
            ("total_virtual", ctypes.c_ulonglong),
            ("available_virtual", ctypes.c_ulonglong),
            ("available_extended_virtual", ctypes.c_ulonglong),
        ]

    status = MemoryStatusEx()
    status.length = ctypes.sizeof(status)
    try:
        success = ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
    except (AttributeError, OSError):
        return {}
    if not success:
        return {}
    swap_total = max(0, status.total_page_file - status.total_physical)
    swap_available = max(0, status.available_page_file - status.available_physical)
    return {
        "total_bytes": status.total_physical,
        "available_bytes": status.available_physical,
        "swap_total_bytes": swap_total,
        "swap_free_bytes": min(swap_total, swap_available),
        "swap_used_bytes": max(0, swap_total - swap_available),
    }


def collect_memory(runtime: Mapping[str, Any]) -> Dict[str, Optional[int]]:
    kind = runtime["kind"]
    if kind in ("native-linux", "wsl"):
        values = parse_proc_meminfo(read_text(Path("/proc/meminfo")) or "")
        swap_total = values.get("SwapTotal")
        swap_free = values.get("SwapFree")
        return {
            "total_bytes": values.get("MemTotal"),
            "available_bytes": values.get("MemAvailable", values.get("MemFree")),
            "free_bytes": values.get("MemFree"),
            "swap_total_bytes": swap_total,
            "swap_free_bytes": swap_free,
            "swap_used_bytes": max(0, swap_total - swap_free)
            if swap_total is not None and swap_free is not None
            else None,
        }
    if kind == "macos":
        total = int_or_none(sysctl_value("hw.memsize"))
        result: Dict[str, Optional[int]] = {
            "total_bytes": total,
            "available_bytes": None,
            "swap_total_bytes": None,
            "swap_free_bytes": None,
            "swap_used_bytes": None,
        }
        vm_stat, _ = run_capture([shutil.which("vm_stat") or "vm_stat"])
        if vm_stat:
            page_match = re.search(r"page size of (\d+) bytes", vm_stat)
            page_size = int(page_match.group(1)) if page_match else 4096
            pages: Dict[str, int] = {}
            for line in vm_stat.splitlines():
                match = re.match(r"([^:]+):\s*(\d+)\.", line)
                if match:
                    pages[match.group(1)] = int(match.group(2))
            available_pages = sum(
                pages.get(name, 0)
                for name in ("Pages free", "Pages inactive", "Pages speculative")
            )
            result["available_bytes"] = available_pages * page_size
        swap = sysctl_value("vm.swapusage")
        if swap:
            fields = dict(re.findall(r"(total|used|free)\s*=\s*([^\s]+)", swap))
            result["swap_total_bytes"] = parse_size(fields.get("total", ""))
            result["swap_used_bytes"] = parse_size(fields.get("used", ""))
            result["swap_free_bytes"] = parse_size(fields.get("free", ""))
        return result
    if kind == "windows":
        return windows_memory()
    return {
        "total_bytes": None,
        "available_bytes": None,
        "swap_total_bytes": None,
        "swap_free_bytes": None,
        "swap_used_bytes": None,
    }


def collect_load(cpu: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        averages = list(os.getloadavg())
    except (AttributeError, OSError):
        averages = []
    logical = cpu.get("logical_available")
    normalized = [value / logical for value in averages] if logical else None
    return {
        "load_average_1m": averages[0] if averages else None,
        "load_average_5m": averages[1] if averages else None,
        "load_average_15m": averages[2] if averages else None,
        "normalized_by_logical_cpu": normalized,
    }


def collect_power(runtime: Mapping[str, Any], cpu: Mapping[str, Any]) -> Dict[str, Any]:
    kind = runtime["kind"]
    result: Dict[str, Any] = {}
    if kind in ("native-linux", "wsl"):
        affinity = cpu.get("available_cpu_ids") or range(cpu.get("logical_available") or 0)
        governors = set()
        preferences = set()
        drivers = set()
        for cpu_id in affinity:
            base = Path(f"/sys/devices/system/cpu/cpu{cpu_id}/cpufreq")
            governor = read_text(base / "scaling_governor")
            preference = read_text(base / "energy_performance_preference")
            driver = read_text(base / "scaling_driver")
            if governor:
                governors.add(governor)
            if preference:
                preferences.add(preference)
            if driver:
                drivers.add(driver)
        result.update(
            {
                "cpu_governors": sorted(governors) or None,
                "energy_performance_preferences": sorted(preferences) or None,
                "scaling_drivers": sorted(drivers) or None,
                "amd_pstate_status": read_text(
                    Path("/sys/devices/system/cpu/amd_pstate/status")
                ),
                "intel_pstate_status": read_text(
                    Path("/sys/devices/system/cpu/intel_pstate/status")
                ),
            }
        )
        power_sources = []
        for path in sorted(Path("/sys/class/power_supply").glob("*")):
            source_type = read_text(path / "type")
            if source_type in ("Mains", "USB", "USB_C"):
                power_sources.append(
                    {
                        "name": path.name,
                        "type": source_type,
                        "online": read_text(path / "online"),
                    }
                )
        result["power_sources"] = power_sources or None
    elif kind == "macos":
        pmset = shutil.which("pmset")
        if pmset:
            battery, battery_error = run_capture([pmset, "-g", "batt"])
            custom, custom_error = run_capture([pmset, "-g", "custom"])
            result["battery_status"] = battery
            result["power_settings"] = custom
            if battery_error:
                result["collection_error"] = battery_error
            elif custom_error:
                result["collection_error"] = custom_error
    elif kind == "windows":
        powercfg = shutil.which("powercfg.exe") or shutil.which("powercfg")
        if powercfg:
            scheme, error = run_capture([powercfg, "/getactivescheme"])
            result["active_power_scheme"] = scheme
            if error:
                result["collection_error"] = error
    return result


def active_thp_mode(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    match = re.search(r"\[([^]]+)]", value)
    return match.group(1) if match else None


def collect_pages(runtime: Mapping[str, Any]) -> Dict[str, Any]:
    if runtime["kind"] not in ("native-linux", "wsl"):
        return {"hugetlb": None, "transparent_huge_pages": None}
    meminfo = parse_proc_meminfo(read_text(Path("/proc/meminfo")) or "")
    enabled = read_text(
        Path("/sys/kernel/mm/transparent_hugepage/enabled")
    )
    defrag = read_text(Path("/sys/kernel/mm/transparent_hugepage/defrag"))
    return {
        "hugetlb": {
            "page_size_bytes": meminfo.get("Hugepagesize"),
            "total_pages": meminfo.get("HugePages_Total"),
            "free_pages": meminfo.get("HugePages_Free"),
            "reserved_pages": meminfo.get("HugePages_Rsvd"),
            "surplus_pages": meminfo.get("HugePages_Surp"),
        },
        "transparent_huge_pages": {
            "enabled": enabled,
            "active_mode": active_thp_mode(enabled),
            "defrag": defrag,
            "defrag_active_mode": active_thp_mode(defrag),
        },
    }


def parse_nvidia_csv(stdout: str, fields: Sequence[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for csv_row in csv.reader(stdout.splitlines(), skipinitialspace=True):
        if len(csv_row) != len(fields):
            continue
        row: Dict[str, Any] = {}
        for field, raw_value in zip(fields, csv_row):
            value = raw_value.strip()
            if value.lower() in ("n/a", "[n/a]", "not supported", ""):
                row[field] = None
                continue
            if field in {
                "index",
                "temperature_c",
                "gpu_utilization_pct",
                "memory_utilization_pct",
                "memory_total_mib",
                "memory_used_mib",
                "sm_clock_mhz",
                "memory_clock_mhz",
            }:
                try:
                    row[field] = int(float(value))
                    continue
                except ValueError:
                    pass
            if field in {"power_draw_w", "power_limit_w"}:
                try:
                    row[field] = float(value)
                    continue
                except ValueError:
                    pass
            row[field] = value
        rows.append(row)
    return rows


def collect_nvidia() -> Dict[str, Any]:
    executable = shutil.which("nvidia-smi") or shutil.which("nvidia-smi.exe")
    if executable is None:
        return {"available": False, "executable": None, "gpus": None}
    query_fields = [
        ("index", "index"),
        ("name", "name"),
        ("uuid", "uuid"),
        ("driver_version", "driver_version"),
        ("pstate", "pstate"),
        ("temperature.gpu", "temperature_c"),
        ("utilization.gpu", "gpu_utilization_pct"),
        ("utilization.memory", "memory_utilization_pct"),
        ("memory.total", "memory_total_mib"),
        ("memory.used", "memory_used_mib"),
        ("power.draw", "power_draw_w"),
        ("power.limit", "power_limit_w"),
        ("clocks.sm", "sm_clock_mhz"),
        ("clocks.mem", "memory_clock_mhz"),
    ]
    stdout, error = run_capture(
        [
            executable,
            "--query-gpu=" + ",".join(field[0] for field in query_fields),
            "--format=csv,noheader,nounits",
        ],
        timeout=8,
    )
    result: Dict[str, Any] = {
        "available": stdout is not None,
        "executable": executable,
        "gpus": parse_nvidia_csv(stdout or "", [field[1] for field in query_fields])
        if stdout
        else None,
    }
    if error:
        result["collection_error"] = error

    apps_stdout, apps_error = run_capture(
        [
            executable,
            "--query-compute-apps=pid,process_name,used_memory",
            "--format=csv,noheader,nounits",
        ],
        timeout=8,
    )
    if apps_stdout:
        result["compute_processes"] = parse_nvidia_csv(
            apps_stdout, ("pid", "process_name", "used_memory_mib")
        )
    else:
        result["compute_processes"] = []
        if apps_error and "No running processes found" not in apps_error:
            result["compute_process_collection_error"] = apps_error
    return result


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_identity(path: Path) -> Dict[str, Any]:
    resolved = path.expanduser().resolve()
    result: Dict[str, Any] = {"path": str(resolved), "exists": resolved.is_file()}
    if not resolved.is_file():
        return result
    stat = resolved.stat()
    result.update(
        {
            "size_bytes": stat.st_size,
            "modified_utc": dt.datetime.fromtimestamp(
                stat.st_mtime, tz=dt.timezone.utc
            ).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "sha256": sha256_file(resolved),
        }
    )
    return result


def git_identity(repo: Path) -> Dict[str, Any]:
    repo = repo.expanduser().resolve()
    result: Dict[str, Any] = {"requested_path": str(repo), "available": False}
    git = shutil.which("git")
    if git is None:
        result["error"] = "git not found"
        return result
    root, error = run_capture([git, "-C", str(repo), "rev-parse", "--show-toplevel"])
    if error or not root:
        result["error"] = error or "not a Git working tree"
        return result
    root_path = Path(root).resolve()
    head, head_error = run_capture([git, "-C", str(root_path), "rev-parse", "HEAD"])
    branch, _ = run_capture(
        [git, "-C", str(root_path), "symbolic-ref", "--quiet", "--short", "HEAD"]
    )
    describe, _ = run_capture(
        [git, "-C", str(root_path), "describe", "--tags", "--always", "--dirty"]
    )
    status, status_error = run_capture(
        [git, "-C", str(root_path), "status", "--porcelain=v1", "--untracked-files=normal"]
    )
    status_lines = status.splitlines() if status else []
    result.update(
        {
            "available": head is not None,
            "root": str(root_path),
            "head": head,
            "branch": branch,
            "describe": describe,
            "dirty": bool(status_lines) if status_error is None else None,
            "changed_path_count": len(status_lines) if status_error is None else None,
        }
    )
    if head_error or status_error:
        result["error"] = head_error or status_error
    return result


def tool_version(name: str, extra_args: Sequence[str] = ()) -> Optional[str]:
    executable = shutil.which(name)
    if executable is None:
        return None
    stdout, _ = run_capture([executable, "--version", *extra_args])
    return stdout


def collect_build(repo: Path, artifacts: Sequence[Path]) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "git": git_identity(repo),
        "rustc_version": tool_version("rustc"),
        "cargo_version": tool_version("cargo"),
        "artifacts": [file_identity(path) for path in artifacts],
    }
    for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml"):
        path = repo.expanduser().resolve() / name
        if path.is_file():
            result[name] = file_identity(path)
    return result


def warning(severity: str, code: str, message: str) -> Dict[str, str]:
    return {"severity": severity, "code": code, "message": message}


def evaluate_warnings(report: Mapping[str, Any]) -> List[Dict[str, str]]:
    warnings: List[Dict[str, str]] = []
    runtime = report["runtime"]
    cpu = report["cpu"]
    memory = report["memory"]
    load = report["load"]
    power = report["power"]
    pages = report["pages"]
    nvidia = report["nvidia"]
    build = report["build"]

    if runtime["kind"] == "wsl":
        warnings.append(
            warning(
                "info",
                "wsl-runtime",
                "WSL virtualization can change CPU scheduling, memory, and GPU timing; compare only like-for-like hosts.",
            )
        )
    logical_host = cpu.get("logical_host")
    logical_available = cpu.get("logical_available")
    if logical_host and logical_available and logical_available < logical_host:
        warnings.append(
            warning(
                "warning",
                "restricted-cpu-affinity",
                f"Only {logical_available} of {logical_host} logical CPUs are available to this process.",
            )
        )

    normalized = load.get("normalized_by_logical_cpu")
    if normalized and normalized[0] >= 0.25:
        warnings.append(
            warning(
                "warning",
                "host-load",
                f"One-minute load is {normalized[0] * 100:.1f}% of visible logical CPU capacity.",
            )
        )
    total = memory.get("total_bytes")
    available = memory.get("available_bytes")
    if total and available is not None and available / total < 0.20:
        warnings.append(
            warning(
                "warning",
                "low-memory",
                f"Only {available / total * 100:.1f}% of memory is available.",
            )
        )
    swap_total = memory.get("swap_total_bytes")
    swap_used = memory.get("swap_used_bytes")
    if swap_total and swap_used and swap_used / swap_total >= 0.05:
        warnings.append(
            warning(
                "warning",
                "swap-in-use",
                f"Swap/pagefile is {swap_used / swap_total * 100:.1f}% used.",
            )
        )

    governors = power.get("cpu_governors") or []
    if governors and any(governor != "performance" for governor in governors):
        warnings.append(
            warning(
                "info",
                "variable-cpu-governor",
                "CPU governor is not fixed to performance: " + ", ".join(governors),
            )
        )
    preferences = power.get("energy_performance_preferences") or []
    if preferences and any(value not in ("performance", "balance_performance") for value in preferences):
        warnings.append(
            warning(
                "info",
                "cpu-energy-preference",
                "CPU energy preference may reduce clocks: " + ", ".join(preferences),
            )
        )
    battery = str(power.get("battery_status") or "").lower()
    if battery and "battery power" in battery:
        warnings.append(
            warning("warning", "battery-power", "Host appears to be running on battery power.")
        )
    mac_settings = str(power.get("power_settings") or "").lower()
    if re.search(r"\blowpowermode\s+1\b", mac_settings):
        warnings.append(
            warning("warning", "low-power-mode", "macOS Low Power Mode is enabled.")
        )
    windows_scheme = str(power.get("active_power_scheme") or "").lower()
    if "balanced" in windows_scheme or "power saver" in windows_scheme:
        warnings.append(
            warning(
                "info",
                "variable-power-plan",
                "The active Windows power plan may vary CPU clocks: "
                + str(power.get("active_power_scheme")),
            )
        )

    hugetlb = pages.get("hugetlb") or {}
    if hugetlb and hugetlb.get("total_pages") == 0:
        warnings.append(
            warning(
                "info",
                "no-reserved-huge-pages",
                "No HugeTLB pages are reserved; record this consistently for memory-hard CPU comparisons.",
            )
        )
    thp = pages.get("transparent_huge_pages") or {}
    if thp.get("active_mode") == "never":
        warnings.append(
            warning(
                "info",
                "transparent-huge-pages-disabled",
                "Transparent huge pages are disabled.",
            )
        )

    for gpu in nvidia.get("gpus") or []:
        index = gpu.get("index", "?")
        utilization = gpu.get("gpu_utilization_pct")
        used = gpu.get("memory_used_mib")
        total_gpu = gpu.get("memory_total_mib")
        if isinstance(utilization, (int, float)) and utilization >= 5:
            warnings.append(
                warning(
                    "warning",
                    "gpu-in-use",
                    f"NVIDIA GPU {index} is already at {utilization}% utilization.",
                )
            )
        if (
            isinstance(used, (int, float))
            and isinstance(total_gpu, (int, float))
            and total_gpu > 0
            and used / total_gpu >= 0.10
        ):
            warnings.append(
                warning(
                    "warning",
                    "gpu-memory-in-use",
                    f"NVIDIA GPU {index} already has {used} MiB of {total_gpu} MiB allocated.",
                )
            )
    processes = nvidia.get("compute_processes") or []
    if processes:
        warnings.append(
            warning(
                "warning",
                "gpu-compute-processes",
                f"nvidia-smi reports {len(processes)} other compute process(es).",
            )
        )
    git = build.get("git") or {}
    if git.get("dirty"):
        warnings.append(
            warning(
                "info",
                "dirty-worktree",
                f"Git worktree has {git.get('changed_path_count')} changed path(s); the exact HEAD is not the full build identity.",
            )
        )
    return warnings


def collect_preflight(repo: Path, artifacts: Sequence[Path]) -> Dict[str, Any]:
    runtime = detect_runtime()
    cpu = collect_cpu(runtime)
    report: Dict[str, Any] = {
        "schema": PREFLIGHT_SCHEMA,
        "captured_at_utc": utc_now(),
        "runtime": runtime,
        "cpu": cpu,
        "memory": collect_memory(runtime),
        "load": collect_load(cpu),
        "power": collect_power(runtime, cpu),
        "pages": collect_pages(runtime),
        "nvidia": collect_nvidia(),
        "build": collect_build(repo, artifacts),
    }
    report["warnings"] = evaluate_warnings(report)
    return report


def human_bytes(value: Optional[int]) -> str:
    if value is None:
        return "unknown"
    amount = float(value)
    for suffix in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(amount) < 1024 or suffix == "TiB":
            return f"{amount:.1f} {suffix}"
        amount /= 1024
    return f"{amount:.1f} TiB"


def print_preflight_assessment(report: Mapping[str, Any], stream: Any = sys.stderr) -> None:
    runtime = report["runtime"]
    cpu = report["cpu"]
    memory = report["memory"]
    load = report["load"]
    gpu_names = [gpu.get("name") for gpu in report["nvidia"].get("gpus") or []]
    parts = [
        str(runtime["kind"]),
        str(cpu.get("model") or cpu.get("architecture") or "unknown CPU"),
        f"{cpu.get('logical_available') or '?'} logical/{cpu.get('physical_cores') or '?'} physical",
        f"{human_bytes(memory.get('total_bytes'))} RAM",
    ]
    load_1m = load.get("load_average_1m")
    if load_1m is not None:
        parts.append(f"load {load_1m:.2f}")
    if gpu_names:
        parts.append(", ".join(str(name) for name in gpu_names if name))
    print("Preflight: " + " | ".join(parts), file=stream)
    warnings = report.get("warnings") or []
    if not warnings:
        print("Preflight: no obvious benchmark confounders detected", file=stream)
    for item in warnings:
        print(
            f"[{item['severity'].upper()} {item['code']}] {item['message']}",
            file=stream,
        )


@dataclass(frozen=True)
class PairedValue:
    pair: str
    baseline: float
    candidate: float
    baseline_order: Optional[str]
    candidate_order: Optional[str]


def pair_sort_key(value: str) -> Tuple[int, Any]:
    try:
        return (0, int(value))
    except ValueError:
        return (1, value)


def load_paired_results(
    path: Path,
    metric: str,
    baseline_label: str,
    candidate_label: str,
    min_pairs: int,
) -> List[PairedValue]:
    if min_pairs < 2:
        raise BenchctlError("minimum pair count must be at least 2")
    path = path.expanduser().resolve()
    try:
        handle = path.open("r", encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise BenchctlError(f"cannot open results TSV {path}: {exc}") from exc
    with handle:
        reader = csv.DictReader(handle, delimiter="\t")
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise BenchctlError("results TSV has no header")
        if len(fieldnames) != len(set(fieldnames)):
            raise BenchctlError("results TSV contains duplicate header names")
        required = {"variant", "pair", metric}
        missing = sorted(required.difference(fieldnames))
        if missing:
            raise BenchctlError("results TSV is missing column(s): " + ", ".join(missing))
        rows: Dict[str, Dict[str, Tuple[float, Optional[str]]]] = {}
        observed = 0
        for line_number, row in enumerate(reader, start=2):
            if None in row:
                raise BenchctlError(f"line {line_number} has more fields than the header")
            if not any((value or "").strip() for value in row.values()):
                continue
            observed += 1
            variant = (row.get("variant") or "").strip()
            pair = (row.get("pair") or "").strip()
            if variant not in (baseline_label, candidate_label):
                raise BenchctlError(
                    f"line {line_number} has unexpected variant {variant!r}; expected "
                    f"{baseline_label!r} or {candidate_label!r}"
                )
            if not pair:
                raise BenchctlError(f"line {line_number} has an empty pair identifier")
            raw_value = (row.get(metric) or "").strip()
            try:
                value = float(raw_value)
            except ValueError as exc:
                raise BenchctlError(
                    f"line {line_number} has non-numeric {metric}: {raw_value!r}"
                ) from exc
            if not math.isfinite(value) or value <= 0:
                raise BenchctlError(
                    f"line {line_number} requires finite, positive {metric}; got {raw_value!r}"
                )
            pair_rows = rows.setdefault(pair, {})
            if variant in pair_rows:
                raise BenchctlError(f"pair {pair!r} has duplicate {variant!r} rows")
            order = (row.get("order") or "").strip() or None
            pair_rows[variant] = (value, order)
        if not observed:
            raise BenchctlError("results TSV contains no data rows")

    paired: List[PairedValue] = []
    for pair in sorted(rows, key=pair_sort_key):
        pair_rows = rows[pair]
        absent = [
            variant
            for variant in (baseline_label, candidate_label)
            if variant not in pair_rows
        ]
        if absent:
            raise BenchctlError(
                f"pair {pair!r} is unpaired; missing " + ", ".join(repr(value) for value in absent)
            )
        baseline, baseline_order = pair_rows[baseline_label]
        candidate, candidate_order = pair_rows[candidate_label]
        if baseline_order and candidate_order and baseline_order == candidate_order:
            raise BenchctlError(
                f"pair {pair!r} has duplicate order value {baseline_order!r}"
            )
        paired.append(
            PairedValue(pair, baseline, candidate, baseline_order, candidate_order)
        )
    if len(paired) < min_pairs:
        raise BenchctlError(
            f"only {len(paired)} complete pair(s); at least {min_pairs} are required"
        )
    return paired


def arithmetic_mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def sample_cv(values: Sequence[float]) -> float:
    mean = arithmetic_mean(values)
    return statistics.stdev(values) / mean


def interpolated_quantile(sorted_values: Sequence[float], probability: float) -> float:
    if not sorted_values:
        raise BenchctlError("cannot calculate a quantile of no values")
    position = (len(sorted_values) - 1) * probability
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return sorted_values[lower]
    fraction = position - lower
    return sorted_values[lower] * (1 - fraction) + sorted_values[upper] * fraction


def bootstrap_log_ratio_ci(
    log_ratios: Sequence[float], samples: int, seed: int
) -> Tuple[float, float]:
    if samples < 100:
        raise BenchctlError("bootstrap sample count must be at least 100")
    rng = random.Random(seed)
    count = len(log_ratios)
    estimates = []
    for _ in range(samples):
        resampled_mean = sum(log_ratios[rng.randrange(count)] for _ in range(count)) / count
        estimates.append(math.expm1(resampled_mean) * 100)
    estimates.sort()
    return (
        interpolated_quantile(estimates, 0.025),
        interpolated_quantile(estimates, 0.975),
    )


def summarize_pairs(
    paired: Sequence[PairedValue], metric: str, bootstrap_samples: int, seed: int
) -> Dict[str, Any]:
    baseline = [pair.baseline for pair in paired]
    candidate = [pair.candidate for pair in paired]
    log_ratios = [math.log(pair.candidate / pair.baseline) for pair in paired]
    baseline_mean = arithmetic_mean(baseline)
    candidate_mean = arithmetic_mean(candidate)
    log_mean = arithmetic_mean(log_ratios)
    geometric_ratio = math.exp(log_mean)
    geometric_delta = math.expm1(log_mean) * 100
    ci_low, ci_high = bootstrap_log_ratio_ci(log_ratios, bootstrap_samples, seed)
    positive = sum(value > 0 for value in log_ratios)
    negative = sum(value < 0 for value in log_ratios)
    ties = len(log_ratios) - positive - negative
    if positive > negative:
        dominant_sign = "candidate_faster"
    elif negative > positive:
        dominant_sign = "candidate_slower"
    else:
        dominant_sign = "mixed"
    result: Dict[str, Any] = {
        "schema": COMPARISON_SCHEMA,
        "metric": metric,
        "pair_count": len(paired),
        "bootstrap": {
            "method": "paired percentile bootstrap of mean log ratio",
            "confidence_level": 0.95,
            "samples": bootstrap_samples,
            "seed": seed,
        },
        "baseline": {
            "arithmetic_mean": baseline_mean,
            "sample_cv": sample_cv(baseline),
        },
        "candidate": {
            "arithmetic_mean": candidate_mean,
            "sample_cv": sample_cv(candidate),
        },
        "comparison": {
            "arithmetic_mean_delta_pct": (candidate_mean / baseline_mean - 1) * 100,
            "paired_geometric_ratio": geometric_ratio,
            "paired_geometric_delta_pct": geometric_delta,
            "paired_geometric_delta_ci95_pct": [ci_low, ci_high],
            "mean_log_ratio": log_mean,
            "sign_consistency": {
                "candidate_faster_pairs": positive,
                "candidate_slower_pairs": negative,
                "tied_pairs": ties,
                "candidate_faster_fraction": positive / len(paired),
                "dominant_sign": dominant_sign,
                "dominant_fraction": max(positive, negative) / len(paired),
            },
        },
        "pairs": [
            {
                "pair": pair.pair,
                "baseline": pair.baseline,
                "candidate": pair.candidate,
                "ratio": pair.candidate / pair.baseline,
                "delta_pct": (pair.candidate / pair.baseline - 1) * 100,
                "baseline_order": pair.baseline_order,
                "candidate_order": pair.candidate_order,
            }
            for pair in paired
        ],
    }
    return result


def validate_exit_code(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("exit code must be an integer") from exc
    if not 1 <= parsed <= 255:
        raise argparse.ArgumentTypeError("exit code must be between 1 and 255")
    return parsed


def nonnegative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be a finite number >= 0")
    return parsed


def evaluate_gates(
    result: Dict[str, Any],
    max_regression_pct: Optional[float],
    require_improvement_pct: Optional[float],
    gate_statistic: str,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    comparison = result["comparison"]
    estimate = comparison["paired_geometric_delta_pct"]
    ci_low, ci_high = comparison["paired_geometric_delta_ci95_pct"]
    gates: List[Dict[str, Any]] = []
    failed_kind: Optional[str] = None
    if max_regression_pct is not None:
        observed = estimate if gate_statistic == "estimate" else ci_high
        threshold = -max_regression_pct
        passed = observed >= threshold
        gates.append(
            {
                "kind": "regression",
                "statistic": "estimate" if gate_statistic == "estimate" else "ci95_upper",
                "observed_pct": observed,
                "required_at_least_pct": threshold,
                "passed": passed,
            }
        )
        if not passed:
            failed_kind = "regression"
    if require_improvement_pct is not None:
        observed = estimate if gate_statistic == "estimate" else ci_low
        threshold = require_improvement_pct
        passed = observed >= threshold
        gates.append(
            {
                "kind": "improvement",
                "statistic": "estimate" if gate_statistic == "estimate" else "ci95_lower",
                "observed_pct": observed,
                "required_at_least_pct": threshold,
                "passed": passed,
            }
        )
        if not passed and failed_kind is None:
            failed_kind = "improvement"
    result["gates"] = gates
    return gates, failed_kind


def print_comparison(result: Mapping[str, Any]) -> None:
    baseline = result["baseline"]
    candidate = result["candidate"]
    comparison = result["comparison"]
    signs = comparison["sign_consistency"]
    ci_low, ci_high = comparison["paired_geometric_delta_ci95_pct"]
    print(f"Compared {result['pair_count']} paired runs using {result['metric']}")
    print(
        f"  arithmetic means: baseline={baseline['arithmetic_mean']:.6g}, "
        f"candidate={candidate['arithmetic_mean']:.6g} "
        f"({comparison['arithmetic_mean_delta_pct']:+.3f}%)"
    )
    print(
        f"  paired geometric delta: {comparison['paired_geometric_delta_pct']:+.3f}% "
        f"(95% bootstrap CI {ci_low:+.3f}% to {ci_high:+.3f}%)"
    )
    print(
        f"  sample CV: baseline={baseline['sample_cv'] * 100:.2f}%, "
        f"candidate={candidate['sample_cv'] * 100:.2f}%"
    )
    print(
        "  pair signs: "
        f"{signs['candidate_faster_pairs']} faster, "
        f"{signs['candidate_slower_pairs']} slower, {signs['tied_pairs']} tied; "
        f"dominant consistency={signs['dominant_fraction'] * 100:.1f}%"
    )
    for gate in result.get("gates") or []:
        state = "PASS" if gate["passed"] else "FAIL"
        print(
            f"  [{state}] {gate['kind']} gate: {gate['statistic']} "
            f"{gate['observed_pct']:+.3f}% >= {gate['required_at_least_pct']:+.3f}%"
        )


def selected_environment() -> Dict[str, str]:
    names = (
        "CUDA_CACHE_DISABLE",
        "CUDA_CACHE_MAXSIZE",
        "CUDA_CACHE_PATH",
        "CUDA_VISIBLE_DEVICES",
        "OMP_NUM_THREADS",
        "RAYON_NUM_THREADS",
        "RUSTFLAGS",
        "SEINE_DATA_DIR",
    )
    return {name: os.environ[name] for name in names if name in os.environ}


def execute_wrapped(
    command: Sequence[str],
    cwd: Path,
    output_dir: Path,
    manifest_path: Path,
    label: Optional[str],
    artifacts: Sequence[Path],
) -> int:
    if not command:
        raise BenchctlError("run requires a command after --")
    cwd = cwd.expanduser().resolve()
    if not cwd.is_dir():
        raise BenchctlError(f"run working directory is not a directory: {cwd}")
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    preflight = collect_preflight(cwd, artifacts)
    preflight_path = output_dir / "preflight.json"
    atomic_write_json(preflight_path, preflight)
    print_preflight_assessment(preflight)

    argv = list(command)
    if argv and argv[0] == "--":
        argv = argv[1:]
    if not argv:
        raise BenchctlError("run requires a command after --")
    resolved_executable = shutil.which(argv[0])
    started_at = utc_now()
    started_monotonic = time.monotonic()
    manifest: Dict[str, Any] = {
        "schema": RUN_SCHEMA,
        "label": label,
        "status": "running",
        "argv": argv,
        "cwd": str(cwd),
        "resolved_executable": resolved_executable,
        "selected_environment": selected_environment(),
        "started_at_utc": started_at,
        "ended_at_utc": None,
        "duration_seconds": None,
        "exit_code": None,
        "preflight_path": str(preflight_path),
        "preflight": preflight,
    }
    atomic_write_json(manifest_path, manifest)
    print("Run: " + json.dumps(argv), file=sys.stderr)
    try:
        completed = subprocess.run(argv, cwd=str(cwd), check=False, shell=False)
        exit_code = completed.returncode
        manifest["status"] = "completed" if exit_code == 0 else "failed"
    except KeyboardInterrupt:
        exit_code = 130
        manifest["status"] = "interrupted"
    except OSError as exc:
        exit_code = 127
        manifest["status"] = "launch_error"
        manifest["launch_error"] = str(exc)
    manifest["ended_at_utc"] = utc_now()
    manifest["duration_seconds"] = time.monotonic() - started_monotonic
    manifest["exit_code"] = exit_code
    atomic_write_json(manifest_path, manifest)
    print(
        f"Run: {manifest['status']} with exit code {exit_code} in "
        f"{manifest['duration_seconds']:.3f}s; manifest={manifest_path}",
        file=sys.stderr,
    )
    return exit_code


def add_artifact_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--artifact",
        action="append",
        default=[],
        type=Path,
        help="executable/build artifact to hash (repeatable)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture Seine benchmark hosts, compare paired A/B results, and wrap runs."
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    preflight = subparsers.add_parser(
        "preflight", help="capture benchmark-host state as JSON"
    )
    preflight.add_argument(
        "--repo", type=Path, default=Path.cwd(), help="repository used for Git/build identity"
    )
    preflight.add_argument("--output", type=Path, help="also write JSON atomically to this path")
    add_artifact_arguments(preflight)

    compare = subparsers.add_parser(
        "compare", help="analyze paired baseline/candidate rows in results.tsv"
    )
    compare.add_argument("results", type=Path, help="results.tsv from a Seine A/B script")
    compare.add_argument("--metric", default="avg_hps", help="positive numeric TSV column")
    compare.add_argument("--baseline-label", default="baseline")
    compare.add_argument("--candidate-label", default="candidate")
    compare.add_argument("--min-pairs", type=int, default=3)
    compare.add_argument("--bootstrap-samples", type=int, default=DEFAULT_BOOTSTRAP_SAMPLES)
    compare.add_argument("--seed", type=int, default=DEFAULT_BOOTSTRAP_SEED)
    compare.add_argument("--json-output", type=Path, help="write full comparison JSON")
    compare.add_argument(
        "--max-regression-pct",
        type=nonnegative_float,
        help="fail if the selected statistic is below the negative of this percentage",
    )
    compare.add_argument(
        "--require-improvement-pct",
        type=nonnegative_float,
        help="fail unless the selected statistic reaches this percentage",
    )
    compare.add_argument(
        "--gate-statistic",
        choices=("confidence", "estimate"),
        default="confidence",
        help="confidence uses CI upper for regression and CI lower for improvement (default)",
    )
    compare.add_argument("--regression-exit-code", type=validate_exit_code, default=10)
    compare.add_argument("--improvement-exit-code", type=validate_exit_code, default=11)

    run = subparsers.add_parser(
        "run", help="record preflight and timing around an argv without a shell"
    )
    run.add_argument("--cwd", type=Path, default=Path.cwd())
    run.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="directory for preflight.json and the default manifest",
    )
    run.add_argument("--manifest", type=Path, help="manifest path (default: OUTPUT_DIR/manifest.json)")
    run.add_argument("--label", help="optional experiment label")
    add_artifact_arguments(run)
    run.add_argument("command", nargs=argparse.REMAINDER, help="command argv after --")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.subcommand == "preflight":
            report = collect_preflight(args.repo, args.artifact)
            if args.output:
                atomic_write_json(args.output, report)
            print_preflight_assessment(report)
            json.dump(report, sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
            return 0
        if args.subcommand == "compare":
            if args.baseline_label == args.candidate_label:
                raise BenchctlError("baseline and candidate labels must differ")
            paired = load_paired_results(
                args.results,
                args.metric,
                args.baseline_label,
                args.candidate_label,
                args.min_pairs,
            )
            result = summarize_pairs(paired, args.metric, args.bootstrap_samples, args.seed)
            result["source"] = str(args.results.expanduser().resolve())
            _, failed_kind = evaluate_gates(
                result,
                args.max_regression_pct,
                args.require_improvement_pct,
                args.gate_statistic,
            )
            if args.json_output:
                atomic_write_json(args.json_output, result)
            print_comparison(result)
            if failed_kind == "regression":
                return args.regression_exit_code
            if failed_kind == "improvement":
                return args.improvement_exit_code
            return 0
        if args.subcommand == "run":
            manifest = args.manifest or (args.output_dir / "manifest.json")
            return execute_wrapped(
                args.command,
                args.cwd,
                args.output_dir,
                manifest,
                args.label,
                args.artifact,
            )
    except BenchctlError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    parser.error(f"unknown command: {args.subcommand}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
