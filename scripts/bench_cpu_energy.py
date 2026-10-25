#!/usr/bin/env python3
"""Read-only CPU package energy capture for a complete Seine benchmark invocation.

This measures package joules per reported completed hash, including startup,
allocation, fences and teardown. It does not claim steady-state or wall-plug
energy. No counters, power limits, governors or page pools are ever written.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from benchctl import PairedValue, summarize_pairs, DEFAULT_BOOTSTRAP_SAMPLES, DEFAULT_BOOTSTRAP_SEED

GIB = 1024**3
ERROR_COUNTERS = (
    "dropped_events", "assignment_enqueue_timeouts", "assignment_execution_timeouts",
    "control_enqueue_timeouts", "control_execution_timeouts", "memory_allocation_failures",
)


class EnergyError(Exception):
    pass


def read_int(path: Path) -> int:
    return int(path.read_text().strip())


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024**2), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def discover(root: Path) -> list[dict]:
    """Traverse only power-zone names; never recurse through sysfs back-links."""
    pending = [(root, 0)]
    visited: set[Path] = set()
    zones = []
    while pending:
        path, depth = pending.pop()
        path = path.resolve()
        if path in visited or not path.is_dir():
            continue
        visited.add(path)
        if (path / "energy_uj").exists():
            zones.append({"path": str(path), "name": (path / "name").read_text().strip(),
                          "max_energy_range_uj": read_int(path / "max_energy_range_uj")})
        if depth < 4:
            pending.extend((child, depth + 1) for child in path.iterdir()
                           if child.name.startswith(("intel-rapl", "amd-rapl")))
    return sorted(zones, key=lambda zone: zone["path"])


def select_packages(zones: list[dict], selected: list[Path] | None) -> list[dict]:
    wanted = {str(path.resolve()) for path in selected} if selected else None
    packages = [zone for zone in zones if (zone["path"] in wanted if wanted is not None
                                          else zone["name"].startswith("package-"))]
    if wanted is not None and {zone["path"] for zone in packages} != wanted:
        raise EnergyError("a requested zone is unavailable")
    if not packages or any(not zone["name"].startswith("package-") for zone in packages):
        raise EnergyError("no readable CPU package energy counters; no benchmark was started")
    if len({zone["name"] for zone in packages}) != len(packages):
        raise EnergyError("duplicate package names from multiple drivers; select one source with --zone")
    available_names = {zone["name"] for zone in zones if zone["name"].startswith("package-")}
    if {zone["name"] for zone in packages} != available_names:
        raise EnergyError("select one counter source for every available CPU package")
    paths = [Path(zone["path"]) for zone in packages]
    if any(a in b.parents for a in paths for b in paths if a != b):
        raise EnergyError("selected energy domains overlap")
    # Only package domains are summed; core/uncore children would double count.
    return packages


def snapshot(zones: list[dict]) -> dict:
    start = time.monotonic()
    energy = {}
    for zone in zones:
        path = Path(zone["path"])
        if read_int(path / "max_energy_range_uj") != zone["max_energy_range_uj"]:
            raise EnergyError("counter range changed during measurement")
        energy[zone["path"]] = read_int(path / "energy_uj")
    end = time.monotonic()
    return {"monotonic": (start + end) / 2, "read_span_secs": end - start, "energy_uj": energy}


def interval_joules(before: dict, after: dict, zones: list[dict], max_watts: float) -> dict[str, float]:
    elapsed = after["monotonic"] - before["monotonic"]
    if not math.isfinite(elapsed) or elapsed <= 0 or not math.isfinite(max_watts) or max_watts <= 0:
        raise EnergyError("invalid counter interval or power bound")
    result = {}
    # A conservative elapsed bound includes sequential sysfs-read uncertainty.
    upper_elapsed = elapsed + before["read_span_secs"] + after["read_span_secs"]
    for zone in zones:
        path, modulus = zone["path"], zone["max_energy_range_uj"]
        old, new = before["energy_uj"][path], after["energy_uj"][path]
        if modulus <= 0 or not (0 <= old < modulus and 0 <= new < modulus):
            raise EnergyError("energy counter outside its advertised range")
        bound = max_watts * upper_elapsed * 1e6
        if bound >= modulus:
            raise EnergyError("sample gap permits multiple wraps at the declared power bound")
        delta = (new - old) % modulus
        if delta > bound:
            raise EnergyError("counter reset or declared maximum domain power exceeded")
        result[path] = delta / 1e6
    return result


def cpu_context(zones: list[dict]) -> dict:
    vmstat = {}
    for line in Path("/proc/vmstat").read_text().splitlines():
        key, value = line.split()
        if key in ("pswpin", "pswpout", "oom_kill"):
            vmstat[key] = int(value)
    policies = {}
    for path in Path("/sys/devices/system/cpu/cpufreq").glob("policy*"):
        policies[path.name] = {name: (path / name).read_text().strip()
                              for name in ("scaling_governor", "energy_performance_preference",
                                           "scaling_min_freq", "scaling_max_freq") if (path / name).exists()}
    constraints = {zone["path"]: {p.name: p.read_text().strip()
                                  for p in Path(zone["path"]).glob("constraint_*") if p.is_file()}
                   for zone in zones}
    return {"vmstat": vmstat, "frequency_policies": policies, "power_constraints": constraints,
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "allowed_cpus": sorted(os.sched_getaffinity(0)),
            "cpuinfo": Path("/proc/cpuinfo").read_text().split("\n\n", 1)[0]}


def check_capacity(threads: int, page_mode: str) -> None:
    info = {line.split(":")[0]: int(line.split()[1]) * 1024
            for line in Path("/proc/meminfo").read_text().splitlines() if ":" in line}
    available = info["MemAvailable"]
    cgroup = next((line.split(":", 2)[2] for line in Path("/proc/self/cgroup").read_text().splitlines()
                   if line.startswith("0::")), None)
    if cgroup:
        root = Path("/sys/fs/cgroup")
        group = root / cgroup.lstrip("/")
        for parent in (group, *group.parents):
            if parent == root.parent:
                break
            current = read_int(parent / "memory.current") if (parent / "memory.current").exists() else 0
            for name in ("memory.high", "memory.max"):
                limit = parent / name
                if limit.exists() and limit.read_text().strip() != "max":
                    available = min(available, max(0, read_int(limit) - current))
    if page_mode == "regular":
        needed = threads * 2 * GIB + GIB
    else:
        needed = GIB
        kib = 1048576 if page_mode == "large-1g" else 2048
        pool = Path(f"/sys/kernel/mm/hugepages/hugepages-{kib}kB")
        free = max(0, read_int(pool / "free_hugepages") - read_int(pool / "resv_hugepages"))
        if free * kib * 1024 < threads * 2 * GIB:
            raise EnergyError("insufficient unreserved huge pages; no settings were changed")
    if available < needed:
        raise EnergyError(f"need {needed / GIB:g} GiB free under host/cgroup limits; have {available / GIB:.2f}")


def validate_report(report: dict, threads: int, page_mode: str, rounds: int, seconds: int, kernel: str) -> int:
    def require(condition: bool, message: str) -> None:
        if not condition:
            raise EnergyError(message)
    require(report["schema_version"] >= 14, "benchmark report too old")
    require(report["bench_kind"] == "end_to_end" and len(report["backends"]) == 1,
            "wrong benchmark scope")
    require(report["warmup_rounds"] == 0, "unreported warmup hashes make energy attribution ambiguous")
    require(report["cpu_threads"] == threads and report["rounds"] == rounds and len(report["runs"]) == rounds,
            "worker count or round count mismatch")
    require(report["bench_secs"] == seconds, "benchmark duration mismatch")
    config = report["config_fingerprint"]
    require(config["cpu_page_mode"] == page_mode and config["cpu_autotune_threads"] is False,
            "page policy mismatch or autotuning enabled")
    require(config["cpu_kernel"] is not None, "missing selected kernel identity")
    if kernel != "auto":
        require(config["cpu_kernel"] == kernel and config["cpu_kernel_requested"] == kernel, "kernel mismatch")
    backing = {"regular": "memory_regular", "large": "memory_explicit_large",
               "large-1g": "memory_explicit_large_1g"}[page_mode]
    total = 0
    for run in report["runs"]:
        require(type(run["hashes"]) is int and run["hashes"] > 0, "missing completed hashes")
        require(run["hashes"] == run["counted_hashes"] + run["late_hashes"], "hash accounting mismatch")
        require(len(run["backend_runs"]) == 1, "unexpected backend count")
        backend = run["backend_runs"][0]
        require(backend["backend"] == "cpu" and backend["hashes"] == run["hashes"], "backend/hash mismatch")
        require(report["backends"] == [f"cpu#{backend['backend_id']}"], "CPU instance identity mismatch")
        require(all(backend[name] == 0 for name in ERROR_COUNTERS), "backend error counters are nonzero")
        require(backend[backing + "_workers"] == threads and backend[backing + "_bytes"] == threads * 2 * GIB,
                "actual arena backing differs from requested backing")
        total += run["hashes"]
    require(report["total_hashes"] == total, "aggregate completed hashes mismatch")
    return total


def run(args: argparse.Namespace, zones: list[dict]) -> dict:
    packages = select_packages(zones, args.zone)
    binary = args.binary.expanduser().resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise EnergyError("binary is not executable")
    check_capacity(args.threads, args.page_mode)
    output = args.output_dir.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=False)
    command = [str(binary), "--bench", "--bench-kind", "end-to-end", "--backend", "cpu",
               "--threads", str(args.threads), "--disable-cpu-autotune-threads",
               "--cpu-page-mode", args.page_mode, "--bench-secs", str(args.seconds),
               "--bench-rounds", str(args.rounds), "--bench-warmup-rounds", "0",
               "--bench-output", str(output / "bench.json"), "--ui", "plain"]
    if args.kernel != "auto":
        command.extend(["--bench-cpu-kernel", args.kernel])
    record = {"schema": 1, "status": "running", "scope": "whole_invocation_package_energy",
              "includes": ["startup", "allocation", "reported_work", "fences", "teardown"],
              "excludes": ["unmeasured_DRAM", "wall_plug_energy"], "label": args.label,
              "command": command, "binary_sha256": sha256(binary), "domains": packages,
              "max_domain_watts_assumption": args.max_domain_watts,
              "context_before": cpu_context(packages), "samples": []}
    proc = None
    totals = {zone["path"]: 0.0 for zone in packages}
    try:
        before = snapshot(packages)
        record["samples"].append(before)
        # Fail before launching if the requested cadence cannot bound wraps.
        probe = dict(before, monotonic=before["monotonic"] + args.interval)
        interval_joules(before, probe, packages, args.max_domain_watts)
        write_json(output / "energy.json", record)
        with (output / "stdout.log").open("w") as stdout, (output / "stderr.log").open("w") as stderr:
            proc = subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True)
            deadline = time.monotonic() + args.timeout
            while True:
                if time.monotonic() >= deadline:
                    raise EnergyError("benchmark exceeded its timeout")
                try:
                    code = proc.wait(timeout=args.interval)
                except subprocess.TimeoutExpired:
                    code = None
                after = snapshot(packages)
                for path, joules in interval_joules(before, after, packages, args.max_domain_watts).items():
                    totals[path] += joules
                record["samples"].append(after)
                before = after
                if code is not None:
                    record["exit_code"] = code
                    if code != 0:
                        raise EnergyError(f"benchmark failed with exit code {code}")
                    break
        record["context_after"] = cpu_context(packages)
        pre, post = record["context_before"], record["context_after"]
        if any(post["vmstat"][key] != pre["vmstat"][key] for key in ("pswpin", "pswpout", "oom_kill")):
            raise EnergyError("swap/OOM counters changed; measurement rejected")
        for key in ("frequency_policies", "power_constraints", "allowed_cpus", "cpuinfo"):
            # cpuinfo includes dynamic MHz on some hosts; retain it as context
            # instead of mistaking normal frequency scaling for a policy change.
            if key != "cpuinfo" and pre[key] != post[key]:
                raise EnergyError(f"{key} changed during measurement")
        if sha256(binary) != record["binary_sha256"]:
            raise EnergyError("benchmark binary changed")
        report = json.loads((output / "bench.json").read_text())
        hashes = validate_report(report, args.threads, args.page_mode, args.rounds, args.seconds, args.kernel)
        if any(value <= 0 for value in totals.values()):
            raise EnergyError("a package energy counter did not advance")
        elapsed = record["samples"][-1]["monotonic"] - record["samples"][0]["monotonic"]
        joules = sum(totals.values())
        record.update(status="complete", completed_hashes=hashes, invocation_secs=elapsed,
                      package_joules=totals, total_package_joules=joules,
                      invocation_joules_per_completed_hash=joules / hashes,
                      invocation_completed_hashes_per_second=hashes / elapsed,
                      average_package_watts=joules / elapsed,
                      benchmark_sha256=sha256(output / "bench.json"))
        return record
    except BaseException as exc:
        record.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        if proc is not None and proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait(timeout=5)
        write_json(output / "energy.json", record)


def load_measurement(path: Path) -> tuple[dict, dict]:
    """Recompute energy from raw samples and hashes from the checksummed report."""
    path = path / "energy.json" if path.is_dir() else path
    record = json.loads(path.read_text())
    if record["status"] != "complete" or record["scope"] != "whole_invocation_package_energy":
        raise EnergyError("comparison requires complete invocation measurements")
    report_path = path.parent / "bench.json"
    if sha256(report_path) != record["benchmark_sha256"]:
        raise EnergyError("benchmark report checksum mismatch")
    report = json.loads(report_path.read_text())
    hashes = validate_report(report, report["cpu_threads"], report["config_fingerprint"]["cpu_page_mode"],
                             report["rounds"], report["bench_secs"],
                             report["config_fingerprint"]["cpu_kernel_requested"] or "auto")
    if len(record["samples"]) < 2:
        raise EnergyError("missing raw energy samples")
    zones = select_packages(record["domains"], None)
    joules = 0.0
    for before, after in zip(record["samples"], record["samples"][1:]):
        joules += sum(interval_joules(before, after, zones, record["max_domain_watts_assumption"]).values())
    elapsed = record["samples"][-1]["monotonic"] - record["samples"][0]["monotonic"]
    if joules <= 0 or elapsed <= 0 or hashes != record["completed_hashes"]:
        raise EnergyError("invalid raw energy or hash accounting")
    expected = {"total_package_joules": joules, "invocation_secs": elapsed,
                "invocation_joules_per_completed_hash": joules / hashes,
                "invocation_completed_hashes_per_second": hashes / elapsed,
                "average_package_watts": joules / elapsed}
    if any(not math.isclose(record[key], value, rel_tol=1e-9) for key, value in expected.items()):
        raise EnergyError("derived energy metrics do not match raw observations")
    return record, report


def compare(baseline: list[Path], candidate: list[Path]) -> dict:
    if len(baseline) < 3 or len(baseline) != len(candidate):
        raise EnergyError("provide at least three complete paired measurements")
    paths = [p.resolve() / "energy.json" if p.is_dir() else p.resolve() for p in baseline + candidate]
    if len(set(paths)) != len(paths):
        raise EnergyError("a measurement cannot be reused in a comparison")
    metrics = ("invocation_joules_per_completed_hash", "invocation_completed_hashes_per_second",
               "average_package_watts")
    paired = {key: [] for key in metrics}
    observations = []
    settings = {"baseline": [], "candidate": []}
    fixed_context = None
    for index, (a_path, b_path) in enumerate(zip(baseline, candidate), 1):
        a, a_report = load_measurement(a_path)
        b, b_report = load_measurement(b_path)
        for label, record, report in (("baseline", a, a_report), ("candidate", b, b_report)):
            common = {key: record[key] for key in ("binary_sha256", "domains", "max_domain_watts_assumption")}
            common.update({key: record["context_before"][key] for key in ("boot_id", "allowed_cpus")})
            common.update({key: report[key] for key in ("pow_fingerprint", "bench_secs", "rounds")})
            common.update({key: report["config_fingerprint"][key]
                           for key in ("cpu_page_mode", "cpu_affinity", "cpu_kernel")})
            common["cpu_identity"] = report["environment"]["cpu_identity"]
            if fixed_context is None:
                fixed_context = common
            for key, value in common.items():
                if value != fixed_context[key]:
                    raise EnergyError(f"keep {key} fixed across all worker/power comparison pairs")
            settings[label].append({"threads": report["cpu_threads"],
                                    "boot_id": record["context_before"]["boot_id"],
                                    "frequency_policies": record["context_before"]["frequency_policies"],
                                    "power_constraints": record["context_before"]["power_constraints"],
                                    "allowed_cpus": record["context_before"]["allowed_cpus"]})
        for key in metrics:
            paired[key].append(PairedValue(str(index), a[key], b[key], None, None))
        observations.append({"pair": index, "baseline": str(a_path), "candidate": str(b_path)})
    for label, values in settings.items():
        if any(item != values[0] for item in values):
            raise EnergyError(f"{label} settings changed between pairs")
    summaries = {}
    for key, values in paired.items():
        summary = summarize_pairs(values, key, DEFAULT_BOOTSTRAP_SAMPLES, DEFAULT_BOOTSTRAP_SEED)
        signs = summary["comparison"]["sign_consistency"]
        # benchctl labels generic ratios as faster/slower. For joules and watts,
        # direction is numerical; a smaller value must not be called slower.
        signs["candidate_higher_pairs"] = signs.pop("candidate_faster_pairs")
        signs["candidate_lower_pairs"] = signs.pop("candidate_slower_pairs")
        signs["candidate_higher_fraction"] = signs.pop("candidate_faster_fraction")
        signs["dominant_sign"] = {"candidate_faster": "candidate_higher", "candidate_slower": "candidate_lower",
                                  "mixed": "mixed"}[signs["dominant_sign"]]
        summaries[key] = summary
    return {"schema": 1, "scope": "whole_invocation_package_energy",
            "settings": {key: values[0] for key, values in settings.items()}, "pairs": observations,
            "metrics": summaries,
            "interpretation": "Lower joules/hash is better. Package energy includes other processes; excludes unmeasured DRAM and wall-plug losses. Pair order must be controlled by the experiment runner."}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--counter-root", type=Path, default=Path("/sys/class/powercap"))
    commands = result.add_subparsers(dest="action", required=True)
    commands.add_parser("probe", help="list available domains without starting a workload")
    measure = commands.add_parser("run")
    measure.add_argument("--binary", type=Path, required=True)
    measure.add_argument("--output-dir", type=Path, required=True)
    measure.add_argument("--threads", type=int, required=True)
    measure.add_argument("--page-mode", choices=("regular", "large", "large-1g"), default="regular")
    measure.add_argument("--kernel", choices=("auto", "scalar", "avx2", "avx512", "avx512-256", "neon"), default="auto")
    measure.add_argument("--seconds", type=int, default=60)
    measure.add_argument("--rounds", type=int, default=3)
    measure.add_argument("--interval", type=float, default=0.25)
    measure.add_argument("--timeout", type=float, default=600)
    measure.add_argument("--max-domain-watts", type=float, default=1000,
                         help="conservative physical upper bound per package; bounds counter wraps")
    measure.add_argument("--label", default="current-settings")
    measure.add_argument("--zone", type=Path, action="append", help="select one source for each package when multiple drivers expose it")
    comparison = commands.add_parser("compare", help="compare three or more paired worker/power measurements")
    comparison.add_argument("--baseline", type=Path, nargs="+", required=True)
    comparison.add_argument("--candidate", type=Path, nargs="+", required=True)
    comparison.add_argument("--output", type=Path)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.action == "probe":
            zones = discover(args.counter_root)
            print(json.dumps({"schema": 1, "domains": zones,
                              "package_counters_available": any(z["name"].startswith("package-") for z in zones)}, indent=2))
        elif args.action == "compare":
            result = compare(args.baseline, args.candidate)
            if args.output:
                write_json(args.output, result)
            print(json.dumps(result, indent=2))
        else:
            if args.threads < 1 or args.seconds < 1 or args.rounds < 1:
                raise EnergyError("threads, seconds and rounds must be positive")
            if not all(math.isfinite(x) and x > 0 for x in (args.interval, args.timeout, args.max_domain_watts)):
                raise EnergyError("interval, timeout and power bound must be finite and positive")
            if args.interval > 5:
                raise EnergyError("counter interval must be at most five seconds")
            def stop(signum, _frame):
                raise KeyboardInterrupt(f"signal {signum}")
            previous = {signum: signal.signal(signum, stop) for signum in (signal.SIGINT, signal.SIGTERM)}
            try:
                record = run(args, discover(args.counter_root))
            finally:
                for signum, handler in previous.items():
                    signal.signal(signum, handler)
            print(json.dumps({key: record[key] for key in ("status", "completed_hashes",
                              "invocation_joules_per_completed_hash", "average_package_watts")}, indent=2))
        return 0
    except KeyboardInterrupt:
        print("CPU energy capture interrupted; owned benchmark stopped", file=sys.stderr)
        return 130
    except (EnergyError, OSError, ValueError, KeyError) as exc:
        print(f"CPU energy capture: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
