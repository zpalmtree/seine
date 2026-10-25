#!/usr/bin/env python3
"""Full-size native correctness followed by bounded, alternating CPU screening.

Raw schema-14 reports are authoritative. Rates include round-end fences and
completed late hashes. Screening never promotes a kernel into production.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "results"


def save(path, obj):
    path.write_text(json.dumps(obj, indent=2) + "\n")


def run(command, logfile, timeout=300):
    with logfile.open("w") as stream:
        subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=timeout)


def counters():
    vm = dict(line.split() for line in Path("/proc/vmstat").read_text().splitlines())
    cpu = [int(v) for v in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
    return {k: int(vm[k]) for k in ("pswpin", "pswpout", "oom_kill")} | {
        "cpu_total": sum(cpu[:8]), "cpu_steal": cpu[7]}


def physical_first():
    rows = subprocess.check_output(["lscpu", "-p=CPU,CORE,SOCKET"], text=True)
    allowed = os.sched_getaffinity(0)
    groups = {}
    for row in rows.splitlines():
        if row.startswith("#"):
            continue
        cpu, core, socket = map(int, row.split(","))
        if cpu in allowed:
            groups.setdefault((socket, core), []).append(cpu)
    result = []
    for sibling in range(max(map(len, groups.values()))):
        for group in groups.values():
            if sibling < len(group):
                result.append(group[sibling])
    assert set(result) == allowed and len(set(result)) == len(result)
    return result


def validate(report, workers, kernel, mode):
    assert report["schema_version"] == 14 and report["cpu_threads"] == workers
    fingerprint = report["config_fingerprint"]
    assert fingerprint["cpu_kernel"] == kernel and not fingerprint["cpu_autotune_threads"]
    for sample in report["runs"]:
        for b in sample["backend_runs"]:
            for field in ("dropped_events", "memory_allocation_failures", "assignment_enqueue_timeouts",
                          "assignment_execution_timeouts", "control_enqueue_timeouts", "control_execution_timeouts"):
                assert b[field] == 0, (field, b[field])
            field = "explicit_large" if mode == "large" else "regular"
            assert b[f"memory_{field}_workers"] == workers
            assert b[f"memory_{field}_bytes"] == workers * 2 * 1024**3
    return sum(r["hashes"] for r in report["runs"]) / sum(r["actual_elapsed_secs"] for r in report["runs"])


def main():
    OUT.mkdir(exist_ok=True)
    assert len(Path("/proc/swaps").read_text().splitlines()) == 1, "dedicated guest must have no swap"
    for name in ("cpuinfo", "meminfo"):
        (OUT / (name + ".txt")).write_text(Path("/proc", name).read_text())
    run(["lscpu", "--json"], OUT / "lscpu.json")
    run(["lscpu", "-e=CPU,NODE,SOCKET,CORE,ONLINE"], OUT / "topology.txt")
    run(["uname", "-a"], OUT / "uname.txt")
    cpus = physical_first()
    arm = platform.machine() == "aarch64"
    identities = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT / "bin").iterdir()}
    save(OUT / "identities.json", identities)
    for verifier in sorted((ROOT / "bin").glob("verify-*")):
        run([str(verifier)], OUT / (verifier.name + ".log"), timeout=600)
    supported = set(re.findall(r"^(\S+): PASS", (OUT / "verify-generic.log").read_text(), re.MULTILINE))
    variants = [("generic", "neon")] if arm else [("generic", k) for k in ("avx2", "avx512", "avx512-256") if k in supported]
    assert variants and all(kernel in supported for _, kernel in variants)
    if arm and (ROOT / "bin/seine-sha3").exists():
        variants.append(("sha3", "neon"))
    print("Full-size reference parity passed for all compiled variants", flush=True)
    old_pool = int(Path("/proc/sys/vm/nr_hugepages").read_text())
    assert old_pool == 0, "dedicated guest must not have an existing HugeTLB reservation"
    memory = {line.split(':')[0]: int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()}
    assert memory['Hugepagesize'] == 2048, "this harness requires an explicit 2 MiB HugeTLB pool"
    memory_cap = (memory['MemAvailable'] * 1024 - 5 * 1024**3) // (2 * 1024**3)
    maximum = min(16 if arm else 12, len(cpus), memory_cap)
    assert maximum >= 1, "insufficient memory for one arena plus headroom"
    assert memory['MemAvailable'] * 1024 >= maximum * 2 * 1024**3 + 5 * 1024**3, "insufficient bounded-memory headroom"
    configs = [("regular", 1)] + [("large", count) for count in sorted({1, min(4, maximum), min(8, maximum), maximum})]
    records = []
    start = time.monotonic()
    try:
        for repeat in range(2):
            ordered = configs if repeat == 0 else list(reversed(configs))
            for mode, workers in ordered:
                index = configs.index((mode, workers))
                pages = maximum * 1024 if mode == "large" else 0
                run(["sudo", "sysctl", "-w", f"vm.nr_hugepages={pages}"], OUT / "pool-last.log")
                assert int(Path("/proc/sys/vm/nr_hugepages").read_text()) == pages
                order = variants[index % len(variants):] + variants[:index % len(variants)]
                if repeat:
                    order = list(reversed(order))
                for binary, kernel in order:
                    assert time.monotonic() - start < 3600, "screening deadline"
                    assert workers <= len(cpus)
                    label = f"r{repeat+1}-{mode}-w{workers}-{binary}-{kernel}"
                    command = ["taskset", "-c", ",".join(map(str, cpus[:workers])), str(ROOT / f"bin/seine-{binary}"),
                               "--bench", "--backend", "cpu", "--threads", str(workers),
                               "--disable-cpu-autotune-threads", "--bench-kind", "backend", "--cpu-affinity", "os",
                               "--cpu-page-mode", mode, "--bench-cpu-kernel", kernel, "--bench-secs", "12",
                               "--bench-rounds", "2", "--bench-warmup-rounds", "1", "--ui", "plain",
                               "--data-dir", str(ROOT / "config"), "--bench-output", str(OUT / (label + ".json"))]
                    before = counters()
                    run(command, OUT / (label + ".log"))
                    after = counters()
                    delta = {k: after[k] - before[k] for k in before}
                    assert all(delta[k] == 0 for k in ("pswpin", "pswpout", "oom_kill")), delta
                    steal = 100 * delta["cpu_steal"] / max(1, delta["cpu_total"])
                    assert steal < 2, f"host steal {steal}%"
                    report = json.loads((OUT / (label + ".json")).read_text())
                    rate = validate(report, workers, kernel, mode)
                    records.append({"label": label, "repeat": repeat+1, "mode": mode, "workers": workers,
                                    "binary": binary, "kernel": kernel, "cpus": cpus[:workers],
                                    "completed_hps": rate, "vm_delta": delta, "steal_pct": steal})
                    save(OUT / "screen.json", records)
                    print(datetime.datetime.now(datetime.timezone.utc).isoformat(), label, f"{rate:.5f} H/s", flush=True)
                    time.sleep(2)
    finally:
        run(["sudo", "sysctl", "-w", f"vm.nr_hugepages={old_pool}"], OUT / "release-pages.log")
        save(OUT / "screen.json", records)
    assert identities == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT / "bin").iterdir()}
    for mode, workers in configs:
        passes = [[(r['binary'], r['kernel']) for r in records
                   if r['mode'] == mode and r['workers'] == workers and r['repeat'] == repeat]
                  for repeat in (1, 2)]
        assert len(passes[0]) == len(variants) and passes[1] == list(reversed(passes[0]))
    save(OUT / 'screen-order-verification.json', {'per_configuration_order_reversed': True,
        'source': 'actual completed record sequence', 'passes': 2})
    (OUT / "SCREEN-COMPLETE").write_text("Screening only; no production promotion.\n")


if __name__ == "__main__":
    main()
