#!/usr/bin/env python3
"""Run the time-boxed interhash experiment on a dedicated Linux guest.

Requires the separately feature-gated interhash_experiment binary. Three
alternating passes compare equal 4 GiB arena budgets. Separate-core and SMT
controls explicitly identify their CPU allocations; never divide pair H/s by
two when comparing completed throughput.
"""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import time

from screen import counters, run, save


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--kernel", required=True)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=False)
    groups = {}
    for row in subprocess.check_output(["lscpu", "-p=CPU,CORE,SOCKET"], text=True).splitlines():
        if row.startswith("#"):
            continue
        cpu, core, socket = map(int, row.split(","))
        groups.setdefault((socket, core), []).append(cpu)
    groups = list(groups.values())
    core0, core1 = groups[0][0], groups[1][0]
    cases = [("sequential", core0, core0), ("pair1", core0, core0),
             ("pair4", core0, core0), ("pair16", core0, core0),
             ("workers", core0, core0), ("workers", core0, core1)]
    if len(groups[0]) > 1:
        cases.append(("workers", core0, groups[0][1]))
    save(a.out / "cases.json", {"cases": cases, "physical_core_groups": groups,
                              "arena_bytes": 4 * 1024**3, "reference_t_cost": 1, "reference_lanes": 1})
    pool = Path("/proc/sys/vm/nr_hugepages")
    old = int(pool.read_text())
    assert old == 0, "previous experiments must release their page pool"
    memory = {line.split(':')[0]: int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()}
    assert memory['Hugepagesize'] == 2048, "this runner requires 2 MiB HugeTLB pages"
    assert memory['MemAvailable'] * 1024 >= 8 * 1024**3, "insufficient headroom for pair plus reference parity"
    records = []
    started = time.monotonic()
    try:
        run(["sudo", "sysctl", "-w", "vm.nr_hugepages=2048"], a.out / "reserve.log")
        assert int(pool.read_text()) == 2048
        run([str(a.binary.resolve()), "verify", a.kernel, "large", "1", str(core0), str(core0)],
            a.out / "full-size-parity.log", timeout=600)
        for repeat in range(3):
            ordered = cases if repeat % 2 == 0 else list(reversed(cases))
            for mode, cpu0, cpu1 in ordered:
                assert time.monotonic() - started < 1500, "interhash screening deadline"
                label = f"r{repeat+1}-{mode}-cpu{cpu0}-{cpu1}"
                before = counters()
                run([str(a.binary.resolve()), mode, a.kernel, "large", "25", str(cpu0), str(cpu1)],
                    a.out / (label + ".log"), timeout=180)
                after = counters()
                delta = {key: after[key] - before[key] for key in before}
                assert all(delta[k] == 0 for k in ("pswpin", "pswpout", "oom_kill")), delta
                steal = 100 * delta["cpu_steal"] / max(1, delta["cpu_total"])
                assert steal < 2, steal
                result = json.loads((a.out / (label + ".log")).read_text().splitlines()[-1])
                assert result["arena_bytes"] == 4 * 1024**3 and result["elapsed_secs"] >= 25
                assert abs(result["completed_hps"] - result["hashes"] / result["elapsed_secs"]) < 1e-7
                records.append({"label": label, "repeat": repeat + 1, "result": result, "vm_delta": delta})
                save(a.out / "results.json", records)
                print(datetime.datetime.now(datetime.timezone.utc).isoformat(), label, result["completed_hps"], flush=True)
                time.sleep(2)
            if repeat == 1:
                clear_losses = []
                for completed_pass in (1, 2):
                    rows = [r['result'] for r in records if r['repeat'] == completed_pass]
                    controls = [r['completed_hps'] for r in rows if r['mode'] == 'sequential'
                                or (r['mode'] == 'workers' and r['cpu1'] in groups[0])]
                    candidates = [r['completed_hps'] for r in rows if r['mode'].startswith('pair')]
                    clear_losses.append(max(candidates) < .95 * max(controls))
                if all(clear_losses):
                    save(a.out / 'EARLY-REJECTION.json', {
                        'reason': 'Every interleaving variant trails the strongest equal-RAM same-physical-core ordinary control by over 5% in both opposite-order passes',
                        'completed_passes': 2})
                    break
    finally:
        run(["sudo", "sysctl", "-w", f"vm.nr_hugepages={old}"], a.out / "release.log")
        save(a.out / "results.json", records)
    (a.out / "COMPLETE").write_text("Full-size parity and equal-RAM screening completed.\n")


if __name__ == "__main__":
    main()
