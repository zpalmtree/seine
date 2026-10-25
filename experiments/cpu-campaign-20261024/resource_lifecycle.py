#!/usr/bin/env python3
"""Black-box adaptive mining checks on a disposable campaign Linux VM.

Uses the real pool client, CPU backend, cache, and process restarts against a
loopback fixture with an impossible target. A service-private hosts file maps
the built-in developer pool to that same fixture; service IP access is restricted
to loopback. No public pool receives work or traffic.
MemoryHigh changes exercise both effective budgets and cgroup reclaim pressure;
MemoryMax remains 16 GiB. Only uniquely named task-owned systemd units change.
"""
import argparse
import datetime
import json
import hashlib
from pathlib import Path
import socketserver
import re
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "results/lifecycle"
SERVER_EVENTS = []


class Fixture(socketserver.StreamRequestHandler):
    def handle(self):
        for raw in self.rfile:
            if not raw.strip():
                continue
            request = json.loads(raw)
            SERVER_EVENTS.append({"at": time.time(), "method": request.get("method"), "id": request.get("id")})
            if request.get("method") == "login":
                response = {"id": request["id"], "status": "ok", "result": {"protocol_version": 2,
                            "capabilities": ["login_negotiation", "submit_claimed_hash"]}}
                job = {"method": "job", "params": {"job_id": "campaign-offline", "header_base": "00" * 92,
                       "target": "00" * 32, "network_target": "00" * 32, "height": 1,
                       "nonce_start": 0, "nonce_end": (1 << 63) - 1}}
                self.wfile.write((json.dumps(response) + "\n" + json.dumps(job) + "\n").encode())
                self.wfile.flush()
            elif request.get("method") == "submit":
                raise AssertionError("impossible target unexpectedly produced a solution")


class Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


def command(args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT, timeout=30)


def save(name, obj):
    (OUT / name).write_text(json.dumps(obj, indent=2) + "\n")


def state(unit):
    properties = dict(line.split("=", 1) for line in command([
        "systemctl", "show", unit, "--property=ControlGroup", "--property=MainPID"]).splitlines())
    path = properties["ControlGroup"]
    group = Path("/sys/fs/cgroup") / path.lstrip("/")
    result = {"time": time.time(), "cgroup": path, "pid": int(properties["MainPID"])}
    rollup = Path("/proc") / properties["MainPID"] / "smaps_rollup"
    if rollup.exists():
        result["smaps_rollup"] = {line.split(":")[0]: line.split(":", 1)[1].strip()
                                  for line in rollup.read_text().splitlines() if ":" in line
                                  and line.split(":")[0] in ("Rss", "Anonymous", "AnonHugePages", "Private_Hugetlb", "Shared_Hugetlb")}
    for name in ("memory.current", "memory.peak", "memory.high", "memory.max", "memory.events", "memory.pressure", "cpu.stat", "cpu.pressure"):
        file = group / name
        if file.exists():
            result[name] = file.read_text().strip()
    return result


def set_high(unit, value):
    command(["sudo", "systemctl", "set-property", "--runtime", unit, "MemoryHigh=" + value])


def fixture_address():
    # Two compressed Ristretto identities are syntactically valid test keys.
    # A literal address avoids the network shortname-resolution startup path.
    payload = bytes(64)
    checksum = hashlib.sha3_256(b"blocknet_stealth_address_checksum" + b"blocknet_mainnet" + payload).digest()[:4]
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    value = int.from_bytes(checksum, "big")
    encoded = ""
    while value:
        value, digit = divmod(value, 58)
        encoded = alphabet[digit] + encoded
    return "1" * 64 + encoded


def start(unit, binary, cache, port, cpus, high, fixed=False, page="auto", runtime=550):
    args = [str(binary), "--mode", "pool", "--pool-url", f"127.0.0.1:{port}", "--address", fixture_address(),
            "--pool-worker", "offline-cpu-campaign", "--backend", "cpu", "--cpu-page-mode", page,
            "--cpu-affinity", "auto", "--cpu-autotune-config", str(cache), "--data-dir", str(OUT / "config"),
            "--ui", "plain", "--stats-secs", "5"]
    if fixed:
        args += ["--threads", "2", "--disable-cpu-autotune-threads"]
    else:
        args += ["--cpu-autotune-max-threads", "4"]
    command(["sudo", "systemd-run", "--quiet", "--unit=" + unit, "--uid=ubuntu",
             "--property=Type=exec", "--property=RuntimeMaxSec=" + str(runtime), "--property=TimeoutStopSec=15",
             "--property=KillSignal=SIGINT", "--property=MemoryHigh=" + high, "--property=MemoryMax=16G",
             "--property=BindReadOnlyPaths=" + str(OUT / "hosts") + ":/etc/hosts",
             "--property=IPAddressDeny=any", "--property=IPAddressAllow=localhost",
             "--property=AllowedCPUs=" + cpus, "--property=CPUQuota=400%",
             "--property=StandardOutput=append:" + str(OUT / (unit + ".log")),
             "--property=StandardError=append:" + str(OUT / (unit + ".log")), *args])


def stop(unit):
    stopped = subprocess.run(["sudo", "systemctl", "stop", unit], capture_output=True, text=True, timeout=30)
    if stopped.returncode:
        assert "not loaded" in stopped.stderr, stopped.stderr
    active = command(["systemctl", "show", unit, "--property=ActiveState", "--value"]).strip()
    assert active not in ("active", "activating", "deactivating"), (unit, active)


def completed_hashes(unit):
    logfile = OUT / (unit + ".log")
    text = logfile.read_text(errors="replace") if logfile.exists() else ""
    return max([0] + [int(n) for n in re.findall(r"\bhashes=(\d+)", text)])


def observe(unit, label, seconds, records):
    begin = time.monotonic()
    initial_hashes = completed_hashes(unit)
    while time.monotonic() - begin < seconds:
        active = command(["systemctl", "show", unit, "--property=ActiveState", "--value"]).strip()
        assert active == "active", (unit, active)
        sample = state(unit)
        sample["phase"] = label
        sample["completed_hashes"] = completed_hashes(unit)
        log = (OUT / (unit + ".log")).read_text(errors="replace")
        counts = re.findall(r"cpu-autotune \| (?:starting with |done — )?(\d+) threads", log)
        sample["requested_threads"] = int(counts[-1]) if counts else None
        assert "oom_kill 0" in sample.get("memory.events", ""), sample
        records.append(sample)
        save(unit + "-samples.json", records)
        time.sleep(2)
    final_hashes = completed_hashes(unit)
    if label not in ("one-arena-budget", "no-arena-budget"):
        assert final_hashes > initial_hashes, (label, "no productive progress", initial_hashes, final_hashes)
    print(datetime.datetime.now(datetime.timezone.utc).isoformat(), unit, label,
          "complete", "completed_hashes_delta=" + str(final_hashes - initial_hashes), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--cpus", default="0-3")
    p.add_argument("--page", choices=["auto", "regular"], default="auto")
    p.add_argument("--initial-seconds", type=int, default=240)
    p.add_argument("--expect-kernel")
    a = p.parse_args()
    assert 240 <= a.initial_seconds <= 600
    assert int(Path("/proc/sys/vm/nr_hugepages").read_text()) == 0, "release benchmark pools before pressure tests"
    OUT.mkdir(parents=True, exist_ok=False)
    (OUT / "hosts").write_text("127.0.0.1 localhost bntpool.com\n::1 localhost\n")
    cache = OUT / "tuning.json"
    server = Server(("127.0.0.1", 3333), Fixture)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    units = []
    try:
        unit = "seine-campaign-lifecycle-first.service"
        units.append(unit)
        start(unit, a.binary.resolve(), cache, server.server_address[1], a.cpus, "12G", page=a.page, runtime=a.initial_seconds + 310)
        records = []
        observe(unit, "initial-stable", a.initial_seconds, records)
        if a.expect_kernel:
            kernel_cache = json.loads(Path(str(cache) + ".kernels.json").read_text())
            save("kernel-before-pressure.json", kernel_cache)
            assert any(w["kernel"] == a.expect_kernel and len(w["samples"]) == 6 for w in kernel_cache["winners"]), "automatic kernel promotion was not confirmed"
        if cache.exists():
            save("seed-before-pressure.json", json.loads(cache.read_text()))
        set_high(unit, "3G")
        observe(unit, "one-arena-budget", 30, records)
        set_high(unit, "1G")
        observe(unit, "no-arena-budget", 30, records)
        set_high(unit, "12G")
        observe(unit, "memory-recovery", 60, records)
        # A separate task-owned unit contends for the same physical CPUs without
        # changing miner configuration, affinity, or memory accounting.
        load = "seine-campaign-cpu-load.service"
        units.append(load)
        code = "import multiprocessing,time; end=time.monotonic()+45\ndef busy():\n while time.monotonic()<end: pass\nps=[multiprocessing.Process(target=busy) for _ in range(4)]\n[p.start() for p in ps]\n[p.join() for p in ps]"
        command(["sudo", "systemd-run", "--quiet", "--unit=" + load, "--uid=ubuntu",
                 "--property=AllowedCPUs=" + a.cpus, "--property=RuntimeMaxSec=50",
                 "--property=MemoryMax=256M", "python3", "-c", code])
        observe(unit, "cpu-contention", 48, records)
        observe(unit, "load-recovery", 90, records)
        stop(unit)
        high_events = [int(re.search(r"^high (\d+)$", sample["memory.events"], re.MULTILINE).group(1)) for sample in records]
        assert max(high_events) > min(high_events), "memory.high did not produce actual reclaim pressure"
        contention = [sample for sample in records if sample["phase"] == "cpu-contention"]
        cpu_pressure = [int(re.search(r"^some .*total=(\d+)", sample["cpu.pressure"], re.MULTILINE).group(1)) for sample in contention]
        assert max(cpu_pressure) > min(cpu_pressure), "CPU load did not produce scheduling pressure"
        assert cache.exists(), "no stable observation was persisted"
        seed = json.loads(cache.read_text())
        save("seed-before-restart.json", seed)
        assert seed["selected_threads"] > 1, "restart cap must be below the remembered count for this test"
        text = (OUT / (unit + ".log")).read_text()
        assert "cpu-autotune | 0 threads" in text, "did not park under insufficient RAM"
        assert "quarantined" not in text.lower(), "backend quarantine"
        assert "cpu-autotune |" in text and "H/s" in text, "no productive automatic mining"
        # A true new process reuses the previous cache under a temporary budget.
        unit = "seine-campaign-lifecycle-restart.service"
        units.append(unit)
        start(unit, a.binary.resolve(), cache, server.server_address[1], a.cpus, "3G", page=a.page)
        records = []
        observe(unit, "constrained-restart", 35, records)
        constrained = json.loads(cache.read_text())
        save("seed-during-constrained-restart.json", constrained)
        assert constrained["selected_threads"] == seed["selected_threads"], "temporary cap overwrote remembered count"
        set_high(unit, "12G")
        observe(unit, "restart-recovery", 210, records)
        assert any(s["phase"] == "restart-recovery" and (s["requested_threads"] or 0) > 1 for s in records), "did not recover above the temporary one-arena cap"
        assert any(s["phase"] == "restart-recovery" and
                   int(s.get("smaps_rollup", {}).get("Anonymous", "0 kB").split()[0]) >= 4 * 1024**2
                   for s in records), "requested growth did not allocate two real 2 GiB arenas"
        stop(unit)
        text = (OUT / (unit + ".log")).read_text()
        assert "cached starting point" in text, "restart did not reuse cache"
        assert "quarantined" not in text.lower()
        unit = "seine-campaign-lifecycle-optout.service"
        units.append(unit)
        start(unit, a.binary.resolve(), cache, server.server_address[1], a.cpus, "12G", fixed=True, page=a.page)
        observe(unit, "explicit-optout", 35, [])
        stop(unit)
        assert "cpu-autotune |" not in (OUT / (unit + ".log")).read_text(), "explicit opt-out ignored"
        (OUT / "COMPLETE").write_text("Budget, pressure, contention, process restart, and opt-out checks passed.\n")
    finally:
        for unit in reversed(units):
            stop(unit)
            subprocess.run(["sudo", "systemctl", "reset-failed", unit], capture_output=True, timeout=30)
        server.shutdown()
        server.server_close()
        save("fixture-events.json", SERVER_EVENTS)


if __name__ == "__main__":
    main()
