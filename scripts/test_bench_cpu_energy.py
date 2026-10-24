#!/usr/bin/env python3
"""Counter arithmetic, evidence rejection and owned-process cleanup checks."""
import argparse
import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import bench_cpu_energy as energy


def report():
    backend = {key: 0 for key in energy.ERROR_COUNTERS}
    backend.update(backend="cpu", backend_id=1, hashes=12, memory_regular_workers=1,
                   memory_regular_bytes=2 * energy.GIB)
    return {"schema_version": 14, "bench_kind": "end_to_end", "backends": ["cpu#1"],
            "pow_fingerprint": {"m": 2097152, "t": 1, "p": 1},
            "environment": {"cpu_identity": {"family": 26}},
            "warmup_rounds": 0, "cpu_threads": 1, "rounds": 1, "bench_secs": 1,
            "total_hashes": 12,
            "config_fingerprint": {"cpu_page_mode": "regular", "cpu_autotune_threads": False,
                                   "cpu_affinity": "auto",
                                   "cpu_kernel": "avx2", "cpu_kernel_requested": "avx2"},
            "runs": [{"hashes": 12, "counted_hashes": 10, "late_hashes": 2,
                      "backend_runs": [backend]}]}


@unittest.skipUnless(sys.platform == "linux", "Linux powercap paths and process groups")
class EnergyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.zone = self.root / "intel-rapl:0"
        self.zone.mkdir()
        (self.zone / "name").write_text("package-0\n")
        (self.zone / "energy_uj").write_text("0\n")
        (self.zone / "max_energy_range_uj").write_text("1000000000\n")
        self.zones = energy.discover(self.root)

    def sample(self, seconds, uj):
        return {"monotonic": seconds, "read_span_secs": 0,
                "energy_uj": {str(self.zone): uj}}

    def test_single_wrap_and_ordinary_increase(self):
        self.assertEqual(energy.interval_joules(self.sample(0, 10_000_000), self.sample(.25, 20_000_000),
                                               self.zones, 1000)[str(self.zone)], 10)
        self.assertEqual(energy.interval_joules(self.sample(0, 950_000_000), self.sample(.25, 50_000_000),
                                               self.zones, 1000)[str(self.zone)], 100)

    def test_ambiguous_wraps_and_implausible_resets_are_rejected(self):
        for start, end, duration in [(0, 0, 2), (100_000_000, 10_000_000, .25), (0, 1, 0)]:
            with self.subTest(start=start, end=end, duration=duration), self.assertRaises(energy.EnergyError):
                energy.interval_joules(self.sample(0, start), self.sample(duration, end), self.zones, 1000)

    def test_child_domains_and_aliases_are_not_double_counted(self):
        child = self.zone / "intel-rapl:0:0"
        child.mkdir()
        for name, value in {"name": "core", "energy_uj": "0", "max_energy_range_uj": "1000000000"}.items():
            (child / name).write_text(value)
        (self.root / "intel-rapl:alias").symlink_to(self.zone)
        zones = energy.discover(self.root)
        self.assertEqual(len(zones), 2)
        self.assertEqual(energy.select_packages(zones, None), self.zones)
        with self.assertRaises(energy.EnergyError):
            energy.select_packages(zones, [child])

    def test_duplicate_package_drivers_require_explicit_selection(self):
        duplicate = dict(self.zones[0], path=str(self.root / "intel-rapl-mmio:0"))
        with self.assertRaises(energy.EnergyError):
            energy.select_packages(self.zones + [duplicate], None)
        self.assertEqual(energy.select_packages(self.zones + [duplicate], [self.zone]), self.zones)

    def test_source_selection_cannot_silently_omit_another_socket(self):
        other_path = self.root / "intel-rapl:1"
        other = dict(self.zones[0], name="package-1", path=str(other_path))
        zones = self.zones + [other]
        with self.assertRaisesRegex(energy.EnergyError, "every available CPU package"):
            energy.select_packages(zones, [self.zone])
        self.assertEqual(energy.select_packages(zones, [self.zone, other_path]), zones)

    def test_complete_hashes_include_fence_completions(self):
        self.assertEqual(energy.validate_report(report(), 1, "regular", 1, 1, "avx2"), 12)

    def test_warmups_partial_backing_missing_telemetry_and_partial_reports_fail(self):
        edits = [lambda r: r.update(warmup_rounds=1),
                 lambda r: r["runs"][0]["backend_runs"][0].update(memory_regular_bytes=energy.GIB),
                 lambda r: r["runs"][0]["backend_runs"][0].pop("control_execution_timeouts"),
                 lambda r: r.update(rounds=2),
                 lambda r: r.update(total_hashes=10),
                 lambda r: r["config_fingerprint"].update(cpu_autotune_threads=True)]
        for edit in edits:
            r = copy.deepcopy(report())
            edit(r)
            with self.assertRaises((energy.EnergyError, KeyError)):
                energy.validate_report(r, 1, "regular", 1, 1, "avx2")

    def args(self, binary, name="result"):
        return argparse.Namespace(binary=binary, output_dir=self.root / name, threads=1,
                                  page_mode="regular", kernel="avx2", seconds=1, rounds=1,
                                  interval=.02, timeout=5, max_domain_watts=1000,
                                  label="test-fixture-not-hardware", zone=None)

    def context(self):
        return {"vmstat": {"pswpin": 0, "pswpout": 0, "oom_kill": 0},
                "boot_id": "test-boot",
                "frequency_policies": {}, "power_constraints": {}, "allowed_cpus": [0], "cpuinfo": "fixture"}

    def test_capture_uses_matching_invocation_energy_and_preserves_raw_samples(self):
        binary = self.root / "fake-miner"
        binary.write_text("#!/usr/bin/env python3\nimport sys,time,json\nfrom pathlib import Path\n"
                          f"counter=Path({str(self.zone / 'energy_uj')!r})\n"
                          "for i in range(1,11):\n time.sleep(.025)\n"
                          " tmp=counter.with_suffix('.tmp')\n tmp.write_text(str(i*1000000))\n tmp.replace(counter)\n"
                          f"Path(sys.argv[sys.argv.index('--bench-output')+1]).write_text({json.dumps(report())!r})\n")
        binary.chmod(0o755)
        with patch.object(energy, "check_capacity"), patch.object(energy, "cpu_context", side_effect=lambda _: self.context()):
            result = energy.run(self.args(binary), self.zones)
        self.assertEqual(result["status"], "complete")
        self.assertAlmostEqual(result["total_package_joules"], 10)
        self.assertAlmostEqual(result["invocation_joules_per_completed_hash"], 10 / 12)
        self.assertGreater(len(result["samples"]), 2)
        self.assertEqual(json.loads((self.root / "result/energy.json").read_text())["status"], "complete")

    def test_missing_counters_prevent_process_launch(self):
        with patch.object(energy.subprocess, "Popen") as start:
            with self.assertRaises(energy.EnergyError):
                energy.run(self.args(self.root / "absent"), [])
            start.assert_not_called()

    def test_timeout_stops_owned_process_and_records_failure(self):
        binary = self.root / "sleeping-miner"
        binary.write_text("#!/usr/bin/env python3\nimport time\ntime.sleep(60)\n")
        binary.chmod(0o755)
        args = self.args(binary)
        args.timeout = .08
        with patch.object(energy, "check_capacity"), patch.object(energy, "cpu_context", side_effect=lambda _: self.context()):
            with self.assertRaisesRegex(energy.EnergyError, "timeout"):
                energy.run(args, self.zones)
        result = json.loads((self.root / "result/energy.json").read_text())
        self.assertEqual(result["status"], "failed")
        self.assertNotIn("invocation_joules_per_completed_hash", result)

    def measurement(self, name, joules):
        folder = self.root / name
        folder.mkdir()
        (folder / "bench.json").write_text(json.dumps(report()))
        record = {"status": "complete", "scope": "whole_invocation_package_energy",
                  "binary_sha256": "fixture", "domains": self.zones,
                  "benchmark_sha256": energy.sha256(folder / "bench.json"),
                  "completed_hashes": 12, "samples": [self.sample(0, 0), self.sample(.5, int(joules * 1e6))],
                  "max_domain_watts_assumption": 1000, "invocation_secs": .5,
                  "total_package_joules": joules, "invocation_joules_per_completed_hash": joules / 12,
                  "invocation_completed_hashes_per_second": 24, "average_package_watts": joules * 2,
                  "context_before": self.context()}
        (folder / "energy.json").write_text(json.dumps(record))
        return folder

    def test_paired_comparison_recomputes_metrics_and_checks_compatibility(self):
        baseline = [self.measurement(f"a{i}", 10) for i in range(3)]
        candidate = [self.measurement(f"b{i}", 8) for i in range(3)]
        compared = energy.compare(baseline, candidate)
        metric = compared["metrics"]["invocation_joules_per_completed_hash"]
        self.assertAlmostEqual(metric["comparison"]["paired_geometric_delta_pct"], -20)
        self.assertEqual(metric["comparison"]["sign_consistency"]["candidate_lower_pairs"], 3)
        altered = candidate[0] / "energy.json"
        record = json.loads(altered.read_text())
        record["invocation_joules_per_completed_hash"] *= .5
        altered.write_text(json.dumps(record))
        with self.assertRaisesRegex(energy.EnergyError, "raw observations"):
            energy.compare(baseline, candidate)

    def test_comparison_rejects_missing_pairs_reused_inputs_and_changed_report(self):
        baseline = [self.measurement(f"a{i}", 10) for i in range(3)]
        candidate = [self.measurement(f"b{i}", 8) for i in range(3)]
        for a, b in [(baseline[:2], candidate[:2]), (baseline, baseline)]:
            with self.assertRaises(energy.EnergyError):
                energy.compare(a, b)
        (candidate[0] / "bench.json").write_text("{}")
        with self.assertRaisesRegex(energy.EnergyError, "checksum"):
            energy.compare(baseline, candidate)

    def test_comparison_rejects_context_changes_between_matched_pairs(self):
        baseline = [self.measurement(f"a{i}", 10) for i in range(3)]
        candidate = [self.measurement(f"b{i}", 8) for i in range(3)]
        for folder in (baseline[1], candidate[1]):
            path = folder / "energy.json"
            record = json.loads(path.read_text())
            record["binary_sha256"] = "different-build-in-second-pair"
            path.write_text(json.dumps(record))
        with self.assertRaisesRegex(energy.EnergyError, "binary_sha256"):
            energy.compare(baseline, candidate)

    def test_comparison_allows_power_changes_but_requires_same_allowed_cpus(self):
        baseline = [self.measurement(f"a{i}", 10) for i in range(3)]
        candidate = [self.measurement(f"b{i}", 8) for i in range(3)]
        for folder in candidate:
            path = folder / "energy.json"
            record = json.loads(path.read_text())
            record["context_before"]["power_constraints"] = {"package-0": {"limit": "100"}}
            path.write_text(json.dumps(record))
        energy.compare(baseline, candidate)
        for folder in candidate:
            path = folder / "energy.json"
            record = json.loads(path.read_text())
            record["context_before"]["allowed_cpus"] = [1]
            path.write_text(json.dumps(record))
        with self.assertRaisesRegex(energy.EnergyError, "allowed_cpus"):
            energy.compare(baseline, candidate)

    def test_sigterm_cleans_up_the_benchmark_process(self):
        pidfile = self.root / "child.pid"
        binary = self.root / "sleeping-miner"
        binary.write_text("#!/usr/bin/env python3\nimport os,time\nfrom pathlib import Path\n"
                          f"Path({str(pidfile)!r}).write_text(str(os.getpid()))\ntime.sleep(60)\n")
        binary.chmod(0o755)
        driver = self.root / "driver.py"
        driver.write_text(f"import sys\nsys.path.insert(0,{str(Path(energy.__file__).parent)!r})\n"
                          "import bench_cpu_energy as e\ne.check_capacity=lambda *a: None\nsys.exit(e.main())\n")
        command = [sys.executable, "-B", str(driver), "--counter-root", str(self.root), "run",
                   "--binary", str(binary), "--threads", "1", "--interval", ".02",
                   "--output-dir", str(self.root / "signal-result")]
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        child_pid = None
        try:
            deadline = time.monotonic() + 3
            while not pidfile.exists() and proc.poll() is None and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(pidfile.exists(), "fixture child did not start")
            child_pid = int(pidfile.read_text())
            proc.terminate()
            proc.communicate(timeout=5)
            self.assertEqual(proc.returncode, 130)
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)
            result = json.loads((self.root / "signal-result/energy.json").read_text())
            self.assertEqual(result["status"], "failed")
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate(timeout=5)
            if child_pid:
                try:
                    os.kill(child_pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


if __name__ == "__main__":
    unittest.main()
