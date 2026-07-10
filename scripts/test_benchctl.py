#!/usr/bin/env python3
"""Standard-library tests for scripts/benchctl.py."""

from __future__ import annotations

import io
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchctl  # noqa: E402


class ParseHelpersTests(unittest.TestCase):
    def test_parse_linux_cpu_list_supports_ranges_and_stride(self) -> None:
        self.assertEqual(benchctl.parse_linux_cpu_list("0-5:2,8,10-11"), [0, 2, 4, 8, 10, 11])

    def test_parse_proc_meminfo_converts_kibibytes(self) -> None:
        parsed = benchctl.parse_proc_meminfo(
            "MemTotal:       1024 kB\nHugePages_Total: 4\nHugepagesize: 2048 kB\n"
        )
        self.assertEqual(parsed["MemTotal"], 1024 * 1024)
        self.assertEqual(parsed["HugePages_Total"], 4)
        self.assertEqual(parsed["Hugepagesize"], 2048 * 1024)

    def test_parse_nvidia_csv_preserves_text_and_numbers(self) -> None:
        rows = benchctl.parse_nvidia_csv(
            "0, NVIDIA RTX, GPU-1, 610.1, P2, 45, 13, 2, 32768, 7000, 99.2, [N/A], 2100, 9000",
            (
                "index",
                "name",
                "uuid",
                "driver_version",
                "pstate",
                "temperature_c",
                "gpu_utilization_pct",
                "memory_utilization_pct",
                "memory_total_mib",
                "memory_used_mib",
                "power_draw_w",
                "power_limit_w",
                "sm_clock_mhz",
                "memory_clock_mhz",
            ),
        )
        self.assertEqual(rows[0]["index"], 0)
        self.assertEqual(rows[0]["gpu_utilization_pct"], 13)
        self.assertEqual(rows[0]["power_draw_w"], 99.2)
        self.assertIsNone(rows[0]["power_limit_w"])


class PairedComparisonTests(unittest.TestCase):
    def write_tsv(self, directory: Path, body: str) -> Path:
        path = directory / "results.tsv"
        path.write_text(body, encoding="utf-8")
        return path

    def test_summary_uses_paired_log_ratios_and_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as raw_directory:
            path = self.write_tsv(
                Path(raw_directory),
                "variant\tpair\torder\tavg_hps\n"
                "baseline\t1\tfirst\t100\n"
                "candidate\t1\tsecond\t110\n"
                "candidate\t2\tfirst\t180\n"
                "baseline\t2\tsecond\t200\n"
                "baseline\t3\tfirst\t400\n"
                "candidate\t3\tsecond\t480\n",
            )
            paired = benchctl.load_paired_results(path, "avg_hps", "baseline", "candidate", 3)
            first = benchctl.summarize_pairs(paired, "avg_hps", 1_000, 42)
            second = benchctl.summarize_pairs(paired, "avg_hps", 1_000, 42)

        expected = (math.exp(sum(math.log(value) for value in (1.1, 0.9, 1.2)) / 3) - 1) * 100
        self.assertAlmostEqual(
            first["comparison"]["paired_geometric_delta_pct"], expected, places=12
        )
        self.assertEqual(
            first["comparison"]["paired_geometric_delta_ci95_pct"],
            second["comparison"]["paired_geometric_delta_ci95_pct"],
        )
        self.assertEqual(first["baseline"]["arithmetic_mean"], 700 / 3)
        self.assertEqual(first["candidate"]["arithmetic_mean"], 770 / 3)
        self.assertEqual(
            first["comparison"]["sign_consistency"]["candidate_faster_pairs"], 2
        )

    def test_rejects_missing_pair_member(self) -> None:
        with tempfile.TemporaryDirectory() as raw_directory:
            path = self.write_tsv(
                Path(raw_directory),
                "variant\tpair\tavg_hps\n"
                "baseline\t1\t100\n"
                "candidate\t1\t101\n"
                "baseline\t2\t100\n",
            )
            with self.assertRaisesRegex(benchctl.BenchctlError, "unpaired"):
                benchctl.load_paired_results(path, "avg_hps", "baseline", "candidate", 2)

    def test_rejects_duplicate_and_nonpositive_rows(self) -> None:
        cases = (
            (
                "variant\tpair\tavg_hps\n"
                "baseline\t1\t100\n"
                "baseline\t1\t101\n"
                "candidate\t1\t102\n",
                "duplicate",
            ),
            (
                "variant\tpair\tavg_hps\n"
                "baseline\t1\t100\n"
                "candidate\t1\t0\n",
                "finite, positive",
            ),
        )
        for body, expected in cases:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as raw_directory:
                path = self.write_tsv(Path(raw_directory), body)
                with self.assertRaisesRegex(benchctl.BenchctlError, expected):
                    benchctl.load_paired_results(
                        path, "avg_hps", "baseline", "candidate", 2
                    )

    def test_rejects_too_few_complete_pairs(self) -> None:
        with tempfile.TemporaryDirectory() as raw_directory:
            path = self.write_tsv(
                Path(raw_directory),
                "variant\tpair\tavg_hps\n"
                "baseline\t1\t100\n"
                "candidate\t1\t101\n"
                "baseline\t2\t100\n"
                "candidate\t2\t101\n",
            )
            with self.assertRaisesRegex(benchctl.BenchctlError, "at least 3"):
                benchctl.load_paired_results(path, "avg_hps", "baseline", "candidate", 3)

    def test_confidence_gates_use_the_correct_bound(self) -> None:
        result = {
            "comparison": {
                "paired_geometric_delta_pct": 4.0,
                "paired_geometric_delta_ci95_pct": [1.5, 7.0],
            }
        }
        gates, failure = benchctl.evaluate_gates(result, 2.0, 1.0, "confidence")
        self.assertIsNone(failure)
        self.assertTrue(all(gate["passed"] for gate in gates))

        result = {
            "comparison": {
                "paired_geometric_delta_pct": -4.0,
                "paired_geometric_delta_ci95_pct": [-6.0, -3.0],
            }
        }
        gates, failure = benchctl.evaluate_gates(result, 2.0, None, "confidence")
        self.assertEqual(failure, "regression")
        self.assertEqual(gates[0]["statistic"], "ci95_upper")

    def test_cli_returns_configured_gate_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as raw_directory:
            path = self.write_tsv(
                Path(raw_directory),
                "variant\tpair\tavg_hps\n"
                "baseline\t1\t100\n"
                "candidate\t1\t90\n"
                "baseline\t2\t200\n"
                "candidate\t2\t180\n"
                "baseline\t3\t400\n"
                "candidate\t3\t360\n",
            )
            with mock.patch("sys.stdout", new_callable=io.StringIO):
                exit_code = benchctl.main(
                    [
                        "compare",
                        str(path),
                        "--bootstrap-samples",
                        "100",
                        "--max-regression-pct",
                        "5",
                        "--regression-exit-code",
                        "23",
                    ]
                )
        self.assertEqual(exit_code, 23)


class RunWrapperTests(unittest.TestCase):
    def minimal_preflight(self) -> dict:
        return {
            "schema": benchctl.PREFLIGHT_SCHEMA,
            "runtime": {"kind": "test"},
            "cpu": {
                "model": "test CPU",
                "architecture": "test",
                "logical_available": 1,
                "physical_cores": 1,
            },
            "memory": {"total_bytes": 1024},
            "load": {"load_average_1m": 0.0},
            "nvidia": {"gpus": []},
            "warnings": [],
        }

    def test_run_records_exact_argv_and_does_not_use_a_shell(self) -> None:
        with tempfile.TemporaryDirectory() as raw_directory:
            directory = Path(raw_directory)
            marker = directory / "must-not-exist"
            hostile_argument = f"; touch {marker}"
            command = [
                sys.executable,
                "-c",
                "import sys; raise SystemExit(0 if sys.argv[1].startswith('; touch') else 9)",
                hostile_argument,
            ]
            manifest_path = directory / "manifest.json"
            with mock.patch("benchctl.collect_preflight", return_value=self.minimal_preflight()):
                exit_code = benchctl.execute_wrapped(
                    command,
                    directory,
                    directory,
                    manifest_path,
                    "test-run",
                    [],
                )

            self.assertEqual(exit_code, 0)
            self.assertFalse(marker.exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["argv"], command)
            self.assertEqual(manifest["status"], "completed")
            self.assertEqual(manifest["exit_code"], 0)
            self.assertIsNotNone(manifest["ended_at_utc"])
            self.assertGreaterEqual(manifest["duration_seconds"], 0)


if __name__ == "__main__":
    unittest.main()
