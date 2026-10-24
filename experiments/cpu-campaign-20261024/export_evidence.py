#!/usr/bin/env python3
"""Publish compact, auditable measurements while retaining raw report digests.

Shared build/configuration metadata is deduplicated. Completed hashes, actual
elapsed/fence times, measured backing and health counters remain per round.
Original reports and downloaded archive manifests remain in ignored data/.
"""
import argparse
import hashlib
import json
from pathlib import Path

from screen import validate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--pattern", default="r*.json")
    parser.add_argument("--status", choices=["screen", "confirmation", "excluded"], required=True)
    parser.add_argument("--note", required=True)
    args = parser.parse_args()
    contexts = {}
    reports = []
    for path in sorted(args.directory.glob(args.pattern)):
        raw = path.read_bytes()
        report = json.loads(raw)
        if not isinstance(report, dict) or report.get("schema_version") != 14 or "runs" not in report:
            continue
        config = report["config_fingerprint"]
        workers = report["cpu_threads"]
        page = config["cpu_page_mode"]
        assert page in ("regular", "large")
        rate = validate(report, workers, config["cpu_kernel"], page)
        assert report["pow_fingerprint"] == {
            "memory_kb": 2097152, "iterations": 1, "parallelism": 1,
            "output_len": 32, "header_base_len": 92}
        environment = {key: value for key, value in report["environment"].items()
                       if key not in ("timestamp_unix_secs", "hostname", "available_memory_bytes",
                                      "cgroup_free_memory_bytes")}
        context = {"environment": environment, "config": config, "pow": report["pow_fingerprint"],
                   "kind": report["bench_kind"], "workers": workers,
                   "seconds": report["bench_secs"], "warmup_rounds": report["warmup_rounds"]}
        key = hashlib.sha256(json.dumps(context, sort_keys=True).encode()).hexdigest()[:16]
        assert key not in contexts or contexts[key] == context
        contexts[key] = context
        rounds = []
        for run in report["runs"]:
            sample = {key: value for key, value in run.items() if key != "backend_runs"}
            sample["backend_runs"] = [
                {key: value for key, value in backend.items()
                 if key.startswith("memory_") or key.endswith("_timeouts")
                 or key in ("backend_id", "backend", "hashes", "dropped_events",
                            "completed_assignment_hashes", "completed_assignment_secs")}
                for backend in run["backend_runs"]]
            rounds.append(sample)
        reports.append({"file": path.name, "raw_sha256": hashlib.sha256(raw).hexdigest(),
                        "context": key, "timestamp_unix_secs": report["environment"]["timestamp_unix_secs"],
                        "completed_hps": rate, "runs": rounds})
    assert reports, "no schema-14 reports matched"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"status": args.status, "note": args.note,
                                      "rate_definition": "sum(completed hashes) / sum(actual elapsed including fence)",
                                      "contexts": contexts, "reports": reports}, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "reports": len(reports), "contexts": len(contexts)}))


if __name__ == "__main__":
    main()
