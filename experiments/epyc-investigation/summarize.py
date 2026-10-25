#!/usr/bin/env python3
"""Extract repeatable result tables from archived per-pair records."""
import argparse
import json
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument('root', type=Path)
args = ap.parse_args()
for path in sorted(args.root.glob('*/w*/summary.json')):
    d = json.loads(path.read_text())
    print(path.parent.parent.name, path.parent.name,
          f"{d['paired_geometric_delta_pct']:+.3f}%",
          '/'.join(f'{v:+.3f}' for v in d['paired_deltas_pct']),
          f"{d['completed_hps']['baseline']:.5f}->{d['completed_hps']['candidate']:.5f}")
