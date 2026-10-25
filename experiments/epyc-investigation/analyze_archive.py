#!/usr/bin/env python3
"""Recompute rates/deltas from pairs; preserve original reports unchanged."""
import argparse
import hashlib
import itertools
import json
import math
from pathlib import Path
import statistics
from bench import percentile

ap = argparse.ArgumentParser()
ap.add_argument('root', type=Path)
ap.add_argument('--out', type=Path, required=True)
args = ap.parse_args()
results = {}
for path in sorted(args.root.glob('*/w*/pairs.json')):
    pairs = json.loads(path.read_text())
    ratios = [p['candidate']['completed_hps'] / p['baseline']['completed_hps'] for p in pairs]
    boot = sorted(100 * (math.exp(statistics.mean(math.log(ratios[i]) for i in picks)) - 1)
                  for picks in itertools.product(range(len(pairs)), repeat=len(pairs)))
    name = str(path.parent.relative_to(args.root))
    results[name] = {
        'protocol': json.loads(path.with_name('protocol.json').read_text()),
        'pairs_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
        'pairs': pairs,
        'completed_hps': {s: statistics.mean(p[s]['completed_hps'] for p in pairs)
                          for s in ['baseline', 'candidate']},
        'paired_delta_pct': 100 * (math.exp(statistics.mean(map(math.log, ratios))) - 1),
        'paired_deltas_pct': [100 * (r - 1) for r in ratios],
        'descriptive_resampling_interval_pct': [percentile(boot, .025), percentile(boot, .975)],
        'complete': path.with_name('COMPLETE').exists(),
    }
args.out.write_text(json.dumps({'method': 'Completed H/s, geometric mean of paired ratios; intervals descriptive only',
                               'comparisons': results}, indent=2) + '\n')
for name, result in results.items():
    print(name, f"{result['paired_delta_pct']:+.3f}%", result['paired_deltas_pct'])
