#!/usr/bin/env python3
"""Summarize paired reports; an incomplete cell never becomes an accepted result."""
import argparse
import json
import math
from pathlib import Path
import statistics

from run import validate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    rows = []
    experiment = json.loads((args.directory / 'experiment.json').read_text())
    for mode, workers in experiment.get('cells', [('large', 1), ('large', 4), ('regular', 4)]):
        pairs = []
        for pair in range(1, 4):
            values = {}
            for variant in ('baseline', 'candidate'):
                path = args.directory / f'{mode}-t{workers}-{pair}-{variant}.json'
                if not path.exists():
                    break
                report = json.loads(path.read_text())
                validate(report, workers, mode, experiment.get(variant + '_kernel', 'avx512'))
                values[variant] = {
                    'counted_hps': report['avg_hps'],
                    'completed_hps': statistics.mean(r['hashes'] / r['elapsed_secs'] for r in report['runs']),
                }
            if len(values) == 2:
                pairs.append({'pair': pair, **values})
        if not pairs:
            continue
        summary = {'page_mode': mode, 'workers': workers, 'complete': len(pairs) == 3, 'pairs': pairs}
        for metric in ('counted_hps', 'completed_hps'):
            summary[metric] = {
                variant: statistics.mean(p[variant][metric] for p in pairs)
                for variant in ('baseline', 'candidate')
            }
            ratios = [p['candidate'][metric] / p['baseline'][metric] for p in pairs]
            summary[metric].update(
                paired_deltas_pct=[100 * (r - 1) for r in ratios],
                geometric_delta_pct=100 * (math.exp(statistics.mean(math.log(r) for r in ratios)) - 1),
            )
        rows.append(summary)
    result = {'complete': (args.directory / 'COMPLETE').exists(), 'experiment': experiment, 'cells': rows}
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
