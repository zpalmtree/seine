#!/usr/bin/env python3
"""Summarize the final three-path, two-pass integration confirmation separately."""
import argparse
import json
from pathlib import Path
import statistics

from run import validate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    rows = []
    for mode in ('large', 'regular'):
        values = {}
        for kernel in ('avx512', 'avx2', 'avx512-256'):
            samples = []
            for pair in (1, 2):
                report = json.loads((args.directory / f'{mode}-{pair}-{kernel}.json').read_text())
                validate(report, 4, mode, kernel, rounds=1)
                sample = report['runs'][0]
                samples.append({'pass': pair, 'counted_hps': report['avg_hps'],
                                'completed_hps': sample['hashes'] / sample['elapsed_secs']})
            values[kernel] = {'samples': samples, **{
                metric: statistics.mean(s[metric] for s in samples)
                for metric in ('counted_hps', 'completed_hps')}}
        ratios = {candidate + '-vs-' + baseline: {
            metric: 100 * (values[candidate][metric] / values[baseline][metric] - 1)
            for metric in ('counted_hps', 'completed_hps')}
            for candidate, baseline in [('avx512-256', 'avx512'),
                                         ('avx512-256', 'avx2'), ('avx2', 'avx512')]}
        rows.append({'mode': mode, 'workers': 4, 'kernels': values, 'delta_pct': ratios})
    print(json.dumps({'complete': (args.directory / 'COMPLETE').exists(),
                      'scope': 'two opposite-order passes; integration confirmation, not a long repeat',
                      'cells': rows}, indent=2))


if __name__ == '__main__':
    main()
