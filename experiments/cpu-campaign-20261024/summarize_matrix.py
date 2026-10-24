#!/usr/bin/env python3
"""Summarize complete alternating matrices without treating screens as confirmation."""
import argparse
import collections
import json
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--candidate', required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    assert (args.directory / 'COMPLETE').exists(), 'matrix is incomplete'
    experiment = json.loads((args.directory / 'experiment.json').read_text())
    records = json.loads((args.directory / 'results.json').read_text())
    groups = collections.defaultdict(dict)
    for row in records:
        if row['variant'] not in (args.baseline, args.candidate):
            continue
        key = (row['workers'], row['repeat'])
        assert row['variant'] not in groups[key], 'duplicate comparison member'
        groups[key][row['variant']] = row['completed_hps']
    result = []
    for workers in experiment['workers']:
        pairs = []
        for repeat in range(1, experiment['pairs'] + 1):
            values = groups[(workers, repeat)]
            baseline, candidate = values[args.baseline], values[args.candidate]
            assert baseline > 0 and candidate > 0
            pairs.append({'repeat': repeat, 'baseline_hps': baseline,
                          'candidate_hps': candidate, 'gain_percent': 100 * (candidate / baseline - 1)})
        result.append({'workers': workers, 'baseline_mean_hps': statistics.mean(p['baseline_hps'] for p in pairs),
                       'candidate_mean_hps': statistics.mean(p['candidate_hps'] for p in pairs),
                       'paired_gain_min_percent': min(p['gain_percent'] for p in pairs),
                       'paired_gain_max_percent': max(p['gain_percent'] for p in pairs), 'pairs': pairs})
    summary = {'experiment': experiment, 'baseline': args.baseline, 'candidate': args.candidate,
               'comparisons': result}
    text = json.dumps(summary, indent=2) + '\n'
    if args.output:
        args.output.write_text(text)
    else:
        print(text, end='')


if __name__ == '__main__':
    main()
