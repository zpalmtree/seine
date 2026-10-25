#!/usr/bin/env python3
"""Export successful black-box lifecycle evidence without private guest identity."""
import argparse
import collections
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--hardware', required=True)
    args = parser.parse_args()
    assert (args.directory / 'COMPLETE').exists(), 'lifecycle did not pass'
    phases = []
    for path in sorted(args.directory.glob('*-samples.json')):
        rows = json.loads(path.read_text())
        groups = collections.defaultdict(list)
        for row in rows:
            groups[row['phase']].append(row)
        for phase, samples in groups.items():
            anonymous = [int(s.get('smaps_rollup', {}).get('Anonymous', '0 kB').split()[0]) * 1024 for s in samples]
            events = [dict(line.split() for line in s['memory.events'].splitlines()) for s in samples]
            assert all(int(e['oom_kill']) == 0 for e in events)
            phases.append({'phase': phase, 'sample_count': len(samples),
                           'first_completed_hashes': samples[0]['completed_hashes'],
                           'last_completed_hashes': samples[-1]['completed_hashes'],
                           'requested_threads_observed': sorted({s['requested_threads'] for s in samples if s['requested_threads'] is not None}),
                           'anonymous_bytes_min': min(anonymous), 'anonymous_bytes_max': max(anonymous),
                           'memory_high_events_max': max(int(e['high']) for e in events),
                           'oom_kills': 0, 'raw_samples_sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    seeds = {}
    for path in sorted(args.directory.glob('seed-*.json')):
        seed = json.loads(path.read_text())
        seeds[path.stem] = {'selected_threads': seed['selected_threads'],
                            'raw_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    recovery = next(p for p in phases if p['phase'] == 'restart-recovery')
    assert recovery['anonymous_bytes_max'] >= 4 * 1024**3
    assert max(recovery['requested_threads_observed']) > 1
    units = {}
    for path in sorted(args.directory.glob('*.service.log')):
        log = path.read_text()
        assert 'quarantined' not in log.lower()
        units[path.name] = {'raw_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                           'autotune_messages': log.count('cpu-autotune |')}
    optout = units['seine-campaign-lifecycle-optout.service.log']
    assert optout['autotune_messages'] == 0
    result = {'hardware': args.hardware, 'status': 'passed', 'phases': phases, 'cache_seeds': seeds,
              'logs': units, 'completion_sha256': hashlib.sha256((args.directory / 'COMPLETE').read_bytes()).hexdigest(),
              'interpretation': 'Actual resident arena growth is checked in addition to requested thread count. Productive phase and pressure assertions are enforced by resource_lifecycle.py.'}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'hardware': args.hardware, 'phases': len(phases), 'restart_anonymous_max_gib': recovery['anonymous_bytes_max'] / 1024**3}))


if __name__ == '__main__':
    main()
