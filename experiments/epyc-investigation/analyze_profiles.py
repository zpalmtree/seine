#!/usr/bin/env python3
"""Summarize archived diagnostics without treating them as backend throughput."""
import argparse
import hashlib
import json
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument('root', type=Path)
ap.add_argument('--out', type=Path, required=True)
args = ap.parse_args()
profiles = {}
for directory in ['phase-results', 'pmu-core', 'pmu-streaming-stores']:
    for path in sorted((args.root / directory).glob('*.json')):
        raw = json.loads(path.read_text())
        workers = raw['pmu']['workers']
        hashes = sum(v['hashes'] for v in raw['values'])
        fraction = min(w['running_ns'] / w['enabled_ns'] for w in workers)
        assert fraction >= .98
        profiles[str(path.relative_to(args.root).with_suffix(''))] = {
            'source_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'kernel': raw['kernel'], 'binary': raw.get('binary', 'epyc-phase'),
            'cpus': raw['cpus'], 'hashes': hashes,
            'phase_seconds_per_hash': raw['phase_seconds_per_hash'],
            'counts_per_hash': {k: sum(w['counts'][k] for w in workers) / hashes
                                for k in workers[0]['counts']},
            'minimum_counter_running_fraction': fraction,
        }
baseline = profiles['pmu-streaming-stores/baseline']['counts_per_hash']
candidate = profiles['pmu-streaming-stores/streaming-ring']['counts_per_hash']
result = {
    'scope': 'Loop diagnostics; main backend results determine throughput',
    'initial_streams': 'same nonce counter per worker',
    'pmu_core_streams': 'cpu<<32 | counter, distinct across workers',
    'streaming_store_scope': 'Normal hashing API, distinct streams, one baseline then one candidate; zero phase fields are placeholders',
    'profiles': profiles,
    'streaming_store_counts_per_hash_delta_pct': {
        k: 100 * (candidate[k] / baseline[k] - 1) for k in baseline},
    'counter_limits': [
        'Guest core counters do not measure socket DRAM bandwidth',
        'WCB closure counts are not validated as a count of all streaming cache lines',
        'One ordered diagnostic pass does not establish counter repeatability',
    ],
}
args.out.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result['streaming_store_counts_per_hash_delta_pct']))
