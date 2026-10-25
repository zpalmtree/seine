#!/usr/bin/env python3
"""Export completed cold-start tuning windows, including negative outcomes."""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    root = args.directory
    hashes = {}

    def read(relative, structured=True):
        path = root / relative
        raw = path.read_bytes()
        hashes[relative] = hashlib.sha256(raw).hexdigest()
        return json.loads(raw) if structured else raw.decode()

    outcome = read('results/outcome.json')
    cleanup = read('results/cleanup.json')
    assert not cleanup['failures'] and cleanup['pool_zero']
    identities = read('joint-binary-identities.json')
    stages = read('results/stages.json')
    topology = json.loads(read('results/topology.log', False))
    windows = []
    order = ['joint-control-a', 'joint-candidate-a', 'joint-candidate-b', 'joint-control-b']
    actual_order = [stage['name'] for stage in stages if stage['name'] in order]
    assert actual_order == order[:len(actual_order)]
    for label in order:
        prefix = 'results/' + label + '/'
        if not (root / prefix / 'COMPLETE').exists():
            continue
        read(prefix + 'COMPLETE', False)
        assert any(stage['name'] == label and stage.get('passed') for stage in stages)
        identity = read(prefix + 'identity.json')
        variant = 'control' if '-control-' in label else 'candidate'
        assert identity['binary_sha256'] == identities['sha256']['seine-joint-' + variant]
        productive = read(prefix + 'productive-window.json')
        observation = read(prefix + 'observation.json')
        window_cleanup = read(prefix + 'cleanup.json')
        samples = read(prefix + 'samples.json')
        assert not window_cleanup['failures'] and observation['initial_capacity_slots_available'] == [12, 12, 12]
        assert all(observation['vm_delta'][key] == 0 for key in ['oom_kill', 'pswpin', 'pswpout'])
        elapsed = productive['elapsed_start_request_through_stopped_seconds']
        completed = productive['completed_hashes_lower_bound']
        assert elapsed >= 900 and completed > 0
        assert abs(completed / elapsed - productive['completed_hps_lower_bound']) < 1e-10
        log = read(prefix + 'miner.log', False)
        trajectory = [line.replace('/home/ubuntu/campaign/', '')
                      for line in log.splitlines() if 'cpu-autotune |' in line]
        selected = None
        if (root / prefix / 'tuning.json').exists():
            selected = read(prefix + 'tuning.json').get('selected_threads')
        windows.append({'label': label, 'variant': variant,
            'binary_sha256': identity['binary_sha256'], 'productive_window': productive,
            'vm_delta': observation['vm_delta'], 'cleanup_passed': True,
            'requested_threads_observed': sorted({s['requested_threads'] for s in samples
                                                  if s.get('requested_threads') is not None}) or None,
            'cached_selected_threads': selected, 'autotune_trajectory': trajectory})

    comparisons = None
    if len(windows) == 4:
        assert [row['label'] for row in windows] == order and actual_order == order
        by_label = {row['label']: row['productive_window'] for row in windows}
        pairs = []
        for suffix in ['a', 'b']:
            control = by_label['joint-control-' + suffix]['completed_hps_lower_bound']
            candidate = by_label['joint-candidate-' + suffix]['completed_hps_lower_bound']
            pairs.append({'pair': suffix, 'control_logged_hps': control,
                          'candidate_logged_hps': candidate,
                          'logged_rate_difference_percent': 100 * (candidate / control - 1)})
        aggregate = {}
        for variant in ['control', 'candidate']:
            rows = [row['productive_window'] for row in windows if row['variant'] == variant]
            aggregate[variant] = sum(row['completed_hashes_lower_bound'] for row in rows) / sum(
                row['elapsed_start_request_through_stopped_seconds'] for row in rows)
        comparisons = {'pairs': pairs, 'aggregate_logged_hps': aggregate,
            'aggregate_logged_rate_difference_percent': 100 * (aggregate['candidate'] / aggregate['control'] - 1),
            'interpretation': 'Ratios compare logged completed-work rates across fresh-process, fresh-tuning-cache windows. They include startup, trials and shutdown; OS caches were not cleared. Last unreported hashes can be omitted, so these are not fixed-kernel rates or a mathematical lower bound on relative gain.'}

    parity = {name: read('results/' + name, False) for name in
              ['control-full-size-parity.log', 'candidate-full-size-parity.log']}
    result = {'status': 'research-only; not promoted', 'controller_outcome': outcome,
        'campaign_scope_decision': read('results/CAMPAIGN-SCOPE-STOP.json')
            if (root / 'results/CAMPAIGN-SCOPE-STOP.json').exists() else None,
        'hardware': {row['field']: row['data'] for row in topology['lscpu'] if row['field'] in
                     ['Model name:', 'CPU(s):', 'Core(s) per socket:', 'Thread(s) per core:', 'Socket(s):', 'L3 cache:']},
        'identities': identities, 'parity': parity,
        'fixed_kernel_gate': read('results/fixed-kernel-gate.json'),
        'actual_started_window_order': actual_order,
        'windows': windows, 'comparisons': comparisons,
        'global_cleanup': cleanup,
        'stages': [{key: row[key] for key in ['name', 'started_at', 'finished_at', 'passed'] if key in row}
                   for row in stages],
        'source_sha256': hashes,
        'limitations': ['The guest is identified by its actual CPU model, not the C5 instance label.',
                       'Cold start means a new process and separate tuning cache, not flushed OS/file caches.',
                       'This does not replicate the earlier Cascade Lake crossover if the model differs.',
                       'A passed execution/cleanup protocol does not establish a performance improvement.']}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'controller_status': outcome['status'], 'completed_windows': len(windows),
                      'comparisons': comparisons}))


if __name__ == '__main__':
    main()
