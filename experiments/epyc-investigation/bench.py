#!/usr/bin/env python3
"""Small paired backend harness. Case manifests carry binary and experiment identity."""
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import time

HARNESS_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def save(path, obj):
    path.write_text(json.dumps(obj, indent=2) + '\n')


def percentile(sorted_values, probability):
    index = probability * (len(sorted_values) - 1)
    lo, hi = math.floor(index), math.ceil(index)
    return sorted_values[lo] + (index - lo) * (sorted_values[hi] - sorted_values[lo])


def counters():
    vm = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
    cpu = list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:]))
    return {k: int(vm[k]) for k in ('pswpin', 'pswpout', 'oom_kill')} | {'ticks': sum(cpu[:8]), 'steal': cpu[7]}


def run_case(root, out, name, case, workers, seconds, rounds, warmup):
    path = out / (name + '.json')
    binary = (root / case['binary']).resolve()
    mode = case.get('mode', 'large')
    kernel = case.get('kernel', 'avx512-256')
    command = [str(binary), '--bench', '--bench-kind', 'backend', '--backend', 'cpu',
               '--threads', str(workers), '--cpu-page-mode', mode, '--cpu-affinity', 'auto',
               '--bench-cpu-kernel', kernel, '--disable-cpu-autotune-threads', '--ui', 'plain',
               '--bench-secs', str(seconds), '--bench-rounds', str(rounds),
               '--bench-warmup-rounds', str(warmup), '--bench-output', str(path),
               '--data-dir', str(root / 'config')]
    env = dict(os.environ, **case.get('env', {}))
    before = counters()
    start = time.monotonic()
    with (out / (name + '.log')).open('w') as log:
        subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=240)
    delta = {k: v - before[k] for k, v in counters().items()}
    checks = {'delta': delta, 'steal_pct': 100 * delta['steal'] / max(1, delta['ticks']),
              'steal_single_cpu_upper_pct': 100 * delta['steal'] / (seconds * rounds * os.sysconf('SC_CLK_TCK')),
              'wall_seconds': time.monotonic() - start}
    save(out / (name + '-checks.json'), checks)
    assert all(delta[k] == 0 for k in ('pswpin', 'pswpout', 'oom_kill'))
    assert checks['steal_pct'] < 2
    assert checks['steal_single_cpu_upper_pct'] < 2
    report = json.loads(path.read_text())
    fp = report['config_fingerprint']
    assert report['cpu_threads'] == workers and len(report['runs']) == rounds
    assert fp['cpu_kernel'] == kernel and fp['cpu_page_mode'] == mode
    assert fp['cpu_affinity'] == 'auto' and fp['cpu_autotune_threads'] is False
    field = {'regular': 'memory_regular', 'large': 'memory_explicit_large',
             'large-1g': 'memory_explicit_large_1g'}[mode]
    for sample in report['runs']:
        assert sum(b[field + '_workers'] for b in sample['backend_runs']) == workers
        assert sum(b[field + '_bytes'] for b in sample['backend_runs']) == workers * 2 * 1024**3
        for backend in sample['backend_runs']:
            for k in ('memory_allocation_failures', 'dropped_events',
                      'assignment_enqueue_timeouts', 'assignment_execution_timeouts',
                      'control_enqueue_timeouts', 'control_execution_timeouts'):
                assert backend[k] == 0, (name, k, backend[k])
    arenas = [json.loads(line.removeprefix('EPYC_ARENA ')) for line in (out / (name + '.log')).read_text().splitlines()
              if line.startswith('EPYC_ARENA ')]
    if case.get('env', {}).get('SEINE_EXPERIMENT_ARENA_POLICY'):
        assert len(arenas) == workers
        for arena in arenas:
            assert arena['query_result'] == 0 and arena['query_error'] is None
            assert arena['samples_by_node'] == {str(arena['local_node']): 1024}, arena
    result = {'counted_hps': report['avg_hps'],
              'completed_hps': statistics.mean(r['hashes'] / r['elapsed_secs'] for r in report['runs'])}
    print(name, json.dumps(result), flush=True)
    time.sleep(2)
    return result


def paired(root, out, baseline, candidate, workers, pairs=3, seconds=20, rounds=2, warmup=1):
    root, out = Path(root).resolve(), Path(out).resolve()
    out.mkdir(exist_ok=False)
    cases = {'baseline': baseline, 'candidate': candidate}
    hashes = {str(root / c['binary']): hashlib.sha256((root / c['binary']).read_bytes()).hexdigest() for c in cases.values()}
    expected_path = root / 'expected-binaries.json'
    if expected_path.exists():
        expected = json.loads(expected_path.read_text())
        for case in cases.values():
            assert hashes[str(root / case['binary'])] == expected[case['binary']], 'Frozen upload hash mismatch'
    save(out / 'protocol.json', {'cases': cases, 'sha256': hashes, 'workers': workers,
         'pairs': pairs, 'seconds': seconds, 'rounds': rounds, 'warmup': warmup,
         'harness_sha256': HARNESS_SHA256})
    values = []
    for pair in range(1, pairs + 1):
        result = {}
        for label in (('baseline', 'candidate') if pair % 2 else ('candidate', 'baseline')):
            result[label] = run_case(root, out, f'{pair}-{label}', cases[label], workers, seconds, rounds, warmup)
        values.append(result)
        save(out / 'pairs.json', values)
    ratios = [p['candidate']['completed_hps'] / p['baseline']['completed_hps'] for p in values]
    boot = sorted(100 * (math.exp(statistics.mean(math.log(ratios[i]) for i in picks)) - 1)
                  for picks in itertools.product(range(pairs), repeat=pairs))
    summary = {'completed_hps': {side: statistics.mean(p[side]['completed_hps'] for p in values) for side in cases},
               'paired_deltas_pct': [100 * (r - 1) for r in ratios],
               'paired_geometric_delta_pct': 100 * (math.exp(statistics.mean(map(math.log, ratios))) - 1),
               'paired_resampling_interval_pct': [percentile(boot, .025), percentile(boot, .975)],
               'interval_method': 'Enumerated paired resampling, linearly interpolated percentiles; descriptive with few pairs',
               'scope': 'within-host repeatability; not whole-architecture uncertainty'}
    assert hashes == {p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in hashes}
    save(out / 'summary.json', summary)
    (out / 'COMPLETE').write_text('All pairs and backing/counter checks passed.\n')
    return summary
