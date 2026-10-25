#!/usr/bin/env python3
"""Fixed-worker NUMA allocation experiment on a disposable two-node host."""
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parent
OUT = ROOT / 'numa-results'


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def counters():
    vm = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
    cpu = list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:]))
    return {k: int(vm[k]) for k in ('pswpin', 'pswpout', 'oom_kill')} | {
        'ticks': sum(cpu[:8]), 'steal': cpu[7]}


def run(name, policy, mode='large', seconds=20, rounds=2, warmup=1, failure=False, expected_fallbacks=0):
    report_path = OUT / (name + '.json')
    log_path = OUT / (name + '.log')
    env = dict(os.environ, SEINE_EXPERIMENT_ARENA_POLICY=policy)
    command = [str(ROOT / 'seine-arena-audit'), '--bench', '--bench-kind', 'backend',
               '--backend', 'cpu', '--threads', '8', '--cpu-page-mode', mode,
               '--cpu-affinity', 'auto', '--bench-cpu-kernel', 'avx2',
               '--disable-cpu-autotune-threads', '--ui', 'plain',
               '--bench-secs', str(seconds), '--bench-rounds', str(rounds),
               '--bench-warmup-rounds', str(warmup), '--bench-output', str(report_path),
               '--data-dir', str(ROOT / 'config')]
    before = counters()
    started = time.monotonic()
    with log_path.open('w') as log:
        result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=180)
    delta = {k: v - before[k] for k, v in counters().items()}
    checks = {'exit': result.returncode, 'seconds': time.monotonic() - started,
              'counter_delta': delta, 'steal_pct': 100 * delta['steal'] / max(1, delta['ticks'])}
    save(OUT / (name + '-checks.json'), checks)
    assert all(delta[k] == 0 for k in ('pswpin', 'pswpout', 'oom_kill'))
    assert checks['steal_pct'] < 2
    if failure:
        assert result.returncode != 0 and not report_path.exists()
        assert 'page-mode arena allocation failed' in log_path.read_text()
        print(name, 'expected allocation failure', flush=True)
        return None
    assert result.returncode == 0
    report = json.loads(report_path.read_text())
    assert report['cpu_threads'] == 8
    assert report['config_fingerprint']['cpu_kernel'] == 'avx2'
    assert report['config_fingerprint']['cpu_page_mode'] == mode
    assert report['config_fingerprint']['cpu_affinity'] == 'auto'
    assert report['config_fingerprint']['cpu_autotune_threads'] is False
    assert len(report['runs']) == rounds
    fields = ('memory_explicit_large', 'memory_explicit_large_1g',
              'memory_transparent_huge', 'memory_regular', 'memory_heap')
    selected = fields if mode == 'auto' else (('memory_explicit_large',) if mode == 'large' else ('memory_regular',))
    for sample in report['runs']:
        assert sum(b[field + '_workers'] for b in sample['backend_runs'] for field in selected) == 8
        assert sum(b[field + '_bytes'] for b in sample['backend_runs'] for field in selected) == 16 * 1024**3
        assert sum(b['memory_allocation_failures'] for b in sample['backend_runs']) == expected_fallbacks
        if mode == 'auto':
            # Benchmark counters are interval deltas after warmup. Startup
            # HugeTLB fallback is established by measured backing, not this
            # interval's allocation-failure count.
            expected_huge = 4 if policy == 'local' else 8
            expected_thp = 4 if policy == 'local' else 0
            for field, count in (('memory_explicit_large', expected_huge), ('memory_transparent_huge', expected_thp)):
                assert sum(b[field + '_workers'] for b in sample['backend_runs']) == count
                assert sum(b[field + '_bytes'] for b in sample['backend_runs']) == count * 2 * 1024**3
        for b in sample['backend_runs']:
            for key in ('dropped_events',
                        'assignment_enqueue_timeouts', 'assignment_execution_timeouts',
                        'control_enqueue_timeouts', 'control_execution_timeouts'):
                assert b[key] == 0, (key, b[key])
    arenas = [json.loads(line.removeprefix('EPYC_ARENA ')) for line in log_path.read_text().splitlines()
              if line.startswith('EPYC_ARENA ')]
    assert len(arenas) == 8 and {a['worker'] for a in arenas} == set(range(8))
    for a in arenas:
        assert a['query_result'] == 0 and a['query_error'] is None
        assert a['bytes'] == 2 * 1024**3
        assert sum(a['samples_by_node'].values()) == 1024
        assert all(int(node) >= 0 for node in a['samples_by_node'])
        if policy in ('local', 'remote'):
            assert a['samples_by_node'] == {str(a['target_node']): 1024}
    save(OUT / (name + '-arenas.json'), arenas)
    rates = {'counted_hps': report['avg_hps'],
             'completed_hps': statistics.mean(r['hashes'] / r['elapsed_secs'] for r in report['runs']),
             'local_fraction': sum(a['samples_by_node'].get(str(a['local_node']), 0) for a in arenas) / (8 * 1024),
             'backing': {f: report['runs'][0]['backend_runs'][0][f + '_workers'] for f in fields}}
    print(name, json.dumps(rates), flush=True)
    time.sleep(2)
    return rates


def main():
    OUT.mkdir(exist_ok=False)
    assert len(Path('/proc/swaps').read_text().splitlines()) == 1
    nodes = sorted(Path('/sys/devices/system/node').glob('node[0-9]*'))
    assert len(nodes) == 2
    pools = [node / 'hugepages/hugepages-2048kB/nr_hugepages' for node in nodes]
    original = [int(p.read_text()) for p in pools]
    assert original == [0, 0]
    digest = hashlib.sha256((ROOT / 'seine-arena-audit').read_bytes()).hexdigest()
    save(OUT / 'protocol.json', {'binary_sha256': digest, 'kernel': 'avx2', 'affinity': 'auto',
         'workers': 8, 'paired_seconds': 20, 'paired_rounds': 2, 'warmup_rounds': 1,
         'order': 'alternating', 'samples_per_arena': 1024, 'sample_stride_bytes': 2097152})
    for cmd, filename in [(['lscpu', '-J'], 'lscpu.json'), (['numactl', '--hardware'], 'numa.txt'),
                          (['lscpu', '-e=CPU,NODE,SOCKET,CORE,ONLINE'], 'cpu-layout.txt')]:
        (OUT / filename).write_text(subprocess.check_output(cmd, text=True))
    def reserve(values):
        for p in pools:
            p.write_text('0\n')
            assert int(p.read_text()) == 0
        for p, value in zip(pools, values):
            p.write_text(str(value) + '\n')
            assert int(p.read_text()) == value
    results = {}
    try:
        reserve([8192, 8192])
        # First verify the full-size reference before any timed comparisons.
        with (OUT / 'verify.log').open('w') as log:
            subprocess.run([str(ROOT / 'verify')], stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
        for pair in range(1, 4):
            for policy in (('audit', 'local') if pair % 2 else ('local', 'audit')):
                name = f'balanced-{pair}-{policy}'
                results[name] = run(name, policy)
        save(OUT / 'rates.json', results)
        for pair in (1, 2):
            for policy in (('audit', 'remote') if pair % 2 else ('remote', 'audit')):
                name = f'remote-control-{pair}-{policy}'
                results[name] = run(name, policy, seconds=12, rounds=1)
        save(OUT / 'rates.json', results)
        reserve([8192, 0])
        results['imbalanced-audit'] = run('imbalanced-audit', 'audit', seconds=12, rounds=1)
        run('imbalanced-local', 'local', seconds=1, rounds=1, warmup=0, failure=True)
        reserve([0, 0])
        # Allocation mechanics control for ordinary first-touch pages.
        for policy in ('audit', 'local'):
            name = 'regular-' + policy
            results[name] = run(name, policy, mode='regular', seconds=12, rounds=1)
        save(OUT / 'rates.json', results)
    finally:
        reserve(original)
    assert digest == hashlib.sha256((ROOT / 'seine-arena-audit').read_bytes()).hexdigest()
    ratios = [results[f'balanced-{i}-local']['completed_hps'] / results[f'balanced-{i}-audit']['completed_hps'] for i in (1, 2, 3)]
    save(OUT / 'summary.json', {'balanced_local_vs_default_pct': 100 * (math.exp(statistics.mean(map(math.log, ratios))) - 1),
                               'paired_deltas_pct': [100 * (r - 1) for r in ratios], 'rates': results})
    (OUT / 'COMPLETE').write_text('NUMA allocation experiment passed all placement and counter checks.\n')


if __name__ == '__main__':
    main()
