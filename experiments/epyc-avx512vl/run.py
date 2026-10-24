#!/usr/bin/env python3
"""Fixed-worker, paired backend comparison on a disposable Linux EPYC host.

Requires bin/{baseline,candidate,verify-baseline,verify-candidate} and sudo.
Both binaries use explicit avx512 selection; their implementation and SHA-256
identities differ. The candidate's implementation is experimental AVX-512VL.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parent
OUT = ROOT / 'results'


def save(path, obj):
    path.write_text(json.dumps(obj, indent=2) + '\n')


def counters():
    vm = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
    cpu = [int(v) for v in Path('/proc/stat').read_text().splitlines()[0].split()[1:]]
    return {k: int(vm[k]) for k in ('pswpin', 'pswpout', 'oom_kill')} | {
        'cpu_total': sum(cpu[:8]), 'cpu_steal': cpu[7]}


def run(command, name, timeout=180):
    before = counters()
    start = time.monotonic()
    with (OUT / (name + '.log')).open('w') as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                       check=True, timeout=timeout)
    after = counters()
    delta = {k: after[k] - before[k] for k in before}
    checks = {'wall_secs': time.monotonic() - start, 'delta': delta,
              'steal_pct': 100 * delta['cpu_steal'] / max(1, delta['cpu_total'])}
    save(OUT / (name + '-checks.json'), checks)
    assert all(delta[k] == 0 for k in ('pswpin', 'pswpout', 'oom_kill')), checks
    assert checks['steal_pct'] < 2, checks


def validate(report, workers, mode, kernel='avx512', rounds=2):
    assert report['cpu_threads'] == workers
    assert report['config_fingerprint']['cpu_kernel'] == kernel
    assert report['config_fingerprint']['cpu_page_mode'] == mode
    assert report['config_fingerprint']['cpu_autotune_threads'] is False
    assert len(report['runs']) == rounds
    for sample in report['runs']:
        backends = sample['backend_runs']
        field = 'memory_explicit_large_workers' if mode == 'large' else 'memory_regular_workers'
        assert sum(b[field] for b in backends) == workers
        byte_field = field.replace('_workers', '_bytes')
        assert sum(b[byte_field] for b in backends) == workers * 2 * 1024**3
        for backend in backends:
            for key in ('memory_allocation_failures', 'dropped_events',
                        'assignment_enqueue_timeouts', 'assignment_execution_timeouts',
                        'control_enqueue_timeouts', 'control_execution_timeouts'):
                assert backend[key] == 0, (key, backend[key])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate-label', default='256-bit AVX-512VL paired rounds')
    parser.add_argument('--baseline-label')
    parser.add_argument('--baseline-kernel', choices=['avx2', 'avx512'], default='avx512')
    parser.add_argument('--cells', nargs='+', default=['large:1', 'large:4', 'regular:4'])
    args = parser.parse_args()
    cells = [(mode, int(workers)) for mode, workers in (cell.split(':') for cell in args.cells)]
    assert all(mode in ('large', 'regular') and workers in (1, 4) for mode, workers in cells)
    OUT.mkdir(exist_ok=False)
    flags = Path('/proc/cpuinfo').read_text().split('flags', 1)[1].split('\n', 1)[0].split()
    assert {'avx512f', 'avx512vl', 'avx2'}.issubset(flags)
    assert len(Path('/proc/swaps').read_text().splitlines()) == 1
    paths = list((ROOT / 'bin').iterdir())
    identities = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    save(OUT / 'binary-sha256.json', identities)
    save(OUT / 'experiment.json', {'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
         'baseline': args.baseline_label or ('existing ' + args.baseline_kernel),
         'candidate': args.candidate_label, 'cells': cells,
         'baseline_kernel': args.baseline_kernel, 'candidate_kernel': 'avx512',
         'pairs': 3, 'seconds': 20, 'rounds': 2, 'warmup_rounds': 1, 'affinity': 'auto', 'autotune': False})
    for command, filename in [(['lscpu', '-J'], 'lscpu.json'),
                               (['lscpu', '-e=CPU,NODE,SOCKET,CORE,ONLINE'], 'topology.txt')]:
        (OUT / filename).write_text(subprocess.check_output(command, text=True))
    for name in ('cpuinfo', 'meminfo'):
        (OUT / (name + '.txt')).write_text(Path('/proc', name).read_text())
    for variant in ('baseline', 'candidate'):
        run([str(ROOT / 'bin' / ('verify-' + variant))], 'verify-' + variant, 300)
    old_pages = int(Path('/proc/sys/vm/nr_hugepages').read_text())
    assert old_pages == 0
    try:
        for mode, workers in cells:
            pages = workers * 1024 if mode == 'large' else 0
            subprocess.run(['sudo', 'sysctl', '-w', f'vm.nr_hugepages={pages}'], check=True)
            assert int(Path('/proc/sys/vm/nr_hugepages').read_text()) == pages
            for pair in range(1, 4):
                variants = ('baseline', 'candidate') if pair % 2 else ('candidate', 'baseline')
                for variant in variants:
                    name = f'{mode}-t{workers}-{pair}-{variant}'
                    kernel = args.baseline_kernel if variant == 'baseline' else 'avx512'
                    path = OUT / (name + '.json')
                    command = [str(ROOT / 'bin' / variant), '--bench', '--bench-kind', 'backend',
                               '--backend', 'cpu', '--threads', str(workers), '--cpu-page-mode', mode,
                               '--cpu-affinity', 'auto', '--bench-cpu-kernel', kernel,
                               '--disable-cpu-autotune-threads', '--ui', 'plain',
                               '--bench-secs', '20', '--bench-rounds', '2', '--bench-warmup-rounds', '1',
                               '--bench-output', str(path), '--data-dir', str(ROOT / 'config')]
                    run(command, name)
                    report = json.loads(path.read_text())
                    validate(report, workers, mode, kernel)
                    print(name, report['avg_hps'], flush=True)
                    time.sleep(2)
    finally:
        subprocess.run(['sudo', 'sysctl', '-w', f'vm.nr_hugepages={old_pages}'], check=True)
    assert identities == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    (OUT / 'COMPLETE').write_text('All paired backend and full-size correctness checks passed.\n')


if __name__ == '__main__':
    main()
