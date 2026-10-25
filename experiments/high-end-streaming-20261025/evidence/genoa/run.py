#!/usr/bin/env python3
"""Explicit-kernel Genoa screen and confirmation; no production policy changes.

Place alongside bench.py, the published seine binary, two full-size verifiers,
and expected-binaries.json. Run on a disposable 64 GiB c7a.8xlarge with an
independent shutdown-to-terminate deadline. This process also has a 27-minute
deadline; the caller must collect evidence and verify cloud cleanup separately.
"""
import hashlib
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import time

from bench import paired, run_case, save

ROOT = Path.cwd()
POOL = Path('/proc/sys/vm/nr_hugepages')
KERNELS = ['avx2', 'avx512', 'avx512-256', 'avx512-256-stream']


def check(name, command, timeout):
    with (ROOT / (name + '.log')).open('w') as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                       check=True, timeout=timeout)
    print(name, 'PASS', flush=True)


def set_pool(value):
    subprocess.run(['sudo', 'tee', str(POOL)], input=str(value), text=True,
                   stdout=subprocess.DEVNULL, check=True, timeout=30)
    assert int(POOL.read_text()) == value, 'HugeTLB reservation incomplete'


def memory():
    return {p.name: p.read_text() for p in [Path('/proc/meminfo'), Path('/proc/vmstat')]}


def deadline(_signum, _frame):
    raise TimeoutError('27-minute local experiment deadline reached')


def main():
    started = time.monotonic()
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(27 * 60)
    expected = json.loads((ROOT / 'expected-binaries.json').read_text())
    for name, digest in expected.items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
    hardware = json.loads(subprocess.check_output(['lscpu', '-J'], text=True))
    save(ROOT / 'hardware.json', hardware)
    cpu = {r['field'].rstrip(':'): r['data'] for r in hardware['lscpu']}
    assert cpu['Vendor ID'] == 'AuthenticAMD' and cpu['CPU family'] == '25'
    assert cpu['Model'] == '17' and cpu['NUMA node(s)'] == '1'
    assert int(cpu['CPU(s)']) == 32 and len(os.sched_getaffinity(0)) == 32
    assert int(POOL.read_text()) == 0, 'Do not alter a pre-existing pool'
    save(ROOT / 'memory-before.json', memory())
    check('version', ['./seine', '--version'], 15)
    check('full-size-kernels', ['./verify_cpu_kernels'], 180)
    check('full-size-switches', ['./verify_streaming_dispatch'], 240)
    save(ROOT / 'protocol.json', {
        'baseline': 'published v0.2.22', 'workers': [8, 24],
        'kernels': KERNELS, 'page_mode': 'large', 'arena_bytes_per_worker': 2 * 1024**3,
        'screen': 'Two reversed passes; 10s warmup + 10s measured; completed hashes / elapsed incl fence',
        'confirmation': 'At 24 workers compare stream to strongest mean uniform screen kernel; four AB/BA pairs, 20s warmup + two 20s measured rounds',
        'promotion': 'No automatic policy promotion from this experiment alone',
        'sha256': expected,
    })
    try:
        set_pool(24576)
        save(ROOT / 'memory-reserved.json', memory())
        screen_dir = ROOT / 'screen'
        screen_dir.mkdir(exist_ok=False)
        results = {}
        for workers in [8, 24]:
            samples = {k: [] for k in KERNELS}
            for pass_index, order in enumerate([KERNELS, list(reversed(KERNELS))], 1):
                for kernel in order:
                    value = run_case(ROOT, screen_dir, f'w{workers}-p{pass_index}-{kernel}',
                                     {'binary': 'seine', 'kernel': kernel}, workers, 10, 1, 1)
                    samples[kernel].append(value['completed_hps'])
                    save(screen_dir / f'w{workers}-samples.json', samples)
            results[str(workers)] = {
                'samples': samples,
                'means': {k: statistics.mean(v) for k, v in samples.items()},
            }
            save(ROOT / 'screen-summary.json', results)
        means = results['24']['means']
        control = max(KERNELS[:-1], key=lambda k: means[k])
        # Predeclared control selection; confirmation includes fresh samples.
        summary = paired(ROOT, ROOT / 'confirmation-w24',
                         {'binary': 'seine', 'kernel': control},
                         {'binary': 'seine', 'kernel': 'avx512-256-stream'},
                         workers=24, pairs=4, seconds=20, rounds=2, warmup=1)
        save(ROOT / 'result.json', {
            'control': control, 'confirmation': summary, 'screen': results,
            'wall_seconds': time.monotonic() - started,
            'automatic_eligibility_changed': False,
        })
        for name, digest in expected.items():
            assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
    finally:
        set_pool(0)
        save(ROOT / 'memory-restored.json', memory())
        signal.alarm(0)
    (ROOT / 'COMPLETE').write_text('Parity, screen, confirmation, health and backing checks passed.\n')


if __name__ == '__main__':
    main()
