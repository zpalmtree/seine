#!/usr/bin/env python3
"""Bounded native release-binary screen, with fresh physical-core topology."""
import base64
import hashlib
import json
import math
import statistics
import struct
import subprocess
import time
from pathlib import Path

from windows_bench import ROOT, WIN, WIN_STR, run, save, setup

KERNELS = ['avx2', 'avx512', 'avx512-256', 'avx512-256-stream']


def checked_run(label, cpus, kernel, seconds=10, rounds=1):
    report = run(label, cpus, len(cpus), kernel=kernel, secs=seconds, rounds=rounds)
    fp = report['config_fingerprint']
    assert report['cpu_threads'] == len(cpus) and len(report['runs']) == rounds
    assert fp['cpu_kernel'] == kernel and fp['cpu_page_mode'] == 'regular'
    assert not fp['cpu_autotune_threads'] and fp['cpu_affinity'] == 'os'
    for sample in report['runs']:
        assert sum(b['memory_regular_workers'] for b in sample['backend_runs']) == len(cpus)
        assert sum(b['memory_regular_bytes'] for b in sample['backend_runs']) == len(cpus) * 2 * 1024**3
        for b in sample['backend_runs']:
            for field in ['dropped_events', 'memory_allocation_failures',
                          'assignment_enqueue_timeouts', 'assignment_execution_timeouts',
                          'control_enqueue_timeouts', 'control_execution_timeouts']:
                assert b[field] == 0, (label, field, b[field])
    hps = statistics.mean(r['hashes'] / r['elapsed_secs'] for r in report['runs'])
    print(label, hps, flush=True)
    time.sleep(3)
    return hps


def main():
    assert (WIN / 'NATIVE-PARITY-COMPLETE').exists(), 'Run native full-size parity first'
    setup()
    assert not (ROOT / 'protocol.json').exists(), 'Do not overwrite an earlier run'
    topology_script = Path(__file__).with_name('windows_topology.ps1')
    (WIN / topology_script.name).write_bytes(topology_script.read_bytes())
    subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-File',
                    WIN_STR + '\\' + topology_script.name, '-Output',
                    WIN_STR + '\\topology.json'], check=True, timeout=30)
    topology = json.loads((WIN / 'topology.json').read_text(encoding='utf-8-sig'))
    raw = base64.b64decode(topology['raw'])
    assert len(raw) % 32 == 0 and topology['logical_processors'] == 32
    assert topology['process_affinity'] == 2**32 - 1
    cores, caches = [], []
    for offset in range(0, len(raw), 32):
        mask, relationship = struct.unpack_from('<QI', raw, offset)
        if relationship == 0:
            cores.append(mask)
        elif relationship == 2 and raw[offset + 16] == 3:
            caches.append({'mask': mask, 'bytes': struct.unpack_from('<I', raw, offset + 20)[0]})
    assert sorted(cores) == [3 << (2 * n) for n in range(16)], cores
    assert sorted(caches, key=lambda c: c['mask']) == [
        {'mask': 65535, 'bytes': 96 * 1024**2},
        {'mask': 4294901760, 'bytes': 32 * 1024**2}], caches
    topology.update(cores=cores, l3_caches=caches)
    save(ROOT / 'topology.json', topology)
    cases = {'w8-spread': [0, 2, 4, 6, 16, 18, 20, 22], 'w16-physical': list(range(0, 32, 2))}
    binary_hash = hashlib.sha256((WIN / 'seine.exe').read_bytes()).hexdigest()
    save(ROOT / 'protocol.json', {'source': 'published v0.2.22 Windows MSVC',
         'binary_sha256': binary_hash, 'cases': cases, 'kernels': KERNELS,
         'page_mode': 'regular', 'max_arena_bytes': 32 * 1024**3,
         'screen': 'Two reversed passes; 10s warmup + 10s measured',
         'confirmation': '16 workers, stream vs strongest uniform screen kernel; four AB/BA pairs, 20s warmup + two 20s measured rounds',
         'scope': 'Native Windows, fixed physical CPU masks; no production eligibility changes'})
    started = time.monotonic()
    results = {}
    for case, cpus in cases.items():
        samples = {k: [] for k in KERNELS}
        for number, order in enumerate([KERNELS, list(reversed(KERNELS))], 1):
            for kernel in order:
                assert time.monotonic() - started < 1800, '30-minute native screen deadline'
                samples[kernel].append(checked_run(f'{case}-p{number}-{kernel}', cpus, kernel))
                save(ROOT / f'{case}-samples.json', samples)
        results[case] = {'samples': samples, 'means': {k: statistics.mean(v) for k, v in samples.items()}}
        save(ROOT / 'screen-summary.json', results)
    control = max(KERNELS[:-1], key=lambda k: results['w16-physical']['means'][k])
    pairs = []
    for number in range(1, 5):
        result = {}
        order = [('baseline', control), ('candidate', 'avx512-256-stream')]
        if number % 2 == 0:
            order.reverse()
        for label, kernel in order:
            assert time.monotonic() - started < 1800, '30-minute native screen deadline'
            result[label] = checked_run(f'confirm-p{number}-{label}', cases['w16-physical'], kernel, 20, 2)
        pairs.append(result)
        save(ROOT / 'confirmation-pairs.json', pairs)
    ratios = [p['candidate'] / p['baseline'] for p in pairs]
    save(ROOT / 'result.json', {'control': control, 'screen': results,
         'completed_hps': {label: statistics.mean(p[label] for p in pairs) for label in ['baseline', 'candidate']},
         'paired_deltas_pct': [100 * (r - 1) for r in ratios],
         'paired_geometric_delta_pct': 100 * (math.exp(statistics.mean(map(math.log, ratios))) - 1),
         'scope': 'One native Windows desktop; within-session repeatability only',
         'automatic_eligibility_changed': False})
    assert binary_hash == hashlib.sha256((WIN / 'seine.exe').read_bytes()).hexdigest()
    (ROOT / 'COMPLETE').write_text('Native screen and confirmation passed backing/health/admission checks.\n')


if __name__ == '__main__':
    main()
