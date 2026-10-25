#!/usr/bin/env python3
"""Pinned phase diagnostics, sequential preparation and synchronized starts.

CPU lists come from the backend's actual EPYC_ARENA placement traces. Each child
validates a full-size RustCrypto reference, then waits. Serial preparation avoids
allocating many ordinary reference arenas alongside the reserved HugeTLB pool.
"""
import argparse
import json
from pathlib import Path
import selectors
import subprocess
import time
from bench import counters, save


def run(root, out, cpus, kernel, seconds, pmu=None, binary='epyc-phase'):
    procs = []
    logs = []
    groups = []
    try:
        for cpu in cpus:
            p = subprocess.Popen([str(root / binary), kernel, str(cpu), 'large', str(seconds)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                bufsize=1)
            procs.append(p)
            with selectors.DefaultSelector() as selector:
                selector.register(p.stdout, selectors.EVENT_READ)
                assert selector.select(timeout=120), f'CPU {cpu} failed to become ready'
            line = p.stdout.readline()
            logs.append(line)
            assert line.startswith('READY ') and 'parity=PASS' in line, line
        if pmu:
            from pmu import Group, EVENTS
            for p in procs:
                groups.append(Group(p.pid, EVENTS[pmu]))
            for group in groups:
                group.enable()
        before = counters()
        start = time.monotonic()
        for p in procs:
            p.stdin.write('START\n')
            p.stdin.flush()
        values = []
        for p in procs:
            text, _ = p.communicate(timeout=seconds + 60)
            logs.append(text)
            assert p.returncode == 0, text
            values.append(json.loads(text.strip()))
        delta = {k: v - before[k] for k, v in counters().items()}
        assert all(delta[k] == 0 for k in ('pswpin', 'pswpout', 'oom_kill'))
        assert delta['steal'] / max(1, delta['ticks']) < .02
        hashes = sum(v['hashes'] for v in values)
        result = {'kernel': kernel, 'binary': binary, 'cpus': cpus, 'seconds': seconds, 'values': values,
                  'delta': delta, 'wall': time.monotonic() - start,
                  'phase_seconds_per_hash': {k: sum(v[k] for v in values) / hashes
                                             for k in ('seed', 'di', 'dd', 'finalize')},
                  'scope': 'Loop diagnostic, not production backend throughput'}
        if pmu:
            result['pmu'] = {'set': pmu, 'workers': [group.read() for group in groups]}
        save(out.with_suffix('.json'), result)
        print(out.name, result['phase_seconds_per_hash'], flush=True)
    finally:
        for group in groups:
            group.close()
        out.with_suffix('.log').write_text(''.join(logs))
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--cpus', type=Path, required=True)
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--seconds', type=int, default=15)
    ap.add_argument('--pmu', choices=['core', 'requests', 'fills'])
    ap.add_argument('--passes', type=int, default=2)
    ap.add_argument('--binary', default='epyc-phase')
    args = ap.parse_args()
    args.out.mkdir(exist_ok=False)
    placements = json.loads(args.cpus.read_text())
    for workers, cpus in placements.items():
        assert len(cpus) == int(workers) and len(set(cpus)) == len(cpus)
        for iteration, order in enumerate((['avx2', 'avx512', 'avx512-256'],
                                          ['avx512-256', 'avx512', 'avx2'])[:args.passes], 1):
            for kernel in order:
                run(Path.cwd(), args.out / f'w{workers}-{iteration}-{kernel}', cpus, kernel, args.seconds, args.pmu, args.binary)
    (args.out / 'COMPLETE').write_text('All phase diagnostics passed.\n')
