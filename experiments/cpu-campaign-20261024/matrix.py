#!/usr/bin/env python3
"""Compare explicit CPU implementations in alternating order on an owned VM."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re
import time

from screen import counters, physical_first, run, save, validate


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--variant", action="append", required=True, help="label:binary-path:kernel")
    p.add_argument("--workers", nargs="+", type=int, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--pairs", type=int, default=2)
    p.add_argument("--seconds", type=int, default=12)
    p.add_argument("--rounds", type=int, default=2)
    p.add_argument("--page", choices=["regular", "large"], default="large")
    p.add_argument("--deadline-seconds", type=int, default=5400)
    a = p.parse_args()
    if Path(str(a.out) + ".defer").exists():
        raise SystemExit("Phase deferred by its explicit .defer marker before allocation")
    variants = []
    for item in a.variant:
        label, binary, kernel = item.split(":", 2)
        assert re.fullmatch(r"[a-zA-Z0-9_-]+", label)
        variants.append((label, Path(binary).resolve(), kernel))
    assert len({v[0] for v in variants}) == len(variants)
    assert len(variants) >= 2 and a.pairs >= 2 and a.seconds >= 10 and a.rounds >= 1
    cpus = physical_first()
    assert len(set(a.workers)) == len(a.workers), 'duplicate worker configuration'
    assert min(a.workers) >= 1 and max(a.workers) <= min(16, len(cpus))
    # Just-finished compiler processes can briefly retain memory. Wait only for
    # the fixed headroom contract; do not lower it or forcibly drop OS caches.
    required = max(a.workers) * 2 * 1024**3 + 5 * 1024**3
    for attempt in range(16):
        memory = {line.split(':')[0]: int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()}
        if memory['MemAvailable'] * 1024 >= required:
            break
        if attempt < 15:
            time.sleep(2)
    assert memory['Hugepagesize'] == 2048
    assert memory['MemAvailable'] * 1024 >= required, ('bounded memory headroom unavailable', memory['MemAvailable'], required)
    assert len(Path('/proc/swaps').read_text().splitlines()) == 1
    pool = Path('/proc/sys/vm/nr_hugepages')
    old = int(pool.read_text())
    assert old == 0
    a.out.mkdir(parents=True, exist_ok=False)
    identities = {label: {"binary": str(binary), "kernel": kernel,
                         "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
                  for label, binary, kernel in variants}
    save(a.out / 'experiment.json', {'variants': identities, 'workers': a.workers, 'cpus': cpus,
                                   'page': a.page, 'pairs': a.pairs, 'seconds': a.seconds,
                                   'rounds': a.rounds, 'warmup_rounds': 1})
    records = []
    started = time.monotonic()
    expected_pow = None
    try:
        pages = max(a.workers) * 1024 if a.page == 'large' else 0
        run(['sudo', 'sysctl', '-w', f'vm.nr_hugepages={pages}'], a.out / 'reserve.log')
        assert int(pool.read_text()) == pages
        for repeat in range(a.pairs):
            workers_order = a.workers if repeat % 2 == 0 else list(reversed(a.workers))
            for workers in workers_order:
                # Rotation belongs to the configuration, not its position in
                # the reversed outer loop; otherwise two reversals cancel.
                index = a.workers.index(workers)
                order = variants[index % len(variants):] + variants[:index % len(variants)]
                if repeat % 2:
                    order = list(reversed(order))
                for variant, binary, kernel in order:
                    assert time.monotonic() - started < a.deadline_seconds
                    label = f'r{repeat+1}-w{workers}-{variant}'
                    before = counters()
                    run(['taskset', '-c', ','.join(map(str, cpus[:workers])), str(binary),
                         '--bench', '--backend', 'cpu', '--threads', str(workers),
                         '--disable-cpu-autotune-threads', '--cpu-affinity', 'os',
                         '--cpu-page-mode', a.page, '--bench-cpu-kernel', kernel,
                         '--bench-kind', 'backend', '--bench-secs', str(a.seconds),
                         '--bench-rounds', str(a.rounds), '--bench-warmup-rounds', '1',
                         '--ui', 'plain', '--data-dir', str(a.out / 'config'),
                         '--bench-output', str(a.out / (label + '.json'))], a.out / (label + '.log'))
                    after = counters()
                    delta = {key: after[key] - before[key] for key in before}
                    assert all(delta[k] == 0 for k in ('pswpin', 'pswpout', 'oom_kill')), delta
                    assert 100 * delta['cpu_steal'] / max(1, delta['cpu_total']) < 2
                    report = json.loads((a.out / (label + '.json')).read_text())
                    if expected_pow is None:
                        expected_pow = report['pow_fingerprint']
                    assert report['pow_fingerprint'] == expected_pow
                    rate = validate(report, workers, kernel, a.page)
                    records.append({'label': label, 'repeat': repeat+1, 'workers': workers,
                                    'variant': variant, 'kernel': kernel, 'completed_hps': rate, 'vm_delta': delta})
                    save(a.out / 'results.json', records)
                    print(datetime.datetime.now(datetime.timezone.utc).isoformat(), label, f'{rate:.6f} H/s', flush=True)
                    time.sleep(2)
    finally:
        run(['sudo', 'sysctl', '-w', f'vm.nr_hugepages={old}'], a.out / 'release.log')
        save(a.out / 'results.json', records)
    for label, binary, _ in variants:
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == identities[label]['sha256']
    for workers in a.workers:
        previous = None
        for repeat in range(1, a.pairs + 1):
            actual = [r['variant'] for r in records if r['workers'] == workers and r['repeat'] == repeat]
            assert len(actual) == len(variants)
            if previous is not None:
                assert actual == list(reversed(previous)), (workers, repeat, actual, previous)
            previous = actual
    save(a.out / 'order-verification.json', {'per_worker_order_reversed_each_pass': True,
        'source': 'actual completed record sequence', 'workers': a.workers, 'passes': a.pairs})
    (a.out / 'COMPLETE').write_text('Alternating measurements and health checks completed.\n')


if __name__ == '__main__':
    main()
