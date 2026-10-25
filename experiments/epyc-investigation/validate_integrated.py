#!/usr/bin/env python3
"""Bounded final validation on a disposable 32-core, 64 GiB Turin guest.

Run from the artifact directory with bench.py, seine, verify_cpu_kernels,
verify_streaming_dispatch, and the optimized Seine test executable (seine-tests).
Reserve 24,576 local 2 MiB HugeTLB pages before starting. This runner never
connects to a pool or daemon. Keep an independent instance termination deadline.
"""
import hashlib
import json
from pathlib import Path
import subprocess
from bench import paired

root = Path.cwd()
identity = json.loads(subprocess.check_output(['lscpu', '-J'], text=True))
cpu = {r['field'].rstrip(':'): r['data'] for r in identity['lscpu']}
assert cpu['Vendor ID'] == 'AuthenticAMD' and cpu['CPU family'] == '26' and cpu['Model'] == '2'
assert cpu['NUMA node(s)'] == '1' and int(cpu['CPU(s)']) >= 24
(root / 'hardware.json').write_text(json.dumps(identity, indent=2) + '\n')
expected = json.loads((root / 'expected-binaries.json').read_text())
for name, digest in expected.items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name


def check(name, args, timeout):
    with (root / (name + '.log')).open('w') as log:
        subprocess.run(args, stdout=log, stderr=subprocess.STDOUT,
                       check=True, timeout=timeout)
    print(name, 'PASS', flush=True)


check('full-size-kernels', ['./verify_cpu_kernels'], 180)
check('full-size-switches', ['./verify_streaming_dispatch'], 180)
check('native-tuner-tests', ['./seine-tests', 'miner::adaptive_', '--test-threads=4'], 120)
for kernel in ['avx2', 'avx512', 'avx512-256']:
    out = root / ('integrated-vs-' + kernel)
    out.mkdir(exist_ok=False)
    result = paired(root, out / 'w24',
                    {'binary': 'seine', 'kernel': kernel},
                    {'binary': 'seine', 'kernel': 'avx512-256-stream'},
                    workers=24, pairs=2, seconds=20, rounds=1, warmup=1)
    print(kernel, json.dumps(result), flush=True)
    minimum = 2 if kernel == 'avx512' else 10
    assert all(delta > minimum for delta in result['paired_deltas_pct']), result

check('productive-selection', ['./seine-tests', '--exact',
      'miner::adaptive_kernel::tests::epyc_streaming_productive_selection',
      '--ignored', '--nocapture', '--test-threads=1'], 950)
assert 'EPYC_PRODUCTIVE_PASS' in (root / 'productive-selection.log').read_text()
for name, digest in expected.items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
(root / 'VALIDATION_COMPLETE').write_text('All integrated comparisons, parity and productive selection passed.\n')
