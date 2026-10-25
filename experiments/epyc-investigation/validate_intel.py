#!/usr/bin/env python3
"""Fixed-path control for the shipping-default binary on a disposable Ice Lake guest."""
import hashlib
import json
from pathlib import Path
import subprocess
from bench import paired

root = Path.cwd()
identity = json.loads(subprocess.check_output(['lscpu', '-J'], text=True))
cpu = {r['field'].rstrip(':'): r['data'] for r in identity['lscpu']}
assert cpu['Vendor ID'] == 'GenuineIntel' and cpu['Model'] == '106'
(root / 'hardware.json').write_text(json.dumps(identity, indent=2) + '\n')
expected = json.loads((root / 'expected-binaries.json').read_text())
for name, digest in expected.items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
for profile, baseline, candidate in [('cpu-only', 'baseline-cpu', 'seine-cpu'),
                                      ('default', 'baseline', 'seine')]:
    for kernel in ['avx2', 'avx512']:
        result = paired(root, root / ('control-' + profile + '-' + kernel),
                        {'binary': baseline, 'kernel': kernel},
                        {'binary': candidate, 'kernel': kernel},
                        workers=12, pairs=2, seconds=20, rounds=2, warmup=1)
        print(profile, kernel, json.dumps(result), flush=True)
(root / 'VALIDATION_COMPLETE').write_text('Both build profiles and fixed paths completed; inspect deltas before merge.\n')
