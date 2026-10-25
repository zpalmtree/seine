#!/usr/bin/env bash
# Generic CPU-only binaries are frozen before transfer; no global ISA flags.
set -euo pipefail
cd /home/ubuntu/campaign
test -f results/SCREEN-COMPLETE
mkdir -p results/portable-regression-build
sha256sum --check portable-x86-binaries.sha256 > results/portable-regression-build/identities.txt
timeout 600 bin/verify-portable-control > results/portable-regression-build/control-full-size-parity.log
timeout 600 bin/verify-portable-candidate > results/portable-regression-build/candidate-full-size-parity.log
touch results/portable-regression-build/COMPLETE
python3 - <<'PY'
from pathlib import Path
import subprocess
from screen import physical_first
memory = {line.split(':')[0]: int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()}
maximum = min(16, len(physical_first()), (memory['MemAvailable'] * 1024 - 5 * 1024**3) // (2 * 1024**3))
assert maximum >= 1
text=Path('results/portable-regression-build/candidate-full-size-parity.log').read_text()
kernels=[kernel for kernel in ['avx2','avx512'] if kernel + ': PASS' in text]
assert kernels
command=['python3','matrix.py','--out','results/portable-regression','--workers',*map(str,sorted({1,maximum})),'--pairs','2','--seconds','20']
for kernel in kernels:
 for variant in ['control','candidate']:
  command += ['--variant',f'{variant}-{kernel}:bin/seine-portable-{variant}:{kernel}']
subprocess.run(command,check=True)
PY
