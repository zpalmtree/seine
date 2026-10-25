#!/usr/bin/env python3
"""Opposite hybrid direction, compared with the stronger uniform control."""
from pathlib import Path
import subprocess
import time

deadline = time.monotonic() + 600
while not Path('hybrid-512-256-screen/COMPLETE').exists():
    assert time.monotonic() < deadline, 'First hybrid did not complete'
    time.sleep(3)
for workers, kernel in [(1, 'avx512'), (24, 'avx512-256')]:
    subprocess.run(['python3', 'run_kernel.py', 'di256-dd512', '--baseline-kernel', kernel,
                    '--workers', str(workers), '--out', f'hybrid-256-512-w{workers}'], check=True)
Path('hybrids-COMPLETE').write_text('Both directions measured.\n')
