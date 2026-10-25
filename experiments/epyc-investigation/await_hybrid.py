#!/usr/bin/env python3
"""First hybrid control: change only the DI half of the 256-bit baseline."""
from pathlib import Path
import subprocess
import time

deadline = time.monotonic() + 900
while not Path('phase-results/COMPLETE').exists():
    assert time.monotonic() < deadline, 'Phase diagnostics did not complete'
    time.sleep(3)
subprocess.run(['python3', 'run_kernel.py', 'di512-dd256',
                '--out', 'hybrid-512-256-screen'], check=True)
