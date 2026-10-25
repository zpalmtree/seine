#!/usr/bin/env python3
"""Advance only after the preceding isolated prefetch experiment completed."""
from pathlib import Path
import subprocess
import time

deadline = time.monotonic() + 420
while not Path('early512-screen/COMPLETE').exists():
    assert time.monotonic() < deadline, 'Preceding experiment did not complete'
    time.sleep(3)
subprocess.run(['python3', 'run_phases.py', '--cpus', 'phase-cpus.json',
                '--out', 'phase-results', '--seconds', '15', '--passes', '1', '--pmu', 'core'], check=True)
