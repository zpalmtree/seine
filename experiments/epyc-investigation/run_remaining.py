#!/usr/bin/env python3
"""Finish best-kernel controls, then execute items five through eight in order."""
import json
from pathlib import Path
import subprocess
import time


def command(args):
    print('RUN', args, flush=True)
    subprocess.run(args, check=True)


deadline = time.monotonic() + 1200
while not Path('hybrids-COMPLETE').exists():
    assert time.monotonic() < deadline, 'Hybrid controls did not complete'
    time.sleep(3)
command(['python3', 'run_kernel.py', 'di512-dd256', '--baseline-kernel', 'avx512',
         '--workers', '1', '8', '--out', 'hybrid-vs512-screen'])
Path('hybrids-controls-COMPLETE').write_text('Direct uniform AVX-512 controls measured.\n')
for variant, kernel in [('dd-t1', 'avx512-256'), ('dd-nta', 'avx512-256'),
                        ('dd-t1-512', 'avx512'), ('dd-nta-512', 'avx512')]:
    command(['python3', 'run_kernel.py', variant, '--baseline-kernel', kernel,
             '--candidate-kernel', kernel, '--workers', '8', '24', '--out', variant + '-screen'])
Path('hints-COMPLETE').write_text('Both hints and both vector widths measured.\n')
command(['python3', 'run_kernel.py', 'streaming-ring', '--out', 'streaming-screen'])
command(['python3', 'run_kernel.py', 'quad-rows', '--out', 'quad-screen'])
Path('kernels-COMPLETE').write_text('All isolated kernel screens complete.\n')

cpus = json.loads(Path('phase-cpus.json').read_text())
Path('hardware-cpus.json').write_text(json.dumps({'24': cpus['24']}, indent=2) + '\n')
for event_set in ['core', 'requests', 'fills']:
    command(['python3', 'run_phases.py', '--cpus', 'hardware-cpus.json', '--out', 'pmu-' + event_set,
             '--seconds', '15', '--passes', '1', '--pmu', event_set, '--binary', 'epyc-phase-distinct'])
Path('CAMPAIGN_COMPLETE').write_text('Sequential kernel screens and available core PMCs complete.\n')
