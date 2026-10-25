#!/usr/bin/env python3
"""Long confirmation against experimental and shipping controls, then cleanup prep."""
import datetime
import json
from pathlib import Path
import subprocess
import time
from run_phases import run


def command(args):
    print('RUN', args, flush=True)
    subprocess.run(args, check=True)


deadline = time.monotonic() + 1000
while not Path('CAMPAIGN_COMPLETE').exists():
    assert time.monotonic() < deadline, 'Sequential screens/profiles did not complete'
    time.sleep(3)
for control in ['avx512-256', 'avx2']:
    command(['python3', 'run_kernel.py', 'streaming-ring', '--baseline-kernel', control,
             '--workers', '24', '--pairs', '3', '--seconds', '20', '--rounds', '2',
             '--out', 'streaming-confirm-' + control])
command(['python3', 'run_kernel.py', 'quad-rows', '--baseline-kernel', 'avx512',
         '--workers', '1', '8', '--out', 'quad-vs512-screen'])

# Leave at least three minutes before the VM's independent 11:14 expiry for
# artifact export and resource deletion. Additional traffic counters are optional.
remaining = (datetime.datetime(2026, 10, 24, 11, 11, tzinfo=datetime.timezone.utc)
             - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
if remaining >= 180:
    cpus = json.loads(Path('phase-cpus.json').read_text())['24']
    out = Path('pmu-streaming-stores')
    out.mkdir(exist_ok=False)
    for variant in ['baseline', 'streaming-ring']:
        run(Path.cwd(), out / variant, cpus, 'avx512-256', 15, 'stores',
            f'loops/{variant}/epyc-loop')
else:
    Path('optional-store-counters-skipped.json').write_text(json.dumps({
        'reason': 'Preserve artifact export and cleanup time', 'seconds_to_export_deadline': remaining}) + '\n')
command(['python3', 'hardware_probe.py'])
Path('CONFIRMATION_COMPLETE').write_text('Long streaming comparisons and stronger quad controls complete.\n')
