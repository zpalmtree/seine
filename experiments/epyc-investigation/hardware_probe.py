#!/usr/bin/env python3
"""Bounded, read-only visibility inventory; never changes firmware or governors."""
import json
from pathlib import Path
import subprocess

out = {'limits': ['Guest topology is not physical socket/channel topology',
                  'Guest firmware strings and CPU MHz do not prove host operating settings']}
for name, command in {
    'numa': ['numactl', '--hardware'],
    'cpu': ['lscpu', '-J'],
    'perf_version': ['perf', '--version'],
}.items():
    p = subprocess.run(command, capture_output=True, text=True, timeout=10)
    out[name] = {'status': p.returncode, 'stdout': p.stdout, 'stderr': p.stderr}
out['pmus'] = sorted(p.name for p in Path('/sys/bus/event_source/devices').iterdir())
out['cpu0_cpufreq'] = {}
for field in ['scaling_driver', 'scaling_governor', 'scaling_cur_freq',
              'cpuinfo_cur_freq', 'cpuinfo_min_freq', 'cpuinfo_max_freq']:
    p = Path('/sys/devices/system/cpu/cpu0/cpufreq') / field
    out['cpu0_cpufreq'][field] = p.read_text().strip() if p.exists() else None
for field in ['bios_vendor', 'bios_version', 'product_name', 'sys_vendor']:
    p = Path('/sys/class/dmi/id') / field
    out[field] = p.read_text().strip() if p.exists() else None
out['edac_controllers'] = [p.name for p in Path('/sys/devices/system/edac/mc').glob('mc[0-9]*')]
out['guest_cpu_mhz'] = [line.split(':', 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines()
                        if line.startswith('cpu MHz')]
Path('hardware-visibility.json').write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps({k: out[k] for k in ['pmus', 'cpu0_cpufreq', 'edac_controllers']}))
