#!/usr/bin/env python3
"""Read-only Linux NUMA inventory. No reservation, pinning, or pool changes."""
import json
import os
from pathlib import Path
import platform
import subprocess


def cpu_list(text):
    result = set()
    for part in text.strip().split(','):
        values = [int(x) for x in part.split('-')]
        if len(values) == 1:
            result.add(values[0])
        elif len(values) == 2 and 0 <= values[0] <= values[1] < 1048576:
            result.update(range(values[0], values[1] + 1))
        else:
            raise ValueError(f'invalid CPU/node range: {part}')
    return sorted(result)


def capture():
    allowed = sorted(os.sched_getaffinity(0))
    status = Path('/proc/self/status').read_text()
    mems = cpu_list(next(line.split(':', 1)[1] for line in status.splitlines()
                         if line.startswith('Mems_allowed_list:')))
    cpus = []
    for cpu in allowed:
        root = Path(f'/sys/devices/system/cpu/cpu{cpu}')
        nodes = [int(p.name[4:]) for p in root.glob('node[0-9]*')]
        cpus.append({'cpu': cpu, 'socket': int((root / 'topology/physical_package_id').read_text()),
                     'core': int((root / 'topology/core_id').read_text()),
                     'nodes': nodes, 'siblings': cpu_list((root / 'topology/thread_siblings_list').read_text())})
    nodes = {}
    for node in mems:
        root = Path(f'/sys/devices/system/node/node{node}')
        pools = {}
        for pool in root.glob('hugepages/hugepages-*kB'):
            pools[pool.name] = {name: int((pool / name).read_text())
                               for name in ('nr_hugepages', 'free_hugepages', 'surplus_hugepages')}
        nodes[str(node)] = {'cpus': cpu_list((root / 'cpulist').read_text()) if (root / 'cpulist').read_text().strip() else [],
                            'distance': [int(x) for x in (root / 'distance').read_text().split()],
                            'meminfo': (root / 'meminfo').read_text(), 'pools': pools}
    def optional(path):
        path = Path(path)
        return path.read_text().strip() if path.exists() else None
    return {'schema': 'seine-numa-inventory/v1', 'kernel': platform.release(),
            'wsl': 'microsoft' in platform.release().lower(),
            'lscpu': json.loads(subprocess.check_output(['lscpu', '-J'], text=True)),
            'allowed_cpus': allowed, 'allowed_memory_nodes': mems, 'cpus': cpus,
            'nodes': nodes, 'meminfo': Path('/proc/meminfo').read_text(),
            'cgroup': Path('/proc/self/cgroup').read_text(),
            'numa_balancing': optional('/proc/sys/kernel/numa_balancing'),
            'transparent_hugepage': optional('/sys/kernel/mm/transparent_hugepage/enabled')}


if __name__ == '__main__':
    print(json.dumps(capture(), indent=2))
