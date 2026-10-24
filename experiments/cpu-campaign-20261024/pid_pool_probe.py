#!/usr/bin/env python3
"""Real pool startup with 16 CPUs, 24 tasks and one arena's RAM budget.

Only for a disposable campaign guest. Reuses the loopback-only Stratum fixture;
the backend-only 12-task reproducer separately tests post-preflight spawn races.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess
import threading
import time

import resource_lifecycle as life


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    args = parser.parse_args()
    out = life.ROOT / 'results/pid-pool'
    out.mkdir(parents=True, exist_ok=False)
    life.OUT = out
    (out / 'hosts').write_text('127.0.0.1 localhost bntpool.com\n::1 localhost\n')
    unit = 'seine-campaign-pid-pool.service'
    log = out / 'miner.log'
    server = life.Server(('127.0.0.1', 3333), life.Fixture)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    records = []
    try:
        command = ['sudo', 'systemd-run', '--quiet', '--unit=' + unit, '--uid=ubuntu',
            '--property=Type=exec', '--property=TasksMax=24', '--property=MemoryHigh=3G',
            '--property=MemoryMax=4G', '--property=AllowedCPUs=0-15',
            '--property=RuntimeMaxSec=100', '--property=TimeoutStopSec=15',
            '--property=KillSignal=SIGINT', '--property=IPAddressDeny=any',
            '--property=IPAddressAllow=localhost',
            '--property=BindReadOnlyPaths=' + str(out / 'hosts') + ':/etc/hosts',
            '--property=StandardOutput=append:' + str(log),
            '--property=StandardError=append:' + str(log),
            'taskset', '-c', '0-15', str(args.binary.resolve()),
            '--mode', 'pool', '--pool-url', '127.0.0.1:3333', '--address', life.fixture_address(),
            '--backend', 'cpu', '--cpu-page-mode', 'regular', '--cpu-affinity', 'auto',
            '--cpu-autotune-config', str(out / 'tuning.json'), '--data-dir', str(out / 'config'),
            '--ui', 'plain', '--stats-secs', '5']
        life.command(command)
        for _ in range(30):
            assert life.command(['systemctl', 'show', unit, '--property=ActiveState', '--value']).strip() == 'active'
            sample = life.state(unit)
            group = Path('/sys/fs/cgroup') / sample['cgroup'].lstrip('/')
            for field in ['pids.max', 'pids.current', 'pids.events', 'cpuset.cpus.effective']:
                sample[field] = (group / field).read_text().strip()
            assert sample['pids.max'] == '24' and sample['cpuset.cpus.effective'] == '0-15'
            assert sample['memory.high'] == str(3 * 1024**3)
            assert int(sample['pids.current']) <= 24 and 'oom_kill 0' in sample['memory.events']
            records.append(sample)
            (out / 'samples.json').write_text(json.dumps(records, indent=2) + '\n')
            time.sleep(2)
        text = log.read_text()
        match = re.search(r'capacity=(\d+), worker slots=(\d+), currently available=(\d+)', text)
        assert match, 'no automatic startup budget logged'
        ceiling, slots, available = map(int, match.groups())
        assert ceiling == 16 and 0 < slots < ceiling and available == 1, match.groups()
        hashes = max([0] + [int(n) for n in re.findall(r'\bhashes=(\d+)', text)])
        assert hashes > 0 and 'quarantined' not in text.lower()
        assert max(int(s.get('smaps_rollup', {}).get('Anonymous', '0 kB').split()[0]) for s in records) >= 2 * 1024**2
        (out / 'result.json').write_text(json.dumps({'capacity': ceiling, 'worker_slots': slots,
            'available_arenas': available, 'completed_hashes': hashes, 'max_tasks': max(int(s['pids.current']) for s in records),
            'scope': 'full CPU-only plain pool runtime; no TUI/API/accelerators'}, indent=2) + '\n')
    finally:
        life.stop(unit)
        subprocess.run(['sudo', 'systemctl', 'reset-failed', unit], capture_output=True, timeout=30)
        server.shutdown()
        server.server_close()
        (out / 'fixture-events.json').write_text(json.dumps(life.SERVER_EVENTS, indent=2) + '\n')
    (out / 'COMPLETE').write_text('Real pool startup, productive mining, bounded tasks/RAM, cleanup passed.\n')


if __name__ == '__main__':
    main()
