#!/usr/bin/env python3
"""Real pool startup with 16 CPUs, a finite task budget and one arena's RAM.

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
    parser.add_argument('--tasks', type=int, default=40)
    parser.add_argument('--label', default='pid-pool')
    parser.add_argument('--expect', choices=['success', 'failure'], default='success')
    args = parser.parse_args()
    assert 20 <= args.tasks <= 48 and re.fullmatch(r'[a-z0-9-]+', args.label)
    out = life.ROOT / 'results' / args.label
    out.mkdir(parents=True, exist_ok=False)
    life.OUT = out
    (out / 'hosts').write_text('127.0.0.1 localhost bntpool.com\n::1 localhost\n')
    unit = 'seine-campaign-' + args.label + '.service'
    log = out / 'miner.log'
    server = life.Server(('127.0.0.1', 3333), life.Fixture)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    records = []
    try:
        command = ['sudo', 'systemd-run', '--quiet', '--unit=' + unit, '--uid=ubuntu',
            '--property=Type=exec', '--property=TasksMax=' + str(args.tasks), '--property=MemoryHigh=3G',
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
            active = life.command(['systemctl', 'show', unit, '--property=ActiveState', '--value']).strip()
            if active != 'active':
                assert args.expect == 'failure', (unit, active)
                break
            sample = life.state(unit)
            group = Path('/sys/fs/cgroup') / sample['cgroup'].lstrip('/')
            for field in ['pids.max', 'pids.current', 'pids.events', 'cpuset.cpus.effective']:
                sample[field] = (group / field).read_text().strip()
            assert sample['pids.max'] == str(args.tasks) and sample['cpuset.cpus.effective'] == '0-15'
            assert sample['memory.high'] == str(3 * 1024**3)
            assert int(sample['pids.current']) <= args.tasks and 'oom_kill 0' in sample['memory.events']
            sample['thread_names'] = sorted(p.read_text().strip() for p in
                (Path('/proc') / str(sample['pid']) / 'task').glob('*/comm') if p.exists())
            records.append(sample)
            (out / 'samples.json').write_text(json.dumps(records, indent=2) + '\n')
            time.sleep(2)
        text = log.read_text()
        if args.expect == 'failure':
            assert active != 'active', 'baseline unexpectedly remained active'
            assert any(word in text.lower() for word in ['fatal:', 'panicked', 'failed to spawn', 'resource temporarily unavailable']), text
            result = {'status': 'expected startup failure', 'task_limit': args.tasks, 'log': text}
        else:
            match = re.search(r'capacity=(\d+), worker slots=(\d+), currently available=(\d+)', text)
            assert match, 'no automatic startup budget logged'
            ceiling, slots, available = map(int, match.groups())
            assert ceiling == 16 and 0 < slots < ceiling and available == 1, match.groups()
            hashes = max([0] + [int(n) for n in re.findall(r'\bhashes=(\d+)', text)])
            assert hashes > 0 and 'quarantined' not in text.lower()
            assert max(int(s.get('smaps_rollup', {}).get('Anonymous', '0 kB').split()[0]) for s in records) >= 2 * 1024**2
            result = {'status': 'passed', 'capacity': ceiling, 'worker_slots': slots,
                'available_arenas': available, 'completed_hashes': hashes, 'task_limit': args.tasks,
                'max_tasks': max(int(s['pids.current']) for s in records)}
        result['scope'] = 'full CPU-only plain pool runtime; no TUI/API/accelerators'
        (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    finally:
        life.stop(unit)
        subprocess.run(['sudo', 'systemctl', 'reset-failed', unit], capture_output=True, timeout=30)
        server.shutdown()
        server.server_close()
        (out / 'fixture-events.json').write_text(json.dumps(life.SERVER_EVENTS, indent=2) + '\n')
    (out / 'COMPLETE').write_text('Expected ' + args.expect + ' outcome and owned-unit cleanup verified.\n')


if __name__ == '__main__':
    main()
