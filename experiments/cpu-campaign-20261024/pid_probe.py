#!/usr/bin/env python3
"""Network-free backend startup probe under owned user-systemd resource limits.

Requires the ignored pid_budget_startup_probe test from the retained research
patch. Units stay inside agents-tool.slice and have independent 90s deadlines.
No mining work, network connections or global system settings are involved.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--label', required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--expect', choices=['stable-failure', 'all-pass'], required=True)
    args = parser.parse_args()
    assert re.fullmatch(r'[a-z0-9-]+', args.label)
    args.output.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve()
    results = []
    for mode in ['legacy', 'stable']:
        unit = 'seine-campaign-pid-' + args.label + '-' + mode + '.service'
        log = args.output.resolve() / (mode + '.log')
        log.write_text('')
        wrapper = '''import os,json,pathlib,sys
membership=pathlib.Path('/proc/self/cgroup').read_text().strip()
relative=next(line[4:] for line in membership.splitlines() if line.startswith('0::/'))
group=pathlib.Path('/sys/fs/cgroup')/relative
print('PID_CONTEXT '+json.dumps({'affinity':sorted(os.sched_getaffinity(0)),'membership':membership,'pids.max':(group/'pids.max').read_text().strip(),'memory.high':(group/'memory.high').read_text().strip()}),flush=True)
os.execv(sys.argv[1],sys.argv[1:])
'''
        command = ['systemd-run', '--user', '--quiet', '--unit=' + unit, '--slice=agents-tool.slice',
                   '--property=Type=exec', '--property=TasksMax=12', '--property=MemoryHigh=3G',
                   '--property=MemoryMax=4G', '--property=RuntimeMaxSec=90', '--property=TimeoutStopSec=5',
                   '--property=StandardOutput=append:' + str(log), '--property=StandardError=append:' + str(log)]
        if mode == 'legacy':
            command += ['--setenv=SEINE_PID_PROBE_LEGACY=1']
        command += ['taskset', '-c', '0-15', 'python3', '-c', wrapper, str(binary),
                    'miner::adaptive_cpu::tests::pid_budget_startup_probe', '--exact', '--ignored', '--nocapture', '--test-threads=1']
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
            for _ in range(60):
                output = subprocess.check_output(['systemctl', '--user', 'show', unit, '--property=ActiveState',
                                                  '--property=ExecMainStatus', '--property=Result'], text=True)
                state = dict(line.split('=', 1) for line in output.splitlines())
                if state['ActiveState'] not in ('active', 'activating', 'deactivating'):
                    break
                time.sleep(.5)
            else:
                raise RuntimeError('startup probe exceeded 30 seconds')
            text = log.read_text()
            context = json.loads(next(line.removeprefix('PID_CONTEXT ') for line in text.splitlines() if line.startswith('PID_CONTEXT ')))
            assert context['affinity'] == list(range(16)) and context['pids.max'] == '12'
            assert context['memory.high'] == str(3 * 1024**3)
            results.append({'mode': mode, 'state': state, 'context': context, 'log': text, 'source': args.source})
            print(mode, state, flush=True)
        finally:
            subprocess.run(['systemctl', '--user', 'stop', unit], capture_output=True, timeout=15)
            subprocess.run(['systemctl', '--user', 'reset-failed', unit], capture_output=True, timeout=15)
            inactive = subprocess.check_output(['systemctl', '--user', 'show', unit, '--property=ActiveState', '--value'], text=True).strip()
            assert inactive == 'inactive', (unit, inactive)
    (args.output / 'results.json').write_text(json.dumps({'scope': 'backend startup only; no network/work assignments',
        'owned_units_cleanup_verified': True, 'expectation': args.expect, 'results': results}, indent=2) + '\n')
    assert 'startup=Ok(())' in results[0]['log']
    if args.expect == 'stable-failure':
        assert 'failed to spawn CPU worker' in results[1]['log']
    else:
        assert 'startup=Ok(())' in results[1]['log']


if __name__ == '__main__':
    main()
