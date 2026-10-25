#!/usr/bin/env python3
"""Start bounded joint-policy confirmation on a fresh, already-owned C5 lease."""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import shlex

import cloud


# Executed only on the explicitly selected disposable guest, never on import.
RUNNER = r'''import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

ROOT = Path('/home/ubuntu/campaign')
os.chdir(ROOT)
OUT = ROOT / 'results'
CUTOFF = dt.datetime(2026, 10, 24, 19, 45, tzinfo=dt.timezone.utc)
plan = json.loads((ROOT / 'joint-plan.json').read_text())
expiry = dt.datetime.fromisoformat(plan['lease_expires_at'])
wall_end = min(CUTOFF, expiry)
started = time.monotonic()
units = set()
stages = []
child = None
pool_owned = False
status = 'FAILED'
reason = None

def save(name, value):
    path = OUT / name
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)

def remaining():
    return min((wall_end - dt.datetime.now(dt.timezone.utc)).total_seconds(),
               5400 - (time.monotonic() - started))

class Deferred(Exception):
    pass

def interrupted(signum, frame):
    raise InterruptedError(signum)

def halt_child():
    global child
    if child is not None:
        # Each child owns its process group; detached systemd units are stopped
        # independently below. Kill the group even if its leader just exited.
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=5)
        child = None

def stage(name, command, seconds, owned_units=()):
    global child
    # Includes bounded child interruption, unit/pool cleanup and result archive.
    if remaining() < seconds + 240:
        raise Deferred(f'{name}: insufficient deadline/lease time for {seconds}s plus cleanup')
    units.update(owned_units)
    row = {'name': name, 'command': command, 'timeout_seconds': seconds,
           'started_at': dt.datetime.now(dt.timezone.utc).isoformat()}
    stages.append(row)
    save('stages.json', stages)
    try:
        with (OUT / (name + '.log')).open('w') as log:
            child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log,
                                     stderr=subprocess.STDOUT, start_new_session=True)
            code = child.wait(timeout=seconds)
            child = None
            if code:
                raise RuntimeError(f'{name} exited {code}')
        row['passed'] = True
    except BaseException:
        halt_child()
        row['passed'] = False
        raise
    finally:
        row['finished_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
        save('stages.json', stages)

def cleanup():
    failures = []
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    try:
        halt_child()
    except Exception as error:
        failures.append('child: ' + repr(error))
    if units:
        try:
            subprocess.run(['sudo', 'systemctl', 'stop', *sorted(units)],
                           capture_output=True, timeout=35)
        except Exception as error:
            failures.append('stop units: ' + repr(error))
    states = {}
    for unit in sorted(units):
        try:
            answer = subprocess.run(['systemctl', 'show', unit, '--property=ActiveState',
                                     '--property=LoadState'], capture_output=True, text=True, timeout=10)
            properties = dict(line.split('=', 1) for line in answer.stdout.splitlines() if '=' in line)
            assert properties.get('ActiveState') in ('inactive', 'failed'), (unit, properties)
            states[unit] = properties
        except Exception as error:
            failures.append(unit + ': ' + repr(error))
    pool_zero = None
    try:
        if pool_owned:
            subprocess.run(['sudo', 'sysctl', '-w', 'vm.nr_hugepages=0'],
                           capture_output=True, check=True, timeout=30)
        pool_zero = int(Path('/proc/sys/vm/nr_hugepages').read_text()) == 0
        assert pool_zero, 'hugepage pool is nonzero; pre-existing pools are not altered'
    except Exception as error:
        failures.append('hugepages: ' + repr(error))
    save('cleanup.json', {'failures': failures, 'unit_states': states,
                         'pool_zero': pool_zero})
    return failures

signal.signal(signal.SIGINT, interrupted)
signal.signal(signal.SIGTERM, interrupted)
try:
    assert os.uname().machine == 'x86_64'
    stage('input-verification', ['sha256sum', '--check', 'JOINT-INPUT-SHA256SUMS'], 30)
    timer = subprocess.check_output(['sudo', 'cat', '/run/systemd/shutdown/scheduled'], text=True, timeout=15)
    (OUT / 'shutdown-timer.txt').write_text(timer)
    fields = dict(line.split('=', 1) for line in timer.splitlines() if '=' in line)
    scheduled = dt.datetime.fromtimestamp(int(fields['USEC']) / 1000000, dt.timezone.utc)
    assert fields['MODE'] == 'poweroff' and scheduled <= expiry + dt.timedelta(seconds=5)
    assert scheduled - dt.datetime.now(dt.timezone.utc) >= dt.timedelta(minutes=90)
    wall_end = min(wall_end, scheduled)
    assert int(Path('/proc/sys/vm/nr_hugepages').read_text()) == 0
    pool_owned = True
    stage('topology', ['lscpu', '--json'], 15)
    for variant in ('control', 'candidate'):
        name = variant + '-full-size-parity'
        stage(name, ['bin/verify-joint-' + variant], 180)
        text = (OUT / (name + '.log')).read_text()
        assert all(kernel + ': PASS' in text for kernel in ('scalar', 'avx2', 'avx512', 'avx512-256'))
    matrix = ['python3', 'matrix.py', '--out', 'results/fixed-kernel', '--workers', '12',
              '--pairs', '2', '--seconds', '12', '--rounds', '2', '--deadline-seconds', '540']
    for kernel in ('avx2', 'avx512'):
        for variant in ('control', 'candidate'):
            matrix += ['--variant', f'{variant}-{kernel}:bin/seine-joint-{variant}:{kernel}']
    stage('fixed-kernel-matrix', matrix, 600)
    fixed = OUT / 'fixed-kernel'
    assert (fixed / 'COMPLETE').exists()
    assert json.loads((fixed / 'order-verification.json').read_text())['per_worker_order_reversed_each_pass']
    rows = json.loads((fixed / 'results.json').read_text())
    assert len(rows) == 8 and int(Path('/proc/sys/vm/nr_hugepages').read_text()) == 0
    comparisons = {}
    for kernel in ('avx2', 'avx512'):
        paired = []
        for repeat in (1, 2):
            values = {}
            for row in rows:
                if row['repeat'] == repeat and row['kernel'] == kernel:
                    assert row['workers'] == 12 and row['variant'] not in values
                    values[row['variant']] = row['completed_hps']
            control, candidate = values['control-' + kernel], values['candidate-' + kernel]
            assert control > 0 and candidate > 0
            paired.append(100 * (candidate / control - 1))
        comparisons[kernel] = paired
    save('fixed-kernel-gate.json', {'paired_gain_percent': comparisons,
                                  'reject_if_same_kernel_both_pairs_below_percent': -3})
    if any(all(gain < -3 for gain in gains) for gains in comparisons.values()):
        status, reason = 'REJECTED', 'Both same-kernel pairs lost more than 3%; long stages skipped'
    else:
        for variant, suffix in (('control', 'a'), ('candidate', 'a'), ('candidate', 'b'), ('control', 'b')):
            label = f'joint-{variant}-{suffix}'
            stage(label, ['python3', 'joint_tuner_probe.py', '--binary', 'bin/seine-joint-' + variant,
                          '--label', label, '--seconds', '900'], 1020,
                  ['seine-campaign-' + label + '.service'])
            assert (OUT / label / 'COMPLETE').exists()
            assert not json.loads((OUT / label / 'cleanup.json').read_text())['failures']
            assert int(Path('/proc/sys/vm/nr_hugepages').read_text()) == 0
        lifecycle_units = ['seine-campaign-lifecycle-' + part + '.service'
                           for part in ('first', 'restart', 'optout')] + ['seine-campaign-cpu-load.service']
        stage('candidate-lifecycle', ['python3', 'resource_lifecycle.py', '--binary',
              'bin/seine-joint-candidate', '--initial-seconds', '240'], 1000, lifecycle_units)
        assert (OUT / 'lifecycle/COMPLETE').exists()
        status = 'COMPLETE'
except Deferred as error:
    status, reason = 'DEFERRED', str(error)
except BaseException as error:
    status, reason = 'FAILED', repr(error)
finally:
    failures = cleanup()
    if failures:
        status, reason = 'FAILED', {'primary': reason, 'cleanup': failures}
    save('outcome.json', {'status': status, 'reason': reason, 'gain_required': False,
                         'finished_at': dt.datetime.now(dt.timezone.utc).isoformat()})
    (OUT / status).write_text(json.dumps({'reason': reason}) + '\n')

# Archive negative/deferred results too; COMPLETE is present only after every
# stage and independent cleanup pass. A separate archive marker proves packing.
subprocess.run(['tar', '-czf', 'joint-confirm-results.tar.gz', 'results',
                'joint-plan.json', 'joint-binary-identities.json', 'JOINT-INPUT-SHA256SUMS',
                'run-joint-confirm.py'], check=True, timeout=60)
archive = ROOT / 'joint-confirm-results.tar.gz'
(ROOT / 'joint-archive.json').write_text(json.dumps({'status': status,
    'archive': archive.name, 'sha256': hashlib.sha256(archive.read_bytes()).hexdigest()}) + '\n')
if status == 'COMPLETE':
    (ROOT / 'JOINT-CONFIRM-COMPLETE').write_text('All stages, cleanup and archive passed.\n')
raise SystemExit(0 if status in ('COMPLETE', 'DEFERRED', 'REJECTED') else 1)
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('instance')
    args = parser.parse_args()
    record, info = cloud.owned(args.instance)
    assert record['type'] == 'c5.4xlarge' and record['timer_verified']
    assert info['State']['Name'] == 'running'
    assert dt.datetime.fromisoformat(record['expires_at']) - cloud.now() >= dt.timedelta(minutes=90)
    bundle = cloud.ROOT / 'portable-joint-x86'
    identity = json.loads((bundle / 'identities.json').read_text())
    assert identity['source_control'] == 'f20e7bb1772be024a12ac5cdcc92685ba89d3ddd'
    assert identity['source_candidate'] == 'e6809bb78b51c6b94c6a6b7ebcff2cf21f95faa3'
    expected = {'seine-joint-control', 'seine-joint-candidate', 'verify-joint-control', 'verify-joint-candidate'}
    assert set(identity['sha256']) == expected
    files = [(bundle / name, 'bin/' + name) for name in sorted(expected)]
    files += [(bundle / 'identities.json', 'joint-binary-identities.json')]
    files += [(Path(__file__).resolve().parent / name, name) for name in
              ('matrix.py', 'screen.py', 'joint_tuner_probe.py', 'resource_lifecycle.py')]
    manifest = {}
    for source, name in files:
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        if source.name in expected:
            assert digest == identity['sha256'][source.name]
        manifest[name] = digest
    cloud.ssh(args.instance, 'test ! -e /home/ubuntu/campaign && mkdir -p /home/ubuntu/campaign/bin /home/ubuntu/campaign/results')
    for source, name in files:
        destination = '/home/ubuntu/campaign/' + name
        cloud.copy(args.instance, str(source), destination)
        assert cloud.ssh(args.instance, 'sha256sum ' + shlex.quote(destination)).split()[0] == manifest[name]
    plan = {'instance_type': record['type'], 'lease_expires_at': record['expires_at'],
            'cutoff_utc': '2026-10-24T19:45:00Z', 'outer_timeout_seconds': 5400,
            'window_order': ['control-a', 'candidate-a', 'candidate-b', 'control-b']}
    manifest['run-joint-confirm.py'] = hashlib.sha256(RUNNER.encode()).hexdigest()
    code = "import hashlib,json,pathlib,subprocess\nroot=pathlib.Path('/home/ubuntu/campaign')\n"
    code += "(root/'run-joint-confirm.py').write_text(" + repr(RUNNER) + ")\n"
    code += "assert hashlib.sha256((root/'run-joint-confirm.py').read_bytes()).hexdigest()==" + repr(manifest['run-joint-confirm.py']) + "\n"
    code += "(root/'joint-plan.json').write_text(" + repr(json.dumps(plan, indent=2) + '\n') + ")\n"
    code += "(root/'JOINT-INPUT-SHA256SUMS').write_text(" + repr(''.join(digest + '  ' + name + '\n' for name, digest in manifest.items())) + ")\n"
    code += "with (root/'joint-confirm.log').open('w') as log:\n"
    code += " proc=subprocess.Popen(['timeout','--kill-after=240','5400','python3','run-joint-confirm.py'],cwd=root,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)\n"
    code += "print(json.dumps({'pid':proc.pid,'timeout_seconds':5400,'cleanup_kill_after_seconds':240}))\n"
    job_path = cloud.ROOT / (args.instance + '-joint-confirm-job.json')
    assert not job_path.exists(), 'joint confirmation already recorded'
    job = {'instance': args.instance, 'identities': identity, 'manifest': manifest,
           'plan': plan, 'requested_at': cloud.now().isoformat(), 'status': 'launch-requested'}
    cloud.save(job_path, job)
    result = cloud.ssh(args.instance, shlex.join(['python3', '-c', code]))
    job.update(status='started', job=json.loads(result))
    cloud.save(job_path, job)
    print(result)


if __name__ == '__main__':
    main()
