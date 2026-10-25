#!/usr/bin/env python3
"""Queue the bounded C5 natural-tuning observation after fixed-count timing."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import shlex

import cloud


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('instance')
    args = parser.parse_args()
    record, info = cloud.owned(args.instance)
    assert record['type'] == 'c5.4xlarge' and record['timer_verified']
    assert info['State']['Name'] == 'running'
    cleanup_horizon = datetime.datetime(2026, 10, 24, 17, 32, tzinfo=datetime.timezone.utc)
    assert datetime.datetime.fromisoformat(record['expires_at']) > cleanup_horizon
    assert cloud.now() + datetime.timedelta(seconds=3000) > cleanup_horizon
    for name in ['joint_tuner_probe.py', 'resource_lifecycle.py']:
        source = Path(__file__).resolve().parent / name
        cloud.copy(args.instance, str(source), '/home/ubuntu/campaign/' + name)
        actual = cloud.ssh(args.instance, 'sha256sum /home/ubuntu/campaign/' + name).split()[0]
        assert actual == hashlib.sha256(source.read_bytes()).hexdigest()
    script = '''import datetime,pathlib,subprocess
root=pathlib.Path('/home/ubuntu/campaign')
cutoff=datetime.datetime(2026,10,24,17,13,tzinfo=datetime.timezone.utc)
import time
while not (root/'results/crossover-confirm/COMPLETE').exists():
 if datetime.datetime.now(datetime.timezone.utc)>=cutoff:
  (root/'joint-probe-SKIPPED').write_text('No complete timing stage before bounded start cutoff.\\n')
  raise SystemExit(0)
 time.sleep(5)
if datetime.datetime.now(datetime.timezone.utc)>=cutoff:
 raise SystemExit('Start cutoff reached; preserve rental cleanup time')
subprocess.run(['python3','joint_tuner_probe.py','--binary','bin/seine-portable-candidate',
 '--label','joint-baseline','--seconds','900'],cwd=root,check=True)
'''
    code = "import pathlib,subprocess,json\nroot=pathlib.Path('/home/ubuntu/campaign')\n"
    code += "assert not (root/'joint-probe-queued.json').exists()\n"
    code += "(root/'run-joint-probe.py').write_text(" + repr(script) + ")\n"
    code += '''with (root/'joint-probe.log').open('w') as log:
 proc=subprocess.Popen(['timeout','3000','python3','run-joint-probe.py'],cwd=root,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
result={'pid':proc.pid,'timeout_seconds':3000,'latest_start_utc':'2026-10-24T17:13:00Z'}
(root/'joint-probe-queued.json').write_text(json.dumps(result)+chr(10))
print(json.dumps(result))
'''
    result = cloud.ssh(args.instance, shlex.join(['python3', '-c', code]))
    cloud.save(cloud.ROOT / (args.instance + '-joint-probe-job.json'), {
        'instance': args.instance, 'started_at': cloud.now().isoformat(), 'job': json.loads(result)})
    print(result)


if __name__ == '__main__':
    main()
