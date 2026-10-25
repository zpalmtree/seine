#!/usr/bin/env python3
"""Queue final same-profile binary regressions behind all native lifecycle checks."""
import argparse
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
    assert record['timer_verified'] and info['State']['Name'] == 'running'
    script_dir = Path(__file__).resolve().parent
    paths = [(script_dir / n, n) for n in ['final_regression.sh', 'portable_regression.sh']]
    if cloud.TYPES[record['type']] == 'amd64':
        for original, name in [('seine-ring-control', 'seine-portable-control'),
                               ('verify-ring-control', 'verify-portable-control')]:
            paths.append((cloud.ROOT / 'intel-ring-portable' / original, name))
    manifest = {}
    for path, name in paths:
        cloud.copy(args.instance, str(path), '/home/ubuntu/campaign/' + name)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        assert cloud.ssh(args.instance, 'sha256sum ' + shlex.quote('/home/ubuntu/campaign/' + name)).split()[0] == digest
        manifest[name] = digest
    control = ''.join(digest + '  ' + name + '\n' for name, digest in manifest.items() if name.endswith('-control'))
    supervisor = '''import pathlib,subprocess,time
root=pathlib.Path('/home/ubuntu/campaign')
deadline=time.monotonic()+4800
while not (root/'results/lifecycle/COMPLETE').exists():
 assert time.monotonic()<deadline, 'preceding lifecycle did not complete'
 time.sleep(5)
# COMPLETE precedes bounded finally cleanup by a few seconds; wait for the
# hardened runner to exit so no service/fixture can overlap the timing phase.
pid=__import__('json').loads((root/'hardened-queued.json').read_text())['pid']
for attempt in range(30):
 stat=pathlib.Path('/proc')/str(pid)/'stat'
 if not stat.exists() or stat.read_text().split()[2]=='Z': break
 time.sleep(1)
else: raise RuntimeError('hardened validation did not exit after completion')
subprocess.run(['bash','final_regression.sh'],cwd=root,check=True)
'''
    code = "import pathlib,subprocess,json\nroot=pathlib.Path('/home/ubuntu/campaign')\n"
    code += "assert not (root/'final-regression-queued.json').exists()\n"
    code += "(root/'portable-control-input.sha256').write_text(" + repr(control) + ")\n"
    code += "(root/'run-final-regression.py').write_text(" + repr(supervisor) + ")\n"
    code += '''with (root/'final-regression.log').open('w') as log:
 proc=subprocess.Popen(['timeout','6600','python3','run-final-regression.py'],cwd=root,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
result={'pid':proc.pid,'timeout_seconds':6600}
(root/'final-regression-queued.json').write_text(json.dumps(result)+chr(10))
print(json.dumps(result))
'''
    result = cloud.ssh(args.instance, shlex.join(['python3', '-c', code]))
    cloud.save(cloud.ROOT / (args.instance + '-final-regression-job.json'), {
        'instance': args.instance, 'manifest': manifest, 'started_at': cloud.now().isoformat(), 'job': json.loads(result)})
    print(result)


if __name__ == '__main__':
    main()
