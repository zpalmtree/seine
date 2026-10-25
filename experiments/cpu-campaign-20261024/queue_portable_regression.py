#!/usr/bin/env python3
"""Run frozen CPU-only x86 binaries on another generation after its ISA screen."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex

import cloud


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('instance')
    parser.add_argument('--bundle', choices=['portable-final-x86', 'portable-codegen-x86'], default='portable-final-x86')
    args = parser.parse_args()
    record, info = cloud.owned(args.instance)
    assert record['timer_verified'] and info['State']['Name'] == 'running'
    assert cloud.TYPES[record['type']] == 'amd64'
    root = cloud.ROOT / args.bundle
    identities = json.loads((root / 'identities.json').read_text())
    assert identities['source_control'] == '359265e325d85c9c7b6e3eed7b5b126fcec63f9c'
    expected = {'portable-final-x86': '636e6887c405c41d732433b1eeb018cefcdc962d',
                'portable-codegen-x86': 'f20e7bb1772be024a12ac5cdcc92685ba89d3ddd'}
    assert identities['source_candidate'] == expected[args.bundle]
    files = [root / name for name in identities['sha256']]
    files += [root / 'portable-x86-binaries.sha256', root / 'identities.json',
              Path(__file__).resolve().parent / 'portable_regression.sh']
    for path in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if path.name in identities['sha256']:
            assert digest == identities['sha256'][path.name]
        cloud.copy(args.instance, str(path), '/home/ubuntu/campaign/' + path.name)
        assert cloud.ssh(args.instance, 'sha256sum ' + shlex.quote('/home/ubuntu/campaign/' + path.name)).split()[0] == digest
    script = '''import pathlib,subprocess,time
root=pathlib.Path('/home/ubuntu/campaign')
deadline=time.monotonic()+2400
while not (root/'results/SCREEN-COMPLETE').exists():
 assert time.monotonic()<deadline, 'baseline screen did not complete'
 time.sleep(5)
for name in ['seine-portable-control','verify-portable-control','seine-portable-candidate','verify-portable-candidate']:
 subprocess.run(['cp',name,'bin/'+name],cwd=root,check=True)
subprocess.run(['bash','portable_regression.sh'],cwd=root,check=True)
'''
    code = "import pathlib,subprocess,json\nroot=pathlib.Path('/home/ubuntu/campaign')\n"
    code += "assert not (root/'portable-regression-queued.json').exists()\n"
    code += "(root/'run-portable-regression.py').write_text(" + repr(script) + ")\n"
    code += '''with (root/'portable-regression.log').open('w') as log:
 proc=subprocess.Popen(['timeout','4200','python3','run-portable-regression.py'],cwd=root,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
result={'pid':proc.pid,'timeout_seconds':4200}
(root/'portable-regression-queued.json').write_text(json.dumps(result)+chr(10))
print(json.dumps(result))
'''
    result = cloud.ssh(args.instance, shlex.join(['python3', '-c', code]))
    cloud.save(cloud.ROOT / (args.instance + '-portable-regression-job.json'), {
        'instance': args.instance, 'identities': identities, 'started_at': cloud.now().isoformat(), 'job': json.loads(result)})
    print(result)


if __name__ == '__main__':
    main()
