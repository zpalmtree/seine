#!/usr/bin/env python3
"""Native corrected-order confirmation using frozen generic ARM binaries.

No compiler installation or new kernel experiment: this repeats the accepted
candidate/control comparisons on fresh G2/G3 leases, including full-size parity.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shlex

import cloud


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('instance')
    parser.add_argument('--regular', action='store_true')
    parser.add_argument('--bundle', choices=['portable-final-arm', 'portable-codegen-arm'], default='portable-final-arm')
    args = parser.parse_args()
    record, info = cloud.owned(args.instance)
    assert record['timer_verified'] and info['State']['Name'] == 'running'
    assert record['type'] in ['m6g.4xlarge', 'm7g.4xlarge']
    kernel = 'neon' if record['type'] == 'm6g.4xlarge' else 'neon-sha3'
    bundle = cloud.ROOT / args.bundle
    identity = json.loads((bundle / 'identities.json').read_text())
    assert identity['source_control'] == '359265e325d85c9c7b6e3eed7b5b126fcec63f9c'
    expected = {'portable-final-arm': '636e6887c405c41d732433b1eeb018cefcdc962d',
                'portable-codegen-arm': 'f20e7bb1772be024a12ac5cdcc92685ba89d3ddd'}
    assert identity['source_candidate'] == expected[args.bundle]
    cloud.ssh(args.instance, 'test ! -e /home/ubuntu/campaign && mkdir -p /home/ubuntu/campaign/bin /home/ubuntu/campaign/results')
    manifest = {}
    files = [(bundle / name, 'bin/' + name) for name in identity['sha256']]
    files += [(Path(__file__).resolve().parent / name, name) for name in ['screen.py', 'matrix.py']]
    for path, name in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if path.name in identity['sha256']:
            assert digest == identity['sha256'][path.name]
        cloud.copy(args.instance, str(path), '/home/ubuntu/campaign/' + name)
        assert cloud.ssh(args.instance, 'sha256sum ' + shlex.quote('/home/ubuntu/campaign/' + name)).split()[0] == digest
        manifest[name] = digest
    script = '''set -euo pipefail
cd /home/ubuntu/campaign
test "$(uname -m)" = aarch64
sha256sum --check FINAL-INPUT-SHA256SUMS
sudo cat /run/systemd/shutdown/scheduled > results/shutdown-timer.txt
lscpu --json > results/lscpu.json
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE > results/topology.txt
cat /proc/cpuinfo > results/cpuinfo.txt
cat /proc/meminfo > results/meminfo.txt
uname -a > results/uname.txt
bin/seine-arm-control --version > results/control-version.txt
bin/seine-arm-candidate --version > results/candidate-version.txt
timeout 600 bin/verify-arm-control > results/control-full-size-parity.log
timeout 600 bin/verify-arm-candidate > results/candidate-full-size-runtime-parity.log
'''
    script += ('grep -q "kernel=neon-sha3" results/candidate-full-size-runtime-parity.log\n' if kernel == 'neon-sha3'
               else 'grep -q "unsupported SHA3 selection rejected" results/candidate-full-size-runtime-parity.log\n')
    script += ('python3 matrix.py --out results/arm-final-confirm --workers 1 16 --pairs 4 --seconds 30 '
               '--variant baseline:bin/seine-arm-control:neon --variant candidate:bin/seine-arm-candidate:' + kernel + '\n')
    if args.regular:
        script += ('python3 matrix.py --out results/arm-final-regular --workers 1 8 --pairs 4 --seconds 20 --page regular '
                   '--variant baseline:bin/seine-arm-control:neon --variant candidate:bin/seine-arm-candidate:' + kernel + '\n')
    script += 'touch results/FINAL-CONFIRM-COMPLETE\n'
    code = "import pathlib,subprocess,json\nroot=pathlib.Path('/home/ubuntu/campaign')\n"
    code += "(root/'FINAL-INPUT-SHA256SUMS').write_text(" + repr(''.join(d + '  ' + n + '\n' for n, d in manifest.items())) + ")\n"
    code += "(root/'final-binary-identities.json').write_text(" + repr(json.dumps(identity, indent=2) + '\n') + ")\n"
    code += "(root/'run-final-arm.sh').write_text(" + repr(script) + ")\n"
    code += '''with (root/'final-arm.log').open('w') as log:
 proc=subprocess.Popen(['timeout','4200','bash','run-final-arm.sh'],cwd=root,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
print(json.dumps({'pid':proc.pid,'timeout_seconds':4200}))
'''
    result = cloud.ssh(args.instance, shlex.join(['python3', '-c', code]))
    cloud.save(cloud.ROOT / (args.instance + '-final-arm-job.json'), {
        'instance': args.instance, 'kernel': kernel, 'regular': args.regular, 'identities': identity,
        'manifest': manifest, 'started_at': cloud.now().isoformat(), 'job': json.loads(result)})
    print(result)


if __name__ == '__main__':
    main()
