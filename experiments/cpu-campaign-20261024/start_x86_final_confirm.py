#!/usr/bin/env python3
"""Repeat final frozen x86 regressions on an already-screened CPU generation."""
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
    assert record['timer_verified'] and info['State']['Name'] == 'running'
    assert record['type'] in ['c6i.4xlarge', 'c7i.4xlarge', 'c8i.4xlarge']
    assert datetime.datetime.fromisoformat(record['expires_at']) - cloud.now() > datetime.timedelta(minutes=30)
    bundle = cloud.ROOT / 'portable-codegen-x86'
    identity = json.loads((bundle / 'identities.json').read_text())
    assert identity['source_control'] == '359265e325d85c9c7b6e3eed7b5b126fcec63f9c'
    assert identity['source_candidate'] == 'f20e7bb1772be024a12ac5cdcc92685ba89d3ddd'
    cloud.ssh(args.instance, 'test ! -e /home/ubuntu/campaign && mkdir -p /home/ubuntu/campaign/bin /home/ubuntu/campaign/results')
    files = [(bundle / name, 'bin/' + name) for name in identity['sha256']]
    files += [(Path(__file__).resolve().parent / name, name) for name in ['screen.py', 'matrix.py']]
    manifest = {}
    for path, name in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if path.name in identity['sha256']:
            assert digest == identity['sha256'][path.name]
        cloud.copy(args.instance, str(path), '/home/ubuntu/campaign/' + name)
        assert cloud.ssh(args.instance, 'sha256sum ' + shlex.quote('/home/ubuntu/campaign/' + name)).split()[0] == digest
        manifest[name] = digest
    script = '''set -euo pipefail
cd /home/ubuntu/campaign
test "$(uname -m)" = x86_64
sha256sum --check FINAL-INPUT-SHA256SUMS
sudo cat /run/systemd/shutdown/scheduled > results/shutdown-timer.txt
lscpu --json > results/lscpu.json
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE > results/topology.txt
cat /proc/cpuinfo > results/cpuinfo.txt
cat /proc/meminfo > results/meminfo.txt
uname -a > results/uname.txt
timeout 180 bin/verify-portable-control > results/control-full-size-parity.log
timeout 180 bin/verify-portable-candidate > results/candidate-full-size-parity.log
python3 - <<'PY'
from pathlib import Path
import subprocess
from screen import physical_first
memory = {line.split(':')[0]: int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()}
maximum = min(12, len(physical_first()), (memory['MemAvailable'] * 1024 - 5 * 1024**3) // (2 * 1024**3))
assert maximum >= 1
text=Path('results/candidate-full-size-parity.log').read_text()
assert all(kernel + ': PASS' in text for kernel in ['avx2','avx512'])
command=['python3','matrix.py','--out','results/final-x86-confirm','--workers',*map(str,sorted({1,maximum})),'--pairs','2','--seconds','20','--deadline-seconds','1500']
for kernel in ['avx2','avx512']:
 for variant in ['control','candidate']:
  command += ['--variant',f'{variant}-{kernel}:bin/seine-portable-{variant}:{kernel}']
subprocess.run(command,check=True)
PY
touch results/FINAL-CONFIRM-COMPLETE
'''
    code = "import pathlib,subprocess,json\nroot=pathlib.Path('/home/ubuntu/campaign')\n"
    code += "(root/'FINAL-INPUT-SHA256SUMS').write_text(" + repr(''.join(d + '  ' + n + '\n' for n, d in manifest.items())) + ")\n"
    code += "(root/'final-binary-identities.json').write_text(" + repr(json.dumps(identity, indent=2) + '\n') + ")\n"
    code += "(root/'run-final-x86.sh').write_text(" + repr(script) + ")\n"
    code += '''with (root/'final-x86.log').open('w') as log:
 proc=subprocess.Popen(['timeout','1800','bash','run-final-x86.sh'],cwd=root,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
print(json.dumps({'pid':proc.pid,'timeout_seconds':1800}))
'''
    result = cloud.ssh(args.instance, shlex.join(['python3', '-c', code]))
    cloud.save(cloud.ROOT / (args.instance + '-final-x86-job.json'), {
        'instance': args.instance, 'identities': identity, 'manifest': manifest,
        'started_at': cloud.now().isoformat(), 'job': json.loads(result)})
    print(result)


if __name__ == '__main__':
    main()
