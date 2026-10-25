#!/usr/bin/env python3
"""Upload a verified payload and start one bounded job on an existing owned lease."""
import argparse
import hashlib
import json

import cloud


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('instance')
    parser.add_argument('--functional', action='store_true', help='run x86 candidate tests/lifecycle after screening')
    parser.add_argument('--recovery', action='store_true', help='run fixed-candidate native recovery validation after timing')
    args = parser.parse_args()
    assert not (args.functional and args.recovery), 'choose one lifecycle candidate'
    record, info = cloud.owned(args.instance)
    assert record['timer_verified'] and info['State']['Name'] == 'running'
    architecture = cloud.TYPES[record['type']]
    payload = cloud.ROOT / f'payload-generation-{architecture}.tar.gz'
    digest = hashlib.sha256(payload.read_bytes()).hexdigest()
    assert digest == payload.with_suffix(payload.suffix + '.sha256').read_text().strip()
    cloud.copy(args.instance, str(payload), '/home/ubuntu/campaign-input.tar.gz')
    # All arguments originate from the ownership-checked lease and fixed names.
    check = cloud.ssh(args.instance, 'sha256sum /home/ubuntu/campaign-input.tar.gz').split()[0]
    assert check == digest
    arm = architecture == 'arm64'
    script = ('set -euo pipefail\ncd /home/ubuntu/campaign\n'
              'sha256sum --check INPUT-SHA256SUMS\n'
              'SEINE_SKIP_GLOBAL_SHA3=1 bash bootstrap.sh\n' +
              ('bash arm_generation.sh\n' if arm else ('bash cpu_functional_validate.sh\n' if args.functional else '')) +
              ('bash recovery_validate.sh\n' if args.recovery else ''))
    command = "python3 - <<'REMOTE'\n" + '''import json, subprocess, tarfile
from pathlib import Path
root=Path('/home/ubuntu/campaign')
root.mkdir(exist_ok=False)
with tarfile.open('/home/ubuntu/campaign-input.tar.gz') as archive:
    archive.extractall(root, filter='data')
''' + 'script=' + repr(script) + '\n' + '''(root/'run-generation.sh').write_text(script)
with (root/'generation.log').open('w') as log:
    process=subprocess.Popen(['timeout','6000','bash',str(root/'run-generation.sh')], cwd=root,
                             stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
print(json.dumps({'pid':process.pid,'timeout_seconds':6000}))
REMOTE'''
    result = cloud.ssh(args.instance, command)
    print(result)
    cloud.save(cloud.ROOT / (args.instance + '-job.json'), {
        'instance_id': args.instance, 'input_archive_sha256': digest,
        'started_at': cloud.now().isoformat(), 'job': json.loads(result)})


if __name__ == '__main__':
    main()
