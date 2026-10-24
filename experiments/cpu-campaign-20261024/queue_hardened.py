#!/usr/bin/env python3
"""Queue frozen validation after existing timing; never overlap benchmark jobs."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex

import cloud


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('instance')
    args = p.parse_args()
    record, info = cloud.owned(args.instance)
    assert record['timer_verified'] and info['State']['Name'] == 'running'
    script_dir = Path(__file__).resolve().parent
    paths = [cloud.ROOT / 'cpu-hardened-636e688.tar.gz'] + [script_dir / name for name in
             ['hardened_validate.sh', 'pid_pool_probe.py']]
    manifest = {}
    for path in paths:
        cloud.copy(args.instance, str(path), '/home/ubuntu/campaign/' + path.name)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        assert cloud.ssh(args.instance, 'sha256sum ' + shlex.quote('/home/ubuntu/campaign/' + path.name)).split()[0] == digest
        manifest[path.name] = digest
    marker = 'results/arm-generation-confirm/COMPLETE' if cloud.TYPES[record['type']] == 'arm64' else 'results/SCREEN-COMPLETE'
    code = '''import pathlib,subprocess,json
root=pathlib.Path('/home/ubuntu/campaign')
assert not (root/'hardened-queued.json').exists()
'''
    supervisor = '''import pathlib,subprocess,time
root=pathlib.Path('/home/ubuntu/campaign')
deadline=time.monotonic()+3000
while not (root/MARKER).exists():
 assert time.monotonic()<deadline, 'preceding timing stage did not complete'
 time.sleep(5)
subprocess.run(['bash','hardened_validate.sh'],cwd=root,check=True)
'''.replace('MARKER', repr(marker))
    code += 'script=' + repr(supervisor) + '\n'
    code += '''(root/'run-hardened.py').write_text(script)
with (root/'hardened.log').open('w') as log:
 proc=subprocess.Popen(['timeout','5800','python3','run-hardened.py'],cwd=root,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
result={'pid':proc.pid,'timeout_seconds':5800}
(root/'hardened-queued.json').write_text(json.dumps(result)+chr(10))
print(json.dumps(result))
'''
    result = cloud.ssh(args.instance, shlex.join(['python3', '-c', code]))
    cloud.save(cloud.ROOT / (args.instance + '-hardened-job.json'), {'instance': args.instance,
        'source': '636e6887c405c41d732433b1eeb018cefcdc962d', 'manifest': manifest,
        'after_marker': marker, 'started_at': cloud.now().isoformat(), 'job': json.loads(result)})
    print(result)


if __name__ == '__main__':
    main()
