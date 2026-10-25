#!/usr/bin/env python3
"""One bounded, tagged experiment VM. Writes only task-specific state/resources."""
import argparse
import base64
import datetime
import ipaddress
import json
from pathlib import Path
import subprocess
import time
import urllib.request


def aws(action, *args):
    result = subprocess.run(['aws', 'ec2', action, '--profile', 'seine-bench',
                             '--region', 'us-east-1', *args, '--output', 'json'],
                            capture_output=True, text=True, timeout=60)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return json.loads(result.stdout or '{}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('action', choices=['launch', 'connect', 'cleanup'])
    ap.add_argument('--root', type=Path, required=True)
    ap.add_argument('--type', default='c6a.8xlarge')
    ap.add_argument('--minutes', type=int, default=35)
    ap.add_argument('--image', help='AMI for launch; required only when launching')
    ap.add_argument('--vpc', help='Owned VPC for launch; required only when launching')
    ap.add_argument('--subnet', help='Owned subnet for launch; required only when launching')
    a = ap.parse_args()
    root = a.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    def save(name, data):
        (root / name).write_text(json.dumps(data, indent=2) + '\n')
    if a.action == 'launch':
        assert not (root / 'session.json').exists()
        assert 10 <= a.minutes <= 120
        assert a.type in ('c6a.8xlarge', 'c6i.4xlarge', 'c7a.8xlarge', 'c8a.8xlarge')
        assert all((a.image, a.vpc, a.subnet)), 'Pass explicit owned launch infrastructure'
        key = root / 'ssh-key'
        subprocess.run(['ssh-keygen', '-t', 'ed25519', '-N', '', '-C', root.name,
                        '-f', str(key)], check=True, capture_output=True)
        address = ipaddress.ip_address(urllib.request.urlopen(
            'https://checkip.amazonaws.com', timeout=15).read().decode().strip())
        assert address.version == 4, 'This launch template uses an IPv4 subnet'
        ip = str(address)
        tags = [{'Key': 'Project', 'Value': 'seine-avx2-validation'},
                {'Key': 'Experiment', 'Value': root.name},
                {'Key': 'Name', 'Value': 'seine-' + root.name}]
        group = aws('create-security-group', '--group-name', 'seine-' + root.name,
                    '--description', 'Bounded EPYC investigation, owner SSH only',
                    '--vpc-id', a.vpc, '--tag-specifications',
                    json.dumps([{'ResourceType': 'security-group', 'Tags': tags}]))['GroupId']
        state = {'security_group': group, 'type': a.type, 'minutes': a.minutes}
        save('session.json', state)
        aws('authorize-security-group-ingress', '--group-id', group, '--ip-permissions',
            json.dumps([{'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22,
                         'IpRanges': [{'CidrIp': ip + '/32'}]}]))
        # A systemd timer invokes immediate shutdown only at expiry. Unlike
        # shutdown +N, it does not create /run/nologin five minutes beforehand.
        user = ('#cloud-config\nssh_authorized_keys:\n  - ' + key.with_suffix('.pub').read_text().strip()
                + '\nruncmd:\n  - [systemd-run, --unit=seine-expiry, --on-active='
                + str(a.minutes) + 'm, /usr/sbin/shutdown, -h, now]\n'
                + '  - [sh, -c, "apt-get update -qq && apt-get install -y -qq numactl"]\n')
        expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=a.minutes)
        tags.append({'Key': 'ExpiresAt', 'Value': expires.isoformat()})
        launch = {'ImageId': a.image, 'InstanceType': a.type,
                  'MinCount': 1, 'MaxCount': 1, 'SubnetId': a.subnet,
                  'SecurityGroupIds': [group], 'InstanceInitiatedShutdownBehavior': 'terminate',
                  'MetadataOptions': {'HttpTokens': 'required'},
                  'UserData': base64.b64encode(user.encode()).decode(),
                  'BlockDeviceMappings': [{'DeviceName': '/dev/sda1', 'Ebs': {
                      'VolumeSize': 8, 'VolumeType': 'gp3', 'DeleteOnTermination': True}}],
                  'TagSpecifications': [{'ResourceType': r, 'Tags': tags} for r in ('instance', 'volume')]}
        save('launch.json', launch)
        try:
            result = aws('run-instances', '--cli-input-json', 'file://' + str(root / 'launch.json'))
            save('launch-response.json', result)
            state['instance_id'] = result['Instances'][0]['InstanceId']
            save('session.json', state)
        except Exception:
            aws('delete-security-group', '--group-id', group)
            key.unlink(missing_ok=True)
            save('launch-failed-cleanup.json', {'security_group_deleted': True, 'key_removed': True})
            raise
        print(json.dumps(state))
    elif a.action == 'connect':
        state = json.loads((root / 'session.json').read_text())
        inst = aws('describe-instances', '--instance-ids', state['instance_id'])['Reservations'][0]['Instances'][0]
        state.update(ip=inst['PublicIpAddress'], launch_time=inst['LaunchTime'],
                     root_volume=inst['BlockDeviceMappings'][0]['Ebs']['VolumeId'])
        save('session.json', state)
        (root / 'ssh-config').write_text(
            f'Host ep\n  HostName {state["ip"]}\n  User ubuntu\n  IdentityFile {root}/ssh-key\n'
            f'  IdentitiesOnly yes\n  StrictHostKeyChecking accept-new\n'
            f'  UserKnownHostsFile {root}/known-hosts\n  ConnectTimeout 8\n  ServerAliveInterval 15\n')
        print(json.dumps(state))
    else:
        state = json.loads((root / 'session.json').read_text())
        record = {}
        if 'instance_id' in state:
            aws('terminate-instances', '--instance-ids', state['instance_id'])
            for _ in range(24):
                inst = aws('describe-instances', '--instance-ids', state['instance_id'])['Reservations'][0]['Instances'][0]
                if inst['State']['Name'] == 'terminated':
                    break
                time.sleep(3)
            assert inst['State']['Name'] == 'terminated'
            record['instance_terminated'] = True
            volume = state.get('root_volume')
            if volume:
                assert not aws('describe-volumes', '--filters', 'Name=volume-id,Values=' + volume)['Volumes']
            record['root_volume_deleted'] = bool(volume)
        aws('delete-security-group', '--group-id', state['security_group'])
        assert not aws('describe-security-groups', '--filters', 'Name=group-id,Values=' + state['security_group'])['SecurityGroups']
        record['security_group_deleted'] = True
        (root / 'ssh-key').unlink(missing_ok=True)
        record['private_key_removed'] = True
        record['completed_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save('cleanup.json', record)
        print(json.dumps(record))


if __name__ == '__main__':
    main()
