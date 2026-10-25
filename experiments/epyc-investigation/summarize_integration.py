#!/usr/bin/env python3
"""Export compact evidence only after both native archives and cleanup verify."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument('--root', type=Path, required=True)
ap.add_argument('--output', type=Path, required=True)
a = ap.parse_args()

def read(path):
    return json.loads(path.read_text())

result = {'build': read(a.root / 'frozen/build-manifest.json'), 'hosts': {}}
for role in ['intel', 'epyc']:
    lease = a.root / (role + '-integration-20261024')
    root = lease / 'artifacts'
    assert (root / 'VALIDATION_COMPLETE').is_file()
    manifest = read(root / 'SHA256SUMS.json')
    for name, digest in manifest.items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
    cleanup = read(lease / 'cleanup.json')
    for field in ['instance_terminated', 'root_volume_deleted',
                  'security_group_deleted', 'private_key_removed']:
        assert cleanup[field], field
    host = {'session': read(lease / 'session.json'), 'cleanup': cleanup,
            'archive': read(lease / 'archive-verified.json'),
            'hardware': read(root / 'hardware.json'),
            'binaries': read(root / 'expected-binaries.json'), 'comparisons': {}}
    for path in sorted(root.rglob('summary.json')):
        directory = path.parent
        protocol = read(directory / 'protocol.json')
        reports = []
        for pair in range(1, protocol['pairs'] + 1):
            for side in ['baseline', 'candidate']:
                name = f'{pair}-{side}'
                report = read(directory / (name + '.json'))
                checks = read(directory / (name + '-checks.json'))
                assert all(checks['delta'][k] == 0 for k in ['pswpin', 'pswpout', 'oom_kill'])
                assert checks['steal_pct'] < 2 and checks['steal_single_cpu_upper_pct'] < 2
                reports.append({'pair': pair, 'side': side,
                                'report_sha256': manifest[str((directory / (name + '.json')).relative_to(root))],
                                'environment': report['environment'],
                                'configuration': report['config_fingerprint'],
                                'checks': checks,
                                'runs': [{'hashes': r['hashes'], 'elapsed_secs': r['elapsed_secs'],
                                          'backend_runs': r['backend_runs']} for r in report['runs']]})
        host['comparisons'][str(directory.relative_to(root))] = {
            'protocol': protocol, 'summary': read(path), 'reports': reports}
    if role == 'epyc':
        host['validation_logs'] = {name: (root / name).read_text() for name in
                                  ['full-size-kernels.log', 'full-size-switches.log',
                                   'native-tuner-tests.log', 'productive-selection.log']}
        assert 'EPYC_PRODUCTIVE_PASS' in host['validation_logs']['productive-selection.log']
    price_file = 'intel-price.json' if role == 'intel' else 'turin-price.json'
    prices = [json.loads(raw) for raw in read(a.root / price_file)['PriceList']]
    assert len(prices) == 1
    price = prices[0]
    dimensions = [d for term in price['terms']['OnDemand'].values()
                  for d in term['priceDimensions'].values()]
    assert len(dimensions) == 1 and dimensions[0]['unit'] == 'Hrs'
    hourly = float(dimensions[0]['pricePerUnit']['USD'])
    elapsed = (datetime.datetime.fromisoformat(cleanup['completed_at'])
               - datetime.datetime.fromisoformat(host['session']['launch_time'])).total_seconds()
    host['compute_cost'] = {'usd_per_hour': hourly, 'sku': price['product']['sku'],
                            'price_published': price['publicationDate'],
                            'launch_through_cleanup_seconds': elapsed,
                            'conservative_usd': max(60, elapsed) / 3600 * hourly,
                            'excludes': 'EBS, public IPv4 and data transfer'}
    result['hosts'][role] = host
# Never copy credentials or the SSH key/config into committed evidence.
for host in result['hosts'].values():
    host['session'].pop('ip', None)
a.output.write_text(json.dumps(result, indent=2) + '\n')
print(a.output)
