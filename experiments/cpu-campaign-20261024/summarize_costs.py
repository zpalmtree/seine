#!/usr/bin/env python3
"""Export conservative estimated cloud costs only after verified final cleanup."""
import argparse
import datetime as dt
import hashlib
import json
import math
from pathlib import Path


def dimensions(path):
    raw = json.loads(path.read_text())
    for item in raw['PriceList']:
        product = json.loads(item) if isinstance(item, str) else item
        for term in product.get('terms', {}).get('OnDemand', {}).values():
            yield from term['priceDimensions'].values()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory', type=Path)
    p.add_argument('output', type=Path)
    args = p.parse_args()
    ledger_path = args.directory / 'cloud-leases.json'
    ledger = json.loads(ledger_path.read_text())
    assert ledger.get('cleanup_verified_at') and ledger.get('security_group') is None
    assert ledger.get('security_group_absence_verified') and ledger.get('local_ssh_material_removed')
    # Raw live-price responses are retained; fail rather than silently apply a
    # different unit/rate. The controller reserves these same conservative rates.
    assert any(d['unit'] == 'GB-Mo' and float(d['pricePerUnit']['USD']) == .08
               for d in dimensions(args.directory / 'price-ebs.json'))
    assert any(float(d['pricePerUnit']['USD']) == .005
               for d in dimensions(args.directory / 'price-ipv4.json'))
    rows = []
    for index, lease in enumerate(ledger['leases'], 1):
        assert lease['timer_verified'] and lease['terminated'] and lease['volumes_deleted']
        elapsed = (dt.datetime.fromisoformat(lease['verified_at']) -
                   dt.datetime.fromisoformat(lease['launched_at'])).total_seconds()
        hours = max(60, math.ceil(elapsed)) / 3600
        # Launch-to-cleanup exceeds actual running time. Add a minute to disk/IP
        # duration and use a short 28-day month for conservative storage pricing.
        ancillary_hours = hours + 1 / 60
        compute = hours * lease['hourly_usd']
        disk = ancillary_hours * 16 * .08 / (28 * 24)
        address = ancillary_hours * .005
        rows.append({'lease': index, 'instance_type': lease['type'],
            'launched_at': lease['launched_at'], 'cleanup_verified_at': lease['verified_at'],
            'independent_deadline': lease['expires_at'], 'compute_usd_per_hour': lease['hourly_usd'],
            'estimated_compute_usd': compute, 'estimated_root_disk_usd': disk,
            'estimated_ipv4_usd': address, 'reserved_usd': lease['cost_reserved_usd'],
            'initial_reserved_usd': lease.get('initial_cost_reserved_usd', lease['cost_reserved_usd']),
            'unused_reservation_released_usd': lease.get('released_reserve_usd', 0),
            'instance_terminated': True, 'root_volumes_deleted': True})
    subtotal = sum(r['estimated_compute_usd'] + r['estimated_root_disk_usd'] + r['estimated_ipv4_usd'] for r in rows)
    reserve = sum(r['reserved_usd'] for r in rows)
    assert reserve <= 40
    assert subtotal + len(rows) <= 40
    history = ledger.get('budget_history', [])
    peak = max([reserve] + [event[field] for event in history
                           for field in ['before_usd', 'after_usd'] if field in event])
    assert peak <= 40
    result = {'scope': 'campaign-created rentals only; estimate, not an AWS invoice',
        'basis': 'live AWS Pricing API; launch through verified cleanup; conservative disk/address duration',
        'compute_storage_ipv4_estimate_usd': subtotal,
        'unmetered_transfer_contingency_usd': len(rows),
        'estimate_plus_transfer_contingency_usd': subtotal + len(rows),
        'cumulative_reserved_ceiling_usd': reserve, 'authorized_cap_usd': 40,
        'initial_reservations_total_usd': sum(r['initial_reserved_usd'] for r in rows),
        'unused_reservations_released_usd': sum(r['unused_reservation_released_usd'] for r in rows),
        'maximum_recorded_commitment_usd': peak,
        'reservation_method': 'Each launch reserves three hours plus transfer allowance. Verified closed leases retain ceil(whole elapsed hours) plus the full transfer allowance; unused live reservations are never released.',
        'budget_history': history,
        'resources': rows, 'security_group_absence_verified': True,
        'local_ssh_material_removed': True, 'cleanup_verified_at': ledger['cleanup_verified_at'],
        'private_ledger_sha256': hashlib.sha256(ledger_path.read_bytes()).hexdigest(),
        'price_response_sha256': {name: hashlib.sha256((args.directory / name).read_bytes()).hexdigest()
                                  for name in ['price-ebs.json', 'price-ipv4.json']}}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k.endswith('_usd')}))


if __name__ == '__main__':
    main()
