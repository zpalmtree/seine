#!/usr/bin/env python3
"""Follow the NUMA control with local auto fallback versus remote huge pages."""
import hashlib
import math
from pathlib import Path
import statistics
import subprocess
import numa_common as common


def main():
    assert subprocess.run(['systemctl', 'is-active', '--quiet', 'seine-numa-test']).returncode != 0
    common.OUT = common.ROOT / 'imbalance-results'
    common.OUT.mkdir(exist_ok=False)
    pools = [n / 'hugepages/hugepages-2048kB/nr_hugepages'
             for n in sorted(Path('/sys/devices/system/node').glob('node[0-9]*'))]
    assert len(pools) == 2 and [int(p.read_text()) for p in pools] == [0, 0]
    results = {}
    common.save(common.OUT / 'protocol.json', {
        'binary_sha256': hashlib.sha256((common.ROOT / 'seine-arena-audit').read_bytes()).hexdigest(),
        'workers': 8, 'mode': 'auto', 'pairs': 3, 'seconds': 20, 'rounds': 2, 'warmup': 1,
        'pools': [8192, 0], 'comparison': 'normal remote HugeTLB fallback vs strict local allocation with page-backing fallback',
        'expected_startup_backing': {'audit': '8 explicit HugeTLB', 'local': '4 explicit HugeTLB + 4 fully THP'},
        'expected_timed_allocation_failures': 0})
    try:
        pools[0].write_text('8192\n')
        assert int(pools[0].read_text()) == 8192
        for pair in range(1, 4):
            for policy in (('audit', 'local') if pair % 2 else ('local', 'audit')):
                name = f'auto-{pair}-{policy}'
                results[name] = common.run(name, policy, mode='auto')
                common.save(common.OUT / 'rates.json', results)
    finally:
        for p in pools:
            p.write_text('0\n')
            assert int(p.read_text()) == 0
    ratios = [results[f'auto-{i}-local']['completed_hps'] / results[f'auto-{i}-audit']['completed_hps'] for i in (1, 2, 3)]
    common.save(common.OUT / 'summary.json', {'paired_delta_pct': 100 * (math.exp(statistics.mean(map(math.log, ratios))) - 1),
                  'paired_deltas_pct': [100 * (r - 1) for r in ratios], 'rates': results})
    (common.OUT / 'COMPLETE').write_text('All imbalance comparisons passed.\n')


if __name__ == '__main__':
    main()
