#!/usr/bin/env python3
"""Page-size trial after completing NUMA tests; pool requirements fail closed."""
from pathlib import Path
import subprocess
import bench


def main():
    root = Path(__file__).resolve().parent
    assert subprocess.run(['systemctl', 'is-active', '--quiet', 'seine-imbalance-test']).returncode != 0
    nodes = sorted(Path('/sys/devices/system/node').glob('node[0-9]*'))
    assert len(nodes) == 2
    small = [n / 'hugepages/hugepages-2048kB/nr_hugepages' for n in nodes]
    giant = [n / 'hugepages/hugepages-1048576kB/nr_hugepages' for n in nodes]
    assert all(p.exists() for p in giant)
    original = {p: int(p.read_text()) for p in small + giant}
    assert all(original[p] == 0 for p in small)
    try:
        for p in giant:
            p.write_text('12\n')
        actual = [int(p.read_text()) for p in giant]
        bench.save(root / 'one-gib-reservation.json', {'requested_per_node': 12, 'actual_per_node': actual})
        assert actual == [12, 12], 'Need boot-time reservation before a valid page comparison'
        for p in small:
            p.write_text('6144\n')
            assert int(p.read_text()) == 6144
        common = {'binary': 'seine-arena-audit', 'kernel': 'avx2',
                  'env': {'SEINE_EXPERIMENT_ARENA_POLICY': 'audit'}}
        result = bench.paired(root, root / 'page-results', common | {'mode': 'large'},
                              common | {'mode': 'large-1g'}, workers=12)
        print(result, flush=True)
    finally:
        for p in small + giant:
            p.write_text(str(original[p]) + '\n')
            assert int(p.read_text()) == original[p]


if __name__ == '__main__':
    main()
