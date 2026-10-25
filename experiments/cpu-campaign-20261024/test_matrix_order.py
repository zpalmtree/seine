#!/usr/bin/env python3
"""Exercise the real matrix control flow without hashes, processes or OS writes."""
import argparse
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import matrix


class MatrixOrderTest(unittest.TestCase):
    def run_plan(self, labels, pairs=2):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            variants = []
            for label in labels:
                binary = root / label
                binary.write_bytes(label.encode())
                variants.append(f'{label}:{binary}:avx2')
            args = argparse.Namespace(variant=variants, workers=[1, 16], out=root / 'out',
                pairs=pairs, seconds=10, rounds=2, page='large', deadline_seconds=100)
            pool = [0]
            read_text = Path.read_text

            def read(path, *a, **kw):
                if str(path) == '/proc/meminfo':
                    return 'MemAvailable: 67108864 kB\nHugepagesize: 2048 kB\n'
                if str(path) == '/proc/swaps':
                    return 'Filename Type Size Used Priority\n'
                if str(path) == '/proc/sys/vm/nr_hugepages':
                    return str(pool[0])
                return read_text(path, *a, **kw)

            def run(command, log):
                if command[:2] == ['sudo', 'sysctl']:
                    pool[0] = int(command[-1].split('=')[1])
                    return
                self.assertEqual(command[0], 'taskset')
                output = Path(command[command.index('--bench-output') + 1])
                output.write_text(json.dumps({'pow_fingerprint': {'test': 'order-only'}}))

            counters = dict.fromkeys(['cpu_total', 'cpu_steal', 'pswpin', 'pswpout', 'oom_kill'], 0)
            with patch.object(argparse.ArgumentParser, 'parse_args', return_value=args), \
                 patch.object(matrix, 'physical_first', return_value=list(range(16))), \
                 patch.object(Path, 'read_text', read), patch.object(matrix, 'run', run), \
                 patch.object(matrix, 'validate', return_value=1.0), \
                 patch.object(matrix, 'counters', return_value=counters), \
                 patch.object(matrix.time, 'sleep'), patch('builtins.print'):
                matrix.main()
            self.assertEqual(pool[0], 0)
            self.assertTrue(json.loads((args.out / 'order-verification.json').read_text())
                            ['per_worker_order_reversed_each_pass'])
            return [(row['workers'], row['variant']) for row in json.loads((args.out / 'results.json').read_text())]

    def test_two_variants_reverse_within_both_worker_counts(self):
        self.assertEqual(self.run_plan(['baseline', 'candidate']), [
            (1, 'baseline'), (1, 'candidate'), (16, 'candidate'), (16, 'baseline'),
            (16, 'baseline'), (16, 'candidate'), (1, 'candidate'), (1, 'baseline')])

    def test_three_variants_reverse_all_relative_orders(self):
        self.assertEqual(self.run_plan(['avx2', 'full', 'narrow']), [
            (1, 'avx2'), (1, 'full'), (1, 'narrow'), (16, 'full'), (16, 'narrow'), (16, 'avx2'),
            (16, 'avx2'), (16, 'narrow'), (16, 'full'), (1, 'narrow'), (1, 'full'), (1, 'avx2')])

    def test_four_pass_confirmation_preserves_balance(self):
        self.assertEqual(self.run_plan(['baseline', 'candidate'], pairs=4), [
            (1, 'baseline'), (1, 'candidate'), (16, 'candidate'), (16, 'baseline'),
            (16, 'baseline'), (16, 'candidate'), (1, 'candidate'), (1, 'baseline')] * 2)


if __name__ == '__main__':
    unittest.main()
