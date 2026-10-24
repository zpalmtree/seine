"""Evidence-gate regression tests; no allocations, rentals, or benchmarks."""
import copy
import json
from pathlib import Path
import tempfile
import os
import subprocess
import sys
import unittest

from run import GIB, execute, schedule, sha, validate


def sample(policy='local'):
    fields = ['memory_explicit_large', 'memory_explicit_large_1g',
              'memory_transparent_huge', 'memory_regular', 'memory_heap']
    backend = {f + suffix: 0 for f in fields for suffix in ('_workers', '_bytes')}
    backend.update(memory_regular_workers=1, memory_regular_bytes=2 * GIB)
    backend.update({key: 0 for key in ['dropped_events', 'assignment_enqueue_timeouts',
                                     'assignment_execution_timeouts', 'control_enqueue_timeouts',
                                     'control_execution_timeouts', 'memory_allocation_failures']})
    report = {'cpu_threads': 1, 'config_fingerprint': {
        'cpu_kernel': 'avx512', 'cpu_page_mode': 'regular', 'cpu_affinity': 'auto',
        'cpu_autotune_threads': False},
        'runs': [{'hashes': 100, 'elapsed_secs': 20, 'backend_runs': [backend]}]}
    arena = {'worker': 0, 'policy': policy, 'policy_restored': True, 'bytes': 2 * GIB,
             'stride_bytes': 2 * 1024**2, 'samples_by_node': {'7': 1024},
             'local_node': 7, 'target_node': 7 if policy == 'local' else None}
    return report, arena


class EvidenceGates(unittest.TestCase):
    def validate_sample(self, report, arena, suffix=''):
        return validate(report, 'NUMA_ARENA ' + json.dumps(arena) + suffix,
                        1, 'local', 'regular', 'avx512', 1)

    def test_full_fence_elapsed_time_drives_rate(self):
        report, arena = sample()
        report['avg_hps'] = 9000  # Deliberately wrong convenience field.
        self.assertEqual(self.validate_sample(report, arena)['completed_hps'], 5)

    def test_reversal_is_independent_at_every_worker_count(self):
        jobs = schedule([1, 24, 48], ['audit', 'local'], 4)
        for count in (1, 24, 48):
            self.assertEqual([p for w, _, p in jobs if w == count],
                             ['audit', 'local', 'local', 'audit'] * 2)

    def test_remote_or_unobserved_pages_are_rejected(self):
        report, arena = sample()
        for locations in ({'8': 1024}, {'7': 1023}, {'7': 1023, '-2': 1}):
            with self.subTest(locations=locations), self.assertRaises(ValueError):
                self.validate_sample(report, arena | {'samples_by_node': locations})

    def test_partial_arenas_wrong_policy_and_missing_restore_are_rejected(self):
        report, arena = sample()
        for change in ({'bytes': GIB}, {'policy_restored': False}, {'target_node': 8}, {'policy': 'audit'}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.validate_sample(report, arena | change)
        with self.assertRaises(ValueError):
            self.validate_sample(report, arena, '\nNUMA_EXPERIMENT_RESTORE_ERROR denied')

    def test_duplicate_arena_audit_is_rejected(self):
        report, arena = sample()
        with self.assertRaises(ValueError):
            self.validate_sample(report, arena, '\nNUMA_ARENA ' + json.dumps(arena))

    def test_backing_fallback_and_backend_errors_are_rejected(self):
        original, arena = sample()
        for change in ({'memory_regular_bytes': GIB}, {'memory_allocation_failures': 1},
                       {'dropped_events': 1}, {'control_execution_timeouts': 1}):
            report = copy.deepcopy(original)
            report['runs'][0]['backend_runs'][0].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.validate_sample(report, arena)

    def test_streaming_hash_works_on_python_310(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'binary'
            path.write_bytes(b'abc')
            self.assertEqual(sha(path), 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')

    def test_timeout_terminates_owned_benchmark_process(self):
        with tempfile.TemporaryDirectory() as temp:
            log = Path(temp) / 'child.log'
            command = [sys.executable, '-c',
                       'import os,time; print(os.getpid(), flush=True); time.sleep(60)']
            with self.assertRaises(subprocess.TimeoutExpired):
                execute(command, log, 0.5, dict(os.environ))
            pid = int(log.read_text().strip())
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)


if __name__ == '__main__':
    unittest.main()
