#!/usr/bin/env python3
"""Disposable PostgreSQL integration tests; never connects to a production DB."""
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import uuid

ROOT = Path(__file__).resolve().parent
PG = Path('/usr/lib/postgresql/16/bin')


@unittest.skipUnless((PG / 'initdb').exists(), 'PostgreSQL 16 binaries required')
class AlertSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='seine-alert-sql-')
        cls.directory = Path(cls.temp.name)
        cls.started = False
        cls.addClassCleanup(cls.cleanup)
        subprocess.run([str(PG / 'initdb'), '-D', str(cls.directory / 'db'), '-A', 'trust', '--no-locale'], check=True, capture_output=True)
        subprocess.run([str(PG / 'pg_ctl'), '-D', str(cls.directory / 'db'), '-l', str(cls.directory / 'pg.log'),
                        '-o', f"-k {cls.directory} -h '' -p 55447", '-w', 'start'], check=True, capture_output=True)
        cls.started = True
        cls.query((ROOT / 'src/schema.sql').read_text())

    @classmethod
    def cleanup(cls):
        if cls.started:
            subprocess.run([str(PG / 'pg_ctl'), '-D', str(cls.directory / 'db'), '-m', 'immediate', 'stop'], capture_output=True, check=True)
        cls.temp.cleanup()

    @classmethod
    def query(cls, sql):
        return subprocess.check_output(['psql', '-h', str(cls.directory), '-p', '55447', '-d', 'postgres',
                                        '-X', '-qAt', '-v', 'ON_ERROR_STOP=1'], input=sql, text=True)

    def setUp(self):
        self.query('TRUNCATE telemetry_report_facts, telemetry_reports;')

    def seed(self, installations=5, mutate=None, sparse=False):
        fixture = json.loads((ROOT / 'fixture.json').read_text())
        rows = []
        for index in range(installations):
            install = uuid.uuid4().hex
            hours = (150, 100, 50, 34, 18, 2) if not sparse else (150, 100, 50, 3, 2, 1)
            for hour in hours:
                payload = copy.deepcopy(fixture)
                payload['miner_version'] = 'v2' if hour < 36 else 'v1'
                payload['hardware']['resources']['available_memory_gib'] = hour
                payload['backends'][0]['hashes'] = 18000 if hour < 36 else 36000
                # Many identical devices must still count as one observation.
                payload['backends'] *= 4
                if mutate:
                    mutate(payload, index, hour)
                text = json.dumps(payload).replace("'", "''")
                rows.append(f"('{uuid.uuid4().hex}', '{install}', now()-interval '{hour} hours', '{text}'::jsonb)")
        self.query('INSERT INTO telemetry_reports VALUES ' + ','.join(rows) + ';')

    def analysis(self):
        result = json.loads(self.query((ROOT / 'alerts.sql').read_text()))
        self.assertNotIn('installation_id', json.dumps(result))
        return result

    def test_paired_drop_free_memory_changes_and_duplicate_devices(self):
        self.seed()
        cohort, = self.analysis()['performance_cohorts']
        self.assertEqual(cohort['installations'], 5)
        self.assertEqual(cohort['affected_installations'], 5)
        self.assertEqual(cohort['median_ratio'], .5)
        self.assertEqual(cohort['model_installations'], 5)
        self.assertEqual(cohort['model_median_ratio'], .5)

    def test_model_figures_span_cohorts_of_the_same_hardware(self):
        # Five installations at four lanes halve; twelve at other lane counts,
        # each too few for a cohort of their own, stay level.
        def mutate(payload, index, hour):
            if index < 5:
                payload['backends'][0]['settings'].update(lanes=4)
            else:
                payload['backends'][0]['settings'].update(lanes=6 + index % 4)
                payload['backends'][0]['hashes'] = 36000
        self.seed(17, mutate=mutate)
        cohort, = self.analysis()['performance_cohorts']
        self.assertEqual(cohort['settings']['lanes'], 4)
        self.assertEqual(cohort['installations'], 5)
        self.assertEqual(cohort['median_ratio'], .5)
        self.assertEqual(cohort['model_installations'], 17)
        self.assertEqual(cohort['model_median_ratio'], 1)

    def test_sparse_and_small_populations_do_not_qualify(self):
        self.seed(4)
        self.assertEqual(self.analysis()['performance_cohorts'], [])
        self.query('TRUNCATE telemetry_report_facts, telemetry_reports;')
        self.seed(sparse=True)
        self.assertEqual(self.analysis()['performance_cohorts'], [])

    def test_settings_environment_build_and_bad_samples_are_separate(self):
        mutations = [
            lambda p: p['backends'][0]['settings'].update(lanes=4),
            lambda p: p['hardware'].update(runtime_environment='wsl'),
            lambda p: p.update(build_features='cpu-only'),
            lambda p: p['backends'][0].update(settings_changed=True),
            lambda p: p['backends'][0].update(observed_secs=10),
            lambda p: p.update(debug_build=True),
            lambda p: p.update(diagnostics={'reason': 'failure'}),
            lambda p: p.update(diagnostics={'reason': 'periodic', 'mode': 'daemon'}),
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.query('TRUNCATE telemetry_report_facts, telemetry_reports;')
                self.seed(mutate=lambda p, i, h: mutation(p) if h < 36 else None)
                self.assertEqual(self.analysis()['performance_cohorts'], [])

    def test_schema14_gpu_context_does_not_split_a_cohort(self):
        # A first NVRTC build and its cache hit are the same kernel on the same GPU.
        def mutate(payload, index, hour):
            payload['backends'][0]['settings'].update(
                gpu_arch='sm_86',
                gpu_kernel_source='nvrtc_cached' if hour < 36 else 'nvrtc')
        self.seed(mutate=mutate)
        cohort, = self.analysis()['performance_cohorts']
        self.assertEqual(cohort['installations'], 5)
        self.assertNotIn('gpu_kernel_source', cohort['settings'])

    def test_release_comparison_allows_source_changes_within_one_mode(self):
        self.seed(mutate=lambda p, i, h: p.update(diagnostics={
            'reason': 'periodic', 'mode': 'pool',
            'build_commit': 'a' * 12 if h < 36 else 'b' * 12,
            'source_fingerprint': 'c' * 16 if h < 36 else 'd' * 16,
        }))
        cohort, = self.analysis()['performance_cohorts']
        self.assertEqual(cohort['median_ratio'], .5)
        self.assertEqual(cohort['installations'], 5)

    def test_healthy_comparison_and_empty_ingestion(self):
        self.assertIsNone(self.analysis()['minutes_since_last_report'])
        self.seed(mutate=lambda p, i, h: p['backends'][0].update(hashes=36000))
        cohort, = self.analysis()['performance_cohorts']
        self.assertEqual(cohort['median_ratio'], 1)
        self.assertEqual(cohort['affected_installations'], 0)


if __name__ == '__main__':
    unittest.main()
