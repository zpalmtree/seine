#!/usr/bin/env python3
"""Check performance cohort boundaries in a disposable local PostgreSQL DB."""
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
class PerformanceSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='seine-performance-sql-')
        cls.directory = Path(cls.temp.name)
        cls.started = False
        cls.addClassCleanup(cls.cleanup)
        subprocess.run([str(PG / 'initdb'), '-D', str(cls.directory / 'db'),
                        '-A', 'trust', '--no-locale'], check=True, capture_output=True)
        subprocess.run([str(PG / 'pg_ctl'), '-D', str(cls.directory / 'db'),
                        '-l', str(cls.directory / 'pg.log'), '-o',
                        f"-k {cls.directory} -h '' -p 55447", '-w', 'start'],
                       check=True, capture_output=True)
        cls.started = True
        cls.query((ROOT / 'src/schema.sql').read_text())

    @classmethod
    def cleanup(cls):
        if cls.started:
            subprocess.run([str(PG / 'pg_ctl'), '-D', str(cls.directory / 'db'),
                            '-m', 'immediate', 'stop'], capture_output=True, check=True)
        cls.temp.cleanup()

    @classmethod
    def query(cls, sql):
        return subprocess.check_output(
            ['psql', '-h', str(cls.directory), '-p', '55447', '-d', 'postgres',
             '-X', '-qAt', '-v', 'ON_ERROR_STOP=1'], input=sql, text=True)

    def setUp(self):
        self.query('TRUNCATE telemetry_report_facts, telemetry_reports;')

    def seed(self, diagnostics=None, rate=20, hours=(26, 13, 1), identities=None):
        fixture = json.loads((ROOT / 'fixture.json').read_text())
        identities = identities or [uuid.uuid4().hex for _ in range(5)]
        rows = []
        for identity in identities:
            for hour in hours:
                payload = copy.deepcopy(fixture)
                payload['installation_id'] = identity
                payload['report_id'] = uuid.uuid4().hex
                if diagnostics is not None:
                    payload['schema_version'] = 3
                    payload['diagnostics'] = copy.deepcopy(diagnostics)
                payload['hardware']['resources']['available_memory_gib'] = hour
                payload['backends'][0]['hashes'] = rate * 3600
                # Duplicate devices must not multiply independent observations.
                payload['backends'] *= 2
                text = json.dumps(payload).replace("'", "''")
                rows.append(f"('{payload['report_id']}', '{identity}', "
                            f"now()-interval '{hour} hours', '{text}'::jsonb)")
        self.query('INSERT INTO telemetry_reports VALUES ' + ','.join(rows) + ';')
        return identities

    def report(self):
        result = json.loads(self.query((ROOT / 'agent_report.sql').read_text()))
        self.assertNotIn('installation_id', json.dumps(result))
        return result

    def view(self):
        return json.loads(self.query(
            "SELECT coalesce(json_agg(v), '[]') FROM telemetry_backend_performance v"))

    def diagnostics(self, **changes):
        value = json.loads((ROOT / 'diagnostics-fixture.json').read_text())['diagnostics']
        value.update(changes)
        return value

    def test_modes_exact_builds_and_legacy_reports_stay_separate(self):
        base = self.diagnostics(mode='pool', build_commit='a' * 12,
                                source_fingerprint='b' * 16, reason='periodic')
        contexts = [base, dict(base, mode='daemon'), dict(base, build_commit='c' * 12),
                    dict(base, source_fingerprint='d' * 16), None]
        for index, context in enumerate(contexts):
            self.seed(context, rate=20 + index * 100)
        for cohorts in [self.report()['performance_candidates'], self.view()]:
            self.assertEqual(len(cohorts), 5)
            self.assertEqual(sorted(c['median_hps'] for c in cohorts), [20, 120, 220, 320, 420])
            self.assertTrue(all(c['installations'] == 5 for c in cohorts))
            by_rate = {c['median_hps']: c['context'] for c in cohorts}
            self.assertEqual(by_rate[20]['mode'], 'pool')
            self.assertEqual(by_rate[120]['mode'], 'daemon')
            self.assertEqual(by_rate[220]['build_commit'], 'c' * 12)
            self.assertEqual(by_rate[320]['source_fingerprint'], 'd' * 16)
            self.assertIsNone(by_rate[420]['mode'])

    def test_final_windows_remain_inventory_but_cannot_establish_performance(self):
        for reason in ['shutdown', 'failure']:
            with self.subTest(reason=reason):
                self.query('TRUNCATE telemetry_report_facts, telemetry_reports;')
                self.seed(self.diagnostics(reason=reason))
                report = self.report()
                self.assertEqual(report['installations'], 5)
                self.assertEqual(report['reports'], 15)
                self.assertEqual(report['performance_candidates'], [])
                self.assertEqual(self.view(), [])

    def test_final_windows_neither_bias_rates_nor_complete_sparse_cohorts(self):
        periodic = self.diagnostics(reason='periodic')
        identities = self.seed(periodic, hours=(26, 13))
        self.seed(dict(periodic, reason='shutdown'), rate=1000, hours=(1,),
                  identities=identities)
        self.assertEqual(self.report()['performance_candidates'], [])
        cohort, = self.view()
        self.assertEqual(cohort['median_hps'], 20)
        self.seed(periodic, hours=(0,), identities=identities)
        self.seed(dict(periodic, reason='failure'), rate=1000, identities=identities)
        for cohorts in [self.report()['performance_candidates'], self.view()]:
            cohort, = cohorts
            self.assertEqual(cohort['median_hps'], 20)
            self.assertEqual(cohort['installations'], 5)


if __name__ == '__main__':
    unittest.main()
