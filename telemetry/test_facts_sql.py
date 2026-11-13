"""Derived report rows against disposable PostgreSQL: trigger, rebuild and removal."""
import copy
import json
import uuid
from pathlib import Path
import unittest
import test_alerts_sql

ROOT = Path(__file__).resolve().parent
COUNT = 'SELECT count(*) FROM telemetry_report_facts'


@unittest.skipUnless((test_alerts_sql.PG / 'initdb').exists(), 'PostgreSQL 16 binaries required')
class ReportFactsSQLTests(unittest.TestCase):
    # Reuse the existing disposable database setup, without inheriting its test cases.
    setUpClass = classmethod(test_alerts_sql.AlertSQLTests.setUpClass.__func__)
    cleanup = classmethod(test_alerts_sql.AlertSQLTests.cleanup.__func__)
    query = classmethod(test_alerts_sql.AlertSQLTests.query.__func__)
    setUp = test_alerts_sql.AlertSQLTests.setUp

    def fixture(self):
        return json.loads((ROOT / 'fixture.json').read_text())

    def insert(self, payload, report=None, hours=0, identity=None):
        report = report or uuid.uuid4().hex
        text = json.dumps(payload).replace("'", "''")
        self.query("INSERT INTO telemetry_reports VALUES "
                   f"('{report}','{identity or uuid.uuid4().hex}',now()-interval '{hours} hours','{text}'::jsonb)")
        return report

    def facts(self, report):
        return json.loads(self.query(
            f"SELECT to_jsonb(f) FROM telemetry_report_facts f WHERE report_id = '{report}'"))

    def test_trigger_derives_one_entry_per_settings_from_stable_samples(self):
        payload = self.fixture()
        fast = copy.deepcopy(payload['backends'][0])
        fast['hashes'] = 72000
        other = copy.deepcopy(payload['backends'][0])
        other['settings']['lanes'] = 4
        other['hashes'] = 7200
        changed = copy.deepcopy(other)
        changed.update(settings_changed=True, hashes=1)
        short = copy.deepcopy(other)
        short.update(observed_secs=59, hashes=1)
        # A kernel source differs between a first build and its cache.
        cached = copy.deepcopy(other)
        cached['settings']['gpu_kernel_source'] = 'nvrtc_cached'
        cached['hashes'] = 14400
        payload['backends'] += [fast, other, changed, short, cached]
        payload['tuning'] = {'settled': {'workers': 8}}
        payload['diagnostics'] = {'reason': 'periodic', 'build_commit': 'a' * 12,
                                  'previous_run_unclean': True, 'dropped_events': 2}
        row = self.facts(self.insert(payload))
        self.assertEqual(row['version'], '0.2.23-fixture')
        self.assertEqual(row['build_commit'], 'a' * 12)
        self.assertIs(row['debug_build'], False)
        for flag in ('periodic', 'has_diagnostics', 'settled', 'previous_run_unclean', 'dropped_diagnostics'):
            self.assertIs(row[flag], True, flag)
        # Two settings in report order, each with the median of its stable samples.
        self.assertEqual(row['hps'], [15, 3])
        self.assertEqual(len(set(row['settings_hashes'])), 2)
        for column in ('hardware_hash', 'build_hash', 'context_hash'):
            self.assertIsInstance(row[column], int)

    def test_hashes_follow_cohort_keys_and_ignore_free_memory(self):
        base = self.fixture()
        less_free = copy.deepcopy(base)
        less_free['hardware']['resources']['available_memory_gib'] = 1
        fewer_cpus = copy.deepcopy(base)
        fewer_cpus['hardware']['resources']['allowed_logical_cpus'] = 4
        daemon = copy.deepcopy(base)
        daemon['diagnostics'] = {'reason': 'shutdown', 'mode': 'daemon'}
        rows = [self.facts(self.insert(p)) for p in (base, less_free, fewer_cpus, daemon)]
        keys = [(r['hardware_hash'], r['build_hash'], r['context_hash'], r['settings_hashes']) for r in rows]
        self.assertEqual(keys[0], keys[1])
        self.assertNotEqual(keys[0][0], keys[2][0])
        self.assertNotEqual(keys[0][2], keys[2][2])
        self.assertEqual(keys[0][0], keys[3][0])
        self.assertNotEqual(keys[0][1], keys[3][1])
        self.assertNotEqual(keys[0][2], keys[3][2])
        self.assertIs(rows[0]['has_diagnostics'], False)
        self.assertIs(rows[0]['periodic'], True)
        self.assertIs(rows[3]['periodic'], False)
        self.assertIs(rows[0]['settled'], False)
        self.assertIs(rows[0]['dropped_diagnostics'], False)
        self.assertIsNone(rows[0]['previous_run_unclean'])

    def test_underivable_report_is_stored_and_rebuild_is_repeatable(self):
        good = self.insert(self.fixture(), hours=30)
        broken = self.fixture()
        broken['backends'] = 'not a list'
        bad = self.insert(broken)
        self.assertEqual(self.query('SELECT count(*) FROM telemetry_reports').strip(), '2')
        self.assertEqual(self.query(COUNT).strip(), '1')
        missing = ("SELECT count(*) FROM telemetry_reports r WHERE NOT EXISTS "
                   "(SELECT 1 FROM telemetry_report_facts f WHERE f.report_id = r.report_id)")
        self.assertEqual(self.query(missing).strip(), '1')
        # Rows lost or stale for any reason come back; a report that still cannot
        # be derived stops the rebuild instead of being skipped silently.
        self.query("DELETE FROM telemetry_reports WHERE report_id = '%s'" % bad)
        before = self.facts(good)
        self.query("UPDATE telemetry_report_facts SET version = 'stale', hps = '{}'")
        late = self.insert(self.fixture(), hours=2)
        self.query("DELETE FROM telemetry_report_facts WHERE report_id = '%s'" % late)
        rebuild = (ROOT / 'rebuild_facts.sql').read_text()
        self.query(rebuild)
        self.assertEqual(self.facts(good), before)
        self.assertEqual(self.facts(late)['hps'], [10])
        unchanged = "SELECT xmin::text FROM telemetry_report_facts ORDER BY report_id"
        versions = self.query(unchanged)
        self.query(rebuild)
        self.assertEqual(self.query(unchanged), versions)

    def test_settled_report_reads_the_latest_report_that_carries_a_record(self):
        for _ in range(6):
            identity = uuid.uuid4().hex
            for hours, workers in ((30, 2), (5, 6), (1, None)):
                payload = self.fixture()
                if workers:
                    payload['tuning'] = {'settled': {
                        'workers': workers, 'kernel': 'avx2', 'settled_ms': 1000 * workers,
                        'comparisons': 3, 'ram_limited': False, 'seed': 'table'}}
                self.insert(payload, hours=hours, identity=identity)
        debug = self.fixture()
        debug.update(debug_build=True, tuning={'settled': {'workers': 32, 'kernel': 'avx512'}})
        self.insert(debug)
        report = json.loads(self.query((ROOT / 'settled_report.sql').read_text()))
        self.assertNotIn('installation_id', json.dumps(report))
        self.assertEqual(report['installations'], 6)
        cohort, = report['cohorts']
        self.assertEqual(cohort['installations'], 6)
        self.assertEqual(cohort['kernel'], 'avx2')
        self.assertEqual(cohort['settled_ms_p50'], 6000)
        self.assertEqual(cohort['share_of_room'], 6 / 32)

    def test_rows_leave_with_their_reports(self):
        report = self.insert(self.fixture())
        self.insert(self.fixture())
        self.assertEqual(self.query(COUNT).strip(), '2')
        self.query("DELETE FROM telemetry_reports WHERE report_id = '%s'" % report)
        self.assertEqual(self.query(COUNT).strip(), '1')
        # A replayed report is ignored by the collector's insert and derives nothing new.
        payload = json.dumps(self.fixture()).replace("'", "''")
        self.query("INSERT INTO telemetry_reports (installation_id, report_id, payload) "
                   f"SELECT installation_id, report_id, '{payload}'::jsonb FROM telemetry_reports "
                   "ON CONFLICT (report_id) DO NOTHING")
        self.assertEqual(self.query(COUNT).strip(), '1')


if __name__ == '__main__':
    unittest.main()
