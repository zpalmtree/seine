"""Exercise diagnostic grouping against disposable PostgreSQL, including legacy reports."""
import copy
import json
import uuid
from pathlib import Path
import unittest
import test_alerts_sql


@unittest.skipUnless((test_alerts_sql.PG / 'initdb').exists(), 'PostgreSQL 16 binaries required')
class DiagnosticSQLTests(unittest.TestCase):
    # Reuse the existing disposable database setup, without inheriting its test cases.
    setUpClass = classmethod(test_alerts_sql.AlertSQLTests.setUpClass.__func__)
    cleanup = classmethod(test_alerts_sql.AlertSQLTests.cleanup.__func__)
    query = classmethod(test_alerts_sql.AlertSQLTests.query.__func__)
    setUp = test_alerts_sql.AlertSQLTests.setUp
    def insert_diagnostic(self, payload, identity=None):
        text = json.dumps(payload).replace("'", "''")
        self.query("INSERT INTO telemetry_reports VALUES "
                   f"('{uuid.uuid4().hex}','{identity or uuid.uuid4().hex}',now(),'{text}'::jsonb)")

    def diagnostics(self):
        result = json.loads(self.query((Path(__file__).parent / 'diagnostics.sql').read_text()))
        alerts = json.loads(self.query((Path(__file__).parent / 'diagnostic_alerts.sql').read_text()))
        for key, value in alerts.items():
            self.assertEqual(value, result[key], key)
        return result

    def test_missing_sample_context_keeps_connection_evidence_separate(self):
        fixture = json.loads((Path(__file__).parent / 'diagnostics-fixture.json').read_text())
        for index in range(5):
            identity = uuid.uuid4().hex
            value = copy.deepcopy(fixture)
            value['backends'] = []
            value['diagnostics']['reason'] = 'periodic'
            value['diagnostics']['phase'] = 'mining'
            value['diagnostics']['events'] = []
            if index < 2:
                value['diagnostics']['events'] = [{
                    'detail': {'operation': 'pool_connect', 'reason': 'connection_timed_out'},
                }]
            for hour in (5, 3, 1):
                payload = json.dumps(value).replace("'", "''")
                self.query("INSERT INTO telemetry_reports VALUES "
                           f"('{uuid.uuid4().hex}','{identity}',now()-interval '{hour} hours','{payload}'::jsonb)")
        result = json.loads(self.query((Path(__file__).parent / 'data_quality.sql').read_text()))
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['versions'][0]['sustained_inventory_only_installations'], 5)
        clusters = result['inventory_only_clusters']
        self.assertEqual({c['connection_warning']: c['installations'] for c in clusters}, {True: 2, False: 3})
        self.assertTrue(all(c['observed_mining_phase'] for c in clusters))
        self.assertNotIn('installation_id', json.dumps(result))

    def test_alert_counts_keep_distinct_installations_and_time_windows(self):
        self.assertEqual(self.diagnostics()['status'], 'unavailable')
        fixture = json.loads((Path(__file__).parent / 'diagnostics-fixture.json').read_text())
        fixture['diagnostics']['previous_run_unclean'] = False
        fixture['diagnostics']['events'] = [
            {'code': 'run_failed', 'kind': 'general', 'count': 1},
            {'code': 'warning', 'kind': 'general', 'count': 1,
             'detail': {'operation': 'pool_login', 'reason': 'pool_connection_limit'}}]
        identity = uuid.uuid4().hex
        for hour, install in ((0.2, identity), (0.4, identity),
                              (2, uuid.uuid4().hex), (24.5, uuid.uuid4().hex),
                              (26, uuid.uuid4().hex)):
            text = json.dumps(fixture).replace("'", "''")
            self.query('INSERT INTO telemetry_reports VALUES '
                       f"('{uuid.uuid4().hex}','{install}',now()-interval '{hour} hours','{text}'::jsonb)")
        result = self.diagnostics()
        self.assertEqual(result['recent_error_versions'][0]['affected_installations'], 2)
        pool = result['pool_connection_warnings']
        self.assertEqual(pool['last_hour_installations'], 1)
        self.assertEqual(pool['last_hour_limit_installations'], 1)
        self.assertAlmostEqual(pool['baseline_hourly_installations'], 2 / 24)

    def test_schema8_context_groups_loaded_software_and_interrupted_cleanup(self):
        fixture = json.loads((Path(__file__).parent / 'reliability-fixture.json').read_text())
        fixture['schema_version'] = 8
        fixture['diagnostics']['previous_run']['miner_version'] = fixture['miner_version']
        for version, stage in ((12080, 'stopping_backends'), (12090, 'stopping_tui')):
            value = copy.deepcopy(fixture)
            d = value['diagnostics']
            d['nvidia_software'] = {'driver_cuda_version': version, 'nvrtc_version': 12090}
            d['shutdown_request'] = 'api_stop'
            d['cleanup_stage'] = 'finalizing_telemetry'
            d['previous_run']['shutdown_request'] = 'tui_quit'
            d['previous_run']['cleanup_stage'] = stage
            d['previous_run']['nvidia_software'] = {'driver_cuda_version': 12060}
            self.insert_diagnostic(value)
        report = self.diagnostics()
        self.assertEqual({c['nvidia_software']['driver_cuda_version'] for c in report['issue_clusters']}, {12080, 12090})
        self.assertEqual({c['last_cleanup_stage'] for c in report['unclean_run_clusters']}, {'stopping_backends', 'stopping_tui'})
        for cluster in report['unclean_run_clusters']:
            self.assertEqual(cluster['previous_nvidia_software'], {'driver_cuda_version': 12060})
        self.assertEqual(report['shutdown_clusters'][0]['affected_installations'], 2)
        self.assertEqual(report['shutdown_clusters'][0]['shutdown_request'], 'api_stop')
        self.assertEqual(report['recent_unclean_versions'][0]['affected_installations'], 2)

    def test_shutdown_request_is_context_not_proof_of_clean_exit(self):
        fixture = json.loads((Path(__file__).parent / 'reliability-fixture.json').read_text())
        fixture['schema_version'] = 7
        fixture['diagnostics']['previous_run']['miner_version'] = fixture['miner_version']
        for reason in (None, 'console_close', 'interrupt'):
            value = copy.deepcopy(fixture)
            if reason:
                value['diagnostics']['previous_run']['shutdown_request'] = reason
            self.insert_diagnostic(value)
        result = self.diagnostics()
        self.assertEqual({c['shutdown_request'] for c in result['unclean_run_clusters']},
                         {None, 'console_close', 'interrupt'})
        self.assertEqual(result['recent_unclean_versions'][0]['affected_installations'], 3)
        self.assertEqual(result['status'], 'attention')

    def test_typed_causes_phases_and_legacy_fields_remain_distinct(self):
        fixture = json.loads((Path(__file__).parent / 'diagnostics-fixture.json').read_text())
        fixture['diagnostics']['events'][0]['count'] = 100
        identity = uuid.uuid4().hex
        self.insert_diagnostic(fixture, identity)
        self.insert_diagnostic(fixture, identity)
        other = copy.deepcopy(fixture)
        other['diagnostics']['events'][0]['detail']['os_error'] = 13
        self.insert_diagnostic(other)
        legacy = copy.deepcopy(fixture)
        legacy['schema_version'] = 3
        legacy['diagnostics'].pop('phase')
        legacy['diagnostics'].pop('previous_run')
        legacy['diagnostics']['events'][0].pop('detail')
        legacy['diagnostics']['events'][0].pop('phase')
        legacy['backends'] = []
        self.insert_diagnostic(legacy)
        report = self.diagnostics()
        self.assertEqual(report['status'], 'ok')
        self.assertEqual(len(report['issue_clusters']), 3)
        causes = {c['detail']['os_error']: c for c in report['issue_clusters'] if c['detail']}
        self.assertEqual(causes[12]['affected_installations'], 1)
        self.assertEqual(causes[12]['occurrences'], 200)
        self.assertEqual(causes[12]['detail']['requested_bytes'], 2147483648)
        self.assertEqual(causes[12]['phase'], 'initializing_backends')
        counts = {r['version']: r['affected_installations'] for r in report['recent_unclean_versions']}
        self.assertEqual(counts, {fixture['miner_version']: 2, 'unknown': 1})
        observed = next(c for c in report['unclean_run_clusters'] if c['last_observed_phase'])
        self.assertEqual(observed['affected_installations'], 2)
        self.assertEqual(observed['installations_hashing_in_restart_report'], 2)
        self.assertEqual(observed['minimum_previous_uptime_secs'], 19)
        unknown = next(c for c in report['unclean_run_clusters'] if not c['last_observed_phase'])
        self.assertIsNone(unknown['previous_build_commit'])
        self.assertEqual(unknown['installations_hashing_in_restart_report'], 0)
        self.assertNotIn(identity, json.dumps(report))
        self.assertNotIn('installation_id', json.dumps(report))

    def test_upgrade_attributes_previous_build_and_unknown_is_not_new_release(self):
        fixture = json.loads((Path(__file__).parent / 'diagnostics-fixture.json').read_text())
        fixture['miner_version'] = 'v0.2.35'
        fixture['diagnostics']['previous_run_unclean'] = False
        fixture['diagnostics'].pop('previous_run')
        fixture['diagnostics']['events'] = []
        self.insert_diagnostic(fixture)
        restart = copy.deepcopy(fixture)
        restart['miner_version'] = 'v0.2.36'
        restart['diagnostics']['build_commit'] = 'bbbbbbbbbbbb'
        restart['diagnostics']['previous_run_unclean'] = True
        restart['diagnostics']['previous_run'] = {
            'build_commit': 'abcdef123456', 'phase': 'mining', 'uptime_secs': 60}
        for _ in range(3):
            self.insert_diagnostic(restart)
        unknown = copy.deepcopy(restart)
        unknown['diagnostics']['previous_run']['build_commit'] = 'cccccccccccc'
        self.insert_diagnostic(unknown)
        result = self.diagnostics()
        counts = {r['version']: r['affected_installations'] for r in result['recent_unclean_versions']}
        self.assertEqual(counts, {'v0.2.35': 3, 'unknown': 1})
        self.assertEqual(result['status'], 'attention')
        self.assertEqual(result['recent_error_versions'], [])
        old = next(c for c in result['unclean_run_clusters'] if c['version'] == 'v0.2.35')
        self.assertEqual(old['reporting_versions'], ['v0.2.36'])

        # Conflicting source labels cannot resolve a historical checkpoint.
        fixture['miner_version'] = 'different-label'
        self.insert_diagnostic(fixture)
        self.assertEqual(self.diagnostics()['recent_unclean_versions'][0]['version'], 'unknown')

    def test_checkpoint_events_are_context_not_new_errors_and_explicit_version_survives(self):
        fixture = json.loads((Path(__file__).parent / 'reliability-fixture.json').read_text())
        fixture['diagnostics']['events'] = []
        fixture['diagnostics']['previous_run']['miner_version'] = 'v0.2.30'
        fixture['diagnostics']['previous_run']['events_truncated'] = True
        identity = uuid.uuid4().hex
        self.insert_diagnostic(fixture, identity)
        self.insert_diagnostic(fixture, identity)
        result = self.diagnostics()
        self.assertEqual(result['recent_error_versions'], [])
        self.assertEqual(result['issue_clusters'], [])
        cluster, = result['previous_run_issue_clusters']
        self.assertEqual(cluster['version'], 'v0.2.30')
        self.assertEqual(cluster['code'], 'rust_panic')
        self.assertEqual(cluster['affected_installations'], 1)
        self.assertNotIn('occurrences', cluster)
        self.assertEqual(result['unclean_run_clusters'][0]['installations_with_truncated_previous_events'], 1)
        self.assertNotIn(identity, json.dumps(result))

    def test_unique_failure_is_not_hidden_by_common_warning_limit(self):
        fixture = json.loads((Path(__file__).parent / 'diagnostics-fixture.json').read_text())
        for line in range(40):
            value = copy.deepcopy(fixture)
            value['diagnostics']['events'][0].update(line=line, count=1000)
            value['diagnostics']['events'][0].pop('detail')
            self.insert_diagnostic(value)
        fixture['diagnostics']['events'][0]['code'] = 'run_failed'
        self.insert_diagnostic(fixture)
        report = self.diagnostics()
        self.assertEqual(len(report['issue_clusters']), 30)
        self.assertEqual(report['issue_clusters'][0]['code'], 'run_failed')


if __name__ == '__main__':
    unittest.main()
