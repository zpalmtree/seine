"""Alert lifecycle, privacy, delivery and threshold tests; no external messages."""
import copy
import io
import json
from pathlib import Path
import tempfile
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import alerts


class AlertsTests(unittest.TestCase):
    def test_query_failure_logs_class_without_database_details(self):
        result = SimpleNamespace(returncode=1, stdout='', stderr=(
            'ERROR: canceling statement due to statement timeout\n'
            'SECRET connection=private report=private'))
        with patch('sys.stderr', new_callable=io.StringIO) as output:
            alerts.check_failure('diagnostics', result=result)
        self.assertEqual(output.getvalue(),
                         'Telemetry diagnostics check failed: database statement timeout.\n')

    def test_failed_query_stays_unknown_and_uses_lightweight_diagnostics(self):
        ok = SimpleNamespace(returncode=0, stdout='{}', stderr='')
        failed = SimpleNamespace(returncode=1, stdout='', stderr='private SQL error')
        with patch.object(alerts, 'run', side_effect=[ok, ok, failed, ok]) as run, \
                patch('sys.stderr', new_callable=io.StringIO) as output:
            results = alerts.collect()
        self.assertIsNone(results['diagnostics'])
        self.assertEqual(run.call_args_list[2].args[1],
                         (Path(__file__).parent / 'diagnostic_alerts.sql').read_text())
        self.assertNotIn('private SQL error', output.getvalue())

    def test_query_timeout_and_invalid_response_are_bounded(self):
        for error, expected in [
                (subprocess.TimeoutExpired('secret command', 45), 'process timeout'),
                (ValueError('private payload'), 'invalid JSON response'),
                (OSError('secret path'), 'process unavailable')]:
            with patch('sys.stderr', new_callable=io.StringIO) as output:
                alerts.check_failure('diagnostics', error=error)
            self.assertEqual(output.getvalue(),
                             f'Telemetry diagnostics check failed: {expected}.\n')

    def setUp(self):
        self.state = {'incidents': {}}
        self.sent = []
        self.saved = []
        self.item = alerts.issue('diagnostics', 'v1', 'Three installations with errors')

    def tick(self, now, active=None, known=None, sender=None):
        return alerts.advance(self.state, [self.item] if active is None else active,
                              {self.item['category']} if known is None else known, now,
                              sender or (lambda *args: self.sent.append(args)),
                              lambda state: self.saved.append(copy.deepcopy(state)))

    def test_confirmation_repeat_and_recovery(self):
        self.item = alerts.issue('infrastructure', 'health', 'Collector unavailable')
        self.tick(1000)
        self.tick(1100)
        self.assertEqual(len(self.sent), 0)
        self.tick(1300)
        self.assertEqual(self.sent[-1][0], 'FIRING')
        self.tick(1600)
        self.assertEqual(len(self.sent), 1)
        self.tick(1300 + alerts.REPEAT_SECONDS)
        self.assertEqual(self.sent[-1][0], 'REMINDER')
        self.tick(16000, [])
        self.tick(16300, [])
        self.assertEqual(self.sent[-1][0], 'CLEARED')
        self.assertEqual(self.state['incidents'], {})

    def test_fleet_incidents_stay_quiet_until_cleared_and_recur(self):
        for category in ('quality', 'diagnostics', 'performance'):
            with self.subTest(category=category):
                self.setUp()
                self.item = alerts.issue(category, 'v1', 'Known fleet finding', affected=3)
                self.tick(1000, known={category})
                self.tick(1300, known={category})
                # Existing state from the old runner is enough to suppress repeats,
                # even after restart, count changes, and several reminder periods.
                self.state = json.loads(json.dumps(self.state))
                self.item.update(affected=7, summary='Updated aggregate count')
                for now in (1600, 16000, 31000, 100000):
                    self.tick(now, known={category})
                self.assertEqual([x[0] for x in self.sent], ['FIRING'])
                self.assertEqual(self.state['incidents'][self.item['key']]['affected'], 7)
                self.tick(100300, [], known={category})
                self.tick(100600, [], known={category})
                self.tick(100900, known={category})
                self.tick(101200, known={category})
                self.assertEqual([x[0] for x in self.sent], ['FIRING', 'CLEARED', 'FIRING'])

    def test_collector_outages_keep_reminders(self):
        for category in ('infrastructure', 'ingestion', 'monitor'):
            with self.subTest(category=category):
                self.setUp()
                self.item = alerts.issue(category, 'outage', 'Collector unavailable')
                self.tick(1000, known={category})
                self.tick(1300, known={category})
                self.tick(1300 + alerts.REPEAT_SECONDS - 1, known={category})
                self.assertEqual(len(self.sent), 1)
                self.tick(1300 + alerts.REPEAT_SECONDS, known={category})
                self.assertEqual([x[0] for x in self.sent], ['FIRING', 'REMINDER'])

    def test_quiet_incident_does_not_suppress_a_new_version(self):
        self.tick(1000)
        self.tick(1300)
        new = alerts.issue('diagnostics', 'v2', 'New version errors')
        self.tick(1600, [self.item, new])
        self.tick(1900, [self.item, new])
        self.assertEqual([x[1]['key'] for x in self.sent], [self.item['key'], new['key']])

    def test_transient_does_not_notify(self):
        self.tick(1000)
        self.tick(1300, [])
        self.assertEqual(self.sent, [])
        self.assertEqual(self.state['incidents'], {})

    def test_monitor_downtime_does_not_confirm_a_pending_incident(self):
        self.tick(1000)
        self.tick(10000)
        self.assertEqual(self.sent, [])
        self.tick(10300)
        self.assertEqual(self.sent[-1][0], 'FIRING')

    def test_failed_delivery_is_not_acknowledged_and_is_retried(self):
        self.tick(1000)
        with self.assertRaises(OSError):
            self.tick(1300, sender=lambda *args: (_ for _ in ()).throw(OSError('offline')))
        self.assertIsNone(self.state['incidents'][self.item['key']]['last_sent'])
        self.tick(1600)
        self.assertEqual(self.sent[-1][0], 'FIRING')
        self.tick(1900, [])
        with self.assertRaises(OSError):
            self.tick(2200, [], sender=lambda *args: (_ for _ in ()).throw(OSError('offline')))
        self.assertIn(self.item['key'], self.state['incidents'])
        self.tick(2500, [])
        self.assertEqual(self.sent[-1][0], 'CLEARED')

    def test_unknown_query_does_not_clear_and_breaks_recovery_confirmation(self):
        self.tick(1000)
        self.tick(1300)
        self.tick(1600, [])
        self.tick(1900, [], known=set())
        self.tick(2200, [])
        self.assertEqual(len(self.sent), 1)
        self.tick(2500, [])
        self.assertEqual(self.sent[-1][0], 'CLEARED')

    def test_restart_preserves_deduplication(self):
        self.tick(1000)
        self.tick(1300)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            alerts.save(path, self.state)
            self.state = json.loads(path.read_text())
            self.tick(1600)
            self.assertEqual(len(self.sent), 1)

    def test_delivery_budget_eventually_sends_backlog(self):
        items = [alerts.issue('diagnostics', str(i), 'error') for i in range(15)]
        self.tick(1000, items)
        self.tick(1300, items)
        self.assertEqual(len(self.sent), 10)
        self.tick(1600, items)
        self.assertEqual(len(self.sent), 15)

    def test_aggregate_thresholds_and_unknown_categories(self):
        results = {'infrastructure': False, 'quality': {'versions': [
            {'version': 'v1', 'sustained_inventory_only_installations': 4},
            {'version': 'v2', 'sustained_inventory_only_installations': 5},
            {'version': 'v0.2.45', 'sustained_inventory_only_installations': 9, 'legacy': True}]},
            'diagnostics': {'recent_error_versions': [
                {'version': 'v1', 'affected_installations': 2},
                {'version': 'v2', 'affected_installations': 3},
                {'version': 'v0.2.45', 'affected_installations': 67, 'needs_attention': False}],
                'recent_unclean_versions': []},
            'performance': None}
        active, known = alerts.evaluate(results)
        self.assertEqual({i['category'] for i in active}, {'monitor', 'infrastructure', 'quality', 'diagnostics'})
        self.assertEqual([i['key'] for i in active if i['category'] == 'diagnostics'],
                         [alerts.issue('diagnostics', ['recent_error_versions', 'v2'], '')['key']])
        quality = {i['version']: i for i in active if i['category'] == 'quality'}
        self.assertEqual((quality['v2']['severity'], quality['v0.2.45']['severity']), ('warning', 'info'))
        self.assertNotIn('performance', known)
        self.assertNotIn('ingestion', known)
        results['performance'] = {'minutes_since_last_report': 120, 'performance_cohorts': []}
        active, known = alerts.evaluate(results)
        self.assertIn('ingestion', {i['category'] for i in active})
        results['performance']['minutes_since_last_report'] = None
        self.assertIn('ingestion', {i['category'] for i in alerts.evaluate(results)[0]})

    def test_unclean_alert_labels_interrupted_release_and_unknown_explicitly(self):
        active, _ = alerts.evaluate({'diagnostics': {
            'recent_error_versions': [], 'recent_unclean_versions': [
                {'version': 'v0.2.35', 'affected_installations': 3, 'reporting_versions': ['v0.2.36']},
                {'version': 'unknown', 'affected_installations': 4, 'reporting_versions': ['v0.2.26']},
            ]}})
        self.assertIn('Previous run v0.2.35', active[0]['summary'])
        self.assertNotIn('v0.2.36:', active[0]['summary'])
        self.assertIn('Unknown previous version', active[1]['summary'])

    def test_update_failures_and_unsupported_work_alert_at_three_installations(self):
        active, _ = alerts.evaluate({'diagnostics': {
            'recent_error_versions': [], 'recent_unclean_versions': [],
            'recent_install_failure_versions': [
                {'version': 'v0.3.0', 'affected_installations': 2},
                {'version': 'v0.3.1', 'affected_installations': 3}],
            'recent_unsupported_work_versions': [
                {'version': 'v0.3.0', 'affected_installations': 4}]}})
        summaries = [item['summary'] for item in active]
        self.assertEqual(len(summaries), 2)
        self.assertIn('v0.3.1: 3 installations with failed self-update installs', summaries[0])
        self.assertIn('v0.3.0: 4 installations with mining paused', summaries[1])

    def test_pool_connection_spike_needs_breadth_above_the_prior_day(self):
        def evaluate(recent, baseline):
            return alerts.evaluate({'diagnostics': {
                'recent_error_versions': [], 'recent_unclean_versions': [],
                'pool_connection_warnings': {
                    'last_hour_installations': recent, 'last_hour_limit_installations': 4,
                    'baseline_hourly_installations': baseline}}})
        # 2026-10-29: the connection cap took warnings from ~45 to 216 per hour.
        active, known = evaluate(216, 45.0)
        self.assertEqual([i['category'] for i in active], ['pool'])
        self.assertIn('pool', known)
        self.assertIn('216 installations', active[0]['summary'])
        self.assertIn('4 were refused', active[0]['summary'])
        self.assertEqual(evaluate(130, 45.0)[0], [])
        self.assertEqual(evaluate(19, 0.0)[0], [], 'a handful of miners is not a fleet spike')
        self.assertEqual(len(evaluate(20, 0.0)[0]), 1)

    def test_regression_requires_breadth_not_one_bad_rig(self):
        cohort = dict(version='v2', baseline_version='v1', hardware={'cpu_model': 'CPU'},
                      build=[], settings={'backend': 'cpu'}, installations=5,
                      affected_installations=2, median_ratio=.7)
        results = {'performance': {'minutes_since_last_report': 0, 'performance_cohorts': [cohort]}}
        self.assertEqual(alerts.evaluate(results)[0], [])
        cohort['affected_installations'] = 3
        self.assertEqual(len(alerts.evaluate(results)[0]), 1)
        cohort['median_ratio'] = .81
        self.assertEqual(alerts.evaluate(results)[0], [])

    def test_transport_sends_embed_without_mentions(self):
        class Response:
            status = 200
            def read(self, size): return b'{"id":"test-message"}'
            def __enter__(self): return self
            def __exit__(self, *args): pass
        with patch('urllib.request.build_opener') as opener:
            opener.return_value.open.return_value = Response()
            alerts.deliver('https://discord.com/api/webhooks/fake/test', 'FIRING', self.item, 1000)
            request = opener.return_value.open.call_args.args[0]
            self.assertTrue(request.full_url.endswith('?wait=true'))
            body = json.loads(request.data)
            self.assertEqual(body['allowed_mentions'], {'parse': []})
            self.assertNotIn('content', body)
            self.assertEqual(body, alerts.format_message('FIRING', self.item, 1000))
        self.assertNotIn('@', alerts.label('@everyone **https://private/**'))
        for url in ('http://discord.com/api/webhooks/a/b', 'https://attacker.test/api/webhooks/a/b', 'https://discord.com/other'):
            with self.assertRaises(ValueError): alerts.validate_webhook(url)
        alerts.validate_webhook('https://discord.com/api/webhooks/a/b')

    def test_known_legacy_quality_explains_upgrade_without_hiding_incident(self):
        for version in ('v0.2.24', '0.2.24', 'v0.2.25', '0.2.25', 'v0.2.26', 'v0.2.27', 'custom'):
            with self.subTest(version=version):
                active, _ = alerts.evaluate({'quality': {'versions': [
                    {'version': version, 'sustained_inventory_only_installations': 99}]}})
                self.assertEqual(len(active), 1)
                record = active[0]
                legacy = version.lstrip('v') in ('0.2.24', '0.2.25')
                self.assertEqual(record['severity'], 'info' if legacy else 'warning')
                self.assertEqual('Upgrade to v0.2.26' in record['action'], legacy)
                embed = alerts.format_message('FIRING', record, 1792965060)['embeds'][0]
                fields = {field['name']: field['value'] for field in embed['fields']}
                self.assertEqual(fields['Version'], version)
                self.assertEqual(fields['Affected installations'], '99')
                self.assertIn('do not establish that mining stopped', embed['description'])
                self.assertEqual(embed['color'], 0x3498DB if legacy else 0xF39C12)

    def test_existing_state_refreshes_presentation_without_resending(self):
        self.tick(1000)
        self.tick(1300)
        self.item.update(version='v0.2.26', affected=7, action='Inspect samples', severity='info')
        self.tick(1600)
        record = self.state['incidents'][self.item['key']]
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(record['affected'], 7)
        self.assertEqual(record['last_sent'], 1300)
        self.assertEqual(record['first_seen'], 1000)

    def test_embed_states_bounds_and_legacy_state(self):
        # Existing state has only category/key/summary; clearing still renders.
        for transition in ('FIRING', 'REMINDER', 'CLEARED', 'SETUP TEST'):
            record = alerts.issue('setup' if transition == 'SETUP TEST' else 'infrastructure',
                                  'fixture', 'x' * 10000, action='a' * 10000)
            embed = alerts.format_message(transition, record, 1792965060)['embeds'][0]
            self.assertLessEqual(len(embed['title']), 256)
            self.assertLessEqual(len(embed['description']), 4096)
            self.assertTrue(all(len(f['value']) <= 1024 for f in embed['fields']))
            self.assertLess(len(json.dumps(embed)), 6000)
            self.assertIn('EDT', embed['footer']['text'])
            self.assertIn(record['key'], embed['footer']['text'])
            if transition == 'CLEARED':
                self.assertEqual(embed['color'], 0x2ECC71)
                self.assertIn('does not prove every miner recovered', json.dumps(embed))
                self.assertNotIn('Next step', json.dumps(embed))
            elif transition == 'SETUP TEST':
                self.assertEqual(embed['color'], 0x3498DB)
            else:
                self.assertEqual(embed['color'], 0xE74C3C)
        cleared = alerts.format_message('CLEARED', self.item, 1000)['embeds'][0]
        self.assertIn('EST', cleared['footer']['text'])


if __name__ == '__main__':
    unittest.main()
