"""Exercise agent briefing outcomes without production access."""
import contextlib
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import agent_report


class BriefingTests(unittest.TestCase):
    def run_briefing(self, quality, query_failure=False, diagnostic_status='unavailable', tuning=None,
                     release=None):
        results = [
            SimpleNamespace(returncode=0, stdout='service healthy', stderr=''),
            SimpleNamespace(returncode=0, stdout=json.dumps(quality), stderr=''),
            SimpleNamespace(returncode=1 if query_failure else 0,
                            stdout=json.dumps({'performance_candidates': []}),
                            stderr='query failed' if query_failure else ''),
            SimpleNamespace(returncode=0, stdout=json.dumps({'status': diagnostic_status}), stderr=''),
            SimpleNamespace(returncode=0, stdout=json.dumps(tuning or {'status': 'unavailable', 'ranked_hints': []}), stderr=''),
            SimpleNamespace(returncode=0, stdout=json.dumps({'status': 'ok', 'cohorts': []}), stderr=''),
            SimpleNamespace(returncode=0, stdout=json.dumps(release or {'status': 'unavailable'}), stderr=''),
        ]
        output = io.StringIO()
        with patch.object(agent_report, 'remote', side_effect=results), \
             patch('sys.argv', ['agent_report.py']), contextlib.redirect_stdout(output):
            code = agent_report.main()
        return code, json.loads(output.getvalue())

    def test_inventory_only_failure_is_not_reported_as_sparse_data(self):
        code, briefing = self.run_briefing({'status': 'error'})
        self.assertEqual(code, 1)
        self.assertEqual(briefing['health']['status'], 'ok')
        self.assertIn('Investigate the miner sampling path', briefing['assessment'])

    def test_sparse_healthy_data_remains_successful(self):
        code, briefing = self.run_briefing({'status': 'ok'})
        self.assertEqual(code, 0)
        self.assertIn('No qualifying performance cohorts', briefing['assessment'])
        self.assertIn('verify producer release adoption', briefing['tuning_assessment'])

    def test_query_failure_keeps_successful_parts(self):
        code, briefing = self.run_briefing({'status': 'ok'}, query_failure=True)
        self.assertEqual(code, 1)
        self.assertEqual(briefing['data_quality']['status'], 'ok')
        self.assertIn('health/query failure', briefing['assessment'])

    def test_tuning_failure_and_hints_keep_their_distinct_interpretation(self):
        _, failed = self.run_briefing({'status': 'ok'}, tuning={'status': 'error'})
        self.assertIn('query failed', failed['tuning_assessment'])
        _, ranked = self.run_briefing({'status': 'ok'}, tuning={'status': 'ok', 'ranked_hints': [{}]})
        self.assertIn('matching native validation', ranked['tuning_assessment'])

    def test_arriving_coverage_only_trials_are_not_reported_as_missing_producer(self):
        _, briefing = self.run_briefing({'status': 'ok'}, tuning={
            'status': 'ok', 'unique_trials': 12, 'coverage_only_trials': 12,
            'missing_context_trials': 12, 'ranked_hints': [],
        })
        self.assertIn('Paired trials are arriving', briefing['tuning_assessment'])
        self.assertIn('12 lack required initial context', briefing['tuning_assessment'])

    def test_missing_diagnostics_are_not_healthy_miners(self):
        code, briefing = self.run_briefing({'status': 'ok'})
        self.assertEqual(code, 0)
        self.assertIn('No diagnostic coverage', briefing['diagnostic_assessment'])

    def test_fleet_failures_trigger_attention_without_losing_performance(self):
        code, briefing = self.run_briefing({'status': 'ok'}, diagnostic_status='attention')
        self.assertEqual(code, 1)
        self.assertIn('issue clusters', briefing['diagnostic_assessment'])
        self.assertEqual(briefing['telemetry']['performance_candidates'], [])

    def test_release_health_states_keep_distinct_meaning(self):
        code, missing = self.run_briefing({'status': 'ok'})
        self.assertEqual(code, 0)
        self.assertIn('unknown, not healthy', missing['release_health_assessment'])
        code, attention = self.run_briefing({'status': 'ok'}, release={'status': 'attention'})
        self.assertEqual(code, 1)
        self.assertIn('install_failure_stages', attention['release_health_assessment'])
        self.assertEqual(attention['diagnostic_assessment'], missing['diagnostic_assessment'])
        code, ok = self.run_briefing({'status': 'ok'}, release={'status': 'ok', 'amd': {}})
        self.assertEqual(code, 0)
        self.assertIn('amd_gpu_not_mining', ok['release_health_assessment'])
        self.assertEqual(ok['release_health']['amd'], {})


if __name__ == '__main__':
    unittest.main()
