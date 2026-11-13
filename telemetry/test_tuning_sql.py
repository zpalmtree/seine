"""Paired-trial screening against a disposable PostgreSQL database."""
import copy
import json
from pathlib import Path
import unittest
import uuid
import test_alerts_sql

ROOT = Path(__file__).parent


@unittest.skipUnless((test_alerts_sql.PG / 'initdb').exists(), 'PostgreSQL 16 binaries required')
class TuningSQLTests(unittest.TestCase):
    setUpClass = classmethod(test_alerts_sql.AlertSQLTests.setUpClass.__func__)
    cleanup = classmethod(test_alerts_sql.AlertSQLTests.cleanup.__func__)
    query = classmethod(test_alerts_sql.AlertSQLTests.query.__func__)
    setUp = test_alerts_sql.AlertSQLTests.setUp

    def fixture(self):
        return json.loads((ROOT / 'tuning-fixture.json').read_text())

    def insert(self, payload, identity=None, hours=0):
        identity = identity or uuid.uuid4().hex
        payload = copy.deepcopy(payload)
        payload['installation_id'] = identity
        payload['report_id'] = uuid.uuid4().hex
        text = json.dumps(payload).replace("'", "''")
        self.query("INSERT INTO telemetry_reports VALUES "
                   f"('{payload['report_id']}','{identity}',now()-interval '{hours} hours','{text}'::jsonb)")
        return identity

    def analysis(self):
        result = json.loads(self.query((ROOT / 'tuning_report.sql').read_text()))
        self.assertNotIn('installation_id', json.dumps(result))
        self.assertNotIn('run_id', json.dumps(result))
        return result

    def seed(self, mutate=None):
        identities = []
        for index in range(5):
            value = self.fixture()
            if mutate:
                mutate(value, index)
            identities.append(self.insert(value))
        return identities

    def test_short_paired_trials_rank_without_day_long_residence(self):
        self.assertEqual(self.analysis()['status'], 'unavailable')
        self.seed()
        result = self.analysis()
        hint, = result['ranked_hints']
        self.assertEqual(result['unique_trials'], 5)
        self.assertEqual(result['qualified_trials'], 5)
        self.assertEqual(hint['installations'], 5)
        self.assertAlmostEqual(hint['median_ratio'], 1.03)
        self.assertAlmostEqual(hint['maximum_quantization_ratio'], .001)
        self.assertEqual(hint['installations_with_resolved_gain'], 5)
        self.assertEqual(hint['median_wall_ms'], 190000)
        self.assertFalse(hint['automatic_default_eligible'])
        self.assertEqual(hint['status'], 'requires_native_validation')
        self.assertEqual(hint['confidence'], 'observational_support')
        self.assertEqual(hint['evidence_coverage']['observed_installations'], 5)

    def test_replays_do_not_add_trials_and_noisy_installation_has_one_vote(self):
        identities = self.seed()
        for _ in range(3):
            self.insert(self.fixture(), identities[0], hours=1)
        value = self.fixture()
        for sample in value['tuning']['trials'][0]['outcome']['samples']:
            if sample['arm'] == 'candidate': sample['hashes'] = 6000
        for trial_id in range(2, 20):
            value['tuning']['trials'][0]['trial_id'] = trial_id
            self.insert(value, identities[0])
        result = self.analysis()
        self.assertEqual(result['unique_trials'], 23)
        hint, = result['ranked_hints']
        self.assertEqual(hint['installations'], 5)
        self.assertAlmostEqual(hint['median_ratio'], 1.03)

    def test_conflicting_identity_is_excluded_not_cherry_picked(self):
        identities = self.seed()
        value = self.fixture()
        value['tuning']['trials'][0]['outcome']['samples'][1]['hashes'] = 5000
        self.insert(value, identities[0])
        result = self.analysis()
        self.assertEqual(result['conflicting_trial_identities'], 1)
        self.assertEqual(result['unique_trials'], 4)
        self.assertEqual(result['ranked_hints'], [])

    def test_changed_unknown_drifting_and_underresolved_contexts_do_not_rank(self):
        def trial(p): return p['tuning']['trials'][0]['outcome']
        mutations = [
            lambda p: trial(p)['samples'][1].update(changed=True),
            lambda p: trial(p)['samples'][1]['context']['members'][0].update(workers=7),
            lambda p: trial(p)['context_after']['resources'].update(allowed_logical_cpus=16),
            lambda p: trial(p)['samples'][1]['context']['resources'].update(effective_memory_mib=32768),
            lambda p: trial(p)['context_before']['resources'].update(placement_fingerprint=None),
            lambda p: trial(p)['samples'][2].update(hashes=6000),
            lambda p: trial(p)['samples'][1].update(elapsed_ms=1000),
            lambda p: trial(p)['samples'][1].update(hashes=100),
            lambda p: trial(p)['samples'][0].update(arm='candidate'),
            lambda p: p['diagnostics'].update(source_fingerprint='unknown'),
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.query('TRUNCATE telemetry_report_facts, telemetry_reports')
                self.seed(lambda p, i: mutation(p))
                result = self.analysis()
                self.assertEqual(result['unique_trials'], 5)
                self.assertEqual(result['qualified_trials'], 0)
                self.assertEqual(result['ranked_hints'], [])

    def test_aborts_inconclusive_and_small_no_gain_results_remain_visible(self):
        def changed(p, i):
            t = p['tuning']['trials'][0]['outcome']
            t.update(decision='aborted', reason='execution_failure', samples=[], applied=None)
        self.seed(changed)
        result = self.analysis()
        self.assertEqual(result['outcomes'][0]['reason'], 'execution_failure')
        self.assertEqual(result['qualified_trials'], 0)
        self.query('TRUNCATE telemetry_report_facts, telemetry_reports')
        self.seed(lambda p, i: p['tuning']['trials'][0]['outcome'].update(decision='inconclusive', reason='baseline_drift'))
        self.assertEqual(self.analysis()['outcomes'][0]['decision'], 'inconclusive')
        self.query('TRUNCATE telemetry_report_facts, telemetry_reports')
        def small(p, i):
            t = p['tuning']['trials'][0]['outcome']
            t.update(decision='no_confirmed_gain', applied='baseline')
            t['context_after'] = copy.deepcopy(t['context_before'])
            for s in t['samples']:
                if s['arm'] == 'candidate': s['hashes'] = 3001
        self.seed(small)
        result = self.analysis()
        self.assertEqual(result['outcomes'][0]['decision'], 'no_confirmed_gain')
        hint, = result['ranked_hints']
        self.assertGreater(hint['median_ratio'], 1)
        self.assertEqual(hint['installations_with_resolved_gain'], 0)
        self.assertFalse(hint['automatic_default_eligible'])

    def test_exact_context_separation_but_release_labels_and_free_memory_can_vary(self):
        def variations(p, i):
            p['miner_version'] = f'v0.2.{35+i}'
            p['diagnostics']['build_commit'] = f'{i+1:012x}'
            t = p['tuning']['trials'][0]['outcome']
            t['cost'] = dict(switching_ms=None, idle_ms=None)
            for j, sample in enumerate(t['samples']):
                sample['context']['resources']['available_memory_mib'] = 40000 - j*100
        self.seed(variations)
        hint, = self.analysis()['ranked_hints']
        self.assertEqual(len(hint['provenance']), 5)
        self.assertIsNone(hint['median_idle_ms'])
        self.assertEqual(hint['trials_with_idle_measurement'], 0)
        for mutation in [
            lambda p: p['diagnostics'].update(source_fingerprint='ffff1234'),
            lambda p: p['hardware'].update(runtime_environment='wsl2'),
            lambda p: p['diagnostics'].update(mode='daemon'),
            lambda p: p['tuning']['trials'][0]['outcome']['policy'].update(revision=2),
        ]:
            self.query('TRUNCATE telemetry_report_facts, telemetry_reports')
            self.seed(lambda p, i: mutation(p) if i < 2 else None)
            self.assertEqual(self.analysis()['ranked_hints'], [])

    def test_each_window_can_use_either_complete_acceptance_rule(self):
        def mixed(p, _):
            t = p['tuning']['trials'][0]['outcome']
            t['policy'].update(minimum_sample_ms=30000, minimum_hashes=32,
                               short_sample_ms=10000, short_minimum_hashes=256)
            t.update(decision='no_confirmed_gain', applied='baseline')
            t['context_after'] = copy.deepcopy(t['context_before'])
            t['samples'] = t['samples'][:3]
            for s in t['samples']:
                s.update(elapsed_ms=30000 if s['arm'] == 'candidate' else 10000,
                         hashes=64 if s['arm'] == 'candidate' else 300)
        self.seed(mixed)
        result = self.analysis()
        self.assertEqual(result['qualified_trials'], 5)
        hint, = result['ranked_hints']
        self.assertAlmostEqual(hint['median_ratio'], (64/30000)/(300/10000))
        self.assertFalse(hint['automatic_default_eligible'])
        for duration, hashes in [(10000, 32), (30000, 31), (5000, 500)]:
            with self.subTest(duration=duration, hashes=hashes):
                self.query('TRUNCATE telemetry_report_facts, telemetry_reports')
                def rejected(p, i):
                    mixed(p, i)
                    p['tuning']['trials'][0]['outcome']['samples'][1].update(
                        elapsed_ms=duration, hashes=hashes)
                self.seed(rejected)
                self.assertEqual(self.analysis()['qualified_trials'], 0)
        # Optional alternative thresholds are part of the exact policy cohort.
        self.query('TRUNCATE telemetry_report_facts, telemetry_reports')
        def split(p, i):
            mixed(p, i)
            if i < 2:
                p['tuning']['trials'][0]['outcome']['policy']['short_minimum_hashes'] = 257
        self.seed(split)
        result = self.analysis()
        self.assertEqual(result['qualified_trials'], 5)
        self.assertEqual(result['ranked_hints'], [])

    def test_failed_confirmation_cannot_hide_behind_positive_median(self):
        for decision in ['gain', 'no_confirmed_gain']:
            with self.subTest(decision=decision):
                self.query('TRUNCATE telemetry_report_facts, telemetry_reports')
                def split_cycles(p, _):
                    t = p['tuning']['trials'][0]['outcome']
                    t['samples'][1]['hashes'] = 3600  # First cycle +20%.
                    t['samples'][4]['hashes'] = 3000  # Second cycle flat.
                    t.update(decision=decision, applied='candidate' if decision == 'gain' else 'baseline')
                    t['context_after'] = copy.deepcopy(t['context_before'])
                    if decision == 'gain':
                        t['context_after']['members'] = copy.deepcopy(t['candidate'])
                self.seed(split_cycles)
                hint, = self.analysis()['ranked_hints']
                self.assertAlmostEqual(hint['median_ratio'], 1.1)
                self.assertEqual(hint['installations_with_resolved_gain'], 0)
                self.assertEqual(hint['confidence'], 'mixed_or_limited_evidence')
        self.query('TRUNCATE telemetry_report_facts, telemetry_reports')
        self.seed(lambda p, _: p['tuning']['trials'][0]['outcome'].update(
            samples=p['tuning']['trials'][0]['outcome']['samples'][:3]))
        hint, = self.analysis()['ranked_hints']
        self.assertEqual(hint['installations_with_resolved_gain'], 0)
        self.query('TRUNCATE telemetry_report_facts, telemetry_reports')
        def unacknowledged(p, _):
            t = p['tuning']['trials'][0]['outcome']
            t['applied'] = 'baseline'
            t['context_after'] = copy.deepcopy(t['context_before'])
        self.seed(unacknowledged)
        hint, = self.analysis()['ranked_hints']
        self.assertEqual(hint['installations_with_resolved_gain'], 0)

    def test_aborts_are_in_confidence_denominator_and_all_outcome_costs(self):
        identities = self.seed()
        aborted = self.fixture()
        t = aborted['tuning']['trials'][0]['outcome']
        t.update(decision='aborted', reason='execution_failure', samples=[], applied=None,
                 finished_ms=t['started_ms'] + 1000000)
        t['context_after']['members'] = []
        t['cost'] = dict(idle_ms=9000, switching_ms=None)
        # One healthy-looking installation with mostly failures is not a vote
        # for a gain, even though only its successful trial has a ratio.
        for trial_id in range(2, 5):
            aborted['tuning']['trials'][0]['trial_id'] = trial_id
            self.insert(aborted, identities[0])
        hint, = self.analysis()['ranked_hints']
        self.assertEqual(hint['installations_with_resolved_gain'], 4)
        # Ten additional failed installations must not disappear from confidence.
        for _ in range(10):
            self.insert(aborted)
        result = self.analysis()
        hint, = result['ranked_hints']
        coverage = hint['evidence_coverage']
        self.assertEqual(coverage['observed_installations'], 15)
        self.assertEqual(coverage['aborted_trials'], 13)
        self.assertEqual(coverage['unqualified_trials'], 13)
        self.assertEqual(coverage['median_wall_ms'], 1000000)
        self.assertEqual(coverage['median_idle_ms'], 9000)
        self.assertEqual(coverage['trials_with_switch_measurement'], 5)
        self.assertEqual(hint['confidence'], 'mixed_or_limited_evidence')

    def test_coverage_only_context_staged_kinds_and_stale_support_are_explicit(self):
        self.seed(lambda p, _: p['tuning']['trials'][0]['outcome']['context_before']['resources'].update(
            allowed_logical_cpus=None, placement_fingerprint=None))
        result = self.analysis()
        self.assertEqual(result['coverage_only_trials'], 5)
        self.assertEqual(result['missing_context_trials'], 5)
        self.assertEqual(result['ranked_hints'], [])
        self.query('TRUNCATE telemetry_report_facts, telemetry_reports')
        self.seed(lambda p, _: p['tuning']['trials'][0]['outcome'].update(kind='cpu_joint'))
        result = self.analysis()
        self.assertEqual(result['staged_kind_trials'], 5)
        self.assertEqual(result['qualified_trials'], 0)
        self.query('TRUNCATE telemetry_report_facts, telemetry_reports')
        for _ in range(5):
            self.insert(self.fixture(), hours=72)
        hint, = self.analysis()['ranked_hints']
        self.assertEqual(hint['installations_with_resolved_gain'], 5)
        self.assertEqual(hint['confidence'], 'mixed_or_limited_evidence')
        self.assertEqual(hint['freshness'], 'latest_qualified_trial_older_than_48h')
        recent_loss = self.fixture()
        t = recent_loss['tuning']['trials'][0]['outcome']
        t.update(decision='no_confirmed_gain', applied='baseline')
        t['context_after'] = copy.deepcopy(t['context_before'])
        t['samples'][1]['hashes'] = 3000
        t['samples'][4]['hashes'] = 3000
        self.insert(recent_loss)
        hint, = self.analysis()['ranked_hints']
        self.assertEqual(hint['freshness'], 'latest_qualified_trial_within_48h')
        self.assertEqual(hint['confidence'], 'mixed_or_limited_evidence')


if __name__ == '__main__':
    unittest.main()
