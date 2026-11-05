"""Exercise schema-14 release-health aggregates against disposable PostgreSQL."""
import copy
import json
import uuid
from pathlib import Path
import unittest
import test_alerts_sql

ROOT = Path(__file__).resolve().parent


@unittest.skipUnless((test_alerts_sql.PG / 'initdb').exists(), 'PostgreSQL 16 binaries required')
class ReleaseHealthSQLTests(unittest.TestCase):
    # Reuse the existing disposable database setup, without inheriting its test cases.
    setUpClass = classmethod(test_alerts_sql.AlertSQLTests.setUpClass.__func__)
    cleanup = classmethod(test_alerts_sql.AlertSQLTests.cleanup.__func__)
    query = classmethod(test_alerts_sql.AlertSQLTests.query.__func__)
    setUp = test_alerts_sql.AlertSQLTests.setUp

    @staticmethod
    def fixture():
        return json.loads((ROOT / 'schema14-fixture.json').read_text())

    def insert(self, payload, identity=None, hours=0):
        text = json.dumps(payload).replace("'", "''")
        self.query("INSERT INTO telemetry_reports VALUES "
                   f"('{uuid.uuid4().hex}','{identity or uuid.uuid4().hex}',"
                   f"now()-interval '{hours} hours','{text}'::jsonb)")

    def report(self):
        result = json.loads(self.query((ROOT / 'release_health.sql').read_text()))
        self.assertNotIn('installation_id', json.dumps(result))
        return result

    def test_empty_and_legacy_windows_are_unavailable_not_healthy(self):
        self.assertEqual(self.report()['status'], 'unavailable')
        legacy = json.loads((ROOT / 'reliability-fixture.json').read_text())
        self.insert(legacy)
        result = self.report()
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['coverage'], [{'version': legacy['miner_version'], 'installations': 1,
                                               'schema14_installations': 0}])
        self.assertEqual(result['update']['versions'], [])
        self.assertEqual(result['pool']['installations'], 0)
        self.assertEqual(result['gpu_kernel_sources'], [])

    def test_update_health_sums_windows_and_counts_latest_auto_install(self):
        base = self.fixture()
        identity = uuid.uuid4().hex
        self.insert(base, identity, hours=3)
        later = copy.deepcopy(base)
        later['diagnostics']['update'].update(auto_install=False, install_failures=2,
                                              last_install_failure='checksum')
        self.insert(later, identity, hours=1)
        clean = copy.deepcopy(base)
        clean['diagnostics']['update'] = {'auto_install': True, 'checks_api': 2, 'checks_web': 0,
                                          'checks_failed': 0, 'installs': 1, 'install_failures': 0}
        self.insert(clean)
        result = self.report()
        version, = result['update']['versions']
        self.assertEqual(version['installations'], 2)
        # The first installation's latest report turned auto-install off.
        self.assertEqual(version['auto_install_installations'], 1)
        self.assertEqual((version['checks_api'], version['checks_web'], version['checks_failed']), (4, 2, 2))
        self.assertEqual((version['installs'], version['installations_installing']), (1, 1))
        self.assertEqual((version['install_failures'], version['installations_with_install_failures']), (3, 1))
        stages = {s['stage']: (s['reports'], s['installations']) for s in result['update']['install_failure_stages']}
        self.assertEqual(stages, {'signature': (1, 1), 'checksum': (1, 1)})
        self.assertEqual(result['status'], 'ok')

    def test_first_hash_counts_each_run_once_and_finds_backends_that_never_hashed(self):
        base = self.fixture()
        identity = uuid.uuid4().hex
        # Two reports of one run repeat the record; a later run is a new observation.
        self.insert(base, identity, hours=3)
        self.insert(base, identity, hours=2)
        restarted = copy.deepcopy(base)
        restarted['diagnostics']['backend_startup']['first_hashes'][2]['first_hash_ms'] = 11000
        self.insert(restarted, identity, hours=1)
        # A second card that never joined, after long enough to be stuck.
        stuck = copy.deepcopy(base)
        stuck['diagnostics']['backend_startup']['expected_amd'] = 2
        self.insert(stuck)
        # Still starting: too early to call the card stuck.
        early = copy.deepcopy(stuck)
        early['diagnostics']['uptime_secs'] = 300
        early['window_secs'] = 300
        self.insert(early)
        # No work at all says nothing about the backends.
        idle = copy.deepcopy(stuck)
        idle['diagnostics']['backend_startup']['first_hashes'] = []
        self.insert(idle)
        startup = self.report()['startup']
        cohorts = {(c['backend'], c['gpu_arch']): c for c in startup['first_hash']}
        amd = cohorts['amd', 'gfx1100']
        # Installations: identity (two runs), stuck and early.
        self.assertEqual((amd['installations'], amd['first_hash_ms_max']), (3, 31000))
        self.assertEqual(amd['runs'], 4)
        self.assertEqual(amd['lag_behind_cpu_ms_p50'], 30100)
        self.assertEqual(cohorts['nvidia', 'sm_86']['lag_behind_cpu_ms_p90'], 3300)
        self.assertEqual(cohorts['cpu', 'n/a']['lag_behind_cpu_ms_p50'], 0)
        missing = {b['backend']: b for b in startup['backends_not_hashing']}
        self.assertEqual(missing['amd']['installations'], 2)
        self.assertEqual(missing['amd']['installations_missing_backends'], 1)
        self.assertEqual(missing['amd']['instances_not_hashing'], 1)
        self.assertEqual(missing['nvidia']['installations_missing_backends'], 0)

    def test_default_metal_idle_before_the_fork_is_not_a_missing_backend(self):
        base = self.fixture()
        base['diagnostics']['backend_startup']['expected_metal'] = 1
        # Before the fork the default Metal backend waits idle on Argon2 work.
        self.insert(base)
        self.insert(base)
        # Once the run mines BLAKE2b, an idle Metal is a missing backend again.
        forked = copy.deepcopy(base)
        forked['schema_version'] = 15
        forked['backends'][0]['settings']['pow'] = 'blake2b'
        self.insert(forked)
        metal = {b['backend']: b for b in self.report()['startup']['backends_not_hashing']}['metal']
        self.assertEqual(metal['installations'], 3)
        self.assertEqual(metal['installations_idle_until_fork'], 2)
        self.assertEqual(metal['installations_missing_backends'], 1)
        self.assertEqual(metal['instances_not_hashing'], 1)

    def test_fork_readiness_counts_paused_installations_and_events_by_version(self):
        base = self.fixture()
        for version, mode in (('v0.3.0', 'pool'), ('v0.3.0', 'pool'), ('v0.2.36', 'daemon')):
            value = copy.deepcopy(base)
            value['miner_version'] = version
            value['diagnostics']['mode'] = mode
            if mode == 'daemon':
                value['diagnostics'].pop('pool')
            self.insert(value)
        ready = copy.deepcopy(base)
        ready['diagnostics']['unsupported_work_paused_secs'] = 0
        ready['diagnostics']['events'] = []
        self.insert(ready)
        result = self.report()
        paused = {(p['version'], p['mode']): p for p in result['fork_readiness']['paused_versions']}
        self.assertEqual(set(paused), {('v0.3.0', 'pool'), ('v0.2.36', 'daemon')})
        self.assertEqual(paused['v0.3.0', 'pool']['paused_installations'], 2)
        self.assertEqual(paused['v0.3.0', 'pool']['paused_secs'], 240)
        events = {e['version']: e for e in result['fork_readiness']['unsupported_work_event_versions']}
        self.assertEqual(events['v0.3.0']['installations'], 2)
        self.assertEqual(events['v0.3.0']['occurrences'], 6)
        self.assertEqual(events['v0.2.36']['installations_24h'], 1)
        self.assertEqual({r['version']: r['affected_installations'] for r in result['recent_unsupported_work_versions']},
                         {'v0.3.0': 2, 'v0.2.36': 1})
        self.assertEqual(result['status'], 'ok')

    def test_amd_not_mining_cohort_excludes_machines_without_amd_gpus(self):
        base = self.fixture()
        variants = [
            # AMD GPU visible to the OS, HIP missing: the ROCm cohort.
            {'hip_unavailable': True, 'found': 0, 'selected': 0, 'integrated_skipped': 0,
             'unsupported_skipped': 0, 'pci_devices': 1},
            # HIP works but only an integrated GPU was found.
            {'hip_unavailable': False, 'found': 1, 'selected': 0, 'integrated_skipped': 1,
             'unsupported_skipped': 0, 'pci_devices': 1},
            # Ordinary non-AMD machines are not AMD problems.
            {'hip_unavailable': True, 'found': 0, 'selected': 0, 'integrated_skipped': 0,
             'unsupported_skipped': 0, 'pci_devices': 0},
            {'hip_unavailable': True, 'found': 0, 'selected': 0, 'integrated_skipped': 0,
             'unsupported_skipped': 0},
        ]
        for detection in variants:
            value = copy.deepcopy(base)
            # A CPU without built-in graphics (Ryzen 5000 desktop).
            value['hardware'].update(cpu_family=25, cpu_model_id=33)
            value['diagnostics']['amd_detection'] = detection
            self.insert(value)
        self.insert(base)  # Mining on one of two AMD GPUs.
        result = self.report()
        # The skipped integrated GPU is not an idle AMD GPU.
        self.assertEqual(result['amd']['amd_gpu_not_mining_installations'], 1)
        cohort, = result['amd']['detection']
        self.assertEqual(cohort['installations'], 5)
        self.assertEqual(cohort['amd_present'], 3)
        self.assertEqual(cohort['mining_on_amd'], 1)
        self.assertEqual(cohort['not_mining_hip_unavailable'], 1)
        self.assertEqual(cohort['not_mining_none_selected'], 0)
        self.assertEqual(cohort['pci_unknown'], 1)
        self.assertEqual(cohort['hip_unavailable_any'], 3)
        self.assertEqual(cohort['installations_skipping_integrated'], 2)

    def test_built_in_ryzen_graphics_are_not_idle_amd_gpus(self):
        base = self.fixture()
        no_hip = {'hip_unavailable': True, 'found': 0, 'selected': 0, 'integrated_skipped': 0,
                  'unsupported_skipped': 0, 'pci_devices': 1}
        # Before schema 15 a Ryzen 7000 desktop counts its own graphics.
        legacy = copy.deepcopy(base)
        legacy['hardware'].update(cpu_model='AMD Ryzen 9 7950X 16-Core Processor',
                                  cpu_family=25, cpu_model_id=97)
        legacy['diagnostics']['amd_detection'] = dict(no_hip)
        self.insert(legacy)
        # Brand strings naming Radeon graphics are APUs.
        apu = copy.deepcopy(legacy)
        apu['hardware'].update(cpu_model='AMD Ryzen 7 5800H with Radeon Graphics',
                               cpu_family=25, cpu_model_id=80)
        self.insert(apu)
        # From schema 15 the count already excludes built-in graphics.
        current = copy.deepcopy(legacy)
        current['schema_version'] = 15
        self.insert(current)
        # A Ryzen 7000 with a discrete card on top still counts it.
        both = copy.deepcopy(legacy)
        both['diagnostics']['amd_detection']['pci_devices'] = 2
        self.insert(both)
        result = self.report()
        self.assertEqual(result['amd']['amd_gpu_not_mining_installations'], 2)

    def test_gpu_kernel_sources_and_perf_levels_use_latest_report_per_installation(self):
        base = self.fixture()
        identity = uuid.uuid4().hex
        old = copy.deepcopy(base)
        old['backends'][1]['settings']['gpu_kernel_source'] = 'nvrtc_failed_prebuilt_ptx'
        old['backends'][2]['settings']['amd_perf_level'] = 'raised'
        self.insert(old, identity, hours=5)
        self.insert(base, identity, hours=1)
        other = copy.deepcopy(base)
        other['backends'].append(copy.deepcopy(other['backends'][1]))
        other['backends'][1]['settings']['gpu_kernel_source'] = 'prebuilt_cubin'
        other['backends'][3]['settings']['gpu_kernel_source'] = 'prebuilt_cubin'
        self.insert(other)
        result = self.report()
        sources = {(k['backend'], k['gpu_arch'], k['gpu_kernel_source']): (k['installations'], k['devices'])
                   for k in result['gpu_kernel_sources']}
        self.assertEqual(sources, {
            ('nvidia', 'sm_86', 'nvrtc_cached'): (1, 1),
            ('nvidia', 'sm_86', 'prebuilt_cubin'): (1, 2),
            ('amd', 'gfx1100', 'hiprtc'): (2, 2),
        })
        level, = result['amd']['perf_levels']
        self.assertEqual((level['gpu_arch'], level['amd_perf_level'], level['installations']),
                         ('gfx1100', 'needs_root', 2))

    def test_pool_settings_and_churn_distribution(self):
        base = self.fixture()
        identity = uuid.uuid4().hex
        self.insert(base, identity, hours=2)
        self.insert(base, identity, hours=1)
        plain = copy.deepcopy(base)
        plain['diagnostics']['pool'] = {'tls': False, 'socks': True, 'payout_memo': False, 'backup_pools': 0,
                                        'failovers': 0, 'secs_on_backup': 0, 'reconnects': 0, 'refills': 0}
        self.insert(plain)
        daemon = copy.deepcopy(base)
        daemon['diagnostics']['mode'] = 'daemon'
        daemon['diagnostics'].pop('pool')
        self.insert(daemon)
        pool = self.report()['pool']
        self.assertEqual(pool['installations'], 2)
        self.assertEqual((pool['tls_installations'], pool['socks_installations'],
                          pool['payout_memo_installations'], pool['backup_pool_installations']), (1, 1, 1, 1))
        self.assertEqual((pool['failovers'], pool['reconnects'], pool['refills'], pool['secs_on_backup']),
                         (2, 6, 8, 1200))
        self.assertEqual((pool['installations_failing_over'], pool['installations_on_backup']), (1, 1))
        self.assertEqual(pool['per_installation']['reconnects_p50'], 3)
        self.assertAlmostEqual(pool['per_installation']['reconnects_p90'], 5.4)
        self.assertEqual(sum(d['reports'] for d in pool['daily']), 3)
        version, = pool['versions']
        self.assertEqual(version['installations_reconnecting'], 1)

    def test_fleet_install_failures_need_three_recent_installations(self):
        base = self.fixture()
        base['diagnostics']['unsupported_work_paused_secs'] = 0
        for hours in (1, 2, 30):
            self.insert(base, hours=hours)
        result = self.report()
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['recent_install_failure_versions'][0]['affected_installations'], 2)
        self.insert(base)
        self.assertEqual(self.report()['status'], 'attention')


if __name__ == '__main__':
    unittest.main()
