#!/usr/bin/env python3
"""Budget reconciliation guards; no AWS calls or files are written."""
import datetime as dt
import unittest

import cloud


class ReservationTests(unittest.TestCase):
    def record(self, seconds=1800):
        start = dt.datetime(2026, 10, 24, 10, tzinfo=dt.timezone.utc)
        return {'terminated': True, 'volumes_deleted': True,
                'volume_ids': ['vol-fixture'],
                'launched_at': start.isoformat(),
                'verified_at': (start + dt.timedelta(seconds=seconds)).isoformat(),
                'hourly_usd': .68,
                'cost_reserved_usd': (.68 + .005 + 16 * .08 / (28 * 24)) * 3 + 1}

    def test_round_up_and_preserve_transfer_allowance(self):
        for seconds, hours in [(0, 1), (3599, 1), (3600, 1), (3601, 2), (9000, 3), (10800, 3)]:
            with self.subTest(seconds=seconds):
                record = self.record(seconds)
                initial = record['cost_reserved_usd']
                released = cloud.finalize_reserve(record)
                self.assertEqual(record['closed_billing_hours_upper'], hours)
                self.assertAlmostEqual(record['cost_reserved_usd'] + released, initial)
                self.assertAlmostEqual(record['cost_reserved_usd'],
                    (.68 + .005 + 16 * .08 / (28 * 24)) * hours + 1)
                self.assertEqual(cloud.finalize_reserve(record), 0)

    def test_requires_both_cleanup_proofs(self):
        for field in ['terminated', 'volumes_deleted']:
            record = self.record()
            record[field] = False
            with self.assertRaises(AssertionError):
                cloud.finalize_reserve(record)

    def test_rejects_negative_or_unbounded_duration(self):
        for seconds in [-1, 10801]:
            with self.assertRaises(AssertionError):
                cloud.finalize_reserve(self.record(seconds))

    def test_unknown_disks_do_not_release_budget(self):
        record = self.record()
        record['volume_ids'] = []
        with self.assertRaises(AssertionError):
            cloud.finalize_reserve(record)


if __name__ == '__main__':
    unittest.main()
