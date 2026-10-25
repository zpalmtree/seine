"""Never replay a potentially executed guest mutation after a transport error."""
import subprocess
import unittest
from unittest.mock import patch

import cloud


class SshRetryTests(unittest.TestCase):
    def setUp(self):
        self.ip = '203.0.113.10'
        self.error = f'ssh: connect to host {self.ip} port 22: Connection timed out\n'
        owner = patch.object(cloud, 'owned', return_value=({}, {'PublicIpAddress': self.ip}))
        sleep = patch.object(cloud.time, 'sleep')
        owner.start()
        sleep.start()
        self.addCleanup(owner.stop)
        self.addCleanup(sleep.stop)

    def test_preconnection_timeout_can_retry(self):
        answers = [subprocess.CompletedProcess([], 255, '', self.error),
                   subprocess.CompletedProcess([], 0, 'ok', '')]
        with patch.object(cloud.subprocess, 'run', side_effect=answers) as run:
            self.assertEqual(cloud.ssh('owned', 'mutation'), 'ok')
            self.assertEqual(run.call_count, 2)

    def test_preconnection_retries_are_bounded(self):
        answer = subprocess.CompletedProcess([], 255, '', self.error)
        with patch.object(cloud.subprocess, 'run', return_value=answer) as run:
            with self.assertRaises(RuntimeError):
                cloud.ssh('owned', 'mutation')
            self.assertEqual(run.call_count, 3)

    def test_ambiguous_or_executed_failures_are_not_replayed(self):
        answers = [subprocess.CompletedProcess([], 1, '', 'remote failure'),
                   subprocess.CompletedProcess([], 255, '', 'Connection reset by peer'),
                   subprocess.CompletedProcess([], 255, 'started', self.error)]
        for answer in answers:
            with self.subTest(answer=answer), patch.object(cloud.subprocess, 'run', return_value=answer) as run:
                with self.assertRaises(RuntimeError):
                    cloud.ssh('owned', 'mutation')
                self.assertEqual(run.call_count, 1)

    def test_command_timeout_is_not_replayed(self):
        with patch.object(cloud.subprocess, 'run', side_effect=subprocess.TimeoutExpired('ssh', 60)) as run:
            with self.assertRaises(subprocess.TimeoutExpired):
                cloud.ssh('owned', 'mutation')
            self.assertEqual(run.call_count, 1)


if __name__ == '__main__':
    unittest.main()
