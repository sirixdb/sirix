"""A normal child exit must not be confused with live observer permission loss."""
import unittest
import json
from pathlib import Path
import tempfile
from unittest.mock import Mock
from unittest.mock import patch

from runner import machine_settings
from runner import power_domains
from runner import process_snapshot
from runner import run_leg


class ObserverTest(unittest.TestCase):
    def test_full_leg_records_three_attempts_for_every_query(self):
        def collect(runtime, database, output, queries, protocol, lease):
            timings = {(q, attempt): float(attempt) for q in queries for attempt in (1, 2, 3)}
            return timings, {'gated_limits_uw': 50_000_000, 'changes': []}

        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            with patch('runner.run_part', side_effect=collect) as collector:
                result = run_leg({'runtime_id': 'frozen'}, directory, Path(directory)/'leg', {}, Mock())
            self.assertEqual(collector.call_count, 1)
            self.assertEqual(list(collector.call_args.args[3]), list(range(43)))
            self.assertEqual(result['result'], [[1., 2., 3.]]*43)
            self.assertNotIn('steering_result', result)
            self.assertEqual(result['rig']['scope'], 'steering')
            self.assertEqual(result['rig']['observed_power']['gated_limits_uw'], 50_000_000)
            self.assertEqual(json.loads((Path(directory)/'leg/leg.json').read_text()), result)

    def test_platform_power_domains_are_recorded_but_never_policy_gated(self):
        # A firmware-managed limit that moves must not invalidate an hours-long collection, so the
        # equality-gated policy excludes every powercap domain while the record still carries them.
        self.assertEqual([path for path in machine_settings() if 'powercap' in path], [])
        for path, value in power_domains().items():
            self.assertIn('powercap', path)
            self.assertEqual(Path(path).read_text().strip(), value)

    def test_teardown_permission_race_is_not_a_failed_measurement(self):
        process = Mock(pid=123)
        process.poll.return_value = 0
        with patch('runner.Path.read_text', side_effect=PermissionError('process exited')):
            self.assertIsNone(process_snapshot(process))

    def test_live_permission_failure_still_invalidates_measurement(self):
        process = Mock(pid=123)
        process.poll.return_value = None
        with patch('runner.Path.read_text', side_effect=PermissionError('live denied')):
            with self.assertRaises(PermissionError):
                process_snapshot(process)

    def test_zombie_counters_are_not_reported_as_live(self):
        with patch('runner.Path.read_text', return_value='123 (java) Z 1 2 3'):
            self.assertIsNone(process_snapshot(Mock(pid=123)))


if __name__ == '__main__':
    unittest.main()
