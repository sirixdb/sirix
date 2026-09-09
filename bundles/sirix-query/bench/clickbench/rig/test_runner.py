"""A normal child exit must not be confused with live observer permission loss."""
import errno
import os
import subprocess
import sys
import time
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

PF_EXITING = 0x4
PF_RANDOMIZE = 0x400000


# The kernel's own field order, captured before any test patches the reader under test.
STAT_TEMPLATE = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()


def stat_line(state, flags):
    """A /proc/PID/stat line in the kernel's own field order, restated with the run state and task
    flags a case needs. Keeping the real layout keeps the reader honest about the parenthesised
    comm field a JVM's command name sits in and the offsets after it."""
    fields = list(STAT_TEMPLATE)
    fields[0], fields[6] = state, str(flags)
    return f'{os.getpid()} (java) ' + ' '.join(fields)


class TeardownWindow:
    """The two /proc reads `process_snapshot` makes, replaying the window a real leg hit: the task
    is still running when its `stat` is read, has detached its address space by the time `io` is
    read -- which is what makes the kernel answer EACCES -- and is reported as leaving by the
    re-read the tolerance depends on. `Popen.poll()` is None throughout, because the child is not
    reaped until the main thread has finished draining its stdout."""

    def __init__(self, *states):
        self.states = list(states)
        self.stat_reads = 0

    def __call__(self, path, *arguments, **keywords):
        if path.name == 'io':
            raise PermissionError(errno.EACCES, 'Permission denied', str(path))
        state, flags = self.states[min(self.stat_reads, len(self.states)-1)]
        self.stat_reads += 1
        return stat_line(state, flags)


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

    def test_a_child_denying_its_counters_on_the_way_out_costs_a_sample_not_the_leg(self):
        """The regression. A complete leg -- all queries answered, exit code 0 -- was discarded as
        ABORT because the observer read `/proc/PID/io` in the instant between the child entering
        do_exit and being reaped, and Linux answers EACCES there. The tolerance cannot be asked of
        `Popen.poll()`, which is still None in that window: the main thread only reaps the child
        after draining its stdout. So the kernel is asked instead."""
        process = Mock(pid=os.getpid())
        process.poll.return_value = None
        window = TeardownWindow(('S', PF_RANDOMIZE), ('R', PF_RANDOMIZE | PF_EXITING))
        with patch('runner.Path.read_text', autospec=True, side_effect=window):
            self.assertIsNone(process_snapshot(process))
        self.assertEqual(window.stat_reads, 2, 'the denial must be settled by re-reading the task')
        process.poll.assert_not_called()

    def test_a_child_reaped_before_the_denial_is_settled_is_no_sample(self):
        """The same window, one step further on: the task is gone by the time it is re-read."""
        window = TeardownWindow(('S', PF_RANDOMIZE))

        def vanishing(path, *arguments, **keywords):
            if path.name == 'stat' and window.stat_reads:
                raise FileNotFoundError(errno.ENOENT, 'No such file or directory', str(path))
            return window(path, *arguments, **keywords)

        process = Mock(pid=os.getpid())
        process.poll.return_value = None
        with patch('runner.Path.read_text', autospec=True, side_effect=vanishing):
            self.assertIsNone(process_snapshot(process))

    def test_a_live_child_denying_its_counters_is_still_a_failed_regime(self):
        """Real kernel denial, not a simulated one: a child that clears its dumpable flag fails the
        same `ptrace_may_access` check that an exiting one fails, while staying alive. Nothing about
        this run is on its way out, so hiding the denial would hide a measurement the observer can
        no longer see."""
        child = subprocess.Popen([sys.executable, '-c',
                                  'import ctypes, time; ctypes.CDLL(None).prctl(4, 0, 0, 0, 0);'
                                  ' print("ready", flush=True); time.sleep(30)'],
                                 stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), 'ready')
            try:
                Path(f'/proc/{child.pid}/io').read_text()
            except PermissionError:
                pass
            else:
                self.skipTest('this kernel discloses /proc/PID/io for a non-dumpable child')
            self.assertIsNone(child.poll(), 'the denial under test must come from a live child')
            with self.assertRaises(PermissionError):
                process_snapshot(child)
        finally:
            child.kill()
            child.stdout.close()
            child.wait()

    def test_a_child_the_kernel_already_reports_as_leaving_is_no_sample(self):
        """A real exited-but-unreaped child: `/proc/PID/stat` still answers, `/proc/PID/io` does
        not, and the task carries PF_EXITING. This is the state the lost leg raced into."""
        child = subprocess.Popen([sys.executable, '-c', 'pass'])
        try:
            # Deliberately not process.wait(): the child must stay unreaped, which is what the
            # observer sees while the main thread is still draining a real measured JVM's stdout.
            deadline = time.monotonic()+30
            while time.monotonic() < deadline:
                fields = Path(f'/proc/{child.pid}/stat').read_text().rsplit(')', 1)[1].split()
                if fields[0] in ('Z', 'X') or int(fields[6]) & PF_EXITING:
                    break
                time.sleep(.005)
            else:
                self.fail('the child never reached an observable exit state')
            self.assertIsNone(process_snapshot(Mock(pid=child.pid)))
        finally:
            child.wait()


if __name__ == '__main__':
    unittest.main()
