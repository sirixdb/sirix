"""CONV-205 timeout ownership with real processes and a simulated Gradle PID receipt.

These tests prove the executor's receipt/termination boundary, including a reused daemon;
they do not launch Gradle or prove its init-script integration against a Gradle distribution.
"""
import contextlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from test_pr_mutation import mutation


DAEMON = '''import json, os, socket, subprocess, sys
from pathlib import Path
root = Path(sys.argv[1])
server = socket.socket(socket.AF_UNIX)
server.bind(str(root / 'daemon.sock'))
server.listen()
while True:
    connection, _ = server.accept()
    with connection:
        request = json.loads(connection.makefile().readline())
        Path(request['receipt']).write_text(str(os.getpid()))
        if request.get('report'):
            report = Path(request['report'])
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text('<mutations><mutation status="KILLED" detected="true">'
                              '<sourceFile>Foo.java</sourceFile><mutatedClass>a.Foo</mutatedClass>'
                              '<lineNumber>11</lineNumber></mutation></mutations>')
        if request['mode'] == 'hang':
            worker = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
                                      start_new_session=True)
            (root / 'worker.pid').write_text(str(worker.pid))
            worker.wait()
        connection.sendall(b'done')
'''
LAUNCHER = '''import json, socket, sys, time
from pathlib import Path
root = Path(sys.argv[1])
request = json.loads(sys.argv[2])
with socket.socket(socket.AF_UNIX) as client:
    for _ in range(500):
        try:
            client.connect(str(root / 'daemon.sock'))
            break
        except (FileNotFoundError, ConnectionRefusedError):
            time.sleep(.01)
    client.sendall((json.dumps(request) + '\\n').encode())
    client.recv(4)
'''
PROBE = '''import subprocess, sys
from pathlib import Path
root = Path(sys.argv[1])
pid = (root / 'worker.pid').read_text()
state = subprocess.run(['ps', '-p', pid, '-o', 'stat='], capture_output=True, text=True).stdout.strip()
assert not state or state.startswith('Z'), 'previous module is still running'
(root / 'next.started').write_text('stopped')
report = Path(sys.argv[2])
report.parent.mkdir(parents=True, exist_ok=True)
report.write_text('<mutations><mutation status="KILLED" detected="true">'
                  '<sourceFile>Foo.java</sourceFile><mutatedClass>a.Foo</mutatedClass>'
                  '<lineNumber>11</lineNumber></mutation></mutations>')
'''


def running(pid):
    state = subprocess.run(['ps', '-p', str(pid), '-o', 'stat='], capture_output=True,
                           text=True, check=False).stdout.strip()
    return bool(state and not state.startswith('Z'))


@contextlib.contextmanager
def daemon_fixture():
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        script = root / 'daemon.py'
        script.write_text(DAEMON)
        daemon = subprocess.Popen([sys.executable, str(script), str(root)],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                  start_new_session=True)
        try:
            yield root, daemon
        finally:
            if (root / 'worker.pid').exists():
                worker = int((root / 'worker.pid').read_text())
                if running(worker):
                    os.kill(worker, signal.SIGKILL)
            if daemon.poll() is None:
                daemon.kill()
            daemon.wait(timeout=5)


def command(root, directory, mode):
    receipt = directory / 'daemon.pid'
    request = {'receipt': str(receipt), 'mode': mode,
               'report': str(directory / 'report/mutations.xml')}
    return [sys.executable, '-c', LAUNCHER, str(root), json.dumps(request),
            '-Dsirix.prMutationDaemonPidFile=' + str(receipt)]


class ReusedDaemonTimeoutTest(unittest.TestCase):
    def test_timeout_waits_for_reused_daemon_and_detached_worker(self):
        with daemon_fixture() as (root, daemon):
            self.assertEqual(mutation.execute(command(root, root, 'quick'), root,
                                               root / 'first.log', 5), 0)
            self.assertTrue(running(daemon.pid))
            with self.assertRaises(subprocess.TimeoutExpired):
                mutation.execute(command(root, root, 'hang'), root, root / 'second.log', 1)
            worker = int((root / 'worker.pid').read_text())
            self.assertFalse(running(worker), 'worker outlived execute')
            self.assertFalse(running(daemon.pid), 'serving daemon outlived execute')

    def test_artifact_reads_and_next_module_follow_completed_termination(self):
        with daemon_fixture() as (root, daemon):
            self.assertEqual(mutation.execute(command(root, root, 'quick'), root,
                                               root / 'warm.log', 5), 0)
            output = root / 'out'
            output.mkdir()
            read_report = mutation.read_report
            observations = []

            def read_stopped_report(*args, **kwargs):
                self.assertFalse(running(int((root / 'worker.pid').read_text())))
                self.assertFalse(running(daemon.pid))
                observations.append('read')
                return read_report(*args, **kwargs)

            def prepare(root, directory, module, sources, result, *args):
                result.update(targets=['a.Foo'], tests=['a.FooTest*'],
                              mutants_per_class=None, restricted=False)
                if module == 'sirix-core':
                    return command(root, directory, 'hang')
                return [sys.executable, '-c', PROBE, str(root),
                        str(directory / 'report/mutations.xml')]

            selected = {'sirix-core': ['Foo.java'], 'sirix-query': ['Foo.java']}
            with patch.object(mutation, 'prepare_analysis', side_effect=prepare), \
                 patch.object(mutation, 'changed_line_index', return_value={}), \
                 patch.object(mutation, 'read_report', side_effect=read_stopped_report):
                results = mutation.analyze_modules(root, output, selected,
                                                  {name: {} for name in selected},
                                                  4, None, False)
            self.assertEqual([r['state'] for r in results], ['timed out', 'complete'])
            self.assertEqual(observations, ['read', 'read'])
            self.assertEqual((root / 'next.started').read_text(), 'stopped')

    def test_stale_receipt_cannot_claim_an_unrelated_process(self):
        with daemon_fixture() as (root, daemon):
            receipt = root / 'daemon.pid'
            receipt.write_text(str(daemon.pid))
            args = [sys.executable, '-c', 'import time; time.sleep(60)',
                    '-Dsirix.prMutationDaemonPidFile=' + str(receipt)]
            with self.assertRaises(subprocess.TimeoutExpired):
                mutation.execute(args, root, root / 'run.log', .1)
            self.assertTrue(running(daemon.pid))
