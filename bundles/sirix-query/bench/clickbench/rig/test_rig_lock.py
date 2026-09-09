"""Kernel lock ownership tests with small Python subprocesses, never benchmark JVMs."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from rig_lock import HOST_FD
from rig_lock import LEGACY_FD
from rig_lock import RigLease
from rig_lock import lock_paths
from rig_lock import verify_descriptor
from runtime import CAMPAIGN_DIRECTORY
from runtime import POINTER_FILE
from runtime import RIG_WORK


class RigLockTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)/'lease'
        self.path.touch()

    def probe(self, shared=False):
        script = ('import fcntl,sys\n'
                  'with open(sys.argv[1], "r+") as f:\n'
                  ' try: fcntl.flock(f.fileno(), int(sys.argv[2]) | fcntl.LOCK_NB)\n'
                  ' except BlockingIOError: sys.exit(7)\n')
        return subprocess.run([sys.executable, '-c', script, str(self.path), '1' if shared else '2'],
                              check=False).returncode

    def test_the_launcher_pins_the_resolved_campaign_pointer_into_every_child(self):
        """A rig-launched JVM must classify its database exactly as the launcher did. The JVM
        resolves the same chain itself, so the launcher hands it the pointer it resolved rather than
        leaving the JVM to whatever the operator's shell happened to hold -- which is how a campaign
        round could take a shared lease while its own manifest recorded it as campaign."""
        work = Path(self.directory.name)/'work'
        work.mkdir()
        campaign = Path(self.directory.name)/'clickbench-seg100m-20260908-1200'
        (campaign/'db').mkdir(parents=True)
        (work/POINTER_FILE).write_text(str(campaign)+'\n')
        with patch.dict(os.environ, {}, clear=True), patch('runtime.rig_work', return_value=work):
            with RigLease(timeout=0, paths=[(HOST_FD, self.path)]) as lease:
                child = lease.child_environment()
        self.assertEqual(child[CAMPAIGN_DIRECTORY], str(campaign))
        # Only the pointer. Exporting the rig work directory too would silently opt every child into
        # the legacy leg.lock its parent never took, and rig_lock.py --check would then refuse.
        self.assertNotIn(RIG_WORK, child)
        with patch.dict(os.environ, child, clear=True):
            self.assertEqual([key for key, _ in lock_paths()], [HOST_FD])
        with patch.dict(os.environ, dict(child, **{RIG_WORK: str(work)}), clear=True):
            self.assertEqual([key for key, _ in lock_paths()], [HOST_FD, LEGACY_FD])

    def test_a_launcher_that_resolves_no_campaign_pointer_invents_one_for_nobody(self):
        with patch.dict(os.environ, {}, clear=True), \
                patch('runtime.rig_work', return_value=Path(self.directory.name)/'absent'):
            with RigLease(timeout=0, paths=[(HOST_FD, self.path)]) as lease:
                child = lease.child_environment()
        self.assertNotIn(CAMPAIGN_DIRECTORY, child)

    def test_stale_empty_file_never_blocks_and_is_never_unlinked(self):
        inode = self.path.stat().st_ino
        with RigLease(timeout=0, paths=[(HOST_FD, self.path)]) as lease:
            lease.verify()
            self.assertEqual(self.probe(), 7)
        self.assertEqual(self.probe(), 0)
        self.assertEqual(self.path.stat().st_ino, inode)

    def test_shared_small_owners_exclude_large_owner(self):
        with RigLease(exclusive=False, timeout=0, paths=[(HOST_FD, self.path)]):
            self.assertEqual(self.probe(shared=True), 0)
            self.assertEqual(self.probe(), 7)

    def test_unlocked_or_wrong_file_descriptor_is_not_ownership(self):
        with self.path.open('r+') as stream:
            with self.assertRaises(ValueError):
                verify_descriptor(stream.fileno(), self.path)
        with RigLease(timeout=0, paths=[(HOST_FD, self.path)]) as lease:
            wrong = self.path.with_name('different-inode')
            wrong.touch()
            with self.assertRaises(ValueError):
                verify_descriptor(lease.pass_fds[0], wrong)

    def test_failed_second_lock_closes_first(self):
        missing = self.path.parent/'absent-directory'/'lease'
        with self.assertRaises(FileNotFoundError):
            with RigLease(timeout=0, paths=[(HOST_FD, self.path), ('SECOND_RIG_FD', missing)]):
                self.fail('second lock should fail to open')
        self.assertEqual(self.probe(), 0)

    def test_shell_exec_preserves_ownership_for_native_profiles(self):
        script = ('from pathlib import Path\nimport sys\n'
                  'from rig_lock import verify_descriptor\n'
                  'verify_descriptor(int(sys.argv[1]), Path(sys.argv[2]))\n')
        with RigLease(timeout=0, paths=[(HOST_FD, self.path)]) as lease:
            result = subprocess.run(['bash', '-c', 'exec "$@"', 'rig-test', sys.executable, '-c', script,
                                     str(lease.pass_fds[0]), str(self.path)], cwd=Path(__file__).parent,
                                    env=lease.child_environment(), pass_fds=lease.pass_fds, check=False)
            self.assertEqual(result.returncode, 0)
            self.assertEqual(self.probe(), 7)

    def test_orphan_child_retains_lease_after_parent_exit(self):
        script = ('from pathlib import Path\n'
                  'import subprocess,sys\n'
                  'from rig_lock import RigLease,HOST_FD\n'
                  'with RigLease(timeout=0,paths=[(HOST_FD,Path(sys.argv[1]))]) as lease:\n'
                  ' child=subprocess.Popen([sys.executable,"-c","import time; time.sleep(30)"],'
                  'stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,pass_fds=lease.pass_fds)\n'
                  ' print(child.pid,flush=True)\n')
        parent = subprocess.run([sys.executable, '-c', script, str(self.path)],
                                cwd=Path(__file__).parent, check=True, capture_output=True, text=True)
        child = int(parent.stdout.strip())
        try:
            self.assertEqual(self.probe(), 7)
        finally:
            os.kill(child, signal.SIGTERM)
        deadline = time.monotonic()+5
        while self.probe() != 0:
            if time.monotonic() > deadline:
                self.fail('orphan exit did not release its lease')
            time.sleep(.05)
        self.assertTrue(self.path.exists())


if __name__ == '__main__':
    unittest.main()
