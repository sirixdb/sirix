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
from runtime import CLASSIFICATION
from runtime import CLASSIFIED_DATABASE
from runtime import POINTER_FILE
from runtime import RIG_WORK
from runtime import classify_target


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

    def lease_environment(self, decision=None):
        with RigLease(timeout=0, paths=[(HOST_FD, self.path)]) as lease:
            return lease.child_environment(decision)

    def test_a_run_the_parent_placed_as_campaign_never_arrives_at_the_child_as_other(self):
        """The regression. Two sources naming different existing directories is what an operator
        holds after a reload, and the campaign database is the one the *second* source names. A
        launcher that exported a pointer overwrote the winning source with the losing one, and the
        child then classified `other` -- a shared host lease, no envelope check and no legacy-process
        refusal on a leg its own plan.json records as campaign. The conclusion travels instead."""
        work = Path(self.directory.name)/'work'
        work.mkdir()
        previous = Path(self.directory.name)/'clickbench-seg100m-20260905-2328'
        campaign = Path(self.directory.name)/'clickbench-seg100m-20260909-1200'
        (previous/'db').mkdir(parents=True)
        (campaign/'db').mkdir(parents=True)
        (work/POINTER_FILE).write_text(str(previous)+'\n')
        parent = {RIG_WORK: str(work), CAMPAIGN_DIRECTORY: str(campaign)}
        with patch.dict(os.environ, parent, clear=True):
            decision = classify_target(campaign/'db')
            self.assertEqual(decision['classification'], 'campaign')
            child = self.lease_environment(decision)
        self.assertEqual(child[CLASSIFICATION], 'campaign')
        self.assertEqual(child[CLASSIFIED_DATABASE], str(campaign/'db'))
        with patch.dict(os.environ, child, clear=True):
            inherited = classify_target(campaign/'db')
        self.assertEqual(inherited['classification'], 'campaign')
        self.assertEqual(inherited['decided_by'], 'inherited-decision')
        # Only the pointers behind the decision are gone; the parent's own chain is untouched.
        self.assertEqual(child[CAMPAIGN_DIRECTORY], str(campaign))
        # Exporting the rig work directory too would silently opt every child into the legacy
        # leg.lock its parent never took, and rig_lock.py --check would then refuse.
        with patch.dict(os.environ, {key: value for key, value in child.items() if key != RIG_WORK}, clear=True):
            self.assertEqual([key for key, _ in lock_paths()], [HOST_FD])
        with patch.dict(os.environ, child, clear=True):
            self.assertEqual([key for key, _ in lock_paths()], [HOST_FD, LEGACY_FD])

    def test_a_stale_pointer_beside_a_live_variable_still_reaches_the_child_as_campaign(self):
        """The state after moving the database and re-exporting the variable: the pointer file names
        a directory that no longer exists. The parent places the run through the variable, and the
        child must agree."""
        work = Path(self.directory.name)/'rotated-work'
        work.mkdir()
        rotated = Path(self.directory.name)/'seg100m-20260905-2328'
        live = Path(self.directory.name)/'seg100m-20260909-1200'
        (live/'db').mkdir(parents=True)
        self.assertFalse(rotated.exists())
        (work/POINTER_FILE).write_text(str(rotated)+'\n')
        with patch.dict(os.environ, {RIG_WORK: str(work), CAMPAIGN_DIRECTORY: str(live)}, clear=True):
            decision = classify_target(live/'db')
            self.assertEqual(decision['classification'], 'campaign')
            child = self.lease_environment(decision)
        with patch.dict(os.environ, child, clear=True):
            self.assertEqual(classify_target(live/'db')['classification'], 'campaign')

    def test_a_decision_taken_for_another_database_never_reclassifies_this_one(self):
        """A conclusion is honoured only for the database it names. Otherwise a value held over from
        an earlier target, or hand-set, would silently reclassify an unrelated run."""
        campaign = Path(self.directory.name)/'seg100m-20260909-1200'
        (campaign/'db').mkdir(parents=True)
        small = Path(self.directory.name)/'seg1m'
        (small/'db').mkdir(parents=True)
        with patch.dict(os.environ, {CAMPAIGN_DIRECTORY: str(campaign)}, clear=True):
            child = self.lease_environment(classify_target(campaign/'db'))
        with patch.dict(os.environ, child, clear=True):
            unrelated = classify_target(small/'db')
        self.assertEqual(unrelated['classification'], 'other')
        self.assertEqual(unrelated['decided_by'], 'campaign-database')

    def test_a_launcher_that_took_no_decision_leaves_the_child_to_make_the_first_one(self):
        """`rig_lock.py -- <command>` names no database, so it decided nothing. Its child is then the
        first process to decide, and it must see the operator's environment exactly as given."""
        campaign = Path(self.directory.name)/'seg100m-20260909-1200'
        (campaign/'db').mkdir(parents=True)
        with patch.dict(os.environ, {CAMPAIGN_DIRECTORY: str(campaign)}, clear=True):
            child = self.lease_environment()
        self.assertNotIn(CLASSIFICATION, child)
        self.assertNotIn(CLASSIFIED_DATABASE, child)
        self.assertEqual(child[CAMPAIGN_DIRECTORY], str(campaign))
        with patch.dict(os.environ, child, clear=True):
            self.assertEqual(classify_target(campaign/'db')['decided_by'], 'campaign-database')

    def test_a_decision_over_an_unnamed_target_is_not_exported(self):
        """`classify_target(None)` records no target, so nothing can guard the conclusion against
        the wrong database; the child derives instead of trusting an unguardable claim."""
        with patch.dict(os.environ, {}, clear=True):
            child = self.lease_environment(classify_target(None))
        self.assertNotIn(CLASSIFICATION, child)
        self.assertNotIn(CLASSIFIED_DATABASE, child)

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
