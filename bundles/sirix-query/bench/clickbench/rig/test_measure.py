"""Entry-point behaviour: every measuring run holds the host lease, and only the scratch this
comparison created is disposable -- everything else under --out is evidence."""
import argparse
import contextlib
import fcntl
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest
from unittest.mock import Mock
from unittest.mock import patch

from measure import cleanup_commands
from measure import compare
from measure import owned_scratch
from measure import run
from rig_lock import HOST_FD
from rig_lock import RigLease
from runtime import ROOT


def prepared_source(output, arm):
    source = Path(output)/('runtime-'+arm)/'source'
    source.mkdir(parents=True)
    return source.resolve()


class ScratchOwnershipTest(unittest.TestCase):
    def arguments(self, **prepared):
        prepared.setdefault('baseline_runtime', None)
        prepared.setdefault('candidate_runtime', None)
        return argparse.Namespace(**prepared)

    def test_each_checkout_is_reported_as_soon_as_it_exists(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            output = Path(directory).resolve()
            self.assertEqual(owned_scratch(self.arguments(), output), [])
            baseline = prepared_source(output, 'baseline')
            self.assertEqual(owned_scratch(self.arguments(), output), [baseline])
            candidate = prepared_source(output, 'candidate')
            self.assertEqual(owned_scratch(self.arguments(), output), [baseline, candidate])

    def test_a_prepared_manifest_arm_is_never_treated_as_this_run_s_scratch(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            output = Path(directory).resolve()
            prepared_source(output, 'baseline')
            candidate = prepared_source(output, 'candidate')
            arguments = self.arguments(baseline_runtime=str(output/'manifest.json'))
            self.assertEqual(owned_scratch(arguments, output), [candidate])

    def test_the_printed_cleanup_command_survives_a_path_needing_quoting(self):
        awkward = Path('/tmp/rig out/source; rm -rf ~')
        argv = shlex.split(cleanup_commands([awkward]))
        self.assertEqual(argv, ['git', '-C', str(ROOT), 'worktree', 'remove', '--force', str(awkward)])


class ComparisonFailureTest(unittest.TestCase):
    def compare_with_failing_candidate(self, directory):
        """Run compare far enough for baseline preparation to succeed and candidate's to fail."""
        output = Path(directory)/'comparison'
        database = Path(directory)/'db'
        database.mkdir()
        arguments = argparse.Namespace(
            out=str(output), db=str(database), lock_timeout=1, power_uw=50_000_000, cool_below=55.,
            pairs=2, effect_ln=.5, seed=1, baseline='A', candidate='B',
            baseline_runtime=None, candidate_runtime=None, baseline_jvm_arg=[], candidate_jvm_arg=[])

        def prepare(args, arm, out):
            prepared_source(out, arm)
            if arm == 'candidate':
                raise RuntimeError('the candidate revision does not compile')
            return {'java_version': '25', 'jdk_sha256': {}}

        errors = io.StringIO()
        with patch('measure.RigLease'), patch('measure.require_no_benchmark'), \
                patch('measure.runtime_for', side_effect=prepare), \
                contextlib.redirect_stderr(errors):
            with self.assertRaises(RuntimeError):
                compare(arguments)
        return output.resolve(), errors.getvalue()

    def test_a_failed_candidate_build_still_names_both_checkouts_to_reclaim(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            output, errors = self.compare_with_failing_candidate(directory)
            reclaim = [line for line in errors.splitlines() if line.startswith('git ')]
            self.assertEqual([shlex.split(line)[-1] for line in reclaim],
                             [str(output/'runtime-baseline'/'source'), str(output/'runtime-candidate'/'source')])
            self.assertTrue((output/'runtime-baseline'/'source').is_dir())

    def test_a_failed_comparison_never_removes_anything(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            with patch('measure.remove_source_worktree') as removal:
                output, _ = self.compare_with_failing_candidate(directory)
            removal.assert_not_called()
            self.assertTrue((output/'runtime-candidate'/'source').is_dir())


class ScratchReleaseTest(unittest.TestCase):
    """A comparison that runs to completion must give back the two checkouts it created -- the
    directory and the git registration -- while every byte of evidence stays. Two pairs cannot
    resolve the target, so this also pins that an UNRESOLVED verdict still releases: the retention
    rule is that the fixed plan finished, not that the answer was good."""

    def git(self, *argv, cwd):
        return subprocess.run(['git', '-C', str(cwd), *argv], check=True, capture_output=True, text=True).stdout

    def fixture_repository(self, directory):
        repository = Path(directory)/'repo'
        repository.mkdir()
        self.git('init', '-q', cwd=repository)
        self.git('config', 'user.email', 'rig@example.invalid', cwd=repository)
        self.git('config', 'user.name', 'rig', cwd=repository)
        (repository/'tracked.txt').write_text('engine source')
        self.git('add', 'tracked.txt', cwd=repository)
        self.git('commit', '-qm', 'first', cwd=repository)
        return repository

    def test_a_completed_unresolved_comparison_returns_both_checkouts_and_keeps_the_evidence(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            repository = self.fixture_repository(directory)
            database = Path(directory)/'db'
            database.mkdir()
            legs = iter(range(1000))

            def prepare(args, arm, out):
                """Stands in for the Gradle build: a real registered worktree, frozen runtime beside it."""
                prepared = Path(out)/('runtime-'+arm)
                prepared.mkdir(parents=True)
                self.git('worktree', 'add', '--detach', '-q', str(prepared/'source'), cwd=repository)
                (prepared/'source'/'build-output').write_text('gradle leftovers')
                (prepared/'frozen').mkdir()
                (prepared/'frozen'/'runtime.json').write_text('{}')
                return dict(runtime_id=arm, java_version='25', jdk_sha256={})

            def leg(runtime, database_path, leg_output, protocol, lease):
                index = next(legs)
                return dict(result=[[1.5+query/100+index/1000]*3 for query in range(43)],
                            rig=dict(scope='steering', complete=True, protocol=protocol,
                                     run_id=f'run-{index}', runtime_id=runtime['runtime_id']))

            arguments = argparse.Namespace(
                out=str(Path(directory)/'comparison'), db=str(database), lock_timeout=1,
                power_uw=50_000_000, cool_below=55., pairs=2, effect_ln=.5, seed=1,
                baseline='A', candidate='B', baseline_runtime=None, candidate_runtime=None,
                baseline_jvm_arg=[], candidate_jvm_arg=[])
            with patch('measure.ROOT', repository), patch('measure.RigLease'), \
                    patch('measure.require_no_benchmark'), patch('measure.verify_runtime'), \
                    patch('measure.verify_shared_dependencies'), patch('measure.verify_shared_harness'), \
                    patch('measure.runtime_for', side_effect=prepare), patch('measure.run_leg', side_effect=leg), \
                    contextlib.redirect_stdout(io.StringIO()):
                code = compare(arguments)
            output = Path(arguments.out).resolve()

            self.assertEqual(code, 3)
            self.assertEqual(json.loads((output/'report.json').read_text())['resolution'], 'UNRESOLVED')
            registered = self.git('worktree', 'list', '--porcelain', cwd=repository)
            for arm in ('baseline', 'candidate'):
                source = output/('runtime-'+arm)/'source'
                self.assertFalse(source.exists())
                self.assertNotIn(str(source), registered)
                self.assertTrue((output/('runtime-'+arm)/'frozen'/'runtime.json').is_file())
            # Only the fixture's own main checkout is left; a deleted directory with a live
            # registration would still pollute `git worktree list`.
            self.assertEqual(registered.count('worktree '), 1)
            for name in ('plan.json', 'pairs.json', 'pair-0001.json', 'pair-0002.json',
                         'report.json', 'report.md', 'queries.csv'):
                self.assertTrue((output/name).is_file(), name)


class HostLeaseTest(unittest.TestCase):
    """Profiling runs are measurements too. Two lanes measuring at once produced a phantom
    late-query regression that survived merge ablation and vanished under exclusive access, so
    `run --diagnostic` must take the same exclusive host lease a scored leg takes."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.lock = self.root/'host.lock'
        self.lock.touch()
        self.database = self.root/'db'
        self.database.mkdir()

    def arguments(self, name, *, diagnostic):
        return argparse.Namespace(
            out=str(self.root/name), db=str(self.database), lock_timeout=0., power_uw=50_000_000,
            cool_below=55., runtime=None, jvm_args='', diagnostic=diagnostic, tries=2 if diagnostic else 3,
            queries='27' if diagnostic else ','.join(map(str, range(43))),
            diagnostic_arg=['-Dsirix.projDiag=true'] if diagnostic else [], diagnostic_args='')

    def test_neither_a_profiling_run_nor_a_scored_leg_starts_while_another_lane_measures(self):
        with patch('rig_lock.lock_paths', return_value=[(HOST_FD, self.lock)]):
            with RigLease(timeout=0, paths=[(HOST_FD, self.lock)]):
                for name, diagnostic in (('profiling', True), ('scored', False)):
                    with patch('measure.run_part') as part, patch('measure.run_leg') as leg:
                        with self.assertRaises(TimeoutError):
                            run(self.arguments(name, diagnostic=diagnostic))
                        part.assert_not_called()
                        leg.assert_not_called()

    def test_a_profiling_run_owns_the_verified_lease_and_records_what_it_executed(self):
        executed = {}

        def observe(runtime, database, output, queries, protocol, lease, *, diagnostic, tries):
            lease.verify()
            competitor = os.open(self.lock, os.O_RDWR)
            try:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(competitor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(competitor)
            executed.update(queries=queries, tries=tries, diagnostic=diagnostic,
                            jvm_args=list(runtime['jvm_args']))
            return None

        prepared = dict(runtime_id='prepared', jvm_args=['-Xms6g'])
        announced = io.StringIO()
        with patch('rig_lock.lock_paths', return_value=[(HOST_FD, self.lock)]), \
                patch('measure.require_no_benchmark'), \
                patch('measure.prepare_current', return_value=prepared), \
                patch('measure.run_part', side_effect=observe), \
                contextlib.redirect_stdout(announced):
            run(self.arguments('profiling', diagnostic=True))
        self.assertIn('no score or confidence claim', announced.getvalue())
        self.assertEqual(executed, dict(queries=[27], tries=2, diagnostic=True,
                                        jvm_args=['-Xms6g', '-Dsirix.projDiag=true']))
        settings = json.loads((self.root/'profiling'/'plan.json').read_text())['diagnostic_settings']
        self.assertEqual((settings['queries'], settings['tries'], settings['jvm_args']),
                         ([27], 2, ['-Xms6g', '-Dsirix.projDiag=true']))


if __name__ == '__main__':
    unittest.main()
