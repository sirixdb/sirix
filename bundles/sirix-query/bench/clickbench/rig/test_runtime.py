"""Reject envelope substitution and mutable dependency contamination before querying."""
import contextlib
from pathlib import Path
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from runtime import CAMPAIGN_DIRECTORY
from runtime import CAMPAIGN_ENVELOPE
from runtime import CANONICAL_ARGS
from runtime import MAIN
from runtime import POINTER_FILE
from runtime import RIG_WORK
from runtime import classify_target
from runtime import freeze_runtime
from runtime import validate_runtime
from runtime import verify_runtime
from runtime import verify_shared_dependencies
from runtime import verify_shared_harness
from runtime import validate_environment
from runtime import command
from runtime import remove_source_worktree
import subprocess


class RuntimeTest(unittest.TestCase):
    def test_the_benchmark_command_carries_one_try_count_and_the_selected_queries(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            ordinary = command(self.runtime(), directory)
            self.assertEqual(ordinary[-2:], ['--tries', '3'])
            self.assertNotIn('--queries', ordinary)
            selected = command(self.runtime(), directory, queries=[2, 42])
            self.assertEqual(selected[-2:], ['--queries', '2,42'])
            for invalid in ([], [43], [2, 2], [-1]):
                with self.assertRaises(ValueError):
                    command(self.runtime(), directory, queries=invalid)
            with self.assertRaises(ValueError):
                command(self.runtime(), directory, tries=0)

    def test_a_scratch_checkout_is_deleted_and_deregistered(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            repository = Path(directory)/'repo'
            repository.mkdir()

            def git(*argv, cwd=None):
                return subprocess.run(['git', '-C', str(cwd or repository), *argv],
                                      check=True, capture_output=True, text=True).stdout

            git('init', '-q')
            git('config', 'user.email', 'rig@example.invalid')
            git('config', 'user.name', 'rig')
            (repository/'tracked.txt').write_text('content')
            git('add', 'tracked.txt')
            git('commit', '-qm', 'first')
            scratch = Path(directory)/'scratch'
            git('worktree', 'add', '--detach', '-q', str(scratch))
            # A built checkout is never pristine; removal must not depend on it being clean.
            (scratch/'build-output').write_text('gradle leftovers')
            self.assertIn(str(scratch.resolve()), git('worktree', 'list', '--porcelain'))
            remove_source_worktree(repository, scratch)
            self.assertFalse(scratch.exists())
            self.assertNotIn(str(scratch.resolve()), git('worktree', 'list', '--porcelain'))
            self.assertTrue((repository/'tracked.txt').exists())

    def test_removal_refuses_a_directory_the_repository_never_registered(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            repository = Path(directory)/'repo'
            repository.mkdir()
            subprocess.run(['git', '-C', str(repository), 'init', '-q'], check=True, capture_output=True)
            stranger = Path(directory)/'not-a-worktree'
            stranger.mkdir()
            (stranger/'precious').write_text('keep me')
            with self.assertRaises(subprocess.CalledProcessError):
                remove_source_worktree(repository, stranger)
            self.assertTrue((stranger/'precious').exists())

    def test_instrumentation_and_query_catalog_must_match_between_arms(self):
        baseline = dict(measurement_harness_sha256={'main': 'same', 'guard': 'same'},
                        query_catalog_sha256='queries')
        verify_shared_harness(baseline, baseline.copy())
        for key in baseline:
            with self.subTest(key=key), self.assertRaises(ValueError):
                verify_shared_harness(baseline, dict(baseline, **{key: 'different'}))
        with self.assertRaises(ValueError):
            verify_shared_harness({}, {})

    def test_implicit_environment_flags_cannot_bypass_any_launcher(self):
        for name in ('JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS'):
            with patch.dict(os.environ, {name: '-Xmx1g'}, clear=True):
                with self.assertRaisesRegex(ValueError, name):
                    validate_environment()

    def runtime(self, extra=()):
        return dict(main_class=MAIN, java=sys.executable, classpath=['frozen'],
                    jvm_args=CANONICAL_ARGS+list(extra), envelope=CAMPAIGN_ENVELOPE)

    def test_the_benchmark_command_binds_a_smaller_envelope_to_its_own_database(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory, \
                patch.dict(os.environ, {RIG_WORK: directory}):
            os.environ.pop(CAMPAIGN_DIRECTORY, None)
            elsewhere = Path(directory)/'elsewhere'
            elsewhere.mkdir()
            small = dict(self.runtime(), envelope=dict(CAMPAIGN_ENVELOPE, maximum_heap=4 << 30),
                         campaign_classification=dict(target=directory, classification='other'))
            command(small, directory)
            with self.assertRaisesRegex(ValueError, 'must not open'):
                command(small, elsewhere)
            unclassified = {key: value for key, value in small.items() if key != 'campaign_classification'}
            with self.assertRaisesRegex(ValueError, 'must not open'):
                command(unclassified, directory)

    def test_module_options_and_equivalent_heap_sizes(self):
        validate_runtime(self.runtime(['--add-modules', 'jdk.incubator.vector',
                                       '--enable-native-access=ALL-UNNAMED', '-Xmx14336m']))

    def test_final_override_cannot_shrink_envelope(self):
        for override in ('-Xmx10g', '-XX:MaxHeapSize=10g', '-Xms1g',
                         '-Dsirix.offheap.bytes=1', '-XX:+UseJVMCICompiler'):
            with self.subTest(override=override), self.assertRaises(ValueError):
                validate_runtime(self.runtime([override]))

    def test_indirect_arguments_and_entrypoint_replacement_fail(self):
        for args in (['@hidden.args'], ['-XX:Flags=hidden'], ['--add-opens'],
                     ['--add-modules', '-Xmx10g'], ['-cp', 'mutable'],
                     ['--class-path=mutable'], ['-jar', 'loader.jar'], ['SomeOtherMain'],
                     ['-Xbootclasspath/a:mutable.jar'], ['-Djava.system.class.loader=OtherLoader'],
                     ['-XX:CompilerDirectivesFile=mutable.json'], ['-XX:CompileCommandFile=mutable.txt']):
            with self.subTest(args=args), self.assertRaises(ValueError):
                validate_runtime(self.runtime(args))

    def test_profiling_is_explicitly_separate(self):
        for arg in ('-XX:StartFlightRecording=filename=run.jfr', '-Xlog:gc',
                    '-XX:+LogCompilation', '-Dsirix.projDiag=true', '-javaagent:a.jar'):
            with self.subTest(arg=arg):
                with self.assertRaises(ValueError):
                    validate_runtime(self.runtime([arg]))
                validate_runtime(self.runtime([arg]), allow_diagnostics=True)

    def test_shared_snapshot_jar_cannot_change_between_builds(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            root = Path(directory)
            def arm(name, external_hash, internal_hash):
                checkout = root/name
                return dict(source_worktree=str(checkout),
                            source_classpath=[str(root/'maven'/'dependency-SNAPSHOT.jar'), str(checkout/'build/main.jar')],
                            classpath=[name+'-external', name+'-internal'],
                            artifact_sha256={name+'-external': external_hash, name+'-internal': internal_hash})
            baseline = arm('baseline', 'stable', 'old')
            verify_shared_dependencies(baseline, arm('candidate', 'stable', 'new'))
            with self.assertRaisesRegex(ValueError, 'changed in place'):
                verify_shared_dependencies(baseline, arm('candidate', 'mutated', 'new'))
            candidate = arm('candidate', 'stable', 'new')
            del candidate['source_classpath']
            with self.assertRaisesRegex(ValueError, 'provenance'):
                verify_shared_dependencies(baseline, candidate)


# What `EXTRA=...` appends to CANONICAL_ARGS in cold-rounds.sh; the last occurrence wins, exactly
# as HotSpot resolves a repeated -Xmx or -D.
EXTRA_ARGS = ['-Xms1g', '-Xmx4g', '-Dsirix.offheap.bytes=2147483648',
              '-Dsirix.projection.eagerMaterializeBytes=1073741824']
SMALL_ARGS = CANONICAL_ARGS+EXTRA_ARGS
SMALL_ENVELOPE = dict(initial_heap=1 << 30, maximum_heap=4 << 30, arena=2 << 30, eager=1 << 30,
                      jvmci_compiler=False)


class EnvelopeScopeTest(unittest.TestCase):
    """The campaign envelope belongs to the campaign database, not to every runtime the rig freezes.
    A cold gate over a 1M or scratch database used to be unable to start at all on a box without
    twenty spare gibibytes, and no flag could shrink what it reserved."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.campaign = self.root/'campaign'
        (self.campaign/'db').mkdir(parents=True)
        self.scratch = self.root/'scratch'
        self.scratch.mkdir()
        self.work = self.root/'work'
        self.work.mkdir()

    @contextlib.contextmanager
    def rig_environment(self, *, pointer=None, campaign=None):
        """The two places the rig names the campaign database, both empty unless a case sets them."""
        with patch.dict(os.environ, {RIG_WORK: str(self.work)}):
            os.environ.pop(CAMPAIGN_DIRECTORY, None)
            if campaign is not None:
                os.environ[CAMPAIGN_DIRECTORY] = campaign
            target = self.work/POINTER_FILE
            if pointer is None:
                target.unlink(missing_ok=True)
            else:
                target.write_text(pointer+'\n')
            yield

    def pointing_at_campaign(self, value=None):
        return self.rig_environment(pointer=str(self.campaign) if value is None else value)

    def test_the_campaign_database_is_resolved_from_the_pointer_file_alone(self):
        """The cold driver never sources rig.env, so a bare CB100M_DIR read cannot see the pointer
        file load100m.sh writes. Classification must, or a campaign round on a box that only has the
        pointer file silently loses its mandatory envelope."""
        with self.rig_environment(pointer=str(self.campaign)):
            decided = classify_target(self.campaign/'db')
            self.assertEqual(decided['classification'], 'campaign')
            self.assertEqual(decided['campaign_pointer'], str(self.campaign))
            self.assertEqual(decided['pointer_source'], str(self.work/POINTER_FILE))
            self.assertEqual(classify_target(self.scratch)['classification'], 'other')

    def test_a_shell_holding_a_rotated_pointer_cannot_demote_the_campaign_database(self):
        rotated = self.root/'rotated'
        (rotated/'db').mkdir(parents=True)
        with self.rig_environment(pointer=str(self.campaign), campaign=str(rotated)):
            for database in (self.campaign/'db', rotated/'db'):
                with self.subTest(database=database):
                    self.assertEqual(classify_target(database)['classification'], 'campaign')
            self.assertEqual(classify_target(self.scratch)['classification'], 'other')

    def test_an_unclassifiable_named_target_is_refused_rather_than_guessed(self):
        with self.rig_environment():
            with self.assertRaisesRegex(ValueError, 'cannot classify'):
                classify_target(self.campaign/'db')
            with self.assertRaisesRegex(ValueError, 'cannot classify'):
                classify_target(self.scratch)
            # An unnamed preparation stays fail-closed rather than refusing.
            self.assertEqual(classify_target(None)['classification'], 'campaign')

    def test_an_explicit_declaration_opens_a_scratch_gate_but_never_the_campaign_database(self):
        with self.rig_environment():
            declared = classify_target(self.scratch, declared=True)
            self.assertEqual((declared['classification'], declared['decided_by'], declared['campaign_pointer']),
                             ('other', 'operator-declaration', 'unset'))
        with self.rig_environment(pointer=str(self.campaign)):
            with self.assertRaisesRegex(ValueError, 'cannot be declared away'):
                classify_target(self.campaign/'db', declared=True)

    def test_a_symlinked_campaign_database_is_still_the_campaign_database(self):
        alias = self.root/'alias'
        alias.symlink_to(self.campaign/'db')
        with self.pointing_at_campaign():
            self.assertEqual(classify_target(alias)['classification'], 'campaign')

    def jdk(self):
        home = self.root/'jdk'
        if home.is_dir():
            return dict(main_class=MAIN, java=str(home/'bin'/'java'), classpath=[str(self.root/'main.jar')])
        (home/'bin').mkdir(parents=True)
        (home/'lib'/'server').mkdir(parents=True)
        java = home/'bin'/'java'
        java.write_text('#!/bin/sh\necho \'openjdk version "25"\' 1>&2\n')
        java.chmod(0o755)
        (home/'lib'/'modules').write_bytes(b'modules')
        (home/'lib'/'server'/'libjvm.so').write_bytes(b'libjvm')
        (home/'release').write_text('JAVA_VERSION="25"\n')
        artifact = self.root/'main.jar'
        artifact.write_bytes(b'engine')
        return dict(main_class=MAIN, java=str(java), classpath=[str(artifact)])

    def freeze(self, name, flags, classification):
        return freeze_runtime(dict(self.jdk(), jvm_args=list(flags)), self.root/('frozen-'+name),
                              classification)

    def test_a_small_cold_round_freezes_verifies_and_launches_at_its_own_envelope(self):
        """The cold gate over a scratch or 1M database used to be unable to prepare at all: EXTRA is
        append-only, so shrinking the heap was refused as a 100M envelope mismatch."""
        with self.pointing_at_campaign():
            frozen = self.freeze('small', SMALL_ARGS, classify_target(self.scratch))
            self.assertEqual(frozen['envelope'], SMALL_ENVELOPE)
            self.assertEqual(frozen['campaign_classification']['classification'], 'other')
            verify_runtime(frozen)
            argv = command(frozen, self.scratch)
            self.assertEqual(argv[1:1+len(SMALL_ARGS)], SMALL_ARGS)
            self.assertEqual(argv[-3:], [str(self.scratch), '--tries', '3'])

    def test_a_campaign_round_refuses_every_attempt_to_shrink_the_envelope(self):
        with self.pointing_at_campaign():
            decided = classify_target(self.campaign/'db')
            self.assertEqual(decided['classification'], 'campaign')
            with self.assertRaisesRegex(ValueError, 'envelope mismatch'):
                self.freeze('campaign-small', SMALL_ARGS, decided)
            for index, override in enumerate(('-Xmx10g', '-Xms1g', '-Dsirix.offheap.bytes=1',
                                              '-XX:+UseJVMCICompiler')):
                with self.subTest(override=override), self.assertRaisesRegex(ValueError, 'envelope mismatch'):
                    self.freeze(f'campaign-override-{index}', CANONICAL_ARGS+[override], decided)
            frozen = self.freeze('campaign', CANONICAL_ARGS, decided)
            self.assertEqual(frozen['envelope'], CAMPAIGN_ENVELOPE)
            verify_runtime(frozen)

    def test_a_preparation_naming_no_database_freezes_the_campaign_envelope(self):
        with self.rig_environment():
            unnamed = self.freeze('unnamed', CANONICAL_ARGS, None)
            self.assertEqual(unnamed['envelope'], CAMPAIGN_ENVELOPE)
            self.assertEqual(unnamed['campaign_classification']['decided_by'], 'unnamed-target')
            with self.assertRaisesRegex(ValueError, 'envelope mismatch'):
                self.freeze('unnamed-small', SMALL_ARGS, None)

    def test_the_campaign_database_refuses_a_runtime_frozen_at_another_envelope(self):
        with self.pointing_at_campaign():
            small = self.freeze('small', SMALL_ARGS, classify_target(self.scratch))
            with self.assertRaisesRegex(ValueError, 'must not open'):
                command(small, self.campaign/'db')
            campaign = self.freeze('campaign', CANONICAL_ARGS, classify_target(self.campaign/'db'))
            self.assertIn(str((self.campaign/'db').resolve()), command(campaign, self.campaign/'db'))

    def test_a_declared_envelope_is_enforced_for_every_later_round(self):
        """launch-runtime.py re-verifies each round against the envelope the run declared, so a
        manifest whose arguments no longer resolve to it can never launch."""
        with self.pointing_at_campaign():
            frozen = self.freeze('small', SMALL_ARGS, classify_target(self.scratch))
        for drift in ('-Xmx8g', '-Dsirix.offheap.bytes=10737418240'):
            with self.subTest(drift=drift), self.assertRaisesRegex(ValueError, 'envelope mismatch'):
                validate_runtime(dict(frozen, jvm_args=frozen['jvm_args']+[drift]))
        with self.assertRaisesRegex(ValueError, 'must declare the JVM envelope'):
            validate_runtime({key: value for key, value in frozen.items() if key != 'envelope'})


if __name__ == '__main__':
    unittest.main()
