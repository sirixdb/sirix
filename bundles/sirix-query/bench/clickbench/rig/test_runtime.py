"""Reject envelope substitution and mutable dependency contamination before querying."""
from pathlib import Path
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from runtime import CANONICAL_ARGS
from runtime import MAIN
from runtime import validate_runtime
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
                    jvm_args=CANONICAL_ARGS+list(extra))

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


if __name__ == '__main__':
    unittest.main()
