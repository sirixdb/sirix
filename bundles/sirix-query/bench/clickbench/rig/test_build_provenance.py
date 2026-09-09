"""A reused worktree's old bytecode must never become a newly attributed scored leg."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock
from unittest.mock import patch

import measure
from mkleg import submission
from runner import run_leg
from runtime import CANONICAL_ARGS
from runtime import MAIN
from runtime import ROOT
from runtime import finish_build
from runtime import freeze_runtime
from runtime import prepare_current
from runtime import source_identity
from runtime import verify_scored_runtime


def completed_build():
    state = dict(executed=True, did_work=True, skipped=False, skip_message=None, no_source=False)
    return dict(rerun_tasks=True, build_cache=False,
                tasks={name: dict(state) for name in (':sirix-core:compileJava', ':sirix-query:compileJava')})


class BuildProvenanceTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.source = self.root/'source'
        self.source.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.email', 'rig@example.invalid')
        self.git('config', 'user.name', 'rig')
        (self.source/'engine').write_text('current source')
        self.git('add', '.')
        self.git('commit', '-qm', 'engine')
        self.java = self.root/'java'
        self.java.write_text('#!/bin/sh\necho fixture-java\n')
        self.java.chmod(0o755)
        self.classes = self.root/'classes'
        self.classes.mkdir()
        (self.classes/'engine.class').write_bytes(b'compiled fixture')
        jdk = patch('runtime.jdk_hashes', return_value={'fixture': 'fixed'})
        jdk.start()
        self.addCleanup(jdk.stop)
        environment = patch.dict(os.environ, {'CB_RIG_WORK': str(self.root/'work')}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.source, text=True).strip()

    def frozen(self, name='frozen', **overrides):
        source = source_identity(self.source)
        runtime = dict(java=str(self.java), classpath=[str(self.classes)], main_class=MAIN,
                       jvm_args=CANONICAL_ARGS, source_worktree=str(self.source),
                       source_commit=source['head'], build_source=source,
                       build_execution=completed_build())
        runtime.update(overrides)
        return freeze_runtime(runtime, self.root/name)

    def assertLaunchRefused(self, runtime, message):
        with patch('runner.run_part') as launch:
            with self.assertRaisesRegex(ValueError, message):
                run_leg(runtime, self.root/'db', self.root/'leg', {}, Mock())
            launch.assert_not_called()
        self.assertFalse((self.root/'leg/leg.json').exists())

    def test_fresh_frozen_build_of_the_current_source_is_accepted(self):
        verify_scored_runtime(self.frozen())

    def test_missing_manifest_refuses_before_launch(self):
        runtime = self.frozen()
        Path(runtime['manifest_path']).unlink()
        self.assertLaunchRefused(runtime, 'existing frozen runtime manifest')

    def test_legacy_manifest_without_execution_evidence_refuses_before_launch(self):
        runtime = self.frozen(build_execution={})
        self.assertLaunchRefused(runtime, 'forced fresh build')

    def test_up_to_date_and_cached_compiles_refuse_before_launch(self):
        for index, outcome in enumerate(('UP-TO-DATE', 'FROM-CACHE', 'NO-SOURCE')):
            with self.subTest(outcome=outcome):
                build = completed_build()
                build['tasks'][':sirix-query:compileJava'].update(
                    skipped=True, did_work=False, skip_message=outcome, no_source=outcome == 'NO-SOURCE')
                self.assertLaunchRefused(self.frozen(str(index), build_execution=build),
                                         ':sirix-query:compileJava')

    def test_checkout_head_drift_refuses_before_launch(self):
        runtime = self.frozen()
        (self.source/'engine').write_text('next tenant')
        self.git('commit', '-qam', 'next')
        self.assertLaunchRefused(runtime, 'source HEAD mismatch')

    def test_changed_and_untracked_inputs_refuse_before_launch(self):
        runtime = self.frozen()
        (self.source/'engine').write_text('unstaged source')
        self.assertLaunchRefused(runtime, 'source inputs changed')
        self.git('restore', 'engine')
        (self.source/'new-input').write_text('untracked source')
        self.assertLaunchRefused(runtime, 'source inputs changed')

    def test_a_mislabelled_source_commit_refuses_before_launch(self):
        runtime = self.frozen(source_commit='0'*40)
        self.assertLaunchRefused(runtime, 'recorded build HEAD')

    def test_a_fresh_build_with_uncommitted_inputs_is_diagnostic_only(self):
        (self.source/'engine').write_text('not committed')
        self.assertLaunchRefused(self.frozen(), 'do not match checked-out HEAD')

    def test_source_change_during_build_prevents_a_freeze(self):
        before = source_identity(self.source)
        (self.source/'engine').write_text('edited during compile')
        export = self.root/'export.json'
        export.write_text(json.dumps(dict(build_execution=completed_build())))
        with self.assertRaisesRegex(ValueError, 'source changed during runtime build'):
            finish_build(export, self.source, before)

    def test_missing_or_mutated_frozen_bytes_refuse_before_launch(self):
        runtime = self.frozen()
        (Path(runtime['classpath'][0])/'engine.class').write_bytes(b'stale tenant bytecode')
        self.assertLaunchRefused(runtime, 'artifact changed')

    def test_scored_run_and_compare_reject_before_any_benchmark(self):
        invalid = self.frozen(build_execution={})
        database = self.root/'db'
        database.mkdir()
        common = dict(out=str(self.root/'run'), db=str(database), lock_timeout=0,
                      runtime=invalid['manifest_path'], diagnostic=False, tries=3,
                      queries=','.join(map(str, range(43))), diagnostic_arg=[], diagnostic_args='',
                      jvm_args='', declare_envelope=False, effect_ln=1., pairs=2, seed=1,
                      baseline_runtime='provided', candidate_runtime='provided')
        with patch('measure.protocol_for', return_value={}), patch('measure.RigLease'), \
                patch('measure.require_no_benchmark'), patch('measure.run_leg') as launch, \
                patch('measure.runtime_for', return_value=invalid):
            with self.assertRaisesRegex(ValueError, 'forced fresh build'):
                measure.run(argparse.Namespace(**common))
            common['out'] = str(self.root/'compare')
            with self.assertRaisesRegex(ValueError, 'forced fresh build'):
                measure.compare(argparse.Namespace(**common))
            launch.assert_not_called()

    def receipt(self):
        runtime = self.frozen()
        def collect(runtime, database, output, queries, protocol, lease):
            output.mkdir(parents=True)
            timings = {(q, attempt): .5 for q in queries for attempt in (1, 2, 3)}
            (output/'suite.log').write_text('\n'.join(
                f'# q{q} try {attempt}: wall={value:.3f} s'
                for (q, attempt), value in timings.items())+'\n')
            (output/'verdict.json').write_text(json.dumps(dict(exit_code=0, issues=[])))
            return timings, {}
        with patch('runner.run_part', side_effect=collect):
            run_leg(runtime, self.root/'db', self.root/'leg', {}, Mock())
        return self.root/'leg/leg.json'

    def test_completed_receipt_exports_exact_timings_and_null_metadata(self):
        document = submission(self.receipt())
        self.assertEqual(document['result'], [[.5]*3]*43)
        self.assertEqual(document['rig']['scope'], 'steering')
        for name in ('machine', 'load_time', 'data_size', 'date'):
            self.assertIsNone(document[name])
        # Completed evidence remains exportable after the build checkout changes or is released.
        self.git('commit', '--allow-empty', '-qm', 'later')
        self.assertEqual(submission(self.root/'leg/leg.json')['result'], document['result'])

    def test_spliced_logs_wrong_runtime_and_failed_verdicts_cannot_export(self):
        receipt = self.receipt()
        document = json.loads(receipt.read_text())
        document['rig']['runtime_id'] = 'another build'
        receipt.write_text(json.dumps(document))
        with self.assertRaisesRegex(ValueError, 'source/runtime identity'):
            submission(receipt)
        runtime = json.loads(Path(document['rig']['runtime_manifest']).read_text())
        document['rig']['runtime_id'] = runtime['runtime_id']
        receipt.write_text(json.dumps(document))
        verdict = receipt.parent/'suite/verdict.json'
        verdict.write_text(json.dumps(dict(exit_code=1, issues=[])))
        with self.assertRaisesRegex(ValueError, 'collection verdict'):
            submission(receipt)
        verdict.write_text(json.dumps(dict(exit_code=0, issues=[])))
        log = receipt.parent/'suite/suite.log'
        log.write_text(log.read_text().replace('0.500', '0.100'))
        with self.assertRaisesRegex(ValueError, 'suite-log hash mismatch'):
            submission(receipt)


class GradleFreshBuildTest(unittest.TestCase):
    def test_reused_outputs_are_recompiled_and_the_frozen_bytes_match_the_source(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            root = Path(directory).resolve()
            source = root/'source'
            source.mkdir()
            (source/'gradlew').symlink_to(ROOT/'gradlew')
            (source/'.gitignore').write_text('.gradle/\n**/build/\n')
            (source/'settings.gradle').write_text(
                "rootProject.name = 'rig-build-fixture'\ninclude 'sirix-core', 'sirix-query'\n")
            (source/'build.gradle').write_text("""
subprojects { apply plugin: 'java' }
project(':sirix-query') {
    dependencies { implementation project(':sirix-core') }
    tasks.register('clickBench', JavaExec) {
        classpath = sourceSets.main.runtimeClasspath
        mainClass = 'io.sirix.query.bench.clickbench.ClickBenchRunMain'
        jvmArgs(project.property('clickbench.jvmArgs').split(' '))
    }
}
""")
            core = source/'sirix-core/src/main/java/Answer.java'
            query = source/'sirix-query/src/main/java/Probe.java'
            core.parent.mkdir(parents=True)
            query.parent.mkdir(parents=True)
            for module in ('sirix-core', 'sirix-query'):
                resources = source/module/'src/main/resources'
                resources.mkdir(parents=True)
                (resources/'fixture.txt').write_text(module)
            core.write_text('public class Answer { public static String value() { return "FRESH"; } }')
            query.write_text('public class Probe { public static void main(String[] a) {'
                             'System.out.println(Answer.value()); } }')
            def git(*args):
                subprocess.run(['git', *args], cwd=source, check=True, capture_output=True)
            git('init', '-q')
            git('config', 'user.email', 'rig@example.invalid')
            git('config', 'user.name', 'rig')
            git('add', '.')
            git('commit', '-qm', 'fixture')
            with patch('runtime.ROOT', source), patch('runtime.harness_provenance', return_value={}):
                first = prepare_current(root/'first')
                verify_scored_runtime(first)
                # Simulate bytecode left by another worktree tenant. The source and HEAD stay put.
                output = source/'sirix-core/build/classes/java/main/Answer.class'
                original = output.read_bytes()
                self.assertIn(b'FRESH', original)
                output.write_bytes(original.replace(b'FRESH', b'STALE'))
                second = prepare_current(root/'second')
                verify_scored_runtime(second)
            self.assertEqual(first['build_source'], second['build_source'])
            self.assertEqual(output.read_bytes(), original)
            answer = subprocess.check_output(
                [second['java'], '-cp', os.pathsep.join(second['classpath']), 'Probe'], text=True)
            self.assertEqual(answer.strip(), 'FRESH')


if __name__ == '__main__':
    unittest.main()
