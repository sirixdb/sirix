"""Behavioral contracts for changed-source selection and honest PIT reporting (no JVM)."""
import importlib.util
import io
import json
from pathlib import Path
import runpy
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'pr_mutation.py'
SPEC = importlib.util.spec_from_file_location('pr_mutation', SCRIPT)
mutation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mutation)


def class_bytes(name, source):
    # Minimal class with a SourceFile attribute; includes double-slot and modern CP tags.
    def utf8(value):
        encoded = value.encode()
        return b'\x01' + struct.pack('>H', len(encoded)) + encoded
    pool = (utf8(name) + b'\x07\x00\x01' + utf8('SourceFile') + utf8(source)
            + b'\x05' + b'\x00' * 8 + b'\x12\x00\x00\x00\x00')
    return (b'\xca\xfe\xba\xbe' + struct.pack('>HHH', 0, 69, 8) + pool
            + struct.pack('>HHHHHHH', 0, 2, 0, 0, 0, 0, 1)
            + struct.pack('>HIH', 3, 2, 4))


class SelectionTest(unittest.TestCase):
    def test_diff_uses_merge_base_and_validates_literal_tree_paths(self):
        source = b'bundles/sirix-core/src/main/java/io/sirix/Foo.java'
        paths = source + b'\x00docs/a b.md\x00'
        base, head = 'a' * 40, 'b' * 40
        with patch.object(mutation.subprocess, 'check_output', side_effect=[paths, source + b'\x00']) as git:
            selected = mutation.changed_sources(Path('/repo'), base, head)
        self.assertEqual(selected, {'sirix-core': ['src/main/java/io/sirix/Foo.java']})
        diff_args = git.call_args_list[0].args[0]
        self.assertIn(base + '...' + head, diff_args)
        self.assertIn('--diff-filter=ACMRT', diff_args)
        self.assertIn('-z', diff_args)
        tree_args = git.call_args_list[1].args[0]
        self.assertIn('--literal-pathspecs', tree_args)
        self.assertEqual(tree_args[-2:], ['--', source.decode()])
        self.assertFalse(git.call_args_list[0].kwargs['shell'])
        self.assertFalse(git.call_args_list[1].kwargs['shell'])

    def test_option_like_and_malformed_revisions_never_reach_git(self):
        root = Path('/repo')
        for hostile in ['--output=owned', '-M', 'HEAD', 'a' * 39, 'a' * 40 + ';id']:
            with self.subTest(hostile=hostile), patch.object(mutation.subprocess, 'check_output') as git:
                with self.assertRaisesRegex(ValueError, 'commit'):
                    mutation.changed_sources(root, hostile, 'b' * 40)
                git.assert_not_called()

    def test_hostile_diff_paths_and_paths_absent_from_tree_are_rejected(self):
        root = Path('/repo')
        for path in ['--output=owned', '/absolute.java', 'bundles/../outside.java',
                     'bundles/sirix-core/src/main/java/-output.java']:
            with self.subTest(path=path), patch.object(mutation.subprocess, 'check_output',
                                                     return_value=path.encode() + b'\x00'):
                with self.assertRaisesRegex(ValueError, 'path'):
                    mutation.changed_sources(root, 'a' * 40, 'b' * 40)
        path = b'bundles/sirix-core/src/main/java/Missing.java\x00'
        with patch.object(mutation.subprocess, 'check_output', side_effect=[path, b'']):
            with self.assertRaisesRegex(ValueError, 'head commit'):
                mutation.changed_sources(root, 'a' * 40, 'b' * 40)

    def test_empty_diff_does_not_query_or_expand_tree(self):
        root = Path('/repo')
        with patch.object(mutation.subprocess, 'check_output', return_value=b'') as git:
            self.assertEqual(mutation.changed_sources(root, 'a' * 40, 'b' * 40), {})
        self.assertEqual(git.call_count, 1)

    def test_real_git_tree_rejects_hostile_tracked_filename(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            def git(*args):
                return subprocess.check_output(['git', '-C', str(root), *args], shell=False).decode().strip()
            git('init', '-q')
            source = root / 'bundles/sirix-core/src/main/java/a/Foo.java'
            source.parent.mkdir(parents=True)
            source.write_text('package a; class Foo {}')
            git('add', '--', '.')
            git('-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
                'commit', '-qm', 'base')
            base = git('rev-parse', 'HEAD')
            source.write_text('package a; class Foo { int x; }')
            git('add', '--', '.')
            git('-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
                'commit', '-qm', 'ordinary change')
            head = git('rev-parse', 'HEAD')
            self.assertEqual(mutation.changed_sources(root, base, head),
                             {'sirix-core': ['src/main/java/a/Foo.java']})
            (source.parent / '-output.java').write_text('hostile filename')
            git('add', '--', '.')
            git('-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
                'commit', '-qm', 'hostile tracked path')
            hostile_head = git('rev-parse', 'HEAD')
            with self.assertRaisesRegex(ValueError, 'path'):
                mutation.changed_sources(root, base, hostile_head)

    def test_filter_main_only_and_map_examples(self):
        paths = ['bundles/sirix-examples/src/main/java/example/App.java',
                 'bundles/sirix-query/src/main/kotlin/io/sirix/Query.kt',
                 'bundles/sirix-core/src/test/java/io/sirix/FooTest.java',
                 'bundles/sirix-core/src/main/java/io/sirix/package-info.java',
                 'bundles/sirix-core/src/main/java/module-info.java', 'README.md']
        self.assertEqual(mutation.select_sources(paths), {
            'sirix-example': ['src/main/java/example/App.java'],
            'sirix-query': ['src/main/kotlin/io/sirix/Query.kt']})
        self.assertEqual(mutation.select_sources([]), {})
        with self.assertRaisesRegex(ValueError, 'Unknown module'):
            mutation.select_sources(['bundles/new-module/src/main/java/X.java'])

    def test_class_metadata_and_invalid_input(self):
        self.assertEqual(mutation.class_source(class_bytes('a/Foo$Inner', 'Foo.java')),
                         ('a.Foo$Inner', 'Foo.java'))
        with self.assertRaises(ValueError):
            mutation.class_source(b'bad')
        truncated = class_bytes('a/Foo', 'Foo.java')[:18]
        with self.assertRaises(ValueError):
            mutation.class_source(truncated)

    def test_exact_source_mapping_includes_nested_other_top_level_and_kotlin(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / 'src/main/java/a/Foo.java'
            source.parent.mkdir(parents=True)
            source.write_text('package a; final class Foo {}')
            kotlin = root / 'src/main/kotlin/a/Tools.kt'
            kotlin.parent.mkdir(parents=True)
            kotlin.write_text('package a\nfun foo() = 1')
            classes = root / 'build/classes/java/main/a'
            classes.mkdir(parents=True)
            for name, file in [('Foo', 'Foo.java'), ('Foo$Inner', 'Foo.java'),
                               ('Other', 'Foo.java'), ('FooExtra', 'FooExtra.java'),
                               ('ToolsKt', 'Tools.kt')]:
                (classes / (name + '.class')).write_bytes(class_bytes('a/' + name, file))
            targets, missing = mutation.resolve_targets(root, [str(source.relative_to(root)),
                                                              str(kotlin.relative_to(root))])
            self.assertEqual(targets, ['a.Foo', 'a.Foo$Inner', 'a.Other', 'a.ToolsKt'])
            self.assertEqual(missing, [])
            self.assertEqual(mutation.resolve_targets(root, ['src/main/java/a/Missing.java'])[1],
                             ['src/main/java/a/Missing.java'])


    def test_focused_tests_and_module_fallback(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / 'src/main/java/a/Foo.java'
            source.parent.mkdir(parents=True)
            source.write_text('package a; class Foo {}')
            test = root / 'src/test/java/a/FooTest.java'
            test.parent.mkdir(parents=True)
            test.touch()
            self.assertEqual(mutation.test_scope(root, ['src/main/java/a/Foo.java']), ['a.FooTest*'])
            test.unlink()
            self.assertEqual(mutation.test_scope(root, ['src/main/java/a/Foo.java']), ['*'])


    def test_reader_skips_member_attributes_and_missing_source_metadata(self):
        payload = struct.pack('>H', 1) + b'\x00' * 6 + struct.pack('>HHI', 1, 1, 3) + b'abc'
        reader = mutation.ClassReader(payload)
        reader.skip_members()
        self.assertEqual(reader.offset, len(payload))
        data = class_bytes('a/Foo', 'Foo.java')
        self.assertEqual(mutation.class_source(data[:-10] + b'\x00\x00'), ('a.Foo', None))
        unknown_attribute = data[:-8] + struct.pack('>HIH', 4, 2, 4)
        self.assertEqual(mutation.class_source(unknown_attribute), ('a.Foo', None))
        reader = mutation.ClassReader(struct.pack('>H', 2) + b'\x02')
        with self.assertRaisesRegex(ValueError, 'constant-pool tag'):
            reader.constant_pool()
        with self.assertRaisesRegex(ValueError, 'magic'):
            mutation.class_source(b'nope')

    def test_source_symlink_cannot_escape_module(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'module'
            root.mkdir()
            outside = Path(temp) / 'outside.java'
            outside.write_text('class Outside {}')
            (root / 'Link.java').symlink_to(outside)
            with self.assertRaisesRegex(ValueError, 'escapes'):
                mutation.source_path(root, 'Link.java')


class ReportingTest(unittest.TestCase):
    def test_multiline_xml_scores_survivors_and_uncovered(self):
        with tempfile.TemporaryDirectory() as temp:
            report = Path(temp) / 'mutations.xml'
            report.write_text('''<mutations>
              <mutation detected="true" status="KILLED"><mutatedClass>a.Foo</mutatedClass></mutation>
              <mutation detected='true' status='TIMED_OUT'><mutatedClass>a.Foo</mutatedClass></mutation>
              <mutation detected='false' status='SURVIVED'>
                <mutatedClass>a.Foo</mutatedClass><mutatedMethod>get</mutatedMethod>
                <lineNumber>42</lineNumber><description>replaced &lt; with &gt;</description>
              </mutation>
              <mutation detected='false' status='NO_COVERAGE'><mutatedClass>a.Bar</mutatedClass></mutation>
            </mutations>''')
            result = mutation.read_report(report)
            self.assertEqual(result['total'], 4)
            self.assertEqual(result['killed'], 1)
            self.assertEqual(result['detected'], 2)
            self.assertEqual(len(result['survivors']), 2)
            self.assertIn('42', result['survivors'][0])
            self.assertFalse(mutation.is_clean([{'state': 'complete', **result}]))
            summary = mutation.format_summary([{'module': 'sirix-core', 'state': 'complete', **result}])
            self.assertIn('25.0%', summary)
            self.assertIn('50.0%', summary)
            self.assertIn('NO_COVERAGE', summary)
            self.assertIn('a.Foo', summary)

    def test_missing_malformed_and_empty_are_never_clean(self):
        with tempfile.TemporaryDirectory() as temp:
            report = Path(temp) / 'mutations.xml'
            for contents in [None, '<broken', '<mutations/>']:
                if contents is not None:
                    report.write_text(contents)
                with self.assertRaises((OSError, ValueError, mutation.ET.ParseError)):
                    mutation.read_report(report)
        self.assertFalse(mutation.is_clean([{'state': 'timed out'}]))
        self.assertFalse(mutation.is_clean([{'state': 'error'}]))
        self.assertTrue(mutation.is_clean([]))
        self.assertTrue(mutation.is_clean([{'state': 'complete', 'total': 1,
                                           'detected': 1, 'survivors': []}]))

    def test_summary_escapes_untrusted_markdown(self):
        text = mutation.format_summary([{'module': '<script>', 'state': 'error | bad',
                                         'error': '<img src=x>'}])
        self.assertNotIn('<script>', text)
        self.assertNotIn('<img ', text)

    def test_run_records_error_and_blocking_switch(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            (output / 'selection.json').write_text(json.dumps({'sirix-core': ['missing.java']}))
            with patch.object(mutation, 'resolve_targets', return_value=([], ['missing.java'])):
                code = mutation.run(Path(temp), output, 10, blocking=False)
                self.assertEqual(code, 0)
                self.assertIn('missing.java', (output / 'summary.md').read_text())
                self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 1)
            (output / 'selection.json').write_text('{}')
            self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 0)
            self.assertIn('No changed', (output / 'summary.md').read_text())

    def test_success_survivors_and_timeout_are_recorded_with_partial_xml(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'proof'
            output.mkdir()
            (output / 'selection.json').write_text(json.dumps({'sirix-core': ['src/main/java/a/Foo.java']}))

            def execute(command, root, log, timeout):
                report = output / 'sirix-core/report/mutations.xml'
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text('<mutations><mutation detected="true" status="KILLED"/></mutations>')
                self.assertGreater(timeout, 0)
                self.assertLessEqual(timeout, 10)
                return 0

            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=['a.FooTest*']), \
                 patch.object(mutation, 'execute', side_effect=execute):
                self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 0)
                result = json.loads((output / 'results.json').read_text())[0]
                self.assertEqual(result['targets'], ['a.Foo'])
                self.assertEqual(result['state'], 'complete')

            def timed_out(command, root, log, timeout):
                execute(command, root, log, timeout)
                raise mutation.subprocess.TimeoutExpired(command, timeout)

            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=['a.FooTest*']), \
                 patch.object(mutation, 'execute', side_effect=timed_out):
                self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 1)
                result = json.loads((output / 'results.json').read_text())[0]
                self.assertEqual(result['state'], 'timed out')
                self.assertEqual(result['killed'], 1)

            with patch.object(mutation, 'resolve_targets', return_value=([], ['missing.java'])):
                self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 1)
                result = json.loads((output / 'results.json').read_text())[0]
                self.assertNotIn('killed', result)  # stale XML was removed before source mapping

    def test_process_runs_literal_arguments_and_captures_log(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            log = root / 'child.log'
            command = [sys.executable, '-c', 'import sys; print(sys.argv[1])', '; echo owned']
            self.assertEqual(mutation.execute(command, root, log, 5), 0)
            self.assertEqual(log.read_text().strip(), '; echo owned')
            self.assertFalse((root / 'owned').exists())

    def test_incomplete_module_errors_never_pass_blocking(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            with patch.object(mutation, 'prepare_analysis', return_value=['unused']), \
                 patch.object(mutation, 'execute', return_value=1):
                result = mutation.analyze_module(output, output, 'sirix-core', [], float('inf'), 10)
            self.assertIn('PIT exited 1', result['error'])
            with patch.object(mutation, 'prepare_analysis', return_value=['unused']), \
                 patch.object(mutation, 'execute') as execute:
                result = mutation.analyze_module(output, output, 'sirix-core', [], 0, 10)
            self.assertEqual(result['state'], 'timed out')
            execute.assert_not_called()
            with patch.object(mutation, 'prepare_analysis', return_value=['unused']), \
                 patch.object(mutation, 'execute', return_value=0):
                result = mutation.analyze_module(output, output, 'sirix-core', [], float('inf'), 10)
            self.assertEqual(result['state'], 'error')  # no XML despite zero process exit
            result = mutation.analyze_module(output, output, '../outside', [], 0, 10)
            self.assertIn('Unknown module', result['error'])
            self.assertFalse((output.parent / 'outside').exists())
            with patch.object(mutation, 'resolve_targets', return_value=([], [])):
                with self.assertRaisesRegex(ValueError, 'Empty target'):
                    mutation.prepare_analysis(output, output, 'sirix-core', [], {})

    def test_summary_limits_display_without_hiding_full_result_count(self):
        result = {'module': 'sirix-core', 'state': 'complete', 'total': 101, 'killed': 0,
                  'detected': 0, 'statuses': {'SURVIVED': 101},
                  'survivors': [f'mutant-{index}' for index in range(101)]}
        text = mutation.format_summary([result])
        self.assertIn('Remaining entries', text)
        self.assertNotIn('- mutant-100', text)
        self.assertEqual(len(result['survivors']), 101)

    def test_process_timeout_terminates_group_and_preserves_log(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            log = root / 'pit.log'
            with patch.object(mutation.subprocess, 'Popen') as process, \
                 patch.object(mutation.os, 'killpg') as kill:
                process.return_value.__enter__.return_value = process.return_value
                process.return_value.wait.side_effect = [mutation.subprocess.TimeoutExpired('pit', 1), -15]
                process.return_value.pid = 123
                with self.assertRaises(mutation.subprocess.TimeoutExpired):
                    mutation.execute(['pit'], root, log, 1)
                kill.assert_called_once_with(123, mutation.signal.SIGKILL)


class CommandLineTest(unittest.TestCase):
    def test_select_cli_writes_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            args = ['pr_mutation.py', 'select', '--base', 'a' * 40, '--head', 'b' * 40,
                    '--output', temp]
            selected = {'sirix-core': ['src/main/java/Foo.java']}
            with patch.object(sys, 'argv', args), patch.object(mutation, 'changed_sources', return_value=selected), \
                 patch.object(sys, 'stdout', new_callable=io.StringIO):
                self.assertEqual(mutation.main(), 0)
            self.assertEqual(json.loads((Path(temp) / 'selection.json').read_text()), selected)

    def test_run_cli_skips_empty_selection_and_module_entrypoint_exits_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            (Path(temp) / 'selection.json').write_text('{}')
            args = ['pr_mutation.py', 'run', '--output', temp, '--blocking']
            with patch.object(sys, 'argv', args):
                self.assertEqual(mutation.main(), 0)
                script = str(SCRIPT)
                with self.assertRaises(SystemExit) as exit_status:
                    runpy.run_path(script, run_name='__main__')
            self.assertEqual(exit_status.exception.code, 0)

    def test_cli_rejects_missing_revisions_and_nonpositive_budget(self):
        with tempfile.TemporaryDirectory() as temp:
            commands = [['select'], ['run', '--seconds', '0']]
            for command in commands:
                args = ['pr_mutation.py', *command, '--output', temp]
                with patch.object(sys, 'argv', args), patch.object(sys, 'stderr', new_callable=io.StringIO), \
                     self.assertRaises(SystemExit) as exit_status:
                    mutation.main()
                self.assertEqual(exit_status.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
