"""Behavioral contracts for changed-source selection and honest PIT reporting (no JVM)."""
import importlib.util
import json
from pathlib import Path
import struct
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
    def test_diff_uses_merge_base_and_preserves_nul_paths(self):
        paths = b'bundles/sirix-core/src/main/java/io/sirix/Foo.java\x00docs/a b.md\x00'
        with patch.object(mutation.subprocess, 'check_output', return_value=paths) as git:
            self.assertEqual(mutation.changed_sources(Path('/repo'), 'abc', 'def'),
                             {'sirix-core': ['src/main/java/io/sirix/Foo.java']})
        args = git.call_args.args[0]
        self.assertIn('abc...def', args)
        self.assertIn('--diff-filter=ACMRT', args)
        self.assertIn('-z', args)

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
        with self.assertRaises(ValueError):
            mutation.class_source(class_bytes('a/Foo', 'Foo.java')[:18])

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

    def test_process_timeout_terminates_group_and_preserves_log(self):
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(mutation.subprocess, 'Popen') as process, \
                 patch.object(mutation.os, 'killpg') as kill:
                process.return_value.__enter__.return_value = process.return_value
                process.return_value.wait.side_effect = [mutation.subprocess.TimeoutExpired('pit', 1), -15]
                process.return_value.pid = 123
                with self.assertRaises(mutation.subprocess.TimeoutExpired):
                    mutation.execute(['pit'], Path(temp), Path(temp) / 'pit.log', 1)
                kill.assert_called_once_with(123, mutation.signal.SIGKILL)


if __name__ == '__main__':
    unittest.main()
