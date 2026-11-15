"""Behavioral contracts for changed-source selection and honest PIT reporting (no JVM)."""
import contextlib
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


@contextlib.contextmanager
def module_tree(sources, tests):
    """A temporary `bundles/sirix-core` holding the given main sources and test sources."""
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp) / 'bundles/sirix-core'
        for path, package in sources.items():
            source = root / 'src/main/java' / path
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text(f'package {package};\nclass {Path(path).stem} {{}}\n')
        for path in tests:
            test = root / 'src/test/java' / path
            test.parent.mkdir(parents=True, exist_ok=True)
            test.touch()
        yield root


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


    def test_named_tests_are_preferred_and_never_widen_to_the_module_suite(self):
        with module_tree({'a/Foo.java': 'a'}, ['a/FooTest.java']) as root:
            self.assertEqual(mutation.test_scope(root, ['src/main/java/a/Foo.java']),
                             (['a.FooTest*'], [], {'src/main/java/a/Foo.java': 'named'}))
        # The regressed behavior: an unresolvable source used to return ['*'], sending the whole
        # module suite (3366 classes for sirix-core) into PIT's coverage phase.
        with module_tree({'a/Foo.java': 'a'}, ['b/UnrelatedTest.java']) as root:
            patterns, unmapped, kinds = mutation.test_scope(root, ['src/main/java/a/Foo.java'])
            self.assertEqual(patterns, [])
            self.assertEqual(unmapped, ['src/main/java/a/Foo.java'])
            self.assertEqual(kinds, {'src/main/java/a/Foo.java': 'unmapped'})

    def test_one_unresolvable_source_keeps_the_other_sources_scoped(self):
        # PR 1351's defect: 8 of 12 changed sirix-core sources had no same-package named test,
        # and the first of them discarded the mappings already found for the rest.
        sources = {'a/Foo.java': 'a', 'c/Baz.java': 'c'}
        with module_tree(sources, ['a/FooTest.java']) as root:
            patterns, unmapped, kinds = mutation.test_scope(
                root, ['src/main/java/a/Foo.java', 'src/main/java/c/Baz.java'])
            self.assertEqual(patterns, ['a.FooTest*'])
            self.assertEqual(unmapped, ['src/main/java/c/Baz.java'])
            self.assertEqual(kinds['src/main/java/a/Foo.java'], 'named')

    def test_related_names_match_across_packages_and_respect_word_boundaries(self):
        cases = [('KeyValueLeafPage', ['b/KeyValueLeafPageSlotTest.java'],
                  ['b.KeyValueLeafPageSlotTest*'], 'related'),
                 ('BufferManagerImpl', ['a/BufferManagerTest.java'],
                  ['a.BufferManagerTest*'], 'related'),
                 ('AbstractReader', ['a/ReaderTest.java'], ['a.ReaderTest*'], 'related'),
                 # 'Page' must not drag in 'PagerTest': the remainder is not a new word.
                 ('Page', ['b/PagerTest.java'], [], 'unmapped'),
                 # A bare 'Test.java' in the source's own package is a helper name, so the
                 # package fallback must not treat it as a test.
                 ('Page', ['a/Test.java'], [], 'unmapped'),
                 # A same-prefix source that is not a test must not enter the scope.
                 ('Page', ['b/PageHelper.java'], [], 'unmapped')]
        for stem, tests, expected, kind in cases:
            source = f'src/main/java/a/{stem}.java'
            with self.subTest(stem=stem, tests=tests):
                with module_tree({f'a/{stem}.java': 'a'}, tests) as root:
                    patterns, _, kinds = mutation.test_scope(root, [source])
                self.assertEqual(patterns, expected)
                self.assertEqual(kinds[source], kind)

    def test_small_packages_fall_back_and_large_packages_do_not(self):
        source = 'src/main/java/a/Foo.java'
        within = [f'a/Case{index:02d}Test.java' for index in range(mutation.PACKAGE_TEST_LIMIT)]
        with module_tree({'a/Foo.java': 'a'}, within + ['a/sub/DeepTest.java']) as root:
            patterns, unmapped, kinds = mutation.test_scope(root, [source])
        self.assertEqual(kinds[source], 'unmapped')  # 26 > PACKAGE_TEST_LIMIT
        self.assertEqual((patterns, unmapped), ([], [source]))
        with module_tree({'a/Foo.java': 'a'}, within[:2] + ['a/sub/DeepTest.java']) as root:
            patterns, unmapped, kinds = mutation.test_scope(root, [source])
        self.assertEqual(kinds[source], 'package')  # subpackages count, and are selected
        self.assertEqual(patterns, ['a.Case00Test*', 'a.Case01Test*', 'a.sub.DeepTest*'])
        self.assertEqual(unmapped, [])

    def test_scope_caps_stay_small_enough_to_collect_baseline_coverage(self):
        # PIT executes every targetTests class to measure coverage, so the caps are what keeps
        # the job inside its budget. sirix-core's suite is ~3366 classes; these stay far below.
        self.assertLess(mutation.PACKAGE_TEST_LIMIT, mutation.MODULE_TEST_LIMIT)
        self.assertLessEqual(mutation.MODULE_TEST_LIMIT, 100)

    def test_package_fallback_matches_subpackages_but_not_sibling_prefixes(self):
        source = 'src/main/java/a/Foo.java'
        with module_tree({'a/Foo.java': 'a'}, ['ab/SiblingTest.java']) as root:
            # 'ab' merely starts with 'a'; it is not a subpackage of it.
            self.assertEqual(mutation.test_scope(root, [source])[2][source], 'unmapped')

    def test_a_source_without_a_package_does_not_select_every_test(self):
        source = 'src/main/java/Foo.java'
        with module_tree({'Foo.java': ''}, ['a/OneTest.java', 'b/TwoTest.java']) as root:
            Path(root / 'src/main/java/Foo.java').write_text('class Foo {}\n')
            patterns, unmapped, kinds = mutation.test_scope(root, [source])
        self.assertEqual((patterns, unmapped), ([], [source]))
        self.assertEqual(kinds[source], 'unmapped')

    def test_module_limit_drops_weakly_resolved_sources_before_named_ones(self):
        limit, packages = mutation.PACKAGE_TEST_LIMIT, ('p0', 'p1', 'p2')
        sources = {'a/Foo.java': 'a'}
        tests = ['a/FooTest.java']
        for package in packages:
            sources[f'{package}/Alpha.java'] = package
            tests.extend(f'{package}/Zebra{index:02d}Test.java' for index in range(limit))
        selected = ['src/main/java/a/Foo.java'] + [f'src/main/java/{p}/Alpha.java'
                                                   for p in packages]
        with module_tree(sources, tests) as root:
            patterns, unmapped, kinds = mutation.test_scope(root, selected)
        self.assertLessEqual(len(patterns), mutation.MODULE_TEST_LIMIT)
        self.assertIn('a.FooTest*', patterns)  # an exact match is never dropped for the cap
        self.assertEqual(kinds['src/main/java/a/Foo.java'], 'named')
        self.assertEqual([kinds[source] for source in selected[1:]],
                         ['package', 'package', 'over-limit'])
        self.assertEqual(unmapped, ['src/main/java/p2/Alpha.java'])

    def test_module_limit_admits_exactly_the_cap_and_rejects_one_more(self):
        limit, cap = mutation.PACKAGE_TEST_LIMIT, mutation.MODULE_TEST_LIMIT
        sources, tests = {'a/Foo.java': 'a'}, ['a/FooTest.java']
        # 1 named + 25 + 25 = 51, then 'q' lands exactly on the cap and 'r' exceeds it by one.
        for package, count in (('p0', limit), ('p1', limit), ('q', cap - 1 - 2 * limit), ('r', 1)):
            sources[f'{package}/Alpha.java'] = package
            tests.extend(f'{package}/Zebra{index:02d}Test.java' for index in range(count))
        selected = ['src/main/java/a/Foo.java'] + [f'src/main/java/{package}/Alpha.java'
                                                   for package in ('p0', 'p1', 'q', 'r')]
        with module_tree(sources, tests) as root:
            patterns, unmapped, kinds = mutation.test_scope(root, selected)
        self.assertEqual(len(patterns), cap)
        self.assertEqual([kinds[source] for source in selected[1:]],
                         ['package', 'package', 'package', 'over-limit'])
        self.assertEqual(unmapped, ['src/main/java/r/Alpha.java'])

    def test_mutant_sampling_scales_with_the_diff_and_stays_within_bounds(self):
        budget, low, high = (mutation.MUTANT_BUDGET, mutation.MIN_MUTANTS_PER_CLASS,
                             mutation.MAX_MUTANTS_PER_CLASS)
        # PR 1351's 57 changed sirix-core classes share the budget; one changed class gets all
        # the mutants the cap allows; a very large diff still samples the floor per class.
        self.assertEqual(mutation.mutants_per_class(['c'] * 57), budget // 57)
        self.assertEqual(mutation.mutants_per_class(['c']), high)
        self.assertEqual(mutation.mutants_per_class(['c'] * (budget * 2)), low)
        self.assertEqual(mutation.mutants_per_class([]), high)  # never divides by zero
        for count in (1, 2, 17, 57, 500):
            with self.subTest(count=count):
                sampled = mutation.mutants_per_class(['c'] * count)
                self.assertGreaterEqual(sampled, low)
                self.assertLessEqual(sampled, high)

    def test_unresolved_sources_are_still_mutated_when_any_test_maps(self):
        sources = {'a/Foo.java': 'a', 'c/Baz.java': 'c'}
        selected = ['src/main/java/c/Baz.java', 'src/main/java/a/Foo.java']
        with module_tree(sources, ['a/FooTest.java']) as root:
            result = {}
            with patch.object(mutation, 'resolve_targets', return_value=(['a.Baz', 'a.Foo'], [])):
                command = mutation.prepare_analysis(root.parent.parent, root, 'sirix-core',
                                                    selected, result)
        self.assertEqual(result['targets'], ['a.Baz', 'a.Foo'])  # R4: unmapped stays mutated
        self.assertEqual(result['unmapped'], ['src/main/java/c/Baz.java'])
        self.assertIn('-PprMutationTests=a.FooTest*', command)

    def test_a_module_with_no_mapped_test_reports_unmapped_instead_of_running_pit(self):
        with module_tree({'a/Foo.java': 'a'}, []) as root:
            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'execute') as execute, \
                 self.assertRaisesRegex(mutation.Unmapped, 'no baseline'):
                mutation.prepare_analysis(root.parent.parent, root, 'sirix-core',
                                          ['src/main/java/a/Foo.java'], {})
            execute.assert_not_called()


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
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=execute):
                self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 0)
                result = json.loads((output / 'results.json').read_text())[0]
                self.assertEqual(result['targets'], ['a.Foo'])
                self.assertEqual(result['state'], 'complete')

            def timed_out(command, root, log, timeout):
                execute(command, root, log, timeout)
                raise mutation.subprocess.TimeoutExpired(command, timeout)

            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=timed_out):
                self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 1)
                result = json.loads((output / 'results.json').read_text())[0]
                self.assertEqual(result['state'], 'timed out')
                self.assertEqual(result['killed'], 1)

            with patch.object(mutation, 'resolve_targets', return_value=([], ['missing.java'])):
                self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 1)
                result = json.loads((output / 'results.json').read_text())[0]
                self.assertNotIn('killed', result)  # stale XML was removed before source mapping

    def test_each_module_gets_a_fair_share_so_one_cannot_starve_the_next(self):
        # PR 1351: sirix-core consumed the single shared deadline, so sirix-rest-api reported
        # 'timed out' although its scope was one test class.
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            (output / 'selection.json').write_text(json.dumps(
                {'sirix-core': ['src/main/java/a/Foo.java'],
                 'sirix-query': ['src/main/java/a/Bar.java']}))
            timeouts = []
            with patch.object(mutation, 'prepare_analysis', return_value=['unused']), \
                 patch.object(mutation, 'execute',
                              side_effect=lambda command, root, log, timeout:
                              timeouts.append(timeout) or 0):
                mutation.run(Path(temp), output, 10, blocking=False)
            self.assertEqual(len(timeouts), 2)
            # A 10s budget is smaller than the floor, so it degrades to an equal split.
            self.assertLess(timeouts[0], 6)
            self.assertGreater(timeouts[0], 4)
            # The mocked first module returned at once, so it donates its remainder.
            self.assertGreater(timeouts[1], 9)

    def test_a_real_budget_reserves_a_floor_without_capping_the_first_module(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            modules = ('sirix-core', 'sirix-query', 'sirix-rest-api')
            (output / 'selection.json').write_text(json.dumps(
                {module: ['src/main/java/a/Foo.java'] for module in modules}))
            timeouts = []
            with patch.object(mutation, 'prepare_analysis', return_value=['unused']), \
                 patch.object(mutation, 'execute',
                              side_effect=lambda command, root, log, timeout:
                              timeouts.append(timeout) or 0):
                mutation.run(Path(temp), output, 1320, blocking=False)
            floor = mutation.MIN_MODULE_SECONDS
            # The module with the most changed classes is usually first; an equal split would
            # have capped it at 440s. It may use everything but the other two modules' floor.
            self.assertGreater(timeouts[0], 1320 - 2 * floor - 1)
            self.assertLess(timeouts[0], 1320 - 2 * floor + 1)
            for timeout in timeouts[1:]:
                self.assertGreaterEqual(timeout, floor)

    def test_survivors_fail_but_uncovered_mutants_only_report(self):
        complete = {'module': 'sirix-core', 'state': 'complete', 'total': 4, 'killed': 2,
                    'survived': 0, 'no_coverage': 2, 'detected': 2, 'statuses': {},
                    'survivors': []}
        self.assertTrue(mutation.is_clean([complete]))
        self.assertTrue(mutation.is_clean([]))  # a docs-only PR skips cleanly
        for broken in ({'survived': 1}, {'state': 'timed out'}, {'state': 'unmapped'},
                       {'state': 'error'}, {'total': 0}):
            with self.subTest(broken=broken):
                self.assertFalse(mutation.is_clean([{**complete, **broken}]))

    def test_report_counts_survived_and_uncovered_statuses(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'mutations.xml'
            path.write_text('<mutations>'
                            '<mutation detected="true" status="KILLED"/>'
                            '<mutation detected="false" status="SURVIVED"/>'
                            '<mutation detected="false" status="NO_COVERAGE"/>'
                            '<mutation detected="true" status="TIMED_OUT"/>'
                            '</mutations>')
            report = mutation.read_report(path)
        self.assertEqual((report['total'], report['killed'], report['survived'],
                          report['no_coverage'], report['detected']), (4, 1, 1, 1, 2))
        self.assertEqual(len(report['survivors']), 2)

    def test_summary_reports_counts_scope_and_unmapped_sources(self):
        result = {'module': 'sirix-core', 'state': 'complete', 'targets': ['a.Baz', 'a.Foo'],
                  'tests': ['a.FooTest*'], 'unmapped': ['src/main/java/c/Baz.java'],
                  'mutants_per_class': 40,
                  'total': 4, 'killed': 2, 'survived': 1, 'no_coverage': 1, 'detected': 2,
                  'statuses': {'KILLED': 2, 'NO_COVERAGE': 1, 'SURVIVED': 1},
                  'survivors': ['SURVIVED | a.Foo | run | 7 | negated conditional']}
        text = mutation.format_summary([result])
        self.assertIn('Changed classes mutated: 2. Test patterns: 1. '
                      'Mutants sampled per class: 40.', text)
        self.assertIn('Survived: 1. No coverage: 1.', text)
        self.assertIn('src/main/java/c/Baz.java', text)
        self.assertIn('negated conditional', text)

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
