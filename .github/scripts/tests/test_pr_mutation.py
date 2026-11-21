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


# A complete module result whose changed lines were all killed.
CLEAN_CHANGED = {'total': 1, 'killed': 1, 'survived': 0, 'no_coverage': 0, 'detected': 1,
                 'survivors': [], 'failing': []}


def write_manifest(output, modules, lines=None):
    """The manifest `select` writes: what to mutate, plus the lines that decide the outcome."""
    changed_lines = {module: {source: [] for source in sources}
                     for module, sources in modules.items()} if lines is None else lines
    (output / 'selection.json').write_text(
        json.dumps({'modules': modules, 'changed_lines': changed_lines}))


def git_repository(temp):
    """An initialized repository with a deterministic identity, for real diff behaviour."""
    def git(*args):
        return subprocess.check_output(
            ['git', '-C', str(temp), '-c', 'user.name=Test',
             '-c', 'user.email=test@example.invalid', *args], shell=False).decode().strip()
    git('init', '-q')
    return git


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
            package = '.'.join(Path(path).parent.parts)
            declaration = f'package {package};\n' if package else ''
            test.write_text(declaration + f'class {Path(path).stem} {{}}\n')
        yield root


# The reviewed association file that replaced the hard-coded release-contract pairs (CONV-204).
REPOSITORY = SCRIPT.parents[2]
ASSOCIATIONS, ASSOCIATION_ERRORS = mutation.parse_associations(
    (REPOSITORY / mutation.ASSOCIATIONS_FILE).read_text())


class SelectionTest(unittest.TestCase):
    def test_diff_uses_merge_base_and_validates_literal_tree_paths(self):
        source = b'bundles/sirix-core/src/main/java/io/sirix/Foo.java'
        paths = source + b'\x00docs/a b.md\x00'
        base, head = 'a' * 40, 'b' * 40
        patch_text = (b'diff --git a/bundles/sirix-core/src/main/java/io/sirix/Foo.java'
                      b' b/bundles/sirix-core/src/main/java/io/sirix/Foo.java\n'
                      b'--- a/bundles/sirix-core/src/main/java/io/sirix/Foo.java\n'
                      b'+++ b/bundles/sirix-core/src/main/java/io/sirix/Foo.java\n'
                      b'@@ -7 +7,2 @@ class Foo {\n-old\n+new\n+added\n')
        with patch.object(mutation.subprocess, 'check_output',
                          side_effect=[paths, source + b'\x00', patch_text]) as git:
            selected, lines = mutation.changed_sources(Path('/repo'), base, head)
        self.assertEqual(selected, {'sirix-core': ['src/main/java/io/sirix/Foo.java']})
        self.assertEqual(lines, {'sirix-core': {'src/main/java/io/sirix/Foo.java': [7, 8]}})
        line_args = git.call_args_list[2].args[0]
        self.assertIn('--merge-base', line_args)
        self.assertIn(base, line_args)
        self.assertIn('-U0', line_args)
        self.assertEqual(line_args[-2:], ['--', 'bundles/'])
        self.assertFalse(git.call_args_list[2].kwargs['shell'])
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
        with patch.object(mutation.subprocess, 'check_output', side_effect=[path, b'', b'']):
            with self.assertRaisesRegex(ValueError, 'head commit'):
                mutation.changed_sources(root, 'a' * 40, 'b' * 40)

    def test_empty_diff_does_not_query_or_expand_tree(self):
        root = Path('/repo')
        with patch.object(mutation.subprocess, 'check_output', return_value=b'') as git:
            self.assertEqual(mutation.changed_sources(root, 'a' * 40, 'b' * 40), ({}, {}))
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
                             ({'sirix-core': ['src/main/java/a/Foo.java']},
                              {'sirix-core': {'src/main/java/a/Foo.java': [1]}}))
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

    def test_declared_test_identity_reaches_every_resolution_path_without_compilation(self):
        """Java/Kotlin declarations define JVM names; filenames and source folders do not."""
        cases = [('KeyValueLeafPage', 'io.sirix.page', 'KeyValueLeafPageTest', 'named'),
                 ('KeyValueLeafPage', 'elsewhere', 'KeyValueLeafPageSlotTest', 'related'),
                 ('Other', 'io.sirix', 'KeyValueLeafPageTest', 'package')]
        for language, suffix in (('java', 'java'), ('kotlin', 'kt')):
            for stem, package, declared, kind in cases:
                with self.subTest(language=language, kind=kind), \
                     module_tree({f'{stem}.java': package}, []) as root:
                    test = root / f'src/test/{language}/different/KeyValueLeafTest.{suffix}'
                    test.parent.mkdir(parents=True)
                    test.write_text(f'package io.sirix.page;\nclass {declared} {{}}\n')
                    source = f'src/main/java/{stem}.java'
                    patterns, unmapped, kinds = mutation.test_scope(root, [source])
                    self.assertEqual(patterns, [f'io.sirix.page.{declared}*'])
                    self.assertEqual(unmapped, [])
                    self.assertEqual(kinds, {source: kind})
                    result = {}
                    with patch.object(mutation, 'resolve_targets',
                                      return_value=([f'{package}.{stem}'], [])):
                        command = mutation.prepare_analysis(root.parent.parent, root, 'sirix-core',
                                                            [source], result)
                    self.assertIn(f'-PprMutationTests=io.sirix.page.{declared}*', command)

    def test_only_top_level_declarations_select_tests(self):
        """Comments, literals and nested types cannot invent extra top-level JVM names."""
        for language, suffix in (('java', 'java'), ('kotlin', 'kt')):
            with self.subTest(language=language), module_tree({'Foo.java': ''}, []) as root:
                test = root / f'src/test/{language}/NotAClass.{suffix}'
                test.parent.mkdir(parents=True)
                fields = ('String value = ', 'String escaped = ', 'char brace = ', 'String block = ') \
                    if language == 'java' else ('val value = ', 'val escaped = ', 'val brace = ', 'val block = ')
                test.write_text('/* package wrong; class FooCommentTest {} */\n'
                                '// class FooLineTest {}\nclass FooTest {\n'
                                'class FooNestedTest {}\n'
                                + fields[0] + '"} class FooStringTest {";\n'
                                + fields[1] + '"\\\"} class FooEscapedTest {";\n'
                                + fields[2] + "'}';\n"
                                + fields[3] + '"""\n} class FooBlockTest {\n""";\n'
                                '}\nclass FooSlotTest {}\n')
                source = 'src/main/java/Foo.java'
                self.assertEqual(mutation.test_scope(root, [source]),
                                 (['FooTest*'], [], {source: 'named'}))
                source = 'src/main/java/Other.java'
                (root / source).write_text('class Other {}')
                self.assertEqual(mutation.test_scope(root, [source]),
                                 (['FooSlotTest*', 'FooTest*'], [], {source: 'package'}))

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

    def test_release_compiler_entry_points_select_the_end_to_end_refusal_contract(self):
        """PIT's emitted test filter must reach refused and admitted public compiler routes."""
        # Independent oracle: the release regression exercises every public compiler factory,
        # both grouping arities, and warm/reopened stores. Name/package heuristics miss it.
        owners = {'io/sirix/query/SirixCompileChain.java': 'io.sirix.query',
                  'io/sirix/query/compiler/translator/SirixBlockPipelineStrategy.java':
                      'io.sirix.query.compiler.translator',
                  'io/sirix/query/compiler/translator/SirixPipelineStrategy.java':
                      'io.sirix.query.compiler.translator',
                  'io/sirix/query/scan/VectorizedRouteRefusedException.java': 'io.sirix.query.scan'}
        contract = 'io.sirix.query.scan.VectorizedReleaseCorrectnessTest'
        for path, package in owners.items():
            source = 'src/main/java/' + path
            # Crowded source packages cannot fall back to an accidental integration match.
            unrelated = [package.replace('.', '/') + f'/Unrelated{index}Test.java'
                         for index in range(mutation.PACKAGE_TEST_LIMIT + 1)]
            with self.subTest(source=source), module_tree(
                    {path: package}, unrelated + [contract.replace('.', '/') + '.java']) as root:
                result = {}
                with patch.object(mutation, 'resolve_targets', return_value=(['changed.Entry'], [])):
                    command = mutation.prepare_analysis(root.parent.parent, root, 'sirix-core',
                                                        [source], result, ASSOCIATIONS)
                self.assertIn('-PprMutationTests=' + contract + '*', command)
                self.assertEqual(result['unmapped'], [])
                # The same associations must obey the module cap, never silently drop it.
                with patch.object(mutation, 'MODULE_TEST_LIMIT', 0):
                    self.assertEqual(mutation.test_scope(root, [source], ASSOCIATIONS),
                                     ([], [source], {source: 'over-limit'}))

    def test_release_associations_do_not_select_absent_tests_or_similarly_named_sources(self):
        source = 'src/main/java/io/sirix/query/SirixCompileChain.java'
        with module_tree({'io/sirix/query/SirixCompileChain.java': 'io.sirix.query'}, []) as root:
            self.assertEqual(mutation.test_scope(root, [source], ASSOCIATIONS),
                             ([], [source], {source: 'unmapped'}))
        with module_tree({'other/SirixCompileChain.java': 'other'},
                         ['io/sirix/query/scan/VectorizedReleaseCorrectnessTest.java']) as root:
            source = 'src/main/java/other/SirixCompileChain.java'
            self.assertEqual(mutation.test_scope(root, [source], ASSOCIATIONS),
                             ([], [source], {source: 'unmapped'}))

    def test_release_associations_retain_heuristic_matches_and_share_their_cap(self):
        source = 'src/main/java/io/sirix/query/compiler/translator/SirixBlockPipelineStrategy.java'
        owner = 'io/sirix/query/compiler/translator/'
        contract = 'io.sirix.query.scan.VectorizedReleaseCorrectnessTest'
        for simple, kind in [('SirixBlockPipelineStrategyTest', 'named'),
                             ('SirixBlockPipelineStrategyControlTest', 'related'),
                             ('OtherTest', 'package')]:
            with self.subTest(kind=kind), module_tree(
                    {owner + 'SirixBlockPipelineStrategy.java': 'io.sirix.query.compiler.translator'},
                    [owner + simple + '.java', contract.replace('.', '/') + '.java']) as root:
                expected = sorted(['io.sirix.query.compiler.translator.' + simple + '*', contract + '*'])
                self.assertEqual(mutation.test_scope(root, [source], ASSOCIATIONS),
                                 (expected, [], {source: kind}))
                with patch.object(mutation, 'MODULE_TEST_LIMIT', 1):
                    self.assertEqual(mutation.test_scope(root, [source], ASSOCIATIONS),
                                     ([], [source], {source: 'over-limit'}))

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
            result = mutation.read_report(report, {})
            self.assertEqual(result['total'], 4)
            self.assertEqual(result['killed'], 1)
            self.assertEqual(result['detected'], 2)
            self.assertEqual(len(result['survivors']), 2)
            self.assertIn('42', result['survivors'][0])
            # No changed source mapped, so the report judged nothing: never clean.
            self.assertEqual(result['unattributed'], 4)
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
                    mutation.read_report(report, {})
        self.assertFalse(mutation.is_clean([{'state': 'timed out'}]))
        self.assertFalse(mutation.is_clean([{'state': 'error'}]))
        self.assertTrue(mutation.is_clean([]))
        self.assertTrue(mutation.is_clean([{'state': 'complete', 'total': 1, 'detected': 1,
                                           'survivors': [], 'unattributed': 0,
                                           'changed': CLEAN_CHANGED}]))
        # A complete report whose changed-line verdict is missing is never silently clean.
        self.assertFalse(mutation.is_clean([{'state': 'complete', 'total': 1,
                                             'detected': 1, 'survivors': []}]))

    def test_summary_escapes_untrusted_markdown(self):
        text = mutation.format_summary([{'module': '<script>', 'state': 'error | bad',
                                         'error': '<img src=x>'}])
        self.assertNotIn('<script>', text)
        self.assertNotIn('<img ', text)

    def test_run_records_error_and_blocking_switch(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            write_manifest(output, {'sirix-core': ['missing.java']})
            with patch.object(mutation, 'resolve_targets', return_value=([], ['missing.java'])):
                code = mutation.run(Path(temp), output, 10, blocking=False)
                self.assertEqual(code, 0)
                self.assertIn('missing.java', (output / 'summary.md').read_text())
                self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 1)
            write_manifest(output, {})
            self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 0)
            self.assertIn('No changed', (output / 'summary.md').read_text())

    def test_success_survivors_and_timeout_are_recorded_with_partial_xml(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'proof'
            output.mkdir()
            source = Path(temp) / 'bundles/sirix-core/src/main/java/a/Foo.java'
            source.parent.mkdir(parents=True)
            source.write_text('package a;\nclass Foo {}\n')
            write_manifest(output, {'sirix-core': ['src/main/java/a/Foo.java']},
                           {'sirix-core': {'src/main/java/a/Foo.java': [2]}})

            def execute(command, root, log, timeout):
                report = output / 'sirix-core/report/mutations.xml'
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text('<mutations><mutation detected="true" status="KILLED">'
                                  '<sourceFile>Foo.java</sourceFile>'
                                  '<mutatedClass>a.Foo</mutatedClass>'
                                  '<lineNumber>2</lineNumber></mutation></mutations>')
                self.assertGreater(timeout, 0)
                self.assertLessEqual(timeout, 10)
                return 0

            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=execute):
                self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 0)
                result = json.loads((output / 'results.json').read_text())['modules'][0]
                self.assertEqual(result['targets'], ['a.Foo'])
                self.assertEqual(result['state'], 'complete')
                self.assertEqual(result['changed_line_count'], 1)
                self.assertEqual(result['changed']['killed'], 1)

            def timed_out(command, root, log, timeout):
                execute(command, root, log, timeout)
                raise mutation.subprocess.TimeoutExpired(command, timeout)

            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=timed_out):
                self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 1)
                result = json.loads((output / 'results.json').read_text())['modules'][0]
                self.assertEqual(result['state'], 'timed out')
                self.assertEqual(result['killed'], 1)

            with patch.object(mutation, 'resolve_targets', return_value=([], ['missing.java'])):
                self.assertEqual(mutation.run(Path(temp), output, 10, blocking=True), 1)
                result = json.loads((output / 'results.json').read_text())['modules'][0]
                self.assertNotIn('killed', result)  # stale XML was removed before source mapping

    def test_each_module_gets_a_fair_share_so_one_cannot_starve_the_next(self):
        # PR 1351: sirix-core consumed the single shared deadline, so sirix-rest-api reported
        # 'timed out' although its scope was one test class.
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            write_manifest(output, {'sirix-core': ['src/main/java/a/Foo.java'],
                                    'sirix-query': ['src/main/java/a/Bar.java']})
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
            write_manifest(output, {module: ['src/main/java/a/Foo.java'] for module in modules})
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

    def test_only_complete_reports_without_undetected_changed_lines_are_clean(self):
        complete = {'module': 'sirix-core', 'state': 'complete', 'total': 4, 'killed': 2,
                    'survived': 0, 'no_coverage': 0, 'detected': 4, 'statuses': {},
                    'survivors': [], 'changed': {'total': 2, 'killed': 2, 'survived': 0,
                                                 'no_coverage': 0, 'detected': 2, 'survivors': [],
                                                 'failing': []}}
        undetected = [{'mutant': 'SURVIVED | a.Foo | run | 3 | x', 'reason': 'no disposition entry'}]
        self.assertTrue(mutation.is_clean([complete]))
        self.assertTrue(mutation.is_clean([]))  # a docs-only PR skips cleanly
        for broken in ({'changed': {**complete['changed'], 'survived': 1, 'detected': 1,
                                    'failing': undetected}},
                       {'changed': {**complete['changed'], 'no_coverage': 1, 'detected': 1,
                                    'failing': undetected}},
                       {'changed': {**complete['changed'], 'no_coverage': 2, 'detected': 0,
                                    'failing': undetected * 2}},
                       {'state': 'timed out'}, {'state': 'unmapped'}, {'state': 'error'},
                       {'total': 0, 'detected': 0}, {'changed': None},
                       {'unattributed': 4}):  # nothing mapped, so nothing was judged
            with self.subTest(broken=broken):
                self.assertFalse(mutation.is_clean([{**complete, **broken}]))
                self.assertFalse(mutation.is_clean([complete, {**complete, **broken}]))
        # Pre-existing debt on unchanged lines of the same classes never fails the run.
        advisory = {**complete, 'survived': 348, 'no_coverage': 2828, 'total': 3193,
                    'detected': 17, 'survivors': ['SURVIVED | a.Foo | old | 9 | x'],
                    'unchanged': {'total': 3176, 'killed': 0, 'survived': 348,
                                  'no_coverage': 2828, 'detected': 0, 'classes': [['a.Foo', 3176]]}}
        self.assertTrue(mutation.is_clean([advisory]))

    def test_completed_uncovered_report_fails_blocking_but_keeps_advisory_reporting(self):
        """PIT's detected=false contract forbids clean scores for all-uncovered reports."""
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            write_manifest(output, {'sirix-core': ['unused.java']})

            def execute(command, root, log, timeout):
                report = output / 'sirix-core/report/mutations.xml'
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text('<mutations><mutation detected="false" status="NO_COVERAGE">'
                                  '<sourceFile>Unused.java</sourceFile>'
                                  '<mutatedClass>Unused</mutatedClass>'
                                  '<lineNumber>1</lineNumber></mutation></mutations>')
                return 0

            with patch.object(mutation, 'prepare_analysis', return_value=['unused']), \
                 patch.object(mutation, 'execute', side_effect=execute), \
                 patch.object(mutation, 'changed_line_index',
                              return_value={('', 'Unused.java'): {1}}):
                self.assertEqual(mutation.run(output, output, 10, blocking=True), 1)
                self.assertEqual(mutation.run(output, output, 10, blocking=False), 0)
            result = json.loads((output / 'results.json').read_text())['modules'][0]
            self.assertEqual(result['state'], 'complete')
            self.assertEqual((result['total'], result['detected'], result['no_coverage']), (1, 0, 1))
            changed = result['changed']
            self.assertEqual({key: changed[key] for key in
                              ('total', 'killed', 'survived', 'no_coverage', 'detected', 'survivors')},
                             {'total': 1, 'killed': 0, 'survived': 0, 'no_coverage': 1,
                              'detected': 0, 'survivors': ['NO_COVERAGE | Unused | ? | 1 | ?']})
            # An uncovered changed line is failing, never dispositionable (CONV-202).
            self.assertEqual([value['mutant'] for value in changed['failing']],
                             ['NO_COVERAGE | Unused | ? | 1 | ?'])
            self.assertIn('never dispositioned', changed['failing'][0]['reason'])
            self.assertIn('No coverage: 1.', (output / 'summary.md').read_text())

    def test_report_counts_survived_and_uncovered_statuses(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'mutations.xml'
            path.write_text('<mutations>'
                            '<mutation detected="true" status="KILLED"/>'
                            '<mutation detected="false" status="SURVIVED"/>'
                            '<mutation detected="false" status="NO_COVERAGE"/>'
                            '<mutation detected="true" status="TIMED_OUT"/>'
                            '</mutations>')
            report = mutation.read_report(path, {})
        self.assertEqual((report['total'], report['killed'], report['survived'],
                          report['no_coverage'], report['detected']), (4, 1, 1, 1, 2))
        self.assertEqual(len(report['survivors']), 2)

    def test_summary_reports_counts_scope_and_unmapped_sources(self):
        result = {'module': 'sirix-core', 'state': 'complete', 'targets': ['a.Baz', 'a.Foo'],
                  'tests': ['a.FooTest*'], 'unmapped': ['src/main/java/c/Baz.java'],
                  'mutants_per_class': 40, 'changed_line_count': 3,
                  'total': 4, 'killed': 2, 'survived': 1, 'no_coverage': 1, 'detected': 2,
                  'statuses': {'KILLED': 2, 'NO_COVERAGE': 1, 'SURVIVED': 1},
                  'survivors': ['SURVIVED | a.Foo | run | 7 | negated conditional'],
                  'changed': {'total': 2, 'killed': 1, 'survived': 1, 'no_coverage': 0,
                              'detected': 1,
                              'survivors': ['SURVIVED | a.Foo | run | 7 | negated conditional'],
                              'failing': [{'mutant': 'SURVIVED | a.Foo | run | 7 | negated conditional',
                                           'reason': 'no disposition entry'}]},
                  'unchanged': {'total': 2, 'killed': 1, 'survived': 0, 'no_coverage': 1,
                                'detected': 1, 'classes': [['a.Baz', 1]]}}
        text = mutation.format_summary([result])
        self.assertIn('Changed target classes: 2. Test patterns: 1. '
                      'Mutants sampled per class: 40.', text)
        self.assertIn('Changed lines: 3. Mutants on changed lines: 2. Killed: 1. '
                      'Survived: 1. No coverage: 0.', text)
        self.assertIn('Changed sources for which no mapped test was selected:', text)
        self.assertNotIn('their mutants report', text)
        self.assertIn('Survived: 1. No coverage: 1.', text)
        self.assertIn('src/main/java/c/Baz.java', text)
        self.assertIn('negated conditional', text)

    def test_summary_distinguishes_unmapped_modules_from_measured_coverage(self):
        result = {'module': 'sirix-core', 'state': 'unmapped', 'targets': ['a.Foo'],
                  'tests': [], 'unmapped': ['src/main/java/a/Foo.java'],
                  'mutants_per_class': 40,
                  'error': 'No test maps to any changed source; PIT has no baseline to collect'}
        text = mutation.format_summary([result])
        self.assertIn('No mutation analysis ran; no mutant has a status.', text)
        self.assertIn('Changed target classes: 1. Test patterns: 0.', text)
        self.assertIn('Changed sources for which no mapped test was selected:', text)
        self.assertIn('src/main/java/a/Foo.java', text)
        self.assertNotIn('NO_COVERAGE', text)
        self.assertNotIn('No coverage:', text)
        self.assertNotIn('classes mutated:', text)
        self.assertNotIn('Killed:', text)

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
                result = mutation.analyze_module(output, output, 'sirix-core', [], {},
                                                 float('inf'), 10)
            self.assertIn('PIT exited 1', result['error'])
            with patch.object(mutation, 'prepare_analysis', return_value=['unused']), \
                 patch.object(mutation, 'execute') as execute:
                result = mutation.analyze_module(output, output, 'sirix-core', [], {}, 0, 10)
            self.assertEqual(result['state'], 'timed out')
            execute.assert_not_called()
            with patch.object(mutation, 'prepare_analysis', return_value=['unused']), \
                 patch.object(mutation, 'execute', return_value=0):
                result = mutation.analyze_module(output, output, 'sirix-core', [], {},
                                                 float('inf'), 10)
            self.assertEqual(result['state'], 'error')  # no XML despite zero process exit
            result = mutation.analyze_module(output, output, '../outside', [], {}, 0, 10)
            self.assertIn('Unknown module', result['error'])
            self.assertFalse((output.parent / 'outside').exists())
            with patch.object(mutation, 'resolve_targets', return_value=([], [])):
                with self.assertRaisesRegex(ValueError, 'Empty target'):
                    mutation.prepare_analysis(output, output, 'sirix-core', [], {})

    def test_summary_limits_display_without_hiding_full_result_count(self):
        survivors = [f'mutant-{index}' for index in range(101)]
        result = {'module': 'sirix-core', 'state': 'complete', 'total': 101, 'killed': 0,
                  'detected': 0, 'statuses': {'SURVIVED': 101}, 'survivors': survivors,
                  'changed': {'total': 101, 'killed': 0, 'survived': 101, 'no_coverage': 0,
                              'detected': 0, 'survivors': survivors,
                              'failing': [{'mutant': value, 'reason': 'no disposition entry'}
                                          for value in survivors]}}
        text = mutation.format_summary([result])
        self.assertIn('Remaining entries', text)
        self.assertNotIn('- mutant-100', text)
        self.assertEqual(len(result['changed']['survivors']), 101)

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
            lines = {'sirix-core': {'src/main/java/Foo.java': [3, 4]}}
            with patch.object(sys, 'argv', args), \
                 patch.object(mutation, 'changed_sources', return_value=(selected, lines)), \
                 patch.object(mutation, 'merge_base_file', return_value='# base\n') as base, \
                 patch.object(sys, 'stdout', new_callable=io.StringIO):
                self.assertEqual(mutation.main(), 0)
            self.assertEqual(json.loads((Path(temp) / 'selection.json').read_text()),
                             {'modules': selected, 'changed_lines': lines,
                              'dispositions_base': '# base\n'})
            self.assertEqual(base.call_args.args[1:], ('a' * 40, mutation.DISPOSITIONS_FILE))

    def test_run_cli_skips_empty_selection_and_module_entrypoint_exits_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            write_manifest(Path(temp), {})
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


class ChangedLineMapTest(unittest.TestCase):
    """The diff-line map: what the PR itself changed, in the numbering PIT reports."""

    def test_real_diff_maps_multi_hunk_renames_deletions_and_ignores_other_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            git = git_repository(root)
            source = root / 'bundles/sirix-core/src/main/java/a/Renamed.java'
            source.parent.mkdir(parents=True)
            moved = source.parent / 'Moved.java'
            other = root / 'bundles/sirix-core/src/test/java/a/FooTest.java'
            other.parent.mkdir(parents=True)
            (source.parent / 'Old.java').write_text(
                '\n'.join(f'line{number}' for number in range(1, 21)) + '\n')
            (source.parent / 'Deleted.java').write_text('package a;\nclass Deleted {}\n')
            moved.write_text('package a;\nclass Moved {}\n')
            other.write_text('package a;\nclass FooTest {}\n')
            (root / 'docs').mkdir()
            (root / 'docs/note.md').write_text('base\n')
            git('add', '--', '.')
            git('commit', '-qm', 'base')
            base = git('rev-parse', 'HEAD')
            # One rename with three separate edits: a changed line, a deleted line, an append.
            git('mv', '--', 'bundles/sirix-core/src/main/java/a/Old.java', str(source))
            kept = [f'line{number}' for number in range(1, 21)]
            kept[4] = 'CHANGED5'
            del kept[7]  # line8 disappears and contributes no new-side line
            source.write_text('\n'.join(kept + ['added1', 'added2']) + '\n')
            # A pure rename, a test source and a doc: none of them may contribute lines.
            git('mv', '--', 'bundles/sirix-core/src/main/java/a/Moved.java',
                'bundles/sirix-core/src/main/java/a/Relocated.java')
            # A deletion is excluded from the selection and contributes no changed line.
            git('rm', '-q', '--', 'bundles/sirix-core/src/main/java/a/Deleted.java')
            other.write_text('package a;\nclass FooTest { void t() {} }\n')
            (root / 'docs/note.md').write_text('base\nchanged\n')
            git('add', '--', '.')
            git('commit', '-qm', 'rename plus edits')
            head = git('rev-parse', 'HEAD')
            selected, lines = mutation.changed_sources(root, base, head)
        self.assertEqual(selected, {'sirix-core': ['src/main/java/a/Relocated.java',
                                                   'src/main/java/a/Renamed.java']})
        self.assertEqual(lines['sirix-core']['src/main/java/a/Renamed.java'], [5, 20, 21])
        self.assertEqual(lines['sirix-core']['src/main/java/a/Relocated.java'], [])
        self.assertEqual(set(lines['sirix-core']), set(selected['sirix-core']))

    def test_added_file_and_working_tree_edits_are_the_measured_lines(self):
        """The new side is the working tree, the content that was compiled and mutated."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            git = git_repository(root)
            added = root / 'bundles/sirix-core/src/main/java/a/Added.java'
            added.parent.mkdir(parents=True)
            (added.parent / 'Keep.java').write_text('package a;\nclass Keep {}\n')
            git('add', '--', '.')
            git('commit', '-qm', 'base')
            base = git('rev-parse', 'HEAD')
            added.write_text('package a;\nclass Added {\n  int x = 1;\n}\n')
            git('add', '--', '.')
            git('commit', '-qm', 'add a file')
            head = git('rev-parse', 'HEAD')
            # An uncommitted edit of the compiled file still belongs to the judged lines.
            added.write_text('package a;\nclass Added {\n  int x = 1;\n  int y = 2;\n}\n')
            selected, lines = mutation.changed_sources(root, base, head)
        self.assertEqual(selected, {'sirix-core': ['src/main/java/a/Added.java']})
        self.assertEqual(lines['sirix-core']['src/main/java/a/Added.java'], [1, 2, 3, 4, 5])

    def test_only_the_branch_point_separates_the_pr_from_the_base_branch(self):
        """The base side is the merge base, so another branch's lines are never judged."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            git = git_repository(root)
            source = root / 'bundles/sirix-core/src/main/java/a/Shared.java'
            source.parent.mkdir(parents=True)
            body = [f'line{number}' for number in range(1, 11)]
            source.write_text('\n'.join(body) + '\n')
            git('add', '--', '.')
            git('commit', '-qm', 'branch point')
            branch_point = git('rev-parse', 'HEAD')
            base_moved = list(body)
            base_moved[0] = 'BASE_BRANCH_EDIT'
            source.write_text('\n'.join(base_moved) + '\n')
            git('commit', '-qam', 'base branch edits line 1')
            base = git('rev-parse', 'HEAD')
            git('checkout', '-q', '-b', 'pr', branch_point)
            pr_body = list(body)
            pr_body[9] = 'PR_EDIT'
            source.write_text('\n'.join(pr_body) + '\n')
            git('commit', '-qam', 'the PR edits line 10')
            head = git('rev-parse', 'HEAD')
            selected, lines = mutation.changed_sources(root, base, head)
        self.assertEqual(selected, {'sirix-core': ['src/main/java/a/Shared.java']})
        self.assertEqual(lines['sirix-core']['src/main/java/a/Shared.java'], [10])

    def test_hunk_header_forms_and_unattributable_hunks(self):
        prefix = 'bundles/sirix-core/src/main/java/a/'
        patch_text = ('diff --git a/{0}Foo.java b/{0}Foo.java\n'
                      '--- a/{0}Foo.java\n+++ b/{0}Foo.java\n'
                      '@@ -4 +4 @@\n-old\n+new\n'            # single-line form, no count
                      '@@ -9,2 +10,0 @@\n-gone1\n-gone2\n'   # deletion: no new-side line
                      '@@ -30,0 +31,3 @@\n+a\n+++ b/evil\n+c\n'  # added text may look like a header
                      '@@ -59,0 +60 @@\n+still Foo\n'          # ... and must not capture this hunk
                      'diff --git a/{0}Gone.java b/{0}Gone.java\n'  # a deleted file: no new side
                      'deleted file mode 100644\n--- a/{0}Gone.java\n+++ /dev/null\n'
                      '@@ -1,2 +0,0 @@\n-package a;\n-class Gone {{}}\n'
                      'diff --git a/{0}Bar.java b/{0}Bar.java\n'
                      '--- a/{0}Bar.java\n+++ b/{0}Bar.java\n'
                      '@@ -1 +1 @@\n-x\n+y\n').format(prefix)
        sources = {prefix + 'Foo.java': ('sirix-core', 'src/main/java/a/Foo.java')}
        with patch.object(mutation.subprocess, 'check_output', return_value=patch_text.encode()):
            lines = mutation.changed_line_map(Path('/repo'), 'a' * 40, sources)
        # Bar.java is outside the selection, so its hunk is measured for nobody.
        self.assertEqual(lines, {prefix + 'Foo.java': [4, 31, 32, 33, 60]})
        for hostile, refusal in [('@@ -1 +1 @@\n+orphan\n', 'precedes any file header'),
                                 ('diff --git a/x b/x\n--- a/x\n'
                                  '+++ "b/quoted name.java"\n@@ -1 +1 @@\n+q\n', 'quoted'),
                                 ('diff --git a/x b/x\n--- a/x\n+++ b/x\n@@ nonsense @@\n',
                                  'Unparsable diff hunk')]:
            with self.subTest(hostile=hostile), \
                 patch.object(mutation.subprocess, 'check_output', return_value=hostile.encode()):
                with self.assertRaisesRegex(ValueError, refusal):
                    mutation.changed_line_map(Path('/repo'), 'a' * 40, sources)

    def test_index_keys_changed_lines_the_way_pit_reports_a_mutant(self):
        sources = {'a/Foo.java': 'a', 'b/Bar.java': 'b.c', 'Plain.java': None,
                   'c/Commented.java': None}
        with module_tree({path: package or '' for path, package in sources.items()}, []) as root:
            (root / 'src/main/java/Plain.java').write_text('// package commented.out;\nclass Plain {}\n')
            (root / 'src/main/java/c/Commented.java').write_text(
                '/*\npackage wrong.one;\n*/\npackage c.real;\nclass Commented {}\n')
            index = mutation.changed_line_index(root, {
                'src/main/java/a/Foo.java': [7, 8], 'src/main/java/b/Bar.java': [],
                'src/main/java/Plain.java': [2], 'src/main/java/c/Commented.java': [5],
                'src/main/java/gone/Absent.java': [1]})
        self.assertEqual(index, {('a', 'Foo.java'): {7, 8}, ('b.c', 'Bar.java'): set(),
                                 ('', 'Plain.java'): {2}, ('c.real', 'Commented.java'): {5}})
        with module_tree({'a/Foo.java': 'a'}, []) as root:
            for hostile in ([0], ['x'], [-1]):
                with self.subTest(hostile=hostile), self.assertRaises(ValueError):
                    mutation.changed_line_index(root, {'src/main/java/a/Foo.java': hostile})


class ChangedLineVerdictTest(unittest.TestCase):
    """Only the PR's own lines decide the outcome; the rest is advisory debt."""

    # `Pure.java` stands for a pure rename: mapped, but with no changed line of its own.
    INDEX = {('a', 'Foo.java'): {42, 43}, ('a', 'Kot.kt'): {5}, ('a', 'Pure.java'): set()}

    def report(self, temp, mutants):
        path = Path(temp) / 'mutations.xml'
        path.write_text('<mutations>' + ''.join(mutants) + '</mutations>')
        return mutation.read_report(path, self.INDEX)

    @staticmethod
    def mutant(status, source, klass, line, detected=None):
        detected = (status in ('KILLED', 'TIMED_OUT')) if detected is None else detected
        return (f'<mutation detected="{str(detected).lower()}" status="{status}">'
                f'<sourceFile>{source}</sourceFile><mutatedClass>{klass}</mutatedClass>'
                f'<mutatedMethod>run</mutatedMethod><lineNumber>{line}</lineNumber>'
                f'<description>negated conditional</description></mutation>')

    def test_report_splits_changed_from_unchanged_lines_across_class_shapes(self):
        with tempfile.TemporaryDirectory() as temp:
            report = self.report(temp, [
                self.mutant('KILLED', 'Foo.java', 'a.Foo', 42),          # changed line
                self.mutant('KILLED', 'Foo.java', 'a.Foo$Inner', 43),    # nested class, changed
                self.mutant('KILLED', 'Kot.kt', 'a.KotKt', 5),           # Kotlin facade, changed
                self.mutant('SURVIVED', 'Foo.java', 'a.Foo', 41),        # unchanged line
                self.mutant('NO_COVERAGE', 'Foo.java', 'a.Extra', 90),   # extra top-level class
                self.mutant('SURVIVED', 'Pure.java', 'a.Pure', 1),       # mapped, no changed line
                self.mutant('SURVIVED', 'Other.java', 'a.Other', 42)])   # file never mapped
        self.assertEqual(report['changed'], {'total': 3, 'killed': 3, 'survived': 0,
                                             'no_coverage': 0, 'detected': 3, 'survivors': [], 'failing': []})
        # A mutant whose file never mapped is advisory too, and counted as unattributed.
        self.assertEqual(report['unchanged']['total'], 4)
        self.assertEqual(report['unchanged']['survived'], 3)
        self.assertEqual(report['unchanged']['no_coverage'], 1)
        self.assertEqual(sorted(report['unchanged']['classes']),
                         [['a.Extra', 1], ['a.Foo', 1], ['a.Other', 1], ['a.Pure', 1]])
        self.assertEqual(report['unattributed'], 1)
        self.assertEqual((report['total'], report['detected']), (7, 3))
        self.assertEqual(len(report['survivors']), 4)  # the artifact keeps every undetected mutant
        self.assertTrue(mutation.is_clean([{'state': 'complete', **report}]))

    def test_an_undetected_changed_line_mutant_fails_whatever_the_class_total_says(self):
        with tempfile.TemporaryDirectory() as temp:
            for status in ('SURVIVED', 'NO_COVERAGE'):
                with self.subTest(status=status):
                    report = self.report(temp, [
                        self.mutant('KILLED', 'Foo.java', 'a.Foo', 42),
                        self.mutant(status, 'Foo.java', 'a.Foo', 43)]
                        + [self.mutant('KILLED', 'Foo.java', 'a.Foo', 99)] * 50)
                    self.assertEqual(report['changed'][status.lower()], 1)
                    self.assertFalse(mutation.is_clean([{'state': 'complete', **report}]))
                    self.assertEqual(len(report['changed']['survivors']), 1)
                    self.assertIn('43', report['changed']['survivors'][0])

    def test_changed_lines_without_any_coverage_fail_even_with_no_survivor(self):
        with tempfile.TemporaryDirectory() as temp:
            report = self.report(temp, [self.mutant('NO_COVERAGE', 'Foo.java', 'a.Foo', 42),
                                        self.mutant('NO_COVERAGE', 'Foo.java', 'a.Foo', 43),
                                        self.mutant('KILLED', 'Foo.java', 'a.Foo', 1)])
        self.assertEqual((report['changed']['total'], report['changed']['no_coverage'],
                          report['changed']['detected']), (2, 2, 0))
        self.assertFalse(mutation.is_clean([{'state': 'complete', **report}]))

    def test_pre_existing_debt_on_unchanged_lines_never_fails_the_run(self):
        with tempfile.TemporaryDirectory() as temp:
            report = self.report(temp, [self.mutant('KILLED', 'Foo.java', 'a.Foo', 42)]
                                 + [self.mutant('SURVIVED', 'Foo.java', 'a.Foo', 7)] * 348
                                 + [self.mutant('NO_COVERAGE', 'Foo.java', 'a.Foo', 8)] * 2828)
        self.assertEqual(report['changed'], {'total': 1, 'killed': 1, 'survived': 0,
                                            'no_coverage': 0, 'detected': 1, 'survivors': [], 'failing': []})
        self.assertEqual((report['unchanged']['survived'], report['unchanged']['no_coverage']),
                         (348, 2828))
        self.assertTrue(mutation.is_clean([{'state': 'complete', **report}]))

    def test_a_diff_with_no_mutable_changed_line_reports_its_count_without_blocking(self):
        with tempfile.TemporaryDirectory() as temp:
            report = self.report(temp, [self.mutant('SURVIVED', 'Foo.java', 'a.Foo', 7)])
        result = {'module': 'sirix-core', 'state': 'complete', 'changed_line_count': 3,
                  'targets': ['a.Foo'], 'tests': ['a.FooTest*'], 'unmapped': [],
                  'mutants_per_class': 40, **report}
        self.assertTrue(mutation.is_clean([result]))
        text = mutation.format_summary([result])
        self.assertIn('Changed lines: 3. Mutants on changed lines: 0.', text)

    def test_summary_separates_the_verdict_from_advisory_debt(self):
        with tempfile.TemporaryDirectory() as temp:
            report = self.report(temp, [self.mutant('SURVIVED', 'Foo.java', 'a.Foo', 42),
                                        self.mutant('SURVIVED', 'Other.java', 'a.Other', 1)]
                                 + [self.mutant('NO_COVERAGE', 'Foo.java', 'a.Foo', 9)] * 5
                                 + [self.mutant('SURVIVED', 'Kot.kt', 'a.KotKt', 80)] * 2)
        text = mutation.format_summary([{'module': 'sirix-core', 'state': 'complete',
                                         'changed_line_count': 3, 'targets': ['a.Foo'],
                                         'tests': ['a.FooTest*'], 'unmapped': [],
                                         'mutants_per_class': 40, **report}])
        self.assertIn('Changed lines: 3. Mutants on changed lines: 1. Killed: 0. '
                      'Survived: 1. No coverage: 0.', text)
        self.assertIn('Unchanged lines of the same classes (advisory, never blocking). '
                      'Mutants: 8. Killed: 0. Survived: 3. No coverage: 5.', text)
        self.assertIn('- a.Foo: 5', text)
        self.assertIn('- a.KotKt: 2', text)
        self.assertIn('Failing mutants on changed lines', text)
        self.assertIn('Mutants whose source file did not map to a changed source: 1.', text)
        # The blocking list holds the changed-line mutant only, not the advisory ones.
        self.assertEqual(text.count('| a.Other |'), 0)

    def test_advisory_class_listing_is_bounded_and_ordered_by_debt(self):
        with tempfile.TemporaryDirectory() as temp:
            mutants = [self.mutant('SURVIVED', 'Foo.java', f'a.C{index:02d}', 7)
                       for index in range(mutation.ADVISORY_CLASS_LIMIT + 2)]
            mutants.append(self.mutant('SURVIVED', 'Foo.java', 'a.C00', 8))
            report = self.report(temp, mutants)
        classes = report['unchanged']['classes']
        self.assertEqual(len(classes), mutation.ADVISORY_CLASS_LIMIT)
        self.assertEqual(classes[0], ['a.C00', 2])  # the heaviest debt first

    def test_a_mutant_without_a_usable_line_is_advisory_and_lists_no_debt(self):
        """PIT always reports a line; one that is missing or unparsable proves nothing."""
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'mutations.xml'
            path.write_text(
                '<mutations>'
                '<mutation detected="true" status="KILLED"><sourceFile>Foo.java</sourceFile>'
                '<mutatedClass>a.Foo</mutatedClass><lineNumber>42</lineNumber></mutation>'
                '<mutation detected="true" status="KILLED"><sourceFile>Foo.java</sourceFile>'
                '<mutatedClass>a.Foo</mutatedClass></mutation>'
                '<mutation detected="true" status="KILLED"><sourceFile>Foo.java</sourceFile>'
                '<mutatedClass>a.Foo</mutatedClass><lineNumber>4 2</lineNumber></mutation>'
                '</mutations>')
            # Line 1 is a changed line here, so a defaulted line number would be judged.
            report = mutation.read_report(path, {('a', 'Foo.java'): {1, 42}})
        self.assertEqual(report['changed']['total'], 1)
        self.assertEqual((report['unchanged']['total'], report['unchanged']['killed']), (2, 2))
        self.assertEqual(report['unchanged']['classes'], [])  # advisory debt, none undetected
        self.assertEqual(report['unattributed'], 0)
        text = mutation.format_summary([{'module': 'sirix-core', 'state': 'complete', **report}])
        self.assertIn('Mutants: 2. Killed: 2.', text)
        self.assertNotIn('Classes with the most undetected', text)
        self.assertTrue(mutation.is_clean([{'state': 'complete', **report}]))

    def test_run_judges_changed_lines_end_to_end(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output = root / 'out'
            output.mkdir()
            source = root / 'bundles/sirix-core/src/main/java/a/Foo.java'
            source.parent.mkdir(parents=True)
            source.write_text('package a;\nclass Foo {}\n')
            write_manifest(output, {'sirix-core': ['src/main/java/a/Foo.java']},
                           {'sirix-core': {'src/main/java/a/Foo.java': [42]}})
            mutants = [self.mutant('KILLED', 'Foo.java', 'a.Foo', 42),
                       self.mutant('SURVIVED', 'Foo.java', 'a.Foo', 7)]

            def execute(command, module_root, log, timeout):
                report = output / 'sirix-core/report/mutations.xml'
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text('<mutations>' + ''.join(mutants) + '</mutations>')
                return 0

            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=execute):
                self.assertEqual(mutation.run(root, output, 10, blocking=True), 0)
                result = json.loads((output / 'results.json').read_text())['modules'][0]
                self.assertEqual(result['changed_line_count'], 1)
                self.assertEqual(result['changed']['killed'], 1)
                self.assertEqual(result['unchanged']['survived'], 1)
                # The same run blocks as soon as the survivor sits on a changed line.
                mutants[1] = self.mutant('SURVIVED', 'Foo.java', 'a.Foo', 42)
                self.assertEqual(mutation.run(root, output, 10, blocking=True), 1)

    def test_run_refuses_a_manifest_without_a_changed_line_map(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            for manifest in ({'sirix-core': ['src/main/java/a/Foo.java']},
                             {'modules': {'sirix-core': ['src/main/java/a/Foo.java']}},
                             {'modules': {'sirix-core': []}, 'changed_lines': {}},
                             {'modules': {'sirix-core': ['src/main/java/a/Foo.java',
                                                         'src/main/java/a/Bar.java']},
                              'changed_lines': {'sirix-core': {'src/main/java/a/Foo.java': [1]}}},
                             []):
                with self.subTest(manifest=manifest):
                    (output / 'selection.json').write_text(json.dumps(manifest))
                    with self.assertRaises(ValueError):
                        mutation.run(output, output, 10, blocking=True)


if __name__ == '__main__':
    unittest.main()
