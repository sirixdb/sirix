"""Behavioral contracts for changed-source selection and honest PIT reporting (no JVM)."""
import contextlib
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
import os
import signal
import time
from pathlib import Path
import runpy
import struct
import subprocess
import sys
import tempfile
import time
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
                # The same associations must obey the source cap, never silently drop it.
                with patch.object(mutation, 'SOURCE_TEST_LIMIT', 0):
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

    def test_release_associations_retain_heuristic_matches_and_share_the_source_cap(self):
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
                with patch.object(mutation, 'SOURCE_TEST_LIMIT', 1):
                    self.assertEqual(mutation.test_scope(root, [source], ASSOCIATIONS),
                                     ([], [source], {source: 'over-limit'}))

    def test_lifecycle_contracts_select_dedicated_owners_instead_of_package_histories(self):
        cases = {
            'io/sirix/access/DatabaseNotifications.java': (
                'io.sirix.access', 'io.sirix.access.DatabaseNotificationLifecycleTest'),
            'io/sirix/access/MutationGate.java': (
                'io.sirix.access', 'io.sirix.access.DatabaseOwnershipTest'),
            'io/sirix/access/trx/node/AbstractNodeTrxImpl.java': (
                'io.sirix.access.trx.node', 'io.sirix.access.trx.node.NodeTransactionAbortLifecycleTest'),
            'io/sirix/query/compiler/translator/DerefDescendantExpr.java': (
                'io.sirix.query.compiler.translator',
                'io.sirix.query.compiler.translator.TraversalCloseOwnershipTest'),
            'io/sirix/query/stream/node/SirixNodeStream.java': (
                'io.sirix.query.stream.node',
                'io.sirix.query.compiler.translator.TraversalCloseOwnershipTest')}
        tests = [name.replace('.', '/') + '.java' for _, name in cases.values()]
        tests += ['io/sirix/access/trx/node/json/JsonIdentityGraphGeneratedTest.java']
        with module_tree({path: package for path, (package, _) in cases.items()}, tests) as root:
            for path, (_, expected) in cases.items():
                source = 'src/main/java/' + path
                patterns, unmapped, kinds = mutation.test_scope(root, [source])
                self.assertIn(expected + '*', patterns)
                self.assertEqual(unmapped, [])
                self.assertEqual(kinds[source], 'related')
                self.assertNotIn('io.sirix.access.trx.node.json.JsonIdentityGraphGeneratedTest*', patterns)

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

    def test_single_invocation_preserves_the_partition_selection_union(self):
        """CONV-205: 3 x 25 package tests remain 75 tests after merging coverage passes."""
        for crowded in (False, True):
            sources = {f'p{i}/Alpha.java': f'p{i}' for i in range(3)}
            tests = [f'p{i}/Zebra{j:02d}Test.java' for i in range(3) for j in range(25)]
            selected = ['src/main/java/' + name for name in sources]
            if crowded:
                sources['crowded/Overflow.java'] = 'crowded'
                tests.extend(f'crowded/OverflowCase{j:02d}Test.java' for j in range(61))
                selected.append('src/main/java/crowded/Overflow.java')
            with self.subTest(crowded=crowded), module_tree(sources, tests) as root:
                together = mutation.test_scope(root, selected)
                partitions = [mutation.test_scope(root, selected[:2]),
                              mutation.test_scope(root, selected[2:])]
            expected = sorted(f'p{i}.Zebra{j:02d}Test*' for i in range(3) for j in range(25))
            self.assertEqual(together[0], expected)
            self.assertEqual(together[0], sorted(set().union(*(set(p[0]) for p in partitions))))
            self.assertEqual(together[1], sorted(set().union(*(set(p[1]) for p in partitions))))
            self.assertEqual(together[2], {source: kind for p in partitions
                                         for source, kind in p[2].items()})
            self.assertEqual(together[1], [selected[-1]] if crowded else [])
            if crowded:
                self.assertEqual(together[2][selected[-1]], 'over-limit')

    def test_source_limit_admits_exactly_the_cap_and_rejects_one_more(self):
        for count, kind in ((60, 'related'), (61, 'over-limit')):
            with self.subTest(count=count), module_tree(
                    {'a/Foo.java': 'a'}, [f'a/FooCase{i}Test.java' for i in range(count)]) as root:
                patterns, unmapped, kinds = mutation.test_scope(root, ['src/main/java/a/Foo.java'])
            self.assertEqual(kinds, {'src/main/java/a/Foo.java': kind})
            self.assertEqual(len(patterns), count if count == 60 else 0)
            self.assertEqual(unmapped, [] if count == 60 else ['src/main/java/a/Foo.java'])

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

    def test_timeout_is_detected_but_not_reported_as_an_assertion_kill(self):
        # CONV-202 retains PIT's detected verdict, including infrastructure detections.
        # It must not relabel these raw statuses as assertion kills or survivors.
        with tempfile.TemporaryDirectory() as temp:
            report = Path(temp) / 'mutations.xml'
            report.write_text('<mutations><mutation detected="true" status="TIMED_OUT">'
                              '<sourceFile>Owner.java</sourceFile><mutatedClass>a.Owner</mutatedClass>'
                              '<mutatedMethod>close</mutatedMethod><lineNumber>7</lineNumber>'
                              '<description>removed producer cancellation</description>'
                              '</mutation></mutations>')
            result = {'module': 'sirix-core', 'state': 'complete',
                      **mutation.read_report(report, {('a', 'Owner.java'): {7}})}
            self.assertEqual(result['changed']['detected'], 1)  # PIT's metric stays available.
            self.assertEqual(result['changed']['killed'], 0)
            self.assertTrue(mutation.is_clean([result]))
            self.assertEqual(result['statuses'], {'TIMED_OUT': 1})
            self.assertEqual(result['changed']['survivors'], [])

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

            with patch.object(mutation, 'resolve_targets', return_value=(['Unused'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['UnusedTest*'], [], {})), \
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
                      'Requested mutant scope: the changed lines, without a per-class limit.', text)
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

    def test_timeout_terminates_a_detached_descendant_before_return(self):
        # Gradle's single-use daemon starts a new session; killing only the launcher group
        # leaves that worker alive. The child is a real executable, not a mocked Popen.
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            child_code = "import os,time; from pathlib import Path; Path('child.pid').write_text(str(os.getpid())); time.sleep(30)"
            parent_code = ("import subprocess,sys,time; "
                           f"subprocess.Popen([sys.executable,'-c',{child_code!r}],start_new_session=True); "
                           "time.sleep(30)")
            try:
                with self.assertRaises(subprocess.TimeoutExpired):
                    mutation.execute([sys.executable, '-c', parent_code], root, root / 'run.log', 1)
                pid = int((root / 'child.pid').read_text())
                # A reparented zombie is no longer executing and owns no resources.
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    state = subprocess.run(
                        ['ps', '-p', str(pid), '-o', 'stat='], capture_output=True, text=True).stdout.strip()
                    if not state or state.startswith('Z'):
                        break
                    time.sleep(0.01)
                self.assertTrue(not state or state.startswith('Z'), 'detached work outlived its timeout boundary')
            finally:
                if (root / 'child.pid').exists():
                    try:
                        os.kill(int((root / 'child.pid').read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_single_use_work_stops_before_artifacts_and_the_next_module(self):
        """CONV-205: model single-use Gradle ancestry with real detached processes, not Gradle."""
        worker_code = ('import os,time; from pathlib import Path; '
                       'Path("worker-observed.pid").write_text(str(os.getpid())); time.sleep(60)')
        daemon_code = ('import os,subprocess,sys,time; from pathlib import Path; '
                       'Path("child-observed.pid").write_text(str(os.getpid())); '
                       f'subprocess.Popen([sys.executable,"-c",{worker_code!r}],start_new_session=True); '
                       'time.sleep(60)')
        report_text = ('<mutations><mutation status="KILLED" detected="true">'
                       '<sourceFile>Foo.java</sourceFile><mutatedClass>a.Foo</mutatedClass>'
                       '<lineNumber>11</lineNumber></mutation></mutations>')
        for phase in ('startup', 'analysis'):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                output = root / 'out'
                output.mkdir()
                wrapper = root / 'gradlew'
                wrapper.write_text(
                    '#!' + sys.executable + '\n'
                    'import subprocess,sys,time\nfrom pathlib import Path\n'
                    'assert "--no-daemon" in sys.argv, "single-use Gradle required"\n'
                    'report = Path(next(a.split("=",1)[1] for a in sys.argv '
                    'if a.startswith("-PprMutationReportDir="))) / "mutations.xml"\n'
                    'if sys.argv[1] == ":sirix-core:pitest":\n'
                    f'    subprocess.Popen([sys.executable,"-c",{daemon_code!r}],start_new_session=True)\n'
                    + ('    report.parent.mkdir(parents=True,exist_ok=True)\n'
                       f'    report.write_text({report_text!r})\n' if phase == 'analysis' else '')
                    + '    time.sleep(60)\n'
                    'else:\n'
                    '    for name in ("child-observed.pid", "worker-observed.pid"):\n'
                    '        pid = Path(name).read_text()\n'
                    '        state = subprocess.run(["ps","-p",pid,"-o","stat="], '
                    'capture_output=True,text=True).stdout.strip()\n'
                    '        assert not state or state.startswith("Z"), "previous work still running"\n'
                    '    Path("next.started").write_text("stopped")\n'
                    '    report.parent.mkdir(parents=True,exist_ok=True)\n'
                    f'    report.write_text({report_text!r})\n')
                wrapper.chmod(0o755)
                attach = mutation.attach_report

                def running(pid):
                    state = subprocess.run(['ps', '-p', str(pid), '-o', 'stat='],
                                           capture_output=True, text=True).stdout.strip()
                    return bool(state and not state.startswith('Z'))

                def attach_after_termination(*args, **kwargs):
                    for name in ('child-observed.pid', 'worker-observed.pid'):
                        self.assertTrue((root / name).exists(), 'single-use work did not start')
                        self.assertFalse(running(int((root / name).read_text())),
                                         'artifact access preceded worker termination')
                    return attach(*args, **kwargs)

                selected = {'sirix-core': ['Foo.java'], 'sirix-query': ['Foo.java']}
                try:
                    with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                         patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                         patch.object(mutation, 'changed_line_index', return_value={}), \
                         patch.object(mutation, 'attach_report', side_effect=attach_after_termination):
                        results = mutation.analyze_modules(root, output, selected,
                                                          {name: {} for name in selected},
                                                          4, None, False)
                    self.assertEqual([r['state'] for r in results], ['timed out', 'complete'])
                    self.assertEqual((root / 'next.started').read_text(), 'stopped')
                    self.assertEqual('total' in results[0], phase == 'analysis')
                finally:
                    for name in ('child-observed.pid', 'worker-observed.pid'):
                        if (root / name).exists():
                            pid = int((root / name).read_text())
                            if running(pid):
                                os.kill(pid, signal.SIGKILL)

    def test_the_child_runs_in_the_repository_in_its_own_session_without_a_shell(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            # The wrapper is relative to the repository, the deadline kills the whole process
            # group, stderr belongs in the one log, and no shell ever sees these arguments.
            reported = root / 'where.log'
            self.assertEqual(mutation.execute(
                [sys.executable, '-c', 'import os, sys; print(os.getcwd(), file=sys.stderr)'],
                root, reported, 5), 0)
            self.assertEqual(reported.read_text().strip(), str(root))
            with patch.object(mutation.subprocess, 'Popen') as popen:
                popen.return_value.__enter__.return_value.wait.return_value = 0
                mutation.execute(['true'], root, root / 'log', 7)
            keywords = popen.call_args.kwargs
            self.assertEqual(keywords['cwd'], root)
            self.assertEqual(keywords['stderr'], mutation.subprocess.STDOUT)
            self.assertIs(keywords['start_new_session'], True)
            self.assertIs(keywords['shell'], False)

    def test_incomplete_module_errors_never_pass_blocking(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            with patch.object(mutation, 'prepare_analysis', return_value=['unused']), \
                 patch.object(mutation, 'execute', return_value=1):
                result = mutation.analyze_module(output, output, 'sirix-core', [], {},
                                                 float('inf'), 10, None, False)
            self.assertIn('PIT exited 1', result['error'])
            with patch.object(mutation, 'prepare_analysis', return_value=['unused']), \
                 patch.object(mutation, 'execute') as execute:
                result = mutation.analyze_module(output, output, 'sirix-core', [], {}, 0, 10,
                                                 None, False)
            self.assertEqual(result['state'], 'timed out')
            execute.assert_not_called()
            with patch.object(mutation, 'prepare_analysis', return_value=['unused']), \
                 patch.object(mutation, 'execute', return_value=0):
                result = mutation.analyze_module(output, output, 'sirix-core', [], {},
                                                 float('inf'), 10, None, False)
            self.assertEqual(result['state'], 'error')  # no XML despite zero process exit
            result = mutation.analyze_module(output, output, '../outside', [], {}, 0, 10, None,
                                             False)
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

    def test_the_share_reaches_the_process_that_must_honour_it(self):
        with tempfile.TemporaryDirectory() as temp:
            log = Path(temp) / 'pitest.log'
            with patch.object(mutation.subprocess, 'Popen') as popen:
                process = popen.return_value.__enter__.return_value
                process.wait.return_value = 0
                mutation.execute(['true'], Path(temp), log, 270)
            process.wait.assert_called_once_with(timeout=270)

    def test_process_timeout_waits_for_launcher_and_preserves_log(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            log = root / 'pit.log'
            command = [sys.executable, '-c',
                       'import os,time; print(os.getpid(),flush=True); '
                       'print("started",flush=True); time.sleep(60)']
            with self.assertRaises(subprocess.TimeoutExpired):
                mutation.execute(command, root, log, 1)
            pid, message = log.read_text().splitlines()
            self.assertEqual(message, 'started')
            remaining = subprocess.run(['ps', '-p', pid, '-o', 'stat='],
                                       capture_output=True, text=True).stdout.strip()
            self.assertEqual(remaining, '', 'launcher was not reaped before execute returned')


class CommandLineTest(unittest.TestCase):
    def test_select_cli_writes_manifest_from_divergent_merge_base(self):
        """CONV-202: selection persists the common ancestor's reviewed dispositions."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            git = git_repository(root)
            disposition = root / '.github/mutation-dispositions.toml'
            disposition.parent.mkdir()
            disposition.write_text('# common ancestor\n')
            git('add', '.')
            git('commit', '-qm', 'common ancestor')
            common = git('rev-parse', 'HEAD')
            git('checkout', '-qb', 'base-tip')
            disposition.write_text('# base branch tip\n')
            git('commit', '-qam', 'base advances')
            base_tip = git('rev-parse', 'HEAD')
            git('checkout', '-qb', 'pr', common)
            disposition.write_text('# PR head\n')
            git('commit', '-qam', 'PR changes dispositions')
            head = git('rev-parse', 'HEAD')
            output = root / 'out'
            args = ['pr_mutation.py', 'select', '--root', str(root), '--base', base_tip,
                    '--head', head, '--output', str(output)]
            with patch.object(sys, 'argv', args), \
                 patch.object(sys, 'stdout', new_callable=io.StringIO):
                self.assertEqual(mutation.main(), 0)
            self.assertEqual(json.loads((output / 'selection.json').read_text()),
                             {'modules': {}, 'changed_lines': {},
                              'dispositions_base': '# common ancestor\n'})

    def test_run_cli_skips_empty_selection_and_module_entrypoint_exits_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            write_manifest(Path(temp), {})
            args = ['pr_mutation.py', 'run', '--root', temp, '--output', temp, '--blocking']
            with patch.object(sys, 'argv', args):
                self.assertEqual(mutation.main(), 0)
                script = str(SCRIPT)
                with self.assertRaises(SystemExit) as exit_status:
                    runpy.run_path(script, run_name='__main__')
            self.assertEqual(exit_status.exception.code, 0)

    def test_the_scheduled_command_documents_its_own_defaults(self):
        parsed = mutation.build_parser().parse_args(['full-scope', '--output', '/tmp'])
        self.assertEqual((parsed.seconds, parsed.window_days), (5400, mutation.WINDOW_DAYS))
        self.assertEqual(mutation.WINDOW_DAYS, 7)

    def test_cli_rejects_missing_revisions_and_nonpositive_budget(self):
        with tempfile.TemporaryDirectory() as temp:
            commands = [['select'], ['run', '--seconds', '0'],
                        ['full-scope', '--seconds', '0'],
                        ['full-scope', '--window-days', '0'],
                        ['full-scope', '--window-days', '-7']]
            for command in commands:
                args = ['pr_mutation.py', *command, '--output', temp]
                with patch.object(sys, 'argv', args), patch.object(sys, 'stderr', new_callable=io.StringIO), \
                     self.assertRaises(SystemExit) as exit_status:
                    mutation.main()
                self.assertEqual(exit_status.exception.code, 2)


class LaneCommandTest(unittest.TestCase):
    """CONV-205: what the lane asks PIT for - the changed lines, unsampled, once per module."""

    SOURCES = ['src/main/java/a/Foo.java']
    INDEX = {('a', 'Foo.java'): {24, 11}, ('', 'Root.java'): {7}}

    @contextlib.contextmanager
    def prepared(self, restrict=True, index=None, targets=None):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            directory = root / 'out/sirix-core'
            directory.mkdir(parents=True)
            result = {}
            resolved = ['a.Foo'] if targets is None else targets
            with patch.object(mutation, 'resolve_targets', return_value=(resolved, [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})):
                command = mutation.prepare_analysis(
                    root, directory, 'sirix-core', self.SOURCES, result, None,
                    self.INDEX if index is None else index, restrict)
            yield command, result, directory

    def argument(self, command, prefix):
        found = [value for value in command if value.startswith(prefix)]
        self.assertEqual(len(found), 1, f'{prefix} in {command}')
        return found[0].split('=', 1)[1]

    def test_pit_targets_and_report_location_follow_the_artifact_contract(self):
        """The documented targets.txt is PIT's exact newline-delimited selector input."""
        for restrict in (True, False):
            with self.subTest(restrict=restrict), self.prepared(
                    restrict=restrict, targets=['a.Foo', 'a.Bar']) as (command, _, directory):
                self.assertEqual(self.argument(command, '-PprMutationTargetsFile='),
                                 str(directory / 'targets.txt'))
                self.assertEqual((directory / 'targets.txt').read_bytes(), b'a.Foo\na.Bar\n')
                self.assertEqual(self.argument(command, '-PprMutationReportDir='),
                                 str(directory / 'report'))

    def test_the_command_restricts_to_the_changed_lines_it_wrote(self):
        with self.prepared() as (command, result, directory):
            self.assertEqual(self.argument(command, '-PprMutationChangedLinesFile='),
                             str(directory / mutation.CHANGED_LINES_FILE))
            # The interceptor writes the census beside the map, which is where the reporter
            # reads it; PIT 1.25.5 cannot carry a second feature parameter.
            self.assertEqual((directory / mutation.CHANGED_LINES_FILE).parent,
                             (directory / mutation.CENSUS_FILE).parent)
            self.assertTrue(result['restricted'])
            self.assertEqual((directory / mutation.CHANGED_LINES_FILE).read_text(),
                             f'{mutation.CHANGED_LINES_HEADER}\n\tRoot.java\t7\n'
                             'a\tFoo.java\t11 24\n')

    def test_the_result_records_the_targets_and_the_tests_it_resolved(self):
        with self.prepared() as (_, result, _):
            self.assertEqual((result['targets'], result['tests']), (['a.Foo'], ['a.FooTest*']))
        text = mutation.format_summary([{'module': 'sirix-core', 'state': 'complete',
                                         'targets': ['a.Foo'], 'tests': ['a.FooTest*'],
                                         'mutants_per_class': None}])
        self.assertIn('Changed target classes: 1. Test patterns: 1.', text)

    def test_the_command_runs_the_repository_wrapper_for_the_module(self):
        with self.prepared() as (command, result, _):
            self.assertTrue(command[0].endswith('/gradlew'), command[0])
            self.assertEqual(command[1], ':sirix-core:pitest')
            self.assertEqual(result['resolution'], {})

    def test_no_per_class_mutant_limit_is_ever_requested(self):
        with self.prepared() as (command, result, _):
            self.assertEqual([value for value in command
                              if value.startswith('-PprMutationMaxMutations')], [])
            self.assertIsNone(result['mutants_per_class'])

    def test_pr_and_full_scope_invocations_use_single_use_daemons_with_bounded_heaps(self):
        for restrict in (True, False):
            with self.subTest(restrict=restrict), self.prepared(restrict=restrict) as (command, _, _):
                self.assertIn('--no-daemon', command)
                self.assertEqual(self.argument(command, '-Dorg.gradle.jvmargs='), '-Xmx3g')

    def test_an_unrestricted_run_passes_no_changed_line_map(self):
        with self.prepared(restrict=False) as (command, result, directory):
            self.assertEqual([value for value in command if 'ChangedLines' in value], [])
            self.assertFalse(result['restricted'])
            self.assertFalse((directory / mutation.CHANGED_LINES_FILE).exists())

    def test_a_source_without_a_changed_line_still_appears_in_the_map(self):
        # A renamed-only file contributes no line; the map must still list it, so the interceptor
        # distinguishes "no changed line" from "a file I was told nothing about".
        with self.prepared(index={('a', 'Foo.java'): set()}) as (_, _, directory):
            self.assertEqual((directory / mutation.CHANGED_LINES_FILE).read_text(),
                             f'{mutation.CHANGED_LINES_HEADER}\na\tFoo.java\t\n')

    def test_a_module_with_no_budget_left_reports_it_instead_of_starting_pit(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'out'
            output.mkdir()
            source = Path(temp) / 'bundles/sirix-core/src/main/java/a/Foo.java'
            source.parent.mkdir(parents=True)
            source.write_text('package a;\nclass Foo {}\n')
            write_manifest(output, {'sirix-core': ['src/main/java/a/Foo.java']},
                           {'sirix-core': {'src/main/java/a/Foo.java': [2]}})
            invocations = []

            def execute(command, root, log, timeout):
                invocations.append(timeout)
                return 0
            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=execute):
                # An earlier module consumed the budget, so this one's deadline is already past.
                result = mutation.analyze_module(
                    Path(temp), output, 'sirix-core', ['src/main/java/a/Foo.java'],
                    {'src/main/java/a/Foo.java': [2]}, time.monotonic() - 1, 270, None, True)
            self.assertEqual(result['state'], 'timed out')
            self.assertEqual(result['error'], '270s module execution share exceeded')
            self.assertEqual(invocations, [], 'PIT must not be started without a share')

    def test_a_restricted_command_without_a_changed_line_map_is_refused(self):
        # CONV-205: restricting to an empty map would mutate nothing and report success.
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            directory = root / 'out'
            directory.mkdir()
            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 self.assertRaisesRegex(ValueError, 'refusing to restrict'):
                mutation.prepare_analysis(root, directory, 'sirix-core', self.SOURCES, {}, None,
                                          {}, True)

    def test_every_pending_module_keeps_its_floor_not_only_the_next_one(self):
        # With two modules a wrong "modules still pending" count reads the same as the right one;
        # with three it does not, and the last module must still get its floor.
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'out'
            output.mkdir()
            modules = {'sirix-core': ['src/main/java/a/Foo.java'],
                       'sirix-query': ['src/main/java/a/Bar.java'],
                       'sirix-rest-api': ['src/main/java/a/Baz.java']}
            for module, sources in modules.items():
                for source in sources:
                    path = Path(temp) / 'bundles' / module / source
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(f'package a;\nclass {path.stem} {{}}\n')
            write_manifest(output, modules,
                           {module: {source: [2] for source in sources}
                            for module, sources in modules.items()})
            shares = []

            def execute(command, root, log, timeout):
                shares.append(round(timeout))
                return 1
            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=execute):
                mutation.run(Path(temp), output, 1000, blocking=True)
            floor = mutation.MIN_MODULE_SECONDS
            # The mock returns at once, so the deadline alone decides: each module reserves a
            # floor for every module still pending after it, and the last one keeps the rest.
            self.assertEqual(shares, [1000 - 2 * floor, 1000 - floor, 1000])

    def test_coverage_is_collected_once_per_module_because_pit_runs_once_per_module(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'out'
            output.mkdir()
            modules = {'sirix-core': ['src/main/java/a/Foo.java'],
                       'sirix-query': ['src/main/java/a/Bar.java']}
            for module, sources in modules.items():
                for source in sources:
                    path = Path(temp) / 'bundles' / module / source
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(f'package a;\nclass {path.stem} {{}}\n')
            write_manifest(output, modules,
                           {module: {source: [2] for source in sources}
                            for module, sources in modules.items()})
            invocations = []

            def execute(command, root, log, timeout):
                invocations.append(command[1])
                return 1
            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=execute):
                mutation.run(Path(temp), output, 10, blocking=True)
            self.assertEqual(invocations, [':sirix-core:pitest', ':sirix-query:pitest'])


class FullScopeTest(unittest.TestCase):
    """CONV-206: the scheduled debt report - every line, no verdict, no outcome."""

    SOURCE = 'src/main/java/a/Foo.java'

    def commit(self, root, name, days_ago, content=None, minutes=0):
        stamp = (datetime.now(timezone.utc) - timedelta(days=days_ago, minutes=-minutes)
                 ).strftime('%Y-%m-%dT%H:%M:%S+0000')
        (root / name).write_text(name if content is None else content)
        environment = {**os.environ, 'GIT_COMMITTER_DATE': stamp, 'GIT_AUTHOR_DATE': stamp,
                       'GIT_COMMITTER_NAME': 'Test', 'GIT_AUTHOR_NAME': 'Test',
                       'GIT_COMMITTER_EMAIL': 'test@example.invalid',
                       'GIT_AUTHOR_EMAIL': 'test@example.invalid'}
        subprocess.check_call(['git', '-C', str(root), 'add', name], env=environment)
        subprocess.check_call(['git', '-C', str(root), 'commit', '-qm', name], env=environment)
        return subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'],
                                       env=environment).decode().strip()

    def test_git_interfaces_keep_shell_metacharacters_literal(self):
        """The lane's literal-argument contract applies to every read-only Git caller."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'repo; touch OWNED; #'
            root.mkdir()
            git_repository(root)
            source = 'bundles/sirix-core/src/main/java/a/Foo.java'
            (root / source).parent.mkdir(parents=True)
            old = self.commit(root, source, 30, 'package a;\nclass Foo {}\n')
            note = 'note; touch OWNED; #'
            base = self.commit(root, note, 20, 'literal file contents\n')
            head = self.commit(root, source, 1, 'package a;\nclass Foo { int x; }\n')
            self.assertEqual(mutation.current_commit(root), head)
            self.assertEqual(mutation.window_base(root, 7), base)
            self.assertEqual(mutation.merge_base_file(root, base, note), 'literal file contents\n')
            self.assertIsNone(mutation.merge_base_file(root, old, note))
            selected, lines = mutation.changed_sources(root, base, head)
            self.assertEqual(selected, {'sirix-core': [self.SOURCE]})
            self.assertEqual(lines, {'sirix-core': {self.SOURCE: [2]}})
            self.assertFalse((root / 'OWNED').exists())
            self.assertFalse((Path(temp) / 'OWNED').exists())

    def test_the_window_base_is_the_last_commit_before_the_window(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            git_repository(root)
            old = self.commit(root, 'old', 30)
            recent = self.commit(root, 'recent', 10)
            self.commit(root, 'head', 0)
            self.assertEqual(mutation.window_base(root, 7), recent)
            self.assertEqual(mutation.window_base(root, 20), old)
            with self.assertRaises(ValueError):
                mutation.window_base(root, 0)
            with self.assertRaises(ValueError):
                mutation.window_base(root, 10_000)  # nothing on the branch is that old

    def test_the_window_excludes_topic_parents_from_its_base(self):
        """CONV-206: merging an older topic during the window must expose its sources."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            git = git_repository(root)
            self.commit(root, 'initial', 30)
            main = git('branch', '--show-current')
            git('checkout', '-qb', 'topic')
            source = 'bundles/sirix-core/' + self.SOURCE
            (root / source).parent.mkdir(parents=True)
            topic = self.commit(root, source, 10, 'package a;\nclass Foo {}\n')
            git('checkout', '-q', main)
            parent = self.commit(root, 'main-change', 20)
            git('merge', '-q', '--no-ff', '--no-edit', 'topic')
            base = mutation.window_base(root, 7)
            self.assertEqual(base, parent)
            self.assertNotEqual(base, topic)
            selected, lines = mutation.changed_sources(root, base, mutation.current_commit(root))
            self.assertEqual(selected, {'sirix-core': [self.SOURCE]})
            self.assertEqual(lines, {'sirix-core': {self.SOURCE: [1, 2]}})

    @contextlib.contextmanager
    def report(self, mutants, exit_code=0, dispositions=None):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output = root / 'out'
            output.mkdir()
            (root / '.github').mkdir()
            if dispositions is not None:
                (root / mutation.DISPOSITIONS_FILE).write_text(dispositions)
            source = root / 'bundles/sirix-core' / self.SOURCE
            source.parent.mkdir(parents=True)
            source.write_text('package a;\nclass Foo {}\n')
            commands = []

            def execute(command, root_path, log, timeout):
                commands.append(command)
                target = output / 'sirix-core/report/mutations.xml'
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text('<mutations>' + ''.join(mutants) + '</mutations>')
                return exit_code
            with patch.object(mutation, 'window_base', return_value='a' * 40), \
                 patch.object(mutation, 'current_commit', return_value='b' * 40), \
                 patch.object(mutation, 'changed_sources',
                              return_value=({'sirix-core': [self.SOURCE]},
                                            {'sirix-core': {self.SOURCE: [11]}})), \
                 patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=execute):
                code = mutation.full_scope(root, output, 30, 7)
            yield (code, commands, json.loads((output / 'results.json').read_text()),
                   (output / 'summary.md').read_text())

    def mutant(self, status, line, klass='a.Foo', method='run', indexes=('7',)):
        index_text = ''.join(f'<index>{index}</index>' for index in indexes)
        detected = 'true' if status == 'KILLED' else 'false'
        return (f"<mutation detected='{detected}' status='{status}' numberOfTestsRun='1'>"
                f'<sourceFile>Foo.java</sourceFile><mutatedClass>{klass}</mutatedClass>'
                f'<mutatedMethod>{method}</mutatedMethod><methodDescription>()V'
                f'</methodDescription><lineNumber>{line}</lineNumber><mutator>m</mutator>'
                f'<indexes>{index_text}</indexes><description>d</description></mutation>')

    def test_every_line_is_mutated_with_no_restriction_no_limit_and_no_verdict(self):
        with self.report([self.mutant('KILLED', 11), self.mutant('SURVIVED', 21)]) as (
                code, commands, results, summary):
            self.assertEqual(code, 0)
            self.assertEqual([value for value in commands[0] if 'ChangedLines' in value
                              or 'Census' in value or 'MaxMutations' in value], [])
            self.assertNotIn('dispositions', results)
            module = results['modules'][0]
            self.assertEqual((module['state'], module['total']), ('complete', 2))
            self.assertEqual(module['unchanged']['survived'], 1)
            self.assertEqual(results['window']['days'], 7)
            self.assertEqual((results['window']['base'], results['window']['head']),
                             ('a' * 40, 'b' * 40))
            self.assertIn('never decides an outcome', summary)
            self.assertIn('last 7 day(s)', summary)

    def test_changed_line_survivors_are_in_the_scheduled_class_debt(self):
        """CONV-206 includes all undetected mutants, including recent changed lines."""
        mutants = [self.mutant('SURVIVED', 11), self.mutant('NO_COVERAGE', 21),
                   self.mutant('KILLED', 11, klass='a.Killed')]
        with self.report(mutants) as (code, _, results, summary):
            module = results['modules'][0]
            self.assertEqual(code, 0)
            self.assertIn('- a.Foo: 2', summary)
            self.assertEqual(module['classes'], [['a.Foo', 2]])
            self.assertEqual(module['unchanged']['classes'], [['a.Foo', 1]])
            self.assertEqual(module['changed']['survived'], 1)
        with self.report([self.mutant('SURVIVED', 11)]) as (_, _, results, summary):
            self.assertEqual(results['modules'][0]['classes'], [['a.Foo', 1]])
            self.assertIn('- a.Foo: 1', summary)

    def test_complete_scheduled_reports_are_never_labelled_incomplete(self):
        """CONV-206 labels the module by its actual completion state."""
        with self.report([self.mutant('KILLED', 11)]) as (_, _, results, summary):
            self.assertEqual(results['modules'][0]['state'], 'complete')
            self.assertIn('## sirix-core: complete', summary)
            self.assertNotIn('Incomplete:', summary)

    def test_git_receives_the_exact_utc_cutoff(self):
        """CONV-206's seven-day window uses Git's ISO-8601 UTC date contract."""
        instant = datetime(2026, 11, 23, 12, 34, 56, tzinfo=timezone.utc)
        with patch.object(mutation, 'datetime') as clock, patch.object(
                mutation.subprocess, 'check_output', return_value=b'a' * 40) as git:
            clock.now.return_value = instant
            self.assertEqual(mutation.window_base(Path('/repo'), 7), 'a' * 40)
        self.assertEqual(git.call_args.args[0],
                         ['git', '-C', '/repo', 'rev-list', '--first-parent', '-1',
                          '--before=2026-11-16T12:34:56Z', 'HEAD'])

    def test_the_debt_report_records_every_class_and_shows_what_it_left_out(self):
        classes = [self.mutant('SURVIVED', 20 + index, klass=f'a.C{index}')
                   for index in range(mutation.ADVISORY_CLASS_LIMIT + 3)]
        with self.report(classes) as (_, _, results, summary):
            self.assertEqual(len(results['modules'][0]['unchanged']['classes']), len(classes))
            self.assertIn('and 3 more; see `results.json`.', summary)

    def test_an_incomplete_module_says_so_and_still_decides_nothing(self):
        with self.report([self.mutant('KILLED', 11)], exit_code=1) as (
                code, _, results, summary):
            self.assertEqual((code, results['modules'][0]['state']), (0, 'error'))
            self.assertIn('not a complete debt report', summary)
            # The module's own failure is in the report, not only in the job log.
            self.assertIn(results['modules'][0]['error'], summary)

    def test_entries_the_complete_report_proves_stale_are_advisory_only(self):
        from test_pr_mutation_dispositions import entry as disposition_entry
        with self.report([self.mutant('KILLED', 11)],
                         dispositions=disposition_entry(**{'class': 'a.Foo', 'method': 'gone',
                                                           'descriptor': '()V'})) as (
                code, _, results, summary):
            stale = results['stale_entries']
            self.assertEqual((code, len(stale['entries'])), (0, 1))
            self.assertEqual(stale['entries'][0]['method'], 'gone')
            self.assertIn('proves stale (advisory)', summary)
            self.assertIn('remove or remap each entry', summary)

    def test_the_report_selects_its_own_scope_from_the_branch_it_is_run_on(self):
        """End to end over a real repository: window base, selection and report, PIT aside.

        The other tests here patch the window and the selection to keep their fixtures small,
        which leaves the wiring between them untested - and a wrong argument there would be
        invisible, because a patched function accepts anything.
        """
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            git_repository(root)
            path = 'bundles/sirix-core/src/main/java/a/Foo.java'
            (root / path).parent.mkdir(parents=True)
            (root / '.github').mkdir()
            self.commit(root, path, 30, 'package a;\nclass Foo {\n  int bound = 4;\n}\n')
            head = self.commit(root, path, 1, 'package a;\nclass Foo {\n  int bound = 5;\n}\n')
            output = root / 'out'
            output.mkdir()

            def execute(command, root_path, log, timeout):
                target = output / 'sirix-core/report/mutations.xml'
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text('<mutations>' + self.mutant('SURVIVED', 3) + '</mutations>')
                return 0
            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=execute):
                self.assertEqual(mutation.full_scope(root, output, 30, 7), 0)
            selection = json.loads((output / 'selection.json').read_text())
            results = json.loads((output / 'results.json').read_text())
            summary = (output / 'summary.md').read_text()
        self.assertEqual(selection['modules'], {'sirix-core': ['src/main/java/a/Foo.java']})
        self.assertEqual(selection['changed_lines'],
                         {'sirix-core': {'src/main/java/a/Foo.java': [3]}})
        self.assertEqual(results['window']['head'], head)
        self.assertEqual(results['modules'][0]['changed']['survived'], 1,
                         'the line the window covers is judged as changed')
        # The report names the revisions it compared, abbreviated as elsewhere in the lane.
        self.assertIn(f'`{head[:12]}`', summary)
        self.assertNotIn(f'`{head[:13]}`', summary)

    def test_a_broken_association_file_is_reported_without_deciding_anything(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output = root / 'out'
            output.mkdir()
            (root / '.github').mkdir()
            (root / mutation.ASSOCIATIONS_FILE).write_text('[[association]]\nclass = "a.*"\n')
            with patch.object(mutation, 'window_base', return_value='a' * 40), \
                 patch.object(mutation, 'current_commit', return_value='b' * 40), \
                 patch.object(mutation, 'changed_sources', return_value=({}, {})):
                self.assertEqual(mutation.full_scope(root, output, 30, 7), 0)
            summary = (output / 'summary.md').read_text()
            results = json.loads((output / 'results.json').read_text())
            self.assertTrue(results['associations']['errors'])
            self.assertIn('the lane would reject it', summary)

    def test_the_report_names_every_value_it_interpolates(self):
        """What the scheduled report prints is its whole output; each field has to be the right one."""
        from test_pr_mutation_dispositions import entry as disposition_entry
        with self.report([self.mutant('SURVIVED', 21)],
                         dispositions=disposition_entry(**{'class': 'a.Foo', 'method': 'gone',
                                                           'descriptor': '()V'})) as (
                _, _, results, summary):
            window = results['window']
            self.assertIn(f"## sirix-core: {results['modules'][0]['state']}", summary)
            self.assertIn(f"`{window['base'][:12]}`", summary)
            self.assertIn(f"head `{window['head'][:12]}`", summary)
            self.assertNotIn(f"`{window['base'][:13]}`", summary)
            self.assertIn(window['produced_at'], summary)
            # The stale entry is named by its own identity, not by another entry's fields.
            self.assertIn('entry 1 [equivalent] a.Foo.gone()V', summary)
            self.assertIn(results['stale_entries']['entries'][0]['mutator'], summary)
            self.assertIn(f"indexes {results['stale_entries']['entries'][0]['indexes']}", summary)

    def test_the_report_names_the_errors_of_both_repository_files(self):
        from test_pr_mutation_dispositions import entry as disposition_entry
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output = root / 'out'
            output.mkdir()
            (root / '.github').mkdir()
            (root / mutation.ASSOCIATIONS_FILE).write_text('[[association]]\nclass = "a.*"\n')
            (root / mutation.DISPOSITIONS_FILE).write_text(disposition_entry(kind='flaky'))
            with patch.object(mutation, 'window_base', return_value='a' * 40), \
                 patch.object(mutation, 'current_commit', return_value='b' * 40), \
                 patch.object(mutation, 'changed_sources', return_value=({}, {})):
                self.assertEqual(mutation.full_scope(root, output, 30, 7), 0)
            results = json.loads((output / 'results.json').read_text())
            summary = (output / 'summary.md').read_text()
        for error in results['associations']['errors']:
            self.assertIn(mutation.escape(error), summary)
        for error in results['stale_entries']['errors']:
            self.assertIn(mutation.escape(error), summary)

    def test_the_window_is_rejected_at_its_boundary_with_a_reason(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            git_repository(root)
            self.commit(root, 'old', 30)
            recent = self.commit(root, 'recent', 2)
            self.commit(root, 'head', 0)
            # One day is a window, not an error; the message says what to change when it is not.
            self.assertEqual(mutation.window_base(root, 1), recent)
            for days in (0, -1):
                with self.subTest(days=days), self.assertRaises(ValueError) as refused:
                    mutation.window_base(root, days)
                self.assertIn('--window-days must be positive', str(refused.exception))
            with self.assertRaises(ValueError) as refused:
                mutation.window_base(root, 10_000)
            self.assertIn('widen --window-days', str(refused.exception))

    def test_a_file_with_one_invalid_entry_judges_none_of_them(self):
        # The parser returns the valid entries beside the errors, and an invalid file disposes of
        # nothing: judging the rest would accept a file the lane rejects.
        from test_pr_mutation_dispositions import entry as disposition_entry
        with self.report([self.mutant('KILLED', 11)],
                         dispositions=disposition_entry(**{'class': 'a.Foo', 'method': 'gone',
                                                           'descriptor': '()V'})
                         + disposition_entry(kind='flaky', indexes=[9])) as (
                code, _, results, summary):
            self.assertEqual(code, 0)
            self.assertTrue(results['stale_entries']['errors'])
            self.assertEqual(results['stale_entries']['entries'], [])
            self.assertIn('no entry was judged', summary)

    def test_the_cli_runs_the_report_and_returns_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'out'
            args = ['pr_mutation.py', 'full-scope', '--root', temp, '--output', str(output),
                    '--seconds', '30', '--window-days', '7']
            with patch.object(sys, 'argv', args), \
                 patch.object(mutation, 'window_base', return_value='a' * 40), \
                 patch.object(mutation, 'current_commit', return_value='b' * 40), \
                 patch.object(mutation, 'changed_sources', return_value=({}, {})):
                self.assertEqual(mutation.main(), 0)
            self.assertIn('Full-scope mutation report', (output / 'summary.md').read_text())
            self.assertEqual(json.loads((output / 'results.json').read_text())['modules'], [])

    def test_the_window_is_anchored_in_utc_not_in_the_machines_timezone(self):
        """The cutoff is labelled `Z`, so it has to be computed in UTC.

        A naive local clock produces a cutoff that is wrong by the machine's offset while still
        claiming to be UTC, which moves the window by that offset and selects another base. CI
        runs in UTC, where the two agree, so the timezone is forced here instead.
        """
        previous = os.environ.get('TZ')
        try:
            os.environ['TZ'] = 'Europe/Berlin'  # UTC+1 in winter, UTC+2 in summer
            time.tzset()
            offset = -time.timezone if time.daylight == 0 else -time.altzone
            self.assertGreaterEqual(offset, 3600, 'the forced zone must be an hour ahead of UTC')
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                git_repository(root)
                days = 7
                # `inside` sits 30 minutes inside the window, so the right cutoff excludes it and
                # a cutoff shifted forward by the machine's offset (at least an hour) includes it.
                outside = self.commit(root, 'outside', days + 1)
                self.commit(root, 'inside', days, minutes=30)
                self.commit(root, 'head', 0)
                self.assertEqual(mutation.window_base(root, days), outside,
                                 'the base is the last commit before the UTC cutoff')
        finally:
            if previous is None:
                os.environ.pop('TZ', None)
            else:
                os.environ['TZ'] = previous
            time.tzset()

    def test_a_manifest_without_changed_lines_names_every_module_it_misses(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            (output / 'selection.json').write_text(json.dumps(
                {'modules': {'sirix-core': ['a.java'], 'sirix-query': ['b.java']},
                 'changed_lines': {}}))
            with self.assertRaises(ValueError) as refused:
                mutation.read_selection(output)
        message = str(refused.exception)
        self.assertIn('Selection manifest has no changed lines for', message)
        self.assertIn('sirix-core', message)
        self.assertIn('sirix-query', message, 'the scan continues past the first module')

    def test_an_entry_that_matches_a_reported_mutant_is_not_called_stale(self):
        from test_pr_mutation_dispositions import entry as disposition_entry
        matching = disposition_entry(**{'class': 'a.Foo', 'method': 'run', 'descriptor': '()V',
                                        'mutator': 'm', 'indexes': [7],
                                        'description': 'd'})
        with self.report([self.mutant('SURVIVED', 21)], dispositions=matching) as (
                code, _, results, summary):
            self.assertEqual(code, 0)
            self.assertEqual(results['stale_entries']['entries'], [],
                             'the report lists this mutant, so the entry is not stale')
            self.assertNotIn('proves stale', summary)

    def test_the_report_survives_a_module_that_produced_nothing(self):
        # `stale_entries` reads each module's mutants; a module whose PIT run wrote no report at
        # all has none, and the report still has to be produced.
        from test_pr_mutation_dispositions import entry as disposition_entry
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output = root / 'out'
            output.mkdir()
            (root / '.github').mkdir()
            (root / mutation.DISPOSITIONS_FILE).write_text(disposition_entry())
            with patch.object(mutation, 'window_base', return_value='a' * 40), \
                 patch.object(mutation, 'current_commit', return_value='b' * 40), \
                 patch.object(mutation, 'changed_sources',
                              return_value=({'sirix-core': [self.SOURCE]},
                                            {'sirix-core': {self.SOURCE: [11]}})), \
                 patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', return_value=0):
                self.assertEqual(mutation.full_scope(root, output, 30, 7), 0)
            results = json.loads((output / 'results.json').read_text())
            summary = (output / 'summary.md').read_text()
        self.assertEqual(results['modules'][0]['state'], 'error')
        self.assertEqual(results['stale_entries']['entries'], [],
                         'a run that mutated nothing proves no entry stale')
        self.assertIn('not a complete debt report', summary)

    def test_an_entry_of_a_class_this_run_never_mutated_is_not_called_stale(self):
        from test_pr_mutation_dispositions import entry as disposition_entry
        with self.report([self.mutant('KILLED', 11)],
                         dispositions=disposition_entry(**{'class': 'a.Elsewhere'})) as (
                code, _, results, summary):
            self.assertEqual((code, results['stale_entries']['entries']), (0, []))
            self.assertNotIn('proves stale', summary)

    def test_an_invalid_dispositions_file_judges_no_entry(self):
        from test_pr_mutation_dispositions import entry as disposition_entry
        with self.report([self.mutant('KILLED', 11)],
                         dispositions=disposition_entry(kind='flaky')) as (
                code, _, results, summary):
            self.assertEqual((code, results['stale_entries']['entries']), (0, []))
            self.assertTrue(results['stale_entries']['errors'])
            self.assertIn('is invalid; no entry was judged', summary)


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

    def test_advisory_class_listing_is_complete_and_ordered_by_debt(self):
        """Every class is recorded; only what is displayed is bounded (`format_advisory`)."""
        extra = 2
        with tempfile.TemporaryDirectory() as temp:
            mutants = [self.mutant('SURVIVED', 'Foo.java', f'a.C{index:02d}', 7)
                       for index in range(mutation.ADVISORY_CLASS_LIMIT + extra)]
            mutants.append(self.mutant('SURVIVED', 'Foo.java', 'a.C00', 8))
            report = self.report(temp, mutants)
        classes = report['unchanged']['classes']
        self.assertEqual(len(classes), mutation.ADVISORY_CLASS_LIMIT + extra)
        self.assertEqual(classes[0], ['a.C00', 2])  # the heaviest debt first
        shown = mutation.format_advisory(report)
        listed = [line for line in shown if line.startswith('- a.C')]
        self.assertEqual(len(listed), mutation.ADVISORY_CLASS_LIMIT)
        self.assertIn(f'- and {extra} more; see `results.json`.', shown)

    def test_a_listing_that_fits_says_nothing_about_more(self):
        with tempfile.TemporaryDirectory() as temp:
            report = self.report(temp, [self.mutant('SURVIVED', 'Foo.java', f'a.C{index:02d}', 7)
                                        for index in range(mutation.ADVISORY_CLASS_LIMIT)])
        shown = mutation.format_advisory(report)
        self.assertEqual(len([line for line in shown if line.startswith('- a.C')]),
                         mutation.ADVISORY_CLASS_LIMIT)
        self.assertFalse([line for line in shown if 'more; see' in line], shown)

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
