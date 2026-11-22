"""Reviewed mutation dispositions and test associations (spec/CONV-conventions.md, CONV-201-204).

Every oracle below is read from the specification text, not from the implementation: fixtures are
PIT `mutations.xml` reports, compiled-class images, TOML files and git repositories, and the
observations are exit statuses, `results.json`, `summary.md`, helper output and file bytes.
"""
import contextlib
import html
import importlib.util
import io
import itertools
import json
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'pr_mutation.py'
SPEC = importlib.util.spec_from_file_location('pr_mutation', SCRIPT)
mutation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mutation)
REPOSITORY = SCRIPT.parents[2]

MUTATOR = 'org.pitest.mutationtest.engine.gregor.mutators.ConditionalsBoundaryMutator'
DESCRIPTION = 'changed conditional boundary'
SOURCE = 'src/main/java/a/Foo.java'
BAZ = 'src/main/java/a/b/Baz.java'
SOURCE_PATH = 'bundles/sirix-core/' + SOURCE
JUSTIFICATION = ('The contract admits only even inputs here (REQ-1), so x < 4 and x <= 4 select '
                 'the same branch for every admitted input and return the same result.')
PATCH = (f'--- a/{SOURCE_PATH}\n+++ b/{SOURCE_PATH}\n@@ -11 +11 @@\n'
         '-    return x < 4;\n+    return x <= 4;\n')
EVIDENCE = {'patch': PATCH,
            'command': ['./gradlew', ':sirix-core:test', '--tests', 'a.FooAllocationTest'],
            'baseline_exit': 0, 'injected_exit': 1,
            'failure': 'expected at most 0 allocated bytes but was 48',
            'sources': [{'path': SOURCE_PATH, 'sha256': 'ab' * 32}]}
OUT_OF_LANE = ('The mutant only allocates a boxed bound on each call; results are equal, and '
               'the clean-JVM allocation budget FooAllocationTest bounds that cost.')
# a.Foo.run(I)Z holds lines 10-12; a.Foo.other()V holds lines 20-21.
METHODS = {('run', '(I)Z'): (10, 11, 12), ('other', '()V'): (20, 21)}


def class_file(name, source, methods, handlers=0, padding=0, abstract=()):
    """A class image with a SourceFile and, per method, a Code attribute with a LineNumberTable.

    `handlers` adds exception-table entries to each Code attribute; `padding` adds constant-pool
    entries before the SourceFile name, so its index can exceed a signed 16-bit value; `abstract`
    names (method, descriptor) pairs declared without a Code attribute.
    """
    pool = []

    def utf8(value):
        encoded = value.encode()
        pool.append(b'\x01' + struct.pack('>H', len(encoded)) + encoded)
        return len(pool)
    this_name = utf8(name.replace('.', '/'))
    pool.append(b'\x07' + struct.pack('>H', this_name))
    this_class = len(pool)
    code_name, table_name = utf8('Code'), utf8('LineNumberTable')
    for index in range(padding):
        utf8(f'p{index}')
    source_name, source_value = utf8('SourceFile'), utf8(source)
    encoded_methods = b''
    for (method, descriptor), lines in methods.items():
        method_name, method_descriptor = utf8(method), utf8(descriptor)
        table = struct.pack('>H', len(lines)) + b''.join(
            struct.pack('>HH', pc, line) for pc, line in enumerate(lines))
        bytecode = b'\xb1'  # return
        exceptions = struct.pack('>H', handlers) + struct.pack('>HHHH', 0, 1, 0, 0) * handlers
        code = (struct.pack('>HHI', 1, 1, len(bytecode)) + bytecode + exceptions
                + struct.pack('>H', 1) + struct.pack('>HI', table_name, len(table)) + table)
        encoded_methods += (struct.pack('>HHHH', 0, method_name, method_descriptor, 1)
                            + struct.pack('>HI', code_name, len(code)) + code)
    for method, descriptor in abstract:
        encoded_methods += struct.pack('>HHHH', 0x401, utf8(method), utf8(descriptor), 0)
    return (b'\xca\xfe\xba\xbe' + struct.pack('>HHH', 0, 69, len(pool) + 1) + b''.join(pool)
            + struct.pack('>HHHHH', 0x21, this_class, 0, 0, 0)
            + struct.pack('>H', len(methods) + len(abstract)) + encoded_methods
            + struct.pack('>HHIH', 1, source_name, 2, source_value))


def mutant(status, line, method='run', descriptor='(I)Z', indexes=(7,), description=DESCRIPTION,
           klass='a.Foo', source='Foo.java', mutator=MUTATOR, killing=''):
    detected = status in ('KILLED', 'TIMED_OUT', 'MEMORY_ERROR', 'RUN_ERROR')
    index_text = ''.join(f'<index>{index}</index>' for index in indexes)
    return (f"<mutation detected='{str(detected).lower()}' status='{status}' numberOfTestsRun='1'>"
            f'<sourceFile>{source}</sourceFile><mutatedClass>{klass}</mutatedClass>'
            f'<mutatedMethod>{html.escape(method)}</mutatedMethod>'
            f'<methodDescription>{descriptor}</methodDescription><lineNumber>{line}</lineNumber>'
            f'<mutator>{mutator}</mutator><indexes>{index_text}</indexes>'
            f'<blocks><block>1</block></blocks><killingTest>{killing}</killingTest>'
            f'<description>{html.escape(description)}</description></mutation>')


def toml_value(value):
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return '[' + ', '.join(toml_value(item) for item in value) + ']'
    return '{' + ', '.join(f'{json.dumps(key)} = {toml_value(item)}'
                           for key, item in value.items()) + '}'


def entry(kind='equivalent', **fields):
    """One `[[mutant]]` table; a field set to None is omitted."""
    values = {'class': 'a.Foo', 'method': 'run', 'descriptor': '(I)Z', 'mutator': MUTATOR,
              'indexes': [7], 'description': DESCRIPTION, 'kind': kind,
              'checked_at': 'c' * 40, 'justification': JUSTIFICATION}
    if kind == 'killed-out-of-lane':
        values.update(test='a.FooAllocationTest#allocatesNothing', evidence=EVIDENCE,
                      justification=OUT_OF_LANE)
    values.update(fields)
    return '[[mutant]]\n' + ''.join(f'{json.dumps(key)} = {toml_value(value)}\n'
                                    for key, value in values.items() if value is not None)


class Lane:
    """A temporary checkout of one changed source `a/Foo.java`, compiled, with a faked PIT run."""

    def __init__(self, root):
        self.root = root
        self.output = root / 'out'
        self.output.mkdir()
        module = root / 'bundles/sirix-core'
        (module / SOURCE).parent.mkdir(parents=True)
        (module / SOURCE).write_text('package a;\nclass Foo {}\n')
        classes = module / 'build/classes/java/main/a'
        classes.mkdir(parents=True)
        (classes / 'Foo.class').write_bytes(class_file('a.Foo', 'Foo.java', METHODS))
        # Compiled but never mutated by the faked run, and a class in a two-segment package.
        (classes / 'Bar.class').write_bytes(class_file('a.Bar', 'Bar.java', METHODS))
        (module / BAZ).parent.mkdir(parents=True)
        (module / BAZ).write_text('package a.b;\nclass Baz {}\n')
        (classes / 'b').mkdir()
        (classes / 'b/Baz.class').write_bytes(class_file('a.b.Baz', 'Baz.java',
                                                         {('go', '()V'): (5, 6)}))
        test = module / 'src/test/java/a/FooAllocationTest.java'
        test.parent.mkdir(parents=True)
        test.write_text('package a;\nclass FooAllocationTest {}\n')
        (root / '.github').mkdir()

    def run(self, mutants, head=None, base=None, changed=(11,), exit_code=0, baz_changed=()):
        dispositions = self.root / mutation.DISPOSITIONS_FILE
        if head is None:
            dispositions.unlink(missing_ok=True)
        else:
            dispositions.write_text(head)
        (self.output / 'selection.json').write_text(json.dumps({
            'modules': {'sirix-core': [SOURCE, BAZ]},
            'changed_lines': {'sirix-core': {SOURCE: list(changed), BAZ: list(baz_changed)}},
            'dispositions_base': base}))

        def execute(command, root, log, timeout):
            report = self.output / 'sirix-core/report/mutations.xml'
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text('<mutations>' + ''.join(mutants) + '</mutations>')
            return exit_code
        with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
             patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
             patch.object(mutation, 'execute', side_effect=execute):
            code = mutation.run(self.root, self.output, 10, blocking=True)
        results = json.loads((self.output / 'results.json').read_text())
        return code, results, (self.output / 'summary.md').read_text()


@contextlib.contextmanager
def lane():
    with tempfile.TemporaryDirectory() as temp:
        yield Lane(Path(temp))


def states(results):
    return [record['state'] for record in results['dispositions']['entries']]


class DispositionRecordTest(unittest.TestCase):
    """CONV-201: the record format; an invalid entry voids the whole file."""

    def errors(self, text):
        return mutation.parse_dispositions(text)[1]

    def test_valid_entries_of_both_kinds_parse_and_an_absent_file_has_none(self):
        entries, errors = mutation.parse_dispositions(entry() + entry(
            'killed-out-of-lane', indexes=[9]))
        self.assertEqual(errors, [])
        self.assertEqual([(value['kind'], value['position']) for value in entries],
                         [('equivalent', 1), ('killed-out-of-lane', 2)])
        self.assertEqual(mutation.parse_dispositions(None), ([], []))
        self.assertEqual(mutation.parse_dispositions('# comments only\n'), ([], []))
        # Array descriptors need `[`; constructors need `<init>`.
        self.assertEqual(self.errors(entry(method='<init>', descriptor='([J[[Ljava/lang/String;)V')),
                         [])

    def test_every_missing_unknown_empty_or_mistyped_field_is_rejected(self):
        cases = {'missing': {'mutator': None}, 'unknown': {'line': 11},
                 'empty': {'description': ''}, 'blank': {'method': '  '},
                 'string index': {'indexes': '7'}, 'empty indexes': {'indexes': []},
                 'boolean index': {'indexes': [True]}, 'negative index': {'indexes': [-1]},
                 'number class': {'class': 7}, 'short revision': {'checked_at': 'c' * 39},
                 'uppercase revision': {'checked_at': 'C' * 40}}
        for name, fields in cases.items():
            with self.subTest(name):
                errors = self.errors(entry(**fields))
                self.assertTrue(errors)
                self.assertTrue(all(error.startswith('entry 1:') for error in errors))
        self.assertTrue(self.errors(entry('killed-out-of-lane', test=None)))
        self.assertTrue(self.errors(entry(test='a.FooTest#x')))  # not a field of `equivalent`

    def test_no_identity_field_accepts_a_pattern(self):
        for field, value in (('class', 'a.*'), ('class', 'a.Fo?'), ('method', 'ru*'),
                             ('method', 'run|stop'), ('descriptor', '(*)Z'),
                             ('descriptor', '(I)Z*'), ('mutator', 'org.pitest.*'),
                             ('description', 'changed * boundary'),
                             ('description', 'changed [a-z]+ boundary'),
                             ('description', 'changed\\nboundary')):
            with self.subTest(field=field, value=value):
                self.assertTrue(self.errors(entry(**{field: value})))

    def test_duplicate_identities_and_unknown_kinds_are_rejected(self):
        errors = self.errors(entry() + entry(checked_at='d' * 40))
        self.assertEqual(errors, ['entry 2: repeats the identity of entry 1'])
        for kind in ('hard to test', 'flaky', 'Equivalent', 'unreachable', None):
            with self.subTest(kind=kind):
                self.assertTrue(self.errors(entry(kind=kind)))

    def test_justification_floor_rejects_short_and_placeholder_arguments(self):
        for text in ('equivalent', ' '.join(['short'] * 9),
                     JUSTIFICATION + ' It is hard to test.', JUSTIFICATION + ' TODO: prove',
                     JUSTIFICATION + ' tbd', JUSTIFICATION + ' FIXME', 'Untestable. ' + JUSTIFICATION,
                     JUSTIFICATION + ' Not testable.', JUSTIFICATION + ' Difficult to test.'):
            with self.subTest(text=text):
                self.assertTrue(self.errors(entry(justification=text)))
        # Whitespace padding never reaches the length floor.
        self.assertTrue(self.errors(entry(justification='a ' + ' ' * 80 + 'b')))

    def test_out_of_lane_evidence_must_record_a_counterfactual(self):
        broken = {'baseline failed': {'baseline_exit': 1}, 'mutant passed': {'injected_exit': 0},
                  'boolean exit': {'injected_exit': True}, 'no command': {'command': []},
                  'not a diff': {'patch': 'just text'},
                  'no prefix': {'patch': PATCH.replace('+++ b/', '+++ ')},
                  'unhashed file': {'sources': [{'path': 'bundles/x.java', 'sha256': 'ab' * 32}]},
                  'bad hash': {'sources': [{'path': SOURCE_PATH, 'sha256': 'AB' * 32}]},
                  'extra field': {'sources': [{'path': SOURCE_PATH, 'sha256': 'ab' * 32,
                                               'note': 'x'}]},
                  'escaping path': {'sources': [{'path': '../etc/passwd', 'sha256': 'ab' * 32}]},
                  'no failure': {'failure': ''}, 'no sources': {'sources': []},
                  'deleting patch': {'patch': PATCH.replace(f'+++ b/{SOURCE_PATH}', '+++ /dev/null')}}
        for name, change in broken.items():
            with self.subTest(name):
                self.assertTrue(self.errors(entry('killed-out-of-lane',
                                                  evidence={**EVIDENCE, **change})))
        self.assertTrue(self.errors(entry('killed-out-of-lane', evidence='see PR')))
        missing = {key: value for key, value in EVIDENCE.items() if key != 'failure'}
        self.assertTrue(self.errors(entry('killed-out-of-lane', evidence=missing)))
        for test in ('FooAllocationTest', 'a.FooAllocationTest#', 'a.Foo*Test#x'):
            with self.subTest(test=test):
                self.assertTrue(self.errors(entry('killed-out-of-lane', test=test)))

    def test_malformed_documents_void_the_file(self):
        for text in ('[[mutant]\n', 'mutant = 3\n', 'mutant = [1]\n', 'other = 1\n' + entry()):
            with self.subTest(text=text):
                self.assertTrue(self.errors(text))


class VerdictTest(unittest.TestCase):
    """CONV-202: the lane verdict, observed through the CLI-equivalent `run`."""

    def test_all_killed_changed_lines_are_clean(self):
        with lane() as work:
            code, results, _ = work.run([mutant('KILLED', 11)])
            self.assertEqual(code, 0)
            self.assertEqual(results['dispositions']['entries'], [])

    def test_an_added_entry_of_each_kind_accepts_its_survivor_and_the_kinds_stay_apart(self):
        mutants = [mutant('SURVIVED', 11), mutant('SURVIVED', 12, indexes=(9,))]
        with lane() as work:
            code, results, _ = work.run(mutants, changed=(11, 12))
            self.assertEqual(code, 1)
            failing = results['modules'][0]['changed']['failing']
            self.assertEqual([value['reason'] for value in failing], ['no disposition entry'] * 2)
            head = entry() + entry('killed-out-of-lane', indexes=[9])
            code, results, summary = work.run(mutants, head=head, changed=(11, 12))
            self.assertEqual(code, 0)
            changed = results['modules'][0]['changed']
            self.assertEqual(changed['failing'], [])
            self.assertEqual([value['entry'] for value in changed['dispositioned_equivalent']], [1])
            self.assertEqual([(value['entry'], value['test'])
                              for value in changed['dispositioned_out_of_lane']],
                             [(2, 'a.FooAllocationTest#allocatesNothing')])
            self.assertEqual(changed['dispositioned_equivalent'][0]['justification'],
                             JUSTIFICATION)
            self.assertIn('Dispositioned as equivalent: 1. '
                          'Dispositioned as killed out of lane: 1. Failing: 0.', summary)
            self.assertIn('Dispositioned as killed by an out-of-lane test:', summary)

    def test_no_coverage_is_never_dispositioned(self):
        with lane() as work:
            code, results, _ = work.run([mutant('NO_COVERAGE', 11)], head=entry())
            self.assertEqual(code, 1)
            self.assertEqual(states(results), ['unconfirmed'])
            failing = results['modules'][0]['changed']['failing']
            self.assertIn('never dispositioned', failing[0]['reason'])
            code, results, _ = work.run([mutant('NO_COVERAGE', 11)],
                                        head=entry('killed-out-of-lane'))
            self.assertEqual(code, 1)

    def test_identity_ignores_the_line_so_a_shifted_mutant_still_matches(self):
        with lane() as work:
            for line in (10, 11, 12):
                with self.subTest(line=line):
                    code, _, _ = work.run([mutant('SURVIVED', line)], head=entry(),
                                          changed=(line,))
                    self.assertEqual(code, 0)

    def test_a_near_miss_identity_never_accepts_the_mutant(self):
        near = [{'description': DESCRIPTION.capitalize()}, {'description': DESCRIPTION + ' '},
                {'indexes': [7, 8]}, {'indexes': [8]}, {'descriptor': '(J)Z'},
                {'mutator': MUTATOR + 'X'}, {'class': 'a.Foo$Inner'}]
        with lane() as work:
            for fields in near:
                with self.subTest(fields=fields):
                    code, results, _ = work.run([mutant('SURVIVED', 11)], head=entry(**fields))
                    self.assertEqual(code, 1)
                    self.assertEqual(results['modules'][0]['changed']['dispositioned_equivalent'],
                                     [])

    def test_new_entries_must_match_a_survivor_of_this_run(self):
        cases = {'class not mutated': entry(**{'class': 'a.Bar'}),
                 'no such mutant': entry(indexes=[99])}
        with lane() as work:
            for name, head in cases.items():
                with self.subTest(name):
                    code, results, _ = work.run([mutant('KILLED', 11)], head=head)
                    self.assertEqual(code, 1)
                    self.assertIn(states(results)[0], ('unconfirmed', 'stale'))
            # An unchanged-line survivor of a changed file may be dispositioned (refactor remap).
            code, results, summary = work.run([mutant('SURVIVED', 21, method='other',
                                                      descriptor='()V')],
                                              head=entry(method='other', descriptor='()V'))
            self.assertEqual(code, 0)
            self.assertEqual(results['dispositions']['entries'][0]['action'],
                             'on an unchanged line of a changed file')

    def test_a_killed_mutant_contradicts_equivalence_and_supersedes_out_of_lane(self):
        killed = [mutant('KILLED', 11, killing='a.FooTest.boundary')]
        with lane() as work:
            code, results, _ = work.run(killed, head=entry())
            self.assertEqual((code, states(results)), (1, ['contradicted']))
            self.assertIn('a.FooTest.boundary', results['dispositions']['entries'][0]['action'])
            # Pre-existing entries are refuted too, even on lines this PR did not change.
            code, results, _ = work.run([mutant('KILLED', 21, method='other', descriptor='()V')],
                                        head=entry(method='other', descriptor='()V'),
                                        base=entry(method='other', descriptor='()V'))
            self.assertEqual((code, states(results)), (1, ['contradicted']))
            code, results, _ = work.run(killed, head=entry('killed-out-of-lane'))
            self.assertEqual((code, states(results)), (1, ['superseded']))
            code, results, _ = work.run([mutant('TIMED_OUT', 11)], head=entry())
            self.assertEqual((code, states(results)), (0, ['detected-other']))

    def test_re_confirmation_is_required_exactly_when_the_entrys_own_method_changed(self):
        unchanged_line = [mutant('SURVIVED', 10)]
        with lane() as work:
            code, results, _ = work.run(unchanged_line, head=entry(), base=entry(),
                                        changed=(11,))
            self.assertEqual((code, states(results)), (1, ['needs re-confirmation']))
            self.assertIn('checked_at', results['dispositions']['entries'][0]['action'])
            # An edit that also moved the mutant's identity points the author to remap.
            code, results, _ = work.run([mutant('SURVIVED', 10, indexes=(12,))], head=entry(),
                                        base=entry(), changed=(11,))
            self.assertEqual((code, states(results)), (1, ['needs re-confirmation']))
            self.assertIn('--remap', results['dispositions']['entries'][0]['action'])
            # Re-reviewed and updated: the same identity with a new checked_at passes.
            code, results, _ = work.run(unchanged_line, head=entry(checked_at='d' * 40),
                                        base=entry(), changed=(11,))
            self.assertEqual((code, states(results)), (0, ['accepted']))
            self.assertEqual(results['dispositions']['re_confirmed'], 1)
            # Another method of the same file changed: no re-confirmation.
            code, results, _ = work.run(unchanged_line, head=entry(), base=entry(),
                                        changed=(20,))
            self.assertEqual((code, states(results)), (0, ['advisory']))
            # A comment-only change outside every method's line table changes no method.
            code, results, _ = work.run(unchanged_line, head=entry(), base=entry(), changed=(5,))
            self.assertEqual((code, states(results)), (0, ['advisory']))

    def test_an_invalid_merge_base_file_makes_every_entry_new(self):
        with lane() as work:
            code, results, _ = work.run([mutant('SURVIVED', 11)], head=entry(),
                                        base='[[mutant]\n')
            self.assertEqual((code, states(results)), (0, ['accepted']))
            self.assertEqual((results['dispositions']['base'], results['dispositions']['new']),
                             ('invalid', 1))

    def test_stale_entries_fail_with_an_actionable_remedy(self):
        with lane() as work:
            cases = {'gone class': entry(**{'class': 'a.Gone'}),
                     'gone method': entry(method='renamed'),
                     'gone test': entry('killed-out-of-lane', test='a.RemovedTest#x')}
            for name, head in cases.items():
                with self.subTest(name):
                    code, results, summary = work.run([mutant('KILLED', 11)], head=head,
                                                      base=head)
                    self.assertEqual((code, states(results)), (1, ['stale']))
                    action = results['dispositions']['entries'][0]['action']
                    self.assertTrue('--remap' in action or 'counterfactual' in action, action)
                    self.assertIn('Entries that fail this run', summary)

    def test_sampled_lane_runs_do_not_judge_an_absent_mutant(self):
        with lane() as work:
            code, results, summary = work.run([mutant('KILLED', 11)],
                                              head=entry(method='other', descriptor='()V'),
                                              base=entry(method='other', descriptor='()V'))
            self.assertEqual((code, states(results)), (0, ['not judged']))
            self.assertIn('Reported entries (not failing)', summary)

    def test_an_invalid_file_disposes_of_nothing(self):
        with lane() as work:
            code, results, summary = work.run([mutant('SURVIVED', 11)],
                                              head=entry() + entry(kind='flaky', indexes=[3]))
            self.assertEqual(code, 1)
            self.assertEqual(results['dispositions']['entries'], [])
            self.assertTrue(results['dispositions']['errors'][0].startswith('entry 2:'))
            self.assertIn('is invalid; no entry applies', summary)
            self.assertEqual(len(results['modules'][0]['changed']['failing']), 1)

    def test_statuses_other_than_survived_and_no_coverage_are_never_dispositioned(self):
        with lane() as work:
            code, results, _ = work.run([mutant('NOT_STARTED', 11)], head=entry())
            self.assertEqual(code, 1)
            self.assertEqual(states(results), ['unconfirmed'])
            self.assertIn('NOT_STARTED is not a dispositionable status',
                          results['modules'][0]['changed']['failing'][0]['reason'])
            # A pre-existing entry of an unchanged method on an uncovered mutant has no effect.
            code, results, _ = work.run([mutant('NO_COVERAGE', 21, method='other',
                                                descriptor='()V')],
                                        head=entry(method='other', descriptor='()V'),
                                        base=entry(method='other', descriptor='()V'))
            self.assertEqual((code, states(results)), (0, ['inactive']))

    def test_malformed_report_indexes_and_manifest_bases_are_rejected_safely(self):
        with lane() as work:
            broken = mutant('SURVIVED', 11).replace('<index>7</index>', '<index>x</index>')
            code, results, _ = work.run([broken], head=entry())
            self.assertEqual(code, 1)
            self.assertEqual(results['modules'][0]['changed']['dispositioned_equivalent'], [])
            (work.output / 'selection.json').write_text(json.dumps(
                {'modules': {}, 'changed_lines': {}, 'dispositions_base': 3}))
            with self.assertRaises(ValueError):
                mutation.run(work.root, work.output, 10, blocking=True)

    def test_two_out_of_lane_entries_are_accepted_and_reported(self):
        with lane() as work:
            head = entry('killed-out-of-lane') + entry('killed-out-of-lane', indexes=[9])
            code, results, summary = work.run(
                [mutant('SURVIVED', 11), mutant('SURVIVED', 12, indexes=(9,))],
                head=head, changed=(11, 12))
            self.assertEqual((code, states(results)), (0, ['accepted', 'accepted']))
            accepted = results['modules'][0]['changed']['dispositioned_out_of_lane']
            self.assertEqual([value['entry'] for value in accepted], [1, 2])
            reported = summary.split('Dispositioned as killed by an out-of-lane test:', 1)[1]
            for value in accepted:
                mutant_text = html.escape(value['mutant']).replace('|', '&#124;')
                self.assertIn(f"{mutant_text} | entry {value['entry']} | test "
                              'a.FooAllocationTest#allocatesNothing', reported)

    def test_class_literal_does_not_make_an_existing_out_of_lane_test_stale(self):
        """CONV-202: named-test staleness follows declarations, including Kotlin tests."""
        with lane() as work:
            tests = work.root / 'bundles/sirix-core/src/test'
            (tests / 'java/a/FooAllocationTest.java').unlink()
            kotlin = tests / 'kotlin/a/Contracts.kt'
            kotlin.parent.mkdir(parents=True)
            kotlin.write_text('package a\nval token = String::class\nclass FooAllocationTest\n')
            code, results, _ = work.run([mutant('SURVIVED', 11)],
                                        head=entry('killed-out-of-lane'))
            self.assertEqual((code, states(results)), (0, ['accepted']))

    def test_incomplete_runs_stay_failing_whatever_the_entries(self):
        with lane() as work:
            code, results, _ = work.run([mutant('SURVIVED', 11)], head=entry(), exit_code=3)
            self.assertEqual(code, 1)
            self.assertEqual(results['modules'][0]['state'], 'error')

    def test_a_docs_only_change_still_judges_the_file(self):
        with lane() as work:
            (work.output / 'selection.json').write_text(json.dumps(
                {'modules': {}, 'changed_lines': {}, 'dispositions_base': None}))
            (work.root / mutation.DISPOSITIONS_FILE).write_text(entry())
            self.assertEqual(mutation.run(work.root, work.output, 10, blocking=True), 1)
            (work.root / mutation.DISPOSITIONS_FILE).write_text(entry())
            (work.output / 'selection.json').write_text(json.dumps(
                {'modules': {}, 'changed_lines': {}, 'dispositions_base': entry()}))
            self.assertEqual(mutation.run(work.root, work.output, 10, blocking=True), 0)


class ReviewTest(unittest.TestCase):
    """CONV-203: visibility, no out-of-band relaxation, monotone removal."""

    def test_summary_opens_with_counts_and_lists_every_added_claim_in_full(self):
        long_argument = JUSTIFICATION + ' ' + 'Further contract detail. ' * 20
        with lane() as work:
            _, results, summary = work.run(
                [mutant('SURVIVED', 11)], head=entry(justification=long_argument),
                base=entry(indexes=[3]))
            section = summary[summary.index('## Mutation dispositions'):]
            self.assertTrue(section.splitlines()[2].startswith(
                'Entries added: 1 (new: 1, re-confirmed: 0). Removed: 1.'))
            added = section[section.index('### Added entries'):]
            self.assertIn(mutation.escape(long_argument), added)
            # The per-module list may truncate; results.json never does.
            self.assertEqual(results['modules'][0]['changed']['dispositioned_equivalent'][0]
                             ['justification'], long_argument)

    def test_file_text_is_escaped_in_the_summary(self):
        argument = JUSTIFICATION + ' <script>alert(1)</script> | cell'
        with lane() as work:
            _, _, summary = work.run([mutant('SURVIVED', 11)], head=entry(justification=argument))
            self.assertNotIn('<script>', summary)
            self.assertNotIn('| cell', summary)

    def test_the_ci_commands_accept_no_disposition_input(self):
        parser = mutation.build_parser()
        actions = {name: {option for action in sub._actions for option in action.option_strings}
                   for name, sub in parser._subparsers._group_actions[0].choices.items()}
        self.assertEqual(actions['run'], {'-h', '--help', '--output', '--seconds', '--blocking',
                                          '--root'})
        self.assertEqual(actions['select'], {'-h', '--help', '--output', '--base', '--head',
                                             '--root'})
        with lane() as work, patch.dict('os.environ', {'PR_MUTATION_DISPOSITIONS': entry()}):
            code, _, _ = work.run([mutant('SURVIVED', 11)])
            self.assertEqual(code, 1)

    def test_removing_entries_never_accepts_a_mutant(self):
        """Property over every subset: accepted(subset) is contained in accepted(all)."""
        mutants = [mutant('SURVIVED', 11), mutant('SURVIVED', 12, indexes=(9,)),
                   mutant('NO_COVERAGE', 12, indexes=(4,)), mutant('KILLED', 10, indexes=(2,))]
        entries = [entry(), entry('killed-out-of-lane', indexes=[9]), entry(indexes=[4]),
                   entry(indexes=[2])]

        def accepted(head, base):
            _, results, _ = work.run(mutants, head=head, base=base, changed=(10, 11, 12))
            changed = results['modules'][0]['changed']
            return {value['mutant'] for value in changed['dispositioned_equivalent']
                    + changed['dispositioned_out_of_lane']}
        with lane() as work:
            for base in (None, ''.join(entries)):
                everything = accepted(''.join(entries), base)
                for size in range(len(entries)):
                    for subset in itertools.combinations(entries, size):
                        with self.subTest(base=bool(base), subset=len(subset)):
                            self.assertLessEqual(accepted(''.join(subset), base), everything)


class RefactorHelperTest(unittest.TestCase):
    """CONV-203: local check, remap and counterfactual helpers; CI never runs them."""

    def report(self, root, mutants):
        path = root / 'mutations.xml'
        path.write_text('<mutations>' + ''.join(mutants) + '</mutations>')
        return path

    def test_remap_proposes_tiered_candidates_without_writing_the_file(self):
        with lane() as work:
            head = (entry() + entry(method='old', descriptor='()V', indexes=[1])
                    + entry(**{'class': 'a.Moved'}, method='other', descriptor='()V', indexes=[5]))
            (work.root / mutation.DISPOSITIONS_FILE).write_text(head)
            before = (work.root / mutation.DISPOSITIONS_FILE).read_bytes()
            report = self.report(work.root, [
                mutant('SURVIVED', 11, indexes=(8,)),                         # same method
                mutant('SURVIVED', 20, method='renamed', descriptor='()V', indexes=(1,)),
                mutant('SURVIVED', 21, method='other', descriptor='()V', indexes=(6,)),
                mutant('SURVIVED', 12, description='negated conditional')])  # other mutator text
            text = mutation.remap_dispositions(work.root, [report])
            self.assertEqual((work.root / mutation.DISPOSITIONS_FILE).read_bytes(), before)
            blocks = text.split('\nentry ')
            self.assertIn('same method, other instruction indexes', blocks[0])
            self.assertIn('indexes = [8]', blocks[0])
            self.assertNotIn('negated conditional', text)
            self.assertIn('renamed method of the same class', blocks[1])
            self.assertIn('method = "renamed"', blocks[1])
            self.assertIn('same method name and descriptor in another class', blocks[2])
            # Proposed identities are well formed: pasted into an entry they validate.
            proposal = '\n'.join(line.strip() for line in blocks[0].splitlines()[2:8])
            values = tomllib.loads(proposal)
            self.assertEqual(mutation.parse_dispositions(entry(**values))[1], [])

    def test_remap_keeps_all_overlapping_tiers_and_deduplicates_reports(self):
        """CONV-203: a same-class decoy must not hide the moved counterpart."""
        for same_method in (False, True):
            with self.subTest(same_method=same_method), lane() as work:
                (work.root / mutation.DISPOSITIONS_FILE).write_text(entry())
                before = (work.root / mutation.DISPOSITIONS_FILE).read_bytes()
                mutants = [mutant('SURVIVED', 20, method='other'),
                           mutant('SURVIVED', 30, klass='a.Bar', source='Bar.java'),
                           mutant('SURVIVED', 40, klass='a.Unrelated', method='other')]
                if same_method:
                    mutants.append(mutant('SURVIVED', 12, indexes=(8,)))
                report = self.report(work.root, mutants)
                text = mutation.remap_dispositions(work.root, [report, report])
                header, *candidates = text.split('\n  # ')
                self.assertIn(f'{3 if same_method else 2} candidate(s)', header)
                proposals = [tomllib.loads('\n'.join(candidate.splitlines()[1:]))
                             for candidate in candidates]
                expected = ([('a.Foo', 'run', [8])] if same_method else [])
                expected += [('a.Foo', 'other', [7]), ('a.Bar', 'run', [7])]
                self.assertEqual([(p['class'], p['method'], p['indexes']) for p in proposals],
                                 expected)
                tiers = (['same method, other instruction indexes'] if same_method else [])
                tiers += ['renamed method of the same class',
                          'same method name and descriptor in another class']
                for candidate, tier, proposal in zip(candidates, tiers, proposals):
                    self.assertIn(tier, candidate.splitlines()[0])
                    self.assertEqual('same instruction indexes' in candidate.splitlines()[0],
                                     proposal['indexes'] == [7])
                self.assertEqual((work.root / mutation.DISPOSITIONS_FILE).read_bytes(), before)

    def test_check_lists_stale_and_re_confirmation_entries_with_their_action(self):
        with lane() as work:
            (work.root / mutation.DISPOSITIONS_FILE).write_text(
                entry() + entry(method='gone', descriptor='()V'))
            report = self.report(work.root, [mutant('SURVIVED', 11)])
            lines = {'sirix-core': {SOURCE: [11]}}
            with patch.object(mutation, 'changed_sources', return_value=({}, lines)), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=entry()):
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40, False)
            self.assertTrue(failing)
            self.assertIn('FAIL entry 1 [needs re-confirmation]', text)
            self.assertIn('FAIL entry 2 [stale]', text)
            self.assertIn('--remap', text)
            # Without a per-class limit the sample is complete, so an absent mutant is stale.
            (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(indexes=[42]))
            lines = {'sirix-core': {SOURCE: [5]}}  # no method changed
            with patch.object(mutation, 'changed_sources', return_value=({}, lines)), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=entry(indexes=[42])):
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40, False)
                self.assertIn('[stale] a.Foo.run: PIT no longer generates', text)
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40, True)
                self.assertIn('[not judged]', text)

    def test_dispositions_cli_runs_check_and_remap_and_reports_failure(self):
        with lane() as work:
            (work.root / mutation.DISPOSITIONS_FILE).write_text(entry())
            report = self.report(work.root, [mutant('SURVIVED', 11)])
            args = ['pr_mutation.py', 'dispositions', '--root', str(work.root), '--report',
                    str(report), '--check', '--remap', '--base', 'f' * 40]
            lines = {'sirix-core': {SOURCE: [11]}}
            with patch.object(sys, 'argv', args), \
                 patch.object(mutation, 'changed_sources', return_value=({}, lines)), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=None), \
                 patch.object(sys, 'stdout', new_callable=io.StringIO) as out:
                self.assertEqual(mutation.main(), 0)
            self.assertIn('ok entry 1 [accepted]', out.getvalue())
            self.assertIn('Every entry matches a mutant of the given reports.', out.getvalue())
            with patch.object(sys, 'argv', args[:-3] + ['--base', 'f' * 40]), \
                 patch.object(mutation, 'changed_sources', return_value=({}, lines)), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=entry()), \
                 patch.object(sys, 'stdout', new_callable=io.StringIO) as out:
                self.assertEqual(mutation.main(), 1)

    def test_remap_lists_the_candidate_with_unchanged_instruction_indexes_first(self):
        """A rename keeps instruction ordinals, so the true counterpart is marked and listed first."""
        with lane() as work:
            (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(method='old', indexes=[9]))
            report = self.report(work.root, [mutant('SURVIVED', 11, indexes=(4,)),
                                             mutant('SURVIVED', 12, indexes=(9,))])
            text = mutation.remap_dispositions(work.root, [report])
            candidates = [line for line in text.splitlines() if line.strip().startswith('indexes')]
            self.assertEqual(candidates, ['  indexes = [9]', '  indexes = [4]'])
            self.assertEqual(text.count('same instruction indexes'), 1)

    def test_remap_reports_invalid_files_and_entries_without_candidates(self):
        with lane() as work:
            (work.root / mutation.DISPOSITIONS_FILE).write_text(
                entry(method='gone', descriptor='()V', description='negated conditional')
                + entry(kind='flaky', indexes=[1]))
            report = self.report(work.root, [mutant('SURVIVED', 11)])
            text = mutation.remap_dispositions(work.root, [report])
            self.assertIn('error: entry 2:', text)
            self.assertIn('no candidate', text)

    def test_dispositions_cli_requires_a_mode_and_a_base_for_check(self):
        for args in (['dispositions', '--report', 'x'],
                     ['dispositions', '--check', '--report', 'x']):
            with self.subTest(args=args), patch.object(sys, 'argv', ['pr_mutation.py', *args]), \
                 patch.object(sys, 'stderr', new_callable=io.StringIO), \
                 self.assertRaises(SystemExit) as exit_status:
                mutation.main()
            self.assertEqual(exit_status.exception.code, 2)


class CounterfactualTest(unittest.TestCase):
    """The counterfactual runner: baseline passes, injected fails, sources restored byte-exact."""

    PATCH = ('--- a/Foo.java\n+++ b/Foo.java\n@@ -1,3 +1,3 @@\n class Foo {\n'
             '-  int bound = 4;\n+  int bound = 5;\n }\n')

    def repository(self, temp):
        root = Path(temp) / 'repo'
        root.mkdir()
        (root / 'Foo.java').write_text('class Foo {\n  int bound = 4;\n}\n')
        (Path(temp) / 'mutant.patch').write_text(self.PATCH)
        return root, Path(temp) / 'mutant.patch'

    @staticmethod
    def checker(expected):
        return [sys.executable, '-c', 'import sys; text = open("Foo.java").read(); '
                f'print("bound check"); sys.exit(0 if {expected!r} in text else 7)']

    def test_evidence_round_trips_and_sources_are_restored(self):
        with tempfile.TemporaryDirectory() as temp:
            root, patch_file = self.repository(temp)
            before = (root / 'Foo.java').read_bytes()
            evidence = mutation.counterfactual(root, patch_file, self.checker('bound = 4'))
            self.assertEqual((root / 'Foo.java').read_bytes(), before)
            self.assertEqual((evidence['baseline_exit'], evidence['injected_exit']), (0, 7))
            self.assertEqual(evidence['failure'], 'bound check')
            self.assertEqual(evidence['sources'][0]['path'], 'Foo.java')
            rendered = mutation.render_evidence(evidence)
            document = tomllib.loads(entry('killed-out-of-lane', evidence=None) + rendered)
            self.assertEqual(document['mutant'][0]['evidence'], evidence)
            self.assertEqual(mutation.evidence_problems(evidence), [])

    def test_runner_refuses_evidence_that_proves_nothing(self):
        with tempfile.TemporaryDirectory() as temp:
            root, patch_file = self.repository(temp)
            before = (root / 'Foo.java').read_bytes()
            for command, message in ((self.checker('bound = 9'), 'fails without the mutant'),
                                     (self.checker('class Foo'), 'passes with the mutant'),
                                     ([], 'no test command')):
                with self.subTest(message=message):
                    with self.assertRaises(mutation.CounterfactualError) as error:
                        mutation.counterfactual(root, patch_file, command)
                    self.assertIn(message, str(error.exception))
                    self.assertEqual((root / 'Foo.java').read_bytes(), before)
            (root / 'Foo.java').write_text('class Foo {\n  int other = 4;\n}\n')
            with self.assertRaises(mutation.CounterfactualError) as error:
                mutation.counterfactual(root, patch_file, self.checker('class Foo'))
            self.assertIn('does not apply', str(error.exception))

    def test_a_test_that_rewrites_sources_cannot_produce_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            root, patch_file = self.repository(temp)
            tamper = [sys.executable, '-c', 'import sys; text = open("Foo.java").read(); '
                      'open("Foo.java", "a").write("// touched\\n"); '
                      'sys.exit(0 if "bound = 4" in text else 7)']
            with self.assertRaises(mutation.CounterfactualError) as error:
                mutation.counterfactual(root, patch_file, tamper)
            self.assertIn('restore Foo.java from version control', str(error.exception))

    def test_a_patch_that_is_not_a_diff_is_refused_before_any_run(self):
        with tempfile.TemporaryDirectory() as temp:
            root, patch_file = self.repository(temp)
            patch_file.write_text('replace 4 by 5')
            with self.assertRaises(mutation.CounterfactualError) as error:
                mutation.counterfactual(root, patch_file, self.checker('bound = 4'))
            self.assertIn('not a unified diff', str(error.exception))

    def test_cli_prints_evidence_or_explains_the_refusal(self):
        with tempfile.TemporaryDirectory() as temp:
            root, patch_file = self.repository(temp)
            args = ['pr_mutation.py', 'counterfactual', '--root', str(root), '--patch',
                    str(patch_file), '--', *self.checker('bound = 4')]
            with patch.object(sys, 'argv', args), \
                 patch.object(sys, 'stdout', new_callable=io.StringIO) as out:
                self.assertEqual(mutation.main(), 0)
            self.assertTrue(out.getvalue().startswith('[mutant.evidence]\n'))
            args[-1] = args[-1].replace('bound = 4', 'bound = 9')
            with patch.object(sys, 'argv', args), \
                 patch.object(sys, 'stderr', new_callable=io.StringIO) as err:
                self.assertEqual(mutation.main(), 1)
            self.assertIn('No evidence:', err.getvalue())

    def test_toml_strings_round_trip_awkward_text(self):
        for value in ('plain', 'two\nlines', "ends with quote'\n'", "has ''' inside\n",
                      'tab\tand\rreturn\n', 'del\x7f\n', 'unicode é \U0001f600\n',
                      'back\\slash\n"quote"'):
            with self.subTest(value=value):
                self.assertEqual(tomllib.loads('v = ' + mutation.toml_string(value))['v'], value)


class AssociationTest(unittest.TestCase):
    """CONV-204: existing behaviour tests the lane must run for a changed class."""

    def test_timestamp_writers_select_the_commit_and_retry_contracts(self):
        """TXN-310/311: the emitted PIT scope must include the executable timestamp contracts."""
        associations, errors = mutation.load_associations(REPOSITORY)
        self.assertEqual(errors, [])
        module = REPOSITORY / 'bundles/sirix-core'
        contracts = {'io.sirix.io.CommitTimestampPrecisionConformanceTest*',
                     'io.sirix.io.CommitTimestampLateFailureTest*'}
        for name in ('NodeStorageEngineWriter', 'StorageEngineWriterFactory'):
            source = f'src/main/java/io/sirix/access/trx/page/{name}.java'
            with self.subTest(source=source):
                patterns, unresolved, _ = mutation.test_scope(module, [source], associations)
                self.assertEqual(unresolved, [])
                self.assertTrue(contracts.issubset(patterns), patterns)

    @contextlib.contextmanager
    def module(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            module = root / 'bundles/sirix-core'
            for path, text in {'src/main/java/a/Foo.java': 'package a;\nclass Foo {}\n',
                               'src/main/java/a/Bar.java': 'package a;\nclass Bar {}\n',
                               'src/test/java/b/AllocationBudgetTest.java':
                                   'package b;\nclass AllocationBudgetTest {}\n'}.items():
                (module / path).parent.mkdir(parents=True, exist_ok=True)
                (module / path).write_text(text)
            (root / '.github').mkdir()
            yield root, module

    def test_an_associated_test_with_another_name_is_selected_for_its_class_only(self):
        with self.module() as (_, module):
            associations = {'a.Foo': ('b.AllocationBudgetTest',)}
            self.assertEqual(mutation.test_scope(module, ['src/main/java/a/Foo.java'],
                                                 associations),
                             (['b.AllocationBudgetTest*'], [],
                              {'src/main/java/a/Foo.java': 'related'}))
            self.assertEqual(mutation.test_scope(module, ['src/main/java/a/Bar.java'],
                                                 associations)[2],
                             {'src/main/java/a/Bar.java': 'unmapped'})
            with patch.object(mutation, 'MODULE_TEST_LIMIT', 0):
                self.assertEqual(mutation.test_scope(module, ['src/main/java/a/Foo.java'],
                                                     associations)[2],
                                 {'src/main/java/a/Foo.java': 'over-limit'})

    def test_associations_follow_every_declared_binary_class_of_a_source(self):
        """CONV-204: language declarations own classes; filenames cannot invent a class."""
        cases = [
            ('java', 'a/Bundle.java',
             'package actual;\nclass First { class Nested {} }\ninterface Second {}\n',
             ('actual.First', 'actual.Second'), ('actual.Bundle', 'actual.Nested')),
            ('kotlin', 'a/Main.kt',
             'package actual\nfun main() {}\nclass First { class Nested }\nobject Second\n',
             ('actual.MainKt', 'actual.First', 'actual.Second'),
             ('actual.Main', 'actual.Nested')),
            ('kotlin', 'a/Types.kt',
             'package actual\nclass First(val amount: Int)\nfun interface Second\n',
             ('actual.First', 'actual.Second'), ('actual.Types', 'actual.TypesKt')),
            ('kotlin', 'a/values.kt', 'val answer = 42\n', ('ValuesKt',), ('values',)),
            ('kotlin', 'a/Named.kt',
             '// @file:JvmName("Ghost")\n@file:JvmName("Api")\n'
             'package actual\nfun run() {}\n',
             ('actual.Api',), ('actual.Named', 'actual.NamedKt')),
        ]
        for language, filename, text, owners, absent in cases:
            with self.subTest(filename=filename), self.module() as (root, module):
                source = f'src/main/{language}/{filename}'
                path = module / source
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text + '// class Phantom {}\n')
                pairs = {}
                for index, owner in enumerate(owners):
                    test = f'b.Contract{index}Test'
                    test_path = module / f'src/test/java/b/Contract{index}Test.java'
                    test_path.write_text(f'package b;\nclass Contract{index}Test {{}}\n')
                    pairs[owner] = (test,)
                association_file = root / mutation.ASSOCIATIONS_FILE
                association_file.write_text(''.join(
                    f'[[association]]\nclass = "{owner}"\ntests = ["{tests[0]}"]\n'
                    for owner, tests in pairs.items()))
                associations, errors = mutation.load_associations(root)
                self.assertEqual((associations, errors), (pairs, []))
                expected = [f'b.Contract{index}Test*' for index in range(len(owners))]
                self.assertEqual(mutation.test_scope(module, [source], associations),
                                 (expected, [], {source: 'related'}))
                for owner in (*absent, 'actual.Phantom', 'actual.Ghost'):
                    association_file.write_text(
                        f'[[association]]\nclass = "{owner}"\n'
                        'tests = ["b.AllocationBudgetTest"]\n')
                    associations, errors = mutation.load_associations(root)
                    self.assertEqual(associations, {})
                    self.assertIn(f'association {owner}: no main source', errors[0])

    def test_kotlin_class_literals_preserve_association_ownership_and_test_discovery(self):
        """CONV-204: references before declarations cannot change their binary identities."""
        references = ('String::class', 'String :: class', 'String::class.java',
                      'object : Any() {}')
        for reference in references:
            for declaration in ('class Foo', 'object Foo {}'):
                with self.subTest(reference=reference, declaration=declaration), \
                     self.module() as (root, module):
                    source = 'src/main/kotlin/a/Different.kt'
                    main = module / source
                    main.parent.mkdir(parents=True)
                    main.write_text(f'package a\nval token = {reference}\n{declaration}\n')
                    test = module / 'src/test/kotlin/b/Contracts.kt'
                    test.parent.mkdir(parents=True)
                    test.write_text(f'package b\nval token = {reference}\nclass ContractTest\n')
                    (root / mutation.ASSOCIATIONS_FILE).write_text(
                        '[[association]]\nclass = "a.Foo"\ntests = ["b.ContractTest"]\n')
                    associations, errors = mutation.load_associations(root)
                    self.assertEqual((associations, errors), ({'a.Foo': ('b.ContractTest',)}, []))
                    self.assertEqual(mutation.test_scope(module, [source], associations),
                                     (['b.ContractTest*'], [], {source: 'related'}))
                    expected = {'b.AllocationBudgetTest', 'b.ContractTest'}
                    self.assertEqual(set(mutation.module_test_classes(module)), expected)
                    self.assertEqual(mutation.declared_tests(root), expected)

    def test_invalid_associations_fail_the_run_with_a_named_entry(self):
        valid = '[[association]]\nclass = "a.Foo"\ntests = ["b.AllocationBudgetTest"]\n'
        cases = {'valid': (valid, False),
                 'unknown field': (valid + 'reason = "x"\n', True),
                 'pattern class': (valid.replace('a.Foo', 'a.*'), True),
                 'pattern test': (valid.replace('b.AllocationBudgetTest', 'b.*Test'), True),
                 'no such class': (valid.replace('a.Foo', 'a.Gone'), True),
                 'undeclared test': (valid.replace('AllocationBudgetTest', 'OtherTest'), True),
                 'duplicate class': (valid + valid, True),
                 'empty tests': (valid.replace('["b.AllocationBudgetTest"]', '[]'), True),
                 'not toml': ('[[association]\n', True), 'other key': ('x = 1\n', True)}
        for name, (text, invalid) in cases.items():
            with self.subTest(name), self.module() as (root, _):
                (root / mutation.ASSOCIATIONS_FILE).write_text(text)
                associations, errors = mutation.load_associations(root)
                self.assertEqual(bool(errors), invalid, errors)
                if invalid:
                    self.assertEqual(associations, {})
                    output = root / 'out'
                    output.mkdir()
                    (output / 'selection.json').write_text(json.dumps(
                        {'modules': {}, 'changed_lines': {}, 'dispositions_base': None}))
                    self.assertEqual(mutation.run(root, output, 10, blocking=True), 1)
                    summary = (output / 'summary.md').read_text()
                    self.assertIn('mutation-test-associations.toml` is invalid', summary)

    def test_repository_files_are_valid_and_keep_the_former_contract_pairs(self):
        associations, errors = mutation.load_associations(REPOSITORY)
        self.assertEqual(errors, [])
        contract = 'io.sirix.query.scan.VectorizedReleaseCorrectnessTest'
        # CONV-204 permits additional valid associations; retain the former pairs.
        for name in (
            'io.sirix.query.SirixCompileChain',
            'io.sirix.query.compiler.translator.SirixBlockPipelineStrategy',
            'io.sirix.query.compiler.translator.SirixPipelineStrategy',
            'io.sirix.query.scan.VectorizedRouteRefusedException'):
            with self.subTest(name=name):
                self.assertIn(contract, associations.get(name, ()))
        entries, errors = mutation.parse_dispositions(
            (REPOSITORY / mutation.DISPOSITIONS_FILE).read_text())
        self.assertEqual(errors, [])
        self.assertTrue(all(entry['kind'] in mutation.KINDS for entry in entries))


class MergeBaseFileTest(unittest.TestCase):
    """`select` records the dispositions file of the merge base, not of the PR's base tip."""

    def test_the_merge_base_version_is_read_and_an_absent_file_is_none(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)

            def git(*args):
                return subprocess.check_output(
                    ['git', '-C', str(root), '-c', 'user.name=Test',
                     '-c', 'user.email=test@example.invalid', *args], shell=False).decode().strip()
            git('init', '-q', '-b', 'main')
            (root / 'README').write_text('x\n')
            git('add', '.')
            git('commit', '-qm', 'start')
            first = git('rev-parse', 'HEAD')
            git('checkout', '-qb', 'pr')
            self.assertIsNone(mutation.merge_base_file(root, first, mutation.DISPOSITIONS_FILE))
            (root / '.github').mkdir()
            (root / mutation.DISPOSITIONS_FILE).write_text('# on main\n')
            git('checkout', '-q', 'main')
            git('add', '.')
            git('commit', '-qm', 'main adds the file')
            main_tip = git('rev-parse', 'HEAD')
            git('checkout', '-q', 'pr')
            git('merge', '-q', '--no-edit', 'main')
            (root / mutation.DISPOSITIONS_FILE).write_text('# on main\n# PR edit\n')
            git('commit', '-qam', 'pr edits')
            self.assertEqual(mutation.merge_base_file(root, main_tip, mutation.DISPOSITIONS_FILE),
                             '# on main\n')
            with self.assertRaises(ValueError):
                mutation.merge_base_file(root, '--help', mutation.DISPOSITIONS_FILE)


class ClassReaderTest(unittest.TestCase):
    def test_methods_and_line_tables_are_read_by_name_and_descriptor(self):
        name, source, methods = mutation.read_class(class_file('a.Foo$Inner', 'Foo.java', METHODS))
        self.assertEqual((name, source), ('a.Foo$Inner', 'Foo.java'))
        self.assertEqual(methods, {key: frozenset(lines) for key, lines in METHODS.items()})
        self.assertEqual(mutation.class_source(class_file('a.Foo', 'Foo.java', {})),
                         ('a.Foo', 'Foo.java'))


if __name__ == '__main__':
    unittest.main()
