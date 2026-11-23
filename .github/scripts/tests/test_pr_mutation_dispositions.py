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


def census_record(line, method='run', descriptor='(I)Z', indexes=(7,), description=DESCRIPTION,
                  klass='a.Foo', source='Foo.java', mutator=MUTATOR):
    """One line of the census the CHANGEDLINES interceptor writes: identity, never a status."""
    return json.dumps({'class': klass, 'method': method, 'descriptor': descriptor,
                       'mutator': mutator, 'indexes': list(indexes), 'description': description,
                       'sourceFile': source, 'lineNumber': line})


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


def lane_report(root, mutants, state='complete', limit=None, partial=False, targets=('a.Foo',),
                restricted=False):
    """The lane artifact contract: module XML plus results.json completion and sampling."""
    output = root / 'out'
    path = output / 'sirix-core/report/mutations.xml'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('<mutations>' + ''.join(mutants) + '</mutations>')
    (output / 'results.json').write_text(json.dumps({'modules': [
        {'module': 'sirix-core', 'state': state, 'mutants_per_class': limit, 'partial': partial,
         'targets': list(targets), 'restricted': restricted, 'total': len(mutants)}]}))
    return path


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

    def run(self, mutants, head=None, base=None, changed=(11,), exit_code=0, baz_changed=(),
            census=(), stream=(), report=True, timed_out=False, stream_unreadable=False,
            report_bytes=None, census_unreadable=False, targets=('a.Foo',)):
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
            directory = self.output / 'sirix-core'
            (directory / 'report').mkdir(parents=True, exist_ok=True)
            if report:
                (directory / 'report/mutations.xml').write_text(
                    '<mutations>' + ''.join(mutants) + '</mutations>')
            if report_bytes is not None:
                (directory / 'report/mutations.xml').write_bytes(report_bytes)
            if stream:
                (directory / 'report' / mutation.STREAM_FILE).write_bytes(
                    stream if isinstance(stream, bytes) else
                    ''.join(record + '\n' for record in stream).encode())
            if stream_unreadable:
                # Whatever stands where the stream belongs, the reporter must not pretend to
                # have read it: a directory is the simplest unreadable thing.
                (directory / 'report' / mutation.STREAM_FILE).mkdir()
            if census:
                (directory / mutation.CENSUS_FILE).write_bytes(
                    census if isinstance(census, bytes) else
                    ''.join(record + '\n' for record in census).encode())
            if census_unreadable:
                (directory / mutation.CENSUS_FILE).mkdir()
            if timed_out:
                raise subprocess.TimeoutExpired(command, timeout)
            return exit_code
        with patch.object(mutation, 'resolve_targets', return_value=(list(targets), [])), \
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


class CompletionEvidenceTest(unittest.TestCase):
    """CONV-202: each rejected fact withholds every completion claim, never rule 4."""

    FACTS = ('unfinished', 'report_unreadable', 'census_unreadable', 'census_malformed',
             'census_violations', 'missing_artifacts', 'sampled', 'invalid_evidence')
    JUDGEMENTS = ('acceptance', 'dispositioned_equivalent', 'dispositioned_out_of_lane',
                  'staleness', 'empty_scope', 'rule_1_status')

    def evidence(self, work, fact, status='SURVIVED'):
        report = lane_report(work.root, [mutant(status, 11)], restricted=True)
        census = report.parent.parent / mutation.CENSUS_FILE
        census.write_text(census_record(21, indexes=(99,)) + '\n')
        manifest = work.output / 'results.json'
        document = json.loads(manifest.read_text())
        module = document['modules'][0]
        module['census'] = {'file': str(census), 'count': 1, 'malformed': 0,
                            'violations': [], 'unreadable': []}
        if fact == 'unfinished':
            module['state'] = 'timed out'
        elif fact == 'report_unreadable':
            module['total'] = 2
        elif fact == 'census_unreadable':
            census.unlink()
            census.mkdir()
        elif fact == 'census_malformed':
            census.write_text('{broken\n')
        elif fact == 'census_violations':
            census.write_text(census_record(11, indexes=(99,)) + '\n')
        elif fact == 'missing_artifacts':
            census.unlink()
        elif fact == 'sampled':
            module['mutants_per_class'] = 1
        elif fact == 'invalid_evidence':
            module['census'] = {'count': 'unknown'}
        manifest.write_text(json.dumps(document))
        loaded, = mutation.load_evidence([report], [census], {('a', 'Foo.java'): {11}})
        result = mutation.read_report(report, {('a', 'Foo.java'): {11}})
        result.update(module='sirix-core', state=module['state'], targets=['a.Foo'],
                      _mutants=loaded.records, _evidence=loaded.completion)
        return loaded, result

    def test_every_rejection_withholds_every_completion_judgement(self):
        self.assertEqual(set(self.FACTS), set(mutation.COMPLETION_FACTS))
        self.assertEqual(set(self.JUDGEMENTS), set(mutation.COMPLETION_JUDGEMENTS))
        for fact, judgement in itertools.product(self.FACTS, self.JUDGEMENTS):
            with self.subTest(fact=fact, judgement=judgement), lane() as work:
                loaded, result = self.evidence(work, fact)
                self.assertIn(fact, loaded.completion.rejections)
                self.assertFalse(loaded.completion.permits(judgement))
                if judgement in ('acceptance', 'dispositioned_equivalent',
                                 'dispositioned_out_of_lane', 'rule_1_status'):
                    kind = ('killed-out-of-lane' if judgement == 'dispositioned_out_of_lane'
                            else 'equivalent')
                    (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(kind))
                    verdict = mutation.judge_run(work.root, [result],
                                                 {'sirix-core': {SOURCE: [11]}}, None)
                    self.assertEqual(verdict['entries'][0]['state'], 'not judged')
                    self.assertIn(fact, verdict['entries'][0]['action'])
                    if judgement.startswith('dispositioned_'):
                        candidate, = mutation.parse_dispositions(entry(kind))[0]
                        mutation.apply_dispositions(
                            result, {mutation.identity_of(candidate): candidate}, [])
                    self.assertEqual(result['changed']['dispositioned_equivalent'], [])
                    self.assertEqual(result['changed']['dispositioned_out_of_lane'], [])
                    self.assertEqual(len(result['changed']['failing']), 1)
                elif judgement == 'staleness':
                    old = entry(indexes=[42])
                    (work.root / mutation.DISPOSITIONS_FILE).write_text(old)
                    verdict = mutation.judge_run(work.root, [result], {}, old)
                    self.assertEqual(verdict['entries'][0]['state'], 'not judged')
                    self.assertIn(fact, verdict['entries'][0]['action'])
                    self.assertEqual(mutation.stale_entries(work.root, [result])['entries'], [])
                else:
                    empty = {'state': 'error'}
                    self.assertFalse(mutation.resolve_empty_scope(empty, {}, loaded.completion))
                    self.assertNotIn('off_diff_only', empty)

    def test_every_rejection_retains_rule_four(self):
        for fact, kind in itertools.product(self.FACTS, ('equivalent', 'killed-out-of-lane')):
            with self.subTest(fact=fact, kind=kind), lane() as work:
                _, result = self.evidence(work, fact, 'KILLED')
                (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(kind))
                verdict = mutation.judge_run(work.root, [result], {}, None)
                self.assertEqual(verdict['entries'][0]['state'],
                                 'contradicted' if kind == 'equivalent' else 'superseded')
                self.assertFalse(verdict['clean'])

    def test_complete_evidence_permits_every_judgement(self):
        for judgement in self.JUDGEMENTS:
            with self.subTest(judgement=judgement), lane() as work:
                loaded, result = self.evidence(work, None)
                self.assertEqual(loaded.completion.rejections, {})
                self.assertTrue(loaded.completion.permits(judgement))
                if judgement == 'empty_scope':
                    empty = {'state': 'error'}
                    self.assertTrue(mutation.resolve_empty_scope(empty, {}, loaded.completion))
                    self.assertTrue(empty['off_diff_only'])
                elif judgement == 'staleness':
                    old = entry(indexes=[42])
                    (work.root / mutation.DISPOSITIONS_FILE).write_text(old)
                    verdict = mutation.judge_run(work.root, [result], {}, old)
                    self.assertEqual(verdict['entries'][0]['state'], 'stale')
                    self.assertEqual(len(mutation.stale_entries(work.root, [result])['entries']), 1)
                else:
                    kind = ('killed-out-of-lane' if judgement == 'dispositioned_out_of_lane'
                            else 'equivalent')
                    (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(kind))
                    verdict = mutation.judge_run(work.root, [result], {}, None)
                    self.assertEqual(verdict['entries'][0]['state'], 'accepted')
                    key = ('dispositioned_out_of_lane' if kind == 'killed-out-of-lane'
                           else 'dispositioned_equivalent')
                    self.assertEqual(len(result['changed'][key]), 1)
                    self.assertTrue(mutation.is_clean([result]))

    def test_checker_uses_rejection_facts_and_retains_rule_four(self):
        for fact, kind, status in itertools.product(
                self.FACTS, ('equivalent', 'killed-out-of-lane'), ('SURVIVED', 'KILLED')):
            with self.subTest(fact=fact, kind=kind, status=status), lane() as work:
                self.evidence(work, fact, status)
                (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(kind))
                directory = work.output / 'sirix-core'
                with patch.object(mutation, 'changed_sources', return_value=(
                        {}, {'sirix-core': {SOURCE: [11]}})), \
                     patch.object(mutation, 'current_commit', return_value='e' * 40), \
                     patch.object(mutation, 'merge_base_file', return_value=None):
                    failing, text = mutation.check_dispositions(
                        work.root, [directory / 'report/mutations.xml'], 'f' * 40, False,
                        [directory / mutation.CENSUS_FILE])
                self.assertTrue(failing)
                expected = ('not judged' if status == 'SURVIVED' else
                            'contradicted' if kind == 'equivalent' else 'superseded')
                self.assertIn('entry 1 [' + expected + ']', text)
                if status == 'SURVIVED':
                    self.assertIn(fact, text)

    def test_lane_violations_reject_survivors_and_empty_scope(self):
        for empty, kind in itertools.product((False, True), ('equivalent', 'killed-out-of-lane')):
            with self.subTest(empty=empty, kind=kind), lane() as work:
                code, results, summary = work.run(
                    [] if empty else [mutant('SURVIVED', 11)], head=entry(kind),
                    census=[census_record(11, indexes=(99,))])
                self.assertEqual(code, 1)
                module = results['modules'][0]
                self.assertEqual(module['state'], 'error')
                self.assertIn('census_violations', module['completion_rejections'])
                self.assertNotIn('off_diff_only', module)
                self.assertNotIn('No mutant of this module sits on a changed line', summary)
                self.assertNotIn('complete and unsampled', summary)
                if not empty:
                    self.assertEqual(states(results), ['not judged'])
                    self.assertIn('census_violations', results['dispositions']['entries'][0]['action'])
                    self.assertEqual(module['changed']['dispositioned_equivalent'], [])
                    self.assertEqual(module['changed']['dispositioned_out_of_lane'], [])


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

    def test_an_absent_mutant_of_a_mutated_class_is_stale(self):
        # CONV-202 rule 5: nothing was sampled and nothing else was restricted away, so a
        # generated set that lists neither a report mutant nor a census identity is complete
        # evidence that this mutant no longer exists.
        with lane() as work:
            code, results, summary = work.run([mutant('KILLED', 11)],
                                              head=entry(method='other', descriptor='()V'),
                                              base=entry(method='other', descriptor='()V'))
            self.assertEqual((code, states(results)), (1, ['stale']))
            self.assertIn('Entries that fail this run', summary)

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

    def report(self, root, mutants, restricted=False):
        return lane_report(root, mutants, restricted=restricted)

    def test_check_and_lane_judge_a_completed_silent_target_stale(self):
        """CONV-203: a completed generated set includes targets that emitted no mutant."""
        with lane() as work:
            old = entry(**{'class': 'a.Bar', 'checked_at': 'b' * 40})
            head = entry(**{'class': 'a.Bar'})
            _, results, _ = work.run([mutant('KILLED', 11)], head=head, base=old,
                                      targets=('a.Foo', 'a.Bar'))
            self.assertEqual(states(results), ['stale'])
            report = work.output / 'sirix-core/report/mutations.xml'
            with patch.object(mutation, 'changed_sources', return_value=(
                    {}, {'sirix-core': {SOURCE: [11]}})), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=old):
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40, False)
            self.assertTrue(failing)
            self.assertIn('FAIL entry 1 [stale] a.Bar.run', text)
            self.assertNotIn('[not judged]', text)

    def test_check_loads_silent_targets_from_census_module_metadata(self):
        """CONV-203: report and census inputs load the same complete target membership."""
        with lane() as work:
            old = entry(**{'class': 'a.Bar', 'checked_at': 'b' * 40})
            (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(**{'class': 'a.Bar'}))
            lane_report(work.root, [], targets=('a.Foo', 'a.Bar'), restricted=True)
            census = work.output / 'sirix-core/census.jsonl'
            census.write_text(census_record(21) + '\n')
            report = work.output / 'sirix-query/report/mutations.xml'
            report.parent.mkdir(parents=True)
            report.write_text('<mutations>' + mutant('KILLED', 5, klass='a.b.Baz',
                source='Baz.java', method='go', descriptor='()V') + '</mutations>')
            manifest = work.output / 'results.json'
            document = json.loads(manifest.read_text())
            document['modules'].append({'module': 'sirix-query', 'state': 'complete',
                                        'mutants_per_class': None, 'targets': ['a.b.Baz']})
            manifest.write_text(json.dumps(document))
            with patch.object(mutation, 'changed_sources', return_value=(
                    {}, {'sirix-core': {SOURCE: [11], BAZ: [5]}})), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=old):
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40,
                                                            False, [census])
            self.assertTrue(failing)
            self.assertIn('FAIL entry 1 [stale] a.Bar.run', text)

    def test_check_unknown_target_membership_cannot_prove_staleness(self):
        """CONV-203: completion alone cannot prove a class was selected."""
        for targets in (None, 'a.Bar', ['*'], [7]):
            with self.subTest(targets=targets), lane() as work:
                old = entry(**{'class': 'a.Bar', 'checked_at': 'b' * 40})
                (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(**{'class': 'a.Bar'}))
                report = lane_report(work.root, [mutant('KILLED', 11)])
                manifest = work.output / 'results.json'
                document = json.loads(manifest.read_text())
                document['modules'][0]['targets'] = targets
                manifest.write_text(json.dumps(document))
                with patch.object(mutation, 'changed_sources', return_value=(
                        {}, {'sirix-core': {SOURCE: [11]}})), \
                     patch.object(mutation, 'current_commit', return_value='e' * 40), \
                     patch.object(mutation, 'merge_base_file', return_value=old):
                    _, text = mutation.check_dispositions(work.root, [report], 'f' * 40, False)
                self.assertNotIn('[stale]', text)

    def test_check_census_without_its_nonempty_report_cannot_prove_absence(self):
        """CONV-202 rule 5: the census cannot replace a missing report component."""
        with lane() as work:
            old = entry(checked_at='b' * 40)
            _, results, _ = work.run([mutant('KILLED', 11)], head=entry(), base=old,
                                      changed=(5,), census=[census_record(21, indexes=(99,))])
            self.assertEqual(states(results), ['contradicted'])
            report = work.output / 'sirix-query/report/mutations.xml'
            report.parent.mkdir(parents=True)
            report.write_text('<mutations>' + mutant('KILLED', 5, klass='a.b.Baz',
                source='Baz.java', method='go', descriptor='()V') + '</mutations>')
            manifest = work.output / 'results.json'
            document = json.loads(manifest.read_text())
            document['modules'].append({'module': 'sirix-query', 'state': 'complete',
                'mutants_per_class': None, 'targets': ['a.b.Baz'], 'restricted': False, 'total': 1})
            manifest.write_text(json.dumps(document))
            census = work.output / 'sirix-core/census.jsonl'
            with patch.object(mutation, 'changed_sources', return_value=(
                    {}, {'sirix-core': {SOURCE: [5]}})), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=old):
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40,
                                                            False, [census])
            self.assertFalse(failing)
            self.assertIn('entry 1 [not judged]', text)
            self.assertNotIn('[stale]', text)

    def test_missing_census_path_cannot_be_used_as_an_empty_census(self):
        """CONV-202: a helper lacks the producer's proof that a nonexistent census is empty."""
        with lane() as work:
            old = entry(indexes=[42])
            work.run([mutant('KILLED', 11)], head=old, base=old, changed=(5,),
                     census=[census_record(21, indexes=(99,))])
            report = work.output / 'sirix-core/report/mutations.xml'
            census = work.output / 'sirix-core/census.jsonl'
            census.unlink()
            with patch.object(mutation, 'changed_sources', return_value=(
                    {}, {'sirix-core': {SOURCE: [5]}})), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=old):
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40,
                                                            False, [census])
            self.assertFalse(failing)
            self.assertIn('entry 1 [not judged]', text)
            self.assertNotIn('[stale]', text)

    def test_omitted_census_cannot_hide_the_lanes_recorded_failures(self):
        """CONV-203: saved census failures remain failures when their file is not supplied."""
        for record, reason in ((census_record(11, indexes=(99,)), 'is on a changed line'),
                               ('{', 'line 1:')):
            with self.subTest(record=record), lane() as work:
                code, _, _ = work.run([mutant('KILLED', 11)], census=[record])
                self.assertEqual(code, 1)
                report = work.output / 'sirix-core/report/mutations.xml'
                with patch.object(mutation, 'changed_sources', return_value=(
                        {}, {'sirix-core': {SOURCE: [11]}})), \
                     patch.object(mutation, 'current_commit', return_value='e' * 40), \
                     patch.object(mutation, 'merge_base_file', return_value=None):
                    failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40, False)
                self.assertTrue(failing)
                self.assertIn(reason, text)

    def test_added_survivors_need_the_complete_restricted_artifact_pair(self):
        """CONV-202: accepting either disposition kind needs completely loaded evidence."""
        for kind in ('equivalent', 'killed-out-of-lane'):
            with self.subTest(kind=kind), lane() as work:
                work.run([mutant('SURVIVED', 11)], head=entry(kind),
                         census=[census_record(21, indexes=(99,))])
                report = work.output / 'sirix-core/report/mutations.xml'
                census = work.output / 'sirix-core/census.jsonl'
                with patch.object(mutation, 'changed_sources', return_value=(
                        {}, {'sirix-core': {SOURCE: [11]}})), \
                     patch.object(mutation, 'current_commit', return_value='e' * 40), \
                     patch.object(mutation, 'merge_base_file', return_value=None):
                    failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40, False)
                    self.assertTrue(failing)
                    self.assertIn('entry 1 [not judged]', text)
                    failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40,
                                                                False, [census])
                    self.assertFalse(failing)
                    self.assertIn('entry 1 [accepted]', text)

    def test_invalid_or_missing_components_cannot_prove_staleness(self):
        """CONV-202 rule 5: completeness requires every supplied component to be valid."""
        for defect in ('malformed-report', 'malformed-census', 'unreadable-census',
                       'missing-report', 'missing-census', 'report-count', 'census-count'):
            with self.subTest(defect=defect), lane() as work:
                old = entry(indexes=[42])
                (work.root / mutation.DISPOSITIONS_FILE).write_text(old)
                report = self.report(work.root, [mutant('KILLED', 11)], restricted=True)
                census = work.output / 'sirix-core/census.jsonl'
                if defect == 'unreadable-census':
                    census.mkdir()
                elif defect != 'missing-census':
                    census.write_text('{' if defect == 'malformed-census' else
                                      census_record(21, indexes=(99,)) + '\n')
                if defect == 'malformed-report':
                    report.write_text('<mutations>')
                elif defect == 'missing-report':
                    report.unlink()
                if defect in ('report-count', 'census-count'):
                    manifest = work.output / 'results.json'
                    document = json.loads(manifest.read_text())
                    if defect == 'report-count':
                        document['modules'][0]['total'] = 2
                    else:
                        document['modules'][0]['census'] = {
                            'file': str(census), 'count': 2, 'malformed': 0,
                            'violations': [], 'unreadable': []}
                    manifest.write_text(json.dumps(document))
                with patch.object(mutation, 'changed_sources', return_value=(
                        {}, {'sirix-core': {SOURCE: [5]}})), \
                     patch.object(mutation, 'current_commit', return_value='e' * 40), \
                     patch.object(mutation, 'merge_base_file', return_value=old):
                    failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40,
                                                                False, [census])
                self.assertEqual(failing, defect not in ('missing-report', 'missing-census'))
                self.assertIn('entry 1 [not judged]', text)
                self.assertNotIn('[stale]', text)

    def test_producer_recorded_zero_report_is_distinct_from_missing_nonempty_evidence(self):
        """CONV-202: a verified zero report has no missing identities to load."""
        for contents in (None, b'', b'<mutations/>'):
            with self.subTest(contents=contents), lane() as work:
                old = entry(**{'class': 'a.Bar', 'checked_at': 'b' * 40})
                head = entry(**{'class': 'a.Bar'})
                _, results, _ = work.run([], head=head, base=old, report=False,
                    report_bytes=contents, targets=('a.Foo', 'a.Bar'), census=[census_record(21)])
                self.assertEqual(states(results), ['stale'])
                report = work.output / 'sirix-core/report/mutations.xml'
                census = work.output / 'sirix-core/census.jsonl'
                with patch.object(mutation, 'changed_sources', return_value=(
                        {}, {'sirix-core': {SOURCE: [11]}})), \
                     patch.object(mutation, 'current_commit', return_value='e' * 40), \
                     patch.object(mutation, 'merge_base_file', return_value=old):
                    failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40,
                                                                False, [census])
                self.assertTrue(failing)
                self.assertIn('FAIL entry 1 [stale] a.Bar.run', text)

    def test_mixed_artifact_path_forms_keep_the_stale_verdict(self):
        """CONV-203: absolute and relative spellings do not change a complete generated set."""
        with lane() as work:
            old = entry(indexes=[42])
            _, results, _ = work.run([mutant('KILLED', 11)], head=old, base=old,
                changed=(5,), census=[census_record(21, indexes=(99,))])
            self.assertEqual(states(results), ['stale'])
            report = work.output / 'sirix-core/report/mutations.xml'
            census = work.output / 'sirix-core/census.jsonl'
            with contextlib.chdir(work.root), \
                 patch.object(mutation, 'changed_sources', return_value=(
                     {}, {'sirix-core': {SOURCE: [5]}})), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=old):
                for relative_report, relative_census in itertools.product((False, True), repeat=2):
                    with self.subTest(report=relative_report, census=relative_census):
                        report_path = report.relative_to(work.root) if relative_report else report
                        census_path = census.relative_to(work.root) if relative_census else census
                        failing, text = mutation.check_dispositions(
                            work.root, [report_path], 'f' * 40, False, [census_path])
                        self.assertTrue(failing)
                        self.assertIn('FAIL entry 1 [stale] a.Foo.run', text)
                        self.assertNotIn('[not judged]', text)

    def test_mixed_artifact_path_forms_preserve_both_kinds_of_acceptance(self):
        """CONV-203: the same supplied pair authorizes the same survivor in either path form."""
        for kind in ('equivalent', 'killed-out-of-lane'):
            with self.subTest(kind=kind), lane() as work:
                work.run([mutant('SURVIVED', 11)], head=entry(kind),
                         census=[census_record(21, indexes=(99,))])
                report = work.output / 'sirix-core/report/mutations.xml'
                census = work.output / 'sirix-core/census.jsonl'
                with contextlib.chdir(work.root), \
                     patch.object(mutation, 'changed_sources', return_value=(
                         {}, {'sirix-core': {SOURCE: [11]}})), \
                     patch.object(mutation, 'current_commit', return_value='e' * 40), \
                     patch.object(mutation, 'merge_base_file', return_value=None):
                    for relative_report, relative_census in itertools.product((False, True), repeat=2):
                        with self.subTest(report=relative_report, census=relative_census):
                            report_path = report.relative_to(work.root) if relative_report else report
                            census_path = census.relative_to(work.root) if relative_census else census
                            failing, text = mutation.check_dispositions(
                                work.root, [report_path], 'f' * 40, False, [census_path])
                            self.assertFalse(failing)
                            self.assertIn('ok entry 1 [accepted]', text)

    def test_duplicate_path_spellings_report_each_failing_mutant_once(self):
        """One recorded mutant is one failure, even if its artifact is supplied twice."""
        with lane() as work:
            work.run([mutant('SURVIVED', 11)], census=[census_record(21, indexes=(99,))])
            report = work.output / 'sirix-core/report/mutations.xml'
            census = work.output / 'sirix-core/census.jsonl'
            with contextlib.chdir(work.root), \
                 patch.object(mutation, 'changed_sources', return_value=(
                     {}, {'sirix-core': {SOURCE: [11]}})), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=None):
                failing, text = mutation.check_dispositions(work.root,
                    [report, report.relative_to(work.root)], 'f' * 40, False,
                    [census.relative_to(work.root), census])
            self.assertTrue(failing)
            failures = [line for line in text.splitlines() if line.startswith('FAIL mutant ')]
            self.assertEqual(len(failures), 1)

    def test_check_preserves_lane_completion_for_both_disposition_kinds(self):
        """CONV-203: identical XML cannot promote failed-run evidence to accepted."""
        for kind in ('equivalent', 'killed-out-of-lane'):
            for exit_code in (1, 0):
                with self.subTest(kind=kind, exit_code=exit_code), lane() as work:
                    _, results, _ = work.run([mutant('SURVIVED', 11)], head=entry(kind),
                                              exit_code=exit_code)
                    report = work.output / 'sirix-core/report/mutations.xml'
                    args = ['pr_mutation.py', 'dispositions', '--root', str(work.root),
                            '--report', str(report), '--check', '--base', 'f' * 40]
                    with patch.object(sys, 'argv', args), \
                         patch.object(mutation, 'changed_sources', return_value=(
                             {}, {'sirix-core': {SOURCE: [11]}})), \
                         patch.object(mutation, 'current_commit', return_value='e' * 40), \
                         patch.object(mutation, 'merge_base_file', return_value=None), \
                         patch.object(sys, 'stdout', new_callable=io.StringIO) as out:
                        self.assertEqual(mutation.main(), int(exit_code != 0))
                    expected = 'not judged' if exit_code else 'accepted'
                    self.assertEqual(states(results), [expected])
                    self.assertIn('entry 1 [' + expected + ']', out.getvalue())
                    if exit_code:
                        self.assertNotIn('[accepted]', out.getvalue())

    def test_check_and_lane_withhold_acceptance_when_census_validation_failed(self):
        """CONV-203: a final analysis error cannot be lost behind a successful PIT exit."""
        with lane() as work:
            _, results, _ = work.run([mutant('SURVIVED', 11)], head=entry(), census=['{'])
            self.assertEqual(states(results), ['not judged'])
            report = work.output / 'sirix-core/report/mutations.xml'
            census = work.output / 'sirix-core/census.jsonl'
            with patch.object(mutation, 'changed_sources', return_value=(
                    {}, {'sirix-core': {SOURCE: [11]}})), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=None):
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40,
                                                            False, [census])
            self.assertTrue(failing)
            self.assertIn('entry 1 [not judged]', text)
            self.assertNotIn('[accepted]', text)

    def test_check_keeps_each_modules_completion_with_its_own_report(self):
        """CONV-203: one completed module cannot authorize another module's survivor."""
        with lane() as work:
            (work.root / mutation.DISPOSITIONS_FILE).write_text(
                entry() + entry(**{'class': 'a.b.Baz', 'method': 'go', 'descriptor': '()V'}))
            complete = lane_report(work.root, [mutant('SURVIVED', 11)])
            failed = work.output / 'sirix-query/report/mutations.xml'
            failed.parent.mkdir(parents=True)
            failed.write_text('<mutations>' + mutant('SURVIVED', 5, klass='a.b.Baz',
                source='Baz.java', method='go', descriptor='()V') + '</mutations>')
            manifest = work.output / 'results.json'
            document = json.loads(manifest.read_text())
            document['modules'].append({'module': 'sirix-query', 'state': 'error',
                                        'mutants_per_class': None, 'targets': ['a.b.Baz']})
            manifest.write_text(json.dumps(document))
            for reports in ([complete, failed], [failed, complete]):
                with self.subTest(reports=reports), \
                     patch.object(mutation, 'changed_sources', return_value=(
                         {}, {'sirix-core': {SOURCE: [11], BAZ: [5]}})), \
                     patch.object(mutation, 'current_commit', return_value='e' * 40), \
                     patch.object(mutation, 'merge_base_file', return_value=None):
                    failing, text = mutation.check_dispositions(work.root, reports, 'f' * 40, False)
                self.assertTrue(failing)
                self.assertIn('ok entry 1 [accepted] a.Foo.run', text)
                self.assertIn('ok entry 2 [not judged] a.b.Baz.go', text)

    def test_check_withholds_staleness_without_completed_unsampled_evidence(self):
        """CONV-202/203: only completed, unsampled generated sets prove absence."""
        for state, limit, partial, expected in (
                ('complete', None, False, 'stale'), ('error', None, False, 'not judged'),
                ('timed out', None, False, 'not judged'), ('complete', 3, False, 'not judged'),
                ('complete', None, True, 'not judged')):
            with self.subTest(state=state, limit=limit, partial=partial), lane() as work:
                old = entry(indexes=[42])
                (work.root / mutation.DISPOSITIONS_FILE).write_text(old)
                report = lane_report(work.root, [mutant('KILLED', 11)], state, limit, partial)
                with patch.object(mutation, 'changed_sources', return_value=(
                        {}, {'sirix-core': {SOURCE: [5]}})), \
                     patch.object(mutation, 'current_commit', return_value='e' * 40), \
                     patch.object(mutation, 'merge_base_file', return_value=old):
                    _, text = mutation.check_dispositions(work.root, [report], 'f' * 40, False)
                self.assertIn('entry 1 [' + expected + ']', text)
                if expected != 'stale':
                    self.assertNotIn('[stale]', text)

    def test_check_unknown_completion_never_authorizes_and_keeps_detected_status_rules(self):
        """CONV-203: absent or invalid completion evidence is not proof of success."""
        for metadata in ('standalone', 'missing', 'malformed', 'no module', 'duplicate'):
            for kind, status, expected in (
                    ('equivalent', 'SURVIVED', 'not judged'),
                    ('killed-out-of-lane', 'SURVIVED', 'not judged'),
                    ('equivalent', 'KILLED', 'contradicted'),
                    ('killed-out-of-lane', 'KILLED', 'superseded')):
                with self.subTest(metadata=metadata, kind=kind, status=status), lane() as work:
                    (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(kind))
                    report = lane_report(work.root, [mutant(status, 11)])
                    manifest = work.output / 'results.json'
                    if metadata == 'standalone':
                        report.rename(work.root / 'mutations.xml')
                        report = work.root / 'mutations.xml'
                    elif metadata == 'missing':
                        manifest.unlink()
                    elif metadata == 'malformed':
                        manifest.write_text('{')
                    elif metadata == 'no module':
                        manifest.write_text('{"modules": []}')
                    else:
                        data = json.loads(manifest.read_text())
                        data['modules'].append(dict(data['modules'][0]))
                        manifest.write_text(json.dumps(data))
                    with patch.object(mutation, 'changed_sources', return_value=(
                            {}, {'sirix-core': {SOURCE: [11]}})), \
                         patch.object(mutation, 'current_commit', return_value='e' * 40), \
                         patch.object(mutation, 'merge_base_file', return_value=None):
                        failing, text = mutation.check_dispositions(
                            work.root, [report], 'f' * 40, False)
                    self.assertTrue(failing)
                    self.assertIn('entry 1 [' + expected + ']', text)
                    self.assertIn('completion', text)

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
            report = self.report(work.root, [mutant('SURVIVED', 11)], restricted=True)
            lines = {'sirix-core': {SOURCE: [11]}}
            with patch.object(mutation, 'changed_sources', return_value=({}, lines)), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=entry()):
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40, False)
            self.assertTrue(failing)
            self.assertIn('FAIL entry 1 [needs re-confirmation]', text)
            self.assertIn('FAIL entry 2 [stale]', text)
            self.assertIn('--remap', text)
            (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(indexes=[42]))
            lines = {'sirix-core': {SOURCE: [5]}}  # no method changed
            with patch.object(mutation, 'changed_sources', return_value=({}, lines)), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=entry(indexes=[42])):
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40, False)
                self.assertFalse(failing)
                self.assertIn('[not judged]', text)
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40, True)
                self.assertIn('[not judged]', text)
                census = work.output / 'sirix-core/census.jsonl'
                census.write_text(census_record(21, indexes=(99,)) + '\n')
                failing, text = mutation.check_dispositions(
                    work.root, [report], 'f' * 40, False, [census])
                self.assertTrue(failing)
                self.assertIn('[stale] a.Foo.run: PIT no longer generates', text)
                failing, text = mutation.check_dispositions(
                    work.root, [report], 'f' * 40, True, [census])
                self.assertFalse(failing)
                self.assertIn('[not judged]', text)

    def test_check_reaches_the_lane_states_when_it_is_given_the_lane_census(self):
        # CONV-203: a local judgement of a lane report must reach the state the lane reached, so
        # the helper reads the same census the lane's verdict read (CONV-205).
        with lane() as work:
            (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(indexes=[42]))
            report = self.report(work.root, [mutant('SURVIVED', 11)], restricted=True)
            census = work.output / 'sirix-core' / mutation.CENSUS_FILE
            census.write_text(census_record(21, indexes=(42,)) + '\n')
            lines = {'sirix-core': {SOURCE: [5]}}  # no method of the entry changed
            with patch.object(mutation, 'changed_sources', return_value=({}, lines)), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=entry(indexes=[42])):
                without, text = mutation.check_dispositions(work.root, [report], 'f' * 40, False)
                self.assertFalse(without)
                self.assertIn('[not judged]', text)
                self.assertNotIn('[stale]', text)
                failing, text = mutation.check_dispositions(work.root, [report], 'f' * 40, False,
                                                            [str(census)])
            self.assertFalse(failing)
            self.assertIn('ok entry 1 [not judged]', text)
            self.assertIn('removed off the diff', text)
            self.assertIn('omit -PprMutationChangedLinesFile', text)

    def test_check_rejects_invalid_census_evidence_and_withholds_completeness(self):
        """CONV-203: the CLI must reject every census defect the lane rejects."""
        invalid = [('malformed', b'{"class":', 'line 1:'),
                   ('unreadable', None, 'Is a directory'),
                   ('changed-line', (census_record(11, indexes=(9,)) + '\n').encode(),
                    'a.Foo:11'),
                   ('torn-utf8', b'{"description":"\xc3', 'utf-8')]
        for kind, contents, reason in invalid:
            for has_entry in (False, True):
                with self.subTest(kind=kind, has_entry=has_entry), lane() as work:
                    old = entry(method='other', descriptor='()V', indexes=[42])
                    if has_entry:
                        (work.root / mutation.DISPOSITIONS_FILE).write_text(old)
                    report = self.report(work.root, [mutant('KILLED', 11)])
                    valid = work.root / 'valid.jsonl'
                    valid.write_text(census_record(21, indexes=(8,)) + '\n')
                    broken = work.root / 'broken.jsonl'
                    if contents is None:
                        broken.mkdir()
                    else:
                        broken.write_bytes(contents)
                    args = ['pr_mutation.py', 'dispositions', '--root', str(work.root),
                            '--report', str(report), '--check', '--base', 'f' * 40,
                            '--census', str(valid), '--census', str(broken)]
                    with patch.object(sys, 'argv', args), \
                         patch.object(mutation, 'changed_sources', return_value=(
                             {}, {'sirix-core': {SOURCE: [11]}})), \
                         patch.object(mutation, 'current_commit', return_value='e' * 40), \
                         patch.object(mutation, 'merge_base_file',
                                      return_value=old if has_entry else None), \
                         patch.object(sys, 'stdout', new_callable=io.StringIO) as out:
                        self.assertEqual(mutation.main(), 1)
                    text = out.getvalue()
                    self.assertIn('broken.jsonl', text)
                    self.assertIn(reason, text)
                    if has_entry:
                        self.assertIn('ok entry 1 [not judged]', text)
                        self.assertNotIn('[stale]', text)

    def test_check_includes_classes_seen_only_in_the_census(self):
        """CONV-202/203: the complete generated set is report plus valid census."""
        with lane() as work:
            old = entry(indexes=[42])
            (work.root / mutation.DISPOSITIONS_FILE).write_text(old)
            report = self.report(work.root, [mutant('KILLED', 11, klass='a.Bar', source='Bar.java')])
            census = work.output / 'sirix-core' / mutation.CENSUS_FILE
            census.write_text(census_record(21) + '\n')
            with patch.object(mutation, 'changed_sources', return_value=(
                    {}, {'sirix-core': {SOURCE: [5]}})), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=old):
                failing, text = mutation.check_dispositions(
                    work.root, [report], 'f' * 40, False, [census])
                self.assertTrue(failing)
                self.assertIn('FAIL entry 1 [stale] a.Foo.run', text)
                sampled_failing, sampled_text = mutation.check_dispositions(
                    work.root, [report], 'f' * 40, True, [census])
                self.assertFalse(sampled_failing)
                self.assertIn('ok entry 1 [not judged]', sampled_text)

    def test_the_cli_passes_every_census_it_is_given(self):
        with lane() as work:
            (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(indexes=[42]))
            report = self.report(work.root, [mutant('SURVIVED', 11)])
            first = work.root / 'first.jsonl'
            first.write_text(census_record(21, method='elsewhere') + '\n')
            second = work.root / 'second.jsonl'
            second.write_text(census_record(21, indexes=(42,)) + '\n')
            args = ['pr_mutation.py', 'dispositions', '--root', str(work.root), '--report',
                    str(report), '--check', '--base', 'f' * 40,
                    '--census', str(first), '--census', str(second)]
            lines = {'sirix-core': {SOURCE: [5]}}
            with patch.object(sys, 'argv', args), \
                 patch.object(mutation, 'changed_sources', return_value=({}, lines)), \
                 patch.object(mutation, 'current_commit', return_value='e' * 40), \
                 patch.object(mutation, 'merge_base_file', return_value=entry(indexes=[42])), \
                 patch.object(sys, 'stdout', new_callable=io.StringIO) as out:
                self.assertEqual(mutation.main(), 0)
            self.assertIn('[not judged]', out.getvalue())

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

    def test_helper_commands_and_patch_paths_keep_shell_metacharacters_literal(self):
        """CONV-203's executable interface preserves literal argv and restores the patch."""
        with tempfile.TemporaryDirectory() as temp:
            root, patch_file = self.repository(temp)
            payload = '; touch OWNED; #'
            echo = mutation.run_test_command(
                [sys.executable, '-c', 'import sys; print(sys.argv[1])', payload], root)
            self.assertEqual((echo.returncode, echo.stdout), (0, (payload + '\n').encode()))
            literal_patch = patch_file.with_name('mutant; touch OWNED; #.patch')
            patch_file.rename(literal_patch)
            command = [sys.executable, '-c',
                       'import sys; from pathlib import Path; '
                       'assert sys.argv[1] == "; touch OWNED; #"; '
                       'sys.exit(0 if "bound = 4" in Path("Foo.java").read_text() else 7)',
                       payload]
            before = (root / 'Foo.java').read_bytes()
            evidence = mutation.counterfactual(root, literal_patch, command)
            self.assertEqual((evidence['baseline_exit'], evidence['injected_exit']), (0, 7))
            self.assertEqual((root / 'Foo.java').read_bytes(), before)
            self.assertFalse((root / 'OWNED').exists())
            self.assertFalse((Path(temp) / 'OWNED').exists())

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

    def test_path_summary_reader_selects_its_timestamp_contract_without_package_fallback(self):
        """CONV-204 named-match precedence keeps the executable VER-201 contract focused."""
        module = REPOSITORY / 'bundles/sirix-core'
        source = 'src/main/java/io/sirix/index/path/summary/PathSummaryReader.java'
        self.assertEqual(mutation.test_scope(module, [source]),
                         (['io.sirix.index.path.summary.PathSummaryReaderTest*'], [],
                          {source: 'named'}))

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


class CensusVerdictTest(unittest.TestCase):
    """CONV-205/CONV-202: what the changed-line restriction removed is evidence of existence.

    A census identity proves the mutant exists and says nothing about its status, so an entry it
    matches is neither stale nor unconfirmed and can never be accepted. The reporter also judges
    the census again from its own changed-line index: a removed mutant on a changed line fails
    the run, whatever the restriction decided.
    """

    OTHER = {'method': 'other', 'descriptor': '()V'}

    def test_an_entry_matching_only_the_census_is_reported_without_a_status(self):
        with lane() as work:
            code, results, summary = work.run(
                [mutant('KILLED', 11)], head=entry(**self.OTHER), base=entry(**self.OTHER),
                census=[census_record(21, **self.OTHER)])
            self.assertEqual((code, states(results)), (0, ['not judged']))
            action = results['dispositions']['entries'][0]['action']
            self.assertIn('removed off the diff', action)
            self.assertIn('Foo.java:21', action)
            self.assertIn('no status was measured', action)
            self.assertIn('Reported entries (not failing)', summary)

    def test_a_new_entry_matching_only_the_census_is_not_judged_rather_than_unconfirmed(self):
        # CONV-202 rule 3: the mutant exists, which is what lets an author re-identify an entry
        # after a refactor without the lane running a single off-diff mutant.
        with lane() as work:
            code, results, _ = work.run([mutant('KILLED', 11)], head=entry(**self.OTHER),
                                        census=[census_record(21, **self.OTHER)])
            self.assertEqual((code, states(results)), (0, ['not judged']))

    def test_the_same_entry_is_stale_when_the_census_does_not_list_its_mutant(self):
        with lane() as work:
            code, results, _ = work.run([mutant('KILLED', 11)], head=entry(**self.OTHER),
                                        base=entry(**self.OTHER),
                                        census=[census_record(21, method='third')])
            self.assertEqual((code, states(results)), (1, ['stale']))

    def test_a_census_identity_accepts_no_changed_line_mutant(self):
        # Rule 1 accepts a changed-line survivor only through an added entry that matches a
        # reported SURVIVED mutant. An entry whose only evidence is a census identity has no
        # measured status, so the survivor it does not match stays failing.
        with lane() as work:
            code, results, _ = work.run([mutant('SURVIVED', 11)], head=entry(**self.OTHER),
                                        census=[census_record(21, **self.OTHER)])
            failing = results['modules'][0]['changed']['failing']
            self.assertEqual((code, states(results)), (1, ['not judged']))
            self.assertEqual(len(failing), 1)
            self.assertEqual(failing[0]['reason'], 'no disposition entry')

    def test_a_removed_mutant_on_a_changed_line_fails_the_run(self):
        with lane() as work:
            code, results, summary = work.run([mutant('KILLED', 11)],
                                              census=[census_record(11, **self.OTHER)])
            census = results['modules'][0]['census']
            self.assertEqual(code, 1)
            self.assertEqual(census['count'], 1)
            self.assertEqual(len(census['violations']), 1)
            self.assertIn('a.Foo:11', census['violations'][0])
            self.assertIn('is on a changed line', census['violations'][0])
            self.assertIn('removed mutants on changed lines', summary)
            # Each violation is listed, not only counted.
            self.assertIn(f"- {mutation.escape(census['violations'][0])}", summary)

    def test_a_census_count_is_reported_but_never_as_a_score(self):
        with lane() as work:
            code, results, summary = work.run([mutant('KILLED', 11)],
                                              census=[census_record(21, **self.OTHER),
                                                      census_record(22, method='third')])
            module = results['modules'][0]
            self.assertEqual((code, module['census']['count'], module['total']), (0, 2, 1))
            self.assertIn('Mutants removed off the diff: 2.', summary)
            self.assertIn('carry no status and no score', summary)
            self.assertIn('Killed: 1/1 (100.0%)', summary)

    def test_a_censused_mutant_of_a_nested_package_is_attributed_to_its_own_source(self):
        # The package of `a.b.Baz` is `a.b`, not `a`: the changed-line index is keyed that way,
        # and a census entry on a changed line of that source must fail the run.
        with lane() as work:
            code, results, _ = work.run([mutant('KILLED', 11)], baz_changed=(5,),
                                        census=[census_record(5, klass='a.b.Baz',
                                                              source='Baz.java', method='go',
                                                              descriptor='()V')])
            census = results['modules'][0]['census']
            self.assertEqual(code, 1)
            self.assertEqual(census['violations'],
                             [f'a.b.Baz:5 ({MUTATOR}) is on a changed line'])

    def test_a_censused_mutant_of_an_unlisted_source_is_no_violation(self):
        # The lane only ever censuses mutants of classes it mutated, but a source the map does
        # not list has no changed line at all, so it cannot be a violation either.
        with lane() as work:
            code, results, _ = work.run([mutant('KILLED', 11)],
                                        census=[census_record(11, klass='z.Other',
                                                              source='Other.java')])
            census = results['modules'][0]['census']
            self.assertEqual((code, census['count'], census['violations']), (0, 1, []))

    def test_a_census_the_reporter_read_completely_bounds_only_what_it_prints(self):
        with lane() as work:
            exactly = ['{"class":'] * mutation.CENSUS_REASON_LIMIT
            code, results, summary = work.run([mutant('KILLED', 11)], census=exactly)
            self.assertEqual((code, results['modules'][0]['census']['malformed']),
                             (1, len(exactly)))
            self.assertNotIn('more; see', summary)

    def test_a_census_record_the_reporter_cannot_read_fails_a_complete_run(self):
        with lane() as work:
            code, results, summary = work.run(
                [mutant('KILLED', 11)], census=[census_record(21, **self.OTHER), '{"class":'])
            module = results['modules'][0]
            self.assertEqual((code, module['state'], module['census']['malformed']),
                             (1, 'error', 1))
            self.assertIn('malformed census record', summary)

    def test_the_lane_summary_names_each_module_and_its_state(self):
        with lane() as work:
            _, results, summary = work.run([mutant('KILLED', 11)])
            module = results['modules'][0]
            self.assertIn(f"## {module['module']}: {module['state']}", summary)

    def test_a_record_two_interceptor_passes_both_wrote_is_counted_once(self):
        # PIT may build its pipeline for a pre-scan pass and again for the analysis pass, and the
        # census appends rather than truncating, so the reporter is what deduplicates - by
        # identity for the count, and by message for a violation it would otherwise list twice.
        with lane() as work:
            code, results, summary = work.run(
                [mutant('KILLED', 11)],
                census=[census_record(21), census_record(21), census_record(22, indexes=(8,)),
                        census_record(11, indexes=(9,)), census_record(11, indexes=(9,))])
            census = results['modules'][0]['census']
            self.assertEqual((code, census['count']), (1, 3))
            self.assertEqual(census['violations'],
                             [f'a.Foo:11 ({MUTATOR}) is on a changed line'])
            self.assertIn('Mutants removed off the diff: 3.', summary)

    def test_a_record_whose_fields_are_not_the_identity_types_is_malformed(self):
        # A near-miss identity would match nothing and read as staleness, so the reporter rejects
        # the record instead of deriving an identity from it - and says which field it refused,
        # because the restriction wrote the record and a defect in it has to be findable.
        wrong = [(json.dumps({**json.loads(census_record(21)), 'indexes': ['7']}),
                  'census `indexes` must be a list of integers'),
                 (json.dumps({**json.loads(census_record(21)), 'indexes': [True]}),
                  'census `indexes` must be a list of integers'),
                 (json.dumps({**json.loads(census_record(21)), 'lineNumber': '21'}),
                  'census `lineNumber` must be an integer'),
                 (json.dumps({**json.loads(census_record(21)), 'lineNumber': True}),
                  'census `lineNumber` must be an integer'),
                 (json.dumps({**json.loads(census_record(21)), 'class': 7}),
                  'census `class` must be a string'),
                 (json.dumps({**json.loads(census_record(21)), 'description': 7}),
                  'census `description` must be a string'),
                 (json.dumps({key: value for key, value in json.loads(census_record(21)).items()
                              if key != 'mutator'}),
                  "census record has no 'mutator' field"),
                 ('{"class":', 'Expecting value')]
        for record, because in wrong:
            with self.subTest(record=record), lane() as work:
                code, results, summary = work.run([mutant('KILLED', 11)], census=[record])
                module = results['modules'][0]
                self.assertEqual((code, module['state'], module['census']['malformed']),
                                 (1, 'error', 1))
                self.assertEqual(module['census']['count'], 0)
                unreadable = module['census']['unreadable']
                self.assertEqual(len(unreadable), 1)
                self.assertTrue(unreadable[0].startswith(f'line 1: {because}'), unreadable[0])
                # The census section lists each refused record on its own line, and the summary
                # escapes file-derived text as CONV-202 requires of all of it.
                self.assertIn(f'- {mutation.escape(unreadable[0])}', summary)
                self.assertIn(because, module['error'])

    def test_the_reported_reasons_are_bounded_but_the_count_is_not(self):
        with lane() as work:
            broken = ['{"class":'] * (mutation.CENSUS_REASON_LIMIT + 3)
            code, results, summary = work.run([mutant('KILLED', 11)], census=broken)
            census = results['modules'][0]['census']
            self.assertEqual((code, census['malformed']), (1, len(broken)))
            self.assertEqual(len(census['unreadable']), mutation.CENSUS_REASON_LIMIT)
            self.assertIn('and 3 more', summary)

    def test_the_summary_points_at_the_census_file_it_read(self):
        with lane() as work:
            _, results, summary = work.run([mutant('KILLED', 11)], census=[census_record(21)])
            self.assertTrue(results['modules'][0]['census']['file'].endswith(
                f'sirix-core/{mutation.CENSUS_FILE}'))
            self.assertIn(f'Identities: `{mutation.CENSUS_FILE}`.', summary)

    def test_an_absent_census_file_is_an_empty_census(self):
        with lane() as work:
            code, results, _ = work.run([mutant('KILLED', 11)])
            self.assertEqual((code, results['modules'][0]['census']['count']), (0, 0))


class EmptyScopeTest(unittest.TestCase):
    """CONV-205: no mutant on a changed line is a measurement when the census explains it.

    PIT generated mutants for the target classes and the restriction removed all of them: the
    lane judged nothing and claims nothing, which is what a diff of comments, declarations or
    lines PIT does not mutate always produced. An empty report the census cannot explain is a
    broken run and stays incomplete.
    """

    def test_report_failures_cannot_be_resolved_by_an_off_diff_census(self):
        """CONV-202/205: removed mutants cannot prove unreadable results empty."""
        for report in (b'<mutation', b'\xff'):
            with self.subTest(report=report), lane() as work:
                code, results, summary = work.run([], report_bytes=report,
                                                  census=[census_record(21)])
                module = results['modules'][0]
                self.assertEqual((code, module['state']), (1, 'error'))
                self.assertIn('cannot read mutations.xml', module['error'])
                self.assertIn(mutation.escape(module['error']), summary)
                self.assertNotIn('off_diff_only', module)

    def test_verified_empty_report_forms_can_resolve_an_off_diff_scope(self):
        """CONV-205: absent, zero-byte and well-formed empty XML are empty evidence."""
        for report in (None, b'', b'<mutations/>'):
            with self.subTest(report=report), lane() as work:
                code, results, _ = work.run([], report=False, report_bytes=report,
                                            census=[census_record(21)])
                module = results['modules'][0]
                self.assertEqual((code, module['state'], module['total']), (0, 'complete', 0))
                self.assertTrue(module['off_diff_only'])

    def test_a_scope_the_census_explains_is_complete_and_clean(self):
        with lane() as work:
            code, results, summary = work.run([], census=[census_record(21),
                                                          census_record(22, method='other',
                                                                        descriptor='()V')])
            module = results['modules'][0]
            self.assertEqual((code, module['state'], module['off_diff_only']),
                             (0, 'complete', True))
            self.assertEqual((module['total'], module['changed']['total']), (0, 0))
            self.assertIn('No mutant of this module sits on a changed line', summary)
            self.assertIn('generated 2 mutant(s)', summary)
            self.assertIn('Changed lines: 1.', summary)

    def test_an_explained_empty_scope_does_not_excuse_a_later_module(self):
        # The verdict skips such a module and keeps going: the run is as clean as its worst
        # module, whatever order they come in.
        results = [{'module': 'a', 'state': 'complete', 'off_diff_only': True, 'total': 0,
                    'census': {'count': 2, 'violations': []}, 'changed': {'failing': []}},
                   {'module': 'b', 'state': 'complete', 'total': 1, 'unattributed': 0,
                    'census': {'count': 0, 'violations': []},
                    'changed': {'failing': [{'mutant': 'SURVIVED | a.Foo', 'reason': 'no entry'}]}}]

        self.assertFalse(mutation.is_clean(results))
        self.assertFalse(mutation.is_clean(list(reversed(results))))
        self.assertTrue(mutation.is_clean(results[:1]))

    def test_an_empty_report_the_census_cannot_explain_is_still_incomplete(self):
        with lane() as work:
            code, results, _ = work.run([])
            module = results['modules'][0]
            self.assertEqual((code, module['state']), (1, 'error'))
            self.assertNotIn('off_diff_only', module)
            self.assertIn(mutation.NO_MUTANTS, module['error'])

    def test_a_failed_run_is_never_read_as_an_empty_scope(self):
        with lane() as work:
            code, results, _ = work.run([], exit_code=1, census=[census_record(21)])
            self.assertEqual((code, results['modules'][0]['state']), (1, 'error'))

    def test_a_malformed_census_is_never_read_as_an_empty_scope(self):
        with lane() as work:
            code, results, _ = work.run([], census=['{"class":'])
            module = results['modules'][0]
            self.assertEqual((code, module['state']), (1, 'error'))
            self.assertNotIn('off_diff_only', module)

    def test_an_explained_empty_scope_still_judges_the_dispositions_file(self):
        # The entry names a method the compiled class does not declare, which is stale from the
        # build alone: judging entries does not depend on this run having analysed a mutant.
        with lane() as work:
            code, results, summary = work.run([], head=entry(method='gone', descriptor='()V'),
                                              census=[census_record(21)])
            self.assertEqual((code, states(results)), (1, ['stale']))
            self.assertIn('no longer exists in a.Foo', summary)


class PartialResultTest(unittest.TestCase):
    """CONV-202: a module its deadline killed reports every status recorded before the kill.

    PIT's XML listener serialises at the end of a run, so a killed partition left a zero-byte
    report and discarded finished mutants (PR 1353: 810 s of 975 s). The STREAM listener flushes
    each analysed class, and a preserved status is evidence only - never a clean run.
    """

    def stream(self, status, line, **fields):
        return mutant(status, line, **fields)

    def test_a_killed_module_reports_what_its_stream_recorded_and_stays_incomplete(self):
        with lane() as work:
            code, results, summary = work.run(
                [], report=False, timed_out=True,
                stream=[self.stream('KILLED', 11), self.stream('SURVIVED', 21, method='other',
                                                               descriptor='()V')])
            module = results['modules'][0]
            self.assertEqual((code, module['state'], module.get('partial')),
                             (1, 'timed out', True))
            self.assertEqual((module['total'], module['killed']), (2, 1))
            self.assertEqual(module['changed']['killed'], 1)
            self.assertIn('Partial results', summary)
            self.assertIn('never clean', summary)

    def test_preserved_statuses_judge_entries_but_cannot_accept_a_changed_line_mutant(self):
        with lane() as work:
            code, results, _ = work.run([], report=False, timed_out=True, head=entry(),
                                        stream=[self.stream('KILLED', 11)])
            # An `equivalent` entry contradicted by a preserved KILLED status still fails.
            self.assertEqual((code, states(results)), (1, ['contradicted']))

    def test_preserved_survivors_never_authorize_either_disposition_kind(self):
        """CONV-202: unfinished-run statuses cannot satisfy rule 1 or disposition counts."""
        runs = [('deadline-stream', False, True, 0),
                ('deadline-xml', True, True, 0),
                ('failed-stream', False, False, 1),
                ('failed-xml', True, False, 1)]
        for kind in ('equivalent', 'killed-out-of-lane'):
            for name, xml_report, timed_out, exit_code in runs:
                for reconfirmation in (False, True):
                    with self.subTest(kind=kind, run=name, reconfirmation=reconfirmation), lane() as work:
                        old = entry(kind, checked_at='b' * 40) if reconfirmation else None
                        survivor = self.stream('SURVIVED', 11)
                        code, results, summary = work.run(
                            [survivor] if xml_report else [], head=entry(kind), base=old,
                            report=xml_report, timed_out=timed_out, exit_code=exit_code,
                            stream=[] if xml_report else [survivor])
                        self.assertEqual((code, states(results)), (1, ['not judged']))
                        module = results['modules'][0]
                        self.assertEqual(module['state'], 'timed out' if timed_out else 'error')
                        self.assertEqual((module['total'], module['changed']['survived']), (1, 1))
                        changed = module['changed']
                        self.assertEqual(changed['dispositioned_equivalent'], [])
                        self.assertEqual(changed['dispositioned_out_of_lane'], [])
                        self.assertEqual(len(changed['failing']), 1)
                        self.assertIn('unfinished run', changed['failing'][0]['reason'])
                        record = results['dispositions']['entries'][0]
                        self.assertFalse(record['failing'])
                        self.assertIn('SURVIVED', record['action'])
                        self.assertIn('unfinished run', record['action'])
                        self.assertIn('state not judged', summary)
                        self.assertIn('Dispositioned as equivalent: 0.', summary)
                        self.assertIn('Dispositioned as killed out of lane: 0.', summary)

    def test_completed_report_survivors_still_authorize_both_disposition_kinds(self):
        """CONV-202: a completed report remains sufficient evidence for an added entry."""
        for kind in ('equivalent', 'killed-out-of-lane'):
            with self.subTest(kind=kind), lane() as work:
                code, results, _ = work.run([mutant('SURVIVED', 11)], head=entry(kind))
                self.assertEqual((code, states(results)), (0, ['accepted']))
                module = results['modules'][0]
                self.assertEqual(module['state'], 'complete')
                self.assertEqual(len(module['changed']['dispositioned_equivalent']),
                                 1 if kind == 'equivalent' else 0)
                self.assertEqual(len(module['changed']['dispositioned_out_of_lane']),
                                 1 if kind == 'killed-out-of-lane' else 0)
                self.assertEqual(module['changed']['failing'], [])

    def test_preserved_detected_statuses_still_apply_rule_four(self):
        """CONV-202: incomplete provenance cannot hide contradiction or supersession."""
        cases = [('equivalent', 'KILLED', 'contradicted'),
                 ('equivalent', 'TIMED_OUT', 'detected-other'),
                 ('equivalent', 'MEMORY_ERROR', 'detected-other'),
                 ('equivalent', 'RUN_ERROR', 'detected-other'),
                 ('killed-out-of-lane', 'KILLED', 'superseded'),
                 ('killed-out-of-lane', 'TIMED_OUT', 'superseded'),
                 ('killed-out-of-lane', 'MEMORY_ERROR', 'superseded'),
                 ('killed-out-of-lane', 'RUN_ERROR', 'superseded')]
        for kind, status, expected in cases:
            for xml_report in (False, True):
                with self.subTest(kind=kind, status=status, xml=xml_report), lane() as work:
                    recorded = self.stream(status, 11, killing='a.FooTest.detects')
                    code, results, summary = work.run(
                        [recorded] if xml_report else [], head=entry(kind), report=xml_report,
                        timed_out=True, stream=[] if xml_report else [recorded])
                    self.assertEqual((code, states(results)), (1, [expected]))
                    changed = results['modules'][0]['changed']
                    self.assertEqual(changed['dispositioned_equivalent'], [])
                    self.assertEqual(changed['dispositioned_out_of_lane'], [])
                    self.assertEqual(changed['detected'], 1)
                    self.assertIn(expected, summary)

    def test_a_record_the_kill_tore_is_dropped_rather_than_guessed_at(self):
        with lane() as work:
            torn = self.stream('SURVIVED', 21)[:40]
            code, results, _ = work.run([], report=False, timed_out=True,
                                        stream=[self.stream('KILLED', 11), torn])
            module = results['modules'][0]
            self.assertEqual((code, module['total'], module['killed']), (1, 1, 1))

    def test_a_torn_utf8_stream_tail_preserves_finished_statuses(self):
        """CONV-202: kill during a UTF-8 write must preserve the complete prefix."""
        prefix = (self.stream('KILLED', 11) + '\n').encode()
        tail = self.stream('SURVIVED', 21, description='torn é').encode().split(b'\xc3')[0] + b'\xc3'
        with lane() as work:
            code, results, _ = work.run([], report=False, timed_out=True, stream=prefix + tail)
            module = results['modules'][0]
            self.assertEqual((code, module['state'], module['total'], module['killed']),
                             (1, 'timed out', 1, 1))
            self.assertTrue(module['partial'])

    def test_an_unreadable_census_fails_without_losing_the_mutant_report(self):
        """CONV-202/205: unreadable evidence is an error with its concrete reason."""
        with lane() as work:
            code, results, summary = work.run([mutant('KILLED', 11)], census_unreadable=True)
            module = results['modules'][0]
            self.assertEqual((code, module['state'], module['total']), (1, 'error', 1))
            self.assertEqual(module['census']['malformed'], 1)
            self.assertIn('Is a directory', module['census']['unreadable'][0])
            self.assertIn('Is a directory', summary)

    def test_a_torn_utf8_census_tail_preserves_identities_and_reports_the_failure(self):
        """CONV-202/205: intact census evidence survives, unreadable evidence fails."""
        prefix = (census_record(21) + '\n').encode()
        tail = census_record(22, description='torn é').replace('\\u00e9', 'é').encode()
        tail = tail.split(b'\xc3')[0] + b'\xc3'
        for timed_out in (False, True):
            with self.subTest(timed_out=timed_out), lane() as work:
                code, results, summary = work.run([mutant('KILLED', 11)],
                                                  timed_out=timed_out, census=prefix + tail)
                module = results['modules'][0]
                self.assertEqual((code, module['census']['count'], module['census']['malformed']),
                                 (1, 1, 1))
                self.assertEqual(module['state'], 'timed out' if timed_out else 'error')
                self.assertIn('line 2:', module['census']['unreadable'][0])
                self.assertIn('utf-8', summary)

    def test_a_stream_with_no_intact_record_leaves_the_module_without_a_report(self):
        with lane() as work:
            code, results, _ = work.run([], report=False, timed_out=True, stream=['<mutation'])
            module = results['modules'][0]
            self.assertEqual((code, module['state']), (1, 'timed out'))
            self.assertNotIn('total', module)

    def test_a_killed_module_proves_no_generated_set_complete(self):
        # The class was mutated, but by a run that did not finish: an unmatched entry of it is
        # reported, never stale (CONV-202's definition of a mutated class).
        with lane() as work:
            code, results, _ = work.run([], report=False, timed_out=True,
                                        head=entry(method='other', descriptor='()V'),
                                        base=entry(method='other', descriptor='()V'),
                                        stream=[self.stream('KILLED', 11)])
            self.assertEqual((code, states(results)), (1, ['not judged']))
            self.assertIn('unfinished', results['dispositions']['entries'][0]['action'])

    def test_blank_lines_in_the_stream_and_the_census_are_ignored(self):
        with lane() as work:
            code, results, _ = work.run([], report=False, timed_out=True,
                                        stream=['', self.stream('KILLED', 11), ''],
                                        census=['', census_record(21), ''])
            module = results['modules'][0]
            self.assertEqual((code, module['total'], module['census']['count']), (1, 1, 1))
            self.assertEqual(module['census']['malformed'], 0)

    def test_a_stream_the_reporter_cannot_read_leaves_the_module_without_a_report(self):
        with lane() as work:
            code, results, _ = work.run([], report=False, timed_out=True,
                                        stream_unreadable=True)
            module = results['modules'][0]
            self.assertEqual((code, module['state']), (1, 'timed out'))
            self.assertNotIn('total', module)

    def test_an_unreadable_stream_is_reported_with_its_cause(self):
        with lane() as work:
            code, results, summary = work.run([], report=False, timed_out=True,
                                              stream_unreadable=True)
            module = results['modules'][0]
            self.assertEqual((code, module['state']), (1, 'timed out'))
            # The deadline stays the module's state; the stream failure says why nothing was
            # recovered from it, instead of being swallowed.
            self.assertEqual(module['error'], '10s module execution share exceeded')
            self.assertIn(mutation.STREAM_FILE, module['stream_error'])
            self.assertIn('Is a directory', module['stream_error'])
            self.assertIn('nothing was recovered from it', summary)

    def test_a_complete_run_with_a_malformed_report_says_what_it_could_not_read(self):
        with lane() as work:
            code, results, summary = work.run(['<mutation'])
            module = results['modules'][0]
            self.assertEqual((code, module['state']), (1, 'error'))
            self.assertIn('cannot read mutations.xml', module['error'])
            self.assertIn('not well-formed', module['error'], 'the cause, not only the file')
            self.assertIn(mutation.escape(module['error']), summary)
            self.assertNotIn('stream_error', module)

    def test_a_complete_run_with_a_malformed_report_is_an_error_not_a_stream_fallback(self):
        with lane() as work:
            code, results, _ = work.run(['<mutation'], stream=[self.stream('KILLED', 11)])
            module = results['modules'][0]
            self.assertEqual((code, module['state']), (1, 'error'))
            self.assertNotIn('total', module)


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
