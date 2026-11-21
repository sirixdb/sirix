"""Edge cases of CONV-201 to CONV-204 found by mutation-testing the reporter itself (mutmut).

Each test states a specified behaviour that an earlier test left unobserved; none asserts an
implementation detail. Fixtures and oracles are shared with test_pr_mutation_dispositions.
"""
import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from test_pr_mutation_dispositions import (EVIDENCE, JUSTIFICATION, REPOSITORY, SOURCE,
                                           SOURCE_PATH, class_file, entry, lane, mutant,
                                           mutation, states)


def errors_of(text):
    return mutation.parse_dispositions(text)[1]


class RecordMessagesTest(unittest.TestCase):
    """CONV-202: a validation error names the entry's position and the violated rule."""

    def assert_names(self, text, *fragments):
        errors = errors_of(text)
        self.assertTrue(errors, text)
        joined = '\n'.join(errors)
        for fragment in fragments:
            self.assertIn(fragment, joined)

    def test_each_field_rule_is_named(self):
        cases = [({'mutator': None}, 'missing field(s): mutator'),
                 ({'line': 11}, 'unknown field(s): line'),
                 ({'class': 'a.'}, '`class` is not an exact, well-formed value'),
                 ({'description': 'a * b'}, '`description` must be exact text'),
                 ({'indexes': ['7']}, '`indexes` must be a non-empty list'),
                 ({'checked_at': 'x'}, '`checked_at` must be a full lowercase hexadecimal'),
                 ({'justification': 'too short'}, '`justification` must argue the claim'),
                 ({'justification': JUSTIFICATION + ' TODO'}, 'contains the placeholder'),
                 ({'kind': 'flaky'}, '`kind` must be one of')]
        for fields, fragment in cases:
            with self.subTest(fields=fields):
                self.assert_names(entry(**fields), 'entry 1:', fragment)
        for field in ('class', 'method', 'descriptor', 'mutator', 'description', 'checked_at',
                      'justification'):
            with self.subTest(empty=field):
                self.assert_names(entry(**{field: ''}), f'`{field}` must be a non-empty string')
        self.assert_names(entry('killed-out-of-lane', test=''), '`test` must be a non-empty')
        self.assert_names(entry('killed-out-of-lane', test='a.T'), '`test` must be `<binary')

    def test_each_evidence_rule_is_named(self):
        cases = [('see PR', '`evidence` must be a table'),
                 ({'patch': PATCH_ONLY}, '`evidence` must have exactly the fields'),
                 ({**EVIDENCE, 'patch': 'text'}, '`evidence.patch`: patch is not a unified diff'),
                 ({**EVIDENCE, 'patch': EVIDENCE['patch'].replace('+++ b/', '+++ x/')},
                  'lacks the `b/` prefix'),
                 ({**EVIDENCE, 'command': ['']}, '`evidence.command` must be a non-empty list'),
                 ({**EVIDENCE, 'command': [1]}, '`evidence.command` must be a non-empty list'),
                 ({**EVIDENCE, 'baseline_exit': 2}, '`evidence.baseline_exit` must be 0'),
                 ({**EVIDENCE, 'injected_exit': 0}, '`evidence.injected_exit` must be a non-zero'),
                 ({**EVIDENCE, 'failure': ''}, '`failure` must be a non-empty string'),
                 ({**EVIDENCE, 'sources': []}, '`evidence.sources` must be a non-empty array'),
                 ({**EVIDENCE, 'sources': {'path': SOURCE_PATH, 'sha256': 'ab' * 32}},
                  '`evidence.sources` must be a non-empty array'),
                 ({**EVIDENCE, 'sources': [{'path': 3, 'sha256': 'ab' * 32}]},
                  'each `evidence.sources` entry needs exactly'),
                 ({**EVIDENCE, 'sources': [{'path': '../x', 'sha256': 'ab' * 32}]},
                  '`evidence.sources`: Unsafe repository path'),
                 ({**EVIDENCE, 'sources': [{'path': 'other.java', 'sha256': 'ab' * 32}]},
                  'must hash each file the patch touches exactly once')]
        for evidence, fragment in cases:
            with self.subTest(fragment=fragment):
                self.assert_names(entry('killed-out-of-lane', evidence=evidence), 'entry 1:',
                                  fragment)

    def test_every_invalid_entry_is_reported_not_only_the_first(self):
        self.assert_names(entry(kind='flaky') + entry(kind='flaky', indexes=[2]),
                          'entry 1:', 'entry 2:')
        self.assert_names(entry() + entry(checked_at='d' * 40) + entry(checked_at='e' * 40),
                          'entry 2: repeats the identity of entry 1',
                          'entry 3: repeats the identity of entry 1')

    def test_boundaries_of_indexes_and_justification(self):
        # PIT numbers instruction ordinals from 0.
        self.assertEqual(errors_of(entry(indexes=[0])), [])
        self.assertEqual(errors_of(entry(justification='x' * 60)), [])
        self.assertTrue(errors_of(entry(justification='x' * 59)))
        # Whitespace runs count as one character: 60 after normalizing, 59 after normalizing.
        self.assertEqual(errors_of(entry(justification='x' * 58 + '   \n x')), [])
        self.assertTrue(errors_of(entry(justification='x' * 57 + '   \n x')))

    def test_patch_headers_follow_unified_diff_rules(self):
        spaced = 'bundles/sirix-core/src/main/java/a/My Foo.java'
        patch_text = (f'--- a/{spaced}\t2026-11-20 10:00:00\n+++ b/{spaced}\t2026-11-20 10:00:01\n'
                      '@@ -1,2 +1,2 @@\n context\n-old\n+++ still body, not a header\n')
        evidence = {**EVIDENCE, 'patch': patch_text,
                    'sources': [{'path': spaced, 'sha256': 'ab' * 32}]}
        self.assertEqual(errors_of(entry('killed-out-of-lane', evidence=evidence)), [])


PATCH_ONLY = EVIDENCE['patch']


class AssociationMessagesTest(unittest.TestCase):
    """CONV-204: an invalid association names its position or class and the rule."""

    VALID = '[[association]]\nclass = "a.Foo"\ntests = ["b.AllocationBudgetTest"]\n'

    def test_each_rule_is_named_and_every_entry_is_reported(self):
        cases = [('[[association]]\nclass = "a.Foo"\n', 'needs exactly the fields'),
                 ('[[association]]\nclass = 3\ntests = ["b.T"]\n', '`class` must be an exact'),
                 ('[[association]]\nclass = "a.Foo"\ntests = [3]\n', '`tests` must be a non-empty'),
                 ('[[association]]\nclass = "a.Foo"\ntests = []\n', '`tests` must be a non-empty'),
                 ('[[association]]\nclass = "a.Foo"\ntests = ["b.T", "b.T"]\n',
                  '`tests` must be a non-empty list of distinct'),
                 (self.VALID + self.VALID, 'association 2: repeats the class a.Foo')]
        for text, fragment in cases:
            with self.subTest(fragment=fragment):
                errors = mutation.parse_associations(text)[1]
                self.assertIn(fragment, '\n'.join(errors))
                self.assertTrue(all(error.startswith('association ') for error in errors))
        twice = ('[[association]]\nclass = 3\ntests = ["b.T"]\n'
                 '[[association]]\nclass = "a.Foo"\ntests = []\n'
                 '[[association]]\nclass = "a.Foo"\n')
        errors = mutation.parse_associations(twice)[1]
        self.assertEqual([error.split(':')[0] for error in errors],
                         ['association 1', 'association 2', 'association 3'])
        self.assertEqual(mutation.parse_associations('# none yet\n'), ({}, []))

    def test_repository_checks_report_every_association(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            module = root / 'bundles/sirix-core'
            for path, text in {'src/main/java/a/Foo.java': 'package a;\nclass Foo {}\n',
                               'src/main/java/Plain.java': 'class Plain {}\n',
                               'src/test/java/b/AllocationBudgetTest.java':
                                   'package b;\nclass AllocationBudgetTest {}\n'}.items():
                (module / path).parent.mkdir(parents=True, exist_ok=True)
                (module / path).write_text(text)
            (root / '.github').mkdir()
            (root / mutation.ASSOCIATIONS_FILE).write_text(
                '[[association]]\nclass = "a.Gone"\ntests = ["b.AllocationBudgetTest"]\n'
                '[[association]]\nclass = "a.Foo"\ntests = ["b.MissingTest"]\n')
            associations, errors = mutation.load_associations(root)
            self.assertEqual(associations, {})
            self.assertIn('association a.Gone: no main source of a lane module declares it',
                          errors[0])
            self.assertIn('association a.Foo: b.MissingTest not declared in the tests of '
                          'sirix-core', errors[1])
            # A class in the default package is found too.
            (root / mutation.ASSOCIATIONS_FILE).write_text(
                '[[association]]\nclass = "Plain"\ntests = ["b.AllocationBudgetTest"]\n')
            self.assertEqual(mutation.load_associations(root),
                             ({'Plain': ('b.AllocationBudgetTest',)}, []))


class VerdictEdgeTest(unittest.TestCase):
    """CONV-202: states, actions and counts that earlier tests did not observe."""

    def test_reconfirmed_new_and_preexisting_entries_of_an_unmutated_class(self):
        bar = {'class': 'a.Bar'}
        with lane() as work:
            code, results, _ = work.run([mutant('KILLED', 11)], head=entry(**bar, checked_at='d' * 40),
                                        base=entry(**bar))
            self.assertEqual((code, states(results)), (0, ['not judged']))
            self.assertIn('its class was not mutated', results['dispositions']['entries'][0]['action'])
            code, results, _ = work.run([mutant('KILLED', 11)], head=entry(**bar))
            self.assertEqual((code, states(results)), (1, ['unconfirmed']))
            self.assertIn('its class was not mutated', results['dispositions']['entries'][0]['action'])
            code, results, _ = work.run([mutant('KILLED', 11)], head=entry(**bar), base=entry(**bar))
            self.assertEqual((code, states(results)), (0, ['out of scope']))
            code, results, _ = work.run([mutant('KILLED', 11)], head=entry(indexes=[99]))
            self.assertIn("not in this run's sample", results['dispositions']['entries'][0]['action'])

    def test_actions_name_what_resolves_each_state(self):
        with lane() as work:
            matched = [mutant('SURVIVED', 10)]
            _, results, _ = work.run(matched, head=entry('killed-out-of-lane'),
                                     base=entry('killed-out-of-lane'))
            action = results['dispositions']['entries'][0]['action']
            self.assertIn('re-run the counterfactual', action)
            self.assertNotIn('--remap', action)
            _, results, _ = work.run(matched, head=entry(), base=entry())
            self.assertNotIn('counterfactual', results['dispositions']['entries'][0]['action'])
            _, results, _ = work.run([mutant('SURVIVED', 11)], head=entry())
            self.assertEqual(results['dispositions']['entries'][0]['action'], 'on a changed line')

    def test_counts_and_merge_base_state(self):
        with lane() as work:
            _, results, summary = work.run([mutant('SURVIVED', 21, method='other', descriptor='()V')],
                                           head=entry(method='other', descriptor='()V'),
                                           base=entry(method='other', descriptor='()V'))
            dispositions = results['dispositions']
            self.assertEqual((dispositions['base'], dispositions['added'], dispositions['removed']),
                             ('valid', 0, 0))
            self.assertIsNone(dispositions['entries'][0]['added'])
            self.assertNotIn('### Added entries', summary)
            _, results, _ = work.run([mutant('SURVIVED', 11)], head=entry())
            self.assertEqual(results['dispositions']['base'], 'absent')
            _, results, _ = work.run([mutant('SURVIVED', 10)], head=entry(checked_at='d' * 40),
                                     base=entry())
            self.assertEqual((results['dispositions']['re_confirmed'],
                              results['dispositions']['removed']), (1, 0))

    def test_an_entry_is_compared_by_content_not_by_field_order(self):
        fields = entry(method='other', descriptor='()V').splitlines()
        reordered = '\n'.join([fields[0]] + list(reversed(fields[1:]))) + '\n'
        with lane() as work:
            code, results, _ = work.run([mutant('SURVIVED', 21, method='other', descriptor='()V')],
                                        head=entry(method='other', descriptor='()V'), base=reordered)
            self.assertEqual((code, states(results), results['dispositions']['added']),
                             (0, ['advisory'], 0))

    def test_a_method_in_a_two_segment_package_needs_re_confirmation(self):
        baz = {'class': 'a.b.Baz', 'method': 'go', 'descriptor': '()V'}
        with lane() as work:
            code, results, _ = work.run([mutant('KILLED', 11)], head=entry(**baz), base=entry(**baz),
                                        baz_changed=(5,))
            self.assertEqual((code, states(results)), (1, ['needs re-confirmation']))

    def test_failing_reasons_name_the_matching_entry_and_unknown_statuses(self):
        with lane() as work:
            _, results, _ = work.run([mutant('SURVIVED', 11)], head=entry(), base=entry())
            reason = results['modules'][0]['changed']['failing'][0]['reason']
            self.assertTrue(reason.startswith('entry 1 is needs re-confirmation: this PR changes'))
            no_status = mutant('SURVIVED', 11).replace(" status='SURVIVED'", '')
            _, results, _ = work.run([no_status])
            self.assertEqual(results['modules'][0]['changed']['failing'][0]['reason'],
                             'UNKNOWN is not a dispositionable status')

    def test_every_undetected_changed_mutant_is_judged_whatever_precedes_it(self):
        with lane() as work:
            code, results, _ = work.run([mutant('KILLED', 11, indexes=(2,)), mutant('SURVIVED', 11)])
            self.assertEqual(code, 1)
            self.assertEqual(len(results['modules'][0]['changed']['failing']), 1)

    def test_a_malformed_report_index_never_matches_a_valid_entry(self):
        with lane() as work:
            broken = mutant('SURVIVED', 11).replace('<index>7</index>', '<index>x</index>')
            for indexes in ([1], [0]):
                with self.subTest(indexes=indexes):
                    code, results, _ = work.run([broken], head=entry(indexes=indexes))
                    self.assertEqual(code, 1)
                    self.assertEqual(states(results), ['unconfirmed'])


class SummaryEdgeTest(unittest.TestCase):
    """CONV-202/203: what the job summary shows for each list."""

    def test_verdict_lines_show_counts_reasons_and_entry_details(self):
        long_argument = 'y' * mutation.SUMMARY_JUSTIFICATION_LIMIT
        mutants = [mutant('TIMED_OUT', 11, indexes=(3,)), mutant('SURVIVED', 11),
                   mutant('SURVIVED', 12, indexes=(9,)), mutant('SURVIVED', 12, indexes=(5,))]
        head = entry(justification=long_argument) + entry('killed-out-of-lane', indexes=[9])
        with lane() as work:
            _, _, summary = work.run(mutants, head=head, changed=(11, 12))
        self.assertIn('Otherwise detected: 1. Dispositioned as equivalent: 1. '
                      'Dispositioned as killed out of lane: 1. Failing: 1.', summary)
        self.assertIn('| no disposition entry', summary)
        equivalent = next(line for line in summary.splitlines()
                          if line.startswith('- SURVIVED') and '| entry 1 |' in line)
        self.assertNotIn('| test ', equivalent)
        self.assertIn(f'| checked at {"c" * 40} | {long_argument}', equivalent)
        self.assertFalse(equivalent.endswith('...'))
        out_of_lane = next(line for line in summary.splitlines() if '| entry 2 |' in line)
        self.assertIn('SURVIVED &#124; a.Foo &#124; run &#124; 12', out_of_lane)
        self.assertIn('| test a.FooAllocationTest#allocatesNothing |', out_of_lane)
        with lane() as work:
            _, _, summary = work.run([mutant('SURVIVED', 11)],
                                     head=entry(justification=long_argument + 'z'))
        line = next(line for line in summary.splitlines() if '| entry 1 |' in line)
        self.assertTrue(line.endswith(long_argument + '...'))

    def test_failing_list_is_cut_only_beyond_the_limit(self):
        with lane() as work, patch.object(mutation, 'SUMMARY_MUTANT_LIMIT', 1):
            _, _, summary = work.run([mutant('SURVIVED', 11)])
            self.assertNotIn('Remaining entries', summary)
            _, _, summary = work.run([mutant('SURVIVED', 11), mutant('SURVIVED', 11, indexes=(8,))])
            self.assertIn('Remaining entries', summary)

    def test_disposition_section_lists_added_failing_and_reported_entries(self):
        with lane() as work:
            _, _, summary = work.run([mutant('SURVIVED', 11)], head=entry(checked_at='d' * 40))
            added = summary[summary.index('### Added entries'):]
            self.assertIn(f'| new | state accepted | checked at {"d" * 40} |', added)
            _, _, summary = work.run([mutant('KILLED', 11)], head=entry())
            failing = summary[summary.index('### Entries that fail this run'):]
            self.assertIn('| contradicted: a test detects this mutant', failing)
            _, _, summary = work.run([mutant('KILLED', 11)],
                                     head=entry(method='other', descriptor='()V'),
                                     base=entry(method='other', descriptor='()V'))
            reported = summary[summary.index('### Reported entries (not failing)'):]
            self.assertIn('| not judged: no reported mutant matches', reported)
            self.assertNotIn('Entries that fail this run', summary)
            _, _, summary = work.run([mutant('SURVIVED', 11)], head=entry(kind='flaky'))
            self.assertIn("- entry 1: `kind` must be one of", summary)
            self.assertNotIn('mutation-test-associations.toml` is invalid', summary)


class RunWiringTest(unittest.TestCase):
    """The run passes associations, budgets and interim results through to what it reports."""

    def test_associated_tests_reach_the_pit_command(self):
        with lane() as work:
            budget = work.root / 'bundles/sirix-core/src/test/java/b/BudgetSuiteTest.java'
            budget.parent.mkdir(parents=True)
            budget.write_text('package b;\nclass BudgetSuiteTest {}\n')
            (work.root / mutation.ASSOCIATIONS_FILE).write_text(
                '[[association]]\nclass = "a.Foo"\ntests = ["b.BudgetSuiteTest"]\n')
            (work.output / 'selection.json').write_text(json.dumps({
                'modules': {'sirix-core': [SOURCE]},
                'changed_lines': {'sirix-core': {SOURCE: [11]}}, 'dispositions_base': None}))
            commands = []

            def execute(command, root, log, timeout):
                commands.append(command)
                return 1
            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'execute', side_effect=execute):
                mutation.run(work.root, work.output, 10, blocking=True)
            selected = next(argument for argument in commands[0]
                            if argument.startswith('-PprMutationTests='))
            self.assertIn('b.BudgetSuiteTest*', selected.split('=', 1)[1].split(','))

    def test_a_timed_out_module_reports_its_share_in_seconds(self):
        with lane() as work:
            (work.output / 'selection.json').write_text(json.dumps({
                'modules': {'sirix-core': [SOURCE]},
                'changed_lines': {'sirix-core': {SOURCE: [11]}}, 'dispositions_base': None}))

            def execute(command, root, log, timeout):
                raise subprocess.TimeoutExpired(command, timeout)
            with patch.object(mutation, 'resolve_targets', return_value=(['a.Foo'], [])), \
                 patch.object(mutation, 'test_scope', return_value=(['a.FooTest*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=execute):
                mutation.run(work.root, work.output, 10, blocking=True)
            result = json.loads((work.output / 'results.json').read_text())['modules'][0]
            self.assertEqual(result['error'], '10s module execution share exceeded')

    def test_interim_results_carry_associations_and_later_modules_are_judged(self):
        with lane() as work:
            query = work.root / 'bundles/sirix-query'
            (query / 'src/main/java/q/Q.java').parent.mkdir(parents=True)
            (query / 'src/main/java/q/Q.java').write_text('package q;\nclass Q {}\n')
            classes = query / 'build/classes/java/main/q'
            classes.mkdir(parents=True)
            (classes / 'Q.class').write_bytes(class_file('q.Q', 'Q.java', {('run', '(I)Z'): (3,)}))
            (work.output / 'selection.json').write_text(json.dumps({
                'modules': {'sirix-core': [SOURCE], 'sirix-query': ['src/main/java/q/Q.java']},
                'changed_lines': {'sirix-core': {SOURCE: [11]},
                                  'sirix-query': {'src/main/java/q/Q.java': [9]}},
                'dispositions_base': entry(**{'class': 'q.Q'})}))
            (work.root / mutation.DISPOSITIONS_FILE).write_text(entry(**{'class': 'q.Q'}))
            seen = []

            def execute(command, root, log, timeout):
                module = 'sirix-query' if ':sirix-query:' in command[1] else 'sirix-core'
                if module == 'sirix-query':
                    seen.append(json.loads((work.output / 'results.json').read_text()))
                report = work.output / module / 'report/mutations.xml'
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text('<mutations>' + mutant('KILLED', 3, klass='q.Q', source='Q.java',
                                                         indexes=(1,)) + '</mutations>')
                return 1 if module == 'sirix-core' else 0

            def targets(module_root, sources):
                return (['q.Q'] if 'sirix-query' in str(module_root) else ['a.Foo']), []
            with patch.object(mutation, 'resolve_targets', side_effect=targets), \
                 patch.object(mutation, 'test_scope', return_value=(['x.T*'], [], {})), \
                 patch.object(mutation, 'execute', side_effect=execute):
                mutation.run(work.root, work.output, 300, blocking=True)
            self.assertEqual(seen[0]['associations'],
                             {'file': mutation.ASSOCIATIONS_FILE, 'errors': [], 'count': 0})
            final = json.loads((work.output / 'results.json').read_text())
            self.assertEqual(states(final), ['not judged'])


class CommandLineEdgeTest(unittest.TestCase):
    """CLI contracts of the CI and helper commands."""

    def exit_code(self, args):
        with patch.object(sys, 'argv', ['pr_mutation.py', *args]), \
             patch.object(sys, 'stderr', new_callable=io.StringIO) as err, \
             patch.object(sys, 'stdout', new_callable=io.StringIO) as out:
            try:
                code = mutation.main()
            except SystemExit as error:
                code = error.code
        return code, out.getvalue(), err.getvalue()

    def test_required_inputs_and_documented_defaults(self):
        for args in ([], ['run'], ['select'], ['dispositions', '--check', '--base', 'f' * 40],
                     ['counterfactual', '--', 'true']):
            with self.subTest(args=args):
                self.assertEqual(self.exit_code(args)[0], 2)
        parsed = mutation.build_parser().parse_args(['run', '--output', 'x'])
        # docs/VERIFICATION.md: PIT processes share a 22-minute execution budget.
        self.assertEqual((parsed.seconds, parsed.blocking, parsed.root), (1320, False, REPOSITORY))
        self.assertIn('requires --check, --remap or both',
                      self.exit_code(['dispositions', '--report', 'r'])[2])
        self.assertIn('--check requires --base',
                      self.exit_code(['dispositions', '--report', 'r', '--check'])[2])

    def test_remap_alone_exits_zero_and_an_absent_file_has_nothing_to_remap(self):
        with lane() as work:
            report = work.root / 'mutations.xml'
            report.write_text('<mutations>' + mutant('SURVIVED', 11) + '</mutations>')
            code, out, _ = self.exit_code(['dispositions', '--root', str(work.root), '--report',
                                           str(report), '--remap'])
            self.assertEqual((code, out.strip()),
                             (0, 'Every entry matches a mutant of the given reports.'))

    def test_counterfactual_command_with_or_without_separator(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'repo'
            root.mkdir()
            (root / 'Foo.java').write_text('class Foo {\n  int bound = 4;\n}\n')
            patch_file = Path(temp) / 'mutant.patch'
            patch_file.write_text('--- a/Foo.java\n+++ b/Foo.java\n@@ -1,3 +1,3 @@\n class Foo {\n'
                                  '-  int bound = 4;\n+  int bound = 5;\n }\n')
            check = [sys.executable, '-c', 'import sys; text = open("Foo.java").read(); '
                     'sys.stderr.write("bound broken\\n"); sys.exit(0 if "bound = 4" in text else 7)']
            for separator in (['--'], []):
                with self.subTest(separator=separator):
                    code, out, _ = self.exit_code(['counterfactual', '--root', str(root), '--patch',
                                                   str(patch_file), *separator, *check])
                    self.assertEqual(code, 0)
                    self.assertTrue(out.endswith('"\n') or out.endswith("'''\n"))
                    self.assertFalse(out.endswith('\n\n'))
                    evidence = tomllib.loads(entry('killed-out-of-lane', evidence=None) + out)
                    recorded = evidence['mutant'][0]['evidence']
                    self.assertEqual(recorded['command'], check)
                    # The failure excerpt includes what the test wrote to standard error.
                    self.assertEqual(recorded['failure'], 'bound broken')

    def test_a_test_that_breaks_the_reverse_patch_yields_a_refusal(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'repo'
            root.mkdir()
            (root / 'Foo.java').write_text('class Foo {\n  int bound = 4;\n}\n')
            patch_file = Path(temp) / 'mutant.patch'
            patch_file.write_text('--- a/Foo.java\n+++ b/Foo.java\n@@ -1,3 +1,3 @@\n class Foo {\n'
                                  '-  int bound = 4;\n+  int bound = 5;\n }\n')
            rewrite = [sys.executable, '-c', 'import sys; text = open("Foo.java").read(); '
                       'open("Foo.java", "w").write(text.replace("bound = 5", "bound = 6")); '
                       'sys.exit(0 if "bound = 4" in text else 7)']
            with self.assertRaises(mutation.CounterfactualError) as error:
                mutation.counterfactual(root, patch_file, rewrite)
            self.assertIn('restore Foo.java from version control', str(error.exception))

    def test_rendered_evidence_keeps_a_multi_line_patch_readable(self):
        rendered = mutation.render_evidence(EVIDENCE)
        self.assertIn("patch = '''\n" + EVIDENCE['patch'] + "'''", rendered)
        for value in ('emoji \U0001f600', 'tab\tonly', 'quote " and \\'):
            with self.subTest(value=value):
                self.assertEqual(tomllib.loads('v = ' + mutation.toml_string(value))['v'], value)

    def test_remap_marks_and_orders_candidates_and_handles_every_entry(self):
        with lane() as work:
            (work.root / mutation.DISPOSITIONS_FILE).write_text(
                entry() + entry(method='old', indexes=[9])
                + entry(**{'class': 'a.Moved'}, method='other', descriptor='()V', indexes=[5]))
            report = work.root / 'mutations.xml'
            report.write_text('<mutations>' + mutant('SURVIVED', 11)
                              + mutant('SURVIVED', 12, method='renamed', descriptor='()V',
                                       indexes=(4,))
                              + mutant('SURVIVED', 12, method='renamed', descriptor='()V',
                                       indexes=(9,))
                              + mutant('SURVIVED', 21, method='other', descriptor='()V',
                                       indexes=(6,)) + '</mutations>')
            text = mutation.remap_dispositions(work.root, [report])
            self.assertNotIn('entry 1 ', text)
            second = text[text.index('entry 2 '):text.index('entry 3 ')]
            marked = second.splitlines()
            marker = next(index for index, line in enumerate(marked)
                          if 'same instruction indexes' in line)
            self.assertEqual(marked[marker + 5].strip(), 'indexes = [9]')
            self.assertEqual(second.count('same instruction indexes'), 1)
            third = text[text.index('entry 3 '):]
            self.assertIn('same method name and descriptor in another class', third)
            self.assertEqual([line.strip() for line in third.splitlines()
                              if line.strip().startswith('method = ')], ['method = "other"'])


@contextlib.contextmanager
def repository():
    """A real git repository holding the lane's module, with a base and a PR commit."""
    with tempfile.TemporaryDirectory() as temp, lane() as work:
        root = Path(temp)

        def git(*args):
            return subprocess.check_output(
                ['git', '-C', str(root), '-c', 'user.name=Test',
                 '-c', 'user.email=test@example.invalid', *args], shell=False).decode().strip()
        git('init', '-q', '-b', 'main')
        subprocess.check_call(['cp', '-r', str(work.root / 'bundles'), str(work.root / '.github'),
                               str(root)])
        source = root / 'bundles/sirix-core' / SOURCE
        source.write_text('package a;\n' + ''.join(f'// line {number}\n' for number in range(2, 30))
                          + 'class Foo {}\n')
        (root / mutation.DISPOSITIONS_FILE).write_text('# none yet\n')
        git('add', '-f', '.')
        git('commit', '-qm', 'base')
        base = git('rev-parse', 'HEAD')
        git('checkout', '-qb', 'pr')
        text = source.read_text().replace('// line 11\n', '// line 11 changed\n')
        source.write_text(text)
        git('commit', '-qam', 'change line 11')
        yield root, base, git


class RealRepositoryTest(unittest.TestCase):
    """`select` and `dispositions --check` against a real git history, without mocks."""

    def test_select_records_lines_and_the_merge_base_file(self):
        with repository() as (root, base, git):
            head = git('rev-parse', 'HEAD')
            with tempfile.TemporaryDirectory() as out, \
                 patch.object(sys, 'argv', ['pr_mutation.py', 'select', '--root', str(root),
                                            '--base', base, '--head', head, '--output', out]), \
                 patch.object(sys, 'stdout', new_callable=io.StringIO):
                self.assertEqual(mutation.main(), 0)
                manifest = json.loads((Path(out) / 'selection.json').read_text())
            self.assertEqual(manifest, {'modules': {'sirix-core': [SOURCE]},
                                        'changed_lines': {'sirix-core': {SOURCE: [11]}},
                                        'dispositions_base': '# none yet\n'})

    def test_check_judges_entries_against_the_real_diff(self):
        with repository() as (root, base, git):
            report = root / 'mutations.xml'
            report.write_text('<mutations>' + mutant('SURVIVED', 11)
                              + mutant('SURVIVED', 12, indexes=(9,)) + '</mutations>')
            (root / mutation.DISPOSITIONS_FILE).write_text(
                entry() + entry('killed-out-of-lane', indexes=[9]))
            failing, text = mutation.check_dispositions(root, [str(report)], base, False)
            self.assertFalse(failing, text)
            self.assertIn('ok entry 1 [accepted] a.Foo.run: on a changed line', text)
            self.assertIn('ok entry 2 [accepted] a.Foo.run: on an unchanged line', text)
            (root / mutation.DISPOSITIONS_FILE).write_text(entry(kind='flaky'))
            failing, text = mutation.check_dispositions(root, [str(report)], base, False)
            self.assertTrue(failing)
            self.assertIn('0 valid entries, 1 errors', text)
            self.assertNotIn('[accepted]', text)
            (root / mutation.DISPOSITIONS_FILE).write_text(entry())
            report.write_text('<mutations>' + mutant('SURVIVED', 11)
                              + mutant('SURVIVED', 11, indexes=(9,)) + '</mutations>')
            failing, text = mutation.check_dispositions(root, [str(report)], base, True)
            self.assertTrue(failing)  # only a failing mutant: the second survivor has no entry
            self.assertNotIn('FAIL entry', text)
            (root / mutation.DISPOSITIONS_FILE).write_text(entry() + entry(indexes=[42]))
            report.write_text('<mutations>' + mutant('SURVIVED', 11) + '</mutations>')
            failing, text = mutation.check_dispositions(root, [str(report)], base, False)
            self.assertTrue(failing)  # only a failing entry: indexes [42] is stale
            self.assertNotIn('FAIL mutant', text)


class SecondRoundTest(unittest.TestCase):
    """Behaviours the second mutmut round showed unobserved (CONV-201 to CONV-204)."""

    def test_class_reader_skips_exception_tables_and_reads_wide_constant_pools(self):
        methods = {('run', '(I)Z'): (10, 11)}
        for image in (class_file('a.Foo', 'Foo.java', methods, handlers=2),
                      class_file('a.Foo', 'Foo.java', methods, padding=33000)):
            with self.subTest(size=len(image)):
                self.assertEqual(mutation.read_class(image),
                                 ('a.Foo', 'Foo.java', {('run', '(I)Z'): frozenset((10, 11))}))

    def test_a_patch_may_delete_one_file_while_injecting_into_another(self):
        deleted = 'bundles/sirix-core/src/main/java/a/Old.java'
        patch_text = (f'--- a/{deleted}\n+++ /dev/null\n@@ -1 +0,0 @@\n-class Old {{}}\n'
                      + EVIDENCE['patch'])
        evidence = {**EVIDENCE, 'patch': patch_text}
        self.assertEqual(errors_of(entry('killed-out-of-lane', evidence=evidence)), [])
        empty = {**EVIDENCE, 'patch': ''}
        self.assertIn('`patch` must be a non-empty string',
                      '\n'.join(errors_of(entry('killed-out-of-lane', evidence=empty))))

    def test_every_invalid_association_is_reported_in_order(self):
        text = ('[[association]]\nclass = "a.Foo"\n'
                '[[association]]\nclass = 3\ntests = ["b.T"]\n'
                '[[association]]\nclass = "a.Foo"\ntests = ["b.T"]\n'
                '[[association]]\nclass = "a.Foo"\ntests = ["b.U"]\n'
                '[[association]]\nclass = "a.Bar"\ntests = []\n')
        errors = mutation.parse_associations(text)[1]
        self.assertEqual([error.split(':')[0] for error in errors],
                         ['association 1', 'association 2', 'association 4', 'association 5'])

    def test_kotlin_sources_and_classes_are_found(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            module = root / 'bundles/sirix-core'
            for path, text in {'src/main/kotlin/a/K.kt': 'package a\nclass K\n',
                               'src/test/java/b/AllocationBudgetTest.java':
                                   'package b;\nclass AllocationBudgetTest {}\n'}.items():
                (module / path).parent.mkdir(parents=True, exist_ok=True)
                (module / path).write_text(text)
            (root / '.github').mkdir()
            (root / mutation.ASSOCIATIONS_FILE).write_text(
                '[[association]]\nclass = "a.K"\ntests = ["b.AllocationBudgetTest"]\n')
            self.assertEqual(mutation.load_associations(root),
                             ({'a.K': ('b.AllocationBudgetTest',)}, []))
        with lane() as work:
            kotlin = work.root / 'bundles/sirix-core/build/classes/kotlin/main/a'
            kotlin.mkdir(parents=True)
            (kotlin / 'K.class').write_bytes(class_file('a.K', 'K.kt', {('run', '(I)Z'): (4,)}))
            code, results, _ = work.run([mutant('KILLED', 11)], head=entry(**{'class': 'a.K'}),
                                        base=entry(**{'class': 'a.K'}))
            self.assertEqual((code, states(results)), (0, ['out of scope']))

    def test_a_nested_test_class_is_found_by_its_top_level_declaration(self):
        with lane() as work:
            code, results, _ = work.run([mutant('SURVIVED', 11)], head=entry(
                'killed-out-of-lane', test='a.FooAllocationTest$Nested#allocatesNothing'))
            self.assertEqual((code, states(results)), (0, ['accepted']))

    def test_summary_labels_entries_and_lists_only_non_failing_reported_states(self):
        with lane() as work:
            _, _, summary = work.run([mutant('KILLED', 11)], head=entry())
            self.assertIn('- entry 1 [equivalent] a.Foo.run(I)Z &#124; ' + MUTATOR_TEXT, summary)
            self.assertNotIn('Reported entries', summary)
            _, _, summary = work.run([mutant('SURVIVED', 11), mutant('SURVIVED', 12, indexes=(9,))],
                                     head=entry() + entry('killed-out-of-lane', indexes=[9]),
                                     changed=(11, 12))
            added = summary[summary.index('### Added entries'):].splitlines()
            first = next(line for line in added if line.startswith('- entry 1 '))
            second = next(line for line in added if line.startswith('- entry 2 '))
            self.assertNotIn('| test', first)
            self.assertNotIn('None', first)
            self.assertIn('| test a.FooAllocationTest#allocatesNothing | new |', second)
            other = {'method': 'other', 'descriptor': '()V'}
            for status, state in (('TIMED_OUT', 'detected-other'), ('NO_COVERAGE', 'inactive'),
                                  ('SURVIVED', 'advisory')):
                with self.subTest(state=state):
                    _, results, summary = work.run([mutant(status, 21, **other)],
                                                   head=entry(**other), base=entry(**other))
                    self.assertEqual(states(results), [state])
                    reported = summary[summary.index('### Reported entries (not failing)'):]
                    self.assertIn(f'| {state}: ', reported)

    def test_results_omit_private_records_and_show_association_errors(self):
        with lane() as work:
            (work.root / mutation.ASSOCIATIONS_FILE).write_text('[[association]]\nclass = "a.Foo"\n')
            _, results, summary = work.run([mutant('SURVIVED', 11)])
            self.assertFalse([key for key in results['modules'][0] if key.startswith('_')])
            self.assertIn('- association 1: needs exactly the fields', summary)

    def test_a_malformed_manifest_base_is_named(self):
        with lane() as work:
            (work.output / 'selection.json').write_text(json.dumps(
                {'modules': {}, 'changed_lines': {}, 'dispositions_base': ['x']}))
            with self.assertRaises(ValueError) as error:
                mutation.run(work.root, work.output, 10, blocking=True)
            self.assertIn('malformed dispositions_base', str(error.exception))

    def test_check_hides_out_of_scope_entries_and_disposes_nothing_from_an_invalid_file(self):
        with lane() as work:
            report = work.root / 'mutations.xml'
            report.write_text('<mutations>' + mutant('SURVIVED', 11) + '</mutations>')
            lines = {'sirix-core': {SOURCE: [11]}}
            bar = entry(**{'class': 'a.Bar'})
            for head, base, absent in ((bar, bar, 'entry 1 ['),
                                       (entry() + entry(kind='flaky', indexes=[3]), None,
                                        '[accepted]')):
                with self.subTest(absent=absent):
                    (work.root / mutation.DISPOSITIONS_FILE).write_text(head)
                    with patch.object(mutation, 'changed_sources', return_value=({}, lines)), \
                         patch.object(mutation, 'current_commit', return_value='e' * 40), \
                         patch.object(mutation, 'merge_base_file', return_value=base):
                        _, text = mutation.check_dispositions(work.root, [str(report)], 'f' * 40,
                                                              False)
                    self.assertNotIn(absent, text)

    def test_the_failure_excerpt_keeps_every_output_line(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'repo'
            root.mkdir()
            (root / 'Foo.java').write_text('class Foo {\n  int bound = 4;\n}\n')
            patch_file = Path(temp) / 'mutant.patch'
            patch_file.write_text('--- a/Foo.java\n+++ b/Foo.java\n@@ -1,3 +1,3 @@\n class Foo {\n'
                                  '-  int bound = 4;\n+  int bound = 5;\n }\n')
            check = [sys.executable, '-c', 'import sys; text = open("Foo.java").read(); '
                     'print("first"); print("second"); sys.exit(0 if "bound = 4" in text else 7)']
            evidence = mutation.counterfactual(root, patch_file, check)
            self.assertEqual(evidence['failure'], 'first\nsecond')

    def test_check_cli_against_a_real_history(self):
        with repository() as (root, base, _):
            report = root / 'mutations.xml'
            report.write_text('<mutations>' + mutant('SURVIVED', 11) + '</mutations>')
            (root / mutation.DISPOSITIONS_FILE).write_text(entry())
            args = ['pr_mutation.py', 'dispositions', '--root', str(root), '--report', str(report),
                    '--check', '--base', base]
            with patch.object(sys, 'argv', args), \
                 patch.object(sys, 'stdout', new_callable=io.StringIO) as out:
                self.assertEqual(mutation.main(), 0)
            self.assertIn('ok entry 1 [accepted] a.Foo.run: on a changed line', out.getvalue())

    def test_a_default_package_class_reaches_its_associated_tests(self):
        with tempfile.TemporaryDirectory() as temp:
            module = Path(temp) / 'bundles/sirix-core'
            for path, text in {'src/main/java/Plain.java': 'class Plain {}\n',
                               'src/test/java/b/AllocationBudgetTest.java':
                                   'package b;\nclass AllocationBudgetTest {}\n'}.items():
                (module / path).parent.mkdir(parents=True, exist_ok=True)
                (module / path).write_text(text)
            patterns, unmapped, kinds = mutation.test_scope(
                module, ['src/main/java/Plain.java'], {'Plain': ('b.AllocationBudgetTest',)})
            self.assertEqual((patterns, unmapped), (['b.AllocationBudgetTest*'], []))


MUTATOR_TEXT = 'org.pitest.mutationtest.engine.gregor.mutators.ConditionalsBoundaryMutator'


if __name__ == '__main__':
    unittest.main()
