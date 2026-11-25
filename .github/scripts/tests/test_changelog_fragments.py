"""Behavioral contracts for changelog and lesson fragments. See changelog.d/README.md."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'changelog_fragments.py'
SPEC = importlib.util.spec_from_file_location('changelog_fragments', SCRIPT)
fragments = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fragments)

REPO_ROOT = Path(__file__).resolve().parents[3]

CHANGELOG = """# Changelog

All notable changes to SirixDB are documented in this file.

## [Unreleased]

### Added

- **One entry** — with a
  continuation line.

### Fixed

- **A fix** — one line.

## [1.0.0-beta7] — 2026-07-15

### Fixed

- **An older fix** — released already.
"""

LESSONS = """# Lessons from failures

Prose that explains the table.

| Failure / broken assumption | Apply next time | Repository evidence |
| --- | --- | --- |
| A first failure. | A first rule. | `FirstTest` |

- **An existing note.** With a
  continuation line.
"""


def fragment(front, body):
    return '---\n%s\n---\n%s' % (front, body)


def write(directory, name, text):
    path = Path(directory) / name
    path.write_text(text, encoding='utf-8')
    return path


def run_cli(root, *arguments):
    """Exercise main with the script installed in a fixture repository, independent of cwd."""
    script = root / '.github' / 'scripts' / SCRIPT.name
    with patch.object(fragments, '__file__', str(script)):
        try:
            return fragments.main(list(arguments))
        except SystemExit as error:
            return error.code


class ParseFragment(unittest.TestCase):
    """The file format: front matter, then a verbatim body."""

    def parse(self, text, name='topic.md'):
        with tempfile.TemporaryDirectory() as directory:
            write(directory, name, text)
            return fragments.load_fragments(Path(directory), fragments.CHANGELOG_SPEC)

    def test_front_matter_value_and_verbatim_body(self):
        body = '- **Entry** — text\n\n  A second paragraph.\n    Deeper indent.'
        loaded = self.parse(fragment('section: Fixed', body + '\n'))
        self.assertEqual(['Fixed'], [item.value for item in loaded])
        self.assertEqual([body], [item.body for item in loaded])

    def test_leading_blank_lines_and_trailing_whitespace_are_stripped(self):
        loaded = self.parse(fragment('section: Added', '\n\n- **Entry** — text\n  \n\n'))
        self.assertEqual(['- **Entry** — text'], [item.body for item in loaded])

    def test_missing_front_matter_is_rejected(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'front matter'):
            self.parse('- **Entry** — no front matter.\n')

    def test_unterminated_front_matter_is_rejected(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'not terminated'):
            self.parse('---\nsection: Fixed\n')

    def test_front_matter_running_into_the_body_is_rejected(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'not "key: value"'):
            self.parse('---\nsection: Fixed\n- **Entry** — text\n')

    def test_unknown_key_is_rejected(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'kind'):
            self.parse(fragment('kind: row', '- **Entry** — text\n'))

    def test_missing_required_key_is_rejected(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'section'):
            self.parse('---\n\n---\n- **Entry** — text\n')

    def test_malformed_front_matter_line_is_rejected(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'front matter'):
            self.parse(fragment('section Fixed', '- **Entry** — text\n'))

    def test_unknown_section_is_rejected(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'Repaired'):
            self.parse(fragment('section: Repaired', '- **Entry** — text\n'))

    def test_empty_body_is_rejected(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'empty'):
            self.parse(fragment('section: Fixed', '\n\n'))

    def test_digit_only_name_is_rejected(self):
        with self.assertRaisesRegex(fragments.FragmentError, '1483.md'):
            self.parse(fragment('section: Fixed', '- **Entry**\n'), name='1483.md')

    def test_name_outside_the_pattern_is_rejected(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'Topic.md'):
            self.parse(fragment('section: Fixed', '- **Entry**\n'), name='Topic.md')

    def test_readme_is_documentation_not_a_fragment(self):
        self.assertEqual([], self.parse('# Changelog fragments\n', name='README.md'))

    def test_unexpected_file_is_rejected_rather_than_skipped(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'topic.txt'):
            self.parse(fragment('section: Fixed', '- **Entry**\n'), name='topic.txt')

    def test_missing_directory_yields_no_fragments(self):
        with tempfile.TemporaryDirectory() as directory:
            absent = Path(directory) / 'changelog.d'
            self.assertEqual([], fragments.load_fragments(absent, fragments.CHANGELOG_SPEC))

    def test_fragments_are_loaded_in_file_name_order(self):
        with tempfile.TemporaryDirectory() as directory:
            write(directory, 'b-second.md', fragment('section: Fixed', '- **B**\n'))
            write(directory, 'a-first.md', fragment('section: Fixed', '- **A**\n'))
            loaded = fragments.load_fragments(Path(directory), fragments.CHANGELOG_SPEC)
        self.assertEqual(['- **A**', '- **B**'], [item.body for item in loaded])


class RenderChangelog(unittest.TestCase):
    """Entries land at the end of their Unreleased section; nothing else moves."""

    def render(self, *bodies_by_section, text=CHANGELOG):
        loaded = [fragments.Fragment(Path('%d.md' % index), section, body)
                  for index, (section, body) in enumerate(bodies_by_section)]
        return fragments.render_changelog(text, loaded)

    def test_no_fragments_leave_the_file_byte_identical(self):
        self.assertEqual(CHANGELOG, self.render())

    def test_entry_is_appended_to_its_existing_section(self):
        rendered = self.render(('Added', '- **New** — text.'))
        self.assertEqual(CHANGELOG.replace('  continuation line.\n',
                                           '  continuation line.\n- **New** — text.\n'), rendered)

    def test_entry_does_not_reach_a_released_section_of_the_same_name(self):
        rendered = self.render(('Fixed', '- **New** — text.'))
        self.assertEqual(CHANGELOG.replace('- **A fix** — one line.\n',
                                           '- **A fix** — one line.\n- **New** — text.\n'),
                         rendered)

    def test_several_entries_keep_their_file_name_order(self):
        rendered = self.render(('Fixed', '- **First**'), ('Fixed', '- **Second**'))
        self.assertIn('- **A fix** — one line.\n- **First**\n- **Second**\n', rendered)

    def test_absent_section_is_created_in_keep_a_changelog_order(self):
        rendered = self.render(('Changed', '- **New** — text.'))
        self.assertIn('  continuation line.\n\n### Changed\n\n- **New** — text.\n\n### Fixed\n',
                      rendered)

    def test_absent_section_after_every_existing_one_is_created_last(self):
        rendered = self.render(('Security', '- **New** — text.'))
        self.assertIn('- **A fix** — one line.\n\n### Security\n\n- **New** — text.\n'
                      '\n## [1.0.0-beta7]', rendered)

    def test_first_section_of_an_empty_release_is_created_under_the_heading(self):
        text = '# Changelog\n\n## [Unreleased]\n\n## [0.9.0] — 2026-01-01\n\n### Added\n\n- Old.\n'
        rendered = fragments.render_changelog(
            text, [fragments.Fragment(Path('a.md'), 'Added', '- **New**')])
        self.assertEqual('# Changelog\n\n## [Unreleased]\n\n### Added\n\n- **New**\n'
                         '\n## [0.9.0] — 2026-01-01\n\n### Added\n\n- Old.\n', rendered)

    def test_an_empty_section_keeps_a_blank_line_before_the_next_one(self):
        text = '# Changelog\n\n## [Unreleased]\n\n### Added\n\n### Fixed\n\n- A fix.\n'
        rendered = fragments.render_changelog(
            text, [fragments.Fragment(Path('a.md'), 'Added', '- **New**')])
        self.assertEqual('# Changelog\n\n## [Unreleased]\n\n### Added\n\n- **New**\n'
                         '\n### Fixed\n\n- A fix.\n', rendered)

    def test_a_section_at_the_end_of_the_file_still_takes_the_entry(self):
        text = '# Changelog\n\n## [Unreleased]\n\n### Added\n\n- An entry.\n'
        rendered = fragments.render_changelog(
            text, [fragments.Fragment(Path('a.md'), 'Added', '- **New**')])
        self.assertEqual(text + '- **New**\n', rendered)

    def test_missing_unreleased_heading_is_an_error(self):
        with self.assertRaisesRegex(fragments.FragmentError, r'\[Unreleased\]'):
            fragments.render_changelog('# Changelog\n\n## [1.0.0]\n\n### Added\n\n- Old.\n',
                                       [fragments.Fragment(Path('a.md'), 'Added', '- **New**')])

    def test_the_last_released_entry_round_trips_byte_for_byte(self):
        original = (REPO_ROOT / 'CHANGELOG.md').read_text(encoding='utf-8')
        reduced, body = cut_last_entry(original, '### Fixed')
        rendered = fragments.render_changelog(
            reduced, [fragments.Fragment(Path('round-trip.md'), 'Fixed', body)])
        self.assertEqual(original, rendered)


class RenderLessons(unittest.TestCase):
    """Rows extend the table; notes land at the end of the file."""

    def render(self, *bodies_by_kind, text=LESSONS):
        loaded = [fragments.Fragment(Path('%d.md' % index), kind, body)
                  for index, (kind, body) in enumerate(bodies_by_kind)]
        return fragments.render_lessons(text, loaded)

    def test_no_fragments_leave_the_file_byte_identical(self):
        self.assertEqual(LESSONS, self.render())

    def test_row_is_appended_to_the_table(self):
        row = '| A second failure. | A second rule. | `SecondTest` |'
        rendered = self.render(('row', row))
        self.assertEqual(LESSONS.replace('| `FirstTest` |\n', '| `FirstTest` |\n%s\n' % row),
                         rendered)

    def test_note_is_appended_at_the_end_of_the_file(self):
        note = '- **A new note.** With\n  a continuation.'
        self.assertEqual(LESSONS + '\n' + note + '\n', self.render(('note', note)))

    def test_rows_and_notes_in_one_release(self):
        row = '| Second. | Rule. | `T` |'
        rendered = self.render(('note', '- **Note.**'), ('row', row))
        self.assertEqual(LESSONS.replace('| `FirstTest` |\n', '| `FirstTest` |\n%s\n' % row)
                         + '\n- **Note.**\n', rendered)

    def test_row_body_that_is_not_a_table_row_is_rejected(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'table row'):
            self.render(('row', '- **Not a row.**'))

    def test_missing_table_is_an_error(self):
        with self.assertRaisesRegex(fragments.FragmentError, 'table'):
            fragments.render_lessons('# Lessons\n\nProse only.\n',
                                     [fragments.Fragment(Path('a.md'), 'row', '| a | b | c |')])

    def test_the_last_table_row_round_trips_byte_for_byte(self):
        original = (REPO_ROOT / 'docs' / 'LESSONS.md').read_bytes()
        lines = original.decode('utf-8').split('\n')
        header = lines.index('| Failure / broken assumption | Apply next time | Repository evidence |')
        end = next(index for index in range(header, len(lines))
                   if not lines[index].startswith('|'))
        last = end - 1
        standalone_rows = [(index, line) for index, line in enumerate(lines)
                           if index > end and line.startswith('|')]
        self.assertTrue(standalone_rows)
        reduced = '\n'.join(lines[:last] + lines[last + 1:])
        rendered = fragments.render_lessons(
            reduced, [fragments.Fragment(Path('round-trip.md'), 'row', lines[last])])
        self.assertEqual(original, rendered.encode('utf-8'))
        rendered_lines = rendered.split('\n')
        self.assertEqual(standalone_rows,
                         [(index, line) for index, line in enumerate(rendered_lines)
                          if index > end and line.startswith('|')])


class Collect(unittest.TestCase):
    """Collecting writes both targets and removes the fragments it rendered."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        (self.root / 'changelog.d').mkdir()
        (self.root / 'docs' / 'lessons.d').mkdir(parents=True)
        (self.root / 'CHANGELOG.md').write_text(CHANGELOG, encoding='utf-8')
        (self.root / 'docs' / 'LESSONS.md').write_text(LESSONS, encoding='utf-8')
        write(self.root / 'changelog.d', 'README.md', '# Fragments\n')

    def test_collect_rejects_repository_root_override_without_changing_files(self):
        path = write(self.root / 'changelog.d', 'topic.md',
                     fragment('section: Fixed', '- **New** — text.\n'))
        self.assertEqual(2, run_cli(self.root, 'collect', '--repo-root', str(self.root)))
        self.assertEqual(CHANGELOG, (self.root / 'CHANGELOG.md').read_text(encoding='utf-8'))
        self.assertEqual(LESSONS, (self.root / 'docs' / 'LESSONS.md').read_text(encoding='utf-8'))
        self.assertTrue(path.exists())

    def test_entries_are_written_and_fragments_removed(self):
        changelog = write(self.root / 'changelog.d', 'topic.md',
                          fragment('section: Fixed', '- **New** — text.\n'))
        lesson = write(self.root / 'docs' / 'lessons.d', 'topic.md',
                       fragment('kind: row', '| F. | R. | `T` |\n'))
        self.assertEqual(0, run_cli(self.root, 'collect'))
        self.assertIn('- **A fix** — one line.\n- **New** — text.\n',
                      (self.root / 'CHANGELOG.md').read_text(encoding='utf-8'))
        self.assertIn('| `FirstTest` |\n| F. | R. | `T` |\n',
                      (self.root / 'docs' / 'LESSONS.md').read_text(encoding='utf-8'))
        self.assertFalse(changelog.exists())
        self.assertFalse(lesson.exists())
        self.assertTrue((self.root / 'changelog.d' / 'README.md').exists())

    def test_nothing_to_collect_leaves_both_targets_untouched(self):
        self.assertEqual(0, run_cli(self.root, 'collect'))
        self.assertEqual(CHANGELOG, (self.root / 'CHANGELOG.md').read_text(encoding='utf-8'))
        self.assertEqual(LESSONS, (self.root / 'docs' / 'LESSONS.md').read_text(encoding='utf-8'))

    def test_one_invalid_fragment_writes_and_deletes_nothing(self):
        good = write(self.root / 'changelog.d', 'good.md',
                     fragment('section: Fixed', '- **New** — text.\n'))
        bad = write(self.root / 'docs' / 'lessons.d', 'bad.md',
                    fragment('kind: paragraph', '- **New** — text.\n'))
        self.assertEqual(2, run_cli(self.root, 'collect'))
        self.assertEqual(CHANGELOG, (self.root / 'CHANGELOG.md').read_text(encoding='utf-8'))
        self.assertEqual(LESSONS, (self.root / 'docs' / 'LESSONS.md').read_text(encoding='utf-8'))
        self.assertTrue(good.exists())
        self.assertTrue(bad.exists())

    def test_a_target_that_cannot_hold_the_entry_writes_nothing(self):
        (self.root / 'CHANGELOG.md').write_text('# Changelog\n\n## [1.0.0]\n', encoding='utf-8')
        path = write(self.root / 'changelog.d', 'topic.md',
                     fragment('section: Fixed', '- **New** — text.\n'))
        self.assertEqual(2, run_cli(self.root, 'collect'))
        self.assertEqual('# Changelog\n\n## [1.0.0]\n',
                         (self.root / 'CHANGELOG.md').read_text(encoding='utf-8'))
        self.assertTrue(path.exists())

    def test_a_missing_target_file_is_an_error(self):
        (self.root / 'CHANGELOG.md').unlink()
        write(self.root / 'changelog.d', 'topic.md',
              fragment('section: Fixed', '- **New** — text.\n'))
        self.assertEqual(2, run_cli(self.root, 'collect'))


def diff(path, hunk):
    return 'diff --git a/%s b/%s\n--- a/%s\n+++ b/%s\n%s' % (path, path, path, path, hunk)


class Guard(unittest.TestCase):
    """A pull request adds a fragment; it does not add an entry to a shared file."""

    def violations(self, text):
        return fragments.entry_additions(text)

    def test_added_changelog_bullet_is_a_violation(self):
        found = self.violations(diff('CHANGELOG.md', '@@ -20,0 +21 @@\n+- **New** — text.\n'))
        self.assertEqual(['CHANGELOG.md'], [item.path for item in found])
        self.assertEqual(['- **New** — text.'], [item.line for item in found])

    def test_added_changelog_section_heading_is_a_violation(self):
        found = self.violations(diff('CHANGELOG.md', '@@ -20,0 +21,2 @@\n+### Security\n+\n'))
        self.assertEqual(['### Security'], [item.line for item in found])

    def test_added_shared_file_headings_are_violations(self):
        for path in ('CHANGELOG.md', 'docs/LESSONS.md'):
            for heading in ('## New entry', '### New entry'):
                with self.subTest(path=path, heading=heading):
                    found = self.violations(diff(path, '@@ -20,0 +21,2 @@\n+%s\n+Prose.\n'
                                                 % heading))
                    self.assertEqual([(path, heading)], [(item.path, item.line) for item in found])

    def test_rewording_shared_file_headings_is_allowed(self):
        for path in ('CHANGELOG.md', 'docs/LESSONS.md'):
            for prefix in ('## ', '### '):
                with self.subTest(path=path, prefix=prefix):
                    found = self.violations(diff(path, '@@ -20 +20 @@\n-%sOld\n+%sNew\n'
                                                 % (prefix, prefix)))
                    self.assertEqual([], found)

    def test_added_lessons_table_row_is_a_violation(self):
        found = self.violations(diff('docs/LESSONS.md', '@@ -9,0 +10 @@\n+| F. | R. | `T` |\n'))
        self.assertEqual(['docs/LESSONS.md'], [item.path for item in found])

    def test_added_lessons_note_is_a_violation(self):
        found = self.violations(diff('docs/LESSONS.md', '@@ -20,0 +21 @@\n+- **Note.**\n'))
        self.assertEqual(['- **Note.**'], [item.line for item in found])

    def test_continuation_and_prose_additions_are_allowed(self):
        found = self.violations(diff('docs/LESSONS.md',
                                     '@@ -20,0 +21,3 @@\n+  A continuation line.\n+\n'
                                     '+Prose about the table.\n'))
        self.assertEqual([], found)

    def test_rewording_an_existing_entry_is_allowed(self):
        found = self.violations(diff('CHANGELOG.md',
                                     '@@ -21 +21 @@\n-- **Old** — text.\n+- **New** — text.\n'))
        self.assertEqual([], found)

    def test_an_entry_added_beside_a_reworded_one_is_a_violation(self):
        found = self.violations(diff('CHANGELOG.md', '@@ -21 +21,2 @@\n-- **Old** — text.\n'
                                     '+- **New** — text.\n+- **Another** — text.\n'))
        self.assertEqual(['- **Another** — text.'], [item.line for item in found])

    def test_each_hunk_is_judged_on_its_own(self):
        found = self.violations(diff('CHANGELOG.md',
                                     '@@ -21 +21 @@\n-- **Old**\n+- **New**\n'
                                     '@@ -40,0 +41 @@\n+- **Added** — text.\n'))
        self.assertEqual(['- **Added** — text.'], [item.line for item in found])

    def test_additions_to_other_files_are_not_judged(self):
        found = self.violations(diff('docs/DEVELOPMENT.md', '@@ -20,0 +21 @@\n+- A bullet.\n'))
        self.assertEqual([], found)

    def test_the_diff_header_is_not_an_addition(self):
        self.assertEqual([], self.violations(diff('CHANGELOG.md', '@@ -20,0 +21 @@\n Context\n')))


class GuardOverGit(unittest.TestCase):
    """The guard reads the diff from git and exempts the release collection commit."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        (self.root / 'changelog.d').mkdir()
        (self.root / 'docs' / 'lessons.d').mkdir(parents=True)
        self.git('init', '-q', '-b', 'main')
        self.git('config', 'user.email', 'test@example.invalid')
        self.git('config', 'user.name', 'Test')
        (self.root / 'CHANGELOG.md').write_text(CHANGELOG, encoding='utf-8')
        (self.root / 'docs' / 'LESSONS.md').write_text(LESSONS, encoding='utf-8')
        write(self.root / 'changelog.d', 'README.md', '# Fragments\n')
        self.git('add', '-A')
        self.git('commit', '-qm', 'base')
        self.base = self.git('rev-parse', 'HEAD').strip()

    def git(self, *args):
        return subprocess.run(('git', '-C', str(self.root)) + args, check=True,
                              capture_output=True, text=True).stdout

    def commit(self, message):
        self.git('add', '-A')
        self.git('commit', '-qm', message)

    def check(self, *extra):
        return run_cli(self.root, 'check', *extra)

    def test_check_rejects_repository_root_override(self):
        self.assertEqual(2, self.check('--base-sha', self.base, '--repo-root', str(self.root)))

    def test_check_rejects_non_hash_bases_without_creating_git_output(self):
        output = self.root / 'injected'
        for base in ('HEAD', 'main', self.base[:12], '0' * 39, '0' * 41, 'g' * 40,
                     'A' * 40, self.base + '\n', self.base + '...HEAD',
                     '--stat', '--output=' + str(output)):
            with self.subTest(base=base):
                self.assertEqual(2, self.check('--base-sha=' + base))
                self.assertFalse(output.exists())
                self.assertFalse(Path(str(output) + '...HEAD').exists())
        self.assertEqual(CHANGELOG, (self.root / 'CHANGELOG.md').read_text(encoding='utf-8'))
        self.assertEqual(LESSONS, (self.root / 'docs' / 'LESSONS.md').read_text(encoding='utf-8'))

    def test_a_fragment_passes(self):
        write(self.root / 'changelog.d', 'topic.md',
              fragment('section: Fixed', '- **New** — text.\n'))
        self.commit('fix with a fragment')
        self.assertEqual(0, self.check('--base-sha', self.base))

    def test_an_entry_written_into_the_shared_file_fails(self):
        path = self.root / 'CHANGELOG.md'
        path.write_text(CHANGELOG.replace('- **A fix** — one line.\n',
                                          '- **A fix** — one line.\n- **New** — text.\n'),
                        encoding='utf-8')
        self.commit('fix with a direct changelog edit')
        self.assertEqual(1, self.check('--base-sha', self.base))

    def test_the_release_collection_commit_is_exempt(self):
        # The fragment is on the base branch, as it is when a release collects what merged.
        entries = (('changelog.d', 'section: Fixed', '- **New** — text.\n'),
                   ('docs/lessons.d', 'kind: row', '| Failure. | Rule. | Evidence. |\n'))
        for selected in (entries[:1], entries[1:], entries):
            with self.subTest(directories=[entry[0] for entry in selected]):
                for directory, front, body in selected:
                    write(self.root / directory, 'topic.md', fragment(front, body))
                self.commit('add fragments')
                self.base = self.git('rev-parse', 'HEAD').strip()
                self.assertEqual(0, run_cli(self.root, 'collect'))
                self.commit('release: collect the fragments')
                self.assertEqual(0, self.check('--base-sha', self.base))

    def test_non_fragment_deletions_exempt_neither_target(self):
        for directory in ('changelog.d', 'docs/lessons.d'):
            for name in ('README.md', '123.md', 'Topic.md', 'notes.txt', 'nested/topic.md'):
                for target in ('CHANGELOG.md', 'docs/LESSONS.md'):
                    with self.subTest(directory=directory, name=name, target=target):
                        deleted = self.root / directory / name
                        deleted.parent.mkdir(parents=True, exist_ok=True)
                        deleted.write_text('Documentation.\n', encoding='utf-8')
                        self.commit('add non-fragment file')
                        self.base = self.git('rev-parse', 'HEAD').strip()
                        deleted.unlink()
                        if deleted.parent != self.root / directory:
                            deleted.parent.rmdir()
                        path = self.root / target
                        path.write_text(path.read_text(encoding='utf-8') + '\n- **Direct entry.**\n',
                                        encoding='utf-8')
                        self.commit('delete non-fragment and add direct entry')
                        self.assertEqual(1, self.check('--base-sha', self.base))

    def test_collecting_one_target_does_not_exempt_the_other(self):
        entries = (('changelog.d', 'section: Fixed', '- **New** — text.\n', 'docs/LESSONS.md'),
                   ('docs/lessons.d', 'kind: row', '| Failure. | Rule. | Evidence. |\n',
                    'CHANGELOG.md'))
        for directory, front, body, other_target in entries:
            with self.subTest(directory=directory, other_target=other_target):
                write(self.root / directory, 'topic.md', fragment(front, body))
                self.commit('add fragment')
                self.base = self.git('rev-parse', 'HEAD').strip()
                self.assertEqual(0, run_cli(self.root, 'collect'))
                path = self.root / other_target
                path.write_text(path.read_text(encoding='utf-8') + '\n- **Direct entry.**\n',
                                encoding='utf-8')
                self.commit('collect one target and add a direct entry to the other')
                self.assertEqual(1, self.check('--base-sha', self.base))

    def test_an_invalid_fragment_fails_with_a_base(self):
        write(self.root / 'changelog.d', 'bad.md',
              fragment('section: Repaired', '- **New** — text.\n'))
        self.commit('add an invalid fragment')
        self.assertEqual(2, self.check('--base-sha', self.base))


class Cli(unittest.TestCase):
    """The command line surface."""

    def test_check_requires_a_base(self):
        with self.assertRaises(SystemExit) as raised:
            fragments.main(['check'])
        self.assertEqual(2, raised.exception.code)

    def test_unknown_command_is_rejected(self):
        with self.assertRaises(SystemExit):
            fragments.main(['render'])

    def test_no_command_is_rejected(self):
        with self.assertRaises(SystemExit):
            fragments.main([])


def cut_last_entry(text, heading):
    """Remove the last entry of a section, returning the reduced text and that entry's body."""
    lines = text.split('\n')
    start = lines.index(heading)
    end = next(index for index in range(start + 1, len(lines))
               if lines[index].startswith('## '))
    entry = max(index for index in range(start + 1, end) if lines[index].startswith('- '))
    last = max(index for index in range(entry, end) if lines[index].strip())
    body = '\n'.join(lines[entry:last + 1])
    return '\n'.join(lines[:entry] + lines[last + 1:]), body


if __name__ == '__main__':
    unittest.main()
