"""Behaviour of the union merge driver on the shared append-only files (docs/DEVELOPMENT.md).

Two branches append a different entry to the same file; a merge or a rebase must keep both
without stopping on a conflict, and the file's own validator must still accept the result.
"""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[1]
REPO_ROOT = SCRIPTS.parents[1]
SPEC_CHANGELOG = 'spec/CHANGELOG.md'
UNION_FILES = (SPEC_CHANGELOG,)
# Not union files. CHANGELOG.md and docs/LESSONS.md take fragments, so a union would let a direct
# entry bypass the fragment guard. The TOML registries cannot union: git keeps a repeated
# `[[table]]` header once, so two appended tables fuse into one with duplicate keys (a parse error).
# docs/VERIFICATION.md and README.md are edited in place, not appended to.
NOT_UNION_FILES = ('CHANGELOG.md', 'docs/LESSONS.md', 'docs/VERIFICATION.md', 'README.md',
                   '.github/mutation-dispositions.toml', '.github/mutation-test-associations.toml',
                   'docs/bench/CONDITIONS.md')


def git(root, *args):
    return subprocess.run(['git', '-C', str(root), '-c', 'user.name=t', '-c', 'user.email=t@t',
                           '-c', 'commit.gpgsign=false', *args],
                          check=True, capture_output=True, text=True).stdout


def spec_entry(name):
    return f'- Add requirement {name} for the {name} subsystem.\n  Its gap is recorded in {name}-chapter.md.\n\n'


class UnionMergeTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        git(self.root, 'init', '-q', '-b', 'main')
        # The repository's own attributes decide which files union; copy them, not a literal.
        (self.root / '.gitattributes').write_text((REPO_ROOT / '.gitattributes').read_text())
        for relative in UNION_FILES:
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text((REPO_ROOT / relative).read_text())
        git(self.root, 'add', '-A')
        git(self.root, 'commit', '-q', '-m', 'base')

    def tearDown(self):
        self.directory.cleanup()

    def append(self, branch, relative, text, prepend_marker=None):
        git(self.root, 'checkout', '-q', '-B', branch, 'main')
        path = self.root / relative
        content = path.read_text()
        if prepend_marker is None:
            content += text
        else:
            at = content.index(prepend_marker) + len(prepend_marker)
            content = content[:at] + text + content[at:]
        path.write_text(content)
        git(self.root, 'commit', '-q', '-am', f'{branch} {relative}')

    def both_sides(self, relative, first, second, marker=None):
        self.append('first', relative, first, marker)
        self.append('second', relative, second, marker)

    def merge_keeps_both(self, relative):
        git(self.root, 'checkout', '-q', 'first')
        git(self.root, 'merge', '-q', '--no-edit', 'second')
        return (self.root / relative).read_text()

    def rebase_keeps_both(self, relative):
        git(self.root, 'checkout', '-q', 'second')
        git(self.root, 'rebase', '-q', 'first')
        return (self.root / relative).read_text()

    def test_the_repository_declares_exactly_the_union_files(self):
        for relative in UNION_FILES:
            self.assertEqual('union', git(self.root, 'check-attr', 'merge', relative).split(': ')[-1].strip(),
                             relative)
        for relative in NOT_UNION_FILES:
            self.assertNotEqual('union',
                                git(REPO_ROOT, 'check-attr', 'merge', relative).split(': ')[-1].strip(),
                                relative)

    # The union may drop the blank line between the two new entries (git hoists the shared blank
    # line out of the conflict); a list without a separator is still valid, so only entry text counts.
    def test_spec_changelog_prepended_entries_both_survive_a_merge(self):
        marker = '## 1.0.0 (unreleased)\n\n'
        self.both_sides(SPEC_CHANGELOG, spec_entry('alpha'), spec_entry('beta'), marker)
        text = self.merge_keeps_both(SPEC_CHANGELOG)
        self.assertTrue(spec_entry('alpha').rstrip() in text, 'alpha entry lost')
        self.assertTrue(spec_entry('beta').rstrip() in text, 'beta entry lost')
        self.assertEqual(1, text.count('## 1.0.0 (unreleased)'))
        self.assertNotIn('<<<<<<<', text)
        self.assertTrue(text.startswith('# Specification changes\n\n## 1.0.0 (unreleased)\n\n'))

    def test_spec_changelog_both_survive_a_rebase(self):
        marker = '## 1.0.0 (unreleased)\n\n'
        self.both_sides(SPEC_CHANGELOG, spec_entry('alpha'), spec_entry('beta'), marker)
        text = self.rebase_keeps_both(SPEC_CHANGELOG)
        self.assertTrue(spec_entry('alpha').rstrip() in text, 'alpha entry lost')
        self.assertTrue(spec_entry('beta').rstrip() in text, 'beta entry lost')


if __name__ == '__main__':
    unittest.main()
