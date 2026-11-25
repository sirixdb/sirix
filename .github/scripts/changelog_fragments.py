#!/usr/bin/env python3
"""Collect changelog and lesson fragments, and guard the shared files. See changelog.d/README.md.

A pull request adds one fragment file per entry instead of editing CHANGELOG.md or
docs/LESSONS.md, so two branches never append to the same lines. `collect` renders the
fragments into those files at release time and removes them; `check` validates the fragments
and fails a pull request that writes an entry into a shared file directly.
"""
import argparse
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
import re
import subprocess
import sys

# Keep a Changelog sections, in the order a release presents them.
CHANGELOG_SECTIONS = ('Added', 'Changed', 'Deprecated', 'Removed', 'Fixed', 'Security')
LESSON_KINDS = ('row', 'note')
UNRELEASED_HEADING = '## [Unreleased]'
FRAGMENT_NAME = re.compile(r'[a-z0-9][a-z0-9._-]*\.md\Z')
FRONT_MATTER_DELIMITER = '---'
FRONT_MATTER_LINE = re.compile(r'([a-z][a-z0-9_-]*):[ \t]*(.*\S)[ \t]*\Z')
DOCUMENTATION = 'README.md'


class FragmentError(Exception):
    """A fragment, or a shared file, that the convention cannot render."""


@dataclass(frozen=True)
class Fragment:
    """One entry: where it belongs (`value`) and the bytes to insert (`body`)."""
    path: Path
    value: str
    body: str


@dataclass(frozen=True)
class EntryAddition:
    """An entry line a diff adds to a shared file."""
    path: str
    line: str


def require_table_rows(body, name):
    """A lessons table row is one or more complete `| ... |` lines."""
    for line in body.split('\n'):
        if not (line.startswith('|') and line.rstrip().endswith('|')):
            raise FragmentError('%s: %r is not a table row' % (name, line))


@dataclass(frozen=True)
class TargetSpec:
    """One shared file, its fragment directory and the front-matter key that fragments carry."""
    target: str
    directory: str
    key: str
    values: tuple
    checks: dict = field(default_factory=dict)


CHANGELOG_SPEC = TargetSpec('CHANGELOG.md', 'changelog.d', 'section', CHANGELOG_SECTIONS)
LESSONS_SPEC = TargetSpec('docs/LESSONS.md', 'docs/lessons.d', 'kind', LESSON_KINDS,
                          {'row': require_table_rows})
SPECS = (CHANGELOG_SPEC, LESSONS_SPEC)

# An entry line of a shared file: what a fragment exists to add. Anything else - a continuation
# line, a blank line, prose - stays an ordinary edit of a line that is already there.
ENTRY_LINES = {
    CHANGELOG_SPEC.target: re.compile(r'(- |#{2,3} )'),
    LESSONS_SPEC.target: re.compile(r'(- |#{2,3} |\|)'),
}


def parse_fragment(text, spec, name):
    """Return the fragment's front-matter value and its verbatim body."""
    lines = text.split('\n')
    if lines[0].strip() != FRONT_MATTER_DELIMITER:
        raise FragmentError('%s: the first line must be the front matter delimiter %r'
                            % (name, FRONT_MATTER_DELIMITER))
    closing = None
    values = {}
    for index in range(1, len(lines)):
        line = lines[index]
        if line.strip() == FRONT_MATTER_DELIMITER:
            closing = index
            break
        if not line.strip():
            continue
        match = FRONT_MATTER_LINE.fullmatch(line)
        if match is None:
            raise FragmentError('%s: front matter line %r is not "key: value"' % (name, line))
        values[match.group(1)] = match.group(2)
    if closing is None:
        raise FragmentError('%s: the front matter is not terminated by %r'
                            % (name, FRONT_MATTER_DELIMITER))
    unknown = sorted(key for key in values if key != spec.key)
    if unknown:
        raise FragmentError('%s: unknown front-matter key(s) %s; %s expects only %r'
                            % (name, ', '.join(unknown), spec.directory, spec.key))
    if spec.key not in values:
        raise FragmentError('%s: the front matter needs %r, one of %s'
                            % (name, spec.key, ', '.join(spec.values)))
    value = values[spec.key]
    if value not in spec.values:
        raise FragmentError('%s: %s: %s is not one of %s'
                            % (name, spec.key, value, ', '.join(spec.values)))
    body = '\n'.join(lines[closing + 1:]).lstrip('\n').rstrip()
    if not body:
        raise FragmentError('%s: the entry body is empty' % name)
    check = spec.checks.get(value)
    if check is not None:
        check(body, name)
    return value, body


def is_fragment_name(name):
    return FRAGMENT_NAME.fullmatch(name) is not None and not Path(name).stem.isdigit()


def load_fragments(directory, spec):
    """Every fragment in one directory, in file-name order. Raises on the first invalid file."""
    directory = Path(directory)
    if not directory.is_dir():
        return []
    fragments = []
    for path in sorted(directory.iterdir(), key=lambda entry: entry.name):
        if path.name == DOCUMENTATION:
            continue
        if not is_fragment_name(path.name):
            raise FragmentError(
                '%s: %s is not a valid fragment name: name a fragment after its branch or topic, '
                'in lower case, never after a pull-request number' % (spec.directory, path.name))
        value, body = parse_fragment(path.read_text(encoding='utf-8'), spec, path.name)
        fragments.append(Fragment(path, value, body))
    return fragments


def _heading_index(lines, heading, name):
    for index, line in enumerate(lines):
        if line.strip() == heading:
            return index
    raise FragmentError('%s: no %r heading to collect into' % (name, heading))


def _release_end(lines, start):
    """The index after the last line of the release that `start` heads."""
    for index in range(start + 1, len(lines)):
        if lines[index].startswith('## '):
            return index
    return len(lines)


def _section_end(lines, start):
    """The index after the last line of the `###` section that `start` heads."""
    for index in range(start + 1, len(lines)):
        if lines[index].startswith('## ') or lines[index].startswith('### '):
            return index
    return len(lines)


def _last_content(lines, start, end):
    """The index of the last non-blank line in [start, end), or None when the block is empty."""
    content = [index for index in range(start, end) if lines[index].strip()]
    return content[-1] if content else None


def _changelog_sections(lines, unreleased, end):
    """The `### <section>` headings of the unreleased release, by name."""
    sections = {}
    for index in range(unreleased + 1, end):
        if lines[index].startswith('### '):
            sections[lines[index][len('### '):].strip()] = index
    return sections


def _insert_section(lines, unreleased, end, section):
    """Create an absent `### <section>` in Keep a Changelog order; return its heading index."""
    existing = _changelog_sections(lines, unreleased, end)
    order = CHANGELOG_SECTIONS[CHANGELOG_SECTIONS.index(section) + 1:]
    later = [existing[name] for name in order if name in existing]
    if later:
        at = min(later)
        lines[at:at] = ['### ' + section, '', '']
        return at
    content = _last_content(lines, unreleased + 1, end)
    at = (unreleased if content is None else content) + 1
    lines[at:at] = ['', '### ' + section, '']
    return at + 1


def _insert_entry(lines, heading, body):
    """Insert an entry body at the end of the `###` section that `heading` heads."""
    section_end = _section_end(lines, heading)
    content = _last_content(lines, heading + 1, section_end)
    if content is not None:
        lines[content + 1:content + 1] = body.split('\n')
        return
    # An empty section owns the blank line under its heading; keep one before what follows.
    at = min(heading + 2, section_end)
    trailing = [''] if at < len(lines) and lines[at].startswith('#') else []
    lines[at:at] = body.split('\n') + trailing


def render_changelog(text, fragments):
    """Append each entry to its `### <section>` of `## [Unreleased]`, changing no other line."""
    if not fragments:
        return text
    lines = text.split('\n')
    for fragment in fragments:
        unreleased = _heading_index(lines, UNRELEASED_HEADING, CHANGELOG_SPEC.target)
        end = _release_end(lines, unreleased)
        heading = _changelog_sections(lines, unreleased, end).get(fragment.value)
        if heading is None:
            heading = _insert_section(lines, unreleased, end, fragment.value)
        _insert_entry(lines, heading, fragment.body)
    return '\n'.join(lines)


def _table_end(lines):
    """The index after the last row of the first table, which is the lessons table."""
    rows = [index for index, line in enumerate(lines) if line.startswith('|')]
    if not rows:
        raise FragmentError('%s: no table to collect a row into' % LESSONS_SPEC.target)
    end = rows[0]
    while end < len(lines) and lines[end].startswith('|'):
        end += 1
    return end


def render_lessons(text, fragments):
    """Append rows to the lessons table and notes to the end of the file, changing no other line."""
    if not fragments:
        return text
    lines = text.split('\n')
    for fragment in fragments:
        if fragment.value == 'row':
            require_table_rows(fragment.body, fragment.path.name)
            at = _table_end(lines)
            lines[at:at] = fragment.body.split('\n')
        else:
            content = _last_content(lines, 0, len(lines))
            at = (0 if content is None else content) + 1
            lines[at:at] = [''] + fragment.body.split('\n')
    return '\n'.join(lines)


RENDERERS = {CHANGELOG_SPEC.target: render_changelog, LESSONS_SPEC.target: render_lessons}


def collect(root):
    """Render every fragment into its shared file and remove the fragments that were rendered."""
    fragments = {}
    rendered = {}
    for spec in SPECS:
        found = load_fragments(root / spec.directory, spec)
        if not found:
            continue
        target = root / spec.target
        if not target.is_file():
            raise FragmentError('%s is missing: nothing to collect %s into'
                                % (spec.target, spec.directory))
        fragments[spec.target] = found
        rendered[spec.target] = RENDERERS[spec.target](target.read_text(encoding='utf-8'), found)
    if not rendered:
        print('no fragments to collect')
        return
    for target, text in sorted(rendered.items()):
        names = ', '.join(fragment.path.name for fragment in fragments[target])
        (root / target).write_text(text, encoding='utf-8')
        print('%s: collected %s' % (target, names))
    for target in rendered:
        for fragment in fragments[target]:
            fragment.path.unlink()


def entry_additions(diff):
    """Every entry line a unified diff adds to a shared file, judged hunk by hunk.

    A hunk that replaces entry lines is reworking what is already there, so only the additions
    beyond the lines the hunk removes are new entries that belong in a fragment.
    """
    found = []
    path = None
    pattern = None
    added = []
    removed = 0
    for line in diff.split('\n'):
        if line.startswith('+++ b/'):
            found.extend(added[removed:])
            path = line[len('+++ b/'):].strip()
            pattern = ENTRY_LINES.get(path)
            added, removed = [], 0
        elif line.startswith('@@'):
            found.extend(added[removed:])
            added, removed = [], 0
        elif pattern is None or line.startswith(('+++', '---')):
            continue
        elif line.startswith('+') and pattern.match(line[1:]):
            added.append(EntryAddition(path, line[1:].rstrip()))
        elif line.startswith('-') and pattern.match(line[1:]):
            removed += 1
    found.extend(added[removed:])
    return found


def _git(root, *arguments):
    command = ['git', '-C', str(root)] + list(arguments)
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def _collects_fragments(root, base, spec):
    deleted = _git(root, 'diff', '--name-only', '-z', '--diff-filter=D', '%s...HEAD' % base,
                   '--', spec.directory)
    directory = Path(spec.directory)
    for name in deleted.split('\0'):
        path = Path(name)
        if path.parent == directory and is_fragment_name(path.name):
            return True
    return False


def check(root, base):
    """Validate every fragment, then refuse an entry written into a shared file directly."""
    if re.fullmatch(r'[0-9a-f]{40}', base) is None:
        raise FragmentError('--base-sha must be a full 40-character lowercase hexadecimal commit hash')
    for spec in SPECS:
        load_fragments(root / spec.directory, spec)
    collected = {spec.target for spec in SPECS if _collects_fragments(root, base, spec)}
    diff = _git(root, 'diff', '--unified=0', '%s...HEAD' % base, '--',
                *[spec.target for spec in SPECS])
    additions = [addition for addition in entry_additions(diff) if addition.path not in collected]
    if not additions:
        print('fragments are valid; no entry is written into a shared file outside collection')
        return 0
    for addition in additions:
        print('::error file=%s::%s adds an entry directly: %s'
              % (addition.path, addition.path, addition.line))
    print('Add one fragment file per entry instead (see changelog.d/README.md): a fragment in '
          'changelog.d/ for a changelog entry, in docs/lessons.d/ for a lesson. Reworking or '
          'extending a line that is already there is allowed; adding an entry is not.')
    return 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('collect', help='render the fragments and remove them')
    check_command = commands.add_parser('check',
                                       help='validate fragments and guard shared files')
    check_command.add_argument('--base-sha', required=True,
                               help='the base commit to compare for shared-file entry additions')
    arguments = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    try:
        if arguments.command == 'collect':
            collect(root)
            return 0
        return check(root, arguments.base_sha)
    except FragmentError as error:
        print('::error::%s' % error)
        return 2


if __name__ == '__main__':
    sys.exit(main())
