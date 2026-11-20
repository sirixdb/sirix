#!/usr/bin/env python3
"""Bounded, secret-free PR mutation analysis. See docs/VERIFICATION.md."""
import argparse
from collections import Counter
import hashlib
import html
import json
import os
from pathlib import Path
import re
import signal
import struct
import subprocess
import sys
import time
import tomllib
import xml.etree.ElementTree as ET

MODULES = {name: name for name in ('sirix-core', 'sirix-query', 'sirix-rest-api',
                                  'sirix-kotlin-api', 'sirix-kotlin-cli',
                                  'sirix-benchmarks', 'sirix-mcp')}
MODULES['sirix-examples'] = 'sirix-example'
DIRECTORIES = {module: directory for directory, module in MODULES.items()}

# PIT collects baseline coverage for every targetTests class. Bound test selection as well as
# mutant sampling, so coverage collection cannot fall back to the whole module suite.
TEST_SUFFIXES = ('Test', 'Tests', 'IT')
PACKAGE_TEST_LIMIT = 25
MODULE_TEST_LIMIT = 60
# Best first: an exactly named test, then a name-related one, then the source's own package.
KIND_ORDER = ('named', 'related', 'package')
UNRESOLVED_KINDS = ('unmapped', 'over-limit')
# Reviewed repository files (spec/CONV-conventions.md, CONV-201 to CONV-204). Fixed paths: no flag,
# environment variable or other input may add, ignore or relax a disposition (CONV-203).
DISPOSITIONS_FILE = '.github/mutation-dispositions.toml'
ASSOCIATIONS_FILE = '.github/mutation-test-associations.toml'
# Execution time reserved for each module still waiting, so none is starved by an earlier one.
MIN_MODULE_SECONDS = 120
# A large diff cannot afford every mutant of every changed class. A mutant whose covering test
# hangs costs a whole minion restart, and those restarts - not the mutant count - exhausted the
# budget for PR 1351's 57 changed sirix-core classes once the test scope was fixed. Sample per
# class instead, so the work scales with the diff and even a small diff has a per-class cap.
MUTANT_BUDGET = 300
MIN_MUTANTS_PER_CLASS = 4
MAX_MUTANTS_PER_CLASS = 40
# Job-summary bounds; GitHub drops a summary above 1 MiB and the artifact retains every entry.
SUMMARY_MUTANT_LIMIT = 100
ADVISORY_CLASS_LIMIT = 10
# A unified-diff hunk header: the new-side start line and, when present, its line count.
HUNK = re.compile(r'^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@')


class Unmapped(Exception):
    """No test maps to any changed source, so PIT has no coverage baseline to collect."""


def select_sources(paths):
    selected = {}
    for path in paths:
        parts = Path(path).parts
        if (len(parts) < 6 or parts[0] != 'bundles' or parts[2:4] != ('src', 'main')
                or parts[4] not in ('java', 'kotlin') or Path(path).suffix not in ('.java', '.kt')
                or parts[-1] in ('package-info.java', 'module-info.java')):
            continue
        if parts[1] not in MODULES:
            raise ValueError(f'Unknown module for changed source: {path}')
        selected.setdefault(MODULES[parts[1]], []).append('/'.join(parts[2:]))
    return {module: sorted(set(sources)) for module, sources in sorted(selected.items())}


def commit_id(value):
    if not re.fullmatch(r'(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})', value):
        raise ValueError('Expected a full hexadecimal commit ID')
    return value.lower()


def repository_path(value):
    parts = value.split('/')
    if any(not part or part in ('.', '..') or part.startswith('-') for part in parts):
        raise ValueError(f'Unsafe repository path: {value!r}')
    return value


def changed_line_map(root, base, sources):
    """New-side line numbers of the PR's own changes, per selected source.

    The new side is the working tree - the revision `Build` compiled and PIT mutates - so a
    mapped line number always describes the file whose mutants are judged. Rename detection
    needs both paths, so the diff covers all of `bundles/` and hunks are kept per selected path.
    """
    patch = subprocess.check_output(
        ['git', '--literal-pathspecs', '-c', 'core.quotePath=false', '-C', str(root), 'diff',
         '-U0', '--no-textconv', '--merge-base', commit_id(base), '--', 'bundles/'], shell=False)
    lines = {path: set() for path in sources}
    current, in_header, named = None, False, False
    for entry in os.fsdecode(patch).splitlines():
        if entry.startswith('diff --git '):
            current, in_header, named = None, True, False
        elif in_header and entry.startswith('+++ '):
            target = entry[len('+++ '):]
            if target.startswith('"'):
                raise ValueError(f'Git quoted a diff path; cannot attribute changed lines: {target}')
            # `/dev/null`: a deleted file has no new side, so it contributes no line.
            current, named = (target[len('b/'):] if target.startswith('b/') else None), True
        elif entry.startswith('@@'):
            # Only a header can start with '@@': every body line starts with '+', '-' or '\'.
            hunk = HUNK.match(entry)
            if not hunk:
                raise ValueError(f'Unparsable diff hunk header: {entry[:60]!r}')
            if not named:
                raise ValueError(f'Diff hunk precedes any file header: {entry[:60]!r}')
            in_header = False
            if current in lines:
                start = int(hunk[1])
                lines[current].update(range(start, start + (1 if hunk[2] is None else int(hunk[2]))))
    return {path: sorted(numbers) for path, numbers in lines.items()}


def changed_sources(root, base, head):
    base_sha, head_sha = commit_id(base), commit_id(head)
    raw = subprocess.check_output(['git', '-C', str(root), 'diff', '--name-only', '-z',
                                   '--diff-filter=ACMRT', f'{base_sha}...{head_sha}', '--', 'bundles/'],
                                  shell=False)
    paths = [repository_path(os.fsdecode(path)) for path in raw.split(b'\0') if path]
    selected = select_sources(paths)
    sources = {f'bundles/{DIRECTORIES[module]}/{source}': (module, source)
               for module, files in selected.items() for source in files}
    lines = {module: {} for module in selected}
    if sources:
        tree = subprocess.check_output(['git', '--literal-pathspecs', '-C', str(root), 'ls-tree',
                                        '-r', '--name-only', '-z', head_sha, '--', *sources],
                                       shell=False)
        tracked = {os.fsdecode(path) for path in tree.split(b'\0') if path}
        if set(sources) != tracked:
            raise ValueError('Selected source path is absent from the head commit')
        for path, numbers in changed_line_map(root, base_sha, sources).items():
            module, source = sources[path]
            lines[module][source] = numbers
    return selected, lines


class ClassReader:
    """Read JVM class structures (JVMS 4.4, 4.7.10), without starting a JVM."""

    def __init__(self, data):
        self.data = data
        self.offset = 0

    def take(self, size):
        if self.offset + size > len(self.data):
            raise ValueError('Truncated class file')
        value = self.data[self.offset:self.offset + size]
        self.offset += size
        return value

    def u2(self):
        return struct.unpack('>H', self.take(2))[0]

    def u4(self):
        return struct.unpack('>I', self.take(4))[0]

    def constant_pool(self):
        pool = [None] * self.u2()
        index = 1
        sizes = {3: 4, 4: 4, 5: 8, 6: 8, 8: 2, 9: 4, 10: 4, 11: 4, 12: 4,
                 15: 3, 16: 2, 17: 4, 18: 4, 19: 2, 20: 2}
        while index < len(pool):
            tag = self.take(1)[0]
            if tag == 1:
                pool[index] = self.take(self.u2()).decode('utf-8', errors='replace')
            elif tag == 7:
                pool[index] = self.u2()
            elif tag in sizes:
                self.take(sizes[tag])
            else:
                raise ValueError(f'Unsupported constant-pool tag: {tag}')
            index += 2 if tag in (5, 6) else 1
        return pool

    def attributes(self, pool):
        for _ in range(self.u2()):
            attribute = pool[self.u2()]
            yield attribute, self.take(self.u4())

    def skip_members(self):
        for _ in range(self.u2()):
            self.take(6)  # access flags, name, descriptor
            for _ in range(self.u2()):
                self.take(2)  # attribute name
                self.take(self.u4())


def code_lines(code, pool):
    """Source lines of one method's `Code` attribute, from its LineNumberTable (JVMS 4.7.3, 4.7.12)."""
    reader = ClassReader(code)
    reader.take(4)  # max_stack, max_locals
    reader.take(reader.u4())  # bytecode
    reader.take(8 * reader.u2())  # exception table
    lines = set()
    for attribute, value in reader.attributes(pool):
        if attribute == 'LineNumberTable':
            table = ClassReader(value)
            for _ in range(table.u2()):
                table.take(2)  # start_pc
                lines.add(table.u2())
    return lines


def read_class(data):
    """The binary name, `SourceFile` and every method's source lines, keyed by name and descriptor."""
    reader = ClassReader(data)
    if reader.take(4) != b'\xca\xfe\xba\xbe':
        raise ValueError('Invalid class magic')
    reader.take(4)  # minor/major version
    pool = reader.constant_pool()
    reader.take(2)  # access flags
    name = pool[pool[reader.u2()]].replace('/', '.')
    reader.take(2)  # superclass
    reader.take(2 * reader.u2())  # interfaces
    reader.skip_members()  # fields
    methods = {}
    for _ in range(reader.u2()):
        reader.take(2)  # access flags
        method, descriptor = pool[reader.u2()], pool[reader.u2()]
        lines = set()
        for attribute, value in reader.attributes(pool):
            if attribute == 'Code':
                lines |= code_lines(value, pool)
        methods[(method, descriptor)] = frozenset(lines)
    source = None
    for attribute, value in reader.attributes(pool):
        if attribute == 'SourceFile':
            source = pool[struct.unpack('>H', value)[0]]
    return name, source, methods


def class_source(data):
    name, source, _ = read_class(data)
    return name, source


def source_path(module_root, source):
    path = module_root / repository_path(source)
    if not path.resolve().is_relative_to(module_root.resolve()):
        raise ValueError(f'Source path escapes the module: {source!r}')
    return path


def declared_package(text):
    """The source's own package, ignoring commented-out declarations."""
    stripped = re.sub(r'/\*.*?\*/|//[^\n]*', '', text, flags=re.S)
    package = re.search(r'^\s*package\s+([\w.]+)', stripped, re.M)
    return package.group(1) if package else ''


def changed_line_index(module_root, lines):
    """Key the changed lines of each source the way PIT names a mutant's location.

    PIT reports `sourceFile` as a bare file name and the package through `mutatedClass`, so this
    key attributes nested, anonymous and extra top-level classes and Kotlin file facades to the
    source they were compiled from. A mapped source with no changed line keeps an empty entry:
    its mutants are advisory, not unattributed.
    """
    index = {}
    for source, numbers in lines.items():
        path = source_path(module_root, source)
        if not path.is_file():
            continue
        selected = set()
        for number in numbers:
            line = int(number)
            if line < 1:
                raise ValueError(f'Changed line {line} of {source!r} is not a line number')
            selected.add(line)
        index.setdefault((declared_package(path.read_text()), path.name), set()).update(selected)
    return index


def resolve_targets(module_root, sources):
    expected = {}
    for source in sources:
        path = source_path(module_root, source)
        if not path.is_file():
            continue
        expected[(declared_package(path.read_text()), path.name)] = source
    found = set()
    targets = set()
    for language in ('java', 'kotlin'):
        for path in (module_root / f'build/classes/{language}/main').rglob('*.class'):
            name, source_file = class_source(path.read_bytes())
            key = (name.rpartition('.')[0], source_file)
            if key in expected:
                targets.add(name)
                found.add(expected[key])
    return sorted(targets), sorted(set(sources) - found)


def module_test_classes(module_root):
    """Discover declared top-level test types and packages without requiring compilation."""
    classes = {}
    for language, suffix in (('java', '.java'), ('kotlin', '.kt')):
        root = module_root / 'src/test' / language
        if not root.is_dir():
            continue
        for path in root.rglob('*' + suffix):
            if path.name in ('package-info.java', 'module-info.java'):
                continue
            text = re.sub(r'//[^\n]*|/\*.*?\*/|""".*?"""|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                          ' ', path.read_text(), flags=re.S)
            declaration = re.search(r'^\s*package\s+([\w.]+)', text, re.M)
            package = declaration.group(1) if declaration else ''
            keywords = 'class|interface|enum|record' if language == 'java' else 'class|interface|object'
            depth = 0
            for token in re.finditer(r'\b(?:' + keywords + r')\s+([\w$]+)|[{}]', text):
                if token[0] == '{':
                    depth += 1
                elif token[0] == '}':
                    depth -= 1
                elif depth == 0:
                    name = token[1]
                    classes[f'{package}.{name}' if package else name] = package
    return classes


def is_test_name(simple):
    return any(simple.endswith(suffix) and simple != suffix for suffix in TEST_SUFFIXES)


def base_names(stem):
    """The names a test class may be built from: the class itself and its unwrapped form."""
    names = [stem]
    if stem.startswith('Abstract') and stem != 'Abstract':
        names.append(stem[len('Abstract'):])
    if stem.endswith('Impl') and stem != 'Impl':
        names.append(stem[:-len('Impl')])
    return [name for index, name in enumerate(names) if name not in names[:index]]


def is_related_test_name(simple, base):
    """`Foo` relates to `FooTest` and `FooSlotTest`, never to a merely similar `FoobarTest`."""
    if not simple.startswith(base) or not is_test_name(simple):
        return False
    # Every entry in TEST_SUFFIXES starts a new word, so one uppercase test covers both
    # `FooTest` (the remainder is the suffix) and `FooSlotTest` (the remainder adds a word).
    return simple[len(base):][:1].isupper()


def source_package(module_root, source):
    return declared_package(source_path(module_root, source).read_text())


def resolve_tests(stem, package, tests):
    """Prefer named and module-wide related tests, with a bounded package fallback."""
    prefix = package + '.' if package else ''
    named = [name for name in (f'{prefix}{stem}{suffix}' for suffix in ('Test', 'Tests'))
             if name in tests]
    if named:
        return sorted(named), 'named'
    bases = base_names(stem)
    related = sorted(name for name in tests
                     if any(is_related_test_name(name.rpartition('.')[2], base) for base in bases))
    if related:
        return related, 'related'
    # `package and ...`: an undeclared package must not make every test a subpackage match.
    in_package = sorted(name for name, owner in tests.items()
                        if (owner == package or (package and owner.startswith(prefix)))
                        and is_test_name(name.rpartition('.')[2]))
    if in_package and len(in_package) <= PACKAGE_TEST_LIMIT:
        return in_package, 'package'
    return [], 'unmapped'


def mutants_per_class(targets):
    """Bound PIT's per-class sampling, so a module's work scales with the size of the diff."""
    return max(MIN_MUTANTS_PER_CLASS,
               min(MAX_MUTANTS_PER_CLASS, MUTANT_BUDGET // max(1, len(targets))))


def test_scope(module_root, sources, associations=None):
    """Bounded per-source test selection. No input may widen the scope to the module suite.

    `associations` maps a changed top-level class to the existing behaviour tests the lane must
    also run for it (CONV-204); they augment the name-based matches and share the module cap.
    """
    associations = associations or {}
    candidates = []
    tests = module_test_classes(module_root)
    for source in sources:
        stem, package = Path(source).stem, source_package(module_root, source)
        patterns, kind = resolve_tests(stem, package, tests)
        associated = set(associations.get(f'{package}.{stem}' if package else stem, ())) & tests.keys()
        if associated:
            patterns = sorted(set(patterns) | associated)
            if kind == 'unmapped':
                kind = 'related'
        candidates.append({'source': source, 'kind': kind, 'patterns': patterns})
    # Admit named matches before related/package matches; any candidate can exceed the cap.
    selected = set()
    for kind in KIND_ORDER:
        for candidate in candidates:
            if candidate['kind'] != kind:
                continue
            merged = selected | set(candidate['patterns'])
            if len(merged) > MODULE_TEST_LIMIT:
                candidate['kind'] = 'over-limit'
                continue
            selected = merged
    # A trailing glob reaches @Nested classes and may also match other name-prefixed classes.
    return (sorted(name + '*' for name in selected),
            sorted(candidate['source'] for candidate in candidates
                   if candidate['kind'] in UNRESOLVED_KINDS),
            {candidate['source']: candidate['kind'] for candidate in candidates})


def mutant_line(element):
    try:
        return int(element.findtext('lineNumber', ''))
    except (TypeError, ValueError):
        return 0


def classify_mutant(element, index):
    """Judge a mutant only when its own line is in the PR's diff; the rest stays advisory."""
    source_file = element.findtext('sourceFile', '')
    package = element.findtext('mutatedClass', '').rpartition('.')[0]
    lines = index.get((package, source_file))
    if lines is None:
        return 'unchanged', False
    return ('changed' if mutant_line(element) in lines else 'unchanged'), True


def describe_mutant(element):
    fields = [element.get('status', 'UNKNOWN')]
    fields.extend(element.findtext(tag, '?') for tag in
                  ('mutatedClass', 'mutatedMethod', 'lineNumber', 'description'))
    return ' | '.join(fields)


def undetected(elements):
    return [element for element in elements if element.get('detected') != 'true']


def tally(elements):
    counts = Counter(element.get('status', 'UNKNOWN') for element in elements)
    return {'total': len(elements), 'killed': counts['KILLED'], 'survived': counts['SURVIVED'],
            'no_coverage': counts['NO_COVERAGE'],
            'detected': sum(element.get('detected') == 'true' for element in elements)}


def read_report(path, index):
    mutations = ET.parse(path).getroot().findall('mutation')
    if not mutations:
        raise ValueError('PIT produced no mutations; no score is available')
    groups = {'changed': [], 'unchanged': []}
    unattributed = 0
    records = []
    for element in mutations:
        kind, attributed = classify_mutant(element, index)
        groups[kind].append(element)
        unattributed += not attributed
        records.append(mutant_record(element, kind == 'changed'))
    changed = tally(groups['changed'])
    changed['survivors'] = [describe_mutant(element) for element in undetected(groups['changed'])]
    # Until dispositions are applied, every undetected changed-line mutant fails (CONV-202).
    changed['failing'] = [{'mutant': value, 'reason': 'no disposition entry'}
                          for value in changed['survivors']]
    advisory = tally(groups['unchanged'])
    debt = Counter(element.findtext('mutatedClass', '?')
                   for element in undetected(groups['unchanged']))
    advisory['classes'] = [[name, count] for name, count in debt.most_common(ADVISORY_CLASS_LIMIT)]
    report = tally(mutations)
    report.update(statuses=dict(sorted(Counter(element.get('status', 'UNKNOWN')
                                               for element in mutations).items())),
                  survivors=[describe_mutant(element) for element in undetected(mutations)],
                  changed=changed, unchanged=advisory, unattributed=unattributed,
                  _mutants=records)
    return report


def is_clean(results):
    """Only failing changed-line mutants and incomplete analysis fail a module (CONV-202)."""
    for result in results:
        if result['state'] != 'complete' or result.get('total', 0) <= 0:
            return False
        # A report whose mutants all failed to map to a changed source judged nothing at all.
        if result['total'] - result.get('unattributed', 0) <= 0:
            return False
        changed = result.get('changed')
        # A complete report without a changed-line verdict is incomplete, never clean.
        if not changed:
            return False
        if 'failing' in changed:
            accepted = (len(changed.get('dispositioned_equivalent', ()))
                        + len(changed.get('dispositioned_out_of_lane', ())))
            # Accepted and failing mutants must account for every undetected one.
            if changed['failing'] or changed['detected'] + accepted != changed['total']:
                return False
        elif changed['detected'] != changed['total']:
            return False
    return True


# ---------------------------------------------------------------------------------------------
# Reviewed dispositions (CONV-201 to CONV-203) and test associations (CONV-204).

KINDS = ('equivalent', 'killed-out-of-lane')
IDENTITY_FIELDS = ('class', 'method', 'descriptor', 'mutator', 'indexes', 'description')
COMMON_FIELDS = IDENTITY_FIELDS + ('kind', 'checked_at', 'justification')
KIND_FIELDS = {'equivalent': frozenset(COMMON_FIELDS),
               'killed-out-of-lane': frozenset(COMMON_FIELDS + ('test', 'evidence'))}
EVIDENCE_FIELDS = frozenset(('patch', 'command', 'baseline_exit', 'injected_exit', 'failure',
                             'sources'))
JUSTIFICATION_MINIMUM = 60
PLACEHOLDERS = re.compile(r'hard to test|difficult to test|untestable|not testable'
                          r'|\b(?:todo|tbd|fixme)\b', re.IGNORECASE)
# Identity fields are compared as exact text; a pattern character is a sign of a waiver attempt.
PATTERN_CHARACTERS = frozenset('*?[]{}|\\')
IDENTIFIER = r'[A-Za-z_$][A-Za-z0-9_$]*'
QUALIFIED_NAME = re.compile(rf'{IDENTIFIER}(?:\.{IDENTIFIER})*')
TOP_LEVEL_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*')
METHOD_NAME = re.compile(r'<init>|<clinit>|[^.;\[\]/<>\s*?{}|\\]+')
FIELD_TYPE = r'(?:[BCDFIJSZ]|L[^;.\[\]<>\s*?{}|\\]+;)'
METHOD_DESCRIPTOR = re.compile(rf'\((?:\[*{FIELD_TYPE})*\)(?:V|\[*{FIELD_TYPE})')
TEST_NAME = re.compile(rf'({IDENTIFIER}(?:\.{IDENTIFIER})*)#({IDENTIFIER})')
SHA256 = re.compile(r'[0-9a-f]{64}')
# Entry states that fail the run (CONV-202); every other state is reported only.
FAILING_STATES = ('stale', 'contradicted', 'superseded', 'needs re-confirmation', 'unconfirmed')
SUMMARY_JUSTIFICATION_LIMIT = 300
REMAP_HINT = ('run `python3 .github/scripts/pr_mutation.py dispositions --remap --report '
              '<mutations.xml>` locally, review the candidates and update the entry, or delete it')


def identity_of(entry):
    return (entry['class'], entry['method'], entry['descriptor'], entry['mutator'],
            tuple(entry['indexes']), entry['description'])


def entry_key(entry):
    """Every field but the file position: equal keys mean an unchanged, already reviewed entry."""
    return json.dumps({name: value for name, value in entry.items() if name != 'position'},
                      sort_keys=True)


def is_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def nonempty_text(entry, name, problems):
    value = entry.get(name)
    if not isinstance(value, str) or not value.strip():
        problems.append(f'`{name}` must be a non-empty string')
        return None
    return value


def patch_paths(patch):
    """Repository paths a unified diff writes; a header is a `+++` line right after `---`."""
    paths, previous = [], ''
    for line in patch.splitlines():
        if line.startswith('+++ ') and previous.startswith('--- '):
            target = line[len('+++ '):].split('\t')[0]
            if target != '/dev/null':
                if not target.startswith('b/'):
                    raise ValueError(f'patch target {target!r} lacks the `b/` prefix')
                paths.append(repository_path(target[len('b/'):]))
        previous = line
    if not paths or not any(line.startswith('@@') for line in patch.splitlines()):
        raise ValueError('patch is not a unified diff with a file header and a hunk')
    return paths


def evidence_problems(evidence):
    if not isinstance(evidence, dict):
        return ['`evidence` must be a table']
    problems = []
    if evidence.keys() != EVIDENCE_FIELDS:
        problems.append('`evidence` must have exactly the fields '
                        + ', '.join(sorted(EVIDENCE_FIELDS)))
        return problems
    touched = []
    patch = nonempty_text(evidence, 'patch', problems)
    if patch is not None:
        try:
            touched = patch_paths(patch)
        except ValueError as error:
            problems.append(f'`evidence.patch`: {error}')
    command = evidence['command']
    if (not isinstance(command, list) or not command
            or not all(isinstance(part, str) and part for part in command)):
        problems.append('`evidence.command` must be a non-empty list of non-empty strings')
    if not is_integer(evidence['baseline_exit']) or evidence['baseline_exit'] != 0:
        problems.append('`evidence.baseline_exit` must be 0: the test passes without the mutant')
    if not is_integer(evidence['injected_exit']) or evidence['injected_exit'] == 0:
        problems.append('`evidence.injected_exit` must be a non-zero integer: the mutant fails the test')
    nonempty_text(evidence, 'failure', problems)
    sources = evidence['sources']
    if not isinstance(sources, list) or not sources:
        problems.append('`evidence.sources` must be a non-empty array of tables')
        return problems
    hashed = []
    for source in sources:
        if (not isinstance(source, dict) or source.keys() != {'path', 'sha256'}
                or not isinstance(source['path'], str) or not isinstance(source['sha256'], str)
                or not SHA256.fullmatch(source['sha256'])):
            problems.append('each `evidence.sources` entry needs exactly `path` and a lowercase '
                            'hexadecimal SHA-256 `sha256`')
            return problems
        try:
            hashed.append(repository_path(source['path']))
        except ValueError as error:
            problems.append(f'`evidence.sources`: {error}')
            return problems
    if len(set(hashed)) != len(hashed) or (touched and set(hashed) != set(touched)):
        problems.append('`evidence.sources` must hash each file the patch touches exactly once')
    return problems


def entry_problems(entry):
    """Every CONV-201 rule an entry violates; an empty list means the entry is well formed."""
    kind = entry.get('kind')
    if kind not in KINDS:
        return [f'`kind` must be one of {", ".join(KINDS)}, not {kind!r}']
    problems = []
    missing, unknown = KIND_FIELDS[kind] - entry.keys(), entry.keys() - KIND_FIELDS[kind]
    if missing:
        problems.append('missing field(s): ' + ', '.join(sorted(missing)))
    if unknown:
        problems.append('unknown field(s): ' + ', '.join(sorted(unknown)))
    if problems:
        return problems
    grammar = {'class': QUALIFIED_NAME, 'mutator': QUALIFIED_NAME, 'method': METHOD_NAME,
               'descriptor': METHOD_DESCRIPTOR}
    for name, pattern in grammar.items():
        value = nonempty_text(entry, name, problems)
        if value is not None and not pattern.fullmatch(value):
            problems.append(f'`{name}` is not an exact, well-formed value: {value!r}')
    description = nonempty_text(entry, 'description', problems)
    if description is not None and (PATTERN_CHARACTERS & set(description)
                                    or any(ord(character) < 32 for character in description)):
        problems.append('`description` must be exact text without pattern or control characters')
    indexes = entry['indexes']
    if (not isinstance(indexes, list) or not indexes
            or not all(is_integer(index) and index >= 0 for index in indexes)):
        problems.append('`indexes` must be a non-empty list of non-negative integers')
    checked = nonempty_text(entry, 'checked_at', problems)
    if checked is not None and not re.fullmatch(r'[0-9a-f]{40}|[0-9a-f]{64}', checked):
        problems.append('`checked_at` must be a full lowercase hexadecimal commit ID')
    justification = nonempty_text(entry, 'justification', problems)
    if justification is not None:
        if len(' '.join(justification.split())) < JUSTIFICATION_MINIMUM:
            problems.append(f'`justification` must argue the claim in at least '
                            f'{JUSTIFICATION_MINIMUM} characters')
        placeholder = PLACEHOLDERS.search(justification)
        if placeholder:
            problems.append(f'`justification` contains the placeholder {placeholder[0]!r}')
    if kind == 'killed-out-of-lane':
        test = nonempty_text(entry, 'test', problems)
        if test is not None and not TEST_NAME.fullmatch(test):
            problems.append('`test` must be `<binary test class name>#<method name>`')
        problems.extend(evidence_problems(entry['evidence']))
    return problems


def parse_dispositions(text):
    """(entries, errors) of a dispositions file; `None` text is an absent file with no entries."""
    if text is None:
        return [], []
    try:
        document = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        return [], [f'{DISPOSITIONS_FILE} is not valid TOML: {error}']
    if document.keys() - {'mutant'}:
        return [], [f'{DISPOSITIONS_FILE}: the only top-level key is `mutant`, found '
                    + ', '.join(sorted(document.keys() - {'mutant'}))]
    mutants = document.get('mutant', [])
    if not isinstance(mutants, list) or not all(isinstance(entry, dict) for entry in mutants):
        return [], [f'{DISPOSITIONS_FILE}: `mutant` must be an array of tables ([[mutant]])']
    entries, errors, seen = [], [], {}
    for position, entry in enumerate(mutants, 1):
        problems = entry_problems(entry)
        if problems:
            errors.extend(f'entry {position}: {problem}' for problem in problems)
            continue
        identity = identity_of(entry)
        if identity in seen:
            errors.append(f'entry {position}: repeats the identity of entry {seen[identity]}')
            continue
        seen[identity] = position
        entries.append({**entry, 'position': position})
    return entries, errors


def parse_associations(text):
    """(class -> tests, errors) of an associations file; structure and names only (CONV-204)."""
    if text is None:
        return {}, []
    try:
        document = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        return {}, [f'{ASSOCIATIONS_FILE} is not valid TOML: {error}']
    rows = document.get('association', [])
    if document.keys() - {'association'} or not isinstance(rows, list):
        return {}, [f'{ASSOCIATIONS_FILE}: the only top-level key is `association`, '
                    f'an array of tables']
    associations, errors = {}, []
    for position, row in enumerate(rows, 1):
        if not isinstance(row, dict) or row.keys() != {'class', 'tests'}:
            errors.append(f'association {position}: needs exactly the fields `class` and `tests`')
            continue
        name, tests = row['class'], row['tests']
        if not isinstance(name, str) or not TOP_LEVEL_NAME.fullmatch(name):
            errors.append(f'association {position}: `class` must be an exact top-level binary '
                          f'class name, not {name!r}')
            continue
        if (not isinstance(tests, list) or not tests or len(set(map(str, tests))) != len(tests)
                or not all(isinstance(test, str) and TOP_LEVEL_NAME.fullmatch(test)
                           for test in tests)):
            errors.append(f'association {position}: `tests` must be a non-empty list of distinct, '
                          f'exact top-level test class names')
            continue
        if name in associations:
            errors.append(f'association {position}: repeats the class {name}')
            continue
        associations[name] = tuple(tests)
    return associations, errors


def declaring_module(root, name):
    """The module whose main sources declare top-level class `name`, if any."""
    package, _, simple = name.rpartition('.')
    for directory, module in sorted(MODULES.items()):
        for language, suffix in (('java', '.java'), ('kotlin', '.kt')):
            path = (root / 'bundles' / directory / 'src/main' / language
                    / Path(*package.split('.')) / (simple + suffix)) if package else (
                root / 'bundles' / directory / 'src/main' / language / (simple + suffix))
            if path.is_file() and declared_package(path.read_text()) == package:
                return module
    return None


def load_associations(root):
    """The validated associations; any invalid entry voids the file and fails the run."""
    path = root / ASSOCIATIONS_FILE
    associations, errors = parse_associations(path.read_text() if path.is_file() else None)
    declared = {}
    for name, tests in associations.items():
        module = declaring_module(root, name)
        if module is None:
            errors.append(f'association {name}: no main source of a lane module declares it; '
                          f'update or delete the association')
            continue
        if module not in declared:
            declared[module] = module_test_classes(root / 'bundles' / DIRECTORIES[module])
        absent = [test for test in tests if test not in declared[module]]
        if absent:
            errors.append(f'association {name}: {", ".join(absent)} not declared in the tests of '
                          f'{module}; update or delete the association')
    return ({} if errors else associations), errors


class CompiledClasses:
    """Read compiled main classes of every lane module on demand, by exact binary name."""

    def __init__(self, root):
        self.root = root
        self.cache = {}

    def find(self, name):
        if name not in self.cache:
            self.cache[name] = None
            relative = Path(*name.split('.'))
            for directory in sorted(MODULES):
                for language in ('java', 'kotlin'):
                    path = (self.root / 'bundles' / directory / f'build/classes/{language}/main'
                            / relative.parent / (relative.name + '.class'))
                    if path.is_file():
                        self.cache[name] = read_class(path.read_bytes())
                        break
                if self.cache[name] is not None:
                    break
        return self.cache[name]


def declared_tests(root):
    """Top-level test class names declared anywhere in the lane modules."""
    names = set()
    for directory in sorted(MODULES):
        module_root = root / 'bundles' / directory
        if module_root.is_dir():
            names.update(module_test_classes(module_root))
    return names


def mutant_identity(element):
    indexes = []
    for index in element.findall('indexes/index'):
        try:
            indexes.append(int(index.text))
        except (TypeError, ValueError):
            indexes.append(-1)  # never equal to a validated entry index
    return (element.findtext('mutatedClass', ''), element.findtext('mutatedMethod', ''),
            element.findtext('methodDescription', ''), element.findtext('mutator', ''),
            tuple(indexes), element.findtext('description', ''))


def mutant_record(element, changed):
    return {'identity': mutant_identity(element), 'status': element.get('status', 'UNKNOWN'),
            'detected': element.get('detected') == 'true', 'line': mutant_line(element),
            'changed': changed, 'text': describe_mutant(element),
            'killing_test': element.findtext('killingTest', '')}


def method_changed(compiled, entry, changed_index):
    """Whether a line of the entry's method, per its line-number table, is a changed line."""
    found = compiled.find(entry['class'])
    if found is None:
        return False
    name, source, methods = found
    lines = methods.get((entry['method'], entry['descriptor']), frozenset())
    return bool(lines & changed_index.get((name.rpartition('.')[0], source), set()))


def static_staleness(compiled, entry, tests):
    """A reason the entry is stale from the build alone, independent of PIT's sample."""
    found = compiled.find(entry['class'])
    if found is None:
        return (f"class {entry['class']} is in no module's compiled main classes; if it was "
                f'renamed or moved, {REMAP_HINT}')
    if (entry['method'], entry['descriptor']) not in found[2]:
        return (f"method {entry['method']}{entry['descriptor']} no longer exists in "
                f"{entry['class']}; {REMAP_HINT}")
    if entry['kind'] == 'killed-out-of-lane':
        test_class = entry['test'].partition('#')[0].partition('$')[0]
        if test_class not in tests():
            return (f'test class {test_class} is no longer declared; name the test that now '
                    f'observes the effect and re-run the counterfactual, or delete the entry')
    return None


def judge_entry(entry, context):
    """(state, action) of one valid entry under CONV-202's rules, first matching rule wins."""
    identity = identity_of(entry)
    added = entry_key(entry) not in context['base_keys']
    new = added and identity not in context['base_identities']
    stale = static_staleness(context['compiled'], entry, context['tests'])
    if stale:
        return 'stale', stale
    mutant = context['mutants'].get(identity)
    if mutant and entry['kind'] == 'equivalent' and mutant['status'] == 'KILLED':
        return 'contradicted', (f"{mutant['killing_test'] or 'a test'} detects this mutant, so "
                                f'it is not equivalent; delete the entry')
    if mutant and entry['kind'] == 'killed-out-of-lane' and mutant['detected']:
        return 'superseded', 'an in-lane test now detects this mutant; delete the entry'
    if not added and (method_changed(context['compiled'], entry, context['changed_index'])
                      or (mutant and mutant['changed'])):
        return 'needs re-confirmation', (
            f"this PR changes method {entry['method']}; re-review the argument"
            + (' and re-run the counterfactual' if entry['kind'] == 'killed-out-of-lane' else '')
            + ', then update the entry, at least `checked_at`')
    if mutant and mutant['detected']:
        return 'detected-other', f"matches a {mutant['status']} mutant; reported, not failed"
    if mutant and mutant['status'] == 'SURVIVED':
        if added:
            return 'accepted', ('on a changed line' if mutant['changed']
                                else 'on an unchanged line of a changed file')
        return 'advisory', 'disposes of an unchanged-line survivor; advisory only'
    if mutant:
        if added:
            return 'unconfirmed', (f"matches a {mutant['status']} mutant; only SURVIVED mutants "
                                   f'can be dispositioned (NO_COVERAGE: see CONV-202)')
        return 'inactive', f"matches a {mutant['status']} mutant of an unchanged method"
    complete = context['mutated'].get(entry['class'])
    if complete:
        return 'stale', f'PIT no longer generates this mutant for its class; {REMAP_HINT}'
    if new:
        where = ("not in this run's sample of its class" if complete is not None
                 else 'its class was not mutated in this run')
        return 'unconfirmed', (f'no reported mutant matches ({where}); a new entry must match a '
                               f'SURVIVED mutant of a class this PR changes')
    if complete is not None:
        return 'not judged', 'no reported mutant matches and the class sample is incomplete'
    if added:
        return 'not judged', 'no reported mutant matches; its class was not mutated in this run'
    return 'out of scope', 'class not mutated in this run'


def judge_dispositions(entries, errors, base_text, context):
    """Judge every entry; `context` holds the run's mutants, classes and changed lines."""
    base_entries, base_errors = parse_dispositions(base_text)
    if base_errors:
        base_entries = []  # an invalid base makes every entry new: stricter, never laxer
    context = {**context, 'base_keys': {entry_key(entry) for entry in base_entries},
               'base_identities': {identity_of(entry) for entry in base_entries}}
    head_keys = {entry_key(entry) for entry in entries}
    judged, accepted = [], {}
    for entry in entries:
        state, action = judge_entry(entry, context)
        added = entry_key(entry) not in context['base_keys']
        record = {name: entry[name] for name in COMMON_FIELDS}
        record.update(indexes=list(entry['indexes']), position=entry['position'], state=state,
                      action=action, failing=state in FAILING_STATES,
                      added=('re-confirmation' if added and identity_of(entry)
                             in context['base_identities'] else 'new') if added else None)
        if entry['kind'] == 'killed-out-of-lane':
            record['test'] = entry['test']
        if state == 'accepted':
            accepted[identity_of(entry)] = record
        judged.append(record)
    added = [record for record in judged if record['added']]
    return {'file': DISPOSITIONS_FILE, 'errors': errors,
            'base': 'absent' if base_text is None else ('invalid' if base_errors else 'valid'),
            'added': len(added), 'new': sum(record['added'] == 'new' for record in added),
            're_confirmed': sum(record['added'] == 're-confirmation' for record in added),
            'removed': sum(entry_key(entry) not in head_keys for entry in base_entries)
                       - sum(record['added'] == 're-confirmation' for record in added),
            'entries': judged, 'failing': sum(record['failing'] for record in judged),
            'clean': not errors and not any(record['failing'] for record in judged)}, accepted


def apply_dispositions(result, accepted, judged):
    """Decide each undetected changed-line mutant of one module: dispositioned or failing."""
    changed = result.get('changed')
    if not changed:
        return
    by_identity = {}
    for record in judged:
        by_identity.setdefault(tuple(record[name] if name != 'indexes' else tuple(record[name])
                                     for name in IDENTITY_FIELDS), record)
    equivalent, out_of_lane, failing = [], [], []
    for mutant in result.get('_mutants', []):
        if not mutant['changed'] or mutant['detected']:
            continue
        entry = accepted.get(mutant['identity'])
        if mutant['status'] == 'SURVIVED' and entry:
            target = equivalent if entry['kind'] == 'equivalent' else out_of_lane
            target.append({'mutant': mutant['text'], 'entry': entry['position'],
                           'checked_at': entry['checked_at'], 'test': entry.get('test'),
                           'justification': entry['justification']})
            continue
        matched = by_identity.get(mutant['identity'])
        if mutant['status'] == 'NO_COVERAGE':
            reason = ('NO_COVERAGE is never dispositioned: test the specified behaviour that '
                      'needs this code, remove code with no specified behaviour, or associate '
                      'the existing tests that cover it (CONV-202, CONV-204)')
        elif mutant['status'] != 'SURVIVED':
            reason = f"{mutant['status']} is not a dispositionable status"
        elif matched:
            reason = f"entry {matched['position']} is {matched['state']}: {matched['action']}"
        else:
            reason = 'no disposition entry'
        failing.append({'mutant': mutant['text'], 'reason': reason})
    changed.update(dispositioned_equivalent=equivalent, dispositioned_out_of_lane=out_of_lane,
                   failing=failing)


def escape(value):
    """Job-summary text that cannot inject Markdown, HTML or a table cell break."""
    return html.escape(str(value)).replace('|', '&#124;').replace('\n', ' ')


def format_verdict(result):
    """The changed-line verdict: the counts that decide the outcome, then their mutants."""
    changed = result.get('changed')
    if not changed:
        return []
    # The changed-line count beside the mutant count keeps a sampled-away verdict visible.
    counted = (f"Changed lines: {result['changed_line_count']}. "
               if 'changed_line_count' in result else '')
    equivalent = changed.get('dispositioned_equivalent', [])
    out_of_lane = changed.get('dispositioned_out_of_lane', [])
    failing = changed.get('failing', [{'mutant': value, 'reason': 'undetected'}
                                      for value in changed['survivors']])
    lines = [f"{counted}Mutants on changed lines: {changed['total']}. "
             f"Killed: {changed['killed']}. Survived: {changed['survived']}. "
             f"No coverage: {changed['no_coverage']}.",
             '', f"Otherwise detected: {changed['detected'] - changed['killed']}. "
             f"Dispositioned as equivalent: {len(equivalent)}. "
             f"Dispositioned as killed out of lane: {len(out_of_lane)}. "
             f"Failing: {len(failing)}.", '']
    if failing:
        lines.extend(['Failing mutants on changed lines (these decide the outcome; '
                      'every one is in `results.json`):', ''])
        lines.extend(f"- {escape(value['mutant'])} | {escape(value['reason'])}"
                     for value in failing[:SUMMARY_MUTANT_LIMIT])
        if len(failing) > SUMMARY_MUTANT_LIMIT:
            lines.append('- Remaining entries: see `results.json`.')
        lines.append('')
    for title, values in (('Dispositioned as equivalent', equivalent),
                          ('Dispositioned as killed by an out-of-lane test', out_of_lane)):
        if values:
            lines.extend([f'{title}:', ''])
            lines.extend(f"- {escape(value['mutant'])} | entry {value['entry']}"
                         + (f" | test {escape(value['test'])}" if value.get('test') else '')
                         + f" | checked at {escape(value['checked_at'])} | "
                         + escape(value['justification'][:SUMMARY_JUSTIFICATION_LIMIT])
                         + ('...' if len(value['justification']) > SUMMARY_JUSTIFICATION_LIMIT
                            else '')
                         for value in values[:SUMMARY_MUTANT_LIMIT])
            lines.append('')
    return lines


def format_report(result):
    """The whole report, for context: PIT's own score over every mutated class."""
    if 'total' not in result:
        return []
    total = result['total']
    lines = [f"Killed: {result['killed']}/{total} ({100 * result['killed'] / total:.1f}%). "
             f"Survived: {result.get('survived', 0)}. "
             f"No coverage: {result.get('no_coverage', 0)}. "
             f"Detected (PIT score, includes timeouts): {result['detected']}/{total} "
             f"({100 * result['detected'] / total:.1f}%).", '', escape(result['statuses']), '']
    if result.get('unattributed'):
        lines.extend([f"Mutants whose source file did not map to a changed source: "
                      f"{result['unattributed']}.", ''])
    return lines


def format_advisory(result):
    """Unchanged-line debt: counts and the classes carrying the most of it. Never blocking."""
    advisory = result.get('unchanged')
    if not advisory or not advisory['total']:
        return []
    lines = [f"Unchanged lines of the same classes (advisory, never blocking). "
             f"Mutants: {advisory['total']}. Killed: {advisory['killed']}. "
             f"Survived: {advisory['survived']}. No coverage: {advisory['no_coverage']}.", '']
    if advisory['classes']:
        lines.extend(['Classes with the most undetected unchanged-line mutants:', ''])
        lines.extend(f'- {escape(name)}: {escape(count)}' for name, count in advisory['classes'])
        lines.append('')
    return lines


def entry_label(record):
    return escape(f"entry {record['position']} [{record['kind']}] {record['class']}."
                  f"{record['method']}{record['descriptor']} | {record['mutator']} | "
                  f"indexes {record['indexes']} | {record['description']}")


def format_dispositions(dispositions, associations):
    """The disposition section: counts first, then every added claim and every entry to act on."""
    if dispositions is None:
        return []
    lines = ['## Mutation dispositions', '',
             f"Entries added: {dispositions['added']} (new: {dispositions['new']}, "
             f"re-confirmed: {dispositions['re_confirmed']}). Removed: {dispositions['removed']}. "
             f"Failing entries: {dispositions['failing']}. Merge-base file: {dispositions['base']}.",
             '']
    if dispositions['errors']:
        lines.extend([f"`{dispositions['file']}` is invalid; no entry applies:", ''])
        lines.extend('- ' + escape(error) for error in dispositions['errors'])
        lines.append('')
    added = [record for record in dispositions['entries'] if record['added']]
    if added:
        lines.extend(['### Added entries: review each claim (CONV-203)', ''])
        for record in added:
            test = f" | test {escape(record['test'])}" if record.get('test') else ''
            lines.append(f"- {entry_label(record)}{test} | {escape(record['added'])} | state "
                         f"{escape(record['state'])} | checked at {escape(record['checked_at'])}"
                         f" | {escape(record['justification'])}")
        lines.append('')
    acting = [record for record in dispositions['entries'] if record['failing']]
    if acting:
        lines.extend(['### Entries that fail this run', ''])
        lines.extend(f"- {entry_label(record)} | {escape(record['state'])}: "
                     f"{escape(record['action'])}" for record in acting)
        lines.append('')
    reported = [record for record in dispositions['entries']
                if not record['failing'] and record['state'] in
                ('not judged', 'detected-other', 'inactive', 'advisory')]
    if reported:
        lines.extend(['### Reported entries (not failing)', ''])
        lines.extend(f"- {entry_label(record)} | {escape(record['state'])}: "
                     f"{escape(record['action'])}" for record in reported)
        lines.append('')
    if associations and associations['errors']:
        lines.extend([f"`{ASSOCIATIONS_FILE}` is invalid; no association applies:", ''])
        lines.extend('- ' + escape(error) for error in associations['errors'])
        lines.append('')
    return lines


def format_summary(results, dispositions=None, associations=None):
    lines = ['# Changed-source mutation testing', '',
             'Blocking criterion: undetected mutants on the lines this pull request changed, '
             'unless a reviewed disposition accepts them (CONV-202). '
             'Mutants on unchanged lines of the same classes are advisory pre-existing debt. '
             'Informational unless `MUTATION_BLOCKING` is enabled. Full reports, targets and logs '
             'are in the `pr-mutation` artifact.', '']
    lines.extend(format_dispositions(dispositions, associations))
    if not results:
        return '\n'.join(lines + ['No changed main Java/Kotlin sources; PIT skipped.', ''])
    for result in results:
        lines.extend([f"## {escape(result['module'])}: {escape(result['state'])}", ''])
        if 'error' in result:
            lines.extend([escape(result['error']), ''])
        if result['state'] == 'unmapped':
            lines.extend(['No mutation analysis ran; no mutant has a status.', ''])
        if 'tests' in result:
            lines.extend([f"Changed target classes: {len(result['targets'])}. "
                          f"Test patterns: {len(result['tests'])}. "
                          f"Mutants sampled per class: {result['mutants_per_class']}.", ''])
        if result.get('unmapped'):
            lines.extend(['Changed sources for which no mapped test was selected:', ''])
            lines.extend('- ' + escape(value) for value in result['unmapped'])
            lines.append('')
        lines.extend(format_verdict(result))
        lines.extend(format_report(result))
        lines.extend(format_advisory(result))
    return '\n'.join(lines)


def execute(command, root, log, timeout):
    with log.open('w') as stream, subprocess.Popen(
            command, cwd=root, stdout=stream, stderr=subprocess.STDOUT,
            start_new_session=True, shell=False) as process:
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise


def prepare_analysis(root, directory, module, sources, result, associations=None):
    module_root = root / 'bundles' / DIRECTORIES[module]
    targets, missing = resolve_targets(module_root, sources)
    if missing:
        raise ValueError('No compiled classes for changed sources: ' + ', '.join(missing))
    if not targets:
        raise ValueError('Empty target selection; refusing whole-module mutation')
    tests, unmapped, mapping = test_scope(module_root, sources, associations)
    mutants = mutants_per_class(targets)
    result.update(targets=targets, tests=tests, unmapped=unmapped, resolution=mapping,
                  mutants_per_class=mutants)
    if not tests:
        raise Unmapped('No test maps to any changed source; PIT has no baseline to collect')
    target_file = directory / 'targets.txt'
    target_file.write_text('\n'.join(targets) + '\n')
    return [str(root / 'gradlew'), f':{module}:pitest', '--no-daemon',
            f'-PprMutationModule={module}', f'-PprMutationTargetsFile={target_file}',
            f'-PprMutationReportDir={directory / "report"}',
            f'-PprMutationMaxMutations={mutants}',
            f'-PprMutationTests={",".join(tests)}']


def attach_report(result, report, index):
    try:
        result.update(read_report(report, index))
    except (OSError, ValueError, ET.ParseError) as error:
        if result['state'] == 'complete':
            result.update(state='error', error=str(error))


def analyze_module(root, output, module, sources, lines, deadline, seconds, associations=None):
    result = {'module': module, 'sources': sources, 'state': 'error'}
    if module not in DIRECTORIES:
        return {**result, 'error': 'Unknown module; refusing to execute PIT'}
    directory = output / module
    directory.mkdir(parents=True, exist_ok=True)
    report = directory / 'report/mutations.xml'
    index = {}
    try:
        # Never attach cached/rerun XML to a new selection, even when mapping fails.
        if report.exists():
            report.unlink()
        index = changed_line_index(root / 'bundles' / DIRECTORIES[module], lines)
        result['changed_line_count'] = sum(len(numbers) for numbers in index.values())
        command = prepare_analysis(root, directory, module, sources, result, associations)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired('PIT module execution share', seconds)
        code = execute(command, root, directory / 'pitest.log', remaining)
        if code != 0:
            raise ValueError(f'PIT exited {code}; see {module}/pitest.log')
        result['state'] = 'complete'
    except subprocess.TimeoutExpired:
        result.update(state='timed out', error=f'{seconds}s module execution share exceeded')
    except Unmapped as error:
        result.update(state='unmapped', error=str(error))
    except (OSError, ValueError, IndexError, struct.error) as error:
        result.update(state='error', error=str(error))
    attach_report(result, report, index)
    return result


def write_results(output, results, dispositions=None, associations=None):
    modules = [{key: value for key, value in result.items() if not key.startswith('_')}
               for result in results]
    document = {'modules': modules, 'dispositions': dispositions, 'associations': associations}
    (output / 'results.json').write_text(json.dumps(document, indent=2) + '\n')
    (output / 'summary.md').write_text(format_summary(results, dispositions, associations))


def merge_base_file(root, base, path):
    """`path` at the merge base of `base` and the checked-out revision, or None when absent."""
    merge_base = subprocess.check_output(['git', '-C', str(root), 'merge-base', commit_id(base),
                                          'HEAD'], shell=False).decode().strip()
    listed = subprocess.check_output(['git', '--literal-pathspecs', '-C', str(root), 'ls-tree',
                                      '--name-only', commit_id(merge_base), '--', path],
                                     shell=False)
    if not listed.strip():
        return None
    return subprocess.check_output(['git', '-C', str(root), 'show', f'{merge_base}:{path}'],
                                   shell=False).decode('utf-8')


def global_changed_index(root, changed):
    index = {}
    for module, lines in changed.items():
        if module in DIRECTORIES:
            for key, numbers in changed_line_index(root / 'bundles' / DIRECTORIES[module],
                                                   lines).items():
                index.setdefault(key, set()).update(numbers)
    return index


def mutated_classes(results):
    """Classes mutated by a completed module, mapped to whether PIT's sample of them is complete.

    Only a run without a per-class limit proves a complete sample: PIT applies CLASSLIMIT before
    later equivalence filters, so a count below the limit may still be a sample (CONV-202).
    """
    mutated = {}
    for result in results:
        if result['state'] != 'complete':
            continue
        for target in result.get('targets', ()):
            mutated[target] = result.get('mutants_per_class') is None
    return mutated


def judge_run(root, results, changed, base_text):
    """Judge the dispositions file against this run and decide each module's changed mutants."""
    path = root / DISPOSITIONS_FILE
    entries, errors = parse_dispositions(path.read_text() if path.is_file() else None)
    if errors:
        entries = []  # an invalid file disposes of nothing (CONV-201)
    tests = []

    def test_names():
        if not tests:
            tests.append(declared_tests(root))
        return tests[0]
    mutants = {}
    for result in results:
        for mutant in result.get('_mutants', ()):
            mutants.setdefault(mutant['identity'], mutant)
    context = {'compiled': CompiledClasses(root), 'tests': test_names, 'mutants': mutants,
               'changed_index': global_changed_index(root, changed),
               'mutated': mutated_classes(results)}
    verdict, accepted = judge_dispositions(entries, errors, base_text, context)
    for result in results:
        apply_dispositions(result, accepted, verdict['entries'])
    return verdict


def read_selection(output):
    """The manifest `select` wrote: what to mutate, plus the lines that decide the outcome."""
    manifest = json.loads((output / 'selection.json').read_text())
    if not isinstance(manifest, dict) or not {'modules', 'changed_lines'} <= set(manifest):
        raise ValueError('selection.json carries no changed-line map; rerun the select command')
    selected, lines = manifest['modules'], manifest['changed_lines']
    base_text = manifest.get('dispositions_base')
    if base_text is not None and not isinstance(base_text, str):
        raise ValueError('selection.json carries a malformed dispositions_base')
    missing = []
    for module, sources in selected.items():
        mapped = lines.get(module)
        if mapped is None:
            missing.append(module)
            continue
        missing.extend(f'{module}/{source}' for source in sorted(set(sources) - set(mapped)))
    if missing:
        raise ValueError('Selection manifest has no changed lines for: ' + ', '.join(missing))
    return selected, lines, base_text


def run(root, output, seconds, blocking):
    selected, changed, base_text = read_selection(output)
    associations, association_errors = load_associations(root)
    association_verdict = {'file': ASSOCIATIONS_FILE, 'errors': association_errors,
                           'count': len(associations)}
    deadline = time.monotonic() + seconds
    results = []
    pending = len(selected)
    for module, sources in selected.items():
        # Reserve time for later modules while allowing this module to use the rest.
        # Divide equally if the remaining budget cannot supply every pending module's floor.
        remaining = max(0.0, deadline - time.monotonic())
        reserve = min(MIN_MODULE_SECONDS, remaining / pending)
        share = remaining - reserve * (pending - 1)
        results.append(analyze_module(root, output, module, sources, changed[module],
                                      time.monotonic() + share, round(share), associations))
        pending -= 1
        write_results(output, results, associations=association_verdict)
    dispositions = judge_run(root, results, changed, base_text)
    write_results(output, results, dispositions, association_verdict)
    clean = is_clean(results) and dispositions['clean'] and not association_errors
    return int(blocking and not clean)


# ---------------------------------------------------------------------------------------------
# Local, read-only helpers that keep a refactor cheap (CONV-203). CI never runs them.

FAILURE_EXCERPT_LINES = 40


class CounterfactualError(Exception):
    """The counterfactual run produced no valid evidence; the message says why."""


def report_records(paths, index):
    records = []
    for path in paths:
        for element in ET.parse(path).getroot().findall('mutation'):
            kind, _ = classify_mutant(element, index)
            records.append(mutant_record(element, kind == 'changed'))
    return records


def current_commit(root):
    return subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'],
                                   shell=False).decode().strip()


def read_dispositions(root):
    path = root / DISPOSITIONS_FILE
    return parse_dispositions(path.read_text() if path.is_file() else None)


def check_dispositions(root, reports, base, limit):
    """Judge the working tree's entries against local reports, as the lane would."""
    _, lines = changed_sources(root, base, current_commit(root))
    records = report_records(reports, global_changed_index(root, lines))
    classes = {record['identity'][0] for record in records}
    entries, errors = read_dispositions(root)
    context = {'compiled': CompiledClasses(root), 'tests': lambda: declared_tests(root),
               'mutants': {record['identity']: record for record in records},
               'changed_index': global_changed_index(root, lines),
               'mutated': dict.fromkeys(classes, limit is None)}
    verdict, accepted = judge_dispositions([] if errors else entries, errors,
                                           merge_base_file(root, base, DISPOSITIONS_FILE), context)
    module = {'changed': {'total': len(records)}, '_mutants': records}
    apply_dispositions(module, accepted, verdict['entries'])
    lines_out = [f'{DISPOSITIONS_FILE}: {len(entries)} valid entries, {len(errors)} errors']
    lines_out.extend(f'error: {error}' for error in errors)
    for record in verdict['entries']:
        if record['state'] != 'out of scope':
            marker = 'FAIL' if record['failing'] else 'ok'
            lines_out.append(f"{marker} entry {record['position']} [{record['state']}] "
                             f"{record['class']}.{record['method']}: {record['action']}")
    lines_out.extend(f"FAIL mutant {value['mutant']}: {value['reason']}"
                     for value in module['changed']['failing'])
    failing = bool(errors or verdict['failing'] or module['changed']['failing'])
    return failing, '\n'.join(lines_out)


def remap_candidates(entry, records):
    """The first non-empty tier of report mutants that may be this entry's mutant after a refactor."""
    identity = identity_of(entry)
    alike = [record for record in records if record['identity'] != identity
             and record['identity'][3] == identity[3] and record['identity'][5] == identity[5]]
    tiers = (('same method, other instruction indexes',
              lambda candidate: candidate[:3] == identity[:3]),
             ('renamed method of the same class', lambda candidate: candidate[0] == identity[0]),
             ('same method name and descriptor in another class',
              lambda candidate: candidate[1:3] == identity[1:3]))
    for label, accepts in tiers:
        matches = [record for record in alike if accepts(record['identity'])]
        if matches:
            # A rename or move keeps the instruction ordinals: list those candidates first.
            matches.sort(key=lambda record: record['identity'][4] != identity[4])
            return label, matches
    return None, []


def remap_dispositions(root, reports):
    """Propose identities for every entry no report mutant matches; never writes the file."""
    records = report_records(reports, {})
    present = {record['identity'] for record in records}
    entries, errors = read_dispositions(root)
    lines = [f'error: {error}' for error in errors]
    for entry in entries:
        if identity_of(entry) in present:
            continue
        label, matches = remap_candidates(entry, records)
        lines.append(f"entry {entry['position']} ({entry['class']}.{entry['method']}"
                     f"{entry['descriptor']}): no report mutant matches; "
                     + (f'{len(matches)} candidate(s), {label}:' if label else 'no candidate'))
        for record in matches:
            name, method, descriptor, mutator, indexes, description = record['identity']
            same = ', same instruction indexes' if indexes == identity_of(entry)[4] else ''
            lines.extend([f"  # line {record['line']}, {record['status']}{same}; re-review the "
                          f'argument and update checked_at before using it',
                          f'  class = {json.dumps(name)}', f'  method = {json.dumps(method)}',
                          f'  descriptor = {json.dumps(descriptor)}',
                          f'  mutator = {json.dumps(mutator)}', f'  indexes = {list(indexes)}',
                          f'  description = {json.dumps(description)}'])
    return '\n'.join(lines) if lines else 'Every entry matches a mutant of the given reports.'


def toml_string(value):
    """A TOML string that loads back to `value`: multi-line literal when safe, else basic."""
    if ('\n' in value and "'''" not in value and not value.endswith("'")
            and not any((ord(character) < 32 and character not in '\n\t') or ord(character) == 127
                        for character in value)):
        return "'''\n" + value + "'''"
    return json.dumps(value, ensure_ascii=False).replace('\x7f', '\\u007f')


def render_evidence(evidence):
    lines = ['[mutant.evidence]', f"patch = {toml_string(evidence['patch'])}",
             'command = [' + ', '.join(toml_string(part) for part in evidence['command']) + ']',
             f"baseline_exit = {evidence['baseline_exit']}",
             f"injected_exit = {evidence['injected_exit']}",
             f"failure = {toml_string(evidence['failure'])}"]
    for source in evidence['sources']:
        lines.extend(['', '[[mutant.evidence.sources]]', f"path = {toml_string(source['path'])}",
                      f"sha256 = {toml_string(source['sha256'])}"])
    return '\n'.join(lines) + '\n'


def run_test_command(command, root):
    return subprocess.run(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          shell=False, check=False)


def counterfactual(root, patch_file, command):
    """Inject the mutant patch, run the test in its own process, restore and hash-check sources."""
    if not command:
        raise CounterfactualError('no test command given after `--`')
    patch_file = Path(patch_file).resolve()
    patch = patch_file.read_text()
    try:
        paths = patch_paths(patch)
    except ValueError as error:
        raise CounterfactualError(str(error)) from error

    def hashes():
        return {path: hashlib.sha256((root / path).read_bytes()).hexdigest() for path in paths}
    before = hashes()
    baseline = run_test_command(command, root)
    if baseline.returncode != 0:
        raise CounterfactualError(f'the test fails without the mutant (exit {baseline.returncode})'
                                  f'; fix the baseline first')
    git = ['git', '-C', str(root), 'apply']
    if subprocess.run(git + ['--check', str(patch_file)], shell=False, check=False).returncode:
        raise CounterfactualError('the patch does not apply to the working tree')
    subprocess.run(git + [str(patch_file)], shell=False, check=True)
    try:
        injected = run_test_command(command, root)
    finally:
        reversed_cleanly = subprocess.run(git + ['-R', str(patch_file)], shell=False,
                                          check=False).returncode == 0
        if not reversed_cleanly or hashes() != before:
            raise CounterfactualError('sources differ after reversing the patch; restore '
                                      + ', '.join(paths) + ' from version control before '
                                      'continuing')
    if injected.returncode == 0:
        raise CounterfactualError('the test passes with the mutant injected; it does not detect '
                                  'this mutant')
    output = os.fsdecode(injected.stdout or b'').splitlines()
    return {'patch': patch, 'command': list(command), 'baseline_exit': 0,
            'injected_exit': injected.returncode,
            'failure': '\n'.join(output[-FAILURE_EXCERPT_LINES:]) or '(no output)',
            'sources': [{'path': path, 'sha256': digest} for path, digest in before.items()]}


def build_parser():
    """The CLI. `select` and `run` are the CI commands and take no disposition input (CONV-203)."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    select = commands.add_parser('select', help='record changed sources and lines (CI)')
    analysis = commands.add_parser('run', help='run bounded PIT and judge the result (CI)')
    for command in (select, analysis):
        command.add_argument('--output', type=Path, required=True)
    select.add_argument('--base')
    select.add_argument('--head')
    analysis.add_argument('--seconds', type=int, default=1320)
    analysis.add_argument('--blocking', action='store_true')
    dispositions = commands.add_parser(
        'dispositions', help='local: check entries or propose identities after a refactor')
    dispositions.add_argument('--report', type=Path, action='append', required=True,
                              help='a PIT mutations.xml; repeatable')
    dispositions.add_argument('--check', action='store_true')
    dispositions.add_argument('--remap', action='store_true')
    dispositions.add_argument('--base', help='commit the change is judged against (--check)')
    dispositions.add_argument('--limit', type=int,
                              help="the run's per-class mutant limit, if one was applied")
    evidence = commands.add_parser(
        'counterfactual', help='local: produce killed-out-of-lane evidence for one mutant')
    evidence.add_argument('--patch', type=Path, required=True,
                          help='unified diff injecting exactly the mutant')
    evidence.add_argument('test_command', nargs=argparse.REMAINDER,
                          help='after `--`: the command that runs the test in its own process')
    for command in (select, analysis, dispositions, evidence):
        command.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    root = args.root.resolve()
    if args.command == 'dispositions':
        if not args.check and not args.remap:
            parser.error('dispositions requires --check, --remap or both')
        if args.check and not args.base:
            parser.error('dispositions --check requires --base')
        failing = False
        if args.check:
            failing, text = check_dispositions(root, args.report, args.base, args.limit)
            print(text)
        if args.remap:
            print(remap_dispositions(root, args.report))
        return int(failing)
    if args.command == 'counterfactual':
        command = args.test_command[1:] if args.test_command[:1] == ['--'] else args.test_command
        try:
            print(render_evidence(counterfactual(root, args.patch, command)), end='')
        except CounterfactualError as error:
            print(f'No evidence: {error}', file=sys.stderr)
            return 1
        return 0
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if args.command == 'select':
        if not args.base or not args.head:
            parser.error('select requires --base and --head')
        selected, lines = changed_sources(root, args.base, args.head)
        manifest = {'modules': selected, 'changed_lines': lines,
                    'dispositions_base': merge_base_file(root, args.base, DISPOSITIONS_FILE)}
        (output / 'selection.json').write_text(json.dumps(manifest, indent=2) + '\n')
        print(json.dumps(manifest, indent=2))
        return 0
    if args.seconds <= 0:
        parser.error('--seconds must be positive')
    return run(root, output, args.seconds, args.blocking)


if __name__ == '__main__':
    raise SystemExit(main())
