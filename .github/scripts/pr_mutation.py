#!/usr/bin/env python3
"""Bounded, secret-free PR mutation analysis. See docs/VERIFICATION.md."""
import argparse
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import cached_property
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
# These entry points share an executable release contract whose name spans several classes.
# Augment the heuristic matches; the ordinary module cap still applies to their union.
CONTRACT_TESTS = {name: ('io.sirix.query.scan.VectorizedReleaseCorrectnessTest',) for name in (
    'io.sirix.query.SirixCompileChain',
    'io.sirix.query.compiler.translator.SirixBlockPipelineStrategy',
    'io.sirix.query.compiler.translator.SirixPipelineStrategy',
    'io.sirix.query.scan.VectorizedRouteRefusedException')}
# Explicit ownership contracts replace broad package fallbacks for these lifecycle owners.
# A package fallback pulled generated identity histories into the lifecycle lane while omitting
# the dedicated cancellation/retirement suites entirely.
LIFECYCLE_TESTS = {}
_DATABASE_TESTS = ('io.sirix.access.DatabaseOwnershipTest',
                   'io.sirix.access.DatabaseCloseStrandingTest',
                   'io.sirix.access.DatabaseNotificationLifecycleTest')
_SESSION_TESTS = ('io.sirix.access.trx.node.AbstractResourceSessionWriteFailureCleanupTest',
                  'io.sirix.access.trx.node.NodeTransactionAbortLifecycleTest',
                  'io.sirix.access.trx.node.SharedCallbackLifetimeTest')
_WRITER_TESTS = _SESSION_TESTS + ('io.sirix.access.trx.page.AsyncFlushFailurePathTest',
                                 'io.sirix.access.trx.page.WriterConstructionFailureCleanupTest')
for _name in ('DatabaseHandle', 'DatabaseManager', 'DatabaseManagerFactory', 'DatabaseNotifications',
              'Databases', 'LocalDatabase', 'MutationGate', 'ResourceStore', 'ResourceStoreImpl'):
    LIFECYCLE_TESTS[f'io.sirix.access.{_name}'] = _DATABASE_TESTS
for _package, _names, _contracts in (
        ('io.sirix.access.trx', ('TransactionImpl',), _SESSION_TESTS),
        ('io.sirix.access.trx.node', ('AbstractResourceSession', 'InternalResourceSession',
                                     'AbstractNodeReadOnlyTrx'), _SESSION_TESTS),
        ('io.sirix.access.trx.node', ('AbstractNodeTrxImpl', 'InternalNodeTrx'), _WRITER_TESTS),
        ('io.sirix.access.trx.node.json', ('JsonNodeTrxImpl', 'JsonResourceSessionImpl'), _WRITER_TESTS),
        ('io.sirix.access.trx.node.xml', ('XmlNodeTrxImpl', 'XmlResourceSessionImpl'), _WRITER_TESTS),
        ('io.sirix.access.trx.page', ('NodeStorageEngineWriter',), _WRITER_TESTS +
         ('io.sirix.access.trx.page.NodeStorageEngineWriterTruncateToRevisionIntegrationTest',)),
        ('io.sirix.api', ('Database', 'ResourceSession', 'NodeTrx', 'PostCommitHook', 'PreCommitHook'),
         _DATABASE_TESTS + _SESSION_TESTS),
        ('io.sirix.api.xml', ('XmlNodeTrx',), _WRITER_TESTS)):
    for _name in _names:
        LIFECYCLE_TESTS[f'{_package}.{_name}'] = _contracts
for _name in ('ConcurrentAxis', 'ConcurrentAxisHelper', 'ConcurrentExceptAxis',
              'ConcurrentIntersectAxis', 'ConcurrentUnionAxis'):
    LIFECYCLE_TESTS[f'io.sirix.axis.concurrent.{_name}'] = (
        'io.sirix.axis.concurrent.ConcurrentAxisLifecycleTest',)
LIFECYCLE_TESTS['io.sirix.api.Axis'] = ('io.sirix.axis.concurrent.ConcurrentAxisLifecycleTest',)
for _package, _names in (('io.sirix.axis', ('ForAxis', 'NestedAxis', 'NonStructuralWrapperAxis')),
                         ('io.sirix.axis.filter', ('FilterAxis', 'PredicateFilterAxis'))):
    for _name in _names:
        LIFECYCLE_TESTS[f'{_package}.{_name}'] = (
            'io.sirix.axis.concurrent.ConcurrentAxisLifecycleTest',)
LIFECYCLE_TESTS['io.sirix.index.projection.ProjectionIndexHOTStorage'] = (
    'io.sirix.index.projection.ProjectionParallelDirectoryWalkTest',)
for _name in ('DerefDescendantExpr', 'SirixLazySequence'):
    LIFECYCLE_TESTS[f'io.sirix.query.compiler.translator.{_name}'] = (
        'io.sirix.query.compiler.translator.TraversalCloseOwnershipTest',)
for _package, _name in (('json', 'SirixJsonStream'), ('node', 'SirixNodeStream')):
    LIFECYCLE_TESTS[f'io.sirix.query.stream.{_package}.{_name}'] = (
        'io.sirix.query.compiler.translator.TraversalCloseOwnershipTest',)
# Execution time reserved for each module still waiting, so none is starved by an earlier one.
MIN_MODULE_SECONDS = 120
# CONV-205: the lane mutates the PR's own lines and nothing else, and applies no per-class sample
# to them. A changed-line count is therefore a complete measurement. The in-repo CHANGEDLINES
# interceptor reads this map and appends the identity of every mutant it removed to the census,
# which it writes beside the map (ChangedLineInterceptorFactory.CENSUS_FILE).
CHANGED_LINES_FILE = 'changed-lines.tsv'
CHANGED_LINES_HEADER = '# sirix-changed-lines 1'
CENSUS_FILE = 'census.jsonl'
# PIT's XML listener serialises at the end of the run, so a killed process wrote a 0-byte report
# and discarded every finished mutant. The STREAM listener flushes each analysed class as it
# arrives; the reporter falls back to it and tolerates a record torn by the kill (CONV-202).
STREAM_FILE = 'mutations-stream.xml'
# One reused daemon with a fixed heap instead of a single-use daemon per invocation. The value
# must be identical for every invocation of a run, or Gradle forks another daemon for each.
DAEMON_JVM_ARGS = '-Xmx3g'
# CONV-206: the scheduled full-scope report mutates every line of the sources the default branch
# changed within its window, which is stated in the report rather than fixed by the criterion.
WINDOW_DAYS = 7
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
        lines = set()  # abstract and native methods have no Code attribute
        for attribute, value in reader.attributes(pool):
            if attribute == 'Code':  # at most one per method (JVMS 4.7.3)
                lines = code_lines(value, pool)
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


def source_classes(path, file_facade=False):
    facade = path.stem[:1].upper() + path.stem[1:] + 'Kt'

    def strip(match):
        nonlocal facade
        if match[1] is not None:
            facade = match[1]
        return ' '

    text = re.sub(r'@file:(?:kotlin\.jvm\.)?JvmName\s*\(\s*"([\w$]+)"\s*\)'
                  r'|//[^\n]*|/\*.*?\*/|""".*?"""|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                  strip, path.read_text(), flags=re.S)
    text = re.sub(r'(?:::|\.)\s*(?:class|object)\b', ' ', text)
    package = declared_package(text)
    keywords = 'class|interface|enum|record' if path.suffix == '.java' else 'class|interface|object'
    depth, members = 0, False
    names = set()
    for token in re.finditer(r'\b(?:' + keywords + r')\s+([\w$]+)'
                             r'|\b(?:fun(?!\s+interface\b)|val|var)\b|[{}()\[\]]', text):
        if token[0] in ('{', '(', '['):
            depth += 1
        elif token[0] in ('}', ')', ']'):
            depth -= 1
        elif depth == 0:
            if token[1] is not None:
                names.add(token[1])
            else:
                members = True
    if file_facade and path.suffix == '.kt' and members:
        names.add(facade)
    return {f'{package}.{name}' if package else name: package for name in names}


def module_classes(module_root, source_set):
    classes = {}
    for language, suffix in (('java', '.java'), ('kotlin', '.kt')):
        root = module_root / 'src' / source_set / language
        for path in root.rglob('*' + suffix):
            if path.name not in ('package-info.java', 'module-info.java'):
                classes.update(source_classes(path, file_facade=source_set == 'main'))
    return classes


def module_test_classes(module_root):
    """Discover declared top-level test types and packages without requiring compilation."""
    return module_classes(module_root, 'test')


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


def test_scope(module_root, sources, associations=None):
    """Bounded per-source test selection. No input may widen the scope to the module suite.

    `associations` maps a changed top-level class to the existing behaviour tests the lane must
    also run for it (CONV-204); they augment the name-based matches and share the module cap.
    """
    associations = associations or {}
    candidates = []
    tests = module_test_classes(module_root)
    for source in sources:
        path = source_path(module_root, source)
        classes = source_classes(path, file_facade=True)
        stem, package = Path(source).stem, declared_package(path.read_text())
        explicit = LIFECYCLE_TESTS.get(f'{package}.{stem}')
        if explicit is None:
            patterns, kind = resolve_tests(stem, package, tests)
        else:
            patterns = sorted(set(explicit) & tests.keys())
            kind = 'related' if patterns else 'unmapped'
        contracts = set(CONTRACT_TESTS.get(f'{package}.{stem}', ())) & tests.keys()
        if contracts:
            patterns = sorted(set(patterns) | contracts)
            if kind == 'unmapped':
                kind = 'related'
        associated = {test for name in classes for test in associations.get(name, ())} & tests.keys()
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


def read_report(path, index, allow_empty=False):
    return build_report(ET.parse(path).getroot().findall('mutation'), index, allow_empty)


def read_stream(path, index):
    """The statuses a killed run already recorded: whole records only, torn tail dropped.

    The STREAM listener writes one `mutation` element per line and flushes each analysed class, so
    an intact prefix is always parsable. A record the kill truncated is discarded rather than
    guessed at, and `None` means nothing survived (CONV-202).
    """
    mutations = []
    for record in path.read_bytes().splitlines():
        if not record.strip():
            continue
        try:
            mutations.append(ET.fromstring(record.decode('utf-8')))
        except (UnicodeError, ET.ParseError):
            break  # only the last write can be torn; stop rather than read past it
    if not mutations:
        return None
    report = build_report(mutations, index)
    report['partial'] = True
    return report


NO_MUTANTS = 'PIT produced no mutations; no score is available'


def build_report(mutations, index, allow_empty=False):
    if not mutations and not allow_empty:
        raise ValueError(NO_MUTANTS)
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
    advisory['classes'] = [[name, count] for name, count in debt.most_common()]
    report = tally(mutations)
    report['classes'] = [[name, count] for name, count in Counter(
        element.findtext('mutatedClass', '?') for element in undetected(mutations)).most_common()]
    report.update(statuses=dict(sorted(Counter(element.get('status', 'UNKNOWN')
                                               for element in mutations).items())),
                  survivors=[describe_mutant(element) for element in undetected(mutations)],
                  changed=changed, unchanged=advisory, unattributed=unattributed,
                  _mutants=records)
    return report


def is_clean(results):
    """Only failing changed-line mutants and incomplete analysis fail a module (CONV-202)."""
    for result in results:
        if result['state'] != 'complete':
            return False
        # The restriction removed a mutant the verdict judges: its own decision is not evidence,
        # so the reporter's independent attribution decides and this run proves nothing (CONV-205).
        if result.get('census', {}).get('violations'):
            return False
        # PIT mutated the targets and no mutant of theirs is on a changed line: nothing to judge.
        if result.get('off_diff_only'):
            continue
        # A report with no mutant, or whose mutants all failed to map to a changed source,
        # judged nothing at all. A complete module always has a report; one without mutants
        # returned earlier as the case its census explains.
        if result['total'] - result.get('unattributed', 0) <= 0:
            return False
        changed = result.get('changed')
        # A complete report without a changed-line verdict is incomplete, never clean. Every
        # undetected changed-line mutant is either failing or accepted by a disposition.
        if not changed or changed['failing']:
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
    """Paths of in-place text edits; reject Git metadata effects outside the hashed paths."""
    paths, previous = [], ''
    for line in patch.splitlines():
        if line.startswith(('old mode ', 'new mode ', 'deleted file mode ', 'new file mode ',
                            'similarity index ', 'dissimilarity index ', 'rename from ',
                            'rename to ', 'copy from ', 'copy to ', 'GIT binary patch',
                            'Binary files ')):
            raise ValueError('patch must modify existing files in place')
        if line.startswith('+++ ') and previous.startswith('--- '):
            original = previous[len('--- '):].split('\t')[0]
            target = line[len('+++ '):].split('\t')[0]
            if original == '/dev/null' or target == '/dev/null':
                raise ValueError('patch must modify existing files in place')
            if not original.startswith('a/'):
                raise ValueError(f'patch source {original!r} lacks the `a/` prefix')
            if not target.startswith('b/'):
                raise ValueError(f'patch target {target!r} lacks the `b/` prefix')
            original = repository_path(original[len('a/'):])
            target = repository_path(target[len('b/'):])
            if original != target:
                raise ValueError('patch must modify existing files in place')
            paths.append(target)
        previous = line
    if not paths or not any(line.startswith('@@') for line in patch.splitlines()):
        raise ValueError('patch is not a unified diff with a file header and a hunk')
    return paths


def evidence_patch_paths(evidence, problems):
    touched = []
    patch = nonempty_text(evidence, 'patch', problems)
    if patch is not None:
        try:
            touched = patch_paths(patch)
        except ValueError as error:
            problems.append(f'`evidence.patch`: {error}')
    return touched


def evidence_execution_problems(evidence):
    problems = []
    command = evidence['command']
    if (not isinstance(command, list) or not command
            or not all(isinstance(part, str) and part for part in command)):
        problems.append('`evidence.command` must be a non-empty list of non-empty strings')
    if not is_integer(evidence['baseline_exit']) or evidence['baseline_exit'] != 0:
        problems.append('`evidence.baseline_exit` must be 0: the test passes without the mutant')
    if not is_integer(evidence['injected_exit']) or evidence['injected_exit'] == 0:
        problems.append('`evidence.injected_exit` must be a non-zero integer: the mutant fails the test')
    nonempty_text(evidence, 'failure', problems)
    return problems


def source_hash_problem(source):
    if (not isinstance(source, dict) or source.keys() != {'path', 'sha256'}
            or not isinstance(source['path'], str) or not isinstance(source['sha256'], str)
            or not SHA256.fullmatch(source['sha256'])):
        return ('each `evidence.sources` entry needs exactly `path` and a lowercase '
                'hexadecimal SHA-256 `sha256`')
    return None


def evidence_source_problems(sources, touched):
    problems = []
    if not isinstance(sources, list) or not sources:
        problems.append('`evidence.sources` must be a non-empty array of tables')
        return problems
    hashed = []
    for source in sources:
        problem = source_hash_problem(source)
        if problem:
            problems.append(problem)
            return problems
        try:
            hashed.append(repository_path(source['path']))
        except ValueError as error:
            problems.append(f'`evidence.sources`: {error}')
            return problems
    if len(set(hashed)) != len(hashed) or (touched and set(hashed) != set(touched)):
        problems.append('`evidence.sources` must hash each file the patch touches exactly once')
    return problems


def evidence_problems(evidence):
    if not isinstance(evidence, dict):
        return ['`evidence` must be a table']
    if evidence.keys() != EVIDENCE_FIELDS:
        return ['`evidence` must have exactly the fields '
                + ', '.join(sorted(EVIDENCE_FIELDS))]
    problems = []
    touched = evidence_patch_paths(evidence, problems)
    problems.extend(evidence_execution_problems(evidence))
    problems.extend(evidence_source_problems(evidence['sources'], touched))
    return problems


def entry_identity_problems(entry):
    problems = []
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
    return problems


def entry_location_problems(entry):
    problems = []
    indexes = entry['indexes']
    if (not isinstance(indexes, list) or not indexes
            or not all(is_integer(index) and index >= 0 for index in indexes)):
        problems.append('`indexes` must be a non-empty list of non-negative integers')
    checked = nonempty_text(entry, 'checked_at', problems)
    if checked is not None and not re.fullmatch(r'[0-9a-f]{40}|[0-9a-f]{64}', checked):
        problems.append('`checked_at` must be a full lowercase hexadecimal commit ID')
    return problems


def entry_justification_problems(entry):
    problems = []
    justification = nonempty_text(entry, 'justification', problems)
    if justification is not None:
        if len(' '.join(justification.split())) < JUSTIFICATION_MINIMUM:
            problems.append(f'`justification` must argue the claim in at least '
                            f'{JUSTIFICATION_MINIMUM} characters')
        placeholder = PLACEHOLDERS.search(justification)
        if placeholder:
            problems.append(f'`justification` contains the placeholder {placeholder[0]!r}')
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
    problems.extend(entry_identity_problems(entry))
    problems.extend(entry_location_problems(entry))
    problems.extend(entry_justification_problems(entry))
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


def load_associations(root):
    """The validated associations; any invalid entry voids the file and fails the run."""
    path = root / ASSOCIATIONS_FILE
    associations, errors = parse_associations(path.read_text() if path.is_file() else None)
    owners = {}
    if associations:
        for directory, module in sorted(MODULES.items()):
            for name in module_classes(root / 'bundles' / directory, 'main'):
                owners.setdefault(name, module)
    declared = {}
    for name, tests in associations.items():
        module = owners.get(name)
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
    # A missing field reads as None, which no validated entry field equals.
    return (element.findtext('mutatedClass'), element.findtext('mutatedMethod'),
            element.findtext('methodDescription'), element.findtext('mutator'),
            tuple(indexes), element.findtext('description'))


def mutant_record(element, changed):
    return {'identity': mutant_identity(element), 'status': element.get('status', 'UNKNOWN'),
            'detected': element.get('detected') == 'true', 'line': mutant_line(element),
            'changed': changed, 'text': describe_mutant(element),
            'killing_test': element.findtext('killingTest')}


def method_changed(found, entry, changed_index):
    """Whether a line of the entry's method, per its line-number table, is a changed line.

    `found` is the entry's compiled class, which `static_staleness` proved holds the method.
    """
    name, source, methods = found
    lines = methods[(entry['method'], entry['descriptor'])]
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
    stale = static_staleness(context['compiled'], entry, context['tests'])
    if stale:
        return 'stale', stale
    mutant = context['mutants'].get(identity)
    if mutant and entry['kind'] == 'equivalent' and mutant['status'] == 'KILLED':
        return 'contradicted', (f"{mutant['killing_test'] or 'a test'} detects this mutant, so "
                                f'it is not equivalent; delete the entry')
    if mutant and entry['kind'] == 'killed-out-of-lane' and mutant['detected']:
        return 'superseded', 'an in-lane test now detects this mutant; delete the entry'
    return judge_entry_scope(entry, context, mutant, added)


def judge_entry_scope(entry, context, mutant, added):
    """Re-confirm changed methods before judging matched or unmatched PIT evidence."""
    censused = None if mutant else context['census'].get(identity_of(entry))
    if not added and (method_changed(context['compiled'].find(entry['class']), entry,
                                     context['changed_index'])
                      or (mutant and mutant['changed'])):
        return 'needs re-confirmation', (
            f"this PR changes method {entry['method']}; re-review the argument"
            + (' and re-run the counterfactual' if entry['kind'] == 'killed-out-of-lane' else '')
            + ', then update the entry, at least `checked_at`'
            + ('' if mutant or censused
               else '; no reported mutant matches it any more, so ' + REMAP_HINT))
    if mutant:
        return judge_matched_mutant(mutant, added)
    if censused:
        evidence = context['evidence'].get(entry['class'])
        if evidence is not None and not evidence.permits('staleness'):
            return 'not judged', evidence.action
        # Generated and removed off the diff: the mutant exists, so the entry is neither stale nor
        # unconfirmed, but this run measured no status for it and cannot accept it (CONV-202).
        return 'not judged', (f"matches a mutant the lane removed off the diff "
                              f"({censused['source']}:{censused['line']}); no status was measured "
                              f"- rerun and omit -PprMutationChangedLinesFile or read the scheduled "
                              f"full-scope report")
    return judge_unmatched_entry(entry, context, added)


def judge_matched_mutant(mutant, added):
    if mutant['detected']:
        return 'detected-other', f"matches a {mutant['status']} mutant; reported, not failed"
    if mutant['status'] == 'SURVIVED':
        evidence = mutant['evidence']
        if not evidence.permits('rule_1_status') or not evidence.permits('acceptance'):
            return 'not judged', 'matches a preserved SURVIVED status; ' + evidence.action
        if added:
            return 'accepted', ('on a changed line' if mutant['changed']
                                else 'on an unchanged line of a changed file')
        return 'advisory', 'disposes of an unchanged-line survivor; advisory only'
    if added:
        return 'unconfirmed', (f"matches a {mutant['status']} mutant; only SURVIVED mutants "
                               f'can be dispositioned (NO_COVERAGE: see CONV-202)')
    return 'inactive', f"matches a {mutant['status']} mutant of an unchanged method"


def judge_unmatched_entry(entry, context, added):
    evidence = context['evidence'].get(entry['class'])
    if evidence is not None and evidence.permits('staleness'):
        return 'stale', f'PIT no longer generates this mutant for its class; {REMAP_HINT}'
    if added and identity_of(entry) not in context['base_identities']:
        where = ("not in this run's sample of its class" if evidence is not None
                 else 'its class was not mutated in this run')
        return 'unconfirmed', (f'no reported mutant matches ({where}); a new entry must match a '
                               f'SURVIVED mutant of a class this PR changes'
                               + ('; ' + evidence.action if evidence is not None else ''))
    if evidence is not None:
        return 'not judged', evidence.action
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
        record.update(position=entry['position'], state=state, action=action,
                      failing=state in FAILING_STATES,
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
    if changed is None:
        return
    by_identity = {}
    for record in judged:
        by_identity.setdefault(tuple(record[name] if name != 'indexes' else tuple(record[name])
                                     for name in IDENTITY_FIELDS), record)
    equivalent, out_of_lane, failing = [], [], []
    for mutant in result['_mutants']:
        if not mutant['changed'] or mutant['detected']:
            continue
        entry = accepted.get(mutant['identity'])
        judgement = ('dispositioned_equivalent' if entry and entry['kind'] == 'equivalent'
                     else 'dispositioned_out_of_lane')
        if (mutant['status'] == 'SURVIVED' and entry
                and mutant['evidence'].permits('rule_1_status')
                and mutant['evidence'].permits(judgement)):
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
    failing = changed['failing']  # interim summaries, before judgment, have no dispositions
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
    if not result.get('total'):
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


def format_census(result):
    """What the changed-line restriction removed: a count, never a score (CONV-205)."""
    census = result.get('census')
    if census is None:
        return []
    lines = [f"Mutants removed off the diff: {census['count']}. These are generated and never "
             f"run, so they carry no status and no score; the scheduled full-scope report "
             f"measures them (CONV-206). Identities: `{Path(census['file']).name}`.", '']
    if census['malformed']:
        lines.extend([f"Census records the run could not read: {census['malformed']}. The "
                      f'restriction wrote them, so this is a defect in it:', ''])
        lines.extend('- ' + escape(value) for value in census['unreadable'])
        if census['malformed'] > len(census['unreadable']):
            lines.append(f"- and {census['malformed'] - len(census['unreadable'])} more; see "
                         f"`{Path(census['file']).name}`.")
        lines.append('')
    if census['violations']:
        lines.extend(['The restriction removed mutants on changed lines, which fails this run '
                      '(CONV-205); every one is in `results.json`:', ''])
        lines.extend('- ' + escape(value) for value in census['violations'][:SUMMARY_MUTANT_LIMIT])
        lines.append('')
    return lines


def format_advisory(result, full_scope=False):
    """Undetected class debt in the requested scope. Never blocking."""
    advisory = result if full_scope else result.get('unchanged')
    if not advisory or not advisory.get('total'):
        return []
    scope = 'All lines' if full_scope else 'Unchanged lines of the same classes'
    lines = [f"{scope} (advisory, never blocking). "
             f"Mutants: {advisory['total']}. Killed: {advisory['killed']}. "
             f"Survived: {advisory['survived']}. No coverage: {advisory['no_coverage']}.", '']
    shown = advisory['classes'][:ADVISORY_CLASS_LIMIT]
    if shown:
        scope = '' if full_scope else 'unchanged-line '
        lines.extend([f'Classes with the most undetected {scope}mutants:', ''])
        lines.extend(f'- {escape(name)}: {escape(count)}' for name, count in shown)
        if len(advisory['classes']) > len(shown):
            lines.append(f"- and {len(advisory['classes']) - len(shown)} more; see "
                         f'`results.json`.')
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
                if record['state'] in ('not judged', 'detected-other', 'inactive', 'advisory')]
    if reported:
        lines.extend(['### Reported entries (not failing)', ''])
        lines.extend(f"- {entry_label(record)} | {escape(record['state'])}: "
                     f"{escape(record['action'])}" for record in reported)
        lines.append('')
    if associations['errors']:
        lines.extend([f"`{ASSOCIATIONS_FILE}` is invalid; no association applies:", ''])
        lines.extend('- ' + escape(error) for error in associations['errors'])
        lines.append('')
    return lines


def format_full_scope(window, results, stale, associations):
    """The scheduled debt report: what no test detects, per class, and nothing blocking."""
    lines = ['# Full-scope mutation report (scheduled)', '',
             f"Every mutant of the sources this branch changed in the last {window['days']} day(s)"
             f" - no changed-line restriction, no per-class sample. Base "
             f"`{escape(window['base'][:12])}`, head `{escape(window['head'][:12])}`, produced "
             f"{escape(window['produced_at'])}. This report never decides an outcome (CONV-206); "
             'the pull-request lane judges the changed lines (CONV-202).', '',
             'A class nothing changed in the window is absent from this report.', '']
    for result in results:
        lines.extend([f"## {escape(result['module'])}: {escape(result['state'])}", ''])
        if 'error' in result:
            lines.extend([escape(result['error']), ''])
        if result['state'] != 'complete':
            lines.extend(['Incomplete: this is not a complete debt report for the module.', ''])
        lines.extend(format_report(result))
        lines.extend(format_advisory(result, full_scope=True))
    if associations['errors']:
        # It changes no outcome here, but a broken file silently narrows the lane's test scope.
        lines.extend([f"`{associations['file']}` is invalid; the lane would reject it:", '']
                     + ['- ' + escape(error) for error in associations['errors']] + [''])
    if stale['errors']:
        lines.extend([f"`{stale['file']}` is invalid; no entry was judged:", '']
                     + ['- ' + escape(error) for error in stale['errors']] + [''])
    if stale['entries']:
        lines.extend([f"## Dispositions entries this report proves stale (advisory)", '',
                      'Their classes were mutated with nothing sampled and nothing restricted, '
                      'and no mutant matches them. The pull-request lane cannot judge this '
                      '(CONV-202); remove or remap each entry in dedicated work.', ''])
        lines.extend(f"- entry {record['position']} [{record['kind']}] {escape(record['class'])}."
                     f"{escape(record['method'])}{escape(record['descriptor'])} | "
                     f"{escape(record['mutator'])} | indexes {escape(record['indexes'])}"
                     for record in stale['entries'])
        lines.append('')
    return '\n'.join(lines)


def format_summary(results, dispositions=None, associations=None):
    lines = ['# Changed-source mutation testing', '',
             'Blocking criterion: undetected mutants on the lines this pull request changed, '
             'unless a reviewed disposition accepts them (CONV-202). '
             'Mutants are generated on those lines only and never sampled per class (CONV-205); '
             'the ones removed off the diff are counted here and measured by the scheduled '
             'full-scope report (CONV-206). '
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
                          'Requested mutant scope: the changed lines, without a per-class limit.', ''])
        if result.get('off_diff_only'):
            lines.extend(['No mutant of this module sits on a changed line: PIT generated '
                          f"{result['census']['count']} mutant(s) for these classes and the "
                          'changed-line restriction removed every one. Nothing is judged here, '
                          'and nothing is claimed as killed.', ''])
        if result.get('partial'):
            lines.extend(['Partial results: this module did not finish, and the statuses below '
                          'are the ones its result stream recorded before the deadline killed it '
                          '(CONV-202). An incomplete module is never clean.', ''])
        if result.get('stream_error'):
            lines.extend(['The result stream could not be read, so nothing was recovered from '
                          f"it: {escape(result['stream_error'])}.", ''])
        if result.get('unmapped'):
            lines.extend(['Changed sources for which no mapped test was selected:', ''])
            lines.extend('- ' + escape(value) for value in result['unmapped'])
            lines.append('')
        lines.extend(format_verdict(result))
        lines.extend(format_report(result))
        lines.extend(format_census(result))
        lines.extend(format_advisory(result))
    return '\n'.join(lines)


def execute(command, root, log, timeout):
    with log.open('w') as stream, subprocess.Popen(
            command, cwd=root, stdout=stream, stderr=subprocess.STDOUT,
            start_new_session=True, shell=False) as process:
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # Gradle's single-use daemon detaches from the launcher's session. Snapshot its
            # descendants before killing any parent, including detached workers in timeout
            # cleanup. This runner already requires POSIX process-group semantics.
            parents = {int(pid): int(parent) for pid, parent in
                       (line.split() for line in subprocess.check_output(
                           ['ps', '-e', '-o', 'pid=,ppid='], text=True).splitlines())}
            descendants = {process.pid}
            while True:
                children = {pid for pid, parent in parents.items() if parent in descendants}
                if children <= descendants:
                    break
                descendants.update(children)
            for pid in descendants - {process.pid}:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise


def write_changed_lines(path, index):
    """The map the CHANGEDLINES interceptor reads: one record per selected source (CONV-205)."""
    records = [CHANGED_LINES_HEADER]
    for (package, source), numbers in sorted(index.items()):
        records.append(f'{package}\t{source}\t' + ' '.join(str(line) for line in sorted(numbers)))
    path.write_text('\n'.join(records) + '\n')
    return path


def prepare_analysis(root, directory, module, sources, result, associations=None, index=None,
                     restrict=False):
    """The PIT command for one module.

    `restrict` is what the lane asks for: mutants on the changed lines of `index` only, with a
    census of what was removed (CONV-205). It is never implied, so a caller that wants the
    restriction states it and supplies the map.
    """
    module_root = root / 'bundles' / DIRECTORIES[module]
    targets, missing = resolve_targets(module_root, sources)
    if missing:
        raise ValueError('No compiled classes for changed sources: ' + ', '.join(missing))
    if not targets:
        raise ValueError('Empty target selection; refusing whole-module mutation')
    tests, unmapped, mapping = test_scope(module_root, sources, associations)
    # No per-class sample (CONV-205): every mutant the lane generates on a changed line is
    # analysed, so a changed-line count is a complete measurement rather than a sample.
    result.update(targets=targets, tests=tests, unmapped=unmapped, resolution=mapping,
                  mutants_per_class=None, restricted=restrict)
    if not tests:
        raise Unmapped('No test maps to any changed source; PIT has no baseline to collect')
    target_file = directory / 'targets.txt'
    target_file.write_text('\n'.join(targets) + '\n')
    # One daemon with a fixed heap, reused across the run's invocations, instead of forking a
    # single-use daemon per module for the workstation-sized heap `gradle.properties` requests.
    command = [str(root / 'gradlew'), f':{module}:pitest',
               f'-Dorg.gradle.jvmargs={DAEMON_JVM_ARGS}',
               f'-PprMutationModule={module}', f'-PprMutationTargetsFile={target_file}',
               f'-PprMutationReportDir={directory / "report"}',
               f'-PprMutationTests={",".join(tests)}']
    if restrict:
        if not index:
            # Restricting to an empty map would mutate nothing and report success (CONV-205).
            raise ValueError('No changed-line map for this module; refusing to restrict the scope')
        # The interceptor writes its census beside the map (CONV-205): PIT 1.25.5's feature
        # parser reads only a feature's first parameter, so the census has no knob of its own.
        command.append(f'-PprMutationChangedLinesFile='
                       f'{write_changed_lines(directory / CHANGED_LINES_FILE, index)}')
    return command


CENSUS_TEXT_FIELDS = ('class', 'method', 'descriptor', 'mutator', 'description', 'sourceFile')
# Enough to name the defect in the restriction that wrote them; the file itself holds the rest.
CENSUS_REASON_LIMIT = 10


def reason(error):
    """One readable cause for a census record the reporter refused, whatever raised it."""
    if isinstance(error, KeyError):
        return f'census record has no {error.args[0]!r} field'
    return str(error) or error.__class__.__name__


def census_identity(record):
    """The CONV-201 identity of a censused mutant, in `mutant_identity`'s field order.

    The census is evidence the verdict reads, so a record whose fields are not of the type the
    identity needs is rejected rather than turned into an identity that matches nothing: a silent
    near-miss would cost an entry its existence proof and read as staleness (CONV-202).
    """
    for field in CENSUS_TEXT_FIELDS:
        if not isinstance(record[field], str):
            raise ValueError(f'census `{field}` must be a string')
    indexes = record['indexes']
    if not isinstance(indexes, list) or not all(
            isinstance(index, int) and not isinstance(index, bool) for index in indexes):
        raise ValueError('census `indexes` must be a list of integers')
    if not isinstance(record['lineNumber'], int) or isinstance(record['lineNumber'], bool):
        raise ValueError('census `lineNumber` must be an integer')
    return (record['class'], record['method'], record['descriptor'], record['mutator'],
            tuple(indexes), record['description'])


def read_census(path, index):
    """What the changed-line restriction removed, judged again by the reporter (CONV-205).

    Returns the identities (never a status) plus every record whose own line is a changed line:
    the restriction decided to remove it, and this independent attribution says it should not
    have, which fails the run. A record the kill truncated is reported, never guessed at, and a
    record two interceptor passes both wrote is counted once.
    """
    census = {'file': str(path), 'count': 0, 'violations': [], 'malformed': 0, 'unreadable': [],
              'readable': True}
    records = {}
    if not path.exists():
        return census, records
    try:
        lines = path.read_bytes().splitlines()
    except OSError as error:
        census.update(malformed=1, unreadable=[reason(error)], readable=False)
        return census, records
    for number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line.decode('utf-8'))
            identity = census_identity(record)
        except (ValueError, TypeError, KeyError) as error:
            census['malformed'] += 1
            # Say why, not only how many: this run fails on it, and the next reader needs to
            # know which field of which record the restriction wrote wrongly.
            if len(census['unreadable']) < CENSUS_REASON_LIMIT:
                census['unreadable'].append(f'line {number}: {reason(error)}')
            continue
        records.setdefault(identity, {'line': record['lineNumber'],
                                      'source': record['sourceFile']})
        # Every record is attributed, not only the first of an identity: a line is not part of a
        # mutant's identity (CONV-201), so judging the deduplicated map could miss a violation.
        package = record['class'].rpartition('.')[0]
        if record['lineNumber'] in index.get((package, record['sourceFile']), set()):
            violation = (f"{record['class']}:{record['lineNumber']} ({record['mutator']}) is on "
                         f"a changed line")
            if violation not in census['violations']:
                census['violations'].append(violation)
    census['count'] = len(records)
    return census, records


def attach_census(result, path, index):
    census, records = read_census(path, index)
    result['census'] = census
    result['_census'] = records


def resolve_empty_scope(result, index, evidence):
    """Promote a census-explained empty report only on accepted completion evidence."""
    if not evidence.permits('empty_scope'):
        return False
    result.pop('error', None)
    result.update(state='complete', off_diff_only=True,
                  **build_report([], index, allow_empty=True))
    return True


def attach_report(result, report, index, stream=None):
    empty_report = False
    try:
        empty_report = not report.exists() or report.stat().st_size == 0
        if not empty_report:
            parsed = read_report(report, index, allow_empty=True)
            empty_report = parsed['total'] == 0
            if not empty_report:
                result['_report_valid'] = True
                result.update(parsed)
                return False
        failure = ValueError(NO_MUTANTS)
    except (OSError, ValueError, ET.ParseError) as error:
        failure = error
    recovered = None
    # Only a run that did not finish writing its report may be read from the stream; a complete
    # run with a malformed report is incomplete, as it always was.
    if result['state'] != 'complete' and stream is not None and stream.exists():
        try:
            recovered = read_stream(stream, index)
        except (OSError, ValueError) as stream_error:
            result['stream_error'] = str(stream_error)
    if recovered:
        result.update(recovered)
        return False
    if result['state'] == 'complete':
        # Name the file: a parse error carries only a line and column, and this run reads two.
        result.update(state='error', error=f'cannot read {Path(report).name}: {failure}')
    return empty_report


def analyze_module(root, output, module, sources, lines, deadline, seconds, associations,
                   restrict):
    result = {'module': module, 'sources': sources, 'state': 'error',
              '_evidence': CompletionEvidence({'unfinished': COMPLETION_FACTS['unfinished']})}
    if module not in DIRECTORIES:
        return {**result, 'error': 'Unknown module; refusing to execute PIT'}
    directory = output / module
    directory.mkdir(parents=True, exist_ok=True)
    report = directory / 'report/mutations.xml'
    stream = directory / 'report' / STREAM_FILE
    census = directory / CENSUS_FILE
    index, executed = {}, False
    try:
        # Never attach cached/rerun output to a new selection, even when mapping fails.
        for stale in (report, stream, census):
            if stale.exists():
                stale.unlink()
        index = changed_line_index(root / 'bundles' / DIRECTORIES[module], lines)
        result['changed_line_count'] = sum(len(numbers) for numbers in index.values())
        command = prepare_analysis(root, directory, module, sources, result, associations, index,
                                   restrict)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired('PIT module execution share', seconds)
        code = execute(command, root, directory / 'pitest.log', remaining)
        if code != 0:
            raise ValueError(f'PIT exited {code}; see {module}/pitest.log')
        result['state'] = 'complete'
        executed = True
    except subprocess.TimeoutExpired:
        result.update(state='timed out', error=f'{seconds}s module execution share exceeded')
    except Unmapped as error:
        result.update(state='unmapped', error=str(error))
    except (OSError, ValueError, IndexError, struct.error) as error:
        result.update(state='error', error=str(error))
    empty_report = attach_report(result, report, index, stream)
    if restrict:
        attach_census(result, census, index)
    value = ModuleEvidence(directory, result, result.get('targets', []), executed,
                           report_supplied=True,
                           report_valid=(result.pop('_report_valid', False)
                                         or empty_report and bool(result.get('census', {}).get('count'))),
                           census_supplied=restrict, census_valid=True,
                           records=result.get('_mutants', []),
                           summaries=[result['census']] if restrict else [])
    evidence = value.completion
    result['_evidence'] = evidence
    result['completion_rejections'] = evidence.rejections
    for record in value.records:
        record['evidence'] = evidence
    if empty_report and result.get('census', {}).get('count'):
        resolve_empty_scope(result, index, evidence)
    if evidence.rejections and result['state'] == 'complete':
        result.update(state='error', error=evidence.action)
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


def class_evidence(results):
    evidence = {}
    for result in results:
        value = result['_evidence']
        for target in result.get('targets', ()):
            previous = evidence.get(target)
            evidence[target] = (value if previous is None else
                                CompletionEvidence({**previous.rejections, **value.rejections}))
    return evidence


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
            mutant['evidence'] = result['_evidence']
            mutants.setdefault(mutant['identity'], mutant)
    # Census identities prove a mutant exists without proving anything about its status, which is
    # what keeps staleness and re-identification judgeable in a changed-line-only run (CONV-202).
    census = {}
    for result in results:
        for identity, record in result.get('_census', {}).items():
            census.setdefault(identity, record)
    context = {'compiled': CompiledClasses(root), 'tests': test_names, 'mutants': mutants,
               'census': census, 'changed_index': global_changed_index(root, changed),
               'evidence': class_evidence(results)}
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


def analyze_modules(root, output, selected, changed, seconds, associations, restrict,
                    progress=None):
    """Run PIT per module inside one budget; one coverage scope per module (CONV-205)."""
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
                                      time.monotonic() + share, round(share), associations,
                                      restrict))
        pending -= 1
        if progress is not None:
            progress(results)
    return results


def run(root, output, seconds, blocking):
    selected, changed, base_text = read_selection(output)
    associations, association_errors = load_associations(root)
    association_verdict = {'file': ASSOCIATIONS_FILE, 'errors': association_errors,
                           'count': len(associations)}
    results = analyze_modules(
        root, output, selected, changed, seconds, associations, restrict=True,
        progress=lambda interim: write_results(output, interim,
                                               associations=association_verdict))
    dispositions = judge_run(root, results, changed, base_text)
    write_results(output, results, dispositions, association_verdict)
    clean = is_clean(results) and dispositions['clean'] and not association_errors
    return int(blocking and not clean)


def window_base(root, days):
    """The last first-parent commit before the report's window (CONV-206)."""
    if days <= 0:
        raise ValueError('--window-days must be positive')
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')
    found = subprocess.check_output(['git', '-C', str(root), 'rev-list', '--first-parent', '-1',
                                     f'--before={cutoff}', 'HEAD'], shell=False).decode().strip()
    if not found:
        raise ValueError(f'no commit of this branch predates {cutoff}; widen --window-days')
    return found


def stale_entries(root, results):
    """Entries this full-scope report proves stale (CONV-202 rule 5), as advisory information.

    Only a run that mutated the class with a complete generated set says anything: the entry's
    mutant is absent from a report that sampled nothing and restricted nothing.
    """
    entries, errors = read_dispositions(root)
    evidence = class_evidence(results)
    reported = set()
    for result in results:
        for mutant in result.get('_mutants', ()):
            reported.add(mutant['identity'])
    stale = [{name: entry[name] for name in IDENTITY_FIELDS + ('position', 'kind')}
             for entry in ([] if errors else entries)
             if entry['class'] in evidence and evidence[entry['class']].permits('staleness')
             and identity_of(entry) not in reported]
    return {'file': DISPOSITIONS_FILE, 'errors': errors, 'entries': stale}


def full_scope(root, output, seconds, days):
    """CONV-206: the debt report the PR lane no longer produces. Never decides an outcome.

    Every line of the sources the branch changed within the window is mutated, with no per-class
    limit and no changed-line restriction, and no disposition verdict is computed.
    """
    base, head = window_base(root, days), current_commit(root)
    selected, changed = changed_sources(root, base, head)
    window = {'days': days, 'base': base, 'head': head,
              'produced_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}
    (output / 'selection.json').write_text(json.dumps(
        {'modules': selected, 'changed_lines': changed, 'window': window}, indent=2) + '\n')
    associations, association_errors = load_associations(root)
    association_verdict = {'file': ASSOCIATIONS_FILE, 'errors': association_errors,
                           'count': len(associations)}
    results = analyze_modules(root, output, selected, changed, seconds, associations,
                              restrict=False)
    stale = stale_entries(root, results)
    modules = [{key: value for key, value in result.items() if not key.startswith('_')}
               for result in results]
    (output / 'results.json').write_text(json.dumps(
        {'window': window, 'modules': modules, 'stale_entries': stale,
         'associations': association_verdict}, indent=2) + '\n')
    (output / 'summary.md').write_text(
        format_full_scope(window, results, stale, association_verdict))
    # A mutation result never decides an outcome here (CONV-206); broken tooling still raises.
    return 0


# ---------------------------------------------------------------------------------------------
# Local helpers (CONV-203). Check/remap are read-only; counterfactual temporarily edits sources.
# CI never runs them, and none writes the dispositions file.

FAILURE_EXCERPT_LINES = 40


class CounterfactualError(Exception):
    """The counterfactual run produced no valid evidence; the message says why."""


COMPLETION_FACTS = {
    'unfinished': 'unfinished run or unknown module completion',
    'report_unreadable': 'the report could not be read completely and validly',
    'census_unreadable': 'the census could not be read',
    'census_malformed': 'the census holds malformed records',
    'census_violations': 'the census holds changed-line violations',
    'missing_artifacts': 'required artifacts or their scope metadata were not supplied',
    'sampled': 'a per-class mutant limit was applied',
    'invalid_evidence': 'recorded evidence failed validation',
}
COMPLETION_JUDGEMENTS = (
    'acceptance', 'dispositioned_equivalent', 'dispositioned_out_of_lane',
    'staleness', 'empty_scope', 'rule_1_status',
)


@dataclass
class CompletionEvidence:
    rejections: dict

    def permits(self, judgement):
        if judgement not in COMPLETION_JUDGEMENTS:
            raise ValueError(f'Unknown completion judgement: {judgement}')
        return not self.rejections

    @property
    def failed(self):
        return any(fact not in ('missing_artifacts', 'sampled') for fact in self.rejections)

    @property
    def action(self):
        return ('completion judgement withheld; evidence rejected: '
                + '; '.join(f'{fact}: {reason}' for fact, reason in self.rejections.items())
                + '; supply complete valid evidence or rerun the analysis')


@dataclass
class ModuleEvidence:
    path: Path
    metadata: dict
    targets: list
    completed: bool | None
    report_supplied: bool = False
    report_valid: bool = False
    census_supplied: bool = False
    census_valid: bool = False
    records: list = field(default_factory=list)
    census: dict = field(default_factory=dict)
    summaries: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    sampled: bool = False

    @cached_property
    def completion(self):
        recorded_facts = self.metadata.get('completion_rejections', {})
        valid = (isinstance(recorded_facts, dict)
                 and all(isinstance(fact, str) and isinstance(reason, str)
                         for fact, reason in recorded_facts.items()))
        facts = dict(recorded_facts) if valid else {
            'invalid_evidence': 'invalid recorded completion rejection metadata'}
        if self.completed is False or self.completed is None and self.report_supplied:
            facts['unfinished'] = COMPLETION_FACTS['unfinished']
        if self.report_supplied and not self.report_valid:
            facts['report_unreadable'] = COMPLETION_FACTS['report_unreadable']
        for summary in self.summaries:
            if summary.get('readable') is False:
                facts['census_unreadable'] = (COMPLETION_FACTS['census_unreadable'] + ': '
                                              + '; '.join(summary['unreadable']))
            if summary['malformed']:
                facts['census_malformed'] = (f"{summary['malformed']} malformed census record(s): "
                                             + '; '.join(summary['unreadable']))
            if summary['violations']:
                facts['census_violations'] = (COMPLETION_FACTS['census_violations'] + ': '
                                              + '; '.join(summary['violations']))
        if self.errors:
            facts['invalid_evidence'] = '; '.join(self.errors)
        total = self.metadata.get('total')
        report_loaded = (self.report_valid if self.report_supplied else
                         type(total) is int and total == 0)
        recorded = self.metadata.get('census', {})
        empty_census = (isinstance(recorded, dict) and type(recorded.get('count')) is int
                        and recorded['count'] == 0)
        census_loaded = self.census_valid if self.census_supplied else empty_census
        restricted = self.metadata.get('restricted')
        if (not self.targets or 'mutants_per_class' not in self.metadata or not report_loaded
                or not (restricted is False or restricted is True and census_loaded)):
            facts['missing_artifacts'] = COMPLETION_FACTS['missing_artifacts']
        if self.sampled or self.metadata.get('mutants_per_class') is not None:
            facts['sampled'] = COMPLETION_FACTS['sampled']
        return CompletionEvidence(facts)


def load_evidence(reports, censuses, index, sampled=False):
    modules = {}
    for kind, paths in (('report', reports), ('census', censuses)):
        for path in dict.fromkeys(Path(value).resolve() for value in paths):
            directory = None
            if kind == 'report' and path.name == 'mutations.xml' and path.parent.name == 'report':
                directory = path.parent.parent
            elif kind == 'census' and path.name == CENSUS_FILE:
                directory = path.parent
            if directory is not None and directory.name not in DIRECTORIES:
                directory = None
            key = directory if directory is not None else path
            evidence = modules.get(key)
            if evidence is None:
                metadata = {}
                if directory is not None:
                    try:
                        document = json.loads((directory.parent / 'results.json').read_text())
                        matching = [module for module in document['modules']
                                    if isinstance(module, dict)
                                    and module.get('module') == directory.name]
                        if len(matching) == 1:
                            metadata = matching[0]
                    except (OSError, ValueError, KeyError, TypeError):
                        pass
                targets = metadata.get('targets', [])
                if not isinstance(targets, list) or any(
                        not isinstance(name, str) or not QUALIFIED_NAME.fullmatch(name)
                        for name in targets):
                    targets = []
                completed = ((metadata.get('state') == 'complete'
                              and metadata.get('partial', False) is False) if metadata else None)
                evidence = ModuleEvidence(key, metadata, targets, completed, sampled=sampled)
                modules[key] = evidence
                if metadata and not completed:
                    evidence.errors.append(f'Run completion is unfinished or unknown for {key}')
                recorded = metadata.get('census')
                if recorded is not None:
                    valid = (isinstance(recorded, dict) and isinstance(recorded.get('file'), str)
                             and all(type(recorded.get(name)) is int and recorded[name] >= 0
                                     for name in ('count', 'malformed'))
                             and all(isinstance(recorded.get(name), list)
                                     and all(isinstance(value, str) for value in recorded[name])
                                     for name in ('violations', 'unreadable')))
                    if not valid:
                        evidence.errors.append(f'Invalid recorded census metadata in {key}')
                    elif recorded['malformed'] or recorded['violations']:
                        evidence.summaries.append(recorded)
                        evidence.errors.append(f'Recorded census failures in {key}')
            if kind == 'report':
                evidence.report_supplied = path.exists()
                if not evidence.completed and not evidence.metadata:
                    evidence.errors.append(f'Run completion is unfinished or unknown for {path}')
                if not evidence.report_supplied:
                    continue
                try:
                    empty = (type(evidence.metadata.get('total')) is int
                             and evidence.metadata['total'] == 0 and path.stat().st_size == 0)
                    elements = [] if empty else ET.parse(path).getroot().findall('mutation')
                    for element in elements:
                        changed, _ = classify_mutant(element, index)
                        evidence.records.append(mutant_record(element, changed == 'changed'))
                    expected = evidence.metadata.get('total')
                    if type(expected) is int and len(elements) != expected:
                        raise ValueError(f'expected {expected} mutations, read {len(elements)}')
                    evidence.report_valid = True
                except (OSError, ValueError, ET.ParseError) as error:
                    evidence.errors.append(f'cannot read {path}: {error}')
            else:
                evidence.census_supplied = path.exists()
                if not evidence.census_supplied:
                    continue
                summary, records = read_census(path, index)
                evidence.summaries.append(summary)
                evidence.census.update(records)
                evidence.census_valid = not summary['malformed'] and not summary['violations']
                recorded = evidence.metadata.get('census', {})
                expected = recorded.get('count') if isinstance(recorded, dict) else None
                if type(expected) is int and summary['count'] != expected:
                    evidence.census_valid = False
                    evidence.errors.append(f'census {path}: expected {expected} identities, '
                                           f"read {summary['count']}")
                if not evidence.census_valid:
                    evidence.errors.append(f'Census validation failed for {path}')
    unassigned = [str(evidence.path) for evidence in modules.values()
                  if evidence.census_supplied and not evidence.census_valid and not evidence.metadata]
    for evidence in modules.values():
        if unassigned and evidence.metadata:
            evidence.errors.append('Cannot attribute invalid census evidence: ' + ', '.join(unassigned))
        completion = evidence.completion
        for record in evidence.records:
            record['evidence'] = completion
    return list(modules.values())


def current_commit(root):
    return subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'],
                                   shell=False).decode().strip()


def read_dispositions(root):
    path = root / DISPOSITIONS_FILE
    return parse_dispositions(path.read_text() if path.is_file() else None)


def check_dispositions(root, reports, base, sampled, censuses=()):
    """Judge the working tree's entries against local reports, as the lane would.

    `censuses` are the lane's `census.jsonl` files: with them a local judgement of a lane report
    reaches the same states the lane reached, since a censused mutant exists without a status.
    """
    _, lines = changed_sources(root, base, current_commit(root))
    index = global_changed_index(root, lines)
    evidence = load_evidence(reports, censuses, index, sampled)
    records, census, results = [], {}, []
    for module in evidence:
        records.extend(module.records)
        census.update(module.census)
        results.append({'targets': module.targets, '_evidence': module.completion})
    entries, errors = read_dispositions(root)
    context = {'compiled': CompiledClasses(root), 'tests': lambda: declared_tests(root),
               'mutants': {record['identity']: record for record in records}, 'census': census,
               'changed_index': index,
               'evidence': class_evidence(results)}
    verdict, accepted = judge_dispositions([] if errors else entries, errors,
                                           merge_base_file(root, base, DISPOSITIONS_FILE), context)
    module = {'changed': {}, '_mutants': records}
    apply_dispositions(module, accepted, verdict['entries'])
    lines_out = [f'{DISPOSITIONS_FILE}: {len(entries)} valid entries, {len(errors)} errors']
    lines_out.extend(f'error: {error}' for error in errors)
    for supplied in evidence:
        lines_out.extend('error: ' + error for error in supplied.errors)
        for summary in supplied.summaries:
            lines_out.extend(format_census({'census': summary}))
        if supplied.completion.rejections and not supplied.errors:
            lines_out.append(f'Generated set incompletely loaded for {supplied.path}; '
                             + supplied.completion.action)
    for record in verdict['entries']:
        if record['state'] != 'out of scope':
            marker = 'FAIL' if record['failing'] else 'ok'
            lines_out.append(f"{marker} entry {record['position']} [{record['state']}] "
                             f"{record['class']}.{record['method']}: {record['action']}")
    lines_out.extend(f"FAIL mutant {value['mutant']}: {value['reason']}"
                     for value in module['changed']['failing'])
    failing = bool(errors or any(value.completion.failed for value in evidence) or verdict['failing']
                   or module['changed']['failing'])
    return failing, '\n'.join(lines_out)


def remap_candidates(entry, records):
    """Report mutants in preference order, each identity labelled with its first eligible tier."""
    identity = identity_of(entry)
    alike = [record for record in records if record['identity'] != identity
             and record['identity'][3] == identity[3] and record['identity'][5] == identity[5]]
    tiers = (('same method, other instruction indexes',
              lambda candidate: candidate[:3] == identity[:3]),
             ('renamed method of the same class', lambda candidate: candidate[0] == identity[0]),
             ('same method name and descriptor in another class',
              lambda candidate: candidate[1:3] == identity[1:3]))
    candidates, seen = [], set()
    for label, accepts in tiers:
        matches = [record for record in alike if accepts(record['identity'])]
        matches.sort(key=lambda record: record['identity'][4] != identity[4])
        for record in matches:
            if record['identity'] not in seen:
                seen.add(record['identity'])
                candidates.append((label, record))
    return candidates


def remap_dispositions(root, reports):
    """Propose identities for every entry no report mutant matches; never writes the file."""
    evidence = load_evidence(reports, (), {})
    records = [record for module in evidence for record in module.records]
    present = {record['identity'] for record in records}
    entries, errors = read_dispositions(root)
    lines = [f'error: {error}' for error in errors]
    lines.extend('error: ' + error for module in evidence if not module.report_valid
                 for error in module.errors)
    for entry in entries:
        if identity_of(entry) in present:
            continue
        matches = remap_candidates(entry, records)
        lines.append(f"entry {entry['position']} ({entry['class']}.{entry['method']}"
                     f"{entry['descriptor']}): no report mutant matches; "
                     + (f'{len(matches)} candidate(s):' if matches else 'no candidate'))
        for label, record in matches:
            name, method, descriptor, mutator, indexes, description = record['identity']
            same = ', same instruction indexes' if indexes == identity_of(entry)[4] else ''
            lines.extend([f"  # line {record['line']}, {record['status']}, {label}{same}; re-review the "
                          f'argument and update checked_at before using it',
                          f'  class = {json.dumps(name)}', f'  method = {json.dumps(method)}',
                          f'  descriptor = {json.dumps(descriptor)}',
                          f'  mutator = {json.dumps(mutator)}', f'  indexes = {list(indexes)}',
                          f'  description = {json.dumps(description)}'])
    return '\n'.join(lines) if lines else 'Every entry matches a mutant of the given reports.'


def toml_string(value):
    """A TOML string that loads back to `value`: multi-line literal when safe, else basic."""
    # TOML 1.0 allows one or two quotes before the closing delimiter, so a value may end in '.
    if ('\n' in value and "'''" not in value
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
                          shell=False)


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
    # `git apply` is atomic: a patch that does not apply changes nothing.
    if subprocess.run(git + [str(patch_file)], shell=False).returncode:
        raise CounterfactualError('the patch does not apply to the working tree')
    try:
        injected = run_test_command(command, root)
    finally:
        reversed_cleanly = subprocess.run(git + ['-R', str(patch_file)],
                                          shell=False).returncode == 0
        if not reversed_cleanly or hashes() != before:
            raise CounterfactualError('sources differ after reversing the patch; restore '
                                      + ', '.join(paths) + ' from version control before '
                                      'continuing')
    if injected.returncode == 0:
        raise CounterfactualError('the test passes with the mutant injected; it does not detect '
                                  'this mutant')
    output = os.fsdecode(injected.stdout).splitlines()  # stdout=PIPE: always bytes
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
    full = commands.add_parser('full-scope',
                               help='scheduled: the unchanged-line debt report (CI, CONV-206)')
    for command in (select, analysis, full):
        command.add_argument('--output', type=Path, required=True)
    select.add_argument('--base')
    select.add_argument('--head')
    analysis.add_argument('--seconds', type=int, default=1320)
    analysis.add_argument('--blocking', action='store_true')
    full.add_argument('--seconds', type=int, default=5400,
                      help="the report's own budget; never the PR lane's")
    full.add_argument('--window-days', type=int, default=WINDOW_DAYS,
                      help='how far back the report looks for changed sources')
    dispositions = commands.add_parser(
        'dispositions', help='local: check entries or propose identities after a refactor')
    dispositions.add_argument('--report', action='append', required=True,
                              help='a PIT mutations.xml; repeatable')
    dispositions.add_argument('--check', action='store_true')
    dispositions.add_argument('--remap', action='store_true')
    dispositions.add_argument('--base', help='commit the change is judged against (--check)')
    dispositions.add_argument('--sampled', action='store_true',
                              help='the reports come from a rerun with a per-class mutant limit')
    dispositions.add_argument('--census', action='append', default=[],
                              help="a lane census.jsonl of mutants removed off the diff; "
                                   'repeatable')
    evidence = commands.add_parser(
        'counterfactual', help='local: produce killed-out-of-lane evidence for one mutant')
    evidence.add_argument('--patch', required=True,
                          help='unified diff injecting exactly the mutant')
    evidence.add_argument('test_command', nargs=argparse.REMAINDER,
                          help='after `--`: the command that runs the test in its own process')
    for command in (select, analysis, full, dispositions, evidence):
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
            failing, text = check_dispositions(root, args.report, args.base, args.sampled,
                                               args.census)
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
    if args.command == 'full-scope':
        if args.window_days <= 0:
            parser.error('--window-days must be positive')
        return full_scope(root, output, args.seconds, args.window_days)
    return run(root, output, args.seconds, args.blocking)


if __name__ == '__main__':
    raise SystemExit(main())
