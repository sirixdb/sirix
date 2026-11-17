#!/usr/bin/env python3
"""Bounded, secret-free PR mutation analysis. See docs/VERIFICATION.md."""
import argparse
from collections import Counter
import html
import json
import os
from pathlib import Path
import re
import signal
import struct
import subprocess
import time
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
# Bound coverage and restart units independently. Every source is assigned exactly once; a
# timed-out partition remains incomplete and cannot consume a later partition's reserved share.
PARTITION_SOURCE_LIMIT = 12

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


def class_source(data):
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
    reader.skip_members()  # methods
    for attribute, value in reader.attributes(pool):
        if attribute == 'SourceFile':
            return name, pool[struct.unpack('>H', value)[0]]
    return name, None


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


def test_scope(module_root, sources, tests=None):
    """Bounded per-source test selection. No input may widen the scope to the module suite."""
    candidates = []
    if tests is None:
        tests = module_test_classes(module_root)
    for source in sources:
        stem, package = Path(source).stem, source_package(module_root, source)
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


def partition_sources(module_root, sources):
    """Keep every source, splitting before either the source or selected-test cap is exceeded."""
    if len(sources) <= 1:
        return [sources] if sources else []
    partitions, current = [], []
    tests = module_test_classes(module_root)
    for source in sources:
        candidate = current + [source]
        _, _, mapping = test_scope(module_root, candidate, tests)
        if current and (len(candidate) > PARTITION_SOURCE_LIMIT
                        or 'over-limit' in mapping.values()):
            partitions.append(current)
            current = []
        current.append(source)
    if current:
        partitions.append(current)
    return partitions


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
    for element in mutations:
        kind, attributed = classify_mutant(element, index)
        groups[kind].append(element)
        unattributed += not attributed
    changed = tally(groups['changed'])
    changed['survivors'] = [describe_mutant(element) for element in groups['changed']
                            if element.get('status') != 'KILLED']
    advisory = tally(groups['unchanged'])
    debt = Counter(element.findtext('mutatedClass', '?')
                   for element in undetected(groups['unchanged']))
    advisory['classes'] = [[name, count] for name, count in debt.most_common(ADVISORY_CLASS_LIMIT)]
    report = tally(mutations)
    report.update(statuses=dict(sorted(Counter(element.get('status', 'UNKNOWN')
                                               for element in mutations).items())),
                  survivors=[describe_mutant(element) for element in undetected(mutations)],
                  changed=changed, unchanged=advisory, unattributed=unattributed)
    return report


def is_clean(results):
    """Only assertion-killed changed-line mutants prove the complete selected scope."""
    for result in results:
        if result['state'] != 'complete' or result.get('total', 0) <= 0:
            return False
        # A report whose mutants all failed to map to a changed source judged nothing at all.
        if result['total'] - result.get('unattributed', 0) <= 0:
            return False
        changed = result.get('changed')
        # A complete report without a changed-line verdict is incomplete, never clean.
        if (not changed or changed['detected'] != changed['total']
                or changed['killed'] != changed['total']):
            return False
    return True


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
    lines = [f"{counted}Mutants on changed lines: {changed['total']}. "
             f"Killed: {changed['killed']}. Survived: {changed['survived']}. "
             f"No coverage: {changed['no_coverage']}.", '']
    if changed['survivors']:
        lines.extend(['Undetected mutants on changed lines (these decide the outcome; '
                      'every undetected mutant is in `results.json`):', ''])
        lines.extend('- ' + escape(value) for value in changed['survivors'][:SUMMARY_MUTANT_LIMIT])
        if len(changed['survivors']) > SUMMARY_MUTANT_LIMIT:
            lines.append('- Remaining entries: see `results.json`.')
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


def format_summary(results):
    lines = ['# Changed-source mutation testing', '',
             'Blocking criterion: mutants without assertion kills on the lines this pull request changed. '
             'Mutants on unchanged lines of the same classes are advisory pre-existing debt. '
             'Informational unless `MUTATION_BLOCKING` is enabled. Full reports, targets and logs '
             'are in the `pr-mutation` artifact.', '']
    if not results:
        return '\n'.join(lines + ['No changed main Java/Kotlin sources; PIT skipped.', ''])
    for result in results:
        label = result['module']
        if 'partition' in result:
            label += f" / part {result['partition']}"
        lines.extend([f"## {escape(label)}: {escape(result['state'])}", ''])
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


def prepare_analysis(root, directory, module, sources, result):
    module_root = root / 'bundles' / DIRECTORIES[module]
    targets, missing = resolve_targets(module_root, sources)
    if missing:
        raise ValueError('No compiled classes for changed sources: ' + ', '.join(missing))
    if not targets:
        raise ValueError('Empty target selection; refusing whole-module mutation')
    tests, unmapped, mapping = test_scope(module_root, sources)
    mutants = result.get("mutants_per_class", mutants_per_class(targets))
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


def analyze_module(root, output, module, sources, lines, deadline, seconds,
                   partition=None, mutant_limit=None):
    result = {'module': module, 'sources': sources, 'state': 'error'}
    if module not in DIRECTORIES:
        return {**result, 'error': 'Unknown module; refusing to execute PIT'}
    directory = output / module
    if partition is not None:
        result['partition'] = partition
        directory /= f'part-{partition}'
    if mutant_limit is not None:
        result['mutants_per_class'] = mutant_limit
    directory.mkdir(parents=True, exist_ok=True)
    report = directory / 'report/mutations.xml'
    index = {}
    try:
        # Never attach cached/rerun XML to a new selection, even when mapping fails.
        if report.exists():
            report.unlink()
        index = changed_line_index(root / 'bundles' / DIRECTORIES[module], lines)
        result['changed_line_count'] = sum(len(numbers) for numbers in index.values())
        command = prepare_analysis(root, directory, module, sources, result)
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


def write_results(output, results):
    (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    (output / 'summary.md').write_text(format_summary(results))


def read_selection(output):
    """The manifest `select` wrote: what to mutate, plus the lines that decide the outcome."""
    manifest = json.loads((output / 'selection.json').read_text())
    if not isinstance(manifest, dict) or not {'modules', 'changed_lines'} <= set(manifest):
        raise ValueError('selection.json carries no changed-line map; rerun the select command')
    selected, lines = manifest['modules'], manifest['changed_lines']
    missing = []
    for module, sources in selected.items():
        mapped = lines.get(module)
        if mapped is None:
            missing.append(module)
            continue
        missing.extend(f'{module}/{source}' for source in sorted(set(sources) - set(mapped)))
    if missing:
        raise ValueError('Selection manifest has no changed lines for: ' + ', '.join(missing))
    return selected, lines


def run(root, output, seconds, blocking):
    selected, changed = read_selection(output)
    deadline = time.monotonic() + seconds
    results = []
    pending = len(selected)
    for module, sources in selected.items():
        # Reserve time for later modules while allowing this module to use the rest.
        # Divide equally if the remaining budget cannot supply every pending module's floor.
        remaining = max(0.0, deadline - time.monotonic())
        reserve = min(MIN_MODULE_SECONDS, remaining / pending)
        share = remaining - reserve * (pending - 1)
        module_deadline = time.monotonic() + share
        parts = partition_sources(root / 'bundles' / DIRECTORIES[module], sources)
        limit = None
        if len(parts) > 1:
            targets, _ = resolve_targets(root / 'bundles' / DIRECTORIES[module], sources)
            limit = mutants_per_class(targets)
        for number, part in enumerate(parts, 1):
            part_share = max(0.0, module_deadline - time.monotonic()) / (len(parts) - number + 1)
            result = analyze_module(root, output, module, part,
                                    {source: changed[module][source] for source in part},
                                    time.monotonic() + part_share, round(part_share),
                                    partition=number if len(parts) > 1 else None,
                                    mutant_limit=limit)
            results.append(result)
            write_results(output, results)
        pending -= 1
    if not results:
        write_results(output, results)
    return int(blocking and not is_clean(results))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['select', 'run'])
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--base')
    parser.add_argument('--head')
    parser.add_argument('--seconds', type=int, default=1320)
    parser.add_argument('--blocking', action='store_true')
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if args.command == 'select':
        if not args.base or not args.head:
            parser.error('select requires --base and --head')
        selected, lines = changed_sources(args.root.resolve(), args.base, args.head)
        manifest = {'modules': selected, 'changed_lines': lines}
        (output / 'selection.json').write_text(json.dumps(manifest, indent=2) + '\n')
        print(json.dumps(manifest, indent=2))
        return 0
    if args.seconds <= 0:
        parser.error('--seconds must be positive')
    return run(args.root.resolve(), output, args.seconds, args.blocking)


if __name__ == '__main__':
    raise SystemExit(main())
