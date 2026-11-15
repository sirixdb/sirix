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
# Execution time reserved for each module still waiting, so none is starved by an earlier one.
MIN_MODULE_SECONDS = 120
# A large diff cannot afford every mutant of every changed class. A mutant whose covering test
# hangs costs a whole minion restart, and those restarts - not the mutant count - exhausted the
# budget for PR 1351's 57 changed sirix-core classes once the test scope was fixed. Sample per
# class instead, so the work scales with the diff and even a small diff has a per-class cap.
MUTANT_BUDGET = 300
MIN_MUTANTS_PER_CLASS = 4
MAX_MUTANTS_PER_CLASS = 40


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


def changed_sources(root, base, head):
    base_sha, head_sha = commit_id(base), commit_id(head)
    raw = subprocess.check_output(['git', '-C', str(root), 'diff', '--name-only', '-z',
                                   '--diff-filter=ACMRT', f'{base_sha}...{head_sha}', '--', 'bundles/'],
                                  shell=False)
    paths = [repository_path(os.fsdecode(path)) for path in raw.split(b'\0') if path]
    selected = select_sources(paths)
    sources = [f'bundles/{DIRECTORIES[module]}/{source}'
               for module, files in selected.items() for source in files]
    if sources:
        tree = subprocess.check_output(['git', '--literal-pathspecs', '-C', str(root), 'ls-tree',
                                        '-r', '--name-only', '-z', head_sha, '--', *sources],
                                       shell=False)
        tracked = {os.fsdecode(path) for path in tree.split(b'\0') if path}
        if set(sources) != tracked:
            raise ValueError('Selected source path is absent from the head commit')
    return selected


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


def resolve_targets(module_root, sources):
    expected = {}
    for source in sources:
        path = source_path(module_root, source)
        if not path.is_file():
            continue
        text = re.sub(r'/\*.*?\*/|//[^\n]*', '', path.read_text(), flags=re.S)
        package = re.search(r'^\s*package\s+([\w.]+)', text, re.M)
        expected[(package.group(1) if package else '', path.name)] = source
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
    text = source_path(module_root, source).read_text()
    package = re.search(r'^\s*package\s+([\w.]+)', text, re.M)
    return package.group(1) if package else ''


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


def test_scope(module_root, sources):
    """Bounded per-source test selection. No input may widen the scope to the module suite."""
    candidates = []
    tests = module_test_classes(module_root)
    for source in sources:
        patterns, kind = resolve_tests(Path(source).stem,
                                       source_package(module_root, source), tests)
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


def read_report(path):
    mutations = ET.parse(path).getroot().findall('mutation')
    if not mutations:
        raise ValueError('PIT produced no mutations; no score is available')
    counts = Counter(mutation.get('status', 'UNKNOWN') for mutation in mutations)
    survivors = []
    for mutation in mutations:
        if mutation.get('detected') != 'true':
            fields = [mutation.get('status', 'UNKNOWN')]
            fields.extend(mutation.findtext(tag, '?') for tag in
                          ('mutatedClass', 'mutatedMethod', 'lineNumber', 'description'))
            survivors.append(' | '.join(fields))
    return {'total': len(mutations), 'killed': counts['KILLED'],
            'survived': counts['SURVIVED'], 'no_coverage': counts['NO_COVERAGE'],
            'detected': sum(m.get('detected') == 'true' for m in mutations),
            'statuses': dict(sorted(counts.items())), 'survivors': survivors}


def is_clean(results):
    return all(result['state'] == 'complete' and result.get('total', 0) > 0
               and result.get('detected') == result['total'] for result in results)


def format_summary(results):
    def escape(value):
        return html.escape(str(value)).replace('|', '&#124;').replace('\n', ' ')

    lines = ['# Changed-source mutation testing', '',
             'Informational unless `MUTATION_BLOCKING` is enabled. Full reports, targets and logs '
             'are in the `pr-mutation` artifact.', '']
    if not results:
        return '\n'.join(lines + ['No changed main Java/Kotlin sources; PIT skipped.', ''])
    for result in results:
        lines.extend([f"## {escape(result['module'])}: {escape(result['state'])}", ''])
        if 'error' in result:
            lines.extend([escape(result['error']), ''])
        if 'tests' in result:
            lines.extend([f"Changed classes mutated: {len(result['targets'])}. "
                          f"Test patterns: {len(result['tests'])}. "
                          f"Mutants sampled per class: {result['mutants_per_class']}.", ''])
        if result.get('unmapped'):
            lines.extend(['No test mapped to these changed sources; their mutants report '
                          '`NO_COVERAGE`:', ''])
            lines.extend('- ' + escape(value) for value in result['unmapped'])
            lines.append('')
        if 'total' in result:
            total = result['total']
            lines.extend([f"Killed: {result['killed']}/{total} ({100 * result['killed'] / total:.1f}%). "
                          f"Survived: {result.get('survived', 0)}. "
                          f"No coverage: {result.get('no_coverage', 0)}. "
                          f"Detected (PIT score, includes timeouts): {result['detected']}/{total} "
                          f"({100 * result['detected'] / total:.1f}%).", '',
                          escape(result['statuses']), ''])
            if result['survivors']:
                lines.extend(['Undetected mutants (all entries also in `results.json`):', ''])
                # Keep GitHub's 1 MiB job-summary limit; the artifact retains every entry.
                lines.extend('- ' + escape(value) for value in result['survivors'][:100])
                if len(result['survivors']) > 100:
                    lines.append('- Remaining entries: see `results.json`.')
                lines.append('')
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


def prepare_analysis(root, directory, module, sources, result):
    module_root = root / 'bundles' / DIRECTORIES[module]
    targets, missing = resolve_targets(module_root, sources)
    if missing:
        raise ValueError('No compiled classes for changed sources: ' + ', '.join(missing))
    if not targets:
        raise ValueError('Empty target selection; refusing whole-module mutation')
    tests, unmapped, mapping = test_scope(module_root, sources)
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


def attach_report(result, report):
    try:
        result.update(read_report(report))
    except (OSError, ValueError, ET.ParseError) as error:
        if result['state'] == 'complete':
            result.update(state='error', error=str(error))


def analyze_module(root, output, module, sources, deadline, seconds):
    result = {'module': module, 'sources': sources, 'state': 'error'}
    if module not in DIRECTORIES:
        return {**result, 'error': 'Unknown module; refusing to execute PIT'}
    directory = output / module
    directory.mkdir(parents=True, exist_ok=True)
    report = directory / 'report/mutations.xml'
    try:
        # Never attach cached/rerun XML to a new selection, even when mapping fails.
        if report.exists():
            report.unlink()
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
    attach_report(result, report)
    return result


def write_results(output, results):
    (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    (output / 'summary.md').write_text(format_summary(results))


def run(root, output, seconds, blocking):
    selected = json.loads((output / 'selection.json').read_text())
    deadline = time.monotonic() + seconds
    results = []
    pending = len(selected)
    for module, sources in selected.items():
        # Reserve time for later modules while allowing this module to use the rest.
        # Divide equally if the remaining budget cannot supply every pending module's floor.
        remaining = max(0.0, deadline - time.monotonic())
        reserve = min(MIN_MODULE_SECONDS, remaining / pending)
        share = remaining - reserve * (pending - 1)
        results.append(analyze_module(root, output, module, sources,
                                      time.monotonic() + share, round(share)))
        pending -= 1
        write_results(output, results)
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
        selection = changed_sources(args.root.resolve(), args.base, args.head)
        (output / 'selection.json').write_text(json.dumps(selection, indent=2) + '\n')
        print(json.dumps(selection, indent=2))
        return 0
    if args.seconds <= 0:
        parser.error('--seconds must be positive')
    return run(args.root.resolve(), output, args.seconds, args.blocking)


if __name__ == '__main__':
    raise SystemExit(main())
