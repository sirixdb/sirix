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


def changed_sources(root, base, head):
    paths = subprocess.check_output(['git', '-C', str(root), 'diff', '--name-only', '-z',
                                     '--diff-filter=ACMRT', f'{base}...{head}', '--', 'bundles/'])
    return select_sources(os.fsdecode(path) for path in paths.split(b'\0') if path)


def class_source(data):
    """Read JVM class identity and SourceFile (JVMS 4.4, 4.7.10), without starting a JVM."""
    offset = 0

    def take(size):
        nonlocal offset
        if offset + size > len(data):
            raise ValueError('Truncated class file')
        value = data[offset:offset + size]
        offset += size
        return value

    def u2():
        return struct.unpack('>H', take(2))[0]

    if take(4) != b'\xca\xfe\xba\xbe':
        raise ValueError('Invalid class magic')
    take(4)  # minor/major version
    pool = [None] * u2()
    index = 1
    sizes = {3: 4, 4: 4, 5: 8, 6: 8, 8: 2, 9: 4, 10: 4, 11: 4, 12: 4,
             15: 3, 16: 2, 17: 4, 18: 4, 19: 2, 20: 2}
    while index < len(pool):
        tag = take(1)[0]
        if tag == 1:
            pool[index] = take(u2()).decode('utf-8', errors='replace')
        elif tag == 7:
            pool[index] = u2()
        elif tag in sizes:
            take(sizes[tag])
        else:
            raise ValueError(f'Unsupported constant-pool tag: {tag}')
        index += 2 if tag in (5, 6) else 1
    take(2)  # access flags
    name = pool[pool[u2()]].replace('/', '.')
    take(2)  # superclass
    take(2 * u2())  # interfaces

    def attributes():
        for _ in range(u2()):
            attribute = pool[u2()]
            value = take(struct.unpack('>I', take(4))[0])
            yield attribute, value

    for _ in range(2):  # fields, methods
        for _ in range(u2()):
            take(6)  # access flags, name, descriptor
            for _ in attributes():
                pass
    for attribute, value in attributes():
        if attribute == 'SourceFile':
            return name, pool[struct.unpack('>H', value)[0]]
    return name, None


def resolve_targets(module_root, sources):
    expected = {}
    for source in sources:
        path = module_root / source
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


def test_scope(module_root, sources):
    """Prefer directly named tests; without one for every source, use the module suite."""
    tests = []
    for source in sources:
        path = Path(source)
        text = (module_root / path).read_text()
        package = re.search(r'^\s*package\s+([\w.]+)', text, re.M)
        prefix = package.group(1) + '.' if package else ''
        matches = []
        for language, suffix in (('java', '.java'), ('kotlin', '.kt')):
            directory = module_root / f'src/test/{language}' / prefix.replace('.', '/')
            for ending in ('Test', 'Tests'):
                name = path.stem + ending
                if (directory / (name + suffix)).is_file():
                    matches.append(prefix + name + '*')
        if not matches:
            return ['*']
        tests.extend(matches)
    return sorted(set(tests))


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
        if 'total' in result:
            total = result['total']
            lines.extend([f"Killed: {result['killed']}/{total} ({100 * result['killed'] / total:.1f}%). "
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
    with log.open('w') as stream:
        with subprocess.Popen(command, cwd=root, stdout=stream, stderr=subprocess.STDOUT,
                              start_new_session=True) as process:
            try:
                return process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise


def run(root, output, seconds, blocking):
    selected = json.loads((output / 'selection.json').read_text())
    deadline = time.monotonic() + seconds
    results = []
    for module, sources in selected.items():
        directory = output / module
        directory.mkdir(parents=True, exist_ok=True)
        report = directory / 'report/mutations.xml'
        result = {'module': module, 'sources': sources, 'state': 'error'}
        try:
            # Never attach cached/rerun XML to a new selection, even when mapping fails.
            if report.exists():
                report.unlink()
            module_root = root / 'bundles' / DIRECTORIES[module]
            targets, missing = resolve_targets(module_root, sources)
            if missing:
                raise ValueError('No compiled classes for changed sources: ' + ', '.join(missing))
            if not targets:
                raise ValueError('Empty target selection; refusing whole-module mutation')
            tests = test_scope(module_root, sources)
            result.update(targets=targets, tests=tests)
            target_file = directory / 'targets.txt'
            target_file.write_text('\n'.join(targets) + '\n')
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired('PIT shared deadline', seconds)
            command = [str(root / 'gradlew'), f':{module}:pitest', '--no-daemon',
                       f'-PprMutationModule={module}', f'-PprMutationTargetsFile={target_file}',
                       f'-PprMutationReportDir={directory / "report"}',
                       f'-PprMutationTests={",".join(tests)}']
            code = execute(command, root, directory / 'pitest.log', remaining)
            if code != 0:
                raise ValueError(f'PIT exited {code}; see {module}/pitest.log')
            result['state'] = 'complete'
        except subprocess.TimeoutExpired:
            result.update(state='timed out', error=f'Shared {seconds}s execution deadline exceeded')
        except (OSError, ValueError, IndexError, struct.error) as error:
            result.update(state='error', error=str(error))
        try:
            result.update(read_report(report))
        except (OSError, ValueError, ET.ParseError) as error:
            if result['state'] == 'complete':
                result.update(state='error', error=str(error))
        results.append(result)
        (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
        (output / 'summary.md').write_text(format_summary(results))
    if not results:
        (output / 'results.json').write_text('[]\n')
        (output / 'summary.md').write_text(format_summary([]))
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
