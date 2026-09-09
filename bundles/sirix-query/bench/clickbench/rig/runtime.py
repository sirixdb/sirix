"""Build and freeze benchmark runtimes before launching any measured JVM."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

RIG = Path(__file__).resolve().parent
ROOT = RIG.parents[4]
MAIN = 'io.sirix.query.bench.clickbench.ClickBenchRunMain'
JVM_ENV_OPTIONS = ('JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS')
JDK_FILES = ('bin/java', 'lib/modules', 'lib/server/libjvm.so', 'release')
BENCH_SOURCES = Path('bundles/sirix-query/src/main/java/io/sirix/query/bench/clickbench')
HARNESS_SOURCES = ('ClickBenchRunMain.java', 'ClickBenchLoadMain.java', 'ClickBenchRigLease.java')
CAMPAIGN_DIRECTORY = 'CB100M_DIR'
CAMPAIGN_ENVELOPE = dict(initial_heap=6 << 30, maximum_heap=14 << 30, arena=10 << 30, eager=5 << 30,
                         jvmci_compiler=False)
CANONICAL_ARGS = [
    '-Xms6g', '-Xmx14g', '-Dsirix.offheap.bytes=10737418240',
    '-XX:+UnlockExperimentalVMOptions', '-XX:-UseJVMCICompiler',
    '-Dsirix.projection.eagerMaterializeBytes=5368709120',
    '-Dsirix.query.autoVectorize=true', '-Dsirix.chunkedBody.enable=true',
    '-Dsirix.chunkedBody.targetChunkBytes=16384',
]


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def validate_environment():
    for name in JVM_ENV_OPTIONS:
        if os.environ.get(name):
            raise ValueError(f'{name} can override the recorded JVM envelope; clear it before measuring')


def jdk_hashes(java):
    home = Path(java).resolve().parent.parent
    return {name: file_hash(home/name) for name in JDK_FILES}


def artifact_hash(path):
    path = Path(path)
    if path.is_file():
        return file_hash(path)
    if not path.is_dir():
        raise ValueError(f'missing runtime artifact: {path}')
    digest = hashlib.sha256()
    for item in sorted(path.rglob('*')):
        if item.is_symlink():
            raise ValueError(f'runtime artifacts must not contain mutable symlinks: {item}')
        if item.is_file():
            digest.update(str(item.relative_to(path)).encode()+b'\0'+bytes.fromhex(file_hash(item)))
    return digest.hexdigest()


def resolve_revision(reference):
    return subprocess.check_output(['git', 'rev-parse', '--verify', '--end-of-options', reference+'^{commit}'],
                                   cwd=ROOT, text=True).strip()


def harness_provenance(source):
    return dict(measurement_harness_sha256={name: file_hash(Path(source)/BENCH_SOURCES/name)
                                           for name in HARNESS_SOURCES},
                query_catalog_sha256=file_hash(Path(source)/BENCH_SOURCES/'ClickBenchQueries.java'))


def campaign_database(database, campaign=None):
    """Whether `database` is the campaign 100M database, by the same rule the JVM lease applies in
    ClickBenchRigLease.isCampaignDatabase: the pointer names a parent whose `db` child is the
    database. Identity decides while both exist, the normalized paths decide otherwise, and an unset
    or stale pointer simply answers False rather than failing an unrelated small run."""
    if campaign is None:
        campaign = os.environ.get(CAMPAIGN_DIRECTORY, '')
    if database is None or not campaign or not campaign.strip():
        return False
    named, target = Path(campaign)/'db', Path(database)
    if named.exists() and target.exists():
        return os.path.samefile(named, target)
    return os.path.normpath(named.absolute()) == os.path.normpath(target.absolute())


def envelope_for(database):
    """The envelope a preparation for `database` must freeze. The campaign 100M database pins the
    published envelope and never negotiates it; every other target -- a 1M lane, a scratch database,
    an unnamed preparation -- declares whatever its own flags ask for, so a general gate is not made
    to reserve the campaign's twenty gibibytes. An unknown target is treated as the campaign one."""
    return CAMPAIGN_ENVELOPE if database is None or campaign_database(database) else None


def prepare_revision(reference, output, extra_args=(), envelope=CAMPAIGN_ENVELOPE):
    output = Path(output).resolve()
    output.mkdir(exist_ok=False, parents=True)
    commit = resolve_revision(reference)
    source = output/'source'
    with (output/'build.log').open('x') as log:
        subprocess.run(['git', 'worktree', 'add', '--detach', str(source), commit], cwd=ROOT,
                       stdout=log, stderr=subprocess.STDOUT, check=True)
        # Both engines must start through the same instrumentation. Otherwise an old
        # baseline omits the process guard and its JVM startup differs from the candidate.
        # The loader is overlaid too: it never runs here, but it calls the guard, so leaving it
        # at source_commit fails the arm's compile whenever that call's signature has moved.
        for name in HARNESS_SOURCES:
            shutil.copyfile(ROOT/BENCH_SOURCES/name, source/BENCH_SOURCES/name)
        provenance = harness_provenance(source)
        export = output/'export.json'
        flags = CANONICAL_ARGS+list(extra_args)
        if any(any(character.isspace() for character in flag) for flag in flags):
            raise ValueError('Gradle JVM argument export requires arguments without embedded whitespace')
        subprocess.run([str(source/'gradlew'), '--no-daemon', '--console=plain', '-I', str(RIG/'export-runtime.gradle'),
                        ':sirix-query:exportRigRuntime', '-Prig.runtimeOut='+str(export),
                        '-Pclickbench.jvmArgs='+' '.join(flags)], cwd=source, stdout=log,
                       stderr=subprocess.STDOUT, check=True)
    runtime = json.loads(export.read_text())
    runtime['source_commit'] = commit
    runtime['source_worktree'] = str(source)
    runtime.update(provenance)
    runtime['harness_overlay'] = 'current benchmark mains and process guard only; engine sources stay at source_commit'
    return freeze_runtime(runtime, output/'frozen', envelope)


def remove_source_worktree(repository, source):
    """Deregister and delete a scratch checkout. Git refuses any path it did not register."""
    subprocess.run(['git', 'worktree', 'remove', '--force', str(Path(source).resolve())],
                   cwd=repository, check=True, capture_output=True, text=True)


def freeze_runtime(runtime, output, envelope=CAMPAIGN_ENVELOPE):
    """`envelope` is the memory and compiler settings this runtime is frozen at, recorded in the
    manifest and hashed into its identity so every later round verifies against the envelope this
    run actually declared. Pass None to declare whatever the prepared flags resolve to."""
    output = Path(output).resolve()
    output.mkdir(exist_ok=False, parents=True)
    copied = []
    hashes = {}
    for index, entry in enumerate(runtime['classpath']):
        original = Path(entry)
        destination = output/f'{index:02}'/original.name
        destination.parent.mkdir()
        original_hash = artifact_hash(original)
        if original.is_dir():
            shutil.copytree(original, destination, symlinks=True)
        else:
            shutil.copy2(original, destination)
        if artifact_hash(destination) != original_hash:
            raise ValueError(f'runtime artifact changed while copying: {original}')
        copied.append(str(destination))
        hashes[str(destination)] = original_hash
    frozen = dict(runtime, source_classpath=[str(Path(entry).resolve()) for entry in runtime['classpath']],
                  classpath=copied, artifact_sha256=hashes)
    frozen['java'] = str(Path(frozen['java']).resolve())
    frozen['java_sha256'] = file_hash(frozen['java'])
    frozen['jdk_sha256'] = jdk_hashes(frozen['java'])
    frozen['java_version'] = subprocess.check_output([frozen['java'], '-version'], stderr=subprocess.STDOUT, text=True)
    frozen['envelope'] = dict(envelope) if envelope is not None else scan_jvm_arguments(frozen['jvm_args'])
    frozen['runtime_id'] = hashlib.sha256(json.dumps(frozen, sort_keys=True).encode()).hexdigest()
    validate_runtime(frozen)
    (output/'runtime.json').write_text(json.dumps(frozen, indent=2)+'\n')
    return frozen


def prepare_current(output, extra_args=(), envelope=CAMPAIGN_ENVELOPE):
    """Freeze the current worktree, including local changes, for wrapper scripts."""
    output = Path(output).resolve()
    output.mkdir(exist_ok=False, parents=True)
    export = output/'export.json'
    flags = CANONICAL_ARGS+list(extra_args)
    if any(any(character.isspace() for character in flag) for flag in flags):
        raise ValueError('Gradle JVM arguments cannot contain embedded whitespace')
    with (output/'build.log').open('x') as log:
        subprocess.run([str(ROOT/'gradlew'), '--no-daemon', '--console=plain', '-I', str(RIG/'export-runtime.gradle'),
                        ':sirix-query:exportRigRuntime', '-Prig.runtimeOut='+str(export),
                        '-Pclickbench.jvmArgs='+' '.join(flags)], cwd=ROOT, stdout=log,
                       stderr=subprocess.STDOUT, check=True)
    runtime = json.loads(export.read_text())
    runtime.update(source_commit=resolve_revision('HEAD'), source_worktree=str(ROOT),
                   tracked_diff_sha256=hashlib.sha256(subprocess.check_output(['git', 'diff', 'HEAD', '--'], cwd=ROOT)).hexdigest(),
                   source_status=subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True))
    runtime.update(harness_provenance(ROOT))
    return freeze_runtime(runtime, output/'frozen', envelope)


def parse_bytes(text):
    match = re.fullmatch(r'([0-9]+)([kKmMgGtT]?)', text)
    if not match:
        raise ValueError(f'invalid JVM byte size: {text}')
    exponent = {'': 0, 'k': 1, 'm': 2, 'g': 3, 't': 4}[match[2].lower()]
    return int(match[1])*1024**exponent


def scan_jvm_arguments(jvm_args, *, allow_diagnostics=False):
    """Reject anything that could replace the entry point or hide arguments, and return the memory
    and compiler settings the arguments resolve to, last occurrence winning as HotSpot resolves
    them."""
    settings = {}
    option_value = False
    paired_options = {'--add-opens', '--add-exports', '--add-reads', '--add-modules',
                      '--limit-modules', '--enable-native-access'}
    for argument in jvm_args:
        if not isinstance(argument, str) or argument.startswith(('@', '-XX:Flags=', '-XX:VMOptionsFile=',
                                                               '-XX:CompileCommandFile=', '-XX:CompilerDirectivesFile=')):
            raise ValueError('JVM arguments must be explicit strings, without argument files')
        if option_value:
            if not argument or argument.startswith('-'):
                raise ValueError('a JVM module option is missing its value')
            option_value = False
            continue
        if (not argument.startswith('-') or argument in ('-jar', '-m', '-cp', '-classpath', '-p')
                or argument.startswith(('-Xbootclasspath', '-Djava.class.path=', '-Djava.system.class.loader='))
                or argument.split('=', 1)[0] in ('--module', '--source', '--class-path', '--module-path',
                                                 '--patch-module', '--upgrade-module-path')):
            raise ValueError('JVM arguments must not replace the frozen classpath or measurement main class')
        if argument in paired_options:
            option_value = True
            continue
        if argument.startswith('-Xms'):
            settings['initial_heap'] = parse_bytes(argument[4:])
        elif argument.startswith('-Xmx'):
            settings['maximum_heap'] = parse_bytes(argument[4:])
        elif argument.startswith('-XX:InitialHeapSize='):
            settings['initial_heap'] = parse_bytes(argument.split('=', 1)[1])
        elif argument.startswith('-XX:MaxHeapSize='):
            settings['maximum_heap'] = parse_bytes(argument.split('=', 1)[1])
        elif argument.startswith('-Dsirix.offheap.bytes='):
            settings['arena'] = int(argument.split('=', 1)[1])
        elif argument.startswith('-Dsirix.projection.eagerMaterializeBytes='):
            settings['eager'] = int(argument.split('=', 1)[1])
        elif argument in ('-XX:+UseJVMCICompiler', '-XX:-UseJVMCICompiler'):
            settings['jvmci_compiler'] = argument.startswith('-XX:+')
        if not allow_diagnostics and (argument.startswith(('-javaagent:', '-agentlib:', '-agentpath:', '-Xlog',
                                                        '-XX:StartFlightRecording', '-XX:LogFile='))
                                      or argument in ('-XX:+LogCompilation', '-Dsirix.projDiag=true')):
            raise ValueError('profiling flags require a diagnostic run, not a scored paired measurement')
    if option_value:
        raise ValueError('a JVM module option is missing its value')
    return settings


def validate_runtime(runtime, *, allow_diagnostics=False):
    if runtime['main_class'] != MAIN:
        raise ValueError('measurement runtimes must execute ClickBenchRunMain, never a loader')
    if not runtime['classpath'] or not Path(runtime['java']).is_file():
        raise ValueError('runtime requires an existing Java executable and nonempty classpath')
    expected = runtime.get('envelope')
    if not expected:
        raise ValueError('a prepared runtime must declare the JVM envelope it was frozen at')
    settings = scan_jvm_arguments(runtime['jvm_args'], allow_diagnostics=allow_diagnostics)
    if settings != expected:
        raise ValueError(f'JVM envelope mismatch: this runtime declares {expected}, observed {settings}')
    return settings


def verify_runtime(runtime):
    validate_runtime(runtime)
    provenance = {key: value for key, value in runtime.items() if key != 'runtime_id'}
    if hashlib.sha256(json.dumps(provenance, sort_keys=True).encode()).hexdigest() != runtime['runtime_id']:
        raise ValueError('runtime manifest changed after preparation')
    if (len(set(runtime['classpath'])) != len(runtime['classpath'])
            or set(runtime['classpath']) != runtime['artifact_sha256'].keys()):
        raise ValueError('every classpath entry must have exactly one frozen artifact hash')
    if file_hash(runtime['java']) != runtime['java_sha256']:
        raise ValueError('Java executable changed after runtime preparation')
    if jdk_hashes(runtime['java']) != runtime.get('jdk_sha256'):
        raise ValueError('JDK modules, VM library or release identity changed; prepare both runtimes again')
    for path, expected in runtime['artifact_sha256'].items():
        if artifact_hash(path) != expected:
            raise ValueError(f'runtime artifact changed after preparation: {path}')


def verify_shared_dependencies(baseline, candidate):
    """Catch a mutable Maven/Gradle artifact changing between the two builds."""
    def external_jars(runtime):
        sources = runtime.get('source_classpath')
        if sources is None or len(sources) != len(runtime['classpath']):
            raise ValueError('prepared runtime is missing original classpath provenance')
        checkout = Path(runtime['source_worktree']).resolve()
        return {str(Path(source).resolve()): runtime['artifact_sha256'][frozen]
                for source, frozen in zip(sources, runtime['classpath'])
                if Path(source).suffix == '.jar' and not Path(source).resolve().is_relative_to(checkout)}
    left, right = external_jars(baseline), external_jars(candidate)
    changed = [path for path in left.keys() & right.keys() if left[path] != right[path]]
    if changed:
        raise ValueError(f'external artifacts changed in place between builds: {changed}; prepare both arms from stable dependencies')


def verify_shared_harness(baseline, candidate):
    for key in ('measurement_harness_sha256', 'query_catalog_sha256'):
        if not baseline.get(key) or baseline[key] != candidate.get(key):
            raise ValueError(f'paired runtimes differ in {key}; rebuild both with the same instrumentation and query catalog')


def command(runtime, database, *, queries=None, tries=3):
    if tries <= 0:
        raise ValueError('tries must be positive')
    database = Path(database).resolve(strict=True)
    if not database.is_dir():
        raise ValueError('database must be an existing directory; no load or build-projection is permitted')
    if campaign_database(database) and runtime.get('envelope') != CAMPAIGN_ENVELOPE:
        raise ValueError('the campaign 100M database requires a runtime frozen at the campaign envelope '
                         f'{CAMPAIGN_ENVELOPE}; prepare one instead of shrinking the envelope to fit')
    argv = [runtime['java'], *runtime['jvm_args'], '-cp', os.pathsep.join(runtime['classpath']),
            runtime['main_class'], str(database), '--tries', str(tries)]
    if queries is not None:
        requested = list(queries)
        if not requested or len(set(requested)) != len(requested) or any(q not in range(43) for q in requested):
            raise ValueError('query indices must be unique and between 0 and 42')
        argv += ['--queries', ','.join(map(str, requested))]
    return argv
