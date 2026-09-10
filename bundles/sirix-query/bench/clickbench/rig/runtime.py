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
RIG_WORK = 'CB_RIG_WORK'
CLASSIFICATION = 'CB_RIG_CLASSIFICATION'
CLASSIFIED_DATABASE = 'CB_RIG_CLASSIFIED_DB'
DEFAULT_WORK = Path('bundles/sirix-query/build/diagnostics/rig')
POINTER_FILE = 'current-100m-dir.txt'
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


def validate_output_location(output, source):
    output = Path(output).resolve()
    source = Path(source).resolve()
    try:
        relative = output.relative_to(source)
    except ValueError:
        return output
    ignored = subprocess.run(['git', 'check-ignore', '--quiet', '--no-index', '--', str(relative)],
                             cwd=source, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if ignored.returncode == 0:
        return output
    if ignored.returncode == 1:
        raise ValueError(f'rig output inside a source checkout must be Git-ignored: {output}; '
                         f'use an ignored build path or a location outside {source}')
    raise RuntimeError(f'cannot determine whether rig output is ignored: {ignored.stderr.strip()}')


def source_identity(source):
    """Bind a build to its checkout, including staged, unstaged and untracked inputs."""
    source = Path(source)
    head = subprocess.check_output(['git', 'rev-parse', '--verify', 'HEAD'], cwd=source, text=True).strip()
    names = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'],
                                    cwd=source).split(b'\0')
    modified = subprocess.check_output(['git', 'diff', '--name-only', '-z', 'HEAD', '--'], cwd=source).split(b'\0')
    untracked = subprocess.check_output(['git', 'ls-files', '-z', '--others', '--exclude-standard'],
                                        cwd=source).split(b'\0')
    digest = hashlib.sha256()
    for name in sorted(set(names)-{b''}):
        path = source/os.fsdecode(name)
        digest.update(name+b'\0')
        if path.is_symlink():
            digest.update(b'link\0'+os.fsencode(os.readlink(path))+b'\0')
        if path.is_file():
            digest.update(bytes.fromhex(file_hash(path)))
        elif not path.exists():
            digest.update(b'deleted\0')
        else:
            raise ValueError(f'cannot attest build input: {path}')
    return dict(head=head, files_sha256=digest.hexdigest(),
                modified=sorted(os.fsdecode(name) for name in set(modified+untracked)-{b''}))


def verify_build_execution(runtime):
    execution = runtime.get('build_execution', {})
    if execution.get('rerun_tasks') is not True or execution.get('build_cache') is not False:
        raise ValueError('runtime lacks a forced fresh build: prepare with --rerun-tasks --no-build-cache')
    tasks = execution.get('tasks', {})
    for name in (':sirix-core:compileJava', ':sirix-query:compileJava'):
        if name not in tasks or tasks[name].get('no_source'):
            raise ValueError(f'runtime build did not compile required task {name}')
    for name, state in tasks.items():
        if state.get('no_source') is True and state.get('skip_message') == 'NO-SOURCE':
            continue
        if (state.get('executed') is not True or state.get('did_work') is not True
                or state.get('skipped') is not False or state.get('skip_message') is not None):
            raise ValueError(f'runtime build task {name} did not execute freshly: {state}; prepare again')


def finish_build(export, source, before):
    after = source_identity(source)
    if after != before:
        raise ValueError(f'source changed during runtime build: before={before}, after={after}')
    runtime = json.loads(Path(export).read_text())
    verify_build_execution(runtime)
    runtime.update(source_commit=before['head'], source_worktree=str(Path(source).resolve()),
                   build_source=before)
    return runtime


def harness_provenance(source):
    return dict(measurement_harness_sha256={name: file_hash(Path(source)/BENCH_SOURCES/name)
                                           for name in HARNESS_SOURCES},
                query_catalog_sha256=file_hash(Path(source)/BENCH_SOURCES/'ClickBenchQueries.java'))


def rig_work():
    """The rig working directory, resolved exactly as rig.env resolves it, so an entry point that
    never sources rig.env still finds the pointer file the campaign load wrote."""
    work = os.environ.get(RIG_WORK)
    return Path(work) if work and work.strip() else ROOT/DEFAULT_WORK


def campaign_pointers():
    """Every place the rig names the campaign 100M database, most authoritative first: the pointer
    file `load100m.sh` rewrites on each reload, then `CB100M_DIR`. Each entry carries the source that
    named it and whether it still resolves. A pointer naming a directory that no longer exists is
    `stale`: it can still identify the campaign database, but it can never prove that some other
    target is not it."""
    found = []
    pointer = rig_work()/POINTER_FILE
    try:
        named = pointer.read_text().strip()
    except OSError:
        named = ''
    if named:
        found.append((named, str(pointer), 'resolved' if Path(named).is_dir() else 'stale'))
    named = (os.environ.get(CAMPAIGN_DIRECTORY) or '').strip()
    if named:
        found.append((named, CAMPAIGN_DIRECTORY, 'resolved' if Path(named).is_dir() else 'stale'))
    return found


def canonical(path):
    """The name an identity comparison and a recorded conclusion must both use: absolute, with every
    symlink resolved as far as the path exists. Operators keep a stable alias pointing at whichever
    campaign directory `load100m.sh` loaded last, so an unresolved alias is a name that can come to
    mean a different database than the one a decision was reached about."""
    return Path(os.path.realpath(Path(path).absolute()))


def same_database(left, right):
    """Identity decides while both exist, as ClickBenchRigLease.sameDatabase does; otherwise the
    canonical paths do, because the campaign load names its target before creating it."""
    left, right = canonical(left), canonical(right)
    if left.exists() and right.exists():
        return os.path.samefile(left, right)
    return left == right


def decided_environment(classification):
    """The campaign-identity conclusion a launcher already reached, as the variables its children
    read: the classification itself and the canonical database it applies to. Children consume this
    instead of re-deriving one, so no disagreement between the two pointer sources can reclassify a
    run after its parent has placed it. A decision that names no database cannot be guarded against
    the wrong target and is therefore not exported at all."""
    target = (classification or {}).get('target')
    return {} if not target else {CLASSIFICATION: classification['classification'],
                                  CLASSIFIED_DATABASE: str(target)}


def inherited_decision(database):
    """The classification a rig launcher already took for `database`, or None when this process is
    the one deciding. The decision is honoured only for the database it names: a value held over
    from another target, or hand-set, must never silently reclassify this one."""
    decided = (os.environ.get(CLASSIFICATION) or '').strip()
    named = (os.environ.get(CLASSIFIED_DATABASE) or '').strip()
    if not decided or not named or not same_database(named, database):
        return None
    return decided, named


def classify_target(database, *, declared=False):
    """Decide which JVM envelope a preparation for `database` must freeze, and record the evidence
    for that decision so the frozen manifest can show a classification happened and what it saw.

    A target the rig can place as the campaign 100M database pins the published envelope and never
    negotiates it. A target some resolved pointer proves is a different database declares whatever
    its own flags ask for, so a 1M or scratch gate is not made to reserve the campaign's twenty
    gibibytes. A named target no pointer can place is neither: it is `unplaceable`, and the envelope
    it may freeze is decided in freeze_runtime, which is the only place that knows whether the
    ambiguity changes anything. A preparation naming no database at all stays campaign. `declared`
    is the operator asserting the target is not the campaign database; it is refused when a pointer
    says otherwise.

    A launcher that already decided this database's identity exports its conclusion, and that
    conclusion is final: no pointer is consulted at all, so this process cannot reach a different
    answer than the parent whose lease it runs under.
    """
    target = None if database is None else str(canonical(database))

    def evidence(entry, classification, decided_by):
        named, source, state = entry if entry else ('unset', 'unset', 'unset')
        return dict(target=target, campaign_pointer=named, pointer_source=source, pointer_state=state,
                    classification=classification, decided_by=decided_by)

    if database is None:
        return evidence(None, 'campaign', 'unnamed-target')
    inherited = inherited_decision(database)
    if inherited is not None:
        decided, named = inherited
        if declared and decided == 'campaign':
            raise ValueError(f'{target} is the campaign 100M database, decided by the rig launcher '
                             f'that owns this lease; its envelope is mandatory and cannot be declared away')
        return evidence((named, CLASSIFICATION, 'inherited'), decided, 'inherited-decision')
    consulted = campaign_pointers()
    for entry in consulted:
        if same_database(Path(entry[0])/'db', database):
            if declared:
                raise ValueError(f'{target} is the campaign 100M database, named by {entry[1]}; its '
                                 'envelope is mandatory and cannot be declared away')
            return evidence(entry, 'campaign', 'campaign-database')
    placed = next((entry for entry in consulted if entry[2] == 'resolved'), None)
    if placed:
        return evidence(placed, 'other', 'campaign-database')
    unplaced = consulted[0] if consulted else None
    if declared:
        return evidence(unplaced, 'other', 'operator-declaration')
    return evidence(unplaced, 'unplaceable', 'unresolved-pointer')


def prepare_revision(reference, output, extra_args=(), classification=None):
    output = validate_output_location(output, ROOT)
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
        before = source_identity(source)
        if before['head'] != commit:
            raise ValueError(f'build checkout HEAD {before["head"]} does not match requested revision {commit}')
        export = output/'export.json'
        flags = CANONICAL_ARGS+list(extra_args)
        if any(any(character.isspace() for character in flag) for flag in flags):
            raise ValueError('Gradle JVM argument export requires arguments without embedded whitespace')
        subprocess.run([str(source/'gradlew'), '--no-daemon', '--console=plain', '--rerun-tasks', '--no-build-cache',
                        '-I', str(RIG/'export-runtime.gradle'),
                        ':sirix-query:exportRigRuntime', '-Prig.runtimeOut='+str(export),
                        '-Pclickbench.jvmArgs='+' '.join(flags)], cwd=source, stdout=log,
                       stderr=subprocess.STDOUT, check=True)
    runtime = finish_build(export, source, before)
    runtime.update(provenance)
    runtime['harness_overlay'] = 'current benchmark mains and process guard only; engine sources stay at source_commit'
    return freeze_runtime(runtime, output/'frozen', classification)


def remove_source_worktree(repository, source):
    """Deregister and delete a scratch checkout. Git refuses any path it did not register."""
    subprocess.run(['git', 'worktree', 'remove', '--force', str(Path(source).resolve())],
                   cwd=repository, check=True, capture_output=True, text=True)


def freeze_runtime(runtime, output, classification=None):
    """`classification` is what classify_target decided about this runtime's database. Both the
    decision and the envelope it implies are recorded in the manifest and hashed into its identity,
    so every later round verifies against the envelope this run actually declared and the artifact
    carries the evidence for its own classification. The default is an unnamed target."""
    classification = classify_target(None) if classification is None else dict(classification)
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
    frozen['campaign_classification'] = classification
    frozen['envelope'] = envelope_for(classification, scan_jvm_arguments(frozen['jvm_args']))
    frozen['manifest_path'] = str(output/'runtime.json')
    frozen['runtime_id'] = hashlib.sha256(json.dumps(frozen, sort_keys=True).encode()).hexdigest()
    validate_runtime(frozen)
    (output/'runtime.json').write_text(json.dumps(frozen, indent=2)+'\n')
    return frozen


def envelope_for(classification, settings):
    """The envelope a runtime may freeze, given what the rig could decide about its database and
    what its flags actually ask for.

    An unplaceable target is the only interesting case. Asking for the campaign envelope decides
    nothing -- it is what CANONICAL_ARGS already give and it is valid on any database -- so the run
    proceeds. Asking for anything else would silently produce an invalid campaign measurement if the
    target turns out to be the 100M database, and the rig cannot tell, so it refuses rather than
    guess. `--declare-envelope` is how a person takes that decision instead.
    """
    if classification['classification'] == 'campaign':
        return dict(CAMPAIGN_ENVELOPE)
    if classification['classification'] == 'unplaceable' and settings != CAMPAIGN_ENVELOPE:
        raise ValueError(
            f'cannot decide the JVM envelope for {classification["target"]}: no campaign pointer '
            f'resolves, so the rig cannot tell whether this is the 100M database, and these flags ask '
            f'for {settings} rather than the campaign envelope {CAMPAIGN_ENVELOPE}. Make a pointer '
            f'resolvable -- {rig_work()/POINTER_FILE} is what load100m.sh writes, or export '
            f'{CAMPAIGN_DIRECTORY} -- or pass --declare-envelope to assert this target is not the '
            f'campaign database.')
    return settings


def prepare_current(output, extra_args=(), classification=None):
    """Freeze the current worktree, including local changes, for wrapper scripts."""
    output = validate_output_location(output, ROOT)
    output.mkdir(exist_ok=False, parents=True)
    export = output/'export.json'
    flags = CANONICAL_ARGS+list(extra_args)
    if any(any(character.isspace() for character in flag) for flag in flags):
        raise ValueError('Gradle JVM arguments cannot contain embedded whitespace')
    before = source_identity(ROOT)
    with (output/'build.log').open('x') as log:
        subprocess.run([str(ROOT/'gradlew'), '--no-daemon', '--console=plain', '--rerun-tasks', '--no-build-cache',
                        '-I', str(RIG/'export-runtime.gradle'),
                        ':sirix-query:exportRigRuntime', '-Prig.runtimeOut='+str(export),
                        '-Pclickbench.jvmArgs='+' '.join(flags)], cwd=ROOT, stdout=log,
                       stderr=subprocess.STDOUT, check=True)
    runtime = finish_build(export, ROOT, before)
    runtime.update(tracked_diff_sha256=hashlib.sha256(subprocess.check_output(['git', 'diff', 'HEAD', '--'], cwd=ROOT)).hexdigest(),
                   source_status=subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True))
    runtime.update(harness_provenance(ROOT))
    return freeze_runtime(runtime, output/'frozen', classification)


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


def verify_scored_runtime(runtime, *, check_source=True):
    """A scored launch needs an on-disk freeze and a witnessed build of this source checkout.

    Paired arms each name their own build checkout; neither is compared to the driver's HEAD.
    Exporting an already completed leg checks the frozen evidence without requiring that its
    scratch source checkout still exist. Collection always checks the checkout again.
    """
    manifest = runtime.get('manifest_path')
    if not manifest or not Path(manifest).is_file():
        raise ValueError('scored run requires an existing frozen runtime manifest; prepare the runtime again')
    if json.loads(Path(manifest).read_text()) != runtime:
        raise ValueError(f'frozen runtime manifest mismatch: {manifest}')
    verify_runtime(runtime)
    verify_build_execution(runtime)
    built = runtime.get('build_source', {})
    if not built.get('head') or not built.get('files_sha256') or built['head'] != runtime.get('source_commit'):
        raise ValueError('runtime source identity does not match its recorded build HEAD')
    allowed = {str(BENCH_SOURCES/name) for name in HARNESS_SOURCES} if runtime.get('harness_overlay') else set()
    if not isinstance(built.get('modified'), list) or set(built['modified'])-allowed:
        raise ValueError(f'build inputs do not match checked-out HEAD {built["head"]}: '
                         f'uncommitted paths {built.get("modified")}; commit changes and prepare again')
    if check_source:
        checkout = Path(runtime['source_worktree'])
        if not checkout.is_dir():
            raise ValueError(f'build checkout is unavailable for source verification: {checkout}; prepare again')
        current = source_identity(checkout)
        if current['head'] != built['head']:
            raise ValueError(f'runtime source HEAD mismatch: built {built["head"]}, checkout {current["head"]} '
                             f'at {checkout}; prepare again')
        if current != built:
            raise ValueError(f'runtime source inputs changed since build at {checkout}; prepare again')


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
    if runtime.get('envelope') != CAMPAIGN_ENVELOPE:
        # A runtime below the campaign envelope may only open the database it was classified and
        # frozen for. That binding needs no pointer, so it still holds where classification cannot.
        target = (runtime.get('campaign_classification') or {}).get('target')
        if target is None or not same_database(target, database):
            raise ValueError(f'this runtime was frozen below the campaign envelope for {target}; it must '
                             f'not open {database}. Prepare a runtime for this database instead.')
        for named, source, _ in campaign_pointers():
            if same_database(Path(named)/'db', database):
                raise ValueError(f'{database} is the campaign 100M database, named by {source}; it requires '
                                 f'a runtime frozen at the campaign envelope {CAMPAIGN_ENVELOPE}')
    argv = [runtime['java'], *runtime['jvm_args'], '-cp', os.pathsep.join(runtime['classpath']),
            runtime['main_class'], str(database), '--tries', str(tries)]
    if queries is not None:
        requested = list(queries)
        if not requested or len(set(requested)) != len(requested) or any(q not in range(43) for q in requested):
            raise ValueError('query indices must be unique and between 0 and 42')
        argv += ['--queries', ','.join(map(str, requested))]
    return argv
