"""Retain completed paired evidence and original steering leg documents; no JVM."""
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tarfile

HERE = Path(__file__).resolve().parent
RIG = HERE.parents[1]


def main(directory):
    source = Path(directory).resolve()
    plan = json.loads((source / 'plan.json').read_text())
    pairs = json.loads((source / 'pairs.json').read_text())
    if plan['planned_pairs'] != 10 or len(pairs) != 10:
        raise ValueError('retention requires the complete prespecified ten-pair study')
    archive_path = HERE / 'comparison.tar.gz'
    if archive_path.exists():
        raise FileExistsError(archive_path)
    reference = {path.name: path.read_bytes() for path in (HERE / 'baseline-answers').iterdir() if path.is_file()}
    expected_names = {'.clickbench-result-format'} | {f'q{query:02}.jsonl' for query in range(43)}
    if reference.keys() != expected_names:
        raise ValueError('the baseline reference must contain exactly 43 answers and the format marker')
    launches = []
    files = []
    documents = {}
    for number, pair in enumerate(pairs, 1):
        for arm, label in (('baseline', 'A'), ('candidate', 'B')):
            leg = source / f'pair-{number:04}-{arm}'
            proof = json.loads((leg / 'answer-equality.json').read_text())
            if not proof['byte_identical'] or proof['mismatches']:
                raise ValueError(f'failed answer proof: {leg}')
            for name, expected in reference.items():
                actual = (leg / 'answers' / name).read_bytes()
                if actual != expected or hashlib.sha256(actual).hexdigest() != proof['sha256'][name]:
                    raise ValueError(f'answer bytes or digest differ: {leg / name}')
            metadata = pair[arm]['rig']
            if metadata['scope'] != 'steering' or metadata['complete'] is not True:
                raise ValueError('only original complete steering legs may be retained')
            destination = RIG / 'legs' / f'query-SEGCP-P{number:02}{label}.json'
            if destination.exists():
                raise FileExistsError(destination)
            documents[destination] = pair[arm]
            launches.append(json.loads((leg / 'launch-allowance.json').read_text()))
            files.extend(path for path in leg.rglob('*') if path.is_file())
    files.extend(path for path in source.iterdir() if path.is_file())
    for arm in ('baseline', 'candidate'):
        runtime = plan[arm + '_runtime']
        # Runtime manifests are retained explicitly below, independent of where a
        # prepared baseline lived. Binaries remain in the frozen worktree runtime.
        (HERE / f'runtime-{arm}.json').write_text(json.dumps(runtime, indent=2) + '\n')
        build = source / f'runtime-{arm}'
        for name in ('build.log', 'export.json'):
            if (build / name).is_file():
                files.append(build / name)
    members = {}
    with tarfile.open(archive_path, 'w:gz') as archive:
        for path in sorted(set(files)):
            name = str(path.relative_to(source))
            data = path.read_bytes()
            members[name] = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
            archive.add(path, arcname=name, recursive=False)
    with tarfile.open(archive_path, 'r:gz') as archive:
        for member in archive:
            data = archive.extractfile(member).read()
            if hashlib.sha256(data).hexdigest() != members[member.name]['sha256']:
                raise ValueError(f'archive verification failed: {member.name}')
    for destination, document in documents.items():
        with destination.open('x') as stream:
            json.dump(document, stream, indent=2)
            stream.write('\n')
    (HERE / 'comparison-manifest.json').write_text(json.dumps(members, indent=2) + '\n')
    for name in ('plan.json', 'pairs.json', 'report.json', 'report.md', 'queries.csv',
                 'study-summary.json', 'query-deltas.csv'):
        shutil.copyfile(source / name, HERE / name)
    (HERE / 'allowance.json').write_text(json.dumps(dict(
        profiling_jvms=1, paired_jvms=len(launches), completed_pairs=len(pairs),
        authorized_maximum_pairs=12, fixed_plan_complete=True,
        minimum_available_bytes=min(row['available_bytes'] for row in launches),
        correct_answers_per_leg=43, all_legs_byte_identical=True,
        further_100m_runs='No additional run is prescribed by this fixed study; ask Firstmate for any new collection, including pipeline verification.'), indent=2) + '\n')
    print(f'Retained and verified {len(members)} archive members, twenty steering legs, and all answer proofs.')


if __name__ == '__main__':
    main(sys.argv[1])
