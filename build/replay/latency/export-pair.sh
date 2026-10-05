#!/usr/bin/env bash
set -eu
if [ "$#" -ne 1 ]; then echo 'baseline-commit' >&2; exit 2; fi
baseline_commit=$(git rev-parse --verify "$1^{commit}")
candidate_commit=$(git rev-parse --verify HEAD)
git diff --quiet -- . ':(exclude)build/replay/**'
git diff --cached --quiet -- . ':(exclude)build/replay/**'
build/replay/run.sh -Dsirix.replay.bench.variant=candidate -I build/replay/latency/export.init.gradle \
  :sirix-core:exportReplayLatency > build/replay/latency/export-candidate.log 2>&1
printf '%s\n' "$candidate_commit" > build/replay/latency/candidate/source-commit.txt
# Only this worktree is edited; no extra checkout or worktree is created.
restore_candidate() { git restore --source="$candidate_commit" --worktree -- bundles; }
trap restore_candidate EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
git restore --source="$baseline_commit" --worktree -- bundles
# Remove only this task's disposable compiled main classes to exclude deleted candidate types.
rm -rf bundles/sirix-core/build/classes/java/main
build/replay/run.sh -Dsirix.replay.bench.variant=baseline -I build/replay/latency/export.init.gradle \
  :sirix-core:exportReplayLatency > build/replay/latency/export-baseline.log 2>&1
printf '%s\n' "$baseline_commit" > build/replay/latency/baseline/source-commit.txt
restore_candidate
trap - EXIT INT TERM
git diff --quiet -- . ':(exclude)build/replay/**'
git diff --cached --quiet -- . ':(exclude)build/replay/**'
if [ -e build/replay/latency/baseline/classes/io/sirix/service/json/replay/JsonIdentityDeltaReader.class ]; then
  echo 'Baseline artifact contains candidate replay classes' >&2; exit 1
fi
test -f build/replay/latency/candidate/classes/io/sirix/service/json/replay/JsonIdentityDeltaReader.class
cmp build/replay/latency/baseline/java.txt build/replay/latency/candidate/java.txt
cmp build/replay/latency/baseline/jvm-args.txt build/replay/latency/candidate/jvm-args.txt
for variant in baseline candidate; do
  python3 - "$variant" <<'PY'
import hashlib
import json
import sys
from pathlib import Path
artifact = Path('build/replay/latency') / sys.argv[1]
classpath = artifact.joinpath('classpath.txt').read_text().split(':')
for entry in classpath:
    if '/bundles/' in entry and '/build/' in entry:
        raise RuntimeError(f'Classpath still references live build output: {entry}')
files = sorted(p for folder in ('classes', 'harness') for p in artifact.joinpath(folder).rglob('*') if p.is_file())
manifest = dict(source_commit=artifact.joinpath('source-commit.txt').read_text().strip(),
                java=artifact.joinpath('java.txt').read_text().strip(), classpath=classpath,
                jvm_args=artifact.joinpath('jvm-args.txt').read_text().splitlines(),
                dependencies={entry: hashlib.sha256(Path(entry).read_bytes()).hexdigest()
                              for entry in classpath[2:]},
                sha256={str(p.relative_to(artifact)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files})
artifact.joinpath('manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(sys.argv[1], manifest['source_commit'], len(files), 'class/resource files')
PY
done
python3 - <<'PY'
import json
from pathlib import Path
root = Path('build/replay/latency')
baseline = json.loads((root / 'baseline/manifest.json').read_text())
candidate = json.loads((root / 'candidate/manifest.json').read_text())
if baseline['dependencies'] != candidate['dependencies']:
    raise RuntimeError('Baseline and candidate external dependencies differ')
print('Verified identical external dependency paths and SHA-256 hashes')
PY
