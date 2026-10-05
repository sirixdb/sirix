#!/usr/bin/env bash
set -eu
for replay_version in FULL DIFFERENTIAL INCREMENTAL SLIDING_SNAPSHOT; do
  replay_log="build/replay/forced-matrix-$replay_version.log"
  if build/replay/run.sh -Dsirix.replay.versioning="$replay_version" -Dsirix.replay.forceRecompute=true \
      :sirix-core:test --tests 'io.sirix.diff.JsonBulkInsertDiffRegressionTest' \
      :sirix-core:spotlessJavaCheck > "$replay_log" 2>&1; then
    replay_rc=0
  else
    replay_rc=$?
  fi
  python3 - "$replay_version" <<'PY'
from pathlib import Path
import json, shutil, sys, xml.etree.ElementTree as ET
variant=sys.argv[1]
output=Path(f'build/replay/forced-matrix-{variant}-results')
output.mkdir(parents=True,exist_ok=True)
summary={key:0 for key in ('tests','failures','errors','skipped')}
files=list(Path('bundles/sirix-core/build/test-results/test').glob('TEST-*.xml'))
for path in files:
    suite=ET.parse(path).getroot()
    for key in summary: summary[key]+=int(suite.get(key,0))
    shutil.copy2(path,output/path.name)
summary['suites']=len(files)
(output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(variant, summary)
PY
  if [ "$replay_rc" -ne 0 ]; then exit "$replay_rc"; fi
  rg -q "REPLAY_CONFIGURATION versioning=$replay_version forceRecompute=true" "$replay_log"
done
