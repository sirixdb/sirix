#!/usr/bin/env bash
set -euo pipefail
cd /home/johannes/.treehouse/sirix-cdde48/3/sirix
work="$PWD/bundles/sirix-query/build/diagnostics/rig-trust"
for i in $(seq -w 1 20); do
  tag="fast33-$i"
  echo "START $tag $(date -Ins)"
  python3 "$work/fast-tries-probe.py" "$tag" --expected-pl1-uw 50000000 --expected-pl2-uw 50000000 > "$work/$tag-controller.log" 2>&1
  python3 - "$work/$tag" <<'PY'
import json,re,sys
from pathlib import Path
p=Path(sys.argv[1]); verdict=json.loads((p/'verdict.json').read_text()); assert verdict['exit_code']==0
rows=re.findall(r'^# q(\d+) try (\d+): wall=([0-9.]+) s',(p/'suite100m.log').read_text(),re.M)
assert len(rows)==165
assert {(int(q),int(t)) for q,t,_ in rows}=={(q,t) for q in (2,3,6,21,42) for t in range(1,34)}
for line in (p/'telemetry.jsonl').read_text().splitlines():
    values=json.loads(line)['values']
    assert all(int(values[f'/sys/class/powercap/intel-rapl:0/constraint_{i}_power_limit_uw'])==50000000 for i in (0,1))
PY
  echo "COMPLETE $tag $(date -Ins)"
done
echo FAST33_SERIES_COMPLETE
