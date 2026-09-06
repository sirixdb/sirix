#!/bin/bash
# Load the 1M corpus with the segment lane into $D1M (default build/diagnostics/rig/seg1m). ~1-2 min.
# A 1M database is a build output with no version stamp: reload it whenever the WRITE path changed,
# or a gate will judge today's code against yesterday's pages.
#   bash load1m.sh                                   default budget -> a few segments
#   BUDGET=4194304 bash load1m.sh                    4 MB dictionary budget -> ~24 segments (the seal repro)
set -u
. "$(dirname "$0")/rig.env"
[ -f "$SRC1M" ] || { echo "ABORT: source $SRC1M missing"; exit 1; }
take_lock 60
refuse_live_jvm
rm -rf "$D1M/db"; mkdir -p "$D1M"
T0=$(date +%s)
cd "$ROOT" && ./gradlew --no-daemon --console=plain :sirix-query:clickBenchLoad -Pclickbench.args="$D1M/db $SRC1M" \
  -Pclickbench.jvmArgs="$JVM1M -Dclickbench.expectedRows=1000000 $LOADFLAGS \
    ${BUDGET:+-Dsirix.segmentDict.budgetBytes=$BUDGET} -Dsirix.projDiag=true ${EXTRA:-}" > "$D1M/load.log" 2>&1
echo "LOAD_EXIT=$? elapsed=$(( $(date +%s) - T0 ))s dir=$D1M"
grep -E 'Load time|Data size|segment lane:|OutOfMemory|Exception' "$D1M/load.log" | head -6
find "$D1M/db" -name 'sirix.data' -printf '%s\n' | awk '{printf "sirix.data %.1f MB\n", $1/1e6}'
