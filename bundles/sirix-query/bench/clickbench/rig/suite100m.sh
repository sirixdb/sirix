#!/bin/bash
# The scoring leg: all 43 queries against the 100M database, N tries (3 for a leg that scores).
#   bash suite100m.sh 3            -> $D100M/suite100m.log
#   python3 mkleg.py SEG4T $D100M/suite100m.log && python3 rank.py SEG4T SEG3T N1FULL1
# Never time with -Dsirix.projDiag=true (use diag100m.sh to read routes) and never launch it while a
# load or another leg runs: two JVMs in the envelope OOM-kill the box.
set -u
. "$(dirname "$0")/rig.env"
require_100m_db
take_lock 120
refuse_live_jvm
T0=$(date +%s)
cd "$ROOT" && ./gradlew --no-daemon --console=plain :sirix-query:clickBench \
  -Pclickbench.args="$D100M/db --tries ${1:-3}" \
  -Pclickbench.jvmArgs="$JVM100M $SERVE ${EXTRA:-}" > "$D100M/suite100m.log" 2>&1
echo "EXIT=$? elapsed=$(( $(date +%s) - T0 ))s log=$D100M/suite100m.log"
echo "--- declines: $(grep -c 'route=NONE' "$D100M/suite100m.log") ---"
echo "--- slowest (hot) ---"
grep -oE '^[0-9]+ +\| +[0-9.]+ +\| +[0-9.]+ .*route=[a-zA-Z+-]*' "$D100M/suite100m.log" \
  | awk '{print $5, "q"$1, $NF}' | sort -gr | head -12
echo "--- totals (seconds are NOT the metric; score with rank.py) ---"
grep -oE '^[0-9]+ +\| +[0-9.]+ +\| +[0-9.]+ ' "$D100M/suite100m.log" \
  | awk '{c+=$3; h+=$5; n++} END {printf "cold=%.1f s  hot=%.1f s  over %d queries\n", c, h, n}'
echo SUITE100M_DONE
