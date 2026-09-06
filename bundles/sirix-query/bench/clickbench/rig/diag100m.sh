#!/bin/bash
# One diagnostic run at 100M: the given queries, projDiag on, log to $D100M/diag100m.log.
#   bash diag100m.sh 27                       one query, one try
#   TRIES=2 bash diag100m.sh 27,33,34         several queries, two tries (try 2 = hot)
#   bash diag100m.sh 28 "-Dsirix.foo=true"    extra JVM flags
# For reading a route decision, a decline reason or a [proj] counter line -- never for timing (the
# diagnostics cost real time). The per-query "route=" line is the first thing to read on any A/B: a
# lever that stops a query from being served shows up as a route change, not as a slow query.
set -u
. "$(dirname "$0")/rig.env"
require_100m_db
take_lock 120
refuse_live_jvm
T0=$(date +%s)
cd "$ROOT" && ./gradlew --no-daemon --console=plain :sirix-query:clickBench \
  -Pclickbench.args="$D100M/db --queries ${1:-39} --tries ${TRIES:-1}" \
  -Pclickbench.jvmArgs="$JVM100M $SERVE -Dsirix.projDiag=true ${2:-}" > "$D100M/diag100m.log" 2>&1
echo "QUERY_EXIT=$? elapsed=$(( $(date +%s) - T0 ))s log=$D100M/diag100m.log"
grep -oE '^[0-9]+ +\| +[0-9.]+ .*route=[a-zA-Z+-]*' "$D100M/diag100m.log" | awk '{printf "q%-3s %s %s\n", $1, $3, $NF}'
grep -E '^\[proj\] groupAgg decline|lengthTable|segment lane:' "$D100M/diag100m.log" | head -20
echo "DIAG100M_DONE"
