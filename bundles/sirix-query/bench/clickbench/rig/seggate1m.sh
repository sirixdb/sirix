#!/bin/bash
# Correctness gate at 1M against the existing $D1M database (load1m.sh first).
#
# Two questions in one run:
#   1. How many segments did the load produce? -Dsirix.projDiag=true prints it. With ONE segment every
#      packed cell's high bits are zero and a width bug in the resolver is invisible, so the count
#      decides what this gate can witness.
#   2. Are the answers RIGHT? A served query that got 100x faster and a wrong top-K look the same in
#      a timing table; DuckDB over the same corpus is the oracle. Requires python3 with duckdb.
# The gate must end with 0 mismatch and 0 missing; "tie-ambiguous" rows are ORDER BY ties and fine.
set -u
. "$(dirname "$0")/rig.env"
[ -d "$D1M/db" ] || { echo "ABORT: no 1M database at $D1M/db (run load1m.sh)"; exit 1; }
take_lock 60
refuse_live_jvm
T1=$(date +%s)
rm -rf "$D1M/results-vec"
cd "$ROOT" && ./gradlew --no-daemon --console=plain :sirix-query:clickBench \
  -Pclickbench.args="$D1M/db --tries 1 --dump $D1M/results-vec" \
  -Pclickbench.jvmArgs="$JVM1M $SERVE -Dsirix.projDiag=true ${EXTRA:-}" > "$D1M/gate-query.log" 2>&1; QE=$?
echo "QUERY_EXIT=$QE elapsed=$(( $(date +%s) - T1 ))s"
echo "--- compile (must say it recompiled after a source change, not UP-TO-DATE) ---"
grep -m1 -E 'compileJava' "$D1M/gate-query.log" || true
echo "--- SEGMENT COUNT (says what this run can witness) ---"
grep -m3 'segment lane:' "$D1M/gate-query.log" || echo "  (no segment-lane line: diagnostics did not print)"
echo "--- declines: $(grep -c 'route=NONE' "$D1M/gate-query.log") ---"
[ $QE -ne 0 ] && { echo "SEGGATE: QUERY LEG FAILED"; tail -25 "$D1M/gate-query.log"; exit 1; }
T2=$(date +%s)
python3 "$BENCH/duckdb_reference.py" --source "$SRC1M" --format json --db "$D1M/hits1m.duckdb" \
  --temp-directory "$D1M/duckdb-tmp" --out "$D1M/results-duckdb" --tries 1 \
  --candidate-reference vectorized="$D1M/results-vec" --memory-limit 8GB --threads 4 > "$D1M/duckdb.log" 2>&1; DE=$?
echo "DUCKDB_EXIT=$DE elapsed=$(( $(date +%s) - T2 ))s"; tail -3 "$D1M/duckdb.log"
rm -f "$D1M/hits1m.duckdb"; rm -rf "$D1M/duckdb-tmp"
python3 "$BENCH/compare-results.py" --strong --bounded-oracle vectorized "$D1M/results-vec" "$D1M/results-duckdb" \
  > "$D1M/compare.log" 2>&1; CE=$?
echo "COMPARE_EXIT=$CE"
grep -E '^summary|mismatch|MISSING|missing' "$D1M/compare.log" | tail -6
echo "SEGGATE_DONE"
