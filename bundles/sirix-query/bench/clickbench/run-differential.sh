#!/usr/bin/env bash
#
# The correctness gate for the ClickBench port: run the same 43 queries over the
# same records three ways and diff the answers.
#
#   leg 1  SirixDB, default configuration (analytical fast paths on)
#   leg 2  SirixDB, -Dsirix.query.autoVectorize=false (generic interpreter)
#   leg 3  DuckDB over byte-identical input data
#
# Leg 1 vs leg 2 catches a fast path that claims a query shape it cannot serve
# correctly; leg 1 vs leg 3 catches a mistranslation of the SQL. Both matter:
# the port has already found one of each (see docs/CLICKBENCH.md).
#
# Usage:
#   ./run-differential.sh [rows] [workdir]
#
#   rows     number of synthetic hits records to generate (default 200000)
#   workdir  scratch directory for the data, the database and the dumps
#            (default ${TMPDIR:-/tmp}/clickbench-differential)
#
# With a real dataset instead of the generator, prepare hits.json with
# ./prepare-data.sh and set HITS_JSON=/path/to/hits.json.

set -euo pipefail

ROWS="${1:-200000}"
WORKDIR="${2:-${TMPDIR:-/tmp}/clickbench-differential}"
SEED="${SEED:-42}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../../.." && pwd)"

GRADLE="${GRADLE:-${repo}/gradlew}"
GRADLE_FLAGS="${GRADLE_FLAGS:---console=plain}"

DATA="${HITS_JSON:-${WORKDIR}/hits.json}"
DB="${WORKDIR}/sirix-db"
OUT_VEC="${WORKDIR}/results-sirix-vectorized"
OUT_GEN="${WORKDIR}/results-sirix-generic"
OUT_DUCK="${WORKDIR}/results-duckdb"

mkdir -p "${WORKDIR}"
rm -rf "${DB}" "${OUT_VEC}" "${OUT_GEN}" "${OUT_DUCK}"

echo "== 0/4 generating ${ROWS} hits records =="
if [ -n "${HITS_JSON:-}" ]; then
    echo "   using ${HITS_JSON} (HITS_JSON is set)"
else
    # Both engines must read byte-identical records, so the generator writes a file
    # rather than streaming straight into the loader.
    "${GRADLE}" ${GRADLE_FLAGS} -p "${repo}" :sirix-query:clickBenchGenerate \
        -Pclickbench.args="${DATA} ${ROWS} ${SEED}"
fi

echo "== 1/4 loading into SirixDB =="
"${GRADLE}" ${GRADLE_FLAGS} -p "${repo}" :sirix-query:clickBenchLoad \
    -Pclickbench.args="${DB} ${DATA}"

echo "== 2/4 SirixDB, default configuration =="
"${GRADLE}" ${GRADLE_FLAGS} -p "${repo}" :sirix-query:clickBench \
    -Pclickbench.args="${DB} --tries 1 --dump ${OUT_VEC}"

echo "== 3/4 SirixDB, generic pipeline (autoVectorize=false) =="
"${GRADLE}" ${GRADLE_FLAGS} -p "${repo}" :sirix-query:clickBench \
    -Pclickbench.args="${DB} --tries 1 --dump ${OUT_GEN}" \
    -Pclickbench.jvmArgs="-Dsirix.query.autoVectorize=false"

echo "== 4/4 DuckDB over the same JSON =="
python3 "${here}/duckdb_reference.py" --source "${DATA}" --format json \
    --out "${OUT_DUCK}" --tries 1

echo
echo "===================== fast path vs interpreter ====================="
python3 "${here}/compare-results.py" "${OUT_VEC}" "${OUT_GEN}" && vec_gen=0 || vec_gen=$?

echo
echo "===================== SirixDB vs DuckDB ============================"
python3 "${here}/compare-results.py" "${OUT_VEC}" "${OUT_DUCK}" && vec_duck=0 || vec_duck=$?

echo
if [ "${vec_gen}" -eq 0 ] && [ "${vec_duck}" -eq 0 ]; then
    echo "DIFFERENTIAL PASSED (${ROWS} rows)"
    exit 0
fi
echo "DIFFERENTIAL FAILED (fast-path-vs-interpreter=${vec_gen}, sirix-vs-duckdb=${vec_duck})"
exit 1
