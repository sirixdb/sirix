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
# ./prepare-data.sh, set HITS_JSON=/path/to/hits.json, and provide either the
# first positional row count or EXPECTED_ROWS. The official corpus contains
# exactly 99,997,497 rows.
#
# Resource controls:
#   SIRIX_LOAD_JVM_ARGS   loader heap/direct/off-heap/HFT JVM arguments
#   SIRIX_QUERY_JVM_ARGS  JVM arguments shared by both query legs
#   DUCKDB_DB             :memory: or a file inside workdir
#   DUCKDB_MEMORY_LIMIT   e.g. 12GB
#   DUCKDB_TEMP_DIRECTORY spill directory inside workdir
#   DUCKDB_THREADS        positive integer
#   REQUIRE_AUTO_GLOBAL_DICT
#                           1 forces AUTO and requires globalDictColumns > 0

set -euo pipefail

ROWS="${1:-200000}"
WORKDIR="${2:-${TMPDIR:-/tmp}/clickbench-differential}"
SEED="${SEED:-42}"

if [ -n "${HITS_JSON:-}" ] && [ -z "${EXPECTED_ROWS:-}" ] && [ "$#" -lt 1 ]; then
    echo "HITS_JSON requires an explicit EXPECTED_ROWS (99997497 for the official corpus)" >&2
    exit 2
fi
EXPECTED_ROWS="${EXPECTED_ROWS:-${ROWS}}"
if ! [[ "${EXPECTED_ROWS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "EXPECTED_ROWS must be a positive decimal integer, got: ${EXPECTED_ROWS}" >&2
    exit 2
fi
SIRIX_LOAD_JVM_ARGS="${SIRIX_LOAD_JVM_ARGS:-}"
SIRIX_QUERY_JVM_ARGS="${SIRIX_QUERY_JVM_ARGS:-}"
REQUIRE_AUTO_GLOBAL_DICT="${REQUIRE_AUTO_GLOBAL_DICT:-0}"
case "${REQUIRE_AUTO_GLOBAL_DICT}" in
    0|1) ;;
    *)
        echo "REQUIRE_AUTO_GLOBAL_DICT must be 0 or 1, got: ${REQUIRE_AUTO_GLOBAL_DICT}" >&2
        exit 2
        ;;
esac
mandatory_load_jvm_args="-Dclickbench.expectedRows=${EXPECTED_ROWS} -Dclickbench.projection=true -Dclickbench.projection.incremental=true -DbuildPathSummary=true"
if [ "${REQUIRE_AUTO_GLOBAL_DICT}" -eq 1 ]; then
    # Appended after caller flags so a stale globalDict=never cannot turn the requested AUTO arm
    # into a per-leaf-dictionary build while leaving every result correct.
    mandatory_load_jvm_args="${mandatory_load_jvm_args} -Dsirix.projection.globalDict=auto"
fi
load_jvm_args="${SIRIX_LOAD_JVM_ARGS} ${mandatory_load_jvm_args}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../../.." && pwd)"

GRADLE="${GRADLE:-${repo}/gradlew}"
GRADLE_FLAGS="${GRADLE_FLAGS:---console=plain}"

DATA="${HITS_JSON:-${WORKDIR}/hits.json}"
DB="${WORKDIR}/sirix-db"
OUT_VEC="${WORKDIR}/results-sirix-vectorized"
OUT_GEN="${WORKDIR}/results-sirix-generic"
OUT_DUCK="${WORKDIR}/results-duckdb"
LOAD_LOG="${WORKDIR}/sirix-load.log"

mkdir -p "${WORKDIR}"
rm -rf "${DB}" "${OUT_VEC}" "${OUT_GEN}" "${OUT_DUCK}"

# DuckDB's official-corpus table is large enough that the correctness run must
# be able to use a file-backed database and a controlled spill directory. Keep
# every writable DuckDB path inside this run's dedicated work directory.
WORKDIR_ABS="$(realpath -m "${WORKDIR}")"
DUCKDB_DB="${DUCKDB_DB:-:memory:}"
DUCKDB_MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-}"
DUCKDB_TEMP_DIRECTORY="${DUCKDB_TEMP_DIRECTORY:-${WORKDIR_ABS}/duckdb-tmp}"
DUCKDB_THREADS="${DUCKDB_THREADS:-}"

require_workdir_path() {
    local label="$1"
    local value="$2"
    local resolved
    resolved="$(realpath -m "${value}")"
    case "${resolved}" in
        "${WORKDIR_ABS}"/*) ;;
        *)
            echo "${label} must be inside ${WORKDIR_ABS}, got: ${resolved}" >&2
            exit 2
            ;;
    esac
    printf '%s' "${resolved}"
}

reject_result_tree() {
    local label="$1"
    local value="$2"
    local reserved
    local resolved_reserved
    for reserved in "${DB}" "${OUT_VEC}" "${OUT_GEN}" "${OUT_DUCK}" "${DATA}" "${LOAD_LOG}"; do
        resolved_reserved="$(realpath -m "${reserved}")"
        case "${value}" in
            "${resolved_reserved}"|"${resolved_reserved}"/*)
                echo "${label} overlaps benchmark input/output path ${resolved_reserved}" >&2
                exit 2
                ;;
        esac
    done
}

if [ "${DUCKDB_DB}" != ":memory:" ]; then
    DUCKDB_DB="$(require_workdir_path DUCKDB_DB "${DUCKDB_DB}")"
    reject_result_tree DUCKDB_DB "${DUCKDB_DB}"
fi
DUCKDB_TEMP_DIRECTORY="$(require_workdir_path DUCKDB_TEMP_DIRECTORY "${DUCKDB_TEMP_DIRECTORY}")"
reject_result_tree DUCKDB_TEMP_DIRECTORY "${DUCKDB_TEMP_DIRECTORY}"
mkdir -p "${DUCKDB_TEMP_DIRECTORY}"
if [ -n "${DUCKDB_THREADS}" ] && ! [[ "${DUCKDB_THREADS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "DUCKDB_THREADS must be a positive decimal integer, got: ${DUCKDB_THREADS}" >&2
    exit 2
fi

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
if ! "${GRADLE}" ${GRADLE_FLAGS} -p "${repo}" :sirix-query:clickBenchLoad \
    -Pclickbench.args="${DB} ${DATA}" \
    -Pclickbench.jvmArgs="${load_jvm_args}" \
    2>&1 | tee "${LOAD_LOG}"; then
    echo "SirixDB load failed" >&2
    exit 1
fi
# Anchored on purpose: the loader's projection banner quotes the notice text
# ("an abandonment prints '[proj] PROJECTION ABANDONED' on stderr"), so a fixed-string
# search would refuse every successful load.
if grep -Eq '^\[proj\] PROJECTION ABANDONED' "${LOAD_LOG}"; then
    echo "SirixDB projection was abandoned; refusing to run row-path queries" >&2
    exit 1
fi
if [ "${REQUIRE_AUTO_GLOBAL_DICT}" -eq 1 ]; then
    global_dict_columns="$(awk '
        $1 == "#" && $2 == "projection:" {
            for (field = 1; field <= NF; field++) {
                if ($field ~ /^globalDictColumns=[0-9]+$/) {
                    split($field, parts, "=")
                    observed = parts[2]
                    observations++
                }
            }
        }
        END {
            if (observations == 1) print observed
            else if (observations > 1) print "AMBIGUOUS"
        }
    ' "${LOAD_LOG}")"
    if ! [[ "${global_dict_columns}" =~ ^[1-9][0-9]*$ ]]; then
        echo "AUTO global dictionary was required, but loader evidence reported " \
             "globalDictColumns=${global_dict_columns:-MISSING}" >&2
        exit 1
    fi
    echo "# AUTO global dictionary evidence: globalDictColumns=${global_dict_columns}"
fi

echo "== 2/4 SirixDB, default configuration =="
"${GRADLE}" ${GRADLE_FLAGS} -p "${repo}" :sirix-query:clickBench \
    -Pclickbench.args="${DB} --tries 1 --dump ${OUT_VEC} --require-vectorized-serving" \
    -Pclickbench.jvmArgs="${SIRIX_QUERY_JVM_ARGS} -Dsirix.query.autoVectorize=true"

echo "== 3/4 SirixDB, generic pipeline (autoVectorize=false) =="
"${GRADLE}" ${GRADLE_FLAGS} -p "${repo}" :sirix-query:clickBench \
    -Pclickbench.args="${DB} --tries 1 --dump ${OUT_GEN} --require-generic-serving" \
    -Pclickbench.jvmArgs="${SIRIX_QUERY_JVM_ARGS} -Dsirix.query.autoVectorize=false"

echo "== 4/4 DuckDB over the same JSON =="
duckdb_args=(
    --source "${DATA}"
    --format json
    --db "${DUCKDB_DB}"
    --temp-directory "${DUCKDB_TEMP_DIRECTORY}"
    --out "${OUT_DUCK}"
    --tries 1
    --candidate-reference "vectorized=${OUT_VEC}"
    --candidate-reference "generic=${OUT_GEN}"
)
if [ -n "${DUCKDB_MEMORY_LIMIT}" ]; then
    duckdb_args+=(--memory-limit "${DUCKDB_MEMORY_LIMIT}")
fi
if [ -n "${DUCKDB_THREADS}" ]; then
    duckdb_args+=(--threads "${DUCKDB_THREADS}")
fi
python3 "${here}/duckdb_reference.py" "${duckdb_args[@]}"

echo
echo "===================== fast path vs interpreter ====================="
python3 "${here}/compare-results.py" "${OUT_VEC}" "${OUT_GEN}" && vec_gen=0 || vec_gen=$?

echo
echo "===================== fast path vs DuckDB (strong) ================="
python3 "${here}/compare-results.py" --strong --bounded-oracle vectorized \
    "${OUT_VEC}" "${OUT_DUCK}" && vec_duck=0 || vec_duck=$?

echo
echo "===================== interpreter vs DuckDB (strong) ==============="
python3 "${here}/compare-results.py" --strong --bounded-oracle generic \
    "${OUT_GEN}" "${OUT_DUCK}" && gen_duck=0 || gen_duck=$?

echo
if [ "${vec_duck}" -eq 0 ] && [ "${gen_duck}" -eq 0 ]; then
    echo "DIFFERENTIAL PASSED (${ROWS} rows)"
    if [ "${vec_gen}" -ne 0 ]; then
        echo "note: supplemental fast-path-vs-interpreter comparison exited ${vec_gen}"
    fi
    exit 0
fi
echo "DIFFERENTIAL FAILED (supplemental-fast-vs-interpreter=${vec_gen}, " \
     "fast-vs-duckdb=${vec_duck}, interpreter-vs-duckdb=${gen_duck})"
exit 1
