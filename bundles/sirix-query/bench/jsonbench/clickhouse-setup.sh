#!/usr/bin/env bash
#
# clickhouse-setup.sh <tier> <cleaned-corpus> <clickhouse-binary> [workdir]
#
# Build the ClickHouse reference side of JSONBench: the table, the reference
# answers, and the cold/hot baseline timings SirixDB is compared against.
#
#   tier              1m | 10m | 100m | 1000m   (labels the output directories)
#   cleaned-corpus    the NDJSON produced by clean-corpus.py -- NOT the raw .gz
#   clickhouse-binary path to a `clickhouse` executable (see README: 26.7.3.19)
#   workdir           where ch-data-<tier>/ and ch-ref-<tier>/ go
#                     (default: the corpus's own directory)
#
# Produces
#   <workdir>/ch-data-<tier>/            the MergeTree table
#   <workdir>/ch-ref-<tier>/qN.tsv       reference answers, generated in UTC
#   <workdir>/ch-ref-<tier>/baseline.txt per-query cold/hot seconds + the sums
#
# Three things here are load-bearing; each was learned by measuring ClickHouse
# rather than assuming what it does.
#
# 1. THE CORPUS MUST BE THE CLEANED ONE. The official JSONBench loader retries a
#    failed file with `input_format_allow_errors_num = 1e9`, i.e. it silently
#    drops rows it cannot parse. That is invisible in a single-engine benchmark
#    and fatal in a differential: the two engines would hold different row sets
#    and every count would differ by an unknown amount. clean-corpus.py drops
#    the same rows, once, explicitly, for both engines.
#
# 2. THE REFERENCES ARE GENERATED IN UTC. `toHour(fromUnixTimestamp64Micro(...))`
#    and DateTime64 *formatting* both read the session timezone. References
#    produced on a Europe/Berlin box answer hour 17 where UTC answers 16, and
#    print `2024-11-21 17:25:49.000167` where UTC prints `16:25:49.000167`.
#    SirixDB's hour key is pure integer arithmetic ((time_us idiv 3600000000)
#    mod 24), which is timezone-free and equals the UTC hour, and it emits raw
#    microseconds -- so the comparator's parse back to microseconds is only well
#    defined against a UTC reference. Every reference query here carries
#    `SETTINGS session_timezone='UTC'`.
#
# 3. THE BASELINE USES THE SAME COLD PROTOCOL AS SIRIXDB. Cold = page cache
#    evicted with common/evict.py over the ClickHouse data directory, one timed
#    run. Hot = best of the next three. Both arms are cool-gated. Anything else
#    compares an evicted engine against a warm one.
#
# If the load fails on fat rows or multi-member gzip input, retry with
# `CH_LOAD_SETTINGS="SETTINGS input_format_parallel_parsing=0"` -- the parallel
# parser splits on newlines and can choke where the serial one does not.
set -u

. "$(cd "$(dirname "${BASH_SOURCE[0]}")/../common" && pwd)/bench-common.sh"

usage() {
  cat >&2 <<'USAGE'
usage: clickhouse-setup.sh <tier> <cleaned-corpus.ndjson> <clickhouse-binary> [workdir]

env: CH_LOAD_SETTINGS="SETTINGS input_format_parallel_parsing=0"   for fat-row loads
     HOT_TRIES=3        hot runs after the cold one (best-of)
     SKIP_BASELINE=1    build table + references, do not time ClickHouse
     FORCE=1            rebuild the table even if ch-data-<tier> exists
USAGE
  exit 2
}

[ "$#" -ge 3 ] && [ "$#" -le 4 ] || usage

TIER="$1"
CORPUS="$2"
CH="$3"
WORKDIR="${4:-$(cd "$(dirname "$2")" && pwd)}"

case "${TIER}" in
  1m|10m|100m|1000m) : ;;
  *) die "unknown tier '${TIER}' (expected 1m, 10m, 100m or 1000m)" ;;
esac

need_file "${CORPUS}" "cleaned corpus (run clean-corpus.py first)"
[ -x "${CH}" ] || die "clickhouse binary is not executable: ${CH}"
need_cmd python3

QUERIES_SQL="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/queries.sql"
need_file "${QUERIES_SQL}" "the 5 JSONBench queries"

DATA_DIR="${WORKDIR}/ch-data-${TIER}"
REF_DIR="${WORKDIR}/ch-ref-${TIER}"
BASELINE="${REF_DIR}/baseline.txt"

mkdir -p "${WORKDIR}" || die "cannot create workdir: ${WORKDIR}"

# The JSON type spec is used twice -- in the column declaration and in the CAST
# on insert -- and the two must agree exactly, so it lives in one variable.
readonly DDL_CAST="JSON(max_dynamic_paths=0, kind LowCardinality(String), commit.operation LowCardinality(String), commit.collection LowCardinality(String), did String, time_us UInt64)"

ch_query() {
  "${CH}" local --path "${DATA_DIR}" --query "$1"
}

# ---------------------------------------------------------------------------
# 1. table
# ---------------------------------------------------------------------------

if [ -d "${DATA_DIR}" ] && [ "${FORCE:-0}" != "1" ]; then
  log "table already present at ${DATA_DIR} (FORCE=1 to rebuild)"
else
  rm -rf "${DATA_DIR}" || die "cannot remove old table directory ${DATA_DIR}"
  mkdir -p "${DATA_DIR}" || die "cannot create ${DATA_DIR}"

  log "creating table 'bluesky' (JSON type, v3 serialization, sorted for the 5 queries)"
  ch_query "
CREATE TABLE bluesky ( \`data\` ${DDL_CAST} CODEC(ZSTD(1)) )
ENGINE = MergeTree
ORDER BY (data.kind, data.commit.operation, data.commit.collection, data.did,
          fromUnixTimestamp64Micro(data.time_us))
SETTINGS object_serialization_version='v3',
         dynamic_serialization_version='v3',
         object_shared_data_serialization_version='advanced';" \
    || die "CREATE TABLE failed -- does this clickhouse build support the JSON type? (26.7+)"

  log "loading ${CORPUS} (this is the long step: ~11 s at 1m, ~26 min at 100m)"
  load_start=$(date +%s)
  ch_query "INSERT INTO bluesky SELECT CAST(json AS ${DDL_CAST})
            FROM file('${CORPUS}', 'JSONAsObject', 'json JSON') ${CH_LOAD_SETTINGS:-}" \
    || die "INSERT failed. If the message mentions parsing, retry with
       CH_LOAD_SETTINGS=\"SETTINGS input_format_parallel_parsing=0\""
  log "load took $(( $(date +%s) - load_start ))s"
fi

ROWS="$(ch_query "SELECT count() FROM bluesky")" || die "count() failed on the loaded table"
log "rows: ${ROWS}    on disk: $(du -sh "${DATA_DIR}" | cut -f1)"
[ "${ROWS}" -gt 0 ] || die "the table is empty -- the load silently did nothing"

# ---------------------------------------------------------------------------
# 2. reference answers, in UTC
# ---------------------------------------------------------------------------

mkdir -p "${REF_DIR}" || die "cannot create ${REF_DIR}"
mapfile -t QUERIES < "${QUERIES_SQL}"
[ "${#QUERIES[@]}" -eq 5 ] || die "expected 5 queries in ${QUERIES_SQL}, found ${#QUERIES[@]}"

log "generating reference answers under session_timezone='UTC'"
for i in 0 1 2 3 4; do
  q="${QUERIES[$i]}"
  # `SETTINGS` has to follow the statement, so the trailing ';' is stripped first.
  "${CH}" local --path "${DATA_DIR}" \
      --query "${q%;} SETTINGS session_timezone='UTC'" > "${REF_DIR}/q$((i + 1)).tsv" \
    || die "reference generation failed for Q$((i + 1))"
  log "  Q$((i + 1)): $(wc -l < "${REF_DIR}/q$((i + 1)).tsv") row(s)"
done

# ---------------------------------------------------------------------------
# 3. the baseline SirixDB is measured against
# ---------------------------------------------------------------------------

if [ "${SKIP_BASELINE:-0}" = "1" ]; then
  log "SKIP_BASELINE=1 -- table and references are ready, no timings taken"
  exit 0
fi

HOT_TRIES="${HOT_TRIES:-3}"
log "timing ClickHouse: cold (evicted) + best of ${HOT_TRIES} hot, cool-gated"

: > "${BASELINE}.part" || die "cannot write ${BASELINE}.part"
cold_sum=0
hot_sum=0
for i in 0 1 2 3 4; do
  q="${QUERIES[$i]}"
  n=$((i + 1))

  cool_gate
  evict_paths "${DATA_DIR}"
  cold="$("${CH}" local --path "${DATA_DIR}" --time --query "${q}" 2>&1 >/dev/null | tail -1)"
  case "${cold}" in
    ''|*[!0-9.]*) die "could not read a cold timing for Q${n} from clickhouse --time (got: '${cold}')" ;;
  esac

  hot=""
  for _try in $(seq 1 "${HOT_TRIES}"); do
    t="$("${CH}" local --path "${DATA_DIR}" --time --query "${q}" 2>&1 >/dev/null | tail -1)"
    case "${t}" in
      ''|*[!0-9.]*) die "could not read a hot timing for Q${n} (got: '${t}')" ;;
    esac
    hot="$(python3 -c "import sys; print(min(float(sys.argv[1]), float(sys.argv[2])) if sys.argv[1] else float(sys.argv[2]))" "${hot}" "${t}")"
  done

  printf 'Q%d %s %s\n' "${n}" "${cold}" "${hot}" >> "${BASELINE}.part"
  log "  Q${n}: cold ${cold}s   hot ${hot}s"
  cold_sum="$(python3 -c "import sys; print(f'{float(sys.argv[1]) + float(sys.argv[2]):.3f}')" "${cold_sum}" "${cold}")"
  hot_sum="$(python3 -c "import sys; print(f'{float(sys.argv[1]) + float(sys.argv[2]):.3f}')" "${hot_sum}" "${hot}")"
done

printf 'SUM %s %s\n' "${cold_sum}" "${hot_sum}" >> "${BASELINE}.part"
mv "${BASELINE}.part" "${BASELINE}" || die "cannot install ${BASELINE}"

log "ClickHouse baseline: cold Σ ${cold_sum}s   hot Σ ${hot_sum}s   -> ${BASELINE}"
log "next: run-benchmark.sh ${TIER} <sirix-db-dir> ${REF_DIR}"
