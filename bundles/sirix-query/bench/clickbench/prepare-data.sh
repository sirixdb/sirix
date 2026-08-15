#!/usr/bin/env bash
#
# prepare-data.sh -- turn the official ClickBench dataset into the JSON encoding
# that the SirixDB ClickBench port ingests.
#
#   ./prepare-data.sh <hits.parquet> <output.json> [rows]
#
# Output contract (fixed; ClickBenchSchema.java and duckdb_reference.py must
# agree with it byte for byte):
#
#   * ONE JSON ARRAY of objects, one object per hit -- "[{...},\n{...}]".
#     SirixDB shreds a JSON array with gson's streaming JsonReader, so the
#     array wrapper (COPY ... FORMAT JSON, ARRAY true) is what this script
#     emits. The loader also accepts newline-delimited JSON -- the shape of the
#     official hits.json.gz -- and frames it into an array while streaming.
#   * ALL 105 columns are present on EVERY object, in duckdb/create.sql order.
#     The order is pinned by the CANONICAL_COLUMNS list below rather than taken
#     from the parquet file, so a wrong/reordered input file fails loudly.
#   * SMALLINT / INTEGER / BIGINT  -> JSON numbers, exact int64, never quoted,
#                                     never floats.
#   * TEXT / VARCHAR / CHAR        -> JSON strings; NULL is coalesced to "" so
#                                     the file can never contain a JSON null
#                                     (the ClickBench data uses "" as its
#                                     "missing" marker).
#   * EventDate (DATE)             -> "YYYY-MM-DD"
#   * EventTime / ClientEventTime / LocalEventTime (TIMESTAMP)
#                                  -> "YYYY-MM-DDTHH:MM:SS"  (ISO-8601, 'T'
#                                     separator, second resolution, no zone).
#     Rationale: JSON has no date type, and ISO-8601 strings order
#     lexicographically -- so ORDER BY EventTime and the EventDate range
#     predicates stay plain string comparisons in JSONiq, and
#     DATE_TRUNC('minute', t) is substring(t, 1, 16).
#
# ---------------------------------------------------------------------------
# int64 EXACTNESS -- verified, DuckDB 1.5.2 (Variegata) 8a5851971f:
#
#   $ duckdb -c "COPY (SELECT 435090932899640449::BIGINT AS UserID,
#                             9223372036854775807::BIGINT AS maxi,
#                             (-9223372036854775807-1)::BIGINT AS mini)
#                TO 'p.json' (FORMAT JSON, ARRAY true);"
#   $ cat p.json
#   [
#           {"UserID":435090932899640449,"maxi":9223372036854775807,"mini":-9223372036854775808}
#   ]
#
# DuckDB's JSON writer (yyjson) serialises BIGINT through an integer path, not
# through a double: all 18 digits of 435090932899640449 survive, and both
# int64 extremes round-trip. NO cast/quoting workaround is needed, and none is
# applied -- the integers are written as bare JSON numbers, which is exactly
# what the encoding contract demands. (Q19 filters on that literal and Q40/Q41
# on 3594120000172545465 / 2868770270353813622, so a double detour would have
# silently produced wrong answers rather than a load error.)
# ---------------------------------------------------------------------------
#
# Source-format tolerance. The official
# datasets.clickhouse.com/hits_compatible/athena/hits.parquet does NOT store
# the temporal columns as TIMESTAMP/DATE: EventTime, ClientEventTime and
# LocalEventTime are INTEGER unix-seconds and EventDate is INTEGER
# days-since-epoch (this is why ClickBench's own duckdb/load applies
# `epoch_ms(EventTime*1000)` and `make_date(EventDate)`). This script inspects
# the actual parquet column types and applies the right conversion, so it works
# on the official file, on a re-exported typed parquet, and on one where the
# columns are already ISO strings.
#
# Idempotence: the export writes to a temporary file and is atomically renamed
# into place, and a sidecar "<output>.meta" records (source, size, mtime, rows).
# Re-running with the same inputs is a no-op unless FORCE=1.
#
# Environment overrides:
#   DUCKDB=/path/to/duckdb   duckdb CLI to use            (default: duckdb)
#   FORCE=1                  re-export even if up to date (default: 0)
#   CHECK_NULLS=0            skip the source NULL audit   (default: 1)
#
set -euo pipefail

readonly HITS_URL="https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet"
readonly TS_FORMAT='%Y-%m-%dT%H:%M:%S'
readonly DATE_FORMAT='%Y-%m-%d'

# duckdb/create.sql order, "<name>:<kind>"; kind is one of int|str|ts|date.
readonly CANONICAL_COLUMNS="\
WatchID:int JavaEnable:int Title:str GoodEvent:int EventTime:ts EventDate:date
CounterID:int ClientIP:int RegionID:int UserID:int CounterClass:int OS:int
UserAgent:int URL:str Referer:str IsRefresh:int RefererCategoryID:int RefererRegionID:int
URLCategoryID:int URLRegionID:int ResolutionWidth:int ResolutionHeight:int ResolutionDepth:int FlashMajor:int
FlashMinor:int FlashMinor2:str NetMajor:int NetMinor:int UserAgentMajor:int UserAgentMinor:str
CookieEnable:int JavascriptEnable:int IsMobile:int MobilePhone:int MobilePhoneModel:str Params:str
IPNetworkID:int TraficSourceID:int SearchEngineID:int SearchPhrase:str AdvEngineID:int IsArtifical:int
WindowClientWidth:int WindowClientHeight:int ClientTimeZone:int ClientEventTime:ts SilverlightVersion1:int SilverlightVersion2:int
SilverlightVersion3:int SilverlightVersion4:int PageCharset:str CodeVersion:int IsLink:int IsDownload:int
IsNotBounce:int FUniqID:int OriginalURL:str HID:int IsOldCounter:int IsEvent:int
IsParameter:int DontCountHits:int WithHash:int HitColor:str LocalEventTime:ts Age:int
Sex:int Income:int Interests:int Robotness:int RemoteIP:int WindowName:int
OpenerName:int HistoryLength:int BrowserLanguage:str BrowserCountry:str SocialNetwork:str SocialAction:str
HTTPError:int SendTiming:int DNSTiming:int ConnectTiming:int ResponseStartTiming:int ResponseEndTiming:int
FetchTiming:int SocialSourceNetworkID:int SocialSourcePage:str ParamPrice:int ParamOrderID:str ParamCurrency:str
ParamCurrencyID:int OpenstatServiceName:str OpenstatCampaignID:str OpenstatAdID:str OpenstatSourceID:str UTMSource:str
UTMMedium:str UTMCampaign:str UTMContent:str UTMTerm:str FromTag:str HasGCLID:int
RefererHash:int URLHash:int CLID:int"

usage() {
  cat >&2 <<USAGE
usage: $0 <hits.parquet> <output.json> [rows]

  hits.parquet   the official ClickBench single-file dataset (or any parquet
                 with the same 105 columns)
  output.json    destination; a single JSON array of 105-key objects
  rows           optional row cap. The subset is the FIRST <rows> rows in
                 physical file order (parquet file_row_number), so it is
                 reproducible across runs and thread counts -- a bare LIMIT is
                 not, and a non-reproducible subset makes the SirixDB/DuckDB
                 differential meaningless.

env: DUCKDB=<path>  FORCE=1  CHECK_NULLS=0
USAGE
  exit 2
}

die() {
  printf '%s: %s\n' "$0" "$1" >&2
  exit "${2:-1}"
}

# SQL single-quote escaping for a string literal.
sql_lit() {
  printf '%s' "${1//\'/\'\'}"
}

main() {
  if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    usage
  fi
  case "${1:-}" in
    -h | --help) usage ;;
    *) : ;;
  esac

  local parquet=$1
  local out=$2
  local rows=${3:-}
  local duckdb=${DUCKDB:-duckdb}
  local force=${FORCE:-0}
  local check_nulls=${CHECK_NULLS:-1}

  if [ -n "$rows" ]; then
    case "$rows" in
      '' | *[!0-9]*) die "rows must be a positive integer, got: $rows" 2 ;;
      *) : ;;
    esac
    [ "$rows" -gt 0 ] || die "rows must be a positive integer, got: $rows" 2
  fi

  if [ ! -f "$parquet" ]; then
    cat >&2 <<MISSING
$0: input parquet not found: $parquet

Download the official ClickBench dataset first (~15 GB uncompressed, 100M rows):

  wget --continue --progress=dot:giga -O '$parquet' '$HITS_URL'

MISSING
    exit 1
  fi

  command -v "$duckdb" >/dev/null 2>&1 \
    || die "duckdb CLI not found (looked for '$duckdb'); set DUCKDB=/path/to/duckdb"

  local outdir
  outdir=$(dirname -- "$out")
  [ -d "$outdir" ] || die "output directory does not exist: $outdir"

  # ---- idempotence -------------------------------------------------------
  local meta="$out.meta"
  local src_size src_mtime stamp
  src_size=$(stat -c '%s' -- "$parquet")
  src_mtime=$(stat -c '%Y' -- "$parquet")
  stamp="source=$(readlink -f -- "$parquet") size=$src_size mtime=$src_mtime rows=${rows:-all}"

  if [ "$force" != "1" ] && [ -f "$out" ] && [ -f "$meta" ] && grep -qxF "$stamp" "$meta"; then
    printf 'up to date, nothing to do (FORCE=1 to re-export): %s\n' "$out"
    printf '  %s\n' "$stamp"
    return 0
  fi

  # ---- inspect the source's actual column types --------------------------
  local pq_lit
  pq_lit=$(sql_lit "$parquet")
  local read_src="read_parquet('$pq_lit', binary_as_string=true)"

  local -A pq_type=()
  local name type
  while IFS=$'\t' read -r name type _; do
    [ -n "$name" ] || continue
    pq_type["$name"]=$type
  done < <("$duckdb" -noheader -list -separator $'\t' \
    -c "DESCRIBE SELECT * FROM $read_src;")

  [ "${#pq_type[@]}" -gt 0 ] || die "could not DESCRIBE $parquet"

  # ---- build the projection, in canonical order --------------------------
  local -a canonical=() line_columns=()
  while read -r -a line_columns; do
    canonical+=("${line_columns[@]}")
  done <<<"$CANONICAL_COLUMNS"
  [ "${#canonical[@]}" -eq 105 ] \
    || die "internal error: CANONICAL_COLUMNS holds ${#canonical[@]} entries, expected 105"

  local select_list='' null_probe='' col kind src_type expr sep='' psep=''
  for col in "${canonical[@]}"; do
    kind=${col##*:}
    name=${col%%:*}
    src_type=${pq_type[$name]:-}
    [ -n "$src_type" ] || die "column '$name' is missing from $parquet -- not a ClickBench hits file?"

    case "$kind" in
      str)
        # never emit a JSON null for a text column
        expr="COALESCE(\"$name\", '') AS \"$name\""
        ;;
      int)
        expr="\"$name\""
        ;;
      ts)
        case "$src_type" in
          TIMESTAMP*) expr="strftime(\"$name\", '$TS_FORMAT') AS \"$name\"" ;;
          VARCHAR) expr="\"$name\"" ;;
          *) # official athena parquet: INTEGER unix seconds
            expr="strftime(epoch_ms(CAST(\"$name\" AS BIGINT) * 1000), '$TS_FORMAT') AS \"$name\"" ;;
        esac
        ;;
      date)
        case "$src_type" in
          DATE) expr="strftime(\"$name\", '$DATE_FORMAT') AS \"$name\"" ;;
          VARCHAR) expr="\"$name\"" ;;
          *) # official athena parquet: INTEGER days since epoch
            expr="strftime(make_date(CAST(\"$name\" AS INTEGER)), '$DATE_FORMAT') AS \"$name\"" ;;
        esac
        ;;
      *)
        die "internal error: unknown column kind '$kind' for '$name'"
        ;;
    esac
    select_list="$select_list$sep$expr"
    sep=$', \n  '

    if [ "$kind" != "str" ]; then
      # NOT NULL in create.sql; a NULL here would become a JSON null and break
      # the encoding contract, so audit the source rather than silently emit it.
      null_probe="$null_probe${psep}count(*) FILTER (WHERE \"$name\" IS NULL) AS \"$name\""
      psep=', '
    fi
  done

  # ---- the row source ----------------------------------------------------
  local from_clause
  if [ -n "$rows" ]; then
    from_clause="(SELECT * FROM read_parquet('$pq_lit', binary_as_string=true, file_row_number=true)
                  WHERE file_row_number < $rows ORDER BY file_row_number)"
  else
    from_clause="$read_src"
  fi

  # ---- audit NULLs in the NOT NULL columns -------------------------------
  if [ "$check_nulls" = "1" ]; then
    printf 'auditing NULLs in the 77 NOT NULL numeric/temporal columns (CHECK_NULLS=0 to skip)...\n'
    local offenders
    offenders=$("$duckdb" -noheader -list -c "
      WITH src AS (SELECT * FROM $from_clause),
           n AS (SELECT $null_probe FROM src)
      SELECT COALESCE(string_agg(col || '=' || nulls, ', '), '')
      FROM (UNPIVOT n ON COLUMNS(*) INTO NAME col VALUE nulls)
      WHERE nulls > 0;")
    if [ -n "$offenders" ]; then
      die "source has NULLs in NOT NULL columns, refusing to emit JSON nulls: $offenders"
    fi
  fi

  # ---- export ------------------------------------------------------------
  local tmp="$out.tmp.$$"
  # shellcheck disable=SC2064  # $tmp is intentionally expanded now, not on exit
  trap "rm -f -- '$tmp'" EXIT

  printf 'exporting %s -> %s (rows=%s)\n' "$parquet" "$out" "${rows:-all}"
  "$duckdb" -c "COPY (SELECT
  $select_list
FROM $from_clause) TO '$(sql_lit "$tmp")' (FORMAT JSON, ARRAY true);"

  mv -f -- "$tmp" "$out"
  trap - EXIT

  printf '%s\n' "$stamp" >"$meta"

  # ---- report ------------------------------------------------------------
  local total written out_size
  total=$("$duckdb" -noheader -list -c "SELECT count(*) FROM $read_src;")
  if [ -n "$rows" ] && [ "$rows" -lt "$total" ]; then
    written=$rows
  else
    written=$total
  fi
  out_size=$(stat -c '%s' -- "$out")
  local human=''
  if command -v numfmt >/dev/null 2>&1; then
    human=" ($(numfmt --to=iec-i --suffix=B "$out_size"))"
  fi

  printf 'done: %s rows, %s bytes%s\n' "$written" "$out_size" "$human"
  printf '  source rows : %s\n' "$total"
  printf '  output      : %s\n' "$out"
  printf '  meta        : %s\n' "$meta"
}

main "$@"
