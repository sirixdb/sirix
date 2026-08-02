#!/usr/bin/env bash
#
# End-to-end driver for docs/COMPARISON_POSTGRES_BULK.md.
#
# Every number published in that document comes from this script. It exists because a benchmark
# nobody can re-run is not evidence: an earlier revision was measured with throwaway drivers in a
# scratch directory, and when the machine was recycled the numbers could not be reproduced or
# even checked. Corpus preparation, both engines' loads, and every timed query now share one code
# path with the published results.
#
# Usage:
#   docs/bench/run-postgres-bulk.sh <corpus.json> <work-dir> [phase ...]
#
#   corpus.json   top-level JSON array of objects. The published run uses movies.json repeated
#                 12x to 2,116,427,425 B / 3,482,208 records (see caveat 3 in the document).
#   work-dir      scratch space for the NDJSON and the SirixDB stores. Needs ~6 GB free.
#   phase         any of: prep pgload sizes pgquery sirixingest sirixquery sirixcold proj
#                 Default: all of them, in that order.
#
# Requirements: JDK 25, a local PostgreSQL 16 the invoking user can restart via pg_ctl, and root
# for drop_caches (the cold phase is skipped with a warning otherwise).
#
# The engines never run concurrently: PostgreSQL is stopped for every SirixDB phase and started
# again afterwards, so neither holds page cache against the other.

set -euo pipefail

CORPUS="${1:?usage: run-postgres-bulk.sh <corpus.json> <work-dir> [phase ...]}"
WORK="${2:?usage: run-postgres-bulk.sh <corpus.json> <work-dir> [phase ...]}"
shift 2
PHASES=("$@")
if [ ${#PHASES[@]} -eq 0 ]; then
  PHASES=(prep pgload sizes pgquery pgindex sirixingest sirixquery sirixcold proj)
fi

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NDJSON="$WORK/corpus.ndjson"
DB="bulkbench"
STORE="$WORK/db-bulk"
ITERS="${ITERS:-12}"

# PostgreSQL layout differs by distro; override if pg_ctl/config live elsewhere.
PGBIN="${PGBIN:-/usr/lib/postgresql/16/bin}"
PGDATA="${PGDATA:-/var/lib/postgresql/16/main}"
PGCONF="${PGCONF:-/etc/postgresql/16/main/postgresql.conf}"
PGUSER_OS="${PGUSER_OS:-postgres}"

# Regime A vs B: matched cache budgets on both sides. -Xmx is NOT the SirixDB figure -- its page
# cache is off-heap, so the caps below are what actually decide residency (see section 1 of the
# document).
SIRIX_A="-Dsirix.cache.recordPage=1073741824 -Dsirix.cache.recordPageFragment=402653184 -Dsirix.cache.page=134217728"
SIRIX_B=""   # defaults: 8 GB record / 3 GB fragment / 1 GB metadata
PG_A="1GB"
PG_B="8GB"

JVM_ARGS="--enable-preview --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED -Xmx8g"

say() { printf '\n=== %s ===\n' "$*"; }
has_phase() { local p; for p in "${PHASES[@]}"; do [ "$p" = "$1" ] && return 0; done; return 1; }

pg() { su "$PGUSER_OS" -c "psql -q -t -A -d $DB"; }
# Idempotent on purpose: the script has to be re-runnable phase by phase, and a half-finished
# run leaves the server up. pg_ctl start against a running server fails the whole script under
# `set -e`, which is a miserable way to lose a 40-minute measurement.
pg_start() {
  if su "$PGUSER_OS" -c "$PGBIN/pg_ctl -D $PGDATA status" >/dev/null 2>&1; then
    return 0
  fi
  su "$PGUSER_OS" -c "$PGBIN/pg_ctl -D $PGDATA -l /tmp/pg-bulk.log -o '-c config_file=$PGCONF' start" >/dev/null
  sleep 4
}
pg_stop()  { su "$PGUSER_OS" -c "$PGBIN/pg_ctl -D $PGDATA -m fast stop" >/dev/null 2>&1 || true; }

pg_set_buffers() {
  local size="$1" conf
  conf="$(dirname "$PGCONF")/conf.d/bulkbench.conf"
  mkdir -p "$(dirname "$conf")"
  cat > "$conf" <<EOF
shared_buffers = $size
work_mem = 64MB
fsync = on
synchronous_commit = on
max_parallel_workers_per_gather = 2
EOF
  chown "$PGUSER_OS:$PGUSER_OS" "$conf"
  pg_stop; pg_start
  printf 'shared_buffers now: '; su "$PGUSER_OS" -c "psql -tAc 'show shared_buffers'"
}

drop_caches() {
  sync
  if [ -w /proc/sys/vm/drop_caches ]; then
    echo 3 > /proc/sys/vm/drop_caches
  else
    echo "WARNING: cannot write /proc/sys/vm/drop_caches -- cold numbers will be meaningless" >&2
  fi
}

mkdir -p "$WORK"
CP="$("$REPO/gradlew" -q -p "$REPO" :sirix-benchmarks:bulkBenchClasspath --console=plain 2>/dev/null | tail -1)"
[ -n "$CP" ] || { echo "could not resolve the benchmark classpath" >&2; exit 1; }

# ---------------------------------------------------------------- corpus

if has_phase prep; then
  say "prep: $CORPUS -> $NDJSON"
  java $JVM_ARGS -cp "$CP" io.sirix.benchmark.PostgresBulkBench ndjson "$CORPUS" "$NDJSON" 2>/dev/null | grep '^#'
fi

# ---------------------------------------------------------------- PostgreSQL load

if has_phase pgload; then
  say "pgload"
  pg_start
  su "$PGUSER_OS" -c "psql -qc 'DROP DATABASE IF EXISTS $DB'" >/dev/null 2>&1 || true
  su "$PGUSER_OS" -c "psql -qc 'CREATE DATABASE $DB'"
  su "$PGUSER_OS" -c "psql -q -d $DB" < "$REPO/docs/bench/postgres-bulk-schema.sql" >/dev/null

  # \copy, not COPY: the server cannot traverse a scratch directory owned by the invoking user,
  # and streaming over the client connection needs no chown of the corpus.
  echo "-- jsonb arm"
  t0=$SECONDS
  su "$PGUSER_OS" -c \
    "psql -q -d $DB -c \"\\copy movies_jsonb (doc) from stdin with (format csv, quote E'\\x01', delimiter E'\\x02')\"" \
    < "$NDJSON"
  printf '   %s s\n' "$((SECONDS - t0))"

  echo "-- normalized arm, projected from the very rows the jsonb arm holds"
  t0=$SECONDS
  su "$PGUSER_OS" -c "psql -q -d $DB -c \"
    INSERT INTO movies_rel
    SELECT doc->>'title', (doc->>'year')::int,
           CASE WHEN doc->'cast'   IS NULL THEN NULL ELSE ARRAY(SELECT jsonb_array_elements_text(doc->'cast'))   END,
           CASE WHEN doc->'genres' IS NULL THEN NULL ELSE ARRAY(SELECT jsonb_array_elements_text(doc->'genres')) END,
           doc->>'href', doc->>'extract', doc->>'thumbnail',
           (doc->>'thumbnail_width')::int, (doc->>'thumbnail_height')::int
    FROM movies_jsonb;\""
  printf '   %s s\n' "$((SECONDS - t0))"

  # Separate -c options, not one semicolon-separated string: psql wraps the latter in a single
  # transaction and VACUUM cannot run inside one.
  su "$PGUSER_OS" -c "psql -q -d $DB -c 'VACUUM (ANALYZE) movies_jsonb' -c 'VACUUM (ANALYZE) movies_rel' -c 'CHECKPOINT'"
  echo -n "rows (jsonb|rel): "; echo "select (select count(*) from movies_jsonb), (select count(*) from movies_rel)" | pg
fi

if has_phase sizes; then
  say "sizes (bytes)"
  printf 'raw corpus          %s\n' "$(stat -c%s "$CORPUS")"
  echo "select 'movies_jsonb        '||pg_total_relation_size('movies_jsonb') union all
        select 'movies_rel          '||pg_total_relation_size('movies_rel')" | pg
  [ -d "$STORE" ] && printf 'sirix store         %s\n' "$(du -sb "$STORE" | cut -f1)"
fi

# ---------------------------------------------------------------- PostgreSQL queries

pg_queries() {
  local arm="$1" sql
  if [ "$arm" = jsonb ]; then
    sql=("SELECT count(*) FROM movies_jsonb;"
         "SELECT count(*) FROM movies_jsonb WHERE (doc->>'year')::int > 1990;"
         "SELECT sum((doc->>'year')::bigint) FROM movies_jsonb;"
         "SELECT count(*) FROM movies_jsonb WHERE doc->>'title' = 'Saleslady';")
  else
    sql=("SELECT count(*) FROM movies_rel;"
         "SELECT count(*) FROM movies_rel WHERE year > 1990;"
         "SELECT sum(year::bigint) FROM movies_rel;"
         "SELECT count(*) FROM movies_rel WHERE title = 'Saleslady';")
  fi
  # One established session for all of it: spawning a psql per iteration adds ~30 ms of process
  # startup, which is invisible against a 400 ms scan and dominates a sub-millisecond index probe.
  { echo '\timing on'
    for q in "${sql[@]}"; do for _ in $(seq 1 "$ITERS"); do echo "$q"; done; done
  } | pg | awk -v iters="$ITERS" -v arm="$arm" '
      /^Time:/ { t[n++] = $2; next }
      { res[nr++] = $0 }
      END {
        split("countAll filterCountYear sumYear titleLookup", name, " ")
        for (q = 0; q < 4; q++) {
          m = 0
          for (i = q * iters; i < (q + 1) * iters; i++) v[m++] = t[i] + 0
          for (a = 1; a < m; a++) { x = v[a]; for (b = a - 1; b >= 0 && v[b] > x; b--) v[b + 1] = v[b]; v[b + 1] = x }
          printf "%-10s %-16s min=%9.1f ms  median=%9.1f ms  result=%s\n",
                 arm, name[q + 1], v[0], v[int(m / 2)], res[q * iters]
        }
      }'
}

if has_phase pgquery; then
  for regime in A B; do
    say "pgquery regime $regime"
    [ "$regime" = A ] && pg_set_buffers "$PG_A" || pg_set_buffers "$PG_B"
    pg_queries jsonb
    pg_queries rel
  done

  say "pgquery cold (restart + drop_caches per query)"
  for q in "SELECT count(*) FROM movies_jsonb;" \
           "SELECT count(*) FROM movies_jsonb WHERE (doc->>'year')::int > 1990;" \
           "SELECT sum((doc->>'year')::bigint) FROM movies_jsonb;" \
           "SELECT count(*) FROM movies_jsonb WHERE doc->>'title' = 'Saleslady';"; do
    pg_stop; drop_caches; pg_start
    su "$PGUSER_OS" -c "psql -q -t -A -d $DB -c '\timing on' -c \"$q\"" | grep '^Time:'
  done
fi

# ---------------------------------------------------------------- SirixDB

if has_phase pgindex; then
  say "pgindex: B-tree indexes on the same two fields the projection covers"
  pg_start
  for idx in "CREATE INDEX IF NOT EXISTS mr_year  ON movies_rel(year)" \
             "CREATE INDEX IF NOT EXISTS mr_title ON movies_rel(title)" \
             "CREATE INDEX IF NOT EXISTS mj_year  ON movies_jsonb(((doc->>'year')::int))" \
             "CREATE INDEX IF NOT EXISTS mj_title ON movies_jsonb((doc->>'title'))"; do
    t0=$SECONDS
    su "$PGUSER_OS" -c "psql -q -d $DB -c \"$idx\""
    printf '   %-58s %s s\n' "${idx%% ON*}" "$((SECONDS - t0))"
  done
  su "$PGUSER_OS" -c "psql -q -d $DB -c 'ANALYZE movies_rel' -c 'ANALYZE movies_jsonb'"
  echo "-- index sizes (bytes)"
  echo "select 'mr_year '||pg_relation_size('mr_year') union all select 'mr_title '||pg_relation_size('mr_title')
        union all select 'mj_year '||pg_relation_size('mj_year') union all select 'mj_title '||pg_relation_size('mj_title')" | pg
  pg_queries jsonb
  pg_queries rel
fi

if has_phase sirixingest; then
  say "sirixingest (PostgreSQL stopped)"
  pg_stop
  rm -rf "$STORE"
  java $JVM_ARGS -cp "$CP" io.sirix.benchmark.PostgresBulkBench \
       ingest "$CORPUS" "$STORE" single true 100000 3 2>/dev/null | grep -E 'round|RESULT'
fi

if has_phase sirixquery; then
  pg_stop
  for regime in A B; do
    say "sirixquery regime $regime"
    # shellcheck disable=SC2086
    java $JVM_ARGS $([ "$regime" = A ] && echo $SIRIX_A || echo $SIRIX_B) -cp "$CP" \
         io.sirix.benchmark.PostgresBulkBench query "$WORK" "$(basename "$STORE")" "$ITERS" 2>/dev/null \
      | grep -E '^query|^countAll|^filterCountYear|^sumYear|^titleLookup'
  done
fi

if has_phase sirixcold; then
  say "sirixcold (fresh JVM + drop_caches per query)"
  pg_stop
  for q in countAll filterCountYear sumYear titleLookup; do
    drop_caches
    java $JVM_ARGS -cp "$CP" io.sirix.benchmark.PostgresBulkBench \
         query "$WORK" "$(basename "$STORE")" 0 "$q" 2>/dev/null | grep -E "^$q"
  done
fi

if has_phase proj; then
  say "proj: columnar projection over (year, title)"
  pg_stop
  # Needs its OWN store, ingested untuned. The tuned configuration the ingest phase measures sets
  # buildPathSummary(false) -- it is one of the features PostgreSQL's plain table has no
  # equivalent for -- and the projection installer reads the path summary to discover the fields
  # to project. Against a tuned store it dies with "Node couldn't be fetched from persistent
  # storage!", which is a confusing way to say "there is no path summary here".
  PROJ_STORE="$WORK/db-proj"
  if [ ! -d "$PROJ_STORE" ]; then
    echo "-- building an untuned store (path summary on) for the projection"
    java $JVM_ARGS -cp "$CP" io.sirix.benchmark.PostgresBulkBench \
         ingest "$CORPUS" "$PROJ_STORE" single false 100000 1 2>/dev/null | grep -E 'RESULT'
  fi
  java $JVM_ARGS -cp "$CP" io.sirix.benchmark.PostgresBulkBench \
       projquery "$WORK" "$(basename "$PROJ_STORE")" "$ITERS" 2>/dev/null \
    | grep -E '^#|^query|^countAll|^filterCountYear|^sumYear|^titleLookup'
fi

say "done"
