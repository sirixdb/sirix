#!/usr/bin/env bash
#
# run-benchmark.sh <tier> <sirix-db-dir> <clickhouse-ref-dir>
#
# The SirixDB side of JSONBench: load if needed, run N cool-gated evicted
# rounds, prove the answers against the ClickHouse reference, and print the
# scoreboard.
#
#   tier          1m | 10m | 100m | 1000m -- selects the config table below
#   sirix-db-dir  the database directory; created by loading DATA if absent
#   ch-ref-dir    ch-ref-<tier>/ from clickhouse-setup.sh (qN.tsv + baseline.txt)
#
# env / options
#   --data F | DATA=F     cleaned corpus, required only when the db must be built
#   --bin B  | SIRIX_BIN=B  run an ahead-of-time image instead of the JVM
#                           (the shipped numbers are all native -- see README)
#   --rounds N | ROUNDS=2   evicted rounds; the suite figure is the min over them
#   --tries N  | TRIES=3    tries per query inside one round
#   --out DIR  | OUT=DIR    per-round JSON + logs (default <db>-results)
#   SKIP_DIFF=1             skip the differential (not recommended: a timing
#                           without a correctness proof is not a result)
#
# ---------------------------------------------------------------------------
# THE PER-TIER CONFIGURATION, AND WHY EACH FLAG IS THERE
#
#   tier   loader                                   runner
#   1m     -Xmx12g -Xms10g                          (image defaults)
#   10m    -Xmx12g -Xms10g                          (image defaults)
#   100m   -Xmx16g -Xms14g -Dsirix.offheap.bytes=8G -Xmx14g -Xms12g
#                                                   -Dsirix.offheap.bytes=8G
#                                                   -Dsirix.projection.promoteMaxBytes=0
#
# * -Xms is set two gigabytes below -Xmx throughout. A native image grows and
#   zeroes its heap during the first try; at 100m that costs 120 ms min-of-2 and
#   250 ms mean, which is pure measurement noise attributable to configuration.
#   (The campaign's own 1m and 10m native runs passed no heap flags at all and
#   used the image's built-in default; the convention is applied here for
#   consistency and changes nothing measurable at those tiers.)
# * -Dsirix.offheap.bytes bounds the off-heap arena. Both benchmark runners
#   initialise the allocator before opening the database, so the property takes
#   effect; a path that opens a database *without* that early init would instead
#   inherit the size persisted in dbsetting.obj and silently ignore the flag.
# * -Dsirix.projection.promoteMaxBytes=0 disables the byte-kernel promotion at
#   100m. This is a WORKAROUND for an open defect (task #36: the promotion tries
#   to materialise ~30 GB of row-group payloads and OOMs a 14 GB heap). Remove
#   it once #36 is fixed -- with promotion enabled the smaller tiers legitimately
#   route Q4/Q5 to the byte kernel, which is why their `groupDense` counter
#   reads zero there.
#
# ---------------------------------------------------------------------------
# RUNNER FACTS THAT COST TIME TO REDISCOVER
#
# * `--queries` is ONE-BASED (`--queries 2` is Q2), while the rows in the JSON
#   output are zero-based. Every early "QN-only" label taken as zero-based was
#   off by one.
# * `--tries N` is try 1 cold plus N-1 hot tries; the hot figure is the best of
#   tries 2..N. `--tries 1` therefore reports a cold number and no hot number.
# * If a query spills, the JVM needs `-Djava.io.tmpdir=<writable dir>`. Without
#   it the spill dies with a swallowed cause (bit:BIDY0300) that names nothing.
set -u

. "$(cd "$(dirname "${BASH_SOURCE[0]}")/../common" && pwd)/bench-common.sh"

usage() {
  sed -n '2,40p' "$0" >&2
  exit 2
}

[ "$#" -ge 3 ] || usage
TIER="$1"; DB="$2"; REF="$3"; shift 3

DATA="${DATA:-}"
SIRIX_BIN="${SIRIX_BIN:-}"
ROUNDS="${ROUNDS:-2}"
TRIES="${TRIES:-3}"
OUT="${OUT:-}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --data)   DATA="${2:-}"; shift 2 ;;
    --bin)    SIRIX_BIN="${2:-}"; shift 2 ;;
    --rounds) ROUNDS="${2:-}"; shift 2 ;;
    --tries)  TRIES="${2:-}"; shift 2 ;;
    --out)    OUT="${2:-}"; shift 2 ;;
    -h|--help) usage ;;
    *) die "unknown option: $1" ;;
  esac
done

case "${TIER}" in
  1m|10m|100m|1000m) : ;;
  *) die "unknown tier '${TIER}' (expected 1m, 10m, 100m or 1000m)" ;;
esac
case "${ROUNDS}" in ''|*[!0-9]*) die "--rounds must be a positive integer" ;; esac
case "${TRIES}"  in ''|*[!0-9]*) die "--tries must be a positive integer" ;; esac
[ "${ROUNDS}" -ge 1 ] || die "--rounds must be >= 1"
[ "${TRIES}"  -ge 1 ] || die "--tries must be >= 1"

OUT="${OUT:-${DB%/}-results}"
HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"

need_cmd python3
need_dir "${REF}" "the ClickHouse reference directory (run clickhouse-setup.sh)"
need_file "${REF}/q1.tsv" "reference answers"
need_file "${HERE}/compare-results.py" "the differential comparator"

# --- the config table -------------------------------------------------------
case "${TIER}" in
  100m)
    LOAD_FLAGS="-Xmx16g -Xms14g -Dsirix.offheap.bytes=8589934592"
    RUN_FLAGS="-Xmx14g -Xms12g -Dsirix.offheap.bytes=8589934592 -Dsirix.projection.promoteMaxBytes=0"
    ;;
  1000m)
    LOAD_FLAGS="-Xmx16g -Xms14g -Dsirix.offheap.bytes=8589934592"
    RUN_FLAGS="-Xmx14g -Xms12g -Dsirix.offheap.bytes=8589934592 -Dsirix.projection.promoteMaxBytes=0"
    warn "the 1000m tier has never been measured; its config is copied from 100m"
    ;;
  *)
    LOAD_FLAGS="-Xmx12g -Xms10g"
    RUN_FLAGS=""
    ;;
esac

mkdir -p "${OUT}" || die "cannot create output directory: ${OUT}"

# --- 1. load, if the database is not there ---------------------------------
if [ -d "${DB}" ]; then
  log "database present: ${DB} ($(du -sh "${DB}" 2>/dev/null | cut -f1))"
else
  [ -n "${DATA}" ] || die "no database at ${DB} and no --data given -- pass the cleaned corpus"
  need_file "${DATA}" "the cleaned corpus"
  log "loading ${DATA} into ${DB} (shred + projection build)"
  run_gradle :sirix-query:jsonBenchLoad \
      -Pjsonbench.args="${DB} ${DATA}" \
      -Pjsonbench.jvmArgs="${LOAD_FLAGS}"
  [ -d "${DB}" ] || die "the loader reported success but ${DB} does not exist"
  log "loaded: $(du -sh "${DB}" | cut -f1)"
fi

# --- 2. how a round is run --------------------------------------------------
# Native image or JVM. The published numbers are native: an AOT binary has no
# JIT warm-up, which is also why it exposes slow code the JIT used to hide.
if [ -n "${SIRIX_BIN}" ]; then
  [ -x "${SIRIX_BIN}" ] || die "not an executable native image: ${SIRIX_BIN}"
  log "engine: native image ${SIRIX_BIN}"
  run_suite() {  # run_suite <json-out> <log-out> [extra args...]
    local json="$1" logf="$2"; shift 2
    "${SIRIX_BIN}" "${DB}" --tries "${TRIES}" --json "${json}" "$@" ${RUN_FLAGS} \
        > "${logf}" 2>&1 \
      || die "the native runner failed; its output is in ${logf}"
  }
else
  log "engine: JVM via gradle (pass --bin for the ahead-of-time image the shipped numbers use)"
  GRADLE_LAUNCHER="$(gradle_cmd)"
  [ -x "${GRADLE_LAUNCHER}" ] || die "gradle launcher is not executable: ${GRADLE_LAUNCHER} (set GRADLE=<path>)"
  run_suite() {
    local json="$1" logf="$2"; shift 2
    local extra=""
    [ "$#" -gt 0 ] && extra=" $*"
    # Called directly rather than through run_gradle so the failure path can
    # show the tail of the log; a swallowed cause here cost three dead runs.
    "${GRADLE_LAUNCHER}" --console=plain ${GRADLE_FLAGS:-} -p "${REPO_ROOT}" \
        :sirix-query:jsonBench \
        -Pjsonbench.args="${DB} --tries ${TRIES} --json ${json}${extra}" \
        -Pjsonbench.jvmArgs="${RUN_FLAGS}" > "${logf}" 2>&1 \
      || { echo "--- last 40 lines of ${logf} ---" >&2; tail -40 "${logf}" >&2;
           die "the JVM runner failed; full output in ${logf}"; }
  }
fi

# --- 3. the timed rounds ----------------------------------------------------
log "running ${ROUNDS} evicted round(s), --tries ${TRIES}, cool gate ${COOL_MAX_C} C"
for round in $(seq 1 "${ROUNDS}"); do
  cool_gate
  evict_paths "${DB}"
  log "round ${round}/${ROUNDS}"
  run_suite "${OUT}/round-${round}.json" "${OUT}/round-${round}.log"
  grep -h '^# served' "${OUT}/round-${round}.log" | sed 's/^/    /' || true
done

# --- 4. the differential ----------------------------------------------------
# Separate from the timed rounds on purpose: --dump writes every result row to
# disk, which is real I/O that has no business inside a measured run.
DIFF_STATUS="skipped"
if [ "${SKIP_DIFF:-0}" != "1" ]; then
  log "differential run (--dump, untimed)"
  rm -rf "${OUT}/dump"
  TRIES_SAVED="${TRIES}"; TRIES=1
  run_suite "${OUT}/dump.json" "${OUT}/dump.log" --dump "${OUT}/dump"
  TRIES="${TRIES_SAVED}"
  # Written first, then echoed: piping into `tee` would test TEE's exit status,
  # so a failing differential would be reported as a pass.
  python3 "${HERE}/compare-results.py" --dump "${OUT}/dump" --ref "${REF}" \
      > "${OUT}/differential.txt" 2>&1
  diff_rc=$?
  cat "${OUT}/differential.txt"
  if [ "${diff_rc}" -eq 0 ]; then
    DIFF_STATUS="PASS"
  else
    DIFF_STATUS="FAIL"
  fi
fi

# --- 5. the scoreboard ------------------------------------------------------
python3 - "${TIER}" "${OUT}" "${ROUNDS}" "${REF}/baseline.txt" "${DIFF_STATUS}" <<'PYEOF' || die "could not summarise the rounds in ${OUT}"
import json, sys
from pathlib import Path

tier, out, rounds, baseline_path, diff_status = sys.argv[1], Path(sys.argv[2]), int(sys.argv[3]), Path(sys.argv[4]), sys.argv[5]

# ClickHouse baselines measured during the campaign, used when the reference
# directory carries no baseline.txt of its own (see clickhouse-setup.sh).
PUBLISHED = {
    "1m":   [(0.021, 0.015), (0.064, 0.058), (0.027, 0.024), (0.036, 0.023), (0.039, 0.025)],
    "10m":  [(0.023, 0.025), (0.277, 0.227), (0.088, 0.078), (0.124, 0.074), (0.131, 0.080)],
    "100m": [(0.108, 0.105), (2.182, 1.921), (0.797, 0.528), (0.489, 0.427), (0.580, 0.523)],
}

# The runner writes one row per query and fills the ones --queries left out with
# nulls, so the executed set is derived rather than assumed -- otherwise a subset
# run sums to a suite figure that is quietly missing queries.
rows_per_round = []
executed = None
total_queries = None
for r in range(1, rounds + 1):
    path = out / f"round-{r}.json"
    if not path.exists():
        sys.exit(f"missing round output: {path}")
    doc = json.loads(path.read_text())
    rows = doc.get("result") or doc.get("results")
    if not rows:
        sys.exit(f"{path} carries no 'result' array -- the run produced no timings")
    ran = [i for i, row in enumerate(rows) if row and row[0] is not None]
    # This script always asks for the whole suite, so a null row is a query that
    # FAILED -- never a query that was not requested. Dropping it from the sum
    # would produce a suite figure that is quietly missing a query.
    missing = [i + 1 for i in range(len(rows)) if i not in ran]
    if missing:
        sys.exit(f"{path}: no timing for Q{', Q'.join(str(m) for m in missing)} -- "
                 f"those queries failed. Read the round log next to this file; the summary "
                 f"is withheld because a partial sum is not a suite result.")
    if executed is None:
        executed, total_queries = ran, len(rows)
    elif ran != executed:
        sys.exit(f"round {r} ran a different query set than round 1 -- not comparable")
    rows_per_round.append(rows)

cold = [min(rd[q][0] for rd in rows_per_round) for q in executed]
# --tries 1 reports a cold number and no hot one; say so instead of printing NaN.
have_hot = len(rows_per_round[0][executed[0]]) > 1
hot = [min(min(rd[q][1:]) for rd in rows_per_round) for q in executed] if have_hot \
    else [None] * len(executed)
queries = len(executed)
full_suite = queries == total_queries

# The baseline is indexed by the QUERY number (1-based, as the runner's --queries
# takes them and as baseline.txt spells them), while `executed` holds 0-based row
# indices; a subset run must line the two up rather than compare Q3 against Q1.
base, base_src = None, ""
if baseline_path.exists():
    parsed = {}
    for line in baseline_path.read_text().splitlines():
        parts = line.split()
        if len(parts) == 3:
            parsed[parts[0]] = (float(parts[1]), float(parts[2]))
    if all(f"Q{i+1}" in parsed for i in executed):
        base = [parsed[f"Q{i+1}"] for i in executed]
        base_src = f"measured on this machine ({baseline_path})"
if base is None and tier in PUBLISHED and max(executed) < len(PUBLISHED[tier]):
    base = [PUBLISHED[tier][i] for i in executed]
    base_src = "published campaign figures (no baseline.txt in the reference dir)"

def ratio(ours, theirs):
    if ours is None or not theirs:
        return "     -"
    r = ours / theirs
    return f"{r:5.2f}x" + (" " if r >= 1 else "*")

def ms(value):
    return "       -" if value is None else f"{value * 1000:8.0f}"

print()
print(f"  JSONBench, tier {tier} -- min over {rounds} evicted round(s)")
print(f"  ClickHouse baseline: {base_src if base else 'unavailable'}")
if not full_suite:
    print(f"  SUBSET: {queries} of {total_queries} queries "
          f"({','.join(str(i + 1) for i in executed)}) -- the Σ row is NOT the suite figure.")
print()
head = "  query |  sirix cold |   sirix hot |     CH cold |      CH hot |  cold |   hot"
print(head)
print("  " + "-" * (len(head) - 2))
for slot, qidx in enumerate(executed):
    bc, bh = base[slot] if base else (0.0, 0.0)
    print(f"     Q{qidx+1} | {ms(cold[slot])} ms | {ms(hot[slot])} ms |"
          f" {bc*1000:8.0f} ms | {bh*1000:8.0f} ms | {ratio(cold[slot], bc)} | {ratio(hot[slot], bh)}")
sc = sum(cold)
sh = sum(hot) if have_hot else None
bc, bh = (sum(x[0] for x in base), sum(x[1] for x in base)) if base else (0.0, 0.0)
print("  " + "-" * (len(head) - 2))
print(f"      Σ | {ms(sc)} ms | {ms(sh)} ms |"
      f" {bc*1000:8.0f} ms | {bh*1000:8.0f} ms | {ratio(sc, bc)} | {ratio(sh, bh)}")
print()
print("  ratios are sirix/ClickHouse; '*' marks the rows where SirixDB is faster.")
print(f"  differential: {diff_status}")
PYEOF

log "per-round JSON and logs: ${OUT}"
if [ "${DIFF_STATUS}" = "FAIL" ]; then
  die "the differential did not pass -- the timings above describe wrong answers"
fi
