#!/usr/bin/env bash
#
# cold-rounds.sh <sirix-db-dir> [options]
#
# The cold-regime measurement protocol for the ClickBench port: evicted page
# cache, cool-gated, interleaved arms, min over rounds. This is the script that
# produced the cold suite figures in docs/BENCHMARK_CAMPAIGNS.md §4.
#
# options
#   --arm NAME=PATH   an ahead-of-time image to measure, repeatable. With two or
#                     more arms the rounds are INTERLEAVED (A B A B), never
#                     blocked (A A B B) -- see below. With no --arm the JVM is
#                     measured through gradle as a single arm.
#   --rounds N        rounds per arm (default 4; the shipped figure is the best
#                     of 4 and the median is reported alongside it)
#   --tries N         tries per query inside a round (default 3)
#   --queries LIST    subset, ZERO-BASED (ClickBench convention: `--queries 18`
#                     is Q19 in the docs). JSONBench's runner is one-based --
#                     the two do not agree, and mixing them up costs a run.
#   --out DIR         per-round JSON + logs (default <db>-cold-results)
#   --duckdb-cold S   DuckDB reference cold suite seconds (default 0.520)
#   --duckdb-hot S    DuckDB reference hot suite seconds  (default 0.351)
#
# WHY INTERLEAVED, AND WHY THE COOL GATE
# --------------------------------------
# The campaign laptop drops to one seventh of its clock at 99 C. Measuring arm A
# as a block and then arm B as a block once produced a clean, consistent, and
# entirely fictitious 1.7x regression -- code was reverted over it before the
# thermal cause was found. Interleaving makes both arms pay the same thermal
# state, and the gate (wait until the package is below 55 C, default) keeps that
# state near the bottom of the range. A 40 W power cap on top of it is what made
# the campaign's numbers repeatable to ~1 %.
#
# Everything is min-of-N, including any internal phase timer you may add: a
# single-sample phase timer on this box once mis-attributed a change by 2.7x
# (reported +73 ms where the truth was -17.7 ms).
#
# The cold regime also needs a fresh PROCESS, not just a cold cache -- catalog
# first-touch and JIT/AOT setup are part of what "cold" means here. Each round
# is therefore a new process, and the suite figure is try 1.
set -u

. "$(cd "$(dirname "${BASH_SOURCE[0]}")/../common" && pwd)/bench-common.sh"

usage() {
  sed -n '2,30p' "$0" >&2
  exit 2
}

[ "$#" -ge 1 ] || usage
DB="$1"; shift

ROUNDS=4
TRIES=3
QUERIES=""
OUT=""
DUCKDB_COLD="0.520"
DUCKDB_HOT="0.351"
ARM_NAMES=()
ARM_PATHS=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    --arm)
      spec="${2:-}"
      case "${spec}" in
        *=*) : ;;
        *) die "--arm expects NAME=PATH, got: ${spec}" ;;
      esac
      ARM_NAMES+=("${spec%%=*}")
      ARM_PATHS+=("${spec#*=}")
      shift 2 ;;
    --rounds)      ROUNDS="${2:-}"; shift 2 ;;
    --tries)       TRIES="${2:-}"; shift 2 ;;
    --queries)     QUERIES="${2:-}"; shift 2 ;;
    --out)         OUT="${2:-}"; shift 2 ;;
    --duckdb-cold) DUCKDB_COLD="${2:-}"; shift 2 ;;
    --duckdb-hot)  DUCKDB_HOT="${2:-}"; shift 2 ;;
    -h|--help)     usage ;;
    *) die "unknown option: $1" ;;
  esac
done

need_dir "${DB}" "the loaded ClickBench database"
need_cmd python3
case "${ROUNDS}" in ''|*[!0-9]*) die "--rounds must be a positive integer" ;; esac
case "${TRIES}"  in ''|*[!0-9]*) die "--tries must be a positive integer" ;; esac
[ "${ROUNDS}" -ge 1 ] || die "--rounds must be >= 1"

OUT="${OUT:-${DB%/}-cold-results}"
mkdir -p "${OUT}" || die "cannot create output directory: ${OUT}"

QUERY_ARG=""
[ -n "${QUERIES}" ] && QUERY_ARG=" --queries ${QUERIES}"

if [ "${#ARM_NAMES[@]}" -eq 0 ]; then
  ARM_NAMES=("jvm")
  ARM_PATHS=("")
  log "no --arm given: measuring the JVM through gradle."
  log "The published ClickBench figures are from an ahead-of-time image; a JVM run"
  log "pays classload and JIT warm-up on top and is NOT comparable to them."
else
  for path in "${ARM_PATHS[@]}"; do
    [ -x "${path}" ] || die "arm binary is not executable: ${path}"
  done
fi

GRADLE_LAUNCHER="$(gradle_cmd)"

run_arm() {  # run_arm <index> <round>
  local idx="$1" round="$2"
  local name="${ARM_NAMES[$idx]}" bin="${ARM_PATHS[$idx]}"
  local json="${OUT}/${name}-round-${round}.json"
  local logf="${OUT}/${name}-round-${round}.log"

  if [ -n "${bin}" ]; then
    # shellcheck disable=SC2086  # QUERY_ARG is an intentional word split
    "${bin}" "${DB}" --tries "${TRIES}" --json "${json}" ${QUERY_ARG} > "${logf}" 2>&1 \
      || { echo "--- last 40 lines of ${logf} ---" >&2; tail -40 "${logf}" >&2;
           die "arm '${name}' failed in round ${round}"; }
  else
    [ -x "${GRADLE_LAUNCHER}" ] || die "gradle launcher is not executable: ${GRADLE_LAUNCHER} (set GRADLE=<path>)"
    "${GRADLE_LAUNCHER}" --console=plain ${GRADLE_FLAGS:-} -p "${REPO_ROOT}" \
        :sirix-query:clickBench \
        -Pclickbench.args="${DB} --tries ${TRIES} --json ${json}${QUERY_ARG}" \
        > "${logf}" 2>&1 \
      || { echo "--- last 40 lines of ${logf} ---" >&2; tail -40 "${logf}" >&2;
           die "the JVM arm failed in round ${round}"; }
  fi
  grep -h '^# served' "${logf}" | sed 's/^/      /' || true
}

log "database: ${DB} ($(du -sh "${DB}" 2>/dev/null | cut -f1))"
log "arms: ${ARM_NAMES[*]}    rounds: ${ROUNDS}    tries: ${TRIES}    cool gate: ${COOL_MAX_C} C"

for round in $(seq 1 "${ROUNDS}"); do
  idx=0
  while [ "${idx}" -lt "${#ARM_NAMES[@]}" ]; do
    cool_gate
    evict_paths "${DB}"
    log "round ${round}/${ROUNDS}, arm '${ARM_NAMES[$idx]}'"
    run_arm "${idx}" "${round}"
    idx=$((idx + 1))
  done
done

python3 - "${OUT}" "${ROUNDS}" "${DUCKDB_COLD}" "${DUCKDB_HOT}" "${QUERIES}" "${ARM_NAMES[@]}" <<'PYEOF' || exit 1
import json, statistics, sys
from pathlib import Path

out, rounds, duck_cold, duck_hot = Path(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
spec, arms = sys.argv[5], sys.argv[6:]

# The runner always writes all 43 rows and fills the ones --queries left out with
# nulls. Two very different things produce a null row, and conflating them is how
# a partial sum gets published as a suite figure:
#   * the query was not requested  -> expected, and the sum is a labelled subset
#   * the query was requested and FAILED -> the summary must refuse to print
# So the requested set is parsed from --queries and compared against what ran.
def parse_spec(text):
    if not text.strip():
        return None            # no --queries: the whole suite was requested
    wanted = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part[1:]:
            lo, hi = part.split("-", 1)
            wanted.update(range(int(lo), int(hi) + 1))
        else:
            wanted.add(int(part))
    return sorted(wanted)

requested = parse_spec(spec)

def load(path):
    if not path.exists():
        sys.exit(f"missing round output: {path}")
    doc = json.loads(path.read_text())
    rows = doc.get("result") or doc.get("results")
    if not rows:
        sys.exit(f"{path} carries no 'result' array -- the run produced no timings")
    executed = [i for i, row in enumerate(rows) if row and row[0] is not None]
    want = requested if requested is not None else list(range(len(rows)))
    failed = [i for i in want if i not in executed]
    if failed:
        sys.exit(f"{path}: no timing for query index {failed} (zero-based) -- those queries "
                 f"failed. Read the log next to this file; the summary is withheld because a "
                 f"partial sum is not a suite result.")
    return rows, executed, len(rows)

reference_set = None
total_queries = None
results = {}
for arm in arms:
    colds, hots = [], []
    for r in range(1, rounds + 1):
        rows, executed, total = load(out / f"{arm}-round-{r}.json")
        if reference_set is None:
            reference_set, total_queries = executed, total
        elif executed != reference_set:
            sys.exit(f"{arm} round {r} ran a different query set than the first run -- "
                     f"these results are not comparable")
        colds.append(sum(rows[i][0] for i in executed))
        if len(rows[executed[0]]) > 1:
            hots.append(sum(min(rows[i][1:]) for i in executed))
    results[arm] = (colds, hots)

full_suite = len(reference_set) == total_queries

print()
print(f"  ClickBench cold protocol -- {rounds} evicted round(s) per arm, interleaved")
if full_suite:
    print(f"  DuckDB reference: cold {duck_cold:.3f} s   hot {duck_hot:.3f} s")
else:
    print(f"  SUBSET: {len(reference_set)} of {total_queries} queries "
          f"({','.join(str(i) for i in reference_set)}, zero-based) -- "
          f"NOT comparable to the published suite figure, and no DuckDB ratio is shown.")
print()
head = "  arm          |  cold best |  cold med. |   hot best | vs duckdb (cold/hot)"
print(head)
print("  " + "-" * (len(head) - 2))

for arm in arms:
    colds, hots = results[arm]
    best_cold = min(colds)
    med_cold = statistics.median(colds)
    best_hot = min(hots) if hots else None
    hot_txt = f"{best_hot:8.3f} s" if best_hot is not None else "        -"
    if full_suite:
        ratio = f"{best_cold / duck_cold:.2f}x"
        ratio += f" / {best_hot / duck_hot:.2f}x" if best_hot is not None else " / -"
    else:
        ratio = "(subset)"
    print(f"  {arm:12s} | {best_cold:8.3f} s | {med_cold:8.3f} s | {hot_txt} | {ratio}")
    print(f"               |            |            |            | rounds: "
          f"{[round(c, 3) for c in colds]}")
print()
PYEOF

log "per-round JSON and logs: ${OUT}"
log "correctness is a separate gate: ./run-differential.sh (fast path vs interpreter vs DuckDB)"
