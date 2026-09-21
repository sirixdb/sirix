#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  printf 'Usage: %s <development|t25k|t100k> <fresh-run-name>\n' "$0" >&2
  exit 2
fi

TIER="$1"
RUN_NAME="$2"
case "${TIER}" in
  development|t25k|t100k) ;;
  *) printf 'Unknown tier: %s\n' "${TIER}" >&2; exit 2 ;;
esac
if [[ ! "${RUN_NAME}" =~ ^[a-zA-Z0-9._-]+$ ]]; then
  printf 'Run name may contain only letters, digits, dot, underscore and hyphen.\n' >&2
  exit 2
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY="$(git -C "${HERE}" rev-parse --show-toplevel)"
ROOT="/var/tmp/sirix-bitemporal/${RUN_NAME}"

if [[ -e "${ROOT}" ]]; then
  printf 'Refusing to overwrite existing run: %s\n' "${ROOT}" >&2
  exit 1
fi

"${HERE}/machine-guard.sh"
mkdir -p "${ROOT}/input" "${ROOT}/logs"

cd "${REPOSITORY}"
/usr/bin/time -v -o "${ROOT}/logs/generate.time" \
  ./gradlew --console=plain :sirix-query:bitemporalGenerate \
  "-Pbitemporal.args=${TIER} ${ROOT}/input" \
  > "${ROOT}/logs/generate.log" 2>&1

ORACLE_ARGS=(--tier "${TIER}" --events "${ROOT}/input/events.jsonl" --out "${ROOT}/oracle")
if [[ "${TIER}" == development ]]; then
  ORACLE_ARGS+=(--cross-check-intervals)
fi
/usr/bin/time -v -o "${ROOT}/logs/oracle.time" \
  python3 "${HERE}/oracle.py" "${ORACLE_ARGS[@]}" \
  > "${ROOT}/logs/oracle.log" 2>&1

"${HERE}/setup-xtdb.sh" > "${ROOT}/logs/xtdb-setup.log" 2>&1

/usr/bin/time -v -o "${ROOT}/logs/sirix-load.time" \
  ./gradlew --console=plain :sirix-query:bitemporalSirixLoad \
  "-Pbitemporal.args=${TIER} ${ROOT}/input/events.jsonl ${ROOT}/sirix" \
  > "${ROOT}/logs/sirix-load.log" 2>&1

/usr/bin/time -v -o "${ROOT}/logs/xtdb-load.time" \
  "${HERE}/run-xtdb.sh" load "${TIER}" "${ROOT}/input/events.jsonl" "${ROOT}/xtdb" \
  > "${ROOT}/logs/xtdb-load.log" 2>&1

/usr/bin/time -v -o "${ROOT}/logs/sirix-run.time" \
  ./gradlew --console=plain :sirix-query:bitemporalSirixRun \
  "-Pbitemporal.args=${ROOT}/sirix ${ROOT}/sirix-results" \
  > "${ROOT}/logs/sirix-run.log" 2>&1

/usr/bin/time -v -o "${ROOT}/logs/xtdb-run.time" \
  "${HERE}/run-xtdb.sh" run "${ROOT}/xtdb" "${ROOT}/xtdb-results" \
  > "${ROOT}/logs/xtdb-run.log" 2>&1

python3 "${HERE}/compare-results.py" \
  --oracle "${ROOT}/oracle" \
  --sirix "${ROOT}/sirix-results" \
  --xtdb "${ROOT}/xtdb-results" \
  --out "${ROOT}/exactness.json" \
  | tee "${ROOT}/logs/exactness.log"

du -B1 -s "${ROOT}/sirix" "${ROOT}/xtdb" | tee "${ROOT}/logs/allocated-bytes.txt"
du -B1 -s --apparent-size "${ROOT}/sirix" "${ROOT}/xtdb" | tee "${ROOT}/logs/logical-bytes.txt"
printf 'SH1_EXACTNESS_PASS tier=%s root=%s\n' "${TIER}" "${ROOT}"
