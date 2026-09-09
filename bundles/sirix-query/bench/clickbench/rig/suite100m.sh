#!/usr/bin/env bash
# One complete steering leg. For an effect with uncertainty use measure.py compare.
# CB_RUN_OUT selects a fresh local evidence directory; the database stays read-only.
set -euo pipefail
. "$(dirname "$0")/rig.env"
require_100m_db
if [[ "${1:-3}" != 3 ]]; then
  echo 'A scored leg requires exactly three tries. Use diag100m.sh for other repetitions.' >&2
  exit 2
fi
OUT=${CB_RUN_OUT:-$ROOT/bundles/sirix-query/build/diagnostics/rig-runs/suite-$(date +%Y%m%dT%H%M%S)-$$}
exec python3 "$RIG/measure.py" run --db "$D100M/db" --out "$OUT" --jvm-args="${EXTRA:-}"
