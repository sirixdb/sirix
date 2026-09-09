#!/usr/bin/env bash
# Profile selected queries under the same process-owned lease and 100M envelope.
# TRIES=2 bash diag100m.sh 27,33,34 "-Dsirix.foo=true"
# Profiling output is explicitly unscored and lives outside the database.
set -euo pipefail
. "$(dirname "$0")/rig.env"
require_100m_db
OUT=${CB_RUN_OUT:-$ROOT/bundles/sirix-query/build/diagnostics/rig-runs/diag-$(date +%Y%m%dT%H%M%S)-$$}
exec python3 "$RIG/measure.py" run --db "$D100M/db" --out "$OUT" \
  --diagnostic --queries "${1:-39}" --tries "${TRIES:-1}" \
  --diagnostic-arg=-Dsirix.projDiag=true --diagnostic-args="${2:-}"
