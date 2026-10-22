#!/usr/bin/env bash
set -eu
duration=${SIRIX_REPLAY_VALIDATION_SECONDS:?Set the maximum validation duration in seconds}
case "$duration" in ''|*[!0-9]*) echo 'Validation duration must be a positive integer' >&2; exit 2;; esac
if [ "$duration" -le 0 ]; then echo 'Validation duration must be positive' >&2; exit 2; fi
deadline=$(date -d "${SIRIX_REPLAY_VALIDATION_DEADLINE:?Set the validation cutoff}" +%s)
now=$(date +%s)
# Reserve the timeout's grace period before the captain's hard cutoff.
if [ "$((now + duration + 30))" -gt "$deadline" ]; then
  echo 'Validation cannot fit before the declared cutoff; no JVM started.' >&2
  exit 73
fi
exec timeout --signal=TERM --kill-after=20s "$duration" "$@"
