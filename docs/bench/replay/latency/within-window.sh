#!/usr/bin/env bash
set -eu
now=$(date +%s)
start=$(date -d '2026-10-05T16:07:36Z' +%s)
last_start=$(date -d '2026-10-06T01:30:00Z' +%s)
if [ "$now" -lt "$start" ] || [ "$now" -ge "$last_start" ]; then
  echo 'Benchmark start is outside the resumed work window (ending 03:30 Berlin).' >&2
  exit 73
fi
printf 'ADMITTED,%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
exec "$@"
