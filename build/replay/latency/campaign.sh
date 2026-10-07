#!/usr/bin/env bash
set -eu
if [ "$#" -ne 6 ]; then echo 'output-directory scenarios pairs warmups samples diagnostics' >&2; exit 2; fi
output=$1
scenarios=$2
pairs=$3
warmups=$4
samples=$5
diagnostics=$6
mkdir -p "$output"
for ((pair=0; pair<pairs; pair++)); do
  printf -v prefix 'p%02d' "$pair"
  if ((pair % 2 == 0)); then variants=(baseline candidate); else variants=(candidate baseline); fi
  for variant in "${variants[@]}"; do
    label="$prefix-$variant"
    printf '%s queued %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$label"
    build/replay/latency/run-fork.sh "$variant" "$label" "$scenarios" "$warmups" "$samples" "$diagnostics" > "$output/$label.log" 2>&1
    printf '%s complete %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$label"
  done
done
