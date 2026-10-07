#!/usr/bin/env bash
set -eu
if [ "$#" -ne 5 ]; then echo 'output-directory scenarios forks warmups samples' >&2; exit 2; fi
output=$1
scenarios=$2
forks=$3
warmups=$4
samples=$5
mkdir -p "$output"
for ((fork=0; fork<forks; fork++)); do
  printf -v label 'p%02d-candidate' "$fork"
  printf '%s queued %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$label"
  build/replay/latency/run-fork.sh candidate "$label" "$scenarios" "$warmups" "$samples" false > "$output/$label.log" 2>&1
  printf '%s complete %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$label"
done
