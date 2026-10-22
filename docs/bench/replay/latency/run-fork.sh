#!/usr/bin/env bash
set -u
heavy() { while :; do a=$(awk '/MemAvailable/{print int($2/1048576)}' /proc/meminfo); if [ "$a" -ge 6 ]; then for s in 1 2; do flock -n -E 75 /var/tmp/fm-heavy-jvm.$s.lock "$@"; rc=$?; [ $rc -ne 75 ] && return $rc; done; fi; sleep 30; done; }
if [ "$#" -ne 6 ]; then echo 'variant label scenarios warmups samples diagnostics' >&2; exit 2; fi
variant=$1
label=$2
scenarios=$3
warmups=$4
samples=$5
diagnostics=$6
artifact="$PWD/docs/bench/replay/latency/$variant"
benchmark_java=$(cat "$artifact/java.txt")
benchmark_classpath=$(cat "$artifact/classpath.txt")
mapfile -t benchmark_jvm_args < "$artifact/jvm-args.txt"
replay_scratch="${SIRIX_REPLAY_SCRATCH:-$PWD/.gradle/replay}"
export TMPDIR="$replay_scratch/tmp"
mkdir -p "$TMPDIR"
# The slot-owning child checks the window again, so queuing cannot start a late JVM.
heavy taskset -c 0-11 bash docs/bench/replay/latency/within-window.sh \
  timeout --signal=TERM --kill-after=10s 300s "$benchmark_java" \
  -ea -Xms512m -Xmx3g "${benchmark_jvm_args[@]}" --add-modules jdk.incubator.vector \
  -Dfile.encoding=UTF-8 -Djava.io.tmpdir="$TMPDIR" \
  -Dlogback.configurationFile="$PWD/docs/bench/replay/latency/quiet.xml" \
  -Dsirix.replay.workDiag="$diagnostics" -Dsirix.hot.mergeDiag="$diagnostics" \
  -Dsirix.replay.bench.scenarios="$scenarios" -cp "$benchmark_classpath" \
  io.sirix.replaybench.ReplayLatency "$label" "$PWD/docs/bench/replay/latency/data/$label" "$warmups" "$samples"
