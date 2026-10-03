#!/usr/bin/env bash
# Run from the repository root, against already-loaded natural-batch task stores.
set -uo pipefail
if [ "$#" -ne 2 ]; then
  echo 'usage: bash docs/performance/cheap-first/measure.sh LABEL TASK_STORE_ROOT' >&2
  exit 2
fi
source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
label=$1
task_stores=$2
case "$label" in ''|*[!a-zA-Z0-9_-]*) echo 'invalid label' >&2; exit 2;; esac
case "$task_stores" in /var/tmp/sirix-bitemporal/sirix-ms-cheap-first-let-materialize) ;; *) echo 'use the task-owned natural-batch stores' >&2; exit 2;; esac
heavy() { while :; do a=$(awk '/MemAvailable/{print int($2/1048576)}' /proc/meminfo); if [ "$a" -ge 6 ]; then for s in 1 2; do flock -n -E 75 /var/tmp/fm-heavy-jvm.$s.lock "$@"; rc=$?; [ $rc -ne 75 ] && return $rc; done; fi; sleep 30; done; }
export GRADLE_USER_HOME="$PWD/build/cheap-first-let/gradle-home"
heavy ./gradlew --no-daemon --max-workers=1 -Dorg.gradle.jvmargs=-Xmx2g \
  -Dmaven.repo.local="$PWD/build/cheap-first-let/m2-private" \
  -PtestHeapMin=512m -PtestHeapMax=2g \
  -I "$source_dir/classpath.init.gradle" :sirix-query:cheapFirstProbeClasspath || exit $?
output="$PWD/build/cheap-first-only"
classpath=$(cat "$output/probe.cp")
mkdir -p "$output/probe"
javac -J-Xmx512m --enable-preview --release 25 --add-modules jdk.incubator.vector \
  -cp "$classpath" -d "$output/probe" "$source_dir/LatencyProbe.java" || exit $?
git rev-parse HEAD > "$output/$label-revision.txt"
git status --porcelain > "$output/$label-worktree.txt"
java -Xmx256m -version > "$output/$label-java.txt" 2>&1
IFS=: read -r -a jars <<< "$classpath"
for jar in "${jars[@]}"; do
  case "$jar" in */brackit-*.jar) sha256sum "$jar";; esac
done > "$output/$label-brackit.txt"
java_args=(--enable-preview --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED -Xms512m -Xmx2g -XX:MaxDirectMemorySize=1g)
probe_args=(-cp "$output/probe:$classpath" LatencyProbe "$task_stores/t100k" /var/tmp/sirix-bitemporal/t100k/oracle)
java "${java_args[@]}" "${probe_args[@]}" "$output/$label-warm" 10 1 2 3 10 > "$output/$label-warm.log" 2>&1 || exit $?
java "${java_args[@]}" "${probe_args[@]}" "$output/$label-warm-q5" 3 5 > "$output/$label-warm-q5.log" 2>&1 || exit $?
: > "$output/$label-cold.log"
for query in 1 2 3 5 10; do
  for repetition in 1 2 3; do
    java "${java_args[@]}" "${probe_args[@]}" "$output/$label-cold" 1 "$query" >> "$output/$label-cold.log" 2>&1 || exit $?
  done
done
for tier in t25k t100k; do
  answers="$output/$label-oracle-$tier"
  java "${java_args[@]}" -cp "$classpath" io.sirix.query.bench.bitemporal.BitemporalSirixRunMain \
    "$task_stores/$tier" "$answers" > "$output/$label-oracle-$tier.log" 2>&1 || exit $?
  for query in {1..12}; do
    cmp "$answers/q$query.tsv" "/var/tmp/sirix-bitemporal/$tier/oracle/q$query.tsv" || exit $?
  done
done
