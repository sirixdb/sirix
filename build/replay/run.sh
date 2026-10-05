#!/usr/bin/env bash
heavy() { while :; do a=$(awk '/MemAvailable/{print int($2/1048576)}' /proc/meminfo); if [ "$a" -ge 6 ]; then for s in 1 2; do flock -n -E 75 /var/tmp/fm-heavy-jvm.$s.lock "$@"; rc=$?; [ $rc -ne 75 ] && return $rc; done; fi; sleep 30; done; }
export GRADLE_USER_HOME="$PWD/build/replay/gradle-home"
export GRADLE_RO_DEP_CACHE=/home/johannes/.gradle/caches
export TMPDIR="$PWD/build/replay/tmp"
validation_guard=()
if [ -n "${SIRIX_REPLAY_VALIDATION_SECONDS:-}" ]; then
  validation_guard=(bash build/replay/admit-validation.sh)
fi
heavy "${validation_guard[@]}" taskset -c 0-11 ./gradlew -Dorg.gradle.jvmargs=-Xmx2g -Dmaven.repo.local="$PWD/build/replay/m2" --no-daemon --max-workers=2 -PtestHeapMin=256m -PtestHeapMax=2g "$@"
