#!/usr/bin/env bash
heavy() { while :; do a=$(awk '/MemAvailable/{print int($2/1048576)}' /proc/meminfo); if [ "$a" -ge 6 ]; then for s in 1 2; do flock -n -E 75 /var/tmp/fm-heavy-jvm.$s.lock "$@"; rc=$?; [ $rc -ne 75 ] && return $rc; done; fi; sleep 30; done; }
export GRADLE_USER_HOME="$PWD/build/replay/nm-gradle-home"
export GRADLE_RO_DEP_CACHE=/home/johannes/.gradle/caches
export TMPDIR="$PWD/build/replay/nm-tmp"
export SIRIX_REPLAY_VALIDATION_SECONDS=${SIRIX_REPLAY_VALIDATION_SECONDS:-900}
export SIRIX_REPLAY_VALIDATION_DEADLINE=2026-10-07T01:40Z
heavy bash build/replay/admit-validation.sh taskset -c 0-11 ./gradlew -Dorg.gradle.jvmargs=-Xmx2g -Dmaven.repo.local="$PWD/build/replay/nm-m2" --no-daemon --max-workers=2 -PtestHeapMin=256m -PtestHeapMax=2g -Dsirix.workBudget.print=true "$@"
