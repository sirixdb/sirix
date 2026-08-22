#!/usr/bin/env bash
#
# Shared protocol helpers for the SirixDB benchmark reproduction kits.
# Source it, do not run it:
#
#     . "$(dirname "$0")/../common/bench-common.sh"
#
# Everything here is deliberately failure-loud: a benchmark harness that
# degrades quietly produces numbers nobody can defend. The one exception is the
# cool gate, which warns and continues when it cannot read a temperature --
# refusing to run at all on a box without `sensors` would make the kit
# unreproducible for exactly the people it is written for.

# Resolve the kit's own directories regardless of the caller's cwd.
BENCH_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${BENCH_COMMON_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/../../.." && pwd)"
EVICT_PY="${BENCH_COMMON_DIR}/evict.py"

# ---------------------------------------------------------------------------
# diagnostics
# ---------------------------------------------------------------------------

log()  { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
warn() { printf '[%s] WARNING: %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# die <message> -- name what failed, then stop. Every caller passes a message
# that identifies the step, because a bare non-zero exit deep in a benchmark
# pipeline costs a whole re-run to attribute.
die() {
  printf '%s: FAILED: %s\n' "${0##*/}" "$*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1${2:+ ($2)}"
}

need_file() {
  [ -f "$1" ] || die "${2:-required file} not found: $1"
}

need_dir() {
  [ -d "$1" ] || die "${2:-required directory} not found: $1"
}

# ---------------------------------------------------------------------------
# cold regime
# ---------------------------------------------------------------------------

# evict_paths <path>... -- drop the page cache for the given trees.
evict_paths() {
  [ -f "${EVICT_PY}" ] || die "evict.py missing from the kit: ${EVICT_PY}"
  python3 "${EVICT_PY}" ${BENCH_EVICT_FLAGS:--q} "$@" \
    || die "page-cache eviction failed for: $*"
}

# ---------------------------------------------------------------------------
# thermal gate
#
# This laptop drops to 1/7 of its clock at 99 C. Block-measuring two arms once
# faked a 1.7x regression and code was reverted over it. Every timed run
# therefore waits for the package to fall below COOL_MAX_C first, and arms are
# interleaved rather than blocked.
# ---------------------------------------------------------------------------

COOL_MAX_C="${COOL_MAX_C:-55}"
COOL_TIMEOUT_S="${COOL_TIMEOUT_S:-900}"
_cool_warned=0

# package_temp -- integer degrees C, or empty if unreadable.
package_temp() {
  command -v sensors >/dev/null 2>&1 || return 0
  sensors 2>/dev/null | sed -n \
    -e 's/^Package id [0-9]*: *+\([0-9][0-9]*\)\..*/\1/p' \
    -e 's/^Tctl: *+\([0-9][0-9]*\)\..*/\1/p' \
    -e 's/^Tdie: *+\([0-9][0-9]*\)\..*/\1/p' \
    -e 's/^CPU: *+\([0-9][0-9]*\)\..*/\1/p' | head -1
}

# cool_gate -- block until the package is below COOL_MAX_C.
# Never fails the run: on a box without lm-sensors it warns once and continues,
# and it gives up (loudly) after COOL_TIMEOUT_S so an idle-but-hot machine
# cannot hang a suite forever. Deliberately tolerant of `set -e`: no bare
# command substitution is allowed to end the script.
cool_gate() {
  local temp waited=0
  temp="$(package_temp || true)"
  if [ -z "${temp}" ]; then
    if [ "${_cool_warned}" -eq 0 ]; then
      warn "no package temperature available (install lm-sensors and run 'sensors-detect')."
      warn "Timings on a thermally throttling machine are NOT comparable across arms --"
      warn "interleave arms and treat single-arm blocks with suspicion."
      _cool_warned=1
    fi
    return 0
  fi
  while [ "${temp}" -ge "${COOL_MAX_C}" ]; do
    if [ "${waited}" -ge "${COOL_TIMEOUT_S}" ]; then
      warn "still ${temp} C after ${waited}s (gate is ${COOL_MAX_C} C) -- proceeding anyway."
      warn "This run is thermally suspect; do not compare it against a cool arm."
      return 0
    fi
    sleep 10
    waited=$((waited + 10))
    temp="$(package_temp || true)"
    [ -n "${temp}" ] || return 0
  done
  return 0
}

# ---------------------------------------------------------------------------
# gradle
#
# The kit prefers the wrapper. GRADLE=<path> overrides it (the campaign box runs
# an offline distribution launcher with its own GRADLE_USER_HOME because
# ~/.gradle is read-only under the sandbox).
# ---------------------------------------------------------------------------

gradle_cmd() {
  if [ -n "${GRADLE:-}" ]; then
    printf '%s' "${GRADLE}"
  else
    printf '%s' "${REPO_ROOT}/gradlew"
  fi
}

# run_gradle <task> <args...> -- run gradle and fail loudly, naming the task.
run_gradle() {
  local task="$1"
  shift
  local gradle
  gradle="$(gradle_cmd)"
  [ -x "${gradle}" ] || die "gradle launcher is not executable: ${gradle} (set GRADLE=<path>)"
  log "gradle ${task}"
  "${gradle}" --console=plain ${GRADLE_FLAGS:-} -p "${REPO_ROOT}" "${task}" "$@" \
    || die "gradle ${task} failed (see the output above; the JVM's own stack trace is the cause)"
}
