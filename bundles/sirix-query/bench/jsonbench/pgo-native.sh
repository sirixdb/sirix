#!/usr/bin/env bash
#
# pgo-native.sh <sirix-db-dir> [options]
#
# Build the ahead-of-time (GraalVM native image) JSONBench runner that every
# published number in docs/BENCHMARK_CAMPAIGNS.md was measured with, through the
# full three-step profile-guided cycle:
#
#   1. instrument   nativeCompile -Ppgo-instrument      -> jb-instr
#   2. collect      jb-instr <db> -XX:ProfilesDumpFile= -> jsonbench.iprof
#   3. optimise     nativeCompile -Ppgo=<iprof>         -> jb
#
# options
#   --tier T      1m | 10m | 100m   (default 1m) -- selects the collect-run flags
#   --out DIR     where the binaries and the profile are stashed
#                 (default <repo>/build/pgo-jsonbench)
#   --tries N     tries for the collect run (default 2: one cold, one hot, so the
#                 profile covers both the first-touch and the settled paths)
#   --steps LIST  comma-separated subset of 1,2,3 (default 1,2,3)
#   --dry-run     print the exact commands instead of running them
#
# ###########################################################################
# # THE PROFILE GOES STALE THE MOMENT A HOT-PATH CLASS CHANGES.             #
# #                                                                         #
# # A profile collected before a new hot loop existed gives that loop the   #
# # default AOT treatment, and the resulting binary under-reads its own     #
# # engine. This is not theoretical: the dense group table showed a 40 %    #
# # win on the JVM and ZERO in the first native build, purely because the   #
# # profile predated it -- a fresh instrument/collect/optimise cycle then   #
# # unlocked both that win and broad gains elsewhere (Q2 hot 0.79 -> 0.41 s #
# # at the 100 M tier). The same trap appeared a second time on the         #
# # parallel chain fetch.                                                   #
# #                                                                         #
# # RULE: after landing ANY change to a hot path, re-run steps 1-3 before   #
# # quoting a native number. Reusing an old .iprof is only valid for a tree #
# # whose hot code is byte-identical.                                       #
# ###########################################################################
#
# Two more things that cost a rebuild to learn:
#
# * COLLECT ON THE TIER YOU WILL MEASURE. A profile taken at 1m and used for a
#   100m binary is the stale-profile trap wearing a different hat: the tiers
#   exercise different branches (promotion, parallel chain fetch, dense table).
# * STASH THE BINARY OUT OF THE BUILD DIRECTORY. `nativeCompile` writes to
#   build/native/nativeCompile/<imageName>, and the next build in that tree
#   replaces it. This script copies each binary to --out immediately, which is
#   what makes an A/B between two builds possible at all.
#
# The builds are long (15-25 min each at -O3 with quick-build off). They are run
# in the foreground on purpose: a backgrounded build gets reaped on this rig.
set -u

. "$(cd "$(dirname "${BASH_SOURCE[0]}")/../common" && pwd)/bench-common.sh"

MAIN_CLASS="io.sirix.query.bench.jsonbench.JsonBenchRunMain"
NATIVE_DIR="${REPO_ROOT}/bundles/sirix-query/build/native/nativeCompile"

usage() {
  sed -n '2,30p' "$0" >&2
  exit 2
}

[ "$#" -ge 1 ] || usage
DB="$1"; shift

TIER="1m"
OUT="${REPO_ROOT}/build/pgo-jsonbench"
TRIES=2
STEPS="1,2,3"
DRY_RUN=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --tier)    TIER="${2:-}"; shift 2 ;;
    --out)     OUT="${2:-}"; shift 2 ;;
    --tries)   TRIES="${2:-}"; shift 2 ;;
    --steps)   STEPS="${2:-}"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage ;;
    *) die "unknown option: $1" ;;
  esac
done

need_dir "${DB}" "the database the profile is collected from"
case "${TIER}" in
  1m|10m|100m|1000m) : ;;
  *) die "unknown tier '${TIER}'" ;;
esac

case "${TIER}" in
  100m|1000m) COLLECT_FLAGS="-Xmx14g -Xms12g -Dsirix.offheap.bytes=8589934592 -Dsirix.projection.promoteMaxBytes=0" ;;
  *)          COLLECT_FLAGS="" ;;
esac

IPROF="${OUT}/jsonbench-${TIER}.iprof"
BUILDER_XMX="${BUILDER_XMX:-12g}"

has_step() { case ",${STEPS}," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }

run() {
  if [ "${DRY_RUN}" -eq 1 ]; then
    printf '  DRY-RUN: '; printf '%q ' "$@"; printf '\n'
    return 0
  fi
  "$@"
}

mkdir -p "${OUT}" || die "cannot create output directory: ${OUT}"

if [ -n "${JAVA_HOME:-}" ] && [ ! -x "${JAVA_HOME}/bin/native-image" ]; then
  warn "JAVA_HOME=${JAVA_HOME} has no bin/native-image -- point it at a GraalVM with native-image installed"
fi

GRADLE_LAUNCHER="$(gradle_cmd)"
[ -x "${GRADLE_LAUNCHER}" ] || die "gradle launcher is not executable: ${GRADLE_LAUNCHER} (set GRADLE=<path>)"

native_build() {  # native_build <imageName> <extra gradle args...>
  local image="$1"; shift
  log "native build '${image}' (this takes 15-25 minutes)"
  run "${GRADLE_LAUNCHER}" --console=plain ${GRADLE_FLAGS:-} -p "${REPO_ROOT}" \
      :sirix-query:nativeCompile \
      -Pnative.mainClass="${MAIN_CLASS}" \
      -Pnative.imageName="${image}" \
      -Pnative.builderXmx="${BUILDER_XMX}" \
      -Pquick-build=false \
      "$@" \
    || die "nativeCompile of '${image}' failed"
  if [ "${DRY_RUN}" -eq 0 ]; then
    [ -x "${NATIVE_DIR}/${image}" ] \
      || die "the build reported success but ${NATIVE_DIR}/${image} is missing"
    cp -f "${NATIVE_DIR}/${image}" "${OUT}/${image}" \
      || die "could not stash ${image} into ${OUT} (the next build would overwrite it)"
    log "stashed: ${OUT}/${image} ($(du -h "${OUT}/${image}" | cut -f1))"
  fi
}

# --- 1. instrumented image --------------------------------------------------
# -Ppgo-instrument also drops jline from the image classpath and forces
# -march=compatibility: instrumenting the jline FFM upcall stub crashes the
# register allocator on GA toolchains, and iprof profiles are march-agnostic, so
# only the final build needs native codegen.
if has_step 1; then
  log "step 1/3: instrumented image"
  native_build "jb-instr" -Ppgo-instrument
fi

# --- 2. profile collection --------------------------------------------------
if has_step 2; then
  log "step 2/3: collecting the profile at tier ${TIER} from ${DB}"
  [ "${DRY_RUN}" -eq 1 ] || [ -x "${OUT}/jb-instr" ] \
    || die "no instrumented binary at ${OUT}/jb-instr -- run step 1 first"
  rm -f "${IPROF}"
  cool_gate
  run "${OUT}/jb-instr" "${DB}" --tries "${TRIES}" \
      -XX:ProfilesDumpFile="${IPROF}" ${COLLECT_FLAGS} \
    || die "the instrumented run failed -- its output above names the cause"
  if [ "${DRY_RUN}" -eq 0 ]; then
    [ -s "${IPROF}" ] || die "no profile was written to ${IPROF}; the optimised build would silently be a plain -O3 build"
    log "profile: ${IPROF} ($(du -h "${IPROF}" | cut -f1))"
  fi
fi

# --- 3. optimised image -----------------------------------------------------
if has_step 3; then
  log "step 3/3: optimised image from the profile"
  [ "${DRY_RUN}" -eq 1 ] || [ -s "${IPROF}" ] \
    || die "no profile at ${IPROF} -- run step 2 first (an absent profile does NOT fail the build, it just produces an unprofiled binary)"
  native_build "jb" -Ppgo="${IPROF}"
fi

log "done. Measure with:"
log "  run-benchmark.sh ${TIER} ${DB} <ch-ref-${TIER}> --bin ${OUT}/jb"
