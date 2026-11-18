#!/usr/bin/env bash
set -euo pipefail
root=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
profile=${1:-all}
output=${2:?Usage: run-smoke.sh core\|query\|all ABSOLUTE_OUTPUT_DIR}
mutation=${SIRIX_SMOKE_OMIT_CONSTRUCTOR:-}
case "$mutation" in ""|PSMS|PSMW) ;; *) echo "Invalid constructor mutation" >&2; exit 2;; esac
[[ -z "$mutation" || "$profile" == core ]] || { echo "Mutations require core profile" >&2; exit 2; }
case "$profile" in core|query|all) ;; *) echo "Invalid profile: $profile" >&2; exit 2;; esac
[[ "$output" = /* ]] || { echo 'Output directory must be absolute' >&2; exit 2; }
if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
  echo 'SKIP: shared-image smoke requires Linux x64'; exit 0
fi
java_home=${GRAALVM_HOME:-${JAVA_HOME:-}}
if [[ -z "$java_home" || ! -x "$java_home/bin/native-image" ]]; then
  echo 'SKIP: set GRAALVM_HOME to a GraalVM JDK 25 with native-image'; exit 0
fi
[[ ! -x /nonexistent/java ]] || { echo 'The empty runtime PATH unexpectedly contains java' >&2; exit 1; }
for tool in cc strace readelf timeout python3; do
  command -v "$tool" >/dev/null || { echo "Missing smoke dependency: $tool" >&2; exit 1; }
done
disk_floor=${SIRIX_SMOKE_DISK_FLOOR_KIB:-62914560}
[[ "$disk_floor" =~ ^[1-9][0-9]*$ ]] || { echo 'Disk floor must be positive KiB' >&2; exit 2; }
mkdir -p "$output"
output=$(realpath "$output")
# Optional local admission wrapper; CI already has an isolated, bounded runner.
heavy() {
  if [[ -n "${SIRIX_HEAVY_RUNNER:-}" ]]; then "$SIRIX_HEAVY_RUNNER" "$@"; else "$@"; fi
}
check_disk() {
  local available
  available=$(df -Pk "$output" | awk 'NR==2 {print $4}')
  if (( available < disk_floor )); then
    echo 'Insufficient free disk space for native smoke' >&2; exit 1
  fi
}
export JAVA_HOME="$java_home"
export PATH="$java_home/bin:$PATH"
profiles=("$profile")
[[ "$profile" != all ]] || profiles=(core query)
for selected in "${profiles[@]}"; do
  check_disk
  stage="$output/$selected"
  mkdir -p "$stage/classes"
  heavy "$root/gradlew" -p "$root" --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx2g \
    -Dsirix.smoke.output="$output" -I "$root/tests/native/smoke.init.gradle" \
    ":sirix-$selected:nativeLibrarySmokeClasspath" >"$stage/gradle.log" 2>&1
  cp=$(cat "$output/$selected.classpath")
  if [[ "$selected" == core && "$cp" =~ sirix-(query|rest|kotlin-cli) ]]; then
    echo 'Core image classpath contaminated by another Sirix module' >&2; exit 1
  fi
  if [[ -n "$mutation" ]]; then
    cp=$(python3 - "$cp" "$stage" "$mutation" <<'PYMUTATION'
import json
import os
import shutil
import sys
from pathlib import Path
entries = sys.argv[1].split(os.pathsep)
relative = Path('META-INF/native-image/io.sirix/sirix-core/reachability-metadata.json')
found = False
for index, entry in enumerate(entries):
    original = Path(entry) / relative
    if not original.is_file():
        continue
    if found:
        raise SystemExit('Ambiguous core metadata ownership')
    found = True
    target = Path(sys.argv[2]) / 'mutated-core-resources'
    shutil.copytree(entry, target, dirs_exist_ok=True)
    metadata = json.loads(original.read_text())
    node = next(item for item in metadata['reflection']
                if item['type'] == 'com.github.benmanes.caffeine.cache.' + sys.argv[3])
    constructor = {'name': '<init>', 'parameterTypes': []}
    if constructor not in node.get('methods', []):
        raise SystemExit('Mutation target constructor is absent already')
    node['methods'].remove(constructor)
    (target / relative).write_text(json.dumps(metadata))
    entries[index] = str(target)
if not found:
    raise SystemExit('Core resources directory missing from Gradle classpath')
print(os.pathsep.join(entries))
PYMUTATION
)
  fi
  sources=("$root/tests/native/java/io/sirix/nativeimage/smoke/CoreSmoke.java")
  defines=()
  if [[ "$selected" == query ]]; then
    sources+=("$root/tests/native/java/io/sirix/nativeimage/smoke/QuerySmoke.java")
    defines=(-DQUERY_SMOKE)
  fi
  heavy "$java_home/bin/javac" --enable-preview --source 25 --target 25 \
    --add-modules org.graalvm.nativeimage,org.graalvm.word -cp "$cp" \
    -d "$stage/classes" "${sources[@]}" >"$stage/javac.log" 2>&1
  check_disk
  heavy "$java_home/bin/native-image" --shared --no-fallback --enable-preview \
    --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector \
    -H:+UnlockExperimentalVMOptions -H:+ForeignAPISupport -H:+VectorAPISupport \
    -H:NumberOfThreads=2 -J-Xmx4g -Ob -R:TearDownWarningSeconds=5 \
    --initialize-at-build-time=org.slf4j,ch.qos.logback \
    --initialize-at-run-time=io.sirix.io.bytepipe.FFILz4Compressor \
    -cp "$stage/classes:$cp" -o "$stage/libsirix_smoke" >"$stage/native-image.log" 2>&1
  # $ORIGIN is expanded by the ELF loader, not this shell.
  cc -std=c11 -Wall -Wextra -Werror "${defines[@]}" -I "$stage" \
    "$root/tests/native/smoke.c" -L "$stage" "-Wl,-rpath,\$ORIGIN" -lsirix_smoke -o "$stage/smoke"
  status=0
  env -i PATH=/nonexistent /usr/bin/timeout --kill-after=5 60 "$stage/smoke" \
    >"$stage/missing-argument.log" 2>&1 || status=$?
  [[ "$status" == 90 ]] || { echo 'Missing C path argument was not rejected' >&2; exit 1; }
  status=0
  env -i PATH=/nonexistent JAVA_HOME=/forbidden /usr/bin/timeout --kill-after=5 60 \
    "$stage/smoke" "$stage/forbidden-environment-db" >"$stage/forbidden-environment.log" 2>&1 || status=$?
  [[ "$status" == 90 && ! -e "$stage/forbidden-environment-db" ]] || {
    echo 'Contaminated runtime environment was not rejected before database creation' >&2; exit 1;
  }
  readelf -d "$stage/libsirix_smoke.so" >"$stage/elf.txt"
  if grep -qi libjvm "$stage/elf.txt"; then echo 'Unexpected libjvm dependency' >&2; exit 1; fi
  for cycle in 1 2; do
    # Unique fixture path; do not erase any existing database supplied by a caller.
    run=$(mktemp -d "$stage/run-$cycle-XXXXXX")
    status=0
    env -i PATH=/nonexistent /usr/bin/timeout --signal=TERM --kill-after=5 60 \
      /usr/bin/strace -f -e trace=execve,execveat,open,openat -o "$run/exec.trace" \
      "$stage/smoke" "$run/db" >"$run/stdout.log" 2>"$run/stderr.log" || status=$?
    python3 - "$run/exec.trace" "$stage/smoke" <<'PY'
import re
import sys
from pathlib import Path
trace = Path(sys.argv[1]).read_text()
if 'libjvm' in trace.lower():
    raise SystemExit('Unexpected JVM library access in native smoke')
execs = [line for line in trace.splitlines() if re.search(r'\bexecve(?:at)?\(', line)]
if len(execs) != 1 or f'execve("{sys.argv[2]}"' not in execs[0] or '= 0' not in execs[0]:
    raise SystemExit('Unexpected executable/spawn in native smoke: ' + repr(execs))
PY
    if [[ -n "$mutation" ]]; then
      if [[ "$status" != 92 ]] || ! grep -q MissingReflectionRegistrationError "$run/stderr.log" ||
          ! grep -q "caffeine.cache.$mutation" "$run/stderr.log"; then
        echo "Mutation did not fail for the expected missing constructor (status=$status)" >&2
        exit 1
      fi
      echo "KILLED: missing $mutation constructor"
      break
    fi
    if [[ "$status" != 0 ]]; then
      echo "FAIL: $selected runtime status=$status; evidence: $run" >&2
      cat "$run/stdout.log" >&2
      exit "$status"
    fi
    cat "$run/stdout.log"
  done
  if [[ "${SIRIX_SMOKE_TEST_TEARDOWN_MUTATION:-0}" == 1 && -z "$mutation" ]]; then
    # $ORIGIN is expanded by the ELF loader, not this shell.
    cc -std=c11 -Wall -Wextra -Werror "${defines[@]}" -DOMIT_SHUTDOWN -I "$stage" \
      "$root/tests/native/smoke.c" -L "$stage" "-Wl,-rpath,\$ORIGIN" -lsirix_smoke -o "$stage/smoke-no-shutdown"
    run=$(mktemp -d "$stage/no-shutdown-XXXXXX")
    status=0
    env -i PATH=/nonexistent /usr/bin/timeout --signal=TERM --kill-after=5 60 \
      "$stage/smoke-no-shutdown" "$run/db" >"$run/stdout.log" 2>"$run/stderr.log" || status=$?
    if [[ "$status" != 124 ]] || ! grep -qx 'core_answer=42' "$run/stdout.log" ||
        ! grep -q 'laggard.*sirix-snapshot-append-' "$run/stderr.log"; then
      echo "Missing shutdown hook did not reproduce snapshot-worker non-return: $run (status=$status)" >&2
      exit 1
    fi
    if [[ "$selected" == query ]] && ! grep -qx 'query_answer=42' "$run/stdout.log"; then
      echo 'Query failed before the teardown mutation' >&2; exit 1
    fi
    echo 'KILLED: omitted snapshot shutdown hook (native watchdog and laggard stack)'
  fi
  [[ -n "$mutation" ]] || echo "PASS: $selected JVM-free C shared-image smoke"
done
