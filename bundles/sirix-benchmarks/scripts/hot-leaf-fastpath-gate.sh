#!/usr/bin/env bash
# Benchmark gate for the packed HOT leaf's single-byte lookup fast path (HOTLeafBody.findValueRef).
#
# Compares the working tree (candidate) with the committed HEAD (reference) on the uncached
# single-byte selection, HOTLeafCodecBenchmark.uncachedImageLookup with keyBytes=1 and ZERO and
# REPEATED slot contents: five alternating fork pairs, the leaf gate's fork settings. Exits 1 when
# the candidate's paired mean is slower than the reference by more than 10 % for either content,
# 0 otherwise. The margin and its derivation are in HOT_LEAF_CODEC_GATE.md.
#
# This is a latency gate, not a test: it measures wall-clock time and therefore NEVER runs in CI or in
# the test suites (CONV-301 forbids wall-clock assertions there). Run it only on a quiet machine with
# nothing else running, such as a granted timing slot. Its use is the counterfactual record of the
# killed-out-of-lane disposition in .github/mutation-dispositions.toml:
#   python3 .github/scripts/pr_mutation.py counterfactual --patch fastpath.patch -- \
#     bash bundles/sirix-benchmarks/scripts/hot-leaf-fastpath-gate.sh
# Uncommitted changes other than the injected mutant are part of the candidate, so start from a
# clean working tree.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
java=${JAVA_HOME:+$JAVA_HOME/bin/}java
margin=0.10
pairs=5

build() { # <source tree> <destination jar>
  "$1/gradlew" -p "$1" --no-daemon --quiet :sirix-benchmarks:jmhJar
  cp "$(ls "$1"/bundles/sirix-benchmarks/build/libs/*-jmh.jar | head -1)" "$2"
}

mkdir "$work/reference"
git -C "$root" archive HEAD | tar -x -C "$work/reference"
build "$work/reference" "$work/reference.jar"
build "$root" "$work/candidate.jar"

flags=(--enable-preview --add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED)
run() { # <arm> <pair>
  "$java" "${flags[@]}" -jar "$work/$1.jar" 'io.sirix.benchmark.HOTLeafCodecBenchmark.uncachedImageLookup$' \
    -p keyBytes=1 -p content=ZERO,REPEATED -p encoding=ENCODED -f 1 -t 1 -wi 4 -w 350ms -i 6 -r 350ms \
    -foe true -jvmArgsAppend "${flags[*]} -Xms512m -Xmx512m -Dsirix.allocator.maxSize=1G" \
    -rf json -rff "$work/$2-$1.json" > "$work/$2-$1.log" 2>&1
}
for ((pair = 1; pair <= pairs; pair++)); do
  if ((pair % 2)); then run reference "$pair"; run candidate "$pair"; else run candidate "$pair"; run reference "$pair"; fi
done

python3 - "$work" "$pairs" "$margin" <<'PY'
import json
import math
import sys
from pathlib import Path

work, pairs, margin = Path(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
ratios = {}
for pair in range(1, pairs + 1):
    scores = {}
    for arm in ('reference', 'candidate'):
        for row in json.loads((work / f'{pair}-{arm}.json').read_text()):
            scores[(arm, row['params']['content'])] = float(row['primaryMetric']['score'])
    for content in ('ZERO', 'REPEATED'):
        ratios.setdefault(content, []).append(math.log(scores[('candidate', content)] / scores[('reference', content)]))
slower = False
for content, values in sorted(ratios.items()):
    point = math.expm1(sum(values) / len(values))
    print(f'{content}: candidate vs reference {100 * point:+.2f} % '
          f'(pairs {", ".join(f"{100 * math.expm1(v):+.1f}" for v in values)}; margin +{100 * margin:.0f} %)')
    slower |= point > margin
print('FAIL: the candidate is slower than the reference beyond the margin' if slower else 'PASS')
sys.exit(1 if slower else 0)
PY
