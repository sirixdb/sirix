# Q12 membership evidence

See [the implementation note](../../../../../../docs/QUERY_MEMBERSHIP_OPTIMIZATION.md) for the
admission rule, semantic boundaries, measurement method, and results.

The **campaign artifacts** here — the `t100k-{before,after}` / `t250k-{before,after}` plan trees, the
`tiers` block of `measurements.json` and the suite counts in `validation.json` — were recorded on
commit `79042b96a` and describe that commit only. The plan trees named `after` therefore show the
superseded let-bound plan, not the current one.

Later rounds added artifacts that describe **later** states. Each measurement block in
`measurements.json` names the commit it was taken at in `measured_at_commit` and pins the source
hashes it was built from — **check that field rather than trusting a block's name.** The latest is
`remeasured_t100k_at_73d6e5daf`; `remeasured_t100k_final_head` (taken at `e24c6b854`, despite the
name) and `remeasured_t100k_after_memo_fix` are earlier re-measurements kept for history and marked
`superseded_by`. Commits landing after a block was taken are recorded in its
`changed_after_this_run`, which says what they touched and whether Q12's path changed; that is the
honest way to read these numbers, because the head moves and a block never does. The
`t100k-after-memofix*.plan.txt` trees show the nested-probe plan every later block ran, which is
still the current plan shape — only runtime behaviour changed after them. Changes made after the
campaign are listed in the addendum of the implementation note.

- `Q12Probe.java.txt`: exact runner used for the recorded SH1 measurements. It canonicalizes the
  answer and checks byte equality with the independent oracle before printing a timing.
- `BitemporalSchema.java.txt`: harness-only scale extension for the existing t50k and t250k inputs.
  This class precedes the production classpath; the production kit is unchanged.
- `t100k-{before,after}.plan.txt` / `t250k-{before,after}.plan.txt`: optimized Q12 plan trees from the
  `79042b96a` campaign runs. Superseded: they still bind the lookup to a `sirix:membership0` variable.
- `t100k-after-memofix*.plan.txt`: optimized Q12 plan trees for the re-measured later states, where the
  lookup is nested inside the probe and no membership variable exists.
- `ClickBenchPlanProbe.java.txt` / `clickbench-plans.json`: all 45 variants of the 43 ClickBench
  queries have identical optimized plan trees on frozen main and the candidate. This is a plan check,
  not an isolated ClickBench timing campaign.
- `measurements.json`: oracle-verified measurements, answer/input hashes, and candidate source hashes
  for the campaign, plus three later oracle-checked re-measurements, each labelled with the commit it
  was taken at — `remeasured_t100k_at_73d6e5daf` (latest), `remeasured_t100k_final_head` and
  `remeasured_t100k_after_memo_fix` (both superseded).
- `validation.json`: complete query-suite and work-budget counts plus the work-counter mutation.

## Reproduction

Baseline query classes were built from main `8aa9f0d9e` and copied aside before engine changes.
The same core classes, dependency jars, generated inputs, and persisted stores served both runs.
Use separate classpaths for the frozen baseline and candidate. The diagnostic system property
`-Dsirix.optimizer.hashMembership=false` also disables just this rule on the candidate.

From the worktree, obtain the runtime classpath with:

```sh
./gradlew --no-daemon --max-workers=2 :sirix-query:classes :sirix-query:printClickBenchRuntimeClasspath
```

Compile the two `.java.txt` SH1 sources as `Q12Probe.java` and `BitemporalSchema.java` in a scratch
source directory, using `javac --enable-preview --release 25 -cp "$runtime_cp" -d "$harness_classes"`.
Use `"$harness_classes:$runtime_cp"` for the runner classpath. Variables below denote caller-owned
scratch paths; `input_root` and `oracle_root` contain the immutable generated tier directories.

```sh
# Run once per tier, with the kit's natural publication batching (no batching override).
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
  -Xms512m -Xmx6g -XX:MaxDirectMemorySize=1g -cp "$runner_cp" \
  io.sirix.query.bench.bitemporal.BitemporalSirixLoadMain \
  "$tier" "$input_root/$tier/input/events.jsonl" "$store"

# A fresh process for each standalone measurement; pass baseline or candidate runner_cp.
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
  -Xms512m -Xmx6g -XX:MaxDirectMemorySize=1g -cp "$runner_cp" Q12Probe \
  "$store" "$output" "$oracle_root/$tier/oracle" 12

# The same runner checks all twelve answers; Q12 here is a warm-process observation.
java --enable-preview --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
  -Xms512m -Xmx6g -XX:MaxDirectMemorySize=1g -cp "$runner_cp" Q12Probe \
  "$store" "$all_output" "$oracle_root/$tier/oracle" 1 2 3 4 5 6 7 8 9 10 11 12
```

After a shared-machine global OOM during the first full-suite/t250k attempt, every new JVM over
2 GiB was admitted through Firstmate's machine-wide two-slot `flock` limiter. The interrupted
store was discarded as measurement evidence; the t250k retry uses a fresh store. Recorded successful
runs are single-process measurements, with concurrent work by other lanes; they support the
order-of-growth conclusion rather than small percentage comparisons.

Validation commands (each admitted through that same limiter):

```sh
./gradlew --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx2g \
  -PtestHeapMin=512m -PtestHeapMax=4g :sirix-query:spotlessCheck :sirix-query:test
./gradlew --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx2g \
  -PtestHeapMin=512m -PtestHeapMax=4g :sirix-core:test \
  --tests 'io.sirix.budget.*' --tests 'io.sirix.index.projection.BatchedSegmentReadWorkBudgetTest'
```
