# Bug: timestamp allocation budget includes cold measurement-loop work

- Fix / issue: correctness-gate test finding T2 for PR #1352.
- Symptom: `ValidTimeTimestampAllocationBudgetTest` fails its exact retained-result
  equality, reporting 1,600,048 writer bytes versus 1,600,000 owned-result bytes.
- Trigger: the existing focused Gradle selector with `-XX:ActiveProcessorCount=2`,
  `-PtestHeapMin=256m` and `-PtestHeapMax=2g`. The failure reproduced before repair;
  a same-package runner of the exact test failed in 13 of 20 fresh constrained JVMs.
- Behaviour / invariants (specified before repair): canonical `parseNullable` into
  reusable storage allocates zero bytes; primitive writer mapping allocates exactly
  its escaping retained `Interval`. Keep the independently constructed escaping
  interval control, allocating `Instant.parse` control and timestamp-value assertions.
  Do not add tolerances or retry/discard failed measurement windows.
- Regression checks (derived before repair): reproduce the exact existing test,
  trace excess allocation, rerun it after repair, explore fresh JVMs with delayed
  compilation and inject an escaping temporary allocation into the measured mapping.
- Root cause: the mixed warmup loop exercises production operations but not the
  separate measurement loops. Runtime initialization in those cold loops contaminates
  the thread-allocation counter. With TLABs disabled, Flight Recorder captured an
  excess 56 bytes: a 24-byte `String` and its 32-byte backing array, attributed to the
  writer measurement loop backedge with no parser or writer frames. The product
  path's only observed allocation was its retained `Interval`. Disabling OSR alone
  did not solve this harness defect (17 of 20 fresh baseline JVMs still failed).
- Why tests missed it: one long mixed loop was assumed to warm every subsequent
  measurement call site. Passing executions depended on when VM setup occurred;
  the owned-result control could also include this transient work.
- Fix: move each measured operation loop into a helper. Warm those same helpers
  through 40,000 short calls covering all ten timestamp spellings before measuring
  40,000 operations. This warms normal entry and loop bodies without changing any
  byte bound, escaping sink, positive control or production implementation.
- Verification: the repaired test passes in 20 of 20 fresh constrained JVMs and
  another 20 of 20 with `-XX:CompileThresholdScaling=5`. Every parser window is zero;
  every writer and owned-result window is exactly 1,600,000 bytes. The focused
  Gradle selector also passes with the reported processor and heap limits.
- Mutation evidence: in a disposable test copy, add an escaping `new Object()` to
  each mapping operation. The unchanged equality assertion rejects it: 2,240,000
  writer bytes versus 1,600,000 owned-result bytes. No production logic changed;
  PIT does not mutate test warmup code.
- Reproduction: `JAVA_TOOL_OPTIONS=-XX:ActiveProcessorCount=2 ./gradlew --no-daemon
  --console=plain --max-workers=2 -Dorg.gradle.jvmargs=-Xmx768m
  -Dsirix.workBudget.print=true -PtestHeapMin=256m -PtestHeapMax=2g
  :sirix-core:test --rerun --tests io.sirix.budget.ValidTimeTimestampAllocationBudgetTest`.
  For repeated fresh JVMs, compile a same-package runner calling
  `new ValidTimeTimestampAllocationBudgetTest().warmedWriterParsingHasNoTemporaryAllocationWithAnAllocatingControl()`
  against the core test runtime classpath; use `--enable-preview`,
  `--enable-native-access=ALL-UNNAMED`, `-XX:ActiveProcessorCount=2`, `-Xms256m`,
  `-Xmx2g` and optionally `-XX:CompileThresholdScaling=5`.
- Lesson: warm the actual measured helper entry and loop body, rather than a
  separate mixed loop. See [Lessons](../LESSONS.md). No complete test suite,
  formatter, linter, static-analysis or pipeline-control command was run.
