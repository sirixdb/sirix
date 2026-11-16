# Bug: XML word-buffer regression was outside the PR mutation scope

- Fix / issue: PR #1357, PIT job `133539093835` in run `43934597178`.
- Symptom: changed-source PIT reported a surviving conditional-boundary mutant in
  `ShellResultWriter.accept`, despite a focused review run killing that mutant.
- Trigger: CI resolves the writer to the exact `ShellResultWriterTest` name and
  does not include the separate `ShellXmlDiffColourTest` integration suite.
- Root cause: the long-word regression lived only in the integration suite. The
  focused review explicitly included it, hiding the default selection gap.
- Invariant / specification: buffered XML words must preserve every character and
  restore the next operation's colour through every writer overload, chunk size,
  flush boundary and finish/reuse transition. The automatically selected writer
  tests must reject an off-by-one buffer boundary. Operation and operand styles
  follow the [shell presentation contract](../shell-presentation.md).
- Regression test: move the three renderer-only XML tests into
  `ShellResultWriterTest`; leave real database integration in
  `ShellXmlDiffColourTest`. Exercise String, char-array and single-character
  writes, chunks 1 through 20, interleaved flushes and finish/reuse. The independent
  oracle is the input payload plus the presentation contract's red delete verb;
  accepting a seventeenth character into the sixteen-character scratch buffer
  must fail rather than survive mutation testing.
- Sibling sites: unsampled writer PIT with only the exact-name test initially
  exposed two further survivors, at the XML number item boundary and the
  constructor-depth check for operation words (`acceptValue`, lines 143 and 158).
  Their existing tests had the same placement problem. Moving both tests closes
  the shared scope gap without changing production or discovery logic.
- Why tests missed it: normal tests included both classes; focused mutation testing
  used a broader test list than CI's documented exact-name-first selection.
- Lesson: place renderer unit regressions in its exact-name test class, and verify
  mutation evidence with CI's automatic selection. See [Lessons](../LESSONS.md).

## Verification

- The downloaded original CI report records the buffer-boundary survivor; the
  prior focused report names the regression in `ShellXmlDiffColourTest` as its
  killer. This reproduces the selection discrepancy without changing production.
- Repeating the original CI target list, six test patterns and per-class sample
  limit of 23 after moving the buffer regression detects all 107 changed-line
  mutants (106 killed, one timed out), with zero surviving or uncovered. The
  original boundary mutant is killed by `ShellResultWriterTest`.
- Unsampled writer PIT with only `ShellResultWriterTest*` initially killed
  168/170 mutants and exposed the two sibling scope gaps above. After moving the
  remaining renderer-only tests, the same command kills **170/170** mutants,
  with zero surviving, uncovered or abnormal outcomes. The number-boundary and
  constructor-depth mutants are killed by the relocated position tests.
- The final targeted test run passes **19 tests**: 18 in
  `ShellResultWriterTest` and one real-diff integration test in
  `ShellXmlDiffColourTest`. This includes the unchanged allocation budget.
- `git diff --check` passes. Production code, discovery rules and work budgets
  are unchanged; no query/read/load path was modified.

Commands used JDK 25, `--no-daemon --max-workers=2` and
`-Dorg.gradle.jvmargs=-Xmx2g`. The targeted test task also used
`-PtestHeapMin=256m -PtestHeapMax=1g`:

```sh
./gradlew :sirix-query:test \
  --tests io.sirix.query.shell.ShellResultWriterTest \
  --tests io.sirix.query.shell.ShellXmlDiffColourTest
./gradlew :sirix-query:pitest -PprMutationModule=sirix-query \
  -PprMutationTargetsFile=build/review-xml-operations/targets.txt \
  '-PprMutationTests=io.sirix.query.shell.ShellResultWriterTest*' \
  -PprMutationReportDir=build/ci-pit-writer-final
```

The writer targets file contains only `io.sirix.query.shell.ShellResultWriter`.
Local evidence remains under this worktree:

- `build/ci-pit-original/summary.md` and `sirix-query/report/mutations.xml`.
- `build/ci-pit-fixed.log`, `build/ci-pit-fixed/verdict.json` and its report.
- `build/ci-pit-writer-full/mutations.xml` (the two sibling survivors).
- `build/ci-pit-writer-final.log` and `build/ci-pit-writer-final/mutations.xml`.
- The two `TEST-io.sirix.query.shell.*.xml` files in
  `bundles/sirix-query/build/test-results/test/`.
