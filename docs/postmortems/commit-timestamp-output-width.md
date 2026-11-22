# Bug: commit timestamp output drops fractional zeros

- Fix / issue: test finding T1 on `fm/sirix-commit-ts-micros`; VER-201.
- Symptom: whole-second and millisecond-aligned commits serialize as `2021-03-04T05:06:07Z`
  and `2021-03-04T05:06:07.123Z` instead of the required `.000000Z` and `.123000Z` forms.
- Trigger: commit those instants and request timestamp metadata through JSON/XML serializers,
  `sdb:timestamp`, MCP resource information/history, or REST history.
- Root cause: these boundaries used `ISO_INSTANT` or `Instant.toString()`, whose fractional
  width varies. Microsecond storage preserves the instant but does not determine its text width.
  VER-201 and the newly added fixtures incorrectly specified the same variable-width behavior,
  despite the author's explicit six-digit requirement. The output boundaries now share an immutable
  six-digit UTC formatter in `CommitTimestamps`; numeric storage and lookup are unchanged.
  Brackit additionally normalizes `xs:dateTime` text, so `sdb:timestamp` returns a dateTime
  value retaining the formatted text while inheriting the existing value semantics.
- Why tests missed it: output fixtures expected the implementation's variable-width strings.
  A six-digit input alone cannot distinguish fixed-width output from `ISO_INSTANT`.
- Regression tests: the four modules' `CommitTimestampOutputTest` classes now assert independent,
  literal six-digit expected strings for whole-second, millisecond and microsecond values and
  parse the emitted values back to the committed instants. Core exercises all three JSON
  serializers and both XML wrappers; query exercises JSONiq and XQuery; MCP exercises one-revision
  and multi-revision metadata and history; REST drives a real localhost HTTP listener for JSON
  and XML and closes it in `finally`. The corrected tests failed before the production fix
  (Gradle exit 1, seven failing cases across four modules). `CommitTimestampsTest` also checks
  the formatter directly against the VER-201 examples and a pre-epoch instant. Query assertions
  also verify the result remains an `xs:dateTime` equal to the committed instant. The first
  post-fix run passed core, MCP and HTTP output but caught Brackit's second normalization step
  (exit 1, two query cases); preserving the dateTime lexical representation addresses that layer.
  Mutation associations select the public output regressions for each changed boundary.
  Final focused results: all 22 selected cases passed (15 core, 3 query, 3 MCP, 1 HTTP);
  the multi-module run passed core/MCP/HTTP, and the query-only rerun passed after the
  dateTime fix. Both completed processes' actual exit statuses were checked.
- Mutation verification: focused PIT runs completed with exit 0 and no survivors or uncovered
  mutants: `CommitTimestamps.format` (1/1 killed) and `GetRevisionTimestamp` plus its
  `CommitDateTime` value (2/2 killed). The existing numeric conversion methods were excluded
  from the local formatter run; this is not a whole-module mutation result. The public-output
  associations retain the broader changed-line mutation check for the outer executor.
- Lesson: [Verify timestamp text independently of timestamp precision](../LESSONS.md#commit-timestamp-text).

Focused reproduction and re-verification use the same output-test selectors:

```sh
./gradlew :sirix-core:test --tests io.sirix.io.CommitTimestampOutputTest
./gradlew :sirix-query:test --tests io.sirix.query.function.sdb.trx.CommitTimestampOutputTest
./gradlew :sirix-mcp:test --tests io.sirix.mcp.CommitTimestampOutputTest
./gradlew :sirix-rest-api:offlineTest --tests io.sirix.rest.CommitTimestampOutputTest
```

The local runs use a disposable Gradle init script to skip unrelated attached test tasks and
keep test temporary data in the worktree. No linter, formatter or static analysis is run.
The reported IN_MEMORY visibility failure is separate: supplied base and target SDK transcripts
both show committed head 0 and rejection of revision 1. This repair does not change that existing
storage behavior or claim that scenario passes.
