# Bug: the shell left its prompt after a query failure, and failed updates leaked

- Fix / issue: [#1430](https://github.com/sirixdb/sirix/issues/1430); branch `ccr-3e4562a1-i50wpv`.
- Symptom: after some query errors `sirix-shell` did not return to its prompt. Investigating the
  shared shell context showed two worse effects of the same missing failure boundary:
  - a failed query's pending updates were committed by the **next** updating query;
  - a failed apply left its writer on the resource session. A JSON writer carried the failed
    query's partial changes into the next commit (`{"a":1,"arr":[1,2],"z":1,"c":3}` instead of
    `{"a":1,"arr":[1,2],"c":3}`). An XML writer stayed rollback-only, so every later update of that
    resource failed with "Transaction is rollback-only after a failed atomic operation". This
    affected every caller of `SirixQueryContext`, not only the shell.
- Trigger:
  1. Shell exit: `jn:doc('db','missing')` at the prompt. `LocalDatabase` throws
     `SirixUsageException`, which is not a `QueryException`; the loop caught only
     `QueryException`, so the exception left the loop, closed both stores and ended the process
     with status -1. With a real terminal the user saw no prompt while the stores closed.
     The `jn:first`/`jn:previous` `ClassCastException` named in the issue had already become the
     coded `SIRIXDBF0005` error.
  2. Leaked pending updates: `for $i in (1,0) return insert json {'y': 1 idiv $i} into $doc`, then
     any updating query on the same session.
  3. Leaked writer: `(insert json {'z':1} into $$, insert json 9 into $$.arr at position 99)`, or the
     XML conflict `(insert node <c/> into $$/a, rename node $$/a as 'b', rename node $$/a as 'd')`.
- Root cause: the shell reuses one `QueryContext` for the whole session, but treated failure as a
  property of one exception type rather than of the query. Nothing ended a failed query's scope: its
  update list stayed in the context, and `SirixQueryContext.applyUpdates` had no failure path for
  the writers it opened. The XML path marked them rollback-only and left them open; the JSON path
  did not handle failure at all. JSON targets reuse any writer already on the session
  (`getNodeTrx().orElseGet(beginNodeTrx)`), so the next query silently adopted the leftover writer.
- Why tests missed it: shell tests drove only `QueryException`s (syntax errors) through the
  interactive loop, and each query ran on a fresh context or never failed after queueing an update.
  Writer-lifecycle tests covered successful applies under both commit strategies, not a failure
  between the first applied operation and commit.
- Regression tests:
  - `MainTest.interactiveShellReturnsToThePromptAfterAnyQueryFailureWithoutApplyingItsUpdates`
    drives a store failure and a dynamic error, each after a queued insert, then a normal update.
    It asserts status 0, one prompt per query, coded one-line reports (`bit:BIDY0300` for the
    store failure), `-d` stack logging and the final document `{"a":1,"c":3}`. Before the fix it
    failed with status -1.
  - `SirixQueryContextFailedApplyTest` (JSON and XML, `AUTO` and `EXPLICIT`) asserts that no writer
    remains after a failed apply and that the next update commits without the failed query's
    changes. Before the fix all four cases failed at the "writer must be released" assertion; with
    that assertion removed they failed with the leaked `"z":1` (JSON) and the rollback-only refusal
    (XML). Control: `failedJsonApplyLeavesACallerHeldWriterOpen` keeps a writer the caller held
    before the query open.
  - Command: `./gradlew :sirix-query:test --tests io.sirix.query.MainTest --tests io.sirix.query.SirixQueryContextFailedApplyTest`.
- Lesson: when a context outlives one unit of work, give each unit an explicit failure boundary that
  discards its pending state and releases the resources it opened, and test failures that happen
  after side effects have started. See [Lessons](../LESSONS.md).

Follow-ups found here and fixed in the same pull request: `-iqf` could not run any query
([write-up](shell-query-script-mode.md), #1470), and `xn:store` ignored its resource name when
creating a database ([write-up](xml-store-resource-name.md), #1471). A failure while committing
(rather than applying) keeps its existing semantics, tracked in #1380.

## Review follow-up: empty interactive submissions

- Invariant (R1): an empty submission preserves the session in explicit `-iq` and default
  terminal mode; only EOF or interrupt terminates it.
- Trigger: press Enter at an empty prompt, then submit `1+1`, then EOF.
- Root cause: the shared reader returned `null` for both an empty submission and EOF, and both
  interactive entry paths interpreted it as termination.
- Why tests missed it: tests terminated nonempty queries with blank lines but never started
  a session with one.
- Regression: `MainTest.emptySubmissionsKeepBothInteractiveModesAliveUntilEof` checks result `2`,
  no error and EOF consumption through both command dispatches. Returning before executing
  the query violates its R1 oracle. Accumulated input still executes at EOF, and interrupt
  still discards accumulated input and ends the session.
- Lesson: distinguish an empty command from end of input at their shared reader boundary;
  see [Lessons](../LESSONS.md).

## Review follow-up: multiline error messages

- Invariant (R2): every coded query failure occupies exactly one report line in plain or colored
  output, while the exception, its cause and their original messages remain available to `-d`.
  This applies to mapped runtime failures and QueryExceptions in explicit/default interactive
  mode and file/stdin scripts, and to batch QueryExceptions sharing the same renderer.
- Trigger: store `jn:store('db','r','{}')`, then evaluate
  `jn:doc('db',concat('bad/',codepoints-to-string(10),'name'))`. Resource-name validation includes
  the supplied LF in its exception message, splitting the rendered report across two lines.
- Root cause: exception mapping stripped only outer whitespace, while both presentation branches
  printed embedded terminators verbatim. Changing the mapped exception would miss QueryException
  passthrough and lose diagnostic fidelity; the report renderer owns the one-line contract.
- Why tests missed it: diagnostic assertions used single-line exception messages or counted only
  lines beginning with `Error:`, which cannot detect an extra unprefixed continuation line.
- Fix: escape CR, LF, vertical tab, form feed, NEL, line separator and paragraph separator in the
  assembled report, retaining each exception and cause unchanged for logging.
- Regressions: `ShellOutputTest.errorReportsEscapeEveryLineTerminatorWithoutChangingTheException`
  uses literal expected escape sequences under every palette and asserts unchanged exception
  messages, descriptions, codes and cause identity. `MainTest.multilineFailuresHaveOneReportLineAndKeepDebugDiagnostics`
  exercises the reported resource-name failure and an explicit query error through both interactive
  dispatches and both script inputs, with debug logging on/off and a successful query afterward.
  R2 supplies the one-line oracle; an unescaped terminator or a modified debug message violates it.
- Lesson: enforce a report's framing at its shared presentation boundary rather than modifying
  diagnostic objects; see [Lessons](../LESSONS.md).
