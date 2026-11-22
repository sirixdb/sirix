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

## CI follow-up: XML apply complexity

- Symptom: CodeFactor rejected PR #1472 with one `Complex Method` issue on
  `SirixQueryContext.applyXmlUpdates` (reported complexity 17).
- Root cause: writer discovery, acquisition, target binding and failure cleanup accumulated
  in the same orchestration method. Runtime regression tests do not measure method complexity.
- Invariant: extraction preserves R4 and its caller-ownership control, including XML resource
  aliases, current/historical snapshots, AUTO/EXPLICIT commits, mixed JSON/XML updates, success,
  rollback-only marking, reverse atomic-operation completion and suppressed release failures.
- Fix: extract the three complete loops and the entire finally cleanup into private static
  helpers. Keep all allocations, loop bodies, ordering and the catch block unchanged; no helper
  call is added per update operation. On failure, mark writers rollback-only, restore the original
  operations, end atomic operations in reverse order, and release only writers this apply opened.
  On success, return the same writers to the existing AUTO/EXPLICIT commit path.
- Regression oracles: existing `SirixQueryContextFailedApplyTest` checks R4's persisted result,
  writer ownership and suppressed failures. `XmlPendingUpdateRegressionTest` and
  `XmlAliasIdentityRegressionTest` check snapshots, alias sharing, caller-supplied writers and
  successful updates. The documented work-budget suites guard counted work and allocation.
  No behavior changes or source-text tests are needed for this maintainability correction;
  the external CodeFactor rerun must confirm the analyzer result after publication.
- Local verification: `:sirix-query:spotlessJavaCheck` and the three named regression suites
  passed, as did all three *Work budgets* commands in `docs/VERIFICATION.md`, including their
  isolated allocation checks. Runs used `-PtestHeapMin=512m -PtestHeapMax=3g`.
- Lesson: split lifecycle orchestration at whole-loop boundaries so method complexity can fall
  without adding per-operation dispatch; see [Lessons](../LESSONS.md).

## CI follow-up: XML snapshot and writer-ownership coverage

- Invariant: an apply-created XML writer starts from the target snapshot, whereas a caller-held
  writer retains its current edits. Retained committed snapshots remain immutable (TXN-201,
  TXN-204). This applies to current and historical targets under AUTO and EXPLICIT commits;
  R4 separately requires failed applies to release only the writers they opened.
- Behavioral oracle: start with revision 1 `<a><b/></a>` and revision 2
  `<a><b/><head/></a>`, then insert `<e/>`. A newly opened writer targeting revision 1 must
  publish the original tree plus `e`, without `head`; targeting revision 2 must retain `head`.
  A caller-held writer with an additional `pending` element must retain both `head` and `pending`
  for either target revision. Both original snapshots must remain unchanged. These literal trees,
  rather than a copy of the writer-selection logic, define the expected results.
- Root cause: the extracted historical-revision branch was selected by PIT, but the mapped
  failed-apply tests only used newly created resources at their latest revision. Existing XML
  alias/pending-update tests also did not distinguish an older update target from the latest head.
  Negating the revision comparison therefore survived the CI sample.
- Regression: `SirixQueryContextFailedApplyTest.xmlUpdatesPreserveSnapshotAndWriterOwnership`
  exercises all eight snapshot/ownership/commit combinations through executable queries and
  checks committed trees, writer lifecycle, and historical immutability. A wrong snapshot base
  or loss of caller edits violates the independently specified trees above.
- Verification: `./gradlew :sirix-query:test --tests io.sirix.query.SirixQueryContextFailedApplyTest
  -PtestHeapMin=512m -PtestHeapMax=3g` passed all 14 cases. Selection against base
  `5b29020fb5114399cf66f0a82ec4136b9139d2fa` and head
  `814d73063fbed3ad06a0a2bf2bed85f4a5619930`, followed by
  `python3 .github/scripts/pr_mutation.py run --output build/ci-1472/local-mutation
  --seconds 1320 --blocking`, passed: 23/23 changed-line mutants killed, no changed-line
  survivors or uncovered mutants, and no failing dispositions. In the downloaded CI report,
  `beginXmlWriters` / `NegateConditionalsMutator` / instruction index 114 survived; in the local
  report the same identity is killed by the new historical-snapshot case. Production sources
  are unchanged. Full local reports remain under `build/ci-1472/`.
- External failure: the CI merge checkout also contained two backup-CLI dispositions absent
  from this PR's head and base. They were judged as new against the event's older base and failed
  confirmation because only query sources were selected. The job checked out merge
  `63c9b4f812b607c724494c86468a525e01a8a9fb` with main parent
  `58ec1a784abfe54a9130ea287ebc2631faec6b7e`, while `BASE_SHA` still named `5b29020...`.
  No backup or disposition changes belong to this correction; a fresh outer-executor run must
  evaluate its actual merge/base pair.

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

## Review follow-up: retained JSON target memoization

- Invariant (R4): after rollback of an apply-owned JSON writer, every affected retained object
  reads its original committed snapshot rather than values from the failed apply. Insert,
  replace and rename must obey this in JSON-only and mixed JSON/XML application, under AUTO
  and EXPLICIT. Caller-owned writers and their pending state remain under caller control.
- Trigger: retain the item for `{"a":1,"arr":[1,2]}`, run `FAILING_JSON_UPDATE`, clear the
  pending update list, and execute `insert json {'c': if (exists($$.z)) then 9 else 3} into $$`
  through the same item. Before recovery invalidated its memo, it committed `c=9` instead of `c=3`.
- Root cause: JSON object insert, replace and rename update the target's field memo after changing
  the writer. Writer rollback restored storage but never invalidated that separate retained state;
  `get` returned the failed value without consulting the restored snapshot. Field values, cached
  first-child position and child count all belong to the existing object memo reset.
- Why tests missed it: the recovery test replaced the context item with a fresh snapshot before
  the next query, bypassing the object whose memo was populated by the failed update.
- Fix: at the shared application failure boundary, reset object memos for operation targets whose
  sessions had no writer before the apply, after the existing writer cleanup. The normal read path,
  successful applications, caller-owned writers and the XML application loops are unchanged.
- Regressions: `SirixQueryContextFailedApplyTest.failedJsonApplyReleasesTheWriterItOpenedWithoutItsChanges`
  now retains the context item across the failing and successful queries. Its insert, replace and
  rename cases cover both application branches and commit strategies. Replacement and rename use
  an invalid array deletion as a later failing operation, so update ordering cannot skip the mutation
  under test. The R4 oracle is committed `c=3` and absence of the failed changes, not cache contents.
  The caller-held JSON writer control also retains its original item and pending inserted value;
  the eight XML snapshot/writer-ownership cases remain intact.
- Lesson: rollback must reset retained query-item state as well as transactional storage; test
  recovery through the exact objects the caller keeps. See [Lessons](../LESSONS.md).
