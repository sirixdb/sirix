# Bug: failed commit timestamp survives a retry

- Fix / issue: review R1 on `fm/sirix-commit-ts-micros`; TXN-310 and VER-201.
- Symptom: a retry supplying timestamp B could successfully publish timestamp A from a failed attempt.
- Trigger: commit A, fail during phase one before publication, then retry the same writer with B.
- Root cause: the revision root used its timestamp field first as the committed predecessor and then as the assigned attempt value. Its assignment flag outlived the failed attempt, so replacing the supplied instant did not recompute the effective value. Clearing only the flag would incorrectly promote failed A to the predecessor. The predecessor now has its own field; the shared storage commit boundary reassigns for each attempt, including retries with no custom instant. Serialization still uses the assigned value repeatedly within the attempt.
- Why tests missed it: successful timestamp round trips and repeated-serialization tests covered separate operations, but no test combined a phase-one failure with a replacement timestamp. Output evidence formatted an Instant directly rather than executing the serializers or handlers.
- Regression test: `CommitTimestampPrecisionConformanceTest.retryUsesItsTimestampAfterSyncOrAsyncPhaseOneFailure` injects a failure after assignment through the encode-pass listener, then verifies both later and earlier retry inputs through public reads and cold reopen. `retryAfterAutomaticAsyncPhaseOneFailureUsesExplicitTimestamp` exercises the asynchronous transaction recovery path. `PageEnvelopeContractTest` and `LegacyEnvelopeImagesTest` retain serialization idempotence evidence. Focused verification passed all 66 selected cases (59 core, 3 query, 3 MCP, 1 REST); no pre-fix run was made because this fix phase requires all edits before verification. Initial verification caught test setup/compilation errors (a missing annotation import, the REST XML sequence wrapper, and a query writer overload); these were corrected before the passing runs.
- Lesson: [Distinguish attempt state from committed state](../LESSONS.md#commit-attempt-state).

Focused commands (the review invocation disabled unrelated attached test tasks):

```sh
./gradlew :sirix-core:test --tests io.sirix.io.CommitTimestampPrecisionConformanceTest --tests io.sirix.io.CommitTimestampOutputTest --tests io.sirix.page.PageEnvelopeContractTest --tests io.sirix.page.LegacyEnvelopeImagesTest
./gradlew :sirix-query:test --tests io.sirix.query.function.sdb.trx.CommitTimestampOutputTest
./gradlew :sirix-mcp:test --tests io.sirix.mcp.CommitTimestampOutputTest
./gradlew :sirix-rest-api:offlineTest --tests io.sirix.rest.CommitTimestampOutputTest
```

## Review follow-up: late failure retained an index entry

- Fix / issue: review R3, TXN-310 and VER-201/202/205.
- Symptom: after a final phase-one flush failed, retrying logical revision 2 at B left A in warm history and could resolve B to nonexistent revision 3. A retry earlier than A failed the monotonicity guard instead.
- Trigger: commit revision 1, write revision 2's root and revision-directory record at A, fail the final buffered flush, then retry revision 2 at B before closing the session.
- Root cause: the backend appends its warm index entry during the root write, before the final phase-one flush. Timestamp reassignment did not discard this abandoned index tail. The first root write also resolves its transaction-log reference, so a retry must register the root again rather than skip it as already written. The shared attempt boundary now retains only index slots below the attempted revision and re-registers a completed root reference before writing it again. Normal commits do not copy; retries use the existing detached-prefix operation so appending the replacement cannot overwrite arrays held by readers of an older snapshot. File-channel and memory-mapped storage share this boundary and the same backend writer. Rewriting the root replaces the deterministic persisted revision slot and its cached locator. The initial focused run exposed that trimming alone omitted this root rewrite; its exact-lookup assertions failed across every late-failure case.
- Why tests missed it: the original fault ran before the root was written and checked timestamp history only after cold reconstruction, which hides the warm-index discrepancy. Neither a late failure nor warm exact-timestamp lookup was exercised.
- Regression test: `CommitTimestampLateFailureTest` injects an I/O-boundary failure in the final phase-one flush, then checks warm and cold history, exact and bounded lookups, the published head and read-back. Cases include later and earlier retry timestamps, both persistent backends, synchronous and asynchronous phase-one calls, a larger index crossing the Eytzinger boundary, and automatic async transaction recovery on its supported file-channel backend. The original conformance test now also checks warm history and exact lookup.
- Verification: all 124 focused cases passed, including 13 late-failure cases, 15 timestamp conformance cases, 52 revision-index cases, 2 serializer-output cases and 42 envelope/idempotence cases. The phase requires all fixes before verification, so no pre-fix run is claimed. The review invocation skipped unrelated attached test tasks.
- Lesson: extend the attempt-state lesson to every metadata publication boundary and assert warm state before rebuilding caches.

```sh
./gradlew :sirix-core:test --tests io.sirix.io.CommitTimestampLateFailureTest --tests io.sirix.io.CommitTimestampPrecisionConformanceTest --tests io.sirix.io.RevisionIndexTest --tests io.sirix.io.CommitTimestampOutputTest --tests io.sirix.page.PageEnvelopeContractTest --tests io.sirix.page.LegacyEnvelopeImagesTest
```

## Review follow-up: derived commits forwarded effective source timestamps

- Fix / issue: review R4 and public lookup Javadoc correction R5; TXN-310/612 and VER-201/202/203/205.
- Symptom: a source commit adjusted to one microsecond beyond its predecessor after a backward clock step could permanently poison derived maintenance for the session.
- Trigger: commit a source revision at T, regress the clock, commit again at the earlier instant, and maintain the revision whose effective timestamp is T + 1 microsecond.
- Root cause: batch publication, aggregate folding and both summary-fold branches supplied an effective timestamp as custom commit input, which correctly rejects future instants. Effective source metadata and the derived timeline's own assignment serve different purposes. All four commits now use ordinary derived clock/predecessor assignment; `DerivedRevisionMapping` retains and validates the exact source effective timestamp unchanged. The public future-input guard is unchanged.
- Why tests missed it: explicit historical timestamps stayed behind the real clock and no test moved the clock backward while maintaining or folding. Public lookup Javadocs also retained obsolete millisecond and tie descriptions, including the numeric history accessor's summary.
- Regression tests: `DerivedIndexMaintainerTest.backwardsClockKeepsDerivedMappingsValidAndMaintenanceUsable` uses a thread-scoped fixed clock and paused manual maintenance for both persistent source backends. It checks T + 1 microsecond ahead of that clock, aggregate folding, mapping validity, continued advancement, future custom input rejection and cold read-back. `summaryFoldsUseTheirOwnTimelineAfterClockRegression` checks summary publications with and without a base mapping, their own timeline increments, and subsequent catch-up without poisoning. Their oracles are TXN-310/612 and fixed microsecond examples; forwarding effective timestamps into custom-input validation is the wrong behavior they reject.
- Verification: all 85 focused cases passed: 4 controlled-clock cases, 8 ordinary derived-maintenance cases, 1 mapping case, 15 timestamp conformance cases, 13 late-retry cases, 2 output cases and 42 envelope/idempotence cases. Unrelated attached test tasks were skipped. No pre-fix execution is claimed because this phase requires all fixes before verification. The initial run passed publication and summary-fold behavior but exposed an undersized aggregate-fold fixture: one update need not trigger folding. The fixture now exercises sustained updates and asserts an observed independent fold commit.
- Lesson: distinguish another timeline's effective metadata from validated custom input; test clock regression through the dependent commits, not only the source assignment.

```sh
./gradlew :sirix-core:test --tests 'io.sirix.index.aggregate.DerivedIndexMaintainerTest.*Clock*' --tests 'io.sirix.index.aggregate.DerivedIndexMaintainerTest.smallPublicationsAdvanceTheBaseWithoutRedundantSummaryCommits' --tests 'io.sirix.index.aggregate.DerivedIndexMaintainerTest.durableSnapshotsLagRevertAndReopen' --tests io.sirix.index.aggregate.DerivedRevisionMappingTest --tests io.sirix.io.CommitTimestampPrecisionConformanceTest --tests io.sirix.io.CommitTimestampLateFailureTest --tests io.sirix.io.CommitTimestampOutputTest --tests io.sirix.page.PageEnvelopeContractTest --tests io.sirix.page.LegacyEnvelopeImagesTest
```

## Rebase follow-up: RAM storage contract fixtures

The test-phase build after the RAM storage merge failed with seven unresolved calls to the old
revision-root timestamp getter/setter in `RAMStorageContractTest`. The API rename intentionally
breaks callers, but these newly merged tests had not been migrated. Existing timestamp checks had
run before that integration, so they could not catch the merged compilation failure.

Migrate the test fixtures to epoch microseconds, use `getRevisionTimestampMicros()` and assign a
new timestamp attempt when testing writer-page ownership. Keep the independent `Instant` oracle
in nanoseconds and preserve all storage assertions. The focused regression is
`RAMStorageContractTest`; compilation of the merged test sources is part of the verification.

Verification: the initial targeted build failed in `compileTestJava` on those seven removed
methods; after fixture migration, `./gradlew --no-daemon :sirix-core:test --tests
'io.sirix.io.ram.RAMStorageContractTest' -PtestHeapMin=256m -PtestHeapMax=2g` passed as part
of the focused timestamp test selection. The test continues to check stored ownership, historical
reads and publication; it now also exercises distinct microseconds within one millisecond.
