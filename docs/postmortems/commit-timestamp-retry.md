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
