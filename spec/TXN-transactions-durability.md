# TXN — Transactions, durability and recovery

Spec target: **1.0.0**. Chapter status: **Draft for adoption before the approximately
9 December 2026 freeze**. Code baseline: `13f65148b117b9c8e1858a7f1510d2f65501bad9` (17 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance.
Existing test links are evidence inventories, not assertions that tests passed in this task.

## Scope

Node and page writers, persistent FILE_CHANNEL/MEMORY_MAPPED resources, IN_MEMORY exceptions, recovery, async modes, lifecycle hooks and backup consistency. Existing sample IDs TXN-101 through TXN-705 retain their subject; new decisions extend the chapter.

## Requirements

### TXN-101 — Process ownership

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [DatabaseLock.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseLock.java); [DatabaseHandle.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseHandle.java):`close`
- **Verified by:** [DatabaseOwnershipTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseOwnershipTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

Only one operating-system process MUST own a database at a time. A second process open MUST fail with `SirixDatabaseLockException`; process exit or crash MUST release the operating-system lock. Multiple handles inside the owner process MUST share ownership safely.

### TXN-102 — Exclusive and bounded writer admission

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [WriteLocksRegistry.java](../bundles/sirix-core/src/main/java/io/sirix/access/WriteLocksRegistry.java); [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`beginNodeTrx`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`createStorageEngineWriter`
- **Verified by:** [ResourceSessionTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/ResourceSessionTest.java); [AbstractResourceSessionWriteFailureCleanupTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AbstractResourceSessionWriteFailureCleanupTest.java) **Planned:** WriterAdmissionConformance: same resource through two handles, independent resource progress, timeout and interruption on both admission paths.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Current timeouts are 5 s for node writers and 20 s for independent page writers. Unifying/configuring them is deferred API work; this chapter does not silently change the existing bounds.

At most one node-bound or independent page writer MUST hold the resource writer permit. Admission MUST be bounded and fail explicitly with `SirixUsageException` if the permit cannot be acquired. Failure during admission MUST leave the permit reusable.

### TXN-103 — Recovery before writer construction

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`createPageTransaction`
- **Verified by:** [CrashRecoveryOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryOrderingTest.java):`recoveryTruncatesBeforeTheRecoveringTransactionReadsAnything`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

Required recovery MUST finish before constructing a new writer or reading resource pages through its caches. Recovery failure MUST prevent that writer from becoming observable.

### TXN-104 — Async mode admission

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`beginNodeTrx`
- **Verified by:** [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java); [AsyncAutoCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncAutoCommitTest.java) **Planned:** AsyncAdmissionConformance: every storage/mode/timer combination, assert unchanged head and reusable permit on rejection.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

KEEP_OPEN_ASYNC_COMMIT MUST be accepted only for FILE_CHANNEL; KEEP_OPEN_ASYNC_FLUSH only for FILE_CHANNEL or MEMORY_MAPPED. Either async mode with timed auto-commit MUST fail with `IllegalArgumentException` before acquiring ownership or mutating data.

### TXN-201 — Snapshot reads

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`beginNodeReadOnlyTrx`
- **Verified by:** [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java):`testSnapshotIsolation`; [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java):`testWriterModifiesReaderUnaffected`; [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java):`concurrentReader_survivesWriterCrashAndRecovery`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

A read-only transaction MUST remain bound to its selected committed revision for its lifetime. Later commits, rollback, revert or recovery MUST NOT change its visible node values or identity. Explicit truncate MUST reject removal of a revision held by an open reader under TXN-404. General automatic pruning remains outside this P0 contract.

### TXN-202 — Private uncommitted changes

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`beginNodeReadOnlyTrx`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`
- **Verified by:** [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java):`testReadOnlyDuringWrite`; [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java):`testReadBeforeCommit`; [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java):`testReadAfterCommit`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

Uncommitted changes MUST be visible only through their owning writer. A new ordinary reader MUST observe the latest published committed revision, not the pending writer lineage.

### TXN-203 — Durable before visible

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`hardenAndPublish`
- **Verified by:** [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java):`asyncCommit_createsQueryableRevisions`; [AsyncCommitFrontierOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AsyncCommitFrontierOrderingTest.java) **Planned:** PublicationBarrierConformance: pause before record durability, repeatedly open readers, cross the commit point, inject cleanup failure and verify exactly the old then new committed view.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current publication follows hardenCommit returning after cleanup; it does not isolate a successful durable-record outcome from cleanup failure.

A pending revision MUST NOT become the latest revision for unrelated readers before the revision-record commit point in TXN-302. Phase-one bytes accessible to a pipelined successor MUST NOT be exposed as committed data to unrelated readers. Once N is known committed, post-point cleanup failure MUST NOT hide or discard N.

### TXN-204 — Immutable historical content

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [VersioningType.java](../bundles/sirix-core/src/main/java/io/sirix/settings/VersioningType.java); [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`revertTo`
- **Verified by:** [JsonRoundTripPropertyTest.java](../bundles/sirix-core/src/test/java/io/sirix/property/JsonRoundTripPropertyTest.java):`committingNewRevisionsNeverAltersEarlierRevisions`; [JsonModelBasedOracleTest.java](../bundles/sirix-core/src/test/java/io/sirix/property/JsonModelBasedOracleTest.java):`fixedSeedOperationSequenceMatchesOracle`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

Committing, rollback, revert and recovery MUST leave every retained reachable historical revision unchanged. Removal of reachability by explicit truncate or retention MUST NOT alter another revision to simulate the removed one.

### TXN-301 — Revision allocation

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [NodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeTrx.java):`commit`; [UberPage.java](../bundles/sirix-core/src/main/java/io/sirix/page/UberPage.java):`getRevisionNumber`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`
- **Verified by:** [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`failedCommitKeepsTransactionUsableAndGuardsClose`; [MultipleCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/MultipleCommitTest.java) **Planned:** RevisionOutcomeAllocationConformance: empty commits, known pre-point failure plus rollback/retry, known durable cleanup failure and unknown outcome resolved both present and absent; assert revision numbers and no duplicate retry.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Captain decision 17 November 2026 replaces the prior durable-failure exception design. Current success/exception paths do not consistently distinguish the three outcomes.

Bootstrap MUST be revision 0. A successful commit, including an empty commit, MUST append the next revision number N. A failure known to precede the commit point MUST mean nothing committed and MUST NOT consume a committed revision number; rollback/retry remains available. A known durable N follows TXN-307 and MUST NOT be retried as failed. An unknown outcome follows TXN-705: writes stop until reopen/recovery determines whether N exists completely or not at all.

### TXN-302 — Revision-record commit point and acknowledgement

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`hardenCommit`; [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`writeUberPageReference`
- **Verified by:** [PowerLossSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/PowerLossSimulationTest.java):`anyPowerLossInstantPreservesAcknowledgedCommits`; [CrashRecoveryInjectionTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/CrashRecoveryInjectionTest.java):`killedWriterNeverCorruptsAcknowledgedData` **Planned:** CommitPointOutcomeConformance: inject before/at/after revision-record durability with independent crash storage; verify complete N after every success, no N for known pre-point failure, and both atomic recovery outcomes for uncertainty. Include synchronous/asynchronous and all persistent profiles.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current hardenCommit treats writeUberPageReference return as acknowledgement; directory records can be written earlier and lazy profiles use a tail log. The durable revision-record publication protocol must be reconciled with data/index/beacon ordering before implementation can claim this contract. Existing crash harnesses are opt-in, not default CI proof.

For persistent backends, the commit point MUST be when the new revision record is durable and establishes the complete revision N. Its referenced pages and required index metadata MUST already be durable. Normal return from explicit commit MUST report committed revision N; known post-point cleanup failures MUST NOT change that success (TXN-307). Failure known to be before this point MUST report the ordinary failure with nothing committed; uncertainty during durability establishment MUST follow TXN-705. Background async scheduling is not an acknowledgement. With a filesystem/device honoring force and DSYNC, later crashes MUST preserve N unless explicitly removed by later truncate/retention. IN_MEMORY remains exempt from persistence durability.

### TXN-303 — Atomic unacknowledged outcome

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`writeUberPageReference`; [AbstractReader.java](../bundles/sirix-core/src/main/java/io/sirix/io/AbstractReader.java):`readUberPageReference`
- **Verified by:** [PowerLossSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/PowerLossSimulationTest.java):`anyPowerLossInstantPreservesAcknowledgedCommits`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Opt-in crash evidence, not a default-lane durability proof.

After a crash during an unacknowledged persistent commit, the candidate revision MUST be wholly absent or wholly readable with the intended content; no partial revision may become visible. A lost response MUST NOT be interpreted as proof that no revision committed.

### TXN-304 — Write-ahead protocol

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`commitWritePages`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`hardenCommit`; [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`writeUberPageReference`
- **Verified by:** [PowerLossSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/PowerLossSimulationTest.java)
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** The current writer stages directory records before the beacon-owned data barrier. Exact record/tail-log/beacon publication mechanics need revision to implement the decided commit point; early record bytes alone do not establish a complete durable revision. Marker-directory-loss crash coverage is also incomplete.

A persistent commit MUST establish recovery responsibility before append and make revision pages plus required changed index metadata durable before publishing the new durable revision record that commits N. Beacon maintenance MUST preserve that committed outcome; SECONDARY MUST precede PRIMARY where the dual-beacon protocol is used. No committed record or durable beacon may reference unforced page data. A failed durability force MUST NOT be retried to turn an unknown result into apparent success (TXN-705).

### TXN-305 — Known pre-commit-point failure

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`markRollbackOnly`
- **Verified by:** [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`failedCommitKeepsTransactionUsableAndGuardsClose`; [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** KnownPrePointFailureConformance: pre-hook, serialization and known-not-durable publication failures, retained edits, rollback then retry, and an uncertainty control that must instead stop resource writes.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Existing pre-hook retry tests cover part of this contract. The broad commitInternal catch currently restores RUNNING even when the storage error occurred after durability or during an uncertain hardening operation.

A failure known to precede the commit point MUST mean nothing committed and MUST throw its ordinary failure; the transaction MUST support rollback and retry. Recoverable pre-hook/serialization failures MUST retain modifications and a usable writer; dirty close remains refused. If a partially applied mutation requires rollback first, the API MUST preserve that rollback path rather than permit unsafe immediate reuse. A durability error whose outcome is uncertain MUST NOT be classified as this recoverable case; TXN-705 applies.

### TXN-306 — Index catalogue ordering

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`serializeIndexDefinitions`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`initializeIndexController`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`invalidateIndexCataloguesAfter`
- **Verified by:** [WriterCatalogueRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/WriterCatalogueRecoveryTest.java); [IndexCatalogueUnchangedCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/IndexCatalogueUnchangedCommitTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Current catalogue ordering is before the beacon write. Reconciliation with the decided durable-record point is part of TXN-304; earlier directory writes cannot be mistaken for the new commit point.

Changed index catalogue `{N}.xml` MUST be durable before N reaches the revision-record commit point, not merely before success is returned. Reading revision r MUST use the most recent catalogue at or below r; an explicitly empty catalogue MUST override older definitions. Recovery MUST invalidate catalogues beyond the resolved durable frontier.

### TXN-307 — Known durable commit survives cleanup failure

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`hardenCommit`
- **Verified by:** [FaultInjectingWriterTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/fault/FaultInjectingWriterTest.java) **Planned:** PostDurableCleanupConformance: inject each intent-log clear, marker deletion, guard cleanup and rebind failure after proven record durability; assert normal result N, logged cause, immediate repair flag, closed writer, preserved N and repair on reopen; combine cleanup failures without masking success.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** This captain decision supersedes the earlier distinct post-durability exception. hardenCommit currently throws from log.clear/marker deletion; commitInternal restores RUNNING on storage failure and rethrows rebind failures. No complete repair-flag/clean-close success path exists. Current Java commit signatures return a writer, so the revision-N success receipt must be finalized before API freeze.

After the commit point is known crossed, commit() MUST return revision N normally. Intent-log clear, marker deletion, rebind and other post-durable cleanup failures MUST NOT be reported as commit failures. They MUST be logged, the resource MUST immediately be flagged for repair to run immediately or on next open, and the affected writer MUST be closed cleanly. It MUST never return to RUNNING with dirty work. Cleanup MUST preserve the committed N and unresolved repair responsibility; it MUST NOT convert a known success into CommitOutcomeUnknownException. Safe worker ownership fences still apply during close.

### TXN-308 — Atomic operation guard

- **Level:** MUST
- **Status:** gap
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`assertNotRollbackOnly`
- **Verified by:** No existing test mapped. **Planned:** AtomicCommitGuardConformance: attempt sync/async commits inside nested scopes, inject post-publication failure, assert rejection then successful rollback recovery.
- **Evidence class:** code-only (implementation inspected)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

Commit inside an atomic publication scope MUST fail with `SirixUsageException`. A rollback-only writer MUST refuse mutation/commit until successful rollback reconstructs a safe lineage.

### TXN-309 — Compound mutation boundary

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`beginCompoundOperation`; [JsonNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java):`replaceObjectRecordValue`
- **Verified by:** [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`autoCommitDoesNotSplitJsonReplaceObjectRecordValue`; [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`autoCommitDoesNotSplitXmlMoveSubtree`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

Count-based auto-commit MUST NOT split a compound node move or replacement into committed intermediate trees. It MUST defer publication until a complete top-level mutation boundary.

### TXN-310 — Strictly increasing commit timestamps

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java); [RevisionRootPage.java](../bundles/sirix-core/src/main/java/io/sirix/page/RevisionRootPage.java):`setCommitTimestamp`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`setCommitMessageAndTimestampIfRequired`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`
- **Verified by:** [CommitTimestampPrecisionConformanceTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/CommitTimestampPrecisionConformanceTest.java):`aTieAndABackwardsClockStepAreAdjustedToOneMicrosecondAfterThePredecessor`, `revertUsesTheLatestCommittedTimestamp` (JSON/XML, both persistent backends, warm and cold), `wallClockCommitsStrictlyIncreaseAndKeepMicrosecondPrecision`, `retryUsesItsTimestampAfterSyncOrAsyncPhaseOneFailure`, `retryAfterAutomaticAsyncPhaseOneFailureUsesExplicitTimestamp`; [CommitTimestampLateFailureTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/CommitTimestampLateFailureTest.java) (post-root final-flush failures, warm index, history and lookup before cold reopen, both persistent backends and synchronous/asynchronous phase one); [RevisionIndexIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexIntegrationTest.java); [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java) **Planned:** StrictCommitTimestampConformance: controlled repeating/regressing live clocks and max(previous+1us,clock) oracle; import equal/decreasing/increasing source sequences with conditional adjustment oracle; original metadata queryability, preserved order, first and unchanged timestamps, live commit after import, sync/async and JSON/XML reopen/recovery without wall-clock timing assertions.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Live commits now assign `max(supplied or wall clock, predecessor + 1 microsecond)` once per commit attempt in `RevisionRootPage.assignCommitTimestampMicros`, with the predecessor seeded from the last committed revision of the timeline rather than from the revision a reverting writer represents, and the microsecond unit of VER-201 makes that increment representable. The history-import adjustment and original-metadata path are still absent. Captain decisions 002 and 010 define the separate live-clock and historical-import assignments under the same strict-order invariant.

A failed attempt before publication MUST NOT become a timestamp predecessor: a retry MUST recompute its effective timestamp from the retry input (or current wall clock) and the original committed predecessor. Repeated serialization within one attempt MUST preserve identical timestamp bytes. Before retrying logical revision R, the in-memory revision index MUST discard abandoned entries at or above R while retaining the original predecessor prefix. The retry MUST write its reassigned timestamp into a new root image even if the failed attempt completed the prior root reference. A successful retry MUST occupy exactly slot R: warm history and exact timestamp lookup MUST expose the retry timestamp without an abandoned timestamp or a revision number above the published head, including failure after the root write in the final phase-one flush on either persistent backend.

For each new live commit on a resource timeline, its commit timestamp MUST equal `max(candidate, previous commit timestamp + 1 microsecond)`, where candidate is the wall clock unless the caller supplies an enabled custom timestamp. A custom timestamp in the future MUST be rejected before adjustment; accepted inputs follow VER-201's conversion rules. Commit timestamps MUST strictly increase even when the wall clock repeats or moves backwards. This applies to explicit, automatic and asynchronous commits, empty commits and commits after revert or reopen. History import instead MUST process source revisions in order: preserve the first source timestamp; preserve a later source timestamp if it is greater than the preceding effective timestamp; otherwise assign the preceding effective timestamp plus 1 microsecond and retain the original as queryable metadata (FMT-402). Import MUST adjust only where needed and MUST NOT replace historical timestamps with import-time wall-clock values. Subsequent live commits MUST use the final effective imported timestamp as their predecessor. The predecessor MUST come from the same resource timeline, not an unrelated resource or a historical state restored by revert. Effective timestamps MUST be stored in the epoch-microsecond unit of VER-201, so the one-microsecond increment is exactly representable and MUST NOT be rounded to a coarser unit; they MUST survive persistence/recovery at that precision, so time lookup selects exactly one revision.

### TXN-311 — Conditional write preconditions

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [NodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeTrx.java); [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [NodeDelegate.java](../bundles/sirix-core/src/main/java/io/sirix/node/delegates/NodeDelegate.java):`getLastModifiedRevisionNumber`; [WriteLocksRegistry.java](../bundles/sirix-core/src/main/java/io/sirix/access/WriteLocksRegistry.java)
- **Verified by:** No existing test mapped. **Planned:** ConditionalWritePreconditionConformance: for each target kind in the table below, a token taken before an interleaved foreign commit that does and does not touch the target, a concurrent reorder and a same-depth move that leave ancestor hashes unchanged, a deleted target, XML attribute and namespace insertion/removal on a non-root element followed by a stale conditional replacement, retained attribute/namespace value or name edits within an XML subtree, XML context-element membership changes at insertion points, unchanged first/last-child insertion points, before-first/after-last sibling inserts after disjoint opposite-end edits, empty-container inserts, opposite-side context-link changes, an anchor-container descendant update under ROLLING and NONE, a reused node key after revert, a token from before a truncate, resource replacement, database recreation and backup restore with coincident revision numbers, a token above the head, a pruned token revision, a document-root target, an empty foreign commit, and a rejected check followed by a successful retry; assert the rejection, the unchanged head and the committed result of the accepted write. Run each case with hashing enabled and disabled and assert the same verdicts. **Planned:** ConditionalWritePreconditionBudgetTest: record-read counters for a scalar target, a deep subtree and an insertion point (CONV-301).
- **Evidence class:** doc-only (unverified); decided target has unverified implementation gaps
- **Gap/note:** No conditional check exists at this baseline; REST's node-hash comparison (REST-204) is the only optimistic guard, it is unavailable with the 1.0 default `HashType.NONE` (CONF-104), and the rationale below records why it is not sound even when hashing is on. The predicate depends on VER-206 stamping and on the incarnation id and last-revert metadata of FMT-209, all decided targets with recorded implementation mismatches.

A write transaction MUST offer a conditional check that takes a token and a target and fails the write when the target changed since the token. The token MUST consist of the resource's incarnation id and a revision number `R` in the history identified by that id, on every resource; it MUST NOT carry a node hash, and a stored node hash MUST NOT decide any precondition, for the reason given in the rationale below. The check MUST be evaluated by the single admitted writer (TXN-102) against the head revision its pending revision is based on, after writer admission and before the mutation it guards; because one writer exists per resource, no concurrent commit can intervene between the check and the guarded mutation, so there is no time-of-check/time-of-use window. A failed check MUST leave the transaction, the resource, its metadata and its visible history unchanged, MUST NOT consume a revision number, and MUST report the resource's current incarnation id and head revision so the caller can re-read and retry. The conflict predicates in the table below are incorporated into this requirement. The following gates MUST precede them for every target kind: the check MUST fail when the token's incarnation id differs from the resource's current incarnation id (FMT-209), which is how truncate, resource replacement and backup restore invalidate tokens issued against the history they ended, subject to the 2^-64 reproduction bound of FMT-209, and it MUST fail when `R` is greater than the head revision of that history. It MUST also fail, for every target kind, when `R` is less than the resource's last-revert revision (FMT-209), because a revert republishes older records with their older stamps and restores the older node-key frontier, so node keys allocated afterwards may denote different nodes. The predicates MUST be identical on resources with and without node hashing; derived hash, descendant-count and child-count updates alone MUST NOT affect stamps (VER-206). The check MUST NOT require revision `R` itself to be readable: a pruned token revision (VER-303) MUST still be evaluable, failing only where the predicates say so. The check MUST be sound — it MUST NOT accept a write whose target changed since the token — and MAY be conservative where it cannot distinguish a change from a non-change, provided each conservative case is documented. Its work MUST be bounded by the target: at most the records of the target subtree in the head revision plus a constant, O(1) records for a record-sized target and for an insertion point, with no scan proportional to resource size, revision count or the distance between `R` and the head; that bound is an official work budget under CONV-301. An implementation MAY use any cheaper equivalent evidence that yields the identical verdict.

### TXN-401 — Rollback state

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`rollback`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`rollback`
- **Verified by:** [RollbackStateTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/RollbackStateTest.java); [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** RollbackMatrixConformance: JSON/XML, all versioning types, sync/async failure, compare serialization, indexes and diff after retry.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

Successful rollback MUST discard uncommitted node changes, diff tuples, pending side batches and load-time index state, and reconstruct the writer on its last committed lineage. Later commits MUST NOT include discarded work.

### TXN-402 — Rollback failure

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`rollback`
- **Verified by:** [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java); [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java):`closedRollbackCannotMarkAnotherWritersResourceForRecovery`, `failedRollbackRejectsMutationUntilReconstructionSucceeds` **Planned:** RollbackMarkerConformance: failure at every reconstruction stage, closed rollback while a new writer owns the resource, marker and permit assertions.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Rollback now retains recovery responsibility through reconstruction and rejects closed writers before recovery handling. See NodeTransactionAbortLifecycleTest; complete outcome classification remains part of TXN-705.

Successful rollback MUST clear the recovery marker only after safe reconstruction. Failed rollback MUST preserve the first failure, keep the writer rollback-only and preserve recovery responsibility. Calling rollback on an already closed transaction MUST NOT touch a subsequent writer’s marker.

### TXN-403 — Revert appends without rewriting history

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`revertTo`; [RevertHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/RevertHandler.kt):`handle`; [ToolHandlers.java](../bundles/sirix-mcp/src/main/java/io/sirix/mcp/ToolHandlers.java)
- **Verified by:** [XmlRevertTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/XmlRevertTest.java); [JsonIdentityHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/json/JsonIdentityHistoryTest.java); [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt):`optionalHeadAndMessageAndRevertToHeadAppendARevision`; [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt):`prunedTargetFailsWithoutAppendingOrReplacingTheTarget` **Planned:** RevertParityConformance: Java/REST/MCP, JSON/XML, current head/old head/pruned target, unchanged history after cold reopen.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Captain call 9 is decided. REST implements current-head append; inspect MCP guard as a known cross-interface implementation gap, not authorization for a no-op.

revertTo(r) MUST stage the content of retained revision r and require commit. That commit MUST append head+1, even when r is the current head; intermediate historical revisions MUST remain unchanged. Pruned targets MUST fail with `SirixRevisionPrunedException`, with no substitution. REST and MCP whole-resource revert MUST follow the same append rule. A commit that publishes a revert MUST record its own revision number as the resource's last-revert revision (FMT-209), because reverted records are republished with the stamps they carried in r and the restored revision root reinstates r's node-key frontier; both make a revert invisible to the per-record evidence TXN-311 otherwise relies on.

### TXN-404 — Explicit truncate

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`nodeTrxMap`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`beginNodeReadOnlyTrx`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`truncateTo`; [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`truncateTo`
- **Verified by:** [ExplicitRollbackCrashSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/ExplicitRollbackCrashSimulationTest.java); [NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java); [RAMStorageTruncateToTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/ram/RAMStorageTruncateToTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java) **Planned:** TruncateReaderFenceConformance: open readers below/at/above target across handles, dispatcher views and independent page-read paths; blocked set named in immediate error, unchanged bytes/history/state, no wait; reader-admission race; close conflicting readers then successful truncate, JSON/XML and supported backends.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Captain chose fail-fast reader protection. The current truncate path checks backend support but does not enumerate/atomically fence all readers of removed revisions or report them. Existing crash tests are not proof of that admission contract. The incarnation-id generation is likewise unimplemented: the current path rewinds the uber-page counter and the next commit reissues a removed number, which is permitted (VER-101) only because the incarnation id, not the revision number, carries token identity across a truncate.

truncateTo(r) MUST generate and durably publish a new resource incarnation id (FMT-209) before removing any data, so that conditional-write tokens issued against a removed revision fail under TXN-311 even though later commits reissue its revision number, subject to the 2^-64 reproduction bound of FMT-209. Publishing the incarnation id before the removal is required rather than convenient: a crash between the two leaves the new incarnation id with the old data, which only rejects tokens that would otherwise have been accepted, whereas the opposite order could accept a token from a destroyed timeline. truncateTo(r) MUST validate that r is retained. If any open reader holds a revision that truncation would remove, it MUST fail immediately with an error identifying those readers and their revisions, without waiting for them and without changing transaction state, metadata, files or visible history. Reader admission and the eligibility check MUST be coordinated so a new reader cannot enter the removed range between validation and mutation. Readers at or below r MUST NOT be rejected solely for being open. When eligible, truncate MUST durably publish the retained frontier before removing later data/directory records; a crash MUST leave the old head or r, never a partly readable history. Unsupported backends MUST reject before mutation.

### TXN-501 — Dirty transaction close

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`close`
- **Verified by:** [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`failedCommitKeepsTransactionUsableAndGuardsClose`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

Public NodeTrx.close with uncommitted modifications MUST throw `SirixUsageException` and leave the caller able to commit or rollback. Close MUST NOT silently commit pending modifications. Session-driven abort follows TXN-505.

### TXN-502 — Close preserves the classified async outcome

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`close`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`awaitPendingAsyncCommit`
- **Verified by:** [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java); [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java):`terminalHardeningFailureAbortsDirtySuccessorAfterTheWorkerFence`, `inlineDoubleFailureReleasesSlotAndRetainsPrimary` **Planned:** AsyncCloseOutcomeConformance: known pre-point failure, durable cleanup failure, uncertain force, dirty successor and repeated close; verify result classification, repair flag, no fsync retry and safe ownership.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Close fences async workers, preserves the first failure and retains recovery responsibility. The three-way outcome/repair contract in TXN-307/705 is not implemented.

Close MUST fence in-flight hardening before freeing its owners and MUST preserve its outcome classification. A known durable N with cleanup failure follows TXN-307: log, flag repair and close cleanly without reporting a commit failure. An unknown outcome MUST remain CommitOutcomeUnknownException with resource writes stopped and recovery responsibility intact; close MUST NOT retry fsync or reinterpret it as a known failure/success. Independent teardown is attempted with safe ownership. Listener shutdown follows TXN-803.

### TXN-503 — Independent cleanup

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`close`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`close`
- **Verified by:** [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java); [AbstractResourceSessionWriteFailureCleanupTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AbstractResourceSessionWriteFailureCleanupTest.java); [BufferManagerTeardownTest.java](../bundles/sirix-core/src/test/java/io/sirix/cache/BufferManagerTeardownTest.java) **Planned:** TeardownConformance: inject failures in reader, log, backend, pool and session children; verify each cleanup is attempted without freeing live-worker state.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Resource-session and buffer-manager cleanup now accumulate independent failures. See AbstractResourceSessionWriteFailureCleanupTest and BufferManagerTeardownTest; dependency ownership must still be retained when a child cannot terminate.

Transaction/session teardown MUST attempt each independent owner even when another cleanup fails, preserve the first failure with later failures suppressed, and be idempotent after success. A retry after failed teardown MUST complete only the remaining safe work.

Concurrent traversal close, reset and exhaustion MUST report a terminated worker's unreported
failure, even when a subsequent close succeeds. Reset MUST NOT reuse a producer after failed
cleanup. An interrupted consumer MUST retain interruption as the primary error, suppress worker
cleanup failures and restore its interrupt status after termination. Repeated close after a
reported failure MUST NOT report that same worker failure again.
Requested cancellation MAY discard the traversal interruption used to stop a nested producer,
but MUST retain its suppressed failures and all independent cleanup failures, including cleanup
that itself reports interruption. Unrequested worker interruption MUST still fail traversal.
Regression mapping: `ConcurrentAxisLifecycleTest.txn503CancellationPreservesFirstCloseFailure`
and `setExhaustionFencesUnusedProducerAndReleasesOwnedReader` cover JSON/XML cancellation,
reset, interrupted consumption and set exhaustion with first-close-only reader failure.

### TXN-504 — Worker ownership fence

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`close`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`awaitPendingAsyncFlush`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`close`
- **Verified by:** [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java); [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java):`stalledFlushRetainsNodeOwnershipUntilWorkerExitAndRetry`, `lateConcurrentCloseRetainsOwnerUntilPermitRelease` **Planned:** WorkerOwnershipConformance: stalled worker, interrupted wait, failed cleanup and retry; assert no successor admission or use-after-free before exit.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Node close now fences scheduler/storage work before exactly-once admission release, retaining unfinished owners for retry. See NodeTransactionAbortLifecycleTest; this does not establish the future outcome API in TXN-705.

A writer MUST NOT release its write permit or free worker-accessible memory until its mutation/flush/hardening workers have terminated. If termination is not established, ownership MUST remain fenced for retry. A listener dispatcher MUST NOT own a mutable writer or block this fence.

### TXN-505 — Session abort

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`close`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`rollback`
- **Verified by:** [JsonDatabaseLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonDatabaseLifecycleTest.java); [AbstractResourceSessionWriteFailureCleanupTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AbstractResourceSessionWriteFailureCleanupTest.java); [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java):`terminalHardeningFailureAbortsDirtySuccessorAfterTheWorkerFence` **Planned:** SessionAbortConformance: JSON/XML known-uncommitted dirty work, known durable cleanup failure, uncertain async durability and rollback failure; assert each preserved outcome and no repeated fsync on close/reopen.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Session close uses internal abort for dirty JSON/XML writers, fences workers and retains unfinished dependencies for retry. See NodeTransactionAbortLifecycleTest. The future three-way durability outcome contract remains incomplete.

Session close MUST safely discard known uncommitted dirty work without committing it, using rollback or an internal abort path. It MUST preserve known committed N under TXN-307 and MUST NOT roll back, retry fsync or claim absence for an unknown candidate under TXN-705. Uncommitted tails, unknown outcomes and incomplete repair MUST retain recovery responsibility; worker fences MUST precede owner release.

### TXN-601 — Marker identity

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`getCommitFile`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`commitWritePages`
- **Verified by:** [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java); [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java):`failedMarkerCreationSurvivesSessionClose` **Planned:** MarkerIdentityConformance: empty/nonempty regular file, directory, creation failure and deletion failure.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Recovery admission now rejects a non-regular marker path and preserves failed marker obligations. See NodeTransactionAbortLifecycleTest; marker-directory durability remains deferred under TXN-602.

The persistent recovery marker MUST be the regular file `<resource>/log/.commit`; its contents are irrelevant. A non-regular entry at that path MUST fail recovery admission explicitly. Ordinary database `.lock` process ownership is a different mechanism.

### TXN-602 — Signal precedes append

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`commitWritePages`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`awaitPendingAsyncFlush`
- **Verified by:** [PowerLossSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/PowerLossSimulationTest.java); [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java):`tornWriteWithoutMarker_lastCommittedRevisionStillReadable` **Planned:** MarkerLossCrashConformance: first append before marker persistence, lost directory entry, stale-forward SECONDARY then second crash.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current marker creation has no explicit parent-directory force and the crash recorder assumes marker presence. Selecting the marker-directory durability mechanism is deferred implementation work; correct recovery under the stated failures remains required.

Persistent writers MUST establish recovery responsibility before appending uncommitted pages, including asynchronous prewrites. Failure to establish the signal MUST NOT be silently treated as a clean write. The crash oracle MUST model marker-directory-entry loss independently of page durability.

### TXN-603 — Marker removal

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`hardenCommit`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`rollback`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`close`
- **Verified by:** [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java); [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java):`failedMarkerDeletionPreservesPublishedRevisionAndBlocksFalseCleanAdmission` **Planned:** MarkerLifecycleConformance: enumerate success and every failure exit, then reopen and recover.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Close now preserves unresolved markers; rollback removes them only after successful reconstruction. NodeTransactionAbortLifecycleTest covers failed marker deletion, rollback reconstruction and recovery admission.

A persistent marker MUST be removed only after successful commit cleanup or successful rollback. Close, commit failure, failed rollback and teardown failure MUST NOT remove unresolved recovery responsibility. A successful recovery admission may leave the marker until the following successful commit/rollback.

### TXN-604 — Recover from durable disk frontier

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists`; [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`truncateTo`
- **Verified by:** [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java); [CrashRecoveryOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryOrderingTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java); [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java):`postBeaconCleanupFailureRecoversPublishedRevision` **Planned:** RecoveryOutcomeConformance: complete durable N with stale beacon/session, staged incomplete record, unknown N both present and absent, sibling cache preservation, repeat recovery and crash during recovery; verify reported outcome before writer admission.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Recovery now reads the verified durable uber beacon before truncation and resource-local cache/catalogue invalidation. Record-point authority and an outcome-reporting API are still missing under TXN-302/705; the physical durable-record evidence protocol remains engineering work.

Recovery MUST determine the complete committed frontier from durable revision records and validated referenced data under TXN-302, reconcile beacons, and resolve an unknown candidate N as wholly present or absent. It MUST preserve every known committed N even if a beacon or session head is stale, and MUST NOT promote a merely staged record with incomplete durability evidence. It MUST truncate only uncommitted tails, invalidate later index catalogues, clear only this resource’s stale cache entries and publish the resolved frontier before admitting a new writer. The API MUST report whether the unknown candidate N committed after recovery.

### TXN-605 — Pending successor exemption

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`createPageTransaction`
- **Verified by:** [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java); [AsyncCommitFrontierOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AsyncCommitFrontierOrderingTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

The explicitly constructed in-process successor based on a pending uber page MUST skip marker recovery while its predecessor hardens. Unrelated writer admissions MUST NOT use this exemption.

### TXN-606 — Failed recovery refuses admission

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`createPageTransaction`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists`
- **Verified by:** [AbstractResourceSessionWriteFailureCleanupTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AbstractResourceSessionWriteFailureCleanupTest.java); [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java):`recoveryFailureRejectsAdmissionAndCanBeRetried`, `recoveryBackendOpenFailureNamesMarkerAndReleasesAdmission` **Planned:** RecoveryFailureConformance: backend open, root discovery, truncate, catalogue/cache and marker failures; fresh reopen revalidates durable evidence, preserves error/stage and never repeats a failed force to infer success.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Recovery admission now closes temporary backend ownership on failure and retains recovery responsibility. NodeTransactionAbortLifecycleTest covers marker/open/truncate failure and retry; the future unknown-outcome API remains incomplete.

Failure at any recovery stage MUST refuse writer admission, identify the marker path and stage, close temporary backend ownership, release the admission permit only when no worker still owns it, and preserve recovery responsibility. A later eligible reopen MUST resume recovery idempotently from fresh durable evidence. This MUST NOT retry a failed fsync/force in the poisoned resource instance or bypass TXN-705; ordinary writer admission remains stopped while durability is unknown.

### TXN-607 — In-memory resources never write markers

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`commitWritePages`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists`; [RAMStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/ram/RAMStorage.java)
- **Verified by:** [RAMStorageTruncateToTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/ram/RAMStorageTruncateToTest.java); [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java):`inMemoryCommitFailureNeverCreatesRecoveryMarkersOrBlocksTheNextWriter`, `inMemoryRecoveryRequestNeverCreatesAMarkerOrPoisonsAdmission` **Planned:** InMemoryMarkerConformance: inject commit/rollback/close failures and reopen admission; assert no marker creation and no recovery wedge.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Commit, recovery and failure teardown now exclude non-durable storage from marker/guard creation. NodeTransactionAbortLifecycleTest covers failed in-memory commits and fresh writer admission; durable backends still reject unsupported recovery.

IN_MEMORY resources MUST NOT create the filesystem commit marker on commit, preflush, rollback, abort or teardown failure. No persistent-recovery obligation may make an in-memory resource permanently unwritable. This exemption MUST NOT permit a persistent backend to skip required recovery.

### TXN-608 — Marker creation failure obligation

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`createPageTransaction`; [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`resourceUuid`
- **Verified by:** [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java):`failedMarkerCreationSurvivesSessionClose`, `removedResourceCannotPoisonRecreatedResourceAtTheSamePath`; [DatabaseNotificationLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseNotificationLifecycleTest.java):`restoredBackupDoesNotInheritDeletedRecovery`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** AbstractResourceSession now retains a process-local recovery obligation keyed by marker path and persisted resource identity. NodeTransactionAbortLifecycleTest covers failed marker creation/retry; DatabaseNotificationLifecycleTest covers deletion retirement. The obligation is not itself durable across process exit.

For a persistent resource, failed marker creation MUST retain a process-local recovery obligation tied to the resource’s persisted identity and path. The next admission MUST re-establish the marker and recover. Deleting/recreating the resource MUST NOT transfer the old obligation to its new identity. Successful removal through `Database.removeResource` MUST release its process-local obligation: restoring a clean backup at the same path, even with the same persisted identity, MUST admit a writer without recreating the deleted resource's recovery marker and MUST preserve the backed-up committed values.

### TXN-609 — Read admission does not perform tail recovery

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`beginNodeReadOnlyTrx`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`createPageTransaction`; [FileChannelReader.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelReader.java):`readRevisionRootPage`
- **Verified by:** [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java):`concurrentReader_survivesWriterCrashAndRecovery` **Planned:** ReadAdmissionRecoveryConformance: marker plus unreachable tail; assert unchanged tail size and marker on reads, then repaired state on writer admission.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

Opening an existing committed resource for a read MUST NOT run writer-tail truncation or remove its marker. Reads MUST remain within the selected committed frontier. This does not forbid checksum-validated revision-record salvage or the separately specified proven-empty bootstrap recovery.

### TXN-610 — Unreachable tails are not committed data

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`initFrontiersIfNeeded`; [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`adoptCachedDataFrontier`
- **Verified by:** [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java):`tornWriteWithoutMarker_lastCommittedRevisionStillReadable`; [PreallocatedCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/PreallocatedCommitTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

Reads MUST ignore bytes beyond the selected durable frontier. A writer MUST derive its reusable frontier from validated committed metadata, not physical preallocated file length, and MUST NOT reuse offsets while stale cache identities can address abandoned writes.

### TXN-611 — Interrupted bootstrap

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [InterruptedFirstCommitRecovery.java](../bundles/sirix-core/src/main/java/io/sirix/io/InterruptedFirstCommitRecovery.java)
- **Verified by:** [InterruptedFirstCommitRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/InterruptedFirstCommitRecoveryTest.java); [InterruptedFirstCommitRecoveryStoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/InterruptedFirstCommitRecoveryStoreFeaturesTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

Automatic empty rebootstrap MUST occur only when the files prove no committed revision exists. A lost revisions file beside committed data, zeroed committed superblock, invalid beacon or unsupported feature MUST fail rather than erase possible committed history.

### TXN-612 — Derived frontier recovery

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [DerivedIndexMaintainer.java](../bundles/sirix-core/src/main/java/io/sirix/index/aggregate/DerivedIndexMaintainer.java)
- **Verified by:** [DerivedIndexCrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedIndexCrashRecoveryTest.java):`restartsAtDurableDerivedFrontier`; [DerivedRevisionMappingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedRevisionMappingTest.java); [DerivedIndexMaintainerTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedIndexMaintainerTest.java):`backwardsClockKeepsDerivedMappingsValidAndMaintenanceUsable`, `summaryFoldsUseTheirOwnTimelineAfterClockRegression`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Crash suite is opt-in. Full cross-resource atomicity is not promised; source/derived revision mapping carries the relationship.

A derived resource MUST recover its own durable state and source-revision mapping, then catch up from the source’s durable frontier. Derived batch publications, aggregate folds and summary folds MUST assign commit timestamps from the derived timeline’s own clock and predecessor under TXN-310. They MUST NOT forward a source or previously effective timestamp as custom input: effective timestamps can legitimately exceed a regressed wall clock. Source-revision mappings MUST retain the source root’s exact effective microsecond timestamp for validation; a backward clock step MUST NOT poison maintenance. Source revision numbers and derived revision numbers MUST NOT be assumed numerically equal. A crash between source and derived publication MUST leave a recoverable source of truth.

### TXN-613 — Backups include derived resources

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [BackupManager.java](../bundles/sirix-core/src/main/java/io/sirix/backup/BackupManager.java):`backupDatabase`, `quiesceDerivedResource`, `copyDerivedResource`, `restoreDatabase`, `verifyDerivedResource`; [DerivedIndexMaintainer.java](../bundles/sirix-core/src/main/java/io/sirix/index/aggregate/DerivedIndexMaintainer.java):`tryLockHistory`; [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`listResources`
- **Verified by:** [BackupDerivedResourceTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupDerivedResourceTest.java):`backupCopiesAndRestoreVerifiesTheDerivedResource`, `restoreRefusesATruncatedDerivedResource`, `backupRefusesWhileDerivedMaintenanceCannotQuiesce`, `backupReservesIdsForRebuiltDerivedResources`, `backupRefusesAPendingDerivedRewind`, `restoreRefusesDerivedMappingsBeyondTheSourceHead`, `restoreRefusesDamagedDerivedRestartMetadata`, `restoreRefusesIncompleteDerivedStructure`; [BackupRestoreTemporalAggregateTest.java](../bundles/sirix-query/src/test/java/io/sirix/query/BackupRestoreTemporalAggregateTest.java):`aRestoredBackupStillServesTheTemporalAggregateIndex`; [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java); [DerivedRevisionMappingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedRevisionMappingTest.java) **Planned:** DerivedBackupConformance: source/derived revisions advancing during copy, backup start barrier, copy hidden resources at pinned heads, restore elsewhere and compare history/mappings/queries before and after catch-up; interrupted copy rejects partial target.
- **CI regressions:** CLI `BackupTest.reportsEveryDerivedResource` checks the reported derived resources in quiet and verbose modes.
- **Evidence class:** code+test (source inspection); the pinned start snapshot this clause references remains unimplemented
- **Gap/note:** Implemented: each backed-up resource's hidden `$derived` sibling is copied with it, under the maintainer's history lock rather than its (permanently held) writer semaphore, and a store whose maintenance does not quiesce within the writer-lock timeout is refused instead of copied mid-build. Restore verifies the derived resources first — a visible resource's open installs the maintainer, which rebuilds a derived store it cannot read — and reports them in `BackupSummary.derivedResources()`. Still missing: the decided common start resource/revision pin set of TXN-614; the backup reads each resource's current head when it acquires that resource's lock, so two resources can be copied at heads minted at different times. The source head is held by its writer lock while both halves are copied; backup refuses a pending derived rewind and validates the durable derived frontier and mappings against that head. Restore repeats these checks through a non-rebuilding tool open before any source session opens.

For resources included under TXN-615 clause 10, backup MUST copy hidden `$derived` resources as well as visible resources from the pinned start snapshot in TXN-614, including settings, catalogues, revision files and source-revision mapping through each pinned head. It MUST omit transient lock/log/temp files. The copied derived source-frontier mapping MUST NOT refer beyond the pinned copied source head. The target database's next resource ID MUST exceed every copied resource ID, including hidden siblings created or rebuilt after database metadata was copied. Backup MUST refuse a pending derived rewind under the source writer lock and derived history lock. Restore MUST validate derived directory structure and durable frontier/mapping metadata through a non-rebuilding open before opening any source session, and MUST reject mappings beyond the copied source head. Restore MUST preserve the copied derived state and permit normal catch-up; silently omitting derived resources or copying a later head is not conforming.

### TXN-614 — Backup start snapshot and fail-fast resource drop

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [BackupManager.java](../bundles/sirix-core/src/main/java/io/sirix/backup/BackupManager.java):`backupDatabase`; [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`removeResource`; [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`listResources`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`beginNodeReadOnlyTrx`
- **Verified by:** [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java); [DatabaseOwnershipTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseOwnershipTest.java) **Planned:** BackupStartSnapshotConformance: barrier at start, concurrent create/commit/drop, assert exact resource/head manifest; pinned drop names backup in progress and preserves all state; later-created resource excluded; two overlapping backups and failed-copy cleanup; restore and compare every pinned revision plus hidden derived state.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current backup enumerates visible resources then copies each under its own writer semaphore, choosing the head at copy time. LocalDatabase.removeResource rejects open sessions generically, but has no backup pin registry or required diagnostic. Exact start pins and admission/failure cleanup remain implementation work.

At backup start, subject to TXN-615 clause 10, backup MUST pin the set of resources and each resource’s current committed revision and MUST copy exactly that set at those pinned revisions. Resources created after the start MUST NOT enter the backup; commits after the start MUST NOT advance a copied head. Pins MUST keep the selected immutable revision data available until copying finishes or fails. Dropping a resource in any running backup’s pinned set MUST fail immediately with a clear "backup in progress" error, without waiting or changing anything. Snapshot capture and resource create/drop admission MUST be coordinated to avoid a membership race. Completion/failure MUST release only that backup’s pins; another active backup’s pins remain effective.

### TXN-615 — In-memory state lives with the open database

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [RAMStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/ram/RAMStorage.java); [InMemoryResources.java](../bundles/sirix-core/src/main/java/io/sirix/access/InMemoryResources.java); [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`close`, `retireInMemoryLeftovers`, `createResource`, `removeResource`, `listResources`, `existsResource`, `beginResourceSession`; [Databases.java](../bundles/sirix-core/src/main/java/io/sirix/access/Databases.java):`openDatabase`, `releaseDatabase`; [DatabaseHandle.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseHandle.java):`close`; [BackupManager.java](../bundles/sirix-core/src/main/java/io/sirix/backup/BackupManager.java):`backupDatabase`, `restoreDatabase`
- **Verified by:** [RAMStorageCommittedRevisionTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/ram/RAMStorageCommittedRevisionTest.java) (AC1); [LocalDatabaseInMemoryLifetimeTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/LocalDatabaseInMemoryLifetimeTest.java) (AC2, AC3, AC4 except its token bullet, AC5, AC8, AC9, AC11); [DatabasesInMemoryLifetimeTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabasesInMemoryLifetimeTest.java) (AC6, AC7, AC12, AC15); [InMemoryLifetimeForkedJvmTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/InMemoryLifetimeForkedJvmTest.java) (AC10, AC13); [BackupManagerInMemoryResourcesTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupManagerInMemoryResourcesTest.java) (AC14); all JSON and XML. **Planned:** AC4's token bullet, behind FMT-209, TXN-311 and JAPI-111.
- **CI regressions:** `DatabasesInMemoryLifetimeTest.repeatedCloseFinishesOnlyTheEndedInstancesCleanup` checks direct/facade retries and newer-instance isolation; `RAMStorageContractTest` checks retained-provider discard, unknown-key boundaries and a budget of 8 KiB allocated per small revision across staging/publication; `InMemoryResourcesTest` checks unknown and tombstone classification; CLI `BackupTest` checks omitted-resource output in quiet and verbose modes.
- **Review regressions:** TXN-615 clauses 3 and 5 cover filesystem-valid resource names up to 220 characters, on close and removal, in `LocalDatabaseInMemoryLifetimeTest.longResourceNamesAreRetiredAndReusable`; clause 7 covers a successful rename followed by failed deletion in `DatabasesInMemoryLifetimeTest.failedTombstoneDeletionStillEndsTheInstance`; clause 10 covers incomplete tombstones in backup and restore in `BackupManagerInMemoryResourcesTest.incompleteTombstonesDoNotBlockBackupOrRestore`. The existing RAM storage contract and committed-revision tests cover caller ownership across scratch reuse.
- **Evidence class:** code+test (source inspection). AC10 and AC13 run forked JVMs; AC12 needs POSIX permissions and a non-root user and is skipped elsewhere, which is not evidence (CONV-102).
- **Gap/note:** Captain decisions of 20 November 2026: lifetime; ownership option 1 of the IN_MEMORY design comparison; backups exclude IN_MEMORY resources (call B, option 1).
  - Implemented: committed state lives in the open instance and its final close discards it.
  - Leftovers are removed at close and at the next open. A resource directory is retired by renaming it to `$retired-<uuid>` before deleting it, so a failure never leaves an unprovable half-deleted directory. The tombstone length is independent of the original name. A successful rename frees the name even if deletion fails; final close still reports the remaining tombstone. Resource names containing `$retired-` are therefore reserved.
  - A failed session teardown keeps the instance for a retry; a failure only in removing leftovers ends it.
  - Backups skip IN_MEMORY resources and list them.
  - A failed initial directory scan remains pending. Resource admission retries it before opening,
    listing, existence checks or creation; no new RAM state is admitted before the scan succeeds.
  - Source settings remain proof until the derived sibling leaves its original name, including
    removal and hidden-name creation retries. Retaining settings never retains cached source content.
  - Review regressions: `failedForcedRemovalReleasesAdmission` covers AC9/AC12's ended-instance
    release; `InMemoryLifetimeForkedJvmTest.failedInitialScanIsRetriedBeforeResourceAdmission` covers
    clause 6 after abrupt exit, and
    `DatabasesInMemoryLifetimeTest.pendingInitialScanBlocksAdmissionUntilDiscoverySucceeds`
    covers refusal and recovery at each admission path in-process; `derivedLeftoversAreRetiredBeforeSourceNameReuse` covers clauses 7/8 using persisted
    source/derived directory fixtures with denied deletion. Missing cache-release behavior on the
    retained-proof branch has no direct public cache-inspection oracle and is reviewed at the shared
    discard boundary; this round adds no internal-cache test.
  - Provisional RAM revision roots remain separate from committed records and the timestamp index.
    Uber-beacon publication adds each committed revision exactly once; failed catalogue persistence
    followed by rollback/retry never contributes a timestamp entry (TXN-503, VER-202).
    `RAMStorageCommittedRevisionTest.failedCommitDoesNotPublishATimestampEntry` checks exact,
    between-commit and future timestamps, historical content and absent revision 3 for JSON/XML.
  - A known path remains pending unless `Files.notExists` proves absence, including when the
    resources directory can be listed but not traversed. The POSIX regression is
    `DatabasesInMemoryLifetimeTest.failedTraversalRemainsPendingUntilRepeatedClose` (clause 7).
  - Resource identities remembered at session creation release known RAM caches after successful
    teardown even when filesystem discovery fails. Registry removal also clears the retired RAM
    store, so retaining a closed session does not retain its committed bytes (AC13).
  - Resource removal uses remembered RAM ownership, including a derived sibling's remembered
    source, even if persisted settings become unreadable. Unremembered directories still require
    readable settings as proof. `knownOwnershipSurvivesUnreadableSettings` checks failed-rename
    proof retention for sources and derived siblings; `unreadableSettingsRemovalReleasesMemory`
    reuses a name while retaining closed sessions under the AC13 heap bound (clauses 6/7/8).
  - Logical discard removes both name/ID mappings, including failed renames and retained source
    proof (`discardedHiddenResourcesHaveNoNameOrId`, clauses 5/7/8).
  - Ended instances retain only exact pending cleanup paths and their original proof UUIDs.
    Repeated close retries these paths under lifecycle admission without scanning or discarding
    newer state (`repeatedCloseRetriesOnlyRemainingCleanup`, `oldClosePreservesANewerResource`).
    Original names are renamed before deletion and derived originals retire before their proof.
  - REST opens and closes a handle per request, and its delete route closes every handle in the process. Either can perform the final close of IN_MEMORY state an embedded user created.

Storage type is chosen per resource, so one database MAY hold IN_MEMORY resources alongside
FILE_CHANNEL or MEMORY_MAPPED resources. This requirement governs only the IN_MEMORY resources
and the state derived from them (clause 8). Persistent resources in the same database keep their
committed revisions and follow TXN-302 unchanged. The committed state of an IN_MEMORY resource
consists of its revisions, its name, its settings and its index definitions. It MUST belong to
the open database instance that holds it, in the process that opened it:

1. **Open database.** While the instance is open, every revision committed on the resource
   MUST stay visible to every later writer, read-only transaction and resource session of that
   instance. This holds through every handle, whether or not other sessions were open in
   between, including after every resource session was closed (TXN-201, TXN-204, VER-101).
   Uncommitted changes stay private (TXN-202).
2. **Handles and threads.** All handles opened on the same database in the owner process share
   one instance and its ownership (TXN-101). Paths that resolve to the same real directory
   (relative paths, `..` segments, symbolic links) MUST reach the same instance and state.
   Closing a handle while another handle to the instance is open MUST NOT discard anything
   (JAPI-103). A database handle and a resource session MAY be shared by several threads, each
   using its own transactions (JAPI-104).
3. **Final close.** The instance closes when its last handle closes. A handle that is never
   closed keeps the instance, and its IN_MEMORY state, open until the process exits. That close
   MUST first tear down its sessions and transactions; a transaction with uncommitted changes is
   aborted, never committed (TXN-505). Then, while it still holds the database's admission, the
   close MUST:
   - discard the committed state of every IN_MEMORY resource of the instance;
   - remove its on-disk footprint: the resource directory (including `ressetting.obj`,
     `indexes/` and `update-operations/`) and its derived state (clause 8).

   Afterwards Sirix MUST retain no reference to that state, so its memory is reclaimable.
   Persistent resources, other than the derived state of clause 8, are untouched.
4. **Close racing an open.** An open of the same database that arrives while the final close
   is in progress MUST NOT observe a partly discarded state. It MUST either complete against the
   closing instance before teardown starts (and then keep it open), or wait and be admitted to a
   new instance only after the discard finished.
5. **Reopen.** A database opened after its instance closed MUST NOT contain the former IN_MEMORY
   resources:
   - They MUST be absent from resource listing and existence checks.
   - Opening one by name MUST fail with `SirixUsageException`, as for a name that never existed.
   - Opening the database itself is not an error, and an all-IN_MEMORY database opens with no
     resources.
   - Creating a resource under a former name MUST succeed. The new resource starts at bootstrap
     revision 0 with no content and a fresh incarnation id (FMT-209), so tokens and cached state
     of the former resource never apply to it (TXN-311).
6. **Abrupt end.** The same holds when the process ends without closing the database. A later
   process MUST treat any directories or settings the former IN_MEMORY resources left behind as
   absent, and MUST NOT expose them as resources without revisions. Reserved retirement names
   containing `$retired-` identify tombstones, not resources, and do not require readable settings.
   For other names, before hiding or removing leftovers, it MUST verify from readable persisted
   settings that they belong to an IN_MEMORY resource or its derived state (clause 8). Leftovers
   whose settings are missing, empty or unreadable MUST be treated as a persistent resource under
   TXN-611, and never removed by this cleanup. No other persistent resource may be hidden or
   removed by it.
7. **Failed cleanup.** This clause extends TXN-503's teardown rule to the final database close.
   - If tearing down sessions and transactions fails, the close MUST throw that failure and discard
     nothing. The instance stays open, owned and complete, and a retry completes only the
     remaining work (TXN-101, TXN-503).
   - Once teardown has succeeded, the IN_MEMORY state MUST be discarded and its memory released,
     even if removing its on-disk footprint fails. That close MUST nevertheless end the instance
     and release the admission, and a later open MUST create a new instance. It throws the first
     failure, with later ones suppressed.
   - Leftovers of a failed removal are treated as in 6. The next open or a repeated close
     completes only the remaining cleanup, and the name remains reusable.
   - Memory reachable from a fenced worker (TXN-504) MUST NOT be released until that worker has
     stopped.
8. **Removal and derived state.** Removing an IN_MEMORY resource from an open database MUST
   discard its committed state and on-disk footprint at once. It is subject to the same
   open-session precondition as removing a persistent resource. A TXN-614 backup pin never
   covers it (clause 10).
   A resource later created under that name follows 5. Removing an open database
   (`Databases.removeDatabase`) MUST close its instance as in 3 before it removes it.

   State derived from an IN_MEMORY resource, including update-diff sidecars and the derived
   index resource built from it, belongs to that resource regardless of its own storage type. It
   MUST be removed when the source's state is discarded (clauses 3, 5 and 8), and MUST NOT
   reappear under a later resource of the same name. A derived resource is identified by its
   reserved name and its recorded source, never by its own storage type. Clauses 3 and 6 protect
   persistent resources other than these.
9. **No durability.** IN_MEMORY state is never visible to another process. A second process
   cannot open the database while the owner holds it (TXN-101, `SirixDatabaseLockException`).
   It has no durability. The TXN-302 exemption and TXN-607 are unchanged.
10. **Backup.** A backup (TXN-613, TXN-614) MUST skip every IN_MEMORY resource and its derived
    state (clause 8). Its pinned set contains only persistent resources.
    The backup's result MUST list each skipped IN_MEMORY resource by name
    as not backed up. A restore MUST never create an IN_MEMORY resource or its derived state.

Acceptance criteria, each observable through the public API, for JSON and XML:

- AC1 (#1463 oracle): in an open database, insert and commit, then close the writer. Open
  another writer, a read-only transaction, and a new resource session after closing every
  session. Each sees the committed node and value, and every earlier revision reads unchanged.
- AC2 (handles): with two handles open, commit through one. Close the handles in both orders.
  After the first close, a new session through the remaining handle sees the committed
  revision. After the second close, AC4 holds.
- AC3 (threads): two threads share one handle and one resource session, each with its own
  transaction. One commits while the other holds a reader on the earlier revision. The reader
  keeps its revision, and a reader opened afterwards sees the commit.
- AC4 (reopen): commit, close the last handle, then reopen.
  - The IN_MEMORY resource is absent from `listResources()` and `existsResource`.
  - `beginResourceSession` with its name fails with `SirixUsageException`.
  - After opening a session on every resource `listResources()` returns,
    `getResourceID(formerName)` throws `SirixUsageException`.
  - `createResource` with the name succeeds, and the new resource reports revision 0 with no
    content.
  - **Planned** behind FMT-209, TXN-311 and JAPI-111: a token (ETag/incarnation) from the former
    resource is rejected by the new one.
- AC5 (mixed database): the database holds one FILE_CHANNEL and one IN_MEMORY resource. Before
  final close, both are listed and readable. After reopen, the FILE_CHANNEL resource is listed,
  reads all its revisions, and `getResourceID` returns its ID once a session on it is opened. The
  IN_MEMORY resource meets AC4.
- AC6 (alias paths, regression guard): open the database through two path spellings of the same
  directory (a relative path, or one with a `..` segment or a symbolic link). Each sees the
  other's commits. Closing both discards the state as in AC4.
- AC7 (close racing open): repeatedly run the final close on one thread and an open of the same
  database on another, without an internal hook. Every open sees either all committed revisions
  or AC4's empty state. None sees a partial state or fails with an internal error. The two
  admissible branches of clause 4 are not distinguished from outside.
- AC8 (dirty transaction at close): close the last handle while a writer with uncommitted
  changes is open, then reopen. The result meets AC4, and nothing uncommitted was committed.
- AC9 (removal):
  - `removeResource` of a committed IN_MEMORY resource in an open database (no sessions open),
    then create the same name: revision 0, no content.
  - With a session open, removal is refused, as for a persistent resource.
  - `Databases.removeDatabase` with handles open, then create and open a database at the same
    path: empty.
- AC10 (abrupt end, forked JVM): a child JVM creates a database holding a committed IN_MEMORY
  resource and a committed FILE_CHANNEL resource, then exits without closing. A fresh JVM opening
  the directory meets AC5. The FILE_CHANNEL resource and its files are intact.
- AC11 (leftover verification): a resource directory whose settings file is missing, empty or
  unreadable, and a FILE_CHANNEL resource whose data files are empty, are never hidden or
  removed by the cleanup of clause 6.
- AC12 (failed cleanup and retry; POSIX, non-root): make an IN_MEMORY resource's directory
  undeletable, then close the last handle.
  - Close throws the cleanup failure, the old handle reports itself closed, and the committed
    state is gone.
  - A later open creates a new instance (it does not rejoin the failed one), and the resource is
    absent there.
  - After the directory becomes deletable again, the name can be created at revision 0.
  - If another process holds database ownership, a repeated close of the ended instance throws
    `SirixDatabaseLockException` without deleting its pending original directory or tombstone.
    After that owner releases the lock, repeated close removes the leftover and becomes idempotent
    (TXN-101). `DatabaseOwnershipTest.repeatedCloseRespectsAnotherProcessesOwnership` checks both
    representations for JSON and XML with a real child-process lock holder.
- AC15 (failed teardown; clause 7): make session teardown fail once at the final close.
  - The close throws, and the handle stays open.
  - An open that rejoins the instance sees every committed revision.
  - After user sessions drain, a failed backend teardown leaves the rejoined instance able
    to admit writers and deliver their pre/post-commit hooks (TXN-801, TXN-802, TXN-803).
  - A retried close succeeds, and the next reopen holds no former IN_MEMORY resource.
- AC13 (memory released at close; forked JVM with a small heap): in a child JVM whose heap
  cannot hold the IN_MEMORY state of many databases at once, repeatedly open a database, create
  an IN_MEMORY resource, commit content of a sizeable fraction of that heap, and close the
  database. The loop completes without OutOfMemoryError. In a second child with the same heap,
  an otherwise identical loop that keeps every database open fails with OutOfMemoryError, which
  proves the bound bites. A third child with the same heap keeps one database open and
  repeatedly creates, fills and removes an IN_MEMORY resource under a new name. Its loop also
  completes (clause 8). The children isolate the failure from the shared test JVM.
- AC14 (backup): back up a mixed database holding a committed FILE_CHANNEL and a committed
  IN_MEMORY resource, then restore it to a new location and open it.
  - The backup's result lists the IN_MEMORY resource as not backed up.
  - The restored FILE_CHANNEL resource is complete: every revision reads as in the source.
  - The IN_MEMORY name is absent there, as in AC4.

### TXN-701 — Async flush semantics

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AfterCommitState.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AfterCommitState.java); [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`awaitPendingAsyncFlush`
- **Verified by:** [AsyncAutoCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncAutoCommitTest.java); [AsyncFlushColdReopenVersioningTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushColdReopenVersioningTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test mapping inspected; execution and full conformance remain to be established.

KEEP_OPEN_ASYNC_FLUSH intermediate thresholds MUST write pages without publishing a revision. Final explicit commit MUST publish content equivalent to the synchronous operation stream. A crash before that commit may lose the unacknowledged work.

### TXN-702 — Abandoned prewrites

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`close`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`createPageTransaction`
- **Verified by:** [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java):`unboundCloseBlocksSuccessorUntilAbortedTrieTailIsSafeToReuse` **Planned:** AsyncTailRecoveryConformance: abort after page append, warm and cold reopen, sibling resource active, assert no stale page.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Teardown now retains a marker or identity-bound recovery obligation; the next admission repairs the durable tail before cache reuse. AsyncFlushFailurePathTest covers stale-page removal and successor reads.

An abandoned persistent async-flush tail MUST become unreachable and be repaired at the next writer admission before offsets are safely reused. Teardown MUST retain a recovery signal rather than relying on unsafe cache reuse.

### TXN-703 — Depth-one async commit pipeline

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`asyncCommitInternal`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`hardenAndPublish`
- **Verified by:** [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java); [AsyncCommitFrontierOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AsyncCommitFrontierOrderingTest.java); [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java):`inlineDoubleFailureReleasesSlotAndRetainsPrimary` **Planned:** AsyncPipelineConformance: barrier-observed successor overlap; successor-construction plus inline-hardening double failure; submission rejection; assert balanced slot and original cause.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Successor setup/submission fallbacks now fence and retire the predecessor while preserving the first failure and releasing the async slot once. NodeTransactionAbortLifecycleTest covers double failures; DatabaseNotificationLifecycleTest covers delivery without blocking successor mutation.

KEEP_OPEN_ASYNC_COMMIT MUST have at most one hardening in flight. Its successor MUST be based on completed phase-one bytes and may overlap independent work with hardening. Commit/rollback/close MUST fence the predecessor before depending on its durable frontier. Every submission, rejection and inline fallback path MUST release the async slot exactly once.

### TXN-704 — Unknown async outcome fences the resource

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`awaitPendingAsyncCommit`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`asyncCommitInternal`
- **Verified by:** [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** AsyncOutcomeFenceConformance: repeated await/mutate/commit/rollback after uncertain force, multiple handles to the same resource, sibling-resource independence, cleanup failure and reopen; assert no further durability attempt or new write admission.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Existing async poisoning is writer-oriented and does not establish a resource-wide uncertainty gate with distinct outcomes.

Async durability uncertainty MUST latch the writer and resource against further writes until reopen/recovery resolves the outcome. Mutation, commit and rollback MUST NOT bypass the fence or replay hardening; safe close MUST preserve recovery responsibility. The first durability error MUST remain the cause of CommitOutcomeUnknownException, and cleanup failures MUST NOT replace it. Known pre-point failures follow TXN-305; known durable cleanup failures follow TXN-307 instead of this uncertainty rule.

### TXN-705 — Unknown durability outcome and no fsync retry

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`hardenCommit`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`hardenAndPublish`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`asyncCommitInternal`; [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`writeUberPageReference`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists`
- **Verified by:** No existing test mapped. **Planned:** UnknownCommitOutcomeConformance: force/DSYNC faults with both persisted and lost storage outcomes, assert exactly one attempt at the failed durability operation, resource-wide rejection across handles and async fallback/close/rollback, then cold recovery and explicit N-present/N-absent result.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** CommitOutcomeUnknownException and the resolved-outcome API do not exist at this baseline. Current hardening/cleanup errors share catches, and async fallback paths require explicit no-retry audit. Existing beacon-centric recovery must be aligned with the durable-record commit point.

If durability cannot be determined, including an I/O error during harden/fsync, the API MUST throw CommitOutcomeUnknownException and MUST stop accepting writes on that resource. It MUST NOT retry the failed fsync/force, whether directly, through async fallback, rollback, close or another handle. The application MUST NOT retry the commit while its outcome is unknown. Reopen MUST run recovery; afterwards candidate revision N MUST exist completely or not at all, and the API MUST report which outcome occurred. A proven pre-point failure is an ordinary failure (TXN-305); a known durable commit is success despite cleanup failure (TXN-307).

### TXN-801 — Pre-commit hooks

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [PreCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PreCommitHook.java); [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [DatabaseHandle.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseHandle.java):`close`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`close`
- **Verified by:** [DatabaseNotificationLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseNotificationLifecycleTest.java):`preHookVetoRemainsSynchronousAndRetryable`, `allLifecycleEntryPointsRejectCallbackReentry`, `timedCommitResumesAfterTemporaryAdmissionRefusal`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Synchronous veto, callback guards and skipped scheduled ticks have source-mapped regressions. Full default-CI conformance is not asserted here.

Pre-commit hooks MUST run synchronously inside the writer transaction before durability; their permitted mutations participate in that commit. A hook MUST NOT end a transaction of the same database. Reentrant commit, rollback, truncate or close, including through another resource session or database handle, MUST immediately throw `IllegalStateException` before taking a lock or waiting. A pre-hook failure before durability follows TXN-305. Temporary notification retirement MAY reject hook admission before publication; a scheduled commit MUST preserve later ticks after this transient refusal.

### TXN-802 — Post-commit dispatcher and view

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [PostCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PostCommitHook.java):`postCommit`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`asyncCommitInternal`
- **Verified by:** [DatabaseNotificationLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseNotificationLifecycleTest.java):`asyncDurabilityAndBlockedDeliveryDoNotBlockSuccessorOrWriterClose`, `notificationViewUsesDurableRootWhilePendingCleanupIsBlocked`; [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** The dispatcher and independent committed views are implemented. Delivery is best effort under TXN-805; durable replay/resume remains follow-up work.

Post-commit notifications MUST be asynchronous on a dedicated per-database dispatcher, ordered per resource. Every notification MUST carry its resource-local committed revision number. The dispatcher MUST supply a fresh read-only view of exactly that committed revision, initially at the document root. Commit MUST NOT wait for listener delivery or execute callbacks on the committing thread. Listener failure MUST NOT roll back durable data or poison its successor. No mutable successor transaction may be passed as the view. Delivery and overflow follow TXN-805; explicit listener exceptions and close follow TXN-803/806.

Opening a resource that needs no derived initialization MUST NOT retire notification admission,
including a cold JSON open with valid-time paths but no declared derived index. Direct and handle
opens MUST preserve subsequent pre-hook and notification delivery while an unrelated listener is
active. Actual initialization MUST revalidate its need after quiescence and before changing state.
Regression mapping: `DatabaseNotificationLifecycleTest.sessionOpenWithoutDerivedDeclarationsPreservesAdmission`
covers cold/warm direct/handle opens; `eagerDerivedRebuildQuiescesOutsideSessionAdmission`
covers actual initialization.

### TXN-803 — Bounded listener shutdown

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [DatabaseHandle.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseHandle.java):`close`; [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`close`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`close`
- **Verified by:** [DatabaseNotificationLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseNotificationLifecycleTest.java):`unreturnedDeliveryRetainsReadersDropsQueueAndAllowsRetry`, `blockedPreHookCannotPinAnExternalCloserForever`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Bounded retirement retains active callback dependencies for retry instead of freeing them. Timeout configuration is owned by CONF-201; this mapping does not certify every boundary value.

Database, handle and session close MUST stop notification admission and wait at most the configured timeout for admitted delivery. At the bound they MUST abandon remaining queued work with a logged warning. If an active callback still owns readers, close MUST report timeout and retain those readers and their storage/registration dependencies for retry; it MUST NOT free state under that callback. Overlapping retirement scopes MUST keep admission paused until the final scope leaves. Publications from other still-open sessions MAY be deferred during retirement, subject to TXN-805; final database close discards them. Writer close MUST fence its physical workers but MUST NOT wait for independent post-notification views. Configuration is owned by CONF-201.

### TXN-804 — Close from every callback context

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [DatabaseHandle.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseHandle.java):`close`; [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`close`; [Databases.java](../bundles/sirix-core/src/main/java/io/sirix/access/Databases.java); [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`close`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`truncateTo`
- **Verified by:** [DatabaseNotificationLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseNotificationLifecycleTest.java):`allLifecycleEntryPointsRejectCallbackReentry`, `closedHandleCreationRejectsCallbackReentry`, `closingHandleRejectsCallbackBeforeItsClosingWait`, `callbackRejectsBeforeWaitingOnAnOuterMutationGate`, `callbackScopeRestoresItsCallerAfterSuccessOrFailure`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Same-database guards cover handle, session, writer, page, mutation-gate and manager/shutdown forwarding. Source-mapped regressions are not a claim of complete default-CI conformance.

Lifecycle calls from any pre-commit hook or post-commit listener targeting the same database, including another handle, session or writer, MUST immediately throw `IllegalStateException` before lock acquisition or waiting. This includes an already-closing handle, mutation-gate entry, page-writer truncation, database-manager and shutdown forwarding. Callback rejection takes precedence over resource-creation argument validation and an already-closed target handle, and its diagnostic MUST identify the callback restriction. A dispatcher MUST NOT wait for itself. Independent read-only work and lifecycle operations on a different database remain permitted.

### TXN-805 — Revision-token replay and bounded queue

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [DatabaseNotifications.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseNotifications.java):`submit`, `retireHistory`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`setLastCommittedUberPage`
- **Verified by:** [DatabaseNotificationLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseNotificationLifecycleTest.java):`saturatedNotificationsDoNotBlockDurableCommits`, `capacityCoversMixedBacklogsAndAbandonment`, `deferredPublicationPreventsHistoryReuse`, `deferredHistoryAdmissionIsScopedToRemovedRevisions`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** The later bounded, non-blocking queue decision supersedes mandatory replay in this change. Resume registration, durable replay and HistoryLostException are deferred; revision reuse after truncation requires a future timeline-aware token.

The dispatcher MUST bound retained notification work across the active job, executor queue and retirement-deferred jobs together. Overflow MUST drop the new job with a warning, without waiting for consumers, running callbacks on the committing thread or vetoing durable publication. Each admitted job runs at most once; source closure, submission failure, timeout or shutdown MAY prevent delivery. Publication can be observed more than once, so handlers SHOULD be idempotent. Hooks expose the committed revision through their view; revision listeners receive it directly. Revision numbers can be reissued after truncation and MUST NOT be presented as durable replay cursors. Destructive history maintenance MUST refuse before invalidating a revision required by a deferred job for that resource, preserving history and work for retry after delivery. Replay/resume is outside this change. Capacity configuration is owned by CONF-201.

### TXN-806 — Listener failure and callback view ownership

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [PostCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PostCommitHook.java):`postCommit`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [NodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeReadOnlyTrx.java):`close`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`beginNodeReadOnlyTrx`
- **Verified by:** [DatabaseNotificationLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseNotificationLifecycleTest.java):`notificationErrorCannotVetoDurabilityOrSkipTheNextView`, `unreturnedDeliveryRetainsReadersDropsQueueAndAllowsRetry`; [NodeTransactionAbortLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/NodeTransactionAbortLifecycleTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Dispatcher-owned revision views and logged failure isolation are implemented. No automatic callback retry or explicit resume API is provided.

A listener exception MUST be logged and MUST NOT trigger automatic retry of that failed delivery. A delivery means a callback invocation, not an acknowledgement of successful application processing. The dispatcher MUST open and close the read-only revision view; it MUST be valid only during that callback and MUST NOT be retained or used afterwards. Retained references MUST fail safely rather than read a mutable successor or freed storage. A callback still running at the close deadline retains backing ownership under TXN-803 until a later retirement attempt can complete.

## Conditional write preconditions (TXN-311)

`R` is the token revision, `head` the head revision the writer is based on, and
`stamp(x)` the last-modified revision of record `x` in the head revision (VER-206).
An insertion point is the triple (parent `P`, anchor `A`, side `s`), where `A` is the
node the insertion is expressed relative to and `s` the side it is inserted on; for a
first-child or last-child insert `A` is `P` and the neighbour position is `P`'s first
or last child, and for a sibling insert the neighbour position is `A`'s sibling on `s`.
The context records are `A` only when `A != P`, and the insertion-side neighbour `B`
when present. `P` supplies the context stamp only when the container is empty.
For XML, a subtree includes its root, its descendants, and the attribute and namespace
records owned by every included element; the subtree predicate MUST scan those records.
Attribute or namespace membership changes MUST stamp the owning element (VER-206),
so removal remains visible even though the removed record is absent at `head`.

| Target kind | Rejected when | Explicitly not a conflict |
|---|---|---|
| Update, delete or replace of node `N` | `N` is absent at `head`; or `stamp(x) > R` for any record `x` in `N`'s subtree at `head`, including `N` and, for XML, attribute/namespace records of every included element | any change outside `N`'s subtree, including to `N`'s ancestors, siblings or their subtrees |
| Insert at insertion point (`P`, `A`, `s`) | `P` or `A` is absent at `head`; or `A != P` and (`A`'s parent at `head` is not `P` or `stamp(A) > R`); or the neighbour `B` on side `s` exists at `head` and (`B`'s parent is not `P` or `stamp(B) > R`); or `P` is empty at `head` and `stamp(P) > R` | a changed child count of `P`; an insert, delete or update elsewhere under `P`, including at the other end of the same array or object, provided it leaves the non-parent anchor (`A != P`) and occupied insertion-side neighbour unchanged in value, name, structural links and attribute/namespace membership |
| Whole document root, or a write with no single node target | `head > R` | nothing; this is the deliberately coarse resource-level case |

Every target kind is rejected additionally when the token's incarnation id differs from the
resource's current incarnation id, when `R > head`, or when `R < lastRevertRevision`. A rejection
names the target kind, the token's incarnation id and revision, and the resource's current incarnation id
and head revision. Weak entity tags are rejected by the interfaces that carry the token
(REST-204).

Informative: the predicates are evaluable from the head revision alone. For an update or
delete, walk the target subtree, including every included XML element's attribute and
namespace records, and stop at the first record with a larger stamp. Membership
removal is detected through the surviving owning element's stamp (VER-206).
For an insert, check `P` and `A` exist and, only when `A != P`, check that `A` is still
under `P` and `stamp(A) <= R`. If the insertion-side neighbour `B` exists, check that it
is under `P` and `stamp(B) <= R`. Relinking either context record stamps it, so these
checks establish the unchanged boundary without opening revision `R`. An absent
neighbour does not require `stamp(P)` while `P` is non-empty: after the last child or
before the first child, the unchanged child's insertion-side link proves that the
position is still empty. Only an empty `P` uses `stamp(P) <= R`; changing between
empty and non-empty changes its first-child link. A value, name, structural-link or
attribute/namespace membership change in either context record, including an
opposite-side link change, remains a conservative conflict even if the insertion-side
link is unchanged. Changes to
`P`'s child count and edits elsewhere under `P` that leave those context records
unchanged do not conflict; neither do derived hash or count changes (VER-206).

Rationale (informative): node hashes are not used to decide preconditions, even on a resource
that stores them, because the rolling hash is insensitive to exactly the changes a precondition must
catch. [AbstractNodeHashing.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeHashing.java):`rollingAdd`
sets an ancestor's hash to its previous value plus `PRIME` times the added child's hash, and
`rollingUpdate` maintains that same sum, so a container's hash is an order-insensitive weighted sum
of its children. Reordering an array's items leaves every ancestor hash unchanged, and moving a node
from one sibling subtree to another at the same depth multiplies the same term by the same power of
`PRIME` on both sides, so their common ancestor is unchanged too. These are not improbable collisions
but systematic ones produced by ordinary editing, so a hash comparison would accept a stale replace
and silently undo a concurrent reorder or move. The stamp predicate has no such blind spot: a reorder
or a move rewrites links and therefore stamps the records it touches (VER-206).

Rationale (informative): a revision number is a monotone logical clock for the resource, so a token
naming one is the same device as etcd's `mod_revision`, where a transaction compares the revision of
the keys it touches rather than of the whole store, and as Elasticsearch's `if_seq_no` per document.
Kubernetes' `resourceVersion` is the stricter whole-object form, rejected here because it would make
every write to a large document conflict. The incarnation id is the Kubernetes `uid` half of that pair: a
`resourceVersion` is only meaningful together with the `uid` of the object incarnation it came from,
because a deleted and recreated object restarts versioning, and truncate and whole-resource
replacement do exactly that to a Sirix resource. An independent cryptographically secure 64-bit draw
at creation, truncate, replacement and restore avoids allocator state being reset by database deletion
or backup restore; reproduction of a particular old id has probability 2^-64 (FMT-209). Paying nothing at write time and deciding on demand is what
distinguishes this from a stored per-subtree token such as the retired node hashes: the hash had to be
maintained up every ancestor on every commit, whereas the stamps TXN-311 reads are a by-product of
writing the records anyway.

## Model and error cases

A persistent commit has four observable stages: private mutation, appended but unacknowledged
pages, durable revision-record commit point, published head. The three outcomes are known pre-point failure (ordinary error, rollback/retry), known committed N (normal success despite cleanup faults), and unknown durability (CommitOutcomeUnknownException, writes stopped, no fsync retry). Cleanup and listener delivery occur after durability;
their failures cannot make the durable stage disappear. The file/device durability assumption
excludes lying flush implementations and arbitrary destruction of both valid copies.

Default source mappings use the core TestCore lanes; REST mappings use TestRestApi. Crash
harnesses are opt-in with `sirix.crash.run=true`. Mapping a suite does not imply every clause
is asserted. Planned conformance cases intentionally include double failures, cold reopen,
interruption, stalled workers and stale-forward secondary beacons.

## Deferred implementation and verification

- TXN-602: marker-directory persistence and the two-crash model need analysis. The future
  CommitRecovery model includes marker durability independently from page durability.
- TXN-802–806: durable replay/resume and timeline-aware token registration remain follow-up work. The implemented bounded queue, overflow policy and callback ownership are specified in those requirements; configuration is owned by CONF-201.
- TXN-102: existing 5-second node-writer and 20-second page-writer admission bounds are recorded; any API consolidation is outside this P0 draft.
- TXN-311: the check, its exception, the subtree walk and its work-budget assertion are unimplemented. It depends on VER-206 stamping and on the FMT-209 incarnation id and last-revert metadata, including generation at resource creation, TXN-404 truncate, REST-210 replacement and TXN-613/614 restore.
- Open (CONV-101), recorded here because TXN-311's rationale establishes it: the rolling hash does not
  reflect child order or a node's placement among same-depth siblings, so an equal stored hash does
  not imply equal content. Every other consumer that treats hash equality as content equality needs
  auditing against that, in particular the hashed diff shortcut in
  [AbstractDiff.java](../bundles/sirix-core/src/main/java/io/sirix/diff/AbstractDiff.java) and
  [DiffFactory.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffFactory.java). No behavior
  is chosen here and no claim is made about which consumers are affected; the audit is filed
  separately.
- TXN-404: fail-fast reader identification/admission fencing is decided; implementation and race tests remain pending. TXN-613/614 define the decided backup start snapshot and fail-fast drop policy.
- The three commit outcomes and CommitOutcomeUnknownException, IN_MEMORY marker exclusion, current-head revert append,
  copying `$derived` and the hook model are decided, not open alternatives.

Planned models: CommitRecovery checks acknowledged survival, atomic candidate revisions,
no stale-offset cache reads and no discarded-revision resurrection. AsyncPipeline checks
exactly-once permit release, no worker use-after-free and callback-independent close.
Neither model nor a crash CI lane is delivered by this chapter.

## Traceability

| Requirement | Existing or planned verification | Status |
|---|---|---|
| TXN-101 | [DatabaseOwnershipTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseOwnershipTest.java) | partial |
| TXN-102 | [ResourceSessionTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/ResourceSessionTest.java); [AbstractResourceSessionWriteFailureCleanupTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AbstractResourceSessionWriteFailureCleanupTest.java) **Planned:** WriterAdmissionConformance: same resource through two handles, independent resource progress, timeout and interruption on both admission paths. | partial |
| TXN-103 | [CrashRecoveryOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryOrderingTest.java):`recoveryTruncatesBeforeTheRecoveringTransactionReadsAnything` | partial |
| TXN-104 | [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java); [AsyncAutoCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncAutoCommitTest.java) **Planned:** AsyncAdmissionConformance: every storage/mode/timer combination, assert unchanged head and reusable permit on rejection. | partial |
| TXN-201 | [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java):`testSnapshotIsolation`; [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java):`testWriterModifiesReaderUnaffected`; [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java):`concurrentReader_survivesWriterCrashAndRecovery` | partial |
| TXN-202 | [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java):`testReadOnlyDuringWrite`; [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java):`testReadBeforeCommit`; [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java):`testReadAfterCommit` | partial |
| TXN-203 | [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java):`asyncCommit_createsQueryableRevisions`; [AsyncCommitFrontierOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AsyncCommitFrontierOrderingTest.java) **Planned:** PublicationBarrierConformance: pause before record durability, repeatedly open readers, cross the commit point, inject cleanup failure and verify exactly the old then new committed view. | designed |
| TXN-204 | [JsonRoundTripPropertyTest.java](../bundles/sirix-core/src/test/java/io/sirix/property/JsonRoundTripPropertyTest.java):`committingNewRevisionsNeverAltersEarlierRevisions`; [JsonModelBasedOracleTest.java](../bundles/sirix-core/src/test/java/io/sirix/property/JsonModelBasedOracleTest.java):`fixedSeedOperationSequenceMatchesOracle` | partial |
| TXN-301 | [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`failedCommitKeepsTransactionUsableAndGuardsClose`; [MultipleCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/MultipleCommitTest.java) **Planned:** RevisionOutcomeAllocationConformance: empty commits, known pre-point failure plus rollback/retry, known durable cleanup failure and unknown outcome resolved both present and absent; assert revision numbers and no duplicate retry. | designed |
| TXN-302 | [PowerLossSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/PowerLossSimulationTest.java):`anyPowerLossInstantPreservesAcknowledgedCommits`; [CrashRecoveryInjectionTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/CrashRecoveryInjectionTest.java):`killedWriterNeverCorruptsAcknowledgedData` **Planned:** CommitPointOutcomeConformance: inject before/at/after revision-record durability with independent crash storage; verify complete N after every success, no N for known pre-point failure, and both atomic recovery outcomes for uncertainty. Include synchronous/asynchronous and all persistent profiles. | designed |
| TXN-303 | [PowerLossSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/PowerLossSimulationTest.java):`anyPowerLossInstantPreservesAcknowledgedCommits` | partial |
| TXN-304 | [PowerLossSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/PowerLossSimulationTest.java) | designed |
| TXN-305 | [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`failedCommitKeepsTransactionUsableAndGuardsClose`; [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** KnownPrePointFailureConformance: pre-hook, serialization and known-not-durable publication failures, retained edits, rollback then retry, and an uncertainty control that must instead stop resource writes. | designed |
| TXN-306 | [WriterCatalogueRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/WriterCatalogueRecoveryTest.java); [IndexCatalogueUnchangedCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/IndexCatalogueUnchangedCommitTest.java) | partial |
| TXN-307 | [FaultInjectingWriterTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/fault/FaultInjectingWriterTest.java) **Planned:** PostDurableCleanupConformance: inject each intent-log clear, marker deletion, guard cleanup and rebind failure after proven record durability; assert normal result N, logged cause, immediate repair flag, closed writer, preserved N and repair on reopen; combine cleanup failures without masking success. | designed |
| TXN-308 | No existing test mapped. **Planned:** AtomicCommitGuardConformance: attempt sync/async commits inside nested scopes, inject post-publication failure, assert rejection then successful rollback recovery. | gap |
| TXN-309 | [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`autoCommitDoesNotSplitJsonReplaceObjectRecordValue`; [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`autoCommitDoesNotSplitXmlMoveSubtree` | partial |
| TXN-310 | [CommitTimestampPrecisionConformanceTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/CommitTimestampPrecisionConformanceTest.java):`aTieAndABackwardsClockStepAreAdjustedToOneMicrosecondAfterThePredecessor`, `revertUsesTheLatestCommittedTimestamp` (JSON/XML, both persistent backends, warm and cold), `wallClockCommitsStrictlyIncreaseAndKeepMicrosecondPrecision`, `retryUsesItsTimestampAfterSyncOrAsyncPhaseOneFailure`, `retryAfterAutomaticAsyncPhaseOneFailureUsesExplicitTimestamp`; [CommitTimestampLateFailureTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/CommitTimestampLateFailureTest.java) (post-root final-flush failures, warm index, history and lookup before cold reopen, both persistent backends and synchronous/asynchronous phase one); [RevisionIndexIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexIntegrationTest.java); [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java) **Planned:** StrictCommitTimestampConformance: controlled repeating/regressing live clocks and max(previous+1us,clock) oracle; import equal/decreasing/increasing source sequences with conditional adjustment oracle; original metadata queryability, preserved order, first and unchanged timestamps, live commit after import, sync/async and JSON/XML reopen/recovery without wall-clock timing assertions. | designed |
| TXN-311 | No existing test mapped. **Planned:** ConditionalWritePreconditionConformance: every target kind, interleaved touching and non-touching commits, concurrent reorder and same-depth move, deleted target, reused node key after revert, tokens from before a truncate and a replacement, token above the head, pruned token revision, document root, empty foreign commit and accepted retry, each with hashing enabled and disabled; **Planned:** ConditionalWritePreconditionBudgetTest record-read bounds (CONV-301). | proposed |
| TXN-401 | [RollbackStateTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/RollbackStateTest.java); [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** RollbackMatrixConformance: JSON/XML, all versioning types, sync/async failure, compare serialization, indexes and diff after retry. | partial |
| TXN-402 | See [requirement mapping](#txn-402--rollback-failure). | designed |
| TXN-403 | [XmlRevertTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/XmlRevertTest.java); [JsonIdentityHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/json/JsonIdentityHistoryTest.java); [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt):`optionalHeadAndMessageAndRevertToHeadAppendARevision`; [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt):`prunedTargetFailsWithoutAppendingOrReplacingTheTarget` **Planned:** RevertParityConformance: Java/REST/MCP, JSON/XML, current head/old head/pruned target, unchanged history after cold reopen. | partial |
| TXN-404 | [ExplicitRollbackCrashSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/ExplicitRollbackCrashSimulationTest.java); [NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java); [RAMStorageTruncateToTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/ram/RAMStorageTruncateToTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java) **Planned:** TruncateReaderFenceConformance: open readers below/at/above target across handles, dispatcher views and independent page-read paths; blocked set named in immediate error, unchanged bytes/history/state, no wait; reader-admission race; close conflicting readers then successful truncate, JSON/XML and supported backends. | designed |
| TXN-501 | [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`failedCommitKeepsTransactionUsableAndGuardsClose` | partial |
| TXN-502 | See [requirement mapping](#txn-502--close-preserves-the-classified-async-outcome). | designed |
| TXN-503 | See [requirement mapping](#txn-503--independent-cleanup). | partial |
| TXN-504 | See [requirement mapping](#txn-504--worker-ownership-fence). | partial |
| TXN-505 | See [requirement mapping](#txn-505--session-abort). | designed |
| TXN-601 | See [requirement mapping](#txn-601--marker-identity). | partial |
| TXN-602 | [PowerLossSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/PowerLossSimulationTest.java); [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java):`tornWriteWithoutMarker_lastCommittedRevisionStillReadable` **Planned:** MarkerLossCrashConformance: first append before marker persistence, lost directory entry, stale-forward SECONDARY then second crash. | designed |
| TXN-603 | See [requirement mapping](#txn-603--marker-removal). | partial |
| TXN-604 | See [requirement mapping](#txn-604--recover-from-durable-disk-frontier). | designed |
| TXN-605 | [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java); [AsyncCommitFrontierOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AsyncCommitFrontierOrderingTest.java) | partial |
| TXN-606 | See [requirement mapping](#txn-606--failed-recovery-refuses-admission). | designed |
| TXN-607 | See [requirement mapping](#txn-607--in-memory-resources-never-write-markers). | partial |
| TXN-608 | See [requirement mapping](#txn-608--marker-creation-failure-obligation). | partial |
| TXN-609 | [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java):`concurrentReader_survivesWriterCrashAndRecovery` **Planned:** ReadAdmissionRecoveryConformance: marker plus unreachable tail; assert unchanged tail size and marker on reads, then repaired state on writer admission. | partial |
| TXN-610 | [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java):`tornWriteWithoutMarker_lastCommittedRevisionStillReadable`; [PreallocatedCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/PreallocatedCommitTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java) | partial |
| TXN-611 | [InterruptedFirstCommitRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/InterruptedFirstCommitRecoveryTest.java); [InterruptedFirstCommitRecoveryStoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/InterruptedFirstCommitRecoveryStoreFeaturesTest.java) | partial |
| TXN-612 | [DerivedIndexCrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedIndexCrashRecoveryTest.java):`restartsAtDurableDerivedFrontier`; [DerivedRevisionMappingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedRevisionMappingTest.java) | partial |
| TXN-613 | [BackupDerivedResourceTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupDerivedResourceTest.java); [BackupRestoreTemporalAggregateTest.java](../bundles/sirix-query/src/test/java/io/sirix/query/BackupRestoreTemporalAggregateTest.java); [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java); [DerivedRevisionMappingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedRevisionMappingTest.java) **Planned:** DerivedBackupConformance: source/derived revisions advancing during copy, backup start barrier, copy hidden resources at pinned heads, restore elsewhere and compare history/mappings/queries before and after catch-up; interrupted copy rejects partial target. | partial |
| TXN-614 | [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java); [DatabaseOwnershipTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseOwnershipTest.java) **Planned:** BackupStartSnapshotConformance: barrier at start, concurrent create/commit/drop, assert exact resource/head manifest; pinned drop names backup in progress and preserves all state; later-created resource excluded; two overlapping backups and failed-copy cleanup; restore and compare every pinned revision plus hidden derived state. | designed |
| TXN-615 | [RAMStorageCommittedRevisionTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/ram/RAMStorageCommittedRevisionTest.java) (AC1); [LocalDatabaseInMemoryLifetimeTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/LocalDatabaseInMemoryLifetimeTest.java) (AC2-AC5, AC8, AC9, AC11); [DatabasesInMemoryLifetimeTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabasesInMemoryLifetimeTest.java) (AC6, AC7, AC12, AC15); [InMemoryLifetimeForkedJvmTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/InMemoryLifetimeForkedJvmTest.java) (AC10, AC13); [BackupManagerInMemoryResourcesTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupManagerInMemoryResourcesTest.java) (AC14) **Planned:** AC4 token bullet behind FMT-209/TXN-311/JAPI-111. | partial |
| TXN-701 | [AsyncAutoCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncAutoCommitTest.java); [AsyncFlushColdReopenVersioningTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushColdReopenVersioningTest.java) | partial |
| TXN-702 | See [requirement mapping](#txn-702--abandoned-prewrites). | partial |
| TXN-703 | See [requirement mapping](#txn-703--depth-one-async-commit-pipeline). | partial |
| TXN-704 | [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** AsyncOutcomeFenceConformance: repeated await/mutate/commit/rollback after uncertain force, multiple handles to the same resource, sibling-resource independence, cleanup failure and reopen; assert no further durability attempt or new write admission. | designed |
| TXN-705 | No existing test mapped. **Planned:** UnknownCommitOutcomeConformance: force/DSYNC faults with both persisted and lost storage outcomes, assert exactly one attempt at the failed durability operation, resource-wide rejection across handles and async fallback/close/rollback, then cold recovery and explicit N-present/N-absent result. | designed |
| TXN-801 | See [requirement mapping](#txn-801--pre-commit-hooks). | partial |
| TXN-802 | See [requirement mapping](#txn-802--post-commit-dispatcher-and-view). | partial |
| TXN-803 | See [requirement mapping](#txn-803--bounded-listener-shutdown). | partial |
| TXN-804 | See [requirement mapping](#txn-804--close-from-every-callback-context). | partial |
| TXN-805 | See [requirement mapping](#txn-805--revision-token-replay-and-bounded-queue). | partial |
| TXN-806 | See [requirement mapping](#txn-806--listener-failure-and-callback-view-ownership). | partial |
