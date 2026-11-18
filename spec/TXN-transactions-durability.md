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
- **Verified by:** [RevisionIndexIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexIntegrationTest.java); [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java) **Planned:** StrictCommitTimestampConformance: controlled repeating/regressing live clocks and max(previous+1us,clock) oracle; import equal/decreasing/increasing source sequences with conditional adjustment oracle; original metadata queryability, preserved order, first and unchanged timestamps, live commit after import, sync/async and JSON/XML reopen/recovery without wall-clock timing assertions.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** PageKind currently serializes clock/custom timestamps as milliseconds without the predecessor clamp. There is no complete history-import adjustment/original-metadata path. Captain decisions 002 and 010 define the separate live-clock and historical-import assignments under the same strict-order invariant.

For each new live commit on a resource timeline, its commit timestamp MUST equal `max(wall clock, previous commit timestamp + 1 microsecond)`. Commit timestamps MUST strictly increase even when the wall clock repeats or moves backwards. This applies to explicit, automatic and asynchronous commits, empty commits and commits after revert or reopen. History import instead MUST process source revisions in order: preserve the first source timestamp; preserve a later source timestamp if it is greater than the preceding effective timestamp; otherwise assign the preceding effective timestamp plus 1 microsecond and retain the original as queryable metadata (FMT-402). Import MUST adjust only where needed and MUST NOT replace historical timestamps with import-time wall-clock values. Subsequent live commits MUST use the final effective imported timestamp as their predecessor. The predecessor MUST come from the same resource timeline, not an unrelated resource or a historical state restored by revert. Effective timestamps MUST survive persistence/recovery at the required precision, so time lookup selects exactly one revision.

### TXN-311 — Conditional write preconditions

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [NodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeTrx.java); [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [NodeDelegate.java](../bundles/sirix-core/src/main/java/io/sirix/node/delegates/NodeDelegate.java):`getLastModifiedRevisionNumber`; [WriteLocksRegistry.java](../bundles/sirix-core/src/main/java/io/sirix/access/WriteLocksRegistry.java)
- **Verified by:** No existing test mapped. **Planned:** ConditionalWritePreconditionConformance: for each target kind in the table below, a token taken before an interleaved foreign commit that does and does not touch the target, a concurrent reorder and a same-depth move that leave ancestor hashes unchanged, a deleted target, a reused node key after revert, a token from before a truncate and from before a resource replacement, a token above the head, a pruned token revision, a document-root target, an empty foreign commit, and a rejected check followed by a successful retry; assert the rejection, the unchanged head and the committed result of the accepted write. Run each case with hashing enabled and disabled and assert the same verdicts. **Planned:** ConditionalWritePreconditionBudgetTest: record-read counters for a scalar target, a deep subtree and an insertion point (CONV-301).
- **Evidence class:** doc-only (unverified); decided target has unverified implementation gaps
- **Gap/note:** No conditional check exists at this baseline; REST's node-hash comparison (REST-204) is the only optimistic guard, it is unavailable with the 1.0 default `HashType.NONE` (CONF-104), and the rationale below records why it is not sound even when hashing is on. The predicate depends on VER-206 stamping and on the epoch and last-revert metadata of FMT-209, all decided targets with recorded implementation mismatches.

A write transaction MUST offer a conditional check that takes a token and a target and fails the write when the target changed since the token. The token MUST consist of the resource's history epoch and a revision number `R` of that epoch, on every resource; it MUST NOT carry a node hash, and a stored node hash MUST NOT decide any precondition, for the reason given in the rationale below. The check MUST be evaluated by the single admitted writer (TXN-102) against the head revision its pending revision is based on, after writer admission and before the mutation it guards; because one writer exists per resource, no concurrent commit can intervene between the check and the guarded mutation, so there is no time-of-check/time-of-use window. A failed check MUST leave the transaction, the resource, its metadata and its visible history unchanged, MUST NOT consume a revision number, and MUST report the resource's current epoch and head revision so the caller can re-read and retry. The conflict predicates in the table below are incorporated into this requirement. Two gates MUST precede them for every target kind: the check MUST fail when the token's epoch differs from the resource's current epoch (FMT-209), which is how truncate and resource replacement invalidate every token issued against the history they ended, and it MUST fail when `R` is greater than the head revision of that epoch. It MUST also fail, for every target kind, when `R` is less than the resource's last-revert revision (FMT-209), because a revert republishes older records with their older stamps and restores the older node-key frontier, so node keys allocated afterwards may denote different nodes. The predicates MUST be identical on resources with and without node hashing. The check MUST NOT require revision `R` itself to be readable: a pruned token revision (VER-303) MUST still be evaluable, failing only where the predicates say so. The check MUST be sound — it MUST NOT accept a write whose target changed since the token — and MAY be conservative where it cannot distinguish a change from a non-change, provided each conservative case is documented. Its work MUST be bounded by the target: at most the records of the target subtree in the head revision plus a constant, O(1) records for a record-sized target and for an insertion point, with no scan proportional to resource size, revision count or the distance between `R` and the head; that bound is an official work budget under CONV-301. An implementation MAY use any cheaper equivalent evidence that yields the identical verdict.

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
- **Verified by:** [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** RollbackMarkerConformance: failure at every reconstruction stage, closed rollback while a new writer owns the resource, marker and permit assertions.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current rollback clears the marker before all reconstruction completes; complete failure/closed-writer fencing remains pending.

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
- **Gap/note:** Captain chose fail-fast reader protection. The current truncate path checks backend support but does not enumerate/atomically fence all readers of removed revisions or report them. Existing crash tests are not proof of that admission contract. The epoch allocation is likewise unimplemented: the current path rewinds the uber-page counter and the next commit reissues a removed number, which is permitted (VER-101) only because the epoch, not the revision number, carries token identity across a truncate.

truncateTo(r) MUST allocate and durably publish a new resource history epoch (FMT-209) before removing any data, so that every conditional-write token issued against a removed revision fails under TXN-311 even though later commits reissue its revision number. Publishing the epoch before the removal is required rather than convenient: a crash between the two leaves an epoch that is ahead of the data, which only rejects tokens that would otherwise have been accepted, whereas the opposite order could accept a token from a destroyed timeline. truncateTo(r) MUST validate that r is retained. If any open reader holds a revision that truncation would remove, it MUST fail immediately with an error identifying those readers and their revisions, without waiting for them and without changing transaction state, metadata, files or visible history. Reader admission and the eligibility check MUST be coordinated so a new reader cannot enter the removed range between validation and mutation. Readers at or below r MUST NOT be rejected solely for being open. When eligible, truncate MUST durably publish the retained frontier before removing later data/directory records; a crash MUST leave the old head or r, never a partly readable history. Unsupported backends MUST reject before mutation.

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
- **Verified by:** [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** AsyncCloseOutcomeConformance: known pre-point failure, durable cleanup failure, uncertain force, dirty successor and repeated close; verify result classification, repair flag, no fsync retry and safe ownership.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current close can log away async failure and delete the marker; the three-way outcome/repair contract is not implemented.

Close MUST fence in-flight hardening before freeing its owners and MUST preserve its outcome classification. A known durable N with cleanup failure follows TXN-307: log, flag repair and close cleanly without reporting a commit failure. An unknown outcome MUST remain CommitOutcomeUnknownException with resource writes stopped and recovery responsibility intact; close MUST NOT retry fsync or reinterpret it as a known failure/success. Independent teardown is attempted with safe ownership. Listener shutdown follows TXN-803.

### TXN-503 — Independent cleanup

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`close`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`close`
- **Verified by:** [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java); [AbstractResourceSessionWriteFailureCleanupTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AbstractResourceSessionWriteFailureCleanupTest.java) **Planned:** TeardownConformance: inject failures in reader, log, backend, pool and session children; verify each cleanup is attempted without freeing live-worker state.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Page-writer cleanup has coverage; resource-session close can still stop at an earlier failing child on this baseline.

Transaction/session teardown MUST attempt each independent owner even when another cleanup fails, preserve the first failure with later failures suppressed, and be idempotent after success. A retry after failed teardown MUST complete only the remaining safe work.

### TXN-504 — Worker ownership fence

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`close`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`awaitPendingAsyncFlush`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`close`
- **Verified by:** [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** WorkerOwnershipConformance: stalled worker, interrupted wait, failed cleanup and retry; assert no successor admission or use-after-free before exit.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current node close releases writer ownership before checking scheduler termination and ignores its timeout result.

A writer MUST NOT release its write permit or free worker-accessible memory until its mutation/flush/hardening workers have terminated. If termination is not established, ownership MUST remain fenced for retry. A listener dispatcher MUST NOT own a mutable writer or block this fence.

### TXN-505 — Session abort

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`close`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`rollback`
- **Verified by:** [JsonDatabaseLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonDatabaseLifecycleTest.java); [AbstractResourceSessionWriteFailureCleanupTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AbstractResourceSessionWriteFailureCleanupTest.java) **Planned:** SessionAbortConformance: JSON/XML known-uncommitted dirty work, known durable cleanup failure, uncertain async durability and rollback failure; assert each preserved outcome and no repeated fsync on close/reopen.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current session close attempts rollback of dirty JSON/XML writers; failure-resistant abort and recovery fences are not complete. The sample’s older claim that no session rollback exists does not describe this baseline.

Session close MUST safely discard known uncommitted dirty work without committing it, using rollback or an internal abort path. It MUST preserve known committed N under TXN-307 and MUST NOT roll back, retry fsync or claim absence for an unknown candidate under TXN-705. Uncommitted tails, unknown outcomes and incomplete repair MUST retain recovery responsibility; worker fences MUST precede owner release.

### TXN-601 — Marker identity

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`getCommitFile`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`commitWritePages`
- **Verified by:** [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java) **Planned:** MarkerIdentityConformance: empty/nonempty regular file, directory, creation failure and deletion failure.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Location exists; strict non-regular-path checks and persistent failure handling remain lifecycle work.

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
- **Status:** designed
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`hardenCommit`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`rollback`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`close`
- **Verified by:** [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java) **Planned:** MarkerLifecycleConformance: enumerate success and every failure exit, then reopen and recover.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current close deletes the marker; intended failure-preserving rule is not implemented throughout.

A persistent marker MUST be removed only after successful commit cleanup or successful rollback. Close, commit failure, failed rollback and teardown failure MUST NOT remove unresolved recovery responsibility. A successful recovery admission may leave the marker until the following successful commit/rollback.

### TXN-604 — Recover from durable disk frontier

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists`; [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`truncateTo`
- **Verified by:** [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java); [CrashRecoveryOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryOrderingTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java) **Planned:** RecoveryOutcomeConformance: complete durable N with stale beacon/session, staged incomplete record, unknown N both present and absent, sibling cache preservation, repeat recovery and crash during recovery; verify reported outcome before writer admission.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current recovery supplies the in-memory lastCommittedRev and uses beacon-based truncation. Record-point authority and an outcome-reporting API are missing; the physical durable-record evidence protocol remains engineering work, not permission to discard a known commit.

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
- **Verified by:** [AbstractResourceSessionWriteFailureCleanupTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AbstractResourceSessionWriteFailureCleanupTest.java) **Planned:** RecoveryFailureConformance: backend open, root discovery, truncate, catalogue/cache and marker failures; fresh reopen revalidates durable evidence, preserves error/stage and never repeats a failed force to infer success.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current createPageTransaction does not cover every temporary-writer failure exit. Existing writer-binding cleanup tests do not establish full recovery-stage cleanup.

Failure at any recovery stage MUST refuse writer admission, identify the marker path and stage, close temporary backend ownership, release the admission permit only when no worker still owns it, and preserve recovery responsibility. A later eligible reopen MUST resume recovery idempotently from fresh durable evidence. This MUST NOT retry a failed fsync/force in the poisoned resource instance or bypass TXN-705; ordinary writer admission remains stopped while durability is unknown.

### TXN-607 — In-memory resources never write markers

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`commitWritePages`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists`; [RAMStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/ram/RAMStorage.java)
- **Verified by:** [RAMStorageTruncateToTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/ram/RAMStorageTruncateToTest.java) **Planned:** InMemoryMarkerConformance: inject commit/rollback/close failures and reopen admission; assert no marker creation and no recovery wedge.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Captain call 2: never write the marker. Current commitWritePages creates it unconditionally and session recovery skips unsupported truncate; neither is the target contract.

IN_MEMORY resources MUST NOT create the filesystem commit marker on commit, preflush, rollback, abort or teardown failure. No persistent-recovery obligation may make an in-memory resource permanently unwritable. This exemption MUST NOT permit a persistent backend to skip required recovery.

### TXN-608 — Marker creation failure obligation

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`createPageTransaction`; [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`resourceUuid`
- **Verified by:** No existing test mapped. **Planned:** RecoveryIdentityConformance: marker creation failure, session close/reopen, same-path resource recreation and persisted identity mismatch.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** No process-local recovery-obligation registry is present on this baseline. The obligation is not itself durable across process exit.

For a persistent resource, failed marker creation MUST retain a process-local recovery obligation tied to the resource’s persisted identity and path. The next admission MUST re-establish the marker and recover. Deleting/recreating the resource MUST NOT transfer the old obligation to its new identity.

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
- **Verified by:** [DerivedIndexCrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedIndexCrashRecoveryTest.java):`restartsAtDurableDerivedFrontier`; [DerivedRevisionMappingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedRevisionMappingTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Crash suite is opt-in. Full cross-resource atomicity is not promised; source/derived revision mapping carries the relationship.

A derived resource MUST recover its own durable state and source-revision mapping, then catch up from the source’s durable frontier. Source revision numbers and derived revision numbers MUST NOT be assumed numerically equal. A crash between source and derived publication MUST leave a recoverable source of truth.

### TXN-613 — Backups include derived resources

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [BackupManager.java](../bundles/sirix-core/src/main/java/io/sirix/backup/BackupManager.java):`backupDatabase`; [BackupManager.java](../bundles/sirix-core/src/main/java/io/sirix/backup/BackupManager.java):`restoreDatabase`; [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`listResources`
- **Verified by:** [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java); [DerivedRevisionMappingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedRevisionMappingTest.java) **Planned:** DerivedBackupConformance: source/derived revisions advancing during copy, backup start barrier, copy hidden resources at pinned heads, restore elsewhere and compare history/mappings/queries before and after catch-up; interrupted copy rejects partial target.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current backup iterates listResources(), which hides $derived, and reads each current head only when its per-resource writer lock is acquired. It lacks the decided common start resource/revision pin set.

Backup MUST copy hidden `$derived` resources as well as visible resources from the pinned start snapshot in TXN-614, including settings, catalogues, revision files and source-revision mapping through each pinned head. It MUST omit transient lock/log/temp files. The copied derived source-frontier mapping MUST NOT refer beyond the pinned copied source head. Restore MUST preserve the copied derived state and permit normal catch-up; silently omitting derived resources or copying a later head is not conforming.

### TXN-614 — Backup start snapshot and fail-fast resource drop

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [BackupManager.java](../bundles/sirix-core/src/main/java/io/sirix/backup/BackupManager.java):`backupDatabase`; [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`removeResource`; [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`listResources`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`beginNodeReadOnlyTrx`
- **Verified by:** [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java); [DatabaseOwnershipTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseOwnershipTest.java) **Planned:** BackupStartSnapshotConformance: barrier at start, concurrent create/commit/drop, assert exact resource/head manifest; pinned drop names backup in progress and preserves all state; later-created resource excluded; two overlapping backups and failed-copy cleanup; restore and compare every pinned revision plus hidden derived state.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current backup enumerates visible resources then copies each under its own writer semaphore, choosing the head at copy time. LocalDatabase.removeResource rejects open sessions generically, but has no backup pin registry or required diagnostic. Exact start pins and admission/failure cleanup remain implementation work.

At backup start, backup MUST pin the set of resources and each resource’s current committed revision and MUST copy exactly that set at those pinned revisions. Resources created after the start MUST NOT enter the backup; commits after the start MUST NOT advance a copied head. Pins MUST keep the selected immutable revision data available until copying finishes or fails. Dropping a resource in any running backup’s pinned set MUST fail immediately with a clear "backup in progress" error, without waiting or changing anything. Snapshot capture and resource create/drop admission MUST be coordinated to avoid a membership race. Completion/failure MUST release only that backup’s pins; another active backup’s pins remain effective.

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
- **Status:** designed
- **Since:** 1.0
- **Code:** [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`close`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`createPageTransaction`
- **Verified by:** [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java):`unboundCloseBlocksSuccessorUntilAbortedTrieTailIsSafeToReuse` **Planned:** AsyncTailRecoveryConformance: abort after page append, warm and cold reopen, sibling resource active, assert no stale page.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current teardown uses cache invalidation/permit retention; admission-driven marker repair is the decided target.

An abandoned persistent async-flush tail MUST become unreachable and be repaired at the next writer admission before offsets are safely reused. Teardown MUST retain a recovery signal rather than relying on unsafe cache reuse.

### TXN-703 — Depth-one async commit pipeline

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`asyncCommitInternal`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`hardenAndPublish`
- **Verified by:** [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java); [AsyncCommitFrontierOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AsyncCommitFrontierOrderingTest.java) **Planned:** AsyncPipelineConformance: barrier-observed successor overlap; successor-construction plus inline-hardening double failure; submission rejection; assert balanced slot and original cause.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Normal overlap exists. Combined failure paths need lifecycle fixes; listener delivery must not destroy this overlap.

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
- **Status:** designed
- **Since:** 1.0
- **Code:** [PreCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PreCommitHook.java); [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [DatabaseHandle.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseHandle.java):`close`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`close`
- **Verified by:** [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java) **Planned:** HookLifecycleConformance: each lifecycle entry point from a pre-hook, JSON/XML, sync/async modes; assert immediate exception, no state change, retained dirty data and reusable ownership.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** The synchronous call exists; complete callback-context lifecycle rejection does not. Decision dated 16 November supersedes older synchronous post-hook/fence proposals.

Pre-commit hooks MUST run synchronously inside the writer transaction before durability; their permitted mutations participate in that commit. A hook MUST NOT end that transaction. Reentrant commit, rollback, truncate or close of the same transaction, resource session, database or database handle from a hook MUST immediately throw `IllegalStateException` before taking a lock or waiting. A pre-hook failure before durability follows TXN-305.

### TXN-802 — Post-commit dispatcher and view

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [PostCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PostCommitHook.java):`postCommit`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`asyncCommitInternal`
- **Verified by:** No existing test mapped. **Planned:** PostCommitDispatcherConformance: stalled listener for N while N+1 commits, thread separation, resource ordering and token identity, exact revision content, read-only ownership; deterministic work/lock counters show no listener wait or replay work on commit.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** Baseline still directly invokes postCommit(this). Dispatcher basics are being developed separately; this source snapshot does not contain the required durable-history replay, resume-token and callback-view lifecycle implementation.

Post-commit notifications MUST be asynchronous on a dedicated per-database dispatcher, ordered per resource, with at least one delivery invocation per retained committed revision while open under the replay contract in TXN-805. Every notification MUST carry its revision number as a resume token, scoped to its resource. The dispatcher MUST supply a read-only view of exactly that committed revision. Commit MUST NOT wait for, be backpressured by, or be slowed by listener delivery/replay; listener failure MUST NOT roll back an acknowledged commit or poison its successor. No mutable successor transaction may be passed as the view. Explicit listener exceptions and close follow TXN-803/806.

### TXN-803 — Bounded listener shutdown

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [DatabaseHandle.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseHandle.java):`close`; [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`close`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`close`
- **Verified by:** No existing test mapped. **Planned:** BoundedListenerCloseConformance: controlled clock/scheduler with default 5-second and custom bounds, never-returning listener, queued replay work, logged abandonment, safe view lifetime and later resume; JSON/XML and close-from-callback rejection.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** Default and ownership are decided. The current baseline has no dispatcher timeout configuration or safe timed-out callback/view retirement implementation; exact configuration key/range and cancellation mechanics remain engineering details.

Close MUST stop admitting new notifications and MUST wait at most the configured timeout for in-flight delivery, default 5 seconds. At the bound it MUST abandon remaining delivery/queued work with a logged warning and return without waiting forever for a listener. Queue abandonment MUST NOT erase committed history: a later listener may resume after its token under TXN-805. Dispatcher-owned read views MUST be safely closed/retired without allowing a still-running callback to access freed backing memory; a listener cannot extend the close bound by retaining its view. Mutable writer teardown and listener shutdown remain separate.

### TXN-804 — Close from every callback context

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [DatabaseHandle.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseHandle.java):`close`; [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`close`; [Databases.java](../bundles/sirix-core/src/main/java/io/sirix/access/Databases.java); [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`close`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`truncateTo`
- **Verified by:** No existing test mapped. **Planned:** CallbackEntryPointConformance: one deterministic regression per public/forwarded lifecycle entry point, pre-hook/listener and JSON/XML; include shutdown forwarding, nested handles and cross-thread blocked callback.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** Required structural guard is pending. Lock-order/fence implementation is not prescribed; immediate rejection is the observable contract.

Close invoked from inside any pre-commit hook or post-commit listener for its own transaction/session/database MUST immediately throw `IllegalStateException`, before lock acquisition or waiting, including DatabaseHandle, database-manager and shutdown-path forwarding. A dispatcher MUST NOT wait for itself. Page-writer truncation and other lifecycle forwarding MUST NOT bypass the same hook guard.

### TXN-805 — Revision-token replay and bounded queue

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [ResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/ResourceSession.java):`getHistory`; [ResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/ResourceSession.java):`beginNodeReadOnlyTrx`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`getRevisionNumber`; [SirixRevisionPrunedException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixRevisionPrunedException.java)
- **Verified by:** No existing test mapped. **Planned:** ListenerResumeConformance: stop at X, commit X+1..N, restart and compare ordered tokens/content with retained history; tiny queue overflow and default 10,000 capacity; two resources with equal revision numbers; retained versus pruned needed revisions, no false HistoryLostException and no commit-thread replay work.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** No resume API, HistoryLostException or bounded notification queue exists at this baseline. History/pruned-revision APIs are building blocks only. Registration/token method signatures and explicit-truncate interaction need engineering/API completion; the replay/overflow policy is decided.

A listener that falls behind or restarts MUST be able to resume after revision X of its resource; delivery MUST replay the retained committed revisions greater than X in order. The bounded in-memory queue, default 10,000 notifications per database, MUST be only a fast path. Overflow MUST cause the affected listener to resume/replay from its last delivered revision instead of losing events, failing commits or applying writer backpressure. The dispatcher MUST raise HistoryLostException only when revisions required for that resume have been pruned; overflow, restart and listener exceptions alone MUST NOT be reported as history loss. A revision token identifies a resource-local timeline position, not a global database revision number.

### TXN-806 — Listener failure and callback view ownership

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [PostCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PostCommitHook.java):`postCommit`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [NodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeReadOnlyTrx.java):`close`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`beginNodeReadOnlyTrx`
- **Verified by:** No existing test mapped. **Planned:** ListenerFailureAndViewConformance: throwing callback invokes once and logs; next token proceeds; explicit consumer resume redelivers from its saved checkpoint; exact-revision view closes after normal/throwing callback, retained access fails safely, timed-out callback cannot read freed memory.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** Current direct postCommit(this) passes the writer and provides neither dispatcher-owned view scope nor explicit resume semantics. No automatic retry policy implementation is present.

A listener exception MUST be logged and MUST NOT trigger automatic retry of that failed delivery. A delivery means a callback invocation, not an acknowledgement of successful application processing; the listener MAY explicitly resume from its own saved revision token. The dispatcher MUST open and close the read-only revision view; it MUST be valid only during that callback and MUST NOT be retained or used afterwards. Retained references MUST fail safely rather than read a mutable successor or freed storage. The listener MUST NOT own or prolong the lifetime of the view. TXN-803 bounds close even when the callback never returns.

## Conditional write preconditions (TXN-311)

`R` is the token revision, `head` the head revision the writer is based on, and
`stamp(x)` the last-modified revision of record `x` in the head revision (VER-206).
An insertion point is the triple (parent `P`, anchor `A`, side `s`), where `A` is the
node the insertion is expressed relative to and `s` the side it is inserted on; for a
first-child or last-child insert `A` is `P` and the neighbour position is `P`'s first
or last child, and for a sibling insert the neighbour position is `A`'s sibling on `s`.

| Target kind | Rejected when | Explicitly not a conflict |
|---|---|---|
| Update, delete or replace of node `N` | `N` is absent at `head`; or `stamp(x) > R` for any record `x` in `N`'s subtree at `head`, `N` included | any change outside `N`'s subtree, including to `N`'s ancestors, siblings or their subtrees |
| Insert at insertion point (`P`, `A`, `s`) | `P` or `A` is absent at `head`; or `A`'s parent at `head` is no longer `P`; or the neighbour position on side `s` is occupied by a different node than at `R`; or `A` is not `P` and `stamp(A) > R` | a changed child count of `P`; an insert, delete or update elsewhere under `P`, including at the other end of the same array or object |
| Whole document root, or a write with no single node target | `head > R` | nothing; this is the deliberately coarse resource-level case |

Every target kind is rejected additionally when the token's epoch differs from the
resource's current epoch, when `R > head`, or when `R < lastRevertRevision`. A rejection
names the target kind, the token's epoch and revision, and the resource's current epoch
and head revision. Weak entity tags are rejected by the interfaces that carry the token
(REST-204).

Informative: the predicates are evaluable from the head revision alone. For an update or
delete, walk the target subtree and stop at the first record with a larger stamp.
For an insert, VER-206 makes the neighbour test local: if the node occupying the
neighbour position has a stamp not greater than `R`, it occupied that position at `R`
already, since any relinking of that record stamps it; if the position is empty at
`head`, `stamp(P) > R` is the conservative rejection, because emptying it stamps `P`
alone. Excluding `P`'s record from the insert predicate is deliberate: `storeChildCount`
rewrites `P` on every insert under it, so a record-level test on `P` would make two
inserts into one array conflict even when their insertion points are disjoint.

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
every write to a large document conflict. The epoch is the Kubernetes `uid` half of that pair: a
`resourceVersion` is only meaningful together with the `uid` of the object incarnation it came from,
because a deleted and recreated object restarts versioning, and truncate and whole-resource
replacement do exactly that to a Sirix resource. Paying nothing at write time and deciding on demand is what
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
- TXN-802–806: registration/token signatures, configuration keys/ranges, explicit-truncate interaction and safe cancellation/retirement mechanics after a timed-out callback. Replay, no callback retry, view ownership, queue default 10,000 and close default 5 seconds are decided.
- TXN-102: existing 5-second node-writer and 20-second page-writer admission bounds are recorded; any API consolidation is outside this P0 draft.
- TXN-311: the check, its exception, the subtree walk and its work-budget assertion are unimplemented. It depends on VER-206 stamping and on the FMT-209 epoch and last-revert metadata, including the epoch allocation that TXN-404 truncate and REST-210 replacement must perform.
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
| TXN-310 | [RevisionIndexIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexIntegrationTest.java); [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java) **Planned:** StrictCommitTimestampConformance: controlled repeating/regressing live clocks and max(previous+1us,clock) oracle; import equal/decreasing/increasing source sequences with conditional adjustment oracle; original metadata queryability, preserved order, first and unchanged timestamps, live commit after import, sync/async and JSON/XML reopen/recovery without wall-clock timing assertions. | designed |
| TXN-311 | No existing test mapped. **Planned:** ConditionalWritePreconditionConformance: every target kind, interleaved touching and non-touching commits, concurrent reorder and same-depth move, deleted target, reused node key after revert, tokens from before a truncate and a replacement, token above the head, pruned token revision, document root, empty foreign commit and accepted retry, each with hashing enabled and disabled; **Planned:** ConditionalWritePreconditionBudgetTest record-read bounds (CONV-301). | proposed |
| TXN-401 | [RollbackStateTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/RollbackStateTest.java); [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** RollbackMatrixConformance: JSON/XML, all versioning types, sync/async failure, compare serialization, indexes and diff after retry. | partial |
| TXN-402 | [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** RollbackMarkerConformance: failure at every reconstruction stage, closed rollback while a new writer owns the resource, marker and permit assertions. | designed |
| TXN-403 | [XmlRevertTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/XmlRevertTest.java); [JsonIdentityHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/json/JsonIdentityHistoryTest.java); [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt):`optionalHeadAndMessageAndRevertToHeadAppendARevision`; [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt):`prunedTargetFailsWithoutAppendingOrReplacingTheTarget` **Planned:** RevertParityConformance: Java/REST/MCP, JSON/XML, current head/old head/pruned target, unchanged history after cold reopen. | partial |
| TXN-404 | [ExplicitRollbackCrashSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/ExplicitRollbackCrashSimulationTest.java); [NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java); [RAMStorageTruncateToTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/ram/RAMStorageTruncateToTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java) **Planned:** TruncateReaderFenceConformance: open readers below/at/above target across handles, dispatcher views and independent page-read paths; blocked set named in immediate error, unchanged bytes/history/state, no wait; reader-admission race; close conflicting readers then successful truncate, JSON/XML and supported backends. | designed |
| TXN-501 | [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`failedCommitKeepsTransactionUsableAndGuardsClose` | partial |
| TXN-502 | [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** AsyncCloseOutcomeConformance: known pre-point failure, durable cleanup failure, uncertain force, dirty successor and repeated close; verify result classification, repair flag, no fsync retry and safe ownership. | designed |
| TXN-503 | [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java); [AbstractResourceSessionWriteFailureCleanupTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AbstractResourceSessionWriteFailureCleanupTest.java) **Planned:** TeardownConformance: inject failures in reader, log, backend, pool and session children; verify each cleanup is attempted without freeing live-worker state. | designed |
| TXN-504 | [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** WorkerOwnershipConformance: stalled worker, interrupted wait, failed cleanup and retry; assert no successor admission or use-after-free before exit. | designed |
| TXN-505 | [JsonDatabaseLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonDatabaseLifecycleTest.java); [AbstractResourceSessionWriteFailureCleanupTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AbstractResourceSessionWriteFailureCleanupTest.java) **Planned:** SessionAbortConformance: JSON/XML known-uncommitted dirty work, known durable cleanup failure, uncertain async durability and rollback failure; assert each preserved outcome and no repeated fsync on close/reopen. | designed |
| TXN-601 | [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java) **Planned:** MarkerIdentityConformance: empty/nonempty regular file, directory, creation failure and deletion failure. | designed |
| TXN-602 | [PowerLossSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/PowerLossSimulationTest.java); [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java):`tornWriteWithoutMarker_lastCommittedRevisionStillReadable` **Planned:** MarkerLossCrashConformance: first append before marker persistence, lost directory entry, stale-forward SECONDARY then second crash. | designed |
| TXN-603 | [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java) **Planned:** MarkerLifecycleConformance: enumerate success and every failure exit, then reopen and recover. | designed |
| TXN-604 | [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java); [CrashRecoveryOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryOrderingTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java) **Planned:** RecoveryOutcomeConformance: complete durable N with stale beacon/session, staged incomplete record, unknown N both present and absent, sibling cache preservation, repeat recovery and crash during recovery; verify reported outcome before writer admission. | designed |
| TXN-605 | [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java); [AsyncCommitFrontierOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AsyncCommitFrontierOrderingTest.java) | partial |
| TXN-606 | [AbstractResourceSessionWriteFailureCleanupTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AbstractResourceSessionWriteFailureCleanupTest.java) **Planned:** RecoveryFailureConformance: backend open, root discovery, truncate, catalogue/cache and marker failures; fresh reopen revalidates durable evidence, preserves error/stage and never repeats a failed force to infer success. | designed |
| TXN-607 | [RAMStorageTruncateToTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/ram/RAMStorageTruncateToTest.java) **Planned:** InMemoryMarkerConformance: inject commit/rollback/close failures and reopen admission; assert no marker creation and no recovery wedge. | designed |
| TXN-608 | No existing test mapped. **Planned:** RecoveryIdentityConformance: marker creation failure, session close/reopen, same-path resource recreation and persisted identity mismatch. | designed |
| TXN-609 | [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java):`concurrentReader_survivesWriterCrashAndRecovery` **Planned:** ReadAdmissionRecoveryConformance: marker plus unreachable tail; assert unchanged tail size and marker on reads, then repaired state on writer admission. | partial |
| TXN-610 | [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java):`tornWriteWithoutMarker_lastCommittedRevisionStillReadable`; [PreallocatedCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/PreallocatedCommitTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java) | partial |
| TXN-611 | [InterruptedFirstCommitRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/InterruptedFirstCommitRecoveryTest.java); [InterruptedFirstCommitRecoveryStoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/InterruptedFirstCommitRecoveryStoreFeaturesTest.java) | partial |
| TXN-612 | [DerivedIndexCrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedIndexCrashRecoveryTest.java):`restartsAtDurableDerivedFrontier`; [DerivedRevisionMappingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedRevisionMappingTest.java) | partial |
| TXN-613 | [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java); [DerivedRevisionMappingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedRevisionMappingTest.java) **Planned:** DerivedBackupConformance: source/derived revisions advancing during copy, backup start barrier, copy hidden resources at pinned heads, restore elsewhere and compare history/mappings/queries before and after catch-up; interrupted copy rejects partial target. | designed |
| TXN-614 | [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java); [DatabaseOwnershipTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseOwnershipTest.java) **Planned:** BackupStartSnapshotConformance: barrier at start, concurrent create/commit/drop, assert exact resource/head manifest; pinned drop names backup in progress and preserves all state; later-created resource excluded; two overlapping backups and failed-copy cleanup; restore and compare every pinned revision plus hidden derived state. | designed |
| TXN-701 | [AsyncAutoCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncAutoCommitTest.java); [AsyncFlushColdReopenVersioningTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushColdReopenVersioningTest.java) | partial |
| TXN-702 | [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java):`unboundCloseBlocksSuccessorUntilAbortedTrieTailIsSafeToReuse` **Planned:** AsyncTailRecoveryConformance: abort after page append, warm and cold reopen, sibling resource active, assert no stale page. | designed |
| TXN-703 | [AsyncCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/AsyncCommitTest.java); [AsyncCommitFrontierOrderingTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AsyncCommitFrontierOrderingTest.java) **Planned:** AsyncPipelineConformance: barrier-observed successor overlap; successor-construction plus inline-hardening double failure; submission rejection; assert balanced slot and original cause. | designed |
| TXN-704 | [AsyncFlushFailurePathTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/AsyncFlushFailurePathTest.java) **Planned:** AsyncOutcomeFenceConformance: repeated await/mutate/commit/rollback after uncertain force, multiple handles to the same resource, sibling-resource independence, cleanup failure and reopen; assert no further durability attempt or new write admission. | designed |
| TXN-705 | No existing test mapped. **Planned:** UnknownCommitOutcomeConformance: force/DSYNC faults with both persisted and lost storage outcomes, assert exactly one attempt at the failed durability operation, resource-wide rejection across handles and async fallback/close/rollback, then cold recovery and explicit N-present/N-absent result. | designed |
| TXN-801 | [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java) **Planned:** HookLifecycleConformance: each lifecycle entry point from a pre-hook, JSON/XML, sync/async modes; assert immediate exception, no state change, retained dirty data and reusable ownership. | designed |
| TXN-802 | No existing test mapped. **Planned:** PostCommitDispatcherConformance: stalled listener for N while N+1 commits, thread separation, resource ordering and token identity, exact revision content, read-only ownership; deterministic work/lock counters show no listener wait or replay work on commit. | designed |
| TXN-803 | No existing test mapped. **Planned:** BoundedListenerCloseConformance: controlled clock/scheduler with default 5-second and custom bounds, never-returning listener, queued replay work, logged abandonment, safe view lifetime and later resume; JSON/XML and close-from-callback rejection. | designed |
| TXN-804 | No existing test mapped. **Planned:** CallbackEntryPointConformance: one deterministic regression per public/forwarded lifecycle entry point, pre-hook/listener and JSON/XML; include shutdown forwarding, nested handles and cross-thread blocked callback. | designed |
| TXN-805 | No existing test mapped. **Planned:** ListenerResumeConformance: stop at X, commit X+1..N, restart and compare ordered tokens/content with retained history; tiny queue overflow and default 10,000 capacity; two resources with equal revision numbers; retained versus pruned needed revisions, no false HistoryLostException and no commit-thread replay work. | designed |
| TXN-806 | No existing test mapped. **Planned:** ListenerFailureAndViewConformance: throwing callback invokes once and logs; next token proceeds; explicit consumer resume redelivers from its saved checkpoint; exact-revision view closes after normal/throwing callback, retained access fails safely, timed-out callback cannot read freed memory. | designed |
