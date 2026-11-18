# JAPI — Embedded Java API

Spec target: **1.0.0**. Chapter status: **Draft for adoption before the approximately
9 December 2026 freeze**. Code baseline: `13f65148b117b9c8e1858a7f1510d2f65501bad9` (17 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance.
Existing test links are evidence inventories, not assertions that tests passed in this task.

## Scope

P0 API stability manifest, typed entry points, ownership, threading, errors, hooks and required JSON replacement identity. Query function signatures and a full node-model chapter remain separate work.

## Requirements

### JAPI-101 — Small stable Java core

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [Databases.java](../bundles/sirix-core/src/main/java/io/sirix/access/Databases.java); [Database.java](../bundles/sirix-core/src/main/java/io/sirix/api/Database.java); [ResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/ResourceSession.java); [NodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeTrx.java); [SirixQueryContext.java](../bundles/sirix-query/src/main/java/io/sirix/query/SirixQueryContext.java); [SirixCompileChain.java](../bundles/sirix-query/src/main/java/io/sirix/query/SirixCompileChain.java); [DiffFactory.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffFactory.java)
- **Verified by:** No existing test mapped. **Planned:** JavaApiCompatibilityConformance: compile a consumer covering every core category against the frozen jar, run against candidate jar; binary/source API comparison and transitive exposed-type closure.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** Captain decision 17 November 2026 fixes the small stable core and three-tier boundary. JAPI-stable-api-manifest.md now marks each inventoried type/member stable, expert or internal and includes concrete query/diff entry points. Compiled/inherited/support-type closure remains release-artifact work; no runtime compatibility certification is claimed.

The 1.0 stable Java API MUST be a small user-facing core covering database/resource opening, read/write transactions, revisions and time travel, diff, history, query entry points and configuration builders. Its explicit manifest MUST classify type identities and individual public/protected signatures, including inherited signatures and exposed support types; public visibility or package membership alone MUST NOT grant stable status. Stable signature or behavioral incompatibilities after freeze MUST follow CONV-105. Low-level members exposed through a stable interface MUST retain their explicit internal classification.

### JAPI-102 — JSON and XML entry points

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [Databases.java](../bundles/sirix-core/src/main/java/io/sirix/access/Databases.java); [Database.java](../bundles/sirix-core/src/main/java/io/sirix/api/Database.java):`beginResourceSession`; [JsonResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonResourceSession.java); [XmlResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/xml/XmlResourceSession.java)
- **Verified by:** [JsonDatabaseLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonDatabaseLifecycleTest.java); [DatabaseConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/conf/DatabaseConfigurationTest.java) **Planned:** JavaEntryPointConformance: create/open/reopen both types, wrong type, missing database/resource and valid configuration round trip.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test inspection only; the stable manifest is a review baseline, not a published compatibility certification.

Databases.createJsonDatabase/openJsonDatabase and createXmlDatabase/openXmlDatabase MUST provide the corresponding typed resource sessions and transactions. Opening a resource MUST respect its persisted database type/configuration and MUST NOT reinterpret JSON as XML or vice versa.

### JAPI-103 — Handle ownership

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [DatabaseHandle.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseHandle.java):`close`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`close`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`close`
- **Verified by:** [DatabaseOwnershipTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseOwnershipTest.java); [JsonDatabaseLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonDatabaseLifecycleTest.java) **Planned:** JavaOwnershipConformance: two handles/two sessions, close in either order, use-after-close, dirty close, failed cleanup/retry and callbacks.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Handle tests exist. Full failure-resistant teardown/hook guard remains designed in TXN-502 through TXN-504 and TXN-801 through TXN-804.

Closing a database handle MUST release that handle’s ownership without invalidating another live handle’s independent ownership. Resource sessions and transactions MUST follow TXN teardown and worker-fence requirements; after successful close, operations requiring live state MUST fail clearly. Try-with-resources MUST NOT imply commit of dirty data.

### JAPI-104 — Transaction threading boundary

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [NodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeTrx.java):`runLocked`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`runLocked`
- **Verified by:** [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java); [AutoCommitRevisionReadStressTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AutoCommitRevisionReadStressTest.java) **Planned:** TransactionThreadingConformance: independent snapshots concurrent with writer, timer races with writer-visible index serving under runLocked.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** The source NodeTrx threading contract is explicit; exhaustive Java API thread-affinity annotations are not present.

Callers MUST confine mutable node-cursor operations to one thread unless an API explicitly coordinates them. Timed auto-commit coordination MUST use the transaction lock, including writer-visible reads that flush pending maintenance. Having an internal auto-commit lock MUST NOT be advertised as general concurrent cursor safety. Independent readers MAY execute on separate transactions.

### JAPI-105 — Cursor movement failure

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [NodeCursor.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeCursor.java):`moveTo`; [AbstractNodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeReadOnlyTrx.java):`moveTo`; [AbstractNodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeReadOnlyTrx.java):`moveToItemList`; [AbstractNodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeReadOnlyTrx.java):`moveToLegacy`
- **Verified by:** [JsonNodeTrxMoveTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonNodeTrxMoveTest.java) **Planned:** CursorMovementConformance: JSON/XML absent/deleted/negative keys from root and non-root, retained historical keys, assert position unchanged.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test inspection only; the stable manifest is a review baseline, not a published compatibility certification.

moveTo(key) MUST return false for an absent node and preserve the prior cursor position; successful movement MUST select exactly that node key. Accessors MUST observe the selected revision’s node. Callers MUST check failed movement before mutation or serialization rather than accidentally using the document root.

### JAPI-106 — Errors preserve outcome distinctions

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [SirixUsageException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixUsageException.java); [SirixIOException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixIOException.java); [SirixRevisionPrunedException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixRevisionPrunedException.java); [UnsupportedStoreFeatureException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/UnsupportedStoreFeatureException.java); [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`
- **Verified by:** [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [StoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StoreFeaturesTest.java); [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java) **Planned:** JavaCommitOutcomeConformance: verify ordinary failure with rollback/retry, normal revision-N receipt after each cleanup fault, CommitOutcomeUnknownException for uncertain I/O, preserved causes, closed/fenced writer state and explicit recovered result; ensure legacy durable-failure classification is absent.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current NodeTrx.commit returns the transaction object and lacks the unknown-outcome exception/receipt API. Storage and rebind failures may throw after durability. Exact result/exception fields and resolved-outcome query signatures must be specified before freezing the compiled manifest; the three outcomes and exception name are decided.

Java APIs MUST distinguish ordinary known pre-commit-point failures, successful committed revision N and unknown durability outcomes. Known post-durable cleanup failures MUST return N normally, log the failure, flag repair and close the writer under TXN-307; they MUST NOT throw a distinct durable-failure exception. Uncertain durability MUST throw CommitOutcomeUnknownException, preserve its I/O cause, stop resource writes without fsync retry, and expose the recovered N-present/N-absent result after reopen. Other APIs MUST distinguish invalid arguments, lifecycle errors, unsupported features, corrupt I/O and pruned revisions. Reentrant hook lifecycle calls MUST use IllegalStateException.

### JAPI-107 — Explicit transaction controls

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [NodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeTrx.java); [ResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/api/ResourceSession.java):`getMostRecentRevisionNumber`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`revertTo`
- **Verified by:** [MultipleCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/MultipleCommitTest.java); [XmlRevertTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/XmlRevertTest.java); [NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java) **Planned:** JavaTransactionStateConformance: compare writer/session/read-only revision numbers before/after all control operations.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test inspection only; the stable manifest is a review baseline, not a published compatibility certification.

commit, rollback, revertTo and truncateTo MUST have the distinct semantics in TXN/VER. Empty commit MUST advance history; revertTo alone MUST NOT publish it. The API MUST expose the pending writer revision separately from the latest committed session revision without conflating them.

### JAPI-108 — Resource-local transaction scope

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [LocalDatabase.java](../bundles/sirix-core/src/main/java/io/sirix/access/LocalDatabase.java):`beginTransaction`; [Database.java](../bundles/sirix-core/src/main/java/io/sirix/api/Database.java):`beginTransaction`
- **Verified by:** No existing test mapped. **Planned:** MultiResourceBoundaryConformance: invoke beginTransaction and assert explicit unsupported failure with unchanged resource heads.
- **Evidence class:** code-only (implementation inspected)
- **Gap/note:** LocalDatabase currently throws UnsupportedOperationException. The manifest classifies beginTransaction and Transaction/TransactionManager as internal; public presence does not add a stable multi-resource guarantee.

The 1.0 embedded baseline MUST NOT advertise implemented multi-resource atomic transactions through Database.beginTransaction. The current unsupported entry point MUST fail explicitly; resource-local beginNodeTrx retains the TXN guarantees. The internal Transaction/TransactionManager declarations MUST NOT imply a stable or implemented cross-resource coordinator.

### JAPI-109 — Stable, expert and internal tiers

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [NodeCursor.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeCursor.java); [StorageEngineReader.java](../bundles/sirix-core/src/main/java/io/sirix/api/StorageEngineReader.java); [StorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/api/StorageEngineWriter.java); [IndexController.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/IndexController.java); [PreCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PreCommitHook.java); [PostCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PostCommitHook.java)
- **Verified by:** No existing test mapped. **Planned:** JavaApiTierConformance: verify exact member-tier coverage and inherited exclusions; assert storage/index/controller/cache/record access stays internal even on stable session/transaction types, and expert callbacks are visibly labelled.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** Three-tier policy is decided. This manifest makes concrete draft declaration classifications; code annotations/Javadoc labels and exact expert deprecation/support windows are still to be completed. Existing index controllers are internal; no unsupported custom-index-hook signature is invented.

The manifest MUST clearly label the stable core, an expert extension tier with weaker compatibility guarantees, and internal APIs. Low-level page, storage, index and cursor internals MUST be explicitly internal and MAY change in any release. Expert extension points, such as custom index hooks when exposed, MUST be labelled outside the stable 1.x signature guarantee; the expert label MUST NOT waive specified behavioral invariants. Basic navigation on user-facing read/write transactions remains part of their explicit stable contract; it MUST NOT stabilize backing cursor implementations. Every inventoried declaration MUST carry a tier, and a stable type MUST NOT implicitly promote an internal member or transitive implementation type.

### JAPI-110 — Signature compatibility gate from 1.0

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [build.gradle](../build.gradle); [build.gradle](../bundles/sirix-core/build.gradle); [gradle.yml](../.github/workflows/gradle.yml)
- **Verified by:** No existing test mapped. **Planned:** JavaApiSignatureGateConformance: fixture jars with removed/changed stable members, inherited breakage and exposed-type changes fail; compatible additions and expert/internal-only changes pass; external client links and executes against the candidate. Confirm the gate runs in default CI without module-export lockdown.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** The inspected Gradle files and workflow do not provide this stable-manifest signature gate. Tool selection/configuration and compiled baseline generation remain planned implementation work; no build, workflow or JPMS descriptor is changed here.

From 1.0, default CI MUST enforce the stable manifest with a signature-compatibility check of the japicmp style against the frozen supported release baselines. It MUST detect incompatible removal, signature/generic/supertype changes and relevant inherited or exposed-type changes, and MUST fail unapproved stable incompatibilities. Expert/internal changes MUST NOT be treated as stable API breaks merely because the declarations are public. The 1.0 release MUST NOT require JPMS export lockdown; documentation, the manifest and CI define the boundary. Signature checks MUST be complemented by behavioral conformance because unchanged signatures do not prove unchanged behavior.

### JAPI-111 — Conditional write precondition

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [NodeTrx.java](../bundles/sirix-core/src/main/java/io/sirix/api/NodeTrx.java); [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java); [SirixUsageException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixUsageException.java); [SirixRevisionPrunedException.java](../bundles/sirix-core/src/main/java/io/sirix/exception/SirixRevisionPrunedException.java)
- **Verified by:** No existing test mapped. **Planned:** JavaConditionalWriteConformance: each precondition kind accepted and rejected against an interleaved foreign commit, JSON and XML, with hashing on and off and identical verdicts; a concurrent reorder and a same-depth move, which a hash comparison would have missed; a mismatched epoch; the exception's epoch, revision, head, target and kind fields; unchanged head and reusable transaction after a rejection; rejection on a closed or rolled-back writer; invalid arguments separated from stale tokens; the guarded mutation and commit after an accepted check.
- **Evidence class:** doc-only (unverified); decided target has unverified implementation gaps
- **Gap/note:** No such method exists at this baseline; REST's `checkHashCode` is the only optimistic guard and compares node hashes, which the 1.0 default `HashType.NONE` leaves unavailable. The exact method and type descriptors are pending implementation and are recorded as a required addition in the manifest, like the JAPI-106 commit-outcome API.

The write transaction MUST expose exactly one stable-tier conditional check implementing TXN-311, taking a precondition value and evaluating it immediately against the head revision the writer is based on. The precondition value MUST carry the token — the resource history epoch and the token revision — together with one of the three target kinds of TXN-311: a node whose subtree must be unchanged, an insertion point that must be unchanged, and the resource itself unchanged. One method MUST therefore guard an update, a delete, a replace and an insert without a separate method per mutation. The value MUST NOT carry a node hash and the check MUST behave identically on hashed and hashless resources, as TXN-311 requires. A failed check MUST throw a distinct precondition-failure exception carrying the token's epoch and revision, the resource's current epoch and head revision, the target and the failing kind, and MUST leave the transaction usable for a re-read and retry, with no revision consumed. The exception MUST be distinguishable from invalid-argument and lifecycle errors under JAPI-106: a negative or malformed token is an argument error, while a stale, rewound or above-head token is a precondition failure. A pruned token revision MUST NOT by itself raise `SirixRevisionPrunedException` here, since TXN-311 does not read the token revision. REST, the native ABI and the SDKs MUST build on this one check rather than reimplement conflict detection.

### JAPI-201 — Hook API guarantees

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [PreCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PreCommitHook.java); [PostCommitHook.java](../bundles/sirix-core/src/main/java/io/sirix/api/PostCommitHook.java); [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`addPreCommitHook`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`addPostCommitHook`
- **Verified by:** No existing test mapped. **Planned:** HookLifecycleConformance; PostCommitDispatcherConformance; BoundedListenerCloseConformance; CallbackEntryPointConformance; ListenerResumeConformance; ListenerFailureAndViewConformance; DispatcherConfigurationConformance.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** Existing hook registration methods are not the completed change-stream API. Resume registration/removal/token signatures, exception class and dispatcher-owned view implementation remain missing at this baseline; the policy/defaults are decided.

The Java hook API MUST implement TXN-801 through TXN-806: synchronous pre-hooks that cannot end the transaction; asynchronous per-database dispatch ordered per resource; revision resume tokens and replay after X; bounded queue overflow recovered from history; HistoryLostException only for needed pruned revisions; no automatic retry after a logged listener exception; a dispatcher-owned read-only view valid only during the callback; configurable close defaulting to 5 seconds; and immediate rejection of close from any hook/listener context. Commit MUST NOT wait for or be backpressured by listeners.

### JAPI-301 — JSON type-changing value replacement

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [JsonNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java):`replaceObjectRecordValue`; [JsonNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java):`updateObjectRecordValue`; [JsonNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java):`adaptUpdateOperationsForReplace`; [AbstractDiff.java](../bundles/sirix-core/src/main/java/io/sirix/diff/AbstractDiff.java); [JsonDiffSerializer.java](../bundles/sirix-core/src/main/java/io/sirix/diff/JsonDiffSerializer.java); [DiffFactory.java](../bundles/sirix-core/src/main/java/io/sirix/diff/DiffFactory.java)
- **Verified by:** [BasicJsonDiffTest.java](../bundles/sirix-core/src/test/java/io/sirix/service/json/BasicJsonDiffTest.java); [JsonDiffSerializerStaleTupleRegressionTest.java](../bundles/sirix-core/src/test/java/io/sirix/diff/JsonDiffSerializerStaleTupleRegressionTest.java); [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`autoCommitDoesNotSplitJsonReplaceObjectRecordValue` **Planned:** JsonReplaceIdentityConformance: every scalar same-kind pair and every different JSON-kind pair, object field and array item, fused/overflow, rollback/reopen, assert node keys, REPLACEDOLD/REPLACEDNEW and exact patch result.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Current fused-object same-kind scalar fast path preserves key; type-changing path removes/reinserts and records REPLACEDNEW sidecar tuple. Sidecar serialization may collapse a semantic pair into one replace operation; that is not a missing semantic diff event. Comprehensive public diff-pair assertions remain planned.

A JSON value replacement that changes its JSON type MUST replace the node: the old identity ends and the new value receives a new node key. Semantic diff MUST represent it as a REPLACEDOLD/REPLACEDNEW pair with the corresponding old/new keys. A same-type scalar value update MUST keep node identity and appear as an update (or no change for an equal value), not a replacement. Same-type here means JSON scalar kind, not the Java Number implementation class. Historical content and old node identities MUST remain readable at retained older revisions.

### JAPI-302 — Replacement copies do not alias source identity

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [JsonNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java):`replaceObjectRecordValue`; [JsonNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java):`copySubtreeAsFirstChild`
- **Verified by:** [JsonIdentityCopyIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/service/json/shredder/JsonIdentityCopyIntegrationTest.java) **Planned:** JsonReplacementCopyConformance: replace with a source object/array, compare keys and values, mutate source independently and verify replacement snapshot.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Sirix identity rule is consistent with the external replacement models cited below; those standards do not prescribe Sirix physical node keys.

A replacement copied from another JSON item MUST NOT reuse that source item’s identity as the replacement target identity. Future mutation of the source MUST NOT mutate the copied replacement. Structural subtree replacement MUST create replacement structure; same-type scalar value-change identity preservation in JAPI-301 MUST NOT be misread as retaining every descendant of a replaced object/array.

## Replacement rationale (informative)

[XQuery Update Facility 1.0 §2.4.3](https://www.w3.org/TR/xquery-update-10/#id-replace)
distinguishes replacing a node from changing its value while preserving identity.
[JSONiq update expressions](https://www.jsoniq.org/docs/JSONiqExtensionToXQuery/html-single/index.html)
copy replacement content for replace-in-object. JAPI-301 applies that distinction to Sirix
JSON type changes; it is a Sirix node-key/diff contract, not a claim that XDM defines JSON node keys.

## Deferred release implementation

- Complete the compiled signature artifact and transitive support-type entries under the decided three-tier boundary; see
  [candidate manifest](JAPI-stable-api-manifest.md).
- Revision-N success receipt, CommitOutcomeUnknownException fields and recovered-outcome API signatures; the exception name and three outcome behaviors are decided.
- Conditional-write precondition method, precondition value type (epoch, revision) and precondition-failure exception for JAPI-111/TXN-311; the single-method shape, the three target kinds, the hash-independence and the error separation are decided, the descriptors are not.
- Dispatcher registration/removal/resume signatures and timeout/queue configuration keys/ranges; view ownership, replay, exception/no-retry policy and 5-second/10,000 defaults are decided.
- Pin externally exposed query-library signatures and complete the expert support/deprecation window. Low-level page/storage/index/cursor internals are decided internal; no JPMS export lockdown is planned for 1.0.

## Traceability

| Requirement | Existing or planned verification | Status |
|---|---|---|
| JAPI-101 | No existing test mapped. **Planned:** JavaApiCompatibilityConformance: compile a consumer covering every core category against the frozen jar, run against candidate jar; binary/source API comparison and transitive exposed-type closure. | designed |
| JAPI-102 | [JsonDatabaseLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonDatabaseLifecycleTest.java); [DatabaseConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/conf/DatabaseConfigurationTest.java) **Planned:** JavaEntryPointConformance: create/open/reopen both types, wrong type, missing database/resource and valid configuration round trip. | partial |
| JAPI-103 | [DatabaseOwnershipTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/DatabaseOwnershipTest.java); [JsonDatabaseLifecycleTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonDatabaseLifecycleTest.java) **Planned:** JavaOwnershipConformance: two handles/two sessions, close in either order, use-after-close, dirty close, failed cleanup/retry and callbacks. | partial |
| JAPI-104 | [JsonConcurrentAccessTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonConcurrentAccessTest.java); [AutoCommitRevisionReadStressTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/AutoCommitRevisionReadStressTest.java) **Planned:** TransactionThreadingConformance: independent snapshots concurrent with writer, timer races with writer-visible index serving under runLocked. | partial |
| JAPI-105 | [JsonNodeTrxMoveTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonNodeTrxMoveTest.java) **Planned:** CursorMovementConformance: JSON/XML absent/deleted/negative keys from root and non-root, retained historical keys, assert position unchanged. | partial |
| JAPI-106 | [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [StoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StoreFeaturesTest.java); [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java) **Planned:** JavaCommitOutcomeConformance: verify ordinary failure with rollback/retry, normal revision-N receipt after each cleanup fault, CommitOutcomeUnknownException for uncertain I/O, preserved causes, closed/fenced writer state and explicit recovered result; ensure legacy durable-failure classification is absent. | designed |
| JAPI-107 | [MultipleCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/MultipleCommitTest.java); [XmlRevertTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/XmlRevertTest.java); [NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java) **Planned:** JavaTransactionStateConformance: compare writer/session/read-only revision numbers before/after all control operations. | partial |
| JAPI-108 | No existing test mapped. **Planned:** MultiResourceBoundaryConformance: invoke beginTransaction and assert explicit unsupported failure with unchanged resource heads. | partial |
| JAPI-109 | No existing test mapped. **Planned:** JavaApiTierConformance: verify exact member-tier coverage and inherited exclusions; assert storage/index/controller/cache/record access stays internal even on stable session/transaction types, and expert callbacks are visibly labelled. | designed |
| JAPI-110 | No existing test mapped. **Planned:** JavaApiSignatureGateConformance: fixture jars with removed/changed stable members, inherited breakage and exposed-type changes fail; compatible additions and expert/internal-only changes pass; external client links and executes against the candidate. Confirm the gate runs in default CI without module-export lockdown. | designed |
| JAPI-111 | No existing test mapped. **Planned:** JavaConditionalWriteConformance: each precondition kind accepted and rejected in both hash modes with identical verdicts, concurrent reorder and same-depth move, mismatched epoch, exception fields, unchanged head, reusable transaction, argument-versus-precondition separation and the guarded commit. | proposed |
| JAPI-201 | No existing test mapped. **Planned:** HookLifecycleConformance; PostCommitDispatcherConformance; BoundedListenerCloseConformance; CallbackEntryPointConformance; ListenerResumeConformance; ListenerFailureAndViewConformance; DispatcherConfigurationConformance. | designed |
| JAPI-301 | [BasicJsonDiffTest.java](../bundles/sirix-core/src/test/java/io/sirix/service/json/BasicJsonDiffTest.java); [JsonDiffSerializerStaleTupleRegressionTest.java](../bundles/sirix-core/src/test/java/io/sirix/diff/JsonDiffSerializerStaleTupleRegressionTest.java); [CommitSafetyTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/CommitSafetyTest.java):`autoCommitDoesNotSplitJsonReplaceObjectRecordValue` **Planned:** JsonReplaceIdentityConformance: every scalar same-kind pair and every different JSON-kind pair, object field and array item, fused/overflow, rollback/reopen, assert node keys, REPLACEDOLD/REPLACEDNEW and exact patch result. | partial |
| JAPI-302 | [JsonIdentityCopyIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/service/json/shredder/JsonIdentityCopyIntegrationTest.java) **Planned:** JsonReplacementCopyConformance: replace with a source object/array, compare keys and values, mutate source independently and verify replacement snapshot. | partial |
