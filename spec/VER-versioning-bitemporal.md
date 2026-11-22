# VER — Revisions and versioning core

Spec target: **1.0.0**. Chapter status: **Draft for adoption before the approximately
9 December 2026 freeze**. Code baseline: `13f65148b117b9c8e1858a7f1510d2f65501bad9` (17 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance.
Existing test links are evidence inventories, not assertions that tests passed in this task.

## Scope

P0 revision numbering, timestamps, point-in-time reads, tombstones, versioning equivalence and revert. Complete valid-time query semantics remain P1; VER-401 fixes their boundary with transaction time.

## Requirements

### VER-101 — Resource-local revision timeline

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [UberPage.java](../bundles/sirix-core/src/main/java/io/sirix/page/UberPage.java):`getRevisionNumber`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`commitInternal`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`rollback`
- **Verified by:** [MultipleCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/MultipleCommitTest.java); [JsonMultiRevisionTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonMultiRevisionTest.java); [NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java) **Planned:** RevisionTimelineConformance: model commit/empty commit/rollback/revert/truncate with every versioning strategy.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source and mapped assertions inspected; no fresh runtime conformance claim.

Revision identity MUST be resource-local and numeric, beginning at bootstrap 0. Successful commit MUST append head+1, including empty commits and commits after revert. Rollback MUST NOT append a revision. Explicit truncate may remove the suffix and later append resumes from the retained head; it is not ordinary version creation. Because a reused number denotes a different revision, a conditional-write token is scoped by the resource incarnation id of TXN-311 rather than by the revision number alone.

### VER-102 — Revision isolation and identity

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [VersioningType.java](../bundles/sirix-core/src/main/java/io/sirix/settings/VersioningType.java); [JsonNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java):`updateObjectRecordValue`
- **Verified by:** [JsonModelBasedOracleTest.java](../bundles/sirix-core/src/test/java/io/sirix/property/JsonModelBasedOracleTest.java); [JsonIdentityHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/json/JsonIdentityHistoryTest.java); [JsonRoundTripPropertyTest.java](../bundles/sirix-core/src/test/java/io/sirix/property/JsonRoundTripPropertyTest.java) **Planned:** HistoricalIdentityConformance: same-type changes, type changes, removal, revert and cold reopen; compare serialized values and per-node history independently.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source and mapped assertions inspected; no fresh runtime conformance claim.

A retained revision MUST identify immutable logical content independent of the versioning strategy, page-cache temperature or reopen. A node that survives a same-type value change MUST retain its node key across revisions. Node replacement semantics follow JAPI-301; a replacement MUST NOT falsely continue the replaced node’s identity history.

### VER-103 — Revision bounds and pruned targets

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`assertRevisionExists`; [RevisionFileData.java](../bundles/sirix-core/src/main/java/io/sirix/io/RevisionFileData.java):`requireReadable`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`revertTo`
- **Verified by:** [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java):`readersAndTransactionsPreserveSparseTimeline`; [BoundedRevisionLookupTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/BoundedRevisionLookupTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source and mapped assertions inspected; no fresh runtime conformance claim.

Negative or future revision requests MUST fail rather than clamp to a different revision. An explicit retained revision MUST open exactly that revision. A pruned revision MUST raise `SirixRevisionPrunedException` carrying its revision and timestamp; readers, revert, truncate and path-summary access MUST NOT substitute a nearby retained revision.

### VER-201 — Timestamp precision and persistence

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [RevisionRootPage.java](../bundles/sirix-core/src/main/java/io/sirix/page/RevisionRootPage.java):`setCommitTimestamp`; [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java); [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`setCommitMessageAndTimestampIfRequired`; [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`decodeRevisionRecord`; [AbstractNodeReadOnlyTrx.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeReadOnlyTrx.java):`getRevisionTimestamp`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`getHistoryTimestampsMicros`; [RevisionIndex.java](../bundles/sirix-core/src/main/java/io/sirix/io/RevisionIndex.java):`findRevision`
- **Verified by:** [CommitTimestampPrecisionConformanceTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/CommitTimestampPrecisionConformanceTest.java): commits inside one millisecond, a tie and a backwards clock step, open-by-time at an exact microsecond, a user-supplied sub-millisecond instant round-tripped across cold reopen, sub-microsecond truncation toward the past, an out-of-range instant rejected without consuming a revision, and disabled custom timestamps; [CommitTimestampsTest.java](../bundles/sirix-core/src/test/java/io/sirix/utils/CommitTimestampsTest.java) (conversion unit, truncation direction and range contract); [CommitTimestampProbeAllocationBudgetTest.java](../bundles/sirix-core/src/test/java/io/sirix/budget/CommitTimestampProbeAllocationBudgetTest.java) (the probe conversion allocates nothing); [RevisionIndexIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexIntegrationTest.java) [CommitTimestampOutputTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/CommitTimestampOutputTest.java) executes all JSON serializers and XML metadata outputs; the owning [REST](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/CommitTimestampOutputTest.kt), [MCP](../bundles/sirix-mcp/src/test/java/io/sirix/mcp/CommitTimestampOutputTest.java), and [query](../bundles/sirix-query/src/test/java/io/sirix/query/function/sdb/trx/CommitTimestampOutputTest.java) tests execute public timestamp outputs for whole-second, millisecond and microsecond fixtures. **Planned:** tail-log recovery at microsecond precision.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Captain decisions of 17 and 21 November 2026 supersede the draft millisecond/equality contract and fix the stored unit at epoch microseconds. The unit, its truncation direction and its range check are implemented by `CommitTimestamps`, which every write and read site of the timestamp word now uses; the unchanged 64-bit field spans roughly ±292,000 years in microseconds, so the range check rejects only instants far outside any revision timeline. `getHistoryTimestampsMicros` replaces the millisecond accessor name outright, so no caller can reinterpret the unit silently. Because 1.0 has exactly one format, FMT-401 rejects direct beta opens and no millisecond-store conversion path exists; FMT-402 specifies conditional timestamp adjustment with queryable original metadata for historical import, which remains unimplemented. Status stays `partial` rather than `enforced` until the trace tooling of CONV-104 records a default-lane result for the mapped tests.

The stored commit timestamp MUST be a signed count of microseconds since the epoch in the existing 64-bit timestamp field; the 1.0 format defines no other commit-timestamp unit, and the engine MUST NOT read or write a millisecond-based store. Converting a supplied `Instant` to that value MUST truncate toward the past to a whole microsecond, and an instant whose microsecond value does not fit a signed 64-bit field MUST be rejected with an explicit error rather than wrapped or saturated. Commit timestamps MUST preserve the strictly increasing values required by TXN-310, including successive commits separated by only one microsecond. Persistence, revision-directory recovery, index lookup and public timestamp round trips MUST preserve every stored microsecond and MUST NOT collapse those values to milliseconds: an `Instant` read back through the public API MUST equal the stored microsecond value exactly, and any timestamp the API exposes as a 64-bit number MUST use the same microsecond unit. Public ISO timestamp text MUST retain ISO_INSTANT formatting: whole seconds omit fractions, millisecond-aligned values use three fractional digits, and other microsecond values use six fractional digits (for example `2021-03-04T05:06:07Z`, `2021-03-04T05:06:07.123Z`, and `2021-03-04T05:06:07.123456Z`). Disabled custom timestamp use MUST fail explicitly; a custom-timestamp facility MUST NOT bypass the strict commit-timestamp invariant.

### VER-202 — Point-in-time floor

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`getRevisionNumber`; [RevisionIndex.java](../bundles/sirix-core/src/main/java/io/sirix/io/RevisionIndex.java):`findRevision`
- **Verified by:** [RevisionIndexTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexTest.java); [RevisionIndexIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexIntegrationTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java) **Planned:** UniqueTimestampFloorConformance: exact and between-commit instants at microsecond separation, before-first/after-last, appended tail, cold rebuild and pruned timeline positions.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Both lookup overloads now convert the probe instant with `CommitTimestamps.toEpochMicros` and compare against stored microseconds, and the mapped conformance test asserts selection at an exact microsecond and one microsecond either side of it. The pruned-selection and cold-rebuild positions keep their existing sparse-history evidence.

getRevisionNumber(t) MUST select the unique revision with the greatest commit timestamp not after t, with before-first requests returning bootstrap 0 and after-last requests returning the head. The comparison MUST be performed at the microsecond resolution of VER-201, with the requested instant truncated toward the past, so an instant naming a stored commit microsecond exactly selects that revision and a sub-microsecond remainder never selects a later one. Strictly increasing timestamps under TXN-310 eliminate ties. A timestamp whose selected revision is pruned MUST still resolve to that revision number; opening its content MUST fail as pruned rather than move backwards.

### VER-203 — Ceiling-bounded point-in-time lookup

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`getRevisionNumber`
- **Verified by:** [BoundedRevisionLookupTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/BoundedRevisionLookupTest.java):`boundedLookupSeparatesMicrosecondNeighboursBeforeAndAfterReopen`; [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [CommitTimestampPrecisionConformanceTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/CommitTimestampPrecisionConformanceTest.java):`openByTimeSelectsTheRevisionOfAnExactMicrosecond`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** The ceiling search now reads `RevisionIndex.getTimestampMicros` and compares the probe in the same unit. Its mapped test commits two revisions per millisecond, one microsecond apart, and asserts that the bounded and unbounded overloads agree on each of them; a millisecond store could answer either.

The ceiling overload MUST reject an invalid numeric ceiling and MUST never return a revision above it. Within the ceiling it MUST select the highest revision whose timestamp is not after t, returning 0 when none qualify. A pruned ceiling remains a timeline position; content readability is checked separately. Both timestamp overloads MUST preserve VER-201 precision and MUST compare at the same microsecond resolution, so the two overloads agree on every instant at or below the ceiling.

### VER-204 — Unique timestamp resolution

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [RevisionIndex.java](../bundles/sirix-core/src/main/java/io/sirix/io/RevisionIndex.java):`findRevision`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`binarySearchLegacy`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`getRevisionNumber`; [RevisionInfo.java](../bundles/sirix-core/src/main/java/io/sirix/api/RevisionInfo.java):`getRevisionTimestamp`
- **Verified by:** [RevisionIndexTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexTest.java) **Planned:** StrictTimestampLookupConformance: constant/regressing live clocks; imported equal/decreasing/increasing sequences and cascades; exact/between-timestamp floor lookup for adjusted times, separate original metadata queries, search threshold/tail/cold-rebuild and both overloads; independent strict-sequence oracle with the stated four-revision example.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Captain decision 010 closes the migration interaction. Live commits now generate strictly increasing effective timestamps in the microsecond unit of VER-201, which is what makes the one-microsecond adjustment representable, so a supplied tie or regression is adjusted rather than rejected and no equal-timestamp lookup remains for a small-index binarySearch to resolve arbitrarily. Importer normalization for beta history and queryable original metadata remain implementation gaps, not unresolved policy.

Each resource timeline, including imported beta history, MUST have strictly increasing effective commit timestamps under TXN-310. Equal or backwards effective timestamps MUST NOT occur. Open-by-timestamp MUST resolve to exactly one revision under VER-202 or VER-203, independent of index size, search strategy, appended-tail state or cold reconstruction. Import MUST preserve already-increasing source timestamps and adjust only ties/regressions to the preceding effective timestamp plus 1 microsecond; each adjusted original MUST remain queryable as revision metadata. Original metadata timestamps MUST NOT participate in revision selection or reintroduce lookup ties. For example, source timestamps [1000, 1000, 999, 1005] microseconds MUST import as [1000, 1001, 1002, 1005], retaining the original 1000 and 999 on the adjusted revisions. A pruned selected revision MUST fail explicitly rather than choose another revision.

### VER-205 — History metadata

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`getHistory`; [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`getHistoryTimestampsMicros`
- **Verified by:** [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt):`restoresJsonAndXmlAndPreservesHistoryAfterColdReopen` **Planned:** HistoryMetadataConformance: range/limit/invalid bounds, author/message escaping, microsecond-separated timestamps and tombstones; adjusted imports expose original and effective timestamps separately after cold reopen and backup/restore; timestamp-only history matches effective full-history values.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** RevisionInfo exposes one effective revision timestamp, author and message, and the timestamp-only accessor is now `getHistoryTimestampsMicros`, reporting the epoch microseconds of VER-201. Separate queryable original import timestamp metadata is absent; its exact field/wire/API representation is deferred implementation work.

History MUST be returned newest-first over its requested revision range and preserve committed author, message and effective commit timestamp. For an imported revision whose timestamp was adjusted under FMT-402, history/metadata querying MUST also expose the original source timestamp distinctly from the effective timestamp. Original metadata MUST survive persistence, backup/restore and cold reopen. Timestamp-only history MUST use effective timestamps, expose them as epoch microseconds in the unit of VER-201 through an accessor whose name states that unit (`getHistoryTimestampsMicros`), and preserve the timeline positions of pruned revisions. The previous millisecond accessor name MUST NOT remain, so every caller fails to compile rather than silently reinterpreting the unit. Full metadata/content access to a pruned revision MUST fail explicitly instead of manufacturing a revision root.

### VER-206 — Last-modified revision stamping

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [NodeDelegate.java](../bundles/sirix-core/src/main/java/io/sirix/node/delegates/NodeDelegate.java):`setLastModifiedRevision`; [NodeKind.java](../bundles/sirix-core/src/main/java/io/sirix/node/NodeKind.java); [JsonNodeFactoryImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeFactoryImpl.java); [JsonNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java):`adaptForInsert`; [JsonNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java):`adaptForRemove`; [JsonNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java):`adaptForMove`; [XmlNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/xml/XmlNodeTrxImpl.java)
- **Verified by:** No existing test mapped. **Planned:** LastModifiedRevisionStampingConformance: for JSON and XML, apply every mutation kind (scalar value update, same-kind and type-changing replace, insert as first child/last child/left sibling/right sibling, remove, move, name change, attribute and namespace mutation, bulk insert, copy-subtree) and assert, after cold reopen, that every record whose value, name, listed structural links or attribute/namespace membership changed carries the writing revision and its preceding last-modified revision, including affected parents and siblings, while records with only hash/count bookkeeping changes keep both revision fields. Derive expected semantic changes from the mutation and an independent tree model, rather than a byte comparison. On a non-root XML element, insert and remove an attribute and a namespace separately; assert the owning element receives the writing revision and its previous stamp, and a conditional subtree replacement with a token from before each membership change is rejected in both hash modes. Also change the value/name of a retained attribute or namespace record and assert that scanning those records rejects the stale subtree replacement; an unchanged subtree and an edit outside it remain accepted. Run the same insert after a scalar-descendant update under an anchor container with ROLLING and NONE and assert identical insertion verdicts; also cover hash-only ancestor updates, count-only updates, reads and empty commits.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** Value-update paths stamp (`setStringValueFused` and the sibling setters, `updateObjectRecordValue`, the XML value path) and new records receive the writing revision from the node factories. The structural adaptation paths do not: `adaptForInsert` rebinds the parent child count and the first/last-child and sibling links, and `adaptForRemove`/`adaptForMove` relink neighbours, all without touching `setLastModifiedRevision`. XML attribute and namespace removal changes the surviving element's membership lists without stamping it; the deleted record is absent from a head-revision scan. A deletion, relinking or membership change is therefore invisible to a stamp-based reader at this baseline, which is exactly the missed-conflict case TXN-311 must not have. `storeNodeHistory` controls the separate record-to-revisions index, not this field; the field is serialized unconditionally by [NodeKind.java](../bundles/sirix-core/src/main/java/io/sirix/node/NodeKind.java).

Every document record whose value, name, structural links (parent, left sibling, right sibling, first child or last child), or attribute or namespace membership a commit changes MUST carry that commit's revision number in its last-modified revision field, and MUST carry its previous last-modified revision in the previous-revision field. A record created in revision N MUST carry N. Structural mutations MUST stamp every record whose listed links change, including affected parents, siblings and records at both ends of a move. Inserting or removing an XML attribute or namespace MUST stamp its owning element because that element's membership changes, even when none of the listed links changes. Derived bookkeeping changes alone — node hash, descendant count and child count — MUST NOT bump either revision field, including when those fields share a document record with the semantic fields. Reading a record MUST NOT stamp it, and an empty commit MUST NOT stamp anything. Thus TXN-311 MUST yield identical verdicts with hashing on or off. Derived structures (path summary, secondary indexes, projections) are out of this requirement's scope; TXN-311 consults document records rather than those structures.

### VER-301 — Versioning-strategy equivalence

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [VersioningType.java](../bundles/sirix-core/src/main/java/io/sirix/settings/VersioningType.java):`combineRecordPages`; [VersioningType.java](../bundles/sirix-core/src/main/java/io/sirix/settings/VersioningType.java):`combineRecordPagesForModification`
- **Verified by:** [VersioningTest.java](../bundles/sirix-core/src/test/java/io/sirix/settings/VersioningTest.java); [ProjectionSideReferenceVersioningMatrixTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/projection/ProjectionSideReferenceVersioningMatrixTest.java); [JsonDiffOverflowVersioningTest.java](../bundles/sirix-core/src/test/java/io/sirix/diff/JsonDiffOverflowVersioningTest.java); [VersioningTypeFsstMetadataCopyTest.java](../bundles/sirix-core/src/test/java/io/sirix/settings/VersioningTypeFsstMetadataCopyTest.java) **Planned:** VersioningDifferentialConformance: independent tree model, identical seeded stream under all four strategies and both persistent backends; compare every revision after reopen, not only current head.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source and mapped assertions inspected; no fresh runtime conformance claim.

FULL, INCREMENTAL, DIFFERENTIAL and SLIDING_SNAPSHOT MUST produce the same logical retained revisions for the same operation stream and settings. Fragment reconstruction MUST include overflow values, deletions, side references, dictionaries and compression metadata; a page boundary or fragment-window transition MUST NOT change content.

### VER-302 — Restore window is not retention

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`maxNumberOfRevisionsToRestore`; [VersioningType.java](../bundles/sirix-core/src/main/java/io/sirix/settings/VersioningType.java)
- **Verified by:** [VersioningTest.java](../bundles/sirix-core/src/test/java/io/sirix/settings/VersioningTest.java); [HOTMultiRevisionFragmentChainTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTMultiRevisionFragmentChainTest.java) **Planned:** RestoreWindowConformance: windows 1, 3 and larger; compare revisions before/at/after multiple window boundaries and reject zero/negative window.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source and mapped assertions inspected; no fresh runtime conformance claim.

maxNumberOfRevisionsToRestore MUST be strictly positive and MUST control fragment reconstruction/checkpointing, not the count of historical revisions retained. Revisions older than the window MUST remain readable unless explicitly pruned or truncated. Default strategy/window MUST be SLIDING_SNAPSHOT/3 until CONF changes the persisted contract.

### VER-303 — Sparse retention timeline

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`decodeRevisionRecord`; [RevisionIndex.java](../bundles/sirix-core/src/main/java/io/sirix/io/RevisionIndex.java):`isPruned`; [RevisionFileData.java](../bundles/sirix-core/src/main/java/io/sirix/io/RevisionFileData.java):`requireReadable`
- **Verified by:** [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** This specifies representation and read behavior only. Automatic retention policy, physical reclamation and legal erasure semantics are not implemented or promised by tombstone support.

Retention tombstones MUST preserve revision numbers and timestamps without renumbering later commits. Timestamp lookup MUST include those timeline positions, while content access fails explicitly. A corrupt record MUST NOT be mistaken for intentional retention and repair MUST NOT resurrect pruned content.

### VER-304 — Revert versus truncate

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`revertTo`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`truncateTo`; [RevertHandler.kt](../bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/crud/RevertHandler.kt):`handle`
- **Verified by:** [XmlRevertTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/XmlRevertTest.java); [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt); [NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java) **Planned:** RevertTruncateDifferentialConformance: identical JSON/XML fixtures, intervening history preserved by revert; protected reader prevents truncate without changing state; closing it permits truncate while older readers remain usable.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source and mapped assertions inspected; no fresh runtime conformance claim.

Revert MUST stage retained historical state for a new commit and preserve intervening history; current-head revert MUST still append when committed. Truncate MUST remove later history only under TXN-404: open readers of removed revisions cause immediate rejection identifying those readers, no waiting and no changes. APIs MUST distinguish these operations and MUST NOT implement revert as truncate.

### VER-401 — Transaction time and valid time are distinct

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [ValidTimeConfig.java](../bundles/sirix-core/src/main/java/io/sirix/access/ValidTimeConfig.java); [DerivedIndexMaintainer.java](../bundles/sirix-core/src/main/java/io/sirix/index/aggregate/DerivedIndexMaintainer.java); [AbstractResourceSession.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractResourceSession.java):`getRevisionNumber`
- **Verified by:** [DerivedRevisionMappingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedRevisionMappingTest.java); [ValidTimeIndexScanDifferentialTest.java](../bundles/sirix-query/src/test/java/io/sirix/query/function/jn/temporal/ValidTimeIndexScanDifferentialTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** This is the P0 separation boundary. Full interval semantics and bitemporal query contracts belong to the later VER/IDX/QRY extension; no unreviewed interval convention is frozen here.

Transaction-time revision selection MUST use commit history. Enabling valid-time configuration MUST NOT redefine commit timestamps, revision numbering or transaction-time lookup. Valid-time indexing MUST preserve the selected source revision; source/derived revision IDs MUST be mapped explicitly.

## Deferred work

- VER-201/204: implement precision-preserving 1.0 timestamp encoding, checked representable-range behavior and custom-input integration. Strict generation and unique lookup are decided. Direct beta access is rejected; imported timestamp normalization and original metadata preservation are decided by FMT-402. Their storage/query implementation remains pending.
- VER-303: automatic retention scheduling and physical reclamation remain deferred designs. Explicit truncate now has the decided fail-fast reader protection in TXN-404.
- VER-206: complete structural stamping is a decided target with a recorded implementation mismatch. It is the precondition the revision-based conditional check of TXN-311 depends on, and it is not delivered by this specification change.
- Storage amplification is strategy-dependent. No unmeasured constant-factor or globally minimal storage promise is introduced; CONV-301 preserves existing work budgets.

## Traceability

| Requirement | Existing or planned verification | Status |
|---|---|---|
| VER-101 | [MultipleCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/MultipleCommitTest.java); [JsonMultiRevisionTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/json/JsonMultiRevisionTest.java); [NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java) **Planned:** RevisionTimelineConformance: model commit/empty commit/rollback/revert/truncate with every versioning strategy. | partial |
| VER-102 | [JsonModelBasedOracleTest.java](../bundles/sirix-core/src/test/java/io/sirix/property/JsonModelBasedOracleTest.java); [JsonIdentityHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/node/json/JsonIdentityHistoryTest.java); [JsonRoundTripPropertyTest.java](../bundles/sirix-core/src/test/java/io/sirix/property/JsonRoundTripPropertyTest.java) **Planned:** HistoricalIdentityConformance: same-type changes, type changes, removal, revert and cold reopen; compare serialized values and per-node history independently. | partial |
| VER-103 | [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java):`readersAndTransactionsPreserveSparseTimeline`; [BoundedRevisionLookupTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/BoundedRevisionLookupTest.java) | partial |
| VER-201 | [CommitTimestampPrecisionConformanceTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/CommitTimestampPrecisionConformanceTest.java); [CommitTimestampsTest.java](../bundles/sirix-core/src/test/java/io/sirix/utils/CommitTimestampsTest.java); [CommitTimestampProbeAllocationBudgetTest.java](../bundles/sirix-core/src/test/java/io/sirix/budget/CommitTimestampProbeAllocationBudgetTest.java); [RevisionIndexIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexIntegrationTest.java) [CommitTimestampOutputTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/CommitTimestampOutputTest.java) executes all JSON serializers and XML metadata outputs; the owning [REST](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/CommitTimestampOutputTest.kt), [MCP](../bundles/sirix-mcp/src/test/java/io/sirix/mcp/CommitTimestampOutputTest.java), and [query](../bundles/sirix-query/src/test/java/io/sirix/query/function/sdb/trx/CommitTimestampOutputTest.java) tests execute public timestamp outputs for whole-second, millisecond and microsecond fixtures. **Planned:** tail-log recovery at microsecond precision. | partial |
| VER-202 | [RevisionIndexTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexTest.java); [RevisionIndexIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexIntegrationTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java) **Planned:** UniqueTimestampFloorConformance: exact and between-commit instants at microsecond separation, before-first/after-last, appended tail, cold rebuild and pruned timeline positions. | partial |
| VER-203 | [BoundedRevisionLookupTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/BoundedRevisionLookupTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java) **Planned:** BoundedTimestampPrecisionConformance: exact and between-commit microsecond instants with ceilings before/at/after the matching revision and after cold reopen. | partial |
| VER-204 | [RevisionIndexTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionIndexTest.java) **Planned:** StrictTimestampLookupConformance: constant/regressing live clocks; imported equal/decreasing/increasing sequences and cascades; exact/between-timestamp floor lookup for adjusted times, separate original metadata queries, search threshold/tail/cold-rebuild and both overloads; independent strict-sequence oracle with the stated four-revision example. | designed |
| VER-205 | [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt):`restoresJsonAndXmlAndPreservesHistoryAfterColdReopen` **Planned:** HistoryMetadataConformance: range/limit/invalid bounds, author/message escaping, microsecond-separated timestamps and tombstones; adjusted imports expose original and effective timestamps separately after cold reopen and backup/restore; timestamp-only history matches effective full-history values. | partial |
| VER-206 | No existing test mapped. **Planned:** LastModifiedRevisionStampingConformance: every mutation kind in JSON and XML; assert stamps and previous stamps for value/name/listed-link and attribute/namespace membership changes against an independent tree model after cold reopen, including insertion/removal on a non-root XML element and stale subtree replacement refusal, retained attribute/namespace edits found by the subtree scan, unchanged fields for hash/count-only bookkeeping, and identical insertion verdicts under ROLLING and NONE after a descendant update. | designed |
| VER-301 | [VersioningTest.java](../bundles/sirix-core/src/test/java/io/sirix/settings/VersioningTest.java); [ProjectionSideReferenceVersioningMatrixTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/projection/ProjectionSideReferenceVersioningMatrixTest.java); [JsonDiffOverflowVersioningTest.java](../bundles/sirix-core/src/test/java/io/sirix/diff/JsonDiffOverflowVersioningTest.java); [VersioningTypeFsstMetadataCopyTest.java](../bundles/sirix-core/src/test/java/io/sirix/settings/VersioningTypeFsstMetadataCopyTest.java) **Planned:** VersioningDifferentialConformance: independent tree model, identical seeded stream under all four strategies and both persistent backends; compare every revision after reopen, not only current head. | partial |
| VER-302 | [VersioningTest.java](../bundles/sirix-core/src/test/java/io/sirix/settings/VersioningTest.java); [HOTMultiRevisionFragmentChainTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTMultiRevisionFragmentChainTest.java) **Planned:** RestoreWindowConformance: windows 1, 3 and larger; compare revisions before/at/after multiple window boundaries and reject zero/negative window. | partial |
| VER-303 | [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java) | partial |
| VER-304 | [XmlRevertTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/node/xml/XmlRevertTest.java); [RevertHandlerTest.kt](../bundles/sirix-rest-api/src/test/kotlin/io/sirix/rest/RevertHandlerTest.kt); [NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/trx/page/NodeStorageEngineWriterTruncateToRevisionIntegrationTest.java) **Planned:** RevertTruncateDifferentialConformance: identical JSON/XML fixtures, intervening history preserved by revert; protected reader prevents truncate without changing state; closing it permits truncate while older readers remain usable. | partial |
| VER-401 | [DerivedRevisionMappingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/aggregate/DerivedRevisionMappingTest.java); [ValidTimeIndexScanDifferentialTest.java](../bundles/sirix-query/src/test/java/io/sirix/query/function/jn/temporal/ValidTimeIndexScanDifferentialTest.java) | partial |
