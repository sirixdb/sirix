# FMT — Disk format and compatibility

Spec target: **1.0.0**. Chapter status: **Draft for adoption before the approximately
9 December 2026 freeze**. Code baseline: `13f65148b117b9c8e1858a7f1510d2f65501bad9` (17 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance.
Existing test links are evidence inventories, not assertions that tests passed in this task.

## Scope

Persistent file identity, geometry, page framing, strict readers, feature negotiation, salts, golden pins, the single-format baseline with its export/import path, and format-relevant HOT decisions, including the runtime safety of HOT writes against page-cache eviction (FMT-304), which persists no bytes of its own but protects the HOT format decisions kept here. Commit ordering belongs to TXN; persisted JSON settings belong to CONF. Full index algorithms remain P1.

## Requirements

### FMT-101 — Resource file set

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`ResourcePaths`; [DatabaseConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseConfiguration.java):`DatabasePaths`; [Superblock.java](../bundles/sirix-core/src/main/java/io/sirix/io/Superblock.java):`validate`
- **Verified by:** [SuperblockResourceUuidTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockResourceUuidTest.java); [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java) **Planned:** FormatFileSetConformance: inspect a JSON and XML resource after creation, commit, cold reopen and backup restore.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Mapped source/tests inspected; runtime conformance was not executed for this documentation change.

A built-in persistent FILE_CHANNEL or MEMORY_MAPPED resource MUST keep its page store in `data/sirix.data`, revision directory in `data/sirix.revisions`, resource settings in `ressetting.obj`, index catalogues in `indexes/`, encryption-key material when configured in `encryption/`, update-diff sidecars in `update-operations/`, and recovery marker at `log/.commit`. The database settings file is `dbsetting.obj`. Settings and page files MUST identify the same resource; a zero UUID or UUID mismatch MUST fail open. IN_MEMORY has no durability guarantee and follows TXN-607 and TXN-615.

### FMT-102 — Superblock bytes

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [Superblock.java](../bundles/sirix-core/src/main/java/io/sirix/io/Superblock.java):`build`; [Superblock.java](../bundles/sirix-core/src/main/java/io/sirix/io/Superblock.java):`validate`
- **Verified by:** [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`superblockBytesArePinned`; [SuperblockSpareBytesTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockSpareBytesTest.java); [SuperblockResourceUuidTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockResourceUuidTest.java); [FormatVersionBaselineTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/FormatVersionBaselineTest.java); [FormatRepairBoundaryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/FormatRepairBoundaryTest.java) **Planned:** SuperblockRejectionConformance: complete wrong-role/offset corruption coverage.
- **Evidence class:** code+test (local verification recorded under FMT-401; CI verification pending)
- **Gap/note:** The zero-revision-stride and zero-UUID acceptances were removed. Ordinary-open and repair-boundary refusals are covered on both persistent backends, including foreign root envelopes combined with damaged settings and recovery triggers; see [FMT-401](#fmt-401--10-format-baseline). Exact frozen byte vectors still need implementation, and offset-field validation at 24/32 remains incomplete.

Both persistent files MUST begin with the 64-byte little-endian superblock in the table below. Writers MUST zero spare bytes. Readers MUST reject short/all-zero headers unless [FMT-401](#fmt-401--10-format-baseline) authorizes recovery, and MUST reject bad magic, unknown layout version, wrong role/endian marker, nonzero spare bytes, wrong slot size and checksum mismatch before any mutation of the store. A resource UUID MUST be nonzero and MUST match the settings identity whenever that identity is supplied; the revisions record stride MUST be 32. A zero stored UUID and a zero stride MUST both be rejected rather than accepted as a development-era exception.

### FMT-103 — Beacon placement and selection

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`PRIMARY_BEACON_OFFSET`; [AbstractReader.java](../bundles/sirix-core/src/main/java/io/sirix/io/AbstractReader.java):`readUberPageReference`; [FileChannelReader.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelReader.java):`readBeaconSlot`; [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`writeUberPageReference`
- **Verified by:** [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java); [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`uberPageBytesArePinned` **Planned:** BeaconSelectionConformance: valid older PRIMARY plus newer SECONDARY, torn each slot, both invalid, and first-commit cases on both persistent backends.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** The existing reader selects PRIMARY/fallback SECONDARY. Recovery of a newer committed record with stale beacons is not implemented by that selector and needs the decided commit-point protocol.

The data file MUST reserve independent 4096-byte beacon slots at offsets 4096 (PRIMARY) and 8192 (SECONDARY); page data starts at 12288. Between valid beacon candidates, PRIMARY MUST take precedence; SECONDARY is fallback when PRIMARY is invalid, not a highest-beacon-revision election. This beacon preference MUST NOT discard a newer revision proved committed by the durable-record protocol in TXN-302/604. Both invalid MUST fail unless committed-record recovery or proven-empty bootstrap recovery establishes the outcome. Unsupported required features MUST fail access, not be hidden by fallback to an older compatible image.

### FMT-104 — Revision directory geometry

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`revisionsFileOffset`; [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`revisionRecordChecksum`; [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`decodeRevisionRecord`
- **Verified by:** [RevisionRootPageHashValidationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRootPageHashValidationTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [FormatRepairBoundaryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/FormatRepairBoundaryTest.java) **Planned:** RevisionRecordGoldenTest: independent exact byte vector including tombstone, one-microsecond distinctions, corruption of each identity word and recovery round trips.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** The timestamp word now holds the signed epoch microseconds of VER-201 on every write and read path, including the tail-log ring and the tombstone encoder. No new numeric layout version is needed, because the word width and slot geometry are unchanged and a 1.0 reader refuses unsupported format versions (FMT-401). The independent exact byte vector of `RevisionRecordGoldenTest` remains planned; no existing golden pin carries a commit timestamp, so none moved with the unit.

Revision r MUST occupy the 32-byte slot at `4096 + 32*r`, including bootstrap r=0. Each slot MUST encode four little-endian 64-bit words: root-page offset, timestamp, checksum, root-image hash or prune tag. The timestamp word MUST be the signed epoch-microsecond value of VER-201; the 1.0 format defines no other timestamp unit, so the word MUST NOT be read as milliseconds. The checksum MUST cover the 24 bytes formed by offset, timestamp and hash/tag in that order. The revision number is the slot position, not a field. Zero root-image hash MUST NOT disable validation.

### FMT-105 — Pruned records are explicit

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`decodeRevisionRecord`; [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`writePrunedRevisionRecord`; [FileChannelReader.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelReader.java):`readRevisionRootPage`
- **Verified by:** [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [FormatRepairBoundaryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/FormatRepairBoundaryTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** The numeric prune-tag constant is pinned by the source-backed wire registry below. No new retention policy or physical erasure guarantee is introduced.

A pruned revision MUST retain its slot and timestamp with offset `Long.MIN_VALUE` and the exact `PRUNED_REVISION_TAG` (0x5052554E45440001) from IOStorage. Checksum verification MUST precede tombstone recognition. A checksum-valid record with any other invalid address/hash combination MUST be treated as corruption, not absence or pruning. Reads of a valid tombstone MUST raise `SirixRevisionPrunedException`.

### FMT-106 — Revision-record tail log

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`REVISION_RECORD_TAIL_LOG_ENTRY_BYTES`; [FileChannelReader.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelReader.java):`readRevisionRootPage`
- **Verified by:** [LazyRevisionRecordRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/LazyRevisionRecordRecoveryTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [FormatRepairBoundaryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/FormatRepairBoundaryTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Field-width table is below; this is recovery of directory records, not arbitrary page self-healing.

Each beacon slot MUST reserve a 16-entry ring of 48-byte revision-record recovery entries at its tail. An entry MUST contain revision, root offset, timestamp, record checksum, root hash and XXH3-64 checksum of its 40-byte prefix, using the field widths in IOStorage and the epoch-microsecond timestamp unit of FMT-104. Repair MUST stay at or below the selected durable frontier and MUST NOT resurrect a checksum-valid tombstone or replace a checksum-valid conflicting identity.

### FMT-107 — Beacon frame bytes

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractReader.java](../bundles/sirix-core/src/main/java/io/sirix/io/AbstractReader.java):`readVerifiedBeacon`; [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`writePageReference`; [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`REVISION_RECORD_TAIL_LOG_SLOT_OFFSET`
- **Verified by:** [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`uberPageBytesArePinned`; [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java); [LazyRevisionRecordRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/LazyRevisionRecordRecoveryTest.java) **Planned:** BeaconFrameConformance: independent length/payload/hash byte vectors and boundary lengths adjacent to the tail-log region.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** The reader currently checks the whole slot bound rather than a dedicated ring-overlap bound; writers use a tiny uber image. A recomputed-checksum oversized image needs explicit rejection coverage.

Each beacon MUST contain a little-endian u32 payload length, the uber-page payload, then an eight-byte canonical big-endian XXH3-64 checksum of that payload. Nonpositive or oversized lengths, truncated payload/trailer and checksum mismatch MUST fail validation. The tail log occupies the separately reserved last 768 bytes, not the uber payload. Writers MUST keep the payload/trailer clear of that reserved ring.

### FMT-108 — Durability profiles preserve one contract

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`writeUberPageReference`; [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`lazyRevisionRecordsEnabled`
- **Verified by:** [PreallocatedCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/PreallocatedCommitTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java); [PowerLossSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/PowerLossSimulationTest.java)
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current profiles use the beacon barrier as acknowledgement and may stage directory/tail-log records earlier. Exact record publication and durable evidence must be reconciled with the new point; preallocation/lazy profiles cannot weaken it. Existing opt-in crash evidence does not prove this revised protocol.

Growing-file commits MUST make page data and required file-size metadata durable before the revision-record commit point (TXN-302). Preallocated commits MAY use a data-only force when allocation metadata is already durable; new allocation MUST upgrade the barrier to include metadata. Buffered/write-through beacon and lazy revision-record profiles MUST preserve the same committed outcome and MUST NOT acknowledge before the new revision record is durably recoverable as a complete N. Tail-log records MUST become durable in the revision file before their only recovery copy is evicted. All profiles MUST honor the no-fsync-retry and resource write-stop rule on uncertain durability (TXN-705).

### FMT-201 — Page envelope and fixed IDs

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java):`readVersionAndFlagsAllowing`; [PagePersister.java](../bundles/sirix-core/src/main/java/io/sirix/page/PagePersister.java):`deserializePage`; [BinaryEncodingVersion.java](../bundles/sirix-core/src/main/java/io/sirix/BinaryEncodingVersion.java):`fromByte`
- **Verified by:** [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`pageKindIdsArePinned`; [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`nodeKindIdsArePinned`; [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`indexTypeIdsArePinned`; [BinaryEncodingVersionStrictTest.java](../bundles/sirix-core/src/test/java/io/sirix/BinaryEncodingVersionStrictTest.java); [StrictReferenceFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StrictReferenceFormatTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** The golden byte vectors are the numeric authority; this chapter does not replace them with another handwritten enum table.

Persisted page images MUST use the registered page-kind byte followed by binary version V0 and its flags byte, then any declared extension block and the kind-specific body. Readers MUST reject unknown page kinds, versions and unsupported flags before interpreting the body. Node-kind, page-kind and index-type IDs MUST remain the values pinned by GoldenFormatTest; enum ordinal is not a wire ID.

### FMT-202 — Page framing and checksum scope

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [FileChannelReader.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelReader.java):`readPageImage`; [FileChannelWriter.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelWriter.java):`write`; [PageHasher.java](../bundles/sirix-core/src/main/java/io/sirix/io/PageHasher.java); [HashAlgorithm.java](../bundles/sirix-core/src/main/java/io/sirix/io/HashAlgorithm.java)
- **Verified by:** [RevisionRootPageHashValidationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRootPageHashValidationTest.java); [GoldenCompositePageTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenCompositePageTest.java) **Planned:** FrameCorruptionConformance: negative/oversized/truncated lengths, bit flips, compressed and uncompressed images; compare FILE_CHANNEL and MEMORY_MAPPED failures.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Mapped source/tests inspected; runtime conformance was not executed for this documentation change.

Each ordinary data-page frame MUST begin with a four-byte length prefix followed by the stored page image. Readers MUST reject invalid lengths, truncated frames and enabled checksum mismatches before exposing decoded content. Page checksums MUST protect the stored representation used by the writer, independently of optional node/Merkle hashing. XXH3 checksums and page salts MUST NOT be presented as cryptographic authentication.

### FMT-203 — Optional page extensions

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [PageExtensions.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageExtensions.java):`readOrRefuse`; [PageExtensions.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageExtensions.java):`Builder`; [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java):`writeVersionAndFlags`
- **Verified by:** [PageExtensionsWriterTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/PageExtensionsWriterTest.java); [PageKindTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/PageKindTest.java):`optionalExtensionsPreserveSaltAndRecordsWhileRequiredEntriesRefuse` **Planned:** ExtensionReadConformance: all truncation positions, repeated/decreasing tags, oversized/overflowing lengths, required versus optional tags and rewritten bytes.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Mapped source/tests inspected; runtime conformance was not executed for this documentation change.

Flag 0x80 MUST introduce a length-delimited extension block immediately after the page flags. Each entry MUST encode an unsigned one-byte tag, unsigned stop-bit payload length and payload. Tags MUST be strictly ascending and unique. Unknown optional entries MUST be skipped within their declared bounds and discarded on rewrite; unknown required entries (tag bit 0x80) MUST fail. Tag bit 0x40 denotes kind-local scope. A writer MUST reject blocks exceeding its 255-byte supported write limit before changing builder state.

### FMT-204 — Store feature access matrix

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [StoreFeatures.java](../bundles/sirix-core/src/main/java/io/sirix/page/StoreFeatures.java):`requireReadable`; [StoreFeatures.java](../bundles/sirix-core/src/main/java/io/sirix/page/StoreFeatures.java):`requireWritable`; [StoreFeatures.java](../bundles/sirix-core/src/main/java/io/sirix/page/StoreFeatures.java):`requireToolCompatible`; [BackupManager.java](../bundles/sirix-core/src/main/java/io/sirix/backup/BackupManager.java):`backupDatabase`
- **Verified by:** [StoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StoreFeaturesTest.java):`everyUnknownBitFollowsTheAccessMatrix`; [InterruptedFirstCommitRecoveryStoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/InterruptedFirstCommitRecoveryStoreFeaturesTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Mapped unit tests prove the matrix assertions; complete open/backup/restore entry-point conformance remains to be run.

The uber page MUST publish three unsigned u32 feature words atomically with the commit beacon: incompat, readOnlyCompat and compat. Unknown incompat bits MUST reject all access; unknown readOnlyCompat bits MUST reject writes while allowing reads; unknown compat bits MAY be ignored by ordinary reads/writes but MUST reject maintenance tools that cannot preserve their semantics. Reserving a bit MUST NOT imply support. At this baseline every nonzero bit is unknown.

### FMT-205 — Uber feature encoding

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [UberPageFormat.java](../bundles/sirix-core/src/main/java/io/sirix/page/UberPageFormat.java):`read`; [UberPageFormat.java](../bundles/sirix-core/src/main/java/io/sirix/page/UberPageFormat.java):`write`; [StoreFeatures.java](../bundles/sirix-core/src/main/java/io/sirix/page/StoreFeatures.java):`union`
- **Verified by:** [StoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StoreFeaturesTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** No feature bits are implemented at this baseline. The planned FMT-209 assignment is in the wire table below; emission still requires implementation and compatibility tests.

An empty feature set MUST retain the original seven-byte uber image (kind, V0, zero flags, revision count). A nonempty set MUST use flag 0x80 and required kind-local tag 0xC1 with exactly 12 little-endian payload bytes in incompat/readOnlyCompat/compat order, preceding the revision count. Malformed lengths, unordered tags, unsupported required tags and trailing bytes MUST fail. Cloning and truncation MUST preserve feature requirements; staging MUST NOT mutate a committed uber page.

### FMT-206 — Record-page salt

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [PageLayout.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageLayout.java):`saltForWrite`; [PageLayout.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageLayout.java):`nonzeroSalt`; [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java):`serializeKeyValueLeafPage`; [RecordPageSaltSource.java](../bundles/sirix-core/src/main/java/io/sirix/page/RecordPageSaltSource.java):`nextSalt`
- **Verified by:** [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`deterministicRecordPageSaltIsPinned`; [RecordPageSaltSourceTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/RecordPageSaltSourceTest.java); [RecordPageSaltAllocationTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/RecordPageSaltAllocationTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Salt reduces identical page images; no uniqueness proof or adversarial tamper-resistance claim is made. CONF owns the default.

A salted record page MUST persist a nonzero 64-bit salt at offset 24 of its 32-byte slotted header. Existing nonzero salt MUST survive copy, reload and rewrite. An unsalted write MUST emit zero. Salt initialization MUST occur on writing, not reading; a random zero is mapped to one. Disabling node hashing MUST NOT remove the page-integrity checksum or salt fields.

### FMT-207 — Strict reference decoding

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java):`deserializeSegmentRefs`; [PagePersister.java](../bundles/sirix-core/src/main/java/io/sirix/page/PagePersister.java):`deserializePage`
- **Verified by:** [StrictReferenceFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StrictReferenceFormatTest.java) **Planned:** StrictReaderFuzzTest: fixed-seed byte mutations, all prefix truncations and differential heap/segment decoders; no unbounded allocation from counts.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Mapped source/tests inspected; runtime conformance was not executed for this documentation change.

Readers MUST reject malformed persisted references, unknown reference tags, impossible counts and truncated trailers before allocating from or traversing them. A corrupt pointer MUST NOT become an empty subtree. The strict behavior MUST apply to both heap and segment input representations.

### FMT-208 — Composite formats and codecs

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java); [RegionCompressionType.java](../bundles/sirix-core/src/main/java/io/sirix/settings/RegionCompressionType.java)
- **Verified by:** [GoldenCompositePageTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenCompositePageTest.java); [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java) **Planned:** FrozenStoreCorpusConformance: immutable hashes plus a feature/index/config coverage manifest, read every store with independent content/history/index expectations in default CI; deliberately incompatible reader fixture fails without changing corpus bytes.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current GoldenFormatTest/GoldenCompositePageTest pin development vectors. They are not a frozen complete 1.0-created store corpus; corpus production, inventory and CI enforcement are pending implementation.

The repository MUST keep an immutable frozen corpus of stores created by the 1.0 release, covering every supported feature, index and persisted configuration, including JSON/XML, storage backends, versioning strategies, compression, feature/extension handling and representative revision histories. Every later release MUST read this corpus correctly, enforced in default CI; unsupported-format refusal MUST NOT substitute for reading a supported 1.0 corpus member. Corpus bytes MUST NOT be regenerated to hide a regression. Composite-page and primitive encoding pins MUST cover record/HOT pages, index containers, revision references, delta varints, roaring64 values and supported codecs; new codecs require an explicit wire ID and unsupported-reader behavior before emission.

### FMT-209 — Conditional-write resource metadata

- **Level:** MUST
- **Status:** proposed
- **Since:** 1.0
- **Code:** [PageExtensions.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageExtensions.java):`readOrRefuse`; [PageExtensions.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageExtensions.java):`Builder`; [RevisionRootPage.java](../bundles/sirix-core/src/main/java/io/sirix/page/RevisionRootPage.java); [StoreFeatures.java](../bundles/sirix-core/src/main/java/io/sirix/page/StoreFeatures.java):`requireWritable`; [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`revertTo`; [NodeStorageEngineWriter.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineWriter.java):`truncateTo`
- **Verified by:** No existing test mapped. **Planned:** ConditionalWriteMetadataFormatConformance: a resource that never reverted has byte-identical revision-root images; the entry appears in the revert-publishing revision and in every later revision root; both fields survive cold reopen, every versioning strategy and both persistent backends; backup retains copied metadata, restore preserves last-revert metadata but draws a fresh incarnation id before exposure; truncate publishes the new id before removing data and a crash between the two retains that published id; controlled RNG draws demonstrate generation at creation, database recreation, replacement, truncate and repeated restore of an older backup, and tokens from the prior id fail even when revision numbers coincide; a store carrying the feature bit refuses writes from an engine that does not know the bit while still reading it; a malformed, truncated, duplicated or misordered entry is refused under FMT-203.
- **Evidence class:** doc-only (unverified); decided target has unverified implementation gaps
- **Gap/note:** Neither field, the extension tag nor the feature bit is implemented at this baseline; the planned assignments are in the wire table below. `RevisionRootPage`'s clone constructor is the carry-forward seam, and it currently copies only the node-key frontiers, timestamp, message and user. Implementing the bit and the tag before the format freeze is required because TXN-311 cannot be made safe by a later additive change: a writer that does not maintain these fields silently destroys the guarantee rather than failing. The split placement below is a Firstmate best-practice choice: the incarnation id changes only at resource creation, truncate, whole-resource replacement and backup restore, so paying revision-root bytes for it on every commit would be waste.

A resource MUST persist two pieces of conditional-write metadata (TXN-311): its incarnation id, identifying the resource incarnation and timeline the revision numbers belong to, and its last-revert revision, the revision number of the most recent commit that published a revert, 0 when none.

The last-revert revision MUST be published atomically with the commit that changes it and MUST be carried forward unchanged by every later revision root, so reading it at the head costs no additional page read. It MUST be encoded as the optional kind-local revision-root page-extension entry defined in the wire table below, under FMT-203 framing, and MUST be omitted entirely by a resource that has never reverted, keeping its revision-root images unchanged; absence MUST read as 0. A store that carries the entry MUST set the named `readOnlyCompat` feature bit in the wire table, so an engine that does not know the field may still read the store but MUST NOT write it: FMT-203 requires unknown optional entries to be skipped and discarded on rewrite, which would silently drop the evidence TXN-311 depends on. A maintenance tool that rewrites revision roots MUST preserve the value or refuse (FMT-204).

The incarnation id MUST be an independent uniform unsigned 64-bit value drawn from a cryptographically secure RNG and persisted in the resource settings as `incarnationId` (CONF-102), rather than per revision. A fresh draw MUST occur at resource creation (including recreation after resource or database deletion), truncate (TXN-404), whole-resource replacement (REST-210), and backup restore (TXN-613/614). Restore MUST regenerate the id for each restored resource before exposing it, while preserving copied history, last-revert metadata and derived state. Truncate MUST durably publish the new id before removing data (TXN-404). Ordinary commits, reopen and settings rewrites MUST preserve the id. No database-level counter or global allocator state is required. For any particular old token, a new draw reproduces its incarnation id with probability 2^-64; recreating or restoring a resource can therefore reproduce that old token only with this probability bound, rather than a guarantee of absolute uniqueness. Every 1.0 resource MUST contain the member; absence MUST NOT default to a reusable sentinel identity.

### FMT-210 — HOT leaf envelope and page-local modes

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java):`deserializeHOTLeaf`; [HOTLeafBody.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTLeafBody.java)
- **Verified by:** [HOTLeafDirectoryEncodingTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/HOTLeafDirectoryEncodingTest.java); [HOTLeafBodyEncodingTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/HOTLeafBodyEncodingTest.java) **Planned:** MixedLeafFormatConformance across backends and revision reconstruction, including standard extensions.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Mixed persisted stores and the complete envelope rejection matrix need default-lane evidence.

A HOT leaf image MUST start with `[kind=0x0c][V0=0x00][flags:u8]`.
Only the following flag bits are assigned.

| Flag | Meaning |
|---|---|
| `0x01` | The complete side-reference map follows the slot region (FMT-211). |
| `0x04` | The slot directory uses unsigned 16-bit offsets (FMT-211). |
| `0x08` | A body-mode byte follows the standard extensions, when present. |
| `0x80` | Standard page extensions immediately follow the flags (FMT-203). |

Kind 12 MUST NOT carry a kind-local codec. Flag `0x02` and all other
unassigned flags MUST fail at the FMT-201 envelope. The configured
resource byte pipeline (CONF) applies to the whole stored image.
Readers MUST reverse it before interpreting the kind-12 envelope.
With flag `0x08` clear, the body mode is zero and occupies no byte.
With it set, the mode byte MUST be nonzero, contain only assigned
bits, and be accompanied by flag `0x04`. Readers MUST reject a
missing or invalid mode byte at the FMT-201 envelope.

| Body-mode bit | Meaning |
|---|---|
| `0x01` | u8 key suffix lengths (FMT-212). |
| `0x02` | u8 value lengths (FMT-212). |
| `0x04` | A bitmap marks same-frame value aliases (FMT-215). |
| `0x08` | Groups of up to 16 slots share key prefixes (FMT-213). |
| `0x10` | CAS or VALIDTIME slots may use frame-of-reference postings (FMT-214). |

Every 1.0 reader MUST read all defined modes of kinds 12 and 9,
independent of writer policy. Mode-zero and packed frames MUST be
able to coexist in the same 1.0 store, index and revision history. These modes MUST use no store-feature negotiation or
feature bit (FMT-204, FMT-205, CONF-107). Store identity and beta
refusal still follow FMT-401. Stored-image framing and checksums
follow FMT-202.

**Acceptance criteria.** Pin the empty, mode-zero and packed vectors
in FMT-211 and FMT-212. Reopen a store with a mode-zero ancestor and
packed descendants. Compare point reads, ordered ranges and historical
reconstruction against independently recorded keys and values. Repeat
across both persistent backends, with identity and compression resource
pipelines and with writer settings changed on reopen. Test the mode byte
with and without a standard extension. Reject flag `0x02`, a missing
mode byte, a zero mode byte and each unassigned mode bit.

### FMT-211 — HOT leaf directory and slot framing

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java):`deserializeHOTLeaf`; [HOTLeafBody.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTLeafBody.java)
- **Verified by:** [HOTLeafDirectoryEncodingTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/HOTLeafDirectoryEncodingTest.java); [HOTLeafBodyEncodingTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/HOTLeafBodyEncodingTest.java) **Planned:** LeafSlotFrameConformance at each input boundary and across materialized and selective readers.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Local test and work-budget runs cover the u16-only directory, bounded stop-bit fields, strict trailer order and trailing-byte refusal. The suite preserves relocated mode-zero slots and tests that the retired wide directory is rejected. Default-lane evidence on the proposed baseline is pending.

After the envelope, the header MUST contain the page key as a
stop-bit integer, the revision as i32 LE, the index type as u8, the
common-prefix length as u16 LE followed by its bytes, the entry-count
word as u32 LE, and the physical slot-region length as i32 LE. A
stop-bit integer uses seven payload bits per byte, least significant
group first. Bit 7 is set when another byte follows. A stop-bit u32
MUST terminate within five bytes and a stop-bit u64 within ten.
Readers MUST reject overflow and unterminated fields. The page key
uses the u64 encoding. The low 31 bits of the entry-count word MUST
hold a slot count from 0 to 512. When the high bit is set, the
fragment contains every leaf entry at that revision. Reconstruction
MUST stop merging older fragments at that complete dump (VER-301).
A sparse fragment MAY set this bit if it already contains every
entry. The physical byte length MUST be in `0..65536` inclusive.
Readers MUST reject a nonzero mode unless the count and physical
length are both nonzero.

The directory MUST contain one u16 LE offset per slot, in key order,
relative to the first byte of the slot region. Flag `0x04` MUST be
set exactly when the slot count is nonzero. Readers MUST reject
a nonempty leaf with the flag clear and an empty leaf with it set.
The empty leaf has no directory bytes. In mode zero the slot region
immediately follows the directory. Mode-zero slot starts MAY leave
physical gaps or be out of physical order. Readers MUST accept both
when the slots are otherwise valid. A nonzero mode MUST lay out the
directory, optional sharing bitmap, optional group table and its prefix bytes,
then the slot region, in that order. Packed slot starts MUST begin at
zero and strictly increase. Each packed slot MUST consume exactly the
span up to the next offset or, for the last slot, the physical
slot-region length. The sum of `2 + ordinarySuffixLength + 2 +
expandedValueLength` across the packed slots MUST be at most 65,536.
The ordinary suffix includes the slot's restored group prefix and
stored suffix. The total excludes the common page prefix and
directory. It counts each aliased value in full and expands each
frame-of-reference posting to its ordinary stored value length.

When flag `0x01` is set, the complete map MUST follow the slot region.
Its stop-bit u32 count MUST be at most `512 × 65536` and is followed
by that many pairs of i64 LE composite key and i64 LE persisted
page offset. Writers MUST emit strictly ascending signed composite
keys. Readers MUST reject duplicate or nonascending composite keys
and negative page offsets. Each fragment carries the entire current
map, including a sparse fragment. A clear flag represents an empty
map. Reconstruction MUST preserve this meaning under VER-301.
Readers MUST bound each directory, metadata, slot and trailer region
against the frame before allocating for it. Bytes after the last
defined section MUST be rejected.
Complete materialization MUST validate the packed slots and expanded
capacity. Selective readers MUST validate the frame regions and the
spans they visit, including any selected alias owner, before returning
a value.

**Acceptance criteria.** These page images omit only the FMT-202
outer framing. Both use page key 7, revision 3, PROJECTION
(index ID `0x0a`) and an empty common prefix. The nonempty leaf
holds entries `(07, 2a)` and `(09, 33)`.

```text
empty     0c 00 00 07 03 00 00 00 0a 00 00 00 00 00 00 00 00 00 00
mode-zero 0c 00 04 07 03 00 00 00 0a 00 00 02 00 00 00 0c 00 00 00 00 00 06 00 01 00 07 01 00 2a 01 00 09 01 00 33
```

Exercise unsigned starts above 32,767 and a valid physical region of
65,536 bytes. Preserve a relocated mode-zero slot fixture. Reject
either mismatch of the directory flag and count, out-of-region
starts, gaps and equal or descending starts in packed slots,
mismatched final spans, expanded-size overflow, truncation and
trailing bytes in both modes. Verify that a complete dump ends fragment
merging and an empty current side map replaces an older nonempty map.
Reject negative offsets, duplicate or descending trailer keys, and
truncated, oversized or unterminated stop-bit fields.

### FMT-212 — Independent HOT slot length widths

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [HOTLeafBody.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTLeafBody.java); [HOTLeafBodyWriter.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTLeafBodyWriter.java)
- **Verified by:** [HOTLeafBodyEncodingTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/HOTLeafBodyEncodingTest.java) **Planned:** SlotLengthWidthConformance through independent fixtures for both mixed-width modes and both boundaries.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** The width boundary and malformed-frame matrix requires a default-lane record.

A slot MUST store its key suffix length, key suffix bytes, then
value length followed by either its inline bytes or an alias
pointer. Lengths default to u16 LE. Body-mode bit `0x01` changes
only the key suffix length to u8. Bit `0x02` changes only the
value length to u8. These choices apply to every slot in the frame.
The key length counts just the bytes stored in that slot. The value
length counts the value in its selected posting encoding, if any,
even when an alias occupies two physical bytes. A length MUST fit
the available bytes of the corresponding slot under FMT-211 and
FMT-215.

**Acceptance criteria.** The following kind-12 image represents the
same entries as FMT-211 with both length fields narrowed.

```text
0c 00 0c 03 07 03 00 00 00 0a 00 00 02 00 00 00 08 00 00 00 00 00 04 00 01 07 01 2a 01 09 01 33
```

Hold one field at 256 and vary the other at 255 and 256 to pin the
independent choices. Decode values and keys containing high-bit
bytes, empty suffixes and empty values. Reject truncated length
fields and suffixes or inline values that cross a slot boundary.

### FMT-213 — Grouped HOT key prefixes

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [HOTLeafBody.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTLeafBody.java); [HOTLeafBodyWriter.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTLeafBodyWriter.java)
- **Verified by:** [HOTLeafBodyEncodingTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/HOTLeafBodyEncodingTest.java) **Planned:** GroupPrefixConformance with independent complete frames at group boundaries and malformed metadata.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** The complete golden boundary and malformed metadata matrix is pending.

With body-mode bit `0x08` set, the group table MUST consist of
`ceil(entryCount / 16)` consecutive `[length:u8][offset:u16 LE]`
pairs, followed by the prefix bytes in group order. Slot `i`
belongs to group `floor(i / 16)`. The final group MAY hold fewer than
16 slots.
Offsets MUST be relative to the start of the prefix bytes. The
first offset MUST be zero and every later offset MUST be the
previous offset plus its length. Empty prefixes are valid. The last
offset plus its length gives the prefix region size. At most 32
groups of at most 255 bytes imply a maximum of 8,160 prefix bytes. The full key MUST be the common page prefix,
then the group prefix, then the stored slot suffix. Its length MUST
fit in u16. Exact, range and prefix lookup MUST compare the
resulting unsigned byte sequences, including their length when one
is a prefix of the other.

**Acceptance criteria.** With mode `0b`, common page prefix `aa`, two
slots and eight physical slot bytes, pin this directory, group
table, prefix region and slot region in order. The logical pairs
are `(aa 07 01, 2a)` and `(aa 07 02, 2b)`.

```text
00 00 04 00 | 01 00 00 | 07 | 01 01 01 2a 01 02 01 2b
```

Compare indexed exact and range results with the sorted logical
keys for lengths and counts crossing 15, 16 and 17 and for a final
partial group. Reject missing metadata, noncontiguous offsets,
prefix bytes crossing their region and an overlong reconstructed
key.

### FMT-214 — HOT posting frames of reference

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [HOTPostingFrame.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTPostingFrame.java); [HOTLeafBody.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTLeafBody.java)
- **Verified by:** [HOTPostingFrameTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTPostingFrameTest.java); [HOTLeafBodyEncodingTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/HOTLeafBodyEncodingTest.java) **Planned:** HOTPostingConformance across both index types, range queries and revision reconstruction.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Selected decoder and query cases have fixtures. Default-lane coverage of all width, index and fragment combinations is pending.

Body-mode bit `0x10` MUST be valid only for CAS (index ID `0x06`)
and VALIDTIME (index ID `0x0b`). Readers MUST reject the frame if
this bit is set for any other index type. In these posting domains,
tag `0x01` MUST require that bit. The posting MUST encode
`[tag:u8=01][count:u8][base:u16 BE][width:u8][count-1 deltas]`.
Count MUST be 1 through 64. The first ordinal is the chunk-local
base. Each remaining ordinal is base plus its unsigned big-endian
delta. Deltas MUST be strictly increasing and positive, and
base plus the final delta MUST be at most 65,535. A singleton MUST
use width zero. Longer postings MUST use width one when the largest
delta is at most 255, and width two otherwise. The slot value
length MUST be exactly `5 + (count - 1) × width`.

Posting values in the CAS and VALIDTIME domains MUST use one of
these tags. Readers MUST reject every unassigned posting tag.

| Tag | Posting representation |
|---|---|
| `00` | Ordinary packed posting with one eight-byte ordinal per entry. |
| `01` | Frame-of-reference posting defined above. |
| `fd` | Reference to a posting stored on a side page. |
| `fe` | Tombstone. |
| `ff` | Roaring64 bitmap. |

Referenced side-page payloads keep their existing encoding. Ordinal
search and sorted intersection MUST preserve the results of the
ordinary postings. Explicitly requested results MAY be materialized.

**Acceptance criteria.** Pin the exact encoded values below.
Compare ordinal search and sorted intersections against independently
constructed ordinary lists in both index domains.

```text
[65535]     01 01 ff ff 00
[256, 511]  01 02 01 00 01 ff
[256, 512]  01 02 01 00 02 01 00
```

Exercise counts 1 and 64, base zero, the maximum ordinal, and
ordinary or special values alongside postings. Reject incorrect
count or width, zero or descending deltas, an ordinal above 65,535,
length mismatch, an undeclared posting tag in the posting domains
and a declared posting mode on another index type. Reject an
unassigned posting tag. **Planned:** `HOTPostingReadAllocationTest`
uses the CONV-301 allocation budget for warmed packed ordinal search
and sorted intersection with preallocated input and result storage.
The measured operations allocate zero bytes.

### FMT-215 — Same-frame HOT value sharing

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [HOTLeafBody.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTLeafBody.java); [HOTLeafBodyWriter.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTLeafBodyWriter.java); [HOTLeafPage.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTLeafPage.java)
- **Verified by:** [HOTLeafBodyEncodingTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/HOTLeafBodyEncodingTest.java) **Planned:** SameFrameAliasConformance on materialized, cached, selective and reconstructed reads.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Alias and fragment cases are mapped. Complete cross-path default-lane evidence is pending.

When body-mode bit `0x04` is set, the bitmap MUST occupy
`ceil(entryCount / 8)` bytes. Bit `i % 8` of byte `floor(i / 8)`
selects alias form for slot `i`. Unused high bits MUST be zero.
An alias MUST write the encoded value length and exactly two
pointer bytes in place of inline value bytes. The pointer MUST
be a u16 LE offset relative to the start of the slot directory,
and MUST name an earlier inline owner's two-byte directory entry.
It MUST be even and strictly less than `2 × i`. The encoded
value length MUST exceed two bytes and match the owner's value
length exactly. The owner's value span MUST fit within its own
slot and end at or before the start of the alias slot.
The owner MUST be in the same emitted frame, including for a
sparse fragment. Writers MUST compare complete encoded value byte
ranges after choosing the posting representation before sharing.

A reader MAY use a view of owner bytes while it holds the frame
live or under a protocol that keeps the memory safe to read and
validates frame identity and integrity before exposing the result.
On failed validation the reader MUST discard the tentative result.
A mutable page and a mutable reconstruction MUST own independent
slot values. Releasing or replacing one fragment MUST preserve
values in every other fragment.

**Acceptance criteria.** With mode `07` and two one-byte keys
`07` and `09`, pin the image below. The second slot reuses the
eight bytes `01 02 03 04 05 06 07 08` from the inline first slot.

```text
0c 00 0c 07 07 03 00 00 00 0a 00 00 02 00 00 00 10 00 00 00 00 00 0b 00 02 01 07 08 01 02 03 04 05 06 07 08 01 09 08 00 00
```

Use a second fixture with a nonzero owner entry offset to distinguish
it from a slot-region offset. Reject unused bitmap bits, pointers
that are odd, self-referential, forward or target another alias,
short or mismatched values, and overlapping or truncated owner
spans. Compare exact value bytes across mixed revision reconstruction
and verify that edits to a mutable copy preserve its former owner's
value and other immutable fragments. Evict and replace a frame
during a read. A valid result corresponds to the held or successfully
validated frame. A failed validation exposes no tentative value.

### FMT-216 — Projection overflow envelope and codecs

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java):`OVERFLOWPAGE`; [ZeroRunByteCodec.java](../bundles/sirix-core/src/main/java/io/sirix/page/ZeroRunByteCodec.java); [ByteRunCodec.java](../bundles/sirix-core/src/main/java/io/sirix/page/ByteRunCodec.java); [SirixLZ77Codec.java](../bundles/sirix-core/src/main/java/io/sirix/page/SirixLZ77Codec.java)
- **Verified by:** [ProjectionOverflowEncodingTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/ProjectionOverflowEncodingTest.java); [ByteRunCodecTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/ByteRunCodecTest.java); [ZeroRunByteCodecTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/ZeroRunByteCodecTest.java) **Planned:** ProjectionOverflowFormatConformance for every codec and raw/compressed projection and ordinary frame.
- **Evidence class:** code+test (source inspection of envelope and selected length checks)
- **Gap/note:** Local fixtures attest to the envelope and length checks, the three accept-fixtures of the acceptance criteria and the enumerated rejections, including after decode-scratch reuse. Every codec now bounds literal, extension and repeat spans by the declared stored length and refuses byte-run counts below two, zero repeat bytes in nonzero repeat runs and the unassigned opcode; the run codecs additionally require exact frame consumption, which the LZ77 grammar does not admit because its trailing literal-only token may go unread. The reader corroborates the codec size against the outer decoded length, and the declared size against what the body bytes after the frame header could emit, before reserving either destination; that ceiling is a necessary condition and exact production stays with the decoders. LZ77 overflow frames take a bounds-checked native entry point whose input checks are compiled into the same loop as the trusted one, so stored bytes never reach an unbounded input loop and a valid frame keeps native decode speed; the Java decoder gives the same refusals where the bundled library is absent. The full rejection matrix still needs default-lane evidence, and token validation is enforced during the single bounded decode into borrowed scratch rather than as a separate pre-pass.

Kind 9 MUST start with `[kind=0x09][V0=0x00][flags:u8]`.
Flag `0x01` selects an outer codec. Flag `0x02` marks a typed
projection payload. These two flags are independent. Flag
`0x80` follows FMT-203. All other flag bits MUST fail at the
FMT-201 envelope. After any extensions, the body MUST start with
a nonnegative i32 LE decoded length. With `0x01` clear, the
writer MUST append exactly that many payload bytes. With the
compression flag set, the writer MUST append
`[storedLength:i32 LE][codec:u8][stored bytes]`. The stored length
MUST be positive and within the remaining frame. For compressed
projection frames it MUST equal the remaining frame length.

| Codec ID | Codec and initial frame marker |
|---|---|
| `0` | Zero-run (`ff`) |
| `2` | Byte-run (`fe`) |
| `3` | Sirix LZ77 (`fd`) |

The codec assignments are local to kind 9. Readers MUST reject
ID `1` and IDs `4..255` at the kind-9 codec header. Record-body
codec IDs and resource compression settings are separate registries.
The codec frame MUST start with its marker and a stop-bit u32
decoded size using the bit order in FMT-211. For the run codecs, opcodes
`00..7f` MUST encode a literal run of opcode plus one bytes.
The remaining opcodes MUST follow this table.

| Codec | Opcode | Meaning |
|---|---|---|
| Zero-run | `80..fe` | Opcode minus `80` plus two zero bytes. |
| Zero-run | `ff` | Stop-bit zero count, two or more. |
| Byte-run | `80..bf` | Opcode minus `80` plus two zero bytes. |
| Byte-run | `c0..fc` | Opcode minus `c0` plus two copies of the following nonzero byte. |
| Byte-run | `fd` | Stop-bit zero count, two or more. |
| Byte-run | `fe` | Stop-bit repeat count, two or more, followed by the nonzero byte. |
| Byte-run | `ff` | Unassigned. |

Readers MUST reject Byte-run long counts below two and zero
repeat bytes in nonzero repeat runs. An LZ77 token MUST use its
upper nibble for the literal length and, when a match follows,
its lower nibble for the match length minus four. A nibble
of 15 extends its length using successive bytes summed until a
byte below 255, included in that sum. The literal length
extension, when present, precedes the literal bytes. When
decoded bytes are still needed after the literals, a u16 LE
distance precedes any match length extension. The distance
MUST be positive and refer into previously decoded bytes. A
match copies from that distance behind the current output
position and MAY overlap the bytes it is producing. Decoding
MUST produce exactly the declared number of decoded bytes.

Readers MUST reject a truncated or invalid outer length before
allocating for decoding. On compressed frames, the codec size
MUST equal the outer decoded length. The reader MUST validate
both lengths and their agreement before allocating decoded bytes.
Before reserving a destination based on the decoded length,
the reader MUST verify that the token stream within the stored
region can produce that exact length. A reader MAY instead
grow storage as it validates tokens, bounded by the output
validated so far. In both cases the reader MUST enforce any
limit specified by the owning payload format, and MUST reject
malformed or truncated tokens and decoded-length mismatches
before exposing payload bytes.

**Acceptance criteria.** Pin the kind-9 envelope and decoding
with independently supplied payload bytes. The following are
projection-typed page images with no standard extension.

```text
raw          09 00 02 04 00 00 00 01 02 03 04
zero-run     09 00 03 40 00 00 00 03 00 00 00 00 ff 40 be
byte-run     09 00 03 40 00 00 00 05 00 00 00 02 fe 40 fe 40 2a
Sirix LZ77  09 00 03 40 00 00 00 07 00 00 00 03 fd 40 1f 2a 01 00 2c
```

The zero-run fixture produces 64 zero bytes. The other encoded
fixtures produce 64 copies of `2a`. Exercise the ordinary
flag combinations and reopen with writer compression disabled.
Reject unknown flags or codecs, disagreement between codec and
outer lengths, invalid repeat bytes or counts, malformed or
truncated tokens, and extra bytes beyond the stored length of a
compressed projection frame. Check the same malformed spans
with heap and segment inputs, including after scratch reuse.
Allocation evidence checks that an impossible decoded size is
rejected without reserving a destination of the declared size.

### FMT-217 — HOT and projection writer conformance

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [HOTLeafBodyWriter.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTLeafBodyWriter.java):`select`; [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java):`serializeHOTLeaf`; [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java):`keepsProjectionOverflowCodec`
- **Verified by:** [HOTLeafBodyEncodingTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/HOTLeafBodyEncodingTest.java); [ProjectionOverflowEncodingTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/ProjectionOverflowEncodingTest.java); [ProjectionSortedFullImagePackingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/projection/ProjectionSortedFullImagePackingTest.java) **Planned:** FrameSelectionConformance and paired latency, allocation and equivalent-workload size gates.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Projection outer compression inherits the existing default-on overflow setting. Its reads-first evidence is pending, including the scored leaf run, native-image allocation checks and comparison store measurements. Default eligibility for this change is therefore unverified.

A writer MUST emit only frames valid under FMT-210 through FMT-216.
An empty leaf MUST use mode zero.

**Writer policy for this change.** Evaluate candidates independently
for each full or sparse frame and check selected source slots and
live bytes against the used region. Prefer one-byte lengths where
all corresponding lengths fit. Use grouping or sharing only if each
saves bytes after its own metadata, and use a frame-of-reference
posting only if smaller than the ordinary posting. Prefer mode zero
for projection keys of eight bytes including the common prefix, or
when the packed candidate fails to save bytes after the mode byte and
metadata compared with the same slots in mode zero.

For projection kind 9, consider outer compression after producing
the typed payload. This change's eligibility rule is an inclusive
20% saving after counting five bytes for stored length and codec ID.
The size comparison is `5 × (storedLength + 5) ≤ 4 × encodedLength`
using wide arithmetic. Apply the rule to the projection writer,
with the reads-first evidence below. Ordinary overflow keeps its
own selection policy. Readers accept valid frames regardless of
writer policy.

**Acceptance criteria.** Compare writer output against independent
mode-zero and packed fixtures. Include an eight-byte projection key
with a shared page prefix and a packed candidate whose savings equal
its metadata costs. Exercise one-byte boundaries, fragment selection
and an invalid source slot beyond the used region. With encoded
length 100, stored lengths 74 and 75 pass the policy's size
comparison and 76 fails. Verify that readers accept valid frames
produced under other policies. Mutation-test decoding and selection
and disposition each survivor under CONV-103. Record the
[read, allocation and size evidence](../docs/VERIFICATION.md#hot-leaf-encoding-release-acceptance)
before approving the default writer policy.

### FMT-301 — HOT fanout legality

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [HOTMalformedSubtreeDetector.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTMalformedSubtreeDetector.java):`firstViolation`; [HOTIndirectPage.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTIndirectPage.java); [NodeUpgradeManager.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/NodeUpgradeManager.java):`determineNodeType`
- **Verified by:** [HOTInvariantValidator.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidator.java); [HOTInvariantValidatorChecksTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidatorChecksTest.java); [HOTHeightOptimalTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTHeightOptimalTest.java) **Planned:** HOTFanoutConformance: inject one-child nodes at every production constructor/decoder/publish boundary; delete down to one child, cold reopen, then scan and point lookup.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Captain call 6 resolved to illegal and enforced. Validator rejects n<2, but production detector lacks that predicate and determineNodeType(1) is accepted in an existing test. These are implementation/test gaps, not an open product choice.

A reachable or persisted HOT indirect node MUST have at least two children and at most its node-type capacity (SPAN_NODE 16; MULTI_NODE 32). A one-child indirect node MUST be rejected or collapsed before publication, including decode, mutation and bulk-build paths; it MUST NOT be accepted as a legal steady state.

### FMT-302 — HOT discriminative-bit height bound

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [HOTInvariantValidator.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidator.java):`walk`; [HOTInvariantValidator.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidator.java):`validateI11TrieCondition`; [HOTMalformedSubtreeDetector.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTMalformedSubtreeDetector.java):`firstViolation`; [HOTIndirectPage.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTIndirectPage.java):`getMostSignificantBitIndex`; [DiscriminativeBitComputer.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/DiscriminativeBitComputer.java):`computeDifferingBit`
- **Verified by:** [HOTInvariantValidatorChecksTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidatorChecksTest.java); [HOTHeightOptimalTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTHeightOptimalTest.java); [HOTStructuralPropertyTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTStructuralPropertyTest.java) **Planned:** HOTHeightBoundConformance: independent encoded-key discriminative-position oracle; empty/leaf-only/one-indirect conventions; ascending, descending, shuffled, long common-prefix and variable-length/prefix keys; valid boundaries and malformed repeated/decreasing/undefined/out-of-range leading positions; assert h <= D and D <= 8*L after mutation and cold reopen, across node types; compare lookup/scan with an ordered-map oracle.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Captain decision 17 November 2026 fixes the numeric bound and requires invariant-validator enforcement. HOTInvariantValidator.walk currently compares depth with supplied maxHeight (default 32), not D or 8*L. validateI11TrieCondition and the production detector enforce increasing defined parent/child MSBs, but skip undefined positions; they do not independently establish the key-derived height bound. HOTHeightOptimalTest minimum-height/canonical-shape Javadoc is not the adopted contract.

Let `h` be the maximum number of indirect nodes on any root-to-leaf path in the logical HOT: an empty trie or leaf-only root has `h = 0`, and one indirect root above leaves has `h = 1`. Leaf pages, overflow chains and non-routing metadata MUST NOT count as indirect levels. Let `D` be the number of distinct distinguishing (discriminative) bit positions among the indexed keys under their key encoding, and `L` the longest such encoded key length in bytes (zero for an empty trie). The guaranteed bound MUST be `h <= D <= 8 * L`. Bit positions MUST use absolute MSB-first numbering: bit 0 is the most significant bit of the first key byte; larger positions are later. Each successive indirect level on a path MUST test a strictly later leading discriminative bit: its smallest discriminative position MUST be greater than its parent’s. Every reachable indirect level MUST have a defined discriminative position within the encoded-key bit range. These strictly increasing positions give each indirect level a distinct position in D and establish the bound. The invariant validator MUST enforce both the bit progression and the derived height bound; an arbitrary fixed depth cap MUST NOT substitute for that verification. Measured typical heights are informative only. The 1.0 contract MUST NOT claim globally minimal height or insertion-order-independent canonical shape; lookup and sorted scan results MUST remain independent of insertion order.

### FMT-303 — No DeweyID HOT index promise

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [IndexType.java](../bundles/sirix-core/src/main/java/io/sirix/index/IndexType.java)
- **Verified by:** [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`indexTypeIdsArePinned`
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Captain call 4 drops this proposal from 1.0. This does not remove stored DeweyIDs or existing document traversal support.

The 1.0 format/API capability set MUST NOT advertise a DeweyID HOT forward/reverse index. Existing DEWEYID_TO_RECORDID support remains distinct; index IDs already assigned to VECTOR and PROJECTION MUST NOT be reassigned to the abandoned proposal.

### FMT-304 — HOT mutation leaf lifetime

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [AbstractHOTIndexWriter.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/AbstractHOTIndexWriter.java):`enterMutation`; [AbstractHOTIndexWriter.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/AbstractHOTIndexWriter.java):`resolveHOTPageForTraversal`; [AbstractHOTIndexWriter.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/AbstractHOTIndexWriter.java):`compareSubtreeExtreme`; [HOTIncrementalInsert.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTIncrementalInsert.java):`consolidateNodeLeaves`
- **Verified by:** [HOTIndexWriterLeafRetentionTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTIndexWriterLeafRetentionTest.java); [HOTIncrementalInsertLeafRetentionTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTIncrementalInsertLeafRetentionTest.java); [HOTIndexWriterLeafRetentionTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTIndexWriterLeafRetentionTest.java):`whenAMutationEndsTheLeavesItHeldBecomeReclaimable` (clause 5), `aRequiredReadThatKeepsLosingItsLeafFailsByName` (the interim bound); [HOTStructuralPropertyTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTStructuralPropertyTest.java):`mutationsSurviveLeafEviction`; [HOTStructuralValidatorLeafRetentionTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTStructuralValidatorLeafRetentionTest.java) (clause 3's "a mutation MUST NOT report the index malformed because a leaf was evicted": the structural validation that follows a published splice, with every unheld leaf retired at the publication boundary and evicted again at every engine call); [HotWriterLeafRetentionAllocationBudgetTest.java](../bundles/sirix-core/src/test/java/io/sirix/budget/HotWriterLeafRetentionAllocationBudgetTest.java) **Planned:** HOTWriterLeafLifetimeConformance: a second thread evicting through the shared page cache while two resources write, with released frames poisoned, on every versioning type. **Planned:** the same eviction sweeps with packed writer output (aliases, groups, postings) once FMT-217 lands, because consolidation then copies entries from packed sources rather than the raw leaves these sweeps used.
- **Evidence class:** code+test (the eviction sweeps evict at every engine call of a mutation, with released frames poisoned, and compare the whole index with a model; the clause 6 allocation budget passes before the fix as well, by design -- the violation it distinguishes is the fix itself adding per-put allocation or a sibling hold, which is what it caught twice during this change)
- **Gap/note:** Deferred reclamation is not implemented yet; it is a separate task, and until it lands this requirement is met only in part. In the interim a required re-resolution is bounded, and at the bound the mutation raises `SirixLeafRetiredException`, which names leaf retirement and is distinct by type from corruption, I/O, exhausted frame memory and an absent key. No published revision changes, a reader opened after the failure sees the same contents as before, and explicit rollback stays available. Commit is refused if the transaction's private index state may have changed. The shared `assertTransactionWritable` seam surfaces that refusal as `SirixIOException` with the leaf-retirement failure as its cause; changing that shared exception type is outside this change. The refusal is implemented in the writer but has no single-threaded witness, because the only reachable instance of the bound is the required read a put performs before it enters its mutation, where nothing private has been built. A 24-point starvation sweep over the consolidation shape shows why: points 1 to 3 fail on that pre-mutation read and every later point completes, since inside a mutation leaves arrive through the guarding resolver, which takes the guard as part of resolution and leaves no window for a handed-out leaf to be retired. The refusal arm, and the lost-guard retry it belongs to, are covered only by the planned concurrent-evictor conformance case, where deferred reclamation removes the window altogether. The eviction tests otherwise evict on the writing thread. The requirement covers one index mutation (put, posting removal, posting-chunk replacement). A writer read outside a mutation takes its own guard or validates an optimistic stamp and is outside these tests. The caller duty behind clause 4 is checked by an assertion that runs only where assertions are enabled, so a standalone caller that violates it in production is not detected. See the [postmortem](../docs/postmortems/2026-11-hot-writer-unheld-leaf-reads.md).

A HOT mutation MUST NOT fail because the page cache evicted a leaf it needs. Eviction MUST NOT
reclaim or reuse a frame while any reader or writer may still read it: reuse is deferred until no
such reader remains (an epoch or cooling stage), a leaf that may still be read is cooled rather than
reclaimed, and a read taken without a hold MUST be validated against the frame's version and
discarded when that validation fails. Within that, while it applies one index mutation, a HOT writer
MUST satisfy all of the following.

1. **No read of reclaimable memory.** A mutation MUST NOT read a leaf's contents -- its keys, values
   or side-reference markers -- from memory that eviction can reclaim or hand to another page during
   that read. A read whose memory cannot be ruled out MUST be validated afterwards by a check that
   proves the memory was not released while it was read, and its result MUST be discarded when that
   check fails.
2. **Eviction changes no logical result.** Eviction during a mutation MUST NOT change the logical
   contents the mutation reads or publishes. After the mutation and after its commit, every key MUST
   map to exactly the values it would map to had no eviction occurred, and the index MUST satisfy
   FMT-301 and FMT-302. Eviction MAY change only the physical shape left by optional maintenance
   that declines under clause 3. No entry MUST be published from, and no structural decision based
   on, memory that was reclaimed under the read.
3. **Only optional work declines; required work waits.** Optional maintenance -- consolidation and
   similar -- MAY decline when a leaf it would read is unavailable, and MUST decline rather than
   proceed on an unsafe read. Work the mutation requires MUST NOT decline and MUST NOT fail: it
   waits for the frame it needs, which deferred reclamation guarantees will still be there. A
   mutation MUST NOT report the index malformed because a leaf was evicted.
4. **Consolidation reads only safe leaves.** Optional consolidation MUST read only leaves that are
   held for the duration of the read, owned by the transaction-intent log, or private to the
   mutation. This includes every surviving child it resolves again while rebuilding the parent. How
   a direct caller of the consolidation primitive discharges that duty, and how the writer does so,
   belong to that method's contract and not to this requirement.
5. **Lifetime of a hold.** A hold MUST NOT be released while the mutation may still read that leaf
   unheld, and every hold MUST be released by the time the mutation ends, normally or by exception.
   Releasing a hold between attempts within a mutation is allowed, and re-reading such a leaf later
   requires holding it again. Evicting a held leaf MUST NOT release or reuse its memory before that
   release; memory MUST NOT be released before the leaf is marked orphaned; and an orphaned leaf
   MUST NOT become live again.
6. **Cost.** A mutation that reads no leaf of the page cache MUST NOT allocate or take a hold for
   this rule, and a put that only merges into its own leaf MUST NOT hold a sibling.

Clauses 1 and 2 are observable, and the acceptance evidence is the poisoned-frame eviction sweeps,
which strike at every call a mutation makes on its storage engine, compare the whole index with a
model, require the structural validator to accept the result, and then commit and verify the
history. Clause 3's decline arm is observable the same way. Clause 5 is observable through the page
cache: once a mutation ends, pressure eviction must be able to take back every leaf it held, and a
hold left unreleased keeps that leaf resident for the session. Clause 6 is observable as allocated
bytes in a clean-JVM budget test. Clause 4 is observable through clause 3's declines, while the
caller duty behind it is checked by a test-only assertion.

### FMT-305 — HOT routable key bound

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [HOTKeySerializer.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTKeySerializer.java):`ROUTABLE_KEY_BYTES`; [NameKeySerializer.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/NameKeySerializer.java):`MAX_NAME_KEY_BYTES`; [NameIndexRecheck.java](../bundles/sirix-core/src/main/java/io/sirix/index/name/NameIndexRecheck.java):`exactPostings`; [HOTIndirectPage.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTIndirectPage.java):`canNameDiscriminativeBytes`; [HOTBulkBuilder.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTBulkBuilder.java):`assembleOverLevels`; [AbstractHOTIndexWriter.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/AbstractHOTIndexWriter.java):`routeCanNameBranchBit`; [CASKeySerializer.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/CASKeySerializer.java):`MAX_STRING_VALUE_BYTES`; [HOTBulkBuilder.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTBulkBuilder.java):`assembleIndirect`; [HOTIndirectPage.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTIndirectPage.java):`computeMostSignificantBitIndex`; [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java):`HOTINDIRECTPAGE`
- **Verified by:** [NameIndexNamespaceTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/name/NameIndexNamespaceTest.java):`longNamespacesRemainExactAcrossBuildMutationAndColdHistory`; [NameIndexRecheckAllocationBudgetTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/name/NameIndexRecheckAllocationBudgetTest.java); [NameIndexLongSharedPrefixRoutingTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/name/NameIndexLongSharedPrefixRoutingTest.java); [NameIndexRecheckWorkBudgetTest.java](../bundles/sirix-core/src/test/java/io/sirix/budget/NameIndexRecheckWorkBudgetTest.java); [NameKeySerializerRoutableBoundTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/NameKeySerializerRoutableBoundTest.java) **Planned:** HOTRoutableKeyBoundConformance: a differential index-versus-scan case per index kind over key families whose discriminative bytes sit below, at and above each layout's addressable end (255 for an extraction byte, 4095 for a leading bit position), built both by the bulk builder and incrementally, at several revisions and after cold reopen; a key at the bound indexes and reads exactly, a key past it reads exactly through its re-check, and no persisted node names a byte it does not route on.
- **Evidence class:** code+test (the committed differential tests fail on the baseline without the level split and pass with it; the budget's zero has a nonzero long-name control)
- **Gap/note:** The bound, level split and exact NAME re-check are implemented. JSON routing/bound cases, XML namespace truncation across build, mutation and cold history, and record/allocation budgets are committed; the cross-kind sweep over every layout boundary remains planned. The maintainer decided the numeric bound and truncate-and-re-check behavior on 24 November 2026. Root cause and regression history belong to the [NAME routing-range write-up](../docs/postmortems/2026-11-hot-name-key-routing-range.md).

A HOT indirect node MUST be able to name, in the layout it persists, every discriminative key byte
it routes on. Discriminative positions are absolute and MSB-first (FMT-302). The `SINGLE_MASK`
layout persists one base byte position and an 8-byte window mask, so it can name key bytes 0 to
65535 and spans at most 8 bytes; the `MULTI_MASK` layout persists one extraction position per
discriminative key byte in a single byte, so it can name only key bytes 0 to 255 and MUST NOT be
chosen for any node that routes on a byte above 255. A builder or splitter that would assemble such
a node MUST narrow the node's discriminative-bit set and leave the remaining bits to deeper levels,
which FMT-302 permits because lookup and scan results do not depend on where the frontier is cut.
Reducing a byte position to its layout's width MUST NOT happen: a reduced position routes the node
on an unrelated key byte, which is a wrong lookup result and not a slower one.

Both layouts persist the node's leading discriminative bit position, which FMT-302's strictly
increasing per-level progression and the range-scan walk-up both read. That value MUST be persisted
without loss over the whole addressable range, or be recomputed on decode from the layout it
belongs to; a value that overflowed its field MUST NOT be accepted as a leading position.

The routable key bound MUST be 4096 bytes, measured on the stored key including its four-byte chunk
index and any posting-delta suffix. That is where the signed 16-bit leading-bit field runs out — bit
32767, key byte 4095 — so the bound costs no change to the persisted layout, and every leading
position a 1.0 engine can write stays inside the field it is written to. Every index kind MUST bound
its keys to it and MUST state that bound rather than inherit it by accident; CAS and VALIDTIME
already truncate their encoded value to one.

For the NAME index the bounded object is the serialized key — namespace sentinel, escaped URI,
terminator and local name — and the bound MUST hold for programmatically supplied names, not only
for names a document parser is likely to produce. A name whose key would exceed the bound MUST NOT
fail an insert. It MUST be stored under a character-aligned prefix of its components with an unambiguous
truncation marker inside the same bound. A truncated key MUST NOT equal any complete key, including
a complete name ending at or immediately below the bound. The marker is a trailing `0xFF` byte,
which cannot end a complete UTF-8 local name; decoding the preceding bytes yields the shortened
expanded name. Namespace framing and the chunk and posting-delta suffixes retain their meanings.
Namespace truncation MUST reserve only the actual UTF-8 width of the first local-name character.
Such a key MAY therefore stand for several names,
so its postings are candidates: a read that answers for a name MUST re-check each candidate against
the name the addressed node actually carries, resolved from the name dictionary rather than from a
cached name on a page-backed view, and MUST return only the postings that match. The local-name key,
URI key and node kind MUST be copied while the candidate record's page is held, before dictionary
access can reconstruct other records and release that hold. A filtered read whose entry key may be
truncated MUST decide inclusion per node for the same reason.

Both public point lookups, `HOTIndexReader.get` and `HOTIndexWriter.get`, MUST apply that same
marker-driven exact re-check after candidate chunk assembly. Stored-key cursors (`iterator`, both
`range` overloads, `iteratorFrom`, `iteratorTo`, and the underlying trie cursors) deliberately expose
serialized buckets and their raw-key metadata, not exact expanded-name ranges. A NAME consumer that
selects names from those cursors MUST apply the marker-driven residual; `NameIndex.openIndex` does so
for includes/excludes. An unfiltered full scan includes every candidate and needs no name residual.
Internal assembly and caches MUST retain candidates so one name cannot remove another's postings
from a shared bucket.

That re-check MUST cost nothing for a name inside the bound: such a name is stored whole, its key
answers exactly, and the read MUST NOT read a record to confirm it. Only a name past the bound pays,
and then one record read per candidate posting. Re-check matching MUST compare the resolved URI
and local-name components without allocating a name object per candidate; include/exclude lookup
preparation belongs to the read, not to each posting. Serializing a complete name MUST NOT add a
character-counting pass to its existing encoding work. Truncation is determined from the bytes
produced by encoding, or from the character length already established by the ASCII fast path;
an exact read MUST reuse that serialization decision rather than re-count the requested name.

Within the bound an index read at revision `r` MUST equal a scan at `r` for keys of any length,
whether the index was bulk-built or maintained incrementally, before and after commit and after cold
reopen. No layout choice, frontier cut or key length inside the bound MAY change which postings a
lookup returns.

### FMT-306 — HOT stored height and spine continuity

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [HOTIndirectPage.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTIndirectPage.java):`setHeightForWrite`; [HOTIndirectPage.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/page/HOTIndirectPage.java):`getHeight`; [AbstractHOTIndexWriter.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/AbstractHOTIndexWriter.java):`refreshOuterHeight`; [AbstractHOTIndexWriter.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/AbstractHOTIndexWriter.java):`propagateStructuralSpliceUpSpine`; [HOTCommitNormalizer.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTCommitNormalizer.java):`normalize`; [SiblingMerger.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/SiblingMerger.java):`mergeSiblings`
- **Verified by:** [HOTSubInsertSpineTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTSubInsertSpineTest.java):`sequentialLeavesFoldIntoOneBlockAtCommit`, `localInsertionRetainsNonzeroOuterChildSlots`, `skewedDescendingMaintainsExactAncestorHeights`, `structuralInsertionRearmsAnUnchangedHeightCommittedSpine`; [HOTOuterHeightMaintenanceTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTOuterHeightMaintenanceTest.java):`propagatesOnlyTheChangedMaximum`; [HOTConstructedShapeIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTConstructedShapeIntegrationTest.java):`consolidationAfterValueUpdatesRearmsTheOuterSpine`; [HOTStoredHeightSpineTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTStoredHeightSpineTest.java); [HOTLookupShapeWorkBudgetTest.java](../bundles/sirix-core/src/test/java/io/sirix/budget/HOTLookupShapeWorkBudgetTest.java); `HOTStoredHeightSpineTest.aWrongStoredHeightMisroutesNothingAndIsStillRejected` asserts that the invariant validator rejects an injected off-by-one stored height. **Planned:** HOTStoredHeightConformance: an independent structural walk asserts exact stored heights and ancestor continuity after every mutation shape that replaces a subtree below a committed ancestor (branch insert, subtree-local split, fold, consolidation, sibling merge, node-type upgrade, posting removal, revert), for growing and shrinking subtrees, on every versioning strategy, live in the writing transaction and after cold reopen; the walk rejects an ancestor still addressing a replaced subtree.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** The height is per-node wire state inside the page image, so this is a format decision rather than an algorithm choice. Writer propagation and commit-time normalization exist and the listed regressions assert exact heights live and cold. The shared test-side invariant validator compares every walked indirect node's stored height with the structure below it when its children can all be walked, so every test that validates a trie enforces exact stored heights. No decoder check exists: a store whose bytes carry an inexact height is accepted. The broader planned conformance case remains outstanding. The height bounds inside those regressions are workload bounds, not this requirement.

Each indirect HOT node carries its own height: the number of indirect levels on the longest path
from that node down to a leaf page below it, counting itself, so a node whose children are all leaf
pages has height 1. For the index root this equals the logical height `h` of FMT-302.

A reachable or persisted indirect node's stored height MUST equal that structural height. This MUST
hold at every point the engine can traverse the node: after each completed index mutation inside the
writing transaction, in every published revision, and after cold decode. Heights MUST be maintained
exactly in both directions, so a subtree that shrinks MUST NOT leave a larger stored height behind,
and an inexact height MUST NOT be left for a later maintenance pass to discover.

A structural change below a node MUST be carried back along the entire parent chain up to the index
root. After the mutation completes, no reachable ancestor may still describe the subtree that the
change replaced: each ancestor on that chain MUST address the published replacement, its routing
metadata MUST describe that replacement's keys, and its stored height MUST be recomputed exactly —
including when the change leaves that ancestor's own height unchanged, because an unchanged maximum
does not imply an unchanged subtree. Publishing a replacement into a locally built spine while its
outer ancestors keep their pre-change references MUST NOT occur, and a mutation that cannot complete
the propagation MUST fail the transaction rather than publish the partial chain.

Locating a key MUST NOT depend on a stored height: a point lookup, a range scan and a writer descent
MUST reach the same leaf for a key whatever the stored heights are, so an inexact height MUST NOT be
able to misroute a key. Mutation and maintenance MAY read stored heights for structural decisions —
merging siblings, upgrading a node type, folding a compatible child, scheduling normalization — which
is why an inexact stored height MUST be treated as a malformed subtree, on the same footing as
FMT-301, rather than as a cost matter. The invariant validator MUST compare each walked node's stored
height with the structure below it and MUST reject any difference.

Optional maintenance that reduces height, such as consolidation or commit-time normalization, MAY
decline under FMT-304 clause 3, and a decline MUST leave every clause above satisfied. The height
such maintenance leaves behind is otherwise unconstrained here: FMT-302 states the only height
guarantee, and in particular a committed trie MAY exceed the smallest height admissible for its own
leaf partitioning.

### FMT-401 — 1.0 format baseline

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [ResourceFormatCompatibility.java](../bundles/sirix-core/src/main/java/io/sirix/io/ResourceFormatCompatibility.java); [Superblock.java](../bundles/sirix-core/src/main/java/io/sirix/io/Superblock.java):`validate`; [BinaryEncodingVersion.java](../bundles/sirix-core/src/main/java/io/sirix/BinaryEncodingVersion.java):`fromByte`; [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`deserialize`; [StoreFeatures.java](../bundles/sirix-core/src/main/java/io/sirix/page/StoreFeatures.java):`requireWritable`
- **Verified by:** [FormatVersionBaselineTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/FormatVersionBaselineTest.java); [FormatRepairBoundaryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/FormatRepairBoundaryTest.java); [StorageTypeTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/StorageTypeTest.java); [InterruptedFirstCommitProofTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/InterruptedFirstCommitProofTest.java); [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java); [GoldenCompositePageTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenCompositePageTest.java) **Planned:** FormatBaselineConformance: mandatory later-reader compatibility with the frozen 1.0 corpus.
- **Evidence class:** code+test (targeted ordinary-open and repair/recovery regressions on both persistent backends; full discovery and CI evidence remain incomplete)
- **Gap/note:** Shared read-only discovery validates both file identities, the uber page, the latest committed-root envelope and live root envelopes within the bounded tail-log window, including checksum-valid historical records. Before empty-bootstrap truncation, with or without revision-zero beacons, it checks the surviving framed prefix, including any readable version in an incomplete frame; malformed or incomplete first-commit tails remain recoverable when no foreign version is readable. The unreadable-settings fallback checks any readable top-level `binaryEncoding` by name, independently of settings field order. Resource-bound proofs carry these checks through storage initialization, orphan-spill cleanup, derived rebuilding and bootstrap recovery; direct reader healing and writer truncation validate the exact descriptors they will modify before writing. An empty revisions file may use the validated data-file identity and compatible tail-log roots; a present revisions header must validate against that identity. Explicit truncate checks its target envelope and the bounded root window ending at the prospective frontier before publishing either downgraded beacon or changing the session revision. Zero-UUID/zero-stride acceptance is removed. Discovery of foreign envelopes outside the latest root and the bounded salvage subset remains incomplete: a complete envelope scan before destructive repair (including a foreign revision-1 envelope below a compatible latest root and outside the bounded window) belongs to task `sirix-repair-format-gate`, which cannot close. The framed-prefix check does not discover envelopes beyond a malformed frame or an unwritten gap. The immutable 1.0 corpus of FMT-208 is also absent, so the later-reader clause has no evidence.

Bounded discovery MUST classify directory records and salvaged tail-log records through `IOStorage.decodeRevisionRecord`. Only a checksum-valid record carrying both prune markers is a tombstone; a positive root offset with a hash equal to the prune tag remains live and its envelope MUST be checked, including at the latest revision.

Compatibility reads MUST make progress: an EOF or zero-byte read while bytes remain to be checked reports incomplete input rather than retrying or treating unread bytes as compatible. This applies to file headers, frame lengths and envelope payloads. Bootstrap recovery still follows the incomplete-frame table below.

Resource-bound proofs accept filesystem aliases of the verified resource, while refusing a different resource. Storage-provider dispatch MUST enforce this ownership and readable-feature contract before invoking an external provider. A failed resource-session completion MUST release newly acquired ownership, preserve existing ownership, and propagate the refusal with any cleanup failure suppressed on it; retrying the open MUST repeat the refusal. Repair acceptance covered here: the mapped tests combine unreadable derived settings, orphan spills, commit markers, torn revision records and missing bootstrap pointers with foreign versions discoverable through the checks above, on both persistent backends, and assert whole-resource byte preservation. With feature-free revision-zero beacons or no beacons, missing directory records and tail-log copies plus an incomplete framed tail follow this shared prefix-boundary rule:

| Envelope header bytes carrying the version | Version value | Outcome for an incomplete first-commit frame |
|---|---|---|
| Present and readable | Not 1.0's | REFUSE, files unchanged |
| Present and readable | 1.0's | RECOVERABLE |
| Not present, or unreadable | n/a | RECOVERABLE |

The available envelope header MUST be inspected even when the advertised body is incomplete; frame completeness alone MUST NOT decide recoverability. Recoverable means the compatibility gate permits the existing empty-bootstrap proof to authorize recovery, not that an incomplete committed root is valid. No scan proceeds beyond the malformed frame. Envelopes outside that frame remain the deferred FMT-401 gap owned by task `sirix-repair-format-gate` (captain decision A, 2026-11-22). Discovery uses read-only descriptors without healing records; destructive recovery requires a resource-bound proof. This evidence does not establish discovery of every surviving envelope before destructive repair; FMT-401 remains partial.

When ordinary settings deserialization fails, every top-level `binaryEncoding` member of any JSON type counts as present. Only the exact string `"V0"` is compatible. Numbers, objects, arrays, booleans, null and every other string MUST cause an unsupported-format-version refusal before a compatibility proof can authorize rebuilding, deletion or another mutation. The fallback MUST apply the same version-value validation as ordinary parsing, regardless of field order; a missing or unreadable value provides no compatible version by itself.

The 1.0 format MUST be the only on-disk format this engine reads or writes. A store's format version is its superblock layout version (FMT-102), its page-envelope binary encoding version (FMT-201) and its persisted `binaryEncoding` settings member (CONF-102). A store carrying any other format version MUST be rejected before any mutation, with a plain error naming the unsupported format version found. The engine MUST NOT detect, name or special-case any pre-1.0 development format, MUST NOT reinterpret bytes written by another format version, and MUST NOT provide a compatibility, upgrade or in-place conversion path for one. Moving data between stores is export and import under FMT-402. Every later release MUST read the immutable 1.0 corpus under FMT-208. Existing IDs, reserved fields and required features MUST NOT be silently reinterpreted.

### FMT-402 — History-preserving export and import

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [JsonSerializer.java](../bundles/sirix-core/src/main/java/io/sirix/service/json/serialize/JsonSerializer.java); [XmlSerializer.java](../bundles/sirix-core/src/main/java/io/sirix/service/xml/serialize/XmlSerializer.java); [JsonShredder.java](../bundles/sirix-core/src/main/java/io/sirix/service/json/shredder/JsonShredder.java); [XmlShredder.java](../bundles/sirix-core/src/main/java/io/sirix/service/xml/shredder/XmlShredder.java); [DumpResourceHistory.kt](../bundles/sirix-kotlin-cli/src/main/kotlin/io/sirix/cli/commands/DumpResourceHistory.kt); [RevisionInfo.java](../bundles/sirix-core/src/main/java/io/sirix/api/RevisionInfo.java); [RevisionRootPage.java](../bundles/sirix-core/src/main/java/io/sirix/page/RevisionRootPage.java):`setCommitTimestamp`, `ensureCommitTimestampMicros`
- **Verified by:** [HistoryPreservingExportImportTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/HistoryPreservingExportImportTest.java):`everyRevisionRoundTripsWithContentAuthorMessageAndTimestamp`, `exportLeavesTheSourceStoreUnchanged` (known content, authors, messages, revision order, already-increasing timestamps and unchanged source files); [CommitTimestampPrecisionConformanceTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/CommitTimestampPrecisionConformanceTest.java):`twoCommitsInsideOneMillisecondKeepDistinctOrderedTimestamps`, `suppliedInstantRoundTripsAcrossColdReopen` (microsecond persistence), `aTieAndABackwardsClockStepAreAdjustedToOneMicrosecondAfterThePredecessor` (commit adjustment, not a history-import round trip); [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java). **Planned:** HistoryExportImportConformance: JSON/XML source histories with equal, regressing and already-increasing timestamps, cascaded adjustments, authors and escaped/empty messages; independent sequential adjustment oracle, original metadata queries for adjusted revisions, unchanged timestamps where possible (including a first timestamp at or before the destination bootstrap), preserved revision order/content, unique time lookup after cold reopen, unavailable/pruned history diagnostics and unchanged source bytes.
- **Evidence class:** code+test (source inspection of round-trip, microsecond persistence and commit-adjustment tests; complete import conformance and default-lane trace evidence remain incomplete)
- **Gap/note:** Timestamp persistence follows VER-201's epoch-microsecond contract, and TXN-310's tie/regression adjustment is implemented and covered by the mapped commit conformance test. `setCommitTimestamp` no longer refuses a value below the preceding revision's: `ensureCommitTimestampMicros` adjusts it instead. The history round-trip fixture preserves already-increasing timestamps later than the destination bootstrap and does not exercise adjustment. Separate queryable original-timestamp metadata remains absent from RevisionInfo/RevisionRootPage. Preserving a first imported timestamp at or before the destination bootstrap also remains unimplemented: custom-timestamp resources bootstrap at `Instant.EPOCH`, and subsequent commits adjust against that predecessor. Serializers, shredders and history dumping remain building blocks rather than one export/import tool; full conditional-adjustment import conformance with queryable originals remains unverified.

Export MUST write every revision of a 1.0 resource with its committed content, commit timestamps, authors and messages. Import MUST load them into a newly created 1.0 store in source revision order without dropping revisions. The first imported timestamp MUST retain its source value. For each later revision, if its source timestamp is greater than the preceding imported effective timestamp, import MUST preserve it unchanged; if it would tie or go backwards, import MUST set its effective commit timestamp to the preceding imported timestamp plus 1 microsecond. For every adjusted revision, its original timestamp MUST be preserved as queryable revision metadata. Only necessary timestamp adjustments are permitted; content, authors, messages and revision order MUST be preserved. Open-by-time MUST use the effective strictly increasing timestamps, not the original metadata values (VER-204). The source resource MUST remain unchanged. Unavailable history MUST be reported rather than claiming a complete export or import.

## Wire tables (FMT-102, FMT-104, FMT-106, FMT-209)

| Superblock offset | Width | Meaning |
|---|---|---|
| 0 | 8 | ASCII `SIRIXDB!` |
| 8 | 4 | layout version 0 |
| 12 | 1 | role 0 data / 1 revisions |
| 13 | 3 | zero spare bytes |
| 16 | 4 | endian check 0x01020304 |
| 20 | 4 | data slot size 4096 / revision stride 32 |
| 24 | 8 | PRIMARY offset 4096 for data / 0 for revisions |
| 32 | 8 | content start 12288 for data / 4096 for revisions |
| 40 | 16 | resource UUID, most-significant then least-significant 64-bit words |
| 56 | 8 | XXH3-64 of bytes [0,56) |

The last-revert revision (FMT-209) is one page-extension entry on the revision root page.
Its tag is kind-local and optional: bit 0x40 set, required bit 0x80 clear.

| Field | Width | Meaning |
|---|---|---|
| tag | 1 | 0x41, kind-local optional revision-root entry |
| length | stop-bit | 4 |
| payload[0..4) | 4 | last-revert revision, little-endian u32 |

| Feature word | Bit | Name | Meaning |
|---|---|---|---|
| readOnlyCompat | 0 (0x00000001) | conditional-write metadata | the store carries the FMT-209 last-revert entry; an engine that does not maintain it may read but MUST NOT write |

The revision ring occupies the last 768 bytes of each beacon slot. Its entry layout is
revision u32, reserved-zero u32, root offset i64, timestamp i64, revision-record checksum i64,
root hash i64, then tail-entry checksum i64. IOStorage's `tailLogEntryChecksum` and numeric constants
are the current encoder evidence; the ring is bounded to 16 recent records.

## Deferred release implementation

- FMT-102/401/402: the single-format baseline, the removal of the zero UUID/stride exceptions and history-preserving export/import are decided. The removal and the scoped refusal checks above are implemented here; complete envelope discovery before destructive repair remains task `sirix-repair-format-gate`. Remaining export/import implementation and evidence gaps are owned by [FMT-402](#fmt-402--history-preserving-export-and-import).
- FMT-302: the numeric bound and indirect-level height convention are decided. Enforcing the key-derived bound and rejecting undefined discriminative positions remain implementation/test gaps, not open product choices.
- FMT-305: the cross-kind/layout-boundary conformance sweep remains planned; see the [requirement and current evidence](#fmt-305--hot-routable-key-bound). Widening the bound later would need the leading-bit field read as unsigned (8192 bytes) or a per-node base for `MULTI_MASK` extraction positions (65536 bytes), so it is a format decision rather than a later addition.
- FMT-306: exact stored heights and outer-spine continuity are decided and implemented in the writer and in commit-time normalization. The shared invariant validator checks exact stored heights. Refusing an inexact height on decode and the broader planned conformance case remain implementation/test gaps, not open product choices.
- FMT-208: produce and check in the immutable 1.0-created feature/index/config corpus and CI coverage manifest; the compatibility policy is decided. The corpus MUST include a resource carrying the FMT-209 last-revert entry and one without it, and a resource whose incarnation id was regenerated by a truncate.
- FMT-209: the extension tag, the feature bit, the carry-forward in the revision-root clone, the settings member and cryptographically secure id generation at the four lifecycle boundaries are unimplemented. The tag and bit assignments above are the first entries in the otherwise empty FMT-203 tag and FMT-205 feature registries and must be reviewed before the format freeze; TXN-311 cannot be retrofitted compatibly afterwards.
- Node hash defaults are owned by [CONF-104](CONF-configuration.md#conf-104--current-creation-defaults).

## Traceability

| Requirement | Existing or planned verification | Status |
|---|---|---|
| FMT-101 | [SuperblockResourceUuidTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockResourceUuidTest.java); [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java) **Planned:** FormatFileSetConformance: inspect a JSON and XML resource after creation, commit, cold reopen and backup restore. | partial |
| FMT-102 | See [FMT-102 evidence and remaining gaps](#fmt-102--superblock-bytes). | See requirement |
| FMT-103 | [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java); [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`uberPageBytesArePinned` **Planned:** BeaconSelectionConformance: valid older PRIMARY plus newer SECONDARY, torn each slot, both invalid, and first-commit cases on both persistent backends. | designed |
| FMT-104 | [RevisionRootPageHashValidationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRootPageHashValidationTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [FormatRepairBoundaryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/FormatRepairBoundaryTest.java) **Planned:** RevisionRecordGoldenTest: independent exact byte vector including tombstone, one-microsecond distinctions, corruption of each identity word and recovery round trips. | designed |
| FMT-105 | [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [FormatRepairBoundaryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/FormatRepairBoundaryTest.java) | partial |
| FMT-106 | [LazyRevisionRecordRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/LazyRevisionRecordRecoveryTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java); [FormatRepairBoundaryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/FormatRepairBoundaryTest.java) | partial |
| FMT-107 | [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`uberPageBytesArePinned`; [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java); [LazyRevisionRecordRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/LazyRevisionRecordRecoveryTest.java) **Planned:** BeaconFrameConformance: independent length/payload/hash byte vectors and boundary lengths adjacent to the tail-log region. | partial |
| FMT-108 | [PreallocatedCommitTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/PreallocatedCommitTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java); [PowerLossSimulationTest.java](../bundles/sirix-core/src/test/java/io/sirix/crash/PowerLossSimulationTest.java) | designed |
| FMT-201 | [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`pageKindIdsArePinned`; [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`nodeKindIdsArePinned`; [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`indexTypeIdsArePinned`; [BinaryEncodingVersionStrictTest.java](../bundles/sirix-core/src/test/java/io/sirix/BinaryEncodingVersionStrictTest.java); [StrictReferenceFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StrictReferenceFormatTest.java) | partial |
| FMT-202 | [RevisionRootPageHashValidationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRootPageHashValidationTest.java); [GoldenCompositePageTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenCompositePageTest.java) **Planned:** FrameCorruptionConformance: negative/oversized/truncated lengths, bit flips, compressed and uncompressed images; compare FILE_CHANNEL and MEMORY_MAPPED failures. | partial |
| FMT-203 | [PageExtensionsWriterTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/PageExtensionsWriterTest.java); [PageKindTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/PageKindTest.java):`optionalExtensionsPreserveSaltAndRecordsWhileRequiredEntriesRefuse` **Planned:** ExtensionReadConformance: all truncation positions, repeated/decreasing tags, oversized/overflowing lengths, required versus optional tags and rewritten bytes. | partial |
| FMT-204 | [StoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StoreFeaturesTest.java):`everyUnknownBitFollowsTheAccessMatrix`; [InterruptedFirstCommitRecoveryStoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/InterruptedFirstCommitRecoveryStoreFeaturesTest.java) | partial |
| FMT-205 | [StoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StoreFeaturesTest.java) | partial |
| FMT-206 | [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`deterministicRecordPageSaltIsPinned`; [RecordPageSaltSourceTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/RecordPageSaltSourceTest.java); [RecordPageSaltAllocationTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/RecordPageSaltAllocationTest.java) | partial |
| FMT-207 | [StrictReferenceFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StrictReferenceFormatTest.java) **Planned:** StrictReaderFuzzTest: fixed-seed byte mutations, all prefix truncations and differential heap/segment decoders; no unbounded allocation from counts. | partial |
| FMT-208 | [GoldenCompositePageTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenCompositePageTest.java); [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java) **Planned:** FrozenStoreCorpusConformance: immutable hashes plus a feature/index/config coverage manifest, read every store with independent content/history/index expectations in default CI; deliberately incompatible reader fixture fails without changing corpus bytes. | designed |
| FMT-209 | No existing test mapped. **Planned:** ConditionalWriteMetadataFormatConformance: absent-entry byte identity, publication on revert, carry-forward, survival across reopen/backup/strategies/backends, regenerated ids on restore before exposure, id published before truncate removes data, controlled fresh draws across creation, database recreation, replacement, truncate and repeated restore, and stale-token refusal for coincident revision numbers, read-only refusal for an engine without the bit, and malformed-entry refusal. | proposed |
| FMT-210 | HOTLeafDirectoryEncodingTest and HOTLeafBodyEncodingTest. **Planned:** MixedLeafFormatConformance across backends, revisions, extensions and rejection cases. | partial |
| FMT-211 | HOTLeafDirectoryEncodingTest and HOTLeafBodyEncodingTest. **Planned:** LeafSlotFrameConformance for u16-only directories, complete-dump reconstruction, strict trailers, stop-bit bounds and exact consumption. | partial |
| FMT-212 | HOTLeafBodyEncodingTest. **Planned:** SlotLengthWidthConformance for mixed widths, byte boundaries and malformed slots. | partial |
| FMT-213 | HOTLeafBodyEncodingTest. **Planned:** GroupPrefixConformance for group boundaries, logical key order and malformed offsets. | partial |
| FMT-214 | HOTPostingFrameTest and HOTLeafBodyEncodingTest. **Planned:** HOTPostingConformance for tag and width rejection, intersections and reconstruction, plus HOTPostingReadAllocationTest. | partial |
| FMT-215 | HOTLeafBodyEncodingTest. **Planned:** SameFrameAliasConformance across read paths, eviction and fragment reconstruction. | partial |
| FMT-216 | ProjectionOverflowEncodingTest. **Planned:** ProjectionOverflowFormatConformance for every codec, bounded decoding, allocation guards and corrupt frames. | partial |
| FMT-217 | HOTLeafBodyEncodingTest, ProjectionOverflowEncodingTest and ProjectionSortedFullImagePackingTest. **Planned:** FrameSelectionConformance and paired latency, allocation and size gates. | partial |
| FMT-301 | [HOTInvariantValidator.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidator.java); [HOTInvariantValidatorChecksTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidatorChecksTest.java); [HOTHeightOptimalTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTHeightOptimalTest.java) **Planned:** HOTFanoutConformance: inject one-child nodes at every production constructor/decoder/publish boundary; delete down to one child, cold reopen, then scan and point lookup. | designed |
| FMT-302 | [HOTInvariantValidatorChecksTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidatorChecksTest.java); [HOTHeightOptimalTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTHeightOptimalTest.java); [HOTStructuralPropertyTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTStructuralPropertyTest.java) **Planned:** HOTHeightBoundConformance: independent encoded-key discriminative-position oracle; empty/leaf-only/one-indirect conventions; ascending, descending, shuffled, long common-prefix and variable-length/prefix keys; valid boundaries and malformed repeated/decreasing/undefined/out-of-range leading positions; assert h <= D and D <= 8*L after mutation and cold reopen, across node types; compare lookup/scan with an ordered-map oracle. | designed |
| FMT-303 | [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`indexTypeIdsArePinned` | partial |
| FMT-304 | See [HOT mutation leaf lifetime](#fmt-304--hot-mutation-leaf-lifetime) for verification and remaining gaps. | partial |
| FMT-305 | [Current evidence and planned conformance](#fmt-305--hot-routable-key-bound) | partial |
| FMT-306 | [HOTSubInsertSpineTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTSubInsertSpineTest.java); [HOTOuterHeightMaintenanceTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTOuterHeightMaintenanceTest.java); [HOTConstructedShapeIntegrationTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTConstructedShapeIntegrationTest.java); [HOTStoredHeightSpineTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTStoredHeightSpineTest.java); [HOTLookupShapeWorkBudgetTest.java](../bundles/sirix-core/src/test/java/io/sirix/budget/HOTLookupShapeWorkBudgetTest.java); injected off-by-one height rejected by the invariant validator in `aWrongStoredHeightMisroutesNothingAndIsStillRejected`. **Planned:** HOTStoredHeightConformance: exact stored heights and ancestor continuity after every subtree-replacing mutation shape, growing and shrinking, on every versioning strategy, live and after cold reopen. | partial |
| FMT-401 | [FormatVersionBaselineTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/FormatVersionBaselineTest.java); [FormatRepairBoundaryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/FormatRepairBoundaryTest.java); [StorageTypeTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/StorageTypeTest.java); [InterruptedFirstCommitProofTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/InterruptedFirstCommitProofTest.java); [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java); [GoldenCompositePageTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenCompositePageTest.java) **Planned:** FormatBaselineConformance: mandatory later-reader compatibility with the frozen 1.0 corpus. | partial |
| FMT-402 | Evidence and planned conformance: [FMT-402](#fmt-402--history-preserving-export-and-import). | partial |
