# FMT — Disk format and compatibility

Spec target: **1.0.0**. Chapter status: **Draft for adoption before the approximately
9 December 2026 freeze**. Code baseline: `13f65148b117b9c8e1858a7f1510d2f65501bad9` (17 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance.
Existing test links are evidence inventories, not assertions that tests passed in this task.

## Scope

Persistent file identity, geometry, page framing, strict readers, feature negotiation, salts, golden pins and format-relevant HOT decisions. Commit ordering belongs to TXN; persisted JSON settings belong to CONF. Full index algorithms remain P1.

## Requirements

### FMT-101 — Resource file set

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`ResourcePaths`; [DatabaseConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseConfiguration.java):`DatabasePaths`; [Superblock.java](../bundles/sirix-core/src/main/java/io/sirix/io/Superblock.java):`validate`
- **Verified by:** [SuperblockResourceUuidTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockResourceUuidTest.java); [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java) **Planned:** FormatFileSetConformance: inspect a JSON and XML resource after creation, commit, cold reopen and backup restore.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Mapped source/tests inspected; runtime conformance was not executed for this documentation change.

A built-in persistent FILE_CHANNEL or MEMORY_MAPPED resource MUST keep its page store in `data/sirix.data`, revision directory in `data/sirix.revisions`, resource settings in `ressetting.obj`, index catalogues in `indexes/`, encryption-key material when configured in `encryption/`, update-diff sidecars in `update-operations/`, and recovery marker at `log/.commit`. The database settings file is `dbsetting.obj`. Settings and page files MUST identify the same resource; a zero UUID or UUID mismatch MUST fail open. IN_MEMORY has no durability guarantee and follows TXN-607.

### FMT-102 — Superblock bytes

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [Superblock.java](../bundles/sirix-core/src/main/java/io/sirix/io/Superblock.java):`build`; [Superblock.java](../bundles/sirix-core/src/main/java/io/sirix/io/Superblock.java):`validate`
- **Verified by:** [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`superblockBytesArePinned`; [SuperblockSpareBytesTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockSpareBytesTest.java); [SuperblockResourceUuidTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockResourceUuidTest.java) **Planned:** BetaStoreRejectionConformance: real beta stores, valid-checksum zero UUID/stride fixtures and wrong-role/offset corruption; clear migration error and byte-for-byte unchanged input.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current Superblock.validate accepts zero revision stride and skips UUID cross-check if either UUID is zero. The 1.0/beta format discriminator and exact frozen byte vectors need implementation; existing layout version 0 is source evidence, not permission to misidentify a beta store as 1.0. Offset-field validation at 24/32 is also incomplete.

Both persistent files MUST begin with the 64-byte little-endian superblock in the table below. Writers MUST zero spare bytes. Readers MUST reject short/all-zero headers, bad magic, unknown layout version, wrong role/endian marker, nonzero spare bytes, wrong slot size and checksum mismatch. The 1.0 reader MUST reject pre-1.0 beta stores with a clear "created by pre-1.0 beta, export and re-import" error before mutation. Legacy development zero-UUID and zero-revisions-stride exceptions MUST be removed; resource UUIDs MUST be nonzero and match settings, and revision stride MUST be 32.

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
- **Verified by:** [RevisionRootPageHashValidationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRootPageHashValidationTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java) **Planned:** RevisionRecordGoldenTest: independent exact byte vector including tombstone, one-microsecond distinctions, corruption of each identity word and recovery round trips.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current timestamp words use epoch milliseconds. The strict microsecond timestamp decision requires a precision-preserving 1.0 wire encoding and the decided export/import migration implementation before freeze; a new numeric unit/version is not chosen by this draft. Slot geometry and checksum evidence describe the current layout.

Revision r MUST occupy the 32-byte slot at `4096 + 32*r`, including bootstrap r=0. Each slot MUST encode four little-endian 64-bit words: root-page offset, timestamp, checksum, root-image hash or prune tag. Timestamp encoding MUST preserve VER-201 precision; the existing millisecond unit is a legacy implementation detail, not the decided 1.0 precision contract. The checksum MUST cover the 24 bytes formed by offset, timestamp and hash/tag in that order. The revision number is the slot position, not a field. Zero root-image hash MUST NOT disable validation.

### FMT-105 — Pruned records are explicit

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`decodeRevisionRecord`; [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`writePrunedRevisionRecord`; [FileChannelReader.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelReader.java):`readRevisionRootPage`
- **Verified by:** [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** The numeric prune-tag constant is pinned by the source-backed wire registry below. No new retention policy or physical erasure guarantee is introduced.

A pruned revision MUST retain its slot and timestamp with offset `Long.MIN_VALUE` and the exact `PRUNED_REVISION_TAG` (0x5052554E45440001) from IOStorage. Checksum verification MUST precede tombstone recognition. A checksum-valid record with any other invalid address/hash combination MUST be treated as corruption, not absence or pruning. Reads of a valid tombstone MUST raise `SirixRevisionPrunedException`.

### FMT-106 — Revision-record tail log

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [IOStorage.java](../bundles/sirix-core/src/main/java/io/sirix/io/IOStorage.java):`REVISION_RECORD_TAIL_LOG_ENTRY_BYTES`; [FileChannelReader.java](../bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelReader.java):`readRevisionRootPage`
- **Verified by:** [LazyRevisionRecordRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/LazyRevisionRecordRecoveryTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Field-width table is below; this is recovery of directory records, not arbitrary page self-healing.

Each beacon slot MUST reserve a 16-entry ring of 48-byte revision-record recovery entries at its tail. An entry MUST contain revision, root offset, timestamp, record checksum, root hash and XXH3-64 checksum of its 40-byte prefix, using the field widths in IOStorage. Repair MUST stay at or below the selected durable frontier and MUST NOT resurrect a checksum-valid tombstone or replace a checksum-valid conflicting identity.

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
- **Gap/note:** Feature-bit assignment registry remains empty. Future features need a named bit and compatibility test before emission.

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
- **Gap/note:** Neither field, the extension tag nor the feature bit exists at this baseline; the extension decoder states that no shared or kind-local tags are assigned yet, and the FMT-205 feature registry is empty. `RevisionRootPage`'s clone constructor is the carry-forward seam, and it currently copies only the node-key frontiers, timestamp, message and user. Assigning the bit and the tag before the format freeze is required because TXN-311 cannot be made safe by a later additive change: a writer that does not maintain these fields silently destroys the guarantee rather than failing. The split placement below is a Firstmate best-practice choice: the incarnation id changes only at resource creation, truncate, whole-resource replacement and backup restore, so paying revision-root bytes for it on every commit would be waste.

A resource MUST persist two pieces of conditional-write metadata (TXN-311): its incarnation id, identifying the resource incarnation and timeline the revision numbers belong to, and its last-revert revision, the revision number of the most recent commit that published a revert, 0 when none.

The last-revert revision MUST be published atomically with the commit that changes it and MUST be carried forward unchanged by every later revision root, so reading it at the head costs no additional page read. It MUST be encoded as the optional kind-local revision-root page-extension entry defined in the wire table below, under FMT-203 framing, and MUST be omitted entirely by a resource that has never reverted, keeping its revision-root images unchanged; absence MUST read as 0. A store that carries the entry MUST set the named `readOnlyCompat` feature bit in the wire table, so an engine that does not know the field may still read the store but MUST NOT write it: FMT-203 requires unknown optional entries to be skipped and discarded on rewrite, which would silently drop the evidence TXN-311 depends on. A maintenance tool that rewrites revision roots MUST preserve the value or refuse (FMT-204).

The incarnation id MUST be an independent uniform unsigned 64-bit value drawn from a cryptographically secure RNG and persisted in the resource settings as `incarnationId` (CONF-102), rather than per revision. A fresh draw MUST occur at resource creation (including recreation after resource or database deletion), truncate (TXN-404), whole-resource replacement (REST-210), and backup restore (TXN-613/614). Restore MUST regenerate the id for each restored resource before exposing it, while preserving copied history, last-revert metadata and derived state. Truncate MUST durably publish the new id before removing data (TXN-404). Ordinary commits, reopen and settings rewrites MUST preserve the id. No database-level counter or global allocator state is required. For any particular old token, a new draw reproduces its incarnation id with probability 2^-64; recreating or restoring a resource can therefore reproduce that old token only with this probability bound, rather than a guarantee of absolute uniqueness. Every 1.0 resource MUST contain the member; absence MUST NOT default to a reusable sentinel identity.

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

### FMT-401 — 1.0 compatibility baseline and beta refusal

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [Superblock.java](../bundles/sirix-core/src/main/java/io/sirix/io/Superblock.java):`validate`; [BinaryEncodingVersion.java](../bundles/sirix-core/src/main/java/io/sirix/BinaryEncodingVersion.java):`fromByte`; [StoreFeatures.java](../bundles/sirix-core/src/main/java/io/sirix/page/StoreFeatures.java):`requireWritable`
- **Verified by:** [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java); [GoldenCompositePageTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenCompositePageTest.java) **Planned:** FormatBaselineConformance: pre-1.0 beta refusal and unchanged source bytes, successful exported-history import to a newly created 1.0 store, and mandatory later-reader compatibility with the frozen corpus.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Legacy zero UUID/stride acceptance remains in source; there is no complete 1.0 corpus/migration workflow at this baseline. The chosen baseline/refusal/export-import policy is decided.

Version 1.0 MUST be the on-disk compatibility baseline. The 1.0 engine MUST NOT directly read pre-1.0 beta stores; it MUST reject them before mutation with a clear "created by pre-1.0 beta, export and re-import" error. Beta migration MUST use history-preserving export/import (FMT-402), not in-place reinterpretation of old bytes. Every later release MUST read the immutable 1.0 corpus under FMT-208. Existing IDs, reserved fields and required features MUST NOT be silently reinterpreted.

### FMT-402 — History-preserving beta export and import

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [JsonSerializer.java](../bundles/sirix-core/src/main/java/io/sirix/service/json/serialize/JsonSerializer.java); [XmlSerializer.java](../bundles/sirix-core/src/main/java/io/sirix/service/xml/serialize/XmlSerializer.java); [JsonShredder.java](../bundles/sirix-core/src/main/java/io/sirix/service/json/shredder/JsonShredder.java); [XmlShredder.java](../bundles/sirix-core/src/main/java/io/sirix/service/xml/shredder/XmlShredder.java); [DumpResourceHistory.kt](../bundles/sirix-kotlin-cli/src/main/kotlin/io/sirix/cli/commands/DumpResourceHistory.kt); [RevisionInfo.java](../bundles/sirix-core/src/main/java/io/sirix/api/RevisionInfo.java)
- **Verified by:** [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java) **Planned:** HistoryMigrationConformance: JSON/XML source histories with equal, regressing and already-increasing timestamps, cascaded adjustments, authors and escaped/empty messages; independent sequential adjustment oracle, original metadata queries for adjusted revisions, unchanged timestamps where possible, preserved revision order/content, unique time lookup after cold reopen, unavailable/pruned history diagnostics and unchanged source bytes.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Captain decision 010 resolves the timestamp-preservation interaction: adjust only ties/regressions and preserve their originals as queryable metadata. Serializers/shredders/history dumping are building blocks, not a complete migration tool; RevisionInfo/RevisionRootPage lack the required original-import-timestamp metadata API/storage at this baseline.

Beta migration MUST export all resource revisions with their committed content, commit timestamps, authors and messages and import them into a newly created 1.0 store in source revision order without dropping revisions. The first imported timestamp MUST retain its source value. For each later revision, if its source timestamp is greater than the preceding imported effective timestamp, import MUST preserve it unchanged; if it would tie or go backwards, import MUST set its effective commit timestamp to the preceding imported timestamp plus 1 microsecond. For every adjusted revision, its original timestamp MUST be preserved as queryable revision metadata. Only necessary timestamp adjustments are permitted; content, authors, messages and revision order MUST be preserved. Open-by-time MUST use the effective strictly increasing timestamps, not the original metadata values (VER-204). An exporter capable of reading the beta version MUST produce the interchange; the 1.0 engine MUST consume it rather than open the beta store directly. The source MUST remain unchanged. Unavailable history MUST be reported rather than claiming a complete conversion.

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

- FMT-102/401/402: beta refusal, removal of zero UUID/stride exceptions and history-preserving export/import are decided. Implement the 1.0 discriminator and migration tooling, including the decided conditional timestamp adjustment and queryable original metadata in TXN-310/VER-204.
- FMT-302: the numeric bound and indirect-level height convention are decided. Enforcing the key-derived bound and rejecting undefined discriminative positions remain implementation/test gaps, not open product choices.
- FMT-208: produce and check in the immutable 1.0-created feature/index/config corpus and CI coverage manifest; the compatibility policy is decided. The corpus MUST include a resource carrying the FMT-209 last-revert entry and one without it, and a resource whose incarnation id was regenerated by a truncate.
- FMT-209: the extension tag, the feature bit, the carry-forward in the revision-root clone, the settings member and cryptographically secure id generation at the four lifecycle boundaries are unimplemented. The tag and bit assignments above are the first entries in the otherwise empty FMT-203 tag and FMT-205 feature registries and must be reviewed before the format freeze; TXN-311 cannot be retrofitted compatibly afterwards.
- Node hash default: the accepted 1.0 design targets NONE; current ResourceConfiguration still falls back to ROLLING. CONF-104 records this implementation gap explicitly.

## Traceability

| Requirement | Existing or planned verification | Status |
|---|---|---|
| FMT-101 | [SuperblockResourceUuidTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockResourceUuidTest.java); [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java) **Planned:** FormatFileSetConformance: inspect a JSON and XML resource after creation, commit, cold reopen and backup restore. | partial |
| FMT-102 | [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`superblockBytesArePinned`; [SuperblockSpareBytesTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockSpareBytesTest.java); [SuperblockResourceUuidTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockResourceUuidTest.java) **Planned:** BetaStoreRejectionConformance: real beta stores, valid-checksum zero UUID/stride fixtures and wrong-role/offset corruption; clear migration error and byte-for-byte unchanged input. | designed |
| FMT-103 | [CrashRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/CrashRecoveryTest.java); [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`uberPageBytesArePinned` **Planned:** BeaconSelectionConformance: valid older PRIMARY plus newer SECONDARY, torn each slot, both invalid, and first-commit cases on both persistent backends. | designed |
| FMT-104 | [RevisionRootPageHashValidationTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRootPageHashValidationTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java) **Planned:** RevisionRecordGoldenTest: independent exact byte vector including tombstone, one-microsecond distinctions, corruption of each identity word and recovery round trips. | designed |
| FMT-105 | [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java) | partial |
| FMT-106 | [LazyRevisionRecordRecoveryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/LazyRevisionRecordRecoveryTest.java); [RevisionRecordDurabilityFrontierTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/RevisionRecordDurabilityFrontierTest.java); [SparseRevisionHistoryTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SparseRevisionHistoryTest.java) | partial |
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
| FMT-301 | [HOTInvariantValidator.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidator.java); [HOTInvariantValidatorChecksTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidatorChecksTest.java); [HOTHeightOptimalTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTHeightOptimalTest.java) **Planned:** HOTFanoutConformance: inject one-child nodes at every production constructor/decoder/publish boundary; delete down to one child, cold reopen, then scan and point lookup. | designed |
| FMT-302 | [HOTInvariantValidatorChecksTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidatorChecksTest.java); [HOTHeightOptimalTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTHeightOptimalTest.java); [HOTStructuralPropertyTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTStructuralPropertyTest.java) **Planned:** HOTHeightBoundConformance: independent encoded-key discriminative-position oracle; empty/leaf-only/one-indirect conventions; ascending, descending, shuffled, long common-prefix and variable-length/prefix keys; valid boundaries and malformed repeated/decreasing/undefined/out-of-range leading positions; assert h <= D and D <= 8*L after mutation and cold reopen, across node types; compare lookup/scan with an ordered-map oracle. | designed |
| FMT-303 | [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java):`indexTypeIdsArePinned` | partial |
| FMT-401 | [GoldenFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenFormatTest.java); [GoldenCompositePageTest.java](../bundles/sirix-core/src/test/java/io/sirix/format/GoldenCompositePageTest.java) **Planned:** FormatBaselineConformance: pre-1.0 beta refusal and unchanged source bytes, successful exported-history import to a newly created 1.0 store, and mandatory later-reader compatibility with the frozen corpus. | designed |
| FMT-402 | [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java) **Planned:** HistoryMigrationConformance: JSON/XML source histories with equal, regressing and already-increasing timestamps, cascaded adjustments, authors and escaped/empty messages; independent sequential adjustment oracle, original metadata queries for adjusted revisions, unchanged timestamps where possible, preserved revision order/content, unique time lookup after cold reopen, unavailable/pruned history diagnostics and unchanged source bytes. | designed |
