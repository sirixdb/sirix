# CONF — Persisted configuration

Spec target: **1.0.0**. Chapter status: **Draft for adoption before the approximately
9 December 2026 freeze**. Code baseline: `13f65148b117b9c8e1858a7f1510d2f65501bad9` (17 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance.
Existing test links are evidence inventories, not assertions that tests passed in this task.

## Scope

P0 database/resource settings, format identity, defaults, validation and accepted opt-in policies. General tuning knobs are outside this persisted-settings freeze slice.

## Requirements

### CONF-101 — Persisted settings own format identity

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`serialize`; [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`deserialize`; [DatabaseConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseConfiguration.java):`deserializeOnce`
- **Verified by:** [ResourceConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/ResourceConfigurationTest.java); [DatabaseConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/conf/DatabaseConfigurationTest.java); [SuperblockResourceUuidTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockResourceUuidTest.java); [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test inspection only; round-trip conformance must run for release.

Resource creation MUST persist the selected configuration in ressetting.obj. Reopen MUST use those persisted format-affecting choices rather than current process defaults. Rewriting a configuration MUST preserve its known values, resource ID and UUID; copying a database MUST bind I/O to the actual directory opened, not the stored informational file path.

### CONF-102 — Resource settings schema

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`JSONNAMES`; [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`expectField`; [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`serialize`; [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`deserialize`
- **Verified by:** [ResourceSettingsExtensionsTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/ResourceSettingsExtensionsTest.java)
- **Evidence class:** code+test (source inspection)
- **Gap/note:** extensions is an optional metadata escape hatch and is not retained by this reader. It cannot carry required decoding semantics; use FMT feature negotiation. Partial valid-time pairs currently become disabled; stricter valid-time schema policy is deferred with the broader P1 valid-time contract.

The resource-settings writer MUST emit the names, nesting and types in the table below, including the historical spelling `numbersOfRevisiontoRestore`. Required fields MUST be read in the established prefix order; supported tail fields may be omitted with their documented defaults. Unknown fields MUST fail with their name except an object-valued `extensions` field, whose contents are skipped. Unknown nested revisioning/valid-time fields MUST fail.

### CONF-103 — Database settings

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [DatabaseConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseConfiguration.java):`serialize`; [DatabaseConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseConfiguration.java):`deserializeOnce`
- **Verified by:** [DatabaseConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/conf/DatabaseConfigurationTest.java); [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java) **Planned:** DatabaseSettingsConformance: atomic replacement under concurrent readers, moved/copied directory with old source present, cache mutation isolation and invalid JSON.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Database parser uses positional/assert checks in some places; resource-settings strict unknown-field guarantees are not claimed for this separate parser.

dbsetting.obj MUST carry file (informational path), ID (maximum resource ID), databaseId (database identity when assigned), databaseType and maxSegmentAllocationSize. Serialization MUST use a temporary file and atomic replacement; reads MUST NOT be redirected to a stale serialized location after move/restore. Cache reuse MUST NOT share mutable configuration instances among callers.

### CONF-104 — Current creation defaults

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`Builder`; [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`selectDefaultByteHandler`
- **Verified by:** [ResourceConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/ResourceConfigurationTest.java); [ResourceSettingsExtensionsTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/ResourceSettingsExtensionsTest.java) **Planned:** ConfigurationDefaultsConformance: clean subprocess per process property, create/serialize/reopen with changed properties; compare all settings.
- **Evidence class:** code+test (source inspection); decided target has unverified implementation gaps
- **Gap/note:** Current hash default is sirix.hashType with fallback ROLLING. Default-off hashing is the 1.0 design target; production change and clean-process default tests are still required. Node history remains on, and page checksums/salts are independent.

For 1.0, resource creation MUST default node hashing to NONE, as targeted by the accepted full-spec design. Other creation defaults MUST follow the table below. Existing resources MUST retain their persisted values when process defaults change. The current-source column is evidence of the implementation gap, not permission to claim that NONE has already landed.

### CONF-105 — Input bounds

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`maxNumberOfRevisionsToRestore`; [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`deweyIdSiblingDistance`; [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`canonicalResourceName`
- **Verified by:** [ResourceConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/ResourceConfigurationTest.java) **Planned:** ResourceConfigurationBoundaryConformance: every invalid bound, absolute/parent/multipart names, null selectors and unchanged existing resource on rejection.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test inspection only; round-trip conformance must run for release.

A restore window of zero/negative and a DeweyID sibling distance that is nonpositive or odd MUST fail with IllegalArgumentException. Null enum/pipeline/serializer choices MUST be rejected. Resource names MUST be canonical single relative names and MUST NOT escape or alias their database resource directory.

### CONF-106 — Keep the three projection/body choices

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [ProjectionSlotLayout.java](../bundles/sirix-core/src/main/java/io/sirix/index/projection/ProjectionSlotLayout.java); [ProjectionIndexMetadata.java](../bundles/sirix-core/src/main/java/io/sirix/index/projection/ProjectionIndexMetadata.java):`slotLayout`; [SegmentDictionaryLane.java](../bundles/sirix-core/src/main/java/io/sirix/index/projection/SegmentDictionaryLane.java):`enabled`; [SegmentDictionaryLane.java](../bundles/sirix-core/src/main/java/io/sirix/index/projection/SegmentDictionaryLane.java); [ChunkedBodyConfig.java](../bundles/sirix-core/src/main/java/io/sirix/page/ChunkedBodyConfig.java):`enabled`
- **Verified by:** [ProjectionSlotLayoutTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/projection/ProjectionSlotLayoutTest.java); [SegmentDictionaryLaneTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/projection/SegmentDictionaryLaneTest.java); [SegmentDictionaryLaneEndToEndTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/projection/SegmentDictionaryLaneEndToEndTest.java) **Planned:** ProjectionDefaultConformance: clean process with unset properties, persisted layouts read independently of writer toggles, invalid segment/chunk combination rejected.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Captain call 7: keep all three as today. This preserves current support and defaults; it does not remove existing column-major reads/builds or promise missing incremental paths.

The existing row-group-major projection default MUST remain unchanged; COLUMN_MAJOR remains its existing opt-in layout and is not newly promised as first-class incremental maintenance by this decision. Segment dictionaries MUST remain opt-in via sirix.projection.segmentDict=false by default. Chunked record bodies MUST remain opt-in via sirix.chunkedBody.enable=false by default. Enabling segment dictionaries without chunked bodies MUST fail rather than emit unreadable pages.

### CONF-107 — Runtime tuning cannot hide persisted requirements

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [ChunkedBodyConfig.java](../bundles/sirix-core/src/main/java/io/sirix/page/ChunkedBodyConfig.java):`enabled`; [PageKind.java](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java):`readVersionAndFlagsAllowing`; [StoreFeatures.java](../bundles/sirix-core/src/main/java/io/sirix/page/StoreFeatures.java):`requireReadable`
- **Verified by:** [ChunkedBodyWireFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/chunked/ChunkedBodyWireFormatTest.java); [StoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StoreFeaturesTest.java) **Planned:** WriterToggleReadConformance: create under each supported opt-in, reopen with it disabled, compare logical historical content.
- **Evidence class:** code+test (source inspection)
- **Gap/note:** Source/test inspection only; round-trip conformance must run for release.

Writer-policy toggles such as chunked-body enablement MUST affect new writes only; readers MUST select decoders from persisted image metadata. Disabling a writer feature in the process MUST NOT make an existing supported image unreadable. Unsupported persisted features MUST follow FMT-204.

### CONF-108 — Backend selection does not silently fall back

- **Level:** MUST
- **Status:** partial
- **Since:** 1.0
- **Code:** [StorageType.java](../bundles/sirix-core/src/main/java/io/sirix/io/StorageType.java):`getStorage`; [StorageType.java](../bundles/sirix-core/src/main/java/io/sirix/io/StorageType.java):`getInstance`
- **Verified by:** No existing test mapped. **Planned:** StorageProviderConformance: missing provider, wrong provider name, removed FILE, and correct built-in reopen with identical logical content.
- **Evidence class:** code-only (implementation inspected)
- **Gap/note:** External provider implementations are outside this repository baseline. Their enum names are present but are not evidence of a shipped 1.0 backend implementation.

StorageType.FILE MUST fail explicitly as removed. IO_URING and S3 MUST require their named external provider; if absent, opening MUST fail with an actionable SirixIOException rather than silently opening another backend. The built-in FMT/TXN persistence claims cover FILE_CHANNEL and MEMORY_MAPPED; provider-specific durability needs its own conformance evidence.

### CONF-201 — Dispatcher queue and close defaults

- **Level:** MUST
- **Status:** designed
- **Since:** 1.0
- **Code:** [AbstractNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeTrxImpl.java):`close`; [ResourceConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java):`Builder`
- **Verified by:** No existing test mapped. **Planned:** DispatcherConfigurationConformance: unset defaults 10,000 and 5 seconds, supported custom timeout/capacity boundaries, invalid/overflow values, tiny-queue replay and stalled-listener close using controlled time.
- **Evidence class:** code-only (implementation inspected); decided target has unverified implementation gaps
- **Gap/note:** Defaults and policies are decided. Property/API names and exact accepted ranges remain to be implemented; the current writer-scheduler timeout is not this setting.

The per-database post-commit queue MUST be bounded with a default capacity of 10,000 notifications and replay overflow semantics from TXN-805. Dispatcher close MUST default to a 5-second finite wait and MUST be configurable. The configuration API MUST document units, accepted bounds and explicit rejection of invalid values; a configured timeout MUST NOT allow unbounded listener waiting. These values MUST NOT authorize freeing memory still used by a writer worker or callback.

## Persisted resource schema (CONF-102)

Prefix fields appear in this order. Names are exact, including legacy spelling.

| JSON member | Type / value domain |
|---|---|
| binaryEncoding | enum name, currently V0 |
| revisioning | object: revisioningClass (VersioningType name), numbersOfRevisiontoRestore (positive integer) |
| byteHandlerClasses | ordered array of handler-class keyed objects; ByteHandlerKind codec |
| storageKind | StorageType enum name |
| hashKind | HashType enum name |
| hashFunction | writer emits XX3; reader also accepts legacy case-insensitive strings containing xx3 |
| compression | boolean (text compression) |
| pathSummary | boolean |
| resourceID | integer |
| deweyIDsStored | boolean |
| persistenter | RecordSerializer implementation class name |
| storeDiffs | boolean |
| customCommitTimestamps | boolean |
| storeNodeHistory | boolean |
| storeChildCount | boolean |

| Tail member | Type | Omitted-field read default |
|---|---|---|
| stringCompressionType | enum name | NONE |
| deweyIdSiblingDistance | even positive integer | 16 |
| verifyChecksumsOnRead | boolean | true |
| hashAlgorithm | enum name | XXH3 |
| validTimeConfig | object with string validFromPath and validToPath | absent/disabled |
| pathStatistics | boolean | false |
| repairBulkInsertHashes | boolean | false |
| resourceUuid | nonzero UUID string | required for 1.0; current absent/null beta fallback is rejected by FMT-102/401 |
| regionCompression | enum name | LZ77 |
| aggregateKernel | boolean | false |
| saltRecordPages | boolean | true |
| extensions | object, arbitrary optional nested values | absent; skipped, not retained |

## Creation defaults (CONF-104, CONF-106)

| Setting | Current default |
|---|---|
| storage / versioning / restore window | FILE_CHANNEL / SLIDING_SNAPSHOT / 3 |
| binary version / record serializer | V0 / NodeSerializerImpl |
| node hashing | current sirix.hashType fallback ROLLING → required 1.0 default NONE; hash function XX3 |
| byte pipeline | sirix.compression, fallback none (empty pipeline); lz4 is selectable |
| text compression / string compression | false / NONE |
| region compression / read checksum algorithm | LZ77 / XXH3 |
| verifyChecksumsOnRead / saltRecordPages | true / true |
| path summary / path statistics | true / false |
| DeweyIDs / sibling distance | false / 16 |
| storeDiffs / storeNodeHistory / storeChildCount | true / true / true |
| custom commit timestamps / aggregate kernel / repair bulk hashes | false / false / false |
| resourceUuid | newly generated UUID at creation |
| segment dictionary / chunked body writer toggles | false / false |

## Deferred work

Default-off hashing, complete valid-time pair validation, database settings parser strictness
and dispatcher configuration keys/ranges remain implementation/verification work. Serializer
and byte-handler internals remain outside the stable core under JAPI-109. Runtime tuning property enumeration beyond these format-affecting choices
is a later CONF extension, not an implicitly frozen API.

## Traceability

| Requirement | Existing or planned verification | Status |
|---|---|---|
| CONF-101 | [ResourceConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/ResourceConfigurationTest.java); [DatabaseConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/conf/DatabaseConfigurationTest.java); [SuperblockResourceUuidTest.java](../bundles/sirix-core/src/test/java/io/sirix/io/SuperblockResourceUuidTest.java); [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java) | partial |
| CONF-102 | [ResourceSettingsExtensionsTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/ResourceSettingsExtensionsTest.java) | partial |
| CONF-103 | [DatabaseConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/conf/DatabaseConfigurationTest.java); [BackupRestoreTest.java](../bundles/sirix-core/src/test/java/io/sirix/backup/BackupRestoreTest.java) **Planned:** DatabaseSettingsConformance: atomic replacement under concurrent readers, moved/copied directory with old source present, cache mutation isolation and invalid JSON. | partial |
| CONF-104 | [ResourceConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/ResourceConfigurationTest.java); [ResourceSettingsExtensionsTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/ResourceSettingsExtensionsTest.java) **Planned:** ConfigurationDefaultsConformance: clean subprocess per process property, create/serialize/reopen with changed properties; compare all settings. | designed |
| CONF-105 | [ResourceConfigurationTest.java](../bundles/sirix-core/src/test/java/io/sirix/access/ResourceConfigurationTest.java) **Planned:** ResourceConfigurationBoundaryConformance: every invalid bound, absolute/parent/multipart names, null selectors and unchanged existing resource on rejection. | partial |
| CONF-106 | [ProjectionSlotLayoutTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/projection/ProjectionSlotLayoutTest.java); [SegmentDictionaryLaneTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/projection/SegmentDictionaryLaneTest.java); [SegmentDictionaryLaneEndToEndTest.java](../bundles/sirix-core/src/test/java/io/sirix/index/projection/SegmentDictionaryLaneEndToEndTest.java) **Planned:** ProjectionDefaultConformance: clean process with unset properties, persisted layouts read independently of writer toggles, invalid segment/chunk combination rejected. | partial |
| CONF-107 | [ChunkedBodyWireFormatTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/chunked/ChunkedBodyWireFormatTest.java); [StoreFeaturesTest.java](../bundles/sirix-core/src/test/java/io/sirix/page/StoreFeaturesTest.java) **Planned:** WriterToggleReadConformance: create under each supported opt-in, reopen with it disabled, compare logical historical content. | partial |
| CONF-108 | No existing test mapped. **Planned:** StorageProviderConformance: missing provider, wrong provider name, removed FILE, and correct built-in reopen with identical logical content. | partial |
| CONF-201 | No existing test mapped. **Planned:** DispatcherConfigurationConformance: unset defaults 10,000 and 5 seconds, supported custom timeout/capacity boundaries, invalid/overflow values, tiny-queue replay and stalled-listener close using controlled time. | designed |
