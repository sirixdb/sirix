# SirixDB On-Disk Format (V0)

Status: **V0 record contract.** We are at
`BinaryEncodingVersion.V0` (pages) and superblock `LAYOUT_VERSION = 0` (files). Every
pre-freeze checklist item has been resolved (implemented or explicitly decided — see §5's
decisions log), and the byte-level contract is pinned by golden tests
(`io.sirix.format.GoldenFormatTest`, `io.sirix.format.GoldenCompositePageTest`): an accidental
change to any pinned structure fails CI. From here, ANY change to the bytes in this document is
a conscious format bump — `BinaryEncodingVersion` (page bodies/records), the page-envelope
flags byte (additive page features), `LAYOUT_VERSION` (file layout), or a sub-structure version
byte (PIXM/PIXD/PIXS/PIX1, per-record versions) — accompanied by an update to the golden constants
and a migration note here. Nothing has been bumped under that rule yet: SirixDB has no released
consumers, so structures still evolve IN PLACE and the golden pins alone make each change
deliberate (see §2, "PathStats trailer").

## 1. Files

A resource directory contains:

| File | Purpose |
|---|---|
| `data/sirix.data` | Append-only page store + dual uber-page beacons |
| `data/sirix.revisions` | Fixed-slot revision index: 32-byte records (`IOStorage.revisionsFileOffset`) |
| resource settings JSON | Format identity: `binaryVersion`, byte-pipeline classes, `storageType`, `hashAlgorithm`, `verifyChecksumsOnRead` |

Both binary files open with the 64-byte superblock (magic, layout version, endianness check,
file role, geometry, resource UUID, XXH3 checksum — see below); the *rest* of the format
identity — compression pipeline, hash-function id, binary encoding version — lives in the JSON,
whose parse is strictly validated (unexpected/misordered fields throw) and which is
cross-linked to the binary files by the **resource UUID**: the JSON persists it, both
superblocks embed it, and opening a data file with a different resource's settings (wrong-backup
restore) fails fast. A zero UUID on either side (legacy dev files) skips the cross-check.

### sirix.data (FILE_CHANNEL, the default)

```
0      : SUPERBLOCK (64 B, see io.sirix.io.Superblock) — magic "SIRIXDB!", layout version (0),
         file role, endianness check, geometry, XXH3 checksum
64     : reserved (sparse zeros) up to 4096
4096   : PRIMARY uber beacon slot  [u32 len][UberPage payload][u64 XXH3 of payload][zero pad]
         ... last 768 B of the slot: REVISION-RECORD TAIL LOG (see below)
8192   : SECONDARY uber beacon slot (identical copy — a SEPARATE filesystem block, so
         block-granular torn writes can no longer kill both)
12288  : DATA_REGION_START — page records, append-only: [u32 len][payload], 8-byte aligned
```

Page keys (`PageReference.getKey()`) are absolute byte offsets of the record's length prefix.
The header region stays a sparse hole until the first commit writes it (`IOStorage.exists()`
checks size > 0 to distinguish fresh resources). Beacon recovery validates
`[len][payload][XXH3]` — both length bounds and checksum — and falls back primary → secondary;
storage open validates the superblock (magic/version/endianness/role/CRC) and fails fast with an
actionable message on any mismatch.

**Commit protocol** (`FileChannelWriter.writeUberPageReference`):
flush buffered tail → write superblocks if missing → **stage the revision record into both beacon
slot images' tail log** → `force(false)` data (write-ahead barrier; it now hardens the tail log
too) → write SECONDARY beacon slot → write PRIMARY beacon slot — both through an O_DSYNC channel
(in-place overwrites; each write durable at return, which gives the secondary-before-primary
ordering AND makes the primary's write-return the commit acknowledge; FUA on capable NVMe stacks).
ONE explicit fsync per commit (the data tail write-ahead barrier); no commit-end barrier exists.
Validated empirically by `CrashRecoveryInjectionTest` (SIGKILL loop, opt-in
`-Dsirix.crash.run=true`).

**Revision-record tail log** (`-Dsirix.commit.lazyRevisionRecord`, default on, requires
`-Dsirix.commit.preallocated`). Previously the protocol carried a THIRD device round-trip: the
revisions channel was opened O_SYNC and the commit additionally issued `force(true)` on it, so the
new revision's 32-byte slot record was durable before any beacon advertised it. That force is
gone. The revisions channel is opened BUFFERED and the record instead rides a 16-entry ring in the
trailing 768 bytes of BOTH beacon slot images:

```
slot + 4096-768 + 48*(revision mod 16) :
    [u32 revision][u32 reserved=0][32-byte revision record][u64 XXH3 of the 40-byte prefix]
```

Because the ring lives inside the beacon image, a committed revision's record is durable exactly
when its beacon is — the ordering guarantee the old force provided, at zero extra round-trips
(3 → 2 per commit). The invariants that make this safe:

- **Eviction guard.** Before a revision's entry would overwrite the ring slot of an older
  revision, the writer forces the revisions file and advances a per-resource durability watermark
  (`RevisionRecordDurability`), so no record ever leaves the ring without being durable in the
  revisions file first. Losing the entire ring therefore costs no committed revision.
- **Salvage on read, not second opinion.** `FileChannelReader.getRevisionFileData` consults the
  ring ONLY when the file record is missing or fails its checksum; an intact record is never
  second-guessed. A salvaged record is written back into the revisions file (self-heal), so the
  next open needs no salvage. A record that is neither intact in the file nor recoverable from an
  entry that passes its own XXH3 is a hard error naming the exhausted salvage source, never a
  silently-served garbage offset.
- **Writer handoff.** Writers are per-transaction, so the ring image and the write frontiers are
  handed writer-to-writer through `RevisionRecordDurability` rather than re-derived from disk on
  every commit; truncation and rollback drop the whole entry, so a stale snapshot can never
  survive a timeline change.

Covered by `LazyRevisionRecordRecoveryTest` (staging, salvage + heal, out-of-window failure,
post-eviction durability, torn-entry rejection).

**Crash-window contract**: a commit whose primary beacon write was lost opens at the previous
revision silently — correct, because that commit was never acknowledged to the client. A torn
PRIMARY with an intact NEWER secondary opens at the durable-but-unacknowledged newer revision
(the secondary was fsynced before the primary was written). Recovery truncation REPAIRS both
beacon slots to the truncated-to revision (`FileChannelWriter.repairBeaconSlotsAfterTruncate`):
without this, a lost-primary crash left the secondary advertising the truncated-away revision
until the next commit, and a primary corruption inside that window made fallback dereference
the stale-forward secondary — resource unopenable despite intact data (regression-tested by
`CrashRecoveryTest.staleForwardSecondaryBeacon_isRepairedOnRecovery`). An EXPLICIT rollback
(`StorageEngineWriter.truncateTo` / `NodeTrx.truncateTo` to an older revision) instead truncates
away the revision BOTH slots advertise; `NodeStorageEngineWriter.truncateTo` therefore rewrites
both beacons with the reconstructed rolled-back uber page (its serialized form is only the
revision count) through the regular dual-beacon protocol — dying between an explicit rollback
and the next commit is no longer an unopenable state (regression-tested by
`NodeStorageEngineWriterTruncateToRevisionIntegrationTest.test_truncate_survives_process_death_before_next_commit`).

### sirix.revisions

```
0      : SUPERBLOCK (64 B, role = revisions; slot-size field = 32, the record stride —
         persisted geometry, validated at open against this build's record size)
64     : reserved (sparse zeros) up to 4096
4096 + 32*revision : [u64 dataFileOffsetOfRevisionRootPage][u64 epochMillis]
                     [u64 recordChecksum][u64 revisionRootPageHash]      (little-endian)
```

This file is **load-bearing**: the serialized UberPage holds only `revisionCount`, so every
RevisionRootPage lookup goes through a slot here — which is why every record now carries a
checksum (verified on read; mismatch is a hard error, not a garbage offset, unless a checksum-valid
copy of the record is still in the beacon tail log, in which case it is salvaged and the slot is
healed in place). The write-only uber-page copies an earlier draft of the layout kept at offsets
0/512 are gone.

The 4th field (formerly `reserved`, zero) now stores the **XXH3-64 of the RevisionRootPage's
compressed on-disk payload** — the same hash the writer puts on every other page's parent
PageReference. It closes the one gap where a page was reached without a parent reference: the
`readRevisionRootPage` path now verifies the body against this hash before deserializing (gated on
`verifyChecksumsOnRead`). The **record checksum covers 24 bytes** (offset + timestamp + hash) when
the hash field is present, so a torn write or bit-rot in the hash itself is also caught.
**Backward-compat rule:** a record whose hash field is `0` is a *legacy* (beta1-and-earlier)
record — its checksum covers only the first **16 bytes** and its RevisionRootPage body is not
hash-verified (there is nothing to check against). The hash field thus doubles as the
format-version discriminator, so older resources open under this build with no false-positive
corruption error. (An all-zero real hash is remapped to a non-zero sentinel before storage so `0`
unambiguously means "legacy".)

### Endianness

The format is **fully little-endian pinned** (checklist item 1 — done). Every multi-byte scalar
that reaches disk goes through an explicitly LE-ordered accessor:

- Superblock, beacon XXH3 trailers, revision records, beacon/page-record length prefixes:
  pinned LE (`ByteOrder.LITTLE_ENDIAN` buffers / `MMFileReader.LAYOUT_INT`).
- `BytesOut`/`BytesIn` payload primitives, the KVLP header+bitmap block, PAX regions,
  flyweight field access, and the FFILz4 frame header: pinned LE via `io.sirix.node.LE`
  layouts (`ByteArrayBytesIn` already decoded LE byte-shifts).
- Deliberate big-endian defined byte sequences: `compactDir` uses explicit byte composition on
  stream reads and a byte-swapped LE-short access on `MemorySegment`; in-blob column length
  prefixes use explicit shifts. HOT discriminative keys use BE loads for lexicographic
  `compareUnsigned`. These are defined wire byte sequences, not host-order dependent.
- The superblock's endianness check remains as a fail-fast guard for headers written by
  pre-pin dev builds on BE hosts (none exist in practice).
- The legacy big-endian FILE backend is removed; `StorageType.FILE` fails fast.

## 2. Pages

Every page serializes as `[pageKind u8][binaryVersion u8][flags u8][body]`
(`PageKind.writeVersionAndFlags` / `readVersionAndFlags`). The flags byte is extension space
**per page kind, not a shared namespace** — the same bit number means a different thing depending on
the kind byte that precedes it, so a bit only has meaning once the kind is known. A kind that
defines no flag writes zero and rejects anything else; a kind that defines flags reads through
`readVersionAndFlagsAllowing(source, allowedMask)`, which rejects any bit outside **that kind's own**
mask as "written by a newer version" instead of misparsing.

The masks declared today — derive this list from the `readVersionAndFlagsAllowing` call sites rather
than trusting it to stay complete, since a kind may add a flag without touching this document:

| kind | bit | constant | meaning |
|---|---|---|---|
| KVLP (1) | `0x01` | `ChunkedBodyConfig.FLAG_CHUNKED_BODY` | body is chunk-framed (one META frame plus heap chunks split at entry boundaries, each independently compressed and checksummed) instead of one monolithic codec frame; writer off by default, both bodies readable |
| KVLP (1) | `0x02` | `FLAG_OVERFLOW_SLOT_SIDECAR` | the page carries a cold overflow-slot sidecar. Set from the data, not a switch: written whenever `getSideSlotCount() != 0` |
| OVERFLOW (9) | `0x01` | `FLAG_OVERFLOW_PAYLOAD_COMPRESSED` | the compressed body described below — **the default** since the payload-compression flip |
| HOT_LEAF (12) | `0x01` | `HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS` | the page carries the segment/blob side map described below |

Every other kind writes zero and refuses any nonzero bit. **Compatibility is one-directional:** a
build that predates a flag bit reads a resource written without it, but a resource written WITH it
cannot be read by that build — the reader refuses the unknown bit loudly rather than misparsing.
Kind ids: 1 KVLP, 2 NAME,
3 UBER, 4 INDIRECT, 5 REVISION_ROOT, 6 PATH_SUMMARY, 8 CAS, 9 OVERFLOW, 10 PATH, 11 DEWEYID,
12 HOT_LEAF, 13 HOT_INDIRECT, 15 VECTOR, 16 PROJECTION, 17 VALID_TIME
(7 and 14 retired/reserved; readers reject both ids).

**Format evolution mechanism (decided):** the unit of node-record/page-body evolution is a
`BinaryEncodingVersion` bump — every node/page (de)serializer receives the
`ResourceConfiguration`, which carries the resource's persisted `binaryEncoding`, and the
per-page version byte fails fast on unknown values. Node kinds that need to evolve
independently of the global version carry a per-record version byte (the VECTOR_NODE /
VECTOR_INDEX_METADATA pattern); sub-structures with their own magic carry their own version
byte (PIXD/PIXS/PIX1, PIXM). Enum-typed bytes on disk are always explicit stable ids with
fail-fast lookups, never ordinals.

- **UberPage** body: `[i32 revisionCount]` — 7 bytes total incl. envelope, no checksum (§5.3).
- **RevisionRootPage**: delegate refs + revision, maxNodeKeys, commit timestamp/message, user.
- **CASPage / PathPage / ProjectionIndexPage / ValidTimeIndexPage**: reference-container delegate
  followed by one sparse HOT allocator map `[i32 count][count × (i32 indexId, i64 maxHotPageKey)]`.
  Entries are emitted in ascending `indexId` order. These secondary-index containers have one
  representation only: their references root HOT trees; no keyed-trie node-key or indirect-level
  counters are persisted. This unreleased layout deliberately replaces the earlier three-map V0
  draft in place; there are no persisted databases requiring a compatibility branch.
- **KeyValueLeafPage** (the data page): 1024 implicit-keyed slots
  (`nodeKey = recordPageKey << 10 | slot`), 160-byte header+slot-bitmap, then a
  compact directory with one big-endian `u16` per populated slot: 10 high bits encode the
  inline length (0..1023 — the field's full reach, which is also `MAX_RECORD_SIZE`; zero is legal
  only for the raw-slot kind), and 6 low bits encode the
  persisted slotted kind (`0` raw sentinel or a supported flyweight kind). Unsupported/retired
  kind ids are corruption, and no length is representable above the cap. The directory is followed by a
  smallest-of-three body codec (`ZeroRunByteCodec` 0 / `ByteRunCodec` 2 / `SirixLZ77Codec` 3 —
  an LZ4-block-format clone, little-endian) over either the offset-table-template dedup layout
  (≤255 templates/page, 1-byte slot ids, hash/value/nameKey elision bitmaps, predictor-coded
  parentKey column, pathNodeKey dictionary) or the inline fallback; then PAX regions
  (`RegionTable`, one slot per stable kind id: 0 Number = frame-of-reference + bit-packing +
  per-tag zone maps, 1 String, 2 Struct, 3 DeweyID, 4 ObjectKeyNameKey, 5 Boolean, 6 Hash,
  7 StructPointers, 8 StringDictSketch = Bloom filter over the string dictionary (omitted on a
  page whose string region suppressed a tag, since the filter's negative is exact and page-wide),
  1 String's completeness is PER TAG: a tag holding a value too large to stay inline leaves the
  region entirely and is named in the header's suppressed-tag list, which is present only when
  `parentDictSize` carries its sign bit,
  9 NumberZoneMap = the number column's per-tag min/max hoisted out so a range predicate can
  prune the page without decompressing it, 10 RecordOrdinal = the slot → record linkage a
  predicate spanning two fields needs, 11 Double = the double-typed column the long-only number
  region cannot hold, each tag encoded PLAIN, ALP, ALP-RD or exact-decimal — see
  `page/pax/DoubleRegion` for that region's wire format and what each encoding means for a
  comparison), overflow pointers, optional FSST symbol table.
- **String-region tag forms (two optional lanes, both OFF by default).** A tag's dictionary is
  normally text plus a length lane whose width is a two-bit code (1/2/4 bytes). The spare code 3 —
  unreachable before, since only three widths exist — now selects a lane instead of a width, and the
  plain-tag bit says which: code 3 alone is the TRIE lane (the dictionary holds global ids into a
  resource-wide dictionary, and the tag stores no value bytes; armed by
  `-Dsirix.projection.trieLane=true` at load), and code 3 WITH the plain bit is the TEMPORAL lane
  (`-Dsirix.page.temporalLane=true`; a tag whose whole dictionary is `"YYYY-MM-DD"` or
  `"YYYY-MM-DD HH:MM:SS"` is stored as frame-of-reference-packed day/second counts via
  `page/pax/TemporalTextCodec`, which refuses anything it could not render back byte-identically).
  Decoding either lane is unconditional, so a page written with one stays readable after the switch
  is turned off. Compatibility is one-directional exactly as for the flags byte: a build predating a
  lane meets an unknown width code and throws, rather than reading an id or timestamp lane as
  lengths. `StringRegion.temporalLaneEnabled()` documents the one read path — a region REBUILT from
  the slotted page — that re-encodes under the current setting rather than the writer's.
- **Node records**: structural keys as zigzag varint deltas against the own node key
  (`DeltaVarIntCodec`), varint revisions, fixed 8-byte rolling hash (elided page-wide when all
  zero), typed number payloads (Double/Float fixed, Int/Long zigzag varint).
- **PathStats trailer** (`NodeKind.PATH`, present iff the resource has `withPathStatistics`):
  `[i64 count][i64 nullCount][i64 sum][i64 sumHi][i64 min][i64 max]`
  `[minBytes][maxBytes][hll][u8 minDirty][u8 maxDirty][f64 sumFraction][u8 sumDirty]`
  `[u8 doubleTyped][u8 countDirty][pageKeys?]`, where each optional block is a length-prefixed
  payload with `-1` meaning absent and the page-key trailer may be missing entirely.
  `sumHi` is the high half of a 128-bit integral accumulator, so what is persisted depends on the
  observed VALUES and not on how many flushes or chunks the load took.
  **No per-record version discriminator, by decision.** This record is versioned by neither a
  leading marker nor `BinaryEncodingVersion`: while SirixDB has no released consumers the layout is
  changed IN PLACE — as it was when `sumHi` was inserted after `sum` — rather than accreting version
  machinery and migration branches that nothing would ever exercise. The consequence is stated
  plainly: a resource is readable only by builds sharing this layout, in either direction, with no
  diagnostic. The golden byte pin in `GoldenFormatTest.pathStatsRecordBytesArePinned` is what keeps
  a layout change deliberate. Add a discriminator when there is a released version to stay
  compatible with, not before.

The **lightweight-compression direction is already the design**: per-section/columnar encodings
inside the page (templates, FOR+bit-packing, dictionaries, elision bitmaps, FSST) rather than a
generic byte-stream compressor. Empirical support (movies-style 22 MB corpus): the outer LZ4
pipeline (`-Dsirix.compression=lz4`) shrinks the store only 25 MB → 21 MB (−16%) while costing
~7% read latency — the structural redundancy is already captured by the inner encodings. The
remaining size lives in string *values*: per-block string dictionaries / wider FSST adoption is
the highest-value future encoding (Umbra "Data Blocks" model), and it slots into the existing
PAX-region + `structuralFlags` extension points without a format break.

## 3. Integrity

- Every page's XXH3-64 (of the compressed payload) is stored in its **parent's** PageReference →
  Merkle-style chain. Verified on read when `verifyChecksumsOnRead` (default true).
- The roots are covered too: both files' superblocks carry a CRC, both uber beacon
  slots carry an XXH3 trailer, and every revision record embeds an XXH3 of its offset+timestamp
  (+ the RevisionRootPage hash when present — 24-byte coverage; see §1).
- **RevisionRootPages are covered now too**: the one page reached without a parent reference
  (via `readRevisionRootPage`) carries its XXH3 in the 4th field of its revisions record, verified
  on read before deserialization exactly like a normal page (legacy `hash==0` records skip this).
- **Still not covered**: record/page length prefixes.

## 4. Storage backends

`FILE_CHANNEL` (default) and `MEMORY_MAPPED` share one on-disk format — MEMORY_MAPPED writes
through the same `FileChannelWriter` (superblock + UUID stamping included) and both backends
run the same superblock validation (magic/version/endianness/role/geometry/UUID) at open. This
is proven end-to-end by `io.sirix.io.StorageBackendInteropTest`: a resource written by either
backend reads back byte-identically AND accepts further commits under the other, and a
swapped-in foreign data file fails the resource-UUID cross-check under both. `IN_MEMORY`
(RAMStorage) persists nothing and is exercised by `StorageTest` alongside the file-backed
backends. The legacy `FILE` backend is removed (`StorageType.FILE` throws with a pointer to
FILE_CHANNEL — it wrote an incompatible layout under the same version); `IO_URING`/`S3`
resolve enterprise providers via SPI and fail fast with actionable errors when absent (also
covered by the interop test).

## 5. PRE-FREEZE CHECKLIST (ranked; decide before first user data)

Already part of V0 (this tree — there are no users, so V0 simply IS the superblocked layout; no migration version exists): templateCount 256-wrap capped at 255; deterministic
revisions-file slots + truncation; ordered dual-copy beacon writes + write-ahead barriers
(data **and** revisions); **superblocks in both files** (magic/version/endianness/role/CRC,
validated at storage open); **beacon slots one filesystem block apart with XXH3 trailers**
(recovery validates checksums, not "deserialization didn't throw"); **checksummed 32-byte
revision records** (little-endian) plus their **checksummed 16-entry tail log in both beacon
slots** (salvage + self-heal, eviction guarded by a per-resource durability watermark);
**legacy FILE backend removed** (`StorageType.FILE` fails
fast — it wrote an incompatible layout under the same version); u8 fragment-count guards;
PATH_SUMMARY writes its delegate byte via the shared helper; PageReference hashes as
`[u8 flag][8 B]` instead of `[i32 len][8 B]`; the `+8`
first-offset quirk and the 256-byte RevisionRootPage alignment are gone (8-byte alignment for
all data pages, data starts exactly at `DATA_REGION_START`).

Done since the audit (see `docs/BINARY_ENCODING_FUTURE_PROOFING_AUDIT.md` for the full list):
**full little-endian pin** (was item 1 — all scalar IO via `io.sirix.node.LE`); **reserved
flags byte in every page envelope** (additive changes no longer force a global version bump);
**revisions record stride persisted in the superblock** and validated at open; **stable id
bytes replace every enum-ordinal on disk** (including HOT node/layout types); **fail-fast unknown
node-kind/page-flags/version errors**; **pure-Java LZ4 block
decoder** (`JavaLz4BlockDecoder` — LZ4-bodied pages and FFILz4-pipeline resources are readable
without `liblz4`; writes fall back to stored-uncompressed frames); **hash-function identity
persisted + validated** (`ressetting.obj` `hashFunction` = "XX3"); **config field validation
throws in production** (was `assert`); **projection substructures carry version bytes**; **DeweyID framing
and record offset-table guards throw instead of truncating**; **long child/descendant counts**;
**golden byte-pinning tests** (`io.sirix.format.GoldenFormatTest` — superblocks, page
envelope, node record, varints, Roaring64 coupling, id registries).

Closed in the final hardening pass — every former checklist item is either implemented or
explicitly decided; **nothing remains open**:

- **Resource UUID** — implemented. Generated per resource, persisted in `ressetting.obj`
  (`resourceUuid`), embedded in both superblocks (bytes [40, 56), checksum-covered), validated
  at storage open in both backends; zero on either side = legacy, cross-check skipped
  (`SuperblockResourceUuidTest`).
- **fsync-order/write-loss validation** — covered. `PowerLossSimulationTest` (opt-in
  `-Dsirix.crash.run=true`) records every channel write and `force()` barrier of real commits,
  then materializes post-power-loss states where unforced writes are lost, applied, or torn in
  any combination/order — exactly the model a missing fsync barrier fails — with a
  seeded-corruption self-test proving the oracle is sharp. Gate run green in this tree.
- **Writer-reopen / truncate-recovery coverage** — covered. Both the SIGKILL gate
  (`CrashRecoveryInjectionTest`) and the power-loss gate verify the crashed resource ACCEPTS A
  WRITER again: `beginNodeTrx` runs the `.commit`-marker truncate-recovery path, the recovery
  commit succeeds, and every pre-crash revision stays intact. Gates run green in this tree.
- **Golden fixtures for composite pages** — implemented.
  `io.sirix.format.GoldenCompositePageTest` byte-pins a populated KeyValueLeafPage (records,
  compact directory, structural encoders, codec bake-off) and a populated HOTLeafPage,
  deterministic across independent runs.
- **KVLP header redundancy** — DECIDED: retained. The header's pageKey/revision/indexType
  duplication and the heapEnd/heapUsed runtime fields stay on disk: the 160-byte block is
  bulk-copied verbatim ("in-memory format = on-disk format" — stripping fields would
  reintroduce a commit-time conversion pass), and the redundancy makes pages self-describing
  for recovery/forensics plus cross-checkable against the parent reference. Cost: 21 bytes per
  ~64 KiB page. The `writeEncodedBody` javadoc and the stale "verified after decompression"
  comments are fixed.
- **Block-aligned page records** — DECIDED: V0 commits to pread/mmap with byte-granular
  8-byte-aligned `[u32 len][payload]` records. O_DIRECT/fixed-frame buffer management is not a
  V0 target; adopting it later is a file-layout change and therefore a superblock
  `LAYOUT_VERSION` bump (the version machinery for exactly this is in place), not a latent
  incompatibility.
- **Per-block string dictionaries / wider FSST** — reclassified: this is a compression
  *performance* roadmap item, not a format gap. It slots into the existing PAX-region +
  structuralFlags + envelope-flags extension points without a format break, so nothing about
  V0's future-proofness depends on it. Tracked in `ROADMAP.md`.


## JSON revision-diff sidecars

`update-operations/diffFromRev<old>toRev<new>.json` is a rebuildable cache written after the
authoritative storage commit. Version 1 sidecars add three top-level fields:

```text
"sirix-diff-format": 1
"operation-count": <number of entries in "diffs">
"operations-sha256": <64 lowercase hexadecimal characters>
```

The digest covers a canonical typed representation of the complete `diffs` array, including
object-field order, names, values, and numeric lexical forms; it is computed incrementally from the
Gson tree without creating another whole-file string or byte array. The writer first creates a
unique sibling temp file and then publishes it by same-directory atomic move where supported.

The core reader performs one strict JSON read and validates resource/revision identity, Unicode
scalar values, format version, count, digest, and the required schema of every operation before
hydrating fragment data. `jn:diff` treats any failure as a cache miss and computes the authoritative
revision diff. Multi-revision resource copy uses the same reader and fails before applying a partial
revision. Files without these version-1 integrity fields are not accepted by this build.

## Projection indexes (segment ⇔ slot layout)

Authoritative design + corner-case catalog: `docs/PROJECTION_INDEX_STORAGE_REDESIGN.md`
(§2.3a for this layout). Wire structs (all little-endian).

**Every segment is its own HOT slot** — the mapping is 1:1. A slot key is
composite, so one row group's descriptor and segments are key-adjacent and a full
read is one range scan rather than a descent per segment:

```
RevisionRootPage → ProjectionIndexPage (PageKind 16) → per-definition HOT sub-tree
  HOT slot key   = PathKeySerializer(slotKey)   (sign-flipped 8-byte BE)
    slotKey = (rowGroupId << 16) | slotKind,  rowGroupId ≥ 1
      slotKind 0                   = the row group's zone-map descriptor
      slotKind columnSegmentId + 1 = that segment's bytes
    slot 0            = metadata (rowGroupId 0 is reserved for it)
    slotKey ≥ 2^42    = fence chunks (above every row-group slot: rowGroupId < 2^24
                        and the shift is 16, so leaf slots stop at 2^40)
  HOT slot value =
    slot 0:    PIXB blob marker  { int "PIXB"; u8 ver=0; int byteLen; u64 xxh3 } [+ inline payload]
               byteLen high bit (0x8000_0000) = INLINE: payload rides the slot value right after
               the 17-byte marker (used when payload ≤ 512 B); else REFERENCED via an OverflowPage.
               payload = PIXM metadata { int "PIXM"; u8 ver=0; u8 flags; int rowGroupCount;
                                         int buildRevision; rootPath; columns[];
                                         setSummaryCapabilityColumns[]; dictionaryAnchors[] }
                                         (a few hundred B → inline; no metadata page on open).
                                         flags: bit0 = STALE; bits1-3 = the StaleReason ordinal
                                         (WIRE VALUES — append only, never renumber; an ordinal
                                         this build does not know reads as UNSPECIFIED and the
                                         entry stays stale). The reason bits are additive: they
                                         were always zero before, so a tombstone written without
                                         them parses identically and ver stays 0.
                                         ver=0 is the ONLY supported version: any other value
                                         parses to null → "no metadata" → fail-closed decline.
    slotKind 0: PIXD descriptor, a PIXB blob whose payload is
                                { int "PIXD"; u8 ver=0; int rowCount; u16 columnCount;
                                  i64 firstRecordKey; i64 lastRecordKey;
                                  u8 kinds[columnCount]; u16 segCount;
                                  segCount × { u16 columnSegmentId; int byteLen;
                                               u64 xxh3; u8 colFlags; i64 min; i64 max } }
               ZONE MAP ONLY — no trailing inline region: a segment's bytes live in the
               segment's own slot, never also here. The encoder emits only this form and
               every storage/read boundary rejects a byteLen inline marker or trailing
               payload; there is no normalization or compatibility reader. ver=0 is the
               ONLY supported version: validation refuses any other value, so a future
               shape change is rejected rather than misread.
               zero-length value = tombstone; rowCount==0 descriptor = live empty row group
    slotKind ≥ 1: BARE segment slot — { u8 kind } [+ raw segment bytes]
               kind 0 = INLINE (bytes follow, used when ≤ 512 B); kind 1 = REFERENCED
               (bytes in one OverflowPage off the side map). The CONTAINER carries no magic,
               no version and no hash — a segment's integrity is its descriptor entry's
               byteLen + xxh3, re-checked at assembly, so a second on-disk hash would be pure
               redundancy. (The bytes themselves are still the self-describing PIXS payload
               below; what is absent is the PIXB marker the descriptor slot carries.)
    slotKey 2^42+chunkId: PIXB blob, payload = one fence chunk for at most 32 physical leaves.
                  Each 144-byte entry is { i64 first; i64 last; i32 docNext; i32 docPrev;
                  i32 ownerBase; i32 numericSkip[25]; i64 baseUpper; i32 freeNext }.
                  Chunks are writer-side routing/document-order units and carry forward independently,
                  so a local update rewrites only touched 4.5 KiB full chunks.
    slotKey 2^42+2^20: PIXB blob, payload = the 32-byte PIFO order header
                  { magic; u8 ver=0 + padding; i32 baseCount; i32 physicalCount; i32 liveCount;
                    i32 freeHead; i32 documentHead; i32 documentTail }.
    slotKey 2^43+(column<<16)+chunk: PIXB blob, payload = one 256-row-group Bloom chunk.
    slotKey 2^44+column: PIXB blob, payload = one bounded set-summary column; an empty payload body
                  is an explicit capability and can be revived by incremental maintenance.
  HOT leaf side map (serialized behind envelope flag 0x01, complete map per fragment):
    (ownerSlotKey << 16 | subId) → page file offset (bare u64)
    subId is always 0 here — a referenced blob or segment slot owns exactly one page, since
    the slot IS the segment. |ownerSlotKey| < 2^47 is enforced by the composite encoder.
  OverflowPage (PageKind 9): offset identity, no fragments, whole-page last-writer-wins;
    integrity = descriptor/marker byteLen + xxh3 (its only checksum). TWO bodies, selected by
    envelope flag 0x01 (FLAG_OVERFLOW_PAYLOAD_COMPRESSED):
      flag clear: { i32 dataLength; bytes[dataLength] }
      flag set:   { i32 dataLength; i32 storedLength; u8 codec; bytes[storedLength] }
                  codec 0 = ZeroRunByte, 2 = ByteRun, 3 = SirixLZ77 (1 is not used here).
                  dataLength is the DECODED size; the decoder refuses a frame that produces
                  anything else rather than returning a short payload.
    The COMPRESSED body is the DEFAULT. A payload of OVERFLOW_COMPRESSION_MIN_BYTES = 64 bytes or
    more is offered to a codec bake-off on write, and takes the flag only when the winner beats the
    raw payload by more than the 5 bytes of extra framing (storedLength + codec byte); payloads
    under 64 bytes, and payloads no codec shrinks by that margin, keep the flag-clear body. Since
    overflow pages carry oversized records, projection column segments and value-dictionary blocks,
    the flag-set form is the common case on a real resource.
    `-Dsirix.page.overflow.compress=false` skips the bake-off and reproduces the flag-clear bytes
    exactly, which is also what every resource written before the flag existed carries.
    (Reused for referenced segments AND referenced blobs; the bespoke ProjectionSegmentPage was retired.)

Segment ids: 0 = KEYS, 4c+1 = BODY(c), 4c+2 = DICT(c), 4c+3 = SET_COUNTS(c),
  4c+4 = STRING_BLOOM(c). DICT_HASHES(c), emitted only for STRING_DICT columns, lives in the
  disjoint trailing region `DICT_HASH_SEGMENT_BASE+c`, where
  `DICT_HASH_SEGMENT_BASE = 4*MAX_COLUMNS+8`; keeping it outside the four-id stride preserves every
  existing V0 segment id. The descriptor's column limit is derived from the 16-bit segment-id
  space across both regions. It was 84 while
  segmentId shared an 8-bit sub-id field with the leaf index; a segment owning its own slot
  freed that space.
Segment wire: { int "PIXS"; u8 ver=0; u8 segKind } +
  KEYS:  i64 first; i64 last; [rows>0] u8 mode(0=delta-FOR asc,1=abs-FOR); i64 base; u8 width; packed
  BODY:  u8 colFlags (bit0 unrepresentable; bit1 non-integral / not-value-exact for doubles);
         [rows>0] i64 min; i64 max; presence marker (0 all-present / 1 all-missing / 2 words);
         NUMERIC (long or double-transform): i64 base; u8 width (65..255 reserved escapes); packed
         BOOLEAN: words verbatim   STRING: u8 idWidth; packed dict-ids
  DICT:  u8 mode (0 raw / 1 FSST / 2 raw+set-row-counts / 3 FSST+set-row-counts);
         raw modes 0/2 = { int dictSize; int lens[dictSize]; concatenated UTF-8 };
         FSST modes 1/3 = { int tableLen; table; int dictSize;
                            per entry int len + stream };
         row-count modes 2/3 append { u8 countWidth; packed unsigned rowCount[dictSize] }
  SET_COUNTS: u16 valueCount; valueCount × { u16 utf8Len; byte value[utf8Len]; u16 rowCount }
  STRING_BLOOM: int bitCount; u64 words[bitCount/64]
  DICT_HASHES: int dictSize; u64 fnv1a64[dictSize] in dictionary-id order
```

Double columns (kind 3) store the sortable-bits transform (negatives flip low 63 bits) —
order-isomorphic to signed longs, so zone maps / predicates / FOR packing are kind-agnostic;
literals are transformed at plan time, aggregates decode per matching row.

There is no projection-format reader bridge or migration path. Explicit index creation/recreation
may install a fresh CoW sub-tree when the target has no describable current layout; ordinary
maintenance never resets the tree and instead fails the owning transaction on inconsistent units.

A PIXM whose version byte is anything other than the one supported value (0) parses to null —
same PIXB/PIXM magic, so the version is the only discriminator — which every caller treats as
"no metadata" and declines; maintenance fails the owning write until the index is explicitly
re-created. That is what the byte is for: rejecting a format rather than misreading it. Earlier
revisions keep serving their own sub-tree.

Global string dictionaries use `ValueDictionaryHeaderNode` layout version 0. The stable metadata
anchor names a CoW header containing an entry count, generation, and forward/reverse radix roots.
The forward root first addresses the high 24 bits of FNV-1a. Its terminal bucket holds at most 128
`(FNV-1a,id)` pairs; a full terminal expands once into an eight-level independent 64-bit digest
radix. Equal secondary digests prepend immutable buckets of at most 128 pairs, and every candidate
is confirmed against its stored UTF-8 bytes. The reverse root addresses fixed 256-id blocks whose
entries are immutable `ValueDictionaryEntryNode` keys. Ordinary maintenance CoWs only the radix
paths and reverse block touched by new values, while IDs, metadata anchors, and historical roots
remain stable. Probe telemetry counts radix, bucket, reverse-block, and value-entry reads actually
performed.
