# SirixDB On-Disk Format (V0)

Status: **V0 record contract.** We are at
`BinaryEncodingVersion.V0` (pages) and superblock `LAYOUT_VERSION = 0` (files). Every
pre-freeze checklist item has been resolved (implemented or explicitly decided — see §5's
decisions log), and the byte-level contract is pinned by golden tests
(`io.sirix.format.GoldenFormatTest`, `io.sirix.format.GoldenCompositePageTest`): an accidental
change to any pinned structure fails CI. Intentional layout changes update the golden constants
and this contract. SirixDB has no released consumers: unreleased layouts evolve in place using
the original V0 value, without a migration reader. A released compatibility contract would need
an explicit versioning decision using `BinaryEncodingVersion` (page bodies/records), the page-envelope
flags byte (additive page features), `LAYOUT_VERSION` (file layout), or a sub-structure version
byte (PIXM/PIXD/PIXS/PIX1, per-record versions). The golden pins make changes to pinned structures
deliberate (see §2, "PathStats trailer").

The current V0 layout omits the eight-byte hash and its offset-table entry from fused JSON primitive leaves
(`OBJECT_NAMED_BOOLEAN`, `OBJECT_NAMED_NUMBER`, `OBJECT_NAMED_STRING`, `OBJECT_NAMED_NULL`).
Their hashes are computed on demand from the existing node identity, links, name, and typed
payload inputs. Objects, arrays, and document roots retain stored subtree hashes. The page
version byte remains 0; this unreleased layout changes in place without a migration reader.
File superblocks and independently versioned projection structures retain their versions.

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
  second-guessed. The [tombstone safeguards below](#sirixrevisions) further restrict salvage and
  healing. A salvaged record is written back into the revisions file (self-heal), so the
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
4096 + 32*revision : [u64 rootOffsetOrPrunedSentinel][u64 epochMillis]
                     [u64 recordChecksum][u64 pageHashOrPrunedTag]      (little-endian)
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
`verifyChecksumsOnRead`). The **record checksum always covers 24 bytes** (offset + timestamp +
hash-or-tag). Zero hashes and old 16-byte-checksum records are not supported. An all-zero real
payload hash is remapped to a nonzero sentinel before storage.

Retention preserves a dense original revision timeline. A pruned revision's slot stores
`[Long.MIN_VALUE][original epochMillis][recordChecksum][0x5052554E45440001]`. The last word is a
record-type tag, not a root hash. Both the sentinel/tag pair and the checksum must match; invalid
offsets, tags, checksums, zero holes, and partial slots are corruption. A live offset with this
hash remains a live record. `IOStorage.decodeRevisionRecord` is the shared decoding boundary;
`IOStorage.writePrunedRevisionRecord(ByteBuffer, long)` writes tombstones into reusable storage.

The in-memory `RevisionIndex` retains every original number and timestamp, with the sentinel in
its offsets array. Metadata and root-read contracts are documented by
[`Reader`](../bundles/sirix-core/src/main/java/io/sirix/io/Reader.java); timestamp selection and
transaction-open behavior by
[`ResourceSession`](../bundles/sirix-core/src/main/java/io/sirix/api/ResourceSession.java), and
revert/truncate targets by [`NodeTrx`](../bundles/sirix-core/src/main/java/io/sirix/api/NodeTrx.java).
Bootstrap and latest revisions must remain live. A valid tombstone bypasses tail-log recovery;
a damaged recognizable tombstone cannot be healed from an obsolete live entry. Compactors must
publish fresh beacons without obsolete tail-log entries and clear caches/reload the index after swap.


### Endianness

Page and record scalar IO is **little-endian pinned** (checklist item 1 — done). Raw payloads
with deliberately big-endian fields use the explicit encodings listed below; neither depends on
host byte order:

- Superblock, beacon XXH3 trailers, revision records, beacon/page-record length prefixes:
  pinned LE (`ByteOrder.LITTLE_ENDIAN` buffers / `MMFileReader.LAYOUT_INT`).
- `BytesOut`/`BytesIn` payload primitives, the KVLP header+bitmap block, PAX regions,
  flyweight field access, and the FFILz4 frame header: pinned LE via `io.sirix.node.LE`
  layouts (`ByteArrayBytesIn` already decoded LE byte-shifts).
- Deliberate big-endian defined byte sequences: `compactDir` uses explicit byte composition on
  stream reads and a byte-swapped LE-short access on `MemorySegment`; in-blob column length
  prefixes use explicit shifts. HOT discriminative keys use BE loads for lexicographic
  `compareUnsigned`. Temporal aggregate slot payloads, part headers and summary scalars, and
  change-log headers and fixed-width slab fields also use explicit big-endian encoding.
  These are defined wire byte sequences, not host-order dependent.
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
| HOT_LEAF (12) | `0x01` | `HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS` | the page carries an overflow-page side map |

Every other kind writes zero and refuses any nonzero bit. **Compatibility is one-directional:** a
build that predates a flag bit reads a resource written without it, but a resource written WITH it
cannot be read by that build — the reader refuses the unknown bit loudly rather than misparsing.
The stable kind-id registry is defined by
[`PageKind`](../bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java) and pinned by
`GoldenFormatTest.pageKindIdsArePinned`; retired ids 7 and 14 are rejected.

**Format evolution mechanism:** unreleased node-record/page-body layouts currently evolve in
place under V0. The hook for a future released-format version decision is `BinaryEncodingVersion`:
every node/page (de)serializer receives the
`ResourceConfiguration`, which carries the resource's persisted `binaryEncoding`, and the
per-page version byte fails fast on unknown values. Node kinds that need to evolve
independently of the global version carry a per-record version byte (the VECTOR_NODE /
VECTOR_INDEX_METADATA pattern); sub-structures with their own magic carry their own version
byte (PIXD/PIXS/PIX1, PIXM). Enum-typed bytes on disk are always explicit stable ids with
fail-fast lookups, never ordinals.

- **UberPage** body: `[i32 revisionCount]` — 7 bytes total incl. envelope, no checksum (§5.3).
- **RevisionRootPage**: delegate refs + revision, maxNodeKeys, commit timestamp/message, user.
  The delegate's reference count and offsets are defined by
  [`RevisionRootPage`](../bundles/sirix-core/src/main/java/io/sirix/page/RevisionRootPage.java).
  The aggregate and change-log references extend the unreleased V0 layout in place; earlier
  databases must be recreated rather than opened through a compatibility reader.
- **CASPage / PathPage / ProjectionIndexPage / ValidTimeIndexPage / TemporalAggregateIndexPage / ChangeLogIndexPage**: reference-container delegate
  followed by one sparse HOT allocator map `[i32 count][count × (i32 indexId, i64 maxHotPageKey)]`.
  Entries are emitted in ascending `indexId` order. These secondary-index containers have one
  representation only: their references root HOT trees; no keyed-trie node-key or indirect-level
  counters are persisted. This unreleased layout deliberately replaces the earlier three-map V0
  draft in place; there are no persisted databases requiring a compatibility branch.
- **Temporal aggregate HOT slots** (`IndexType` id 12, container `PageKind` id 18): revision-root
  reference offset 11. All fixed-width slot and part payload scalars are big-endian.
  Prefix 0 holds format 5, the part-id allocator and unsupported-row count;
  prefix 1 maps typed grouping values to `(live rows i64, checkpoint tier i32, part count i32,
  checkpoint bytes i64, delta bytes i64)`
  followed by `(part id i64, tier u8)` pairs. The first part is the checkpoint; each lower tier holds
  fewer than sixteen deltas. There are at most six delta tiers and 91 parts, within one inline slot. Prefix 3 maps `(part id, chunk number)` to at most 2,048 opaque bytes. Parts hold sorted
  endpoint prefix counts and signed 128-bit sums, a compressed canonical time-cover tree and
  deduplicated sorted signed value-frequency prefixes for declared extrema (`TAP1` magic). Small
  deltas use `TAD1` magic and at most 64 sorted `(from, to, value, sign)` candidates per extrema lane
  instead of a time-tree directory. Prefix 4 in physical aggregate tree zero maps a source revision
  `(prefix u8=4, source revision i32)` to `(source revision i32, derived revision i32, projection id i32,
  mapping format i32=1, source timestamp i64, source identity high i64, source identity low i64)`.
  Source revision -1 stores the latest maintained source revision as i32. These mappings and the
  aggregate pages commit atomically in the hidden `<source>$derived` resource. A projection id of -1
  identifies a revision with no aggregate declarations and is never admitted by the aggregate route.
  Coordinates use day-aligned millisecond radix. See
  [TEMPORAL_AGGREGATE_INDEX.md](TEMPORAL_AGGREGATE_INDEX.md) for declaration, maintenance and admission semantics.
- **Projection change log** (`IndexType` id 13, container `PageKind` id 19): revision-root
  reference offset 12, one HOT tree per source projection id. The prefix-0 header holds big-endian
  `(format i32=4, first revision i32, column count i32, exceptional row count i64)`.
  Prefix 1 `(record key i64, chunk i32)` holds sparse row-local provenance: object/unsupported
  kind byte, then one flag byte per column (without presence), in chunks of at most 2,048 bytes.
  Chunk -1 holds the total length as i32; a zero-length directory removes an exception.
  Prefix 2 `(revision i32, chunk i32)` holds change slabs in chunks of at most 2,048 bytes; chunk
  -1 is the 24-byte `(total byte length i32, chunk count i32, identity high i64, identity low i64)` extent.
  Its random 128-bit publication identity prevents a derived mapping from matching a truncated and
  replaced revision, even when the replacement uses the same revision number and timestamp.
  Prefix 5 in physical change-log tree zero uses `(5, -1 i32)` for the next aggregate id (i32),
  and `(5, aggregate id i32)` with value `1 u8` for each reserved declaration;
  this keeps declarations from allocating aggregate pages in the source. All roots are revision owned.
  A slab begins with `SCLG` magic, format 4, projection id, revision, base revision and entry count
  (all i32), followed by a u16 column count, normalized u8 cell kinds and a slab-local UTF-8
  dictionary (i32 count, unsigned-varint byte lengths). Entries are sorted by record key and hold
  unsigned-varint key deltas accumulated from zero, big-endian u64 changed-column bitmap words,
  then old and new rows. An empty slab occupies 30 bytes plus one byte per column.
  Row kind 0 is absent, 1 object, 2 unsupported; present rows carry per-cell flag bytes (presence,
  unrepresentable, non-integral, non-double-source). Representable present cells carry zig-zag
  varint primitive lanes or unsigned-varint dictionary ids; sets carry an unsigned-varint count
  and ids. Present timestamp cells also retain their epoch-second lane when the unrepresentable
  flag is set; that flag can describe lexical reconstruction rather than an unusable epoch.
  Kinds 1..7 are long, string, boolean, double transform bits, string set, timestamp, date.
  The log records projected cells, including unsupported flags, rather than duplicating arbitrary
  document subtrees. `base revision` records the represented revision after a revert, rather than
  assuming the immediately preceding published revision. Repeated maintenance barriers replace
  the current slab with the coalesced first-old/final-new cells. No-op revisions have empty slabs.
  Slabs are written only while a declared aggregate consumes the projection; the first-revision
  header does not imply gap-free coverage. Redeclaration rebuilds sparse provenance, retaining
  earlier slabs and historical roots.
  `ChangeLogSlabTest` pins format 4 with a golden byte fixture. Earlier formats are rejected;
  this unreleased layout has no backward-compatibility reader.
  Compact temporal operands use prefix 7 `(revision i32, aggregate id i32)` for a 60-byte header:
  `TAS1` magic, revision, represented base, aggregate id, group count, block count, chunk count,
  total serialized bytes including the header (all i32), signed unsupported-row delta (i64),
  unservable/overflow flag (i32, 0 or 1), and the raw slab's 128-bit publication identity.
  Prefix 6 keys are `(revision i32, aggregate id i32, identity 16 bytes, block upper-fence group,
  chunk u16)`. The identity isolates replacements within the same revision. A block begins with
  total block bytes (i32) and group count (u16), then sorted records `(group-key length u16,
  operand length u16, group bytes, operand bytes)`. Blocks fit one 2,048-byte slot; a single large
  group can use exactly two consecutive slots, chunks 0 and 1. Operand length zero is an explicit
  decline marker. Each block's key carries its last group as an upper fence.
  Operands contain unsigned-varint endpoint and multiplicity counts, followed by endpoint-coordinate
  deltas and zig-zag-varint signed row/presence counts and low/high signed-128 sum halves for SUM
  lanes. Frequency entries are `(lane, from, length)` unsigned varints and `(value, multiplicity)`
  zig-zag varints. Empty net operands are omitted, but every declared revision retains a coverage header.

  The derived resource's CHANGE_LOG tree zero also holds maintainer-only binary summary folds.
  Keys are `(8 u8, aggregate id i32, range end i32, level u8, chunk i32)`; level `k` covers
  `[end - 2^k + 1, end]`, with `end` divisible by `2^k`. Chunk `-1` is a 24-byte header:
  payload length i32 (`-1` means unavailable), group count i32, source-end publication identity
  (16 bytes). Payload slots are at most 2,048 bytes. Their concatenation contains sorted groups
  `(key length i32, operand length i32, key bytes, operand bytes)`. Operands use the same signed
  endpoint/frequency codec as source summaries, with no single-source-operand size restriction.
  Key `{10}` records the fold frontier `(source revision i32, identity projection i32, identity
  16 bytes)`. Publication, historical range retention and identity validation follow
  [the temporal aggregate contract](TEMPORAL_AGGREGATE_INDEX.md).

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
  (`DeltaVarIntCodec`), varint revisions and typed number payloads (Double/Float fixed,
  Int/Long zigzag varint). Kinds with a stored hash use a fixed 8-byte rolling hash (elided
  page-wide when all zero); the primitive-leaf exception is specified in the V0 contract above.
  Per-kind field counts and indices are defined by
  [`NodeFieldLayout`](../bundles/sirix-core/src/main/java/io/sirix/page/NodeFieldLayout.java).
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

### CAS and VALIDTIME posting chunks

A logical posting list is split into chunks of 65,536 node keys. A base slot uses
`logicalPrefix || chunkIndex_BE4`; its ordinary `NodeReferences` payload holds the low 16 bits.
The chunk index is unsigned, preserving node keys throughout `[0, 2^48)`.

CAS prefixes have a fixed 10-byte header (sign-flipped path class, type id), followed by the
atomic value's existing order-preserving encoding with `00` escaped as `00 FF`, then `00 00`.
The value is capped at **236 escaped bytes**, ending before an escape that cannot fit in full.
This leaves room for the header, terminator, four-byte chunk index and four-byte delta suffix
within a 256-byte stored key: MultiMask byte positions range from 0 to 255. Over-long values
share a key and require a document-value re-check. Decimal exact suffixes reserve room for a
fully escaped eight-byte double prefix and their own terminator; their bound is
`CASKeySerializer.MAX_DECIMAL_SUFFIX_BYTES`. Capped suffixes also share a key. Candidate
re-checking is specified in [HOT index specification §4.4.3](HOT_INDEX_SPECIFICATION.md#443-index-level-ranges).
Framing keeps logical keys prefix-free without changing the atomic ordering or the direct path-class read.
VALIDTIME prefixes remain fixed at 17 bytes.

For chunks below index `0x80000000`, a base payload of at least 256 bytes activates append-only
changes. Each delta is `baseKey || suffix_BE4`, where
`suffix = 0x80000000 | (sequence << 1) | removal`. Its payload contains exactly one low-16 posting
bit. It sorts after its base and before the next chunk. The 16th effective change folds the prior 15
deltas plus that operation in memory, replaces the base once, and tombstones the old delta
slots. Subsequent changes reuse sequence slots. Duplicate additions and absent removals write
nothing. Higher chunk indices retain direct updates; they cannot be confused with suffixes because
readers identify the logical key boundary, not the high bit of a chunk index.

A folded payload of at least 256 bytes lives in an `OverflowPage`. Its leaf slot stores the
21-byte marker `[FD][referenceKey_BE8][payloadLength_BE4][payloadHash_BE8]`; the payload length must be
in `[1, 65535]`. The payload hash uses the same XXH3-64 function as projection side-page descriptors.
The side-map key is a 47-bit FNV-1a hash of the composite key, mixed with `hash ^= hash >>> 29`,
shifted left 16 bits with sub-id 1.
A same-leaf hash collision leaves a newly folded payload inline. Frontier rebuilds that bring
colliding owners together rename a marker to a free side-map key before attaching its payload.
Readers resolve the side page through their revision and validate its length against the marker.
When `verifyChecksumsOnRead` is enabled, every reader and writer verifies the payload hash before
decoding, including after a cold reopen.
Splits and frontier rebuilds route references by the actual composite owner key; consolidation
skips leaves carrying side pages. A fold that becomes cold or empty removes the old side reference.

Every revision reads its own base and live deltas in sequence order. Folds replace immutable side
pages and do not alter historical postings. This is the only CAS key encoding and posting layout;
there are no old-format readers, migrations, or compatibility switches.

## 3. Integrity

- Ordinary parent `PageReference`s store the page's XXH3-64 of the compressed payload, verified on
  read when `verifyChecksumsOnRead` (default true). HOT child, fragment, and side-map references
  persist offsets without hashes; their integrity limits are specified in
  [HOT index specification §3.6](HOT_INDEX_SPECIFICATION.md#36-hot-format-and-integrity).
- The roots are covered too: both files' superblocks carry a CRC, both uber beacon
  slots carry an XXH3 trailer, and revision records and readable RevisionRootPages follow the
  integrity contract in [§1, sirix.revisions](#sirixrevisions).
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

Already part of V0 (the unreleased layout changes in place; no migration version exists): templateCount 256-wrap capped at 255; deterministic
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

An ordinary subtree insertion is represented by its inserted root; descendants are carried by
that fragment. When ingestion skips the input root, each inserted sibling subtree has its own
root entry, exactly once. No entry for the skipped root is emitted. The emission lifecycle and
bulk revision baseline are documented in [JSON_UPDATE_DIFFS.md](JSON_UPDATE_DIFFS.md#which-resources-pay-the-cost).

Insert anchors name the parent or left sibling in the new revision; serialization does not
rebase them. These presentation operations are not the resource-replication protocol.

The digest covers a canonical typed representation of the complete `diffs` array, including
object-field order, names, values, and numeric lexical forms; it is computed incrementally from the
Gson tree without creating another whole-file string or byte array. The writer first creates a
unique sibling temp file and then publishes it by same-directory atomic move where supported.

The core reader performs one strict JSON read and validates resource/revision identity, Unicode
scalar values, format version, count, digest, and the required schema of every operation before
hydrating fragment data. Missing or invalid sidecars are cache misses: `jn:diff` computes the
revision diff, while `getUpdateOperations()` uses the historically named
`BasicJsonDiff.generateDiffForReplay` to expose edits inside retained fragments. Multi-revision
resource copy uses the separate identity protocol described below. Public
`BasicJsonDiff.generateDiff` keeps hash-based equal-subtree skips and compact
inserted-subtree traversal. Files without these version-1 integrity fields are not accepted by
this build.

`jn:diff` uses a sidecar only for adjacent revisions with stored Dewey IDs. The REST `/diff`
endpoint can use an adjacent-revision sidecar without Dewey IDs for an unfiltered request;
sidecar filtering requires Dewey IDs. Other revision pairs use the computed diff.

## JSON identity replay

`JsonResourceCopy.Builder.copyAllRevisionsUpToMostRecent()` imports committed target state through
the internal version-1 `JsonReplayManifest` / `JsonIdentityDelta` protocol. This is a typed
in-process contract, not a persisted JSON or binary sidecar format. A manifest identifies the
source resource UUID and path, exact base/target revisions, destination revision mapping,
allocation frontiers and hash/Dewey configuration. Its unordered PUT/DELETE set carries final
logical records keyed by persistent node identity, including final links and revision metadata.

The first epoch enumerates the complete source snapshot. Subsequent epochs compare authoritative
document-record pages and resolve their full logical records under the resource's versioning
mode. Only identical immutable persisted page/fragment references permit skipping a region;
presentation hashes and diff sidecars are never identity proofs. Sparse frontiers do not cause
a scan of every numeric key, and an unchanged epoch does not reconstruct the whole document.

The transaction-local importer requires a clean document epoch and rejects incompatible epochs or
configurations before staging. It stages identities and payloads before installing their final
links, then validates graph closure, compatible parents, reciprocal ordered links, acyclicity,
enabled counts and allocation bounds. It maintains logical names, path summaries and the destination's
configured indexes, then publishes one complete revision. Threshold/time commits cannot expose
staging records.
Failure before publication rolls back the current epoch; previously completed copied revisions
remain available.
Import memory includes the detached delta, epoch/validation state and staged page buffers, and
scales with the imported epoch, or with the document size for the initial snapshot. Compound
import suppresses intermediate rotation and does not request an async flush, so staged page
buffers remain retained until commit even under `KEEP_OPEN_ASYNC_FLUSH`.
The intent log does not enforce a resident-page bound for identity import.
Work budgets constrain counted traversal and maintenance work; they do not establish a fixed
memory bound, constant total heap, a streaming manifest or a latency guarantee.

History copy requires a fresh destination document and matching hash, Dewey, child-count,
path-summary/statistics and node-history configuration. It preserves node keys, allocation
frontiers and stored Dewey IDs. A source history suffix beginning at revision S maps S to
destination revision one; predecessor revisions outside that suffix are unavailable. Ordinary
subtree snapshot copies continue to allocate destination keys normally. Imported epochs omit
presentation sidecars, so public diff consumers use their ordinary cache-miss path.

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
                                         ver=0 (row-group-major slots) and ver=1 (column-major
                                         slots, see Compatibility) are the only supported
                                         versions: any other value parses to null → "no
                                         metadata" → fail-closed decline.
    slotKind 0: PIXD descriptor, a PIXB blob whose payload is
                                { int "PIXD"; u8 ver; int rowCount; u16 columnCount;
                                  i64 firstRecordKey; i64 lastRecordKey;
                                  u8 kinds[columnCount]; u16 segCount;
                                  segCount × { u16 columnSegmentId; int byteLen;
                                               u64 xxh3; u8 colFlags; i64 min; i64 max } }
               ZONE MAP ONLY — no trailing inline region: a segment's bytes live in the
               segment's own slot, never also here. The encoder emits only this form and
               every storage/read boundary rejects a byteLen inline marker or trailing
               payload; there is no normalization or compatibility reader. ver=0 describes
               persisted column segments. ver=1 describes the merged base plus open row tail;
               the segments remain the base until a fold. These are two states of the same
               format, with identical descriptor layout. Other versions are rejected.
               zero-length value = tombstone; rowCount==0 descriptor = live empty row group
    slotKey 2^46+2^45+(rowGroupId<<16): PIXB blob with tail header payload
               { int "PIXH"; u8 ver=1; int baseDescriptorLen; u8 baseDescriptor[];
                 int tailBlobCount; int tailRowCount }.
               The base descriptor is untailed and describes the persisted segment slots.
    slotKey 2^46+2^45+(rowGroupId<<16)+seq: referenced PIXB blob with PIXT rows,
               { int "PIXT"; u8 ver=1; int rowCount; u16 columnCount; u8 kinds[];
                 rowCount × { i64 recordKey; u8 orderFlags; u16 orderLabelLen;
                              u8 orderLabel[]; columnCount × { u8 flags; value } } }.
               orderFlags bit0 = order exception; column flags bits0-3 = present,
               unrepresentable, non-integral, non-double-source. Other bits are rejected.
               value = i64 lane value (including resolved STRING_GLOBAL ids), u8 boolean,
               int UTF-8 byteLen + bytes for STRING_DICT, or int elementCount followed by
               int UTF-8 byteLen + bytes per STRING_SET element. seq starts at 1.
               Only row-group-major slots carry tails. Append eligibility, reader resolution
               and atomic folding are specified in PROJECTION_INDEX_INCREMENTAL_MAINTENANCE.md §9.
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
    slotKey 16+column: PIXB blob, payload = the 21-byte PBMF Bloom manifest
                  { u32 magic=0x464D4250; u8 ver=1; i32 liveRowGroupCount;
                    i32 physicalRowGroupCount; i32 chunkLeaves=256; i32 chunkCount }.
                  Version 1 is the only accepted manifest version; chunkCount is
                  ceil(physicalRowGroupCount / 256), including any open chunk.
    slotKey 2^43+(column<<16)+chunk: PIXB blob, payload = one sealed 256-row-group Bloom block
                  { u32 magic=0x50424C4D; u8 ver=0; i32 leafCount=256;
                    i32 offsets[257]; concatenated STRING_BLOOM segments }.
                  Offsets are relative to the concatenated payload; an empty slice supplies no evidence.
    slotKey 2^44+2^43+(column<<24)+rowGroupId: PIXB blob, payload = one open row group's raw
                  PIXS STRING_BLOOM segment (including its segment header, not a block wrapper).
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
  BODY:  u8 colFlags (bit0 unrepresentable; bit1 non-integral / not-value-exact for doubles;
                     bit2 kind-specific source provenance);
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

Bloom blocks and tails are indexed by physical row-group id. For a manifest's physical count P,
there are floor(P / 256) sealed blocks; its open chunk has P mod 256 per-row-group tails and no
block. At an exact boundary there are no open tails. The manifest is published after its blobs;
revision-bound readers derive the split from that manifest. A missing, malformed or unsupported
manifest disables manifest-backed pruning for its column. An unusable sealed block keeps its
span eligible, and an unusable tail keeps its one row group eligible. Entirely empty blocks and
tails need not occupy slots. See [Bloom maintenance](SEGMENT_PROJECTION_INDEXES.md#64-bloom-chunks)
for folding and recovery ownership.

The BODY provenance flag `0x04`, also mirrored in the descriptor and PIX1 presence tail, means
`PURE_DOUBLE_SOURCE` for NUMERIC_DOUBLE and `PURE_UTC_TIMESTAMP` for TIMESTAMP; other kinds reject
it. Timestamp provenance proves every present cell is a canonical Z-suffixed source in years
0001–9999. Missing provenance, duplicate fields and structured cells fail closed. Such timestamps
retain `UNREPRESENTABLE` for lexical consumers, which must preserve the original spelling; consumers
checking UTC provenance may use the epoch lane independently of that lexical flag.

Double columns (kind 3) store the sortable-bits transform (negatives flip low 63 bits) —
order-isomorphic to signed longs, so zone maps / predicates / FOR packing are kind-agnostic;
literals are transformed at plan time, aggregates decode per matching row.

There is no projection-format reader bridge or migration path. Explicit index creation/recreation
may install a fresh CoW sub-tree when the target has no describable current layout; ordinary
maintenance never resets the tree and instead fails the owning transaction on inconsistent units.

A PIXM whose version byte is anything other than a supported value (0 or 1) parses to null —
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

### Encoded sorted views

A `projectionSort` definition adds a revisioned sorted view. Its current format is implemented by
`ProjectionSortedLeaf`, `ProjectionSortedDataBlock`, `ProjectionSortedDirectory`,
`ProjectionSortedDirectoryNode`, `ProjectionSortedLeafBounds` and `ProjectionSortedGroupSma`.

| Structure | Logical slot |
|---|---|
| Data chunk | `2^46 + leafId` |
| Directory header / nodes | `2^46 + 2^32` / header slot + node id |
| Group summary | `2^46 + 2^33 + leafId` |
| Bounds record | `2^46 + 3·2^32 + ((leafId − 1) >> 4)` |
| Group-extrema header / nodes | `2^46 + 5·2^32` / header slot + node id |
| Indirect directory separator | `2^46 + 6·2^32 + separatorId` |

Encoded sorted-view chunks target 6144 bytes with an 8192-byte cap and at most 256 rows,
independent of the 512-byte inline payload limit. Ordinary data chunks, summaries and directory
nodes above that inline limit are stored as referenced side pages (generic `OverflowPage` blobs);
payloads at or below it remain inline. An individual oversized tuple can exceed the packing cap,
up to the leaf codec's 64 KiB limit, and remains a single generic referenced blob. The former
whole-leaf overflow layout is no longer written or supported as a separate sorted-view layout.

`PSL1` has two current encoding schemes; encoders choose the smaller eligible representation.
Integers and numeric deltas are little-endian, while key/dictionary bytes keep their ordered encoding.
Both schemes validate dimensions and strict row order on open and support direct row access and
binary search without block decompression.

**Prefix rows (scheme 1)** also encode directory and summary entries:

| Offset | Width | Field |
|---|---|---|
| 0 | i32 | magic `0x314C5350` (`PSL1`) |
| 4 | u8 | scheme 1 |
| 5 | u16 | rows (1..256) |
| 7 | u16 | prefixBytes |
| 9 | prefixBytes | common key prefix |
| 9+p | i32 × (rows+1) | absolute entry offsets; first starts the entry area, last equals blob length |
| … | per entry | `u16 suffixLen, u16 payloadLen, suffix, payload` |

Every offset and entry length is validated against the exact encoded slice.

**Dictionary/delta columns (scheme 2)** require keys ending in a present long and record id, with
zero to four covering payload words:

```text
PSL1:i32, scheme=2:u8, rows:u16, dictionarySize:u16,
payloadWords:u8, valueWidth:u8, recordWidth:u8,
valueMin:i64, valueMax:i64, recordMin:u64;
(width:u8, min:u64)[payloadWords];
dictionaryOffsets:u16[dictionarySize+1], dictionaryBytes;
dictionaryIds:u8[rows] only when dictionarySize > 1;
valueDeltas[rows], recordDeltas[rows], payloadWordDeltas[payloadWords][rows]
```

Each delta column uses its declared byte width (0..8); width zero denotes a constant column.
The leading key fields form the ordered dictionary; its entries must be unique, strictly ordered
and not prefix-overlapping. Opening checks dictionary offsets/IDs, exact encoded length, row order
and the header's exact value extrema.

**Directory header version 6** is the only supported version; its size is `39 + fieldCount` bytes:

| Offset | Width | Field |
|---|---|---|
| 0 | i32 | magic `0x31445350` (`PSD1`) |
| 4 | u8 | version 6 |
| 5 | u8 | height (0..32) |
| 6 | i32 | rootId (0 = empty) |
| 10 | i32 | nodeCount (node-id high-water mark) |
| 14 | i32 | dataLeafCount |
| 18 | i32 | maxLeafId |
| 22 | i64 | unencodableRows |
| 30 | i64 | exact missing aggregate rows; −1 for a non-aggregate layout |
| 38 | u8 | key-layout field count (1..255) |
| 39 | n | key-layout field shapes |

Parsing checks `rootId == 0 ⇔ dataLeafCount == 0 ⇔ height == 0`, nonnegative dimensions/counts,
`rootId ≤ nodeCount`, `maxLeafId ≥ dataLeafCount`, a valid key layout and a missing-value count
consistent with that layout. Direct nodes store each child's first key and an i32 child id. When
two full fences cannot fit within the encoded-node cap, an indirect node stores one or two bounded
entries with an `(i32 childId, i32 separatorId)` payload; its ordered fences are read from the
separator slots. Nodes and separators follow the same inline/reference residence rule as data.

**Bounds version 2** is the only supported version. Each inline 272-byte record covers 16 chunk ids:
`PSB1:i32` (`0x31425350`), `version=2:u8`, `log2Chunks=4:u8`, two zero bytes, a u64 flags word,
then sixteen `(i64 min, i64 max)` pairs. Flags 0–15 mark valid extrema; flags 16–31 prove that the
corresponding chunk contains one group. Other bits must be zero, and a proof requires valid bounds.
A proven chunk supplies its group key from the directory fence and its extrema from bounds.
Replacing/removing a summary clears its proof and invalidates the group-extrema tree before
publication; ordinary maintenance republishes proofs after writing consistent data and summaries.

**Group-extrema tree** metadata uses a 24-byte header: `magic:i32` (`0x414D5347`), `root:i32`,
`height:i32`, `highWater:i32`, `groups:i64`. Root/height/groups are zero together for an empty tree;
height is in 0..32, groups is nonnegative and root is within the nonnegative high-water mark.
Nodes are bounded to the inline payload limit: leaf entries hold an encoded group key and `(i64 min, i64 max)`; interior
entries hold a first-group fence and an i32 child id.
Eligibility and edit maintenance are described in
[Projection read performance](PROJECTION_READ_PERFORMANCE.md#sorted-views).

### Compatibility

These persisted projection structures were added after the previous release:

- **Flag-summary chunks** (`ProjectionFlagSummaryChunks`): per-leaf liveness and BODY-flag evidence
  for projections of at most eight columns, 32 leaves per chunk at `2^45 + chunkId`, with a header
  at `2^45 + 2^20`.
- **Numeric-proof chunks** (`ProjectionNumericProofs`): optional evidence binding an integral BODY's
  extrema and full presence to its content hash, 64 row groups per chunk at
  `2^46 + 2^34 + column·2^18 + chunk`, with a header at `2^46 + 2^34 + 2^32 − 1`.
- **Value-summary capabilities for dictionary string columns**: PIXM `setSummaryCapabilityColumns[]`
  may name `STRING_DICT` columns, not only `STRING_SET` columns. Their bounded summaries use the
  existing `2^44 + column` slots.
- **Version-1 set-summary chunks**: a summary with a missing-value entry is written with version
  byte 1; a u16 length of `0xFFFF` marks that entry and no value bytes follow. Summaries without one
  are still written as version 0.
- **`projectionSort` element** of the projection `IndexDef`: the ordered sort-column list, one
  `keyColumn column="N"` child per key column. Column numbers index the declared projection fields.
- **Encoded sorted views**: the slots, encoding schemes, directory/bounds versions and optional
  group-extrema metadata are specified [above](#encoded-sorted-views).
- **Column-major slot layout** (PIXM version 1), created only by a fresh bulk build with
  `-Dsirix.projection.columnMajorSlots=true`: descriptor and segment slots move to
  `2^41 | slotKind << 25 | rowGroupId`.

The nonsorted additions retain their existing readers: version-0 metadata and set-summary chunks
still parse; missing flag-summary and numeric-proof slots fall back to the descriptor and BODY
paths, and a definition without `projectionSort` has no sorted view. Sorted views require the encoded chunk layout:
older directory and bounds versions are rejected, with no migration or compatibility mode.
Directory version 6 moves indirect separators from family `4·2^32` to `6·2^32` above the
`2^46` base, avoiding the numeric-proof family. Directory versions 3, 4 and 5 are obsolete:
queries decline the index and maintenance fails the owning transaction with a rebuild-required
format error. Drop and rebuild the projection index before using its sorted view with this code;
there is no in-place conversion or dual-family reader.

Projection indexes built by this code cannot be opened by the previous release. It rejects metadata
that names a value-summary capability on a dictionary string column, version-1 set-summary chunks,
and version-6 sorted directory headers, as corrupt: queries decline the index and maintenance fails
the owning transaction. It also declines PIXM version 1 as unsupported. It ignores the remaining
slots and the `projectionSort` element, so its maintenance would leave them stale even in an index
it does not reject. When downgrading, drop the projection index and rebuild it with the previous
release; there is no in-place conversion.
