# SirixDB On-Disk Format (V0)

Status: **V0 contract complete — no open format work items.** We are at
`BinaryEncodingVersion.V0` (pages) and superblock `LAYOUT_VERSION = 0` (files). Every
pre-freeze checklist item has been resolved (implemented or explicitly decided — see §5's
decisions log), and the byte-level contract is pinned by golden tests
(`io.sirix.format.GoldenFormatTest`, `io.sirix.format.GoldenCompositePageTest`): an accidental
change to any pinned structure fails CI. From here, ANY change to the bytes in this document is
a conscious format bump — `BinaryEncodingVersion` (page bodies/records), the page-envelope
flags byte (additive page features), `LAYOUT_VERSION` (file layout), or a sub-structure version
byte (PIXM/PIXC/PIX1, per-record versions) — accompanied by an update to the golden constants
and a migration note here.

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
flush buffered tail → write superblocks if missing → `force(false)` data (write-ahead barrier) →
**`force(true)` revisions file** (the new revision's slot record must be durable before any
beacon advertises it; the 32-byte record itself is written through an O_SYNC channel and is
durable at write-return) → write SECONDARY beacon slot → write PRIMARY beacon slot — both through
an O_DSYNC channel (in-place overwrites; each write durable at return, which gives the
secondary-before-primary ordering AND makes the primary's write-return the commit acknowledge;
FUA on capable NVMe stacks). ONE explicit fsync per commit (the data tail write-ahead barrier);
no commit-end barrier exists. Validated empirically by `CrashRecoveryInjectionTest` (SIGKILL loop,
opt-in `-Dsirix.crash.run=true`).

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
checksum (verified on read; mismatch is a hard error, not a garbage offset). The write-only
uber-page copies an earlier draft of the layout kept at offsets 0/512 are gone.

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
- Deliberate exceptions (byte-shift-encoded, endian-independent): `compactDir` and in-blob
  column length prefixes use big-endian *byte layout* via explicit shifts; HOT discriminative
  keys use BE loads for lexicographic `compareUnsigned` — both are defined byte sequences, not
  host-order dependent.
- The superblock's endianness check remains as a fail-fast guard for headers written by
  pre-pin dev builds on BE hosts (none exist in practice).
- The legacy big-endian FILE backend is removed; `StorageType.FILE` fails fast.

## 2. Pages

Every page serializes as `[pageKind u8][binaryVersion u8][flags u8][body]`
(`PageKind.writeVersionAndFlags` / `readVersionAndFlags`). The flags byte is reserved
extension space for **every** page kind — all bits zero in V0, and a reader rejects nonzero
flags as "written by a newer version" instead of misparsing. Kind ids: 1 KVLP, 2 NAME,
3 UBER, 4 INDIRECT, 5 REVISION_ROOT, 6 PATH_SUMMARY, 8 CAS, 9 OVERFLOW, 10 PATH, 11 DEWEYID,
12 HOT_LEAF, 13 HOT_INDIRECT, 14 BITMAP_CHUNK, 15 VECTOR, 16 PROJECTION, 17 VALID_TIME
(7 retired/reserved).

**Format evolution mechanism (decided):** the unit of node-record/page-body evolution is a
`BinaryEncodingVersion` bump — every node/page (de)serializer receives the
`ResourceConfiguration`, which carries the resource's persisted `binaryEncoding`, and the
per-page version byte fails fast on unknown values. Node kinds that need to evolve
independently of the global version carry a per-record version byte (the VECTOR_NODE /
VECTOR_INDEX_METADATA pattern); sub-structures with their own magic carry their own version
byte (PIXC/PIX1, PIXM). Enum-typed bytes on disk are always explicit stable ids with
fail-fast lookups, never ordinals.

- **UberPage** body: `[i32 revisionCount]` — 7 bytes total incl. envelope, no checksum (§5.3).
- **RevisionRootPage**: delegate refs + revision, maxNodeKeys, commit timestamp/message, user.
- **KeyValueLeafPage** (the data page): 1024 implicit-keyed slots
  (`nodeKey = recordPageKey << 10 | slot`), 160-byte header+slot-bitmap, then a
  smallest-of-three body codec (`ZeroRunByteCodec` 0 / `ByteRunCodec` 2 / `SirixLZ77Codec` 3 —
  an LZ4-block-format clone, little-endian) over either the offset-table-template dedup layout
  (≤255 templates/page, 1-byte slot ids, hash/value/nameKey elision bitmaps, predictor-coded
  parentKey column, pathNodeKey dictionary) or the inline fallback; then PAX regions
  (NumberRegion = frame-of-reference + bit-packing + zone maps; String/Boolean/PathNodeKey
  regions), overflow pointers, optional FSST symbol table.
- **Node records**: structural keys as zigzag varint deltas against the own node key
  (`DeltaVarIntCodec`), varint revisions, fixed 8-byte rolling hash (elided page-wide when all
  zero), typed number payloads (Double/Float fixed, Int/Long zigzag varint).

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
revision records** (little-endian); **legacy FILE backend removed** (`StorageType.FILE` fails
fast — it wrote an incompatible layout under the same version); u8 fragment-count guards;
BITMAP_CHUNK honors its version byte; PATH_SUMMARY writes its delegate byte via the shared
helper; PageReference hashes as `[u8 flag][8 B]` instead of `[i32 len][8 B]`; the `+8`
first-offset quirk and the 256-byte RevisionRootPage alignment are gone (8-byte alignment for
all data pages, data starts exactly at `DATA_REGION_START`).

Done since the audit (see `docs/BINARY_ENCODING_FUTURE_PROOFING_AUDIT.md` for the full list):
**full little-endian pin** (was item 1 — all scalar IO via `io.sirix.node.LE`); **reserved
flags byte in every page envelope** (additive changes no longer force a global version bump);
**revisions record stride persisted in the superblock** and validated at open; **stable id
bytes replace every enum-ordinal on disk** (HOT node/layout types, BitmapChunkPage index
type); **fail-fast unknown node-kind/page-flags/version errors**; **pure-Java LZ4 block
decoder** (`JavaLz4BlockDecoder` — LZ4-bodied pages and FFILz4-pipeline resources are readable
without `liblz4`; writes fall back to stored-uncompressed frames); **hash-function identity
persisted + validated** (`ressetting.obj` `hashFunction` = "XX3"); **config field validation
throws in production** (was `assert`); **PIXC/PIX1 carry version bytes**; **DeweyID framing
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
