# SirixDB On-Disk Format (V0)

Status: **pre-freeze**. We are at `BinaryEncodingVersion.V0` (pages) and superblock
`LAYOUT_VERSION = 0` (files) with no external users — breaking changes are still free and there
is exactly ONE format: V0. This document is the contract for what V0 *is* (§1–§4, verified
against the code) and the ranked checklist of what must be decided **before** the format
freezes (§5). Once V0 ships to users, every change here costs a migration.

## 1. Files

A resource directory contains:

| File | Purpose |
|---|---|
| `data/sirix.data` | Append-only page store + dual uber-page beacons |
| `data/sirix.revisions` | Fixed-slot revision index: 32-byte records (`IOStorage.revisionsFileOffset`) |
| resource settings JSON | Format identity: `binaryVersion`, byte-pipeline classes, `storageType`, `hashAlgorithm`, `verifyChecksumsOnRead` |

Neither binary file carries a magic number, version, or endianness marker — identity lives only
in the JSON (§5.6).

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
0      : SUPERBLOCK (64 B, role = revisions)
64     : reserved (sparse zeros) up to 4096
4096 + 32*revision : [u64 dataFileOffsetOfRevisionRootPage][u64 epochMillis]
                     [u64 XXH3 of the previous 16 bytes][u64 reserved]   (little-endian)
```

This file is **load-bearing**: the serialized UberPage holds only `revisionCount`, so every
RevisionRootPage lookup goes through a slot here — which is why every record now carries a
checksum (verified on read; mismatch is a hard error, not a garbage offset). The write-only
uber-page copies an earlier draft of the layout kept at offsets 0/512 are gone.

### Endianness

- Superblock, beacon XXH3 trailers, and revision records: pinned **little-endian**.
- Page-record length prefixes and `BytesOut` payload primitives: platform order — the
  superblock's endianness check makes a foreign-endian open fail fast instead of misread
  (full LE pinning is checklist item 1).
- Inside page payloads: `compactDir` and in-blob column length prefixes big-endian; the three
  body codecs and all PAX regions pinned little-endian.
- The legacy big-endian FILE backend is removed; `StorageType.FILE` fails fast.

## 2. Pages

Every page serializes as `[pageKind u8][binaryVersion u8][body]`. Kind ids: 1 KVLP, 2 NAME,
3 UBER, 4 INDIRECT, 5 REVISION_ROOT, 6 PATH_SUMMARY, 8 CAS, 9 OVERFLOW, 10 PATH, 11 DEWEYID,
12 HOT_LEAF, 13 HOT_INDIRECT, 14 BITMAP_CHUNK, 15 VECTOR, 16 PROJECTION (7 retired/reserved).

- **UberPage** body: `[i32 revisionCount]` — 6 bytes total, no checksum (§5.3).
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
  slots carry an XXH3 trailer, and every revision record embeds an XXH3 of its offset+timestamp.
- **Still not covered**: RevisionRootPages read via `readRevisionRootPage` (no parent
  reference on that path) and record length prefixes.

## 4. Storage backends

`FILE_CHANNEL` (default) and `MEMORY_MAPPED` share one on-disk format. The legacy `FILE`
backend is removed (`StorageType.FILE` throws with a pointer to FILE_CHANNEL). `IO_URING`/`S3`
resolve enterprise providers via SPI and fail fast when absent.

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

Remaining before freeze:

1. **Full endianness pin** (SEV-2): the superblock's endianness check gates foreign-endian
   hosts (fail-fast), and all NEW structures (superblock, beacon trailers, revision records)
   are pinned little-endian — but page-record length prefixes and `BytesOut` payload
   primitives are still platform-order, and the KVLP body mixes BE `compactDir` with LE
   codecs/PAX. Pinning everything LE makes files portable instead of merely mismatch-safe.
2. **Block-aligned page records** (SEV-2, only if O_DIRECT is on the roadmap): byte-granular
   `[u32 len][payload]` records with 8-byte alignment are hostile to direct I/O and fixed-frame
   buffer managers. Decide: pad data pages to a block multiple, or commit to mmap/pread.
3. **Per-block string dictionaries / wider FSST** (compression): strings dominate the residual
   size (generic LZ4 buys only −16% for +7% read latency); slots into the existing PAX-region
   + structuralFlags extension points without a format break.
4. **KVLP hygiene** (SEV-3): drop the 8 stale runtime header bytes + the duplicated
   pageKey/revision/indexType; fix the `writeEncodedBody` javadoc drift and the stale
   "verified after decompression" comments.
5. **Resource UUID** in the superblock's reserved field (plumbing from ResourceConfiguration).
6. **fsync-ORDER validation** (dm-flakey/trace-replay): the SIGKILL gate validates write order
   only — it never reorders or drops writes that the page cache already accepted, so a missing
   or misplaced fsync barrier would still pass it.
7. **Crash-recovery truncate path is not exercised by the read-only gate verifier**: the gate
   only reopens readers, so the commit-lock-file truncation (`truncateTo`) never runs — extend
   the gate to reopen a writer per iteration.
