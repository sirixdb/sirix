# SirixDB On-Disk Format (V0)

Status: **pre-freeze**. We are at `BinaryEncodingVersion.V0` with no external users — breaking
changes are still free. This document is the contract for what V0 *is* (§1–§4, verified against
the code) and the ranked checklist of what must be decided **before** the format freezes (§5).
Once V0 ships to users, every change here costs a migration.

## 1. Files

A resource directory contains:

| File | Purpose |
|---|---|
| `data/sirix.data` | Append-only page store + dual uber-page beacons |
| `data/sirix.revisions` | Fixed-slot revision index: 16-byte records (`IOStorage.revisionsFileOffset`) |
| resource settings JSON | Format identity: `binaryVersion`, byte-pipeline classes, `storageType`, `hashAlgorithm`, `verifyChecksumsOnRead` |

Neither binary file carries a magic number, version, or endianness marker — identity lives only
in the JSON (§5.6).

### sirix.data (FILE_CHANNEL, the default)

```
0     : PRIMARY uber beacon  [u32 len][UberPage payload][zero pad]  — 512-byte slot
512   : SECONDARY uber beacon (identical copy)
1024  : FIRST_BEACON (= UBER_PAGE_BYTE_ALIGN << 1)
1032+ : page records, append-only: [u32 len][payload]
        RevisionRootPages aligned to 256, all other pages to 8
```

Page keys (`PageReference.getKey()`) are absolute byte offsets of the record's length prefix.

**Commit protocol** (`FileChannelWriter.writeUberPageReference`):
flush buffered tail → `force(false)` data (write-ahead barrier) → **`force(true)` revisions file**
(the new revision's slot record must be durable before any beacon advertises it) → write+fsync
SECONDARY beacon → write PRIMARY beacon → commit-end `forceAll()`.
Validated empirically by `CrashRecoveryInjectionTest` (SIGKILL loop, opt-in
`-Dsirix.crash.run=true`).

### sirix.revisions

```
0    : copy of latest UberPage payload   } write-only — nothing reads these (§5.5)
512  : second copy                        }
1024 + 16*revision : [u64 dataFileOffsetOfRevisionRootPage][u64 epochMillis]
```

This file is **load-bearing**: the serialized UberPage holds only `revisionCount`, so every
RevisionRootPage lookup goes through a slot here.

### Endianness (current state — see §5.2)

- FILE_CHANNEL + MEMORY_MAPPED: native-order frames/beacons/records (interchangeable files;
  MM writes through FileChannelWriter).
- FILE (legacy): big-endian, secondary beacon at offset 104, first record at 1024 — an
  **incompatible layout under the same nominal V0**.
- Inside page payloads: `compactDir` and in-blob column length prefixes big-endian; the three
  body codecs and all PAX regions pinned little-endian; `BytesOut` primitives native.

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
- **Not covered**: the uber beacons (recovery "validation" is essentially kind+version byte +
  bounded length), every `sirix.revisions` record, RevisionRootPages read via
  `readRevisionRootPage`, record length prefixes (§5.3).

## 4. Storage backends

`FILE_CHANNEL` (default) and `MEMORY_MAPPED` share one on-disk format. `FILE` is a legacy,
big-endian, layout-divergent backend (§5.2). `IO_URING`/`S3` resolve enterprise providers via
SPI and fail fast when absent.

## 5. PRE-FREEZE CHECKLIST (ranked; decide before first user data)

Fixed already in V0 (no action): templateCount 256-wrap (capped at 255), deterministic
revisions-file slots + truncation, ordered dual-copy beacon writes + write-ahead barriers
(data **and** revisions) on both FILE_CHANNEL and FILE, revisions-slot formula centralized in
`IOStorage.revisionsFileOffset` (all readers/writers).

1. **Superblock** (SEV-1): add a 64-byte header at offset 0 of BOTH files — magic, format
   version, flags, endianness, page-size parameters, resource UUID, storage-type fingerprint,
   CRC. Removes the "opened with the wrong backend/version" corruption class and creates the
   file-level extension space V0 currently lacks. Requires shifting the beacon block — do it
   together with (2).
2. **Beacon placement** (SEV-1): both uber copies currently share one 4 KiB filesystem block
   (offsets 0/512) — block-granular tearing can kill both. Place them ≥4 KiB apart (e.g. 4096
   and 8192 after the superblock; FIRST_BEACON → 12288). Drop or repurpose the write-only uber
   copies in `sirix.revisions`.
3. **Checksums for the roots** (SEV-0 class): per-beacon XXH3/CRC + monotone commit counter
   (recovery = pick newest valid); add a checksum (or the RRP hash) to each 16-byte revisions
   record (→ 24 or 32 bytes/slot).
4. **Endianness pin** (SEV-2): declare little-endian for every frame/header/record and replace
   native-order accessors with explicit LE layouts (zero cost on x86/ARM; makes files portable
   and the spec writable). Unify the BE `compactDir` remnants at the same time.
5. **FILE backend** (SEV-0 class): delete or quarantine. It writes an incompatible layout under
   the same version (BE, beacon at 104, no uber-size guard, pipeline bugs in
   `readRevisionRootPage`), and nothing on disk detects the mismatch.
6. **u8 wire truncations** (SEV-1): guard fragment counts and similar `(byte) size()` casts
   (wrap silently past 255 today).
7. **Block-aligned page records** (SEV-2, only if O_DIRECT is on the roadmap): byte-granular
   `[u32 len][payload]` records with 8-byte alignment are hostile to direct I/O and fixed-frame
   buffer managers. Decide now: either pad data pages to a block multiple or commit to
   mmap/pread forever. (The 256-byte RevisionRootPage alignment currently buys nothing — the
   full offset is stored anyway — and wastes ~128 B/revision; drop it or make it block-sized.)
8. **Hygiene** (SEV-3, cheap): BITMAP_CHUNK must *honor* its version byte (it ignores it);
   PATH_SUMMARY should write its delegate-type byte via the shared helper; KVLP persists 8
   stale runtime header bytes + duplicates pageKey/revision/indexType; PageReference hashes
   are `[i32 len][8 B]` where 1 flag byte suffices; the first-record `+8` offset quirk (1032)
   should be made intentional or fixed; fix the stale "verified after decompression" comments
   and the `writeEncodedBody` javadoc drift.
