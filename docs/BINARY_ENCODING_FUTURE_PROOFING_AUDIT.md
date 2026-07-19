# Binary Encoding Future-Proofing Audit

> **Status update (2026-07): all fixable findings below are FIXED on this branch.**
> SEV-1 items 1–3 (stable ids, UTF-8 pin, fail-fast kind lookup), item 4's evolution
> mechanism (documented in `DISK_FORMAT.md` §2), SEV-2 items 5–8 (LE pin, pure-Java LZ4
> decode fallback, long counts, hash-identity persistence), and SEV-3 items 9–13 (golden
> tests in `io.sirix.format.GoldenFormatTest`, config validation, reserved envelope flags
> byte + superblock stride, PIXC/PIX1 versions, DeweyID/offset-table guards) are done; item
> 14 (Roaring format coupling) is pinned by a golden test. Still open by design: superblock
> resource-UUID plumbing and whole-page composite golden fixtures — tracked in
> `DISK_FORMAT.md` §5. The findings below are retained as the audit record.

Scope: the complete persisted format — file headers, page envelope, page bodies, node records,
secondary indexes, codecs, and configuration — audited for evolvability (can V1 be introduced
without breaking V0 files?), portability (endianness, native libraries, JVM/platform behavior),
and fail-fast behavior on unknown/foreign input. All references verified against this tree.

Overall verdict: **the versioning skeleton is in place and mostly sound** (superblocks, per-page
kind + version bytes, explicit stable kind ids, checksummed roots, fail-fast on unknown
versions), but a handful of encodings bypass it — two enum-ordinal encodings, a
platform-charset bug, native-endian scalars, and unversioned node-record layouts — and there is
no test harness that pins the format. Ranked findings below; SEV-1 items should be fixed before
the V0 freeze (`docs/DISK_FORMAT.md` §5).

## What is already future-proof (verified)

- **File identity**: 64-byte superblock in both `sirix.data` and `sirix.revisions` — magic,
  layout version, file role, endianness check, XXH3 checksum — validated at storage open in
  both backends (`io/Superblock.java`, `FileChannelStorage`, `MMStorage`). Unknown layout
  version, foreign endianness, wrong role, and torn header all fail fast with actionable
  messages.
- **Page envelope**: every page serializes as `[pageKind u8][binaryVersion u8][body]`. All 16
  active kinds write and validate the version byte via `BinaryEncodingVersion.fromByte`, which
  throws on an unknown value; `PageKind.getKind(byte)` throws on an unknown kind id
  (`PageKind.java:5162-5168`). No silent misparse at the envelope level.
- **Stable id registries**: page-kind ids are explicit constructor-assigned bytes with a
  retired-id gap (7), never ordinals (`PageKind.java:93-4196`). Node-kind ids are likewise
  explicit and sparse (`NodeKind.java`), `IndexType` has explicit id bytes, and the KVLP
  offset-table template pool self-describes `fieldCount` alongside `kindId` precisely so a
  future kind-id reuse doesn't need a migration (`OffsetTableTemplatePool.java:29-35`).
- **Body codec identification**: the KVLP body carries an explicit per-page codec id byte
  (0 = ZeroRun RLE, 1 = LZ4, 2 = ByteRun, 3 = SirixLZ77); the writer runs a smallest-wins
  bake-off and records the winner, the reader dispatches on the id. FSST presence is a header
  flag bit with the symbol table embedded in the page.
- **Integrity chain**: page XXH3 in the parent `PageReference` (Merkle-style), checksummed
  superblocks, beacon slots with XXH3 trailers, checksummed 32-byte revision records, and the
  RevisionRootPage covered via the revision record's 4th field — with a legacy discriminator
  (`hash == 0`) that keeps beta1 resources readable.
- **No JVM-unstable serialization**: no `ObjectOutputStream` in persisted data, no
  `String.hashCode` on disk, node hashes are pinned XXH3, map-backed metadata serializes in
  deterministic indexed/bitmap order, varints are byte-oriented (endian-free), DeweyIDs use a
  deterministic bit-prefix code.
- **Graceful degradation where limits bind**: template pool caps at 255 and falls back to the
  inline layout; fragment count > 255 throws at write time (not read time); 3-byte
  `dataLength` has an explicit write-side guard; LZ4 decompression is size-bounded against
  allocation bombs.
- **Newer structures already carry their own versions**: VECTOR_NODE / VECTOR_INDEX_METADATA
  write and check a per-record version byte; projection-index metadata has magic `PIXM` plus a
  version byte and degrades to rebuild (correct for a derived index).

## SEV-1 — encodings that bypass the versioning discipline (fix before freeze)

1. **Enum-ordinal on-disk encodings.** `HOTIndirectPage.NodeType` and `LayoutType` are
   persisted as `.ordinal()` and read via `values()[id]` (`PageKind.java:3956-3957`,
   `:3838-3843`); `BitmapChunkPage` persists `indexType.ordinal()` and reads
   `IndexType.values()[ordinal]` (`BitmapChunkPage.java:449`, `:503`) — bypassing the stable
   `IndexType.getID()` byte used everywhere else (including HOT leaf pages). Reordering or
   inserting an enum constant silently corrupts existing pages, and an out-of-range id throws
   `ArrayIndexOutOfBoundsException` instead of a versioned error. Fix: write stable id bytes
   with a lookup that throws a named error, exactly like `PageKind`/`IndexType` already do.
2. **Platform-default charset in a persisted path.** The NAMERB (name red-black index node)
   serializer calls `getBytes()` with no charset (`NodeKind.java:865-871`) while its
   deserializer decodes explicit UTF-8 (`:846-847`). Under any JVM where the default charset
   is not UTF-8 (`-Dfile.encoding`, pre-JEP-400 runtimes), non-ASCII names corrupt on
   round-trip. This is the only charset-less `getBytes()` in the persisted store path. Fix:
   `getBytes(Constants.DEFAULT_ENCODING)`.
3. **Unknown node-kind id fails as NPE/AIOOBE, not a versioned error.**
   `NodeKind.getKind(byte)` indexes a 128-slot array with no null/range check
   (`NodeKind.java:2085-2087`), so a record written by a newer version (or a corrupt kind
   byte) dies with an opaque `NullPointerException` in `NodeSerializerImpl` or
   `ArrayIndexOutOfBoundsException` for ids ≥ 128 — while the flyweight path already throws a
   clean `IllegalArgumentException` (`FlyweightNodeFactory.java:90-91`). Fix: null/range-check
   with a "node kind N not supported by this version" error.
4. **Node-record layouts are unversioned and unskippable.** Core node kinds' layouts are
   implied solely by `(binaryVersion, nodeKind)`; `NodeFieldLayout` field counts are exact
   with no reserved slot, and no deserializer branches on `BinaryEncodingVersion` yet. Adding
   one field to one node type today means either a new kind id or a global V1 with full
   dispatch plumbing that doesn't exist. Acceptable pre-freeze, but the freeze decision should
   pick the evolution mechanism (per-record version byte like VECTOR_NODE, reserved
   flags/fields, or committed global-version dispatch) — silence is the only wrong option.

## SEV-2 — portability and hard ceilings (decide before freeze)

5. **Native-endian scalars** (already checklist item 1 in `DISK_FORMAT.md` §5, confirmed):
   `MemorySegmentBytesIn`/`GrowingMemorySegment` use unaligned *native-order* layouts, the
   KVLP 160-byte header+bitmap is bulk-copied raw, and beacon/page-record length prefixes are
   platform order — while LZ77/PAX/superblock/revision records are pinned LE and `compactDir`
   is BE. Three conventions coexist; the superblock endian check makes foreign-endian opens
   fail fast (mismatch-safe) but files are not portable. Pin everything LE before freeze.
6. **Native-LZ4 body pages hard-fail without `liblz4`.** Codec id 1 is only decodable when the
   FFI LZ4 probe succeeds (`PageKind.java:338-341` throws otherwise), and an outer FFILz4
   `ByteHandlerPipeline` has the same property. A resource written on an LZ4-capable host is
   unreadable on a host without the native library even though pure-Java codecs exist. Either
   ship a pure-Java LZ4-block decoder fallback (the wire format is documented and
   `SirixLZ77Codec` is already an LZ4-block clone — likely reusable as the decoder) or
   document the dependency as a format requirement.
7. **`int`-narrowed child/descendant counts.** Structural serializers cast
   `getChildCount()`/`getDescendantCount()` from `long` to `int` and decode 32-bit
   (`NodeKind.java:187,191` and peers), overflowing beyond ~2.1 B descendants — while another
   path writes the same quantity as a full `long` (`:2191`). Varints make widening cheap:
   encode the `long` now, before freeze, since the delta encoding means small values cost the
   same bytes.
8. **Hash-function identity is no longer validated.** The persisted `hashFunction` config
   field is read and explicitly skipped (`ResourceConfiguration.java:730-733`); XXH3 is
   hard-coded. Changing the node-hash function later would be undetectable — old resources
   would re-verify against the wrong function with no mismatch error. Re-persist (or fold into
   the binary version) the hash identity before freeze.

## SEV-3 — hardening gaps

9. **No golden-file format tests.** There are no checked-in binary fixtures or byte-exact
   serialization tests; round-trip tests pass even when the format silently changes. At freeze,
   add golden fixtures (a small committed resource + expected bytes/checksums per page kind) so
   any accidental format change fails CI.
10. **Config↔binary identity gap.** `ressetting.obj` is unchecksummed, unversioned JSON, yet it
    is the sole source of pipeline/compression identity; the superblock's 16 reserved UUID
    bytes are unused, so config and data file are not cross-linked (checklist item 5 covers the
    UUID; consider also checksumming the JSON). Field-order validation in
    `ResourceConfiguration.deserialize` uses `assert` (`:695` ff.), a no-op in production.
11. **No reserved bytes outside KVLP.** Only the KVLP header carries flags + 8 reserved bytes;
    UberPage/IndirectPage/RevisionRootPage/NamePage/etc. have none, and `RevisionRootPage`
    hard-codes 11 delegate references — any additive change is a global version bump. Likewise
    the 32-byte revision record has no remaining reserved field and its stride is not recorded
    in the superblock geometry.
12. **Unversioned magics.** `ProjectionIndexLeafCodec` (`PIXC`) and the `PIX1` presence-tail
    footer carry magic but no version byte — a future layout change needs a new magic rather
    than a version bump. Index-definition files (`indexes/<revision>.xml`) have no
    magic/version/checksum.
13. **Small unguarded limits.** Per-field offset-table entries are a single unchecked byte
    (`PageLayout.java:589,602`) — an offset > 255 silently truncates, held off only by the
    convention that the variable-length field is last; DeweyID delta framing caps at 255 bytes
    (`NodeSerializerImpl.java:53-69`); HOT keys/values cap at 64 KiB (u16). Add write-side
    guards that throw, mirroring the existing `dataLength`/fragment-count guards.
14. **Third-party format coupling.** NamePage and node-reference sets embed
    `Roaring64Bitmap.serialize` bytes — a documented stable format, but on-disk compatibility
    is now coupled to that library's format stability; pin the portable format version in a
    comment/test.

## Suggested order of work

Fix 1-3 now (small, mechanical, and each is a latent corruption or crash). Decide 4-8 as part
of the `DISK_FORMAT.md` §5 pre-freeze checklist — items 5 (endianness) and 6 (LZ4
portability) change bytes on disk and are free only while there are no deployed users. Item 9
(golden files) should land together with the freeze itself, as it is what makes the freeze
enforceable.
