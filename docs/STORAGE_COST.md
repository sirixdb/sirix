# Per-Revision Storage Cost — Decomposition & Reduction Plan

**TL;DR.** A 10k-commit history where each commit changes one number field costs **15.94 MB
(1,671 B/commit)** for an ~8-byte logical change. We instrumented the write path and walked the
data file structurally: the 1.6 KB decomposes into a **~35% leaf-fragment cost (most of it fixed
per-fragment framing, not data), ~19% revision root, ~19% unchanged-but-rewritten index root
pages, ~16% node-history index, ~9% diff files, ~2% revision record**. Roughly **60–70% is
removable** without changing the COW design — V0 is unreleased, so the on-disk format may still
change. A realistic post-fix figure is **~400–600 B/commit** (default features) and
**~250–300 B/commit** (lean profile), against a naive Postgres trigger-history row of
~100–200 B.

Workload: `{"counter":N,"label":…,"tags":[a,b,c]}` (10 JSON nodes), one `setNumberValue` +
`commit()` per revision. Defaults: `SLIDING_SNAPSHOT` (window K=3), `ROLLING` hashes,
path summary, child counts, node history (`RECORD_TO_REVISIONS`), `storeDiffs`,
`sirix.compression=none` (inner page codecs active), `FILE_CHANNEL` storage.

## How it was measured

Two independent instruments that must (and do) agree:

1. **Structural data-file walk** — `bundles/sirix-core/src/test/java/io/sirix/bench/StorageCostBenchMain.java`
   builds N=1,000 (and 10,000) single-field-update commits, then walks `sirix.data` from
   `DATA_REGION_START` (12,288): every record is `[8-byte-align pad][u32 len][payload]`. Each
   payload's V0 header is decoded (PageKind id; for `KeyValueLeafPage` also index type, slot
   count, body-codec length, and the section split prefix/body/tail). Records are attributed to
   commits via the revision-root offsets in `sirix.revisions`. Accounting closes exactly:
   `12,288 + Σgross == file size` (delta = 0).
2. **Writer-side profiler** — the existing `io.sirix.io.file.StorageProfile`
   (`-Dsirix.storage.profile=true`, hooked in `FileChannelWriter`) records payload bytes per page
   class at write time. Totals match the walk byte-for-byte (e.g. RevisionRootPage 308,236 B in
   both). `-Dsirix.pageSectionDiag=true` additionally splits KVLP payloads into
   headerBitmap/encodedBody/regionTable/overlong/fsst.

Run matrix: default, `storeDiffs=false`, `hashKind=NONE`, `buildPathSummary=false`,
`storeNodeHistory=false`, `versioning=FULL`, all-minimal — each via `-Dbench.*` knobs (see the
bench javadoc for the exact `javac`/`java` invocations).

## The table: per-commit average bytes by page kind

Steady state (commits 501–1,000 of N=1,000; N=10,000 is within 0.5% — column “@10k” shown where
it differs). “gross” = payload + 4-byte length header + alignment pad.

| component | pages/commit | B/commit (default) | share | composition (per page) |
|---|---:|---:|---:|---|
| KeyValueLeafPage[DOCUMENT] | 1 | **586.8** | 35.3% | prefix 187 / body 147 / PAX-tail 244 / len+pad 8. 2-of-3 commits: 3-slot fragment (gross 528); every 3rd: full 8-slot snapshot (gross 704) — sliding window K=3 |
| RevisionRootPage | 1 | **312.0** | 18.8% | 216 B delegate (10 refs, each ~18 B = u64 key + flag + 8-B XXH3; + 4 × 12-B fragment keys) + 90 B fixed fields (≈49 B is the User: name + UUID **as a 36-char string**, every revision) |
| KeyValueLeafPage[RECORD_TO_REVISIONS] | 1 | **265.6** (272 @10k) | 16.0% | prefix 186 / body 58 (65 @10k — the delta-coded revision list grows slowly) / tail 14 |
| diff file (`update-operations/diffFromRevXtoRevY.json`) | 1 file | **154.7** | 9.3% | logical bytes; **physical = 4,096 B/commit** (one 4 KiB FS block per file!) |
| NamePage | 1 | **136.0** | 8.2% | **unchanged every commit** — rewritten anyway (incl. Roaring live-key bitmaps) |
| PathSummaryPage | 1 | **64.0** | 3.8% | unchanged, rewritten |
| DeweyIDPage | 1 | **40.0** | 2.4% | unchanged, rewritten |
| revisions-file record (`sirix.revisions`) | — | **32.0** | 1.9% | u64 offset + u64 timestamp + u64 XXH3 + u64 reserved |
| CASPage | 1 | **24.0** | 1.4% | unchanged, rewritten |
| PathPage | 1 | **24.0** | 1.4% | unchanged, rewritten |
| ProjectionIndexPage | 1 | **24.0** | 1.4% | unchanged, rewritten |
| UberPage beacons | (2 writes) | **0 growth** | — | two fixed 4 KiB slots overwritten in place (8 KiB/commit write amplification, no file growth) |
| IndirectPage | 0 | 0 | — | document/index tries are single-leaf here; see scaling note below |
| **TOTAL** | ~9 | **1,663** | 100% | N=10,000 measured: 15.94 MB total = 1,671 B/commit (matches the headline 15.9 MB) |

One-time costs: 12,288 B data-file header region (superblock + 2 beacon slots), 4,096 B
revisions-file superblock, bootstrap revision ≈ 2 KB.

### Feature attribution (steady-state B/commit, N=1,000)

| configuration | B/commit | Δ vs default |
|---|---:|---:|
| default (all on) | 1,663.1 | — |
| `storeDiffs=false` | 1,508.4 | **−154.7** |
| `hashKind=NONE` | 1,604.4 | **−58.7** (doc-fragment body 147→90: stored node hashes) |
| `buildPathSummary=false` | 1,573.1 | **−90.0** (PathSummaryPage 64 + smaller rev-root/diffs) |
| `storeNodeHistory=false` | 1,357.5 | **−305.6** (RECORD_TO_REVISIONS fragment 265.6 + rev-root −40) |
| `versioning=FULL` | 1,729.7 | +66.6 (full 8-slot page every commit) |
| all-minimal | 1,072.1 | **−591.0** |

Even all-minimal still pays 1,072 B for an 8-byte change — the remaining cost is framing, not
features: doc fragment 528 (of which **187 fixed prefix + 224 PAX tail around 26 B of heap
data**), rev root 264, unchanged singletons 264, revision record 32.

## Analysis of the dominant terms

### 1. KeyValueLeafPage fragments (DOCUMENT 587 + RECORD_TO_REVISIONS 266 = 51% combined)

V0 wire format per fragment (from `PageKind.KEYVALUELEAFPAGE.serializePage`):
`[kind 1][ver 1][varlong recordPageKey][i32 revision][u8 indexType]` +
**160 B raw header+bitmap** (`PageLayout.DISK_HEADER_BITMAP_SIZE`: 32-B header + full 128-B
slot bitmap for 1,024 slots) + `[i32 populated][i32 heapSize][u8 templateCount][u8 structFlags]
[i32 poolBytes][i32 bodyLen][u8 codec]` + compressed body + **PAX region table** + overlong
bitmap + FSST table.

For a 1-slot fragment (26 B of record data, `hashKind=NONE`): payload = 449 B =
**187 B fixed prefix + 38 B body + 224 B tail**. Categories:

- **(c) waste — duplicated fields:** `recordPageKey`/`revision`/`indexType` are written in the
  prefix AND live at offsets 0/8/22 inside the 160-B header (~13 B × 2 fragments/commit).
- **(b) compressible — the 128-B slot bitmap is written raw** even when 1 of 1,024 bits is set.
  The body goes through ZeroRun/ByteRun/LZ77 codecs, the header+bitmap deliberately doesn't
  ("on-disk format, never changes"). A sparse fragment form (varint slot-id list when
  `populated ≤ ~64`) shrinks 160→~10 B.
- **(b) compressible — PAX region-table tail (224–244 B on doc fragments).** The columnar
  regions (NumberRegion/StringRegion/ObjectKeyNameKey/PathNodeKey + 5-B framing each) are
  rebuilt per fragment to accelerate scans; value elision moves some heap bytes into them, but
  on a 1–3-slot fragment the fixed region scaffolding is ~4–9× the actual record data. The
  r2r index shows the floor: no regions → tail = 14 B.
- **(a) floor:** the changed record itself (26 B) + slot directory entry (4 B) + codec framing.
- The sliding snapshot behaves as designed: 2-of-3 commits write only changed slots (3 slots
  under ROLLING because ancestor hashes change in the same page; **1 slot** under
  `hashKind=NONE`), every 3rd commit re-emits all 8 live slots so reads touch ≤ K fragments.
  A minimal 1-slot fragment **would be ~45–60 B** (varint pageKey/rev/idx + slot id + record
  + codec byte); we write 449–528 B today — **~9×**.

### 2. RevisionRootPage (312 B, 19%)

- **(a) floor:** one new revision root per commit is the COW design's anchor.
- **(b) compressible:** 10 delegate references à ~18 B (8-B key + 8-B XXH3 + flags) — refs to
  *unchanged* pages re-serialize the same key+hash every revision; 4 × 12-B fragment keys
  (i32 revision + i64 key, fixed-width); 3 × 8-B maxNodeKey counters; 8-B ms timestamp.
- **(c) waste:** the **User is serialized per revision as UTF-8 name + UUID.toString()**
  (36-char string instead of 16 raw bytes) ≈ 49 B/commit for the same "admin" user, 10k times.

### 3. Unchanged singleton index pages (312 B/commit total, 19% — pure waste (c))

`StorageEngineWriterFactory` (lines ~175–215) pre-stages deep copies of NamePage, CASPage,
PathPage, ProjectionIndexPage, DeweyIDPage (and PathSummaryPage) into the TransactionIntentLog
on every transaction (re-)instantiation — needed for mutation isolation — and commit writes
**everything in the TIL**, dirty or not. The COW design already supports pointing the new
revision root at the previous offsets of untouched subtrees; these six pages just never take
that path. NamePage is the worst (136 B: maxNodeKeys/maxHotPageKeys maps + per-dictionary
Roaring bitmaps re-serialized verbatim).

### 4. Diff files (155 B logical, 9% — but 4,096 B physical, category (c) at the FS level)

One `diffFromRev{N-1}toRev{N}.json` file per commit
(`{"database":…,"resource":…,"diffs":[{"update":{"nodeKey":2,"path":"/counter",…}}]}`).
At 10k commits: 1.58 MB logical but **40.96 MB in 4 KiB FS blocks + 10k inodes** — the diff
*directory* physically outweighs the 14.8 MB data file. (The envelope also repeats
database/resource/revisions per file, ~100 B of its 155 B.)

### 5. RECORD_TO_REVISIONS (266 B, 16%)

Per-feature cost of `storeNodeHistory`: appends the revision to the touched node's
`RevisionReferencesNode` and rewrites that fragment each commit — ~12 B of logical information
wrapped in the same 186-B KVLP prefix. The delta-coded list grows mildly (body 58 B@1k →
65 B@10k; no quadratic blow-up observed). Cheapest fix is the shared sparse-fragment format
(proposal 2); a dedicated revision-log encoding could go further.

### Scaling caveat (not exercised by this workload)

The 10-node document keeps every trie at height 0 — **zero IndirectPages per commit**. On large
resources each commit additionally rewrites 1–3 IndirectPages per touched trie level, and an
IndirectPage re-serializes **all** populated references (up to 1,024 × ~18 B ≈ 18 KB) even when
one child changed: the same unchanged-sibling pathology as items 1/3, one level up. Page
fragments only version KVLPs, not IndirectPages. Any "big resource, tiny commit" benchmark
should re-run this walker before drawing conclusions.

## Ranked reduction proposals

Estimated against the default 1,663 B/commit; all on-disk format changes are legal pre-release
(V0 unreleased) but flagged.

| # | proposal | est. saving | risk | format change |
|---|---|---:|---|---|
| 1 | **Skip clean singleton pages at commit** — dirty-flag (or cheap content check) on the 6 TIL pre-staged pages; revision root keeps the prior reference (offset+hash) when clean. | **−312 B (−19%)** | Low–Med: TIL bookkeeping + making the retained reference carry the old offset/hash; isolation copies stay as-is. No read-path change. | no |
| 2 | **Sparse small-fragment KVLP form (V1 bit in the version byte)** — when `populated ≤ ~64`: varint slot-id list instead of the 128-B bitmap; drop the duplicated rpk/rev/idxType prefix; flags byte to elide empty region/overlong/FSST sections; skip PAX regions under a slot threshold (rebuild on read or on the next full snapshot). Doc fragment 587→~180, r2r 266→~90. | **−580 B (−35%)** | Med: second decode path; scan code that expects regions on every fragment must fall back to heap decode for small fragments. | **yes** |
| 3 | **Consolidate diffs into one append-only log** (or a blob in the revisions file / revision root), offset-indexed per revision; drop the per-file JSON envelope. | −0 logical / **−3.9 KB physical per commit** (10k commits: 41 MB→~0.6 MB on disk, no inode churn) | Low–Med: `DiffHandler`/REST read path needs an index lookup instead of `open(file)`; concurrent readers of the tail. | no (sidecar format) |
| 4 | **RevisionRootPage trim** — User as 16-B binary UUID + interned name id (−33), varint counters/revision/timestamp-delta (−20), omit hash bytes for references that also appear as fragment-key heads or are clean (−16–40). | **−70–90 B (−5%)** | Low | **yes** |
| 5 | **Defaults audit** — `storeNodeHistory` (−306) and `storeDiffs` (−155) are both ON by default; for an "every commit is one small update" profile these are 28% of bytes. Make them opt-in per resource profile, or document the lean profile. | up to −460 B (config-only) | Low (product decision — features disappear) | no |
| 6 | **Hash storage** — `hashKind=NONE` saves 59 B; alternatively store rolling hashes column-elided like values (they're already zero-elided when absent). | −30–59 B | Low | partial |
| 7 | (Scaling follow-up) **Indirect-page reference deltas** — version IndirectPages like record pages (write only changed refs + bitmap), bounding big-resource commits. | n/a here; ~KBs/commit on large tries | High | yes |

Combined 1+2+3+4 keeps every feature on: **~700 B/commit logical, ~750 B physical** (vs 1,663
logical / ~5,604 physical today — physical = pages 1,476 + revisions 32 + one 4 KiB diff block).
The lean profile (no diffs/node-history) lands ~**250–300 B/commit**.

## Honest competitive framing

A Postgres trigger-history row for the same change — `(id, ts, field, old, new)` — is ~28-B tuple
header + ~50–100 B data ≈ **100–200 B heap**, plus WAL (~1.5–2× per row), plus a
`(id, ts)` b-tree entry (~40–60 B), plus 8 KB-page write amplification and VACUUM debt. Call it
**~250–400 B of real disk traffic per change** in steady state.

SirixDB today is **1,663 B logical / ~5.6 KB physical** per tiny commit — 4–14× worse, which is
not defensible for a headline metric. After proposals 1–4 it is **~700 B with full time-travel
machinery** (whole-document snapshots at every revision, O(K)-bounded point lookups in any
revision, structural diffs, per-node history) — i.e. ~2–3× a bare history row while answering
queries the history table fundamentally cannot (subtree reconstruction at any revision without
replay, temporal axes, diffs without re-diffing). With the lean profile it is at parity. What a
history table never pays, we must keep paying: one revision root + one leaf fragment per commit
— that floor is ~120–250 B and is the price of O(1)-addressable snapshots rather than
O(history) replay.

## Reproducing

```
javac --enable-preview --release 25 --add-modules jdk.incubator.vector \
  -cp "$(cat /tmp/sirix-test-cp.txt)" -d /tmp/wave5-a/classes \
  bundles/sirix-core/src/test/java/io/sirix/bench/StorageCostBenchMain.java
java --enable-preview --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
  -Xms1g -Xmx4g [-Dbench.label=… -Dbench.storeDiffs=… -Dbench.hashKind=… \
  -Dbench.buildPathSummary=… -Dbench.storeNodeHistory=… -Dbench.versioning=… \
  -Dsirix.pageSectionDiag=true -Dsirix.storage.profile=true] \
  -cp "/tmp/wave5-a/classes:$(cat /tmp/sirix-test-cp.txt)" \
  io.sirix.bench.StorageCostBenchMain 1000
```

Measured 2026-06-11 on the pre-release working tree (V0 format, `sirix.compression=none`
default). Raw run logs: `/tmp/wave5-a/runs/*.txt`.
