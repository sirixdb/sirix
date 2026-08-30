# Row-Store Ranged Decode Plan — Chunked KVLP Body Format (V0+CHUNKED)

**Status**: Decision-grade design, pre-implementation. Synthesized from four source surveys (format+writer, readers, versioning, precedents+codecs), all claims anchored to file:line or to measurements on the real ClickBench corpus (`/tmp/claude-1000/cb/db-1m`, 1.357 GB, 105,185 DOCUMENT KVLPs, all 210,539 frames walked).
**Problem**: One slot read costs one whole-page decode — full blob decompress + O(populatedCount≈1019) expansion (`PageKind.deserializeSlottedPage`, `PageKind.java:247-1535`), ~1 ms per first-touch page (`SirixSortedScanExpr.java:88-92`). Q19-shape: 179 point winners ≈ 179 whole-page decodes.
**Non-negotiable finding that shapes everything**: the millisecond is NOT decompression (5.4 KB→26 KB LZ77 is ~10–20 µs); it is the O(N) post-decompress expansion — compactDir parse, `StructuralKeyColumnCodec.decodeAll`, elision reinjection, and the in-memory heap build for ~1019 slots. **Chunking the compressed bytes alone caps the win; the design below makes the expansion chunk-granular and the directory build metadata-only.**

---

## 1. DESIGN

### 1.1 Decisions (each final, with the reason)

| # | Decision | Choice | Reason |
|---|---|---|---|
| D1 | Chunking unit | Heap only; chunk boundaries in **entry space** (populated-bitmap rank order), slot-aligned, greedy fill to target C raw bytes | compactDir is length-only prefix sums; boundaries in slot space are undefined under holes (F+W corner 1). Heap carries the cross-record value redundancy (heap-only recompress ratio 0.215, i.e. 4.7× compressible) |
| D2 | Metadata placement | ALL non-heap blob sections (compactDir, templatePool, slotTemplateIds, zeroHashBitmap, parentKey column, pathNodeKey column, valueElision, nameKeyElision) move to a single **META section**, compressed whole, outside the chunks | Sections are page-global, entry-ordinal-indexed, and tiny (7.7 KB raw, ratio 0.186 → ~1.4 KB wire). Per-chunk slicing of elision cursors buys nothing and multiplies corner cases (F+W corner 3; versioning corner 2) |
| D3 | Chunk size | Target **C = 4 KiB raw heap bytes → K≈5 chunks/page** on this corpus; per-resource tunable, writer clamps K ≤ 128 | Measured: C=4096 → body ×1.158 → **file +6.8%**; C=8192 → +4.0% but 1.7× larger decode unit (~340 vs ~200 slots). At C=4 KiB the decode unit is ~5× smaller for +2.8 file points over C=8 KiB. C=2048 (+11%) and below lose ratio steeply (×1.254→×1.419) for shrinking returns since META parse becomes the floor. Framing overhead at K=5 is ~110 B = 0.9% of a 12.6 KB frame — noise vs ratio loss |
| D4 | Priming dictionary | **NONE.** Chunks compress independently | Measured: the 133 B template pool as dict recovers 2–4 points of a 16–42 point loss; a 4 KB self-dictionary loses to its own storage cost at C ≥ 4 KiB. No codec on the classpath has dictionary support bound (`SirixLZ77Codec` encoder snapshots from 0, `SirixLZ77Codec.java:254-258`; native decoder can't address before `outputOff`, `SirixLZ77NativeDecoder.java:171-181`; `JavaLz4BlockDecoder` explicitly rejects pre-buffer matches, :122-126). C-side + Java-fallback surgery for <3 points is a bad trade |
| D5 | Codec per chunk | Page-level sticky bake-off election exactly as today (`emitSmallestBody` rules, probe every 16th page), applied to every chunk; per-chunk **STORED** override when winner output ≥ raw | Preserves the pinnable determinism contract (probeInterval=1 golden tests); per-chunk election would multiply the non-determinism surface (F+W corner 5) |
| D6 | Checksum | **Two-level**: per-chunk + per-META XXH3-64 embedded in the tables (~48 B/page at K=5); parent `PageReference` whole-payload hash **unchanged** | Full-frame reads (the entire current I/O path) verify the parent hash exactly as today — zero behavior change. The embedded hashes make chunk contents verifiable at materialize time and make a future bounded-pread slot path possible without the `regionChunkEligible` decline-on-verify dead-end (`FileChannelReader.java:335-338`). Precedent: uber beacon `[len][payload][xxh3]` (`FileChannelWriter.java:691,727`) |
| D7 | FSST | Dict id hoisted into the chunked body prefix (`varLong fsstDictId`, 0 = none); no tail FSST marker on chunked pages | The tail position is why `FileChannelReader.readRegionsOnly` declines FSST resources today; hoisting fixes both that and per-chunk string decode (readers §C; versioning corner 9) |
| D8 | Version gating | Page-envelope **flags bit 0x01 = CHUNKED_BODY**. Envelope stays binaryVersion=0. Old readers throw "page written by a newer version" on any nonzero flag (`PageKind.readVersionAndFlags` :4940-4948) — a clean, designed fence. Pattern precedent: `readVersionAndFlagsAllowing` + HOTLeafPage `FLAG_OVERFLOW_PAGE_REFS` (:5004, :4271) | No resource-level break: the new reader keeps the V0 monolith path byte-for-byte untouched, forever. Mixed fragment chains are legal by construction (§4.2) |
| D9 | Lazy materialization granularity | Chunk-granular, into the page's own preallocated heap segment, behind a per-chunk materialized-bit gate; **directory build is META-only** (see 1.4) | Lets all five direct heap readers (`getSlot` :3114, the inlined locate `AbstractNodeReadOnlyTrx` ~:840, `FlyweightNodeFactory.createAndBind`, `VersioningType` slot copies, `ColumnarPageExtractor`) keep their zero-copy contract after a single acquire-load gate; heap addresses are stable because the segment is sized up front |
| D10 | Sequential predictors | `StructuralKeyColumnCodec` and pathNodeKey dict stay **bulk-decoded at META parse** into thread-local scratch; no per-chunk restart seeds | decodeAll over ≤1024 varint deltas ≈ 10–20 µs — three orders below the target win; per-slot decode is quadratic by design warning (PageKind.java ~:600). Restart seeds are format complexity purchasing microseconds |

### 1.2 Byte-level layout (chunked body, flags bit 0x01 set)

Everything outside the body — disk framing, `[pageKind][binaryVersion][flags]` envelope, `varLong recordPageKey / int revision / byte indexType`, the 160 B LE header+bitmap block, RegionTable, overlong entries — is **unchanged**. All new multi-byte fields use the same big-endian `Bytes` primitives as the existing body prefix ints.

```
int      populatedCount            // unchanged; cross-checked vs bitmap popcount (throws)
int      onDiskHeapSize            // unchanged
u8       templateCount             // unchanged; 0 = degenerate/inline
[u8      structuralFlags]          // iff templateCount > 0, unchanged bit meanings
int      templatePoolBytes         // unchanged (0 in degenerate)
varLong  fsstDictId                // NEW: 0 = none; replaces the tail FSST marker on chunked pages
int      bodyTotalLen              // NEW: bytes from end of this field to start of RegionTable — the
                                   //      compressedLen-equivalent O(1) total-skip for all 3 prefix parsers
--- META frame ---
int      metaRawLen
int      metaEncLen
u8       metaCodec                 // 0 ZeroRun | 2 ByteRun | 3 LZ77 | 4 STORED
u64      metaXxh3                  // over the metaEncLen stored bytes
--- chunk table ---
u8       chunkCount                // K, 0..128 (writer clamps; 0 legal for empty pages)
K × {
  u16    firstEntry                // 0-based rank in populated-bitmap order; contiguous, monotone
  u16    entryCount                // Σ over K == populatedCount (throws otherwise)
  int    rawLen                    // Σ over K == onDiskHeapSize (throws otherwise); int because a chunk
                                   //   can aggregate beyond u16 although one encoded slot allocation is capped at 512 B
  int    encLen
  u8     codec                     // 0|1|2|3|4 — 1 (LZ4) accepted on read, never written
  u64    xxh3                      // over the encLen stored bytes
}                                  // 21 B/chunk; K=5 → 106 B incl. count (0.9% of frame)
byte[metaEncLen]                   // META payload: compactDir ++ templatePool ++ slotTemplateIds ++
                                   //   [zeroHashBitmap] ++ [parentKeyColumn] ++ [pathNodeKeyColumn] ++
                                   //   [valueElision] ++ [nameKeyElision] — byte-identical section
                                   //   encodings to today's blob, same order, heap removed
K × byte[encLen_k]                 // chunk payloads: the exact heap byte ranges
                                   //   ([kindId][templateId][stripped data] per record; degenerate:
                                   //   verbatim inline records incl. offset tables), split at entry
                                   //   boundaries, compressed independently
--- unchanged tail ---
RegionTable                        // same wire, same WRITE_ORDER (STRING last)
BitSet + int count + count×long    // overlong entries
                                   // NO tail FSST marker on chunked pages (see fsstDictId above)
```

**Degenerate body (templateCount==0)**: META = compactDir only; chunks = verbatim inline record bytes. One layout, two semantics — exactly mirroring today's dedup/inline split.
**Empty page (populatedCount==0)**: metaRawLen covers an empty compactDir, chunkCount=0. Valid, ~30 bytes of body.

### 1.3 Write algorithm

Steps 1–5 of `serializePage` (PageKind.java:1538) unchanged: compressed-segment replay, `compressStringValues`, `addReferences` slot-CoW, bitmap scan scratches, **`buildRegionTable` before heap encode** (the `slotRegionAbsIdx` assignment that valueElision keys on is order-critical and preserved). Then `writeChunkedBody` replaces `writeEncodedBody`:

1. Stage the same sections as today into scratch buffers — but keep META sections and heap separate instead of concatenating into one codec frame. The inline-abort path (any `onDiskLen<2`, :2379-2384) discards the dedup staging and restages as degenerate — legal because nothing has been emitted yet (all lengths are known before the first byte is written; `bodyTotalLen` is computed, not backpatched).
2. Chunk plan: walk entries in rank order, accumulate on-disk lengths; close a chunk at ≥ C; a single record ≥ C gets its own chunk; if K would exceed 128, double C and replan (only reachable near the 256 KiB `MAX_SLOTTED_PAGE_CAPACITY`).
3. Elect the codec once per page (existing sticky bake-off, same probe/warmup/tie rules); encode META and each chunk with the winner; per-frame STORED override when output ≥ raw; XXH3-64 each stored payload.
4. Emit prefix + META frame + chunk table + payloads + unchanged tail. `compressAndCache` and the unresolved-overflow-refs skip (#1076, :1698-1714) unchanged — chunked serialization is deterministic under probeInterval=1, so the idempotent-double-serialize contract holds.

### 1.4 Read algorithms

**Full decode** (combine, writer CoW, page-streaming scans): parse prefix → decode META → decode all K chunks into the existing thread-local staging → run the **existing** expansion loop unchanged downstream. The staging upper-bound dance (`maxBlobBytes` :445-463) dies: exact section lengths are now on the wire. Output is byte-identical in-memory pages vs the monolith path for the same logical content. K decompress calls vs 1 cost ~nothing (decode cost ∝ output bytes; K×frame setup is nanoseconds).

**META-only directory build — the core enabler.** Extract the per-entry in-memory-length arithmetic of the current expansion loop (:1058-1446) into one pure function `inMemLengthOf(entry) = f(compactDir len, template, zeroHashBitmap bit, decoded pk/pnk values → re-encoded varint widths, valueElision width, nameKeyElision width)` — every input lives in META. Both the monolith expansion and the new directory build call this **shared** function, so equivalence is by construction, then swept by T1. Consequence: on load we can allocate the exact-size heap segment, build the complete directory by prefix sum, and materialize **zero** chunks.

**Chunk-lazy load path** (new; used when the load is triggered by point lookup — `IndexLogKey` callers; scan loaders, prefetcher, and combine request eager):
1. Parse prefix; decode META (~1.4 KB, ~5 µs); bulk-decode pk/pnk columns + rank prefixes for nameKeyElision + valueElision slot-map (~20–30 µs total, O(N) with tiny constants); build directory; copy compressed chunk payloads into page-owned **native** segments with the decoder tail slack (IN=16/OUT=64, `RegionTable.java:497-503, 796-804`); load regions per existing rules.
2. `ensureChunkFor(slotIdx)`: acquire-load per-chunk materialized bit; if unset → `synchronized(page)`: decompress (verify chunk XXH3 first), expand only that chunk's entries into their precomputed directory offsets (template inflate, hash zero-fill + zeroHash bit, pk/pnk re-encode from scratch arrays, value/nameKey injection from regions by `regionAbsIdx`/rank), release-store the bit. Publication pattern copied from `RegionTable.materializeDeferred` (:176-244); atomicity vs `ClockSweeper` close via the same page monitor + guard counts; `serveFromCached` seqlock rules unchanged.
3. All five direct heap readers gain the gate. A page-level ALL-materialized bit short-circuits to one predictable load for scans; eager loads set ALL at construction, so B-consumers pay a single hoisted branch.
4. Cache accounting: retained = heap segment + Σ compressed pending chunks, counted without decoding (`retainedBytes` precedent :400-422). Records are still never cached in `records[]` on the read path (snapshot-isolation rule, reader :660-665).

Projected point-read cost at C=4 KiB: ~5 µs META decode + ~30 µs O(N) light passes + ~5 µs chunk decode + ~1/5 of the heap-build cost ≈ **150–250 µs first slot vs ~1000 µs today (~4–6×)**, subsequent slots in other chunks pay chunk cost only. This is a projection anchored on measured components; commit 4's benchmark gates it (§4.4).

**Skip parsers (3, lockstep)**: `probeRegionTableOffset` (:135-171), `deserializeRegionsOnlyPage` (:182-245), and the full deserializer branch on the flag; chunked skip = read prefix through `bodyTotalLen`, `skip(bodyTotalLen)` — O(1), preserving the region-chunk-read and column-only paths untouched. Bonus: with compactDir out of the blob, `DirectPageScanner` kind counts and `ArrayPageRangeSequence` slot rejection become META-only.

### 1.5 Fate of every existing format lever under chunking

| Lever | Fate |
|---|---|
| Template pool + slotTemplateIds | Unchanged encoding, lives in META, always whole (avg 133 B / 12.1 templates) |
| hashElision (`zeroHashBitmap`) | META; entry-indexed bitmap → O(1) random access per entry; reinject width feeds `inMemLengthOf` |
| parentKey column (`StructuralKeyColumnCodec`) | META; bulk `decodeAll` at META parse (D10); writer activation predicate (:2235) unchanged |
| pathNodeKey column | META; dict decoded at META parse; slot-bitmap-indexed lookup unchanged (:796-830) |
| valueElision | META; section parsed into a slot→(type,width,regionAbsIdx) map at META parse (371 pages on corpus — tiny); injection happens per-chunk from regions by absolute region index; regions loaded before first materialize |
| nameKeyElision | META; all-or-nothing activation unchanged; per-entry rank = fused-named-slot prefix count, precomputed in one O(N) pass over slotTemplateIds at META parse |
| Codec bake-off + sticky election | Page-level, unchanged rules; applied per frame (META + K chunks) with STORED override; probeInterval=1 still yields byte-pure output |
| LZ4 read-compat (codec 1) | Still accepted per chunk and on legacy monolith bodies; never written (`HEAP_LZ4_DISABLED` unchanged) |
| FSST | Dict id in prefix (D7); `compressStringValues`, revision-table resolution, decompress-on-merge all unchanged; `regionChunkEligible` FSST decline lifted for chunked pages |
| DeweyIDs | 2-byte trailer stays inside slot bytes — chunking transparent; `getRecordOnlyLength` contract preserved by per-chunk expansion |
| Overlong/overflow | Unchanged, outside the body; chunk membership computed from the final bitmap after `processEntries` (overflow-migrated slots have cleared bits, are in no chunk) |
| Outer pipeline (`sirix.compression=lz4`) | Legal but re-wraps the page: CPU-ranged decode still works (frame is decompressed before body parse); the future bounded-pread path declines it, same gate as `regionChunkEligible` today |
| Disable toggles (5 elision/codec props) | Unchanged meaning. New: `sirix.chunkedBody.disable` (kill switch), `sirix.chunkedBody.targetChunkBytes` (default 4096), `sirix.chunkedBody.diag` (counters: lazyLoads, chunkMaterializations, eagerFallbacks, nativeDecoderMisses) |

---

## 2. CORNER-CASE LEDGER

Every corner case from the four surveys, consolidated. **Behavior** = what the design does; **Pin** = the test that locks it (G = exhaustive generator §3.3, S = sabotage suite, V = versioned sweep, CB = ClickBench bar, X = existing test must stay green with flag on).

### Format states

| # | Corner case | Design behavior | Pin |
|---|---|---|---|
| 1 | templateCount==0 degenerate body (1/105,185 pages here, but produced by raw slab writes) | Chunked twin: META=compactDir only, chunks=verbatim inline records, no structuralFlags byte; expansion = memcpy | G(T0 dim) |
| 2 | Mid-encode inline abort (`onDiskLen<2`, :2379-2384) | Abort before any emission; restage as degenerate chunked; all prefix fields consistent because lengths are computed pre-emit | G(crafted sub-2-byte record) |
| 3 | All 2^5 structuralFlags combos | Each section in META with the random-access strategy of §1.5; reader re-derives writer predicates from META exactly as today (:776-906) | G(32-combo axis) |
| 4 | valueElision keyed on absolute PAX region index (:452-456) | `buildRegionTable`-before-encode order preserved; per-chunk inject reads the region payload at regionAbsIdx; regions load precedes first materialize | G + S(corrupt regionAbsIdx) |
| 5 | nameKeyElision indexed by "n-th fused-named slot on page" | Rank prefix array from slotTemplateIds at META parse; chunk-boundary-straddling fused runs get correct ranks | G(fused slots straddling boundary) |
| 6 | parentKey sequential predictor needs slots 0..i-1 (`StructuralKeyColumnCodec.java:29-31`); per-slot decode is quadratic | Bulk decodeAll at META parse into scratch (D10); never per-chunk restarted | G + T1 |
| 7 | pathNodeKey dict is slot-bitmap-indexed | Dict decoded once at META parse; per-entry lookup unchanged | G |
| 8 | zeroHashBitmap is entry-indexed, not slot-indexed | Entry rank = chunk-table firstEntry + intra-chunk position; direct bit test | G(hash-elided entries at boundaries) |
| 9 | Sticky codec election breaks byte-purity unless probeInterval=1 (+ `resetStickyCodecElectionForCurrentThread`) | Election stays page-level; golden tests pin probeInterval=1 exactly as today | T5 fixed-point |
| 10 | Codec byte 1 (LZ4) must stay readable (old prototype DBs) | Accepted per chunk and on monolith path; Java fallback decoder retained | G(crafted LZ4 chunk fixture) |
| 11 | FSST marker semantics: 0 / −1+dictId / legacy positive throws (:1500-1514) | Monolith path unchanged; chunked pages carry `fsstDictId` in prefix, no tail marker; legacy-positive still throws on V0 | G(FSST axis) + X |
| 12 | populatedCount==0 / overflow-refs-only pages; zero-byte blob today | chunkCount=0, empty-compactDir META; prefix + tail valid | G(pop=0) |
| 13 | Overlong entries + overflow refs; slot cleared only on overflow migration (:5442-5445) | Untouched, outside body; chunk membership from final bitmap after processEntries | G(overflow axis) + V(#1076 shadowing) |
| 14 | DeweyID trailer inside slot bytes; valueWidth = record-only length (:2077, :3120) | Chunks carry the trailer verbatim; `getSlot` record-only-length contract asserted per slot | G(dewey axis) |
| 15 | `-Dsirix.compression=lz4` outer re-wrap defeats ranged I/O | Allowed for CPU-ranged decode; bounded-pread path declines (gate identical to `regionChunkEligible`); documented | X(config combo) |

### Page shapes

| # | Corner case | Design behavior | Pin |
|---|---|---|---|
| 16 | 1-slot delta fragments (routine under auto-commit) | K=1, ~40 B framing overhead over monolith; degrade is graceful by construction | G(pop=1) |
| 17 | Bitmap holes / single-high-slot / sparse shapes | Entry-space chunking is hole-blind; slot↔entry strictly via bitmap rank | G(4 bitmap shapes) |
| 18 | Encoded slot allocation up to `MAX_RECORD_SIZE = 512` B inline, including any Dewey payload/trailer | Ordinary greedy chunk membership; aggregate chunk lengths remain `int` because a chunk can exceed `u16` | G(max-record + Dewey) |
| 19 | Record exactly = C; chunk of exactly 1 entry; boundary at last entry | Greedy plan rules deterministic; boundary cases enumerated | G(crafted sizes) |
| 20 | Page near MAX_SLOTTED_PAGE_CAPACITY (256 KiB) → K could exceed table width | Writer clamps K ≤ 128 by doubling C | G(256 KiB synthetic) |
| 21 | All-zero bitmap ≠ absent bitmap (the ChunkedRegionReadTest trap) | Reader never infers absence from zero content; popcount cross-check retained; new Σ-checks added (#41) | X + S |

### Versioning / mutation

| # | Corner case | Design behavior | Pin |
|---|---|---|---|
| 22 | Mixed monolith/chunked fragment chains — guaranteed on first post-ship commit | Per-fragment format branch at deserialize; fragments decode independently; combine consumes decoded slots → **zero combine changes** (`VersioningType.java` untouched) | V(mixed-format chains, all 4 modes) |
| 23 | Tombstones: DeletedNode IS a record; populated bit ≠ live | Chunks containing only tombstones materialize like any other; no skip-dead heuristic exists to resurrect deletes at merge | V(tombstone transitions) |
| 24 | Overflow shadowing rules (slot↔reference, #1076) consult bitmap + refmap only | Both outside the body; unchanged | V |
| 25 | `addReferences` lazy slot copy from completePage's decoded heap (:5325-5344) | Source page gets `ensureAllChunks()` before the copy loop (write path is not the latency path) | V(preservation sweep, DIFFERENTIAL/INCREMENTAL/SLIDING) |
| 26 | Idempotent double-serialize: unresolved overflow refs force second pass; compressed-segment cache + generation guard | Deterministic staging (D5) ⇒ byte-identical second pass; cache-skip rule unchanged | G(serialize-twice byte-compare) |
| 27 | Rollback = TIL clear; committed bytes never rewritten | Append-only safety inherited; nothing to do | X |
| 28 | N-fragment read = N whole-blob decodes today | Per-fragment ranged decode applies (fragments are full pages in the same format); combine path uses eager materialize | V |
| 29 | SLIDING_SNAPSHOT out-of-window carry, bulk `copySlottedPageFrom` seed (:691-711) | Operates on decoded in-memory pages; unchanged; seed memcpy requires source ALL-materialized (eager on combine path) | V(mode sweep) |
| 30 | MAX_FRAGMENT_CHAIN=64 guard; 255-fragment reference cap | Untouched | X |

### Consumers

| # | Corner case | Design behavior | Pin |
|---|---|---|---|
| 31 | `moveToSingletonSlowPath` inlines the slot locate, bypassing `getSlot` (:838-842) | Explicitly on the gate list — all five direct heap readers gated (D9); missing one = silent wrong bytes, so the gate is asserted by the lazy random-order sweep | G(lazy vs eager differential) |
| 32 | Flyweight zero-copy bind onto heap memory | Bind only after `ensureChunkFor`; heap addresses stable (exact-size preallocation, no growth on materialize) | G(flyweight field equality) |
| 33 | Whole-heap consumers B.1–B.7 (combine, writer CoW, ensureRegionsFor, PageScanIterator/ColumnarScanAxis, ArrayPageRangeSequence, prefetcher, DirectPageScanner group-by) | Eager `ensureAll` at load; inner loops see one hoisted ALL-bit branch; regression budget ≤2% on Σ43 ClickBench | CB + JMH |
| 34 | DirectPageScanner kind counts, ArrayPageRangeSequence directory-kind reject | Become META-only for free (compactDir out of blob); counters assert zero chunk materializations | G(counter assert) |
| 35 | Regions-only machinery: `getRecordPageRegionsOnly`, fragment regions all-or-nothing, `claimSlots` deletion-in-bitmap rule, executor region caches, deferred regions | Body skip via bodyTotalLen is the only change; RegionsOnlyPage semantics untouched | X + T7 |
| 36 | RecordPagePrefetcher exists because decode ≈54% of cold scan CPU | Keeps eager whole-decode (it warms scans); point path stops needing it; miss-rate suspend unchanged | X |
| 37 | All KVLP index types share the format (DOCUMENT, PATH_SUMMARY, PATH, CAS, NAME, VECTOR, CHANGED_NODES, RECORD_TO_REVISIONS); `Names.fromStorage` point reads | Format change is index-type-blind; NAME-page point reads benefit automatically | G(indexType ∈ {DOCUMENT, NAME} targeted) |
| 38 | Snapshot isolation: `records[]` never populated on read path | Lazy materialization touches heap segment + bits only; asserted post-sweep | G(records[] null assert) |

### Integrity

| # | Corner case | Design behavior | Pin |
|---|---|---|---|
| 39 | Parent hash covers final compressed payload; verified pre-deserialize; RRP hash normalization (0="no hash") | Unchanged for all full-frame reads — today's entire I/O path | X |
| 40 | Chunk/META payload corruption | XXH3 verified at materialize/decode time; mismatch → `SirixIOException` naming page, chunk, expected/actual | S(bit flips per section) |
| 41 | Corrupt chunk table | Fail-loud cross-checks: Σ entryCount == populatedCount; firstEntry contiguous+monotone; Σ rawLen == onDiskHeapSize; Σ encLen + tables ≤ bodyTotalLen; codec ∈ {0..4}; K ≤ 128 | S(each violation crafted) |
| 42 | Truncated frame mid-body | Bounds vs bodyTotalLen and frame length → throw; truncation ladder at every section boundary | S |
| 43 | Stale comment `AbstractReader.java:53-55` ("verified after decompression") | Fixed in commit 2; behavior is and remains hash-over-compressed for all kinds | doc |
| 44 | `verifyChecksumsOnRead` + future bounded pread (the regionChunkEligible dead-end; MMFileReader faults the whole page) | Full-frame path verifies parent hash as today; the future pread path verifies chunks via table hashes; a coherently corrupted table+hash pair is outside the bit-rot threat model (status quo); decline is counted, never silent | S + diag counters |

### Concurrency

| # | Corner case | Design behavior | Pin |
|---|---|---|---|
| 45 | Lazy materialize on a shared cached page vs concurrent readers | `synchronized(page)` materialize + VarHandle release-store bit / acquire-load gate (RegionTable :176-244 pattern; note RegionTable was thread-confined, KVLP is not — hence the monitor) | stress suite |
| 46 | Materialize vs ClockSweeper eviction / `close()` releasing the pooled segment (:3053-3072) | close under the same monitor; guard counts remain the only liveness protection; failed-acquire-means-dead-mapping respected (`ShardedPageCache.java:253-286`) | stress suite |
| 47 | `serveFromCached` seqlock; FSST table load must run OUTSIDE cache compute (:1327-1334) | Both unchanged; fsstDictId read from prefix does not alter the resolution point | X |
| 48 | Native decoder tail-slack precondition — sized-exactly buffers silently fall back to Java decoder (RegionTable :796-804 caught this) | Pending-chunk buffers and decode targets carry IN=16/OUT=64 slack; test suite asserts the native-decoder-hit counter | G(counter assert) |

### Platform

| # | Corner case | Design behavior | Pin |
|---|---|---|---|
| 49 | AOT monomorphism: mixed heap/native segments segfault vector paths (RegionTable EMPTY sentinel, oracle/graal#14255) | All chunk stores, staging, and heap segments uniformly native; EMPTY sentinel is a native segment | native-image smoke on existing AOT rig |
| 50 | 160 B header pinned LE; body ints BE via `Bytes` | New fields follow body convention (BE); layout doc updated; golden fixtures byte-compare | golden bytes |
| 51 | No liblz4 / no native LZ77 present | Per-chunk decode fully functional on pure-Java fallbacks; conformance suite re-run with natives force-disabled | G(fallback run) |

---

## 3. FORMAL VERIFICATION PLAN

Style precedent: `docs/HOT_PAPER_IMPOSSIBILITY.md` theorem/proof-sketch discipline + exhaustive (not sampled) conformance generators.

### 3.1 Invariants

**I1 — Slot-read equivalence.** ∀ page content P, ∀ lever state L ∈ (templateCount states × structuralFlags × FSST × dewey × codec), ∀ slot s: the bytes (and record-only length) returned by `getSlot(s)` after chunk-lazy materialization ≡ the bytes after monolith full decode of the same logical content, and flyweight-bound field values agree.
*Proof sketch.* Two lemmas over the format-state case analysis. **Lemma A (layout purity)**: the in-memory directory is a pure function of META — `inMemLengthOf` consumes only compactDir length, template, zeroHash bit, decoded pk/pnk values, and elision widths, all META-resident; it is the *same shared function* invoked by both decode paths, so agreement is by construction, leaving only well-definedness (each of the 32 flag combos contributes a width term that exists iff its META section exists — exhaustive case check). **Lemma B (expansion locality)**: expansion of entry e reads only (META scratch arrays, e's chunk bytes, region payloads at e's regionAbsIdx/rank) — after D10 bulk-decodes the two sequential predictors into scratch, no per-entry input crosses a chunk boundary; enumerate the six reinjection kinds (pk, pnk, hash, value, nameKey, dewey-trailer passthrough) and show each input set. I1 = Lemma A (offsets agree) + Lemma B (bytes agree) restricted to any chunk order, hence to any lazy access order.
*Machine check*: generator sweep (§3.3), assertion (b): every slot, lazy in random chunk order vs eager, byte-compare + record-only length + flyweight fields.

**I2 — Combine commutes with ranged decode.** For any fragment chain f₁..fₙ with any per-fragment format ∈ {monolith, chunked} and any materialization mode, `combineRecordPages` output ≡ the all-monolith reference, for all four versioning types.
*Proof sketch.* Combine consumes only `getSlot` outputs, populated bitmaps, and reference maps (`VersioningType.java:1009-1026`); bitmaps and refmaps are format-invariant (outside the body); I1 gives per-fragment slot equality; combine is a deterministic function of these inputs. Tombstone and overflow shadowing decisions read only format-invariant state. ∎ modulo I1.
*Machine check*: versioned sweep — 4 modes × transition matrix {overwrite, tombstone, resurrect, overflow-migrate, overflow-shadow-back, shrink-back, out-of-window carry} × chain formats {all-old, all-new, alternating, old-full+new-delta, new-full+old-delta}, asserting combined page equality slot-by-slot, plus the header-bitmap-skip regression (versioning corner 10).

**I3 — Integrity soundness.** Any single corrupted or truncated byte range in {prefix, META frame, chunk table, chunk payload, regions, tail} yields an attributable `SirixIOException` or a checksum mismatch, never silently wrong slot bytes.
*Proof sketch.* Coverage partition: (i) full-frame reads — parent XXH3 over the whole payload, verified before parse (status quo, covers everything); (ii) materialize-time — chunk/META payloads by embedded XXH3; (iii) structure — table and prefix fields are cross-checked by four Σ/monotonicity invariants (#41) plus the existing popcount check, each violation throwing; (iv) expansion — every width round-trip is already verified fail-loud (`onDiskPos != onDiskHeapSize` and 10+ SirixIOException validations retained per chunk). Residual: an adversarially coherent table+hash corruption is undetectable without the parent hash — identical to today's region-chunk path; threat model is bit-rot (XXH3 is non-cryptographic).
*Machine check*: sabotage suite on representative generator pages — (1) bit-flip in each of 8 sections, (2) truncation at every section boundary, (3) swap two chunk hashes, (4) chunkCount=0 on populated page, (5) overlapping firstEntry, (6) Σ rawLen off-by-one, (7) invalid codec byte, (8) META hash mismatch, (9) corrupt regionAbsIdx, (10) entryCount sum ≠ populatedCount, (11) bodyTotalLen short/long, (12) encLen past frame end. Each must throw with attribution; none may return bytes.

**I4 — Fallback totality (old format forever readable, new format cleanly fenced).** Every V0 monolith page decodes through the byte-untouched legacy path; every chunked page presented to a pre-change reader throws the designed "newer version" error; no page is silently misparsed.
*Proof sketch.* The flag bit partitions the format space; `readVersionAndFlags` throws on nonzero flags in old readers (:4946) — the fence is pre-existing, shipped behavior. The new reader's branch point is the flag test; the V0 arm is textually unchanged code.
*Machine check*: checked-in golden binary fixtures of pre-change pages (all lever combos) decoded by the new reader → byte-identical in-memory pages and re-serialization fixed points; flags round-trip test; a chunked fixture fed to the V0 parser branch must throw.

**I5 — Write-read roundtrip identity.** Under probeInterval=1: serialize(deserialize(serialize(P))) is a byte fixed point; the async-commit double-serialize (unresolved overflow refs) is byte-identical across passes.
*Proof sketch.* All nondeterminism sources enumerated: codec election (pinned), chunk plan (deterministic greedy over deterministic scratches), section order (fixed), hashes (functions of payloads). ∎
*Machine check*: generator assertion (c) + serialize-twice test (#26).

**I6 — Concurrent materialization safety.** ensureChunkFor is linearizable: every reader observes either fully-expanded chunk bytes or takes the materialize path; no torn heap reads; materialize/close/evict are mutually atomic.
*Proof sketch.* Writes to the heap range happen-before the release-store of the chunk bit; readers acquire-load the bit before touching the range; monitor serializes materialize vs close; ClockSweeper's guard-count protocol is unchanged and materialize holds a guard.
*Machine check*: stress suite — N reader threads on distinct slots of one shared page racing materialization + a ClockSweeper eviction thread; run under both JIT and native image; assertion mode where any full-decode fallback throws (the "flag nobody sets" counter-disease, `AbstractReader` :323-336 lesson).

**I7 — Skip-parser lockstep.** For every generator page, `probeRegionTableOffset`, `deserializeRegionsOnlyPage`, and the full deserializer compute identical region-table offsets and identical region/tail content.
*Machine check*: three-way offset+content assert in the generator loop.

### 3.2 Why the case analysis is closed

The format state space is finite and enumerated: body shape ∈ {dedup, degenerate, empty} × structuralFlags ∈ 2^5 × codec ∈ {0,2,3,4 written; 1 read} × FSST ∈ 2 × dewey ∈ 2 × overflow ∈ 2 × chunk plan boundary classes ∈ {entry mid-chunk, entry first-in-chunk, entry last-in-chunk, singleton chunk, oversized-record chunk}. Lemmas A and B are proven by exhausting exactly these axes; the generator (§3.3) sweeps the same axes, so any gap between proof and code surfaces as a differential failure, not a latent bug. (The validate-with-a-rare-literal lesson: rare shapes get *enumerated*, not sampled.)

### 3.3 Exhaustive generator — dimensions and cross-product size

Writer flags are content-driven, so the generator forces OFF via the five disable properties and ON via crafted content, then **asserts the achieved structuralFlags byte equals the intended combo** — an unreachable combo fails loudly and is recorded as a proven-unreachable ledger fact.

Core sweep (full cross-product, no sampling):
- structuralFlags: 32 (dedup shapes) — degenerate shape has no flags byte (separate arm)
- templateCount ∈ {1, 3} → 2; degenerate arm templateCount=0
- populatedCount ∈ {0, 1, 2, 511, 512, 513, 1023, 1024} → 8 (chunk-boundary straddles by construction)
- bitmap shape ∈ {dense-prefix, alternating holes, single-high-slot, seeded-random(fixed seed)} → 4
- chunk target C ∈ {1 KiB, 4 KiB, ∞ (K=1)} → 3

Core = 2×32×8×4×3 = **6,144** dedup pages + 1×8×4×3 = **96** degenerate pages.

Targeted overlay (remaining axes × 4 representative flag settings {none, all, parentKey-only, value+nameKey}):
- record/slot-boundary shapes ∈ {1 B, mixed, 257 B with C=256 B, 512 B with C=512 B, body + Dewey payload/trailer totaling 512 B} → 5
- overflow ∈ {0, present} → 2; dewey ∈ {off, on} → 2; FSST ∈ {off, on} → 2
- forced codec ∈ {ZeroRun, ByteRun, LZ77, STORED} → 4; tombstones ∈ {none, some} → 2

Overlay = 4 × (5×2×2×2×4×2) = **2,560** pages. Plus indexType ∈ {DOCUMENT, NAME} on a 64-page subset.

**Total ≈ 8,800 pages — fully enumerable, runs in seconds-to-minutes.** Per page: (a) chunked full decode ≡ monolith decode of same records, slot-by-slot; (b) lazy random-chunk-order ≡ eager (I1); (c) roundtrip fixed point (I5); (d) three-parser lockstep (I7); (e) checksum verify green; sabotage suite (I3) on a per-arm representative. Entire suite re-run once with native decoders force-disabled (#51) and once under native image (#49).

### 3.4 System-level bars

1. **ClickBench 43/43 byte-identical** after re-ingest with the chunked writer ON, vs the current committed answers (three-way differential harness unchanged).
2. **Point-read bench** (Q19-shape `materializeOne`, 179 scattered winners): ≥3× first-touch page-decode CPU reduction at C=4 KiB, measured with interleaved A/B arms in one build and an MHz check before quoting (thermal-throttling protocol).
3. **Scan regression gate**: Σ43 hot ≤ +2% vs monolith baseline; `ArrayPageRangeSequence` and combine micro-benches individually ≤ +3%.
4. **Versioned conformance sweep** (I2 matrix) across all 4 versioning modes, mixed-format chains.
5. **Diag counters non-zero in CI**: chunkLazyLoads > 0 on the point bench, nativeDecoderMisses == 0 — the silently-disabled-twice disease is fenced by assertion, not hope.

---

## 4. MIGRATION + ROLLOUT

**4.1 Version gate.** Page-envelope flags bit 0x01; the row-store body keeps both ranged and monolith readers. The resource and every current internal projection header emit version zero. A future incompatible row-store or projection layout must advance its fail-closed version byte before any resource using it is written.

**4.2 Mixed-version fragment policy.** Fully supported, permanent. Fragments are self-framed full pages; format is branched per fragment at deserialize; combine operates on decoded slots and is untouched. The first commit after enablement necessarily creates mixed chains — this is the tested path (I2), not an edge case.

**4.3 Re-ingest.** **None required.** Existing resources stay readable and keep working at old point-read cost until their pages are naturally rewritten by commits (each rewritten fragment becomes chunked). Re-ingest is optional for benchmark corpora that want full ranged coverage immediately.

**4.4 Escape hatches.** (1) `sirix.chunkedBody.disable` — writer reverts to monolith instantly; already-written chunked fragments remain readable; no data rewrite ever needed to roll back. (2) `sirix.chunkedBody.targetChunkBytes` — storage/latency dial; C=∞ degenerates to K=1 ≈ monolith + ~40 B. (3) Lazy mode is reader-side policy, independently disableable (eager-always) without touching the wire.

**4.5 Landing order (reviewable commits, each green + gated):**
1. **Refactor, no wire change**: extract blob staging into an explicit `BodySections` carrier + extract `inMemLengthOf` as the shared pure function; golden byte-identity test proves zero wire drift. Fix the stale `AbstractReader.java:53-55` comment.
2. **Wire format**: chunked writer + eager chunked reader behind flag (default OFF); three prefix parsers updated in lockstep; V0 golden fixtures checked in; I4/I5/I7 tests.
3. **Exhaustive conformance**: generator (§3.3) + sabotage suite; I1(eager)/I3 green.
4. **Ranged path**: META-only directory build, chunk-lazy materialization, the five consumer gates, concurrency stress suite; I1(lazy)/I6 green; point-read benchmark + scan regression gate.
5. **Versioning**: I2 mixed-chain sweep, ClickBench re-ingest 43/43, perf bars; diag counters wired.
6. **Flip default ON for new resources**; `docs/` format spec update; CLICKBENCH.md note.

---

## 5. RISKS, RANKED

| # | Risk | Severity | Mitigation |
|---|---|---|---|
| 1 | **Hot-path gate regresses whole-heap scans** (B-consumers are the ClickBench engine) | High | Single page-level ALL-bit checked once per consumer loop; eager loads set it at construction so scans pay one predictable branch; JMH + Σ43 gate ≤2%; interleaved A/B + MHz check (thermal trap) |
| 2 | **Concurrency defect in lazy materialize on shared cached pages** (eviction race, torn reads) | High | Only proven patterns reused (RegionTable VarHandle publication, page monitor, guard counts, seqlock); I6 stress under JIT + native image; assertion mode fails tests on unexpected fallback |
| 3 | **A rare lever combo expands differently ranged vs monolith** (silent wrong bytes — the worst failure class) | High | Shared `inMemLengthOf` function (equivalence by construction), Lemma B locality proof, and an *enumerated* 8,800-page sweep — the rare-literal lesson institutionalized |
| 4 | **Storage growth** (+6.8% file at C=4 KiB) | Medium | Regions dominate the file (55%); C is a per-resource dial (C=8 KiB → +4.0%, K=1 → ~0%); growth is bought CPU, priced by measured corpus math, not hope |
| 5 | **Silent feature death** (counters nobody reads; the region path was silently disabled twice) | Medium | CI asserts lazyLoads > 0 on the point bench and nativeDecoderMisses == 0; decline paths counted, never silent |
| 6 | **Byte-determinism regression** breaking golden tests / idempotent double-serialize | Medium | Election stays page-level; all nondeterminism sources enumerated in I5; serialize-twice test on unresolved-overflow pages |
| 7 | **Native decoder tail-slack violation per chunk** → silent 100% Java-decoder fallback (happened to RegionTable) | Medium | Slack baked into every chunk buffer; native-hit counter asserted in tests |
| 8 | **AOT segfault via mixed segment provenance** (graal#14255 class) | Medium | All chunk/staging/heap segments uniformly native incl. EMPTY sentinel; native-image smoke in CI rig |
| 9 | **Scope creep toward per-chunk predictor restarts / per-slot decode** reintroducing the quadratic StructuralKeyColumnCodec trap | Low | D10 is final: bulk decode at META parse (~20 µs); guardrail comment + a decode-cost budget assert on a 1024-slot page |
| 10 | **Future bounded-pread expectations** (this plan ranges CPU, not I/O — frames are still read whole) | Low | Explicitly scoped: format is pread-ready (bodyTotalLen skip, per-chunk hashes, prefix FSST id), the pread reader is a separate follow-on with its own eligibility gate |

---

**Key files touched**: `bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java` (writer/reader/3 parsers), `io/sirix/page/KeyValueLeafPage.java` (lazy state, gates, close), `io/sirix/page/PageLayout.java` (doc only), `io/sirix/access/trx/page/NodeStorageEngineReader.java` (lazy-load policy, `AbstractNodeReadOnlyTrx` inlined-locate gate), `io/sirix/io/AbstractReader.java` (comment), `io/sirix/io/filechannel/FileChannelReader.java` (eligibility, later phase), `io/sirix/access/ResourceConfiguration.java` (per-resource setting), new `io/sirix/page/ChunkedBodyCodec` + conformance suite under `bundles/sirix-core/src/test/java/io/sirix/page/chunked/`.

---

# ADVERSARIAL VERIFICATION APPENDIX

VERDICT: **GAPS** — the design is substantially sound and nearly all file:line citations check out against the code, but two gaps are wrong-answer-class and the formal statements need repair before the proofs are trustworthy.

**Verified and held** (spot checks against `/home/johannes/IdeaProjects/sirix`): envelope fence throws on nonzero flags exactly as claimed (PageKind.java:4940-4948, :5004-5015); structuralFlags is exactly 5 bits (:5590-5616) so 2^5=32 is right; the cross-product arithmetic is correct (6,144+96+2,560=8,800); `PageConstants.MAX_RECORD_SIZE` delegates to the shared `Constants.MAX_RECORD_SIZE = 512` encoded-slot ceiling (an inline Dewey payload/trailer counts toward it; after overflow, the record body moves to `OverflowPage` and the Dewey ID remains in page metadata); inline abort is pre-emission (:2375-2384) so the restage claim is legal; value/nameKey elision cursors are sequential and the plan correctly precomputes rank prefixes; FSST'd string bytes live verbatim in heap and regions so injection is a straight copy needing no symbol table at materialize (:2117, :2548, :3590-3598); the compressed-segment replay covers the whole page including envelope (serializePage :1542-1547) so I5 replay is trivially byte-identical; unresolved-overflow double-serialize skip (:1706-1714); JavaLz4BlockDecoder pre-buffer rejection (:122-126); native-decoder tail slack (pax/RegionTable.java:796-804); retainedBytes-without-decode (:410-424); AbstractReader comment is indeed stale (:48-60 claims KVLP verifies after decompression); regionChunkEligible (FileChannelReader.java:335-338); VersioningType shadowing helpers consult only bitmap+refmap (:1009-1026).

**Gaps, ranked:**

1. **HIGH — Lemma A's input model for `inMemLengthOf` contradicts the code and re-specifies a known wrong-answer failure mode.** The plan says widths come from "decoded pk/pnk values → re-encoded varint widths". The real loop (PageKind.java:769-830) derives pkWidth/pnkWidth from **template offset-table deltas** plus a column-participation test (`PathNodeKeyRegion.pathNodeKeyForSlot(...) >= 0`), and the comment at :772-775 explicitly warns that deciding from decoded values diverges the day a field legitimately holds NULL_NODE_KEY (writer strips, reader never reinjects). Fix: restate Lemma A's inputs as {compactDir onDiskLen+kindId, template field count + offset deltas, zeroHash bit, pnk-participation bit, valueElision width, nameKeyElision width}; mandate the extraction commit lift the existing loop verbatim; add a NULL_NODE_KEY-parentKey page to the generator (currently on no axis).

2. **HIGH — D7's FSST-decline lift opens a silent-wrong-answer path with no ledger row: the bounded region-read plumbing never carries the FSST dict id.** `probeRegionTableOffset` returns only {pageKey, revision, populatedCount} (:135-171); `deserializeRegionTableAt` takes `fsstSymbolTableId` as a caller parameter (:173-179) — safe today only because `regionChunkEligible` excludes FSST resources wholesale. Lifting the decline without (a) surfacing prefix `fsstDictId` through the probe (out[3]) and (b) making eligibility **per-page** (a monolith fragment inside an FSST resource must still decline at probe time — eligibility is per-resource, format is per-fragment) builds RegionsOnlyPages with NO_FSST on FSST data → string predicates compare raw literals against FSST-STORED dictionary bytes. Fix: extend the probe contract, per-page decline on flag==0, new ledger row + differential pin (FSST resource, mixed-format pages, string predicate via column path).

3. **HIGH — the "five direct heap readers" enumeration is undercounted; the gate list is not closed.** Verified ungated `getSlottedPage()`+heap reads: AbstractNodeReadOnlyTrx **:711** (same-page singleton fast path — fires only when currentPage==target, so the random-order sweep can miss it), **:840**, **:999** (three sites; plan lists one), plus `JsonNodeFactoryImpl.bindWriteSingleton` :522 and `NodeStorageEngineReader.getRecordFromSlottedPage` :536-541 (directory read safe post-META, but the following `getDeweyIdAsByteArray` heap read must be ordered after the same slot's gate). Fix: replace the fixed list with a mechanical audit plus test-mode enforcement — poison-fill (0xCC) unmaterialized chunk ranges and assert on access via a PageLayout debug hook, so any ungated reader (including future ones) fails deterministically inside the generator sweep.

4. **MEDIUM — I1 proves slot-granular equality but I2/I5 consume whole-segment state.** Combine also consumes `getSlotNodeKindId`, per-fragment FSST tables via `FsstAwareSlotCopier` (VersioningType :211-:917), bulk `copySlottedPageFrom` seeds (:668, :692-695; KeyValueLeafPage.java:1211 = whole-segment memcpy), and dewey trailers. `getSlot` slices **exclude** the DeweyID trailer, so a trailer-placement bug passes I1's byte-compare as written. Fix: strengthen I1's machine check to full slotted-segment equality (header+directory+heap) after ensureAll vs monolith — subsumes slots, dewey, kind ids — and restate I2's input list (FSST tables and seeds are format-invariant, so the theorem survives, but the proof must say it).

5. **MEDIUM — I4's fence is real but overstated, and there is no open-time fence.** On the bounded path an old reader first *swallows* the fence exception (FileChannelReader :365-366 `catch (… IllegalStateException) return null`) and only the subsequent whole-page parse throws — still fail-loud, but from a different site than claimed. And an old binary opens a chunked resource fine, failing mid-query page-by-page; project precedent (V0 reject-at-open) argues for a `chunkedBodiesWritten` bit in resource config at first chunked commit so pre-chunking binaries refuse at open. Add a ledger row for "old binary, bounded probe, chunked page."

6. **LOW/MEDIUM — corner 28 is self-contradictory; the point-read win silently assumes chain length 1.** "Per-fragment ranged decode applies … combine uses eager materialize": since combine is eager, N≥2-fragment pages pay N full decodes and chunk-lazy serves only single-fragment pages. True on ClickBench (single commit) so the Q19 bar stands, but the formal claim is dead code as designed. Fix: scope the claim explicitly, or specify winner-chunk-only materialization in combine as follow-on.

7. **LOW — I7 counts three parsers; there are four entry points and the assert misses a field.** `deserializeRegionsOnlyPage` parses the tail FSST marker today (:224-244) while chunked pages move it to the prefix — I7 must assert `fsstSymbolTableId` equality by name; `deserializeRegionTableAt` (:173-179) is the fourth entry point whose contract changes under gap 2.

8. **LOW — cross-product soft spots.** (a) Codec × flag-combo × boundary is sampled, not exhausted (codec forced only in the overlay at 4 flag settings); sound only under an explicit codec-independence lemma (codec wraps opaque chunk bytes; per-codec round-trip identity) — state it rather than leave it implicit. (b) Content-driven unreachable flag combos get zero coverage recorded as "proven-unreachable" — acceptable, but that proof is per-writer-version and must re-run whenever activation predicates (e.g. :2228-2242) change.
---

# AMENDMENTS (post-verification, BINDING)

Each gap above is resolved as follows; implementation MUST follow these over the original text
where they conflict.

- **A1 (gap 1, wrong-answer class):** Lemma A is restated over the REAL width inputs
  {compactDir onDiskLen+kindId, template field count + offset deltas, zeroHash bit,
  pnk-participation bit (PathNodeKeyRegion.pathNodeKeyForSlot >= 0), valueElision width,
  nameKeyElision width}. The extraction commit lifts the existing loop (PageKind.java:769-830)
  VERBATIM — never re-derive widths from decoded values (NULL_NODE_KEY divergence, comment
  :772-775). Generator gains a NULL_NODE_KEY-parentKey axis.
- **A2 (gap 2, wrong-answer class):** the bounded region-read probe contract is extended to
  surface prefix fsstDictId (out[3]); FSST eligibility becomes PER-PAGE (flag==0 monolith
  fragment inside an FSST resource declines at probe time). New ledger row + differential pin:
  FSST resource × mixed-format pages × string predicate via the column path.
- **A3 (gap 3):** the gated-reader list is NOT closed by enumeration. Enforcement is
  mechanical: test-mode poison-fill (0xCC) of unmaterialized chunk ranges + PageLayout debug
  hook asserting on access, run inside the generator sweep; the three additional verified
  ungated sites (AbstractNodeReadOnlyTrx :711/:840/:999, JsonNodeFactoryImpl :522,
  getRecordFromSlottedPage :536-541 dewey-after-gate ordering) join the gate set.
- **A4 (gap 4):** I1's machine check compares the FULL slotted segment
  (header+directory+heap, dewey trailers included) after ensureAll vs monolith; I2's proof
  text lists FSST tables and copy seeds as format-invariant inputs.
- **A5 (gap 5):** ledger row added for "old binary, bounded probe, chunked page" (fence fires
  at the whole-page parse after the probe's swallowed exception); resource config gains
  chunkedBodiesWritten at first chunked commit so pre-chunking binaries refuse AT OPEN
  (V0-reject precedent).
- **A6 (gap 6):** the point-read win claim is SCOPED to single-fragment pages; winner-chunk-
  only materialization in combine is a named follow-on, not part of this plan's proofs.
- **A7 (gap 7):** I7 asserts fsstSymbolTableId equality by name across all FOUR prefix-parser
  entry points (deserializeRegionsOnlyPage tail-marker path included).
- **A8 (gap 8):** the codec-independence lemma is stated explicitly (codec wraps opaque chunk
  bytes; per-codec round-trip identity proven separately); proven-unreachable flag combos are
  recorded with the writer-version they were proven against and re-proven when activation
  predicates change.

---

# WIRE-COMPATIBLE DELIVERY (BINDING — supersedes conflicting text above)

The ranged-decode work does not require a storage-format break. This section supersedes A5 and
every statement above that requires a binary-version bump, deletion of the monolith reader, or
rejection of an otherwise supported resource.

- Existing monolith pages remain readable and writable; their deserializer is retained.
- Chunked bodies are an optional page representation selected by the existing per-page dispatch,
  so mixed-format resources and fragment chains remain valid inputs to the conformance sweep.
- Chunked writes remain disabled by default. Enabling them is an explicit resource choice and does
  not change the binary version of monolith pages.
- Cold reopen, bounded probes, FSST eligibility, and full-fragment combination are verified for
  both representations. Unsupported page flags fail at the page boundary without making older
  monolith resources fail at resource open.
- Benchmark corpora may be re-ingested for measurement, but re-ingestion is not a compatibility
  requirement and is never used to avoid testing the retained reader.
- Entry-space chunking, the META section, codec election, checksums, lazy expansion, and the proof
  obligations in A1 through A4 and A6 through A8 otherwise remain unchanged.
