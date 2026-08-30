# Storage size and query speed — one plan (ClickBench 100M, projection-index track)

Status: **DRAFT 6.1 — 2026-08-30 16:35 — plan of record after both independent reviews and round 1's addendum** (round 1: 16 findings,
round 2: 20 findings; every load-bearing anchor re-verified in the code) and the B0 measurement. User decisions:
projection indexes are the path; the node store stays at 1,024 nodes per leaf; everything general (§0); query
compilation later (C1). Outcome required: storage drops considerably AND queries are served with very low latency.

## 0. Generality contract (every lever, every brief)
Every mechanism is triggered by **data, statistics or resource configuration — never a column name, field list,
query id or benchmark**. Benchmark-specific = harness only (`io.sirix.query.bench.*`: which columns a resource
projects and their declared types, loaders, oracles, dump compares). Main code never mentions ClickBench; witnesses
run on synthetic fixtures. Reviewers check it per brief.

## 1. Baseline (measured; B0-consistent)
**100M `sirix.data` = 131.9 GB = 1,319 B/row.** Projection (`ProjDiskDump` over KEYS/BODY/DICT/BLOOM): 178.5 B/row at
100M; the full-accounting `ProjectionDiskDump` (B0r) at 1M gives **115.5 B/row** = column segments 101.1 (DICT_HASHES
4.3 B/row over the per-leaf string columns, blooms 1.2) + keys 12.8 + descriptors 1.3 + framing 0.3 — i.e. the omitted
kinds add ~6 B/row at 1M, more at 100M where the fat columns are per-leaf. Working figures: **projection ≈ 195–205 B/row
≈ 20 GB at 100M; the node trie ≈ 1,110 B/row ≈ 111 GB (84 %)**.

Projection at 100M (B/row): URL 36.4 + hashes, Referer 27.3, Title 26.6, EventTime 24.7 (ISO strings, per-leaf
dictionaries), KEYS segment 13.7 (= **Dewey order labels**: `int32` offsets 4 B/row + arithmetic labels 8–10 B/row;
the record keys inside it are already delta-FOR at ~1 B/row), 64-bit hashes 8.0 each (entropy floor), UserID 7.2,
SearchPhrase 5.4, all other numerics ≤ 4.

Trie leaf at 1M (B0 run, per record **on the wire, scaled to the written file: 10.3 B**; a row is 106 records — the
object plus 105 fields; a page holds ~9.7 rows, ~71 numeric and ~34 string tags):

| section | B/record on wire | what is inside (raw) |
|---|---|---|
| encoded body | **6.8** | staged 22.5 raw → 0.39× by the codec bake-off: directory 1.4, templates + slot ids 0.8, **heap 15.2** = per fused record kind + template id (both repeated — the directory and slot ids already carry them), value-elision metadata 4–5 (gap/type/width/region-index varints), name-key elision width 1, pathNodeKey dictionary id 1, right/left sibling deltas 2, prevRev + lastModRev 2, framing — **plus every string inline on the ~50 % of pages that hold one overflow record**, because an overflow descriptor sets `stringRegionComplete = false` and the string region is then not written at all (no string elision, no sketch, no SIMD string scan) |
| region table (LZ77) | **3.2** | raw ~11: NUMBER region written as **plain 8-byte longs on every page** — bit-packing needs a page-wide spread < 2^56 and the 64-bit hash columns always exceed it — plus per-tag min/max inside it AND a second copy in the zone-map region (22 B + 24 B/tag); string region 16 B/tag + per-tag dictionaries over ~10 rows; name-key region 128-B bitmap + 4 B/name; record ordinals |
| header + bitmap, overlong, sidecar | 0.3 | 160-B fixed header/bitmap, carriers' framing |

Fused record anatomy as the writer emits it (round-1 addendum, `NodeFieldLayout`, `NodeKind` ~918–955, `PageKind`
`stageEncodedHeap` ~2692): in memory `kindId + 9-byte offset table + data`; on disk `kindId + templateId + data` with
these strips — parentKey → parent-key column, pathNodeKey → column, nameKey → name-key region (all-or-nothing per
page), hash → zero-hash bitmap, payload → region (per slot, when the saving beats the wire cost). **Never stripped:**
rightSibling δ, leftSibling δ, prevRev, lastModRev — four 1-byte varints in a bulk-loaded revision. So the minimum
heap record is 6 B pre-codec (kind, template, four varints) + a 2-byte directory entry + 1 B slot-template id outside
the heap, with kind and template each stored twice. The cheapest first cut, in order: drop the duplicated kind and
template bytes, elide the two revision varints when equal to the page revision, then the STRUCT_POINTERS columns for
the siblings.

**Per-page fixed overhead at ~10 rows/page is ~3–4 B/record on its own** (tag headers, two zone-map copies, string tag
headers, name-key bitmap, page header, template pool) — the whole M1 budget before any value byte. The cost model
for every T1 estimate is therefore *fixed per page ÷ 10 rows + per value*, not per-value alone.

References (`data_size`, c6a.4xlarge, all 105 columns): Umbra 83 B/row, CedarDB 85, ClickHouse 153, DuckDB 205.

## 1b. Rebuild #1 at 100M (2026-08-30 20:55, commit `eb5a307b7`)
**90.4 GB (−31.4 % from 131.9 GB)**: leaf class 70.8 GB (≈ 708 B/row), overflow/projection 17.4 GB, HOT 1.9 GB;
load 46m 55s unchanged; projection acceptance OK. Not yet in this build: B5-c per-tag FOR + string framing + zone-map
fold and B3-a d4 (together another −24 % on the leaf class at 1M) → rebuild #2 expected ≈ 70 GB.

## 2. Targets
| milestone | 100M | trie B/record | how |
|---|---|---|---|
| today | 131.9 GB | 10.3 | |
| **M1** | **≤ 50 GB** (expected 45–50) | ~3.7 | T1 d → c → a → b, P3, P-ET, then P2 |
| **M2** | ≤ 30 GB | ≤ 2.2 | T1-c's **cross-page page schema** (per-tag directories and template pools content-hashed and stored once per resource, referenced by a varint id per page) removes most of the fixed per-page overhead — the in-track M2 lever; dictionary-coded fat strings in fused records remain the alternative for the string payload |

Speed: Σ cold at 8 GB is **807 s** (3,803 s pre-lever; 43/43 on their routes). Target **Σ cold ≤ 150 s, no query above
15 s.** Mechanism per query in §4; P2 is the lever most of the string-bound queries hinge on, and T1/P3 speed *no*
group-by (the projection routes never read the trie; KEYS is read only by sorted emission and the row path).

## 3. Levers

### T1 — Trie leaf compaction, in the order the reviews established
- **(d) FIRST — the fused cap and string-region completeness.** Raise `MAX_RECORD_SIZE` 512 → 1,023 (the 10-bit
  directory length already allows it; `Constants`, `PageLayout.MAX_COMPACT_DIR_DATA_LENGTH`, `OverflowSlotSidecar`),
  and decouple region completeness from overflow descriptors with a per-tag completeness flag (the pattern the
  orphan handling already uses) so one long Title never evicts the other 33 columns' strings. Witness: a diagnostic
  counter "pages without a string region because of overflow" and string-elision bytes before/after; the
  region-only read tests on pages with carriers. Expected: string elision on ~100 % of pages instead of 76 %;
  the inline string bytes leave the heap.
- **(c) Per-tag FOR in the number region with the zone map folded into the header.** Values are already grouped by
  tag; give each tag (base, width, bit offset) — base is the min, max = base + (2^width − 1) or an explicit delta —
  and derive/drop the duplicate `NumberZoneMapRegion`. The SIMD kernels and `decodeValueAt` assume one width today
  (moderate, not trivial). **Cross-page page schema:** tag lists per region kind and the template pool are
  content-hashed and stored once per resource (a NamePage sub-trie, like the value dictionary), each page referencing
  its schema by a varint id — general for any record-shaped JSON, no page-size change; this is what removes the
  ~3–4 B/record fixed overhead and reaches M2.
- **(a) Structure as columns.** The documented `KIND_STRUCT_POINTERS` region (parentKey, left/right sibling,
  firstChild; only parentKey implemented today) via the existing `StructuralKeyColumnCodec` (2 bits/slot: null /
  key + stride / repeat / explicit; RLE for dense runs, FOR for the rest) plus elision of the revision fields on a
  page whose records share the page revision (constant columns). Derive the value-elision metadata (gap, type,
  width, region index are functions of what the page holds) and bit-pack the pathNodeKey / name-key dictionary
  ids. No dense-bitmap/exception special case, no path-summary derivation. Measure the heap composition first
  (B0 counters: non-elided value bytes by kind, inline-path pages, elision-metadata bytes).
- **(b) LAST, if the corrected diagnostic says the directory matters post-codec:** drop the per-record kind and
  template id (both already in the directory/slot ids); template-implied lengths; > 255 templates falls to the
  inline path with no elisions — count those pages.
Witnesses: golden-byte pins (`GoldenFormatTest`, `GoldenCompositePageTest`, sticky-codec reset rule), record
read/write differentials, region-only read tests, the section diagnostic at 1M, the 1M leg. Kill switch per lever.
Expected on the wire: body 6.8 → ~1.0–1.5 (d, a, b), regions 3.2 → ~2.0 (c), fixed overhead → ~0.5 (schema).

### P3 — Synthesized order labels (−12 B/row)
The KEYS segment's order-label lane: store (base label, stride, exception list) and derive offsets; readers are
codec-local (`decodeKeysView`, `KeysView.compareOrderLabelAt/copyOrderLabelAt`; consumers `ProjectionIndexFences`,
`ProjectionPersistedRecordLookup`). Acceptance ≤ 1.5 B/row. Record keys stay as they are (already delta-FOR).

### P-ET — Declared `TIMESTAMP` / `DATE` column types (−22 B/row; q23/q24 numeric sort; q6, q36–q42 numeric dates)
No detection exists in core (the loader's ISO check is bench-side and only tests `isString`). A declared type per
column with one canonical shape (19-char `dddd-dd-ddTdd:dd:dd`; 10-char dates) validated per value at build — a
non-conforming value is a build error, like a string in a long column — with an epoch numeric lane, an exact
formatter on emission (round trip witnessed), a literal→bound rule for partial string literals (prefix compares map
exactly or the numeric arm declines), and the substring shapes (`substring(…,15,2)`, `substring(…,1,16)`) as
arithmetic on the long. A DATE variant is required or q6/q36–q42 miss the lever.

### P2 — Global dictionaries for fat string columns (the speed lever; −110…120 B/row gross incl. DICT_HASHES)
The read side is already disk-resident (raw 256-entry value blocks behind a 3-byte radix; `ReadView` has a fixed
footprint). **The gate is the build:** the writer and probe front hold every distinct value in heap (twice on the
streaming load; budget 4 × rows × (avg + 52) ≥ 20 GB at 100M against a 2 GiB cap), post-pass builds are capped at
16,384 distinct per generation, ids are minted during ingestion into immutable FOR-packed leaves, entries compare in
UTF-16 order, and the header has no bulk/append boundary. Design (build-first, post-load): spilled distinct set →
rank pass in UTF-16 order → codes = rank → remap every leaf's id lane for the column (`convertStringDictColumnToGlobal`
is the leaf primitive) and drop its DICT / BLOOM / DICT_HASHES segments; reuse the existing raw block store (FSST'd
4 K blocks would break the zero-copy `sliceSlot`/`compareIds`/`stringOpVerdict` paths); add the boundary field;
appends take codes above it. **Executor scope, all today declining or comparing entries for `STRING_GLOBAL`:**
ungrouped COUNT(DISTINCT) (admit kind 5 in the numeric distinct arm — q5's bitmap is a new route, cheap), ungrouped
MIN/MAX, sorted scans comparing entries per heap comparison with best-first disabled and `sortColumnsOrderable` false,
`keyIsNumeric` refusing in-kernel ORDER BY on a global group key, zone-map skipping exempted for verdict predicates,
and `stringOpVerdict` sweeping every id single-threaded per query (q20/q23's `LIKE` is bounded by that sweep: make it
parallel and cached per query, or evaluate `contains` only on candidate rows of best-first-visited leaves — decide in
the design). Fallbacks if P2 slips: per-leaf STRING zone maps for best-first string sort keys; count-distinct over
`DICT_HASHES` sized by the path-statistics HLL; varint lengths + hashes only where the leaf dictionary dedupes < 2×
(10–15 B/row); per-leaf 3-gram blooms for `LIKE '%x%'`.

### R1 — Residency: headroom-gated retention, release at query end, no LRU
`retainedFillBytes` only grows (no decrement anywhere), the budget is a static maxMemory/4 that never consults
`HeapHeadroom`, the store is JVM-static across sessions, and fills are handed to running workers as arrays — an
LRU evicting under a running query would be a use-after-free. Σ hot ≈ Σ cold at 100M/8 GB, so retention buys little
there and causes the Full GCs and every state-dependent decline seen today. Rule: retain at publish time only when the
fill fits within a fraction of `HeapHeadroom`, otherwise serve windowed (already every route's fallback); release at
query end through a per-query pin count. The pass budget's `maxMemory/8` cap binds before the headroom share — move
both together.

### P4 by measurement · T2 dropped (per-region LZ77 and the body bake-off already exist; only LZ4-HC/zstd for the body blob remains a question) · C1 later (fallback-path cliff removal for arbitrary JSONiq; per-query fusion; ClassFile-generated plans first, Truffle as the general form)

## 4. Expected per-query effect, with the mechanism
| tier | today | after | mechanism |
|---|---|---|---|
| q23/q24 | 105 / 60 s | 1–3 s | P-ET numeric sort key + (P2) codes; `LIKE` verdict parallel/cached or candidate-only |
| q25/q26 (`ORDER BY SearchPhrase`) | 38 / 56 s | 1–3 s (P2) or 10–20 s (string zone maps) | P2 / fallback |
| q5 | 47 s | < 1 s (P2 new distinct route) or ~10 s (hash fold) | P2 / fallback |
| q20 | 23 s | 1–2 s (P2 with a parallel verdict) | P2 / 3-gram blooms |
| q18 / q32 | 109 / 88 s | 30–50 s | R1 headroom (cap + share) → fewer passes |
| q13/q14/q16/q17/q33/q34 | 22–38 s | 5–15 s | P2 codes as group keys, R1 |
| q6, q36–q42 | 0.5–5 s | sub-second | P-ET DATE |
| point/row-path queries | — | unchanged or faster | T1 (smaller pages), never slower |

## 5. Order of work — two 100M rebuilds
1. **B0 residual counters** (non-elided value bytes by kind, pages without a string region because of overflow,
   inline-path pages, elision-metadata bytes) + **full-accounting `ProjDiskDump`** in the bench package.
2. **B5-d** (cap + completeness) → **B5-c** (per-tag FOR + folded zone map, then the page schema) → **B3-a** →
   **B4-b** (only if the diagnostic says so); **B2** (order labels) and **B1** (`TIMESTAMP`/`DATE`) beside them on
   disjoint files; every step gated at 1M (pins, differentials, section diagnostic), template dedupe and the carrier
   share checked at 10M.
3. **Rebuild #1 at 100M** (load ~46 min + leg ~40 min, `-DbuildPathStatistics` on for the fat-column cardinalities)
   → **M1** measured: `sirix.data`, 43 dumps byte-identical to `results-vec-8g-prelever1`, Σ cold, `# served`.
4. **B6 (R1)** — query side, no rebuild, before #2's leg so the leg measures it.
5. **B7 (P2)** design (build-first) → two reviews → post-load build over #1's DB → **rebuild #2's leg** → the string
   tier gone; then the page schema's M2 measurement.

### Wave 3 — L1: schema-derived leaf size, measured (user's open question, decided by numbers, after T1 d+c)
Make the leaf slot exponent a resource-creation option (`Constants.NDP_NODE_COUNT` → `ResourceConfiguration`;
15 files; arena size classes extended; default 2^10 unchanged) **derived from the data, not declared per benchmark:**
the resource carries a target *records per leaf* (like rows per page/block elsewhere; explicit override allowed); the
bulk importer samples its first chunk (or reads the path summary it builds) for the average nodes per record and sets
`slotExponent = ceil(log2(target × avgNodesPerRecord))` before the first page is written (a 106-node row → 2^17 for
1,024 rows per leaf; a 12-node document → 2^14; a flat scalar array → 2^10). Streaming-insert resources without a bulk
load keep 2^10; a resource marked transactional keeps 2^10 regardless of shape (point reads, history, COW cost). Then
measure the grid records-per-leaf target ∈ {8 (today), 64, 256, 1,024} × {ClickBench 1M/10M, JSONBench Bluesky 1M/10M
— the nested, real-world-shaped counterpart}: `sirix.data`, column-scan
latency through the projection vs through per-tag region groups (with a region directory + range reads), point
lookup by key, update cost and history-read cost under SLIDING_SNAPSHOT (and FULL for contrast). Decision rules:
analytical resources drop the projection only if region-group scans land within ~1.5× of the projection at the
chosen slot count; the document-data default moves only if JSONBench shows the gain too. Representativeness gate for
every storage lever: report its byte gain on BOTH corpora.

## 6. Briefs (self-contained; one active writer; the lead reviews and runs the gates)
Common: explicit imports, HFT-style code, no format-version machinery (layouts change in place; golden pins
re-recorded with the sticky-codec reset rule), a `-Dsirix.<lever>.disable` kill switch (the cheapest mutation
witness), the mutation that must fail, the acceptance number at 1M, the test classes to extend, file ownership.
- **B0r — DONE 17:30** (impl-b0r): `PageSectionDiag` counters for U1–U4 (staged elision metadata by kind, staged heap
  by record kind with inline payload bytes, body path encoded/inline with reasons, value elision by index type,
  string region suppressed-by-overflow + stranded bytes, overflow-descriptor histogram, region bytes as written vs
  raw), `io.sirix.query.bench.projection.ProjectionDiskDump` (all segment kinds + descriptors + framing; 1M:
  115.49 B/row), `PageSectionDiagCountersTest` 4/4; one `build.gradle` line enables the gate for the core test task
  (assert-and-provide, like `sirix.hot.mergeDiag`).
- **B5-d — DONE 18:25 (impl-b5d), measured 18:36–18:47:** cap 512 → 1,023 (one source of truth, class-load check),
  per-TAG string-region completeness (suppressed-tag list behind the sign bit of `parentDictSize`; sketch withheld
  when a tag is suppressed; kill switch `-Dsirix.page.stringRegion.perTagCompleteness=false`). Structural acceptance
  met at 1M (region on 100 % of document pages, stranded 0, elision 99.9 %, descriptor pages 0.2 %) but the leaf
  class grew +58.6 MB: every newly elided slot writes the `appendValueElision` tuple (slot gap, type, width, region
  index ≈ 4–5 B; 3.65 B/record raw — TWICE the heap bytes elision removes). Found alongside: the sticky codec
  election wrote record pages with index pages' zero-run winner — **fixed by the lead (always compare zero-run and
  LZ77; kill switch `-Dsirix.codecBakeoff.stickyOnly=true`; `BodyCodecElectionTest`): leaf −5.3 %, file 1,854.3 →
  1,812.3 MB net, load time unchanged.** Under the fix the cap raise alone is leaf +18.2 MB (string region +13.8,
  body ≈ +3) against overflow −37.4 MB = file −16.8 MB (the "+110 MB body wire" was the section diag re-counting
  re-serialized pages; judge by the StorageProfile leaf class), completeness alone leaf +34.7 MB (the tuples).
  ACCEPTED. Consequence: B3-a starts with DERIVED elision metadata (bitmap + per-tag running rank + canonical
  widths) — the largest trie lever measured so far.
- **B5-d — cap + completeness (original brief).** Files: `settings/Constants.java`, `page/PageLayout.java`, `page/OverflowSlotSidecar.java`,
  `page/PageKind.java` (`stringRegionComplete`), `page/pax/StringRegion.java` (per-tag completeness). Witness: the
  counter above drops to 0 on a bulk fixture with long strings; region-only reads on pages with carriers; mutation:
  a page with an overflow record must not lose its other strings' elision. Acceptance at 1M: string-elision pages
  ≈ 100 %, carriers < 1 %.
- **B5-c — deliverable 1 DONE (per-tag FOR, kind 6; zone map V2 varint), measured 20:27 at 1M: number region
  1.49 → 0.67 B/record, zone map 0.63 → 0.38, leaf 680.2 → 559.2 MB (−17.8 %); cumulative file −32.1 % / leaf
  −48.9 % vs the wave-1 baseline. Deliverable 2 (string framing + plain lane) DONE, measured 20:46: string region 1.79 → 1.52 B/record, leaf
  559.2 → 528.6 MB — cumulative file −33.9 % / leaf −51.7 % vs the wave-1 baseline. Deliverable 3 measured the fixed
  per-page overhead (1.21 B/record; schema-sharable 0.37 → Proposal A parked, §8); deliverable 4 = the zone-map fold
  (Proposal B, 0.435 B/record) being built.**
- **B5-c — per-tag FOR + folded zone map; page schema (original brief).** Files: `page/pax/NumberRegion.java`, `NumberRegionCompact`,
  `NumberRegionSimd`, `BitUnpackSimd`, `NumberZoneMapRegion.java` (derive or drop), `PageKind.java` (region build /
  read), `page/pax/RegionTable.java`; the schema sub-trie in a NamePage-keyed store. Witness: parity of every
  region-only scan and zone-map prune; mutation: one width for all tags. Acceptance: regions ≤ 2 B/record at 1M;
  with the schema, fixed overhead ≤ 0.5 B/record.
- **B3-a — deliverable 1 COMPLETE, measured 19:58 at 1M: leaf 1,070.4 → 680.2 MB (−36.5 %), file 1,770.4 →
  1,376.1 MB (−22.3 %; −25.8 % vs the wave-1 baseline); staged elision metadata 6.43 → 0.71 B/record (value 0.13,
  name-key 0, pathNodeKey column 0.27 via `PathNodeKeyRegion`'s compact form); latent pathNodeKey sentinel defect
  fixed on the way. Deliverable 2 = MEASURED NEGATIVE (sibling columns +13.7 B/page on real records: LZ77 already collapses the
  constant deltas) → shipped dormant; revision elision + T1-b not funded. Deliverable 3 = post-codec attribution
  instrument (body 0.95 B/record on disk at 1M; regions 3.54 = 75 % of the leaf). Deliverable 4 = run-length lane +
  delta dictionary for the columns (pathNodeKey column 0.14 → 0.04 on the fixture). B3-a COMPLETE; d2–d4 in the
  combined wave-3 commit.** Launched 18:48 (impl-b3a), deliverable 1 = DERIVED elision metadata (elided-slot bitmap, per-tag
  running rank instead of a region index, canonical widths/types with exception lists, name-key width derived;
  kill switch `-Dsirix.page.body.derivedElision=false` proven against HEAD bytes; acceptance staged elision
  metadata ≤ 0.6 B/record and leaf class ≤ 950 MB at 1M), deliverable 2 = structure as columns + revision elision.
- **B3-a — structure as columns + derived elision metadata + revision elision (original brief).** Files: `page/PageKind.java`
  (`writeEncodedBody`, `BodySections.appendValueElision` / `appendNameKeyElision` and readers, the reinject site,
  flag bits, activation guards), `node/StructuralKeyColumnCodec.java` (+ its test), `page/pax/RecordOrdinalRegion.java`
  / the STRUCT_POINTERS region, `node/NodeKind.java`. Fixtures the mutation "assume predicted" must fail on: a deleted
  middle field, a moved subtree, a nested object crossing a page boundary, `{}`, an array of scalars, a
  `SLIDING_SNAPSHOT` fragment with 3 modified slots. Acceptance: heap ≤ 1.5 B/record on the wire at 1M.
- **B4-b — DROPPED on measurement (B3-a d3 attribution at 1M): the compact directory is 0.14 B/record post-codec
  (91.4 % of entries predictable, LZ77 already collapses them); templates+slotIds 0.16.** The whole encoded body is
  0.95 B/record on disk at 1M; the region table (3.54 B/record) is 75 % of the leaf class — B5-c's levers.
- **B4-b — directory residue (original brief).** Files: `page/PageLayout.java`, `page/PageKind.java`.
- **B2 — DONE 17:45** (impl-b2): the order-label lane's leading `int32` is a sign-discriminated marker (legacy ≥ 0
  byte-identical; −1 synthesized = anchors + packed tail deltas under a general byte-level rule; −2 front-coded);
  mode by encoded size per leaf; `decodeKeysView` allocation-free; kill switch
  `-Dsirix.projection.orderLabels.synthesized=false` proven byte-identical against a HEAD-compiled encoder; fixture
  KEYS 13.909 → 0.945 B/row (label lane 13.002 → 0.038). **1M acceptance met: KEYS 12.78 → 0.94 B/row, projection
  115.5 → 103.6 B/row (`measure1m.sh wave1`).**
- **B1 — DONE 18:52 (impl-b1), committed `32b7c2a37`:** kinds 6/7 in the long lane, `isOrderedLongKind` for the
  ordering kernels, exact literal→bound rule (prefix `eq` is constant-false — the brief's range rule was wrong),
  substring windows as idiv/mod, min/max from zone maps, kill switch `-Dsirix.projection.temporalKinds=false`.
  **1M acceptance met: EventTime 2.16 B/row, EventDate 0.03 B/row; file 1,854.3 → 1,770.4 MB with B5-d + the
  codec fix.** Wave-2 gates: full query suite green; full core green after re-basing three overflow fixtures on the
  cap constant (`194e52299`); one pre-existing order-dependent Mockito flake noted.
- **B1 — `TIMESTAMP` / `DATE` (original brief).** Files: `ProjectionIndexBuilder.java` (`mapTypeToColumnKind`), `ProjectionIndexRowGroupPage.java`,
  `ProjectionIndexColumnSegmentCodec.java`, `SirixVectorizedExecutor.java` (emission formatter, literal→bound rule,
  substring arithmetic, the ISO-minute arm), harness `ClickBenchProjection.projectionType` declares the types.
  Witness: exact round trip on every value (mutation: a formatter that drops seconds); the sorted-scan, group and
  min/max differentials on timestamp and date columns; a non-canonical value fails the build. Acceptance: EventTime
  ≤ 3 B/row, EventDate ≤ 0.2 B/row at 1M.
- **B6 — DONE, committed `5e5f281c0` (impl-b6):** `HeapHeadroom.plannedShareBytes` = min(max/8, headroom/4) is the
  one figure for the group budget, the distinct ceiling and the store's residency budget; `ProjectionResidencyScope`
  pins published/resident columns per open query scope and releases the largest unpinned lanes at the scope's exit
  (no LRU, no timers); kill switch `-Dsirix.projection.residency.headroom=false`. Gates: full query green, full core
  green modulo the already-fixed cap fixtures. 100M A/B to watch: the default residency budget is now ≤ max/8.
- **B6 — R1 (original brief).** Files: `ProjectionColumnStore.java` (publish-time headroom gate, per-query pin count, release),
  `ProjectionIndexCatalog.java`, `GroupTableSpill.java` / `HeapHeadroom.java` (cap and share together). Witness: a
  fill over the headroom fraction serves windowed and retains nothing; released bytes return at query end; the pass
  count on a q18-shaped fixture drops when headroom is raised. No rebuild.
- **B7 — P2.** Design document first, build-first (spilled distinct set, UTF-16 rank pass, leaf id remap, boundary
  field, raw block reuse, the executor site list above, the verdict strategy), two reviews, then build. Acceptance:
  the four fat columns ≤ 30 B/row total at 100M; q5/q20/q23–q26 within §4.

## 7. Risks and open points
- T1-c's page schema is the lever that reaches M2; if it slips, M1 (~45–50 GB) stands and M2 needs the
  dictionary-coded-strings decision.
- P2 is the outcome's critical lever for speed; the build-side design is the risk; fallbacks are scheduled.
- The fat-column count-distinct route materializes Strings (`count(distinct-values(URL))` at 100M OOMed an 8 GB
  heap in a probe): P2's new distinct route fixes it; until then the route must decline on a headroom check.
- Cardinalities of URL/Title/Referer come from rebuild #1's path statistics.
- Answered by B0r's counters (1M, 17:53): **U1** the wire body = elision metadata (5.83 B/record raw: value 3.23,
  pathNodeKey column 1.39, name-key 0.90, hash bitmap 0.11, parent column 0.19) + four structural varints + kind/template
  + inline strings (3.0 B per string slot on the pages without a string region), compressed together at 0.394;
  **U2** overflow descriptors are per page: 11.8 % of document pages hold ≥ 1 (3.5 % one, 5.9 % two–three, 2.4 % four+)
  and lose the string region — 3.75 M stranded values, 53.7 MB; **U3** value elision is active on 88.1 % of DOCUMENT
  pages, refused on 11.9 % (the overflow pages); the 15,887 inline-path pages are NAME/PATH_SUMMARY index pages;
  **U4** regions as written 4.18 B/record: number 1.45 (LZ77 0.212 of raw), string 1.45 (0.604), zone map 0.61,
  name-key 0.51, sketch 0.08, ordinal 0.07. Still open: U5 (cardinalities → rebuild #1 path statistics), U6 (q23 tie
  behaviour at 100M).

## 8. Parked (user decisions on record)
- **T1-d page schema (B5-c Proposal A, measured 20:41):** a NamePage-keyed, content-hashed region-shape descriptor
  (capped pool, inline fallback) would cut the schema-sharable 0.37 B/record of per-page framing; with the zone-map
  fold (Proposal B, being built) the fixed overhead reaches ≈ 0.54 B/record, below 0.5 only with schema-declared
  string length modes on top. Parked: generality is fine (data-driven, capped), but the gain is below P2's.
Configurable slots per leaf (a per-resource counterpart of picking a larger page class; costs: COW amplification,
point-lookup I/O, arena size classes, `NDP_NODE_COUNT` in 15 files) — 1,024 stays for point-query / history /
reconstruction resources; scheduling L1 after T1 with a measured gate is open. One record per JSON object — a
data-model change, not planned. Dictionary-coded fat strings inside fused records — the alternative M2 route.
