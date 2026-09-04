# ClickBench correctness + HFT campaign — resumption plan (2026-08-29, post-crash)

Status: **REVISION 2 — FINAL, implementation starts.** §2 is done and gated. §5's draft carries the
round-1 corrections (§10) and was verified correct by round 2; §4 and §6 are the round-2 designs.

Revision log
- r0: initial plan (crash RCA, three reds, P2 draft, protocol).
- r1: folded review round 1 — P1 (executor): A/B/C insufficient, two wrong-count classes (§3.1,
  §4 R1/R2), witnesses redesigned; P2 (storage): 11 invariants confirmed, `deepCopy` pending-ref
  hazard, silent inertness, unobservable cap, cadence (§5, §10); protocol rewritten (§6): telemetry
  gate, exact row count, query-leg envelope, disk sequencing, single-log GC check, 1M pre-check.
- r2: folded review round 2 — P2: all seven corrections verified; ISE kept (R8); importer failure
  unwind required (§5.6); staging precondition mirrored; `deepCopy` sharing pinned by a unit test.
  P1: page-wide purity must be narrowed to OBJECT/ARRAY children and paired with a READER-side
  promise check (else poisoned pages serve 0 — F1); orphan-run rules (W3); R1 must thread the
  trailing gap into `settleTrailingSeamOnly`; R2(i) is a HEAD bug fix (phantom boundary ≥ 2);
  fixtures corrected (7-node parity, J = 1023, same-name W2, F1/W3 pages, (f) declines); duplicate
  keys excluded. Protocol: `--strong --bounded-oracle`, one DuckDB run with both candidate dirs, CLI
  flags not env vars, generic leg flags, SIGTERM-only kills, `globalDictColumns=0` confirmation.
  Order of work changed: P2 → gates → 1M pre-check → 100M diagnostic → 100M load (P1 coded, not
  compiled, while it runs) → queries → P1 gates during the DuckDB reference.

## 0. Mandate and standing constraints (verbatim intent, unchanged)

Goal: all 43 ClickBench queries execute through the intended structural/vectorized routes and match
DuckDB exactly; ingestion and query code stay incremental, correct for every `VersioningType`, and
HFT-grade with no major/full GC.

Constraints: preserve every existing change; never reset/clean/stash/rebase/switch/checkout/delete
unrelated files/commit/push; one writer; Gradle strictly serial; no claims without artifacts; do not
weaken fail-closed semantics to make a test pass; every guard needs a positive witness and a
mutation that turns it red.

## 1. State at resumption

**The crash.** 18:29 local: the previous session launched the 100M load in the background with the
`clickBenchLoad` defaults (24 GiB off-heap arena, `-Xmx12g`) on a 31 GB host, while a Gradle test
JVM ran in the foreground. 18:35:47 the kernel OOM-killer shot the load JVM (23.4 GB RSS, 250 GB
virtual); the session's queued task notifications were never processed. The load's own log shows the
projection was ABANDONED at 18:30 (AUTO had elected fat string columns; the launch lacked the
`-Dclickbench.expectedRows` hint that lets the election decline them — §3.3). That run also had a
profiler attached and telemetry off: its gc.log (1.26 s mixed pause at 253 s) is NOT gate evidence.

**The tree.** 403 modified / 33 deleted / 98 untracked files: Codex's work (Aug 28 08:04 → Aug 29
08:00) plus the crashed session's (Aug 29 09:22 → 18:28). Nothing to repair. `origin/main` =
`257430dd8` (PR #1185); HEAD is 14 commits ahead of it. The tree is dirty and stays dirty: anything
that demands a clean build identity (§6, P-1) is out of reach for this campaign.

**Campaign status before the crash.** Central 1M gate MET: 43/43 under
`--require-vectorized-serving`, strong compare `0 mismatch, 0 missing`. Full core suite green. Query
suite: three deterministic reds. 100M: not started. One untested edit landed minutes before the crash
(coordinator-lane abandonment tolerance in `ProjectionBulkLoad`/`ParallelBulkJsonImporter`).

## 2. Closed this session — with witnesses

| Red | Root cause | Fix | Witness |
|---|---|---|---|
| `JsonIntegrationTest.testNesting19` | New `requireSameDefinition` guard fires at COMMIT (`reInstantiateIndexes`) because a definition is compared with its own persisted copy: `IndexDef` persists `Path.toString()`; brackit parses `foo` with a CHILD step, prints `./foo`, re-parses it as CHILD_OBJECT_FIELD — `Path.equals` is false | `IndexDef.hasSameDefinition` compares paths/projection fields in PERSISTED form (printed text). No index-content change: both spellings are illegal match patterns (`Path.matches` throws/false) | `IndexDefPersistedDefinitionTest` (4): red 3/4 on old code, green on new; `JsonIntegrationTest` 93/93 under Gradle |
| `PinnedTrieProjectionSpillColdReopenTest.fourResources…` | Child JVM (no `--add-opens`) died on its FIRST page read: dirty-tree `HashAlgorithm.computeHashLong(ByteBuffer)` called openhft `hashBytes(ByteBuffer)`, whose linkage resolves `sun.nio.ch.DirectBuffer` → `IllegalAccessError`. Gradle's worker flags masked it; any plain JVM would fail | Every non-array buffer hashes through `MemorySegment.ofBuffer` + the existing `HashAccesses.SEGMENT` kernel (pure FFM, no Unsafe, bit-identical); heap buffers through the byte[] kernel with `arrayOffset+position` | `HashAlgorithmBufferAccessTest` (2) green WITHOUT add-opens (red before); the saturation child exits 0 with `saturatedActiveWorkers=1 queuedTasks=1 admissionWaiters=2`; class 20/20 under Gradle |

Follow-up, out of scope: bare-name index paths are illegal `Path.matches` patterns in brackit
(`./foo` throws for deeper targets; XML `foo` NPEs in `PathParser.name`). Rejecting relative paths at
definition time is a behaviour decision for the user.

## 3. Open problems — root causes and evidence

### 3.1 P1 — `ArrayContainsPredicateTest`: the column route is never attempted, and lifting the gate alone would make it wrong
Evidence: instrumented scratch copy of the executor — every scan prints `arrayElementPathKey=6`
(resolves) but `structuralSourceMatcherNull=false`; decline counters 0; `regionOnlyPagesServed=0`.
Provenance: HEAD's gate is `singleArrayContains && arrayElementPathKey > 0 && columnarWorthIt` (0
refs to `structuralSourceMatcher` in HEAD, 41 in the tree); HEAD's `computeTargetPathNodeKey`
answers −1 ("unscoped") for an ARRAY-valued field. The dirty tree's fail-closed matcher rule turns
that −1 into a mandatory cursor-ancestry proof, which the page-only route cannot give → dead route.

Round-1 review facts the design must respect (file:line in `SirixVectorizedExecutor`):
- Anchors are selected by NAME KEY only (9333); the regions-only page has no per-slot path-key
  column, so the layer key scopes VALUES (tag lookup 9310), never the anchor slot.
- Element strings are tagged with their parent ARRAY slot's path key (`KeyValueLeafPage`
  6676-6712); orphan elements (array spilled from the previous page) carry NO path identity
  (`TAG_ORPHAN_ELEMENTS`).
- `decideOneArrayContains` (the seam record path) is reached ONLY when the orphan lookup answers
  UNDECIDABLE (9501, 9519, 9551); `ORPHANS_CONTAIN` settles a seam from columns with no scope proof.
- Any STRING_VALUE under a non-fused array drops the whole page's element staging
  (`elementsUsable=false`, 6699-6703).
- The certificate's boundary term is `next − here` over record ordinals (9379); ordinals are
  assigned to the immediately enclosing OBJECT in first-appearance order.

Two wrong-count classes survive the naive fix (A/B/C): **(W1)** a nested same-name array whose node
is the page's LAST populated slot with its elements spilled to the next page — the trailing seam is
settled from the orphan tag (+1) without any ancestry proof; **(W2)** certificate cancellation on one
page — a mixed-type array over-counts `covered` (non-string elements are in the gap but not in the
tag) while an array-of-objects-then-string or a fused OBJECT-valued anchor under-counts it, so
`covered == elementCount` holds and the values are attributed to the wrong segment.

### 3.2 P2 — 100M arena exhaustion: leaves with unresolved overflow carriers are pinned until final commit
Diagnostic run (8 GiB arena, `-Xmx8g`, hint set, alone) died at 100 s with `FrameSlotAllocator: size
class 4 exhausted` after 3.6 GB on disk. Allocator dump: class[4] `live == freshIdx == 121,727`
frames = the whole arena is a HELD working set. Heap histogram at 60 s: 36,358 `PageContainer`,
70,047 `KeyValueLeafPage`, 42,067 `TransactionLogReference`, **34,646 `DataRecord[]` + 92,544
`ObjectNamedStringNode`** — every page written so far is still in the intent log; ~95 % of them carry
heap-resident refused records.

Mechanism (verified): `adoptDocumentLeafPage` leaves the builder's cold direct-write fallbacks in
`records[]` and therefore does NOT mark the page immutable-for-flush → the flush lane `deepCopy`s it
every epoch → `serializeDisposablePage` runs `processEntries` on the copy → each refused fused
record becomes a canonical overflow carrier with a NULL key → `hasUnresolvedOverflowReferences` →
`SNAPSHOT_PROMOTE_TO_TIL` → `cleanupSnapshot` PINS it (pinned entries are never retried by design).
With ~4.8 % of ClickBench records over the 512-byte fused cap (the q20 fix correctly stopped
generic-inlining them), ~40 % of pages pin for the whole load: 100M rows ≈ 250 GB of frames. The 1M
gate never saw it. The crashed run's 65 MB/s heap growth is the heap side of the same pinned set.

The writer already has the bounded single-owner lane for immutable pages written before the root
(`stageUncommittedOverflowPage`, native reservoir, `writeSnapshotSidePages`, keys published by
`SidePageBatch.publishCompletedWrites()` immediately before `log.cleanupSnapshot()`); the projection
HOT storage uses it. KVL overflow carriers do not.

### 3.3 P3 — launch-protocol defects (all confirmed against the code this round)
1. `-Dclickbench.expectedRows` is what makes AUTO decline the fat string columns; the value that
   passes post-load acceptance is the official count **99,997,497** (`README.md:102/120`,
   `ClickBenchLoadMain:190`); `100000000` fails `ClickBenchProjectionAcceptance` AFTER the ~2 h load.
2. `-Dsirix.hft.telemetry=true` makes `ClickBenchLoadMain` call `HftRuntimeEvidence.capture`, which
   demands `-Dsirix.hft.gitSha`, a clean embedded build identity equal to HEAD, an EMPTY
   `git status --porcelain`, and gc+safepoint unified logging on stdout — it throws before
   `HFT_MEASURE_START` on this tree. Campaign runs go without HFT telemetry.
3. Envelope: `clickBenchLoad` defaults 24 GiB arena + `-Xmx12g`; the `clickBench` (query) task is
   `-Xmx12g` and `ClickBenchRunMain` defaults to a 24 GiB arena too — 36 GiB per JVM on a 31 GB host,
   in BOTH phases. The arena RSS is a never-decommitted high-water mark.
4. Disk: 141 GB available; a 100M DB is ≥ 137 GB (the pre-fix figure is a floor: more carriers
   now); the DuckDB reference needs ~20.5 GiB table + temp; no 100M DuckDB reference exists on the
   box (only 1M). The two cannot coexist → sequencing in §6.
5. The abandonment-tolerance edit has no test; with the hint it will not fire on ClickBench, but it
   is production code (§5.5).
6. The seven STRING_GLOBAL-served shapes (q20-23/27/28/39) have never run under
   `--require-vectorized-serving` with the global dictionaries DECLINED — which is the 100M state.

### 3.4 P4 — measured, not assumed
Steady-state RSS, TIL depth, epoch cadence and GC pauses of the 100M load after P2; flush-lane cost
of ~4.8 M extra side pages (~2.9 GB through 2×64 MiB reservoirs); anything else scaling with rows.

## 4. Design — P1 (ArrayContains column route; round-2 final; IMPLEMENTED 2026-08-30 — see §4.1 for the corrections the implementation forced)

Ground truth the design rests on (round-2 verified, `SirixVectorizedExecutor` = E, `PageKind` = PK,
`KeyValueLeafPage` = KVL): anchors are selected by NAME KEY only (E:9333); element strings are
tagged with their parent ARRAY slot's path key; a page's element staging is published under the
promise encoding `ENC_DICT_BITPACKED_ZM_ELEMENTS` only when it is complete; orphan elements (array
spilled from the previous page) carry no path identity; the record ordinal = immediately enclosing
OBJECT, first-appearance order; the seam record path is reached only when the orphan lookup is
UNDECIDABLE. Element staging is written at TWO sites — the writer (PK:3697-3770, promise byte
PK:3913) and the derive site for versioned multi-fragment pages (KVL:6653-6776) — and every rule
below applies at both.

**A. Scope resolution.** `resolveCountPathScope(sourcePath, field).anonymousArrayPathNodeKey()`
(E:5687-5771): ancestry-exact, ambiguity-aware, cached; `arrayLayerPathNodeKey` goes away.

**B. Gate.** Single-array-contains branch: `arrayElementPathKey > 0 && columnarWorthIt`. Comment:
the element key proves scope for column-decided VALUES; anchors, seams and attribution rely on
R1/R2/PC below.

**C. Seams through the records.** `decideOneArrayContains` positions via
`positionOnScopedAnchor(rtx, arrayKey, matcher)` (E:5620-5626) — the same helper as the record
fallback, so the two cannot drift — then evaluates.

**PC. Reader-side promise check (closes F1 and the latent flag-off serve).** The route certifies a
page only if `sh.encodingKind == StringRegion.ENC_DICT_BITPACKED_ZM_ELEMENTS` (new decline reason
`ELEMENTS_NOT_STAGED` beside E:9307). Without it a page whose element staging was refused (no tags,
`elementCount == 0`) with anchors whose gaps end at a nested object key balances the certificate at
0 and serves 0 for records that do match.

**R1. Seam scope proof (closes W1).** An anchor with an EMPTY on-page gap is never settled from the
orphan tag: trailing (E:9494-9503), leading==trailing (E:9510-9521) AND `settleTrailingSeamOnly`
(reached with `elementCount > 0` via the DICT_ID_ABSENT exit E:9433-9439 — thread the trailing gap
`populatedInRange(from+1, 1024)` into it) branch on `gap > 0`; an empty-gap seam goes to
`decideOneArrayContains` with the matcher. A non-empty trailing gap that balanced the certificate
carries the layer key ⇒ scope proven ⇒ the orphan lookup stays sound. Cost: one record rebuild per
page whose last populated slot is an array node — (array nodes per record)/(nodes per record) of
pages, not 1/1024.

**R2(i). Certificate arithmetic (a HEAD bug fix).** `boundaries = next > here ? 1 : 0` (E:9379):
ordinals are first-appearance by parent, so `next > here` means the next object key is its parent's
FIRST field and that parent is either the single bare OBJECT in the gap or the anchor itself; the old
`next − here` subtracts phantom objects (≥ 2 after leaving a nested object) — a real under-count.
Per-segment sanity on the segment's OWN `populatedInRange` before subtraction, non-trailing segments
only: `populated < boundary` (a fused OBJECT-valued anchor) → decline.

**R2(ii). Writer-side purity, NARROW (closes W2 without breaking test (i)).** Element staging for the
page is refused (`elemUsable = false`, no promise) when: a slot of kind OBJECT(24)/ARRAY(25) has an
on-page parent of kind OBJECT_NAMED_ARRAY(53). Scalars (27/28/29) are NOT poison — they sit inside
the gap and over-count into a decline; poisoning them would kill every page of
`ArrayContainsPredicateTest` (`"tags":[3,true]` on every record). Per-array poisoning is UNSOUND
(the array's strings become invisible outside its truncated gap); page-wide is the lever, paired with
PC. Parent lookup exists for every kind (`getSlotParentKey`, `onPageParentSlot`,
`PageLayout.getDirNodeKindId`); publication is deferred, so the rule is order-independent.

**W3. Orphan-run rules (closes the spilled-array under-count).** Also refuse staging when: (a) a bare
scalar (27/28/29) has an OFF-page parent (only array elements have these kinds); (b) a bare
OBJECT/ARRAY has an off-page parent whose key is not the top-level container's (over-refuses
`{"data":[…]}` page heads: exact, slower; never under-refuses); (c) an orphan STRING's parent key
differs from the first orphan's (nested spill). Without (a), `genres:["Comedy", 3×2043, "Zzz"]` gives
page N+1 all numbers, tag absent → ORPHANS_ABSENT → "Zzz" never counted.

**R8. Hygiene.** `scratch.clearPendingBoundary()` at the entry of `countArrayContainsFromRegions`.

**Exactness after A/B/C/PC/R1/R2/W3 (round-2 verdicts):** (a) nested `meta.genres`: EXACT (foreign
tags over-count → decline; empty → 0; trailing empty → matcher). (b) nested last-slot spill: EXACT
(R1). (c) arrays of objects: EXACT (R2(ii)+PC decline; mixed scalars decline). (d) EXACT. (e)
string-valued anchor: EXACT; OBJECT-valued: R2(i) sanity declines. (f) `[]/items/[]` with foreign
top-level `genres`: EXACT but DECLINES on essentially every page (item OBJECTs are OBJECT children of
the fused `items` array → page-wide poison); serving it needs a per-path-key poison list in the region
header — an in-place format addition the standing ruling allows, deferred. (g) no summary: EXACT.
Leading+trailing combinations: EXACT. Trailing anchor with elements on both pages: EXACT. Duplicate
keys in one record: pre-existing three-way divergence (column route 1, record fallback up to 2,
brackit 1) — excluded from the differential and ledgered.

**Tests.** (i) `ArrayContainsPredicateTest` green with `regionOnlyPagesServed > 0`.
(ii) New `ArrayContainsScopeDifferentialTest` (sirix-query). `ARRAY_ELEMENT_STRINGS_IN_REGION = true`
BEFORE each `jn:store` (read at serialization and derivation); `ARRAY_CONTAINS_COLUMNAR_ENABLED`
per scan; reset the region counters before each served-pages assertion. `jn:store` shreds
sequentially (keys in document order: root 0, top-level array 1, records from 2 — deterministic).
- Corpus A: top-level `genres` arrays; sparse nested `meta.genres` (≈1 per several pages); nested
  objects AFTER the top-level array; disjoint literals per level; scalars allowed inside arrays.
  Assert route == generic pipeline for nested-only / top-level-only / shared literals;
  `regionOnlyPagesServed > 0`.
- Corpus B (m1): 1,200 × `{"id":i,"meta":{"genres":["N"+(i%3)]},"z":i}`; truth 0; with the chain
  filter removed in `computeCountPathScope`: `> 0` served and `> 0` count.
- Seam fixture: 7-node records both ways — `{"genres":["Top"],"meta":{"x":1,"y":2},"f":1}` vs
  `{"genres":["Top"],"meta":{"genres":["Nested"]},"f":1}`; `key(x_J) = 7J+6 ≡ 1023 (mod 1024)` ⇔
  `J = 1023`: `meta_J.genres` at key 7167 = slot 1023 of page 6, "Nested" at slot 0 of page 7; assert
  the shape. U: without R1 the count for `'Nested'` is 1 vs truth 0 (via the DICT_ID_ABSENT exit).
  P (`"p":[["x"]]` in J+1): m2 witness only — dropping the matcher in `decideOneArrayContains` flips
  0 → 1.
- F1 page: every `genres` on one page is `[{"a":1},"Drama"]` → truth N; without PC the route serves 0.
- W2 page (same field name!): `genres` = `["Comedy",3,4]`, `[{"a":1},"Drama"]`, `[{"a":1},"Drama"]` →
  declines-or-exact (truth 2).
- W3 page: first record `{"genres":["Comedy", 3×2043 numbers, "Zzz"]}`; query `'Zzz'` → 1; without
  rule (a) → 0.
- R2(i) page: `{"meta":{"x":1},"genres":["Drama"]}` records — HEAD's `next − here` subtracts 2 →
  VALUES_UNCLAIMED on every page; with the fix → served and exact.
- (e) pin: `$m.genres[]` over a string-valued field returns empty in the generic pipeline.
- (f) fixture expects declines (`regionOnlyPagesServed == 0`) and exact counts.
(iii) `NoPathSummarySourceScopeDifferentialTest`, `ArrayPageRangeSequenceOverflowTest`, scan suite
subset green. Both flags keep their defaults (off): no production exposure today.

### 4.1 Implementation record and corrections (2026-08-30)

Implemented as designed: A (`resolveCountPathScope(...).anonymousArrayPathNodeKey()`, `arrayLayerPathNodeKey`
deleted), B (gate without the matcher), C (`decideOneArrayContains` positions via `positionOnScopedAnchor`
with the matcher, then re-positions — the matcher moves the cursor), PC (`ELEMENTS_NOT_STAGED`), R1 (trailing gap
threaded through all three seam sites incl. `settleTrailingSeamOnly`), R2(i) (`next > here ? 1 : 0` +
`GAP_NARROWER_THAN_BOUNDARY`), R2(ii) at BOTH staging sites via one shared helper
`KeyValueLeafPage.elementStagingStaysPure` (bare OBJECT/ARRAY whose on-page parent is a fused or bare array →
page-wide refusal), R8.

**Not kept — W3(a) and W3(c).** Both were implemented, mutation-tested and found unobservable, then removed
rather than left as unwitnessed guards: (a) an all-scalar orphan page has NO string column, and
`orphanElementsContain` already answers UNDECIDABLE there (record path decides); the W3 fixture — restructured so
page 0 is actually SERVED (1020 strings fill it; the plan's `"Comedy", 3×2043 numbers` put numbers inside the
anchor's own gap, which unbalances the certificate and declines page 0 before any orphan rule matters) — is exact
with the rule on and off. (c) a nested spill's parent array poisons its own page under R2(ii) before the reader
reaches the orphan run. W3(b) (bare OBJECT/ARRAY with an off-page parent that is not the top-level container) is
kept as the off-page arm of R2(ii).

**(e) was wrong in the design.** `$m.genres[]` over a string is NOT empty in the interpreter: brackit raises
XPTY0004 (`Illegal operand type 'xs:string' where 'array()' is expected`), and the auto-wired route answered 0 —
kernels AND record path. Closed at planning time: `acceptsPredicate` declines an `ArrayContains` field whose
scoped named path node has more references than its anonymous ARRAY child (a scalar/object/null value exists
in this revision) or that is unscoped; `arrayContainsAt` throws `BIT_DYN_INT_ERROR` if it ever meets a
non-array anchor (the summary admitted the field as array-only). Witnesses: the (e) pin (outcome equality —
`error=err:XPTY0004` on both routes) and `mixedArrayAndStringFieldRaisesInBothRoutes` (1 in 5 records
string-valued; proves the reference-count formulation on fused nodes).

**Fixture corrections.** Numbers or objects INSIDE the queried array are type errors for the interpreter
(XPTY0004 / FOTY0012), so F1, W2 and W3 assert truth by construction and corpus A keeps its mixed elements in an
unqueried `tags` array. The seam fixture needs FLAT filler records: `RecordOrdinalRegion.encode` refuses a
spanning record whose object keys run off-page, on-page, off-page (`meta:{x,y}` then `f`), so with the plan's
7-node shape page 6 was never served (`NO_RECORD_LINKAGE` on 5 of 10 pages) and the mutation arms could not bite.
F1 carries a fused string field (`"t":"s"`) so the page keeps a string column and the promise check is what
stands between refused staging and a served zero.

**Seams (test-only; production values unchanged):** `SirixVectorizedExecutor.SEAM_SETTLE_EMPTY_GAP_FROM_ORPHANS`
(R1), `SEAM_UNSCOPED_SEAM_RECORDS` (C), `SEAM_SKIP_ELEMENT_PROMISE` (PC), `KeyValueLeafPage.ELEMENT_STAGING_PURITY`
(R2(ii)). Witnessed by inversion: R1 (seam-U: 0 → 1), C (seam-P: 0 → 1), PC (F1: 600 → ≠ 600). R2(ii) has no
witness reachable through a valid query (an OBJECT element makes the interpreter raise) — kept as the
reviewer-required safe direction; its only observable effect is declining the (f) shape on every page.

**Known divergence, pre-existing and ledgered (not ClickBench-reachable):** for non-string elements the column
route and the record path answer "no member" where the interpreter raises (XPTY0004 for numbers, FOTY0012 for
objects). Closing it at planning time would need value-kind counts under the array layer, which the summary does
not keep.

**Results (out-of-tree rig, 2026-08-30):** `ArrayContainsScopeDifferentialTest` 11/11,
`ArrayContainsPredicateTest` 4/4 with `regionOnlyPagesServed > 0` (the vacuous red of §3.1 is closed),
`NoPathSummarySourceScopeDifferentialTest` 4/4, `ArrayPageRangeSequenceOverflowTest` 1/1. Gradle gates follow the
100M load (nothing heavy runs beside it).

## 5. Design — P2 (overflow-carrier staging at adoption; DRAFT with round-1 corrections, compiles)

### 5.1 The change (three files)
1. `KeyValueLeafPage`
   - `materializePendingRecords(config)`: runs `processEntries` now; every entry is consumed by
     contract (inline, carrier, or bound-flyweight shortcut) and a survivor THROWS; `records = null`.
   - `overflowReferenceState()`: RESOLVED / PENDING_SIDE_WRITES / UNRESOLVED (one map pass).
   - `flushDeferrals` (byte) + `noteFlushDeferral()`.
   - `deepCopy()` SHARES a pending carrier reference instead of cloning it (the copy constructor
     refuses pending references; the HOT leaf CoW follows the same rule).
2. `NodeStorageEngineWriter`
   - `adoptDocumentLeafPage`: materialize (BEFORE the trie is touched) → `prepareLeafOfTree` → append
     → `stageAdoptedOverflowCarriers` → `markAdoptedImmutableForFlush()` unconditionally.
   - `stageAdoptedOverflowCarriers`: backend gates evaluated once per writer
     (`carrierStagingSupported()`, WARN once per JVM naming `sirix.commit.preallocated` /
     `sirix.arena.strategy`), carriers larger than one batch counted separately, staged ones through
     `stageUncommittedOverflowPage`.
   - Flush worker: state classified once; PENDING_SIDE_WRITES and `flushDeferrals < 2` → skip
     serialization, `SNAPSHOT_RETRY_NEXT_EPOCH`; an adopted RESOLVED page skips the serializer's
     redundant reference pass (`carriersKnownResolved`); a PENDING page reaching the promote path past
     the cap is counted (`kvlPagesPinnedAfterDeferralCap`).
   - Diagnostics: `adoptedCarriersStaged/Unstaged/Oversized`, `kvlPagesPinnedAfterDeferralCap`;
     HFT line gains `kvlDeferredPages` so attempted == written + promoted + deferred. Test seams:
     `STAGE_ADOPTED_OVERFLOW_CARRIERS`, `MAX_KVL_FLUSH_DEFERRALS` (package-private, non-final).
3. `TransactionIntentLog`
   - `SNAPSHOT_RETRY_NEXT_EPOCH`; `cleanupSnapshot` re-promotes with the ordinary cross-generation
     `put` (never pins); `kvlPagesPinnedByPromotion` / `kvlPagesRetriedNextEpoch` diagnostics.

### 5.2 Invariants — all eleven CONFIRMED by review round 1 (file:line in the review record), with
these wording corrections: I1 — the side-only epoch that may start inside staging never snapshots the
TIL but DOES clean and re-promote other deferred leaves (same thread, safe); I2 — `readOverflowPage`
swizzles a heap page onto a COMPLETED carrier (page field only, benign); I10 — "immutable" means
frame + slots + reference-map structure; three post-mark object-level mutations remain
(`noteFlushDeferral`, carrier completion, the `addedReferences` flag).

Q2 (review): a frozen leaf's pending carriers are always in the SAME epoch's side batch
(`log.snapshot()` and `rotateSidePageBatch()` run back-to-back under the permit; staging happens only
between epochs on that thread), so exactly ONE deferral is reachable; the cap of 2 is slack that
converts a future ordering regression into a counted pin.

### 5.3 Cost model (HFT lens)
Per adopted page with carriers: one reference-map pass, one `copyToNative` per carrier (~600 B), one
deferred epoch of residency. Removed: the 64 KB `deepCopy` per epoch for ~40 % of pages, the pinned
frame until commit, `records[]` + snapshot-node heap (142 MB per minute of load). Side-page volume at
100M ≈ 2.9 GB. Cadence: re-promoted leaves count toward the 16-entry epoch boundary
(`isAsyncFlushLogBoundaryReached`) → ~1.6× more combined epochs at 40 % deferral; decided from the
diagnostic (`hftCombinedEpochs` is unavailable without telemetry — use the epoch count from the
allocator/TIL diagnostics and the `sirix.asyncFlush.maxLogEntries` property), not by new policy code.

### 5.4 Decisions (round 2 arbitrated)
- R8: KEEP the `IllegalStateException` — a read through a closed staged view can only happen on a view
  held outside its reference (publication nulls the page and closes the view in one transition); it is
  an ownership bug, and a `SirixIOException` would be swallowed by the reader's catches into
  `return null` = a silently wrong query answer.
- R10 (follow-up, not blocking): serialize carriers straight into the side reservoir.
- Q3-d (review): the writer's `getRecord`/prepare paths refuse adopted pages; pages with cold
  fallbacks (~40 %) were readable/CoW-able in the load transaction before P2 and are refused now — the
  refusal's own contract; no in-tree caller. Ledgered.

### 5.6 Round-2 required additions (implemented with §5.5)
- Importer failure unwind: `adoptDocumentLeafPage` retires the page itself (`retire()` =
  `markOrphaned` + idempotent `close`) if it fails BEFORE `appendLogRecord` — after that the intent log
  owns it and rollback closes it; `ParallelBulkJsonImporter` retires the untouched remainder of a
  burst, every completed in-flight build's pages (cancelling pending futures) and the held tail page
  on any coordinator failure, then rethrows. Pre-existing leak (frames outside the TIL survive the
  process), one site wider now.
- `stageAdoptedOverflowCarriers` mirrors the full staging precondition (`logKey == NULL_ID_INT`, no
  transaction-log handle) so a malformed carrier is counted and skipped, never thrown after the page
  is already in the live log; the unreachable trailing `else` becomes a distinct counter.
- `KeyValueLeafPageDeepCopyPendingReferenceTest`: a page holding a `bindPendingPageWrite` reference →
  `deepCopy()` shares it (`assertSame`), non-pending ones are cloned.
- The WARN in `carrierStagingSupported()` is invisible in the bench JVM (logback `root=error`): the
  counter line is the only witness.

### 5.5 Tests
- `io.sirix.access.trx.page.AdoptedOverflowCarrierStagingTest` (core), `@EnumSource(VersioningType)`,
  `@Isolated`, counters reset before each load, `assumeTrue` on FILE_CHANNEL +
  `supportsReclaimableUncommittedWrites` + `SharedArenas.supportsDeterministicClose()`:
  parallel bulk load (`beginNodeTrx(n, KEEP_OPEN_ASYNC_FLUSH)` + `ParallelBulkJsonImporter.assembleBytes`)
  of a corpus whose fused strings span 430–700 uniform printable bytes (both sides of the 512 cap so
  inline and carrier records share pages), sized > 32 pages or with `sirix.asyncFlush.maxLogEntries`
  lowered so ≥ 2 combined epochs happen before commit. Positive arm, before commit:
  `adoptedCarriersStaged() > 0` (precondition), `adoptedCarriersUnstaged() == 0`,
  `kvlPagesPinnedByPromotion() == 0`, `kvlPagesRetriedNextEpoch() > 0`,
  `kvlPagesPinnedAfterDeferralCap() == 0`, `log.pinnedSize()` sampled at every `after-flush` hook site
  ≤ structural bound. After commit + COLD reopen: every value exact, record count exact, PAX census
  exact (`getObjectKeySlotsForNameKey` vs the document — the q20 defect class); a second revision
  through the ordinary transaction updating some carrier records → exact again.
  Mutation arms: seam off → `kvlPagesPinnedByPromotion() > 0 && kvlPagesRetriedNextEpoch() == 0`;
  cap = 0 → every pending page pins (`pinnedAfterDeferralCap > 0`, retried == 0).
- Focused test for the abandonment edit: coordinator-fed load (`createProjectionIndexAtLoadStart` +
  parallel importer) with a tiny `sirix.projection.globalDict.budgetBytes` → the load completes,
  every record present, the projection reports abandoned/stale, exactly one notice; mutation: with
  the catch removed the load fails (pre-edit exit-1 behaviour). Modelled on
  `ProjectionDictionaryBudgetAbandonNoticeTest`.
- Reruns: `FusedRecordDirectoryKindCompletenessTest`, `FusedOverflowDescriptorVersioningTest`,
  `OverflowSlotSidecarVersioningTest`, `AsyncFlushLogBookkeepingTest`, `AdoptedPageRefusalTest`,
  `ParallelBulk*`, `PinnedTrieProjectionSpillColdReopenTest`, then the full core and query suites.

## 6. 100M protocol (round-2 final)
Preconditions: nothing else running; `MemAvailable ≥ 26 GB`; `sirix.arena.strategy` unset (shared),
`sirix.commit.preallocated` unset/true (else staging is inert — the counter line is the witness; the
WARN is discarded by the bench logback); `hits.json.gz` (23.7 GB, `build/diagnostics/
clickbench-official-100m/`) never globbed or cleaned; NO `-Dsirix.hft.telemetry` on this tree; all
kills are SIGTERM to the JVM pid (`pgrep -f ClickBenchLoadMain`), never to the Gradle client, never
SIGKILL — shutdown hooks print the counter line; `-XX:+ExitOnOutOfMemoryError` only in step 3.
`ClickBenchLoadMain` gains an idempotent, flushed `# storage: adoptedCarriersStaged=… unstaged=…
oversized=… kvlPinnedByPromotion=… kvlRetriedNextEpoch=… kvlPinnedAfterCap=…` line at
`HFT_MEASURE_END` and from a shutdown hook (runs on SIGTERM, on the arena's Java-thrown OOME, and on
`System.exit`).
1. **1M pre-check (minutes):** fresh load with `-Dsirix.projection.globalDict=never` (read per build
   via `System.getProperty`, so `-Pclickbench.jvmArgs` reaches it) + 43 queries
   `--require-vectorized-serving` + `--strong --bounded-oracle vectorized` against the 1M DuckDB
   reference. Equals the 100M state only if AUTO elects zero columns there — confirmed from the load's
   `globalDictColumns=` line in step 3.
2. **Diagnostic (GO/NO-GO):** `-Xmx8g`, 8 GiB arena, `expectedRows=99997497`, `-Dsirix.til.diag=50`
   (in-process TIL census: `tilSize`, `pinned`, residents), histograms via `jcmd GC.class_histogram`
   from outside the sandbox at 60/120/180 s, RSS every 10 s, SIGTERM at 200 s, throwaway DB. GO only
   if: no `size class … exhausted`; RSS Δ over the last 60 s < 5 %; `tilSize` bounded (tens) and
   `pinned` = structural only; `PageContainer` ≤ a few hundred, `DataRecord[]` ≈ 0; counter line shows
   `unstaged=0 kvlPinnedByPromotion=0 kvlRetriedNextEpoch>0 kvlPinnedAfterCap=0`; an allocator dump,
   if any, must NOT show `live == freshIdx`.
3. **Load**, alone: `-Dclickbench.expectedRows=99997497 -Dclickbench.projection=true
   -Dclickbench.projection.incremental=true -DbuildPathSummary=true -Xms4g -Xmx10g
   -Dsirix.offheap.bytes=12884901888 -XX:+ExitOnOutOfMemoryError -XX:MaxDirectMemorySize=1g
   -Xlog:gc*,safepoint:file=…` (the task's own `-Xmx12g` precedes; last `-Xmx` wins), no profiler,
   RSS/avail watchdog (SIGTERM when MemAvailable < 3 GB), `df` watchdog, grep `ABANDONED` and record
   `globalDictColumns=`. Disk first: delete the killed run's 5.4 GB partial DB
   (`bundles/sirix-query/build/diagnostics/clickbench-100m-fixed-20260829-1822/db`); candidates the user
   may release: `build/async-profiler` (3.2 GB), three stale 1M diagnostic DBs (6.3 GB).
4. **Queries**, both legs `-Xmx8g -Dsirix.offheap.bytes=12884901888 -Xlog:gc*,safepoint:file=…`:
   vectorized `--tries 3 --dump <vec> --require-vectorized-serving -Dsirix.query.autoVectorize=true`;
   generic `--dump <gen> --require-generic-serving -Dsirix.query.autoVectorize=false`.
5. **Delete the Sirix DB** (keep dumps/logs), then ONE DuckDB run: `duckdb_reference.py --source
   hits.json.gz --format json --db <workdir>/hits.duckdb --temp-directory <workdir>/duckdb-tmp
   --memory-limit 12GB --threads 4 --out results-duckdb --tries 3 --candidate-reference vectorized=<vec>
   --candidate-reference generic=<gen>` (CLI flags — the script reads no environment), then
   `compare-results.py --strong --bounded-oracle vectorized` and `… generic` exactly as
   `run-differential.sh` does (plain `--strong` reports MISSING for every windowed query). Hours;
   source sha256 in the ledger.
6. **GC check (single log, weaker than the paired gate):** no `Pause Full`, no `Pause Remark`/concurrent
   cycle, max pause and histogram for the load AND both query JVMs. The paired 1M/4M `hft_gc_gate.py`
   needs a clean commit and stays for later.
7. Ledger + memory notes.

### 6.7 Findings from the first 100M query legs (2026-08-30)

- Load: 2777 s, acceptance OK at rows=99997497, `# storage:` all carriers staged / none pinned, RSS ≤ 5.0 GB, GC
  Full = 0 (max pause 100 ms, STW 2.4 s). The P2 mechanism holds at 100M.
- Vectorized leg: q0-q7 served; q8 (`RegionID, COUNT(DISTINCT UserID)`) OOM'd the 8 GB heap. Histogram sampling
  (`diagq.sh`) showed 13.4M `JsonDBObject`s: the arm had DECLINED (per-worker 2^24 cap < 17.6M distinct users) and
  the interpreter ran the group-by. **A mid-query decline is invisible to `--require-vectorized-serving`** — the
  fallback runs inside the query. Fixed by `GroupDistinctAccumulator` (shared, striped, heap-derived ceiling,
  inserts stop at the ceiling); see the ledger entry of 02:15.
- Residual heap after q5 ≈ 3.3 GB (decoded slices ~1 GB, leaf bytes ~2 GB, 5.4M `PageReference` ~0.4 GB): the sum
  of independent quarter-heap budgets (§3 defect list, open). The re-run carries `-Xlog:gc*` and a histogram
  sample per minute (`queries100m-vec.sh`) so a further OOM is diagnosable from the artifacts.
- Generic leg: > 6 min per query at 100M (a full record scan each) ⇒ 4-6 h for 43. It runs LAST, after the
  vectorized leg and before the Sirix DB is deleted for the DuckDB reference (the reference cannot coexist with
  the DB on this disk).

- **Strict serving (2026-08-30 03:30).** Every serving arm's fail-soft `catch (RuntimeException)` falls back to the
  generic pipeline and only counts the defect; a proof run therefore cannot see an arm FAILURE until the interpreter
  finishes. `SirixVectorizedExecutor.STRICT_SERVING` (`-Dsirix.query.strictServing`; the runner sets it under
  `--require-vectorized-serving`) makes the shared `failSoft` hook rethrow at all nine sites. Production default
  unchanged. Witness: `StrictServingTest` (fault seam `GROUP_AGG_TEST_FAULT`).
- q13 served alone (30.9 s) and after q8,q10-q12 (32.8 s): the leg's interpreter fallback depended on the full q0-q12
  history; reproduction `diag-full13` with diagnostics pending at the time of writing.

- **q13 root cause and the group-table spill (03:41-03:55).** With q0-q12's residual heap, q13's worker OOMed
  (`Parallel scan failed — OutOfMemoryError`) and the fail-soft catch sent it to the interpreter: the group arms kept
  one table PER WORKER (workers × groups). `GroupTableSpill` now flushes a worker's table into shared partition tables
  past a threshold (default 2^18 groups; sub-chunk 64 leaves) and the post-scan merge starts from the shared table;
  wired into all four arms. Witnesses: the four group differentials under a forced 16-group threshold (63/63) and
  `GroupTableSpillDifferentialTest`. The vectorized leg was relaunched at `-Xmx12g` (strict serving) for the
  correctness deliverable while the spill was being built; the 8 GB envelope is re-tested afterwards.

- **q18 and hash-range passes (04:00-04:30).** A ~100M-group state fits no heap; the group arms now abort a pass
  past a heap-derived group budget and restart with P passes over partition ranges (table-level discard handle,
  accumulator pass filter, persistent per-partition selectors). Witness `GroupHashRangePassTest`; forced-pass sweeps
  of the group differentials. Re-run q18 at 100M pending the leg's end.

- **q23-q26 and the windowed leaf access (04:35-05:00).** The sorted top-k kernel needed whole-column residency
  of the fat `URL`/`Title` columns (~8 GB of per-leaf dictionaries at 100M); it now reads leaves through
  `ProjectionColumnStore.LeafColumnAccess` (resident when the fill budget allows, windowed otherwise; sort role
  unmasked, predicate role keep-masked). Witnesses `ProjectionColumnScanParityTest`, `SortedScanWindowedAccessTest`.
  Re-run of q23-q26 at 100M pending the leg's end.

- **Status 2026-08-30 10:15.** 8 GB strict leg: 43/43 served on vectorized routes (q19 via its re-run after the
  value-emission fix); everything committed as `09a20540c`. USER DECISION: keep the 100M DB, defer the DuckDB
  reference, do the HFT work on the 8 GB envelope next (levers: sliced group kernels over the windowed leaf access,
  heap-relative residency, q5's string-free distinct tier). The DuckDB step (§6.5) runs afterwards.

- **HFT lever 1 (2026-08-30 10:30, implemented, rig-witnessed).** The whole-leaf fallback of the group arms
  (q9/q16-q18/q29-q32/q35's slow tier: every column of every row group streamed per try × hash-range passes) is
  replaced, when only the fill budget blocks residency, by the SAME sliced kernels over per-worker windowed slice
  arrays (`WindowedSliceArrays`, 64-leaf sub-chunks, 2-window per-column cache, keep-masked): the gate decides by
  KIND, residency by FIT. Emission reads winners' leaves through one-leaf accesses. Not windowed (routed as before a
  refusal): dictionary COUNT(DISTINCT) identity fills, deferred string extrema, the dense lane, the legacy legs.
  Witness `GroupWindowedSlicesTest` (25 runs, three mutations confirmed load-bearing).
  **Measured at 100M/8 GB (slow tier, 3 tries, dumps byte-identical):** q16 144→25.7 s, q17 133→21.6 s, q18 253→59.0 s,
  q30 178→5.4 s, q31 247→9.6 s, q32 696→33.5 s, q35 105→4.8 s cold; q29 unchanged (const-group tier, not a group arm).
  q9 exposed the FIT half's per-column judgement (two fills retained, the third refused, whole-leaf re-entry: 172 s cold
  vs 1.5 s windowed) → `ProjectionColumnStore.columnsFitWithinBudget` prices the COMBINED fill at the gate and the budget
  handler re-enters windowed before whole-leaf; witness `columnsThatEachFitButNotTogetherGoWindowedOnTheFirstTry`.
  **Full leg 12:24: Σ cold 3,803 → 1,327 s (964 s without q19), 42 dumps byte-identical, 41/43 on their routes; the two
  in-context failures (q19 value-emission decline, q32 group-arm exception — both serve alone in a fresh JVM) are open:
  residency accumulated over the leg (retained fills + charged blooms + payload windows) is now a correctness item.**

- **12:31 USER DIRECTION.** Query fixes first, in this branch, with the projection index — nothing else additionally
  (base-store/PAX-per-path parked). NEXT phase: storage size. ClickBench `data_size` (c6a.4xlarge, 100M): Umbra 8.30 GB,
  CedarDB 8.46 GB, ClickHouse 15.26 GB, DuckDB 20.46 GB, PostgreSQL 106.49 GB; ours 131.9 GB (`sirix.data`, trie +
  projection). Step 1 = per-page-class breakdown (`-Dsirix.storage.profile=true`, `storage1m.sh`), then levers.
- **12:36 Windowed twins completed after the full leg's build:** deferred string-extrema pass 2 (q21/q22/q28 shapes),
  the const-group fold (q29: 164 s in context → windowed), both no-LIMIT legacy legs (q7: 20.7 s → 0.23 s). The full leg's
  two in-context failures (q19 decline, q32 exception) are being reproduced with diagnostics (q0-q32 in one JVM).

- **13:25 Both in-context failures root-caused by the q0-q32 replay and fixed generally.** q19: `leafAccess` priced an
  already-retained predicate column at zero, then `columnMasked` re-priced its masked projection against a full budget
  ("masked slice fill adds 117 MB beside 2,118 MB already retained") → `columnMasked` prices incrementally,
  `columnMaskedView` masks a published column in place (resident access + shared predicate resolver). q32: a worker
  `OutOfMemoryError` on the second try — per-pass budgets planned against maxMemory with 5.9 GB live → `HeapHeadroom`
  (post-collection pool usage) drives `GroupTableSpill.groupBudget()`/the distinct ceiling, and a worker OOM aborts the
  pass and restarts with more passes. Witnesses with mutations: core parity (masked pricing), `HeapHeadroomBudgetTest`,
  `GroupPassOutOfMemoryRestartTest`, `SortedScanWindowedAccessTest` (resident predicate under a full budget). Final
  pipeline launched 13:30: gates → 1M storage profile → full 43-query leg at 8 GB.

- **14:12 Storage phase plan:** `docs/STORAGE_AND_SPEED_PLAN.md` (draft 2). Measured: the projection is 109.6 B/row on
  disk at 1M (numerics bit-packed; per-leaf string dictionaries 22-25 B/row; keys chain 12.8); the node trie ~1,090 B/row
  (~105 field records × 10.4 B) is ~85 % of the 131.9 GB. Trie leaf compaction is the storage step (M1 ≤ 55 GB),
  disk-resident order-preserving global dictionaries the speed step; DuckDB-class size needs per-path value regions.

## 7. Order of work
P2 (§5.6 additions, §5.5 tests, load-main counter line) → focused core suites → full core suite →
§6.1 1M pre-check → §6.2 diagnostic → §6.3 100M load (P1 is CODED while it runs — no JVM work beside
the load) → §6.4 queries → §6.5 DuckDB reference (P1 compiled and gated meanwhile: core/query focused
suites, then full query suite) → §6.6 GC check → 1M gate rerun with P1 in → ledger/memory. Gradle
strictly serial; no JVM work overlaps the diagnostic or the load.

## 8. Rollback and safety
P2 sites are guarded by `STAGE_ADOPTED_OVERFLOW_CARRIERS` (behaviour) and the deferral cap (fallback
to the unchanged pin path, counted). P1 touches only the array-contains branch, the scope resolver
call site, and flag-gated element staging. No format change; no versioning machinery (standing
ruling).

## 9. Review record
Round 1: P1 reviewer (two wrong-count classes, witnesses, protocol), P2 reviewer (I1–I11 confirmed,
R1–R10). Round 2: P2 reviewer (all corrections correct; unwind; ISE kept), P1 reviewer (F1/W3,
purity narrowing, PC, fixture parity, protocol flags). Questions asked in round 2 (kept for the record)
1. P1: does A/B/C/R1/R2/R8 leave any reachable wrong count? Re-examine (b), (c), (e), (f) and any
   page with MULTIPLE seams. Is R2(ii)'s purity rule implementable at the staging site without a
   second pass, and does it interact with the orphan leading run?
2. P1 tests: are the fixtures buildable deterministically (slot-1023 placement), and does each
   mutation flip the named assertion?
3. P2: are the round-1 corrections (§10) complete and correct as coded? Anything the re-promotion
   `put` breaks for a leaf already forwarded once? Is the ISE-vs-SirixIOException decision right?
4. Protocol: any remaining launch-killer? Are the GO/NO-GO thresholds observable with telemetry off?

## 10. Round-1 corrections applied to the P2 draft (for verification)
R1a `KeyValueLeafPage.deepCopy` shares pending references; R1b survivor loop → throw, unconditional
immutability mark, dead `hasHeapRecords` loop removed; R2 once-per-writer backend gate with a WARN
naming the properties, `ADOPTED_CARRIERS_OVERSIZED`; R3 `KVL_PAGES_PINNED_AFTER_DEFERRAL_CAP` at the
promote site, `kvlDeferredPages` in the HFT line; R4 materialize before `prepareLeafOfTree`; R6
`carriersKnownResolved` overload of `serializeDisposableSnapshotKeyValuePage`; R9 `getSnapshotDiskOffset`
javadoc, cap comment rewritten. Not applied: R7 (measure first), R8 (see §5.4), R10 (follow-up).

## Status 2026-08-30 19:20 — wave 2 committed, rebuild #1 pending wave 3
- Committed on `codex/clickbench-port-rebased-20260827`: `1c43bcbbe` (wave 1: section diagnostics, synthesized order
  labels), `3647d58e1` (B5-d cap 1,023 + per-tag string-region completeness + body codec election fix),
  `32b7c2a37` (B1 declared timestamp/date kinds), `194e52299` (fixtures re-based on the cap).
- 1M: file 1,854.3 → 1,770.4 MB (−4.5 %); leaf class 1,093.3 → 1,070.4 MB; projection 103.5 B/row.
- Rebuild #1 at 100M happens after B3-a (derived elision metadata — the largest measured trie lever) and B5-c; it
  needs the old 124 GB campaign DB (`clickbench-100m-campaign-20260830-0058/db`) deleted first (15 GB free now) and
  the harness's declared `date`/`timestamp` types — every old DB is invalid for the new cap/region/kind layouts.

## Status 2026-08-30 20:55 — rebuild #1 done: 90.4 GB
- `clickbench-100m-campaign-20260830-2007/db` at `eb5a307b7`: 90.4 GB (−31.4 %), load 46m 55s, acceptance OK.
- Query leg running from the gate worktree (`queries100m-vec-wt.sh <dir> 8g`); compare with
  `clickbench-100m-campaign-20260830-0058/results-vec` (43 dumps) and `query-vec-8g.log` (Σ cold 807 s / hot 705 s).
