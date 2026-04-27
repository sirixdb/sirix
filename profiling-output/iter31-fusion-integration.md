# iter#31 — OBJECT_NAMED_* fusion integration (Option-B pivot) (2026-04-23)

## TL;DR

- **Pivot from Option A to Option B** mid-iter per coordinator directive: fused
  `OBJECT_NAMED_*` nodes are now true leaves (not virtual-child-bearing) and both
  name + primitive value are read directly off the single fused record.
- **Phase 2/3/4 scan + serializer + diff fixes landed**; test failure count
  dropped from iter#30's ~250 to **188** (102 sirix-query + 86 sirix-core).
- **Phase 5 green-gate did NOT meet**. Per hard rule "stop if >10 failures after
  2 hours of Phase 5 iteration", Phase 6 native-PGO + cold A/B is NOT run —
  measurements on a broken tree are unreliable. The 5.0 GB fused DB at
  `/tmp/sirix-100m-fused-native/` is preserved for iter#32.

## State taken from iter#30

Started from `iter22-work` base (`2bba747fa`) with iter#30's working-tree diff
plus untracked files (`ObjectNamed{Boolean,Null,Number,String}Node.java`,
`ObjectNamedNodesTest.java`, `JsonShredFusedRoundTripTest.java`,
`FusedDbRoundtripTest.java`) applied. Foundation tests (`ObjectNamedNodesTest`,
`JsonShredFusedRoundTripTest`, `FusedDbRoundtripTest`) all pass.

## Phase 2 — scan-path (projection-index builder) — DONE

`bundles/sirix-core/src/main/java/io/sirix/index/projection/ProjectionIndexBuilder.java`:

- `getPathNodeKeyAtCursor` now recognises OBJECT_NAMED_* and returns the
  fused node's pathNodeKey (same namespace as legacy OBJECT_KEY).
- `extractRow` added a fused-kind branch that reads the inline primitive value
  directly via `rtx.isNumberValue()/getNumberValue()` etc. — zero-alloc,
  no synthetic-child indirection.
- `readFusedValueIntoRow` helper mirrors `readValueIntoRow` semantics but
  dispatches by fused record kind rather than cursor-mode kind. All four
  fused kinds handled; unknown kind leaves default.

`ProjectionIndexByteScan.java` needs **no changes** — it reads projection leaf
bytes which are uniform across storage shapes. The byte-scan kernel is
transparent to fused vs legacy storage.

`bundles/sirix-core/src/main/java/io/sirix/page/KeyValueLeafPage.java`:

- Added `isFusedObjectNamedKindId(kindId)` (kindIds 48-51) + companion
  `getFusedObjectNamedNameKeyFromSlot(slot)`.
- `buildObjectKeySlotsForNameKey` slow-path now matches BOTH OBJECT_KEY and
  fused records by nameKey — so `getObjectKeySlotsForNameKey` in the
  vectorised executor slow-path sees fused slots too.
- Fast path (PAX `ObjectKeyNameKeyRegion`) is **not yet extended** to include
  fused slots; this remains a follow-up for iter#32 (small per-page region
  addition, high impact on bench queries).

## Phase 3 — diff / FMSE — DONE (direct-edit approach, not de-sugar)

Fused node plays "OBJECT_KEY + primitive value" in one record. Updated:

- `JsonFMSE.insertAsFirstChild` / `insertAsRightSibling` → copySubtree routes
  accept fused kinds (copyNode already handles them from iter#30).
- `emitUpdate` → `setStringValue/setNumberValue/setBooleanValue` called with
  fused kinds; setters already route to `replaceObjectRecordValue` (iter#30).
- `JsonFMSEVisitor` labels fused as leaves; `countDescendants` treats them
  as +1 like OBJECT_KEY (structural role).
- `JsonLabelFMSEVisitor` labels each fused kind distinctly (their own
  NodeKind label) for leaf matching.
- `JsonLeafNodeComparator` + `JsonFMSENodeComparisonUtils.typedValuesEqual`
  compare fused by name-AND-value in a single step (no parent walk).
- `DeleteJsonFMSEVisitor` visits fused kinds and applies the same delete
  branches as OBJECT_KEY/primitive pairs.

JsonFMSETest: 70 failures → **7 failures**. Remaining 7 are test-fixture
mismatches (tests navigate `moveToFirstChild` thrice and expect a primitive
`getValue` — with Option-B fused has no child so cursor stays on fused and
`getValue` returns the fused value, which is what the test intended but the
dispatch assumes legacy kinds).

## Phase 4 — serializer + REST-API + query item factory — DONE

- `JsonSerializer.emitNode` fused OBJECT_NAMED_* branch already existed
  (iter#30). Verified it emits `"name": value` inline and skips the
  descendant step.
- `JsonItemFactory.getSequence` now returns typed primitive `AtomicStrJsonDBItem`
  / `AtomicBooleanJsonDBItem` / `NumericJsonDBItem` / `AtomicNullJsonDBItem`
  for fused kinds reading inline value. Fixed `JsonIntegrationTest` 68 →
  44 failures.
- `JsonDBObject.get(QNm)` / `getValueSequenceAtIndex` / `trx.setField` all
  branch on legacy OBJECT_KEY vs fused OBJECT_NAMED_* and skip the
  `moveToFirstChild` descent in the fused case.

## Phase 5 — full test suite — 86 core + 102 query failures

Remaining failure categories (ordered by count):

| Category | #Core | #Query | Root cause |
|---|---|---|---|
| Descendant-deref on array-of-objects | – | ~15 | `DerefDescendantExpr` buildQuery still uses legacy `ChildAxis` in some composite paths — not all rewritten to `FieldValueAxis` |
| HOT CAS/PATH index integration | 30+ | – | Index listeners updated, but some HOT-index tests rebuild indexes via visitor walks that need deeper audit |
| Temporal queries | – | ~15 | Prior-revision queries see fused records that didn't exist in old revisions — test-fixture mismatch, correctness work needed |
| JsonDiffEdgeCase / BasicJsonDiff | 8 | – | NodeKey shifts cascade; tests expect legacy tree shape with explicit nodeKey counts |
| JsonSerializer / JsonRecordSerializer | 7 | – | `descendantCount` + `type` metadata mismatches (legacy expected OBJECT_KEY, fused emits OBJECT_NAMED_NUMBER) |
| Flyweight cursor regressions | 6 | – | Pre-existing — `FlyweightCursorTest` failed before iter#30 |
| PredicateTreeCount / SimpleQuery | – | 15 | Vectorized executor PAX fast-path reads only OBJECT_KEY slots; fused slots invisible to SIMD region scan |

New infrastructure landed this iter:

- `io.sirix.axis.FieldValueAxis`: one-shot axis that yields fused-node-as-self
  OR descends via `moveToFirstChild` for legacy OBJECT_KEY. Replaces the
  "descend-to-primitive-child" step in `DerefDescendantExpr` at 6 call-sites.
- `JsonNodeReadOnlyTrx.isFusedSyntheticChild()` method added to the public
  interface (vestigial now that Option B is live — flag never flips true —
  kept for backward compat with any extension points).

## Phase 6 — SKIPPED (hard stop rule)

Per user directive "stop before Phase 6 if >10 failures after 2 hours of
Phase 5 iteration": we stand at **188 failures** after ~3 h of Phase 5.
Running cold-100M A/B on the fused DB against the iter#25 reference would
produce numbers we cannot attribute to either the fusion change or the
broken tree. The measurement is deferred until iter#32 closes Phases 5–6.

## Disk scoreboard (carry-over)

| iter | DB size | fold vs previous |
|---|---|---|
| pre-campaign (legacy, no LZ4) | 22.62 GB | baseline |
| iter#22 FOR-BP (no LZ4) | 17.0 GB | 0.75 × |
| iter#27 LZ4 | 6.1 GB | 0.36 × |
| **iter#30 fusion (no LZ4)** | **5.0 GB** | **0.82 ×** vs LZ4-alone |
| iter#30 fusion + LZ4 (projected, not measured) | ~2 GB | ~0.33 × |

iter#31 does not add disk gains — it's an integration iter.

## MongoDB-ballpark verdict

Storage side: unchanged 5.0 GB (or ~2 GB stacked projected). Query side
blocked until vectorised scan path sees fused slots natively. The big-impact
follow-up is extending the PAX `ObjectKeyNameKeyRegion` to include fused
kinds; once there, the SIMD SIMD-matching scan path reads fused identically
to legacy and iter#25's 1.53 s cold median should carry over with minor
change.

## Open risks for iter#32

1. **Vectorised scan path PAX region extension** (#1 priority). Without it
   every bench query hits slow-path and the cold-100M wall will regress from
   1.53 s substantially.
2. **`DerefDescendantExpr.buildQuery` remaining legacy `ChildAxis` usage**.
   Complete the `FieldValueAxis` sweep — currently only covers the outer
   decomposition, not every nested-segment branch.
3. **HOT-index integration**: visitors now dispatch fused visits, but some
   HOT range-query tests still assume legacy value-node adjacency. Needs
   a pass over `HOTIndexIntegrationTest` + friends.
4. **Test fixtures with explicit nodeKeys** (`BasicJsonDiffTest`,
   `JsonSerializerTest` metadata.descendantCount etc.). Mass update
   expected — fused DBs have ~50% fewer nodes for record-shaped data.

## Files touched (iter#31)

- `bundles/sirix-core/src/main/java/io/sirix/access/trx/node/AbstractNodeReadOnlyTrx.java`
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeReadOnlyTrxImpl.java`
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/ForwardingJsonNodeReadOnlyTrx.java`
- `bundles/sirix-core/src/main/java/io/sirix/api/json/JsonNodeReadOnlyTrx.java`
- `bundles/sirix-core/src/main/java/io/sirix/api/visitor/JsonNodeVisitor.java`
- `bundles/sirix-core/src/main/java/io/sirix/axis/ChildAxis.java`
- `bundles/sirix-core/src/main/java/io/sirix/axis/FieldValueAxis.java` (NEW)
- `bundles/sirix-core/src/main/java/io/sirix/axis/visitor/DeleteJsonFMSEVisitor.java`
- `bundles/sirix-core/src/main/java/io/sirix/diff/algorithm/fmse/json/JsonFMSE.java`
- `bundles/sirix-core/src/main/java/io/sirix/diff/algorithm/fmse/json/JsonFMSENodeComparisonUtils.java`
- `bundles/sirix-core/src/main/java/io/sirix/diff/algorithm/fmse/json/JsonFMSEVisitor.java`
- `bundles/sirix-core/src/main/java/io/sirix/diff/algorithm/fmse/json/JsonLabelFMSEVisitor.java`
- `bundles/sirix-core/src/main/java/io/sirix/diff/algorithm/fmse/json/JsonLeafNodeComparator.java`
- `bundles/sirix-core/src/main/java/io/sirix/index/cas/CASIndexBuilder.java`
- `bundles/sirix-core/src/main/java/io/sirix/index/cas/json/JsonCASIndexBuilder.java`
- `bundles/sirix-core/src/main/java/io/sirix/index/cas/json/JsonCASIndexListener.java`
- `bundles/sirix-core/src/main/java/io/sirix/index/name/json/JsonNameIndexBuilder.java`
- `bundles/sirix-core/src/main/java/io/sirix/index/name/json/JsonNameIndexListener.java`
- `bundles/sirix-core/src/main/java/io/sirix/index/path/json/JsonPathIndexBuilder.java`
- `bundles/sirix-core/src/main/java/io/sirix/index/path/json/JsonPathIndexListener.java`
- `bundles/sirix-core/src/main/java/io/sirix/index/projection/ProjectionIndexBuilder.java`
- `bundles/sirix-core/src/main/java/io/sirix/node/json/ObjectNamedBooleanNode.java`
- `bundles/sirix-core/src/main/java/io/sirix/node/json/ObjectNamedNullNode.java`
- `bundles/sirix-core/src/main/java/io/sirix/node/json/ObjectNamedNumberNode.java`
- `bundles/sirix-core/src/main/java/io/sirix/node/json/ObjectNamedStringNode.java`
- `bundles/sirix-core/src/main/java/io/sirix/page/KeyValueLeafPage.java`
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java`
- `bundles/sirix-query/src/main/java/io/sirix/query/compiler/translator/DerefDescendantExpr.java`
- `bundles/sirix-query/src/main/java/io/sirix/query/json/JsonDBObject.java`
- `bundles/sirix-query/src/main/java/io/sirix/query/json/JsonItemFactory.java`
