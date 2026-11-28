# QRY — Query execution: column-served predicated aggregates

Spec target: **1.0.0**. Chapter status: **Draft; captain decisions of 2026-11-28 folded in**.
Code baseline: sirix `321a718f63929166e8bcb9ccd3ddfabc20635e83`, brackit
`master` `e6515a9ffa0a9565db7baa024b1a9a9c42ec63c0` (28 November 2026).
[Conventions](CONV-conventions.md) define requirement status, evidence and conformance. Every
requirement below is `proposed`. No test named here exists yet; each one is **Planned**.
Tests are derived from this chapter by a separate test author before implementation (CONV-103).

## Scope

This chapter covers one route in the vectorized executor: aggregates over a conjunctive filter in
which a nested array-membership test appears next to boolean flags. The route answers them from a
page's in-page PAX regions (columns) without rebuilding the page's records. The shape it serves is

```
count(for $r in SRC where B and M return $r)
count(for $r in SRC where B and M return $r.N)
sum  (for $r in SRC where B and M return $r.N)
```

where `B` is zero or more boolean field tests, `M` is zero or more array-membership tests over a
path of one or more named object steps (`$r.user.tags[] = "admin"`), at least one of `B`/`M` is
present, and `N` is a numeric field of the record. QRY-101 defines the exact set. The chapter
also covers the brackit detection that hands this shape to the executor and the fallback for
everything outside it.

Out of scope: secondary indexes (CAS, path, name), the projection index and the question of
giving it a membership column (a separate specification, decision D3), group-by, order-by, string or numeric literals
compared with something other than `=`/`eq`, and every other query shape, which keeps its current
route.

**Terms.** *Interpreter*: brackit's generic FLWOR pipeline with vectorization off
(`sirix.query.autoVectorize=false`, or no executor installed). It is the only oracle in this
chapter. *Route*: the new column-served evaluation. *Record*: one item bound to `$r`, an
object that is a member of the source array. *Page*: one KeyValueLeafPage of the document index
(1024 node-key slots). *Serve*: a page's contribution is computed from its regions. *Page
decline*: the page's contribution is computed by the exact record-at-a-time path of the
executor instead. *Query refusal*: the executor returns no result (or throws
`VectorizedRouteRefusedException`) and the interpreter evaluates the whole query.
*Seam record*: a record whose nodes lie on more than one page.

## Background (informative)

Measured on 2026-11-27/28 on an i7-12700H (P-cores 0–11), GraalVM 25.0.3, 5,000,000 records of
Lemire's `simdjson_compressed_demo` corpus (812 MB NDJSON, records
`{"id","active","user":{"name","tags":[...]},"score","note"}`), loaded with the parallel
bulk importer (FULL versioning, path summary on):

| query | 1 thread | 12 threads |
|---|---:|---:|
| `sum(for $r in $d[] where $r.active and $r.user.tags[] = "admin" return $r.score)` | 5.50 s | 6.26 s |
| the same, interpreter only | — | 4.58 s |
| `count(for $r in $d[] where $r.active return $r)` (region scan, existing) | 0.27 s | 0.107 s |
| ClickHouse MergeTree, `sumIf(score, active AND has(user.tags,'admin'))` | 0.40 s | 0.105 s |

Expected answers: count 5,000,000, active 3,332,024, admin sum −2,273,353. Every route returned
them. The admin sum, the plain `sum(... where $r.active return $r.score)` and the nested
membership count incremented no served counter: they ran at about 1 µs per record.

Code reading of the baseline gives four reasons, and each one shapes a requirement below:

1. **Detection.** `VectorizedGroupByDetection.arrayContains` claims only
   `some $g in $r.F[] satisfies $g eq "lit"`, with `F` a *direct* field of the loop variable
   (`directLoopVarDerefField`). Neither the general comparison `$r.F[] = "lit"` nor a nested path
   `$r.user.tags` is claimed, so the query never reaches the executor (QRY-101, QRY-102).
2. **No predicated region sum.** Region kernels exist for predicated *counts*
   (`countPageFromRegions`: number, string, boolean, fused and tree plans) and for the
   *unpredicated* integral sum (`aggregateIntegralPageFromRegions`). A predicated sum always
   takes `parallelGenericPredicateAggregate`, which rebuilds every record.
3. **Record alignment with nested objects.** The fused kernels pair columns by record through
   `RecordOrdinalRegion.alignedLead`. It requires a field's slots to map to ordinals `0..m-1` in
   order. Ordinals number every *enclosing object* in first-appearance order, so a nested object
   such as `user` takes its own ordinal between two records (record 0 → 0, its `user` → 1,
   record 1 → 2, …). By code reading, the certificate then fails on every page of this corpus;
   no probe has confirmed that yet (QRY-302).
4. **The membership column route is opt-in.** `countArrayContainsFromRegions` is used only for a
   single, unconjoined `ArrayContains`, and only when `sirix.scan.arrayContainsColumnar=true`
   (`ARRAY_CONTAINS_COLUMNAR_ENABLED`, default `false`). Its Javadoc records ~43 % of pages served
   on the movie corpus, with the remaining declines undiagnosed (QRY-601).

## Requirements — detection (brackit)

### QRY-101 — Claimed shapes

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** brackit [VectorizedGroupByDetection.java] `extractPredicate`, `arrayContains`, `tryAnnotate`; brackit [PredicateNode.java]
- **Verified by:** **Planned:** brackit `NestedArrayMembershipDetectionTest`: for each claimed form below, the annotated `PREDICATE_TREE`, `VECTORIZED_AGGREGATE`/`AGGREGATE_FIELD` or `VECTORIZED_COUNT` equals the expected value. For each form listed in QRY-103, no claim is made.
- **Evidence class:** doc-only
- **Gap/note:** Today only the `some … satisfies` form over a direct field is claimed.

The detector MUST claim, and represent as one `PredicateNode` tree, a `where` clause that is a
conjunction (`and`, parentheses allowed, conjuncts in any order, one or more `where` clauses) of
leaves of these two kinds:

1. **Flag test:** `$r.F`, the effective boolean value of a direct field: today's `BoolRef(F)`.
2. **Membership test** over a *member path* `P = p1 … pk` (k ≥ 1) of named object steps, each a
   plain `.name` deref with no array step, filter, descendant deref (`..`) or function between
   them, and a string literal `L`. Accepted forms, each with either operand order:
   - `$r.p1…pk[] = L` (general comparison `=`), and
   - `some $g in $r.p1…pk[] satisfies $g eq L` or `… satisfies $g = L`.

The surrounding expression MUST be claimed as follows:

- `count(for $r in SRC <where> return $r)` as the filtered count;
- `count(…)` or `sum(…)` with `return $r.N`, `N` a direct field, as the predicated aggregate;
- `avg`, `min` and `max` with `return $r.N` MAY be claimed through the same annotation. QRY-205
  says when they may be served.

Claims MUST keep today's strict-anchor rule: a predicated aggregate is claimed only when
`findSoundAnchorField()` finds a field. A membership leaf is false on every record that lacks
`p1`, so its anchor field is `p1`.

### QRY-102 — Representing a nested member path

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** brackit `PredicateNode` (sealed); sirix `SirixVectorizedExecutor` switches over it
- **Verified by:** **Planned:** brackit `PredicateNodeMemberPathTest`: `ArrayContainsPath` keeps its path through `collectFields`, `excludesRecordsMissingField(p1)` is true, `excludesRecordsMissingField(pj)` for j > 1 alone does not make the leaf anchorable, equality and `toString` distinguish `["user","tags"]` from `["user.tags"]`. sirix compile of every exhaustive switch.
- **Evidence class:** doc-only
- **Gap/note:** Representation and landing order decided 2026-11-28 (D1).

The membership leaf MUST be a new permitted subtype of `PredicateNode`,
`ArrayContainsPath(List<String> path, String value)`. It carries the member path as an ordered,
immutable list of exact field names, with at least one step. `ArrayContains(field, value)`
MUST stay unchanged, and a depth-1 `ArrayContainsPath` MUST mean exactly what it means. A path
MUST NOT be encoded by joining names with a separator, because field names may contain `.` and
`/`. Two leaves are equal only if their paths are equal step by step and their literals are
equal.

Brackit is consumed by sirix as a moving SNAPSHOT, and `PredicateNode` is sealed. A new
permitted subtype or a changed record component makes exhaustive sirix switches fail to compile
when brackit publishes. The brackit change and the sirix change MUST therefore land so that sirix
`main` compiles at every published brackit snapshot. The landing order MUST be:

1. a sirix change that compiles against both the current and the new brackit, for example by a
   `default` arm in each exhaustive switch over `PredicateNode`, which refuses an unknown leaf;
2. the brackit change that adds `ArrayContainsPath` and its detection;
3. the sirix route.

### QRY-103 — Shapes that stay unclaimed

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** brackit `VectorizedGroupByDetection`
- **Verified by:** **Planned:** `NestedArrayMembershipDetectionTest` negative table (one row per bullet).
- **Evidence class:** doc-only
- **Gap/note:** —

The detector MUST NOT claim a membership leaf for any of these forms. Each one either has
different semantics or raises an error the route would hide:

- a value comparison over the unboxed array (`$r.P[] eq L`): it is a singleton comparison and
  raises an error on an array with more than one element;
- `!=`, ordering operators, `every … satisfies`, or a negated membership (`not(…)`) outside the
  rules PredicateNode already applies to negation;
- an index or slice (`$r.P[[1]]`, `$r.P[1:2]`) instead of `[]`;
- a non-string literal, a variable or a computed right-hand side;
- a path step that is an array step (`$r.a[].b[]`), a descendant deref or a function call;
- a disjunction that contains a membership leaf, unless an executor declares decomposable
  support for this leaf kind (it does not today).

## Requirements — results

### QRY-201 — Result equality with the interpreter

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** sirix `SirixVectorizedExecutor` `executePredicateAggregate`, `executePredicateCount`; `VectorizedFallbackExpr`
- **Verified by:** **Planned:** `RegionPredicatedAggregateDifferentialTest`: every query in the QRY-202/203/204 corpus table runs with the interpreter and with the route. The serialized results and the error codes are compared exactly, and the served and declined counters (QRY-503) are asserted for each row. **Planned:** jqwik `RegionPredicatedAggregateProperty`: random documents over the same value domain (record count 0–3,000, so pages and seams vary) and random claimed queries; the property is equality with the interpreter.
- **Evidence class:** doc-only
- **Gap/note:** —

For every claimed query, the observable result with the route MUST equal the interpreter's
result on the same revision: the same item type (for example `xs:integer` against `xs:decimal`)
and the same value. Where the interpreter raises an error, the query MUST raise an error with
the same error code. The route meets this either by computing the same answer or by query
refusal, after which the interpreter's own evaluation produces the result or the error. The
route MUST NOT return a result for a query on which the interpreter raises an error.

### QRY-202 — Existential per-record semantics

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** new route kernel; record-path evaluator for nested member paths
- **Verified by:** **Planned:** `RegionPredicatedAggregateDifferentialTest` rows E1–E9; the sum rows assert the sum is unchanged when a matching record's array repeats the literal.
- **Evidence class:** doc-only
- **Gap/note:** —

A record satisfies a membership leaf exactly when at least one element of the array reached by
`$r.p1…pk[]` equals `L` under the interpreter's string equality, which is codepoint equality with
no normalization. Each record that satisfies the whole conjunction MUST contribute exactly once,
whatever the number of matching elements. A sum MUST add its `N` value once, and a count MUST
add one. The corpus MUST cover:

| row | record |
|---|---|
| E1 | the literal once in the array |
| E2 | the literal twice or more in the same array |
| E3 | non-matching elements only; an empty array `[]` |
| E4 | `tags` missing; `user` missing; `user` an empty object |
| E5 | `tags` is the string `L` itself (not an array) |
| E6 | `user` is an array of objects that carry `tags` |
| E7 | an element that is a prefix, suffix or case variant of `L`; `L = ""`; a non-ASCII and an escaped `L` |
| E8 | an array of more than 1024 elements, so its elements span pages |
| E9 | the literal on no page at all; on exactly one page |

### QRY-203 — Missing, NULL and type-mismatch values

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** new route kernel; `checkedAggregateResult`, `MixedAgg`
- **Verified by:** **Planned:** `RegionPredicatedAggregateDifferentialTest` rows T1–T8, each in a record that satisfies the rest of the conjunction and again in one that does not.
- **Evidence class:** doc-only
- **Gap/note:** The interpreter's results for T2, T3 and T5 decide the expected values. The test author records each one as observed. The route must reproduce them, not a value chosen here.

The route MUST decide a record from columns only when the record's in-scope values have the
types it models: booleans under each flag path, string elements under each member path, and
integral numbers (QRY-204) under `N`. A page holding any other in-scope value MUST be declined
for that page. If the record path cannot evaluate such a value with interpreter semantics, the
query MUST be refused. Type checks MUST look only at values that the interpreter would evaluate:
a record that fails a flag test MUST NOT cause an error because of its `N` value. The corpus
MUST cover:

| row | value |
|---|---|
| T1 | flag field missing |
| T2 | flag field `null`, a string, a number, an object or an array |
| T3 | an array element that is a number, `null`, a boolean, a nested array holding `L`, or an object holding `L`, before the matching element and after it |
| T4 | `N` missing in a matching record (sum adds nothing; `count(… return $r.N)` adds 0) |
| T5 | `N` is `null`, a string or a boolean in a matching record (the interpreter's error) |
| T6 | `N` a decimal (`1.5`) or a double (a shredder-typed double) in a matching record |
| T7 | the same field name nested elsewhere in the record (`user.active`, `user.score`, `profile.tags`) |
| T8 | records whose keys come in different orders (`tags` before `name`, `user` last, `active` after `user`) |

### QRY-204 — Numeric fold exactness

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `MixedAgg` (`addLong`, `addLongBatch`, `addDecimal`, `result`), `requireOrderIndependentAggregate`
- **Verified by:** **Planned:** `RegionPredicatedAggregateDifferentialTest` rows N1–N4.
- **Evidence class:** doc-only
- **Gap/note:** The rule is today's predicated-aggregate rule, applied to the new route.

Integer sums MUST be exact over any number of records, including totals outside the signed
64-bit range (N1: values near `Long.MAX_VALUE` and `Long.MIN_VALUE` whose total overflows a
`long`). The result type and value MUST be the interpreter's. Decimal values MUST be summed
exactly. If any value that contributes to a `sum` (or `avg`) is a double, the query MUST be
refused (N2), because the interpreter's sum of doubles depends on order. An empty selection MUST
give `xs:integer` 0 for `sum` and `count` (N3) and the empty sequence for `avg`/`min`/`max` (N4).

### QRY-205 — Other aggregate functions

- **Level:** MAY · **Status:** proposed · **Since:** 1.0
- **Code:** `predicateStatsCache`, `statsToSequence`
- **Verified by:** **Planned:** the differential test runs `avg`/`min`/`max` on every row and asserts equality only (served or refused).
- **Evidence class:** doc-only
- **Gap/note:** —

The route MAY serve `avg`, `min` and `max` from the same count/sum/min/max pass, provided every
QRY-2xx requirement holds for them. Otherwise it MUST refuse them.

## Requirements — scope, attribution and fallback

### QRY-301 — Scope proof from the path summary

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `resolveCountPathScope`, `resolveTargetPathNodeKey`, `structuralSourcePathMatcher`
- **Verified by:** **Planned:** `RegionPredicatedAggregateScopeTest`: fixtures with `tags` under `/[]/tags`, `/[]/user/tags`, `/[]/user/profile/tags`, `/[]/other/tags` and `/[]/user/[]/tags`, each queried by every one of those paths. Each answer equals the interpreter's, and the counters show where the route served and where it refused. Mutation seams, test-only and off by default as in `ArrayContainsScopeDifferentialTest`, bypass each proof, and with a seam bypassed the answer must be wrong.
- **Evidence class:** doc-only
- **Gap/note:** —

Columns are found by name key and are tagged by path node key, so a value in a column does not
by itself say which field path holds it. The route MUST count, test or add a value only after
proving, from the bound revision's path summary, that it belongs to the queried path. For each
flag field, member path and `N`, the full path (source path + field steps, and for a member path
its anonymous array layer `…/pk/[]`) MUST resolve to exactly one path node. If a path is
ambiguous, has no summary, or is reachable through another structure that the interpreter's
deref would also reach (for example `user` holding an array of objects, row E6), the query MUST
be refused or decided by the record path with the structural source matcher. A path that the
summary proves absent MUST contribute nothing; this is not an error. Proving that the path is
absent MUST NOT turn into a name-only scan of the whole resource.

### QRY-302 — Record attribution with nested objects

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** new route kernel; `RecordOrdinalRegion`, `ObjectKeyNameKeyRegion`, `StringRegion`, `BooleanRegion`, `NumberRegion`
- **Verified by:** **Planned:** `RegionPredicatedAggregateAttributionTest`: pages built so that each certificate below fails on its own (a duplicated flag field in one record, a missing `user` on one record, `tags` before `name`, a periodic key order of the `(a,c)/(b,d)` kind from `RecordOrdinalRegion`'s Javadoc, an object element inside `tags`). Each answer equals the interpreter's, and the counter for the expected reason increments. **Planned:** the Lemire-shaped fixture of QRY-501 is served with no page declines.
- **Evidence class:** doc-only
- **Gap/note:** A design note follows; it is not normative.

From columns alone, the route MUST attribute each flag bit, each matching element and each `N`
value to exactly the record that holds it. A page MUST be declined when the attribution cannot
be proven exactly, never served approximately. In particular:

1. Ordinals of non-record enclosing objects (for example each `user` object) MUST NOT be counted
   as records. Records are the enclosing objects of fields at the source-path level.
2. A member-path element MUST be attributed to its record through the chain *element → its
   array's key slot → the enclosing object at each step `pk-1 … p1` → the record*, and each link
   MUST be checked against the stored record-ordinal and name-key columns.
3. For each column used, the route MUST check that no record on the page carries that field
   more than once. A page where one does MUST be declined.
4. The element-count certificate of `countArrayContainsFromRegions` (segments account for the
   tag's values exactly) MUST hold for each member path the route uses.

The route MUST use only region kinds that exist in the 1.0 disk format. If an implementation
finds it needs a new region kind or new region contents, that is an FMT change: it needs its own
specification and the captain's decision before it is built.

*Design note (informative).* On a preorder shred, the key slot of `user` is a fused
`OBJECT_NAMED_OBJECT` node with the record's ordinal `o_r`. The first key slot inside it carries a
new ordinal `o_u`, the next unused one. That pair gives a map `o_u → o_r` in one pass over the
ordinal column. A generalised `alignedLead` that ranks only record ordinals would then pair the
`active`, `tags` and `score` columns by record.

### QRY-303 — Page seams

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** new route kernel; `settleTrailingSeamOnly`, `RegionHeaderScratch` pending boundary
- **Verified by:** **Planned:** `RegionPredicatedAggregateSeamTest`: `jn:store` shreds sequentially, so node keys follow document order. Use that to place each of these on the last or first slot of a page: the record object, its `active` key, its `user` key, its `tags` array node, a matching element, a non-matching element, its `score` key. Cover each position with the literal before and after the seam, and run at 1 and 4 threads.
- **Evidence class:** doc-only
- **Gap/note:** —

A seam record MUST be decided exactly once across the whole scan, by exactly one page or by the
record path, and the flag test, membership test and `N` value MUST all come from the same
record. A page that hands a seam record to the record path MUST NOT also count it. When a page
declines, it MUST clear any seam state it set, so that no later page applies that state.

### QRY-304 — Fallback granularity

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `parallelGenericPredicateAggregate`, `evalCompiled`, `loadFieldsTypedGroups`, `VectorizedFallbackExpr`
- **Verified by:** **Planned:** `RegionPredicatedAggregateDifferentialTest` asserts, for every row, which outcome occurred (served, page-declined or refused) and that the result is equal in each case. A mixed fixture (one page holding a double `score`) shows exactly one page declined and the others served.
- **Evidence class:** doc-only
- **Gap/note:** The record-at-a-time evaluator does not descend nested member paths today. Declined pages and seam records need that descent, or the query has to be refused.

A page decline MUST give exactly that page's records' contributions, computed by a record path
that evaluates the full claimed predicate, nested member paths included, with interpreter
semantics. If the record path cannot evaluate any leaf of the predicate, the route MUST refuse
the whole query before scanning, not page by page. A query refusal MUST discard every partial
result and MUST NOT publish anything to a result cache. A query that is claimed but not servable
(for example an open write transaction, where `wtx != null`) MUST be refused.

## Requirements — concurrency and revisions

### QRY-401 — Multi-threaded scan

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `parallel`, `PageScanSchedule`, per-worker scratch
- **Verified by:** **Planned:** `RegionPredicatedAggregateThreadsTest`: the QRY-303 and QRY-501 fixtures at 1, 2, 3, 4 and 8 executor threads give bit-identical results, equal to the interpreter's, and equal served plus declined page totals (each page handled once). Run each configuration 20 times, so that work-stealing order varies.
- **Evidence class:** doc-only
- **Gap/note:** —

The result MUST NOT depend on the executor's thread count or on scheduling order. Each page
MUST be handled (served or declined) exactly once. Each record MUST contribute at most once, and
exactly once if it matches. Workers MUST NOT share mutable state in the per-page loop beyond
what the existing scans already share (the page cursor and the page-skip bitmap publication).
Partial results MUST be merged only after every worker has finished. A refusal or failure on any
worker MUST lead to query refusal or propagation as in QRY-304, never to a partial result.

### QRY-402 — Revision and time-travel correctness

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** executor `revision` binding; `getRecordPageRegionsOnly` on reconstructed/merged pages
- **Verified by:** **Planned:** `RegionPredicatedAggregateVersioningTest`, parameterized over FULL, INCREMENTAL, DIFFERENTIAL and SLIDING_SNAPSHOT. Revision 2 changes the data in each of these ways: it flips `active`, adds the literal to an array, removes it, replaces `score`, deletes a matching record, inserts a record mid-array, renames `tags`, and turns `tags` into a string. Querying every revision with `jn:doc(db, res, n)` and by timestamp gives the interpreter's answer for that revision, cold and warm, and old revisions are queried after newer ones are written.
- **Evidence class:** doc-only
- **Gap/note:** —

The route MUST read the page versions, region columns and path summary of the revision that the
query's source resolves to, and no other. On a page assembled from several fragments, the
merged columns MUST equal the columns of the reconstructed page; otherwise the page MUST be
declined. Values that were updated or removed in a later or an earlier fragment MUST NOT be
resurrected or counted twice.

### QRY-403 — Result and plan caches

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `predicateAggregateCache`, `predicateStatsCache`, region page caches, page-skip bitmaps
- **Verified by:** **Planned:** `RegionPredicatedAggregateVersioningTest` interleaves queries on revisions 1, 2, 1, 2 with one session and asserts each answer. A cache test runs the same predicate with literal `"admin"` and then `"guest"`, and with path `user.tags` and then `tags`, and asserts that no answer is reused.
- **Evidence class:** doc-only
- **Gap/note:** —

A cached result or plan MUST NOT be used for a different revision, member path, literal, flag
set, aggregate function or `N`. Cache keys MUST distinguish member paths step by step (see
QRY-102). A page-skip bitmap records only where a field occurs, not where it matches. It MAY be
reused across literals and functions, but MUST NOT be reused across revisions or for another
anchor field or path.

## Requirements — cost

### QRY-501 — Work budget

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** new route kernel; served/declined counters (QRY-503)
- **Verified by:** **Planned:** `RegionPredicatedAggregateWorkBudgetTest` (sirix-query `budget` package, CONV-301). Fixture: FILE_CHANNEL, FULL, path summary on, 20,000 Lemire-shaped records generated with a fixed seed (about 180 pages at ~9.2 nodes per record, seams on most of them), cold (`Databases.clearGlobalCaches()`), one executor thread, then four.
- **Evidence class:** doc-only
- **Gap/note:** Bounds are in counted work, never in time (CONV-301).

For the admin-sum query and for the nested membership count on the fixture:

1. every scheduled page MUST be served: zero page declines and zero query refusals;
2. the number of full record-page reads (QRY-503) MUST be at most the number of seam records
   handed to the record path, and at most two seam records per served page may be handed over;
3. a page in which the anchor field does not occur MUST read no region beyond the name-key
   column.

A positive control MUST show that the counters see the work: on the same fixture, the route
switched off (QRY-601) MUST show at least one full record-page read per scheduled page. A
negative control MUST show that the bound detects a page being declined: one page holding a
double `score` MUST show exactly one declined page and at least one more full record-page read.

### QRY-502 — Allocation budget

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** per-worker scratch, `RegionHeaderScratch`
- **Verified by:** **Planned:** `RegionPredicatedAggregateAllocationBudgetTest` (clean allocation JVM, escape analysis and Graal JIT off as in CONV-302 allocation legs; skipped where thread allocation accounting is unsupported). It measures allocated bytes for the warm route over fixtures of P and 4P pages.
- **Evidence class:** doc-only
- **Gap/note:** Decided 2026-11-28 (D4): the 4 KiB tolerance is a starting value. The first measurement fixes it, and after that it is never widened.

After warm-up, the route's per-page and per-record loop MUST NOT allocate. Allocation per query
MUST have no term in page count or record count: `A(4P) − A(P) ≤ 4 KiB`. Literal encodings,
member-path keys and scratch arrays MUST be resolved once per scan, not once per page.

### QRY-503 — Observability

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `SirixVectorizedExecutor` static counters
- **Verified by:** **Planned:** every QRY test above asserts these counters, both that the route was reached and the decline reason.
- **Evidence class:** doc-only
- **Gap/note:** —

The executor MUST expose, with a reset hook for tests: pages served by this route, pages
declined by reason (one enum value per decline condition in QRY-203, QRY-301, QRY-302 and
QRY-303, in the style of `ArrayContainsDecline`), seam records handed to the record path,
query refusals by reason, and full record-page reads (`getRecordPage`). The last counter counts
every such read the executor makes while it evaluates a claimed predicated count or aggregate,
whether the read comes from this route's record path or from `parallelGenericPredicateAggregate`.
Without that, the QRY-501 positive control could not see the reads of the route that is
switched off. Counters MUST be `LongAdder`s or equivalents, updated at most once per
page or per seam record, never once per value.

### QRY-504 — Timed acceptance

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** bench harness; `bundles/sirix-benchmarks` `ArrayMembershipRouteBenchmark` (to extend)
- **Verified by:** **Planned:** a CONV-302 timed verdict, route against baseline, on the Lemire 5M corpus on the reference rig at 1 and 12 threads. The other CONV-302 suites show no regression for queries the route does not claim.
- **Evidence class:** doc-only
- **Gap/note:** Decided 2026-11-28 (D4): the CONV-302 verdict is the gate, and the time target is informational.

The route MUST obtain a CONV-302 "faster" verdict against the baseline for the admin-sum query at
1 and 12 threads. It MUST NOT obtain a "slower" verdict on any existing suite query. Target,
informational (D4): ≤ 0.6 s at 1 thread and ≤ 0.25 s at 12 threads on the
reference rig. That is about twice the measured region `count(active)`, because the route also
reads the string, number and ordinal columns. The full `{count, active, admin_sum}` object query
MUST reach this route for its `admin_sum` member, as shown by the served counter.

## Requirements — switch

### QRY-601 — Kill switch and default

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** new system property; brackit `brackit.predicate.arrayContains` (existing)
- **Verified by:** **Planned:** `RegionPredicatedAggregateDifferentialTest` runs once with the switch on and once with it off. With it off, the route counters stay at zero and the results are unchanged.
- **Evidence class:** doc-only
- **Gap/note:** Default decided 2026-11-28 (D2).

The route MUST have its own system property, independent of `sirix.scan.arrayContainsColumnar`,
that turns it off with no rebuild. Turning it off MUST give interpreter-equal results through the
record path. The detector's claim of the new forms MUST be switchable on its own too, either by
the existing `brackit.predicate.arrayContains` or by a new property.

The route MUST be on by default. The change that turns the default on MUST carry passing QRY-501
evidence (zero page declines on the fixture) and a CONV-302 "faster" verdict (QRY-504). Until
both exist, the route MUST stay off by default.

## Decisions (captain, 2026-11-28)

| # | Question | Decided | Why |
|---|---|---|---|
| D1 | How brackit represents a nested member path (QRY-102) | A new sealed subtype `ArrayContainsPath(List<String> path, String value)`, with `ArrayContains` kept for depth 1, landed in three steps: a sirix change that compiles against both brackit snapshots, then the brackit change, then the sirix route. Rejected: changing `ArrayContains` itself to carry a path | Additive: the existing depth-1 leaf and its tests stay untouched |
| D2 | Default of the route switch (QRY-601) | On by default once QRY-501 (zero declines on the fixture) and QRY-504 (faster verdict) pass. Rejected: opt-in, like `arrayContainsColumnar` | The shape is exact or refused by construction, and a route kept opt-in is not used by the benchmark it was built for |
| D3 | A membership (set) column in projection indexes, so the indexed route can answer the same shape | A separate specification after this route lands, not part of this chapter | It is an index-format change (FMT/CONF) with its own build and maintenance cost |
| D4 | Whether the QRY-504 time target and the QRY-502 4 KiB tolerance are gates | The time target is informational and the CONV-302 verdict is the gate. The tolerance is fixed by the first measurement and is never widened after that. Rejected: hard gates now | A time figure from one rig is not a portable gate; the timed verdict already decides faster or slower |

## Deferred work

The projection-index membership column (D3) gets its own specification. Disjunctions that contain a membership leaf, membership over arrays of objects (`$r.a[].b`),
numeric and boolean literals in membership tests, nested `N` (`$r.user.score`), and group-by over
this filter keep their current route. None of them is claimed by QRY-101.

## Traceability

| Requirement | Planned verification | Status |
|---|---|---|
| QRY-101 | brackit `NestedArrayMembershipDetectionTest` | proposed |
| QRY-102 | brackit `PredicateNodeMemberPathTest`; sirix compile against both snapshots | proposed |
| QRY-103 | `NestedArrayMembershipDetectionTest` negative table | proposed |
| QRY-201 | `RegionPredicatedAggregateDifferentialTest`; jqwik `RegionPredicatedAggregateProperty` | proposed |
| QRY-202 | `RegionPredicatedAggregateDifferentialTest` rows E1–E9 | proposed |
| QRY-203 | `RegionPredicatedAggregateDifferentialTest` rows T1–T8 | proposed |
| QRY-204 | `RegionPredicatedAggregateDifferentialTest` rows N1–N4 | proposed |
| QRY-205 | `RegionPredicatedAggregateDifferentialTest` avg/min/max columns | proposed |
| QRY-301 | `RegionPredicatedAggregateScopeTest` with mutation seams | proposed |
| QRY-302 | `RegionPredicatedAggregateAttributionTest`; QRY-501 fixture | proposed |
| QRY-303 | `RegionPredicatedAggregateSeamTest` | proposed |
| QRY-304 | `RegionPredicatedAggregateDifferentialTest` outcome column; mixed-page fixture | proposed |
| QRY-401 | `RegionPredicatedAggregateThreadsTest` | proposed |
| QRY-402 | `RegionPredicatedAggregateVersioningTest` | proposed |
| QRY-403 | `RegionPredicatedAggregateVersioningTest` interleaving; cache-key test | proposed |
| QRY-501 | `RegionPredicatedAggregateWorkBudgetTest` with positive and negative controls | proposed |
| QRY-502 | `RegionPredicatedAggregateAllocationBudgetTest` | proposed |
| QRY-503 | counter assertions in every test above | proposed |
| QRY-504 | CONV-302 timed verdict on the Lemire 5M corpus | proposed |
| QRY-601 | `RegionPredicatedAggregateDifferentialTest` switch-off run; default flip carries QRY-501 and QRY-504 evidence | proposed |

[VectorizedGroupByDetection.java]: https://github.com/sirixdb/brackit/blob/master/src/main/java/io/brackit/query/compiler/optimizer/walker/topdown/VectorizedGroupByDetection.java
[PredicateNode.java]: https://github.com/sirixdb/brackit/blob/master/src/main/java/io/brackit/query/compiler/optimizer/PredicateNode.java
