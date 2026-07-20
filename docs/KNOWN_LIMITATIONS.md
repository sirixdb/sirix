# Known limitations and disabled tests

This document inventories every `@Disabled` / `@Ignore` test in `sirix-core`,
explains *why* it's off, and points at the tracking artifact (issue,
documented gap, or follow-up plan). The point: a future reader should never
have to guess whether a disabled test is hiding a real bug.

Categories used:

- **bench/manual** — long-running performance benchmark. Disabled in CI;
  expected to be removed locally and run on-demand. **Not** a correctness gap.
- **manual-utility** — helper tooling (e.g., dumps a canonical encoded
  frame for an external bench). Not a regression risk.
- **known-gap** — real correctness or capability gap, intentionally disabled
  until the underlying feature lands. Each entry below names the tracking
  doc.
- **legacy** — refers to a code path that has been superseded; the test
  documents behavior of removed/replaced code and is kept only for archeology.
  Should be deleted on next cleanup pass.

---

## sirix-core

| Test | Category | Notes |
|---|---|---|
| `service/xml/xpath/FunctionsTest.java:217-450` (19× `@Ignore`) | legacy | Tests for *not-yet-implemented* XPath 1.0 functions in the sirix-native XPath axis. XQuery is now dispatched through Brackit; this legacy axis is no longer the canonical query path. Safe to delete on a cleanup pass — they document feature gaps of a deprecated component, not active bugs. |
| `service/json/shredder/JsonShredderTest.java:74,154,209,272` | bench/manual | Four Chicago-dataset shredder benchmarks (3.6 GB JSON corpus, not in CI). Use `removeAnnotation` locally + ≥ 16 GB RAM. |
| `service/json/shredder/JsonShredderTest.java:573` | known-gap | "Duplicate keys" — JSON spec allows duplicate object keys; Sirix's shredder de-duplicates. Test documents the behavior gap; no current customer ask. |
| `service/json/shredder/JacksonJsonShredderTest.java:417` | known-gap | Same "Duplicate keys" gap as above (Jackson variant). |
| `service/json/shredder/JacksonJsonShredderTest.java:562,597,608` | bench/manual | Manual large-file shredding benchmarks. |
| `access/node/json/JsonNodeTrxInsertTest.java:45,89` | bench/manual | 650 000-document insertion benchmarks with `println` instrumentation — pure scratch tests. Should be tagged `@Tag("benchmark")`. |
| ~~`index/IndexIntegrationTest.java:1339`~~ | re-enabled | NAME-with-HOT variable-length key serialization issue — verified to pass against the current HOT strict-Binna conformance work (see `docs/HOT_PAPER_IMPOSSIBILITY.md`). Test re-enabled this session. |
| ~~`index/hot/HOTFormalVerificationTest.java:3123`~~ | re-enabled | Was "SirixIOException: leaf page capacity exhaustion at high-dup counts". Root cause found while fixing the projection grow-rebuild bug: stale swizzled `PageReference` copies kept reading `HOTLeafPage` instances the TIL had closed after a CoW overwrite (use-after-close on recycled 64 KiB frames → garbage keys → runaway splits / spurious capacity exhaustion). `PageReference.getPage()` now treats a closed HOT leaf as a cache miss and callers re-resolve via `logKey`. Test re-enabled and green (≈72 s). |
| ~~`index/hot/HOTFormalVerificationTest.java:3198`~~ | re-enabled | Was "FrameSlotAllocator size-class-4 exhaustion at 100K random" — same stale-swizzle use-after-close root cause as :3123 (the runaway splits exhausted the 64 KiB frame class). Re-enabled and green (≈3 s). |
| `index/hot/HOTMicrobenchmark.java` (class-level) | bench/manual | HOT microbenchmark suite. Designed to pollute the regular suite — enable manually. |
| `index/hot/HOTIndexIntegrationTest.java:685` | (not actually disabled) | The `@Disabled` here is in a comment, not on a test. False positive in `grep`. |
| `page/SirixLZ77DumpEncodedTest.java:22` | manual-utility | Utility that dumps a canonical encoded frame for the standalone C bench. Not a test. |
| ~~`io/ChecksumVerificationTest.java:295`~~ | re-enabled | 4 `SirixCorruptionException` constructor tests — re-enabled this session; all pass. |
| ~~`access/AsyncAutoCommitTest.java:asyncAutoCommit_underDocumentedConstraints_works`~~ | re-enabled | Surfaced the `KeyedTrieWriter.prepareIndirectPage:176` ClassCastException under `KEEP_OPEN_ASYNC_FLUSH` — root cause was a cross-generation `logKey` collision in `TransactionIntentLog.put`. Fixed this session by adding an `activeTilGeneration == currentGeneration` guard before reusing `existingKey`. Test now passes. |

## Platform

- **Windows + MEMORY_MAPPED storage: interrupted-first-commit recovery cannot re-initialize in
  place.** Windows hard-locks memory-mapped files, so the recovery path that truncates and
  re-bootstraps a damaged bootstrap-only resource fails while any mapping may still be live.
  Use the default `FILE_CHANNEL` backend on Windows (recovery is fully covered there); the
  corresponding `InterruptedFirstCommitRecoveryTest` cases are skipped on Windows.

## Disabled-but-by-design

A handful of test classes ship `@Disabled` at the class level intentionally —
they're soak / stress harnesses gated behind a system property. These are
**not** in the table above because they're CI-correct by design:

- `stress/BitemporalSoakStressTest.java` — soak driver, gated on
  `-Dhot.soak.run=true`. Default path is `@EnabledIfSystemProperty`; class-level
  `@Disabled` is absent.
- `index/hot/HOTSoakTest` family — similarly gated.

## CI-enforced

The full sirix-core test suite (with the disabled tests excluded) runs on
every PR. Disabled tests count toward "skipped" in the JUnit XML output but do
not block CI. A future improvement: a CI step that asserts the count of
`@Disabled` does not increase between commits without an accompanying entry
here.

## Vectorized executor (analytical fast paths)

The `SirixVectorizedExecutor` fast paths (group-by, filtered count, aggregates,
count-distinct, multi-key group-by) only claim a pipeline when the brackit
detection proves the query's shape matches what the executor emits — anything
else falls back to the generic (always correct) pipeline.

### Fixed gaps (no longer limitations)

- **Moved subtree kept its old pathNodeKey when the target path already
  existed (path summary).** `PathSummaryWriter#processFoundPathNode` (the
  merge branch of `adaptPathForChangedNode`, shared by XML renames and JSON
  subtree moves) bumped reference counts and adapted DESCENDANT NameNodes but
  never reset the moved/renamed node's own `pathNodeKey` — only the
  create-new-path branch did. First move to a fresh path: correct; second
  move to the now-existing path: the moved record still claimed the OLD
  record set's path, so path-scoped scans attributed it to the wrong root
  (reference counts were right, node attribution wasn't). The merge branch
  now resets the root's `pathNodeKey` like the create branch. Caught by the
  `ProjectionIndexStressTest` tombstone→rebuild cycles (cycle 1's fallback
  answered 54 instead of 52).
- **Unscoped name sweep after re-open (generic aggregate path).** The
  path-scoped aggregate resolves its target path node by matching named
  ancestors, but read `PathNode#getName()` directly — which is only populated
  for path nodes created in the CURRENT process. On a deserialized summary
  every named ancestor looked anonymous, resolution failed, and the aggregate
  fell back to an UNSCOPED name sweep: `sum($doc.records[].age)` also counted
  `age` fields under OTHER roots (e.g. records moved out to an archive array).
  Names now resolve through the positioned `PathSummaryReader` (nameKey
  lookup), with the `__array__` sentinel treated as anonymous so fresh and
  re-opened summaries walk identically. Caught by the
  `ProjectionIndexStressTest` tombstone→rebuild cycle (post-tombstone fallback
  answered 55 instead of 54).
- **Stale in-process registry serving after invalidation (projection paths).**
  The committed lookup fell back from the revision-scoped catalog to the
  in-memory `ProjectionIndexRegistry`, whose handles are gated only by
  `validFromRevision` — so after an invalidating commit (e.g. a subtree move
  out of the record set) a warm registry handle installed at create time kept
  serving the PRE-tombstone snapshot to every later revision in the same
  process. The catalog is now authoritative whenever the resource carries
  catalogued projection definitions at the executor's revision: a catalog miss
  there means "not usable here" and the registry may not answer (it remains
  only for bench/test wiring without catalogued definitions). Caught by the
  `ProjectionIndexStressTest` tombstone→rebuild cycle soak.
- **Sparse fields (projection paths).** Projection leaves now carry per-column
  presence bitmaps + per-column "unrepresentable value" flags (leaf format v2,
  self-describing tail — see `ProjectionIndexLeafPage`). Predicates over a
  missing field are false (the stored default never matches), group-by routes
  missing keys to the missing bucket (`'m'` segment / null group key),
  aggregates skip missing rows. Columns that ever saw null/object/array/
  kind-mismatched values are flagged and the projection paths decline them
  (typed kernels / generic pipeline answer correctly instead).
- **Sparse group fields (scan path, single key).** The typed kernel
  synthesizes the missing-key group from `recordCount - visited` for
  top-level-array sources (this used to be a loud `QueryException`).
- **OR/NOT over a missing anchor field.** Brackit's detection now refuses any
  predicate without a *sound anchor* — a referenced field whose absence
  provably falsifies the predicate (`PredicateNode.findSoundAnchorField`,
  with correct three-valued Not/De-Morgan handling). `a > 1 or b > 1` runs
  on the generic pipeline; same-field ORs still vectorize. The executor
  re-checks at compile and fails loudly if an unsound tree slips through
  (e.g. an old brackit).
- **Double/decimal predicate literals.** `where $u.score > 9.99` was silently
  truncated to `> 9` (detection called `Number#longValue()` on xs:double and
  xs:decimal literals). Detection now emits exact leaves: `NumCmp` for
  long-representable literals, `FpCmp` for finite xs:double (double-space
  comparison — the interpreter's own promotion, including its precision loss
  for integers above 2^53), `DecCmp` for other xs:decimal literals carrying
  the exact `BigDecimal`. The scan path evaluates long/double/decimal
  document values per the interpreter's per-type dispatch; the projection
  path rewrites fractional thresholds over provably-integral columns into
  exact long-space predicates (`x > 9.99 ⟺ x >= 10`, verified by brute force
  against the promotion oracle in `FpCmpIntegralRewriteTest`).
- **Double projection columns (since the segment-directory redesign).** `jn:create-projection-index`
  accepts `double`/`decimal` column types; cells store exact doubles in an order-preserving
  encoding (predicate literals transform at plan time, aggregates surface `xs:double`). The
  value-exactness gate is fail-closed: a column that ever absorbed a lossy `BigDecimal`→double
  conversion declines value-exact serving and falls back to the generic pipeline. **Value
  aggregates (sum/avg/min/max) are served only under the pure-double-source provenance bit**
  (`COLUMN_FLAG_PURE_DOUBLE_SOURCE`, flags bit2): every cell of the column must have shredded
  from a `Double` source (in practice, JSON exponent-form literals like `1.25E0` that
  round-trip through `Double.toString`), under which the interpreted fallback provably
  accumulates in double space and types the result `xs:double`. Served sums/avgs use a
  seed-first document-order fold (bit-identical to the interpreter's pairwise fold,
  including lone `-0.0` and ill-conditioned association-order cases) and served min/max use
  `Double.compare` total order (`-0.0 < 0.0`, like the interpreter's comparator). Plain
  JSON decimals (`1.25`) shred as `BigDecimal`, the fallback accumulates them
  decimal-exactly (`Dec`-typed), and those columns stay count-only — as do integer-fed
  double columns (exactness is not the bar; result TYPE parity is) and `Float`-fed ones
  (the fallback types those `xs:float`). Predicates (incl. promoted decimal literals) and
  counts are served regardless. ALP compression for
  double segments is a reserved follow-up (numeric width bytes 65–255 are format escapes);
  today double bodies pack via FOR over the transformed bits.
- **Legacy (pre-descriptor) projection stores.** The segment-directory layout replaced chunked
  storage without a metadata version bump (no deployed databases): a rebuild over a legacy
  store detects it structurally (slot-0 payload is not a blob marker) and swaps in a fresh
  sub-tree. Old pages remain on disk (append-only store) but are unreachable from new
  revisions; a resource copy/re-import sheds them.
- **Mixed int/double columns under predicates.** Document doubles are no
  longer truncated to longs during predicate evaluation (the `rating` 3 vs
  3.7 family), and the NumberRegion zone-map page prune now requires the tag
  to cover EVERY anchor slot before skipping a page (a long-only region says
  nothing about double-valued rows).
- **Mixed-type numeric group keys merge like the interpreter.** The typed
  key encodings canonicalize: integral doubles below 2^53 and long-representable
  integral decimals encode in the long key space; decimals equal to their
  shortest double form encode in the double image space; remaining decimals are
  scale-canonicalized (`2.5` ≡ `2.50`). So `18`, `18.0` and `18.00` are ONE
  group, matching the generic pipeline's `hash(doubleValue)` + `atomicCmp`
  equality. Mixtures whose interpreter grouping is ORDER-DEPENDENT fail
  LOUDLY instead: long keys + integral doubles at/above 2^53 (one double is
  `atomicCmp`-equal to several distinct longs — probe-verified order-dependent
  merging), long keys + non-shortest-form decimals with small integral images,
  and double keys image-colliding with non-shortest-form decimals (the
  interpreter merges them but renders the FIRST tuple's lexical). A `-0.0`
  double group key also fails loudly (interpreter merges it with `0` but
  renders first-tuple lexical) — unreachable from JSON ingestion, which loses
  the zero sign at shred time.
- **Negative-hash anchors now use the page-skip registry.** nameKeys are
  String hashes and may be negative (`active`, `amount`); the scheduler's
  publish/lookup guards exclude only the `-1` missing sentinel, so scans
  anchored on negative-hash fields populate and reuse the page-skip bitmap
  like everyone else (was: permanent full page sweeps).
- **Multi-key group-by with a sparse anchor — re-anchor on a PROVABLY-DENSE
  group field (scan path).** An anchor-based scan never visits a record missing
  the anchor field, so a SPARSE anchor used to fail loudly even when another
  group field was present on every record. The unpredicated multi-key kernel now
  chooses the scan anchor among the group fields by PROVABLE density: a field is
  dense iff the path summary's OBJECT_KEY reference count for its (unambiguously
  resolved, query-scoped) path equals the top-level array's record total —
  `getReferences()` is incremented once per referencing node, and for a
  top-level array of objects a key occurs at most once per record, so equality
  proves presence on every record. The scan then anchors on the dense field,
  visits every record, and the remaining (possibly sparse) keys fall out as
  their values, including the `'m'` missing bucket the typed encoder already
  emits. Works for the dense field in ANY group-key position and across
  provenance (jn:store / shredder). Density is never guessed: if NO group field
  is provably dense (path summary off, nested source, ambiguous path, or every
  candidate genuinely sparse) the kernel keeps the loud bail / projection
  fallback. It also REFUSES to re-anchor when the FIRST group field is absent on
  EVERY record — brackit's interpreter collapses such a grouping to a single
  all-null tuple (an empty FIRST grouping key swallows the whole tuple), which a
  dense-anchored scan cannot reproduce, so that shape fails closed.
- **Aggregate edge semantics.** `avg`/`min`/`max` over zero contributing rows
  return the empty sequence (was a fabricated 0); `count(... return $u.f)`
  counts non-empty derefs of ANY value type (was: numeric values only, and
  ignored `f`'s presence under predicates).
- **Decimal-valued aggregate fields.** `jn:store`/`JsonShredder` keep plain
  fractional JSON literals as BigDecimal (only round-trippable exponent-form
  literals become compact doubles), and the typed redo path folded them via
  `Double.parseDouble` while the interpreter sums xs:decimal exactly and
  divides via `Dec#div`. The `MixedAgg` accumulator now folds decimal rows
  exactly (order-free — parallel folds cannot drift) and delegates the avg
  division to brackit for digit-for-digit parity. The companion brackit fix
  (branch `feat/fpcmp-predicates`) repairs `divideBigDecimal`'s scale rule,
  which HALF_EVEN-rounded terminating quotients away (`1.0 div 2.0` = 0,
  `10.2 div 4` = 2.6).

### Remaining limitations

- **Multi-key group-by where NO group field is provably dense (scan path).**
  Re-anchoring (see fixed gaps) handles a sparse anchor whenever ANOTHER group
  field is provably dense. When density cannot be proven for ANY group field —
  every candidate genuinely sparse, the source is nested (record total not
  cheaply known), the path summary is off, or the path is ambiguous — the anchor
  walk still cannot reconstruct the secondary key values of unvisited records,
  so it fails LOUDLY (`QueryException` naming the anchor field). A covering
  projection index serves the same query correctly (presence bitmaps see every
  record). Likewise an entirely-absent FIRST group field fails closed: the
  interpreter collapses that grouping to one all-null tuple, which the scan
  cannot reproduce.
- **Nested sources with sparse group fields (scan path).** The record total
  is only cheaply known for top-level array sources; for nested sources a
  sparse single group key still yields silently-partial groups (pre-existing,
  unchanged). Projection-backed queries are exact.
- **Non-numeric aggregates.** `min`/`max`/`sum`/`avg` over fields holding only
  strings/booleans/nulls fail LOUDLY (the interpreter applies string/error
  semantics the numeric kernels cannot reproduce; historically this silently
  returned 0). Mixed numeric/non-numeric columns keep the legacy
  "skip non-numeric values" fold, which diverges from the interpreter's type
  error — unchanged.
- **Double-bearing aggregate columns and parallel summation order.** Once a
  column contains xs:double rows the interpreter's running sum becomes
  double and the sequential fold order matters at the last ulp; the parallel
  accumulator re-associates additions, so adversarial double data can differ
  in the final ulp. Exact for binary-fraction values (the differential gate's
  data); pure long/decimal columns are immune (exact, order-free folds).
- **`xs:float` document values in predicate fields** fail loudly: the
  interpreter compares xs:float operands in FLOAT space (`Float.compare`),
  which double-space evaluation cannot reproduce. JSON ingestion has not
  produced floats since the alpha13 narrowing removal.
- **Legacy (pre-presence) projection leaves** carry no presence information;
  every projection fast path declines them (scan kernels answer instead).
  Rebuild persisted projections with `-Dsirix.projection.forceRebuild=true`
  to migrate to the v2 leaf format.

The `TypedGroupByDifferentialTest` suite (89 cases) pins vectorized ≡
interpreted for the supported shapes, including typed (numeric/boolean/double)
and multi-key group keys, the negative-hash nameKey regressions, adversarial
sparse shapes (missing-on-30%, missing-on-all, present-but-null, mixed-kind
columns, sparse group keys/aggregates/predicates, OR/NOT-over-sparse), the
double/decimal predicate family across scan and projection paths, exact
decimal/genuine-double aggregate parity (jn:store vs shredder provenance), and
the multi-key dense-anchor selection (sparse anchor + dense second key in both
orders, dense anchor as the 2nd/3rd key, numeric dense anchor, mixed
provenance, plus the fail-closed cases: both-sparse-no-projection loud bail with
the same query correct via a covering projection, and absent-first-key loud
bail).

## Cleanup actions queued

1. Delete the 19 legacy XPath `@Ignore` tests (`FunctionsTest.java:217-450`)
   — they're 14+ years stale and refer to a non-canonical code path.
2. ~~Re-enable or remove `ChecksumVerificationTest.CorruptionExceptionTests`~~
   **Done this session.**
3. Tag the bench/manual tests with `@Tag("benchmark")` so they can be excluded
   via JUnit Platform configuration rather than `@Disabled`.
