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
| ~~`access/AsyncAutoCommitTest.java:asyncAutoCommit_underDocumentedConstraints_works`~~ | re-enabled | Surfaced the `KeyedTrieWriter.prepareIndirectPage:176` ClassCastException under `KEEP_OPEN_ASYNC` — root cause was a cross-generation `logKey` collision in `TransactionIntentLog.put`. Fixed this session by adding an `activeTilGeneration == currentGeneration` guard before reusing `existingKey`. Test now passes. |

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
- **Mixed int/double columns under predicates.** Document doubles are no
  longer truncated to longs during predicate evaluation (the `rating` 3 vs
  3.7 family), and the NumberRegion zone-map page prune now requires the tag
  to cover EVERY anchor slot before skipping a page (a long-only region says
  nothing about double-valued rows).
- **Aggregate edge semantics.** `avg`/`min`/`max` over zero contributing rows
  return the empty sequence (was a fabricated 0); `count(... return $u.f)`
  counts non-empty derefs of ANY value type (was: numeric values only, and
  ignored `f`'s presence under predicates).

### Remaining limitations

- **Multi-key group-by with a sparse FIRST key (scan path).** The anchor walk
  cannot reconstruct the secondary key values of unvisited records — it fails
  LOUDLY (`QueryException` naming the field). A covering projection index
  serves the same query correctly (presence bitmaps see every record).
- **Nested sources with sparse group fields (scan path).** The record total
  is only cheaply known for top-level array sources; for nested sources a
  sparse single group key still yields silently-partial groups (pre-existing,
  unchanged). Projection-backed queries are exact.
- **Mixed int/double group KEYS.** A column containing both `18` and `18.0`
  groups them separately in the typed kernel, while XQuery `eq` semantics in
  the generic pipeline merge them. Single-typed columns are exact.
- **Non-numeric aggregates.** `min`/`max`/`sum`/`avg` over fields holding only
  strings/booleans/nulls fail LOUDLY (the interpreter applies string/error
  semantics the numeric kernels cannot reproduce; historically this silently
  returned 0). Mixed numeric/non-numeric columns keep the legacy
  "skip non-numeric values" fold, which diverges from the interpreter's type
  error — unchanged.
- **`xs:float` document values in predicate fields** fail loudly: the
  interpreter compares xs:float operands in FLOAT space (`Float.compare`),
  which double-space evaluation cannot reproduce. JSON ingestion has not
  produced floats since the alpha13 narrowing removal.
- **Legacy (pre-presence) projection leaves** carry no presence information;
  every projection fast path declines them (scan kernels answer instead).
  Rebuild persisted projections with `-Dsirix.projection.forceRebuild=true`
  to migrate to the v2 leaf format.
- **Negative-hash anchor fields skip the page-skip registry** (the registry
  treats negative nameKeys as unpublishable), so scans anchored on such fields
  (e.g. `active`, `amount`) do full page sweeps — a performance note only;
  results are correct.

The `TypedGroupByDifferentialTest` suite (65 cases) pins vectorized ≡
interpreted for the supported shapes, including typed (numeric/boolean/double)
and multi-key group keys, the negative-hash nameKey regressions, adversarial
sparse shapes (missing-on-30%, missing-on-all, present-but-null, mixed-kind
columns, sparse group keys/aggregates/predicates, OR/NOT-over-sparse), and the
double/decimal predicate family across scan and projection paths.

## Cleanup actions queued

1. Delete the 19 legacy XPath `@Ignore` tests (`FunctionsTest.java:217-450`)
   — they're 14+ years stale and refer to a non-canonical code path.
2. ~~Re-enable or remove `ChecksumVerificationTest.CorruptionExceptionTests`~~
   **Done this session.**
3. Tag the bench/manual tests with `@Tag("benchmark")` so they can be excluded
   via JUnit Platform configuration rather than `@Disabled`.
