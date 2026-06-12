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
else falls back to the generic (always correct) pipeline. Within the claimed
shapes, the following gaps are inherent to anchor-based page scanning and are
currently documented rather than fixed:

- **Sparse group fields.** An anchor-based scan never visits a record that
  lacks the anchor field, while the generic pipeline groups such records under
  the empty key. For an unpredicated group-by over a top-level array this is
  detected (visited count vs. array `childCount`) and surfaces as a loud
  `QueryException` instead of silently missing groups. Nested sources and
  predicated scans can't be checked this way.
- **OR-predicates / NOT over a missing anchor field.** `where $u.a > 1 or
  $u.b > 1` anchors on `a`; a record carrying only `b` is invisible to the
  scan even though the disjunct holds. Same class: `where not($u.missing)`.
  Dense, uniformly-shaped record sets (the supported target workload) are
  unaffected.
- **Mixed int/double group keys.** A column containing both `18` and `18.0`
  groups them separately in the typed kernel, while XQuery `eq` semantics in
  the generic pipeline merge them. Single-typed columns are exact.
- **Negative-hash anchor fields skip the page-skip registry** (the registry
  treats negative nameKeys as unpublishable), so scans anchored on such fields
  (e.g. `active`, `amount`) do full page sweeps — a performance note only;
  results are correct.

The `TypedGroupByDifferentialTest` suite pins vectorized ≡ interpreted for the
supported shapes, including typed (numeric/boolean/double) and multi-key group
keys and the negative-hash nameKey regressions.

## Cleanup actions queued

1. Delete the 19 legacy XPath `@Ignore` tests (`FunctionsTest.java:217-450`)
   — they're 14+ years stale and refer to a non-canonical code path.
2. ~~Re-enable or remove `ChecksumVerificationTest.CorruptionExceptionTests`~~
   **Done this session.**
3. Tag the bench/manual tests with `@Tag("benchmark")` so they can be excluded
   via JUnit Platform configuration rather than `@Disabled`.
