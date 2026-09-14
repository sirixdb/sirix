# Parallel column directories and sorted-leaf bounds — 2026-09-14

The combined native candidate beats the local ClickHouse baseline on the five-query JSONBench warm geometric score at **99,999,968 rows**. Warm ratios are **0.8504 / 0.8105**; lower is better. The recorded visible-eviction (`cold`) ratios were **0.8701 / 0.8636**, but lower filesystem cache state was uncontrolled on this eCryptfs workspace, so those are not verified disk-cold rankings. Every query matched ClickHouse exactly. These scores use the campaign's `geomean((Sirix seconds + .010) / (ClickHouse seconds + .010))` formula. The [final combined validation](../20260914-writer-readback-cache/README.md#cold-cache-qualification) records the later I/O audit and same-database control.

| Query | Sirix warm seconds, rounds 1 / 2 | ClickHouse warm seconds, rounds 1 / 2 |
|---|---:|---:|
| Q1 | .011 / .010 | .077 / .081 |
| Q2 | 1.779 / 1.753 | 1.583 / 1.560 |
| Q3 | 1.538 / 1.493 | .491 / .551 |
| Q4 | .034 / .034 | .264 / .257 |
| Q5 | .906 / .905 | .267 / .275 |

Q2, Q3, and Q5 remain slower individually. The sum of warm query times is also higher: Sirix **4.268 / 4.195 s**, ClickHouse **2.682 / 2.724 s**. Winning the geometric score does not mean every workload is faster.

## Isolated changes

Column-major descriptor collection now partitions the physical row-group domain across at most eight readers of the same committed revision. Each lane reads its own range, validates it against the persisted physical order, and fills disjoint logical positions. All started readers close before errors propagate. Small directories retain serial collection; segment bodies remain lazy.

The same-binary reverse-order comparison measured Q2 warm **2.212 → 1.5995 s** (27.69% less time) and Q3 **2.0275 → 1.633 s** (19.46% less). The initial ABBA also improved both, but its first baseline Q2 was unusually slow; both raw runs are retained. `-Dsirix.projection.parallelColumnDirectory=false` selects serial collection.

Sorted leaves now maintain a revisioned min/max pair in a 1,040-byte chunk covering 64 physical leaf IDs. Grouped minimum top-K visits summaries in ascending leaf-minimum order. It retains K+1 distinct groups, merges repeated group identities, and stops only when the next leaf minimum is strictly worse than the retained cutline. Winning or cutline ties retain the existing exact fallback. Other grouped-extrema orders continue using complete summaries.

At 100M, same-binary bounds off/on/on/off measured Q4 warm **.9105 → .036 s** (96.05% less time, 25.29× speedup). Q5 was effectively flat: **.899 → .892 s**. At 1M, Q4 was **.0345 → .009 s**. `-Dsirix.projection.sortedMinBounds=false` selects the preceding summary scan.

## Storage and correctness

The full backfill added revision 2 to `db-100m-column-summary-v58`. All **32,727 source leaves** remain byte-identical to revision 1. An independent audit reconstructed every summary and bound from **8,377,929 original sorted tuples**. The bounds occupy **512 chunks / 532,480 bytes**. All 129 sampled nodes retained their original revision-1 creation history. The 1M cross-revision audit also passed.

Fine-grained writes invalidate an old bound before changing its source and publish new bounds after the summary. Chunk mutation copies bytes that may be shared with immutable or asynchronously flushed pages. Missing optional metadata falls back to exact scanning. Candidate arrays are capped at 24 MiB; larger views use the streaming scan.

All **56 focused tests** passed, including all four versioning strategies, asynchronous flushes, splits, updates, deletion, rollback, cold historical reads, sparse/reordered physical IDs, cross-leaf groups, ties, missing bounds, and malformed descriptors. The tests require cleanup after parallel-reader failure.

A subsequent fresh 10M diagnostic load exposed an initial-build bug: rewriting a shared bounds chunk conflicted with append-only staged side-page keys. That load failed before completing its commit and is not a throughput result. The v65 fix accumulates one chunk while consecutive initial leaf IDs arrive and publishes each chunk once, including the final partial chunk. Ordinary edits retain the revisioned per-chunk update path. The regression reproduced the failure under all four versioning strategies before the fix; **70 focused tests** passed afterward. A fresh combined 1M load then verified all five queries, all bounds, column layout and 129 creation histories. The read format is unchanged. Records are under `bounds-bulk-build-v65/`, adjacent to the raw root below.

The 43-query ClickBench ABBA on the retained **200,000-row fixture** was exact throughout. Hot-median sums were **1.708 / 1.735 s** for the baseline and **1.719 / 1.747 s** for the candidate. The candidate mean was 0.67% higher, within the observed repeat spread; this screen shows no clear regression at that scale. It is not a full-100M ClickBench measurement.

The paired native measurements used fresh processes, verified visible-file eviction followed by warm OS-cache attempts, two engine-order-swapped rounds, and separate differential verification. The lower filesystem cache was not controlled. The 24 GiB memory / zero-swap and thermal guards passed. No loader, compiler, tests, or profiler overlapped ranked measurements. These query results include predicate-slice reuse from v62. They do not measure ingestion overhead or claim a combined ingestion improvement.

## Reproduction records

Raw root: `build/jsonbench-campaign/source-flag-summary-v47/sorted-min-bounds-v64/`.

- `directory-100m-{abba,baab}/summary.json`: isolated directory controls.
- `bounds-{1m,100m}-abba/summary.json`: isolated bounds controls.
- `paired-100m-attempt1/`: protocol, differential results, query timings, cold-cache evidence and process usage.
- `backfill-100m/`: source/bounds reconstruction and cross-revision audits.
- `clickbench-summary.json`, `tests-summary.json`, `test-results/`: validation.
- `full-suite-{scope.scope.json,verdict.json}`, `isolation-*`, `directory-repeat-*`: resource and thermal evidence.
- `candidate-manifest.json`, `classpath.txt`: frozen v64 classes; the classpath also includes frozen v63 and v62 overlays.

Native image: `build/jsonbench-campaign/pgo-sorted-min-bounds-v64-100m/jb`, SHA-256 `b55adef7f668cb8f19742ea19ced5e0bb87ec14da5c1a556a31fa321abe1ae16`. Its `build-outcome.json` records the unchanged PGO inputs and all class hashes. All changes remain uncommitted.

That image is the measured read-only query image against the backfilled database. Fresh bulk builds require the v65 writer fix described above; the image's original bulk-builder implementation predates that correction.
