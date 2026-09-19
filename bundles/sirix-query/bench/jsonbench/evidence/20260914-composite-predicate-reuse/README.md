# Composite predicate slice reuse — 2026-09-14

On the same 100M column-layout database, native Q3 warm time fell from **2.387 to 1.992 seconds**, a **16.55% reduction**, in an isolated baseline/candidate/candidate/baseline comparison. All 16 Q2/Q3 cold and warm results matched ClickHouse. A separate native check verified Q1/Q3/Q4/Q5. This is a same-database improvement; the five-query ClickHouse score has not yet been rerun for this candidate.

| Query | Baseline warm seconds | Candidate warm seconds | Mean change |
|---|---|---|---:|
| Q2 | 2.598 / 2.431 | 2.138 / 2.423 | −9.31% |
| Q3 | 2.361 / 2.413 | 2.046 / 1.938 | −16.55% |

Q2 is a control: this change targets the composite group path used by Q3. Its lower observed mean is not attributed to slice reuse. Both binaries use the same PGO profiles and database; all 2,053 frozen base classes were checked before building the candidate. Each query uses a fresh process and a verified cold attempt followed by a warm OS-cache attempt. The 24 GiB / zero-swap scope and thermal guard passed.

## Change

The composite group executor now shares its current query's verified predicate slices with group keys and aggregate operands that name the same column. Previously it read and decoded a masked predicate column, then filled the entire column again for grouping. The kernel evaluates the filter before reading keys or operands, so pruned slices are safe to share within that query. Masked arrays never enter the full-column cache and cannot establish a full-column identity proof.

The native diagnostic confirms that Q3 no longer performs the second full fill of column 3. The diagnostic's fill counters cover resident full-column fills, not every allocation or masked fill, so they are not a whole-query memory measurement. Storage, commits, and update semantics are unchanged by this executor optimization.

## Verification

All 23 focused tests passed. The new regression verifies flat and OR predicates, overlapping group keys and numeric aggregate operands, physical leaves pruned by the predicate, and a following unfiltered query against the interpreter. It also requires that reused operands avoid a full-column fill. Existing JSONBench shape, exact string-identity, and hash-collision checks passed.

The 43-query ClickBench ABBA screen on the retained **200,000-row fixture** was exact in every arm. Baseline hot-median sums were 1.738 / 1.751 seconds; candidate sums were 1.754 / 1.720 seconds, effectively flat at this scale.

Raw evidence root: `build/jsonbench-campaign/source-flag-summary-v47/composite-predicate-reuse-v62/`.

- `native-100m-abba/summary.json`: isolated query timings and exactness.
- `full-verification/`: remaining-query differential and native phase diagnostics.
- `clickbench-summary.json`: all four ClickBench arms.
- `tests-summary.json`, `test-results/`: final 23 passing tests.
- `candidate-manifest.json`, `classpath.txt`: frozen executor classes.

Native binary: `build/jsonbench-campaign/pgo-composite-reuse-v62-100m/jb`, SHA-256 `0cdf4c3dc6a34c99924c3d1c633a709675ff9ca6cfc0c091ad1572066988f9cd`.

The change is uncommitted. `-Dsirix.projection.reuseGroupPredicateSlices=false` retains the previous fill behavior for diagnostic comparisons.
