# Bounded writer readback cache — 2026-09-14

In a repeated fresh 10M ingestion comparison, the combined candidate reduced average load time from **236.5075 to 160.8265 seconds**: **32.00% less elapsed time / 1.4706× throughput**. Both arms use parallel ingestion, incremental projections, ordinary creation-history recording, the same warmed input, and the same resource limits. Load time includes projection finalization, store close, and durable filesystem synchronization.

| Arm | Load seconds | Exact queries | Layout / creation history | Bounds audit |
|---|---:|---|---|---|
| Baseline 1 | 235.715 | Q1–Q5 | Passed | Not present in baseline |
| Candidate 2 | 161.250 | Q1–Q5 | Passed | Passed |
| Candidate 3 | 160.403 | Q1–Q5 | Passed | Passed |
| Baseline 4 | 237.300 | Q1–Q5 | Passed | Not present in baseline |

The baseline is the previously accepted v61 writer (eight-page readback cache plus radix-child batching). The candidate combines the larger indexed cache with predicate-slice reuse, parallel column directories, sorted-leaf bounds, and the corrected append-only bounds builder. This is a net comparison of that combined candidate; it is not an isolated attribution of every millisecond to the cache lookup table.

## Full-100M verification

The subsequent isolated pair on **99,999,968 rows** reduced load time from **2,654.245 to 1,959.165 seconds**: **26.19% less elapsed time / 1.3548× throughput**, saving 695.080 seconds. Both arms consumed the identical verified gzip input with the same limits and durable synchronization. This is one full-scale pair following the repeated 10M ABBA.

Both databases passed row-count, column-layout, 129-node creation-history, all-five-query, and exhaustive sorted-summary checks. The candidate also passed reconstruction of all **32,727 leaf bounds**, covering 8,377,929 source tuples in 512 chunks / 532,480 bytes. The 24 GiB / zero-swap and thermal guards passed; scope peak memory was 18,801,405,952 bytes, with no memory-limit or OOM events.

Process CPU time fell from **5,486.41 to 4,776.40 seconds**. GC pauses totaled **1.408 / 1.394 seconds** across 114 / 88 pauses, respectively. These pauses explain little of the wall-time difference. Reported file sizes were 34,519,582,408 / 34,519,582,409 bytes; the writer retains preallocated file tails, so reported physical file length does not measure each added metadata payload separately.

The timed v67 classes remained frozen. The final v68 checksum-identity correction and combined-build validation are complete, as recorded below. The full-scale ingestion gain above was measured with v67; the small subsequent correction was cost-checked separately at 10M.

## Why this change

A 45-second async-profiler capture of the preceding combined writer found dictionary planning responsible for about **25% of sampled CPU** and **70% of sampled allocations**, mostly through repeated NAME-page reads. The capture completed before a separate bounds-finalization bug caused that diagnostic load to fail. It is useful as a hot-path profile, not a completed load or a throughput measurement. The bounds bug was reproduced and fixed before the comparison above.

A separate dictionary-only experiment compared eight, 64, and 256 cached pages in forward/reverse order, with identical cold dictionary verification. Mean process times were **13.740 / 8.179 / 5.998 s**. Page misses were **15,911 / 6,356 / 2,031**. Last-32-generation foreground allocations were **479.3 / 404.5 / 369.6 MB**. These focused numbers establish retention pressure; they are not whole-loader speedups.

The production cache replaces the linear slot search with a pre-sized primitive `long → int` map. An offset selects a slot; checksum presence, content hash and copied fragment-offset/revision history must also match. A changed identity replaces its old entry. A victim is retired before a replacement is allocated, and failed reads cannot leave an indexed stale page. No boxing or map growth is needed in the lookup path.

Capacity follows the configured native arena: a power of two between eight and 256 pages, using at most 1/32 of the arena in maximum-size frames above the original eight-page floor. A 512 MiB arena selects 64 pages; an arena of at least 2 GiB selects 256. The frame ceiling is 64 MiB per writer at maximum capacity; this is a native-frame bound, not a claim about total JVM heap usage. Commit, rollback, truncation and close drain the writer-owned cache. Mutable transaction pages still take priority over durable readback.

## Verification and scope

All **105 original focused tests** passed, covering cache identity, arena sizing, full-capacity lookups, failed replacement cleanup, dictionary generations, bulk projection equivalence, sorted bounds, and query serving. The final v68 run passed **106 tests**, including the additional checksum-presence regression. Async readback and historical revisions are exercised under all four versioning strategies.

All four 10M loads returned exactly the saved Q1–Q5 results and passed row-count, column-layout, and 129-node creation-history checks. Both candidate databases also passed complete reconstruction of their **3,499 sorted-leaf bounds** from original source keys (55 chunks / 57,200 bytes). The 24 GiB memory limit, zero-swap policy and thermal guard passed. No other load, compiler, tests or profiler overlapped ranked loads. GC logging was enabled equally; pause totals in the first baseline/candidate pair were about **.310 / .321 s**, so pause time does not explain the 74-second load difference.

## Final combined validation — 2026-09-15

Review found that `PageReference.getHashAsLong()` returns zero both for an absent checksum and for a present checksum whose value is zero. Since the reader uses `hasHash()` to decide whether verification is required, those identities must remain distinct. The new regression failed against v67 and passed after adding a preallocated boolean array to the cache. Hits still allocate no objects and introduce no synchronization. All 106 focused tests passed without failures, errors or skips.

An isolated 10M v67/v68 pair measured **161.526 / 162.646 seconds**: the correction added 0.69% elapsed time in this single pair. Both loads passed exact query, layout and creation-history checks; the final candidate also passed exhaustive bounds verification. This does not substitute for a repeated full-scale v68 ingestion comparison.

The rebuilt native image ran against the freshly ingested **99,999,968-row** candidate database. All five answers matched the independent ClickHouse references. Two engine-order-swapped rounds produced warm geometric ratios of **0.769964 / 0.766888**, using `geomean((Sirix seconds + .010) / (ClickHouse seconds + .010))`; lower is better.

| Query | Sirix warm seconds, rounds 1 / 2 | ClickHouse warm seconds, rounds 1 / 2 |
|---|---:|---:|
| Q1 | .011 / .011 | .078 / .082 |
| Q2 | 1.670 / 1.650 | 1.655 / 1.711 |
| Q3 | 1.548 / 1.584 | .576 / .584 |
| Q4 | .033 / .033 | .299 / .264 |
| Q5 | .877 / .894 | .282 / .306 |

The warm sums remain higher for Sirix: **4.139 / 4.172 s**, versus **2.890 / 2.947 s** for ClickHouse. Q3 and Q5 remain substantially slower; Q2 is near parity. The geometric-score win is not a claim of lower total elapsed time or a win on every query.

The final ClickBench screen used **200,000 rows and all 43 queries**, comparing the final JVM classes with the accepted v64 query candidate. All 344 query dumps across ABBA and BAAB matched the independently verified reference. Mean hot-median sums were:

| Order | Baseline seconds | Candidate seconds | Candidate change |
|---|---:|---:|---:|
| ABBA | 1.7305 | 1.7470 | +0.95% |
| BAAB | 1.7820 | 1.7785 | −0.20% |

The direction does not repeat and the combined mean difference is +0.37%, within the observed run spread. This screen shows no consistent regression at this scale; it is not a full-100M ClickBench result or a statistical proof of equivalence.

### Cold-cache qualification

The raw protocol's `cold` labels mean a fresh process after `fsync`, `posix_fadvise(DONTNEED)` and zero visible-file residency from `mincore`. The workspace uses [eCryptfs, a stacked filesystem](https://www.kernel.org/doc/html/latest/filesystems/ecryptfs.html). These checks do not establish the lower filesystem's cache state. Earlier v64 Q2 runs reported only 0 / 208 input blocks, whereas the first final-build Q2 read reported **4,529,144 512-byte blocks**. The final visible-eviction geometric ratios were **1.3356 / 1.0774**; they cannot establish a like-for-like disk-cold regression or victory over the earlier database.

A subsequent same-database v64/v68 ABBA reproduced the changing I/O behavior with the older executable too: successive Q2 visible-eviction runs read **2,862,184 / 64,328 / 9,936 / 3,680 blocks**, in baseline/candidate/candidate/baseline order. Warm baseline/candidate means for Q2–Q5 were **1.8005/1.609, 1.560/1.532, .0330/.0355, .884/.883 seconds**. The control found no broad query regression; the small Q4 difference is retained in the evidence. Unequal underlying cache state is an inference supported by the stacked filesystem and observed I/O. True disk-cold performance remains unverified.

### Completed diagnostic profile

A separate final-build 10M load completed with exact queries, exhaustive bounds and 129 creation-history checks. A 45-second async-profiler capture collected CPU, wall-clock and allocation events; this instrumented load is explicitly **not ranked**. Dictionary forward planning accounted for **14.73% of sampled CPU**; background disposable snapshot serialization accounted for **29.78%**. Estimated sampled allocations totaled **23.50 GB** in the capture, with **45.27%** under writer-cache read misses. Those inclusive categories overlap other call paths and must not be added together.

The hit path is primitive and preallocated, but page misses, dictionary mutation, record history and serialization still allocate. This is not a zero-allocation ingestion pipeline. The old failed-load profile covered a different window, so differences in profile shares are not a matched whole-loader allocation reduction. Full-load GC pause totals above remain the appropriate evidence about pause time.

The final validation sequence, same-database control and ClickBench repeat all passed the 24 GiB / zero-swap and thermal guards with no memory-limit or OOM events. The final sequence peaked at **22,158,790,656 bytes**. Ranked measurements ran without overlapping compilation, tests, profiling or other benchmarks.

Final native image: `build/jsonbench-campaign/pgo-combined-v68-100m/jb`, SHA-256 `175fe322da7b4f5ecad8ad85839a9fb861c6064c5419f904f9af01a347c674d4`. Final writer class SHA-256: `02fcd626457ffaebe77fd80bd1296ddfa171bcb49c3e11a0a5b0e66d47304884`. The manifest records the matching source hash and the native build records all frozen inputs.

## Reproduction records

Raw roots under `build/jsonbench-campaign/source-flag-summary-v47/`:

- `writer-cache-index-v67/load-10m-abba/`: all load commands, timings, GC logs, query results, bounds and history checks.
- `writer-cache-index-v67/load-100m-pair/`: the full-scale pair, CPU/GC totals, exact source-summary/bounds audits and creation-history checks.
- `writer-cache-index-v67/{candidate-manifest.json,classpath.txt,tests-summary.json,test-results/}`: frozen candidate and passing tests.
- `writer-cache-index-v67/load10m-{scope.scope.json,verdict.json}`: resource and thermal checks.
- `writer-cache-identity-v68/{candidate-manifest.json,review.json,test-before.xml,tests-summary.json,test-results/}`: final identity fix, failing regression and passing tests.
- `writer-cache-identity-v68/load-10m-pair/`: cost check for the final correction.
- `writer-cache-identity-v68/paired-final-100m-attempt1/`: exact native query results and paired ClickHouse measurements.
- `writer-cache-identity-v68/{clickbench-final-summary.json,clickbench-repeat-summary.json}`: ABBA and reverse-order BAAB ClickBench screens.
- `writer-cache-identity-v68/same-database-query-abba/`: older/final executable control with process I/O counters.
- `writer-cache-identity-v68/combined-load-profile-10m/`: completed CPU, wall-clock and allocation capture plus GC log; not ranked.
- `writer-cache-identity-v68/{final,same-db,clickbench-repeat}-{scope.scope.json,verdict.json}`: final resource and thermal checks.
- `writer-cache-capacity-v66/screen-96-generations/`: capacity experiment and exact dictionary checks.
- `sorted-min-bounds-v64/combined-load-profile-10m/`: original CPU, wall-clock, allocation and GC evidence; explicitly marked as an incomplete load.
- `bounds-bulk-build-v65/`: staged-build regression before/after, 70 passing tests and a verified fresh 1M load.

The candidate is generic writer-side code; it does not inspect benchmark names or query text. All changes remain uncommitted.
