# Writer dictionary ingestion work — 2026-09-14

Status: correctness and allocation diagnostics passed. The isolated 10M ABBA screen measured **6.79% less load time (1.073× throughput)**. The smaller 1M screen did not show a throughput gain.

The 45-second async-profiler attachment to the frozen 100M column-layout/leaf-summary loader attributed 36.85% of CPU samples and 70.30% of sampled allocation bytes to dictionary-generation flushes. Repeated reads through `NodeStorageEngineWriter.getRecord` reconstructed durable pages for individual dictionary radix records. These are inclusive stack shares from one interval, not percentages of the whole load.

The candidate keeps up to eight writer-owned NAME readback pages. Each hit requires the same durable offset, expected page hash, and fragment keys/revisions; current transaction pages retain priority. Commit, rollback, truncate, and close release the cache. Index-overflow swizzling disables this cache to avoid retaining unbounded overflow payloads. Returned records remain detached from page memory.

A second change batches updates to forward radix upper nodes. Up to four child updates retain the sparse path; larger updates fill one bounded array before constructing the replacement. Persistent key reservation and the wire format are unchanged.

## Allocation diagnostics

Each arm uses a fresh JVM and database, with the same 8,192 distinct values per append and an asynchronous flush after each generation. The counter is `ThreadMXBean.getThreadAllocatedBytes` around the foreground dictionary flush only. It excludes interning, the background flush worker, and final commit. Four arms run in ABBA order. Every arm reopens the dictionary and checks its exact entry count plus regularly spaced reverse-ID values (1,019 samples at 196,608 entries; 4,075 at 786,432).

| Change | Dictionary entries | Counted generations | Baseline bytes, two arms | Candidate bytes, two arms | Reduction |
|---|---:|---:|---|---|---:|
| Eight-page readback cache | 196,608 | Last 20 of 24 | 246,367,456 / 247,333,480 | 216,473,736 / 219,863,424 | 11.6% |
| Eight-page readback cache | 786,432 | Last 32 of 96 | 791,401,880 / 795,171,960 | 614,132,952 / 616,771,160 | 22.4% |
| Radix batching, cache enabled in both arms | 786,432 | Last 32 of 96 | 615,774,480 / 617,124,592 | 480,453,416 / 479,181,896 | 22.2% |

These runs overlapped the full loader and, for some arms, native compilation. Only allocation counts are used here. Recorded wall times are excluded from performance claims. The larger cache screen resumed after a reporting-key typo following its already verified first arm; the first arm was retained and the remaining three completed.

## Correctness

- Writer-focused gates: 59 passed, including cache bounds, hash/fragment identity, failure cleanup, all four versioning strategies, async epochs, rollback, historical reads, and dictionary tail copy-on-write.
- Broader writer gates: 112 core and 22 query checks passed; two opt-in async telemetry tests skipped.
- Final radix gates: 86 core and 10 query checks passed, including dictionary collision/storage, budget, append, versioning, and query parity coverage.

## Isolated loader screen

Four fresh 1M databases were loaded in baseline/candidate/candidate/baseline order. Both arms used the same warmed source file, six processors, 12 GiB maximum heap, 8 GiB off-heap budget, column-major layout, parallel importer, incremental projection and creation history. No compiler, profiler, other load or performance test overlapped the measurements. The 24 GiB / zero-swap scope and thermal guard passed.

Baseline load times were 46.367 / 46.008 seconds; candidate times were 48.317 / 45.731 seconds. Means were 46.188 / 47.024 seconds: the candidate was 1.81% slower in this short screen, within the spread of its individual runs. This does not establish either a throughput win or a stable regression. All five queries were byte-identical across all four databases; row counts, persisted layout and 129 sampled creation histories passed in each arm.

The same protocol on the 10M cleaned source prefix measured baseline times of **252.613 / 247.839 seconds**, versus candidate **232.850 / 233.607 seconds**. Means were 250.226 / 233.229 seconds. The scope and thermal guard passed. All five query dumps were byte-identical across all four resources; each retained 10,000,000 rows, column-major layout, revision 1, and creation history for all 129 sampled keys. These matched results support a 10M ingestion improvement, not a claim that the same ratio holds at 100M.

Raw results: `build/jsonbench-campaign/source-flag-summary-v47/dictionary-radix-batch-v61/load-{1m,10m}-abba/`. The 10M runs include neither the later parallel descriptor reader nor the later sorted-leaf bounds candidate. The input file was derived from the first 10M cleaned records of the full corpus and is identified in the preceding layout experiment's source manifest.

Machine-readable allocations, test counts, and raw evidence paths are in [summary.json](summary.json). Frozen candidate manifests preserve the exact class/source hashes. The completed 100M column-layout load used its earlier immutable runtime and includes neither ingestion candidate.
