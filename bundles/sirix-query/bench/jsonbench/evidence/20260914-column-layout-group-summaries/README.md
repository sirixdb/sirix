# Column layout and sorted group summaries — 2026-09-14

The verified native 100M candidate measured **1.605× and 1.670× ClickHouse** on the five-query warm score, where lower is better. All five results matched ClickHouse. These are fresh-process queries with warm OS caches, not warmed JVM measurements.

| Query | Sirix warm seconds, rounds 1 / 2 | ClickHouse warm seconds, rounds 1 / 2 |
|---|---:|---:|
| Q1 | 0.012 / 0.010 | 0.085 / 0.077 |
| Q2 | 1.983 / 2.130 | 1.678 / 1.640 |
| Q3 | 2.479 / 2.305 | 0.550 / 0.536 |
| Q4 | 0.882 / 0.877 | 0.297 / 0.246 |
| Q5 | 0.880 / 0.892 | 0.285 / 0.294 |

The score is the geometric mean of `(Sirix seconds + 0.010) / (ClickHouse seconds + 0.010)`. Each query ran in alternating engine order over two rounds, with one verified cold-cache attempt and one warm-cache attempt per round. The 24 GiB memory limit, zero-swap scope, and thermal guard passed. Cold scores were 1.809× and 1.637×. The earlier 2.34–2.40× result used the older row-group layout at a different time; the entire gap between those campaigns is not an isolated causal estimate.

## Storage changes and exactness

The new resource contains 99,999,968 rows in 97,657 row groups, with persisted `COLUMN_MAJOR` slot layout. Columns therefore resolve their own contiguous logical slot ranges. The previous full-scale resource was `ROW_GROUP_MAJOR`; small matched row/column comparisons established the reason to reload, rather than assuming the old resource already had the new layout.

Sorted `(string group, ordered long, record key)` leaves now optionally carry per-group min/max summaries. Each summary is an immutable prefix-compressed blob associated with one bounded source leaf. Inserts, deletes and splits update or remove the affected summary through the same writer transaction. A missing summary falls back to the exact source scan. Tie cutlines retain the existing fallback. Original sorted leaf and directory formats are unchanged; older revisions and resources remain readable.

The full read-only audit reconstructed **all 32,727 summary leaves** from their source leaves and compared the encoded bytes. They cover 8,377,929 source rows and 1,374,093 leaf-local group entries. Encoded leaf payload falls from 390,220,697 source bytes to 66,712,537 summary bytes, 82.9% less. This is the covered sorted projection's payload reduction, not a claim about total database size.

The database remains at revision 1, occupies 34,519,582,401 bytes, and retains creation history for all 129 sampled node keys. Tests cover all four versioning strategies, fine writes, rollback, splits, cold historical reopen, missing/corrupt summaries, and asynchronous flushes with overflow-backed summaries. The restored accepted reader passed 13 summary tests and 12 query tests after the rejected batch experiment was removed.

## Controlled smaller screens

- Matched 10M row/column layout loads took 263.557 / 251.519 seconds. Column layout reduced Q2 warm mean by about 16% and Q3 by 30.7%; Q4/Q5 were approximately unchanged.
- On the same 10M column-layout database and native binary, summary off/on ABBA measured Q4 at 0.266 / 0.283 seconds off and 0.158 / 0.154 on; Q5 at 0.275 / 0.285 off and 0.155 / 0.162 on. All results were exact.
- The 43-query ClickBench ABBA screen was exact in every arm. Baseline hot-median sums were 1.782 / 1.804 seconds and candidate sums 1.694 / 1.765 seconds. This is a short regression screen, not a full-scale ClickBench claim.

A subsequent physical batch reader was **rejected and removed**: on the same full database, Q4 warm means increased by 3.98%, while Q5 changed by +0.40%. Correctness alone did not justify retaining the change.

## Reproduction and limitations

The full reload's application time was 2929.154 seconds, but it overlapped tests, profiling and native compilation. It is diagnostic only and must not be used to claim an ingestion speedup. The separate writer-cache and radix candidates were not part of this reload or native image.

Raw artifacts live under `build/jsonbench-campaign/`:

- Paired results: `source-flag-summary-v47/paired-column-summary-v58-100m-attempt1/`
- Native image: `pgo-sorted-summary-v58-100m/jb`, SHA-256 `3cfb8eb8b33f9f57954074610a3a39729e12706ecda329447487106e1d9d3033`
- Frozen JVM runtime: `runtime-column-summary-100m-v58/classpath.txt`, manifest SHA-256 `a8b569d2187ecf688b03d1961810dc0d1a69ce3dde85b839d250c8605bdfa953`
- Load, smaller screens and ClickBench checks: `source-flag-summary-v47/sorted-group-leaf-summary-v58/`
- Full source/summary, layout and history audits: `source-flag-summary-v47/sorted-summary-batch-v59/full-verification/`
- Rejected batch comparison: `source-flag-summary-v47/sorted-summary-batch-v59/native-100m-abba/`

[summary.json](summary.json) retains the paired numbers and evidence identities. The working changes are uncommitted.
