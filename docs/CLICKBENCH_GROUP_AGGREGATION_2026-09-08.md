# Shared aggregation: exclusive paired measurement, 8 September 2026

The exclusive pair improves C6A hot geomean **4.496 → 4.269**, saving **2.226 sum-ln**. Both heads rank **16/140** against the campaign's checked-in board snapshot. This exclusive pair supersedes the earlier baseline-to-candidate sum-ln reading, which was taken under concurrent load and is not a valid performance result.

The fixed 14-query tail improves **1.175579×** by geometric mean. The candidate remains above the campaign's top-10 geomean target; the measured generic gain is the result delivered here.

## Changes

- `NumericGroupAggTable` can place accumulator records sequentially behind a compact hash index. Index entries hold a 32-bit hash tag and a record handle; complete keys and exact identity lanes still prove equality. Index growth preserves record handles. All chunks remain at most 128 KiB. `GroupTableSpill` enables this layout and owns a separate scan-local index recycler; existing payload recycling and pass budgets remain unchanged. `sirix.projection.groupTable.denseIndex=false` restores sparse indexing.
- Integral COUNT/SUM/AVG composite groups ordered solely by row count use present-count/sum operand pairs. The selector expands only the winning accumulators into the ordinary output layout. Other orders, HAVING, string/deferred operands, and MIN/MAX retain ordinary state.
- Sliced numeric/composite DISTINCT aggregation feeds worker batches directly instead of retaining per-group sink objects. Singleton distinct sets hold their first value inline. Disjoint group stripes publish final counts on the existing query worker pool, and merge bucketing visits the count shards without constructing one large intermediate hash map. The original four value stripes per group remain in effect.

There is no additional input scan, new global dictionary or data cache, persisted-format change, or database rebuild. Dictionary identity and value comparisons are unchanged. Row counts, checked sum overflow, missing operands, and first-seen tie ordering retain their existing semantics.

## Validation

Commit **`f13a32e68fbad8ade9e3f8d3140606e07726d4c5`** passed **123 focused tests** covering collisions, table growth, zero keys, pass ownership and spill, compact sums and overflow, parallel distinct publication/failure/retry, and interpreted-versus-vectorized grouping/top-n differentials with forced passes and ties.

The full **1M** DuckDB gate reports **0 mismatch, 0 missing, 0 unverifiable, and 0 serving declines**. All 43 outputs match the original 1M baseline byte-for-byte. That database has two segments. Both exclusive **100M** legs completed 43 queries × 3 tries with no serving declines; all 43 result files are byte-identical between the heads.

Only the nine changed Java files were formatted with the repository's Spotless/Eclipse profile. The benchmark runner is unmodified and uninstrumented.

## Exclusive pair

Baseline: `aa4d81d547fb0e2353ede959786d6e8ba442edf2`. Candidate: `f13a32e68fbad8ade9e3f8d3140606e07726d4c5`. Both ran from frozen runtime artifacts on the same host, using Oracle GraalVM 25.0.3 with C2, 20 workers, `-Xms6g -Xmx14g`, a 10 GiB off-heap arena, and the same read-only 100M database. The common lock was held continuously from **2026-09-08T02:30:48.119121+00:00** through **2026-09-08T02:35:46.452368+00:00**. No other JVM referenced the database when the lock was acquired or between legs. Firstmate was notified immediately on release.

Baseline ended at `2026-09-08T02:33:31.056085+00:00`; candidate started at `2026-09-08T02:33:31.075036+00:00`. Hot time is the minimum of tries 2 and 3. The sum of hot query times decreases **37.544 → 31.904 seconds**. Full per-try times, CPU times, and output hashes are in [the measurement JSON](CLICKBENCH_GROUP_AGGREGATION_2026-09-08.json).

| Tail query | Baseline hot seconds | Candidate hot seconds | Speedup |
|---|---:|---:|---:|
| q5 | 0.591 | 0.747 | 0.791× |
| q8 | 0.562 | 0.659 | 0.853× |
| q9 | 0.653 | 0.771 | 0.847× |
| q12 | 0.737 | 0.731 | 1.008× |
| q13 | 2.376 | 1.271 | 1.869× |
| q14 | 0.986 | 0.807 | 1.222× |
| q16 | 2.060 | 1.693 | 1.217× |
| q18 | 3.956 | 3.116 | 1.270× |
| q28 | 8.867 | 8.864 | 1.000× |
| q30 | 0.539 | 0.345 | 1.562× |
| q31 | 1.500 | 0.901 | 1.665× |
| q32 | 6.798 | 4.179 | 1.627× |
| q33 | 2.300 | 2.157 | 1.066× |
| q34 | 2.229 | 2.124 | 1.049× |

The improvements span count-only grouping and wider aggregate state: q13/q14/q15/q16/q18/q30/q31/q32/q33/q34/q35 all improve in this pair. Regressions remain visible in the aggregate score: q8 and q9 slow by about 17–18%; q5 changes 0.591 → 0.747 s, q10 0.248 → 0.349 s, q25 0.118 → 0.170 s, and q39 0.264 → 0.401 s. This pair does not establish a cause for every per-query regression. No gains from the parallel string worker are added to these measurements.

## Exclusive access is required

Measurement on this box requires exclusive access. On the identical baseline head, q31's hot wall time was **3.031 s under overlapping load** and **1.500 s with exclusive access**. Its corresponding CPU times were **28.1 s** and **25.8 s**. That observed 2.02× wall-time spread can masquerade as an optimization even though the code has not changed. The larger provisional campaign reading is superseded by the clean **2.226 sum-ln** paired result above.

Hold the shared rig lock continuously across baseline and candidate, check the process table for other JVMs against the 100M database after acquiring it and between legs, and serialize benchmark work on the box. A leftover lock file is not evidence of a live holder; never unlink a lock that may be held. Preserve the 6/14 GiB heap and 10 GiB arena envelope. Record the two heads, JVM flags, run times, and lock-release time with the results.

## Profile and discarded experiments

Exploratory JFR captures put 42–65% of original hot CPU samples for q16/q18/q30/q31/q32 inside the group table, including calls from spill/merge. Top-n was at most 1.6% and already bounded. q8/q9 concentrated approximately 92% of samples in grouped distinct aggregation. q13 also allocated many sinks, singleton sets and map entries; a later profile put 478 of 2552 samples in serial distinct-count publication and 98 in subsequent bucketing. These are CPU sample shares, not independent wall-time fractions.

Row staging for high-uniqueness groups, fused fresh-stripe copying in the sparse table, and unrolled identity comparison were removed after exploratory measurements failed to show a useful broad gain. A lower pass-budget charge and increased distinct value striping were also removed. The budget-only explanation for an earlier q33/q34 slowdown was disproved by another run; Firstmate subsequently identified overlapping run windows. Those earlier timings and causal explanations are not delivery evidence.

Rig mechanics and scoring commands remain documented in [the segment-lane handoff](HANDOFF_SEGMENT_LANE_2026-09-06.md) and `bundles/sirix-query/bench/clickbench/rig/`. Local raw evidence is under `bundles/sirix-query/build/diagnostics/groupby/exclusive1-*`; the paired driver uses one `fcntl.flock` context for both JVMs.
