# Attribution of ClickBench measurement variance, 2026-09-08

**The full-suite excess is concentrated in a set of short queries and shared run state; it is neither uniform across all 43 queries nor dominated by q33/q39.** q6 contributes 15.0%, q3 10.0%, and q42 7.6% of the observed suite variance. The top five account for 45.6%, and the top ten for 71.8%. Positive cross-query covariance accounts for 46.1% of the total variance. These are descriptive attributions, not identified causes or estimates of variance removable by fixing a query.

Firstmate inbox 014 reports an independent DuckDB series on this same capped quiet machine: 20 legs, 1.592 ln range and MDE80 0.491 ln. Those results show that the earlier Sirix-only studies did not establish a universal hardware resolution floor. Our measured floor of about 1.35 ln is specific to the Sirix protocol/runtime in those cohorts. The DuckDB figures are supplied by Firstmate; this report does not reanalyze that lane’s raw data. The cache-drop arm is cancelled; there is no authorization to run it under the current plan, it never ran, and its harness code and `sudo` hook have since been removed rather than shipped unused — the authorization request preserved under `evidence/split50-20260908/` records what it would have done.

## Method and identities

Analyze the 20 full and 20 split observations separately. For each run let S be the sum of its 43 per-query ln contributions. The additive variance attribution of query q is Cov(q,S)/Var(S); these signed shares sum to 100%. The diagonal sum is the sum of the 43 individual variances. The remainder is twice the sum of all distinct cross-query covariances. Negative shares represent cancellation, not invalid data. The same calculation on the ten complete chronological A-A differences explains the noise relevant to the paired estimate. No query is removed, score definition changed, or protocol pooled.

The observed max-minus-min range is a different quantity: subtract every per-query contribution of the minimum-suite leg from the corresponding value in the maximum-suite leg. Those 43 signed differences sum exactly to the observed range. Endpoints were selected after observation, so this attribution is descriptive and is not a confidence interval.

| Cohort | Suite variance ln² | Sum of individual variances | Cross-query covariance remainder | Cross-query share |
|---|---:|---:|---:|---:|
| Full | 0.784485 | 0.422930 | 0.361554 | 46.1% |
| Split | 1.054766 | 0.559325 | 0.495440 | 47.0% |

## Full-suite attribution

| Query | Mean hot s | Suite variance share | Paired variance share | Contribution to observed 3.023 ln range |
|---|---:|---:|---:|---:|
| q6 | 0.021500 | 15.01% | 13.09% | +0.419854 ln |
| q3 | 0.043150 | 10.02% | 13.94% | +0.190044 ln |
| q42 | 0.039350 | 7.61% | 8.26% | +0.192904 ln |
| q21 | 0.184750 | 6.96% | 10.53% | +0.217065 ln |
| q2 | 0.044550 | 6.02% | 3.50% | +0.276987 ln |
| q19 | 0.012250 | 5.75% | 4.97% | +0.182322 ln |
| q20 | 0.031450 | 5.54% | 4.98% | +0.105361 ln |
| q24 | 0.017750 | 5.41% | 4.57% | +0.143101 ln |
| q39 | 0.283550 | 5.11% | 5.90% | -0.095310 ln |
| q23 | 0.119150 | 4.35% | 6.53% | -0.047467 ln |
| q17 | 0.048800 | 4.01% | 4.52% | +0.179048 ln |
| q7 | 0.021900 | 3.79% | 6.42% | +0.054067 ln |
| q25 | 0.106250 | 3.60% | 3.89% | +0.066375 ln |
| q38 | 0.040200 | 2.96% | 3.16% | +0.080043 ln |
| q10 | 0.282200 | 2.75% | 3.27% | +0.243184 ln |

The paired-noise leaders are q3 (13.9%), q6 (13.1%), q21 (10.5%), and q42 (8.3%). They account for 45.8% of paired variance. q33 contributes only 0.77% of full-suite variance; q39 contributes 5.11%. The original suspects are therefore insufficient as the sole investigation targets.

The observed range compares **quiet50-20 minus quiet50-08**. Its largest terms are q6 +0.420, q2 +0.277, q10 +0.243, q21 +0.217, and q36 +0.196 ln. Those five explain 44.8% of that endpoint difference; the top ten explain 75.4%. Range and variance rankings differ because a single pair of endpoints cannot characterize the whole series.

## Split comparison

Splitting changes the attribution: q42 contributes 22.7% of suite variance, followed by q25 8.3%, q7 7.5%, q22 7.1%, and q36 6.2%. The top ten account for 74.7%, while cross-query covariance accounts for 47.0%. q42 is also the largest paired-noise term (13.0%). The range compares split50-04 minus split50-17; q42 alone contributes +1.062 ln of the 3.467 ln range. The restart therefore moves variability between queries rather than eliminating shared variation. This observation does not isolate JIT, GC, cache, or thermal effects.

## Existing resource clues

- q3’s worst full-cohort hot pair is quiet50-14: try 2 is 0.134 s, 2.0 process CPU s, zero GC collections; try 3 is 0.126 s, 1.7 process CPU s, one collection and 0.09 s collection time. A collection could account for much of try 3’s excess, but cannot by itself explain the slow try 2. The min-of-two score stays bad when both hot tries are affected by potentially different mechanisms.
- q2, q21, q39 and q42 have zero reported GC collections in all 20 selected full-cohort hot tries. q39 still ranges from 0.141 to 0.350 s. Direct recorded collection overlap cannot be the complete explanation for these swings; earlier collection effects, allocation budgets, compiler activity, safepoints and memory stalls remain possible.
- q6, the leading covariance term, is MIN/MAX(EventDate), served by the string-min-max route. Its selected hot time ranges from 0.011 to 0.031 s, so small absolute overhead changes carry substantial ln weight. q3 is AVG(UserID), served by projection aggregation. q21/q39/q42 include grouping routes. This mix makes front-end/JIT lifecycle and shared process pressure plausible leads, not established causes.
- CPU counters cover all JVM threads, including compiler and collector threads. A higher CPU total cannot be attributed to query workers alone. The existing resource label says “pauses,” but it reads GarbageCollectorMXBean collection-time counters rather than an explicit safepoint pause trace. Use GC/safepoint logs for precise attribution.

## Timed-region audit and next evidence

In `ClickBenchRunMain.runSuite`, the wall timer surrounds `execute(chain,ctx,text)`. That method constructs a new Query, executes it, and fully serializes the result to a StringWriter. Per-try executor construction/installation, serving/resource-counter reads before the start timestamp, executor cleanup, printing, correctness dumps and final file-size/report output are outside the wall timer. Catalog warmup is also outside. There is no evidence here that logging itself is being charged inside the reported wall time.

Query construction and full materialization are real timed work. Removing either on faith would change what earlier legs measured. The next diagnostic should retain the exact engine/runtime/envelope, capture compilation events, explicit GC and safepoint times, and per-process/host memory pressure, swap and reclaim around query boundaries. It should distinguish process-wide compiler CPU from query work and mark instrumented runs as diagnostic. No engine implementation change or hardware tuning is justified by this decomposition alone.

No new query JVM or cache operation was launched to produce this analysis. The 50 W setting is unchanged. All forty original observations are retained. The [machine-readable decomposition](../bundles/sirix-query/bench/clickbench/rig/evidence/variance-decomposition-20260908/summary.json), [full query CSV](../bundles/sirix-query/bench/clickbench/rig/evidence/variance-decomposition-20260908/full-queries.csv), and [split query CSV](../bundles/sirix-query/bench/clickbench/rig/evidence/variance-decomposition-20260908/split-queries.csv) cover every query and include leave-one-out sensitivity ranges. Those ranges are not confidence intervals.

## Campaign practice

Firstmate inbox 015 records that two lanes spent hours on an apparent 2.13× q33 regression, including four A-B-B-A repeats. In this quiet cohort q33 contributes only 0.77% of suite variance. That does not retroactively diagnose the earlier anomaly, but it establishes the campaign practice: decompose suite variance before choosing an individual query to investigate. Prioritize the contributions that affect the campaign’s measured score.

The top-five share (45.6%) and the cross-query share (46.1%) overlap: Cov(q,S) already includes q’s covariance with other queries. They must not be added or treated as independent halves of the variance. These five queries total 0.333 s of mean hot time, about 0.9% of the 37.656 s full suite, so their large ln-noise weight is disproportionate to their wall-time cost.

The resource-counter interpretation follows the Java 25 [GarbageCollectorMXBean contract](https://docs.oracle.com/en/java/javase/25/docs/api/java.management/java/lang/management/GarbageCollectorMXBean.html): collection time is approximate and need not increase for every very short collection. Explicit GC and safepoint recordings are required to distinguish pauses from that aggregate counter.
