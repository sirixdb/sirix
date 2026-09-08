# Fast-query additional-try variance study

Twenty unchanged-runtime fresh JVMs each ran q2/q3/q6/q21/q42 33 times. The full campaign envelope, C2 selection, flat 50 W cap, cooldown gate, exclusive process-owned leases and natural cache state were retained. All twenty processes exited 0, all 3,300 unique timing records were present, and the controlled MSR limits stayed at PL1=PL2=50 W throughout. The platform-managed MMIO long-term limit, which the rig does not set, read 76 W throughout — see the closing paragraph. No profiling flags were added.

Additional tries reduced the component variance substantially, but q6 did not stabilize. Nine tries
are a reasonable candidate for a separate full-context validation: they reduced component ln SD from
0.584 to 0.214 with 1.5261 s additional timed execution across the five queries; five tries cost
0.5407 s. This is not a claim that the full suite can resolve 0.5 ln.

**The measurement harness no longer exposes this as a live mode.** It was briefly available as
`--steering-fast-tries 5|9`; that option, its parallel selector, its duplicate C6A projection report
and its per-query try plumbing were removed pending the separate full-context calibration this report
says it needs. The single documented comparison is the three-try C6A path. Nothing below is retracted
— the try-count curve is a real measurement and the campaign's best lead on the sampling half of the
variance — but it is a component result awaiting validation, not a resolution the harness can offer.

**How to read the ~2.4x figure.** The AA MDE80 falls from 0.891453 ln at k=3 to 0.372578 ln at k=5,
a factor of about 2.4. That is a **k=3 versus k=5 prefix comparison within the same twenty fast33
JVMs**, all 664 of whose samples recorded 76 W MMIO, so the two prefix estimates share their power
envelope and are directly comparable with each other. It is **not** computed against the quiet-20
cohort's 1.345005 ln full-suite MDE80, and it must not be quoted as an improvement over it: those are
different query sets, different protocols, and — separately — different observed envelopes, since 16
of the quiet-20 legs sampled an MMIO limit below 50 W.

| Tries | Estimator | Component ln SD | Range ln | AA MDE80 ln | AA 95% halfwidth | Warm execution s |
|---:|---|---:|---:|---:|---:|---:|
| 3 | minimum | 0.583518 | 1.973287 | 0.891453 | 0.640268 | 0.967700 |
| 3 | median | 0.403626 | 1.595475 | 0.422299 | 0.303307 | 0.967700 |
| 3 | tail_median | 0.603275 | 2.680062 | 0.809834 | 0.581646 | 0.967700 |
| 5 | minimum | 0.260154 | 0.942455 | 0.372578 | 0.267596 | 1.508400 |
| 5 | median | 0.432588 | 1.471213 | 0.647868 | 0.465318 | 1.508400 |
| 5 | tail_median | 0.306028 | 1.131331 | 0.475638 | 0.341617 | 1.508400 |
| 9 | minimum | 0.213700 | 0.804212 | 0.268422 | 0.192789 | 2.493800 |
| 9 | median | 0.238925 | 0.783706 | 0.319115 | 0.229197 | 2.493800 |
| 9 | tail_median | 0.208916 | 0.738322 | 0.234600 | 0.168496 | 2.493800 |
| 17 | minimum | 0.185489 | 0.708308 | 0.226538 | 0.162706 | 4.469500 |
| 17 | median | 0.207386 | 0.788082 | 0.200089 | 0.143710 | 4.469500 |
| 17 | tail_median | 0.245300 | 0.874480 | 0.273040 | 0.196105 | 4.469500 |
| 33 | minimum | 0.189980 | 0.599621 | 0.209965 | 0.150803 | 8.272000 |
| 33 | median | 0.219162 | 0.719485 | 0.245673 | 0.176449 | 8.272000 |
| 33 | tail_median | 0.220221 | 0.671543 | 0.275397 | 0.197798 | 8.272000 |

Minimum at k=3 is the historical min(try2,try3) projection for these five queries, recorded separately. All other rows are alternative internal estimators.

Per-query ln SD for the internal minimum estimator:

| Query | 3 tries | 5 tries | 9 tries | 17 tries | 33 tries |
|---|---:|---:|---:|---:|---:|
| q2 | 0.124445 | 0.090418 | 0.044183 | 0.044700 | 0.035650 |
| q3 | 0.157458 | 0.101617 | 0.032863 | 0.027769 | 0.025251 |
| q6 | 0.126412 | 0.132276 | 0.146113 | 0.142653 | 0.140854 |
| q21 | 0.198941 | 0.039590 | 0.040148 | 0.031661 | 0.028270 |
| q42 | 0.497408 | 0.133354 | 0.071771 | 0.045213 | 0.041268 |

q6 remained at approximately 0.14 ln SD even after 33 tries; its minimum varied from 0.009 to 0.019 s across fresh processes. q2/q3/q21/q42 improved markedly. More samples therefore do not remove every process-level difference.

The mean process duration for all 33 tries was 17.539 s; mean cooldown was 35.536 s. Prefix execution costs exclude startup, cooldown, result printing and executor setup/teardown. They are not measured end-to-end savings from actually launching a shorter protocol.

The k=3 minimum is the historical min(try2,try3) projection for only these five queries. The other estimators use additional observations and are internal alternatives, never a redefinition of published C6A. The board offset remains +0.01 in every contribution. All earlier selected queries had 33 tries even when analyzing a short prefix of a later query. q42 also lacked the preceding full-suite grouped workload. Absolute scores and warmup state cannot be transferred to the earlier full-suite cohort.

All prefix lengths and estimators were specified before analysis, but choosing the best-looking row is still exploratory. The nominal ten chronological null-pair intervals and normal-model power estimates are not simultaneous guarantees across the table. More tries alone can bias a minimum downward as the number of samples grows; both candidate arms must use exactly the same prespecified estimator and trial counts.

Full-context five/nine-try variance has not been calibrated, which is why the live mode was removed
rather than shipped. Firstmate subsequently canceled the GC-budget switch
comparison after the explicit pause in that initial profile was attributed to
q18's cold try and only 2.09% of traced GC pause time. Later profiles show that the
explicit call is not universally cold. Its budget-sizing performance tradeoff
remains a follow-up. The completed JIT/GC diagnostics, including `-Xbatch`, are
reported [separately](CLICKBENCH_RIG_JIT_GC_2026-09-08.md); they must not be pooled
with this unprofiled cohort.

[Raw evidence, exact plan and replay](../bundles/sirix-query/bench/clickbench/rig/evidence/fast-tries-20260908/README.md) include all attempt distributions and per-query means, SD, minima and maxima.

The pilot's 20 JVMs sampled 50/50 W MSR and 76 W MMIO throughout, so its effective envelope was the
50 W MSR cap. That differs from the quiet and split cohorts, most of whose legs sampled an MMIO limit
below 50 W — see the
[per-leg power audit](../bundles/sirix-query/bench/clickbench/rig/evidence/power-audit-20260908/README.md).
Do not compare this pilot's variance against those cohorts as if the envelopes were identical.
