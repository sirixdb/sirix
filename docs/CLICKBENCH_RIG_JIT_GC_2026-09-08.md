# GC and compilation during the timed tries

The follow-up diagnostics support ongoing compilation and ordinary allocation as
measurement mechanisms. They do not establish how much suite variance either
causes, and they do not justify a new default JVM policy. The validated default
therefore retains the original three-try C6A definition and reports uncertainty.
At ten null pairs, the quiet 50 W calibration still cannot resolve 0.5 ln.

The actionable result is the fast-query try-count curve reported
[separately](CLICKBENCH_RIG_FAST_TRIES_2026-09-08.md). It was briefly available as a
`--steering-fast-tries 5|9` measurement mode; that mode has been removed pending a full-context
calibration, because neither the fast-query pilot nor these diagnostics establish its full-suite
resolution. Future mechanism work should start from the variance attribution,
not an isolated slow query: q6 contributed 15.0%, while q33 contributed only 0.77%.
Per-query covariance allocations already contain the cross-query terms; the top
five's 45.6% and cross-query covariance's 46.1% are overlapping quantities.

Four diagnostic processes used the unchanged `aa4d81d54` runtime: full suite,
five fast queries with 33 tries each, the same fast cohort again, then full suite.
The envelope remained 6/14 GiB heap, 10 GiB arena, 5 GiB eager residency and C2,
with the same 50 W cap, cooldown gate and exclusive leases. JFR used the earlier
profile; perf sampled the native main thread. No affinity, cache or engine policy
changed. A separate full-suite process tested `-Xbatch` with JFR and without perf.

| Diagnostic | All compilations | C2 compilations | All GC phase pauses | Approx. selected-hot GC overlap | Approx. cold GC overlap |
|---|---:|---:|---:|---:|---:|
| Full 01 | 8,494 | 2,746 | 7.933 s | 1.224 s | 4.320 s |
| Fast 02, 33 tries | 6,486 | 1,823 | 0.739 s | 0 s | 0.380 s |
| Fast 03, 33 tries | 6,489 | 1,787 | 0.716 s | 0 s | 0.390 s |
| Full 04 | 8,498 | 2,779 | 8.253 s | 1.596 s | 4.161 s |
| Full, `-Xbatch` | 8,105 | 2,751 | 8.045 s | 1.219 s | 4.408 s |

“Selected hot” always means the smaller of the original tries 2 and 3, including
in the fast cohort. Its five-query component is not a full-suite score. These
sequential profiles differ in overhead and are not a paired speed comparison.
Query windows use light output receipt minus rounded wall time; residual pipe and
scheduling latency means overlaps are approximate, especially for short queries.

q13 and q28 account for approximately 71% and 74% of selected-hot GC overlap in
the two full profiles. In the earlier unchanged twenty-leg calibration, those
queries together accounted for only 1.25% of suite ln variance by covariance
allocation. Their pauses matter to elapsed time, but counting GC seconds alone
does not identify the main sources of score noise. Nor does this allocation prove
that changing GC would have no indirect effect on other queries.

JFR allocation samples identify page/node decoding, projection slice decoding,
dictionary string construction and group canonicalisation as substantial allocation
sites. The leading sampled sites in both full profiles include
`NodeKind$40.deserialize`, `PageKind.deserializeCompressedOverflowPayload`,
`GlobalValueDictionary.ReadView.valueAsString`, and
`SegmentGroupCanonicaliser.canonicaliseMemoised`. Their weights estimate sampled
allocation volume; they are not exact byte counters, retained heap sizes or causal
shares of pause time. These are engine investigation leads. This task changed none
of those execution paths.

C2 compilation still overlaps selected hot attempts of q2/q3/q6/q21/q39/q42 in
the full profiles. `-Xbatch` did not create a settled workload: 35 of 43 selected
hot attempts still overlap C2 compilation, including q6. Its q6 hot tries were
0.066 and 0.032 s. Synchronous compilation can move compilation cost into a timed
attempt; one such run supplies no evidence of reduced leg-to-leg variance. It is
not adopted as a default or treated as a measured speed improvement.

For the next investigation, the completed evidence narrows these hypotheses:

| Hypothesis | Observation and practical conclusion |
|---|---|
| More tries alone settle q6 | No collections in its 660 unprofiled attempts, but process timing levels persist through 33 tries. Do not assume the wider mode fixes q6. |
| Most GC seconds identify the noisy queries | q13/q28 hold 71–74% of selected-hot pause overlap but only 1.25% of earlier suite variance. Pause volume alone does not allocate score noise; indirect effects remain possible. |
| P/E placement explains the observed profiled plateaus | Main-thread samples were mostly on P cores; the profiles do not support this explanation. Profiling changed timing levels, so unprofiled placement is not eliminated. |
| Explicit GC dominates the noise | The initial profile's explicit collection was 2.09% of pause time and in a discarded cold try. Later profiles place some calls in try 2, so neither its share of variance nor universally cold timing is established. The switch study was canceled. |
| Synchronous compilation settles the timed work | The completed `-Xbatch` diagnostic still had C2 overlap in 35 of 43 selected hot attempts. Ongoing compilation remains a live suspect; causality and removable variance are unmeasured. |

The original fast33 cohort reported no collection in any of q6's 660 attempts,
yet q6 retained different timing levels across fresh JVMs. The CPU samples in
the later fast diagnostics were almost entirely on this host's P cores; there
was one mixed-core attempt and no E-core-only attempt. JFR confirmed that perf
sampled the actual native main thread. Those observations do not support a P/E
placement explanation for the profiled plateaus. They also do not rule it out
for the unprofiled cohort: profiled q6 minima of 0.025 and 0.023 s were above many
unprofiled minima of 0.009–0.019 s, showing that the diagnostic setup perturbed
the timing levels. No affinity change is justified by these observations.

The first profile's single explicit `System.gc()` occurred in q18's cold try and
accounted for 2.09% of its GC phase-pause time. That location is not universal:
Full 01 above has an explicit call during q18 try 2; `-Xbatch` has calls during
q18 try 2 and q32 try 1. The q18 second tries were slower than try 3 and therefore
not selected for the hot score in these observations. The canceled GC-budget
switch study remains a separate performance follow-up; it is not resumed or
claimed to explain the noise.

The first placement process completed all 129 timings and exited 0, but its
observer misclassified normal perf termination when the native main thread exited
before the whole JVM. Its perf recording also omitted CPU ids. The original
verdict and reconciliation remain visible. The remaining three processes used a
locally verified CPU-sampling option and corrected exit handling, and completed
without issues. They do not replace the first observation in a calibration.

The new production launcher subsequently completed a full 100M leg in 153.498 s
with exit 0 and no observer issues. This is a wiring/envelope/ownership check,
not a variance or candidate-effect estimate. Focused Java tests, native Java/Python
lease tests, raw-entrypoint rejection and a two-pair 1M wiring test also passed;
the latter correctly returned `UNRESOLVED`, with no 100M precision inference.

[Diagnostic evidence](../bundles/sirix-query/bench/clickbench/rig/evidence/placement-jit-20260908/README.md)
retains the derived query, JIT, GC, allocation and CPU summaries. Unrelated initial environment
and system-property events were removed from exported JFR copies; original local
recordings remain intact. The committed raw JFR/`perf` archive was discarded from the deliverable
and cannot be regenerated, so its replay is no longer runnable; see
[evidence retention](../bundles/sirix-query/bench/clickbench/rig/evidence/RETENTION.md). See the separate
[launcher validation](../bundles/sirix-query/bench/clickbench/rig/evidence/harness-validation-20260908/README.md)
and [measurement command](../bundles/sirix-query/bench/clickbench/rig/README.md).
