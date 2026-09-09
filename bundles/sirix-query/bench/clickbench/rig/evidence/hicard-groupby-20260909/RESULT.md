# Bounded aggregation heap allowance: fixed paired result

Baseline `b815d459d1218ab9081f360257ae6f16eff9f476`; candidate `ddf3f2794bec3feb483ed4bba9e08e3cb8c87c85`.

**Outcome:** the prespecified seven-query family improved by 0.409295 ln and 1.137250 seconds; the whole suite saved 1.095500 seconds. Their paired intervals exclude zero. **Whole-suite score improvement is not established**: its +0.289810-ln mean has a confidence interval spanning zero. This change narrows the measured family gap, but it does not reach DuckDB parity or establish a top-10 score.

**Rig signs:** benefit = baseline − candidate, positive means improvement. In the table, delta = candidate − baseline, positive means regression. Rows are sorted by delta descending (regressions first).

TARGET RESOLVED UNDER THE NOISE MODEL: actual change remains subject to its confidence interval

12 fixed balanced pairs, seed 0; C6A hot min(try2,try3) with the 0.01-second scoring floor. The 1.5-ln prespecified target is a resolution target, not the measured effect.

| Measure | Baseline | Candidate | Benefit (baseline − candidate) |
|---|---:|---:|---:|
| Suite hot seconds | 27.620750 | 26.525250 | +1.095500 |
| Sum-ln | 58.015935 | 57.726125 | +0.289810 |

Paired suite benefit 95% CI: [-0.670193, +1.249813] ln. Seconds-saved 95% CI: [+0.903446, +1.287554].
Rig estimated 80%-power detectable effect: 1.342700 ln; required pairs 12, conservative requirement 22. No additional runs were launched.

The rig’s “TARGET RESOLVED” label applies to the prespecified 1.5-ln planning target, not to the observed 0.29-ln suite mean. Asked after the study about a 0.29-ln suite effect, the rig reports **UNRESOLVED** and estimates 216 pairs (516 conservatively). Those estimates do not authorize more runs. For the prespecified family, a post-study resolution check at the observed approximately 0.4-ln scale requires 10 pairs, including the conservative estimate; 12 were completed, and its detectable scale is 0.083738 ln. These are reporting diagnostics, not changes to the fixed stopping rule; exact outputs are in family-resolution.json.

## Correctness and execution

Every one of all 43 lossless answer files was byte-identical in every one of the 24 legs, including each baseline against the pre-change profile. The separate sparse SUM-ordering defect remains known and non-passing; this candidate reproduces the same wrong bytes, as documented in SUM_ORDERING_DEFECT.md. It was not fixed or hidden by this lever.

Local validation: 89 passing tests, one explicitly skipped known-defect test. Synthetic allocation failure and incorrect cardinality estimates recovered through the existing abort/restart path with exact answers. The probing prototype at 1703ebe2d is absent from the measured candidate.

| Query | Baseline profile passes | Predicted candidate | Candidate hot-pass counts (frequency across 24 tries) | Candidate hot abort counts |
|---|---:|---:|---|---|
| q16 | 2 | 1 | {1: 24} | {0: 24} |
| q18 | 4 | 1 | {1: 24} | {0: 24} |
| q32 | 7 | 2 | {2: 24} | {0: 24} |

Baseline pass counts come from the unchanged baseline profile; the new optional compact plan summary is available only in the candidate. Both runtime manifests carry the same diagnostic property. Fewer passes do not divide lookup work by the old pass count: range filtering already rejects other partitions before probing. Larger tables can increase cache misses, and the paired result above is the evidence of payoff.

## Family effects

| Family | Baseline seconds | Candidate seconds | Benefit ln | Paired 95% CI ln |
|---|---:|---:|---:|---|
| seven | 12.968417 | 11.831167 | +0.409295 | [+0.349424, +0.469166] |
| composite | 9.487917 | 8.368250 | +0.406805 | [+0.349733, +0.463877] |
| string | 3.480500 | 3.462917 | +0.002490 | [-0.022083, +0.027063] |

The seven-query family saves 1.137250 seconds, paired 95% CI [+1.061938, +1.212562].

## Every query, sorted by delta

| Query | Baseline s | Candidate s | Delta s | Baseline ln | Candidate ln | Delta ln |
|---|---:|---:|---:|---:|---:|---:|
| q39 | 0.144333 | 0.192000 | +0.047667 | 1.570937 | 1.790380 | +0.219443 |
| q25 | 0.099417 | 0.111250 | +0.011833 | 2.370195 | 2.445976 | +0.075781 |
| q36 | 0.062000 | 0.066917 | +0.004917 | 1.276337 | 1.339580 | +0.063243 |
| q41 | 0.033167 | 0.035500 | +0.002333 | 1.194493 | 1.251121 | +0.056629 |
| q20 | 0.026083 | 0.028167 | +0.002083 | -0.657757 | -0.609607 | +0.048150 |
| q3 | 0.032833 | 0.034750 | +0.001917 | 1.044443 | 1.084117 | +0.039674 |
| q19 | 0.010417 | 0.011167 | +0.000750 | 0.710908 | 0.748039 | +0.037130 |
| q14 | 0.719917 | 0.730417 | +0.010500 | 1.329917 | 1.343916 | +0.013999 |
| q34 | 1.985583 | 2.008917 | +0.023333 | 2.011219 | 2.022952 | +0.011733 |
| q22 | 0.293000 | 0.296417 | +0.003417 | 2.278421 | 2.289040 | +0.010619 |
| q35 | 0.404583 | 0.408917 | +0.004333 | 1.136887 | 1.147315 | +0.010429 |
| q13 | 1.186500 | 1.195583 | +0.009083 | 1.321629 | 1.329153 | +0.007524 |
| q40 | 0.021750 | 0.021750 | +0.000000 | 0.966672 | 0.972127 | +0.005455 |
| q24 | 0.017000 | 0.017167 | +0.000167 | 0.992794 | 0.997402 | +0.004608 |
| q4 | 0.306667 | 0.307750 | +0.001083 | 1.303284 | 1.306685 | +0.003401 |
| q9 | 0.724417 | 0.726000 | +0.001583 | 1.130888 | 1.132875 | +0.001987 |
| q8 | 0.637167 | 0.637417 | +0.000250 | 1.336745 | 1.337167 | +0.000422 |
| q30 | 0.330333 | 0.330417 | +0.000083 | 1.286527 | 1.286772 | +0.000245 |
| q0 | 0.001000 | 0.001000 | +0.000000 | 0.095310 | 0.095310 | -0.000000 |
| q15 | 0.434833 | 0.434750 | -0.000083 | 1.028748 | 1.028574 | -0.000174 |
| q37 | 0.043000 | 0.042667 | -0.000333 | 1.255889 | 1.254740 | -0.001149 |
| q28 | 6.030750 | 6.016417 | -0.014333 | 1.530539 | 1.528234 | -0.002305 |
| q5 | 0.658500 | 0.657000 | -0.001500 | 2.050572 | 2.048254 | -0.002317 |
| q12 | 0.688250 | 0.685000 | -0.003250 | 1.406648 | 1.402165 | -0.004483 |
| q38 | 0.040000 | 0.039667 | -0.000333 | 1.345110 | 1.337294 | -0.007817 |
| q6 | 0.019750 | 0.019167 | -0.000583 | 1.071632 | 1.061612 | -0.010020 |
| q21 | 0.101000 | 0.099917 | -0.001083 | 1.150211 | 1.139799 | -0.010412 |
| q33 | 2.072333 | 2.047500 | -0.024833 | 2.053968 | 2.041962 | -0.012006 |
| q29 | 0.031250 | 0.030333 | -0.000917 | 1.008267 | 0.987697 | -0.020570 |
| q7 | 0.025333 | 0.027333 | +0.002000 | 0.916094 | 0.893373 | -0.022720 |
| q26 | 0.019583 | 0.018917 | -0.000667 | 1.083347 | 1.060393 | -0.022954 |
| q31 | 0.935250 | 0.913167 | -0.022083 | 1.915478 | 1.892300 | -0.023178 |
| q23 | 0.120750 | 0.117250 | -0.003500 | 1.336935 | 1.312440 | -0.024495 |
| q2 | 0.044500 | 0.042500 | -0.002000 | 1.683486 | 1.654237 | -0.029249 |
| q42 | 0.042333 | 0.035500 | -0.006833 | 1.218461 | 1.176079 | -0.042382 |
| q10 | 0.219833 | 0.210000 | -0.009833 | 1.881220 | 1.831364 | -0.049856 |
| q17 | 0.043333 | 0.040667 | -0.002667 | 1.671284 | 1.620752 | -0.050532 |
| q1 | 0.033000 | 0.030917 | -0.002083 | 1.360216 | 1.308207 | -0.052009 |
| q27 | 0.234667 | 0.221417 | -0.013250 | 0.956065 | 0.900378 | -0.055687 |
| q16 | 1.524417 | 1.427417 | -0.097000 | 2.022217 | 1.957313 | -0.064904 |
| q11 | 0.193667 | 0.178583 | -0.015083 | 1.667385 | 1.595047 | -0.072338 |
| q32 | 4.097917 | 3.615250 | -0.482667 | 2.468377 | 2.343380 | -0.124997 |
| q18 | 2.930333 | 2.412417 | -0.517917 | 1.233936 | 1.040209 | -0.193727 |

## Supplied DuckDB reference

DuckDB values are the supplied 20-leg per-query medians from this laptop at 50 W; no DuckDB run was performed. Sirix values here are means of the rig hot selector across 12 candidate legs, so this is a reference comparison, not a new paired cross-engine study.

| Query | Candidate s | Supplied DuckDB s | Candidate / DuckDB | Remaining parity gap ln |
|---|---:|---:|---:|---:|
| q12 | 0.685000 | 0.333 | 2.057 | +0.706181 |
| q14 | 0.730417 | 0.392 | 1.863 | +0.610761 |
| q16 | 1.427417 | 0.819 | 1.743 | +0.550383 |
| q18 | 2.412417 | 1.514 | 1.593 | +0.463427 |
| q31 | 0.913167 | 0.507 | 1.801 | +0.579767 |
| q32 | 3.615250 | 1.904 | 1.899 | +0.638728 |
| q33 | 2.047500 | 1.734 | 1.181 | +0.165310 |

## CPU and GC diagnostics

These are means across both hot tries in every leg (24 observations per arm), rather than the min-of-two selector used for scoring. GC pause durations are wall-time diagnostics, not a decomposition of total CPU time.

| Query | Baseline CPU s | Candidate CPU s | Baseline GC s | Candidate GC s | Baseline GC pauses | Candidate GC pauses |
|---|---:|---:|---:|---:|---:|---:|
| q12 | 10.404 | 10.488 | 0.023 | 0.032 | 1.17 | 1.46 |
| q14 | 11.567 | 11.700 | 0.023 | 0.028 | 1.79 | 2.04 |
| q16 | 23.462 | 21.958 | 0.050 | 0.060 | 2.21 | 2.54 |
| q18 | 49.846 | 40.929 | 0.042 | 0.125 | 2.17 | 3.83 |
| q31 | 16.104 | 16.000 | 0.048 | 0.054 | 1.04 | 0.83 |
| q32 | 77.612 | 70.708 | 0.067 | 0.212 | 0.54 | 3.17 |
| q33 | 32.717 | 32.579 | 0.098 | 0.087 | 5.21 | 5.50 |

## Interpretation and next lever

The arithmetic prediction held in every hot execution: q16/q18/q32 used 1/1/2 passes, with no hot restarts. Speed changed much less than pass counts: q16 improved from 1.524417 to 1.427417 seconds, q18 from 2.930333 to 2.412417, and q32 from 4.097917 to 3.615250. CPU fell on those queries, while q18 GC pause time rose from 0.042 to 0.125 seconds and q32 from 0.067 to 0.212 seconds (means across both hot tries). Those are measured diagnostics. They are consistent with larger resident state adding collection work; without another profile they do not identify every remaining CPU cost.

**Calibration finding:** cutting q32 from seven passes to two saved 0.482667 seconds of 4.097917 seconds (11.8%); q18 saved 17.7%, and q16 saved 6.4%. The net benefit of removing rescans is approximately 12–18% on q32/q18, not six-sevenths of q32’s cost. This is an observed net saving, not an exact decomposition: larger tables also increased GC time. The baseline profiles identify per-group table work — lookup, merge acquisition and spill/copy — as the largest shared target on q32/q18/q16/q31. Reducing pass counts retains that work for every group, so it remains the leading next target; no post-change CPU profile was collected.

The source also shows why seven-to-two passes cannot promise a 3.5x improvement: non-owned groups were already rejected before table probing. The final exact table work still processes all groups. The string-dominated subset is unchanged within uncertainty (+0.002490 ln, CI spanning zero), as the profile predicted. The largest observed individual score regression is q39 (+0.219443 delta ln, +0.047667 seconds); it is retained in the all-query report rather than hidden by the family gain.

Keep this candidate as a measured family/time improvement, with the whole-suite score limitation explicit. The probing prototype has **not earned a place in this change**: it has no paired measurement, and this experiment does not measure its incremental benefit. It remains a plausible separate experiment because exact lookup work survives the reduced pass count; q31, already one pass and with almost unchanged CPU here, is a useful case. The composite four retain about 2.232 ln of reference parity gap (q32 0.639, q31 0.580, q16 0.550, q18 0.463). That residual gap and the baseline lookup samples justify evaluating the parked prototype after this change lands, rebased onto this budget policy. They do not establish that append-only or cached probing will win: extra partial records, merge traffic and larger resident state can offset saved probes. A brief new profile should verify the remaining cost before a paired comparison on those four queries. A further profile and paired test need a new Firstmate allowance. No probing or disk-spill implementation is folded into this candidate. The accepted disk-spill scope remains in PASS_BUDGET.md.

## Envelope, artifacts and remaining allowance

Every 100M JVM ran under the rig exclusive lease, with MemAvailable >= 26 GiB immediately before launch, one benchmark JVM, the unchanged -Xms6g -Xmx14g heap, 10 GiB arena and canonical 5 GiB eager budget. Both MSR power limits were checked at 50 W; platform-managed domains are retained in the telemetry and rig report. No DuckDB run or database/corpus write occurred.

Observed power summary: `{"distinct_starting_domain_readings": 6, "legs": 24, "legs_with_a_limit_change": 21, "legs_without_a_power_record": 0, "limit_changes": 91}`.

Two profile JVMs and 12 paired comparisons consumed this task allowance. **Remaining paired allowance: zero, including no-mistakes fix agents and verifiers.** Any further 100M run needs Firstmate authorization.

The original leg documents, raw logs, telemetry, commands, answer proofs, manifests and replay inputs
were archived off-tree on 2026-09-09 in `build/rig-evidence-archive-20260909.tar.gz`; the findings
and numbers in this result are retained here.
