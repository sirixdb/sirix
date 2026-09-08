# Split cooldown protocol study, 2026-09-08

Twenty unchanged-code split composites did not improve observed measurement resolution. The sum-ln spread was **3.467**, compared with **3.023** across the twenty quiet full-suite control legs. At ten chronological A-A pairs, the estimated 80%-power detectable effect was **1.359 ln**, versus **1.345 ln** for the control. **Neither protocol resolves the 0.5 ln target at ten pairs.**

Both cohorts used the preserved `aa4d81d547fb0e2353ede959786d6e8ba442edf2` runtime and original flags: 6/14 GiB heap, 10 GiB arena, 5 GiB eager residency, and disabled JVMCI compiler. PL1 and PL2 remained 50 W in every recorded sample. Other lanes stayed down. The split series completed at 2026-09-08T10:31:58Z. All 20 prespecified composites are included; there was no stopping or selection by score.

Each composite runs zero-based q0..20, then q21..42, in two fresh JVMs. Each half uses the original gate of three readings below 55 C, five seconds apart, plus a launch recheck. No OS cache drop occurred. Each query keeps all three tries in one JVM. C6A arithmetic is unchanged: hot=min(try2,try3), the +0.01 offset, and the same fixed board bests. These are steering-only observations and must never be pooled with full-suite history or published-board legs.

| Protocol | Mean sum-ln | SD ln | Min ln | Max ln | Spread ln | Mean hot s | SD hot s | Min hot s | Max hot s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Full suite | 64.620426 | 0.885711 | 63.152371 | 66.175146 | 3.022775 | 37.656150 | 0.316714 | 37.217000 | 38.403000 |
| Split cooldown | 65.929275 | 1.027018 | 64.211588 | 67.678462 | 3.466875 | 37.332600 | 0.378549 | 36.770000 | 38.118000 |

**Seconds and score diverge again:** splitting saved 0.324 mean hot seconds (0.86%) but increased mean sum-ln by 1.309. This is a sequential protocol comparison, not a paired estimate of an engine change.

The ten adjacent split A-A differences have mean **-0.595 ln**, SD **1.364 ln**, and a nominal 95% paired t interval **-0.595 ± 0.976 ln**. Conditional normal independent-pair planning gives 61 pairs for a 0.5 ln effect at 80% power, or 161 pairs using the one-sided 95% upper SD bound. Future balanced AB/BA plans round these to **62** and **162** pairs. The observed pair SD ratio to full is 1.010; this is no practical improvement in this sample, not proof that population variances are identical. These chronological null pairs were not randomized candidate comparisons, and neither stationarity nor power is guaranteed over a much longer campaign.

## Restart boundary

The first query in the second JVM, q21, has a 17.53-fold higher mean first-try time. Its hot mean is slightly lower, while its ln SD is 1.38 times the full-suite value. Fresh-process initialization, JIT, heap residency, page cache and thermal state changed together; these observations cannot isolate a single cause.

| Query | Full first try s | Split first try s | First-try ratio | Full hot s | Split hot s | Hot ratio | Split minus full mean ln |
|---|---:|---:|---:|---:|---:|---:|---:|
| q18 | 6.447750 | 6.433450 | 0.998 | 3.922850 | 3.887600 | 0.991 | -0.008983 |
| q19 | 0.030550 | 0.021700 | 0.710 | 0.012250 | 0.012000 | 0.980 | -0.011100 |
| q20 | 1.913350 | 1.698050 | 0.887 | 0.031450 | 0.035000 | 1.113 | +0.080400 |
| q21 | 0.260350 | 4.563650 | 17.529 | 0.184750 | 0.168550 | 0.912 | -0.095404 |
| q22 | 5.272450 | 5.010300 | 0.950 | 0.329000 | 0.315850 | 0.960 | -0.047938 |
| q23 | 0.495550 | 0.396350 | 0.800 | 0.119150 | 0.073750 | 0.619 | -0.422816 |
| q24 | 0.072150 | 0.066800 | 0.926 | 0.017750 | 0.019950 | 1.124 | +0.076313 |

## Remaining instability and sensitivity

q33 remains comparatively stable: 2.180–2.340 s, ln SD 0.0160. The original twofold q33 failure did not reproduce in either quiet cohort. q39 remains unstable at 0.124–0.360 s, ln SD 0.2882, versus 0.2051 in full runs. q42 becomes the largest ln-noise contributor by individual SD: 0.046–0.182 s, ln SD 0.3891. Its hot mean is 3.291 times the full-suite mean and its mean contribution increases by 0.982 ln. q23 improves its mean contribution by 0.423 ln. The [complete comparison CSV](../bundles/sirix-query/bench/clickbench/rig/evidence/split50-20260908/comparison-with-full.csv) includes all 43 queries.

No query or suite modality screen rejects unimodality after Holm adjustment across 45 tests. At n=20 this does not prove a single mode, particularly with millisecond-rounded short-query times. The [query summary](../bundles/sirix-query/bench/clickbench/rig/evidence/split50-20260908/query-summary.csv) gives mean, SD, min, max, ln spread and modality screens for every query.

The real CPU-side sensitivity requested in inbox 010 is **not yet established** for either protocol. The 1.345/1.359 ln values describe detectable score differences under their noise models; they do not quantify how much CPU work a query optimization must save. The cache-cleared protocol and a separate CPU perturbation calibration remain outstanding. Low process CPU utilization alone would not prove I/O bottlenecking because serial CPU work also has low utilization. A process-affinity perturbation would measure a specific resource response, not a known percentage of CPU throughput on this heterogeneous CPU.

Root cache clearing is pending authorization. `sudo -n true` reports a password is required; no cache-clearing command or fallback was executed. The proposed hook requests only `sudo -n /usr/bin/tee /proc/sys/vm/drop_caches`, with exactly `1` on stdin, before cooldown for each fresh half and never between query tries. No cap tuning, engine change, database write, load, or corpus modification occurred.

The split and full cohorts cannot be assumed to share a power envelope: the
[per-leg power audit](../bundles/sirix-query/bench/clickbench/rig/evidence/power-audit-20260908/README.md)
finds 39 of these 40 split JVMs, and 16 of the 20 full-suite control legs, sampled an MMIO long-term
limit below the verified 50 W MSR cap. The variance comparison below is what was measured; it is not
a comparison at a proven common envelope.

**The split protocol was removed from the harness.** It made variance worse rather than better, so
`measure.py` no longer offers `--parts` and every leg now runs the full suite in one JVM. This report
and the `split50-20260908` evidence remain as the measurement that settled it; reviving the arm means
rebuilding it against fresh evidence.

[Full-suite control report](CLICKBENCH_RIG_VARIANCE_2026-09-08.md). [Split evidence](../bundles/sirix-query/bench/clickbench/rig/evidence/split50-20260908/README.md) retains the complete derived study; its raw-log archive and analyzer were discarded from the deliverable and cannot be regenerated, so the study cannot be recomputed from raw logs ([retention](../bundles/sirix-query/bench/clickbench/rig/evidence/RETENTION.md)). These are completed evidence cohorts, not completion of the instrumentation task.
