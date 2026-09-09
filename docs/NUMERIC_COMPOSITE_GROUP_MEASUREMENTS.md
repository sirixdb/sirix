# Numeric composite grouping: measured costs and discarded folds

Measured on 2026-09-07 from campaign base `ca4c34d38`, on the shared i7-12700H
(12 P-core logical CPUs, 8 E-core CPUs, 32 GB). These are single-query **diagnostics**,
not a scored ClickBench suite or a leaderboard improvement. Database:
`clickbench-seg100m-20260905-2328`, 99,997,497 rows, 97,737 leaves, 148 segments.

The change skips the aggregate fold after a composite key acquires `DISCARD_HANDLE`.
The key's transforms, missing mask, exact identity and identity proofs still execute.
The owning hash-range pass performs every aggregate fold and distinct insertion.
Partitioning, probes, worker sizing, group budgets and persisted data are unchanged.
The gain is in q32; **q31 has no discarded rows and earns no mechanism-level gain**.

## Route evidence comes before the hypothesis

Both `diag100m.sh 31` and `diag100m.sh 32` serve `route=group-aggregate`, without
`[proj] groupAgg decline`. Their paths differ despite having the same numeric key:

| Hot diagnostic | q31 | q32 |
|---|---:|---:|
| Qualifying rows per scan | 13,172,392 | 99,997,497 |
| Completed hot passes | 1 | 7 |
| Column access | windowed | resident |
| Baseline scan / final-merge wall | 4,023 / 402 ms | 4,499 / 2,758 ms, summed over passes |
| Baseline hot CPU / wall | 34.9 / 4.463 s | 134.8 / 7.284 s |
| Baseline hot GC | 4 pauses, 0.07 s | none |

q31 prints `[proj] groupAgg windowed slices: the fill budget refused residency,
workers decode per-sub-chunk windows of the needed columns`. q32 does not.
Both use stride 15, a 14,680,064-group budget, 1,024 partitions, and zero shared
rehashes on the completed passes. q32's cold seed aborts at one pass and restarts
with seven; its hot execution starts at seven without a restart. q31 completes one
pass without a restart. `spilled` excludes workers' final local tables, so it must
not be mistaken for qualifying-row or final-group count.

## CPU stacks and hardware counters

The prepared launcher enables inherited `perf stat --no-scale` counters after the
try-1 resource line and disables them after try 2. async-profiler records CPU stacks
at 1 ms over approximately the same window; `rig/collapsed.py` folds the stacks.
Both profiler boundaries succeeded. Perf enable/disable acknowledgements were
819/1,531 microseconds for q31 and 1,031/1,153 microseconds for q32. Attach and marker
latency mean the windows are not nanosecond-identical.

| Baseline inclusive CPU samples | q31 (30,154 samples) | q32 (122,020 samples) |
|---|---:|---:|
| Column read/decode (`decodeLeafSlices`) | 55.3% | approximately 0% |
| Local `acquireExact` | 6.39% | 20.33% |
| Merge `acquireExact` | 13.43% | 24.84% |
| `mergeStripes`, including its probes | 12.0% | 25.9% |
| `foldSliced` | 0.6% | 9.6% |
| `buildPartitionIndex` | 2.3% | 6.7% |

Inclusive rows overlap and must not be added indiscriminately. For q31, file-channel
reads alone cover 45.9% of samples. Its user-space PMU counters do not cover the
substantial kernel work visible in the CPU profile.

P-core raw slot categories below use the sum of retiring, bad speculation, frontend
and backend counts as denominator, as specified before measuring. These are raw
categories, not corrected named TMA metrics. Memory is a subset of backend; core
backend is the difference. IPC and stall fractions use co-scheduled counters.

| Hot hardware evidence | q31 | q32 |
|---|---:|---:|
| P retiring slots | 31.52% | 37.09% |
| P bad-speculation slots | 9.16% | 6.44% |
| P frontend slots | 10.01% | 5.52% |
| P memory slots | 38.41% | 40.00% |
| P non-memory backend slots | 10.90% | 10.95% |
| P instructions / cycle | 1.1627 | 0.9991 |
| P `memory_activity.stalls_l3_miss` / cycles | 45.01% | 48.33% |
| P retired L3 misses / 1,000 instructions | 1.0496 | 1.2916 |
| E instructions / cycle | 1.2428 | 1.1895 |
| E `mem_bound_stalls.load` / cycles | 21.08% | 26.31% |
| E `mem_bound_stalls.load_dram_hit` / cycles | 16.25% | 23.43% |

Perf reports P/E running fractions of 54%/45% for q31 and 59%/40% for q32. The
hybrid PMUs cannot count time spent on the other CPU type; `--no-scale` retains
actual domain counts instead of extrapolating them. All events in each domain
share their group's runtime. Neither domain reported unsupported or uncounted
events. Do not combine their IPCs or equate these fractions with multiplexing.

**Interpretation:** q32 has substantial memory-stall pressure alongside substantial
instruction work; the stacks place that pressure in grouping, not decoding or GC.
The handoff's diagnosis is supported for q32. It does not explain q31's dominant
windowed I/O/decode cost. These counters do not prove DRAM bandwidth saturation,
and whole-JVM PMUs cannot assign every stall to an individual probe instruction.

## The three predeclared candidates

1. **Skip discarded folds: retain for q32, refute for q31.** q31 runs one pass and
   discards nothing. q32 repeats numeric aggregate loads, sum checks and scratch
   updates for six non-owning passes. This is measurable work worth removing even
   though it does not remove a probe. All three composite loops use the same guard.
2. **Bypass worker hashing for almost-unique input: do not land in this change.**
   Near uniqueness is confirmed, but the predeclared shared-compaction/copying
   falsifier is also present: merge acquisition exceeds local acquisition in both
   profiles, before accounting for stripe copying. Removing local acquisition alone
   leaves these larger costs. This is not an experimental proof that every bounded
   radix design would fail. A design that also avoids copying could still win; its
   duplicate-heavy fallback and latency bound need their own implementation and
   measurements. Repeating a smaller-worker-table experiment is not such a design.
3. **Collapse adjacent duplicate runs: refuted.** q31 has zero repeated adjacent
   identities among 13,172,392 qualifying rows. q32 has two per full scan, or 14
   observations over seven passes. Local new groups total 99,997,493 versus
   99,997,497 admitted rows. There is effectively nothing to pre-aggregate locally.

History checked before these measurements: smaller worker tables worsened the
recorded q32/q18/q13 hot total from 12.557 s at 65,536 groups to 14.238–14.453 s at
16,384–4,096 groups (`014c1a400`). More partitions alone and doubling the group
budget also failed earlier campaign measurements (`docs/P2_SEGMENT1_BRIEF.md`).
Those latter two are verified adjacent experiments; the repository does not establish
that they are the other two unnamed failed probe levers in the handoff.

## Work reduction and timing cross-check

Every work count below comes from **temporary** instrumentation: invocation-local
primitive counters, combined only at each morsel's end, counting selected rows, pass
rejections, new local groups, adjacent equal identities within a leaf, and executed
folds. It was removed after the experiment, so these numbers are an archived
measurement and are not reproducible from the shipped source. `experiment.patch` in
the evidence directory preserves it. The change itself ships **no** counter.

| q32 hot work, seven passes | Before | After |
|---|---:|---:|
| Selected rows / acquire calls | 699,982,479 | 699,982,479 |
| Pass rejections | 599,984,982 | 599,984,982 |
| Local new groups | 99,997,493 | 99,997,493 |
| Executed aggregate folds | 699,982,479 | 99,997,497 |
| Shared rehashes | 0 | 0 |

One hot try of q32 skipped **599,984,982 folds**, or 85.7% of them; q31 skipped
none. No extra retained row state, per-row allocation or per-row synchronization is
introduced: execution pays one handle comparison per selected row and nothing else.
Duplicate-heavy and one-pass inputs keep their existing table and spill paths rather
than copying extra stripes.

| q32 measurement, all diagnostic | Baseline wall / CPU | Candidate wall / CPU |
|---|---:|---:|
| CPU profile + PMU, work counters off | 7.284 / 134.8 s | 6.954 / 128.6 s |
| Work counters on, profiling off | 7.614 / 140.3 s | 7.101 / 132.4 s |
| Final implementation, profiling off | — | 6.736 / 124.4 s |

The roughly **5% saving is paired-profile CPU time** (134.8 to 128.6 CPU seconds,
4.6%). The **599,984,982 removed folds are a work count**, not a time saving.
Neither measurement is a scored delta-ln or a leaderboard result.

In the paired profiles, `foldSliced` falls from 11,744 samples (9.6%) to 3,207
(2.7%). This agrees with the work counters. The table probes remain the dominant
remaining cost. Final q32 scan/merge sums are 3,982/2,714 ms; the scan reduction
and nearly unchanged merge cost agree with where the change acts.

q31 hot wall time varied from 4.463 s in the initial profile to 1.447 s in the final
diagnostic while its work counter stayed zero. Later runs spent less time reading
columns. **Do not credit that wall-clock change to this optimization**, or claim
that it proves a general one-pass latency improvement. q31 needs a separate look
at column residency and reads; changing the group hash table is the wrong primary
lever for the observed q31 run.

## Correctness and reproduction

`CompositeDiscardedFoldTest` exercises all three composite loops that fold under a
hash-range pass: the sliced plain and transformed loops and the whole-leaf kernel
(`ProjectionIndexByteScan.conjunctiveAggregateByGroupCompositeFlat`, the arm taken
when the column store neither fits nor windows). Two distinct, excluded groups with
individually valid `Long.MAX_VALUE` sums must not overflow a shared discard
accumulator. The owning pass still rejects a real within-group overflow. Partitioned
and unpartitioned results agree on duplicate multiplicity, absent versus zero keys,
sums, extrema, first-seen ordinals and aux. Key-transform overflow must still decline
before ownership filtering. Disabling any skip guard makes that loop's
discarded-overflow witness fail; restoring the exact tested source returns them all
to green.

Targeted validation: 75 tests passed across `CompositeDiscardedFoldTest`,
`CompositeGroupIdentityCollisionTest`, `GroupStripeSpillTest`,
`GroupHashRangePassTest` and `GroupTopKDifferentialTest`. The last two also compare
serving with the interpreter, including grouped distinct values and ties.
The fresh local 1M database has two segments. `bash seggate1m.sh` reports 34 matches,
9 tie-ambiguous results (0 unverifiable), **0 mismatch, 0 missing, 0 declines**.

The ignored evidence directory is `bundles/sirix-query/build/diagnostics/q31q32/`:
`q{31,32}-route.log`, `q{31,32}-hot.log`, `q{31,32}-hot.collapsed`,
`q{31,32}-hot-perf.csv`, `q{31,32}-counts-baseline.log`, `q32-counts-candidate.log`,
`q{31,32}-candidate-hot.*`, and `q{31,32}-final.log`. The route summaries preserve
every `[proj]` line. `experiment.patch` preserves the removed instrumentation.
`hardware-evidence.md` records event groups, preflight checks and interpretation
criteria established before measuring; `hardware-results.txt` contains derived ratios.

Use the campaign rig environment and shared `leg.lock`; require 26 GiB available
and stop the build daemon before any 100M query. The local launcher uses the prebuilt
runtime classpath and writes logs inside the disposable worktree. The 100M database
and source corpora were only read. No scored 100M suite was run.
