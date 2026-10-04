# Historical HOT last-key verification

Status: firstmate accepted the completed pinned-core campaigns on 2026-10-04,
including the explicitly documented parent p99 straddle below.

## Scope and artifacts

This fixes the historical max/span regressions observed after
https://github.com/sirixdb/sirix/pull/1237. The comparison baseline is
`8bcd55a358c797b6506c596b3e0587d97120800e`; the separate pre-change current-main
baseline is `28a95fe8e`. Both baseline core artifacts and the fixed artifact
were frozen before acceptance timing. Driver bytecode, JDK, dependencies and
artifact hashes are retained with the raw evidence. The parent SH1 comparison
includes intervening query changes on current main, so it measures the delivered
artifact and does not isolate every query difference to this HOT fix.

## Diagnosis and change

Before production edits, JFR and work counters compared the parent and current
read paths at revisions 1, 65 and 130, with 32,768 rows and 129 edits. Warm max
and span calls performed no new fragment reconstruction or fragment walk after
the preceding scan. Repeated fragment I/O therefore does not explain these
warm-cell regressions.

The new detached-entry path copied an overflow reference for every present slot.
Inline summary values consequently entered an empty side map and performed two
atomic read-modify-writes each. Revision-65 max/span read 151 entries per call,
150 inline. Native binary search also compared a five-byte suffix through up to
five scoped byte loads per step. It was the largest sampled CPU block; redundant
side-map operations were another measured cost. Skipping the map alone did not
close the focused max gap.

The production lookup and provenance rules are owned by
[Projection read performance](PROJECTION_READ_PERFORMANCE.md#versioned-hot-projection-slot-reads).
No feature switch, alternate historical path or format-compatibility branch was added.

Revision 1 span uses a summary-bound shortcut with two slot reads; revision 65
span consumes 151. Revision 130 has a different balance of lookup costs: the
pre-change warmed profile was slower on the parent despite its cheaper side-map
policy. Original endpoint confidence bounds straddled the threshold, so they
never established absence of overhead at revisions 1 or 130. There is no special
storage threshold at revision 65. The paired measurements below determine
endpoint acceptance; sampled stacks and unpaired mechanism experiments do not.

## Deterministic guard and suites

`HOTHistoricalBlobReadWorkBudgetTest` reconstructs revision 65 first, then 1 and
130, across all four versioning policies. The original implementation fails all
four cases at revision 65: 28 native suffix lanes and one inline side-map probe.
The unchanged guard passes with the fix: last lookup 7 lanes/0 map probes,
first/last probes 15 lanes/0 probes, overflow marker 8 lanes/1 probe. The ceilings
are 8, 16 and 8 lanes respectively. Existing work-budget bounds were not widened.

`HOTLongSuffixSearchTest` covers all eight prefix lengths, padded and exact-tail
segments, explicit probe lengths, absent/insertion positions, mixed stored
lengths and unsigned sign-boundary ordering. The focused entry/history/guard
run passed 69 cases; the final suffix run passed all nine cases.

Full sirix-core: 12,122 cases, 76 skipped, no failures or errors. Full sirix-query:
1,894 cases, 7 skipped, two failures and no errors. The failures are
`GroupTopKDifferentialTest.stringMinAndMaxWithCollationAdversariesAndAllMissingGroup`
and `StringPredicateDifferentialTest.supplementaryCharacterOrderingMatchesTheInterpreter`.
Both reproduce with the frozen pre-change core and are the known Brackit
codepoint-order issue tracked separately by firstmate. Firstmate authorized
proceeding with these two failures; no unrelated query fix was included.
Core/query Java formatting checks and `git diff --check` passed. These suite
counts describe the frozen `28a95fe8e` source, before the separately delivered
codepoint-order repair. Pipeline rebase and validation own checks on newer main;
the earlier query failures are not represented here as a clean full-suite pass.

All builds and benchmark JVMs use the mandated two-slot memory limiter. Builds
and profiles use at most 2 GiB heap; acceptance JVMs retain the archived 6 GiB
heap by explicit firstmate instruction. Maven uses the task-private
`build/history-lastkey/m2` repository. No installs were made into `~/.m2`.

## Measurement protocol

Each comparison uses twelve matched alternating pairs per restore window
(3 and 32), with the unchanged driver and full 130-revision oracle. Each shape
has 2,001 warm samples at revisions 1, 65 and 130, plus 501 cold samples at
revision 130. Ratios use paired bootstrap medians, 5,000 draws, seeds 29032
(read/SH1) and 29033 (writes), nearest-rank 95% intervals. Lower bound >1.05
confirms regression; upper bound <=1.05 meets. No trimming or pooling.

The resource protocol requires 14 GiB available memory stable for 60 seconds
before launch, at least 8 GiB during each operation, and at most 10 GiB own
process-tree RSS. Firstmate authorized a 25 GiB free-disk floor after the
inherited 50 GiB floor interrupted a limiter wait. All eight completed SH1 runs
and two completed read runs were retained; only the interrupted operation was
archived and replayed. Heaps, memory requirements and statistical bounds were
unchanged. Stores live only under the authorized task scratch and are removed
after each run. JIT XML is losslessly compressed and SHA-256 verified only after
the workload JVM exits; no compressor overlaps timing.

## Pinned-core acceptance

The initial unpinned campaigns failed the requested gates (details below).
A follow-up diagnostic identified CPU placement as a plausible confounder on
the hybrid i7-12700H laptop. Firstmate authorized two fresh complete campaigns,
with **every workload JVM in both arms restricted by `taskset -c 0-11`** to the
same performance-core set. Resource observations verify each completed JVM's
allowed CPU list. CPU affinity changes experimental conditions and JVM-detected
CPU availability; these results apply to that declared condition. System power
and energy settings were not changed.

Artifacts, driver, heaps, sample counts, alternating order, memory guards,
byte controls and bootstrap thresholds stayed unchanged. Each comparison has
48 exact read-driver runs over all 130 revisions; together they have 96.
The parent campaign also has eight alternating SH1 runs. A memory-only pause
retained complete runs and archived/replayed the interrupted operation. Old
unpinned samples are retained separately and never pooled with pinned samples.
Both comparisons have zero byte-control mismatches and **no confirmed read or
write percentile regressions**.

Eight of nine target cells meet against the parent. The remaining
`w32/latency/65/max/p99` cell straddles: paired median 0.768, bounds
0.592–1.113. It does **not** meet the original upper-bound criterion.
Firstmate explicitly accepted this as passing: its median is 23% faster,
and it meets the bound against current main. All nine targets meet against
current main. The statistical verdict remains a straddle; acceptance is an
explicit firstmate decision, not a widened bound or a claim of equivalence.

Ratios below are fix/baseline; bounds use unrounded values for verdicts.

| Target | Parent median | Parent 95% bounds | Parent verdict | Current median | Current 95% bounds |
| --- | ---: | --- | --- | ---: | --- |
| w3/latency/65/max/p50 | 0.793 | 0.774–0.803 | meets | 0.750 | 0.735–0.766 |
| w3/latency/65/max/p95 | 0.757 | 0.628–0.918 | meets | 0.819 | 0.556–0.900 |
| w3/latency/65/max/p99 | 0.855 | 0.796–0.967 | meets | 0.883 | 0.822–0.911 |
| w32/latency/65/max/p50 | 0.853 | 0.815–0.880 | meets | 0.759 | 0.613–0.783 |
| w32/latency/65/max/p95 | 0.887 | 0.792–1.048 | meets | 0.653 | 0.473–1.013 |
| w32/latency/65/max/p99 | 0.768 | 0.592–1.113 | unconfirmed straddle | 0.769 | 0.646–0.960 |
| w32/latency/65/span/p50 | 0.882 | 0.809–0.918 | meets | 0.857 | 0.830–0.911 |
| w32/latency/65/span/p95 | 0.806 | 0.717–1.003 | meets | 0.759 | 0.689–0.875 |
| w32/latency/65/span/p99 | 0.849 | 0.695–0.917 | meets | 0.797 | 0.697–0.869 |

## SH1 conclusion

All eight pinned alternating t25k runs match all twelve oracle answers exactly.
Under the campaign rule, regression is confirmed only when the 95% lower bound
exceeds 1.05. The pinned P-core matched pairs show **no confirmed SH1 regression
against either the parent or current main**, so the earlier unpinned lean is
not reproduced under that rule. Cells whose 95% interval straddles 1.05 are
**not claimed equivalent within 5%**, including parent load, q3 and q7 with
slower medians and wide intervals. q2 meets the parent bound. q12's large
artifact improvement includes intervening query changes and is not attributed
to this HOT fix. The separate earlier unpinned eight-run campaign likewise
confirmed no SH1 slowdown. The parent comparison is shown below.

| Cell | Median ratio | 95% bounds | Verdict |
| --- | ---: | --- | --- |
| load | 1.056 | 0.893–1.206 | unconfirmed straddle |
| q1 | 1.034 | 0.924–1.165 | unconfirmed straddle |
| q2 | 1.021 | 0.874–1.047 | meets |
| q3 | 1.149 | 1.022–1.287 | unconfirmed straddle |
| q4 | 1.022 | 0.879–1.167 | unconfirmed straddle |
| q5 | 1.103 | 1.008–1.212 | unconfirmed straddle |
| q6 | 1.051 | 0.894–1.357 | unconfirmed straddle |
| q7 | 1.231 | 0.994–1.368 | unconfirmed straddle |
| q8 | 0.995 | 0.928–1.196 | unconfirmed straddle |
| q9 | 0.945 | 0.909–1.394 | unconfirmed straddle |
| q10 | 1.052 | 0.828–1.360 | unconfirmed straddle |
| q11 | 1.026 | 0.924–1.066 | unconfirmed straddle |
| q12 | 0.014 | 0.012–0.014 | meets |

## Earlier unpinned evidence

The complete initial campaigns also had 96 exact read runs, zero byte-control
mismatches and eight exact SH1 runs. Three parent targets met; six straddled,
despite faster medians. Against the parent, cold revision-130 min p95 regressed
(1.165, bounds 1.073–1.255), as did p99 (1.296, 1.122–1.381). Against current
main, all nine targets met, but warm w32 revision-65 range32 p99 newly regressed
(1.174, 1.086–1.494; baseline/fix median per-run p99 29.08/36.19 us).
These results failed the gate and remain recorded as failed evidence.

A separate 2 GiB diagnostic recorded Java thread CPU IDs before/after 200-call
blocks. Current/fix range32 medians were 20.594/21.072 us on performance cores
and 31.386/32.463 us on efficiency cores: same-class ratios 1.023 and 1.034.
The fix had 455 efficiency-core blocks versus current's 110. Equal endpoint CPU
IDs cannot exclude migration within a block. Original acceptance samples had
no CPU IDs, so they cannot be retroactively filtered or corrected. These
observations supported the fresh placement-controlled experiment; they do not
prove CPU placement exclusively caused the failed tails.

The selected HOT/seek JIT compilations did not occur inside any retained
revision-65 max/span window. Some range32 windows coincided with compilation,
but correlation is not exclusive causal proof. Maxima, GC/JIT correlations and
first-call/phase allocation totals remain descriptive, not extra acceptance
cells or steady-state allocation claims.

## Reproducibility and retained evidence

Raw task-local evidence is under `build/history-lastkey/`:

- `pinned/{parent-vs-fix,current-vs-fix}/final-report.json`, CSV/JSON summaries,
  raw read samples, compressed GC/JIT logs and resource observations;
- comparison `provenance.json`, frozen artifact/driver/JDK/dependency hashes,
  scripts, statistical plans and SH1 exact-answer TSVs;
- unpinned comparison directories, `gate-failure-report.md`, CPU diagnostic
  CSV/summary, pre-edit JFR/work profiles and red/green guard results;
- full-suite XML archives, baseline query-failure reproduction and final
  suffix-boundary/formatting logs.

The reused kit was copied from the read-only earlier worker evidence; that
copy was never modified. Build artifacts are local evidence, not repository
source or portable URLs. This committed report records the verdicts and
limitations; the pinned JSON keeps `targetsAllMeet: false` for the parent.
The benchmark candidate's production-source hashes identify the frozen source
before pipeline lint housekeeping. No production edits were made between the
unpinned failure and pinned campaigns.

## Focused latest-revision span test setup correction

The pipeline's independent 129-slot, window-32 direct-blob test reported a
revision-130 span p99 regression against immediate pre-fix `0700b54f3`: ratio
1.976, bounds 1.192–3.157. An unchanged twelve-pair reproduction straddled
(1.364, 0.688–3.190), with periodic slow samples despite improved medians.
The original failed evidence remains separate; neither result is pooled below.

The test used `-Xms256m -Xmx2g` without heap pre-touch. A Linux measuring-thread
`getrusage` diagnostic associated 102 of 104 span samples exceeding 1.2 us
with minor page faults, roughly one fault per twenty calls. This threshold is
descriptive, not an acceptance bound. The corresponding diagnostic with a
fixed, pre-touched heap recorded zero minor faults in that span. Warm lookup
iterations had warmed the database pages without ensuring resident heap pages.

The focused test now launches both arms through
`bundles/sirix-query/bench/common/hot-history-jvm.sh`, which sets
`-Xms2g -Xmx2g -XX:+AlwaysPreTouch`. Supply the remaining JVM flags, classpath
and driver arguments through the same shared limiter and `taskset -c 0-11`;
omit separate heap flags. This fixes the warm-test setup without HOT production
edits. It applies only to this independent 2-GiB workload; the archived 6-GiB
acceptance campaigns and the accepted SH1 numbers and conclusion are unchanged.

Revalidation used the original `WarmBlobLatencyProbe` bytecode and databases,
250,000 warm iterations per shape, 2,001 samples per shape, twelve alternating
fresh-JVM pairs, and the unchanged 5,000-draw paired bootstrap with seed 29032.
All 24 runs returned exact values. All 27 measured percentile cells met,
including all six revision-65 max/span cells, against the immediate pre-fix baseline.
The revised setup is assessed independently of the earlier samples.

| Window-32 revision-130 span | Pre-fix us | Candidate us | Paired median ratio | 95% bounds | Verdict |
| --- | ---: | ---: | ---: | --- | --- |
| p50 | 0.461 | 0.301 | 0.670 | 0.611–0.680 | meets |
| p95 | 0.827 | 0.332 | 0.682 | 0.506–0.719 | meets |
| p99 | 0.887 | 0.444 | 0.685 | 0.512–0.726 | meets |

These are median per-run percentiles and paired ratios, not a ratio of the two
displayed medians. Fault instrumentation was used only for diagnosis; the
paired verdict uses the original uninstrumented driver. This Test phase ran
the focused workload, not full suites or new SH1 runs. Its generated databases,
classes, build outputs and scratch samples were removed after recording results.
