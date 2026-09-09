# Composite worker probing: resolved family benefit, suite UNRESOLVED

The prespecified q16/q18/q31/q32 family improves by **+0.181907 ln**, nominal
95% paired t interval **[+0.097559, +0.266254]**, over **10 fixed balanced pairs**.
Mean hot time across these four falls from **8.3982 to 7.9266 seconds**, saving
**0.4716 seconds**. All 43 lossless answer files are byte-identical to the freshly
profiled landed baseline in every one of the twenty legs. The mechanism is
retained under the brief's resolved-positive-family landing rule.

Firstmate authorized landing this result on 2026-09-09. **The remaining 100M
allowance is now zero for every pipeline agent, including fixers and verifiers.**
Use local fixtures and offline artifact replay for delivery validation. Do not
launch another profile, correctness dump, or benchmark against the 100M database.

**Sign:** benefit = baseline minus candidate, so positive improves. Every delta
below is candidate minus baseline, so **negative improves**. Ln deltas are means
of per-leg log ratios with the rig's 0.01-second floor, not log ratios of the
averaged seconds. Rows are sorted by delta ln, largest regression first.

| Query | Baseline hot s | Candidate hot s | Delta s | Delta ln | Individual result |
|---|---:|---:|---:|---:|---|
| q16 | 1.451900 | 1.423500 | -0.028400 | -0.019622 | UNRESOLVED |
| q31 | 0.934700 | 0.908100 | -0.026600 | -0.028255 | UNRESOLVED |
| q18 | 2.420500 | 2.320200 | -0.100300 | -0.042122 | nominal improvement |
| q32 | 3.591100 | 3.274800 | -0.316300 | -0.091908 | nominal improvement |

The family is the prespecified primary contrast. Individual-query intervals are
exploratory, nominal, and unadjusted for multiple comparisons. A family benefit
does not establish that every member improved. The q18 and q32 benefit intervals
are [+0.024822, +0.059422] and [+0.079918, +0.103897] ln; q16 and q31 span zero.

## Resolution and the fixed stopping rule

Before collection, `planning.json` asked the rig about a **0.25-ln family scale**
using the prior twelve pass-budget pairs. Both ordinary and conservative
requirements were ten pairs. Ten pairs, seed 0, were fixed before collection;
every leg ran all 43 queries and three tries in the standard order. Scored hot
time is min(try 2, try 3). No observation was replaced or discarded.

The new family paired SD is 0.117910 ln and the estimated 80%-power detectable
effect is **0.117438 ln**. At the observed **0.181907-ln** effect, the rig's
ordinary requirement is **10 pairs**, which were completed; its resolution label
is `TARGET RESOLVED UNDER THE NOISE MODEL`, and the benefit interval excludes zero.
**The conservative requirement using the upper 95% noise bound is 12 pairs and
was not met.** This result is resolved under the ordinary paired model, not a
claim of the conservative twelve-pair precision. The fixed ten-pair plan was
not extended after seeing the result. All estimates remain conditional on the
rig's independence, approximate normality, and stability assumptions.

The **whole-suite score is UNRESOLVED**, with a negative benefit point estimate:
**-0.245162 ln**, interval **[-1.080341, +0.590016]**. Whole-suite mean sum-ln is
57.279146 baseline and 57.524308 candidate. Suite hot seconds fall from 26.5807
to 26.0286, a descriptive 0.5521-second saving; means of seconds and log scores
weight queries differently. The suite detection floor is 1.162831 ln. At its
observed magnitude the rig projects 180 pairs, or 484 conservatively, neither
authorized nor run. The largest per-query score regression is q25 (+0.238674 ln,
+0.0295 seconds); it is retained below. No suite speedup, publication rank,
DuckDB parity, or top-ten result is claimed.

## What the fresh profile and measurement establish

The baseline is `2016aa8d31fc97022c5341e56b0056f73f44958d`, verified as the campaign
head before work. Candidate engine `e8633fe92` adapts parked prototype `1703ebe2d`
to that head. The landed pass-count budget and SUM ordering fix remain intact.
The only later engine edits are the repository formatter's whitespace changes.

The [fresh profile](PROFILE.md), collected before candidate timing, confirms hot
pass counts q16/q18/q31/q32 = 1/1/1/2. Lookup shares are respectively
31.4/38.3/18.4/46.1%; worker acquisition is 21.3/20.8/15.1/23.2%; exact merge
acquisition is 10.1/17.5/3.4/22.8%; spill/copy is 10.1/14.4/10.3/21.6%.
These are inclusive CPU-sample shares, not wall shares, and can overlap.
q31 instead has column unpacking as its largest leaf (27.7%).

The candidate samples 8,192 ordinary worker acquisitions online. Low-cardinality
workers keep exact probing; intermediate cardinality uses a bounded direct
cache; nearly distinct samples append partial records without probing. Every
record still reaches an exact partition table before selection. Distinct sinks
are excluded. There is no input prepass, query identifier or literal in the
engine mechanism, global data structure, or persistent-format change. The
prototype's global diagnostic activation counter was removed; core tests observe
the state of their own tables.

The original 0.721-ln working payoff was a hypothesis, not a result. The observed
0.181907-ln point estimate is about one quarter of it. The fresh worker-only
half-removable hypothesis is 0.424604 ln, with a 0.901276-ln all-worker-acquisition
ceiling; the point estimate is also below that working hypothesis. Acquisition
samples include record initialization and payload work that append mode still
pays, while exact merge and stripe copying remain. The result does not identify
the precise share removed: there is no candidate CPU profile in this study.

The practical calibration is that lookup occupied 46.1% of q32's baseline CPU
samples, yet the mechanism saved 0.3163 seconds of 3.5911 seconds (8.8%). Much of
the table-acquisition work remains, including hashing, record writes, and exact
merge. This supports looking beyond probing; it does not partition the remaining
cost without a candidate profile. Together with the earlier 0.406805-ln
pass-budget result, the two experiments bought about 0.59 ln against the brief's
roughly 2.25-ln parity opportunity. Those are separate-study estimates, not one
joint paired comparison. Returns from composite-key probing are thinning: the
next profile should examine key width, hashing, and merge before another probing
experiment.

For a next generic table experiment, investigate eliminating intermediate
partition indexing and stripe copying together with worker probes, while keeping
the duplicate-heavy fallback, bounded residency, and exact final merge. Merely
removing more worker probes leaves the 22.8% exact-merge and 21.6% spill/copy
baseline shares on q32. Re-profile the resulting candidate before assigning it
new percentages. For q31, the fresh unpacking hotspot is a stronger target than
assuming the family gain transfers to it. No follow-up mechanism or run is
included here.

## Correctness, conditions, and allowance

- **127 local tests pass, zero failures/errors/skips**, covering dense tables,
  displaced partial records, exact identity collisions, SUM overflow, spill and
  pass ownership, budget planning, top-K, and differential execution.
  `SparseSumOrderingOriginTest` remains enabled and passing, unchanged from the
  landed fix. Its sparse SUM cases are essential because the 43 benchmark queries
  do not expose that prior defect.
- **Twenty of twenty legs pass all 43 answer comparisons.** Retention independently
  re-read every answer and checked its SHA-256 against the per-leg proof and
  baseline reference. It verified every one of 1,120 archive members.
- Every 100M launch held the exclusive rig lease, ran one benchmark JVM, and used
  the unchanged 6/14 GiB heap, 10 GiB arena, 5 GiB eager budget, and JVMCI policy.
  Minimum pre-launch MemAvailable in the comparison was **27.4895 GiB**, above
  the required 26 GiB. No benchmark reload, corpus mutation, or external workload
  termination occurred. An idle leftover Gradle daemon was stopped through
  Gradle's own stop command before preparation.
- Both pinned MSR limits stayed at 50 W and launch cooldown gates passed. The
  platform-managed limits had ten distinct starting readings and moved in
  eighteen of twenty legs, 57 changes total. These retained physical conditions
  limit transfer to another power regime; balanced pairing is not proof of no
  bias from drift correlated with the arms.
- **Allowance used: one profiling JVM plus ten paired comparisons.** The fixed
  study is complete. Two pairs of the twelve-pair authorization cap were not
  spent. Firstmate subsequently set the remaining allowance to **zero for every
  pipeline agent, including fixers and verifiers**. This study ran one fresh
  profile and ten pairs; profiles and pairs are recorded separately. **Do not
  launch any additional 100M collection.** Validation must use fixtures and
  artifact replay.
- Campaign-branch CI runs no checks. Delivery must be described as pushed and
  locally validated when the pipeline ships it, never as CI green.

## Retained evidence and replay

`profile.tar.gz` and its SHA-256 manifest retain the baseline JFR, query windows,
commands, telemetry, answer files, and runtime provenance. `ReadSamples.java`
regenerates the deliberately unarchived `samples.tsv` from that JFR;
`analyze-profile.py` regenerates its summary. `validation.tar.gz` retains local
test logs and available XML, with counts in `validation.json`.

`plan.json`, `pairs.json`, `report.json`, `report.md`, and `queries.csv` are the
original rig outputs. `analyze.py` adds the family contrast, observed-effect pair
requirements, and sorted `query-deltas.csv`. Its historical replay reproduced the
previous study's +0.406804929564694-ln family result and unresolved suite result.
`comparison.tar.gz` and `comparison-manifest.json` retain each raw leg's commands,
logs, cooling, telemetry, answer proof and answer bytes. Runtime manifests are
also retained separately; frozen binaries remain under the worktree's build
directory. Only the rig's candidate scratch checkout was automatically removed.

All twenty original leg documents are committed as `rig/legs/query-SEGCP-P*.json`.
They carry `steering` provenance and cannot publish a rank.

Replay without a JVM, database access, or a new benchmark:

```sh
build/rig-python/bin/python3 bundles/sirix-query/bench/clickbench/rig/measure.py analyze \
  bundles/sirix-query/bench/clickbench/rig/evidence/composite-probe-20260909
build/rig-python/bin/python3 \
  bundles/sirix-query/bench/clickbench/rig/evidence/composite-probe-20260909/analyze.py \
  bundles/sirix-query/bench/clickbench/rig/evidence/composite-probe-20260909
```

The first command exits 3 because the whole-suite target is unresolved; that is
an analysis outcome, not an incomplete collection. The family result is separate.

## All-query deltas

Delta = candidate minus baseline; **negative improves**, largest regression
first. All intervals below are nominal 95% intervals for **benefit**, the opposite
sign. Per-query verdicts additionally require the observed magnitude to meet the
rig's estimated 80%-power resolution and are exploratory, without multiplicity
adjustment.

| Query | Baseline s | Candidate s | Delta s | Delta ln | 95% benefit interval | Individual result |
|---|---:|---:|---:|---:|---|---|
| q25 | 0.088000 | 0.117500 | +0.029500 | +0.238674 | [-0.417819, -0.059529] | UNRESOLVED |
| q07 | 0.019800 | 0.022800 | +0.003000 | +0.100474 | [-0.249620, +0.048672] | UNRESOLVED |
| q06 | 0.017200 | 0.019800 | +0.002600 | +0.091467 | [-0.229214, +0.046280] | UNRESOLVED |
| q17 | 0.038400 | 0.042400 | +0.004000 | +0.081129 | [-0.159872, -0.002387] | UNRESOLVED |
| q10 | 0.198300 | 0.209300 | +0.011000 | +0.056636 | [-0.156013, +0.042741] | UNRESOLVED |
| q11 | 0.164900 | 0.175100 | +0.010200 | +0.050468 | [-0.148256, +0.047320] | UNRESOLVED |
| q38 | 0.038200 | 0.040700 | +0.002500 | +0.049452 | [-0.093652, -0.005252] | UNRESOLVED |
| q37 | 0.039500 | 0.041300 | +0.001800 | +0.036318 | [-0.085796, +0.013161] | UNRESOLVED |
| q42 | 0.035400 | 0.037000 | +0.001600 | +0.034862 | [-0.139446, +0.069722] | UNRESOLVED |
| q19 | 0.010500 | 0.011000 | +0.000500 | +0.023551 | [-0.078302, +0.031200] | UNRESOLVED |
| q26 | 0.018800 | 0.019400 | +0.000600 | +0.020579 | [-0.056084, +0.014926] | UNRESOLVED |
| q36 | 0.062300 | 0.063900 | +0.001600 | +0.020228 | [-0.145101, +0.104645] | UNRESOLVED |
| q15 | 0.435300 | 0.443800 | +0.008500 | +0.017001 | [-0.064655, +0.030653] | UNRESOLVED |
| q13 | 1.175100 | 1.194000 | +0.018900 | +0.015699 | [-0.066135, +0.034737] | UNRESOLVED |
| q29 | 0.030800 | 0.031200 | +0.000400 | +0.011581 | [-0.082864, +0.059701] | UNRESOLVED |
| q21 | 0.101600 | 0.102800 | +0.001200 | +0.010937 | [-0.135882, +0.114009] | UNRESOLVED |
| q35 | 0.407500 | 0.410900 | +0.003400 | +0.007888 | [-0.029641, +0.013865] | UNRESOLVED |
| q40 | 0.023600 | 0.023900 | +0.000300 | +0.005900 | [-0.082589, +0.070788] | UNRESOLVED |
| q27 | 0.225900 | 0.227100 | +0.001200 | +0.004942 | [-0.026894, +0.017010] | UNRESOLVED |
| q14 | 0.729900 | 0.733100 | +0.003200 | +0.004264 | [-0.040538, +0.032010] | UNRESOLVED |
| q34 | 2.001900 | 2.009000 | +0.007100 | +0.003466 | [-0.028937, +0.022006] | UNRESOLVED |
| q24 | 0.017000 | 0.017100 | +0.000100 | +0.003087 | [-0.037044, +0.030871] | UNRESOLVED |
| q33 | 2.054600 | 2.057700 | +0.003100 | +0.001421 | [-0.014762, +0.011921] | UNRESOLVED |
| q08 | 0.643300 | 0.643700 | +0.000400 | +0.000656 | [-0.022485, +0.021173] | UNRESOLVED |
| q00 | 0.001000 | 0.001000 | -0.000000 | -0.000000 | unavailable | UNRESOLVED |
| q04 | 0.311600 | 0.310800 | -0.000800 | -0.002535 | [-0.028481, +0.033551] | UNRESOLVED |
| q09 | 0.729200 | 0.726700 | -0.002500 | -0.003424 | [-0.019281, +0.026130] | UNRESOLVED |
| q12 | 0.701300 | 0.693500 | -0.007800 | -0.010753 | [-0.018073, +0.039578] | UNRESOLVED |
| q28 | 6.086900 | 5.982800 | -0.104100 | -0.017281 | [-0.000899, +0.035461] | UNRESOLVED |
| q23 | 0.121800 | 0.119300 | -0.002500 | -0.017724 | [-0.061131, +0.096578] | UNRESOLVED |
| q01 | 0.032800 | 0.032000 | -0.000800 | -0.017907 | [-0.021228, +0.057042] | UNRESOLVED |
| q20 | 0.028200 | 0.027600 | -0.000600 | -0.018143 | [-0.158156, +0.194442] | UNRESOLVED |
| q16 | 1.451900 | 1.423500 | -0.028400 | -0.019622 | [-0.002642, +0.041886] | UNRESOLVED |
| q31 | 0.934700 | 0.908100 | -0.026600 | -0.028255 | [-0.037193, +0.093703] | UNRESOLVED |
| q05 | 0.671000 | 0.651100 | -0.019900 | -0.028602 | [-0.016333, +0.073537] | UNRESOLVED |
| q22 | 0.312500 | 0.301600 | -0.010900 | -0.034019 | [-0.044070, +0.112109] | UNRESOLVED |
| q41 | 0.036800 | 0.035100 | -0.001700 | -0.036500 | [-0.018847, +0.091848] | UNRESOLVED |
| q18 | 2.420500 | 2.320200 | -0.100300 | -0.042122 | [+0.024822, +0.059422] | nominal improvement |
| q03 | 0.034500 | 0.032000 | -0.002500 | -0.048169 | [-0.050983, +0.147320] | UNRESOLVED |
| q02 | 0.047700 | 0.044400 | -0.003300 | -0.063375 | [-0.055744, +0.182494] | UNRESOLVED |
| q30 | 0.328700 | 0.307000 | -0.021700 | -0.066298 | [+0.054597, +0.077998] | nominal improvement |
| q32 | 3.591100 | 3.274800 | -0.316300 | -0.091908 | [+0.079918, +0.103897] | nominal improvement |
| q39 | 0.160700 | 0.142600 | -0.018100 | -0.098881 | [-0.182630, +0.380392] | UNRESOLVED |
