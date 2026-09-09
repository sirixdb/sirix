# Composite probing: fresh landed-head profile

Baseline: `2016aa8d31fc97022c5341e56b0056f73f44958d`, verified as the campaign
head through gh-axi before this study. The landed bounded-pass budget and sparse
SUM ordering fix are preserved. No candidate performance claim is made here.

One full-suite 100M diagnostic JVM completed, in normal q0–q42 order with three
tries, under the rig's exclusive lease and canonical envelope (6/14 GiB heap,
10 GiB arena, 5 GiB eager budget, JVMCI disabled). JFR execution sampling was
requested at 2 ms. The launch gate checked MemAvailable >= 26 GiB. The rig
reported exit 0, no issues, no power-domain changes, and 140.60 seconds of JVM
duration. The 50 W MSR limits and cooldown checks passed. All 43 lossless answer
files were retained as the comparison's baseline reference.

The table combines samples from tries 2 and 3. Wall and phase times are means
of those tries, not the scored hot minimum. CPU percentages are inclusive JFR
execution/native sample fractions, **not wall-time fractions**. Worker and merge
acquisition partition lookup; spill/copy can overlap lookup. Do not add the
columns. Windows use output receipt minus the rounded query duration and are
approximate. `ReadSamples.java`, `analyze-profile.py`, the raw JFR, query
boundaries, and `profile-summary.json` preserve the attribution procedure.

| Query | Diagnostic wall s | Hot passes | Scan ms | Merge ms | Samples | Lookup | Worker acquisition | Merge acquisition | Spill/copy |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| q32 | 3.599 | 2, 2 | 2273.5 | 1300.5 | 10936 | 46.1% | 23.2% | 22.8% | 21.6% |
| q18 | 2.459 | 1, 1 | 1153.5 | 566.0 | 6934 | 38.3% | 20.8% | 17.5% | 14.4% |
| q16 | 1.440 | 1, 1 | 547.0 | 197.5 | 3954 | 31.4% | 21.3% | 10.1% | 10.1% |
| q31 | 1.827 | 1, 1 | 1657.0 | 141.5 | 5488 | 18.4% | 15.1% | 3.4% | 10.3% |

Lookup remains the largest shared composite-table cost. In q32 it exceeds the
pre-budget profile's 43.8%, while spill/copy has risen from 14.1% to 21.6% of
samples. Worker acquisition still represents 20.8–23.2% on q16/q18/q32. Exact
merge acquisition is equally large on q32 and remains outside the prototype's
direct optimization. String value merging accounts for 26.2% on q16 and 12.9%
on q18. Top-K remains small (0.8–2.4%).

**q31 differs:** `ProjectionIndexRowGroupCodec.unpackInto` is its largest leaf
at 27.7%, ahead of lookup. Its diagnostic wall time also exceeds the earlier
paired hot timing; this profile is not a controlled explanation of that change.
Generic decode work is the stronger q31-specific follow-up. Nevertheless,
worker lookup remains material on q31, and the other three retain acquisition
as their leading shared target. The fresh evidence supports measuring bounded
worker probing across the prespecified four, with a limited expectation for
q31 and with extra merge/spill traffic treated as a real countervailing cost.

The prototype's online sample is part of ordinary acquisition, adds no input
pass, and changes only intermediate worker deduplication. Every partial record
merges into an exact table before selection. Distinct sinks remain excluded.
The adaptation removes the prototype's global activation counter and retains
per-table activation assertions in core tests. The landed SUM regression stays
enabled and unchanged.

`planning.json` asks the rig's uncertainty model about a 0.25-ln family scale
using all twelve prior pass-budget pairs. Both the ordinary and conservative
requirements are **10 pairs**. The fixed plan is ten balanced pairs, seed 0,
with all 43 queries and three tries in every leg. The four-query family is the
primary comparison; the whole-suite result is secondary and may be unresolved.
This is a planning target, not a promised effect. There will be no extension
until significance. Any new answer mismatch stops collection before a speed
claim. The task authorization caps all workers and later validators at twelve
pairs total; it does not authorize using the remainder to chase significance.
