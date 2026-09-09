# ClickBench C6A score 4 — aggregate-fix leg

## Headline

This is one scored 100M leg from source commit `465f208946549e8a264598e986585942e1c8868c`, using
the repaired rig, three tries per query, and `hot = min(try 2, try 3)`.

| metric | result |
|---|---:|
| C6A hot geomean | **3.939639** |
| C6A rank | **15 / 140** |
| C6A sum-ln | **58.956827** |
| remaining to the requested rank-10 target (51.99) | **6.966827 ln** |
| total hot suite time | **27.890 s** |

The leg improves the standing SEG6T sum-ln (63.442089) by 4.485263 ln and moves the displayed rank
from 16 to 15. This is a single-leg change, not repeated-run statistical evidence.

## Uncertainty and per-query result

The repaired rig's single-leg `run` mode reports no paired uncertainty or per-query detection floor;
its own documented uncertainty output is produced by `measure.py compare`, which requires paired
baseline/candidate runs. Consequently every delta below is **UNRESOLVED**: no delta clears the rig's
own detection floor from this one leg. The numbers are score arithmetic and directional observations,
not claims of resolved improvements.

The table is sorted by delta, worst first. `delta` is candidate contribution minus SEG6T contribution;
negative is better.

| query | candidate ln contribution | delta vs SEG6T | resolvability |
|---:|---:|---:|---|
| q31 | 1.887723 | -1.191808 | UNRESOLVED |
| q13 | 1.317357 | -0.853397 | UNRESOLVED |
| q21 | 0.987947 | -0.719395 | UNRESOLVED |
| q32 | 2.501530 | -0.464009 | UNRESOLVED |
| q30 | 1.317491 | -0.396881 | UNRESOLVED |
| q28 | 1.532159 | -0.396442 | UNRESOLVED |
| q11 | 1.532898 | -0.378592 | UNRESOLVED |
| q16 | 2.006659 | -0.299368 | UNRESOLVED |
| q07 | 0.619039 | -0.297252 | UNRESOLVED |
| q18 | 1.248079 | -0.274517 | UNRESOLVED |
| q14 | 1.317949 | -0.222866 | UNRESOLVED |
| q20 | -0.623189 | -0.217723 | UNRESOLVED |
| q17 | 1.547563 | -0.192904 | UNRESOLVED |
| q22 | 2.299354 | -0.177455 | UNRESOLVED |
| q41 | 1.196251 | -0.150823 | UNRESOLVED |
| q12 | 1.403686 | -0.098440 | UNRESOLVED |
| q34 | 2.031985 | -0.089626 | UNRESOLVED |
| q29 | 1.053150 | -0.067441 | UNRESOLVED |
| q33 | 2.091540 | -0.054019 | UNRESOLVED |
| q19 | 0.741937 | -0.046520 | UNRESOLVED |
| q38 | 1.326871 | -0.040005 | UNRESOLVED |
| q05 | 2.046942 | -0.028129 | UNRESOLVED |
| q10 | 1.895690 | -0.025425 | UNRESOLVED |
| q36 | 1.308333 | -0.013423 | UNRESOLVED |
| q35 | 1.113538 | -0.012270 | UNRESOLVED |
| q09 | 1.105620 | -0.008345 | UNRESOLVED |
| q00 | 0.095310 | 0.000000 | UNRESOLVED |
| q40 | 1.203973 | 0.000000 | UNRESOLVED |
| q15 | 1.031415 | +0.022677 | UNRESOLVED |
| q01 | 1.386294 | +0.022990 | UNRESOLVED |
| q04 | 1.298225 | +0.028988 | UNRESOLVED |
| q08 | 1.356441 | +0.037041 | UNRESOLVED |
| q24 | 1.098612 | +0.068993 | UNRESOLVED |
| q03 | 1.098612 | +0.093090 | UNRESOLVED |
| q37 | 1.299283 | +0.095310 | UNRESOLVED |
| q42 | 1.331235 | +0.186102 | UNRESOLVED |
| q27 | 1.021226 | +0.189242 | UNRESOLVED |
| q25 | 2.322388 | +0.194156 | UNRESOLVED |
| q26 | 1.223775 | +0.194156 | UNRESOLVED |
| q39 | 1.758970 | +0.223144 | UNRESOLVED |
| q02 | 1.887070 | +0.238411 | UNRESOLVED |
| q06 | 1.335001 | +0.305382 | UNRESOLVED |
| q23 | 1.400893 | +0.332134 | UNRESOLVED |

The table is numerically ordered by delta; all values come from the rig result and SEG6T JSON. The
aggregate score itself is computed against the unchanged C6A board bests, not against averaged query
times.

## Aggregation verdict requested by the captain

The six PR-1201 target queries all moved in the favorable direction in this leg, but none is
measurably resolved by the rig because this was only one leg:

| query | hot seconds, SEG6T → this leg | delta ln | verdict |
|---:|---:|---:|---|
| q13 | 2.786 → 1.181 | -0.853397 | favorable observation; **UNRESOLVED** |
| q16 | 2.027 → 1.500 | -0.299368 | favorable observation; **UNRESOLVED** |
| q18 | 3.914 → 2.972 | -0.274517 | favorable observation; **UNRESOLVED** |
| q28 | 8.982 → 6.039 | -0.396442 | favorable observation; **UNRESOLVED** |
| q31 | 3.013 → 0.908 | -1.191808 | favorable observation; **UNRESOLVED** |
| q32 | 6.743 → 4.236 | -0.464009 | favorable observation; **UNRESOLVED** |

Therefore the honest aggregation verdict is: **the leg is directionally consistent with PR-1201
helping all six queries, but it does not establish a measurable effect.** Resolving that question
would require another 100M run, which this task explicitly forbids self-authorizing.

## Sirix versus DuckDB

The requested `data/sirix-cb-duckdb-ref-1/report.md` and its 20-leg median table are not present in
this checkout. DuckDB was not re-run. The supplied campaign brief provides these existing reference
ratios (Sirix/DuckDB; >1 means DuckDB faster), which are retained here without pretending that a
missing median table was reconstructed:

| query | prior 20-leg Sirix/DuckDB ratio | this-leg Sirix hot (s) | DuckDB median |
|---:|---:|---:|---:|
| q13 | 3.686x | 1.181 | not available in checkout |
| q16 | 2.451x | 1.500 | not available in checkout |
| q18 | 2.587x | 2.972 | not available in checkout |
| q28 | 1.611x | 6.039 | not available in checkout |
| q31 | 2.952x | 0.908 | not available in checkout |
| q32 | 3.583x | 4.236 | not available in checkout |
| q22 | 0.388x | 0.299 | not available in checkout |

The remaining queries have the same missing reference-table limitation. This is a report-input
availability issue, not permission to run DuckDB again.

## Lever queue

This queue is ordered by plausible achievable speedup factor multiplied by the number of scored
queries touched, not by current ln contribution:

1. **Generic dense group-index plus count/sum aggregation:** target q13, q16, q18, q28, q31, q32;
   observed one-leg envelope is roughly 1.35–3.32x, six scored queries, but unresolved.
2. **Generic group-aggregate/numeric-group-by fast path:** target the broader group-aggregate family
   (at least q7–q18 and q28–q42 routes); potential suite-wide leverage is higher than any one query,
   but no speedup claim is resolved by this leg.
3. **Projection aggregate/count-distinct shared execution:** touches the projection-heavy query
   family (including q2, q4, q5, q8–q16); prioritize only after paired evidence separates it from
   cache/JIT variance.
4. **Query-specific tail work:** q21/q22/q30/q33/q34 and other isolated tails; fewer scored queries
   touched, so lower campaign leverage than generic mechanisms.

## Evidence

The durable leg JSON is `bundles/sirix-query/bench/clickbench/rig/legs/query-SEG7T.json`. The full
retained rig evidence is under
`bundles/sirix-query/build/diagnostics/score4-leg-20260909T105540/leg`; it records the exclusive
lease, pinned envelope, 43×3 timings, and the one MMIO limit change (76 W → 45 W) that the rig
accepted without invalidating the leg.
