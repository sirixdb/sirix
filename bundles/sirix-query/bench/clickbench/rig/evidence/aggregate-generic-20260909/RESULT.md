# Generic aggregation paired result: UNRESOLVED

All 12 prespecified pairs completed. Every one of the 43 answer files is byte-identical to the first current-baseline leg in all 24 legs. The candidate does **not establish a suite improvement or a benefit across the requested aggregation family**. No top-10 result is claimed.

| Metric, mean per leg | Baseline | Candidate |
|---|---:|---:|
| Sum-ln | 58.166314 | 57.783069 |
| Total hot suite seconds | 27.800750 | 27.574333 |

Observed suite benefit (baseline minus candidate): **+0.383245 ln**, nominal 95% paired t interval **[-0.237177, +1.003666]**. The point estimate saves 0.226417 suite seconds; that is descriptive, not an established speedup.

The estimated 80%-power detection floor is **0.867747 ln**. The rig's `TARGET RESOLVED` label refers to the prespecified **2.0-ln** planning target. The actual observed benefit is smaller than the floor and its interval includes zero: **UNRESOLVED**. Applying the same noise estimate to the observed magnitude projects 54 pairs, or 126 under the upper noise bound. These are post hoc planning figures, not an approved plan. No additional runs were made.

For the requested q7–q18 and q28–q42 family together, preserving covariance within each pair, the observed benefit is **+0.004791 ln**, nominal 95% interval **[-0.512162, +0.521744]**: **UNRESOLVED**. The experiment does not demonstrate that this shared path improves that family.

## Per-query contributions and benefits

Benefit is baseline minus candidate, the sign convention the rig's own `queries.csv` publishes and the only one used in this directory: **positive means the candidate is faster, negative means it is slower**. Rows are in `queries.csv` order, benefit ascending, so the largest regression is first. Contributions are means of per-leg ln contributions, not scores of averaged times. Per-query intervals and detection floors are exploratory, nominal and unadjusted for 43 comparisons. A nominal interval excluding zero alone is not labeled resolved when the observed magnitude is below its 80%-power floor.

| Query | Baseline ln | Candidate ln | Benefit ln | 95% benefit interval | Baseline s | Candidate s | Verdict |
|---|---:|---:|---:|---|---:|---:|---|
| q07 | 0.889711 | 1.043255 | -0.153544 | [-0.354493, +0.047405] | 0.024417 | 0.032000 | UNRESOLVED |
| q40 | 0.938137 | 1.024972 | -0.086836 | [-0.180261, +0.006589] | 0.020750 | 0.023833 | UNRESOLVED |
| q10 | 1.765794 | 1.843015 | -0.077221 | [-0.201151, +0.046710] | 0.196583 | 0.212250 | UNRESOLVED |
| q11 | 1.544336 | 1.608733 | -0.064397 | [-0.152067, +0.023274] | 0.168167 | 0.181667 | UNRESOLVED |
| q01 | 1.324920 | 1.364686 | -0.039766 | [-0.111973, +0.032441] | 0.031417 | 0.033250 | UNRESOLVED |
| q21 | 1.137914 | 1.173144 | -0.035229 | [-0.174671, +0.104212] | 0.099833 | 0.105167 | UNRESOLVED |
| q17 | 1.612832 | 1.647527 | -0.034695 | [-0.141337, +0.071947] | 0.040417 | 0.042167 | UNRESOLVED |
| q41 | 1.173725 | 1.204996 | -0.031272 | [-0.091036, +0.028493] | 0.032167 | 0.033500 | UNRESOLVED |
| q42 | 1.088326 | 1.111399 | -0.023072 | [-0.090852, +0.044707] | 0.031667 | 0.032833 | UNRESOLVED |
| q25 | 2.358380 | 2.372556 | -0.014176 | [-0.285730, +0.257379] | 0.101667 | 0.100167 | UNRESOLVED |
| q09 | 1.140743 | 1.151374 | -0.010631 | [-0.036547, +0.015285] | 0.731833 | 0.739833 | UNRESOLVED |
| q28 | 1.525062 | 1.534360 | -0.009298 | [-0.032306, +0.013710] | 5.997333 | 6.054000 | UNRESOLVED |
| q24 | 0.979623 | 0.986069 | -0.006446 | [-0.051063, +0.038171] | 0.016667 | 0.016833 | UNRESOLVED |
| q16 | 2.017762 | 2.024056 | -0.006294 | [-0.016629, +0.004041] | 1.517083 | 1.526833 | UNRESOLVED |
| q32 | 2.460364 | 2.464352 | -0.003988 | [-0.008720, +0.000745] | 4.064833 | 4.081083 | UNRESOLVED |
| q08 | 1.347016 | 1.350048 | -0.003032 | [-0.013523, +0.007459] | 0.643833 | 0.645833 | UNRESOLVED |
| q05 | 2.034660 | 2.036459 | -0.001799 | [-0.019646, +0.016049] | 0.648000 | 0.649250 | UNRESOLVED |
| q30 | 1.289252 | 1.290462 | -0.001210 | [-0.008778, +0.006358] | 0.331250 | 0.331667 | UNRESOLVED |
| q02 | 1.696759 | 1.697788 | -0.001029 | [-0.097716, +0.095658] | 0.044833 | 0.044917 | UNRESOLVED |
| q00 | 0.095310 | 0.095310 | +0.000000 | unavailable | 0.001000 | 0.001000 | UNRESOLVED |
| q04 | 1.294793 | 1.294289 | +0.000504 | [-0.031551, +0.032560] | 0.304000 | 0.303917 | UNRESOLVED |
| q18 | 1.232593 | 1.232053 | +0.000540 | [-0.007726, +0.008807] | 2.926417 | 2.924667 | UNRESOLVED |
| q33 | 2.053912 | 2.051354 | +0.002559 | [-0.005147, +0.010264] | 2.072250 | 2.066917 | UNRESOLVED |
| q03 | 1.125253 | 1.122230 | +0.003023 | [-0.126709, +0.132755] | 0.036667 | 0.036500 | UNRESOLVED |
| q26 | 1.081099 | 1.078056 | +0.003043 | [-0.036331, +0.042417] | 0.019500 | 0.019417 | UNRESOLVED |
| q12 | 1.413898 | 1.408321 | +0.005577 | [-0.013945, +0.025100] | 0.693250 | 0.689417 | UNRESOLVED |
| q15 | 1.028036 | 1.020284 | +0.007752 | [+0.001854, +0.013650] | 0.434500 | 0.431083 | UNRESOLVED |
| q36 | 1.302485 | 1.293935 | +0.008550 | [-0.090356, +0.107456] | 0.064000 | 0.063417 | UNRESOLVED |
| q34 | 2.019207 | 2.009849 | +0.009358 | [-0.007690, +0.026406] | 2.001583 | 1.982667 | UNRESOLVED |
| q14 | 1.363486 | 1.349900 | +0.013586 | [-0.021505, +0.048677] | 0.745167 | 0.734750 | UNRESOLVED |
| q35 | 1.139297 | 1.125578 | +0.013719 | [+0.007131, +0.020308] | 0.405583 | 0.399917 | nominal improvement |
| q29 | 0.991511 | 0.976905 | +0.014606 | [-0.034544, +0.063757] | 0.030500 | 0.029917 | UNRESOLVED |
| q13 | 1.331147 | 1.309109 | +0.022038 | [-0.001204, +0.045281] | 1.198083 | 1.171667 | UNRESOLVED |
| q23 | 1.315179 | 1.289616 | +0.025563 | [-0.127440, +0.178567] | 0.118417 | 0.115250 | UNRESOLVED |
| q37 | 1.268639 | 1.242182 | +0.026456 | [-0.054273, +0.107185] | 0.043500 | 0.042167 | UNRESOLVED |
| q19 | 0.758408 | 0.727513 | +0.030896 | [-0.008487, +0.070278] | 0.011417 | 0.010750 | UNRESOLVED |
| q20 | -0.588291 | -0.630667 | +0.042376 | [-0.140009, +0.224761] | 0.029000 | 0.027000 | UNRESOLVED |
| q38 | 1.371916 | 1.327790 | +0.044126 | [-0.017237, +0.105489] | 0.041333 | 0.039167 | UNRESOLVED |
| q22 | 2.333168 | 2.284416 | +0.048752 | [-0.048430, +0.145934] | 0.310750 | 0.295333 | UNRESOLVED |
| q06 | 1.049024 | 0.997578 | +0.051445 | [-0.100220, +0.203111] | 0.019000 | 0.017667 | UNRESOLVED |
| q31 | 2.009872 | 1.929187 | +0.080685 | [-0.141024, +0.302393] | 1.105667 | 0.949000 | UNRESOLVED |
| q39 | 1.897433 | 1.636706 | +0.260726 | [-0.055120, +0.576572] | 0.212333 | 0.159167 | UNRESOLVED |
| q27 | 0.953622 | 0.682325 | +0.271296 | [+0.219158, +0.323434] | 0.234083 | 0.176500 | nominal improvement |

## Provenance and validation

- Baseline: `b815d459d`, the campaign branch head; candidate: `a483a0df0`. Engine mechanisms are generic; query identifiers appear only in benchmark evidence.
- Exactly 12 balanced pairs, seed 0, fixed before collection; three tries per query, hot = min(try 2, try 3), pinned C6A envelope, unchanged board arithmetic.
- All 24 JVM launches passed the exclusive rig lease and pre-launch memory check. Minimum MemAvailable was 27.444145 GiB. No load, corpus change, index rebuild, or external process termination was performed.
- Both MSR limits remained at the pinned 50 W. Platform-managed limits moved in all 24 legs, 90 changes total, with 10 distinct starting readings. The full telemetry is retained; the result remains conditional on these observed regimes and the paired model assumptions.
- Both arms used the same untimed lossless answer dumps. `compare.py` records that addition in the protocol and every command, compares all 43 files after each leg, and aborts on any mismatch.
- Focused validation passed 34 core tests and 204 query tests. The full 100M comparison then passed byte equality in every leg.
- **100M allowance exhausted: 24 of 24 authorized benchmark JVMs used. Validation/fix agents have zero remaining benchmark runs and need Firstmate authorization for any additional run.**
- These are steering legs; no publication provenance or new board rank is assigned.

## Retained artifacts

- `planning.json`: the prespecified 2.0-ln target and rig pair-count estimates from the historical calibration.
- `plan.json`, `pairs.json`, `report.json`, `report.md`, `queries.csv`: unmodified rig plan, paired data, and analysis outputs. `pairs.json` holds all 24 unmodified steering legs, each as the `baseline` and `candidate` arm of its pair together with that pair's execution `order`; it is the single retained copy of the legs, and `raw-evidence.tar.gz` carries their raw logs. `query-deltas.csv` adds only what `queries.csv` does not publish: the per-query 95% benefit interval, the 80%-power detection floor and the verdict, in `queries.csv` row order and under its `benefit_ln` sign convention.
- `summary.json`: correctness hashes, launch allowance, suite and requested-family uncertainty.
- `raw-evidence.tar.gz` and `raw-members.json`: every leg log, command, answer file, answer check, cooling/telemetry/verdict record, runtime manifest, and validation log. Every exported member was checked against its recorded SHA-256. Frozen runtime binaries remain in the original worktree diagnostics; the rig removed only its reproducible source-checkout scratch after successful collection.

Replay the unmodified paired analysis without Java:

```sh
build/rig-python/bin/python3 bundles/sirix-query/bench/clickbench/rig/measure.py analyze \
  bundles/sirix-query/bench/clickbench/rig/evidence/aggregate-generic-20260909
```
