# High-cardinality aggregation: profile and correctness stop

Status: Firstmate resolved the correctness stop through a baseline counterfactual; see SUM_ORDERING_DEFECT.md. Both engines produce the same wrong sparse-SUM result. The known defect remains a disabled, non-passing regression. No speedup, paired result, or rank is claimed.

Baseline engine: `b815d459d1218ab9081f360257ae6f16eff9f476`, the campaign branch head verified through gh-axi. The prior generic experiment's RESULT.md was read from `fm/sirix-cb-agg-generic-1`; its mechanism was reverted and its reported family benefit was unresolved.

Two baseline 100M diagnostic JVMs completed under the rig exclusive lease, with the unchanged canonical envelope, 50 W pinned MSR limits, three tries per query, projection diagnostics, and JFR execution sampling requested at 2 ms. MemAvailable was checked immediately before each launch against 26 GiB. The first selected the seven target queries. Because selected-query context made q31 windowed I/O unrepresentative, the second ran all 43 in their normal order and retained lossless answer files. Neither is a scored leg. No DuckDB execution, reload, corpus mutation, or process termination was performed.

These figures use the second profile, averaging tries 2 and 3. CPU columns are inclusive execution/native-sample fractions, not wall fractions. Query windows use output-receipt timestamps minus rounded wall duration and are approximate. JIT inlining changes attribution between the selected and full-suite profiles; the raw recordings are retained.

| Query | Diagnostic wall s | Scan ms | Merge ms | Passes | Lookup samples | String value-merge samples | Top-K samples |
|---|---:|---:|---:|---:|---:|---:|---:|
| q12 | 0.729 | 147.0 | 51.0 | 1 | 1.8% | 57.4% | 0.9% |
| q14 | 0.776 | 191.5 | 52.0 | 1 | 13.4% | 47.4% | 1.9% |
| q16 | 1.538 | 619.5 | 206.5 | 2 | 33.5% | 23.6% | 1.8% |
| q18 | 2.988 | 1655.5 | 596.0 | 4 | 33.5% | 11.6% | 1.7% |
| q31 | 1.001 | 826.5 | 149.5 | 1 | 21.1% | 0.0% | 1.1% |
| q32 | 4.130 | 2691.0 | 1408.5 | 7 | 43.8% | 0.0% | 2.4% |
| q33 | 2.173 | 467.0 | 147.5 | 1 | 7.0% | 49.2% | 1.3% |

There is no single dominant cost across all seven. String canonicalization dominates q12/q14/q33; repeated table acquisition is the largest shared composite-group cost. q32 spends 25.0% of samples acquiring worker records and 18.8% acquiring records during merge. Top-K extraction is small. Shared partition-table rehash counters were zero for the recorded hot passes; q31 still sampled some worker-table rehashing. Spill/copy work is material (14.1% inclusive on q32), and q16/q18/q32 rescan the input in 2/4/7 bounded hash-range passes respectively. Worker scan spreads are narrow in the retained pass logs. The first selected-query profile's q31 hot wall was approximately 4.1 s, versus approximately 1.0 s in full-suite context; those are diagnostics, not a candidate comparison.

The prototype allows dense worker tables to retain duplicate partial records after an online 8,192-acquisition sample. It uses a bounded direct cache at intermediate cardinality and appends without probing when almost every sampled row was distinct. All records must merge into exact partition tables before selection. Low-cardinality worker tables retain exact probing. Distinct sinks are excluded. No extra input pass or persistent format change was added.

Core tests passed for complete accumulator equivalence across displaced records, same-hash exact identities, growth, spill and final-worker merging, partial pass ownership, and checked SUM overflow. The query integration test established serving and partial-group activation for COUNT ordering and COUNT ordering with SUM/AVG emission. A later case found a mismatch for SUM descending with sparse operands: expected the SUM-30 groups first; actual output began with the groups whose operands were entirely missing (SUM 0). The exact query and assertion are in `correctness-failure.txt`. The unchanged baseline was subsequently observed to fail identically, and its wrong output matches the prototype byte for byte. SUM_ORDERING_DEFECT.md retains the exact reproduction, baseline proof, and masking condition. Firstmate authorized continuing this lever without fixing the pre-existing defect; it remains explicitly non-passing.

Run allowance used: two baseline profiling JVMs; zero paired comparisons. No candidate 100M answer-equivalence proof has been completed.

Local evidence remains under `build/hicard/`: `profile-01/`, `profile-full-02/` (including the JFR recordings, exact commands, runtime manifest, logs, answers and telemetry), `ReadSamples.java`, `analyze-profile.py`, `profile-summary.py`, and the validation logs. The full-suite recording observed two platform-managed power-limit changes; the pinned MSR limits held. `profile-summary.json` retains the hot-window sample counts and pass timing totals.

## Redirect to pass-count budgeting

Firstmate inbox 002 redirected the unspent paired allowance to pass-count reduction, and inbox 003 accepted `PASS_BUDGET.md` and authorized its grouped-only allowance. The probing prototype is parked at commit `1703ebe2d` on `fm/sirix-cb-hicard-probing-prototype`; it is absent from the measured candidate. Its local mutation witness is archived with the profiles and is not validation of the budget change.

The budget candidate changes planning only. It charges dense payload plus three compact-index lanes, gives eligible bounded aggregates at most half the heap and three quarters of effective headroom, preserves the 2^26 cap, and leaves distinct and column-fill consumers unchanged. It retains the existing abort/restart and winner selection mechanisms. `validation.json` records 89 passing tests and one explicitly skipped known-defect test; raw logs/XML are in `validation.tar.gz`. `budget-origin.json` and `budget-origin.tar.gz` prove the budget candidate reproduces the exact same pre-existing sparse SUM-ordering error as the clean baseline. No new semantic mismatch is known from local validation; 100M all-43 equality remains the comparison's stop gate.
