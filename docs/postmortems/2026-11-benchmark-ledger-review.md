# Bug: benchmark acceptance and ledger boundary defects

- Fix / issue: review findings R1–R9 on `fm/sirix-perf-gate-statistics`; requirements CONV-302 and CONV-303.
- Symptom: a positive rounded allocation delta could pass; an unchanged fingerprint could not start a
  new baseline; dataset tiers could share a trend series; a run could erase an earlier stop point;
  aborted attempts could disappear; retrying a durable append could duplicate processes. Rejected
  allocation legs could hide a check, failed hot tries could become accepted figures, and a missing
  marker could allow local rejection of measurements already accepted in the ledger.
- Trigger: the worked examples and corresponding observable regressions below.
- Root cause: the tools applied acceptance, identity and durability rules at boundaries that lacked
  part of the contract. Fixes use the shared analysis, import and append boundaries.
- Why tests missed it: allocation tests pinned per-arm rounding, conditions tests forbade repeated
  fingerprints, importer tests omitted datasets, stop tests checked only the immediately preceding
  boundary, kit tests failed between queries, and commit tests assumed the marker followed append.
  Rejected allocation tests retained an accepted leg, hot-import tests omitted partial failures,
  and recovery tests checked duplicate identities without conflicting contents or late rejection.
- Regression test: the focused command is
  `python3 -B -m unittest test_timed_gate test_bench_ledger test_kit_recording test_bench_trend`
  from `bundles/sirix-query/bench/common`. The first focused run exposed a lost-arguments re-exec
  introduced while moving validation before the lock; preserving the original argument array fixes
  that ordinary path. It also corrected the ZERO fixture oracle: the raw median delta rounds to
  +25 bytes although the displayed arm figures differ by 24. No pre-fix execution or new mutation
  totals are claimed here.
- Lesson: [boundary contracts](../lessons.d/benchmark-ledger-boundaries.md), collected into
  [LESSONS.md](../LESSONS.md).

| Finding | Broken invariant and correction | Independent oracle and rejected wrong behavior |
|---|---|---|
| R1 | Round the difference of unrounded allocation medians, keeping display rounding separate. | CONV-302: 994.6128 → 995.2031 B/op yields +1 byte and fails; exact half-byte deltas use ties to even. Both compactMaterialize A/A allocation failures are fixture defects; timed verdicts stay unchanged. |
| R2 | Fingerprints identify captured fields, while the latest matching dated entry owns comparability. Remove permanent fingerprint ownership. | CONV-303 allows a new power-cap baseline for the same fingerprint. New runs use the latest id while prior run records keep theirs; unknown continuations and reused baseline ids remain invalid. |
| R3 | Dataset identity belongs in every query, suite, cold and hot series key. Carry import parameters from both kits. | CONV-303 and the maintainer decision require JSONBench `tier` and caller-stated ClickBench `dataset`; importing 1m and 100m remains distinct. Missing or path-valued dataset names are refused before engines run. |
| R4 | Every eligible earlier pair boundary must be considered, because precision is not monotone. | CONV-302: ten +0.05 log ratios already stop with a regression; appending ten −0.10 ratios must be refused even if the final threshold rises. |
| R5 | Completed raw attempts must be imported before exit commits, while imports remain outside timed attempt sequences. | CONV-303: failure or SIGTERM on Q1 attempt 2 preserves Q1 attempt 1 with its cold metric, process and dataset identity. Normal completion still records every attempt once. |
| R6 | A durable ledger append, rather than a later marker, determines which logical rows exist. Reconcile row identities under the append lock. | CONV-303: marker-write failure and a complete-line partial append are retried without changing existing bytes or adding duplicate run, measurement or verdict rows. |
| R7 | Record the measured allocation series before filtering rejected legs; require an accepted leg for both arms. | CONV-302: rejecting either arm or both arms refuses judgment and names the series and missing arm. No allocation legs, or legs outside the plan, still require no check. |
| R8 | Null hot tries are failures, not absent samples to filter out. Withhold query and suite hot figures at the shared importer. | CONV-303 and ClickBench's hot contract: `[0.039, 0.004, null]` keeps its cold figure but has no hot figure. `ImporterTest` and the stub-driven `ClickBenchColdRoundsTest` cover both row output and persisted exit-path evidence. |
| R9 | Rejection checks durable measurements; reconciliation compares contents for every identity before append, regardless of the marker. | CONV-303: a marker-write failure cannot permit later rejection. Conflicting run, measurement and verdict rows are refused with and without a marker, and an unrelated pending row is not appended alongside a conflict. Identical retries and rejection before measurement persistence still succeed. |

The changes preserve the vision's cost contract and measurement discipline: thresholds and pair
limits are unchanged, fixture values are untouched, and timing is never substituted for counted work.
The existing mutation figures predate this round and require remeasurement in the validation phase.
